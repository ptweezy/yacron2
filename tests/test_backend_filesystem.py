"""The shared-mount (filesystem) leader-election backend.

Unlike the etcd/kubernetes backends there is no network transport to fake:
the store IS a directory, so these tests drive real elections over a
``tmp_path`` shared by two backend instances.  Time is never slept on --
the wall clock is the ``yacron2.state._now`` /
``yacron2.backends.filesystem._wallclock`` seams and the monotonic gates
are ``yacron2.backends.filesystem._monotonic``, all patched together; the
renew loop's background task is never started (rounds are driven with
``await b._renew_once()``), matching the etcd test recipe.

Config-builder validation for the ``cluster.filesystem`` block lives in
tests/test_config_backends.py.
"""

import asyncio
import contextlib

import pytest

import yacron2.backends.filesystem as fsb_mod
import yacron2.state as state_mod
from yacron2.backends.filesystem import FilesystemBackend, display_name
from yacron2.config import ConfigError, parse_config_string
from yacron2.leadership import make_backend

# --- helpers ---------------------------------------------------------------


def _yaml(tmp_path, node="node-a", extra=""):
    path = str(tmp_path).replace("\\", "/")
    return (
        "cluster:\n"
        "  backend: filesystem\n"
        "  nodeName: " + node + "\n"
        "  filesystem:\n"
        "    path: " + path + "\n" + extra
    )


def _backend(tmp_path, node="node-a", jsid="v1:jobs", extra=""):
    cfg = parse_config_string(_yaml(tmp_path, node, extra), "").cluster_config
    return FilesystemBackend(cfg, lambda: jsid)


class _Clock:
    """One logical clock driving every time seam consistently."""

    def __init__(self, monkeypatch, wall=1_000_000.0, mono=100.0):
        self.wall = wall
        self.mono = mono
        monkeypatch.setattr(state_mod, "_now", lambda: self.wall)
        monkeypatch.setattr(fsb_mod, "_wallclock", lambda: self.wall)
        monkeypatch.setattr(fsb_mod, "_monotonic", lambda: self.mono)

    def advance(self, seconds):
        self.wall += seconds
        self.mono += seconds


async def _started(backend):
    # start only the embedded store (never start(), whose probe/round/task
    # lifecycle is tested separately); rounds are driven by hand.
    await backend._store.start()
    return backend


async def _stop(*backends):
    for b in backends:
        with contextlib.suppress(Exception):
            await b._store.stop()


# --- pure helpers ----------------------------------------------------------


def test_display_name_strips_process_token():
    assert display_name("node-a#deadbeef0123") == "node-a"
    assert display_name(None) is None
    # an unnameable holder must never surface as None: leader_name() None
    # reads as "run anyway" and double-runs PreferLeader fleet-wide.
    assert display_name("#deadbeef0123") == fsb_mod._UNKNOWN_HOLDER


def test_holder_token_is_process_unique(tmp_path):
    a1 = _backend(tmp_path)
    a2 = _backend(tmp_path)
    # duplicate nodeNames (a scaled deployment) must not share holder
    # strings, or one replica would adopt the other's lease.
    assert a1._holder_token != a2._holder_token
    assert a1._holder_token.startswith("node-a#")


def test_cadence_derivation(tmp_path):
    b = _backend(tmp_path, extra="    ttl: 15\n")
    assert b.ttl == 15.0
    assert b.renew_period == 5.0
    # round_deadline + renew_period <= ttl - skew: the gap between two
    # successful renews stays inside the lease window.
    assert b.round_deadline + b.renew_period <= b.ttl - 1.0 + 1e-9


# --- election rounds over one shared directory -----------------------------


async def test_first_round_elects_and_second_node_defers(
    tmp_path, monkeypatch
):
    clock = _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    b = await _started(_backend(tmp_path, "node-b"))
    try:
        await a._renew_once()
        assert a.is_leader() is True
        assert a.is_quorate() is True
        assert a.leader_name() == "node-a"
        clock.advance(1.0)
        await b._renew_once()
        assert b.is_leader() is False
        assert b.is_quorate() is True  # a fresh, positive read of the store
        assert b.leader_name() == "node-a"
        # a quorate follower that can see the holder defers PreferLeader
        assert b.is_available_leader() is False
        assert a.is_available_leader() is True
    finally:
        await _stop(a, b)


async def test_renew_keeps_fence_takeover_bumps_it(tmp_path, monkeypatch):
    clock = _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    b = await _started(_backend(tmp_path, "node-b"))
    try:
        await a._renew_once()
        first_fence = a._lease.fence
        clock.advance(5.0)
        await a._renew_once()  # a same-holder renew of a valid lease
        assert a._lease.fence == first_fence
        # expire far beyond the challenger's skew margin, then B campaigns
        clock.advance(a.ttl + 5.0)
        await b._renew_once()
        assert b.is_leader() is True
        assert b._lease.fence == first_fence + 1
        # A's next round positively observes the takeover -> follower
        await a._renew_once()
        assert a.is_leader() is False
        assert a.leader_name() == "node-b"
    finally:
        await _stop(a, b)


async def test_challenger_waits_out_the_skew_margin(tmp_path, monkeypatch):
    # the takeover margin: an expiry that is not yet a full _SKEW_SECONDS
    # in the past (by the challenger's clock) is treated as live, so a
    # holder whose clock trails ours keeps its lease.
    clock = _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    b = await _started(_backend(tmp_path, "node-b"))
    try:
        await a._renew_once()
        clock.advance(a.ttl + fsb_mod._SKEW_SECONDS / 2)  # expired < margin
        await b._renew_once()
        assert b.is_leader() is False
        assert b.leader_name() == "node-a"  # observed, deferred to
        clock.advance(fsb_mod._SKEW_SECONDS)  # now past the margin
        await b._renew_once()
        assert b.is_leader() is True
    finally:
        await _stop(a, b)


async def test_self_demotion_is_monotonic_and_local(tmp_path, monkeypatch):
    clock = _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    try:
        await a._renew_once()
        assert a.is_leader() is True
        # the fence lapses a skew margin BEFORE the wall expiry, with no
        # store I/O; the raw win flag is kept so the never-skip
        # self-demotion window holds PreferLeader on this node.
        clock.mono += a.ttl  # monotonic only: a stalled renew loop
        assert a.is_leader() is False
        assert a._is_self_demoted_holder() is True
        assert a.is_available_leader() is True
    finally:
        await _stop(a)


async def test_quorum_is_a_fixed_deadline(tmp_path, monkeypatch):
    clock = _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    try:
        await a._renew_once()
        assert a.is_quorate() is True
        clock.mono += a.ttl + 1.0
        # no store contact since: the freshness window lapsed, and Leader
        # jobs fail closed while PreferLeader runs (never-skip).
        assert a.is_quorate() is False
        assert a.leader_name() is None
        assert a.is_available_leader() is True
    finally:
        await _stop(a)


async def test_renew_refused_sets_lease_lost_keeps_raw_flag(
    tmp_path, monkeypatch
):
    clock = _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    b = await _started(_backend(tmp_path, "node-b"))
    try:
        await a._renew_once()
        clock.advance(a.ttl + 5.0)
        await b._renew_once()  # B takes over the expired lease
        # A's renew is refused; the same round then positively observes B,
        # so A lands as a genuine follower (win flag cleared by the round).
        await a._renew_once()
        assert a._lease_lost is False  # cleared by the applied round
        assert a._is_leader is False
        assert a.is_leader() is False
    finally:
        await _stop(a, b)


async def test_unreadable_store_extends_no_quorum(tmp_path, monkeypatch):
    # a lease API that answers None everywhere (fail-closed store) must NOT
    # count as store contact: quorum lapses instead of a sick node staying
    # quorate with holder None (which would double-run PreferLeader on
    # every such node).
    clock = _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    try:
        await a._renew_once()
        assert a.is_quorate() is True

        async def _none(*_args, **_kw):
            return None

        monkeypatch.setattr(a._store, "acquire_lease", _none)
        monkeypatch.setattr(a._store, "renew_lease", _none)
        monkeypatch.setattr(a._store, "read_lease", _none)
        clock.advance(a.ttl + 5.0)
        await a._renew_once()
        assert a.is_quorate() is False
    finally:
        await _stop(a)


async def test_adopts_own_lease_after_abandoned_acquire(
    tmp_path, monkeypatch
):
    # the documented timeout-is-UNKNOWN case: an acquire abandoned by its
    # timeout lands on disk anyway; the next round's read recognises OUR
    # holder token and adopts the lease instead of treating it as foreign.
    _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    try:
        landed = await a._store.acquire_lease(
            a.election_name, a._holder_token, a.ttl
        )
        assert landed is not None
        assert a._lease is None  # the backend never saw it
        await a._renew_once()
        assert a._lease is not None
        assert a._lease.holder == a._holder_token
    finally:
        await _stop(a)


# --- @reboot-ran persistence ------------------------------------------------


async def test_reboot_ran_round_trips_between_nodes(tmp_path, monkeypatch):
    _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a", jsid="v1:s1"))
    b = await _started(_backend(tmp_path, "node-b", jsid="v1:s1"))
    try:
        await a.mark_reboot_ran("boot-job")
        assert a.reboot_ran("boot-job") is True
        await b._refresh_reboot_ran()
        # a failover holder must not re-run the one-shot
        assert b.reboot_ran("boot-job") is True
    finally:
        await _stop(a, b)


async def test_reboot_ran_scoped_to_job_set_id(tmp_path, monkeypatch):
    _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a", jsid="v1:s1"))
    c = await _started(_backend(tmp_path, "node-c", jsid="v1:s2"))
    try:
        await a.mark_reboot_ran("boot-job")
        await c._refresh_reboot_ran()
        # a mark recorded under another job set must not suppress the
        # (redefined) one-shot -- may-re-run is the safe direction.
        assert c.reboot_ran("boot-job") is False
    finally:
        await _stop(a, c)


async def test_reboot_ran_appends_are_unioned(tmp_path, monkeypatch):
    # append-only records union by construction: two nodes marking
    # different one-shots can never clobber each other's marks.
    _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a", jsid="v1:s1"))
    b = await _started(_backend(tmp_path, "node-b", jsid="v1:s1"))
    try:
        await a.mark_reboot_ran("job-a")
        await b.mark_reboot_ran("job-b")
        await a._refresh_reboot_ran()
        assert a.reboot_ran("job-a") is True
        assert a.reboot_ran("job-b") is True
    finally:
        await _stop(a, b)


async def test_takeover_refreshes_ran_set_before_leading(
    tmp_path, monkeypatch
):
    # CRITICAL regression: a takeover must force the ran-set re-read even
    # when the periodic throttle is nowhere near due, and it must complete
    # BEFORE leadership is usable -- a failover leader whose cache
    # predates the old leader's mark would otherwise re-run the one-shot.
    clock = _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a", jsid="v1:s1"))
    b = await _started(_backend(tmp_path, "node-b", jsid="v1:s1"))
    try:
        await a._renew_once()
        clock.advance(1.0)
        await b._renew_once()  # follower: cache read while stream is empty
        assert b._reboot_ran_synced is True
        await a.mark_reboot_ran("boot-job")  # the old leader's boot run
        # expire A well inside B's 60s refresh throttle, then B takes over
        clock.advance(a.ttl + 5.0)
        await b._renew_once()
        assert b.is_leader() is True
        assert b.reboot_ran("boot-job") is True  # no double-fire
    finally:
        await _stop(a, b)


async def test_takeover_with_failing_ran_refresh_defers_one_shots(
    tmp_path, monkeypatch
):
    # CRITICAL regression: when the takeover's forced ran-set re-read
    # FAILS, the new leader must not answer "not ran" from its stale cache
    # (cron would launch the one-shot the old leader already marked).  The
    # conservative answer is a raise -- cron's guard keeps the one-shot
    # pending -- and the refresh throttle must NOT be pre-advanced, so the
    # very next round retries and recovers.
    clock = _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a", jsid="v1:s1"))
    b = await _started(_backend(tmp_path, "node-b", jsid="v1:s1"))
    try:
        await a._renew_once()
        clock.advance(1.0)
        await b._renew_once()  # follower: last good refresh predates the mark
        throttle_before = b._reboot_refresh_next
        await a.mark_reboot_ran("boot-job")  # the old leader's boot run
        clock.advance(a.ttl + 5.0)  # still inside B's refresh throttle

        broken = {"on": True}
        real_list = b._store.list_records

        async def _list(*args, **kw):
            if broken["on"]:
                raise OSError("mount recovering")
            return await real_list(*args, **kw)

        monkeypatch.setattr(b._store, "list_records", _list)
        await b._renew_once()  # takeover; the forced re-read fails
        assert b.is_leader() is True
        # the answer is UNKNOWN, never a stale False: cron treats the raise
        # as "not known to have run" and keeps the one-shot pending.
        with pytest.raises(fsb_mod.RebootRanUnknownError):
            b.reboot_ran("boot-job")
        # positive answers stay available while the gate is closed: a mark
        # this node made itself is not deferred.
        await b.mark_reboot_ran("b-local")
        assert b.reboot_ran("b-local") is True
        # the throttle was NOT pre-advanced by the failed attempt
        assert b._reboot_refresh_next == throttle_before
        # a non-holder is never gated: a stale False cannot launch there
        # (cron's _cluster_allows refuses a follower), so no raise.
        c = _backend(tmp_path, "node-c", jsid="v1:s1")
        assert c._reboot_ran_synced is False
        assert c.reboot_ran("boot-job") is False
        # store recovers: the NEXT round's retry re-reads and recovers
        broken["on"] = False
        clock.advance(b.renew_period)
        await b._renew_once()
        assert b.is_leader() is True
        assert b.reboot_ran("boot-job") is True  # no double-fire, no loss
    finally:
        await _stop(a, b)


# --- lock-fidelity probe -----------------------------------------------------


async def test_verify_locking_passes_on_a_real_directory(tmp_path):
    backend = _backend(tmp_path)
    await backend._store.start()
    try:
        assert await backend._store.verify_locking() is None
    finally:
        await _stop(backend)


async def test_verify_locking_detects_noop_locks(tmp_path, monkeypatch):
    backend = _backend(tmp_path)
    await backend._store.start()
    try:

        @contextlib.contextmanager
        def _noop_lock(fileno, *, blocking=True):
            yield  # grants every request: exactly the fiction to catch

        monkeypatch.setattr(state_mod, "exclusive_file_lock", _noop_lock)
        reason = await backend._store.verify_locking()
        assert reason is not None
        assert "no-ops" in reason
    finally:
        await _stop(backend)


async def test_verify_locking_detects_local_lock_mounts(
    tmp_path, monkeypatch
):
    backend = _backend(tmp_path)
    await backend._store.start()
    try:
        monkeypatch.setattr(
            state_mod, "_mount_entry", lambda path: ("nfs4", "rw,nolock")
        )
        reason = await backend._store.verify_locking()
        assert reason is not None
        assert "nolock" in reason
    finally:
        await _stop(backend)


def test_local_lock_reason_parses_mount_options(monkeypatch):
    cases = [
        (("nfs4", "rw,relatime,local_lock=flock"), "local_lock=flock"),
        (("nfs4", "rw,local_lock=all"), "local_lock=all"),
        (("nfs4", "rw,nolock,addr=10.0.0.1"), "nolock"),
        (("nfs4", "rw,local_lock=none"), None),
        (("nfs4", "rw,local_lock=posix"), None),  # flock still remote
        (("ext4", "rw,nolock"), None),  # not NFS: option is foreign
        (None, None),  # no /proc: cannot tell
    ]
    for entry, expect in cases:
        monkeypatch.setattr(state_mod, "_mount_entry", lambda p, e=entry: e)
        reason = state_mod._local_lock_reason("/anywhere")
        if expect is None:
            assert reason is None
        else:
            assert reason is not None and expect in reason


async def test_start_hard_refuses_untrusted_locks(tmp_path, monkeypatch):
    backend = _backend(tmp_path)

    async def _bad_locks(self):
        return "the mount lies about its locks"

    monkeypatch.setattr(
        state_mod.FilesystemStateBackend, "verify_locking", _bad_locks
    )
    with pytest.raises(ConfigError, match="refusing to elect"):
        await backend.start()
    # half-started state was cleaned up; nothing left to stop.
    assert backend._task is None


# --- lifecycle ---------------------------------------------------------------


async def test_start_stop_lifecycle_releases_the_lease(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    try:
        # the start contract ran one best-effort round: leadership is real
        # before the first spawn pass.
        assert backend.is_leader() is True
    finally:
        await backend.stop()
    assert backend.is_leader() is False
    # released (marked expired in place), so a successor acquires at once
    # and the fence keeps counting from where it was.
    other = _backend(tmp_path, "node-b")
    await other._store.start()
    try:
        lease = await other._store.acquire_lease(
            other.election_name, other._holder_token, other.ttl
        )
        assert lease is not None
        assert lease.fence >= 2
    finally:
        await _stop(other)


async def test_stop_survives_release_failure_on_a_sick_mount(
    tmp_path, monkeypatch
):
    # CRITICAL regression: stop() runs unguarded inside cron.run()'s
    # shutdown sequence, and the lease release is a best-effort courtesy
    # write -- a sick/vanished mount raises OSError (ESTALE/EIO/ENOENT)
    # IMMEDIATELY rather than timing out, and an escape here would skip
    # the job drain, the DAG shutdown and the state flush. Nothing the
    # release raises may abort a shutdown; TTL expiry is the fallback.
    backend = _backend(tmp_path)
    await backend.start()
    assert backend.is_leader() is True

    async def _sick(_lease):
        raise OSError("stale file handle")

    monkeypatch.setattr(backend._store, "release_lease", _sick)
    await backend.stop()  # must complete without raising
    assert backend.is_leader() is False
    assert backend._task is None


async def test_view_dict_and_lease_detail_shape(tmp_path, monkeypatch):
    _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    try:
        await a._renew_once()
        view = a.view_dict()
        assert view["backend"] == "filesystem"
        assert view["is_leader"] is True
        assert view["leader"] == "node-a"
        detail = view["lease"]
        assert detail["electionName"] == "cluster/leader"
        assert detail["identity"] == "node-a"
        assert detail["holder"] == "node-a"
        assert detail["fence"] == 1
        assert detail["expiry"] is not None
        assert detail["path"] == a._store.root
    finally:
        await _stop(a)


def test_make_backend_dispatches_filesystem(tmp_path):
    cfg = parse_config_string(_yaml(tmp_path), "").cluster_config
    backend = make_backend(cfg, lambda: "v1:jobs")
    assert isinstance(backend, FilesystemBackend)
    assert backend.node_name == "node-a"
    assert backend.distribution == "single-leader"


async def test_renew_loop_survives_store_errors(tmp_path, monkeypatch):
    # the loop must treat store trouble as a transient round failure (warn,
    # let the deadlines lapse), never die on it.
    _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    try:

        async def _boom():
            raise OSError("mount gone")

        monkeypatch.setattr(a, "_renew_once", _boom)
        task = asyncio.create_task(a._renew_loop())
        await asyncio.sleep(0)  # one loop tick: the round raised inside
        await asyncio.sleep(0)
        assert not task.done()  # survived
        # Tear the loop down the way production stop() does: set the stop
        # event FIRST, then cancel. A raw cancel() of a task parked in
        # asyncio.wait_for(Event.wait(), ...) deadlocks on Python <= 3.11
        # (whose wait_for predates the 3.12 asyncio.timeout reimplementation);
        # setting the event unblocks the inner wait so the loop exits cleanly.
        a._stop.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        assert task.done()  # cancellable while alive, exits on stop
    finally:
        await _stop(a)


async def test_wedged_renew_does_not_re_extend_fence_from_read(
    tmp_path, monkeypatch
):
    # CRITICAL regression: when the locked renew write is wedged (times
    # out), the round changes nothing and returns -- it must NOT fall
    # through to the unlocked read and re-extend the leadership fence,
    # which would keep this node "leader" past the frozen on-disk expiry
    # while a challenger takes the lease after the skew margin (two
    # leaders). A transient timeout is survived (the fence from the last
    # good renew has not lapsed); a persistent one self-demotes purely on
    # the monotonic deadline, never ratcheted forward off a read.
    clock = _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    try:
        await a._renew_once()
        assert a.is_leader() is True
        deadline = a._lease_deadline_mono

        async def _timeout(*_args, **_kw):
            raise asyncio.TimeoutError

        monkeypatch.setattr(a._store, "renew_lease", _timeout)
        # transient: still inside the last good renew's window -> leader
        clock.advance(a.renew_period)
        await a._renew_once()
        assert a._lease is not None  # kept for the next locked renew retry
        assert a._lease_deadline_mono == deadline  # fence untouched
        assert a.is_leader() is True  # survived the transient timeout
        # persistent: advance past the monotonic fence deadline
        clock.advance(a.ttl)
        await a._renew_once()
        assert a._lease_deadline_mono == deadline  # STILL not re-extended
        assert a.is_leader() is False  # self-demoted with no I/O
    finally:
        await _stop(a)


async def test_confirm_after_denied_acquire_does_not_assert_leadership(
    tmp_path, monkeypatch
):
    # a denied acquire whose confirming (unlocked) read shows our own token
    # -- an earlier abandoned acquire landed -- adopts the lease but must
    # not grant leadership from the read; the next locked renew does.
    clock = _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    try:
        # an abandoned acquire already on disk under our token
        landed = await a._store.acquire_lease(
            a.election_name, a._holder_token, a.ttl
        )
        assert landed is not None

        async def _deny(*_args, **_kw):
            return None  # every acquire is denied this round

        monkeypatch.setattr(a._store, "acquire_lease", _deny)
        # force the campaign path: the observe read must not short-circuit
        # into the own-token adoption branch, so hide the lease from the
        # first read but reveal it to the confirm read.
        reads = {"n": 0}
        real_read = a._store.read_lease

        async def _read(name):
            reads["n"] += 1
            if reads["n"] == 1:
                return None  # observe: looks absent -> campaign
            return await real_read(name)  # confirm: our token

        monkeypatch.setattr(a._store, "read_lease", _read)
        clock.advance(1.0)
        await a._renew_once()
        assert a._lease is not None  # adopted
        assert a._lease_deadline_mono is None  # leadership not asserted
        assert a.is_leader() is False
    finally:
        await _stop(a)
