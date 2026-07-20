"""The shared-mount (filesystem) leader-election backend.

Unlike the etcd/kubernetes backends there is no network transport to fake:
the store IS a directory, so these tests drive real elections over a
``tmp_path`` shared by two backend instances.  Time is never slept on --
the wall clock is the ``cronstable.state._now`` /
``cronstable.backends.filesystem._wallclock`` seams and the monotonic gates
are ``cronstable.backends.filesystem._monotonic``, all patched together; the
renew loop's background task is never started (rounds are driven with
``await b._renew_once()``), matching the etcd test recipe.

Config-builder validation for the ``cluster.filesystem`` block lives in
tests/test_config_backends.py.
"""

import asyncio
import contextlib
import datetime
import logging

import pytest

import cronstable.backends.filesystem as fsb_mod
import cronstable.state as state_mod
from cronstable.backends.filesystem import FilesystemBackend, display_name
from cronstable.config import ConfigError, parse_config_string
from cronstable.leadership import make_backend

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


def _failing_list_records(monkeypatch, backend):
    """Break the backend store's ran-stream listing; return a call counter."""
    calls = {"n": 0}

    async def _list(*args, **kw):
        calls["n"] += 1
        raise OSError("records unreadable")

    monkeypatch.setattr(backend._store, "list_records", _list)
    return calls


def _ran_read_failures(caplog):
    return [
        r
        for r in caplog.records
        if "could not read the @reboot-ran set" in r.getMessage()
    ]


async def test_follower_failing_ran_read_is_debug_and_throttled(
    tmp_path, monkeypatch, caplog
):
    # regression: a permanent follower whose ran-stream read persistently
    # fails (e.g. a records/ subtree owned by a different user) used to
    # emit the leader-worded "deferring pending @reboot one-shots" WARNING
    # every renew round.  A follower defers nothing (reboot_ran's raise
    # gate is leader-gated) and only needs the set by takeover -- which
    # forces its own re-read -- so it logs DEBUG and retries the read at
    # most once per _REBOOT_RAN_REFRESH.
    clock = _Clock(monkeypatch)
    ttl = "    ttl: 30\n"
    a = await _started(_backend(tmp_path, "node-a", jsid="v1:s1", extra=ttl))
    b = await _started(_backend(tmp_path, "node-b", jsid="v1:s1", extra=ttl))
    try:
        calls = _failing_list_records(monkeypatch, b)
        caplog.set_level(
            logging.DEBUG, logger="cronstable.backends.filesystem"
        )
        for _ in range(6):  # spans just under one refresh period
            await a._renew_once()  # A keeps leading throughout
            await b._renew_once()
            assert b.is_leader() is False
            clock.advance(10.0)
        assert calls["n"] == 1  # ONE probe in the period, not one per round
        failures = _ran_read_failures(caplog)
        assert failures and all(
            r.levelno == logging.DEBUG for r in failures
        )
        assert not any(
            "deferring pending @reboot one-shots" in r.getMessage()
            for r in caplog.records
        )
        # the next period gets exactly one more retry
        await a._renew_once()
        await b._renew_once()
        assert calls["n"] == 2
        assert all(
            r.levelno == logging.DEBUG for r in _ran_read_failures(caplog)
        )
    finally:
        await _stop(a, b)


async def test_leader_failing_ran_read_warns_once_per_period(
    tmp_path, monkeypatch, caplog
):
    # a holder really IS deferring its one-shots, so the WARNING stays --
    # but at most once per _REBOOT_RAN_REFRESH; the read itself must keep
    # retrying EVERY round (the takeover-recovery cadence pinned by
    # test_takeover_with_failing_ran_refresh_defers_one_shots).
    clock = _Clock(monkeypatch)
    ttl = "    ttl: 30\n"
    a = await _started(_backend(tmp_path, "node-a", jsid="v1:s1", extra=ttl))
    try:
        calls = _failing_list_records(monkeypatch, a)
        caplog.set_level(
            logging.DEBUG, logger="cronstable.backends.filesystem"
        )
        for _ in range(6):  # spans just under one warn period
            await a._renew_once()
            assert a.is_leader() is True
            clock.advance(10.0)
        assert calls["n"] == 6  # the read retried every round, unthrottled

        def _warnings():
            return [
                r
                for r in _ran_read_failures(caplog)
                if r.levelno == logging.WARNING
            ]

        assert len(_warnings()) == 1  # once per period, not per round
        assert "deferring pending @reboot one-shots" in (
            _warnings()[0].getMessage()
        )
        # a period later the (still failing, still deferring) leader may
        # surface it again
        await a._renew_once()
        assert calls["n"] == 7
        assert len(_warnings()) == 2
    finally:
        await _stop(a)


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


# --- _renew_once state machine and pure-helper edge cases -------------------


def test_utcnow_is_tz_aware_utc():
    now = fsb_mod._utcnow()
    assert isinstance(now, datetime.datetime)
    # deadlines/display must be an aware UTC stamp, never naive local time.
    assert now.tzinfo is datetime.timezone.utc


def test_is_quorate_false_before_any_round(tmp_path):
    # a fresh backend has never contacted the store: the freshness deadline
    # is unset, so it is not quorate and names no leader (Leader jobs fail
    # closed, PreferLeader runs -- the documented cold-start posture).
    a = _backend(tmp_path)
    assert a._quorum_deadline_mono is None
    assert a.is_quorate() is False
    assert a.leader_name() is None


def test_apply_round_without_expiry_clears_display_deadline(tmp_path):
    # a positive round that carries no wall-clock expiry (expires_at None)
    # clears the dashboard deadline and, being a non-leader outcome, drops
    # the held lease and the monotonic fence; the holder falls back to the
    # unknown-holder sentinel, never None.
    a = _backend(tmp_path)
    a._lease_deadline = datetime.datetime.now(datetime.timezone.utc)
    a._apply_round(None, False, None, None)
    assert a._lease_deadline is None
    assert a._lease_deadline_mono is None
    assert a._holder == fsb_mod._UNKNOWN_HOLDER
    assert a._quorum_deadline_mono is not None  # freshness always extends


async def test_first_round_elects_and_follower_defers(tmp_path, monkeypatch):
    clock = _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    b = await _started(_backend(tmp_path, "node-b"))
    try:
        await a._renew_once()  # campaign -> acquire
        assert a.is_leader() is True
        assert a.leader_name() == "node-a"
        assert a._lease is not None
        clock.advance(1.0)
        await b._renew_once()  # observe a live foreign holder -> defer
        assert b.is_leader() is False
        assert b.is_quorate() is True
        assert b.leader_name() == "node-a"
    finally:
        await _stop(a, b)


async def test_holding_renew_keeps_fence_no_regain(tmp_path, monkeypatch):
    # a same-holder renew of a valid lease is the not-gaining path: the
    # fence is preserved and leadership never re-syncs the ran-set.
    clock = _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    try:
        await a._renew_once()
        first_fence = a._lease.fence
        clock.advance(a.renew_period)
        await a._renew_once()
        assert a.is_leader() is True
        assert a._lease.fence == first_fence  # renew, not a takeover bump
    finally:
        await _stop(a)


async def test_renew_refused_sets_lease_lost_then_follows(
    tmp_path, monkeypatch
):
    # B takes over A's long-expired lease; A's next renew is positively
    # refused (renew_lease -> None), so A marks the lease lost and drops it,
    # then the same round's read observes B and A lands a clean follower.
    clock = _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    b = await _started(_backend(tmp_path, "node-b"))
    try:
        await a._renew_once()
        clock.advance(a.ttl + 5.0)
        await b._renew_once()  # takeover
        assert b.is_leader() is True
        await a._renew_once()  # renew refused -> lease_lost, then observe B
        assert a._lease is None
        assert a.is_leader() is False
        assert a.leader_name() == "node-b"
    finally:
        await _stop(a, b)


async def test_adopts_own_lease_then_next_renew_regains(
    tmp_path, monkeypatch
):
    # an acquire abandoned by its timeout landed on disk under our token.
    # Round one's unlocked read recognises the token and ADOPTS the lease
    # object without asserting leadership (is_leader stays False); round two
    # then renews that adopted lease under the lock and GAINS leadership --
    # the renew-gaining branch that forces a ran-set re-read before applying.
    _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    try:
        landed = await a._store.acquire_lease(
            a.election_name, a._holder_token, a.ttl
        )
        assert landed is not None
        assert a._lease is None

        await a._renew_once()  # observe own token -> adopt, not leader yet
        assert a._lease is not None
        assert a.is_leader() is False

        await a._renew_once()  # locked renew of the adopted lease -> gain
        assert a.is_leader() is True
        assert a._reboot_ran_synced is True
        assert a.leader_name() == "node-a"
    finally:
        await _stop(a)


async def test_denied_acquire_confirm_adopts_own_token(tmp_path, monkeypatch):
    # a denied acquire whose confirming (unlocked) read shows OUR token --
    # an earlier abandoned acquire landed -- adopts the lease but must not
    # grant leadership from the read; the next locked renew does.
    clock = _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    try:
        landed = await a._store.acquire_lease(
            a.election_name, a._holder_token, a.ttl
        )
        assert landed is not None

        async def _deny(*_args, **_kw):
            return None  # every acquire denied this round

        monkeypatch.setattr(a._store, "acquire_lease", _deny)

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
        assert a._lease is not None  # adopted via the confirm read
        assert a._lease_deadline_mono is None  # leadership not asserted
        assert a.is_leader() is False
    finally:
        await _stop(a)


async def test_renew_timeout_leaves_state_to_its_deadlines(
    tmp_path, monkeypatch
):
    # a locked renew that TIMES OUT is UNKNOWN, not refused: the round must
    # change nothing and return (never fall through to the unlocked read and
    # re-extend the fence off a read), keeping _lease for the next retry.
    clock = _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    try:
        await a._renew_once()
        assert a.is_leader() is True
        held = a._lease
        deadline = a._lease_deadline_mono

        async def _timeout(*_args, **_kw):
            raise asyncio.TimeoutError

        monkeypatch.setattr(a._store, "renew_lease", _timeout)
        clock.advance(a.renew_period)
        await a._renew_once()
        assert a._lease is held  # kept for the next locked-renew retry
        assert a._lease_deadline_mono == deadline  # fence untouched by a read
        assert a.is_leader() is True  # survived the transient timeout
    finally:
        await _stop(a)


async def test_observe_read_timeout_changes_nothing(tmp_path, monkeypatch):
    # a non-holder whose observe read times out learns nothing: the round
    # returns without extending any deadline, so the node stays unquorate.
    _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    try:

        async def _timeout(*_args, **_kw):
            raise asyncio.TimeoutError

        monkeypatch.setattr(a._store, "read_lease", _timeout)
        await a._renew_once()
        assert a.is_leader() is False
        assert a.is_quorate() is False
    finally:
        await _stop(a)


async def test_acquire_timeout_changes_nothing(tmp_path, monkeypatch):
    # the store is empty (observe reads absent), the campaign acquire times
    # out (UNKNOWN, not denied): change nothing and return -- an own-holder
    # adoption next round self-heals if the write actually landed.
    _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    try:

        async def _timeout(*_args, **_kw):
            raise asyncio.TimeoutError

        monkeypatch.setattr(a._store, "acquire_lease", _timeout)
        await a._renew_once()  # observe absent -> campaign -> acquire times out
        assert a.is_leader() is False
        assert a.is_quorate() is False
        assert a._lease is None
    finally:
        await _stop(a)


async def test_denied_acquire_with_absent_confirm_stays_follower(
    tmp_path, monkeypatch
):
    # a denied acquire whose confirming read also answers nothing (the store
    # failed closed, not a lost race): no positive observation, so no
    # deadline extends and the node is left unquorate.
    _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    try:

        async def _none(*_args, **_kw):
            return None

        monkeypatch.setattr(a._store, "read_lease", _none)  # observe + confirm
        monkeypatch.setattr(a._store, "acquire_lease", _none)  # denied
        await a._renew_once()
        assert a.is_leader() is False
        assert a.is_quorate() is False
    finally:
        await _stop(a)


async def test_denied_acquire_confirm_foreign_holder_follows(
    tmp_path, monkeypatch
):
    # the lost-race path: observe read saw the slot absent, our acquire was
    # denied, and the confirming read shows a FOREIGN holder (a rival won).
    # We adopt no lease (not our token) but the read is store contact, so we
    # land a quorate follower deferring to the rival.
    clock = _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    b = await _started(_backend(tmp_path, "node-b"))
    try:
        await b._renew_once()  # B genuinely holds the lease on disk
        assert b.is_leader() is True

        async def _deny(*_args, **_kw):
            return None  # A's acquire is denied this round

        monkeypatch.setattr(a._store, "acquire_lease", _deny)
        reads = {"n": 0}
        real_read = a._store.read_lease

        async def _read(name):
            reads["n"] += 1
            if reads["n"] == 1:
                return None  # observe: looks absent -> campaign
            return await real_read(name)  # confirm: B's foreign lease

        monkeypatch.setattr(a._store, "read_lease", _read)
        clock.advance(1.0)
        await a._renew_once()
        assert a._lease is None  # a foreign token is never adopted
        assert a.is_leader() is False
        assert a.is_quorate() is True  # the confirm read counts as contact
        assert a.leader_name() == "node-b"
    finally:
        await _stop(a, b)


async def test_confirm_read_timeout_changes_nothing(tmp_path, monkeypatch):
    # a denied acquire whose confirming read TIMES OUT is UNKNOWN: the round
    # returns without extending quorum.
    _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    try:

        async def _deny(*_args, **_kw):
            return None

        monkeypatch.setattr(a._store, "acquire_lease", _deny)
        reads = {"n": 0}

        async def _read(name):
            reads["n"] += 1
            if reads["n"] == 1:
                return None  # observe: absent -> campaign
            raise asyncio.TimeoutError  # confirm times out

        monkeypatch.setattr(a._store, "read_lease", _read)
        await a._renew_once()
        assert a.is_leader() is False
        assert a.is_quorate() is False
    finally:
        await _stop(a)


# --- start's first-round-failure path --------------------------------------


async def test_start_survives_first_round_failure_unquorate(
    tmp_path, monkeypatch, caplog
):
    # start() runs one bounded best-effort round so state is real before the
    # first spawn pass; an OSError there must be swallowed (logged) and the
    # node just starts unquorate rather than aborting start.
    backend = _backend(tmp_path)

    async def _boom():
        raise OSError("mount not answering yet")

    monkeypatch.setattr(backend, "_renew_once", _boom)
    caplog.set_level(logging.WARNING, logger="cronstable.backends.filesystem")
    await backend.start()
    try:
        assert backend._task is not None  # the renew loop was still launched
        assert backend.is_quorate() is False
        assert any(
            "first round did not complete" in r.getMessage()
            for r in caplog.records
        )
    finally:
        await backend.stop()


# --- the renew loop: error handling and inter-round sleep ------------------


async def test_renew_loop_warns_and_survives_store_error(
    tmp_path, monkeypatch, caplog
):
    # a round that raises OSError is a transient failure: the loop warns and
    # keeps going (the fixed quorum deadline lapses, the next round retries).
    _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    try:

        rounds = {"n": 0}

        async def _boom():
            rounds["n"] += 1
            raise OSError("mount gone")

        monkeypatch.setattr(a, "_renew_once", _boom)
        monkeypatch.setattr(
            type(a), "renew_period", property(lambda self: 0.001)
        )
        caplog.set_level(
            logging.WARNING, logger="cronstable.backends.filesystem"
        )
        task = asyncio.create_task(a._renew_loop())
        try:
            # Poll for the outcome instead of counting `sleep(0)` hops: on
            # 3.11 and older, `asyncio.wait_for` wraps the coroutine in a
            # Task, so the raise takes one more loop iteration to surface
            # than on 3.12+, where `wait_for` awaits the coroutine inline.
            #
            # The behaviour this test is named for: the loop RAN AGAIN after
            # the raising round rather than merely staying alive. Neither
            # `task.done()` nor `task.cancelled() or task.exception() is
            # None` could show that; both are true by construction after an
            # awaited cancel, whatever the loop did.
            for _ in range(2000):
                if rounds["n"] >= 2:
                    break
                await asyncio.sleep(0.001)
            assert not task.done()  # survived the raising round
            assert any(
                "election round failed" in r.getMessage()
                for r in caplog.records
            )
            assert (
                rounds["n"] >= 2
            ), "the loop did not run again after the raise"
        finally:
            # Cancel in `finally`: a failing assertion above must not leak a
            # loop still spinning at the patched 1ms cadence, which wedges
            # the whole session rather than just failing this test.
            a._stop.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
    finally:
        await _stop(a)


async def test_renew_loop_logs_unexpected_error_and_survives(
    tmp_path, monkeypatch, caplog
):
    # a NON-store error (not OSError/TimeoutError) must be caught by the
    # keep-the-loop-alive guard: it is logged at exception level and the
    # loop keeps running rather than dying.
    _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    try:

        rounds = {"n": 0}

        async def _boom():
            rounds["n"] += 1
            raise ValueError("unexpected")

        monkeypatch.setattr(a, "_renew_once", _boom)
        monkeypatch.setattr(
            type(a), "renew_period", property(lambda self: 0.001)
        )
        caplog.set_level(
            logging.ERROR, logger="cronstable.backends.filesystem"
        )
        task = asyncio.create_task(a._renew_loop())
        try:
            # Poll for the outcome instead of counting `sleep(0)` hops: see
            # test_renew_loop_warns_and_survives_store_error for why the hop
            # count differs on 3.11 and older.
            #
            # The behaviour this test is named for: the loop RAN AGAIN after
            # the raising round rather than merely staying alive. Neither
            # `task.done()` nor `task.cancelled() or task.exception() is
            # None` could show that; both are true by construction after an
            # awaited cancel, whatever the loop did.
            for _ in range(2000):
                if rounds["n"] >= 2:
                    break
                await asyncio.sleep(0.001)
            assert not task.done()
            assert any(
                "unexpected error in the filesystem election loop"
                in r.getMessage()
                for r in caplog.records
            )
            assert (
                rounds["n"] >= 2
            ), "the loop did not run again after the raise"
        finally:
            # Cancel in `finally`: a failing assertion above must not leak a
            # loop still spinning at the patched 1ms cadence, which wedges
            # the whole session rather than just failing this test.
            a._stop.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
    finally:
        await _stop(a)


async def test_renew_loop_sleeps_between_rounds_until_stopped(
    tmp_path, monkeypatch
):
    # between rounds the loop parks on the stop event for renew_period; when
    # that wait times out (no stop yet) it just loops again for the next
    # round.  Drive a tiny renew_period so the timeout path is exercised
    # without a real wait, and stop on the second round.
    _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a"))
    monkeypatch.setattr(
        type(a), "renew_period", property(lambda self: 0.001)
    )
    try:
        rounds = {"n": 0}

        async def _round():
            rounds["n"] += 1
            if rounds["n"] >= 2:
                a._stop.set()  # exit after the inter-round sleep timed out

        monkeypatch.setattr(a, "_renew_once", _round)
        task = asyncio.create_task(a._renew_loop())
        # wait_for both bounds the hang and re-raises any error the loop died
        # of, so a `task.done()` assertion after it would add nothing.
        await asyncio.wait_for(task, timeout=2.0)
        assert rounds["n"] >= 2  # ran a second round after the sleep timeout
    finally:
        await _stop(a)


async def test_renew_loop_reraises_cancellation(tmp_path, monkeypatch):
    # a cancel that lands inside the round's wait_for must propagate out of
    # the loop (the CancelledError re-raise), not be swallowed as a store
    # error -- otherwise stop() could never tear the loop down.
    _Clock(monkeypatch)
    a = _backend(tmp_path, "node-a")
    try:

        async def _hang():
            await asyncio.sleep(3600)

        monkeypatch.setattr(a, "_renew_once", _hang)
        task = asyncio.create_task(a._renew_loop())
        for _ in range(3):
            await asyncio.sleep(0)  # let the loop park inside the round
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert task.cancelled()
    finally:
        await _stop(a)


# --- reboot-ran refresh bookkeeping ----------------------------------------


async def test_synced_leader_refresh_failure_logs_debug(
    tmp_path, monkeypatch, caplog
):
    # once a leader is synced, a periodic ran-set re-read that later FAILS is
    # merely a stale-cache annoyance, not a deferral (the set is already
    # trusted): it logs DEBUG "could not refresh", never the leader-worded
    # "deferring" WARNING, and leadership is unaffected.
    clock = _Clock(monkeypatch)
    a = await _started(_backend(tmp_path, "node-a", extra="    ttl: 90\n"))
    try:
        await a._renew_once()  # acquire -> forced re-read completes -> synced
        assert a.is_leader() is True
        assert a._reboot_ran_synced is True

        _failing_list_records(monkeypatch, a)
        caplog.set_level(
            logging.DEBUG, logger="cronstable.backends.filesystem"
        )
        clock.advance(fsb_mod._REBOOT_RAN_REFRESH + 1.0)  # throttle now due
        await a._renew_once()  # renew succeeds; the throttled refresh fails
        assert a.is_leader() is True  # election work untouched by the failure

        refresh_debugs = [
            r
            for r in caplog.records
            if "could not refresh the @reboot-ran set" in r.getMessage()
        ]
        assert refresh_debugs
        assert all(r.levelno == logging.DEBUG for r in refresh_debugs)
        assert not any(
            "deferring pending @reboot one-shots" in r.getMessage()
            for r in caplog.records
        )
    finally:
        await _stop(a)
