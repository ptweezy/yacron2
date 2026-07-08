"""Fleet-HA scheduler features over the shared state store.

Covers cluster-wide concurrencyPolicy (the ``concurrencyScope: cluster``
slot lease: claim/deny/adopt, the Replace pursuit and its cancel channel,
release-on-finish, onStoreUnavailable behaviour, the lock-fidelity latch),
in-flight run records and crash reconciliation (same-host at rehydration,
cross-host on a slot takeover, the onMissed-aware watermark), and
cross-node retry resume (handoff on ownership move, the leased claim scan,
the consume-time newest-record re-check).  The filesystem leadership
backend itself is tested in tests/test_backend_filesystem.py; config
builders in tests/test_config_backends.py.

Style notes: as in the other state test files there is no frozen clock;
tests seed explicit aware datetimes, monkeypatch module seams, and assert
ordering/completion -- never durations (Windows CI has coarse timers).
Subprocess-running jobs use the list-form interpreter commands from
tests._commands.
"""

import asyncio
import datetime
import types

import pytest

import cronstable.platform as platform_mod
from cronstable.config import ConfigError, parse_config, parse_config_string
from cronstable.cron import Cron
from cronstable.fingerprint import job_digest
from cronstable.job import JobRetryState
from tests._commands import cmd_print, cmd_sleep, yaml_command
from tests.test_state import (
    _count_launcher,
    _drain_state_writes,
    _state_cfg,
)

_UTC = datetime.timezone.utc


def _now_utc() -> datetime.datetime:
    return datetime.datetime.now(_UTC)


def _iso(dt) -> str:
    return dt.isoformat()


_FORBID_JOB = (
    """
jobs:
  - name: j
"""
    + yaml_command(cmd_print("hi"))
    + """
    schedule: "0 0 * * *"
    concurrencyPolicy: Forbid
    concurrencyScope: cluster
"""
)

_REPLACE_JOB = (
    """
jobs:
  - name: j
"""
    + yaml_command(cmd_print("hi"))
    + """
    schedule: "0 0 * * *"
    concurrencyPolicy: Replace
    concurrencyScope: cluster
"""
)

_REPLACE_SLEEPER = (
    """
jobs:
  - name: j
"""
    + yaml_command(cmd_sleep(30))
    + """
    schedule: "0 0 * * *"
    concurrencyPolicy: Replace
    concurrencyScope: cluster
"""
)

_RETRY_JOB = """
jobs:
  - name: j
    command: ls
    schedule: "0 0 * * *"
    onFailure:
      retry:
        maximumRetries: 5
        initialDelay: 1
        maximumDelay: 60
        backoffMultiplier: 2
"""

_RETRY_EVERYNODE = """
jobs:
  - name: j
    command: ls
    schedule: "0 0 * * *"
    clusterPolicy: EveryNode
    onFailure:
      retry:
        maximumRetries: 5
        initialDelay: 1
        maximumDelay: 60
        backoffMultiplier: 2
"""

_RETRY_REBOOT = """
jobs:
  - name: j
    command: ls
    schedule: "@reboot"
    onFailure:
      retry:
        maximumRetries: -1
        initialDelay: 1
        maximumDelay: 60
        backoffMultiplier: 2
"""

_PLAIN_JOB = """
jobs:
  - name: j
    command: ls
    schedule: "0 0 * * *"
"""

_CATCHUP_JOB = """
jobs:
  - name: j
    command: ls
    schedule: "0 0 * * *"
    onMissed: run-once
"""


def _stub_manager(**over):
    base = {
        "distribution": "single-leader",
        "is_leader": lambda: True,
        "is_quorate": lambda: True,
        "is_available_leader": lambda: True,
        "has_conflict": lambda: False,
        "view_settled": lambda: True,
    }
    base.update(over)
    return types.SimpleNamespace(**base)


async def _stateful_cron(tmp_path, yaml, extra_state=""):
    cron = Cron(None, config_yaml=yaml)
    cfg = _state_cfg("state:\n  path: {}\n{}".format(tmp_path, extra_state))
    await cron.start_stop_state(cfg)
    assert cron.state_backend is not None
    return cron


async def _resume_cron(tmp_path, yaml, host):
    """A cron with cross-node retry resume active over a 'shared' store."""
    cron = await _stateful_cron(
        tmp_path, yaml, extra_state="  topology: shared\n"
    )
    cron._state_host = host
    cron._elect_leader_configured = True
    cron.cluster_manager = _stub_manager()
    assert cron._retry_resume_active() is True
    return cron


async def _newest(cron, stream):
    recs = await cron.state_backend.list_records(
        stream, limit=1, newest_first=True
    )
    return recs[0] if recs else None


async def _stop_state(cron):
    for name in list(cron.retry_state):
        await cron.cancel_job_retries(name, settle=None)
    cron._cancel_coordination_tasks()
    for task in list(cron._slot_renewers.values()):
        task.cancel()
    cron._slot_renewers.clear()
    await _drain_state_writes(cron)
    if cron.state_backend is not None:
        await cron.state_backend.stop()
        cron.state_backend = None


async def _wait_until(pred, tries=300, interval=0.05):
    for _ in range(tries):
        if pred():
            return True
        await asyncio.sleep(interval)
    return False


# --- config surface ---------------------------------------------------------


def test_concurrency_scope_defaults_to_node():
    cfg = parse_config_string(_PLAIN_JOB, "")
    assert cfg.jobs[0].concurrencyScope == "node"


def test_cluster_scope_with_allow_is_refused():
    # Allow bounds nothing, so widening its scope gates nothing: an inert
    # safety option must be an error the operator sees, not a silent no-op.
    yaml = """
jobs:
  - name: j
    command: ls
    schedule: "0 0 * * *"
    concurrencyScope: cluster
"""
    with pytest.raises(ConfigError, match="no effect with"):
        parse_config_string(yaml, "")


def test_cluster_scope_requires_state_section(tmp_path):
    # the parse-time cross-section check runs on the FINAL assembled
    # config, so a config dir may keep state: and the jobs in separate
    # files -- only the aggregate must carry both.
    without = tmp_path / "solo.yaml"
    without.write_text(
        "jobs:\n"
        "  - name: j\n"
        "    command: ls\n"
        '    schedule: "0 0 * * *"\n'
        "    concurrencyPolicy: Forbid\n"
        "    concurrencyScope: cluster\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="offending job.*j"):
        parse_config(str(without))
    confdir = tmp_path / "conf.d"
    confdir.mkdir()
    (confdir / "00-state.yaml").write_text(
        "state:\n  path: {}\n".format(str(tmp_path).replace("\\", "/")),
        encoding="utf-8",
    )
    (confdir / "10-jobs.yaml").write_text(
        without.read_text(encoding="utf-8"), encoding="utf-8"
    )
    cfg = parse_config(str(confdir))
    assert cfg.jobs[0].concurrencyScope == "cluster"


def test_fingerprint_stable_for_node_scope_only():
    # the digest of a pre-Phase4 config must not change on upgrade (it
    # keys persisted retry invalidation and @reboot markers), so the new
    # field is included only when set to "cluster".
    plain = parse_config_string(_PLAIN_JOB, "").jobs[0]
    from cronstable.fingerprint import canonical_job

    assert "concurrencyScope" not in canonical_job(plain)
    clustered = parse_config_string(_FORBID_JOB, "").jobs[0]
    assert canonical_job(clustered)["concurrencyScope"] == "cluster"
    assert job_digest(plain) != job_digest(clustered)


def test_slot_ttl_floor():
    with pytest.raises(ConfigError, match="slotTtlSeconds"):
        _state_cfg("state:\n  path: /tmp/x\n  slotTtlSeconds: 2\n")


# --- the cluster concurrency slot -------------------------------------------


async def test_slot_claimed_and_released_around_a_run(tmp_path):
    cron = await _stateful_cron(tmp_path, _FORBID_JOB)
    try:
        job = cron.cron_jobs["j"]
        assert await cron.maybe_launch_job(job) is True
        backend = cron.state_backend
        lease = await backend.read_lease("slots/j")
        assert lease is not None
        assert lease.holder == cron._slot_holder()
        assert "j" in cron._slot_renewers
        rj = cron.running_jobs["j"][0]
        await rj.wait()
        await cron._handle_finished_job(rj)
        await _drain_state_writes(cron)
        # released on the last instance's finish; observers see it free
        assert await backend.read_lease("slots/j") is None
        assert "j" not in cron._slot_renewers
        # the in-flight record opened and closed around the run
        rec = await _newest(cron, "inflight/j")
        assert rec is not None and rec["kind"] == "closed"
    finally:
        await _stop_state(cron)


async def test_forbid_skips_when_foreign_holder_is_live(tmp_path):
    cron = await _stateful_cron(tmp_path, _FORBID_JOB)
    try:
        backend = cron.state_backend
        foreign = await backend.acquire_lease("slots/j", "node-b#feed", 60)
        assert foreign is not None
        assert await cron.maybe_launch_job(cron.cron_jobs["j"]) is False
        assert not cron.running_jobs.get("j")
    finally:
        await _stop_state(cron)


async def test_replace_pursuit_cancels_and_relaunches(tmp_path):
    # cross-node Replace: the denied launcher never waits inline -- it
    # records a fence-targeted cancel request, and once the holder yields
    # (release here; TTL expiry in the crash case) the launch re-runs
    # through the normal gates.
    cron = await _stateful_cron(
        tmp_path, _REPLACE_JOB, extra_state="  slotTtlSeconds: 6\n"
    )
    try:
        backend = cron.state_backend
        foreign = await backend.acquire_lease("slots/j", "node-b#feed", 60)
        assert await cron.maybe_launch_job(cron.cron_jobs["j"]) is False
        assert "j" in cron._slot_pursuits
        # the cancel request landed, aimed at the holder's exact fence
        assert await _wait_until(
            lambda: cron._pending_state_writes or True
        )
        rec = None
        for _ in range(100):
            rec = await _newest(cron, "slots/j")
            if rec is not None:
                break
            await asyncio.sleep(0.05)
        assert rec is not None and rec["kind"] == "cancel"
        assert rec["fence"] == foreign.fence
        # the holder yields; the pursuit claims the slot and launches
        await backend.release_lease(foreign)
        assert await _wait_until(lambda: bool(cron.running_jobs.get("j")))
        rj = cron.running_jobs["j"][0]
        await rj.wait()
        await cron._handle_finished_job(rj)
    finally:
        await _stop_state(cron)


async def test_stale_cancel_records_are_inert(tmp_path):
    # a cancel aimed at a PREVIOUS incarnation's fence must not touch the
    # current holder: takeovers always bump the fence, so the old request
    # is definitionally satisfied.
    cron = await _stateful_cron(
        tmp_path, _REPLACE_SLEEPER, extra_state="  slotTtlSeconds: 5\n"
    )
    try:
        backend = cron.state_backend
        await backend.append_record(
            "slots/j",
            {"kind": "cancel", "fence": 999, "by": "node-b", "at": "x"},
        )
        assert await cron.maybe_launch_job(cron.cron_jobs["j"]) is True
        rj = cron.running_jobs["j"][0]
        await asyncio.sleep(2.0)  # > one renew period at ttl 5
        assert rj.replaced is False  # wrong fence: ignored
        rj.cancelled = True
        await rj.cancel()
        await rj.wait()
        await cron._handle_finished_job(rj)
    finally:
        await _stop_state(cron)


async def test_holder_observes_cancel_and_yields(tmp_path):
    cron = await _stateful_cron(
        tmp_path, _REPLACE_SLEEPER, extra_state="  slotTtlSeconds: 5\n"
    )
    try:
        backend = cron.state_backend
        assert await cron.maybe_launch_job(cron.cron_jobs["j"]) is True
        lease = cron._slot_leases["j"]
        rj = cron.running_jobs["j"][0]
        await backend.append_record(
            "slots/j",
            {
                "kind": "cancel",
                "fence": lease.fence,
                "by": "node-b",
                "at": _iso(_now_utc()),
            },
        )
        # the renew task notices within ~one renew period and replaces
        assert await _wait_until(lambda: rj.replaced, tries=300)
        await rj.wait()
        await cron._handle_finished_job(rj)
        await _drain_state_writes(cron)
        assert await backend.read_lease("slots/j") is None  # released
    finally:
        await _stop_state(cron)


async def test_slot_unavailable_degrade_vs_fail_closed(tmp_path):
    # the state section is configured but the backend is down: degrade
    # falls back to node-local enforcement, fail-closed skips the launch.
    cron = await _stateful_cron(tmp_path, _FORBID_JOB)
    try:
        backend = cron.state_backend
        cron.state_backend = None
        cron._state_on_unavailable = "fail-closed"
        assert await cron.maybe_launch_job(cron.cron_jobs["j"]) is False
        cron._state_on_unavailable = "degrade"
        assert await cron.maybe_launch_job(cron.cron_jobs["j"]) is True
        rj = cron.running_jobs["j"][0]
        await rj.wait()
        await cron._handle_finished_job(rj)
        cron.state_backend = backend
    finally:
        await _stop_state(cron)


async def test_slot_sick_store_follows_policy(tmp_path, monkeypatch):
    # acquire answers None and the confirming read answers None too: that
    # is a sick store, NOT a denial -- degrade must still launch (a
    # fast-failing mount must not silently convert degrade to fail-closed).
    cron = await _stateful_cron(tmp_path, _FORBID_JOB)
    try:
        backend = cron.state_backend

        async def _none(*_a, **_k):
            return None

        monkeypatch.setattr(backend, "acquire_lease", _none)
        monkeypatch.setattr(backend, "read_lease", _none)
        cron._state_on_unavailable = "fail-closed"
        assert await cron.maybe_launch_job(cron.cron_jobs["j"]) is False
        cron._state_on_unavailable = "degrade"
        assert await cron.maybe_launch_job(cron.cron_jobs["j"]) is True
        rj = cron.running_jobs["j"][0]
        await rj.wait()
        await cron._handle_finished_job(rj)
    finally:
        await _stop_state(cron)


async def test_slot_lock_fidelity_latch(tmp_path, monkeypatch):
    # a store whose locks are demonstrably no-ops cannot fence: the claim
    # treats it per onStoreUnavailable, latched once per backend.
    cron = await _stateful_cron(tmp_path, _FORBID_JOB)
    try:
        backend = cron.state_backend

        async def _fake(*_a, **_k):
            return "its locks are no-ops"

        monkeypatch.setattr(backend, "verify_locking", _fake)
        cron._state_on_unavailable = "fail-closed"
        assert await cron.maybe_launch_job(cron.cron_jobs["j"]) is False
        assert cron._slot_fidelity  # latched
        cron._state_on_unavailable = "degrade"
        assert await cron.maybe_launch_job(cron.cron_jobs["j"]) is True
        rj = cron.running_jobs["j"][0]
        await rj.wait()
        await cron._handle_finished_job(rj)
    finally:
        await _stop_state(cron)


# --- in-flight records and crash reconciliation ------------------------------


def _open_record(cron, proc="00ddba11feed", pid=999999, started=None):
    return {
        "kind": "open",
        "host": cron._state_host,
        "proc": proc,
        "pid": pid,
        "startedAt": _iso(started or _now_utc()),
        "jobDigest": job_digest(cron.cron_jobs["j"]),
    }


async def test_reconcile_closes_a_crashed_run(tmp_path, monkeypatch):
    cron = await _stateful_cron(tmp_path, _PLAIN_JOB)
    try:
        monkeypatch.setattr(platform_mod, "pid_alive", lambda pid: False)
        started = _now_utc() - datetime.timedelta(minutes=5)
        rec = _open_record(cron, started=started)
        await cron.state_backend.append_record("inflight/j", rec)
        cron._state_rehydrated = False
        await cron._rehydrate_from_state()
        await _drain_state_writes(cron)
        info = cron.last_run["j"]
        assert info.outcome == "unknown"
        assert info.started_at is None  # duration stats stay clean
        assert info.duration is None
        closed = await _newest(cron, "inflight/j")
        assert closed["kind"] == "closed"
        assert closed["reason"] == "reconciled-crash"
        # onMissed: skip (the default): the synthetic row advances the
        # watermark over exactly the interrupted slot.
        run = await _newest(cron, "runs/j")
        assert run["outcome"] == "unknown"
        assert run["finished_at"] == rec["startedAt"]
        assert await cron.durable_last_run_at("j") == rec["startedAt"]
    finally:
        await _stop_state(cron)


async def test_reconcile_preserves_catchup_for_onmissed_jobs(
    tmp_path, monkeypatch
):
    # under onMissed run-once/run-all the interrupted slot is still OWED:
    # the synthetic row must not advance the durable watermark (crash
    # recovery must not silently downgrade those jobs to at-most-once).
    cron = await _stateful_cron(tmp_path, _CATCHUP_JOB)
    try:
        monkeypatch.setattr(platform_mod, "pid_alive", lambda pid: False)
        rec = _open_record(cron)
        await cron.state_backend.append_record("inflight/j", rec)
        cron._state_rehydrated = False
        await cron._rehydrate_from_state()
        await _drain_state_writes(cron)
        run = await _newest(cron, "runs/j")
        assert run["outcome"] == "unknown"
        assert "finished_at" not in run
        assert run["interruptedAt"] == rec["startedAt"]
        assert await cron.durable_last_run_at("j") is None
        # still visible in memory (and, via the interruptedAt fallback, on
        # later restarts too)
        assert cron.last_run["j"].outcome == "unknown"
    finally:
        await _stop_state(cron)


async def test_reconcile_skips_when_pid_still_alive(tmp_path, monkeypatch):
    # a daemon crash does not kill spawned job processes: a live pid means
    # the run may still be executing, so it is left alone.
    cron = await _stateful_cron(tmp_path, _PLAIN_JOB)
    try:
        monkeypatch.setattr(platform_mod, "pid_alive", lambda pid: True)
        await cron.state_backend.append_record(
            "inflight/j", _open_record(cron)
        )
        cron._state_rehydrated = False
        await cron._rehydrate_from_state()
        await _drain_state_writes(cron)
        assert "j" not in cron.last_run
        rec = await _newest(cron, "inflight/j")
        assert rec["kind"] == "open"  # untouched
    finally:
        await _stop_state(cron)


async def test_reconcile_skips_own_process_and_foreign_hosts(
    tmp_path, monkeypatch
):
    cron = await _stateful_cron(tmp_path, _PLAIN_JOB)
    try:
        monkeypatch.setattr(platform_mod, "pid_alive", lambda pid: False)
        # our own token: a state-section reload rebuilt the backend under
        # a live run -- never reconcile this process's own records.
        own = _open_record(cron, proc=cron._proc_token)
        await cron.state_backend.append_record("inflight/j", own)
        cron._state_rehydrated = False
        await cron._rehydrate_from_state()
        await _drain_state_writes(cron)
        assert "j" not in cron.last_run
        # a foreign host's open record is the slot takeover's business
        foreign = dict(_open_record(cron), host="node-elsewhere")
        await cron.state_backend.append_record("inflight/j", foreign)
        cron._state_rehydrated = False
        await cron._rehydrate_from_state()
        await _drain_state_writes(cron)
        assert "j" not in cron.last_run
    finally:
        await _stop_state(cron)


async def test_slot_takeover_reconciles_foreign_open_record(tmp_path):
    # a fresh slot win proves the previous holder made no successful
    # renewal for a whole TTL: its orphaned open record is closed and the
    # interrupted run made visible before the new run starts.
    cron = await _stateful_cron(tmp_path, _FORBID_JOB)
    try:
        foreign = {
            "kind": "open",
            "host": "node-elsewhere",
            "proc": "feedfacef00d",
            "pid": 4242,
            "startedAt": _iso(_now_utc() - datetime.timedelta(minutes=10)),
            "jobDigest": job_digest(cron.cron_jobs["j"]),
        }
        await cron.state_backend.append_record("inflight/j", foreign)
        assert await cron.maybe_launch_job(cron.cron_jobs["j"]) is True
        rj = cron.running_jobs["j"][0]
        await rj.wait()
        await cron._handle_finished_job(rj)
        await _drain_state_writes(cron)
        recs = await cron.state_backend.list_records("inflight/j")
        reasons = [r.get("reason") for r in recs if r["kind"] == "closed"]
        assert "reconciled-takeover" in reasons
        outcomes = [
            r["outcome"]
            for r in await cron.state_backend.list_records("runs/j")
        ]
        assert "unknown" in outcomes  # the orphan, made visible
        assert cron.run_history["j"][0].outcome == "unknown"
    finally:
        await _stop_state(cron)


# --- cross-node retry resume -------------------------------------------------


async def test_abandon_hands_off_on_shared_store(tmp_path):
    cron = await _resume_cron(tmp_path, _RETRY_JOB, "node-a")
    try:
        state = JobRetryState(1, 2, 60)
        state.next_delay()
        cron.retry_state["j"] = state
        cron._abandon_retry(cron.cron_jobs["j"], 1)
        await _drain_state_writes(cron)
        rec = await _newest(cron, "retries/j")
        assert rec["kind"] == "handoff"
        assert rec["attempt"] == 1
        assert rec["fromHost"] == "node-a"
        assert rec["jobDigest"] == job_digest(cron.cron_jobs["j"])
        assert "j" not in cron.retry_state
        # no "cancelled" run-history record: the attempt moved, not died
        assert "j" not in cron.last_run
    finally:
        await _stop_state(cron)


async def test_handoff_is_claimed_and_fires_elsewhere(tmp_path):
    giver = await _resume_cron(tmp_path, _RETRY_JOB, "node-a")
    taker = await _resume_cron(tmp_path, _RETRY_JOB, "node-b")
    try:
        state = JobRetryState(1, 2, 60)
        state.next_delay()
        giver.retry_state["j"] = state
        giver._abandon_retry(giver.cron_jobs["j"], 1)
        await _drain_state_writes(giver)
        calls, fake = _count_launcher()
        taker.maybe_launch_job = fake  # type: ignore[method-assign]
        await taker._maybe_claim_retry("j", taker.cron_jobs["j"])
        assert "j" in taker.retry_state
        task = taker.retry_state["j"].task
        assert task is not None
        await asyncio.wait_for(task, timeout=20)
        assert calls == ["j"]  # the claimed attempt fired on the taker
        await _drain_state_writes(taker)
        rec = await _newest(taker, "retries/j")
        assert rec["kind"] == "settled"
        assert rec["reason"] == "launched"
        assert rec["host"] == "node-b"
    finally:
        await _stop_state(giver)
        await _stop_state(taker)


async def test_claim_respects_the_staleness_grace(tmp_path):
    taker = await _resume_cron(tmp_path, _RETRY_JOB, "node-b")
    try:
        fresh = {
            "kind": "pending",
            "attempt": 1,
            "notBefore": _iso(_now_utc()),
            "jobDigest": job_digest(taker.cron_jobs["j"]),
            "host": "node-a",
            "at": _iso(_now_utc()),
        }
        await taker.state_backend.append_record("retries/j", fresh)
        await taker._maybe_claim_retry("j", taker.cron_jobs["j"])
        # freshly due: its owner may be about to fire it -- not claimable
        assert "j" not in taker.retry_state
        stale = dict(
            fresh,
            notBefore=_iso(_now_utc() - datetime.timedelta(seconds=120)),
            at=_iso(_now_utc() - datetime.timedelta(seconds=120)),
        )
        await taker.state_backend.append_record("retries/j", stale)
        await taker._maybe_claim_retry("j", taker.cron_jobs["j"])
        assert "j" in taker.retry_state  # crashed owner: claimed
        rec = await _newest(taker, "retries/j")
        assert rec["kind"] == "pending"
        assert rec["host"] == "node-b"
        assert rec["claimedFrom"] == "node-a"
    finally:
        await _stop_state(taker)


async def test_claim_declines_ineligible_ladders(tmp_path):
    # EveryNode ladders are per-node; @reboot ladders are boot-anchored;
    # a digest mismatch means the job changed -- none of them move hosts.
    stale_kw = {
        "notBefore": _iso(_now_utc() - datetime.timedelta(seconds=120)),
        "at": _iso(_now_utc() - datetime.timedelta(seconds=120)),
    }
    for sub, yaml in (
        ("everynode", _RETRY_EVERYNODE),
        ("reboot", _RETRY_REBOOT),
    ):
        taker = await _resume_cron(tmp_path / sub, yaml, "n-b")
        try:
            rec = {
                "kind": "pending",
                "attempt": 1,
                "jobDigest": job_digest(taker.cron_jobs["j"]),
                "host": "node-a",
                **stale_kw,
            }
            await taker.state_backend.append_record("retries/j", rec)
            await taker._maybe_claim_retry("j", taker.cron_jobs["j"])
            assert "j" not in taker.retry_state
        finally:
            await _stop_state(taker)
    taker = await _resume_cron(tmp_path / "digest", _RETRY_JOB, "node-b")
    try:
        rec = {
            "kind": "pending",
            "attempt": 1,
            "jobDigest": "not-this-config",
            "host": "node-a",
            **stale_kw,
        }
        await taker.state_backend.append_record("retries/j", rec)
        await taker._maybe_claim_retry("j", taker.cron_jobs["j"])
        assert "j" not in taker.retry_state
    finally:
        await _stop_state(taker)


async def test_claim_settles_superseded_by_run(tmp_path):
    # the durable ledger outranks a stale pending: a run that finished
    # after the ladder was armed proves it resolved (perhaps its settle
    # write was dropped); re-firing it would be the exact resurrection
    # the re-arm guards exist to prevent.
    taker = await _resume_cron(tmp_path, _RETRY_JOB, "node-b")
    try:
        armed = _now_utc() - datetime.timedelta(seconds=300)
        await taker.state_backend.append_record(
            "retries/j",
            {
                "kind": "pending",
                "attempt": 1,
                "notBefore": _iso(armed),
                "jobDigest": job_digest(taker.cron_jobs["j"]),
                "host": "node-a",
                "at": _iso(armed),
            },
        )
        await taker.state_backend.append_record(
            "runs/j",
            {
                "outcome": "success",
                "exit_code": 0,
                "started_at": _iso(armed),
                "finished_at": _iso(
                    _now_utc() - datetime.timedelta(seconds=60)
                ),
                "duration": 1.0,
                "fail_reason": None,
            },
        )
        await taker._maybe_claim_retry("j", taker.cron_jobs["j"])
        assert "j" not in taker.retry_state
        await _drain_state_writes(taker)
        rec = await _newest(taker, "retries/j")
        assert rec["kind"] == "settled"
        assert rec["reason"] == "superseded-by-run"
    finally:
        await _stop_state(taker)


async def test_consume_aborts_when_ladder_was_claimed_away(tmp_path):
    # the load-bearing re-check: a gate-deferred owner can wake
    # arbitrarily late (its re-check cadence is its own ladder delay), so
    # at consume time the newest record must still be OUR host's -- a
    # foreign record means a peer claimed the ladder, and firing anyway
    # would be the cross-node double-run. The abort settles NOTHING: the
    # claimer's pending must stay newest.
    owner = await _resume_cron(tmp_path, _RETRY_JOB, "node-a")
    try:
        state = JobRetryState(1, 2, 60)
        state.next_delay()
        owner.retry_state["j"] = state
        calls, fake = _count_launcher()
        owner.maybe_launch_job = fake  # type: ignore[method-assign]
        # the real-life ordering: the owner ARMS first (its pending record
        # lands), then a peer claims while the owner sleeps/defers.
        task = asyncio.create_task(owner.schedule_retry_job("j", 1.0, 1))
        state.task = task
        await asyncio.sleep(0)  # let the arm-time persist get queued
        await _drain_state_writes(owner)
        claimed = {
            "kind": "pending",
            "attempt": 1,
            "notBefore": _iso(_now_utc()),
            "jobDigest": job_digest(owner.cron_jobs["j"]),
            "host": "node-b",
            "at": _iso(_now_utc()),
        }
        await owner.state_backend.append_record("retries/j", claimed)
        await asyncio.wait_for(task, timeout=20)
        assert calls == []  # not fired here
        assert "j" not in owner.retry_state
        rec = await _newest(owner, "retries/j")
        assert rec == claimed  # nothing written on top of the claim
    finally:
        await _stop_state(owner)


async def test_consume_unchanged_without_resume(tmp_path):
    # single-node stores keep the classic (pre-cluster) consume semantics: the
    # newest-record re-check and the claim lease never engage.
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        assert cron._retry_resume_active() is False
        state = JobRetryState(1, 2, 60)
        state.next_delay()
        cron.retry_state["j"] = state
        calls, fake = _count_launcher()
        cron.maybe_launch_job = fake  # type: ignore[method-assign]
        await asyncio.wait_for(
            cron.schedule_retry_job("j", 0.01, 1), timeout=20
        )
        assert calls == ["j"]
    finally:
        await _stop_state(cron)


async def test_concurrent_claims_yield_exactly_one_owner(tmp_path):
    # the per-job claim lease serializes rival scanners: the loser either
    # fails the acquire or sees the winner's fresh pending on its re-read.
    a = await _resume_cron(tmp_path, _RETRY_JOB, "node-a")
    b = await _resume_cron(tmp_path, _RETRY_JOB, "node-b")
    try:
        stale = _now_utc() - datetime.timedelta(seconds=120)
        await a.state_backend.append_record(
            "retries/j",
            {
                "kind": "pending",
                "attempt": 2,
                "notBefore": _iso(stale),
                "jobDigest": job_digest(a.cron_jobs["j"]),
                "host": "node-dead",
                "at": _iso(stale),
            },
        )
        await asyncio.gather(
            a._maybe_claim_retry("j", a.cron_jobs["j"]),
            b._maybe_claim_retry("j", b.cron_jobs["j"]),
        )
        claimed_by = [
            c._state_host for c in (a, b) if "j" in c.retry_state
        ]
        assert len(claimed_by) == 1
    finally:
        await _stop_state(a)
        await _stop_state(b)


async def test_claim_scan_spawned_from_housekeeping(tmp_path):
    cron = await _resume_cron(tmp_path, _RETRY_JOB, "node-a")
    try:
        cron._state_periodic()
        assert cron._retry_claim_task is not None
        await asyncio.wait_for(cron._retry_claim_task, timeout=20)
    finally:
        await _stop_state(cron)


# --- review regressions -----------------------------------------------------


async def test_phantom_release_no_ops_when_a_live_claim_owns_the_slot(
    tmp_path,
):
    # regression (slot-protocol): a phantom release (a degraded launch left
    # our per-process holder on disk but no local Lease) must not revoke a
    # FRESH claim's lease -- the holder string matches (same process), so
    # without the mutex + live-lease guard it would free a slot a live run
    # believes it holds, and a peer's Forbid claim would then double-run.
    cron = await _stateful_cron(tmp_path, _FORBID_JOB)
    try:
        backend = cron.state_backend
        holder = cron._slot_holder()
        lease = await backend.acquire_lease("slots/j", holder, cron._slot_ttl)
        cron._slot_leases["j"] = lease
        cron._slot_refs["j"] = 1
        await cron._release_phantom_slot("j")
        assert await backend.read_lease("slots/j") is not None
    finally:
        cron._slot_leases.clear()
        cron._slot_refs.clear()
        await _stop_state(cron)


async def test_phantom_release_cleans_a_true_phantom(tmp_path):
    # the flip side: with NO local lease, a stale on-disk lease under our
    # own holder is a genuine phantom and IS released so peers are not
    # blocked for a whole TTL.
    cron = await _stateful_cron(tmp_path, _FORBID_JOB)
    try:
        backend = cron.state_backend
        holder = cron._slot_holder()
        await backend.acquire_lease("slots/j", holder, cron._slot_ttl)
        assert "j" not in cron._slot_leases
        await cron._release_phantom_slot("j")
        assert await backend.read_lease("slots/j") is None
    finally:
        await _stop_state(cron)


async def test_claim_dropped_when_local_ladder_armed_during_claim(tmp_path):
    # regression (retry-resume): _maybe_claim_retry's awaits yield, and a
    # scheduled fire can arm a LIVE local ladder in that window. The claim
    # must be dropped -- overwriting retry_state would strand that task as a
    # second same-node ladder, and since both write host==ours the foreign-
    # record abort never fires: a same-node double-fire.
    taker = await _resume_cron(tmp_path, _RETRY_JOB, "node-b")
    try:
        stale = _now_utc() - datetime.timedelta(seconds=120)
        await taker.state_backend.append_record(
            "retries/j",
            {
                "kind": "pending",
                "attempt": 2,
                "notBefore": _iso(stale),
                "jobDigest": job_digest(taker.cron_jobs["j"]),
                "host": "node-a",
                "at": _iso(stale),
            },
        )
        fresh = JobRetryState(1, 2, 60)
        fresh.next_delay()

        async def _never():
            await asyncio.Event().wait()

        fresh.task = asyncio.create_task(_never())
        orig = taker._claim_retry_under_lease

        async def _claim_and_race(*a, **k):
            ok = await orig(*a, **k)
            taker.retry_state["j"] = fresh
            return ok

        taker._claim_retry_under_lease = _claim_and_race  # type: ignore[method-assign]
        await taker._maybe_claim_retry("j", taker.cron_jobs["j"])
        assert taker.retry_state["j"] is fresh
        assert not fresh.task.done()
    finally:
        await _stop_state(taker)


async def test_takeover_reconcile_spares_a_live_local_orphan(
    tmp_path, monkeypatch
):
    # regression (reconcile): a slot takeover whose orphaned open record is
    # from a PREVIOUS daemon on THIS host with a still-alive pid must not be
    # reconciled -- a daemon crash does not kill the job process it spawned.
    cron = await _stateful_cron(tmp_path, _FORBID_JOB)
    try:
        monkeypatch.setattr(platform_mod, "pid_alive", lambda pid: True)
        orphan = {
            "kind": "open",
            "host": cron._state_host,
            "proc": "0ldpr0ct0ken",
            "pid": 4321,
            "startedAt": _iso(_now_utc() - datetime.timedelta(minutes=3)),
            "jobDigest": job_digest(cron.cron_jobs["j"]),
        }
        await cron.state_backend.append_record("inflight/j", orphan)
        await cron._reconcile_takeover_inflight(cron.cron_jobs["j"])
        await _drain_state_writes(cron)
        rec = await _newest(cron, "inflight/j")
        assert rec["kind"] == "open"
        assert "j" not in cron.last_run
    finally:
        await _stop_state(cron)


async def test_inflight_open_sorts_before_close_for_a_fast_run(tmp_path):
    # regression (reconcile): the per-job inflight tail chains open and its
    # paired close so the close can never sort ahead of the open.
    cron = await _stateful_cron(tmp_path, _PLAIN_JOB)
    try:
        job = cron.cron_jobs["j"]
        assert await cron.maybe_launch_job(job) is True
        rj = cron.running_jobs["j"][0]
        await rj.wait()
        await cron._handle_finished_job(rj)
        await _drain_state_writes(cron)
        recs = await cron.state_backend.list_records(
            "inflight/j", newest_first=True
        )
        assert recs[0]["kind"] == "closed"
        assert any(r["kind"] == "open" for r in recs)
    finally:
        await _stop_state(cron)


async def test_abandoned_claim_write_does_not_orphan_own_host_pending(
    tmp_path, monkeypatch
):
    # regression (retry-resume): a claim-write that times out is shielded,
    # so it would otherwise land LATER as an own-host pending that this
    # node's scans skip and rehydration never re-arms -- an unreclaimable
    # orphan. The timeout cancels the shielded write so the foreign record
    # stays newest and re-claimable.
    import cronstable.cron as cron_mod

    taker = await _resume_cron(tmp_path, _RETRY_JOB, "node-b")
    try:
        stale = _now_utc() - datetime.timedelta(seconds=120)
        foreign = {
            "kind": "pending",
            "attempt": 1,
            "notBefore": _iso(stale),
            "jobDigest": job_digest(taker.cron_jobs["j"]),
            "host": "node-a",
            "at": _iso(stale),
        }
        await taker.state_backend.append_record("retries/j", foreign)
        monkeypatch.setattr(cron_mod, "STATE_OP_TIMEOUT", 0.05)

        def _slow_queue(name, record):
            async def _slow():
                if record.get("host") == taker._state_host:
                    await asyncio.sleep(0.5)
                await taker._append_retry_record(name, record)

            task = taker._track_state_write(_slow())
            taker._retry_write_tail[name] = task
            return task

        taker._queue_retry_write = _slow_queue  # type: ignore[method-assign]
        ok = await taker._claim_retry_under_lease(
            "j", taker.cron_jobs["j"], foreign, 1, stale
        )
        assert ok is False
        await asyncio.sleep(0.6)
        await _drain_state_writes(taker)
        rec = await _newest(taker, "retries/j")
        assert rec["host"] == "node-a"
    finally:
        await _stop_state(taker)
