"""Scheduler-side durability features.

Covers restart-surviving retries (absolute-deadline re-arming + per-job digest
invalidation), standalone @reboot dedupe via the durable boot marker, durable
Prometheus counter snapshots, and the /jobs/{name}/trends SLA aggregates --
plus the scheduler hooks:
the onStoreUnavailable policy, the manifest stream, automatic GC's keep-set
assembly, and the dropped-write accounting.  Backend-level mechanics (GC
deletion rules, migrate-schema, op stats, rate limiting) live in
tests/test_state_admin.py.

Style notes: like the other state test files there is no frozen clock here;
tests pass explicit aware datetimes, monkeypatch module seams, and assert
ordering/completion -- never durations (Windows CI has coarse timers).
"""

import asyncio
import datetime
import json

import pytest
from aiohttp import web

import cronstable.platform as platform_mod
from cronstable.cron import Cron
from cronstable.fingerprint import job_digest
from cronstable.job import JobRetryState
from cronstable.prometheus import PrometheusMetrics
from tests.test_state import (
    _count_launcher,
    _drain_state_writes,
    _info,
    _state_cfg,
)

_UTC = datetime.timezone.utc


def _now_utc() -> datetime.datetime:
    return datetime.datetime.now(_UTC)


_RETRY_JOB = """
jobs:
  - name: j
    command: ls
    schedule: "0 0 * * *"
    onFailure:
      retry:
        maximumRetries: 3
        initialDelay: 1
        maximumDelay: 60
        backoffMultiplier: 2
"""

_RETRY_JOB_DEADLINE = """
jobs:
  - name: j
    command: ls
    schedule: "0 0 * * *"
    startingDeadlineSeconds: 3600
    onFailure:
      retry:
        maximumRetries: 3
        initialDelay: 1
        maximumDelay: 60
        backoffMultiplier: 2
"""

_RETRY_JOB_DISABLED = """
jobs:
  - name: j
    command: ls
    schedule: "0 0 * * *"
    enabled: false
    onFailure:
      retry:
        maximumRetries: 3
        initialDelay: 1
        maximumDelay: 60
        backoffMultiplier: 2
"""

_REBOOT_JOB = """
jobs:
  - name: r
    command: ls
    schedule: "@reboot"
"""

_REBOOT_KEEPALIVE_JOB = """
jobs:
  - name: r
    command: ls
    schedule: "@reboot"
    onFailure:
      retry:
        maximumRetries: -1
        initialDelay: 1
        maximumDelay: 60
        backoffMultiplier: 2
"""

_DEP_JOB = """
jobs:
  - name: d
    command: ls
    schedule: "0 0 * * *"
    onlyIfLastSucceeded: true
"""


async def _stateful_cron(tmp_path, yaml, extra_state=""):
    cron = Cron(None, config_yaml=yaml)
    cfg = _state_cfg(
        "state:\n  path: {}\n{}".format(tmp_path, extra_state)
    )
    await cron.start_stop_state(cfg)
    assert cron.state_backend is not None
    return cron


async def _newest(cron, stream):
    recs = await cron.state_backend.list_records(
        stream, limit=1, newest_first=True
    )
    return recs[0] if recs else None


async def _stop_state(cron):
    await _drain_state_writes(cron)
    if cron.state_backend is not None:
        await cron.state_backend.stop()
        cron.state_backend = None


# --- durable retries: persistence points ---------------------------------


async def test_retry_pending_record_written(tmp_path):
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        state = JobRetryState(1, 2, 60)
        state.next_delay()  # ladder position: arming attempt #1
        cron.retry_state["j"] = state
        task = asyncio.create_task(cron.schedule_retry_job("j", 600.0, 1))
        state.task = task
        await asyncio.sleep(0)  # let the task persist its pending record
        await _drain_state_writes(cron)
        rec = await _newest(cron, "retries/j")
        assert rec is not None
        assert rec["kind"] == "pending"
        assert rec["attempt"] == 1
        assert rec["jobDigest"] == job_digest(cron.cron_jobs["j"])
        not_before = datetime.datetime.fromisoformat(rec["notBefore"])
        remaining = (not_before - _now_utc()).total_seconds()
        # absolute deadline about `delay` ahead (generous bounds; never a
        # tight window on Windows CI)
        assert 0 < remaining <= 600
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        await _stop_state(cron)


async def test_retry_settled_on_success(tmp_path):
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        state = JobRetryState(1, 2, 60)
        state.next_delay()
        cron.retry_state["j"] = state
        await cron.cancel_job_retries("j", settle="succeeded")
        await _drain_state_writes(cron)
        rec = await _newest(cron, "retries/j")
        assert rec is not None
        assert rec["kind"] == "settled"
        assert rec["reason"] == "succeeded"
    finally:
        await _stop_state(cron)


async def test_shutdown_cancel_does_not_settle(tmp_path):
    # settle=None (the graceful-shutdown path) leaves the pending record on
    # top: surviving the restart is the point of durable retries.
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        state = JobRetryState(1, 2, 60)
        state.next_delay()
        cron.retry_state["j"] = state
        await cron.cancel_job_retries("j", settle=None)
        await _drain_state_writes(cron)
        assert await _newest(cron, "retries/j") is None
    finally:
        await _stop_state(cron)


async def test_settle_skipped_when_no_retry_was_scheduled(tmp_path):
    # a ladder armed at launch but never used (count == 0) has nothing
    # durable to settle -- and must not add a write to every successful run.
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        cron.retry_state["j"] = JobRetryState(1, 2, 60)
        await cron.cancel_job_retries("j", settle="succeeded")
        await _drain_state_writes(cron)
        assert await _newest(cron, "retries/j") is None
    finally:
        await _stop_state(cron)


async def test_job_removed_mid_retry_settles(tmp_path):
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        cron.retry_state["ghost"] = JobRetryState(1, 2, 60)
        await cron.schedule_retry_job("ghost", 0, 1)
        await _drain_state_writes(cron)
        rec = await _newest(cron, "retries/ghost")
        assert rec is not None
        assert rec["kind"] == "settled"
        assert rec["reason"] == "job-removed"
        assert "ghost" not in cron.retry_state
    finally:
        await _stop_state(cron)


async def test_abandon_retry_settles_owner_moved(tmp_path):
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        state = JobRetryState(1, 2, 60)
        state.next_delay()
        cron.retry_state["j"] = state
        # a single-node store: cross-node resume is inactive, so the
        # abandonment settles the ladder dead (the legacy behaviour).
        cron._abandon_retry(cron.cron_jobs["j"], 1)
        await _drain_state_writes(cron)
        rec = await _newest(cron, "retries/j")
        assert rec is not None
        assert rec["kind"] == "settled"
        assert rec["reason"] == "owner-moved"
    finally:
        await _stop_state(cron)


async def test_abandon_retry_handoff_carries_armed_at(tmp_path, monkeypatch):
    # The cross-node hand-off must carry the attempt's ORIGINAL arm time in
    # ``armedAt`` (notBefore stays "now" for prompt resume), so the new owner's
    # superseded-by-run guard anchors on the arm, not the hand-off instant.
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        monkeypatch.setattr(
            cron, "_retry_cross_node_eligible", lambda job: True
        )
        state = JobRetryState(1, 2, 60)
        state.next_delay()
        t_arm = datetime.datetime(2026, 7, 1, 0, 0, 0, tzinfo=_UTC)
        state.armed_at = t_arm
        cron.retry_state["j"] = state
        cron._abandon_retry(cron.cron_jobs["j"], 1)
        await _drain_state_writes(cron)
        rec = await _newest(cron, "retries/j")
        assert rec is not None
        assert rec["kind"] == "handoff"
        assert rec["armedAt"] == t_arm.isoformat()
        assert rec["notBefore"] != t_arm.isoformat()  # "now", not the arm
    finally:
        await _stop_state(cron)


async def test_retry_handoff_armedat_anchors_superseded_guard(tmp_path):
    # A hand-off whose armedAt predates a run the new owner ALREADY completed
    # must read as RESOLVED (not claimable) -- else the completed retry re-runs
    # across failover. Without armedAt the guard anchors on the (fresh)
    # hand-off instant and mis-reads the earlier run as older: the bug.
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        job = cron.cron_jobs["j"]
        t_arm = datetime.datetime(2026, 7, 1, 0, 0, 0, tzinfo=_UTC)
        t_handoff = datetime.datetime(2026, 7, 1, 0, 0, 50, tzinfo=_UTC)
        # a run finished at 00:00:30 -- AFTER the arm, BEFORE the hand-off.
        cron.last_run["j"] = _info(second=30)
        base = {
            "kind": "handoff",
            "attempt": 2,
            "notBefore": t_handoff.isoformat(),
            "jobDigest": job_digest(job),
            "fromHost": "other",
            "at": t_handoff.isoformat(),
        }
        with_armed = dict(base, armedAt=t_arm.isoformat())
        assert cron._retry_record_claimable("j", job, with_armed) is None
        # control: the SAME record without armedAt anchors on the hand-off
        # instant and (wrongly) reads as claimable -- the double-fire the
        # field closes.
        assert cron._retry_record_claimable("j", job, base) is not None
    finally:
        await _stop_state(cron)


# --- durable retries: restart re-arming -----------------------------------


async def _seed_pending(cron, name, attempt, not_before, digest=None):
    job = cron.cron_jobs.get(name)
    await cron.state_backend.append_record(
        "retries/" + name,
        {
            "kind": "pending",
            "attempt": attempt,
            "notBefore": not_before.isoformat(),
            "jobDigest": (
                digest if digest is not None else job_digest(job)
            ),
            "at": _now_utc().isoformat(),
        },
    )


async def test_rearm_pending_retry_after_restart(tmp_path):
    # The flagship: a pending retry with a PAST absolute deadline re-arms on
    # boot at its persisted ladder position and launches immediately.
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        await _seed_pending(
            cron, "j", 2, _now_utc() - datetime.timedelta(seconds=5)
        )
        calls, fake = _count_launcher()
        cron.maybe_launch_job = fake  # type: ignore[method-assign]
        cron._state_rehydrated = False
        await cron._rehydrate_from_state()
        state = cron.retry_state.get("j")
        assert state is not None
        # ladder replayed to the persisted position
        assert state.count == 2
        assert state.task is not None
        await state.task
        assert calls == ["j"]
        await _drain_state_writes(cron)
        rec = await _newest(cron, "retries/j")
        assert rec["kind"] == "settled"
        assert rec["reason"] == "launched"
    finally:
        await _stop_state(cron)


async def test_rearm_sleeps_only_remaining_delay(tmp_path):
    # a FUTURE deadline re-arms but does not launch yet
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        await _seed_pending(
            cron, "j", 1, _now_utc() + datetime.timedelta(seconds=600)
        )
        calls, fake = _count_launcher()
        cron.maybe_launch_job = fake  # type: ignore[method-assign]
        cron._state_rehydrated = False
        await cron._rehydrate_from_state()
        state = cron.retry_state.get("j")
        assert state is not None
        await asyncio.sleep(0)
        assert calls == []
        state.task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await state.task
    finally:
        await _stop_state(cron)


async def test_rearm_invalidated_on_config_digest_mismatch(tmp_path):
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        await _seed_pending(
            cron,
            "j",
            1,
            _now_utc() - datetime.timedelta(seconds=5),
            digest="v-something-else",
        )
        cron._state_rehydrated = False
        await cron._rehydrate_from_state()
        assert "j" not in cron.retry_state
        await _drain_state_writes(cron)
        rec = await _newest(cron, "retries/j")
        assert rec["kind"] == "settled"
        assert rec["reason"] == "config-changed"
    finally:
        await _stop_state(cron)


async def test_rearm_invalidated_when_exhausted(tmp_path):
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        # attempt 4 > maximumRetries 3
        await _seed_pending(
            cron, "j", 4, _now_utc() - datetime.timedelta(seconds=5)
        )
        cron._state_rehydrated = False
        await cron._rehydrate_from_state()
        assert "j" not in cron.retry_state
        await _drain_state_writes(cron)
        rec = await _newest(cron, "retries/j")
        assert rec["reason"] == "exhausted"
    finally:
        await _stop_state(cron)


async def test_rearm_invalidated_when_disabled(tmp_path):
    cron = await _stateful_cron(tmp_path, _RETRY_JOB_DISABLED)
    try:
        await _seed_pending(
            cron, "j", 1, _now_utc() - datetime.timedelta(seconds=5)
        )
        cron._state_rehydrated = False
        await cron._rehydrate_from_state()
        assert "j" not in cron.retry_state
        await _drain_state_writes(cron)
        rec = await _newest(cron, "retries/j")
        assert rec["reason"] == "disabled"
    finally:
        await _stop_state(cron)


async def test_rearm_invalidated_past_starting_deadline(tmp_path):
    cron = await _stateful_cron(tmp_path, _RETRY_JOB_DEADLINE)
    try:
        # two hours stale > startingDeadlineSeconds (1h)
        await _seed_pending(
            cron, "j", 1, _now_utc() - datetime.timedelta(seconds=7200)
        )
        cron._state_rehydrated = False
        await cron._rehydrate_from_state()
        assert "j" not in cron.retry_state
        await _drain_state_writes(cron)
        rec = await _newest(cron, "retries/j")
        assert rec["reason"] == "deadline-passed"
    finally:
        await _stop_state(cron)


async def test_rearm_ignores_settled_top_and_garbage(tmp_path):
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        await _seed_pending(
            cron, "j", 1, _now_utc() - datetime.timedelta(seconds=5)
        )
        await cron.state_backend.append_record(
            "retries/j",
            {"kind": "settled", "reason": "succeeded"},
        )
        cron._state_rehydrated = False
        await cron._rehydrate_from_state()
        assert "j" not in cron.retry_state
        # a corrupt pending record settles as invalid rather than crashing
        await cron.state_backend.append_record(
            "retries/j",
            {"kind": "pending", "attempt": "NaN", "notBefore": 12},
        )
        cron._state_rehydrated = False
        await cron._rehydrate_from_state()
        assert "j" not in cron.retry_state
        await _drain_state_writes(cron)
        rec = await _newest(cron, "retries/j")
        assert rec["reason"] == "invalid-record"
    finally:
        await _stop_state(cron)


async def test_rearm_reboot_keepalive_continuity(tmp_path, monkeypatch):
    # an @reboot keep-alive (maximumRetries -1) re-arms ONLY when the boot
    # marker proves the boot run already happened this OS boot; a fresh
    # boot's @reboot fire supersedes the stale ladder instead.
    monkeypatch.setattr(platform_mod, "os_boot_id", lambda: "boot-A")
    cron = await _stateful_cron(tmp_path, _REBOOT_KEEPALIVE_JOB)
    try:
        job = cron.cron_jobs["r"]
        await cron.state_backend.append_record(
            "reboot/r",
            {
                "host": cron._state_host,
                "bootId": "boot-A",
                "bootTime": None,
                "jobDigest": job_digest(job),
                "at": _now_utc().isoformat(),
            },
        )
        await _seed_pending(
            cron, "r", 3, _now_utc() + datetime.timedelta(seconds=600)
        )
        cron._state_rehydrated = False
        await cron._rehydrate_from_state()
        state = cron.retry_state.get("r")
        assert state is not None and state.count == 3
        state.task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await state.task
        cron.retry_state.pop("r", None)

        # same pending, but the machine rebooted (marker from another boot)
        monkeypatch.setattr(platform_mod, "os_boot_id", lambda: "boot-B")
        await _seed_pending(
            cron, "r", 3, _now_utc() + datetime.timedelta(seconds=600)
        )
        cron._state_rehydrated = False
        await cron._rehydrate_from_state()
        assert "r" not in cron.retry_state
        await _drain_state_writes(cron)
        rec = await _newest(cron, "retries/r")
        assert rec["reason"] == "superseded-by-reboot"
    finally:
        await _stop_state(cron)


async def test_rearm_superseded_by_newer_run(tmp_path):
    # A run that finished AFTER the retry was armed proves the ladder was
    # resolved somehow (its settle write may have been dropped while the
    # store was down): settle, never re-run. Pins the double-run repro
    # where the state section is removed, the ladder resolves statelessly,
    # and the section is re-added later.
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        armed = _now_utc() - datetime.timedelta(seconds=120)
        job = cron.cron_jobs["j"]
        await cron.state_backend.append_record(
            "retries/j",
            {
                "kind": "pending",
                "attempt": 1,
                "notBefore": armed.isoformat(),
                "jobDigest": job_digest(job),
                "at": armed.isoformat(),
            },
        )
        finished = _now_utc() - datetime.timedelta(seconds=30)
        await cron.state_backend.append_record(
            "runs/j",
            {
                "outcome": "success",
                "exit_code": 0,
                "started_at": None,
                "finished_at": finished.isoformat(),
                "duration": None,
                "fail_reason": None,
            },
        )
        cron._state_rehydrated = False
        await cron._rehydrate_from_state()  # warms last_run, then re-arms
        assert "j" not in cron.retry_state
        await _drain_state_writes(cron)
        rec = await _newest(cron, "retries/j")
        assert rec["kind"] == "settled"
        assert rec["reason"] == "superseded-by-run"
    finally:
        await _stop_state(cron)


async def test_rearm_skips_other_hosts_pending(tmp_path):
    # a pending record written by ANOTHER host (shared store) is that
    # host's live ladder: neither re-armed nor settled here.
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        await cron.state_backend.append_record(
            "retries/j",
            {
                "kind": "pending",
                "attempt": 1,
                "notBefore": _now_utc().isoformat(),
                "jobDigest": job_digest(cron.cron_jobs["j"]),
                "host": "some-other-node",
                "at": _now_utc().isoformat(),
            },
        )
        cron._state_rehydrated = False
        await cron._rehydrate_from_state()
        assert "j" not in cron.retry_state
        await _drain_state_writes(cron)
        rec = await _newest(cron, "retries/j")
        assert rec["kind"] == "pending"  # untouched
    finally:
        await _stop_state(cron)


async def test_rearm_settles_when_retries_disabled(tmp_path):
    # maximumRetries edited to 0 since the ladder was armed: the stale
    # pending must settle now, not lurk until a later config revert.
    no_retry_yaml = (
        "jobs:\n  - name: j\n    command: ls\n    schedule: '0 0 * * *'\n"
    )
    cron = await _stateful_cron(tmp_path, no_retry_yaml)
    try:
        await cron.state_backend.append_record(
            "retries/j",
            {
                "kind": "pending",
                "attempt": 1,
                "notBefore": _now_utc().isoformat(),
                "jobDigest": "digest-from-the-retrying-definition",
                "at": _now_utc().isoformat(),
            },
        )
        cron._state_rehydrated = False
        await cron._rehydrate_from_state()
        assert "j" not in cron.retry_state
        await _drain_state_writes(cron)
        rec = await _newest(cron, "retries/j")
        assert rec["kind"] == "settled"
        assert rec["reason"] == "config-changed"
    finally:
        await _stop_state(cron)


async def test_retry_consume_settles_before_launch(tmp_path):
    # record-before-run: the settled("launched") record must land BEFORE
    # maybe_launch_job runs, so a crash right after the launch cannot
    # re-arm the attempt that already ran.
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        events = []
        backend = cron.state_backend
        real_append = backend.append_record

        async def spy_append(stream, data, *, prune_keep=None):
            if stream.startswith("retries/"):
                events.append("append:" + data.get("kind", "?"))
            return await real_append(stream, data, prune_keep=prune_keep)

        backend.append_record = spy_append  # type: ignore[method-assign]

        async def fake_launch(job, *, with_retries=True):
            events.append("launch")
            return True

        cron.maybe_launch_job = fake_launch  # type: ignore[method-assign]
        state = JobRetryState(1, 2, 60)
        state.next_delay()
        cron.retry_state["j"] = state
        await cron.schedule_retry_job("j", 0, 1)
        await _drain_state_writes(cron)
        assert "launch" in events
        launch_at = events.index("launch")
        assert "append:settled" in events[:launch_at]
        # and the pending append was ordered before the settle
        assert events.index("append:pending") < events.index("append:settled")
    finally:
        await _stop_state(cron)


# --- durable retries: the pre-launch consume marker -----------------------


async def test_retry_consume_ok_stateless_is_free(tmp_path):
    cron = Cron(None, config_yaml=_RETRY_JOB)
    assert await cron._retry_consume_ok("j", 1, quiet=False)


async def test_retry_consume_policy_on_store_error(tmp_path):
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:

        async def _boom(*_a, **_k):
            raise OSError("store down")

        cron.state_backend.append_record = _boom  # type: ignore[method-assign]
        # degrade (default): launch anyway, drop counted
        assert await cron._retry_consume_ok("j", 1, quiet=False)
        # fail-closed: defer like a closed gate
        cron._state_on_unavailable = "fail-closed"
        assert not await cron._retry_consume_ok("j", 1, quiet=False)
    finally:
        cron.state_backend = None


async def test_retry_consume_fail_closed_without_backend(tmp_path):
    cron = Cron(None, config_yaml=_RETRY_JOB)
    cron._state_configured = True
    cron._state_on_unavailable = "fail-closed"
    assert not await cron._retry_consume_ok("j", 1, quiet=False)
    cron._state_on_unavailable = "degrade"
    assert await cron._retry_consume_ok("j", 1, quiet=False)


async def test_retry_consume_survives_raised_store_error(
    tmp_path, monkeypatch
):
    # regression (crash-safety): the cross-node consume's claim-lease calls
    # were guarded only against TimeoutError, but the filesystem backend
    # RAISES OSError on a sick shared mount (flock ENOLCK/EIO/ESTALE). The
    # escape killed the schedule_retry_job task -- silently dropping the
    # due retry -- and was later re-raised out of cancel_job_retries'
    # awaiter, crashing the whole scheduler. A raised store error must
    # follow onStoreUnavailable exactly like a timeout: degrade (the
    # default) proceeds unserialized rather than dying.
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        monkeypatch.setattr(
            cron, "_retry_cross_node_eligible", lambda job: True
        )

        async def _boom(*_a, **_k):
            raise OSError("no locks available")

        cron.state_backend.acquire_lease = _boom  # type: ignore[method-assign]
        cron.state_backend.read_lease = _boom  # type: ignore[method-assign]
        calls, fake = _count_launcher()
        cron.maybe_launch_job = fake  # type: ignore[method-assign]
        state = JobRetryState(1, 2, 60)
        state.next_delay()
        cron.retry_state["j"] = state
        task = asyncio.create_task(cron.schedule_retry_job("j", 0.01, 1))
        state.task = task
        # on regression this re-raises the escaped OSError
        await asyncio.wait_for(task, timeout=20)
        assert task.exception() is None  # nothing stored for a later awaiter
        assert calls == ["j"]  # the retry fired instead of being dropped
        # the job's next fire cancels the ladder: must not re-raise either
        await cron.cancel_job_retries("j", settle="superseded")
    finally:
        await _stop_state(cron)


async def test_retry_consume_fail_closed_defers_on_raised_store_error(
    tmp_path, monkeypatch
):
    # fail-closed maps the raised store error to a deferral (the ladder
    # stays armed and re-checks), never to a launch without a claim -- and
    # never to an escape.
    cron = await _stateful_cron(
        tmp_path,
        _RETRY_JOB,
        extra_state="  onStoreUnavailable: fail-closed\n",
    )
    try:
        monkeypatch.setattr(
            cron, "_retry_cross_node_eligible", lambda job: True
        )

        async def _boom(*_a, **_k):
            raise OSError("no locks available")

        cron.state_backend.acquire_lease = _boom  # type: ignore[method-assign]
        cron.state_backend.read_lease = _boom  # type: ignore[method-assign]
        decision = await cron._retry_consume_decision(
            cron.cron_jobs["j"], 1, quiet=False
        )
        assert decision == "defer"
    finally:
        await _stop_state(cron)


async def test_cancel_job_retries_swallows_dead_task_error(tmp_path, caplog):
    # belt and suspenders for the same crash: cancel_job_retries runs on
    # the launch path (launch_scheduled_job), outside run()'s try/except,
    # so an exception stored in a dead retry task must be logged and
    # swallowed there -- never re-raised into the scheduler loop.
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        state = JobRetryState(1, 2, 60)
        state.next_delay()

        async def _dead():
            raise OSError("flock: no locks available")

        task = asyncio.create_task(_dead())
        await asyncio.wait({task})
        state.task = task
        cron.retry_state["j"] = state
        await cron.cancel_job_retries("j", settle="superseded")
        assert "j" not in cron.retry_state
        assert any("retry task died" in r.getMessage() for r in caplog.records)
    finally:
        await _stop_state(cron)


# --- standalone @reboot dedupe --------------------------------------------


async def test_reboot_marker_dedupes_daemon_restart(tmp_path, monkeypatch):
    monkeypatch.setattr(platform_mod, "os_boot_id", lambda: "boot-A")
    monkeypatch.setattr(platform_mod, "os_boot_time", lambda: None)

    cron = await _stateful_cron(tmp_path, _REBOOT_JOB)
    try:
        calls, fake = _count_launcher()
        cron.maybe_launch_job = fake  # type: ignore[method-assign]
        await cron._spawn_reboot_jobs()
        assert calls == ["r"]
        rec = await _newest(cron, "reboot/r")
        assert rec["bootId"] == "boot-A"
        assert rec["host"] == cron._state_host
        assert rec["jobDigest"] == job_digest(cron.cron_jobs["r"])

        # "daemon restart" within the same boot: a fresh Cron over the same
        # store skips the re-run
        cron2 = await _stateful_cron(tmp_path, _REBOOT_JOB)
        try:
            calls2, fake2 = _count_launcher()
            cron2.maybe_launch_job = fake2  # type: ignore[method-assign]
            await cron2._spawn_reboot_jobs()
            assert calls2 == []
        finally:
            await _stop_state(cron2)

        # a genuine reboot runs it again
        monkeypatch.setattr(platform_mod, "os_boot_id", lambda: "boot-B")
        cron3 = await _stateful_cron(tmp_path, _REBOOT_JOB)
        try:
            calls3, fake3 = _count_launcher()
            cron3.maybe_launch_job = fake3  # type: ignore[method-assign]
            await cron3._spawn_reboot_jobs()
            assert calls3 == ["r"]
        finally:
            await _stop_state(cron3)
    finally:
        await _stop_state(cron)


async def test_reboot_redefined_job_runs_again(tmp_path, monkeypatch):
    monkeypatch.setattr(platform_mod, "os_boot_id", lambda: "boot-A")
    cron = await _stateful_cron(tmp_path, _REBOOT_JOB)
    try:
        await cron.state_backend.append_record(
            "reboot/r",
            {
                "host": cron._state_host,
                "bootId": "boot-A",
                "bootTime": None,
                "jobDigest": "digest-of-an-older-definition",
                "at": _now_utc().isoformat(),
            },
        )
        calls, fake = _count_launcher()
        cron.maybe_launch_job = fake  # type: ignore[method-assign]
        await cron._spawn_reboot_jobs()
        assert calls == ["r"]
    finally:
        await _stop_state(cron)


async def test_reboot_marker_recorded_before_launch(tmp_path, monkeypatch):
    # at-most-once ordering, like the cluster mark_reboot_ran path: a crash
    # between record and spawn must err toward NOT re-running.
    monkeypatch.setattr(platform_mod, "os_boot_id", lambda: "boot-A")
    cron = await _stateful_cron(tmp_path, _REBOOT_JOB)
    try:
        events = []
        backend = cron.state_backend
        real_append = backend.append_record

        async def spy_append(stream, data, *, prune_keep=None):
            if stream.startswith("reboot/"):
                events.append("record")
            return await real_append(stream, data, prune_keep=prune_keep)

        backend.append_record = spy_append  # type: ignore[method-assign]

        async def fake_launch(job):
            events.append("launch")

        cron.launch_scheduled_job = fake_launch  # type: ignore[method-assign]
        await cron._spawn_reboot_jobs()
        assert events == ["record", "launch"]
    finally:
        cron.state_backend = None


async def test_reboot_store_error_policy(tmp_path, monkeypatch):
    monkeypatch.setattr(platform_mod, "os_boot_id", lambda: "boot-A")
    cron = await _stateful_cron(tmp_path, _REBOOT_JOB)
    try:

        async def _boom(*_a, **_k):
            raise OSError("store down")

        cron.state_backend.list_records = _boom  # type: ignore[method-assign]
        cron.state_backend.append_record = _boom  # type: ignore[method-assign]
        # degrade: run anyway (at-least-once, the stateless behaviour)
        assert await cron._reboot_boot_gate(cron.cron_jobs["r"])
        # fail-closed: prefer not running
        cron._state_on_unavailable = "fail-closed"
        assert not await cron._reboot_boot_gate(cron.cron_jobs["r"])
        # fail-closed with the store configured but not started
        cron.state_backend = None
        assert not await cron._reboot_boot_gate(cron.cron_jobs["r"])
    finally:
        cron.state_backend = None


def test_same_boot_time_tolerance(monkeypatch):
    cron = Cron(None, config_yaml=_REBOOT_JOB)
    monkeypatch.setattr(platform_mod, "os_boot_id", lambda: None)
    monkeypatch.setattr(platform_mod, "os_boot_time", lambda: 1000.0)
    assert cron._same_boot({"bootTime": 1030.0})
    assert not cron._same_boot({"bootTime": 2000.0})
    # cannot tell -> not provably the same boot -> the job runs
    monkeypatch.setattr(platform_mod, "os_boot_time", lambda: None)
    assert not cron._same_boot({"bootTime": 1000.0})
    monkeypatch.setattr(platform_mod, "os_boot_id", lambda: "boot-A")
    assert cron._same_boot({"bootId": "boot-A"})
    assert not cron._same_boot({"bootId": "boot-B"})


# --- durable Prometheus counters ------------------------------------------


def test_counter_snapshot_seed_roundtrip():
    src = PrometheusMetrics()
    src.job_run_recorded("j", "success", 2.0)
    src.job_run_recorded("j", "failure", None)
    src.job_retry_launched("j")
    src.job_run_recorded("gone", "success", 1.0)
    snap = src.counters_snapshot()

    dst = PrometheusMetrics()
    dst.job_run_recorded("j", "success", 1.0)  # pre-seed event survives
    assert dst.seed_counters(snap, keep={"j"}) == 1
    job = dst._jobs["j"]
    assert job.runs == {"success": 2, "failure": 1}
    assert job.retries == 1
    assert job.duration_count == 2  # 1 live + 1 seeded
    assert "gone" not in dst._jobs  # prune contract: only loaded jobs


def test_counter_seed_skips_mismatched_buckets():
    src = PrometheusMetrics()
    src.set_duration_buckets((1.0, 5.0))
    src.job_run_recorded("j", "success", 0.5)
    snap = src.counters_snapshot()

    dst = PrometheusMetrics()  # default buckets != (1.0, 5.0)
    assert dst.seed_counters(snap, keep={"j"}) == 1
    job = dst._jobs["j"]
    # outcome counters seeded; histogram left at zero (bucket-change rule)
    assert job.runs == {"success": 1}
    assert job.duration_count == 0
    assert all(c == 0 for c in job.bucket_counts)


def test_counter_seed_survives_garbage():
    dst = PrometheusMetrics()
    assert dst.seed_counters({"jobs": "nope"}, keep={"j"}) == 0
    snap = {
        "buckets": "wat",
        "jobs": {"j": {"runs": {"success": "many"}, "retries": True}},
    }
    assert dst.seed_counters(snap, keep={"j"}) == 1
    job = dst._jobs["j"]
    assert job.runs == {}
    assert job.retries == 0


async def test_counters_persist_and_rehydrate(tmp_path):
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        cron._record_run("j", _info(second=1, outcome="success"))
        await _drain_state_writes(cron)
    finally:
        await _stop_state(cron)

    cron2 = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        assert cron2._counters_seeded
        assert cron2.metrics._jobs["j"].runs.get("success") == 1
        # once per process: a second rehydration cannot double-count
        cron2._state_rehydrated = False
        await cron2._rehydrate_from_state()
        assert cron2.metrics._jobs["j"].runs.get("success") == 1
    finally:
        await _stop_state(cron2)


async def test_final_counter_snapshot_not_throttled(tmp_path):
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        cron.metrics.job_run_recorded("j", "success", 1.0)
        # simulate the throttle window being closed
        cron._counter_snapshot_next = asyncio.get_running_loop().time() + 999
        await cron._persist_counter_snapshot(throttled=True)
        assert await _newest(cron, cron._counters_stream()) is None
        # the shutdown path writes unthrottled
        await cron._persist_counter_snapshot()
        rec = await _newest(cron, cron._counters_stream())
        assert rec is not None
        assert rec["jobs"]["j"]["runs"] == {"success": 1}
    finally:
        await _stop_state(cron)


# --- SLA trends endpoint ---------------------------------------------------


class _FakeRequest:
    def __init__(self, name):
        self.match_info = {"name": name}
        self.headers = {}


async def test_trends_from_durable_ledger(tmp_path):
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    cron.web_config = {}
    try:
        now = _now_utc()

        async def put(minutes_ago, outcome):
            finished = now - datetime.timedelta(minutes=minutes_ago)
            await cron.state_backend.append_record(
                "runs/j",
                {
                    "outcome": outcome,
                    "exit_code": 0,
                    "started_at": (
                        finished - datetime.timedelta(seconds=10)
                    ).isoformat(),
                    "finished_at": finished.isoformat(),
                    "duration": 10.0,
                    "fail_reason": None,
                },
            )

        await put(30, "success")  # inside 1h
        await put(60 * 30, "failure")  # inside 7d/30d, outside 24h
        resp = await cron._web_job_trends(_FakeRequest("j"))
        body = json.loads(resp.body)
        assert body["source"] == "durable"
        assert body["windows"]["1h"]["total"] == 1
        assert body["windows"]["24h"]["total"] == 1
        assert body["windows"]["7d"]["total"] == 2
        assert body["windows"]["all"]["total"] == 2
        assert body["windows"]["7d"]["success_rate"] == 0.5
    finally:
        await _stop_state(cron)


async def test_trends_degrades_to_memory_and_404s(tmp_path):
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    cron.web_config = {}
    try:

        async def _boom(*_a, **_k):
            raise OSError("store down")

        cron.state_backend.list_records = _boom  # type: ignore[method-assign]
        cron._record_run("j", _info(second=1, outcome="success"))
        resp = await cron._web_job_trends(_FakeRequest("j"))
        body = json.loads(resp.body)
        assert body["source"] == "memory"
        assert body["windows"]["all"]["total"] == 1
        with pytest.raises(web.HTTPNotFound):
            await cron._web_job_trends(_FakeRequest("nope"))
    finally:
        cron.state_backend = None


# --- onStoreUnavailable and the depends-on-past gate -----------------------


async def test_depends_on_past_fail_closed_on_store_error(tmp_path):
    cron = await _stateful_cron(
        tmp_path, _DEP_JOB, extra_state="  onStoreUnavailable: fail-closed\n"
    )
    try:
        assert cron._state_on_unavailable == "fail-closed"

        async def _boom(*_a, **_k):
            raise OSError("store down")

        cron.state_backend.list_records = _boom  # type: ignore[method-assign]
        assert not await cron._depends_on_past_ok(cron.cron_jobs["d"])
        # degrade decides from the (empty) memory instead: allow
        cron._state_on_unavailable = "degrade"
        assert await cron._depends_on_past_ok(cron.cron_jobs["d"])
    finally:
        cron.state_backend = None


async def test_depends_on_past_fail_closed_store_down(tmp_path):
    cron = Cron(None, config_yaml=_DEP_JOB)
    cron._state_configured = True
    cron._state_on_unavailable = "fail-closed"
    assert not await cron._depends_on_past_ok(cron.cron_jobs["d"])
    cron._state_on_unavailable = "degrade"
    assert await cron._depends_on_past_ok(cron.cron_jobs["d"])


# --- manifests, GC keep-set, dropped-write accounting ----------------------


async def test_state_periodic_writes_manifest(tmp_path):
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        cron._state_periodic()
        await _drain_state_writes(cron)
        rec = await _newest(cron, cron._manifest_stream())
        assert rec is not None
        assert rec["jobs"] == ["j"]
        assert rec["host"] == cron._state_host
        assert rec["jobSetId"].startswith("v1:")
        # not due again immediately
        before = cron._manifest_next
        cron._state_periodic()
        assert cron._manifest_next == before
    finally:
        await _stop_state(cron)


async def test_collect_state_garbage_keeps_manifested_jobs(
    tmp_path, monkeypatch
):
    import cronstable.state as state_mod

    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        backend = cron.state_backend
        old_epoch = state_mod._now() - 7200.0
        # records whose stream must die (old + unreferenced) vs survive
        monkeypatch.setattr(state_mod, "_now", lambda: old_epoch)
        await backend.append_record("runs/orphan", {"finished_at": "x"})
        await backend.append_record("runs/manifested", {"finished_at": "x"})
        await backend.append_record("runs/j", {"finished_at": "x"})
        monkeypatch.undo()
        # an OLD manifest satisfying the history-depth guard (the manifest
        # window must span the grace before anything may be deleted) ...
        # manifests are per-host streams under "manifests/<host>".
        await backend.append_record(
            "manifests/old-host",
            {
                "jobSetId": "v1:old",
                "host": "old-host",
                "jobs": [],
                "at": (
                    _now_utc() - datetime.timedelta(seconds=7200)
                ).isoformat(),
            },
        )
        # ... and a recent manifest from "another node" claiming 'manifested'
        await backend.append_record(
            "manifests/other-host",
            {
                "jobSetId": "v1:other",
                "host": "other-host",
                "jobs": ["manifested"],
                "at": _now_utc().isoformat(),
            },
        )
        cron._state_gc_grace = 3600.0
        await cron._collect_state_garbage()
        streams = {
            "runs/orphan": await backend.list_records("runs/orphan"),
            "runs/manifested": await backend.list_records("runs/manifested"),
            "runs/j": await backend.list_records("runs/j"),
        }
        assert streams["runs/orphan"] == []
        assert len(streams["runs/manifested"]) == 1  # manifest kept it
        assert len(streams["runs/j"]) == 1  # loaded config kept it
    finally:
        await _stop_state(cron)


async def test_manifest_per_host_streams_survive_large_fleet(
    tmp_path, monkeypatch
):
    # Regression test: every node used to write to ONE shared, count-pruned
    # manifest stream, so a large fleet's write volume pushed the retained
    # history's oldest record younger than gcGraceSeconds and automatic GC
    # deferred FOREVER (removed jobs' streams then grew without bound).
    # Per-host streams (manifests/<host>) mean one host's own retained span
    # never shrinks no matter how many OTHER hosts join the fleet.
    import cronstable.state as state_mod

    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        backend = cron.state_backend
        old_epoch = state_mod._now() - 7200.0
        monkeypatch.setattr(state_mod, "_now", lambda: old_epoch)
        await backend.append_record("runs/orphan", {"finished_at": "x"})
        monkeypatch.undo()
        # this node's own manifest satisfies the history-depth guard.
        await backend.append_record(
            cron._manifest_stream(),
            {
                "jobSetId": "v1:self",
                "host": cron._state_host,
                "jobs": [],
                "at": (
                    _now_utc() - datetime.timedelta(seconds=7200)
                ).isoformat(),
            },
        )
        # a "fleet" of 50 OTHER hosts, each writing its OWN fresh manifest --
        # exactly the write-volume scenario that starved the old shared
        # stream's retained history once the fleet grew past a few nodes.
        for i in range(50):
            await backend.append_record(
                "manifests/other-{}".format(i),
                {
                    "jobSetId": "v1:other",
                    "host": "other-{}".format(i),
                    "jobs": [],
                    "at": _now_utc().isoformat(),
                },
            )
        cron._state_gc_grace = 3600.0
        await cron._collect_state_garbage()
        # GC proceeded rather than deferring: the orphan stream is gone.
        assert await backend.list_records("runs/orphan") == []
    finally:
        await _stop_state(cron)


async def test_list_stream_names_finds_prefix_members_only(tmp_path):
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        backend = cron.state_backend
        await backend.append_record("manifests/a", {"x": 1})
        await backend.append_record("manifests/b", {"x": 1})
        await backend.append_record("runs/unrelated", {"x": 1})
        names = await backend.list_stream_names("manifests/")
        assert set(names) == {"manifests/a", "manifests/b"}
    finally:
        await _stop_state(cron)


async def test_dropped_writes_counted_and_rendered(tmp_path):
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:

        async def _boom(*_a, **_k):
            raise OSError("store down")

        cron.state_backend.append_record = _boom  # type: ignore[method-assign]
        cron._record_run("j", _info(second=1, outcome="failure"))
        await _drain_state_writes(cron)
        assert cron.metrics._state_dropped.get("run-record") == 1
        text = cron.metrics.render(cron)
        assert (
            'cronstable_state_dropped_writes_total{kind="run-record"} 1'
            in text
        )
    finally:
        cron.state_backend = None


async def test_state_metric_families_rendered(tmp_path):
    from tests.test_prometheus import sample_value

    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        cron._record_run("j", _info(second=1, outcome="success"))
        await _drain_state_writes(cron)
        text = cron.metrics.render(cron)
        assert (
            sample_value(text, "cronstable_state_ops_total", op="append") >= 1
        )
        assert (
            sample_value(text, "cronstable_state_op_errors_total", op="append")
            == 0
        )
        assert (
            sample_value(
                text, "cronstable_state_op_seconds_total", op="append"
            )
            is not None
        )
        assert (
            sample_value(
                text,
                "cronstable_state_info",
                backend="filesystem",
                topology="single-node",
            )
            == 1
        )
        # scrape survives a stats() blow-up (degrade, never 500)
        cron.state_backend.stats = _raise_runtime  # type: ignore[method-assign]
        text = cron.metrics.render(cron)
        assert "cronstable_state_ops_total" not in text
        assert (
            sample_value(
                text,
                "cronstable_job_runs_total",
                **{"job_name": "j", "status": "success"},
            )
            == 1
        )
    finally:
        await _stop_state(cron)


def _raise_runtime():
    raise RuntimeError("boom")
