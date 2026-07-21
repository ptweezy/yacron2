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
from cronstable.cron import Cron, PauseInfo
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
        # Recorded through _record_run, not poked into last_run: the guard
        # reads the skip-blind watermark that only _record_run maintains.
        cron._record_run("j", _info(second=30))
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


# --- superseded-by-run vs. pause-held slots -------------------------------
#
# A pause-held slot appends a synthetic "skipped" ledger row stamped NOW so
# the catch-up watermark keeps advancing across the window. Nothing ran, so
# that row must never satisfy the retry ladder's superseded-by-run guards:
# not in memory, not across a restart, not on a peer's claim scan.


async def _seed_run_record(cron, name, finished, outcome, ran_at=True):
    """Append a hand-written ledger row the way a real run would land."""
    rec = {
        "outcome": outcome,
        "exit_code": 0 if outcome == "success" else 1,
        "started_at": None,
        "finished_at": finished.isoformat(),
        "duration": None,
        "fail_reason": None if outcome == "success" else "boom",
    }
    if ran_at:
        rec["ranAt"] = finished.isoformat()
    await cron.state_backend.append_record("runs/" + name, rec)


async def _seed_pending_armed_at(cron, name, attempt, not_before, at):
    """`_seed_pending` with an explicit arm instant (its ``at``)."""
    await cron.state_backend.append_record(
        "retries/" + name,
        {
            "kind": "pending",
            "attempt": attempt,
            "notBefore": not_before.isoformat(),
            "jobDigest": job_digest(cron.cron_jobs[name]),
            "at": at.isoformat(),
        },
    )


async def _hold_slots(cron, name, count):
    """Pause the job and drive `count` scheduled fires into the pause gate."""
    await cron.pause_job_by_name(name, duration=7200)
    for _ in range(count):
        await cron.launch_scheduled_job(cron.cron_jobs[name])
    await _drain_state_writes(cron)


async def test_pause_skip_row_is_absent_from_the_run_watermark(tmp_path):
    # The two watermarks part ways over a held slot: catch-up's keeps
    # advancing (finished_at), the supersede guards' does not (ranAt).
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        cron._record_run("j", _info())
        await _drain_state_writes(cron)
        ran = cron.last_run["j"].finished_at
        await _hold_slots(cron, "j", 3)
        assert cron.last_run["j"].outcome == "skipped"
        skipped_at = cron.last_run["j"].finished_at
        assert skipped_at > ran
        assert await cron.durable_last_run_at("j") == skipped_at.isoformat()
        completed = await cron.durable_last_completed_at("j")
        assert completed == ran.isoformat()
        assert cron._last_completed_at["j"] == ran
    finally:
        await _stop_state(cron)


async def test_pause_skip_rows_do_not_settle_a_ladder_across_a_restart(
    tmp_path,
):
    # The restart half of the in-process promise that a paused job's pending
    # retry "waits and fires after the resume": the held slots stamped after
    # the arm must not read as the run that resolved the ladder.
    armed = _now_utc() - datetime.timedelta(seconds=900)
    not_before = _now_utc() + datetime.timedelta(seconds=600)
    first = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        await _seed_run_record(
            first, "j", armed - datetime.timedelta(seconds=5), "failure"
        )
        await _seed_pending_armed_at(first, "j", 1, not_before, armed)
        await _hold_slots(first, "j", 3)
        rows = await first.state_backend.list_records("runs/j")
        assert [r["outcome"] for r in rows[-3:]] == ["skipped"] * 3
    finally:
        await _stop_state(first)

    second = await _stateful_cron(tmp_path, _RETRY_JOB)  # the restart
    try:
        state = second.retry_state.get("j")
        assert state is not None and state.count == 1
        state.task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await state.task
        second.retry_state.pop("j", None)
        await _drain_state_writes(second)
        rec = await _newest(second, "retries/j")
        assert rec["kind"] == "pending"  # never settled by the pause
    finally:
        await _stop_state(second)


async def test_real_run_under_a_pause_still_settles_the_ladder(tmp_path):
    # The other half: the guard must track the newest NON-skipped instant,
    # not merely ignore itself whenever the newest row is a skip. A real run
    # that landed after the arm and was then buried under held slots still
    # resolves the ladder, because no-run beats double-run.
    armed = _now_utc() - datetime.timedelta(seconds=900)
    ran = _now_utc() - datetime.timedelta(seconds=300)
    not_before = _now_utc() + datetime.timedelta(seconds=600)
    first = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        await _seed_pending_armed_at(first, "j", 1, not_before, armed)
        await _seed_run_record(first, "j", ran, "success")
        await _hold_slots(first, "j", 3)
    finally:
        await _stop_state(first)

    second = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        assert "j" not in second.retry_state
        await _drain_state_writes(second)
        rec = await _newest(second, "retries/j")
        assert rec["kind"] == "settled"
        assert rec["reason"] == "superseded-by-run"
    finally:
        await _stop_state(second)


@pytest.mark.parametrize("held", [48, 49, 50, 55])
async def test_real_run_under_a_long_pause_still_settles_the_ladder(
    tmp_path, held
):
    # The warmed ring is capped at RUN_HISTORY_LIMIT (50), so once a pause
    # holds that many slots the real run that resolved the ladder is flooded
    # out of the ring and _last_completed_at is never seeded from history.
    # The re-arm must then fall back to the flood-independent durable fold,
    # or a ladder a real run already superseded is re-armed and double-runs.
    # This is a REGRESSION from the memo switch: pre-memo the skip row's own
    # fresh finished_at settled this case by accident. AT and ACROSS the
    # boundary: 48/49 keep the run inside the ring (still correct today),
    # 50/55 push it out (broken without the durable seed).
    armed = _now_utc() - datetime.timedelta(seconds=900)
    ran = _now_utc() - datetime.timedelta(seconds=300)
    not_before = _now_utc() + datetime.timedelta(seconds=600)
    first = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        await _seed_pending_armed_at(first, "j", 1, not_before, armed)
        await _seed_run_record(first, "j", ran, "success")
        await _hold_slots(first, "j", held)
        # nothing was pruned (maxRunsPerJob defaults to 0/unlimited): the real
        # run is still in the ledger, just below the newest-50 window the
        # warm-up reads.
        rows = await first.state_backend.list_records("runs/j")
        assert sum(1 for r in rows if r["outcome"] == "skipped") == held
        assert sum(1 for r in rows if r["outcome"] == "success") == 1
    finally:
        await _stop_state(first)

    second = await _stateful_cron(tmp_path, _RETRY_JOB)  # the restart
    try:
        assert "j" not in second.retry_state  # settled, never re-armed
        await _drain_state_writes(second)
        rec = await _newest(second, "retries/j")
        assert rec["kind"] == "settled"
        assert rec["reason"] == "superseded-by-run"
    finally:
        await _stop_state(second)


@pytest.mark.parametrize("held", [49, 50, 55])
async def test_pre_ranat_run_under_a_long_pause_still_settles_the_ladder(
    tmp_path, held
):
    # #7 residual: the resolving run was written BEFORE the ``ranAt`` field
    # existed (finished_at only), so derive_max("ranAt") sees nothing and the
    # pre-ranAt compatibility fold in durable_last_completed_at must find it.
    # That fold reads only the newest RUN_HISTORY_LIMIT (50) rows, so once the
    # pause holds >= 50 slots the real run falls outside that window and the
    # capped fold returns None, re-arming a resolved ladder into a double-run
    # unless the deeper pre-ranAt re-read fires. AT and ACROSS the boundary:
    # held=49 keeps the run inside the capped window (settles today), 50/55
    # push it out (needs the deep re-read). Reached through the LOCAL retry
    # rehydrate seeding _last_completed_at, not a peer claim scan.
    armed = _now_utc() - datetime.timedelta(seconds=900)
    ran = _now_utc() - datetime.timedelta(seconds=300)
    not_before = _now_utc() + datetime.timedelta(seconds=600)
    first = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        await _seed_pending_armed_at(first, "j", 1, not_before, armed)
        await _seed_run_record(first, "j", ran, "success", ran_at=False)
        await _hold_slots(first, "j", held)
        rows = await first.state_backend.list_records("runs/j")
        assert sum(1 for r in rows if r["outcome"] == "skipped") == held
        assert sum(1 for r in rows if r["outcome"] == "success") == 1
        # the resolving run carries no ``ranAt`` (the pre-upgrade shape).
        assert not any("ranAt" in r for r in rows)
    finally:
        await _stop_state(first)

    second = await _stateful_cron(tmp_path, _RETRY_JOB)  # the restart
    try:
        assert "j" not in second.retry_state  # settled, never re-armed
        await _drain_state_writes(second)
        rec = await _newest(second, "retries/j")
        assert rec["kind"] == "settled"
        assert rec["reason"] == "superseded-by-run"
    finally:
        await _stop_state(second)


async def test_claim_scan_ignores_a_peers_pause_skip_rows(tmp_path):
    # The scan's in-memory half: every node rehydrates the SHARED ledger, so
    # the pausing owner's held slots become this node's last_run as well.
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        cron._state_host = "node-b"
        cron._record_run("j", _info())
        await _hold_slots(cron, "j", 3)
        assert cron.last_run["j"].outcome == "skipped"
        stale = _now_utc() - datetime.timedelta(seconds=120)
        foreign = {
            "kind": "pending",
            "attempt": 1,
            "notBefore": stale.isoformat(),
            "jobDigest": job_digest(cron.cron_jobs["j"]),
            "host": "node-a",
            "at": (_now_utc() - datetime.timedelta(seconds=900)).isoformat(),
        }
        assert cron._retry_record_claimable(
            "j", cron.cron_jobs["j"], foreign
        ) == (1, stale)
    finally:
        await _stop_state(cron)


async def test_peer_claim_folds_pre_ranat_rows_but_not_skips(tmp_path):
    # An upgraded ledger: rows written before ``ranAt`` existed still prove
    # the ladder resolved, while the skip rows appended over them do not.
    armed = _now_utc() - datetime.timedelta(seconds=900)
    ran = _now_utc() - datetime.timedelta(seconds=300)
    not_before = _now_utc() + datetime.timedelta(seconds=600)
    owner = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        await _seed_run_record(owner, "j", ran, "success", ran_at=False)
        await _hold_slots(owner, "j", 3)
    finally:
        await _stop_state(owner)

    taker = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        taker._state_host = "node-b"
        taker._last_completed_at.pop("j", None)  # only the ledger may decide
        foreign = {
            "kind": "pending",
            "attempt": 1,
            "notBefore": not_before.isoformat(),
            "jobDigest": job_digest(taker.cron_jobs["j"]),
            "host": "node-a",
            "at": armed.isoformat(),
        }
        await taker.state_backend.append_record("retries/j", foreign)
        claimed = await taker._claim_retry_under_lease(
            "j", taker.cron_jobs["j"], foreign, 1, not_before
        )
        assert claimed is False
        await _drain_state_writes(taker)
        rec = await _newest(taker, "retries/j")
        assert rec["kind"] == "settled"
        assert rec["reason"] == "superseded-by-run"
    finally:
        await _stop_state(taker)


async def test_peer_claim_ignores_pause_skip_rows_in_the_ledger(tmp_path):
    # The cross-node half: the claim scan judges supersession off the shared
    # ledger, where the pausing owner's held slots are all a peer can see.
    armed = _now_utc() - datetime.timedelta(seconds=900)
    not_before = _now_utc() + datetime.timedelta(seconds=600)
    owner = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        owner._state_host = "node-a"
        await _seed_run_record(
            owner, "j", armed - datetime.timedelta(seconds=5), "failure"
        )
        await _hold_slots(owner, "j", 3)
    finally:
        await _stop_state(owner)

    taker = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        taker._state_host = "node-b"
        foreign = {
            "kind": "pending",
            "attempt": 1,
            "notBefore": not_before.isoformat(),
            "jobDigest": job_digest(taker.cron_jobs["j"]),
            "host": "node-a",
            "at": armed.isoformat(),
        }
        await taker.state_backend.append_record("retries/j", foreign)
        claimed = await taker._claim_retry_under_lease(
            "j", taker.cron_jobs["j"], foreign, 1, not_before
        )
        assert claimed is True
        await _drain_state_writes(taker)
        rec = await _newest(taker, "retries/j")
        assert rec["kind"] == "pending"
        assert rec["host"] == "node-b"
        assert rec["claimedFrom"] == "node-a"
    finally:
        await _stop_state(taker)


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


async def test_paused_reboot_defers_its_boot_run_across_a_restart(
    tmp_path, monkeypatch
):
    # a pause DEFERS an @reboot job's boot run, it does not forfeit it: the
    # once-per-boot marker stays unwritten while the job is paused, so the
    # run is still owed after a daemon restart inside the same OS boot, and
    # it fires exactly once when the pause lifts.
    monkeypatch.setattr(platform_mod, "os_boot_id", lambda: "boot-A")
    monkeypatch.setattr(platform_mod, "os_boot_time", lambda: None)
    cron = await _stateful_cron(tmp_path, _REBOOT_JOB)
    try:
        await cron.pause_job_by_name("r", duration=3600)
        await _drain_state_writes(cron)
        calls, fake = _count_launcher()
        cron.maybe_launch_job = fake  # type: ignore[method-assign]
        await cron._spawn_reboot_jobs()
        assert calls == []
        # the boot token is NOT spent on a run that did not happen
        assert await _newest(cron, "reboot/r") is None
        assert "r" in cron._paused_reboot_jobs

        # daemon restart inside the same OS boot: still paused, still owed
        cron2 = await _stateful_cron(tmp_path, _REBOOT_JOB)
        try:
            assert cron2._pause_active("r") is not None
            calls2, fake2 = _count_launcher()
            cron2.maybe_launch_job = fake2  # type: ignore[method-assign]
            await cron2._spawn_reboot_jobs()
            assert calls2 == []
            assert await _newest(cron2, "reboot/r") is None

            # the pause lifts -> the deferred boot run happens, once
            await cron2.resume_job_by_name("r")
            await _drain_state_writes(cron2)
            await cron2._process_paused_reboots()
            assert calls2 == ["r"]
            rec = await _newest(cron2, "reboot/r")
            assert rec["bootId"] == "boot-A"
            await cron2._process_paused_reboots()
            assert calls2 == ["r"]
            assert not cron2._paused_reboot_jobs
        finally:
            await _stop_state(cron2)

        # ...and a later restart in the same OS boot does not repeat it
        cron3 = await _stateful_cron(tmp_path, _REBOOT_JOB)
        try:
            calls3, fake3 = _count_launcher()
            cron3.maybe_launch_job = fake3  # type: ignore[method-assign]
            await cron3._spawn_reboot_jobs()
            assert calls3 == []
        finally:
            await _stop_state(cron3)
    finally:
        await _stop_state(cron)


async def test_paused_reboot_in_the_record_then_run_window_still_runs(
    tmp_path, monkeypatch
):
    # #8 residual (a REGRESSION-adjacent loss the defer fix did not close):
    # a pause that arrives DURING the record-then-run window -- after
    # _reboot_boot_gate has burnt this OS boot's marker, before the launcher's
    # pause gate -- must NOT forfeit the boot run. The once-per-boot token is
    # already spent and cannot be un-spent, so an @reboot job is exempt from
    # launch_scheduled_job's pause gate. Standalone path (via
    # _spawn_reboot_jobs); the cluster path shares the same launcher exemption.
    monkeypatch.setattr(platform_mod, "os_boot_id", lambda: "boot-A")
    monkeypatch.setattr(platform_mod, "os_boot_time", lambda: None)
    cron = await _stateful_cron(tmp_path, _REBOOT_JOB)
    try:
        calls, fake = _count_launcher()
        cron.maybe_launch_job = fake  # type: ignore[method-assign]

        real_gate = cron._reboot_boot_gate

        async def gate_then_pause(job):
            # spend the token (write the marker) via the real gate, then a
            # concurrent pause task installs the pause mid-window -- exactly
            # what _pause_periodic / a web pause handler can do at any await
            # point between the gate and the launcher.
            allowed = await real_gate(job)
            await cron.pause_job_by_name(job.name, duration=3600)
            return allowed

        cron._reboot_boot_gate = gate_then_pause  # type: ignore[method-assign]
        await cron._spawn_reboot_jobs()
        await _drain_state_writes(cron)

        # the token WAS spent (the marker is present)...
        assert await _newest(cron, "reboot/r") is not None
        # ...so the boot run must actually happen, not be skipped
        assert calls == ["r"]
        # and no synthetic "skipped" row stands in for a lost run
        rows = await cron.state_backend.list_records("runs/r")
        assert [r for r in rows if r.get("outcome") == "skipped"] == []
    finally:
        await _stop_state(cron)


async def test_launch_gate_still_skips_a_paused_non_reboot_job(tmp_path):
    # The @reboot pause-gate exemption is NARROW: an ordinary scheduled job
    # that is paused still skips at the launcher and writes its synthetic
    # skip row, so the catch-up watermark keeps advancing. Guards the fix
    # against over-broadening the exemption.
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:
        calls, fake = _count_launcher()
        cron.maybe_launch_job = fake  # type: ignore[method-assign]
        await cron.pause_job_by_name("j", duration=3600)
        await _drain_state_writes(cron)
        await cron.launch_scheduled_job(cron.cron_jobs["j"])
        assert calls == []  # never launched
        assert cron.last_run["j"].outcome == "skipped"
    finally:
        await _stop_state(cron)


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


async def test_trends_single_pass_matches_filter_per_window(
    tmp_path, monkeypatch
):
    """The one-pass window bucketing equals the old filter-per-window math.

    Crafted history spanning every TREND_WINDOWS bucket, with a record
    sitting exactly ON each window edge (the inclusive `<= seconds` rule
    must keep it in that window) and one just past it, appended OUT of
    finished_at order (fire-and-forget persistence and shared-mount merges
    do not guarantee time order in the ledger).  The payload's windows must
    match a reference computed with the original per-window filter over the
    same rehydrated infos, including the order-sensitive last_* fields.
    """
    from cronstable.cron import (
        TREND_WINDOWS,
        _job_run_info_from_dict,
        _run_stats,
    )

    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    cron.web_config = {}
    try:
        now = _now_utc()
        # freeze the module seam so "exactly at the edge" stays exact when
        # the builder computes ages
        monkeypatch.setattr("cronstable.cron.get_now", lambda tz: now)

        async def put(seconds_ago, outcome, duration):
            finished = now - datetime.timedelta(seconds=seconds_ago)
            await cron.state_backend.append_record(
                "runs/j",
                {
                    "outcome": outcome,
                    "exit_code": 0 if outcome == "success" else 1,
                    "started_at": (
                        finished - datetime.timedelta(seconds=duration)
                    ).isoformat(),
                    "finished_at": finished.isoformat(),
                    "duration": duration,
                    "fail_reason": None,
                },
            )

        # (seconds_ago, outcome): each window edge exactly, each edge just
        # missed, plus interior points; scrambled so append order differs
        # from time order. Distinct durations make last_duration/avg detect
        # any membership or ordering drift.
        history = [
            (86400, "success"),  # exactly the 24h edge
            (60, "success"),  # interior 1h
            (2592001, "failure"),  # just past 30d: "all" only
            (3600, "success"),  # exactly the 1h edge
            (604801, "failure"),  # just past 7d
            (3601, "failure"),  # just past 1h
            (2592000, "success"),  # exactly the 30d edge
            (86401, "failure"),  # just past 24h
            (604800, "success"),  # exactly the 7d edge
        ]
        for i, (seconds_ago, outcome) in enumerate(history):
            await put(seconds_ago, outcome, duration=float(i + 1))
        # a poison record is skipped by the parse, not counted anywhere
        await cron.state_backend.append_record(
            "runs/j", {"finished_at": "not-a-date"}
        )

        payload = await cron.job_trends_payload("j")
        assert payload is not None
        assert payload["source"] == "durable"

        # reference: the original filter-per-window semantics over the same
        # rehydrated, append-ordered infos
        recs = await cron.state_backend.list_records(
            "runs/j", limit=5000, newest_first=True
        )
        recs.reverse()
        infos = [
            info
            for info in (_job_run_info_from_dict(rec) for rec in recs)
            if info is not None
        ]
        expected = {
            label: _run_stats(
                [
                    info
                    for info in infos
                    if (now - info.finished_at).total_seconds() <= seconds
                ]
            )
            for label, seconds in TREND_WINDOWS
        }
        expected["all"] = _run_stats(infos)
        assert payload["windows"] == expected
        # and the inclusive edges landed where the old code put them
        totals = {
            label: payload["windows"][label]["total"]
            for label in ("1h", "24h", "7d", "30d", "all")
        }
        assert totals == {"1h": 2, "24h": 4, "7d": 6, "30d": 8, "all": 9}
    finally:
        await _stop_state(cron)


async def test_trends_cancellation_propagates(tmp_path):
    # cancellation is not a store failure: it must re-raise, never degrade
    # to the in-memory history
    cron = await _stateful_cron(tmp_path, _RETRY_JOB)
    try:

        async def _cancelled(*_a, **_k):
            raise asyncio.CancelledError

        cron.state_backend.list_records = _cancelled  # type: ignore[method-assign]
        with pytest.raises(asyncio.CancelledError):
            await cron.job_trends_payload("j")
    finally:
        cron.state_backend = None


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


_PAUSE_GC_JOBS = """
jobs:
  - name: alive
    command: ls
    schedule: "0 0 * * *"
  - name: dead
    command: ls
    schedule: "0 0 * * *"
  - name: held
    command: ls
    schedule: "0 0 * * *"
"""


async def test_collect_state_garbage_reclaims_dead_pause_streams(
    tmp_path, monkeypatch
):
    # A job paused then resumed keeps a paused/<job> stream that the
    # per-minute refresh re-reads forever just to conclude "not paused". GC
    # reclaims the dead stream (grace-gated) while leaving a live pause alone,
    # so the steady-state refresh only reads streams with an active pause.
    import cronstable.state as state_mod

    cron = await _stateful_cron(tmp_path, _PAUSE_GC_JOBS)
    try:
        backend = cron.state_backend
        old_epoch = state_mod._now() - 7200.0
        # a resumed (dead) pause stream for a configured job, written long
        # enough ago that its newest record is older than the grace window.
        monkeypatch.setattr(state_mod, "_now", lambda: old_epoch)
        await backend.append_record(
            "paused/dead",
            {
                "kind": "resumed",
                "by": "op",
                "channel": "cli",
                "at": (
                    _now_utc() - datetime.timedelta(seconds=7200)
                ).isoformat(),
                "host": "h",
            },
        )
        # a job paused on THIS node whose durable stream still tops out at an
        # OLD resume (the pause write not yet landed): the local-pause guard
        # must keep it even though the durable record reads as dead.
        await backend.append_record(
            "paused/held",
            {
                "kind": "resumed",
                "by": "op",
                "channel": "cli",
                "at": (
                    _now_utc() - datetime.timedelta(seconds=7200)
                ).isoformat(),
                "host": "h",
            },
        )
        monkeypatch.undo()
        cron._paused["held"] = PauseInfo(
            since=_now_utc(),
            until=_now_utc() + datetime.timedelta(hours=1),
            note="",
            by="op",
            channel="cli",
        )
        # a live pause on another configured job -> must be kept.
        await backend.append_record(
            "paused/alive",
            {
                "kind": "paused",
                "since": _now_utc().isoformat(),
                "until": (
                    _now_utc() + datetime.timedelta(hours=1)
                ).isoformat(),
                "note": "",
                "by": "op",
                "channel": "cli",
                "at": _now_utc().isoformat(),
                "host": "h",
            },
        )
        # this node's own manifest, old enough to satisfy the depth guard so
        # GC actually proceeds (see the sibling test above).
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
        cron._state_gc_grace = 3600.0
        await cron._collect_state_garbage()
        # the dead resumed stream is reclaimed; the live pause and the
        # locally-held job (stale durable record notwithstanding) survive.
        assert await backend.list_records("paused/dead") == []
        assert len(await backend.list_records("paused/alive")) == 1
        assert len(await backend.list_records("paused/held")) == 1
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


# --- durable runtime pause/resume (paused/<job> stream) --------------------

_PAUSE_JOB = """
jobs:
  - name: p
    command: ls
    schedule: "* * * * *"
"""

_PAUSE_CATCHUP_JOB = """
jobs:
  - name: p
    command: ls
    schedule: "* * * * *"
    onMissed: run-all
"""


async def test_pause_record_written_and_survives_restart(tmp_path):
    cron = await _stateful_cron(tmp_path, _PAUSE_JOB)
    try:
        await cron.pause_job_by_name(
            "p", duration=3600, note="maint", by="parker", channel="api"
        )
        await _drain_state_writes(cron)
        rec = await _newest(cron, "paused/p")
        assert rec is not None
        assert rec["kind"] == "paused"
        assert rec["note"] == "maint"
        assert rec["by"] == "parker"
        assert rec["channel"] == "api"
        assert rec["host"] == cron._state_host
        until = datetime.datetime.fromisoformat(rec["until"])
        since = datetime.datetime.fromisoformat(rec["since"])
        assert (until - since).total_seconds() == 3600
    finally:
        await _stop_state(cron)

    # a fresh process over the same store: the pause is rehydrated and the
    # fire gate honours it end to end (skip row, no launch).
    cron2 = await _stateful_cron(tmp_path, _PAUSE_JOB)
    try:
        live = cron2._pause_active("p")
        assert live is not None
        assert live.note == "maint"
        launched = []

        async def fake(job, *, with_retries=True):
            launched.append(job.name)
            return True

        cron2.maybe_launch_job = fake  # type: ignore[method-assign]
        await cron2.launch_scheduled_job(cron2.cron_jobs["p"])
        assert launched == []
        assert cron2.last_run["p"].outcome == "skipped"
        assert cron2.last_run["p"].skip_reason == "paused"
    finally:
        await _stop_state(cron2)


async def test_resume_record_clears_pause_on_restart(tmp_path):
    cron = await _stateful_cron(tmp_path, _PAUSE_JOB)
    try:
        await cron.pause_job_by_name("p", duration=3600)
        await cron.resume_job_by_name("p", by="parker")
        await _drain_state_writes(cron)
        # the per-job write chain keeps newest-record-wins honest: the
        # resume lands ON TOP of the pause it revokes, never inverted.
        rec = await _newest(cron, "paused/p")
        assert rec is not None
        assert rec["kind"] == "resumed"
        assert rec["by"] == "parker"
    finally:
        await _stop_state(cron)

    cron2 = await _stateful_cron(tmp_path, _PAUSE_JOB)
    try:
        assert cron2._pause_active("p") is None
    finally:
        await _stop_state(cron2)


async def test_expired_durable_pause_not_rehydrated(tmp_path):
    cron = await _stateful_cron(tmp_path, _PAUSE_JOB)
    try:
        past = _now_utc() - datetime.timedelta(seconds=60)
        await cron.state_backend.append_record(
            "paused/p",
            {
                "kind": "paused",
                "since": (past - datetime.timedelta(seconds=60)).isoformat(),
                "until": past.isoformat(),
                "note": "",
                "by": "parker",
                "channel": "api",
                "at": past.isoformat(),
                "host": "elsewhere",
            },
        )
    finally:
        await _stop_state(cron)

    cron2 = await _stateful_cron(tmp_path, _PAUSE_JOB)
    try:
        # skip-expired at rehydrate: the window ended while nobody ran
        assert cron2._pause_active("p") is None
        assert "p" not in cron2._paused
    finally:
        await _stop_state(cron2)


async def test_pause_window_excuses_only_the_slots_inside_it(tmp_path):
    # the daemon was DOWN across an (expired) pause window: slots inside
    # the window are never owed, slots after its end are owed as normal.
    cron = await _stateful_cron(tmp_path, _PAUSE_CATCHUP_JOB)
    try:
        await cron.state_backend.append_record(
            "runs/p",
            {
                "outcome": "success",
                "exit_code": 0,
                "started_at": None,
                "finished_at": "2026-07-01T10:00:00+00:00",
                "duration": None,
                "fail_reason": None,
            },
        )
        now = datetime.datetime(2026, 7, 1, 10, 10, 30, tzinfo=_UTC)
        job = cron.cron_jobs["p"]
        # without a pause record: every slot since the watermark is owed
        count, _ = await cron._missed_occurrences(job, now)
        assert count == 10
        await cron.state_backend.append_record(
            "paused/p",
            {
                "kind": "paused",
                "since": "2026-07-01T10:00:30+00:00",
                "until": "2026-07-01T10:05:00+00:00",
                "note": "",
                "by": "parker",
                "channel": "api",
                "at": "2026-07-01T10:00:30+00:00",
                "host": "elsewhere",
            },
        )
        # 10:01..10:04 fall inside [since, until) and are excused; 10:05
        # lands exactly on `until`, where _pause_active already reports the
        # job as unpaused, so it is owed like 10:06..10:10.
        count, _ = await cron._missed_occurrences(job, now)
        assert count == 6
    finally:
        await _stop_state(cron)


async def test_pause_window_leaves_the_pre_pause_backlog_owed(tmp_path):
    # the pause began AFTER the daemon went down (a peer paused the job
    # while this node was off): the slots that came due before the operator
    # paused are genuine downtime and stay owed. A floor at `until` erased
    # all of them.
    cron = await _stateful_cron(tmp_path, _PAUSE_CATCHUP_JOB)
    try:
        await cron.state_backend.append_record(
            "runs/p",
            {
                "outcome": "success",
                "exit_code": 0,
                "started_at": None,
                "finished_at": "2026-07-01T10:00:00+00:00",
                "duration": None,
                "fail_reason": None,
            },
        )
        await cron.state_backend.append_record(
            "paused/p",
            {
                "kind": "paused",
                "since": "2026-07-01T10:05:30+00:00",
                "until": "2026-07-01T10:08:00+00:00",
                "note": "",
                "by": "parker",
                "channel": "api",
                "at": "2026-07-01T10:05:30+00:00",
                "host": "elsewhere",
            },
        )
        now = datetime.datetime(2026, 7, 1, 10, 10, 30, tzinfo=_UTC)
        job = cron.cron_jobs["p"]
        # 10:01..10:05 predate the window, 10:06 and 10:07 are inside it,
        # 10:08..10:10 are at/after its end: 8 owed, 2 excused.
        count, _ = await cron._missed_occurrences(job, now)
        assert count == 8
    finally:
        await _stop_state(cron)


async def test_pause_window_does_not_erase_an_open_checkpoint_backlog(
    tmp_path,
):
    # an open catch-up checkpoint deliberately hoists the window back to an
    # older watermark; the pause excusal must not clamp that hoist away.
    cron = await _stateful_cron(tmp_path, _PAUSE_CATCHUP_JOB)
    try:
        await cron.state_backend.append_record(
            "runs/p",
            {
                "outcome": "success",
                "exit_code": 0,
                "started_at": None,
                "finished_at": "2026-07-01T10:07:00+00:00",
                "duration": None,
                "fail_reason": None,
            },
        )
        await cron._checkpoint_catchup(
            "p", "open", "2026-07-01T10:00:00+00:00"
        )
        await cron.state_backend.append_record(
            "paused/p",
            {
                "kind": "paused",
                "since": "2026-07-01T10:05:30+00:00",
                "until": "2026-07-01T10:08:00+00:00",
                "note": "",
                "by": "parker",
                "channel": "api",
                "at": "2026-07-01T10:05:30+00:00",
                "host": "elsewhere",
            },
        )
        now = datetime.datetime(2026, 7, 1, 10, 10, 30, tzinfo=_UTC)
        count, watermark = await cron._missed_occurrences(
            cron.cron_jobs["p"], now
        )
        assert watermark == "2026-07-01T10:00:00+00:00"
        assert count == 8
    finally:
        await _stop_state(cron)


async def test_catch_up_defers_a_paused_job_and_backfills_on_resume(tmp_path):
    # a pause taken after the downtime must not forfeit the backlog: the
    # evaluation stays unresolved while the pause is live, then replays
    # every owed pre-pause slot once it lifts.
    cron = await _stateful_cron(tmp_path, _PAUSE_CATCHUP_JOB)
    try:
        await cron.state_backend.append_record(
            "runs/p",
            {
                "outcome": "success",
                "exit_code": 0,
                "started_at": None,
                "finished_at": "2026-07-01T10:00:00+00:00",
                "duration": None,
                "fail_reason": None,
            },
        )
        backfills = []

        async def _fake_backfill(job, count, offset, now):
            backfills.append((job.name, count))

        cron._run_catch_up = _fake_backfill  # type: ignore[method-assign]
        # the pause is taken now, i.e. long after the owed slots came due
        await cron.pause_job_by_name("p", duration=3600)
        await _drain_state_writes(cron)
        now = datetime.datetime(2026, 7, 1, 10, 10, 30, tzinfo=_UTC)

        await cron._catch_up(now)
        assert cron._caught_up is False  # deferred, not latched
        assert "p" not in cron._catchup_done
        assert backfills == []

        await cron.resume_job_by_name("p")
        await _drain_state_writes(cron)
        cron._catchup_next_retry = 0.0
        await cron._catch_up(now)
        await asyncio.sleep(0)  # let the backfill task run
        assert cron._caught_up is True
        assert backfills == [("p", 10)]
    finally:
        await _stop_state(cron)


async def _seed_real_run(cron, when):
    # a real run carries `ranAt` (JobRunInfo.to_dict); a skip row never does.
    await cron.state_backend.append_record(
        "runs/p",
        {
            "outcome": "success",
            "exit_code": 0,
            "started_at": None,
            "finished_at": when,
            "duration": None,
            "fail_reason": None,
            "ranAt": when,
        },
    )


async def _seed_held_slot(cron, when):
    # the synthetic row launch_scheduled_job writes for a slot held by a live
    # pause: no `ranAt`, but a `finished_at` that advances durable_last_run_at.
    await cron.state_backend.append_record(
        "runs/p",
        {
            "outcome": "skipped",
            "exit_code": None,
            "started_at": None,
            "finished_at": when,
            "duration": None,
            "fail_reason": None,
            "skip_reason": "paused",
        },
    )


async def _seed_pause_window(cron, since, until):
    await cron.state_backend.append_record(
        "paused/p",
        {
            "kind": "paused",
            "since": since,
            "until": until,
            "note": "",
            "by": "parker",
            "channel": "api",
            "at": since,
            "host": "elsewhere",
        },
    )


def _make_pause_live(cron):
    # a live in-memory pause so _pause_active (wall-clock) routes the catch-up
    # evaluation into the deferral branch, independent of the FIXED durable
    # window the store carries for the excusal walk.
    real_now = _now_utc()
    cron._paused["p"] = PauseInfo(
        since=real_now,
        until=real_now + datetime.timedelta(hours=1),
        note="",
        by="parker",
        channel="api",
    )


@pytest.mark.parametrize("n_held", [0, 1, 10])
async def test_catch_up_pins_backlog_against_held_slot_rows(tmp_path, n_held):
    # #37 residual: while a job is paused every held slot writes a synthetic
    # "skipped" ledger row whose finished_at advances durable_last_run_at (the
    # watermark _missed_occurrences reads). Merely deferring the evaluation is
    # not enough: by the time the pause lifts the derived watermark has walked
    # past _catchup_reference and the pre-pause backlog reads as nothing owed,
    # forfeited forever. The deferral must PIN the pre-pause watermark (an open
    # checkpoint at the last real run, which is skip-blind) so the backlog
    # survives however many held rows land. AT (1) and ACROSS (10) the
    # one-held-row boundary the owed count must stay 9.
    cron = await _stateful_cron(tmp_path, _PAUSE_CATCHUP_JOB)
    try:
        await _seed_real_run(cron, "2026-07-01T10:00:00+00:00")
        # since=10:10 == the reference below, so 10:01..10:09 predate the pause
        # (owed) and only 10:10 is inside the window (excused): 9 owed.
        await _seed_pause_window(
            cron, "2026-07-01T10:10:00+00:00", "2026-07-01T10:20:00+00:00"
        )
        _make_pause_live(cron)
        backfills = []

        async def _fake_backfill(job, count, offset, now):
            backfills.append((job.name, count))

        cron._run_catch_up = _fake_backfill  # type: ignore[method-assign]
        ref = datetime.datetime(2026, 7, 1, 10, 10, 0, tzinfo=_UTC)

        # first pass: paused, so defer AND pin the pre-pause watermark.
        await cron._catch_up(ref)
        assert cron._caught_up is False
        assert backfills == []
        assert (
            await cron._pending_catchup_watermark("p")
            == "2026-07-01T10:00:00+00:00"
        )

        # the held slots fire while paused, advancing durable_last_run_at.
        for i in range(n_held):
            await _seed_held_slot(
                cron,
                datetime.datetime(
                    2026, 7, 1, 10, 11 + i, 0, tzinfo=_UTC
                ).isoformat(),
            )
        # the window expires: the durable record stays 'paused', only memory
        # clears (the reader-enforced auto-expiry _pause_active applies).
        del cron._paused["p"]

        cron._catchup_next_retry = 0.0
        await cron._catch_up(ref)
        await asyncio.sleep(0)  # let the backfill task run
        assert cron._caught_up is True
        assert backfills == [("p", 9)]
    finally:
        await _stop_state(cron)


@pytest.mark.parametrize("n_held", [0, 1, 5])
async def test_catch_up_pins_partial_window_backlog_against_held_rows(
    tmp_path, n_held
):
    # #9 residual boundary: the pause window does NOT cover _catchup_reference
    # (the operator paused mid-downtime), so only part of the backlog is
    # excused and the rest stays owed. A single held-slot skip row still
    # advances the derived watermark past the reference and forfeits the owed
    # part unless the pre-pause watermark is pinned. AT (1) and ACROSS (5) the
    # one-held-row boundary the owed count must stay 5.
    cron = await _stateful_cron(tmp_path, _PAUSE_CATCHUP_JOB)
    try:
        await _seed_real_run(cron, "2026-07-01T10:00:00+00:00")
        # since=10:05:30 < reference 10:10:30: 10:01..10:05 predate the pause
        # (owed), 10:06..10:10 fall inside the window (excused): 5 owed.
        await _seed_pause_window(
            cron, "2026-07-01T10:05:30+00:00", "2026-07-01T10:20:00+00:00"
        )
        _make_pause_live(cron)
        backfills = []

        async def _fake_backfill(job, count, offset, now):
            backfills.append((job.name, count))

        cron._run_catch_up = _fake_backfill  # type: ignore[method-assign]
        ref = datetime.datetime(2026, 7, 1, 10, 10, 30, tzinfo=_UTC)

        await cron._catch_up(ref)
        assert cron._caught_up is False
        assert backfills == []
        assert (
            await cron._pending_catchup_watermark("p")
            == "2026-07-01T10:00:00+00:00"
        )

        for i in range(n_held):
            await _seed_held_slot(
                cron,
                datetime.datetime(
                    2026, 7, 1, 10, 11 + i, 0, tzinfo=_UTC
                ).isoformat(),
            )
        del cron._paused["p"]

        cron._catchup_next_retry = 0.0
        await cron._catch_up(ref)
        await asyncio.sleep(0)
        assert cron._caught_up is True
        assert backfills == [("p", 5)]
    finally:
        await _stop_state(cron)


@pytest.mark.parametrize("n_held", [0, 1, 5])
async def test_missed_occurrences_backstops_an_unpinned_pause_window(
    tmp_path, n_held
):
    # #9 residual: a pause whose whole lifetime slipped between two startup
    # catch-up passes is never pinned by _evaluate_catch_up (its pause branch
    # only fires for a LIVE pause it reaches). With no pin, held-slot skip
    # rows advance durable_last_run_at past the pre-pause backlog and
    # _missed_occurrences forfeits it. The read-time backstop must fall back
    # to the skip-blind durable_last_completed_at when a durable pause window
    # exists and it is older. Here NO in-memory pause is ever set (never
    # _make_pause_live), so nothing pins the checkpoint; only the backstop can
    # save the backlog. AT (1) and ACROSS (5) the one-held-row boundary the
    # owed count must stay 11; n_held=0 is the control (no skip row advanced
    # the watermark, so nothing is forfeited either way).
    cron = await _stateful_cron(tmp_path, _PAUSE_CATCHUP_JOB)
    try:
        await _seed_real_run(cron, "2026-07-01T10:00:00+00:00")
        # window [10:05:30, 10:20): 10:01..10:05 predate it (owed),
        # 10:06..10:19 fall inside (excused), 10:20..10:25 follow it (owed).
        await _seed_pause_window(
            cron, "2026-07-01T10:05:30+00:00", "2026-07-01T10:20:00+00:00"
        )
        # the held slots fired while paused; the pause has since expired from
        # memory unpinned (the pin never ran because it was never live here).
        for i in range(n_held):
            await _seed_held_slot(
                cron,
                datetime.datetime(
                    2026, 7, 1, 10, 6 + i, 0, tzinfo=_UTC
                ).isoformat(),
            )
        assert await cron._pending_catchup_watermark("p") is None
        ref = datetime.datetime(2026, 7, 1, 10, 25, 0, tzinfo=_UTC)
        count, watermark = await cron._missed_occurrences(
            cron.cron_jobs["p"], ref
        )
        assert count == 11  # 5 pre-pause + 6 post-window: backlog preserved
        assert watermark == "2026-07-01T10:00:00+00:00"
    finally:
        await _stop_state(cron)


async def test_refresh_picks_up_foreign_pause_and_resume(tmp_path):
    # cross-node propagation: a pause written by ANY host is honoured here
    # (host is audit info only, unlike retry records), and its revocation
    # propagates the same way.
    cron = await _stateful_cron(tmp_path, _PAUSE_JOB)
    try:
        until = _now_utc() + datetime.timedelta(seconds=3600)
        await cron.state_backend.append_record(
            "paused/p",
            {
                "kind": "paused",
                "since": _now_utc().isoformat(),
                "until": until.isoformat(),
                "note": "peer",
                "by": "other-node",
                "channel": "api",
                "at": _now_utc().isoformat(),
                "host": "other-host",
            },
        )
        await cron._refresh_pauses_from_store()
        live = cron._pause_active("p")
        assert live is not None
        assert live.by == "other-node"
        await cron.state_backend.append_record(
            "paused/p",
            {
                "kind": "resumed",
                "by": "other-node",
                "channel": "api",
                "at": _now_utc().isoformat(),
                "host": "other-host",
            },
        )
        await cron._refresh_pauses_from_store()
        assert cron._pause_active("p") is None
    finally:
        await _stop_state(cron)


async def test_refresh_keeps_last_known_state_on_store_error(
    tmp_path, caplog
):
    cron = await _stateful_cron(tmp_path, _PAUSE_JOB)
    try:
        await cron.pause_job_by_name("p", duration=3600)
        await _drain_state_writes(cron)

        async def _boom(*_a, **_k):
            raise OSError("store down")

        cron.state_backend.list_stream_names = _boom  # type: ignore[method-assign]
        await cron._refresh_pauses_from_store()
        # under BOTH degrade and fail-closed: keep last known state + warn
        assert cron._pause_active("p") is not None
        assert any(
            "cannot refresh pause state" in r.getMessage()
            for r in caplog.records
        )
    finally:
        cron.state_backend = None


def _peer_pause_record(until, *, by="other-node"):
    return {
        "kind": "paused",
        "since": _now_utc().isoformat(),
        "until": until.isoformat(),
        "note": "peer",
        "by": by,
        "channel": "api",
        "at": _now_utc().isoformat(),
        "host": "other-host",
    }


def _peer_resume_record(by="other-node"):
    return {
        "kind": "resumed",
        "by": by,
        "channel": "api",
        "at": _now_utc().isoformat(),
        "host": "other-host",
    }


def _gate_pause_read(cron, stream_name):
    """Hold the refresh inside its read of one paused/ stream.

    Returns (entered, release): await `entered` once the refresh is parked
    mid-read, mutate pause state, then set `release`. The snapshot is taken
    BEFORE the wait, so the refresh resumes holding a pre-mutation read,
    which is the window every TOCTOU test below needs.
    """
    real = cron.state_backend.list_records
    entered = asyncio.Event()
    release = asyncio.Event()

    async def gated(stream, **kw):
        recs = await real(stream, **kw)
        if stream == stream_name:
            entered.set()
            await release.wait()
        return recs

    cron.state_backend.list_records = gated  # type: ignore[method-assign]
    return entered, release


async def test_refresh_does_not_clobber_a_pause_taken_during_its_read(
    tmp_path,
):
    # the store's newest record says "not paused"; an operator pause lands
    # AND its durable write completes while the refresh is parked inside
    # that read. The acknowledged pause must survive.
    cron = await _stateful_cron(tmp_path, _PAUSE_JOB)
    try:
        await cron.state_backend.append_record(
            "paused/p", _peer_resume_record()
        )
        entered, release = _gate_pause_read(cron, "paused/p")
        refresh = asyncio.create_task(cron._refresh_pauses_from_store())
        await entered.wait()
        await cron.pause_job_by_name("p", duration=3600, by="parker")
        # drain it: the write tail entry is deleted by its own done-callback,
        # so a liveness check would find no trace of the pause by now.
        await _drain_state_writes(cron)
        assert "p" not in cron._pause_write_tail
        release.set()
        await refresh
        live = cron._pause_active("p")
        assert live is not None
        assert live.by == "parker"
    finally:
        await _stop_state(cron)


async def test_refresh_does_not_resurrect_a_pause_resumed_during_its_read(
    tmp_path,
):
    # the mirror case: the store still holds the live `paused` record the
    # operator has just revoked, and the refresh is parked inside that read.
    cron = await _stateful_cron(tmp_path, _PAUSE_JOB)
    try:
        await cron.pause_job_by_name("p", duration=3600)
        await _drain_state_writes(cron)
        entered, release = _gate_pause_read(cron, "paused/p")
        refresh = asyncio.create_task(cron._refresh_pauses_from_store())
        await entered.wait()
        await cron.resume_job_by_name("p", by="parker")
        await _drain_state_writes(cron)
        assert "p" not in cron._pause_write_tail
        release.set()
        await refresh
        assert cron._pause_active("p") is None
    finally:
        await _stop_state(cron)


_PAUSE_TWO_JOBS = """
jobs:
  - name: a-job
    command: ls
    schedule: "* * * * *"
  - name: z-job
    command: ls
    schedule: "* * * * *"
"""


async def test_unreadable_pause_stream_does_not_starve_later_jobs(
    tmp_path, caplog
):
    # streams are swept in the store's sorted order, so a stream that fails
    # every pass would starve every job after it forever (this sweep is also
    # the boot rehydrate). Only the failing job keeps its last known state.
    cron = await _stateful_cron(tmp_path, _PAUSE_TWO_JOBS)
    try:
        until = _now_utc() + datetime.timedelta(seconds=3600)
        for name in ("a-job", "z-job"):
            await cron.state_backend.append_record(
                "paused/" + name, _peer_pause_record(until)
            )
        real = cron.state_backend.list_records

        async def flaky(stream, **kw):
            if stream == "paused/a-job":
                raise OSError("stream directory unreadable")
            return await real(stream, **kw)

        cron.state_backend.list_records = flaky  # type: ignore[method-assign]
        await cron._refresh_pauses_from_store()
        assert cron._pause_active("z-job") is not None
        assert cron._pause_active("a-job") is None
        assert any(
            "cannot refresh pause state for a-job" in r.getMessage()
            for r in caplog.records
        )
    finally:
        cron.state_backend = None


async def test_backend_swap_cancels_the_in_flight_pause_refresh(tmp_path):
    # the refresh holds a local binding to the OLD backend across its awaits
    # (and a filesystem store keeps serving reads after stop()), so a pass
    # left running through a swap re-installs the dead store's pauses.
    store_a = tmp_path / "a"
    store_b = tmp_path / "b"
    cron = await _stateful_cron(store_a, _PAUSE_JOB)
    peer = await _stateful_cron(store_b, _PAUSE_JOB)
    try:
        await peer.state_backend.append_record(
            "paused/p", _peer_resume_record()
        )
    finally:
        await _stop_state(peer)
    cfg_b = _state_cfg("state:\n  path: {}\n".format(store_b))
    try:
        await cron.pause_job_by_name("p", duration=3600)
        await _drain_state_writes(cron)
        entered, release = _gate_pause_read(cron, "paused/p")
        cron._pause_periodic()
        stale = cron._pause_refresh_task
        assert stale is not None
        await entered.wait()

        await cron.start_stop_state(cfg_b)
        cancelled = cron._pause_refresh_task is None
        # release before asserting: a pass left parked here would sit out
        # the whole store timeout on the way to teardown.
        release.set()
        await asyncio.gather(stale, return_exceptions=True)
        # the new store says resumed, and the abandoned pass must not undo it
        assert cron._pause_active("p") is None
        assert cancelled
    finally:
        await _stop_state(cron)


async def test_pause_taken_while_the_store_is_down_survives_its_return(
    tmp_path, caplog
):
    # a configured-but-down store is transient. Dropping the record would
    # leave `resumed` newest in the stream, and the refresh that follows the
    # store's return would silently revoke the operator's pause.
    cron = await _stateful_cron(tmp_path, _PAUSE_JOB)
    cfg = _state_cfg("state:\n  path: {}\n".format(tmp_path))
    try:
        await cron.pause_job_by_name("p", duration=3600)
        await cron.resume_job_by_name("p")
        await _drain_state_writes(cron)
        assert (await _newest(cron, "paused/p"))["kind"] == "resumed"

        cron.state_backend = None
        await cron.pause_job_by_name("p", duration=3600, by="parker")
        assert cron._pause_active("p") is not None
        assert any(
            "holds in memory only" in r.getMessage()
            and r.levelname == "WARNING"
            for r in caplog.records
        )

        await cron.start_stop_state(cfg)
        await _drain_state_writes(cron)
        # the housekeeping pass that follows the store's return must find
        # the operator's pause on top of the stream, not the record it
        # superseded.
        await cron._refresh_pauses_from_store()
        assert cron._pause_active("p") is not None
        rec = await _newest(cron, "paused/p")
        assert rec["kind"] == "paused"
        assert rec["by"] == "parker"
    finally:
        await _stop_state(cron)


async def test_resume_taken_while_the_store_is_down_survives_its_return(
    tmp_path,
):
    # the mirror case: the live `paused` record must not re-pause a job the
    # operator resumed during the outage.
    cron = await _stateful_cron(tmp_path, _PAUSE_JOB)
    cfg = _state_cfg("state:\n  path: {}\n".format(tmp_path))
    try:
        await cron.pause_job_by_name("p", duration=3600)
        await _drain_state_writes(cron)

        cron.state_backend = None
        await cron.resume_job_by_name("p", by="parker")
        assert cron._pause_active("p") is None

        await cron.start_stop_state(cfg)
        await _drain_state_writes(cron)
        await cron._refresh_pauses_from_store()
        assert cron._pause_active("p") is None
        rec = await _newest(cron, "paused/p")
        assert rec["kind"] == "resumed"
        assert rec["by"] == "parker"
    finally:
        await _stop_state(cron)


async def test_stateless_pause_holds_in_memory_without_a_warning(caplog):
    # no `state` section: memory-only is the contract, so nothing is
    # buffered for replay and nothing is logged about a store that was
    # never configured.
    cron = Cron(None, config_yaml=_PAUSE_JOB)
    await cron.pause_job_by_name("p", duration=3600)
    assert cron._pause_active("p") is not None
    assert cron._pause_pending_writes == {}
    assert not [r for r in caplog.records if r.levelname == "WARNING"]


async def test_backend_swap_cancels_the_refresh_before_the_teardown_awaits(
    tmp_path,
):
    # cancelling the refresh at the END of the teardown block is too late:
    # backend.stop() and _stop_job_api() both yield (with one keep-alive
    # connection open the latter spans several loop turns), and a pass whose
    # store read resolves in that window runs its whole mutation loop against
    # the store being abandoned. Memory holds no pause here and only store A
    # does, so anything installed came from the dead store; store B has no
    # paused/p stream at all, so its rehydrate never visits the job and the
    # stale window would stick until its own `until` elapsed.
    store_a = tmp_path / "a"
    store_b = tmp_path / "b"
    cron = await _stateful_cron(store_a, _PAUSE_JOB)
    cfg_b = _state_cfg("state:\n  path: {}\n".format(store_b))
    try:
        until = _now_utc() + datetime.timedelta(seconds=3600)
        await cron.state_backend.append_record(
            "paused/p", _peer_pause_record(until)
        )
        entered, release = _gate_pause_read(cron, "paused/p")
        cron._pause_periodic()
        stale = cron._pause_refresh_task
        assert stale is not None
        await entered.wait()

        # stand in for the teardown's own awaits deterministically: release
        # the parked read as the first of them starts, and burn the loop
        # turns the real _stop_job_api would.
        seen = {}
        real_stop = cron.state_backend.stop

        async def yielding_stop():
            seen["task"] = cron._pause_refresh_task
            release.set()
            for _ in range(4):
                await asyncio.sleep(0)
            await real_stop()

        cron.state_backend.stop = yielding_stop  # type: ignore[method-assign]
        await cron.start_stop_state(cfg_b)
        await asyncio.gather(stale, return_exceptions=True)
        assert "p" not in cron._paused
        assert cron._pause_active("p") is None
        # and the ordering that guarantees it
        assert seen["task"] is None
    finally:
        await _stop_state(cron)


async def test_pause_whose_durable_append_fails_is_not_reverted(tmp_path):
    # the store is UP and the append fails anyway (ENOSPC, EACCES, a
    # transient NFS error). The record it meant to supersede is still newest
    # in the stream, so a dropped write lets the next refresh silently
    # revoke the operator's pause: the same class as the store-down case.
    cron = await _stateful_cron(tmp_path, _PAUSE_JOB)
    try:
        await cron.pause_job_by_name("p", duration=3600)
        await cron.resume_job_by_name("p")
        await _drain_state_writes(cron)
        assert (await _newest(cron, "paused/p"))["kind"] == "resumed"

        real_append = cron.state_backend.append_record

        async def failing(stream, record, **kw):
            if stream == "paused/p":
                raise OSError("ENOSPC")
            return await real_append(stream, record, **kw)

        cron.state_backend.append_record = failing  # type: ignore[method-assign]
        await cron.pause_job_by_name("p", duration=3600, by="parker")
        await _drain_state_writes(cron)
        assert cron._pause_active("p") is not None
        # buffered for retry, not dropped
        assert cron._pause_pending_writes["p"]["kind"] == "paused"

        # a refresh before the retry lands must keep memory, not revert it
        # to the `resumed` record still on top of the stream.
        await cron._refresh_pauses_from_store()
        live = cron._pause_active("p")
        assert live is not None
        assert live.by == "parker"

        # and the housekeeping pass retries the write once the store heals
        cron.state_backend.append_record = real_append  # type: ignore[method-assign]
        cron._pause_periodic()
        await _drain_state_writes(cron)
        rec = await _newest(cron, "paused/p")
        assert rec["kind"] == "paused"
        assert rec["by"] == "parker"
        assert cron._pause_pending_writes == {}
    finally:
        await _stop_state(cron)


async def test_buffered_pause_does_not_outlive_a_resume_taken_stateless(
    tmp_path,
):
    # a pause buffered during an outage, then the `state` section removed and
    # the job resumed memory-only: the superseded buffer must not be replayed
    # as fresh intent when the section comes back and re-pause the job.
    cron = await _stateful_cron(tmp_path, _PAUSE_JOB)
    cfg = _state_cfg("state:\n  path: {}\n".format(tmp_path))
    try:
        await _drain_state_writes(cron)
        cron.state_backend = None  # the store goes down
        await cron.pause_job_by_name("p", duration=3600, by="parker")
        assert cron._pause_pending_writes["p"]["kind"] == "paused"

        await cron.start_stop_state(None)  # the `state` section is removed
        assert cron.state_backend is None
        await cron.resume_job_by_name("p", by="parker")
        assert cron._pause_active("p") is None
        assert "p" not in cron._pause_pending_writes

        await cron.start_stop_state(cfg)  # the section comes back
        await _drain_state_writes(cron)
        await cron._refresh_pauses_from_store()
        assert cron._pause_active("p") is None
    finally:
        await _stop_state(cron)


_PAUSE_JOB_RENAMED = """
jobs:
  - name: q
    command: ls
    schedule: "* * * * *"
"""


async def test_refresh_drops_a_job_a_reload_removed_during_its_read(tmp_path):
    # the membership test runs BEFORE the store read, and the generation guard
    # does not cover this: _apply_reload prunes _pause_gen with _paused, so the
    # sampled and current generations are both 0 and it passes. Installing the
    # read would leave a permanent stale _paused entry and resurrect the metric
    # series prune() just dropped.
    cfg_file = tmp_path / "c.yaml"
    cfg_file.write_text(_PAUSE_JOB)
    cron = Cron(str(cfg_file))
    cfg = _state_cfg("state:\n  path: {}\n".format(tmp_path / "store"))
    await cron.start_stop_state(cfg)
    assert cron.state_backend is not None
    try:
        until = _now_utc() + datetime.timedelta(seconds=3600)
        await cron.state_backend.append_record(
            "paused/p", _peer_pause_record(until)
        )
        entered, release = _gate_pause_read(cron, "paused/p")
        refresh = asyncio.create_task(cron._refresh_pauses_from_store())
        await entered.wait()

        cfg_file.write_text(_PAUSE_JOB_RENAMED)
        cron.update_config()
        assert "p" not in cron.cron_jobs
        assert "p" not in cron.metrics._jobs

        release.set()
        await refresh
        assert "p" not in cron._paused
        assert cron._pause_active("p") is None
        assert "p" not in cron.metrics._jobs
    finally:
        await _stop_state(cron)
