"""Regression tests for the hardened durable-state plumbing in cron.py.

Each test pins a bug confirmed by the adversarial review of the
scheduler-side state integration (:mod:`cronstable.cron`); if one of those
fixes regresses, the matching test here must fail.  Covered:

* store errors and hung reads must degrade the stateful features, never
  crash or stall the scheduling paths that call them;
* foreign/naive ledger records must not poison schedule arithmetic;
* the depends-on-past gate's freshness rules (memory vs ledger);
* the catch-up latch (retry vs forfeit) and checkpointed resume;
* backfill serialization under ``concurrencyPolicy: Forbid`` and the
  live retry-ladder capture;
* output-archival edge cases and rehydration races.
"""

import asyncio
import datetime
import os

from cronstable import cron as cron_mod
from cronstable.cron import Cron, JobRunInfo, _job_run_info_from_dict
from cronstable.job import JobOutputStream, JobRetryState
from cronstable.redact import REDACTED
from cronstable.state import make_state_backend
from tests.test_state import (
    _NOW,
    _UTC,
    _catchup_yaml,
    _count_launcher,
    _cron_with_watermark,
    _state_cfg,
)

_ONE_JOB = (
    "jobs:\n  - name: j\n    command: 'true'\n    schedule: '* * * * *'\n"
)

_DEP_JOB = (
    "jobs:\n"
    "  - name: j\n"
    "    command: 'true'\n"
    "    schedule: '* * * * *'\n"
    "    onlyIfLastSucceeded: true\n"
)

_FORBID_JOB = (
    "jobs:\n"
    "  - name: j\n"
    "    command: 'true'\n"
    "    schedule: '* * * * *'\n"
    "    concurrencyPolicy: Forbid\n"
    "    onMissed: run-all\n"
)


def _state_yaml(path):
    return "state:\n  path: " + str(path)


async def _dep_cron(tmp_path):
    cron = Cron(None, config_yaml=_DEP_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    return cron


def _mem_run(outcome, minute):
    """A finished-run entry as _record_run would put it in memory."""
    dt = datetime.datetime(2026, 7, 1, 10, minute, 0, tzinfo=_UTC)
    return JobRunInfo(
        outcome=outcome,
        exit_code=0 if outcome == "success" else 1,
        started_at=dt,
        finished_at=dt,
        fail_reason=None,
        output=JobOutputStream(),
    )


async def _put_ledger(cron, outcome, iso, name="j"):
    await cron.state_backend.append_record(
        cron._run_stream(name),
        {
            "outcome": outcome,
            "exit_code": 0,
            "started_at": None,
            "finished_at": iso,
            "duration": None,
            "fail_reason": None,
        },
    )


async def _raise_oserror(*args, **kwargs):
    raise OSError("state store went away")


async def _seed_gc_anchor(cron, covered=True):
    """Manifests letting a GC pass with grace 3600 prove absence.

    One manifest older than the grace (history-depth guard) plus one recent
    one; ``covered`` controls whether the recent manifest advertises its
    scopes/dags (all-new-fleet) or predates them (mid-rolling-upgrade).
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    backend = cron.state_backend
    await backend.append_record(
        "manifests/old-host",
        {
            "jobSetId": "v1:old",
            "host": "old-host",
            "jobs": [],
            "at": (now - datetime.timedelta(seconds=7200)).isoformat(),
        },
    )
    recent = {
        "jobSetId": "v1:other",
        "host": "other-host",
        "jobs": [],
        "at": now.isoformat(),
    }
    if covered:
        recent["scopes"] = []
        recent["dags"] = []
    await backend.append_record("manifests/other-host", recent)


# --- scheduler-crash containment on store errors --------------------------


async def test_depends_on_past_fails_open_on_store_error(tmp_path):
    # The CRITICAL from the review: an OSError out of the ledger read
    # used to escape _depends_on_past_ok, and the launch path runs
    # outside run()'s try/except -- a flaky mount took the scheduler
    # down.  It must degrade to the in-memory view (empty here, so
    # allow) instead of raising.
    cron = await _dep_cron(tmp_path)
    cron.state_backend.list_records = _raise_oserror  # type: ignore[method-assign]
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is True


async def test_catch_up_defers_on_store_error(tmp_path):
    # A store error during the catch-up pass used to either crash the
    # pass or latch _caught_up, silently forfeiting the owed backfill.
    # It must defer: no exception, no latch (so a later pass retries),
    # nothing scheduled, and the job left unresolved.
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    cron.state_backend.list_records = _raise_oserror  # type: ignore[method-assign]
    await cron._catch_up(_NOW)  # must not raise
    assert cron._caught_up is False
    assert cron._catchup_tasks == set()
    assert "j" not in cron._catchup_done


async def test_catch_up_survives_hung_store_read(tmp_path, monkeypatch):
    # A hung mount (dead NFS server) is worse than an error: without a
    # bound on the read, _catch_up would block the scheduler loop
    # indefinitely.  The watermark read is capped by STATE_OP_TIMEOUT
    # and the timeout defers the evaluation like any store error.
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )

    async def hang(stream, field):
        await asyncio.sleep(999)

    cron.state_backend.derive_max = hang  # type: ignore[method-assign]
    monkeypatch.setattr(cron_mod, "STATE_OP_TIMEOUT", 0.2)
    # generous outer bound: on regression (no per-read timeout) this
    # fails the test instead of hanging the suite for the full sleep.
    await asyncio.wait_for(cron._catch_up(_NOW), timeout=20)
    assert cron._caught_up is False
    assert cron._catchup_tasks == set()


# --- naive-watermark poison record -----------------------------------------


async def test_missed_occurrences_pins_naive_watermark(tmp_path):
    # A foreign/hand-written record with a NAIVE finished_at used to
    # raise TypeError out of the schedule arithmetic on every boot -- a
    # crash loop until the record was deleted by hand.  The parser pins
    # it to UTC, so the count comes out as for the aware equivalent.
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00", onmissed="run-all"
    )
    count, _ = await cron._missed_occurrences(cron.cron_jobs["j"], _NOW)
    assert count == 10  # slots 10:01..10:10, as with an aware watermark


# --- depends-on-past gate ---------------------------------------------------


async def test_depends_on_past_blocks_while_still_running(tmp_path):
    # An unfinished previous instance has not "succeeded", and letting
    # the answer depend on whether it happens to finish before the gate
    # is read would make the gate a race: a running instance must close
    # it outright, even over a ledger that says success.
    cron = await _dep_cron(tmp_path)
    await _put_ledger(cron, "success", "2026-07-01T10:00:00+00:00")
    cron.running_jobs["j"].append(object())  # a live RunningJob stand-in
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is False


async def test_depends_on_past_memory_beats_stale_ledger(tmp_path):
    # The durable write behind _record_run is fire-and-forget, so the
    # ledger can be a beat stale: a failure already recorded in memory
    # but not yet flushed must still close the gate, or the job re-runs
    # right behind its own failure.  Appended straight to run_history
    # (the in-memory effect of _record_run) so the ledger STAYS stale.
    cron = await _dep_cron(tmp_path)
    await _put_ledger(cron, "success", "2026-07-01T10:00:00+00:00")
    cron.run_history["j"].append(_mem_run("failure", 5))
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is False


async def test_depends_on_past_newer_ledger_beats_memory(tmp_path):
    # The other direction: on a shared mount another node's NEWER
    # success must re-open the gate over this node's older in-memory
    # failure, or the job stays blocked on every node but the one that
    # saw the success.
    cron = await _dep_cron(tmp_path)
    cron.run_history["j"].append(_mem_run("failure", 0))
    await _put_ledger(cron, "success", "2026-07-01T10:05:00+00:00")
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is True


async def test_depends_on_past_skips_non_run_outcomes(tmp_path):
    # cancelled entries are not verdicts on the job: both sources must
    # skip them when finding the last real run, or a newest "cancelled"
    # record would mask the decisive success/failure beneath it.
    cron = await _dep_cron(tmp_path)
    cron.run_history["j"].append(_mem_run("failure", 0))
    cron.run_history["j"].append(_mem_run("cancelled", 10))
    await _put_ledger(cron, "success", "2026-07-01T10:05:00+00:00")
    await _put_ledger(cron, "cancelled", "2026-07-01T10:12:00+00:00")
    # last REAL run is the ledger's 10:05 success (memory's real run is
    # the older 10:00 failure); the two cancelled entries are noise.
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is True


async def test_depends_on_past_survives_a_pause_flooding_the_ring():
    # A pause writes one synthetic "skipped" row per held slot, and
    # run_history is a bounded ring: a pause longer than the ring evicts the
    # failure that closed the gate. Without the eviction-proof memo the gate
    # would find no real outcome, fall through to "nothing to depend on", and
    # run the job against exactly the unrepaired state onlyIfLastSucceeded
    # exists to protect. No backend here, so the ring is the only source.
    cron = Cron(None, config_yaml=_DEP_JOB)
    cron._record_run("j", _mem_run("failure", 0))
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is False

    paused_at = datetime.datetime(2026, 7, 1, 11, 0, 0, tzinfo=_UTC)
    for _ in range(cron_mod.RUN_HISTORY_LIMIT + 5):
        cron._record_run(
            "j",
            JobRunInfo(
                outcome="skipped",
                exit_code=None,
                started_at=None,
                finished_at=paused_at,
                fail_reason=None,
                output=JobOutputStream(),
                skip_reason="paused",
            ),
        )
    assert not [
        info
        for info in cron.run_history["j"]
        if info.outcome in ("success", "failure")
    ]
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is False

    # and a genuine success after the pause still reopens the gate
    cron._record_run("j", _mem_run("success", 30))
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is True


async def test_depends_on_past_widens_past_non_run_probe_page(tmp_path):
    # More than a probe page of cancelled records sits at the head of the
    # ledger, above the decisive failure. The gate probes a small page first;
    # a probe-only read would see only cancels and wrongly ALLOW. It must widen
    # to the full window, find the buried failure, and block.
    cron = await _dep_cron(tmp_path)
    await _put_ledger(cron, "failure", "2026-07-01T10:00:00+00:00")
    for i in range(cron_mod.DEPENDS_GATE_PROBE + 3):
        await _put_ledger(
            cron, "cancelled", "2026-07-01T10:{:02d}:00+00:00".format(10 + i)
        )
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is False


async def test_depends_on_past_probe_page_suffices_without_widening(tmp_path):
    # Common case: the newest ledger record is a real outcome, so the gate
    # reads a single probe page and never widens to the full 50-record window.
    cron = await _dep_cron(tmp_path)
    await _put_ledger(cron, "success", "2026-07-01T10:00:00+00:00")
    await _put_ledger(cron, "failure", "2026-07-01T10:05:00+00:00")
    calls = []
    real = cron.state_backend.list_records

    async def counting(stream, **kw):
        calls.append(kw.get("limit"))
        return await real(stream, **kw)

    cron.state_backend.list_records = counting  # type: ignore[method-assign]
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is False
    assert calls == [cron_mod.DEPENDS_GATE_PROBE]  # one read, no widening


# --- catch-up latch fixes ---------------------------------------------------


async def test_catch_up_retries_when_backend_not_started(tmp_path, caplog):
    # `state` IS configured but the backend failed to start (bad mount
    # at boot; start_stop_state retries it every housekeeping pass).
    # Latching here forfeited the backfill forever, and warning "needs
    # a state backend" was wrong -- one is configured.
    cron = Cron(None, config_yaml=_catchup_yaml(onmissed="run-all"))
    cron._state_configured = True
    assert cron.state_backend is None
    await cron._catch_up(_NOW)
    assert cron._caught_up is False  # stays pending, retried later
    assert cron._catchup_next_retry > 0.0  # a recheck was scheduled
    assert not any("needs a" in r.getMessage() for r in caplog.records)


async def test_catch_up_warns_and_latches_without_state_config(caplog):
    # No `state` section at all: there is no watermark and never will
    # be, so catch-up warns and latches.  This is the only correct
    # latch-on-unresolved case; retrying would just warn forever.
    cron = Cron(None, config_yaml=_catchup_yaml(onmissed="run-all"))
    await cron._catch_up(_NOW)
    assert cron._caught_up is True
    assert any(
        "needs a" in r.getMessage() and "state" in r.getMessage()
        for r in caplog.records
    )


async def test_catch_up_retries_transient_cluster_denial(tmp_path):
    # A fail-closed cluster denial with NO positive owner elsewhere
    # (still electing at boot, lost quorum) is transient: latching, or
    # marking the job done, would mean nobody ever backfills it.  The
    # job must stay unresolved and be re-evaluated.
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    cron._cluster_allows = lambda job: False  # type: ignore[method-assign]
    cron._cluster_owner_moved = lambda job: False  # type: ignore[method-assign]
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    await cron._catch_up(_NOW)
    assert calls == []
    assert cron._caught_up is False
    assert "j" not in cron._catchup_done
    # ownership resolves to this node: the retry pass must schedule the
    # backfill (force the recheck gate open; the 30s interval itself is
    # not under test).
    cron._cluster_allows = lambda job: True  # type: ignore[method-assign]
    cron._catchup_next_retry = 0.0
    await cron._catch_up(_NOW)
    await asyncio.gather(*list(cron._catchup_tasks))
    assert calls == ["j"]
    assert cron._caught_up is True


async def test_catch_up_resolves_when_owner_is_elsewhere(tmp_path):
    # A POSITIVE observation that another node owns the job is final:
    # that owner reads the same ledger and does the backfill itself, so
    # this node resolves the job without launching -- and with every
    # job resolved the whole evaluation may latch.
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    cron._cluster_allows = lambda job: False  # type: ignore[method-assign]
    cron._cluster_owner_moved = lambda job: True  # type: ignore[method-assign]
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    await cron._catch_up(_NOW)
    assert calls == []
    assert cron._caught_up is True


# --- catch-up checkpoint (intent) resume ------------------------------------


async def test_open_checkpoint_anchors_watermark_until_closed(tmp_path):
    # A backfill records an "open" intent before launching.  Ordinary
    # runs finishing afterwards advance the run ledger's derived
    # watermark past the still-missing slots, so without the checkpoint
    # a restart mid-backfill would silently forfeit the owed runs.
    t0 = "2026-07-01T10:00:00+00:00"
    cron = await _cron_with_watermark(tmp_path, t0, onmissed="run-all")
    await cron.state_backend.append_record(
        cron._catchup_stream("j"),
        {"kind": "open", "watermark": t0, "at": _NOW.isoformat()},
    )
    # an ordinary run lands, advancing the ledger past the missed slots
    await _put_ledger(cron, "success", "2026-07-01T10:10:00+00:00")
    job = cron.cron_jobs["j"]
    count, watermark = await cron._missed_occurrences(job, _NOW)
    assert watermark == t0  # anchored at the open intent, not the ledger
    assert count == 10
    # once the cycle closes, the (newer) run-ledger watermark rules
    # again and nothing is owed.
    await cron.state_backend.append_record(
        cron._catchup_stream("j"),
        {"kind": "close", "watermark": t0, "at": _NOW.isoformat()},
    )
    count, watermark = await cron._missed_occurrences(job, _NOW)
    assert count == 0
    assert watermark == "2026-07-01T10:10:00+00:00"


# --- backfill serialization + Forbid ----------------------------------------


async def test_run_catch_up_serializes_forbid_backfill(tmp_path):
    # run-all under concurrencyPolicy: Forbid used to fire its launches
    # back to back: the first instance was still running, so Forbid
    # swallowed the other N-1 and the "replayed" runs never happened.
    # The backfill must drain the previous instance before each launch.
    cron = Cron(None, config_yaml=_FORBID_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    await _put_ledger(cron, "success", "2026-07-01T10:00:00+00:00")
    now = datetime.datetime(2026, 7, 1, 10, 3, 30, tzinfo=_UTC)  # 3 owed
    launched = []
    swallowed = []

    async def fake_launch(job, *, with_retries=True):
        # mirror the real Forbid gate: a still-running instance swallows
        # the launch, which is exactly the regression this guards.
        if cron.running_jobs.get(job.name):
            swallowed.append(job.name)
            return False
        launched.append(job.name)
        marker = object()
        cron.running_jobs[job.name].append(marker)
        # the "run" lasts a few event-loop ticks, then finishes; purely
        # event-based, no duration is ever asserted.
        asyncio.get_running_loop().call_later(
            0.05, lambda: cron.running_jobs[job.name].remove(marker)
        )
        return True

    cron.maybe_launch_job = fake_launch  # type: ignore[method-assign]
    await cron._run_catch_up(cron.cron_jobs["j"], 3, 0.0, now)
    assert launched == ["j", "j", "j"]
    assert swallowed == []


# --- backfill must not capture the live retry ladder ------------------------


async def test_backfill_does_not_capture_live_retry_ladder(monkeypatch):
    # A backfill launching while a scheduled fire's retry ladder is
    # armed used to hand that live JobRetryState to its RunningJob: the
    # backfill's failures then burned the scheduled run's retry budget
    # toward a premature onPermanentFailure.  with_retries=False must
    # launch bare; the default path still carries the armed state.
    cron = Cron(None, config_yaml=_ONE_JOB)
    job = cron.cron_jobs["j"]
    armed = JobRetryState(1.0, 2.0, 10.0)
    cron.retry_state["j"] = armed
    captured = []

    class FakeRunningJob:
        def __init__(self, config, retry_state, **kwargs):
            captured.append(retry_state)
            self.config = config

        async def start(self):
            return None

    monkeypatch.setattr(cron_mod, "RunningJob", FakeRunningJob)
    assert await cron.maybe_launch_job(job, with_retries=False) is True
    assert captured == [None]
    cron.running_jobs.clear()  # a fresh, idle launch for the default
    assert await cron.maybe_launch_job(job) is True
    assert captured[1] is armed


# --- archival ---------------------------------------------------------------


def _archive_yaml(save_limit=None, redact=True):
    lines = [
        "jobs:",
        "  - name: j",
        "    command: 'true'",
        "    schedule: '* * * * *'",
        "    archiveOutput: true",
        "    redactArchivedSecrets: " + ("true" if redact else "false"),
    ]
    if save_limit is not None:
        lines.append("    saveLimit: " + str(save_limit))
    return "\n".join(lines) + "\n"


async def _archive_cron(tmp_path, **kw):
    cron = Cron(None, config_yaml=_archive_yaml(**kw))
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    return cron


def _output_run(pairs, limit=None):
    out = JobOutputStream() if limit is None else JobOutputStream(limit)
    for stream_name, line in pairs:
        out.publish(stream_name, line)
    dt = datetime.datetime(2026, 7, 1, 10, 0, 0, tzinfo=_UTC)
    return JobRunInfo(
        outcome="success",
        exit_code=0,
        started_at=dt,
        finished_at=dt,
        fail_reason=None,
        output=out,
    )


async def test_archive_save_limit_zero_writes_nothing(tmp_path):
    # saveLimit: 0 is the operator's explicit "retain no output"; the
    # archive must honour it rather than persist the live-tail ring the
    # web UI keeps anyway.
    cron = await _archive_cron(tmp_path, save_limit=0)
    info = _output_run([("stdout", "must never be stored")])
    await cron._archive_output(cron.cron_jobs["j"], info)
    logs = await cron.state_backend.list_records(cron._log_stream("j"))
    assert logs == []


async def test_archive_accounts_dropped_lines(tmp_path):
    # lines evicted from the ring before archiving must be accounted
    # for in dropped_lines, not silently lost -- otherwise the archived
    # tail presents itself as the whole output.
    cron = await _archive_cron(tmp_path)
    pairs = [("stdout", "line-%d" % i) for i in range(1, 9)]
    info = _output_run(pairs, limit=5)
    await cron._archive_output(cron.cron_jobs["j"], info)
    (rec,) = await cron.state_backend.list_records(cron._log_stream("j"))
    assert rec["dropped_lines"] == 3
    stored = [ln["line"] for ln in rec["lines"]]
    assert stored == ["line-4", "line-5", "line-6", "line-7", "line-8"]


async def test_archive_redacts_multiline_pem_body(tmp_path):
    # the base64 body lines ARE the key material; per-line patterns
    # cannot recognise them in isolation, so the whole block (header,
    # body, footer) must come out redacted.
    cron = await _archive_cron(tmp_path)
    info = _output_run(
        [
            ("stdout", "-----BEGIN RSA PRIVATE KEY-----"),
            ("stdout", "MIIEpAIBAAKCAQEA7v0Kq1QYb3x2"),
            ("stdout", "u5m3o9CqkQxJ0Zb2n8T4w6YcAaBb"),
            ("stdout", "-----END RSA PRIVATE KEY-----"),
        ]
    )
    await cron._archive_output(cron.cron_jobs["j"], info)
    (rec,) = await cron.state_backend.list_records(cron._log_stream("j"))
    assert [ln["line"] for ln in rec["lines"]] == [REDACTED] * 4


async def test_archive_verbatim_when_redaction_off(tmp_path):
    # redactArchivedSecrets: false is an explicit opt-out: the archive
    # must be exactly what the job printed.
    cron = await _archive_cron(tmp_path, redact=False)
    info = _output_run([("stdout", "password=hunter2")])
    await cron._archive_output(cron.cron_jobs["j"], info)
    (rec,) = await cron.state_backend.list_records(cron._log_stream("j"))
    assert rec["redacted"] is False
    assert rec["lines"][0]["line"] == "password=hunter2"


# --- rehydration ------------------------------------------------------------


def test_rehydrate_corrupt_outcome_is_unknown():
    # a record missing (or corrupting) `outcome` must NOT rehydrate as
    # a fabricated "success": that skewed the dashboard stats and could
    # wrongly open the depends-on-past gate.
    info = _job_run_info_from_dict(
        {"finished_at": "2026-07-01T10:00:00+00:00"}
    )
    assert info is not None
    assert info.outcome == "unknown"


def test_rehydrate_mixed_naive_aware_duration():
    # a naive started_at next to an aware finished_at used to make the
    # .duration property raise TypeError on every dashboard request;
    # both timestamps are pinned aware now.
    info = _job_run_info_from_dict(
        {
            "finished_at": "2026-07-01T10:00:00+00:00",
            "started_at": "2026-07-01T09:59:00",
        }
    )
    assert info is not None
    assert info.duration == 60.0


async def test_rehydration_does_not_regress_fresh_run(tmp_path):
    # the ledger read awaits (and so yields): a run can finish in that
    # window.  Appending the snapshot's OLD records behind the fresh
    # run would regress last_run and scramble the history's order, so
    # rehydration must re-check after the await and stand down.
    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    cron._state_rehydrated = False  # force a fresh warm-up below
    fresh = _mem_run("failure", 9)
    old_rec = {
        "outcome": "success",
        "exit_code": 0,
        "started_at": None,
        "finished_at": "2026-07-01T00:00:00+00:00",
        "duration": None,
        "fail_reason": None,
    }

    async def racing_list(stream, *, limit=None, newest_first=False):
        # rehydration also reads the counters/retries streams these days;
        # only the run-history read carries this test's race.
        if not stream.startswith("runs/"):
            return []
        for _ in range(3):
            await asyncio.sleep(0)  # the read is "in flight"
        # a run finishes while the read is in flight: _record_run's
        # in-memory effect lands before the snapshot returns.
        cron.last_run["j"] = fresh
        cron.run_history["j"].append(fresh)
        return [old_rec]

    cron.state_backend.list_records = racing_list  # type: ignore[method-assign]
    await cron._rehydrate_from_state()
    assert cron.last_run["j"] is fresh  # not regressed to the old record
    assert list(cron.run_history["j"]) == [fresh]


async def test_state_path_change_rewarms_from_new_store(tmp_path):
    # switching the state path tears the old backend down; without
    # resetting _state_rehydrated, the replacement store never warmed
    # the dashboard history -- the old store's (here: empty) view was
    # served forever.
    path_a = tmp_path / "a"
    path_b = tmp_path / "b"
    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(path_a)))
    assert cron._state_rehydrated is True
    assert not cron.run_history.get("j")  # store A is empty
    # seed store B out of band, as a previous deployment would have
    seed = make_state_backend(_state_cfg(_state_yaml(path_b)), lambda: "s")
    await seed.start()
    await seed.append_record(
        "runs/j",
        {"outcome": "success", "finished_at": "2026-06-30T00:00:00+00:00"},
    )
    await cron.start_stop_state(_state_cfg(_state_yaml(path_b)))
    assert cron._state_rehydrated is True  # re-latched by the new warm-up
    assert len(cron.run_history["j"]) == 1
    assert (
        cron.last_run["j"].finished_at.isoformat()
        == "2026-06-30T00:00:00+00:00"
    )


_REPLACE_DEP_JOB = (
    "jobs:\n"
    "  - name: j\n"
    "    command: 'true'\n"
    "    schedule: '* * * * *'\n"
    "    onlyIfLastSucceeded: true\n"
    "    concurrencyPolicy: Replace\n"
)


async def test_depends_on_past_replace_policy_skips_running_block(tmp_path):
    # Replace's contract is that a new fire supersedes the running instance
    # (maybe_launch_job cancels it), so the gate's still-running block must
    # not apply: otherwise one hung run freezes a gated Replace job forever
    # (the fire never reaches the policy that would reap it).  The gate then
    # judges the last FINISHED outcome, exactly as before the hardening.
    cron = Cron(None, config_yaml=_REPLACE_DEP_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    await _put_ledger(cron, "success", "2026-07-01T10:00:00+00:00")
    cron.running_jobs["j"].append(object())  # a hung RunningJob stand-in
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is True
    # ...while a last-finished FAILURE still closes the gate for Replace.
    await _put_ledger(cron, "failure", "2026-07-01T10:05:00+00:00")
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is False


async def test_deferred_catch_up_anchors_to_first_evaluation_instant(
    tmp_path,
):
    # The backend can come up minutes after boot (start_stop_state retries).
    # In between, the live scheduler fired jobs statelessly, so a deferred
    # evaluation must count missed slots against the FIRST attempt's
    # instant: counting up to the (later) recovery instant would replay
    # runs that actually ran.
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    backend = cron.state_backend
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    # first attempt: backend "not started yet" -> deferred, reference pinned.
    cron.state_backend = None
    await cron._catch_up(_NOW)  # 10 slots missed as of _NOW (10:10:30)
    assert cron._caught_up is False and calls == []
    # backend recovers; the retry arrives 5 minutes later.
    cron.state_backend = backend
    cron._catchup_next_retry = 0.0
    later = _NOW + datetime.timedelta(minutes=5)
    await cron._catch_up(later)
    await asyncio.gather(*list(cron._catchup_tasks))
    # still the 10 pre-boot slots -- not 15.
    assert len(calls) == 10


async def test_backfill_revalidates_between_launches(tmp_path):
    # A serialized run-all backfill spans count x run-duration: a reload
    # disabling/removing the job mid-backfill must stop the remaining
    # launches (the old code revalidated only once, after the jitter).
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    calls = []

    async def fake(job, *, with_retries=True):
        calls.append(job.name)
        if len(calls) == 2:
            del cron.cron_jobs["j"]  # a reload removes the job
        return True

    cron.maybe_launch_job = fake  # type: ignore[method-assign]
    await cron._run_catch_up(cron.cron_jobs["j"], 5, 0.0, _NOW)
    assert calls == ["j", "j"]  # the remaining 3 launches were dropped


async def test_backfill_idle_wait_is_bounded_for_allow_policy(
    tmp_path, monkeypatch
):
    # An Allow job whose scheduled instances always overlap keeps
    # running_jobs non-empty forever; the idle wait between backfill
    # launches is pacing there, not correctness, so it must give up and
    # launch rather than starve the backfill and hold the checkpoint open.
    monkeypatch.setattr(
        "cronstable.cron.CATCHUP_IDLE_WAIT_LIMIT", 0.0
    )  # give up immediately: no wall-clock waiting in the test
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    cron.running_jobs["j"].append(object())  # ever-running scheduled instance
    await cron._run_catch_up(cron.cron_jobs["j"], 1, 0.0, _NOW)
    assert calls == ["j"]


# --- cluster slot: stale release vs fresh same-fence re-claim ----------------


_FORBID_CLUSTER_JOB = (
    "jobs:\n"
    "  - name: j\n"
    "    command: 'true'\n"
    "    schedule: '* * * * *'\n"
    "    concurrencyPolicy: Forbid\n"
    "    concurrencyScope: cluster\n"
)


async def _cluster_cron(tmp_path):
    cron = Cron(None, config_yaml=_FORBID_CLUSTER_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    return cron


async def _stop_cluster_cron(cron):
    for task in list(cron._slot_renewers.values()):
        task.cancel()
    cron._slot_renewers.clear()
    cron._slot_leases.clear()
    cron._slot_refs.clear()
    await asyncio.gather(*list(cron._pending_state_writes))
    if cron.state_backend is not None:
        await cron.state_backend.stop()
        cron.state_backend = None


async def test_stale_slot_release_stands_down_for_fresh_reclaim(tmp_path):
    # regression (slot-protocol): _release_cluster_slot pops the lease under
    # the per-job mutex but writes the on-disk release fire-and-forget, and
    # a same-holder re-acquire KEEPS the fence -- so a stale release landing
    # after a fresh re-claim still matched on disk and revoked the new
    # claim's lease, letting a peer's Forbid claim double-run. The release
    # must re-check under the mutex and stand down for a live claim.
    cron = await _cluster_cron(tmp_path)
    try:
        backend = cron.state_backend
        holder = cron._slot_holder()
        stale = await backend.acquire_lease("slots/j", holder, cron._slot_ttl)
        fresh = await backend.acquire_lease("slots/j", holder, cron._slot_ttl)
        assert stale is not None and fresh is not None
        assert fresh.fence == stale.fence  # the kept-fence re-acquire
        cron._slot_leases["j"] = fresh
        cron._slot_refs["j"] = 1
        await cron._release_slot_lease("j", stale)
        assert await backend.read_lease("slots/j") is not None
        # ...while with no live claim the release still frees the slot
        cron._slot_leases.pop("j", None)
        cron._slot_refs.pop("j", None)
        await cron._release_slot_lease("j", fresh)
        assert await backend.read_lease("slots/j") is None
    finally:
        await _stop_cluster_cron(cron)


async def test_gc_reclaims_removed_scope_artifacts_and_orphan_blobs(
    tmp_path, monkeypatch
):
    # regression (GC review): artifact streams were absent from the daemon
    # GC's keep map ("unrecognised: kept forever") and the fully-implemented
    # blob sweep had no production caller, so a removed job's artifacts --
    # and every orphaned payload blob -- leaked without bound.  One pass
    # must age out a removed scope's stream and sweep its blob, while a
    # config job's scope, the shared scope, a referenced blob, and a
    # just-written (not-yet-recorded) blob all survive.
    import cronstable.state as state_mod
    from cronstable import jobstate

    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    try:
        backend = cron.state_backend
        old_epoch = state_mod._now() - 7200.0
        monkeypatch.setattr(state_mod, "_now", lambda: old_epoch)
        try:
            gone = await jobstate.artifact_put(
                backend, "gone", "a", b"gone-payload"
            )
            kept = await jobstate.artifact_put(
                backend, "j", "k", b"job-payload"
            )
            shared = await jobstate.artifact_put(
                backend, "global", "g", b"shared-payload"
            )
        finally:
            monkeypatch.undo()
        for rec in (gone, kept, shared):
            path = backend._blob_path(rec["sha256"])
            os.utime(path, (old_epoch, old_epoch))
        # unreferenced but young: the put-then-record window's blob.
        young = await backend.put_blob(b"just-put-no-record-yet")
        await _seed_gc_anchor(cron)
        cron._state_gc_grace = 3600.0
        await cron._collect_state_garbage()
        assert await backend.list_records("artifacts/gone") == []
        assert await backend.get_blob(gone["sha256"]) is None
        assert len(await backend.list_records("artifacts/j")) == 1
        assert await backend.get_blob(kept["sha256"]) == b"job-payload"
        assert len(await backend.list_records("artifacts/global")) == 1
        assert await backend.get_blob(shared["sha256"]) == b"shared-payload"
        assert await backend.get_blob(young) is not None
    finally:
        await cron.state_backend.stop()
        cron.state_backend = None


async def test_gc_blob_sweep_skipped_when_artifact_stream_hidden(
    tmp_path, monkeypatch
):
    # the fail-safe: a legacy length-truncated stream directory without its
    # name sidecar is skipped by enumeration, so its records -- and the blob
    # references inside them -- are invisible.  The sweep must then not run
    # at all this pass (the hidden stream's blob would otherwise read as an
    # orphan and a LIVE payload would be deleted).
    import cronstable.state as state_mod
    from cronstable import jobstate

    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    try:
        backend = cron.state_backend
        old_epoch = state_mod._now() - 7200.0
        hidden_scope = "S" * 200
        monkeypatch.setattr(state_mod, "_now", lambda: old_epoch)
        try:
            hidden = await jobstate.artifact_put(
                backend, hidden_scope, "a", b"hidden-payload"
            )
        finally:
            monkeypatch.undo()
        os.utime(
            backend._blob_path(hidden["sha256"]), (old_epoch, old_epoch)
        )
        stream_dir = backend._stream_dir("artifacts/" + hidden_scope)
        os.unlink(os.path.join(stream_dir, state_mod._STREAM_NAME_SIDECAR))
        await _seed_gc_anchor(cron)
        cron._state_gc_grace = 3600.0
        await cron._collect_state_garbage()
        # the unclassifiable stream is kept (existing collect_garbage rule)
        # AND its blob survived, proving the sweep stood down.
        recs = await backend.list_records("artifacts/" + hidden_scope)
        assert len(recs) == 1
        assert await backend.get_blob(hidden["sha256"]) == b"hidden-payload"
    finally:
        await cron.state_backend.stop()
        cron.state_backend = None


async def test_gc_leaves_artifacts_unmanaged_without_scope_manifests(
    tmp_path, monkeypatch
):
    # rolling-upgrade safety: while any recent manifest predates scope/dag
    # advertising, its node's shared artifact scopes are unknowable, so
    # artifact streams must stay wholly unmanaged (kept) -- even an aged,
    # unreferenced scope -- while ordinary job streams still collect.
    import cronstable.state as state_mod
    from cronstable import jobstate

    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    try:
        backend = cron.state_backend
        old_epoch = state_mod._now() - 7200.0
        monkeypatch.setattr(state_mod, "_now", lambda: old_epoch)
        try:
            gone = await jobstate.artifact_put(
                backend, "gone", "a", b"gone-payload"
            )
            await backend.append_record("runs/orphan", {"finished_at": "x"})
        finally:
            monkeypatch.undo()
        os.utime(backend._blob_path(gone["sha256"]), (old_epoch, old_epoch))
        await _seed_gc_anchor(cron, covered=False)
        cron._state_gc_grace = 3600.0
        await cron._collect_state_garbage()
        assert await backend.list_records("runs/orphan") == []
        assert len(await backend.list_records("artifacts/gone")) == 1
        # its record survived, so its blob is referenced and kept too.
        assert await backend.get_blob(gone["sha256"]) == b"gone-payload"
    finally:
        await cron.state_backend.stop()
        cron.state_backend = None


async def test_gc_pass_reclaims_only_ephemeral_leases(tmp_path, monkeypatch):
    # the daemon pass wires the ephemeral-lease prefix through to the
    # backend: a dead-past-grace dagadvance/ per-run lease is reclaimed
    # while a slots/ lease of the same age survives -- its fence can live
    # on in durable Replace-cancel records (cron._request_replace /
    # _slot_renewer), so no grace window ever makes a slot fence reset
    # safe.
    import cronstable.state as state_mod

    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    try:
        backend = cron.state_backend
        old_epoch = state_mod._now() - 7200.0
        monkeypatch.setattr(state_mod, "_now", lambda: old_epoch)
        try:
            assert await backend.acquire_lease(
                "dagadvance/d/r1", "A", ttl=10.0
            )
            assert await backend.acquire_lease("slots/j", "A", ttl=10.0)
        finally:
            monkeypatch.undo()
        dag_lock, dag_lease = backend._lease_paths("dagadvance/d/r1")
        slot_lock, slot_lease = backend._lease_paths("slots/j")
        for path in (dag_lease, slot_lease):
            os.utime(path, (old_epoch, old_epoch))
        await _seed_gc_anchor(cron)
        cron._state_gc_grace = 3600.0
        await cron._collect_state_garbage()
        assert not os.path.exists(dag_lease)
        assert not os.path.exists(dag_lock)
        assert os.path.exists(slot_lease)  # the fence line's only home
        assert os.path.exists(slot_lock)
    finally:
        await cron.state_backend.stop()
        cron.state_backend = None


async def test_slot_release_write_yields_to_racing_reclaim(tmp_path):
    # the same hazard through the production path: the finish-path release
    # schedules its write fire-and-forget, the job's next fire re-claims
    # immediately (same holder, fence kept), and only then does the
    # scheduled write run. The slot must still be held on disk afterwards,
    # under the original fence -- the new run's claim survived.
    cron = await _cluster_cron(tmp_path)
    try:
        job = cron.cron_jobs["j"]
        backend = cron.state_backend
        assert await cron._claim_cluster_slot(job) is True
        first = cron._slot_leases["j"]
        await cron._release_cluster_slot(job)  # schedules the stale write
        assert await cron._claim_cluster_slot(job) is True  # fresh re-claim
        await asyncio.gather(*list(cron._pending_state_writes))
        observed = await backend.read_lease("slots/j")
        assert observed is not None
        assert observed.holder == cron._slot_holder()
        assert observed.fence == first.fence
    finally:
        await _stop_cluster_cron(cron)
