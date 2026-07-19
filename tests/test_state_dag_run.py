"""The DAG runtime driven against a real backend + loopback API.

Where test_state_dag.py exercises the pure state machine, this file drives
:class:`cronstable.dagrun.DagScheduler` end to end: a real
:class:`~cronstable.state.FilesystemStateBackend` in a temp dir, the real
loopback
job-state API bound to an ephemeral port, and real task subprocesses (launched
through the same :class:`~cronstable.job.RunningJob` path a job uses).  A small
in-test pump stands in for ``Cron.run``'s reaper: it awaits each launched task,
routes its completion through ``cron._handle_finished_job`` (which the DAG
scheduler picks up), then flushes the spawned advances, looping until the run
is terminal.

Style: bare ``async def`` tests, ``asyncio_mode = auto``, no frozen clock and
no duration asserts (the drive loop is progress-driven, not time-driven).  Task
commands are ``[sys.executable, ...]`` argv lists (no shell), so they are
cross-platform; the fan-out / XCom test uses the real ``cronstable xcom`` CLI
over the loopback endpoint.
"""

import asyncio
import datetime
import json
import sys

import pytest

from cronstable import dag, dagrun, jobstate
from cronstable.cron import Cron
from cronstable.state import Lease
from tests.test_state_job_primitives import _break_record_reads

_PY = sys.executable
_UTC = datetime.timezone.utc


def _utcnow():
    return datetime.datetime.now(_UTC)


def _state_cfg(yaml):
    from cronstable.config import parse_config_string

    return parse_config_string(yaml, "").state_config


async def _drain_pending(cron):
    # run every spawned state-write (advances launch the next tasks); advances
    # spawn further advances, so loop until the set is quiet.
    for _ in range(50):
        pend = [t for t in list(cron._pending_state_writes) if not t.done()]
        if not pend:
            return
        await asyncio.gather(*pend, return_exceptions=True)


async def _reap_running(cron):
    """Await every currently-running task and route its completion."""
    rjs = [
        rj for jobs in list(cron.running_jobs.values()) for rj in list(jobs)
    ]
    for rj in rjs:
        await rj.wait()
        await cron._handle_finished_job(rj)
    return bool(rjs)


async def _drive(cron, dag_name, run_key, *, max_rounds=60):
    """Pump the run to a terminal state (or until it parks/blocks)."""
    for _ in range(max_rounds):
        reaped = await _reap_running(cron)
        await _drain_pending(cron)
        body = await cron._dag.get_run(dag_name, run_key)
        if body and body.get("state") in (dag.SUCCESS, dag.FAILED):
            return body
        if not reaped and not any(cron.running_jobs.values()):
            # nothing running and not terminal: force one more advance (a due
            # retry / a just-recorded completion) then re-check.
            await cron._dag.advance_one((dag_name, run_key))
            await _drain_pending(cron)
            body = await cron._dag.get_run(dag_name, run_key)
            if body and body.get("state") in (dag.SUCCESS, dag.FAILED):
                return body
            if not any(cron.running_jobs.values()):
                return body  # blocked (e.g. an approval gate): stop here
    return await cron._dag.get_run(dag_name, run_key)


async def _make_cron(tmp_path, yaml):
    state = "state:\n  path: {}\n".format(tmp_path)
    cron = Cron(None, config_yaml=state + yaml)
    await cron.start_stop_state(_state_cfg(state + yaml))
    return cron


async def _teardown(cron):
    await cron._dag.shutdown()
    await cron._stop_job_api()
    if cron.state_backend is not None:
        await cron.state_backend.stop()


def _set_cmd(cron, dag_name, task_id, argv, env=None):
    """Override a task template's command/env with a real argv (no shell)."""
    tmpl = cron.cron_dags[dag_name].task_templates[task_id]
    tmpl.command = argv
    if env is not None:
        tmpl.environment = [{"key": k, "value": v} for k, v in env.items()]


def _states(body):
    return {k: v["state"] for k, v in body["tasks"].items()}


# --------------------------------------------------------------------------
# Linear + failure propagation
# --------------------------------------------------------------------------

_LINEAR = """
dags:
  - name: lin
    tasks:
      - id: a
        command: 'x'
      - id: b
        command: 'x'
        dependsOn:
          - a
"""


async def test_linear_dag_runs_to_success(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        _set_cmd(cron, "lin", "a", [_PY, "-c", "pass"])
        _set_cmd(cron, "lin", "b", [_PY, "-c", "pass"])
        run_key = await cron._dag.trigger_run("lin")
        body = await _drive(cron, "lin", run_key)
        assert body["state"] == dag.SUCCESS
        assert _states(body) == {"a": dag.SUCCESS, "b": dag.SUCCESS}
    finally:
        await _teardown(cron)


async def test_failure_propagates_downstream(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        _set_cmd(cron, "lin", "a", [_PY, "-c", "import sys; sys.exit(3)"])
        _set_cmd(cron, "lin", "b", [_PY, "-c", "pass"])
        run_key = await cron._dag.trigger_run("lin")
        body = await _drive(cron, "lin", run_key)
        assert body["state"] == dag.FAILED
        assert body["tasks"]["a"]["state"] == dag.FAILED
        assert body["tasks"]["b"]["state"] == dag.UPSTREAM_FAILED
    finally:
        await _teardown(cron)


async def test_task_retry_then_success(tmp_path):
    # a: fails on its first attempt (no flag file), succeeds on the retry
    # (flag now exists). retries: 1 -> two attempts.
    flag = tmp_path / "flag"
    script = (
        "import os,sys; f=r'{}';"
        "sys.exit(0) if os.path.exists(f) else "
        "(open(f,'w').close() or sys.exit(1))".format(flag)
    )
    yaml = (
        "dags:\n  - name: r\n    tasks:\n"
        "      - id: a\n        command: 'x'\n        retries: 1\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        _set_cmd(cron, "r", "a", [_PY, "-c", script])
        run_key = await cron._dag.trigger_run("r")
        body = await _drive(cron, "r", run_key)
        assert body["state"] == dag.SUCCESS
        assert body["tasks"]["a"]["attempt"] == 1  # one retry consumed
    finally:
        await _teardown(cron)


# --------------------------------------------------------------------------
# Fan-out + XCom, end to end through the real CLI
# --------------------------------------------------------------------------

_FANOUT = """
dags:
  - name: fan
    tasks:
      - id: gen
        command: 'x'
      - id: work
        command: 'x'
        dependsOn:
          - gen
        expand:
          fromTask: gen
          key: items
      - id: collect
        command: 'x'
        dependsOn:
          - work
      - id: producer
        command: 'x'
      - id: consumer
        command: 'x'
        dependsOn:
          - producer
"""


async def test_e2e_fanout_and_xcom(tmp_path):
    cron = await _make_cron(tmp_path, _FANOUT)
    try:
        items_file = tmp_path / "items.json"
        items_file.write_text('["alpha", "beta", "gamma"]')
        msg_file = tmp_path / "msg.txt"
        msg_file.write_text("hello-downstream")
        consumed = tmp_path / "consumed.txt"
        outdir = tmp_path / "work"
        outdir.mkdir()

        # gen publishes the fan-out list through the real
        # `cronstable xcom` CLI.
        _set_cmd(
            cron,
            "fan",
            "gen",
            [
                _PY,
                "-m",
                "cronstable",
                "xcom",
                "push",
                "--key",
                "items",
                str(items_file),
            ],
        )
        # each mapped worker writes its injected item to work/<index>.
        worker = (
            "import os;"
            "open(os.path.join(r'{}', os.environ['CRONSTABLE_DAG_MAP_INDEX']),"
            "'w').write(os.environ['CRONSTABLE_DAG_MAP_ITEM'])".format(outdir)
        )
        _set_cmd(cron, "fan", "work", [_PY, "-c", worker])
        _set_cmd(cron, "fan", "collect", [_PY, "-c", "pass"])
        _set_cmd(
            cron,
            "fan",
            "producer",
            [
                _PY,
                "-m",
                "cronstable",
                "xcom",
                "push",
                "--key",
                "msg",
                str(msg_file),
            ],
        )
        _set_cmd(
            cron,
            "fan",
            "consumer",
            [
                _PY,
                "-m",
                "cronstable",
                "xcom",
                "pull",
                "--task",
                "producer",
                "--key",
                "msg",
                "-o",
                str(consumed),
            ],
        )

        run_key = await cron._dag.trigger_run("fan")
        body = await _drive(cron, "fan", run_key, max_rounds=80)

        assert body["state"] == dag.SUCCESS, _states(body)
        # fan-out expanded deterministically to the published list
        assert body["mapped"]["work"]["items"] == ["alpha", "beta", "gamma"]
        assert body["tasks"]["work#1"]["mapItem"] == "beta"
        # each worker received and wrote its own item (JSON-encoded string)
        assert json.loads((outdir / "0").read_text()) == "alpha"
        assert json.loads((outdir / "2").read_text()) == "gamma"
        # the XCom hand-off round-tripped through the real pull CLI
        assert consumed.read_text() == "hello-downstream"
    finally:
        await _teardown(cron)


# --------------------------------------------------------------------------
# Crash-resume: a task left running by a dead process is resumed from state
# --------------------------------------------------------------------------


async def test_mapped_upstream_publishes_nothing_expands_empty(tmp_path):
    # a mapped task whose upstream SUCCEEDS but never publishes its list must
    # not wedge the run: it fans out to zero instances and its downstream runs.
    yaml = (
        "dags:\n  - name: em\n    tasks:\n"
        "      - id: gen\n        command: 'x'\n"
        "      - id: work\n        command: 'x'\n        dependsOn:\n"
        "          - gen\n        expand:\n"
        "          fromTask: gen\n          key: items\n"
        "      - id: after\n        command: 'x'\n        dependsOn:\n"
        "          - work\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        _set_cmd(cron, "em", "gen", [_PY, "-c", "pass"])  # publishes nothing
        _set_cmd(cron, "em", "work", [_PY, "-c", "pass"])
        _set_cmd(cron, "em", "after", [_PY, "-c", "pass"])
        run_key = await cron._dag.trigger_run("em")
        body = await _drive(cron, "em", run_key)
        assert body["state"] == dag.SUCCESS, _states(body)
        assert body["mapped"]["work"]["items"] == []
        assert body["tasks"]["after"]["state"] == dag.SUCCESS
    finally:
        await _teardown(cron)


async def test_mapped_expansion_transient_read_error_is_not_an_empty_fanout(
    tmp_path, monkeypatch
):
    # The counterpart to the test above: an upstream that DID publish its list,
    # read at an instant the store cannot answer for (an ESTALE/EIO blip on a
    # shared NFS/EFS mount). Expansion is recorded once and never recomputed,
    # so reading that blip as "published nothing" would freeze it into a
    # vacuously-successful empty fan-out -- the whole task's work silently
    # skipped, with downstream tasks seeing success. It must read as UNKNOWN
    # (None), leaving the task unexpanded to retry on a later pass.
    yaml = (
        "dags:\n  - name: tr\n    tasks:\n"
        "      - id: gen\n        command: 'x'\n"
        "      - id: work\n        command: 'x'\n        dependsOn:\n"
        "          - gen\n        expand:\n"
        "          fromTask: gen\n          key: items\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        backend = cron.state_backend
        scope = dag.xcom_scope("tr", "run-1")
        await jobstate.artifact_put(
            backend, scope, dag.xcom_name("gen", "items"), b'["a", "b"]'
        )
        read = cron._dag._read_xcom_list
        assert await read("run-1", "tr", "gen", "items") == ["a", "b"]

        _break_record_reads(
            monkeypatch, backend, jobstate.ARTIFACT_STREAM_PREFIX + scope
        )
        # the fix: None (unknown -> retry), NOT [] (empty -> permanent)
        assert await read("run-1", "tr", "gen", "items") is None

        # the blip clears and the real fan-out is still there to be found: the
        # run resumes with its work intact rather than having skipped it.
        monkeypatch.undo()
        assert await read("run-1", "tr", "gen", "items") == ["a", "b"]
    finally:
        await _teardown(cron)


async def test_mapped_upstream_nonportable_value_does_not_wedge(tmp_path):
    # A mapped task whose upstream publishes a value that PARSES but is not
    # fleet-portable (an int outside the 64-bit window) must not wedge the run.
    # On the stdlib-json baseline the value stays an exact out-of-range int, so
    # embedding it in the run document would raise UnsupportedValue on EVERY
    # advance forever; _read_xcom_list now drops it to an empty fan-out. (With
    # orjson the loader coerces/rejects it first; either way the run must reach
    # a terminal state, never wedge.)
    yaml = (
        "dags:\n  - name: np\n    tasks:\n"
        "      - id: gen\n        command: 'x'\n"
        "      - id: work\n        command: 'x'\n        dependsOn:\n"
        "          - gen\n        expand:\n"
        "          fromTask: gen\n          key: items\n"
        "      - id: after\n        command: 'x'\n        dependsOn:\n"
        "          - work\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        items_file = tmp_path / "items.json"
        items_file.write_text("[100000000000000000000]")  # 10^20 > 2^64 - 1
        _set_cmd(
            cron,
            "np",
            "gen",
            [
                _PY,
                "-m",
                "cronstable",
                "xcom",
                "push",
                "--key",
                "items",
                str(items_file),
            ],
        )
        _set_cmd(cron, "np", "work", [_PY, "-c", "pass"])
        _set_cmd(cron, "np", "after", [_PY, "-c", "pass"])
        run_key = await cron._dag.trigger_run("np")
        body = await _drive(cron, "np", run_key, max_rounds=80)
        assert body["state"] == dag.SUCCESS, _states(body)  # not wedged
        assert body["tasks"]["after"]["state"] == dag.SUCCESS
    finally:
        await _teardown(cron)


async def test_e2e_crash_resume(tmp_path):
    yaml = (
        "dags:\n  - name: cr\n    tasks:\n"
        "      - id: a\n        command: 'x'\n        retries: 2\n"
        "      - id: b\n        command: 'x'\n        dependsOn:\n"
        "          - a\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        _set_cmd(cron, "cr", "a", [_PY, "-c", "pass"])
        _set_cmd(cron, "cr", "b", [_PY, "-c", "pass"])
        run_key = await cron._dag.trigger_run("cr")
        ns = dag.DAG_RUN_NS_PREFIX + "cr"

        # Simulate a crash mid-run: a PRIOR daemon claimed+launched task `a`
        # then died. Rewrite the durable doc so `a` is running under a foreign
        # proc token with a dead pid, and drop this scheduler's ownership (as a
        # restart would). No live subprocess exists for it.
        await _reap_running(cron)  # reap whatever the initial advance launched
        await _drain_pending(cron)

        def _crash(body):
            entry = body["tasks"]["a"]
            entry["state"] = dag.RUNNING
            entry["proc"] = "dead-daemon#deadbeef"
            entry["pid"] = 2147480000  # a pid that is not alive
            entry["host"] = cron._state_host
            entry["finishedAt"] = None
            return body, None

        await cron.state_backend.mutate_document(ns, run_key, _crash)
        cron._dag.forget()  # as if this process is a fresh restart

        # Boot reconciliation adopts the run and recovers `a` from durable
        # state (dead pid -> retryable), then the drive resumes it to success.
        await cron._dag.reconcile_on_boot()
        body = await _drive(cron, "cr", run_key)
        assert body["state"] == dag.SUCCESS, _states(body)
        assert body["tasks"]["a"]["state"] == dag.SUCCESS
        assert body["tasks"]["b"]["state"] == dag.SUCCESS
    finally:
        await _teardown(cron)


# --------------------------------------------------------------------------
# Approval gate over the (in-process) control surface
# --------------------------------------------------------------------------


async def test_approval_gate_blocks_then_resumes(tmp_path):
    yaml = (
        "dags:\n  - name: ap\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
        "      - id: gate\n        type: approval\n        dependsOn:\n"
        "          - a\n"
        "      - id: b\n        command: 'x'\n        dependsOn:\n"
        "          - gate\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        _set_cmd(cron, "ap", "a", [_PY, "-c", "pass"])
        _set_cmd(cron, "ap", "b", [_PY, "-c", "pass"])
        run_key = await cron._dag.trigger_run("ap")
        body = await _drive(cron, "ap", run_key)
        # blocked at the gate: not terminal, b not started
        assert body["state"] == dag.RUNNING
        assert body["tasks"]["gate"]["awaitingApproval"] is True
        assert body["tasks"]["b"]["state"] == dag.PENDING
        # a wrong task key is rejected cleanly
        bad = await cron._dag.approve(
            "ap", run_key, "nope", approved=True, by="me"
        )
        assert bad["ok"] is False
        # approve and resume to completion
        ok = await cron._dag.approve(
            "ap", run_key, "gate", approved=True, by="alice"
        )
        assert ok["ok"] is True
        body = await _drive(cron, "ap", run_key)
        assert body["state"] == dag.SUCCESS
        assert body["tasks"]["gate"]["approval"]["by"] == "alice"
    finally:
        await _teardown(cron)


async def test_approval_reject_skip_cascades(tmp_path):
    yaml = (
        "dags:\n  - name: rj\n    tasks:\n"
        "      - id: gate\n        type: approval\n        onReject: skip\n"
        "      - id: b\n        command: 'x'\n        dependsOn:\n"
        "          - gate\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        _set_cmd(cron, "rj", "b", [_PY, "-c", "pass"])
        run_key = await cron._dag.trigger_run("rj")
        await _drive(cron, "rj", run_key)
        res = await cron._dag.approve(
            "rj", run_key, "gate", approved=False, by="bob"
        )
        assert res["ok"] is True
        body = await _drive(cron, "rj", run_key)
        assert body["state"] == dag.SUCCESS  # skip is not a failure
        assert body["tasks"]["gate"]["state"] == dag.SKIPPED
        assert body["tasks"]["b"]["state"] == dag.SKIPPED
    finally:
        await _teardown(cron)


# --------------------------------------------------------------------------
# Manual trigger, introspection, backfill
# --------------------------------------------------------------------------


async def test_trigger_and_introspection(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        _set_cmd(cron, "lin", "a", [_PY, "-c", "pass"])
        _set_cmd(cron, "lin", "b", [_PY, "-c", "pass"])
        dags = await cron._dag.list_dags()
        assert dags[0]["name"] == "lin"
        assert {t["id"] for t in dags[0]["tasks"]} == {"a", "b"}
        run_key = await cron._dag.trigger_run("lin")
        await _drive(cron, "lin", run_key)
        runs = await cron._dag.list_runs("lin")
        assert len(runs) == 1
        assert runs[0]["state"] == dag.SUCCESS
        assert runs[0]["kind"] == "manual"
        one = await cron._dag.get_run("lin", run_key)
        assert one["runKey"] == run_key
        assert await cron._dag.get_run("lin", "nope") is None
        assert await cron._dag.trigger_run("ghost") is None
    finally:
        await _teardown(cron)


async def test_backfill_creates_runs(tmp_path):
    yaml = (
        "dags:\n  - name: bf\n    schedule: '0 * * * *'\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        _set_cmd(cron, "bf", "a", [_PY, "-c", "pass"])
        # three hourly instants in the window
        res = await cron._dag.backfill(
            "bf", "2026-01-01T00:00:00+00:00", "2026-01-01T02:30:00+00:00"
        )
        assert res["ok"] is True
        assert res["created"] == 3
        # idempotent: re-running the same backfill creates no duplicates
        res2 = await cron._dag.backfill(
            "bf", "2026-01-01T00:00:00+00:00", "2026-01-01T02:30:00+00:00"
        )
        assert res2["created"] == 3  # stepped again, but create-if-absent
        runs = await cron._dag.list_runs("bf")
        keys = {r["runKey"] for r in runs}
        assert len(keys) == 3  # exactly three distinct runs
        bad = await cron._dag.backfill("bf", "bad", "worse")
        assert bad["ok"] is False
    finally:
        await _teardown(cron)


async def test_backfill_nonutc_range_dedupes_with_utc(tmp_path):
    # A backfill range in a non-UTC offset must derive the SAME (UTC) run keys
    # as the scheduled/catch-up path, or the same logical instant gets a second
    # run document and every task double-fires.
    yaml = (
        "dags:\n  - name: bf\n    schedule: '0 * * * *'\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        _set_cmd(cron, "bf", "a", [_PY, "-c", "pass"])
        # 00:00, 01:00, 02:00 UTC
        res = await cron._dag.backfill(
            "bf", "2026-01-01T00:00:00+00:00", "2026-01-01T02:30:00+00:00"
        )
        assert res["created"] == 3
        # the SAME instants expressed at -05:00 (prior day 19:00-21:30 local):
        # must dedupe onto the UTC keys, not create three more runs.
        await cron._dag.backfill(
            "bf", "2025-12-31T19:00:00-05:00", "2025-12-31T21:30:00-05:00"
        )
        keys = {r["runKey"] for r in await cron._dag.list_runs("bf")}
        assert len(keys) == 3  # deduped across the offset, not 6
        assert all(k.startswith("2026-01-01T0") for k in keys)  # canonical UTC
    finally:
        await _teardown(cron)


# --------------------------------------------------------------------------
# Scheduling: prompt firing (no busy-spin), reload reseed (no flood)
# --------------------------------------------------------------------------

_SCHED = (
    "dags:\n  - name: sch\n    schedule: '* * * * *'\n    tasks:\n"
    "      - id: a\n        command: 'x'\n"
)


async def test_scheduled_fire_is_prompt_and_advances(tmp_path):
    # regression: a scheduled fire must be a first-class wake source (no
    # busy-spin) and must advance its index when fired (no re-fire).
    # Deterministic: the index is driven with explicit instants rather than a
    # wall-clock boundary, so there is no timing flake.
    cron = await _make_cron(tmp_path, _SCHED)
    try:
        _set_cmd(cron, "sch", "a", [_PY, "-c", "pass"])
        # simulate a completed seed pass
        cron._dag._seeded["sch"] = cron._dag._sched_sig(cron.cron_dags["sch"])
        cron._dag._next_sched_check = dagrun._now() + 20.0
        # a FUTURE fire is advertised as a wake candidate -> the loop sleeps a
        # positive interval instead of busy-spinning at 0
        cron._dag._next_logical["sch"] = _utcnow() + datetime.timedelta(
            minutes=30
        )
        delay = cron._dag.next_wake_delay()
        assert delay is not None and delay > 0
        # a DUE fire is created directly (not waiting for the seed cadence) and
        # the index advances strictly PAST the fired instant -> it will not
        # re-fire the same instant / busy-spin.
        fired_at = _utcnow() - datetime.timedelta(seconds=70)
        cron._dag._next_logical["sch"] = fired_at
        await cron._dag._fire_scheduled(dagrun._now())
        runs = await cron._dag.list_runs("sch")
        assert len(runs) >= 1
        assert runs[0]["kind"] == "scheduled"
        assert cron._dag._next_logical["sch"] > fired_at
        for r in runs:
            await _drive(cron, "sch", r["runKey"])
    finally:
        await _teardown(cron)


async def test_reload_disable_reenable_reseeds_future(tmp_path):
    # regression: disabling then re-enabling a DAG must re-seed strictly-future
    # rather than backfilling every slot of the disabled window.
    yaml = (
        "dags:\n  - name: h\n    schedule: '0 * * * *'\n"
        "    onMissed: run-all\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        now = dagrun._now()
        await cron._dag._seed_dags(now)
        assert "h" in cron._dag._seeded
        # simulate a long disabled window by freezing the index in the past
        cron._dag._next_logical["h"] = _utcnow() - datetime.timedelta(hours=6)
        cron.cron_dags["h"].enabled = False
        await cron._dag._seed_dags(now)
        assert "h" not in cron._dag._next_logical
        assert "h" not in cron._dag._seeded
        cron.cron_dags["h"].enabled = True
        await cron._dag._seed_dags(now)
        assert cron._dag._next_logical["h"] > _utcnow() - datetime.timedelta(
            seconds=1
        )
        # crucially, no run was created for the 6h disabled gap
        assert await cron._dag.list_runs("h") == []
    finally:
        await _teardown(cron)


async def test_bounded_schedule_exhausts_without_crash(tmp_path):
    # regression: a fixed-year schedule that runs out of occurrences must DROP
    # its index (not store None), or the loop's .timestamp() sleep/due
    # candidates would crash the whole scheduler on the final occurrence.
    yaml = (
        "dags:\n  - name: once\n    schedule:\n"
        "      minute: '0'\n      hour: '0'\n      dayOfMonth: '1'\n"
        "      month: '1'\n      year: '2020'\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        _set_cmd(cron, "once", "a", [_PY, "-c", "pass"])
        # force the last (only) occurrence to be due, then fire it
        cron._dag._seeded["once"] = cron._dag._sched_sig(
            cron.cron_dags["once"]
        )
        cron._dag._next_logical["once"] = datetime.datetime(
            2020, 1, 1, tzinfo=_UTC
        )
        await cron._dag._fire_scheduled(dagrun._now())
        # the exhausted index was dropped, not poisoned with None ...
        assert "once" not in cron._dag._next_logical
        # ... so neither of the loop's consumers raises
        assert cron._dag.next_wake_delay() is not None
        cron._dag.service()  # must not raise
        runs = await cron._dag.list_runs("once")
        assert len(runs) == 1
        await _drive(cron, "once", runs[0]["runKey"])
    finally:
        await _teardown(cron)


async def test_awaiting_approval_polls_promptly(tmp_path):
    # a run blocked on an approval gate must be re-advanced soon (so a decision
    # made on a NON-owning node -- which cannot advance the run itself -- is
    # picked up by the owner within a few seconds, not a full idle cycle).
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        spec = cron.cron_dags["lin"].spec
        now = 1000.0
        body = {
            "tasks": {
                "gate": {"state": dag.RUNNING, "awaitingApproval": True},
                "other": {"state": dag.SUCCESS},
            }
        }
        wake = cron._dag._compute_wake(spec, body, now)
        assert wake == now + dagrun.APPROVAL_POLL_INTERVAL
        assert wake < now + 60.0
    finally:
        await _teardown(cron)


async def test_finish_removed_task_is_noop(tmp_path):
    # regression: a completion routed for a task the reload removed must not
    # crash (its spec is gone).
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        _set_cmd(cron, "lin", "a", [_PY, "-c", "pass"])
        run_key = await cron._dag.trigger_run("lin")
        await cron._dag._finish_task(
            cron.cron_dags["lin"],
            ("lin", run_key),
            "ghost",
            "ghost",
            success=True,
            exit_code=0,
            fail_reason=None,
        )  # no exception
        await _drive(cron, "lin", run_key)
    finally:
        await _teardown(cron)


# --------------------------------------------------------------------------
# Release-review regressions: busy-loops, wedges, stale owners, poisoned dags
# --------------------------------------------------------------------------


async def test_decision_on_unowned_run_does_not_pin_wake(tmp_path):
    # regression: approve() on a node that does NOT own the run (the
    # documented cross-node decision flow) left a permanent 0.0 wake entry;
    # next_wake_delay() then returned 0.0 forever and the main loop busy-spun
    # at 100% CPU until restart.
    yaml = (
        "dags:\n  - name: ap2\n    tasks:\n"
        "      - id: gate\n        type: approval\n"
        "      - id: b\n        command: 'x'\n        dependsOn:\n"
        "          - gate\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        _set_cmd(cron, "ap2", "b", [_PY, "-c", "pass"])
        run_key = await cron._dag.trigger_run("ap2")
        body = await _drive(cron, "ap2", run_key)
        assert body["tasks"]["gate"]["awaitingApproval"] is True
        ref = ("ap2", run_key)
        # simulate the non-owning peer: this node holds no advance lease
        cron._dag._drop_owned(ref)
        cron._dag._next_sched_check = dagrun._now() + 20.0
        res = await cron._dag.approve(
            "ap2", run_key, "gate", approved=True, by="bob"
        )
        assert res["ok"] is True  # the decision IS durably recorded
        await _drain_pending(cron)
        # the stale wake hint is gone and the loop sleeps a positive interval
        assert ref not in cron._dag._wake
        delay = cron._dag.next_wake_delay()
        assert delay is not None and delay > 0
        body = await cron._dag.get_run("ap2", run_key)
        assert body["tasks"]["gate"]["approval"]["by"] == "bob"
    finally:
        await _teardown(cron)


async def test_next_wake_delay_prunes_stale_advance_locks(tmp_path):
    # regression: advance_one setdefaults a per-ref Lock -- including for
    # peer-owned runs reached via an approval or a recorded completion --
    # and nothing pruned the map, so a long-lived daemon accumulated one
    # Lock per run it ever touched.  The sweep must also never drop a lock
    # that is held, awaited, or owned: a waiter resumes holding the OLD
    # object, and a fresh setdefault would advance the same run twice.
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        stale = ("lin", "manual-stale")
        held = ("lin", "manual-held")
        cron._dag._locks.setdefault(stale, asyncio.Lock())
        held_lock = cron._dag._locks.setdefault(held, asyncio.Lock())
        await held_lock.acquire()
        cron._dag.next_wake_delay()
        assert stale not in cron._dag._locks  # unowned + idle: swept
        assert held in cron._dag._locks  # held: kept

        # the release window: a waiter has been woken (its future resolved)
        # but has not resumed -- the lock reads unlocked, yet dropping it now
        # would strand the waiter on the old object while a newcomer mints a
        # fresh one.  The sweep must skip it.
        async def _waiter():
            async with held_lock:
                pass

        task = asyncio.ensure_future(_waiter())
        await asyncio.sleep(0)  # _waiter registers itself as a waiter
        held_lock.release()  # wakes the waiter; it has not run yet
        assert not held_lock.locked()
        cron._dag.next_wake_delay()
        assert held in cron._dag._locks  # waiter pending: kept
        await task  # the waiter acquires + releases the SURVIVING lock
        cron._dag.next_wake_delay()
        assert held not in cron._dag._locks  # now truly idle: swept
    finally:
        await _teardown(cron)


async def test_wake_ignores_inflight_sensor_poke(tmp_path):
    # regression: a RUNNING sensor whose poke subprocess is in flight kept
    # its stale PAST nextPokeAt as a wake candidate, pinning the loop's sleep
    # at 0 for the poke's whole duration (full advance per iteration).
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        spec = cron.cron_dags["lin"].spec
        now = 1000.0
        inflight = {
            "tasks": {
                "s": {
                    "state": dag.RUNNING,
                    "nextPokeAt": now - 100.0,  # stale due instant
                    "proc": "tok",
                    "pid": 4242,
                }
            }
        }
        assert cron._dag._compute_wake(spec, inflight, now) > now
        # an IDLE sensor's due instant still drives the wake
        idle = {
            "tasks": {
                "s": {
                    "state": dag.RUNNING,
                    "nextPokeAt": now + 30.0,
                    "proc": None,
                    "pid": None,
                }
            }
        }
        assert cron._dag._compute_wake(spec, idle, now) == now + 30.0
    finally:
        await _teardown(cron)


async def test_failed_completion_record_is_retried(tmp_path):
    # regression: a single failed completion RMW (a >10s store stall) left
    # the task RUNNING under our own proc token forever -- protected from
    # reconciliation, its lease renewed indefinitely, the run wedged until a
    # daemon restart.  The completion must be queued and retried until it
    # lands.
    yaml = (
        "dags:\n  - name: fc\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        _set_cmd(cron, "fc", "a", [_PY, "-c", "pass"])
        run_key = await cron._dag.trigger_run("fc")
        ref = ("fc", run_key)

        # the store stalls exactly when the reaper records the completion
        async def _stalled(dag_name, key, transform):
            raise asyncio.TimeoutError()

        orig = cron._dag._mutate
        cron._dag._mutate = _stalled
        await _reap_running(cron)
        await _drain_pending(cron)
        cron._dag._mutate = orig
        # the completion was queued, not lost; the entry is still RUNNING
        assert (ref, "a") in cron._dag._pending_completions
        body = await cron._dag.get_run("fc", run_key)
        assert body["tasks"]["a"]["state"] == dag.RUNNING
        assert cron._dag.next_wake_delay() is not None  # retry is a wake
        # a later service pass (store healthy again) lands it
        for pc in cron._dag._pending_completions.values():
            pc["nextTryAt"] = 0.0
        await cron._dag._retry_completions(dagrun._now())
        await _drain_pending(cron)
        assert not cron._dag._pending_completions
        body = await _drive(cron, "fc", run_key)
        assert body["state"] == dag.SUCCESS
        assert body["tasks"]["a"]["state"] == dag.SUCCESS
    finally:
        await _teardown(cron)


async def test_stale_sensor_poke_completion_retry_is_fenced(tmp_path):
    # regression: a sensor completion whose mutate TIMED OUT but actually
    # landed (partial landing: wait_for raised while the executor's write
    # still committed) was queued for retry; the next poke was then claimed
    # under the SAME proc token and attempt (a re-poke bumps only pokeCount),
    # so the stale retry passed the proc+attempt fence and cleared the LIVE
    # poke's proc under its running subprocess.  The poke-number fence must
    # drop it, and the settled queue entry must not retry forever.
    yaml = (
        "dags:\n  - name: sn\n    tasks:\n"
        "      - id: s\n        type: sensor\n        command: 'x'\n"
        "        pokeIntervalSeconds: 0\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        flag = tmp_path / "cond"
        _set_cmd(
            cron,
            "sn",
            "s",
            [
                _PY,
                "-c",
                "import os,sys; sys.exit(0 if os.path.exists({!r}) "
                "else 1)".format(str(flag)),
            ],
        )
        run_key = await cron._dag.trigger_run("sn")
        ref = ("sn", run_key)

        # poke 0's completion mutate COMMITS but the caller times out
        async def _partial(dag_name, key, transform):
            await orig(dag_name, key, transform)
            raise asyncio.TimeoutError()

        orig = cron._dag._mutate
        cron._dag._mutate = _partial
        await _reap_running(cron)
        cron._dag._mutate = orig
        # the completion was queued (as a poke-0 completion) even though it
        # landed on disk
        pc = cron._dag._pending_completions.get((ref, "s"))
        assert pc is not None and pc["poke"] == 0
        # pokeIntervalSeconds 0 < COMPLETION_RETRY_DELAY: the next poke is
        # claimed BEFORE the retry fires
        await _drain_pending(cron)
        body = await cron._dag.get_run("sn", run_key)
        entry = body["tasks"]["s"]
        assert entry["pokeCount"] == 1  # poke 0's completion DID land
        assert entry["proc"] == cron._proc_token  # poke 1 is in flight
        # the stale queued completion fires: the poke fence must drop it (the
        # live poke keeps its claim) and settle the queue entry
        for q in cron._dag._pending_completions.values():
            q["nextTryAt"] = 0.0
        await cron._dag._retry_completions(dagrun._now())
        assert not cron._dag._pending_completions
        body = await cron._dag.get_run("sn", run_key)
        entry = body["tasks"]["s"]
        assert entry["state"] == dag.RUNNING
        assert entry["proc"] == cron._proc_token  # NOT cleared by the retry
        assert entry["pokeCount"] == 1
        await _drain_pending(cron)
        # the live poke's own completion still lands and the run converges
        flag.write_text("ok")
        body = await _drive(cron, "sn", run_key)
        assert body["state"] == dag.SUCCESS
        assert body["tasks"]["s"]["state"] == dag.SUCCESS
    finally:
        await _teardown(cron)


async def test_superseded_owner_stops_advancing_after_lease_lapse(tmp_path):
    # regression: after a lease lapse + peer takeover (store unreachable, so
    # the renew loop never positively learned of it), the stale owner kept
    # advancing and would reconcile-fail the new owner's LIVE tasks.  An
    # expired local lease must be verified against the store's fence before
    # any mutate; positively superseded -> drop ownership, touch nothing.
    yaml = (
        "dags:\n  - name: st\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        dagcfg = cron.cron_dags["st"]
        run_key = "manual-takeover"
        assert await cron._dag._create_doc(dagcfg, run_key, None, "manual")
        ref = ("st", run_key)
        lease_name = cron._dag._lease_name(ref)
        lease = await cron.state_backend.acquire_lease(
            lease_name, cron._slot_holder(), 30.0
        )
        cron._dag._owned[ref] = lease
        cron._dag._locks.setdefault(ref, asyncio.Lock())
        # the peer re-claimed task `a` under its own proc token; its
        # subprocess is live on the peer host (the pid is dead HERE)
        ns = dag.DAG_RUN_NS_PREFIX + "st"

        def _peer_claims(body):
            entry = body["tasks"]["a"]
            entry["state"] = dag.RUNNING
            entry["proc"] = "peer-proc#1"
            entry["host"] = "peer-host"
            entry["pid"] = 2147480000
            return body, None

        await cron.state_backend.mutate_document(ns, run_key, _peer_claims)
        # our lease lapsed while the store was unreachable, and the peer took
        # it over (expire-in-place + acquire bumps the fence, as a real
        # expiry takeover does)
        lease.expires_at = dagrun._now() - 5.0
        await cron.state_backend.release_lease(lease)
        peer = await cron.state_backend.acquire_lease(
            lease_name, "peer-node", 30.0
        )
        assert peer is not None and peer.fence == lease.fence + 1
        # the stale owner's next advance must NOT touch the run
        await cron._dag.advance_one(ref)
        assert ref not in cron._dag._owned  # ownership dropped
        body = await cron._dag.get_run("st", run_key)
        entry = body["tasks"]["a"]
        assert entry["state"] == dag.RUNNING  # the live task was NOT failed
        assert entry["proc"] == "peer-proc#1"
        assert entry["failReason"] is None
        assert not any(cron.running_jobs.values())  # nothing launched here
    finally:
        await _teardown(cron)


async def test_noncrontab_schedule_does_not_crash_service(tmp_path):
    # regression: a schedule string the parser passes through verbatim (the
    # documented "@reboot") crashed the seed/backfill paths (an
    # AttributeError once -OO strips the assert in _compute_next_fire),
    # starving every other dag's service work.  It must degrade to "never
    # fires" plus a clean backfill refusal.
    yaml = (
        "dags:\n"
        "  - name: bad\n    schedule: '* * * * *'\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
        "  - name: good\n    schedule: '* * * * *'\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        cron.cron_dags["bad"].schedule_job.schedule = "@reboot"
        await cron._dag._seed_dags(dagrun._now())  # must not raise
        assert "good" in cron._dag._seeded
        assert "bad" not in cron._dag._next_logical  # it simply never fires
        res = await cron._dag.backfill(
            "bad", "2026-01-01T00:00:00+00:00", "2026-01-01T01:00:00+00:00"
        )
        assert res["ok"] is False  # clean refusal, not an exception/500
    finally:
        await _teardown(cron)


async def test_one_dag_seed_failure_does_not_starve_others(tmp_path):
    # regression: one dag's raising seed aborted the WHOLE service pass
    # (fire/adopt/advance/GC for every other dag) every cycle, spamming the
    # log.  It must be isolated, and logged/attempted once per signature.
    yaml = (
        "dags:\n"
        "  - name: p\n    schedule: '0 * * * *'\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
        "  - name: q\n    schedule: '0 * * * *'\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        calls = {"p": 0}
        orig = cron._dag._seed_dag

        async def flaky(dagcfg, now_dt):
            if dagcfg.name == "p":
                calls["p"] += 1
                raise RuntimeError("poisoned")
            return await orig(dagcfg, now_dt)

        cron._dag._seed_dag = flaky
        await cron._dag._seed_dags(dagrun._now())
        assert "q" in cron._dag._seeded  # the healthy dag still seeded
        assert "p" in cron._dag._seed_failed
        # the poisoned dag is not re-attempted (and re-logged) every cadence
        await cron._dag._seed_dags(dagrun._now())
        assert calls["p"] == 1
    finally:
        await _teardown(cron)


async def test_trigger_with_backend_down_raises(tmp_path):
    # regression: trigger_run returned a runKey (-> HTTP 200 + a success
    # toast) even when the run document was never written because no state
    # backend was available; the run silently never existed.
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        backend = cron.state_backend
        cron.state_backend = None  # the store failed to start / is down
        with pytest.raises(RuntimeError, match="could not be recorded"):
            await cron._dag.trigger_run("lin")
        cron.state_backend = backend
        assert await cron._dag.trigger_run("ghost") is None  # unknown: 404
    finally:
        await _teardown(cron)


# --------------------------------------------------------------------------
# HTTP control API (real server)
# --------------------------------------------------------------------------


async def _start_web(cron):
    await cron.start_stop_web_app(
        {"listen": ["http://127.0.0.1:0"], "ui": False}
    )
    port = cron.web_runner.addresses[0][1]
    return "http://127.0.0.1:{}".format(port)


async def test_http_dag_introspection_and_trigger(tmp_path):
    import aiohttp

    cron = await _make_cron(tmp_path, _LINEAR)
    base = await _start_web(cron)
    try:
        _set_cmd(cron, "lin", "a", [_PY, "-c", "pass"])
        _set_cmd(cron, "lin", "b", [_PY, "-c", "pass"])
        async with aiohttp.ClientSession() as s:
            async with s.get(base + "/dags") as r:
                assert r.status == 200
                assert (await r.json())[0]["name"] == "lin"
            async with s.post(base + "/dags/lin/trigger") as r:
                assert r.status == 200
                run_key = (await r.json())["runKey"]
            await _drive(cron, "lin", run_key)
            async with s.get(base + "/dags/lin/runs") as r:
                assert r.status == 200
                runs = (await r.json())["runs"]
                assert runs[0]["state"] == dag.SUCCESS
            url = base + "/dags/lin/runs/" + run_key
            async with s.get(url) as r:
                assert r.status == 200
                assert (await r.json())["runKey"] == run_key
            async with s.get(base + "/dags/ghost/runs") as r:
                assert r.status == 404
            async with s.post(base + "/dags/ghost/trigger") as r:
                assert r.status == 404
    finally:
        await cron.start_stop_web_app(None)
        await _teardown(cron)


async def test_http_approval_decision_and_backfill(tmp_path):
    import aiohttp

    yaml = (
        "dags:\n  - name: g\n    schedule: '0 * * * *'\n    tasks:\n"
        "      - id: gate\n        type: approval\n"
        "      - id: b\n        command: 'x'\n        dependsOn:\n"
        "          - gate\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    base = await _start_web(cron)
    try:
        _set_cmd(cron, "g", "b", [_PY, "-c", "pass"])
        run_key = await cron._dag.trigger_run("g")
        await _drive(cron, "g", run_key)
        async with aiohttp.ClientSession() as s:
            dec = base + "/dags/g/runs/{}/tasks/gate/decision".format(run_key)
            # not-awaiting task key -> 409
            bad = base + "/dags/g/runs/{}/tasks/none/decision".format(run_key)
            async with s.post(bad, json={"decision": "approve"}) as r:
                assert r.status == 409
            # bad decision value -> 400
            async with s.post(dec, json={"decision": "maybe"}) as r:
                assert r.status == 400
            # approve -> 200, then the run completes
            async with s.post(
                dec, json={"decision": "approve", "by": "carol"}
            ) as r:
                assert r.status == 200
            body = await _drive(cron, "g", run_key)
            assert body["state"] == dag.SUCCESS
            assert body["tasks"]["gate"]["approval"]["by"] == "carol"
            # backfill over the API
            async with s.post(
                base + "/dags/g/backfill",
                json={
                    "from": "2026-03-01T00:00:00+00:00",
                    "to": "2026-03-01T01:30:00+00:00",
                },
            ) as r:
                assert r.status == 200
                assert (await r.json())["created"] == 2
    finally:
        await cron.start_stop_web_app(None)
        await _teardown(cron)


# --------------------------------------------------------------------------
# Retention / removed-dag GC and XCom blob reclamation
# --------------------------------------------------------------------------

_RETAIN_ONE = """
dags:
  - name: rt
    retainRuns: 1
    tasks:
      - id: a
        command: 'x'
"""


async def test_retention_prune_releases_xcom_blobs_to_the_sweep(tmp_path):
    # THE leak from the GC review: dagrun's retention pruned XCom RECORD
    # streams (keep=0) but the content-addressed payload blobs they named
    # were never unlinked -- a dag pushing unique XCom payloads leaked one
    # blob per run forever.  Once records are pruned, the daemon sweep must
    # reclaim exactly the pruned run's blobs and keep the retained run's.
    import os

    import cronstable.state as state_mod
    from cronstable import jobstate

    cron = await _make_cron(tmp_path, _RETAIN_ONE)
    try:
        _set_cmd(cron, "rt", "a", [_PY, "-c", "pass"])
        backend = cron.state_backend
        keys, digests = [], []
        for i in range(2):
            run_key = await cron._dag.trigger_run("rt")
            body = await _drive(cron, "rt", run_key)
            assert body["state"] == dag.SUCCESS
            keys.append(run_key)
            scope = dag.xcom_scope("rt", str(body["runId"]))
            rec = await jobstate.artifact_put(
                backend, scope, "a#0/k", "unique-{}".format(i).encode()
            )
            digests.append(rec["sha256"])
        # retainRuns 1: the older run's document AND its XCom stream go.
        await cron._dag._gc_one_dag(backend, "rt", cron.cron_dags["rt"])
        docs = await backend.list_documents("dagrun/rt")
        assert [b["runKey"] for b in docs] == [keys[1]]
        # both payloads are old enough to sweep; only the orphan may go.
        old = state_mod._now() - 7200.0
        for digest in digests:
            os.utime(backend._blob_path(digest), (old, old))
        await cron._sweep_orphan_artifact_blobs(backend, 3600.0)
        assert await backend.get_blob(digests[0]) is None
        assert await backend.get_blob(digests[1]) == b"unique-1"
    finally:
        await _teardown(cron)


async def test_gc_removed_dags_grace_and_active_run_protection(tmp_path):
    # a dag briefly removed during a config edit must not lose run history:
    # gc_removed_dags takes only a TERMINAL run older than the grace, never
    # a recent or an active one -- and a dag still in config is untouched
    # even when named.
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        _set_cmd(cron, "lin", "a", [_PY, "-c", "pass"])
        _set_cmd(cron, "lin", "b", [_PY, "-c", "pass"])
        backend = cron.state_backend
        run_key = await cron._dag.trigger_run("lin")
        body = await _drive(cron, "lin", run_key)
        assert body["state"] == dag.SUCCESS
        # still configured: nothing may be collected even when named.
        await cron._dag.gc_removed_dags(backend, {"lin"}, 3600.0)
        assert len(await backend.list_documents("dagrun/lin")) == 1
        cron.cron_dags.clear()  # the dag is removed from config
        # terminal but recent (within the grace): kept.
        await cron._dag.gc_removed_dags(backend, {"lin"}, 3600.0)
        assert len(await backend.list_documents("dagrun/lin")) == 1

        def _age(cur):
            cur["updatedAt"] = cur["updatedAt"] - 7200.0
            return cur, None

        await backend.mutate_document("dagrun/lin", run_key, _age)

        # an ACTIVE (non-terminal) aged run of the removed dag: kept too.
        def _activate(cur):
            cur["state"] = dag.RUNNING
            return cur, None

        await backend.mutate_document("dagrun/lin", run_key, _activate)
        await cron._dag.gc_removed_dags(backend, {"lin"}, 3600.0)
        assert len(await backend.list_documents("dagrun/lin")) == 1

        # terminal AND aged past the grace: collected.
        def _finish(cur):
            cur["state"] = dag.SUCCESS
            return cur, None

        await backend.mutate_document("dagrun/lin", run_key, _finish)
        await cron._dag.gc_removed_dags(backend, {"lin"}, 3600.0)
        assert await backend.list_documents("dagrun/lin") == []
    finally:
        await _teardown(cron)


async def test_removed_dag_history_collected_by_daemon_gc_pass(
    tmp_path, monkeypatch
):
    # end to end through cron._collect_state_garbage: a dag removed from
    # config has its aged terminal run document deleted, its XCom stream
    # pruned, and the pruned records' payload blob swept -- while an active
    # run of the same removed dag survives untouched.
    import os

    import cronstable.state as state_mod
    from cronstable import jobstate

    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        _set_cmd(cron, "lin", "a", [_PY, "-c", "pass"])
        _set_cmd(cron, "lin", "b", [_PY, "-c", "pass"])
        backend = cron.state_backend
        run_key = await cron._dag.trigger_run("lin")
        body = await _drive(cron, "lin", run_key)
        assert body["state"] == dag.SUCCESS
        scope = dag.xcom_scope("lin", str(body["runId"]))
        old_epoch = state_mod._now() - 7200.0
        monkeypatch.setattr(state_mod, "_now", lambda: old_epoch)
        try:
            rec = await jobstate.artifact_put(
                backend, scope, "a#0/k", b"xcom-payload"
            )
        finally:
            monkeypatch.undo()
        os.utime(backend._blob_path(rec["sha256"]), (old_epoch, old_epoch))

        def _age(cur):
            cur["updatedAt"] = old_epoch
            return cur, None

        await backend.mutate_document("dagrun/lin", run_key, _age)
        # a second, still-active run: trigger only, never driven.
        run_key2 = await cron._dag.trigger_run("lin")
        await _drain_pending(cron)
        cron.cron_dags.clear()  # the dag is removed from config
        now = _utcnow()
        await backend.append_record(
            "manifests/old-host",
            {
                "jobSetId": "v1:old",
                "host": "old-host",
                "jobs": [],
                "at": (now - datetime.timedelta(seconds=7200)).isoformat(),
            },
        )
        await backend.append_record(
            "manifests/other-host",
            {
                "jobSetId": "v1:other",
                "host": "other-host",
                "jobs": [],
                "scopes": [],
                "dags": [],
                "at": now.isoformat(),
            },
        )
        cron._state_gc_grace = 3600.0
        await cron._collect_state_garbage()
        docs = await backend.list_documents("dagrun/lin")
        assert [b["runKey"] for b in docs] == [run_key2]
        assert await backend.list_records("artifacts/" + scope) == []
        assert await backend.get_blob(rec["sha256"]) is None
    finally:
        await _teardown(cron)


async def test_monitored_task_resources_land_in_run_record(tmp_path):
    # a finished DAG task's sampled usage (RunningJob.resource_usage) must be
    # recorded on its task record in the dag_run document and ride the run
    # API (get_run returns the raw document, which _web_dag_run serves).
    from cronstable.resources import ResourceUsage

    yaml = (
        "dags:\n  - name: mon\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
        "        monitorResources: true\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        _set_cmd(cron, "mon", "a", [_PY, "-c", "pass"])
        run_key = await cron._dag.trigger_run("mon")
        await _drain_pending(cron)
        if not any(cron.running_jobs.values()):
            await cron._dag.advance_one(("mon", run_key))
            await _drain_pending(cron)
        rjs = [rj for jobs in cron.running_jobs.values() for rj in jobs]
        assert len(rjs) == 1
        rj = rjs[0]
        await rj.wait()
        # the run is far too short for the sampler to capture anything
        # reliably, so stand in for the ResourceMonitor: inject the finished
        # usage it would have produced (wait() has already finalised the real
        # monitor) and route the completion through the real reaper path.
        usage = ResourceUsage(2.0, 1.0, 2048, 5)
        rj.resource_usage = usage
        await cron._handle_finished_job(rj)
        await _drain_pending(cron)
        body = await _drive(cron, "mon", run_key)
        assert body["state"] == dag.SUCCESS
        assert body["tasks"]["a"]["resources"] == usage.to_dict()
        # and the tolerant parser round-trips what was stored
        stored = body["tasks"]["a"]["resources"]
        assert ResourceUsage.from_dict(stored) == usage
    finally:
        await _teardown(cron)


# ===========================================================================
# Scheduler internals: catch-up replay, lease upkeep, orphan adoption,
# XCom fan-out reads, and the degraded-store guards.
# ===========================================================================

_HOURLY = (
    "dags:\n  - name: cu\n    schedule: '0 * * * *'\n"
    "    onMissed: run-all\n    tasks:\n"
    "      - id: a\n        command: 'x'\n"
    "  - name: cu1\n    schedule: '0 * * * *'\n"
    "    onMissed: run-once\n    tasks:\n"
    "      - id: a\n        command: 'x'\n"
)


def test_jitter_bounds():
    assert dagrun._jitter(0) == 0.0
    assert dagrun._jitter(-1) == 0.0
    assert 0.0 <= dagrun._jitter(5.0) <= 5.0


async def test_scheduler_tolerates_missing_backend():
    # a Cron without a state section still builds the DAG scheduler; every
    # store-touching entry point must degrade, not crash
    cron = Cron(None, config_yaml=_LINEAR)
    assert await cron._dag._read("lin", "k") is None
    await cron._dag._adopt_orphans()
    assert await cron._dag._read_xcom_list("rid", "lin", "a", "k") is None


async def test_catch_up_replays_missed_slots(tmp_path):
    cron = await _make_cron(tmp_path, _HOURLY)
    try:
        base = datetime.datetime(2026, 1, 1, 0, 0, tzinfo=_UTC)
        now_dt = datetime.datetime(2026, 1, 1, 3, 45, tzinfo=_UTC)
        for name, expected_kinds in (
            ("cu", 3),  # run-all: 01:00, 02:00 and 03:00 all replayed
            ("cu1", 1),  # run-once: only the newest missed slot
        ):
            dagcfg = cron.cron_dags[name]
            await cron._dag._create_run(dagcfg, base, "scheduled")
            await cron._dag._catch_up(dagcfg, now_dt)
            runs = await cron._dag.list_runs(name, limit=10)
            kinds = [r["kind"] for r in runs]
            assert kinds.count("catchup") == expected_kinds, name
    finally:
        await _teardown(cron)


async def test_catch_up_honours_starting_deadline(tmp_path):
    cron = await _make_cron(tmp_path, _HOURLY)
    try:
        dagcfg = cron.cron_dags["cu"]
        sched = dagcfg.schedule_job
        sched.startingDeadlineSeconds = 3600.0
        base = datetime.datetime(2026, 1, 1, 0, 0, tzinfo=_UTC)
        now_dt = datetime.datetime(2026, 1, 1, 3, 45, tzinfo=_UTC)
        await cron._dag._create_run(dagcfg, base, "scheduled")
        await cron._dag._catch_up(dagcfg, now_dt)
        runs = await cron._dag.list_runs("cu", limit=10)
        # only 03:00 is younger than the 1h deadline window
        assert [r["kind"] for r in runs].count("catchup") == 1
    finally:
        await _teardown(cron)


async def test_catch_up_without_prior_run_is_noop(tmp_path):
    cron = await _make_cron(tmp_path, _HOURLY)
    try:
        dagcfg = cron.cron_dags["cu"]
        await cron._dag._catch_up(
            dagcfg, datetime.datetime(2026, 1, 1, tzinfo=_UTC)
        )
        assert await cron._dag.list_runs("cu", limit=10) == []
    finally:
        await _teardown(cron)


def _instant_sleep(monkeypatch):
    real_sleep = asyncio.sleep

    async def fast(_delay):
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fast)


async def test_renew_loop_retries_then_drops_on_takeover(
    tmp_path, monkeypatch
):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        ref = ("lin", "rk1")
        first = Lease("dag-run/lin/rk1", "h#1", 1, 9e18)
        fresh = Lease("dag-run/lin/rk1", "h#1", 2, 9e18)
        cron._dag._owned[ref] = first
        outcomes = [TimeoutError(), RuntimeError("blip"), fresh, None]

        async def scripted(lease, ttl):
            outcome = outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome

        monkeypatch.setattr(cron.state_backend, "renew_lease", scripted)
        _instant_sleep(monkeypatch)
        await asyncio.wait_for(cron._dag._renew_loop(ref), 5)
        # the fresh lease was adopted on the way, then the takeover dropped it
        assert ref not in cron._dag._owned
        assert outcomes == []
    finally:
        await _teardown(cron)


async def test_release_swallows_backend_errors(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        ref = ("lin", "rk1")
        cron._dag._owned[ref] = Lease("dag-run/lin/rk1", "h#1", 1, 9e18)

        async def broken(lease):
            raise RuntimeError("store on fire")

        monkeypatch.setattr(cron.state_backend, "release_lease", broken)
        await cron._dag._release(ref)
        assert ref not in cron._dag._owned
    finally:
        await _teardown(cron)


async def test_try_own_shortcuts_and_failures(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        dagcfg = cron.cron_dags["lin"]
        ref = ("lin", "rk1")
        # already owned: no store round-trip
        cron._dag._owned[ref] = Lease("dag-run/lin/rk1", "h#1", 1, 9e18)
        assert await cron._dag._try_own(dagcfg, ref) is True
        del cron._dag._owned[ref]

        async def denied(name, holder, ttl):
            return None  # a peer holds it

        monkeypatch.setattr(cron.state_backend, "acquire_lease", denied)
        assert await cron._dag._try_own(dagcfg, ref) is False

        async def hanging(name, holder, ttl):
            await asyncio.Event().wait()

        monkeypatch.setattr(cron.state_backend, "acquire_lease", hanging)
        monkeypatch.setattr(dagrun, "STATE_OP_TIMEOUT", 0.05)
        assert await cron._dag._try_own(dagcfg, ref) is False
    finally:
        await _teardown(cron)


async def test_advance_releases_run_of_removed_dag(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        ref = ("ghost", "rk1")
        cron._dag._owned[ref] = Lease("dag-run/ghost/rk1", "h#1", 1, 9e18)
        await cron._dag.advance_one(ref)
        assert ref not in cron._dag._owned
    finally:
        await _teardown(cron)


async def test_adopt_orphans_keys_pass_and_full_pass(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        _set_cmd(cron, "lin", "a", [_PY, "-c", "pass"])
        _set_cmd(cron, "lin", "b", [_PY, "-c", "pass"])
        done_key = await cron._dag.trigger_run("lin")
        await _drive(cron, "lin", done_key)  # terminal
        open_key = await cron._dag.trigger_run("lin")
        # simulate this daemon losing every hold (a restarted peer's view)
        for ref in list(cron._dag._owned):
            cron._dag._drop_owned(ref)
        backend = cron.state_backend
        dagcfg = cron.cron_dags["lin"]
        await cron._dag._adopt_one_dag(backend, "lin", dagcfg, full=False)
        assert ("lin", open_key) in cron._dag._owned
        assert done_key in cron._dag._terminal_run_keys["lin"]
        # a second keys-only pass skips both via the caches
        await cron._dag._adopt_one_dag(backend, "lin", dagcfg, full=False)
        # and the periodic full pass re-verifies from the bodies
        for ref in list(cron._dag._owned):
            cron._dag._drop_owned(ref)
        await cron._dag._adopt_one_dag(backend, "lin", dagcfg, full=True)
        assert ("lin", open_key) in cron._dag._owned
    finally:
        await _teardown(cron)


async def test_read_xcom_list_guards(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        backend = cron.state_backend
        scope = dag.xcom_scope("lin", "rid1")

        async def read(key):
            return await cron._dag._read_xcom_list("rid1", "lin", "a", key)

        # never published: no items to map
        assert await read("absent") == []
        # junk payloads definitively map to an empty fan-out
        await jobstate.artifact_put(
            backend, scope, dag.xcom_name("a", "junk"), b"not json"
        )
        assert await read("junk") == []
        await jobstate.artifact_put(
            backend, scope, dag.xcom_name("a", "object"), b'{"a": 1}'
        )
        assert await read("object") == []
        # a real list expands to itself
        await jobstate.artifact_put(
            backend, scope, dag.xcom_name("a", "items"), b"[1, 2, 3]"
        )
        assert await read("items") == [1, 2, 3]
        # a store that cannot be read leaves the fan-out unknown (None)
        _break_record_reads(
            monkeypatch, backend, jobstate.ARTIFACT_STREAM_PREFIX + scope
        )
        assert await read("items") is None
    finally:
        await _teardown(cron)


async def test_compute_wake_prefers_due_retry(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        spec = cron.cron_dags["lin"].spec
        now = 1000.0
        body = {
            "tasks": {
                "a": {"state": dag.UP_FOR_RETRY, "nextRetryAt": 1010.0},
                "b": {"state": dag.PENDING},
            }
        }
        assert cron._dag._compute_wake(spec, body, now) == 1010.0
    finally:
        await _teardown(cron)
