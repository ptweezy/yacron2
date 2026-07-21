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
    # The reaper batches DAG-task completions and records them once per run
    # after draining a batch of finished jobs; mirror that flush here (the
    # completions are only buffered until it runs).
    await cron._dag.flush_completions()
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


async def test_list_dags_caches_terminal_runs(tmp_path):
    # list_dags' rollup must serve immutable terminal runs from cache: after a
    # first pass populates it, a second pass reads no documents at all (no bulk
    # list_documents, no per-run read_document), while the rollup stays correct.
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        _set_cmd(cron, "lin", "a", [_PY, "-c", "pass"])
        _set_cmd(cron, "lin", "b", [_PY, "-c", "pass"])
        for _ in range(3):
            rk = await cron._dag.trigger_run("lin")
            body = await _drive(cron, "lin", rk)
            assert body["state"] == dag.SUCCESS

        backend = cron.state_backend
        calls = {"list_documents": 0, "read_document": 0}
        real_list = backend.list_documents
        real_read = backend.read_document

        async def counting_list(ns):
            calls["list_documents"] += 1
            return await real_list(ns)

        async def counting_read(ns, key):
            calls["read_document"] += 1
            return await real_read(ns, key)

        backend.list_documents = counting_list
        backend.read_document = counting_read

        first = await cron._dag.list_dags()
        lin = next(d for d in first if d["name"] == "lin")
        assert lin["totalRuns"] == 3
        assert lin["runCounts"] == {"success": 3}
        assert lin["latestRun"]["state"] == "success"
        # cold cache: three terminal runs read individually (below the bulk
        # threshold), no full list_documents sweep needed
        assert calls == {"list_documents": 0, "read_document": 3}

        calls["list_documents"] = 0
        calls["read_document"] = 0
        second = await cron._dag.list_dags()
        lin2 = next(d for d in second if d["name"] == "lin")
        # identical rollup, and served entirely from cache (keys listing only)
        assert lin2["totalRuns"] == 3
        assert lin2["runCounts"] == {"success": 3}
        assert lin2["latestRun"] == lin["latestRun"]
        assert calls == {"list_documents": 0, "read_document": 0}
    finally:
        await _teardown(cron)


async def test_parallel_completions_recorded_in_one_batched_rmw(
    tmp_path, monkeypatch
):
    # Three independent tasks finish together in one reaper batch; the flush
    # must record all three in a SINGLE mark_tasks_finished transform (one
    # document RMW + fsync), not one per task.
    yaml = (
        "dags:\n  - name: fan\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
        "      - id: b\n        command: 'x'\n"
        "      - id: c\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        for t in ("a", "b", "c"):
            _set_cmd(cron, "fan", t, [_PY, "-c", "pass"])
        batch_sizes = []
        real = dagrun.dag.mark_tasks_finished

        def spy(marks, now):
            batch_sizes.append(len(marks))
            return real(marks, now)

        monkeypatch.setattr(dagrun.dag, "mark_tasks_finished", spy)
        run_key = await cron._dag.trigger_run("fan")
        body = await _drive(cron, "fan", run_key)
        assert body["state"] == dag.SUCCESS
        assert all(
            body["tasks"][t]["state"] == dag.SUCCESS for t in ("a", "b", "c")
        )
        # exactly one batched completion RMW, carrying all three tasks
        assert batch_sizes == [3]
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


# --------------------------------------------------------------------------
# Advance-pass RMW economy: batched pid stamping, single-RMW quiescence
# --------------------------------------------------------------------------


def _wait_for_flag_script(flag):
    """A bounded wait-for-file command body (so a failed test cannot leak
    an immortal subprocess)."""
    return (
        "import os,sys,time\n"
        "for _ in range(600):\n"
        "    if os.path.exists(r'{}'): sys.exit(0)\n"
        "    time.sleep(0.05)\n"
        "sys.exit(1)"
    ).format(flag)


async def test_launch_batch_stamps_pids_in_one_rmw(tmp_path):
    # regression (performance): the launch loop used to run one full
    # document RMW PER launched task just to record its pid; the whole
    # batch must now land through a single set_task_pids RMW, with every
    # pid actually recorded.
    yaml = (
        "dags:\n  - name: par\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
        "      - id: b\n        command: 'x'\n"
        "      - id: c\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        flag = tmp_path / "go"
        script = _wait_for_flag_script(flag)
        for tid in ("a", "b", "c"):
            _set_cmd(cron, "par", tid, [_PY, "-c", script])
        stamped = []
        orig = cron._dag._set_pids

        async def spy(ref, stamps):
            stamped.append(list(stamps))
            await orig(ref, stamps)

        cron._dag._set_pids = spy
        run_key = await cron._dag.trigger_run("par")
        await _drain_pending(cron)
        # one batched write covered all three launches
        assert len(stamped) == 1
        assert {s[0] for s in stamped[0]} == {"a", "b", "c"}
        body = await cron._dag.get_run("par", run_key)
        for tid in ("a", "b", "c"):
            entry = body["tasks"][tid]
            assert entry["state"] == dag.RUNNING
            assert entry["proc"] == cron._proc_token
            assert isinstance(entry["pid"], int)
        cron._dag._set_pids = orig
        flag.write_text("go")
        body = await _drive(cron, "par", run_key)
        assert body["state"] == dag.SUCCESS
    finally:
        await _teardown(cron)


async def test_quiescent_advance_is_single_kept_rmw(tmp_path):
    # regression (performance): an advance of a quiescent run (its one task
    # in flight under our own proc token) used to pay a reconcile RMW plus
    # a claim RMW; the combined transform must make it ONE RMW that keeps
    # the document (no rewrite, updatedAt untouched).
    yaml = (
        "dags:\n  - name: q1\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        flag = tmp_path / "go"
        _set_cmd(cron, "q1", "a", [_PY, "-c", _wait_for_flag_script(flag)])
        run_key = await cron._dag.trigger_run("q1")
        await _drain_pending(cron)
        ref = ("q1", run_key)
        before = await cron._dag.get_run("q1", run_key)
        assert before["tasks"]["a"]["state"] == dag.RUNNING
        calls = []
        orig = cron._dag._mutate

        async def counting(dag_name, key, transform):
            calls.append((dag_name, key))
            return await orig(dag_name, key, transform)

        cron._dag._mutate = counting
        await cron._dag.advance_one(ref)
        cron._dag._mutate = orig
        assert calls == [ref]  # the whole advance was one document RMW
        after = await cron._dag.get_run("q1", run_key)
        assert after["updatedAt"] == before["updatedAt"]  # kept, not rewritten
        assert after["tasks"]["a"]["state"] == dag.RUNNING
        assert cron._dag._wake[ref] > dagrun._now()  # wake floor scheduled
        flag.write_text("go")
        body = await _drive(cron, "q1", run_key)
        assert body["state"] == dag.SUCCESS
    finally:
        await _teardown(cron)


async def test_expansion_advance_falls_back_to_two_rmws(tmp_path):
    # the combined transform cannot read XCom, so an advance that finds a
    # mapped task awaiting expansion must flag it and run the classic
    # pre-read + plan_and_claim RMW as a second step, then expand exactly
    # as before.
    yaml = (
        "dags:\n  - name: fb\n    tasks:\n"
        "      - id: gen\n        command: 'x'\n"
        "      - id: work\n        command: 'x'\n        dependsOn:\n"
        "          - gen\n        expand:\n"
        "          fromTask: gen\n          key: items\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        items_file = tmp_path / "items.json"
        items_file.write_text('["only"]')
        _set_cmd(
            cron,
            "fb",
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
        _set_cmd(cron, "fb", "work", [_PY, "-c", "pass"])
        run_key = await cron._dag.trigger_run("fb")
        ref = ("fb", run_key)
        # let gen finish and its completion land, WITHOUT the follow-up
        # advance observing it yet
        await _reap_running(cron)
        # cancel the completion's spawned auto-advance (it has not started:
        # _reap_running never yields after creating it) so the counted
        # advance below is the only one and burst coalescing cannot skew
        # the RMW tally
        pend = [
            t for t in list(cron._pending_state_writes) if not t.done()
        ]
        for t in pend:
            t.cancel()
        await asyncio.gather(*pend, return_exceptions=True)
        body = await cron._dag.get_run("fb", run_key)
        assert body["tasks"]["gen"]["state"] == dag.SUCCESS
        calls = []
        orig = cron._dag._mutate

        async def counting(dag_name, key, transform):
            calls.append((dag_name, key))
            return await orig(dag_name, key, transform)

        cron._dag._mutate = counting
        await cron._dag.advance_one(ref)
        cron._dag._mutate = orig
        body = await cron._dag.get_run("fb", run_key)
        assert body["mapped"]["work"]["items"] == ["only"]
        assert body["tasks"]["work#0"]["state"] == dag.RUNNING
        # combined RMW (flagged expansions) + claim RMW + batched pid RMW
        assert [c for c in calls if c == ref] == [ref, ref, ref]
        body = await _drive(cron, "fb", run_key)
        assert body["state"] == dag.SUCCESS, _states(body)
    finally:
        await _teardown(cron)


async def test_combined_advance_reconciles_crashed_task_inline(tmp_path):
    # a crashed foreign claim must be recovered AND re-claimed by the one
    # combined RMW (the old flow needed the reconcile RMW plus the claim
    # RMW to do the same).
    yaml = (
        "dags:\n  - name: rc\n    tasks:\n"
        "      - id: a\n        command: 'x'\n        retries: 1\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        flag = tmp_path / "go"
        _set_cmd(cron, "rc", "a", [_PY, "-c", _wait_for_flag_script(flag)])
        run_key = await cron._dag.trigger_run("rc")
        ref = ("rc", run_key)
        await _drain_pending(cron)

        # a prior daemon's claim: foreign proc token, dead pid, our host
        def _crash(body):
            entry = body["tasks"]["a"]
            entry["proc"] = "dead-daemon#deadbeef"
            entry["pid"] = 2147480000
            entry["host"] = cron._state_host
            return body, None

        ns = dag.DAG_RUN_NS_PREFIX + "rc"
        await cron.state_backend.mutate_document(ns, run_key, _crash)
        await cron._dag.advance_one(ref)
        body = await cron._dag.get_run("rc", run_key)
        entry = body["tasks"]["a"]
        assert entry["state"] == dag.RUNNING
        assert entry["proc"] == cron._proc_token  # re-claimed here
        assert entry["attempt"] == 1  # the crashed attempt was consumed
        flag.write_text("go")
        body = await _drive(cron, "rc", run_key)
        assert body["state"] == dag.SUCCESS
    finally:
        await _teardown(cron)


async def test_failed_batch_pid_write_does_not_fail_tasks(tmp_path):
    # the batched pid stamp stays best-effort, like the old per-task write:
    # a store hiccup on it must neither abort the advance nor fail the
    # already-running tasks (the claim-time proc token protects them).
    yaml = (
        "dags:\n  - name: bp\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        flag = tmp_path / "go"
        _set_cmd(cron, "bp", "a", [_PY, "-c", _wait_for_flag_script(flag)])

        async def broken(ref, stamps):
            raise RuntimeError("store on fire")

        orig = cron._dag._set_pids
        cron._dag._set_pids = broken
        run_key = await cron._dag.trigger_run("bp")
        await _drain_pending(cron)
        body = await cron._dag.get_run("bp", run_key)
        entry = body["tasks"]["a"]
        assert entry["state"] == dag.RUNNING  # launched despite the failure
        assert entry["proc"] == cron._proc_token  # still owned/protected
        assert entry["pid"] is None  # only the optimisation was lost
        cron._dag._set_pids = orig
        flag.write_text("go")
        body = await _drive(cron, "bp", run_key)
        assert body["state"] == dag.SUCCESS
    finally:
        await _teardown(cron)


async def test_one_launch_failure_does_not_skip_the_batch(tmp_path):
    # one task's launch blowing up must fail exactly that task (exit 127)
    # while the rest of the claimed batch still launches, and its pid still
    # lands through the batched stamp.
    yaml = (
        "dags:\n  - name: lf\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
        "      - id: b\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        _set_cmd(cron, "lf", "a", [_PY, "-c", "pass"])
        _set_cmd(cron, "lf", "b", [_PY, "-c", "pass"])
        orig = cron._dag._launch_task

        async def flaky(dagcfg, ref, run_id, intent):
            if intent.task_id == "a":
                raise RuntimeError("boom")
            return await orig(dagcfg, ref, run_id, intent)

        cron._dag._launch_task = flaky
        run_key = await cron._dag.trigger_run("lf")
        await _drain_pending(cron)
        body = await cron._dag.get_run("lf", run_key)
        assert body["tasks"]["a"]["state"] == dag.FAILED
        assert body["tasks"]["a"]["exitCode"] == 127
        assert body["tasks"]["a"]["failReason"] == "launch error"
        # b was still launched (and its pid recorded) despite a's failure
        assert body["tasks"]["b"]["state"] in (dag.RUNNING, dag.SUCCESS)
        cron._dag._launch_task = orig
        body = await _drive(cron, "lf", run_key)
        assert body["state"] == dag.FAILED
        assert body["tasks"]["b"]["state"] == dag.SUCCESS
    finally:
        await _teardown(cron)


async def test_subprocess_start_failure_fails_task_cleanly(
    tmp_path, monkeypatch
):
    # a launch whose start() blows up is failed explicitly with exit 127 by
    # the launch path itself, contributes NO pid stamp to the batch, and the
    # run still terminalises.
    from cronstable.job import RunningJob

    yaml = (
        "dags:\n  - name: sf\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        _set_cmd(cron, "sf", "a", [_PY, "-c", "pass"])

        async def boom(self):
            raise RuntimeError("start blew up")

        monkeypatch.setattr(RunningJob, "start", boom)
        run_key = await cron._dag.trigger_run("sf")
        monkeypatch.undo()
        await _drain_pending(cron)
        body = await _drive(cron, "sf", run_key)
        assert body["state"] == dag.FAILED
        entry = body["tasks"]["a"]
        assert entry["state"] == dag.FAILED
        assert entry["exitCode"] == 127
        assert entry["failReason"] == "launch failed"
        assert entry["pid"] is None
    finally:
        await _teardown(cron)


async def test_advance_of_missing_document_releases_ownership(tmp_path):
    # the combined RMW observing NO document (a GC'd or never-created run)
    # must release the ref, exactly like the old reconcile step did.
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        ref = ("lin", "manual-ghost")
        cron._dag._owned[ref] = Lease(
            "dagadvance/lin/manual-ghost", "h#1", 1, 9e18
        )
        cron._dag._locks.setdefault(ref, asyncio.Lock())
        await cron._dag.advance_one(ref)
        assert ref not in cron._dag._owned
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


# ===========================================================================
# XCom read-side helpers and list_dags run rollups: driven directly against a
# real backend (no task subprocesses, no scheduler loop). Run documents are
# minted with _create_doc and XCom records seeded through jobstate.artifact_*,
# then xcom_for_run / _read_xcom_list / the _bulk_rollup + _dag_run_rollup pair
# are called and their merged/aggregated results asserted.
# ===========================================================================

_XC_YAML = (
    "dags:\n  - name: xc\n    tasks:\n"
    "      - id: a\n        command: 'x'\n"
    "      - id: b\n        command: 'x'\n"
)


async def _mint_run(cron, run_key):
    """Create a bare (running) run document and return its runId."""
    dagcfg = cron.cron_dags["xc"]
    created = await cron._dag._create_doc(dagcfg, run_key, None, "manual")
    assert created
    body = await cron._dag.get_run("xc", run_key)
    return body["runId"]


# --------------------------------------------------------------------------
# xcom_for_run: assemble the dashboard's flat XCom list from the artifact store
# --------------------------------------------------------------------------


async def test_xcom_for_run_inlines_and_flags_values(tmp_path):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        run_id = await _mint_run(cron, "r1")
        scope = dag.xcom_scope("xc", str(run_id))
        await jobstate.artifact_put(cron.state_backend, scope, "a/small", b"hi")
        await jobstate.artifact_put(
            cron.state_backend, scope, "b/big", b"0123456789"
        )
        await jobstate.artifact_put(
            cron.state_backend, scope, "c/bin", b"\xff\xfe"
        )

        # max_value_bytes 4: "hi" inlines, the 10-byte value is oversize, and
        # the 2-byte non-UTF-8 value is fetched but flagged binary.
        res = await cron._dag.xcom_for_run(
            "xc", "r1", max_value_bytes=4
        )
        assert res["dag"] == "xc"
        assert res["runKey"] == "r1"
        assert res["runId"] == run_id
        assert res["truncated"] is False

        by = {(e["taskkey"], e["key"]): e for e in res["entries"]}
        assert by[("a", "small")]["value"] == "hi"
        assert by[("a", "small")]["size"] == 2
        assert by[("b", "big")].get("oversize") is True
        assert "value" not in by[("b", "big")]
        assert by[("c", "bin")].get("binary") is True
        assert "value" not in by[("c", "bin")]
        # every entry carries the record's digest through
        assert all(e["sha256"] for e in res["entries"])
    finally:
        await _teardown(cron)


async def test_xcom_for_run_truncates_beyond_max_entries(tmp_path):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        run_id = await _mint_run(cron, "r1")
        scope = dag.xcom_scope("xc", str(run_id))
        for i in range(3):
            await jobstate.artifact_put(
                cron.state_backend, scope, "t/k{}".format(i), b"v"
            )
        res = await cron._dag.xcom_for_run("xc", "r1", max_entries=1)
        assert res["truncated"] is True
        assert len(res["entries"]) == 1
    finally:
        await _teardown(cron)


async def test_xcom_for_run_unknown_dag_or_run_returns_none(tmp_path):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        await _mint_run(cron, "r1")
        # unknown dag: not in the configured set
        assert await cron._dag.xcom_for_run("ghost", "r1") is None
        # known dag, unknown run key: no document
        assert await cron._dag.xcom_for_run("xc", "nope") is None
    finally:
        await _teardown(cron)


async def test_xcom_for_run_without_run_id_returns_empty(tmp_path):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        await _mint_run(cron, "r1")
        ns = dag.DAG_RUN_NS_PREFIX + "xc"

        def _strip(body):
            body["runId"] = ""
            return body, None

        await cron.state_backend.mutate_document(ns, "r1", _strip)
        # a run with no runId cannot address an XCom scope: empty, not an error
        res = await cron._dag.xcom_for_run("xc", "r1")
        assert res["runId"] == ""
        assert res["entries"] == []
        assert res["truncated"] is False
    finally:
        await _teardown(cron)


async def test_xcom_for_run_backend_hiccup_degrades(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        run_id = await _mint_run(cron, "r1")
        scope = dag.xcom_scope("xc", str(run_id))
        await jobstate.artifact_put(cron.state_backend, scope, "a/k", b"hi")

        async def _boom(*a, **k):
            raise OSError("store hiccup")

        monkeypatch.setattr(jobstate, "artifact_list", _boom)
        # a store blip listing the stream degrades to an empty result rather
        # than 500-ing the XCom tab.
        res = await cron._dag.xcom_for_run("xc", "r1")
        assert res["runId"] == run_id
        assert res["entries"] == []
        assert res["truncated"] is False
    finally:
        await _teardown(cron)


async def test_xcom_for_run_swept_blob_has_no_value(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        run_id = await _mint_run(cron, "r1")
        scope = dag.xcom_scope("xc", str(run_id))
        await jobstate.artifact_put(cron.state_backend, scope, "a/k", b"hi")

        async def _unreadable(digest):
            raise OSError("blob read failed")  # swept / unreadable payload

        monkeypatch.setattr(cron.state_backend, "get_blob", _unreadable)
        # a small-enough record whose blob is unreadable reads back as no value
        # at all (neither inlined nor flagged binary), skipped like any other
        # unreadable value rather than failing the tab.
        res = await cron._dag.xcom_for_run("xc", "r1")
        entry = res["entries"][0]
        assert entry["taskkey"] == "a" and entry["key"] == "k"
        assert "value" not in entry
        assert "binary" not in entry
    finally:
        await _teardown(cron)


# --------------------------------------------------------------------------
# _read_xcom_list: the list a mapped task fans out over, with strict absence
# --------------------------------------------------------------------------


async def test_read_xcom_list_parses_published_list(tmp_path):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        scope = dag.xcom_scope("xc", "run-x")
        await jobstate.artifact_put(
            cron.state_backend, scope, dag.xcom_name("gen", "items"),
            b'["a", "b", "c"]',
        )
        got = await cron._dag._read_xcom_list("run-x", "xc", "gen", "items")
        assert got == ["a", "b", "c"]
    finally:
        await _teardown(cron)


async def test_read_xcom_list_absent_key_is_empty(tmp_path):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        # upstream succeeded but never published this key -> no items to map
        got = await cron._dag._read_xcom_list("run-x", "xc", "gen", "missing")
        assert got == []
    finally:
        await _teardown(cron)


async def test_read_xcom_list_invalid_json_is_empty(tmp_path):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        scope = dag.xcom_scope("xc", "run-x")
        await jobstate.artifact_put(
            cron.state_backend, scope, dag.xcom_name("gen", "items"),
            b"this is not json",
        )
        got = await cron._dag._read_xcom_list("run-x", "xc", "gen", "items")
        assert got == []
    finally:
        await _teardown(cron)


async def test_read_xcom_list_non_list_json_is_empty(tmp_path):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        scope = dag.xcom_scope("xc", "run-x")
        await jobstate.artifact_put(
            cron.state_backend, scope, dag.xcom_name("gen", "items"),
            b'{"a": 1}',
        )
        got = await cron._dag._read_xcom_list("run-x", "xc", "gen", "items")
        assert got == []
    finally:
        await _teardown(cron)


async def test_read_xcom_list_oversized_blob_is_empty(tmp_path, monkeypatch):
    # A fan-out blob larger than MAX_MAPPED_XCOM_BYTES is refused BEFORE the
    # blob is fetched/decoded (the OOM guard) and mapped to empty, like the
    # other definitively-unusable outputs. Shrink the ceiling so a tiny blob
    # trips it without materialising megabytes in the test.
    monkeypatch.setattr(dag, "MAX_MAPPED_XCOM_BYTES", 4)
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        scope = dag.xcom_scope("xc", "run-x")
        await jobstate.artifact_put(
            cron.state_backend, scope, dag.xcom_name("gen", "items"),
            b'["a", "b", "c"]',  # 15 bytes > the 4-byte ceiling
        )
        got = await cron._dag._read_xcom_list("run-x", "xc", "gen", "items")
        assert got == []
    finally:
        await _teardown(cron)


async def test_read_xcom_list_over_item_cap_returns_unchanged(
    tmp_path, monkeypatch
):
    # Past MAX_MAPPED_ITEMS the list is returned as-is for _apply_expansions to
    # fail the task; _read_xcom_list must skip the O(len) portability walk here
    # rather than map to empty. Shrink the cap so a short list trips it.
    monkeypatch.setattr(dag, "MAX_MAPPED_ITEMS", 3)
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        scope = dag.xcom_scope("xc", "run-x")
        await jobstate.artifact_put(
            cron.state_backend, scope, dag.xcom_name("gen", "items"),
            b"[1, 2, 3, 4, 5]",  # 5 > 3
        )
        got = await cron._dag._read_xcom_list("run-x", "xc", "gen", "items")
        assert got == [1, 2, 3, 4, 5]
    finally:
        await _teardown(cron)


async def test_read_xcom_list_timeout_is_none(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        async def _slow(*a, **k):
            raise asyncio.TimeoutError()

        monkeypatch.setattr(jobstate, "artifact_get", _slow)
        # a transient store timeout must NOT read as "published nothing": it
        # stays unknown (None) so the mapped task retries on a later pass.
        got = await cron._dag._read_xcom_list("run-x", "xc", "gen", "items")
        assert got is None
    finally:
        await _teardown(cron)


async def test_read_xcom_list_missing_blob_is_empty(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        async def _gone(*a, **k):
            raise jobstate.JobStateError("blob missing", status=410)

        monkeypatch.setattr(jobstate, "artifact_get", _gone)
        # the record survives but its payload blob is gone (410): definitively
        # unrecoverable -> empty fan-out, not an infinite retry.
        got = await cron._dag._read_xcom_list("run-x", "xc", "gen", "items")
        assert got == []
    finally:
        await _teardown(cron)


async def test_read_xcom_list_store_error_is_none(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        async def _err(*a, **k):
            raise RuntimeError("unreadable record")

        monkeypatch.setattr(jobstate, "artifact_get", _err)
        # an I/O error / a record only a newer node understands leaves the
        # fan-out unknown (None), never empty.
        got = await cron._dag._read_xcom_list("run-x", "xc", "gen", "items")
        assert got is None
    finally:
        await _teardown(cron)


async def test_read_xcom_list_non_portable_value_is_empty(
    tmp_path, monkeypatch
):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        scope = dag.xcom_scope("xc", "run-x")
        await jobstate.artifact_put(
            cron.state_backend, scope, dag.xcom_name("gen", "items"),
            b'["a", "b"]',
        )

        # Force the portability check to reject the parsed list, standing in
        # for an out-of-64-bit-window int / non-finite float that the stdlib
        # accepts but orjson cannot round-trip. (Such values cannot be
        # produced as literal bytes on an orjson host, so the branch is driven
        # by making the collaborating check raise.)
        def _reject(_value):
            raise dagrun._json.UnsupportedValue("not fleet-portable")

        monkeypatch.setattr(dagrun._json, "ensure_portable", _reject)
        got = await cron._dag._read_xcom_list("run-x", "xc", "gen", "items")
        assert got == []  # mapped to empty rather than wedging every advance
    finally:
        await _teardown(cron)


# --------------------------------------------------------------------------
# list_dags run rollups: _bulk_rollup (full sweep) + _dag_run_rollup (cached)
# --------------------------------------------------------------------------


async def test_bulk_rollup_builds_cache_and_rollup(tmp_path):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        await _mint_run(cron, "r1")
        await _mint_run(cron, "r2")
        ns = cron._dag._ns("xc")
        roll = await cron._dag._bulk_rollup(cron.state_backend, ns, "xc")
        assert roll["totalRuns"] == 2
        assert roll["runCounts"] == {"running": 2}
        assert roll["latestRun"]["runKey"] in {"r1", "r2"}
        # the sweep also (re)builds the per-dag summary cache keyed by run key
        cache = cron._dag._dag_summary_cache["xc"]
        assert set(cache) == {"r1", "r2"}
    finally:
        await _teardown(cron)


async def test_dag_run_rollup_reads_and_caches(tmp_path):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        await _mint_run(cron, "r1")
        await _mint_run(cron, "r2")
        roll = await cron._dag._dag_run_rollup(cron.state_backend, "xc")
        assert roll["totalRuns"] == 2
        assert roll["runCounts"] == {"running": 2}
        assert set(cron._dag._dag_summary_cache["xc"]) == {"r1", "r2"}
    finally:
        await _teardown(cron)


async def test_dag_run_rollup_serves_terminal_from_cache(
    tmp_path, monkeypatch
):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        await _mint_run(cron, "r1")
        # cold pass reads and caches the one run
        await cron._dag._dag_run_rollup(cron.state_backend, "xc")
        # mark it terminal in the cache so the next pass may skip the re-read
        cron._dag._dag_summary_cache["xc"]["r1"]["terminal"] = True

        reads = {"n": 0}
        real = cron.state_backend.read_document

        async def counting(ns, key):
            reads["n"] += 1
            return await real(ns, key)

        monkeypatch.setattr(cron.state_backend, "read_document", counting)
        roll = await cron._dag._dag_run_rollup(cron.state_backend, "xc")
        assert roll["totalRuns"] == 1
        assert reads["n"] == 0  # terminal run served entirely from cache
    finally:
        await _teardown(cron)


async def test_dag_run_rollup_keys_none_falls_back_to_bulk(
    tmp_path, monkeypatch
):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        await _mint_run(cron, "r1")

        async def _no_keys(ns):
            return None

        monkeypatch.setattr(
            cron.state_backend, "list_document_keys", _no_keys
        )
        # a backend that cannot list keys only degrades to a full parse
        roll = await cron._dag._dag_run_rollup(cron.state_backend, "xc")
        assert roll["totalRuns"] == 1
        assert roll["runCounts"] == {"running": 1}
    finally:
        await _teardown(cron)


async def test_dag_run_rollup_threshold_falls_back_to_bulk(
    tmp_path, monkeypatch
):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        await _mint_run(cron, "r1")
        await _mint_run(cron, "r2")
        # a large read delta (more than the threshold) takes the single bulk
        # sweep rather than N per-run reads
        monkeypatch.setattr(dagrun, "DAG_ROLLUP_BULK_THRESHOLD", 0)
        roll = await cron._dag._dag_run_rollup(cron.state_backend, "xc")
        assert roll["totalRuns"] == 2
        assert set(cron._dag._dag_summary_cache["xc"]) == {"r1", "r2"}
    finally:
        await _teardown(cron)


async def test_dag_run_rollup_degrades_on_keys_error(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        await _mint_run(cron, "r1")

        async def _boom(ns):
            raise OSError("keys listing failed")

        monkeypatch.setattr(cron.state_backend, "list_document_keys", _boom)
        # a hiccup omits the rollup (None) rather than failing /dags
        assert await cron._dag._dag_run_rollup(cron.state_backend, "xc") is None
    finally:
        await _teardown(cron)


async def test_bulk_rollup_degrades_on_error(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        await _mint_run(cron, "r1")

        async def _boom(ns):
            raise OSError("list failed")

        monkeypatch.setattr(cron.state_backend, "list_documents", _boom)
        ns = cron._dag._ns("xc")
        assert await cron._dag._bulk_rollup(cron.state_backend, ns, "xc") is None
    finally:
        await _teardown(cron)


async def test_dag_run_rollup_prunes_gone_cache_keys(tmp_path):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        await _mint_run(cron, "r1")
        # a stale cache entry for a run that no longer exists must be dropped
        cron._dag._dag_summary_cache["xc"] = {
            "stale": {
                "runKey": "stale",
                "state": "success",
                "kind": "manual",
                "createdAt": 1.0,
                "updatedAt": 1.0,
                "terminal": True,
            }
        }
        roll = await cron._dag._dag_run_rollup(cron.state_backend, "xc")
        assert "stale" not in cron._dag._dag_summary_cache["xc"]
        assert roll["totalRuns"] == 1
        assert roll["latestRun"]["runKey"] == "r1"
    finally:
        await _teardown(cron)


async def test_dag_run_rollup_degrades_on_read_error(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        await _mint_run(cron, "r1")

        async def _boom(ns, key):
            raise OSError("read failed")

        monkeypatch.setattr(cron.state_backend, "read_document", _boom)
        # a per-run read that errors omits the rollup (None), not a 500
        assert await cron._dag._dag_run_rollup(cron.state_backend, "xc") is None
    finally:
        await _teardown(cron)


async def test_dag_run_rollup_missing_body_is_skipped(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        await _mint_run(cron, "r1")

        async def _gone(ns, key):
            return None  # deleted between the listing and the read

        monkeypatch.setattr(cron.state_backend, "read_document", _gone)
        # a key that vanished after the listing is dropped, not fatal: with
        # nothing readable the rollup is empty
        roll = await cron._dag._dag_run_rollup(cron.state_backend, "xc")
        assert roll == {}
        assert "r1" not in cron._dag._dag_summary_cache["xc"]
    finally:
        await _teardown(cron)


# --------------------------------------------------------------------------
# forget() on a backend swap: every per-store cache is dropped, so /dags stops
# serving the OLD store's finished run for the NEW store's live one.
# --------------------------------------------------------------------------


async def test_forget_clears_every_per_store_cache(tmp_path):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        await _mint_run(cron, "r1")
        await cron._dag._dag_run_rollup(cron.state_backend, "xc")
        assert cron._dag._dag_summary_cache["xc"]

        d = cron._dag
        ref = ("xc", "r1")
        # every attribute scoped to the CURRENT store, dirtied by hand so
        # forget() has something to drop in each one. This is the cheap guard
        # for the next attribute added to the class and forgotten here; the
        # user-visible consequence of missing one is the test below.
        d._owned[ref] = Lease(
            name="l", holder="h", fence=1, expires_at=dagrun._now() + 3600.0
        )
        renewer = asyncio.ensure_future(asyncio.sleep(3600))
        d._renewers[ref] = renewer
        d._locks[ref] = asyncio.Lock()
        d._wake[ref] = 1.0
        d._next_logical["xc"] = _utcnow()
        d._seeded["xc"] = "sig"
        d._seed_failed["xc"] = "sig"
        d._pending_completions[(ref, "a")] = {}
        d._completion_buffer[ref] = [{"taskId": "a"}]
        d._terminal_run_keys["xc"] = {"r1"}
        d._advance_again.add(ref)
        # ALL four cadence fields are pushed into the future, not just the
        # full-adopt one: they default to 0.0, so asserting they are 0.0 after
        # forget() without dirtying them first passes whether or not forget()
        # touches them.
        d._next_full_adopt = dagrun._now() + 3600.0
        d._next_adopt = dagrun._now() + 3600.0
        d._next_sched_check = dagrun._now() + 3600.0
        d._next_gc = dagrun._now() + 3600.0

        d.forget()

        assert not d._owned
        assert not d._renewers
        assert not d._locks
        assert not d._wake
        assert not d._next_logical
        assert not d._seeded
        assert not d._seed_failed
        assert not d._pending_completions
        assert not d._completion_buffer
        assert not d._terminal_run_keys
        assert not d._dag_summary_cache
        assert not d._advance_again
        # every cadence is brought forward to now: the next adopt pass has to
        # be a FULL one (that is what rebuilds _terminal_run_keys from the new
        # store's bodies).
        assert d._next_full_adopt == 0.0
        assert d._next_adopt == 0.0
        assert d._next_sched_check == 0.0
        assert d._next_gc == 0.0
        # renewing a lease against a store that is gone is pointless
        with pytest.raises(asyncio.CancelledError):
            await renewer
    finally:
        await _teardown(cron)


async def test_forget_drops_stale_terminal_summary_across_backend_swap(
    tmp_path,
):
    store_a = tmp_path / "store-a"
    store_b = tmp_path / "store-b"
    store_a.mkdir()
    store_b.mkdir()
    cron = await _make_cron(store_a, _LINEAR)
    try:
        _set_cmd(cron, "lin", "a", [_PY, "-c", "pass"])
        _set_cmd(cron, "lin", "b", [_PY, "-c", "pass"])

        # store A: one run driven to SUCCESS, then rolled up so its summary is
        # cached. A terminal summary is immutable, so nothing ever re-reads it.
        run_key = await cron._dag.trigger_run("lin")
        assert (await _drive(cron, "lin", run_key))["state"] == dag.SUCCESS
        roll = await cron._dag._dag_run_rollup(cron.state_backend, "lin")
        assert roll["runCounts"] == {dag.SUCCESS: 1}
        assert cron._dag._dag_summary_cache["lin"][run_key]["terminal"]
        assert run_key in cron._dag._terminal_run_keys["lin"]

        # store B: a PEER node creates a live run under the SAME key (run keys
        # are deterministic: dag name + logical date), so it collides with what
        # store A left in this node's caches. It has to be a peer, since this
        # node's own _create_doc evicts the key from both caches on the way in.
        peer = await _make_cron(store_b, _LINEAR)
        try:
            assert await peer._dag._create_doc(
                peer.cron_dags["lin"], run_key, None, "manual"
            )
        finally:
            await _teardown(peer)

        # dirty the remaining per-store bookkeeping so the swap has something
        # to clear in each of them.
        ref = ("lin", run_key)
        cron._dag._completion_buffer[ref] = [{"taskId": "a"}]
        cron._dag._advance_again.add(ref)
        cron._dag._pending_completions[(ref, "a")] = {}
        cron._dag._next_full_adopt = dagrun._now() + 3600.0

        # the swap: a different path is a different store config, which is what
        # drives Cron.start_stop_state through _dag.forget().
        swap = "state:\n  path: {}\n".format(store_b) + _LINEAR
        await cron.start_stop_state(_state_cfg(swap))
        assert cron.state_backend is not None

        # the user-visible symptom, asserted first because it is the whole
        # point: /dags must report the new store's RUNNING run, not the old
        # store's finished one. With the summary cache left standing this key
        # is skipped as "terminal, immutable" and never re-read, so the stale
        # success is served indefinitely (nothing rebuilds that cache on a
        # timer, unlike _terminal_run_keys, which the full adopt pass heals).
        live = await cron._dag.get_run("lin", run_key)
        assert live["state"] == dag.RUNNING  # the peer's run, in store B
        roll = await cron._dag._dag_run_rollup(cron.state_backend, "lin")
        assert roll["runCounts"] == {dag.RUNNING: 1}
        assert roll["latestRun"]["state"] == dag.RUNNING

        # and nothing else the old store populated survived. (Ownership and
        # the wake/lock maps are NOT asserted empty here: the swap ends in
        # _rehydrate_from_state, which legitimately re-adopts the new store's
        # live run. test_forget_clears_every_per_store_cache pins those.)
        d = cron._dag
        assert run_key not in d._terminal_run_keys.get("lin", set())
        assert not d._completion_buffer
        assert not d._pending_completions
        assert not d._advance_again

        await _drive(cron, "lin", run_key)  # reap the re-adopted launch
    finally:
        await _teardown(cron)


# ===========================================================================
# DagScheduler lifecycle plumbing: scheduled firing, orphan adoption,
# advance-lease usability, task-run preparation, completion flushing, boot
# reconciliation, and removed-dag GC. The individual lifecycle methods are
# called directly so their store-error / reload / takeover branches run
# deterministically -- no wall-clock races, no real network.
# ===========================================================================


# --------------------------------------------------------------------------
# _fire_scheduled: fires due seeded dags, skips unseeded / unscheduled ones,
# and isolates a per-dag firing failure.
# --------------------------------------------------------------------------


async def test_fire_scheduled_fires_due_skips_unseeded_and_isolates_errors(
    tmp_path, monkeypatch
):
    yaml = (
        "dags:\n"
        "  - name: s1\n    schedule: '* * * * *'\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
        "  - name: s2\n    schedule: '* * * * *'\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
        "  - name: plain\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        _set_cmd(cron, "s1", "a", [_PY, "-c", "pass"])
        _set_cmd(cron, "s2", "a", [_PY, "-c", "pass"])
        due = _utcnow() - datetime.timedelta(seconds=90)

        # s1 is seeded and due -> a scheduled run is created.
        cron._dag._seeded["s1"] = cron._dag._sched_sig(cron.cron_dags["s1"])
        cron._dag._next_logical["s1"] = due
        # s2 is due but NOT seeded -> skipped (waits for the seed cadence).
        cron._dag._next_logical["s2"] = due
        # `plain` has no schedule -> skipped (schedule_job is None).

        await cron._dag._fire_scheduled(dagrun._now())

        s1_runs = await cron._dag.list_runs("s1")
        assert len(s1_runs) >= 1
        assert all(r["kind"] == "scheduled" for r in s1_runs)
        assert await cron._dag.list_runs("s2") == []  # unseeded: never fired
        assert await cron._dag.list_runs("plain") == []  # unscheduled

        # One dag's firing raising must NOT abort the whole pass: seed s2 and
        # make s1's _fire_forward blow up; s2 still fires.
        cron._dag._seeded["s2"] = cron._dag._sched_sig(cron.cron_dags["s2"])
        cron._dag._next_logical["s2"] = due
        real_fire = cron._dag._fire_forward

        async def flaky(dagcfg, now_dt):
            if dagcfg.name == "s1":
                raise RuntimeError("firing s1 blew up")
            return await real_fire(dagcfg, now_dt)

        monkeypatch.setattr(cron._dag, "_fire_forward", flaky)
        await cron._dag._fire_scheduled(dagrun._now())  # must not raise
        monkeypatch.undo()
        assert len(await cron._dag.list_runs("s2")) >= 1  # healthy dag fired

        # Reap every launched task so the run docs settle and nothing leaks.
        for name in ("s1", "s2"):
            for r in await cron._dag.list_runs(name):
                await _drive(cron, name, r["runKey"])
    finally:
        await _teardown(cron)


# --------------------------------------------------------------------------
# _adopt_one_dag: the incremental (keys-only) pass owns an orphaned active run
# and caches a terminal one; the full pass rebuilds the terminal cache.
# --------------------------------------------------------------------------


async def test_adopt_one_dag_keys_only_then_full(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        _set_cmd(cron, "lin", "a", [_PY, "-c", "pass"])
        _set_cmd(cron, "lin", "b", [_PY, "-c", "pass"])
        dagcfg = cron.cron_dags["lin"]
        backend = cron.state_backend

        # one terminal run, plus an active run created directly (unowned, no
        # subprocess launched).
        term_key = await cron._dag.trigger_run("lin")
        assert (await _drive(cron, "lin", term_key))["state"] == dag.SUCCESS
        assert await cron._dag._create_doc(dagcfg, "full-active", None, "manual")
        full_ref = ("lin", "full-active")

        # a fresh scan starts with no ownership and no terminal cache.
        cron._dag.forget()
        cron._dag._terminal_run_keys.pop("lin", None)

        # FULL pass: parses every body, claims the active run, and rebuilds the
        # terminal cache from truth.
        await cron._dag._adopt_one_dag(backend, "lin", dagcfg, full=True)
        assert full_ref in cron._dag._owned  # active run claimed
        assert cron._dag._terminal_run_keys["lin"] == {term_key}

        # drive the claimed run to completion, then a second active run drives
        # the incremental (keys-only) pass.
        await _drive(cron, "lin", "full-active")
        assert await cron._dag._create_doc(dagcfg, "keys-active", None, "manual")
        keys_ref = ("lin", "keys-active")
        cron._dag.forget()
        cron._dag._terminal_run_keys.pop("lin", None)

        # keys-only pass: reads only the not-known-terminal bodies, owns the
        # active run, and records the terminal runs in the cache.
        await cron._dag._adopt_one_dag(backend, "lin", dagcfg, full=False)
        assert keys_ref in cron._dag._owned
        assert cron._dag._terminal_run_keys["lin"] == {term_key, "full-active"}
        assert "keys-active" not in cron._dag._terminal_run_keys["lin"]
        await _drive(cron, "lin", "keys-active")  # reap the adopted launch
    finally:
        await _teardown(cron)


# --------------------------------------------------------------------------
# _lease_usable: live -> usable; expired + store down -> fail closed (kept);
# expired but still ours -> fail closed (kept); expired + peer took over ->
# ownership dropped.
# --------------------------------------------------------------------------


async def test_lease_usable_live_stale_and_taken_over(tmp_path, monkeypatch):
    yaml = (
        "dags:\n  - name: lu\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        dagcfg = cron.cron_dags["lu"]
        run_key = "manual-lu"
        assert await cron._dag._create_doc(dagcfg, run_key, None, "manual")
        ref = ("lu", run_key)
        lease_name = cron._dag._lease_name(ref)
        lease = await cron.state_backend.acquire_lease(
            lease_name, cron._slot_holder(), 30.0
        )
        cron._dag._owned[ref] = lease

        # 1. a live (unexpired) lease gates the run.
        assert await cron._dag._lease_usable(ref, lease) is True

        # 2. expired + store unreachable: unverifiable -> fail closed, but
        # ownership is retained (the renew loop can still recover it).
        lease.expires_at = dagrun._now() - 5.0
        backend = cron.state_backend
        cron.state_backend = None
        assert await cron._dag._lease_usable(ref, lease) is False
        cron.state_backend = backend
        assert ref in cron._dag._owned

        # 3. expired but the store still shows OUR holder+fence (untaken):
        # fail closed, ownership retained.
        assert await cron._dag._lease_usable(ref, lease) is False
        assert ref in cron._dag._owned

        # 3b. expired and the store cannot answer (read_lease raises):
        # unverifiable -> fail closed, ownership retained.
        async def _boom_read(name):
            raise RuntimeError("store unreachable")

        monkeypatch.setattr(cron.state_backend, "read_lease", _boom_read)
        assert await cron._dag._lease_usable(ref, lease) is False
        assert ref in cron._dag._owned
        monkeypatch.undo()

        # 4. expired AND a peer took the lease over (fence bumped): positively
        # superseded -> drop ownership here.
        await cron.state_backend.release_lease(lease)
        peer = await cron.state_backend.acquire_lease(
            lease_name, "peer-node", 30.0
        )
        assert peer.fence == lease.fence + 1
        assert await cron._dag._lease_usable(ref, lease) is False
        assert ref not in cron._dag._owned  # dropped: at-least-once handoff
    finally:
        await _teardown(cron)


# --------------------------------------------------------------------------
# _prepare_task_run: the no-jobApi early return, and the secret-resolution loop
# (a good secret is staged; a broken one is skipped, not fatal).
# --------------------------------------------------------------------------


async def test_prepare_task_run_env_and_secrets(tmp_path):
    missing = tmp_path / "nope.secret"  # never created -> fromFile raises
    yaml = (
        "dags:\n  - name: sec\n    tasks:\n"
        "      - id: a\n        command: 'x'\n        secrets:\n"
        "          - name: GOOD\n            value: sval\n"
        "          - name: BAD\n            fromFile: {}\n".format(missing)
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        dagcfg = cron.cron_dags["sec"]
        template = dagcfg.task_templates["a"]
        intent = dag.LaunchIntent(
            task_id="a",
            taskkey="a",
            map_index=None,
            map_item=None,
            attempt=0,
            is_sensor=False,
            poke_number=0,
        )
        run_id = "0123456789abcdef"

        # With the loopback API down there is no run to register: the DAG env
        # is still returned, token is None.
        api = cron._job_api
        cron._job_api = None
        token, env = cron._dag._prepare_task_run(
            dagcfg, run_id, "manual-1", intent, template
        )
        assert token is None
        assert env[dag.ENV_DAG_NAME] == "sec"
        assert env[dag.ENV_DAG_TASK] == "a"
        assert env[dag.ENV_DAG_RUN_ID] == run_id
        cron._job_api = api

        # With the API up, the secret loop stages GOOD and skips the broken
        # BAD (its fromFile cannot be read) rather than failing the launch.
        token, env = cron._dag._prepare_task_run(
            dagcfg, run_id, "manual-1", intent, template
        )
        assert token is not None
        ctx = cron._job_api._runs[token]
        assert ctx.secrets == {"GOOD": "sval"}  # BAD dropped, not fatal
        assert env[dag.ENV_DAG_NAME] == "sec"
        await cron._job_api.finish_run(token)  # unregister; no leak
    finally:
        await _teardown(cron)


# --------------------------------------------------------------------------
# flush_completions: a run whose flush raises has its WHOLE batch re-queued for
# retry (never dropped), and one run's failure never loses another's.
# --------------------------------------------------------------------------


async def test_flush_completions_requeues_batch_on_failure(
    tmp_path, monkeypatch
):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        ref = ("lin", "manual-fc")
        cron._dag._completion_buffer[ref] = [
            {
                "taskkey": "a",
                "taskId": "a",
                "success": True,
                "exitCode": 0,
                "failReason": None,
                "proc": "tok",
                "attempt": 0,
                "poke": None,
                "resources": None,
            }
        ]

        async def _boom(r, entries):
            raise RuntimeError("store stall on flush")

        monkeypatch.setattr(cron._dag, "_flush_run_completions", _boom)
        await cron._dag.flush_completions()  # swallows + re-queues

        assert (ref, "a") in cron._dag._pending_completions  # queued, not lost
        assert cron._dag._completion_buffer == {}  # buffer was consumed
    finally:
        await _teardown(cron)


# --------------------------------------------------------------------------
# _flush_run_completions: a completion for a task the reload removed is dropped
# (and its queued retry purged); a completion for a whole removed dag drops the
# whole batch.
# --------------------------------------------------------------------------


async def test_flush_run_completions_drops_removed_task_and_dag(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        dagcfg = cron.cron_dags["lin"]
        run_key = "manual-drop"
        assert await cron._dag._create_doc(dagcfg, run_key, None, "manual")
        ref = ("lin", run_key)

        def _entry(taskkey, task_id):
            return {
                "taskkey": taskkey,
                "taskId": task_id,
                "success": True,
                "exitCode": 0,
                "failReason": None,
                "proc": "tok",
                "attempt": 0,
                "poke": None,
                "resources": None,
            }

        # a completion for a task the DAG no longer defines: dropped, along with
        # any queued retry of it (which would otherwise re-run forever). Nothing
        # to record -> no advance, no crash.
        cron._dag._queue_completion(
            ref, "ghost", "ghost",
            success=True, exit_code=0, fail_reason=None,
            proc="tok", attempt=0, poke=None,
        )
        assert (ref, "ghost") in cron._dag._pending_completions
        await cron._dag._flush_run_completions(ref, [_entry("ghost", "ghost")])
        assert (ref, "ghost") not in cron._dag._pending_completions

        # a completion for a dag the reload removed entirely: the whole batch's
        # queued retries are purged and nothing is recorded.
        cron._dag._queue_completion(
            ref, "a", "a",
            success=True, exit_code=0, fail_reason=None,
            proc="tok", attempt=0, poke=None,
        )
        assert (ref, "a") in cron._dag._pending_completions
        del cron.cron_dags["lin"]
        await cron._dag._flush_run_completions(ref, [_entry("a", "a")])
        assert (ref, "a") not in cron._dag._pending_completions
    finally:
        await _teardown(cron)


# --------------------------------------------------------------------------
# _flush_run_completions: a failed RMW re-queues every live entry for retry; a
# fenced-out (stale/duplicate) completion is settled, not retried forever.
# --------------------------------------------------------------------------


async def test_flush_run_completions_requeues_on_stall_and_settles_stale(
    tmp_path,
):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        dagcfg = cron.cron_dags["lin"]
        run_key = "manual-live"
        assert await cron._dag._create_doc(dagcfg, run_key, None, "manual")
        ref = ("lin", run_key)
        entry = {
            "taskkey": "a",
            "taskId": "a",
            "success": True,
            "exitCode": 0,
            "failReason": None,
            "proc": "tok",
            "attempt": 0,
            "poke": None,
            "resources": None,
        }

        # the recording RMW stalls: the live entry is queued for retry, never
        # dropped (unrecorded, it would stay RUNNING under our proc forever).
        real_mutate = cron._dag._mutate

        async def _stalled(name, key, transform):
            raise asyncio.TimeoutError()

        cron._dag._mutate = _stalled
        await cron._dag._flush_run_completions(ref, [entry])
        cron._dag._mutate = real_mutate
        assert (ref, "a") in cron._dag._pending_completions
        await _drain_pending(cron)  # let the trailing advance settle

        # the RMW now succeeds, but task `a` was never claimed (still PENDING,
        # proc None): the completion's proc="tok" fails the fence, so nothing is
        # applied and the settled queue entry is popped (not retried forever).
        cron._dag._pending_completions[(ref, "a")] = {
            "ref": ref, "taskkey": "a", "taskId": "a", "success": True,
            "exitCode": 0, "failReason": None, "proc": "tok", "attempt": 0,
            "poke": None, "resources": None, "delay": 1.0, "nextTryAt": 0.0,
        }
        await cron._dag._flush_run_completions(ref, [entry])
        assert (ref, "a") not in cron._dag._pending_completions  # settled
        body = await cron._dag.get_run("lin", run_key)
        assert body["tasks"]["a"]["state"] == dag.PENDING  # fence held: no-op
        await _drain_pending(cron)
    finally:
        await _teardown(cron)


# --------------------------------------------------------------------------
# reconcile_on_boot: adopts this node's active runs, skips terminal ones, and
# swallows a per-dag listing timeout.
# --------------------------------------------------------------------------


async def test_reconcile_on_boot_adopts_active_and_tolerates_timeout(
    tmp_path, monkeypatch
):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        _set_cmd(cron, "lin", "a", [_PY, "-c", "pass"])
        _set_cmd(cron, "lin", "b", [_PY, "-c", "pass"])
        dagcfg = cron.cron_dags["lin"]

        term_key = await cron._dag.trigger_run("lin")
        assert (await _drive(cron, "lin", term_key))["state"] == dag.SUCCESS
        assert await cron._dag._create_doc(dagcfg, "manual-active", None, "manual")
        active_ref = ("lin", "manual-active")
        cron._dag.forget()  # a fresh restart owns nothing

        await cron._dag.reconcile_on_boot()
        assert active_ref in cron._dag._owned  # active run adopted here
        assert ("lin", term_key) not in cron._dag._owned  # terminal skipped
        await _drive(cron, "lin", "manual-active")  # reap the adopted launch

        # a listing timeout for a dag is logged and skipped, never raised.
        async def _timeout(namespace):
            raise asyncio.TimeoutError()

        monkeypatch.setattr(cron.state_backend, "list_documents", _timeout)
        cron._dag.forget()
        await cron._dag.reconcile_on_boot()  # must not raise
        monkeypatch.undo()
    finally:
        await _teardown(cron)


# --------------------------------------------------------------------------
# gc_removed_dags: a terminal run of a dag gone from every config is deleted
# once older than the grace; a recent one, and an actively-owned one, are kept.
# --------------------------------------------------------------------------


async def test_gc_removed_dags_deletes_old_terminal_keeps_recent_and_owned(
    tmp_path, monkeypatch
):
    yaml = (
        "dags:\n  - name: gc\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        _set_cmd(cron, "gc", "a", [_PY, "-c", "pass"])
        run_key = await cron._dag.trigger_run("gc")
        assert (await _drive(cron, "gc", run_key))["state"] == dag.SUCCESS
        dagcfg = cron.cron_dags["gc"]
        backend = cron.state_backend
        ns = dag.DAG_RUN_NS_PREFIX + "gc"

        # a still-active run of the removed dag is never collected (only a
        # terminal run is), so a re-added dag resumes it where it stopped.
        assert await cron._dag._create_doc(dagcfg, "gc-active", None, "manual")
        active_keys = {run_key, "gc-active"}

        del cron.cron_dags["gc"]  # gone from every live config

        # a large grace keeps the still-recent terminal run (and the active).
        await cron._dag.gc_removed_dags(backend, {"gc"}, grace=100000.0)
        docs = await backend.list_documents(ns)
        assert {b["runKey"] for b in docs} == active_keys

        # an owned terminal run is never collected, even when old enough.
        cron._dag._owned[("gc", run_key)] = object()  # sentinel lease
        await cron._dag.gc_removed_dags(backend, {"gc"}, grace=0.0)
        docs = await backend.list_documents(ns)
        assert {b["runKey"] for b in docs} == active_keys
        del cron._dag._owned[("gc", run_key)]

        # a per-dag failure is isolated, not raised, so the pass survives it.
        real_list = backend.list_documents

        async def _boom(namespace):
            raise RuntimeError("listing blew up")

        monkeypatch.setattr(backend, "list_documents", _boom)
        await cron._dag.gc_removed_dags(backend, {"gc"}, grace=0.0)  # no raise
        monkeypatch.setattr(backend, "list_documents", real_list)

        # unowned + past the grace: the terminal run is deleted; the still
        # active run survives.
        await cron._dag.gc_removed_dags(backend, {"gc"}, grace=0.0)
        assert {b["runKey"] for b in await backend.list_documents(ns)} == {
            "gc-active"
        }
    finally:
        await _teardown(cron)


# ===========================================================================
# service() / _run_service() gating, cadence branches, and error isolation.
# ===========================================================================


async def test_service_skips_when_a_pass_is_in_flight_or_nothing_due(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        d = cron._dag
        # a pass already in flight: service() is a no-op (does not spawn a
        # second one).
        async def _sleep():
            await asyncio.sleep(3600)

        running = asyncio.ensure_future(_sleep())
        d._service_task = running
        d._next_sched_check = 0.0  # would otherwise be due
        d.service()
        assert d._service_task is running  # not replaced
        running.cancel()
        try:
            await running
        except asyncio.CancelledError:
            pass
        d._service_task = None

        # nothing due: every cadence is in the future and no wake / completion /
        # logical fire is pending, so service() returns without spawning.
        future = dagrun._now() + 3600.0
        d._next_sched_check = future
        d._next_adopt = future
        d._next_gc = future
        d._wake.clear()
        d._pending_completions.clear()
        d._next_logical.clear()
        d.service()
        assert d._service_task is None
    finally:
        await _teardown(cron)


async def test_run_service_cadence_branches(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        d = cron._dag
        # defaults (all cadences 0.0): seed, adopt and gc all run.
        await d._run_service()
        # all cadences pushed into the future: each guard's false branch is
        # taken (fire_scheduled / retry_completions / advance_owned still run).
        future = dagrun._now() + 3600.0
        d._next_sched_check = future
        d._next_adopt = future
        d._next_gc = future
        await d._run_service()
    finally:
        await _teardown(cron)


async def test_run_service_isolates_and_propagates_errors(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        d = cron._dag

        async def _boom(now):
            raise RuntimeError("seed blew up")

        monkeypatch.setattr(d, "_seed_dags", _boom)
        d._next_sched_check = 0.0
        await d._run_service()  # a bad pass is logged, never kills the loop

        async def _cancel(now):
            raise asyncio.CancelledError()

        monkeypatch.setattr(d, "_seed_dags", _cancel)
        d._next_sched_check = 0.0
        with pytest.raises(asyncio.CancelledError):
            await d._run_service()  # cancellation propagates
    finally:
        await _teardown(cron)


async def test_next_wake_delay_prunes_unowned_wake(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        d = cron._dag
        ref = ("lin", "manual-unowned")
        d._wake[ref] = 0.0  # a stale wake for a run this node does not own
        assert ref not in d._owned
        d.next_wake_delay()
        assert ref not in d._wake  # pruned so it cannot pin the loop at 0
    finally:
        await _teardown(cron)


# ===========================================================================
# Seeding branches: seed_failed pruning, cluster denial, catch-up isolation.
# ===========================================================================


async def test_seed_dags_prunes_failed_denies_cluster_and_propagates_cancel(
    tmp_path, monkeypatch
):
    yaml = (
        "dags:\n  - name: sd\n    schedule: '0 * * * *'\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        d = cron._dag
        # a stale seed_failed entry for a dag no longer present is pruned.
        d._seed_failed["ghost"] = "old-sig"
        # this node's cluster does not own the schedule: the dag is skipped.
        monkeypatch.setattr(cron, "_cluster_allows", lambda sched: False)
        await d._seed_dags(dagrun._now())
        assert "ghost" not in d._seed_failed
        assert "sd" not in d._seeded  # cluster denied
        monkeypatch.undo()

        # a seed raising CancelledError propagates out of the pass.
        async def _cancel(dagcfg, now_dt):
            raise asyncio.CancelledError()

        monkeypatch.setattr(d, "_seed_dag", _cancel)
        with pytest.raises(asyncio.CancelledError):
            await d._seed_dags(dagrun._now())
    finally:
        await _teardown(cron)


async def test_seed_dag_catch_up_error_is_isolated(tmp_path, monkeypatch):
    yaml = (
        "dags:\n  - name: cs\n    schedule: '0 * * * *'\n"
        "    onMissed: run-all\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        d = cron._dag
        dagcfg = cron.cron_dags["cs"]
        now_dt = _utcnow()

        async def _boom(cfg, nd):
            raise RuntimeError("catch-up blew up")

        monkeypatch.setattr(d, "_catch_up", _boom)
        await d._seed_dag(dagcfg, now_dt)  # swallowed: the dag still seeds
        assert "cs" in d._seeded

        async def _cancel(cfg, nd):
            raise asyncio.CancelledError()

        monkeypatch.setattr(d, "_catch_up", _cancel)
        with pytest.raises(asyncio.CancelledError):
            await d._seed_dag(dagcfg, now_dt)
    finally:
        await _teardown(cron)


async def test_fire_scheduled_cluster_deny_and_cancel(tmp_path, monkeypatch):
    yaml = (
        "dags:\n  - name: fs\n    schedule: '* * * * *'\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        d = cron._dag
        d._seeded["fs"] = d._sched_sig(cron.cron_dags["fs"])
        d._next_logical["fs"] = _utcnow() - datetime.timedelta(seconds=90)
        # cluster denies: the seeded, due dag is skipped (no run created).
        monkeypatch.setattr(cron, "_cluster_allows", lambda sched: False)
        await d._fire_scheduled(dagrun._now())
        assert await d.list_runs("fs") == []
        monkeypatch.undo()

        # a firing raising CancelledError propagates.
        async def _cancel(dagcfg, now_dt):
            raise asyncio.CancelledError()

        monkeypatch.setattr(d, "_fire_forward", _cancel)
        with pytest.raises(asyncio.CancelledError):
            await d._fire_scheduled(dagrun._now())
    finally:
        await _teardown(cron)


async def test_fire_forward_stops_at_catchup_cap(tmp_path, monkeypatch):
    yaml = (
        "dags:\n  - name: ff\n    schedule: '* * * * *'\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        d = cron._dag
        dagcfg = cron.cron_dags["ff"]
        created = []

        async def _noop(cfg, when, kind):
            created.append(when)

        monkeypatch.setattr(d, "_create_run", _noop)
        # far enough back that the per-minute schedule has many more due
        # instants than the cap: the fire loop stops at DAG_MAX_CATCHUP.
        d._next_logical["ff"] = _utcnow() - datetime.timedelta(hours=5)
        await d._fire_forward(dagcfg, _utcnow())
        assert len(created) == dagrun.DAG_MAX_CATCHUP
    finally:
        await _teardown(cron)


# ===========================================================================
# Catch-up + durable watermark branches.
# ===========================================================================


async def test_catch_up_deadline_not_binding(tmp_path):
    cron = await _make_cron(tmp_path, _HOURLY)
    try:
        dagcfg = cron.cron_dags["cu"]
        sched = dagcfg.schedule_job
        sched.startingDeadlineSeconds = 1000000.0  # far wider than the gap
        base = datetime.datetime(2026, 1, 1, 0, 0, tzinfo=_UTC)
        now_dt = datetime.datetime(2026, 1, 1, 2, 30, tzinfo=_UTC)
        await cron._dag._create_run(dagcfg, base, "scheduled")
        await cron._dag._catch_up(dagcfg, now_dt)
        # the cutoff falls before the watermark, so it never advances `after`:
        # both missed slots (01:00, 02:00) are still replayed.
        runs = await cron._dag.list_runs("cu", limit=10)
        assert [r["kind"] for r in runs].count("catchup") == 2
    finally:
        await _teardown(cron)


async def test_catch_up_no_missed_slots_returns(tmp_path):
    cron = await _make_cron(tmp_path, _HOURLY)
    try:
        dagcfg = cron.cron_dags["cu"]
        # the only prior run is the current slot; the next fire is in the
        # future, so nothing is missed.
        now_dt = datetime.datetime(2026, 1, 1, 3, 0, tzinfo=_UTC)
        await cron._dag._create_run(dagcfg, now_dt, "scheduled")
        await cron._dag._catch_up(dagcfg, now_dt)
        runs = await cron._dag.list_runs("cu", limit=10)
        assert [r["kind"] for r in runs].count("catchup") == 0
    finally:
        await _teardown(cron)


async def test_durable_watermark_no_backend_and_skips_undated(tmp_path):
    cron = await _make_cron(tmp_path, _HOURLY)
    try:
        dagcfg = cron.cron_dags["cu"]
        backend = cron.state_backend
        cron.state_backend = None
        assert await cron._dag._durable_watermark(dagcfg) is None
        cron.state_backend = backend

        # a manual run has no logicalDate and is skipped by the scan; the
        # scheduled run's date is the watermark.
        base = datetime.datetime(2026, 1, 1, 0, 0, tzinfo=_UTC)
        await cron._dag._create_doc(dagcfg, "manual-x", None, "manual")
        await cron._dag._create_doc(dagcfg, "sched-x", base.isoformat(), "sc")
        wm = await cron._dag._durable_watermark(dagcfg)
        assert wm == base
    finally:
        await _teardown(cron)


async def test_create_run_naive_datetime_is_read_as_utc(tmp_path):
    cron = await _make_cron(tmp_path, _HOURLY)
    try:
        _set_cmd(cron, "cu", "a", [_PY, "-c", "pass"])
        dagcfg = cron.cron_dags["cu"]
        naive = datetime.datetime(2026, 5, 1, 12, 0)  # no tzinfo
        ref = await cron._dag._create_run(dagcfg, naive, "backfill")
        assert ref[1].startswith("2026-05-01T12")  # keyed canonically as UTC
        await _drive(cron, "cu", ref[1])
    finally:
        await _teardown(cron)


# ===========================================================================
# Ownership plumbing: _try_own / _renew_loop / _release degraded branches.
# ===========================================================================


async def test_try_own_without_backend_fails_closed(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        dagcfg = cron.cron_dags["lin"]
        backend = cron.state_backend
        cron.state_backend = None
        assert await cron._dag._try_own(dagcfg, ("lin", "rk")) is False
        cron.state_backend = backend
    finally:
        await _teardown(cron)


async def test_renew_loop_exits_when_ref_no_longer_owned(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        _instant_sleep(monkeypatch)
        # the ref is not in _owned, so the first wake finds no lease and returns.
        await asyncio.wait_for(cron._dag._renew_loop(("lin", "gone")), 5)
    finally:
        await _teardown(cron)


async def test_renew_loop_propagates_cancel(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        ref = ("lin", "rk-cancel")
        cron._dag._owned[ref] = Lease(
            "dagadvance/lin/rk-cancel", "h#1", 1, 9e18
        )

        async def _cancel(lease, ttl):
            raise asyncio.CancelledError()

        monkeypatch.setattr(cron.state_backend, "renew_lease", _cancel)
        _instant_sleep(monkeypatch)
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(cron._dag._renew_loop(ref), 5)
    finally:
        await _teardown(cron)


async def test_release_unowned_is_noop_and_propagates_cancel(
    tmp_path, monkeypatch
):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        # a ref this node never owned: nothing to release, no crash.
        await cron._dag._release(("lin", "never-owned"))

        ref = ("lin", "rk-rel")
        cron._dag._owned[ref] = Lease("dagadvance/lin/rk-rel", "h#1", 1, 9e18)

        async def _cancel(lease):
            raise asyncio.CancelledError()

        monkeypatch.setattr(cron.state_backend, "release_lease", _cancel)
        with pytest.raises(asyncio.CancelledError):
            await cron._dag._release(ref)
    finally:
        await _teardown(cron)


# ===========================================================================
# Adoption: full/incremental gating, error isolation, degraded read branches.
# ===========================================================================


async def test_adopt_orphans_incremental_pass_and_isolation(
    tmp_path, monkeypatch
):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        d = cron._dag
        # push the full refresh into the future so this pass is incremental.
        d._next_full_adopt = dagrun._now() + 3600.0
        seen = []

        async def _spy(backend, name, dagcfg, *, full):
            seen.append(full)

        monkeypatch.setattr(d, "_adopt_one_dag", _spy)
        await d._adopt_orphans()
        assert seen == [False]
        monkeypatch.undo()

        # a per-dag adoption failure is isolated, not raised.
        async def _boom(backend, name, dagcfg, *, full):
            raise RuntimeError("adopt blew up")

        monkeypatch.setattr(d, "_adopt_one_dag", _boom)
        await d._adopt_orphans()
        monkeypatch.undo()

        # a CancelledError propagates.
        async def _cancel(backend, name, dagcfg, *, full):
            raise asyncio.CancelledError()

        monkeypatch.setattr(d, "_adopt_one_dag", _cancel)
        with pytest.raises(asyncio.CancelledError):
            await d._adopt_orphans()
    finally:
        await _teardown(cron)


async def test_adopt_one_dag_keys_timeout_returns(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        dagcfg = cron.cron_dags["lin"]

        async def _timeout(ns):
            raise asyncio.TimeoutError()

        monkeypatch.setattr(cron.state_backend, "list_document_keys", _timeout)
        await cron._dag._adopt_one_dag(
            cron.state_backend, "lin", dagcfg, full=False
        )
    finally:
        await _teardown(cron)


async def test_adopt_one_dag_keys_none_falls_back_to_full(
    tmp_path, monkeypatch
):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        _set_cmd(cron, "lin", "a", [_PY, "-c", "pass"])
        _set_cmd(cron, "lin", "b", [_PY, "-c", "pass"])
        d = cron._dag
        dagcfg = cron.cron_dags["lin"]
        open_key = await d.trigger_run("lin")
        for ref in list(d._owned):
            d._drop_owned(ref)

        async def _no_keys(ns):
            return None

        monkeypatch.setattr(cron.state_backend, "list_document_keys", _no_keys)
        # keys None: the pass falls through to the full body listing, which
        # still adopts the active run.
        await d._adopt_one_dag(cron.state_backend, "lin", dagcfg, full=False)
        assert ("lin", open_key) in d._owned
        monkeypatch.undo()
        await _drive(cron, "lin", open_key)
    finally:
        await _teardown(cron)


async def test_adopt_one_dag_read_timeout_returns(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        d = cron._dag
        dagcfg = cron.cron_dags["lin"]
        assert await d._create_doc(dagcfg, "active-k", None, "manual")
        d._terminal_run_keys.pop("lin", None)

        async def _timeout(ns, key):
            raise asyncio.TimeoutError()

        monkeypatch.setattr(cron.state_backend, "read_document", _timeout)
        await d._adopt_one_dag(cron.state_backend, "lin", dagcfg, full=False)
    finally:
        await _teardown(cron)


async def test_adopt_one_dag_skips_deleted_and_nonstr_runkey(
    tmp_path, monkeypatch
):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        d = cron._dag
        dagcfg = cron.cron_dags["lin"]
        assert await d._create_doc(dagcfg, "gone-k", None, "manual")
        assert await d._create_doc(dagcfg, "weird-k", None, "manual")
        d._terminal_run_keys.pop("lin", None)

        async def _read(ns, key):
            if key == "gone-k":
                return None  # deleted since the listing
            return {"state": dag.RUNNING, "runKey": 123, "tasks": {}}

        monkeypatch.setattr(cron.state_backend, "read_document", _read)
        await d._adopt_one_dag(cron.state_backend, "lin", dagcfg, full=False)
        assert ("lin", "weird-k") not in d._owned  # non-str runKey never owned
    finally:
        await _teardown(cron)


async def test_adopt_one_dag_full_timeout_and_nonstr_runkeys(
    tmp_path, monkeypatch
):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        d = cron._dag
        dagcfg = cron.cron_dags["lin"]

        async def _timeout(ns):
            raise asyncio.TimeoutError()

        monkeypatch.setattr(cron.state_backend, "list_documents", _timeout)
        await d._adopt_one_dag(cron.state_backend, "lin", dagcfg, full=True)
        monkeypatch.undo()

        async def _docs(ns):
            return [
                {"state": dag.SUCCESS, "runKey": 1, "tasks": {}},  # terminal
                {"state": dag.RUNNING, "runKey": 2, "tasks": {}},  # active
            ]

        monkeypatch.setattr(cron.state_backend, "list_documents", _docs)
        await d._adopt_one_dag(cron.state_backend, "lin", dagcfg, full=True)
        # neither non-str run key is cached or owned.
        assert d._terminal_run_keys["lin"] == set()
    finally:
        await _teardown(cron)


# ===========================================================================
# Advancing: _advance_owned, unusable-lease back-off, exception-after-drop,
# _lease_usable cancel.
# ===========================================================================


async def test_advance_owned_advances_due_ref(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        _set_cmd(cron, "lin", "a", [_PY, "-c", "pass"])
        _set_cmd(cron, "lin", "b", [_PY, "-c", "pass"])
        run_key = await cron._dag.trigger_run("lin")
        ref = ("lin", run_key)
        if ref in cron._dag._owned:
            cron._dag._wake[ref] = 0.0
            await cron._dag._advance_owned(dagrun._now())
        await _drive(cron, "lin", run_key)
    finally:
        await _teardown(cron)


async def test_advance_locked_backs_off_on_unusable_lease(tmp_path):
    yaml = (
        "dags:\n  - name: ul\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        dagcfg = cron.cron_dags["ul"]
        run_key = "manual-ul"
        assert await cron._dag._create_doc(dagcfg, run_key, None, "manual")
        ref = ("ul", run_key)
        lease_name = cron._dag._lease_name(ref)
        lease = await cron.state_backend.acquire_lease(
            lease_name, cron._slot_holder(), 30.0
        )
        cron._dag._owned[ref] = lease
        cron._dag._locks.setdefault(ref, asyncio.Lock())
        # expired but nobody took it over: _lease_usable fails closed yet keeps
        # ownership, so the advance backs off instead of touching the run.
        lease.expires_at = dagrun._now() - 5.0
        await cron._dag.advance_one(ref)
        assert ref in cron._dag._owned
        assert cron._dag._wake[ref] > dagrun._now()
    finally:
        await _teardown(cron)


async def test_advance_locked_exception_after_ownership_lost(
    tmp_path, monkeypatch
):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        d = cron._dag
        ref = ("lin", "rk-exc")
        d._owned[ref] = Lease("dagadvance/lin/rk-exc", "h#1", 1, 9e18)
        d._locks.setdefault(ref, asyncio.Lock())

        async def _boom(dagcfg, r):
            d._drop_owned(r)  # ownership lost mid-advance
            raise RuntimeError("advance blew up")

        monkeypatch.setattr(d, "_do_advance", _boom)
        await d.advance_one(ref)  # swallowed; no wake set (no longer owned)
        assert ref not in d._owned
        assert ref not in d._wake
    finally:
        await _teardown(cron)


async def test_lease_usable_propagates_cancel(tmp_path, monkeypatch):
    yaml = (
        "dags:\n  - name: lc\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        ref = ("lc", "rk")
        lease = Lease(
            cron._dag._lease_name(ref), "h#1", 1, dagrun._now() - 5.0
        )

        async def _cancel(name):
            raise asyncio.CancelledError()

        monkeypatch.setattr(cron.state_backend, "read_lease", _cancel)
        with pytest.raises(asyncio.CancelledError):
            await cron._dag._lease_usable(ref, lease)
    finally:
        await _teardown(cron)


# ===========================================================================
# _do_advance: expansion second-RMW edge cases, wide fan-out defer, empty
# fan-out terminalising after the claim, and a launch CancelledError.
# ===========================================================================

_EXPAND = (
    "dags:\n  - name: xd\n    tasks:\n"
    "      - id: gen\n        command: 'x'\n"
    "      - id: work\n        command: 'x'\n        dependsOn:\n"
    "          - gen\n        expand:\n"
    "          fromTask: gen\n          key: items\n"
)


async def _advance_expand_setup(cron, tmp_path):
    """Push a one-item list from gen, let gen finish, and quiesce the
    spawned auto-advance so a counted advance is the only one that runs."""
    items_file = tmp_path / "items.json"
    items_file.write_text('["only"]')
    _set_cmd(
        cron, "xd", "gen",
        [_PY, "-m", "cronstable", "xcom", "push", "--key", "items",
         str(items_file)],
    )
    _set_cmd(cron, "xd", "work", [_PY, "-c", "pass"])
    run_key = await cron._dag.trigger_run("xd")
    await _reap_running(cron)  # gen finishes; its completion lands
    pend = [t for t in list(cron._pending_state_writes) if not t.done()]
    for t in pend:
        t.cancel()
    await asyncio.gather(*pend, return_exceptions=True)
    return run_key


async def test_do_advance_expansion_second_rmw_edge_cases(tmp_path):
    cron = await _make_cron(tmp_path, _EXPAND)
    try:
        run_key = await _advance_expand_setup(cron, tmp_path)
        ref = ("xd", run_key)
        body = await cron._dag.get_run("xd", run_key)
        assert body["tasks"]["gen"]["state"] == dag.SUCCESS
        orig = cron._dag._mutate

        # 1. the plan_and_claim RMW gets no backend answer (result None): the
        # advance returns without expanding.
        calls = {"n": 0}

        async def _no_answer(dag_name, key, transform):
            calls["n"] += 1
            if calls["n"] == 2:
                return None, None
            return await orig(dag_name, key, transform)

        cron._dag._mutate = _no_answer
        await cron._dag.advance_one(ref)
        body = await cron._dag.get_run("xd", run_key)
        assert "work" not in body.get("mapped", {})

        # 2. the plan_and_claim RMW returns a result but no stored body
        # (claimed None): the launch loop runs over an empty result and the
        # advance schedules a wake without crashing.
        calls2 = {"n": 0}

        async def _no_body(dag_name, key, transform):
            calls2["n"] += 1
            if calls2["n"] == 2:
                return None, dag.AdvanceResult()
            return await orig(dag_name, key, transform)

        cron._dag._mutate = _no_body
        await cron._dag.advance_one(ref)
        cron._dag._mutate = orig

        # the blips clear and the run finishes normally.
        body = await _drive(cron, "xd", run_key)
        assert body["state"] == dag.SUCCESS, _states(body)
    finally:
        await _teardown(cron)


async def test_launch_cancel_propagates_out_of_advance(tmp_path, monkeypatch):
    yaml = (
        "dags:\n  - name: lp\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        _set_cmd(cron, "lp", "a", [_PY, "-c", "pass"])

        async def _cancel(dagcfg, ref, run_id, intent):
            raise asyncio.CancelledError()

        monkeypatch.setattr(cron._dag, "_launch_task", _cancel)
        with pytest.raises(asyncio.CancelledError):
            await cron._dag.trigger_run("lp")
    finally:
        await _teardown(cron)


async def test_wide_fanout_defers_then_completes(tmp_path):
    cron = await _make_cron(tmp_path, _EXPAND)
    try:
        n = dag.MAX_CLAIMS_PER_PASS + 2
        items_file = tmp_path / "items.json"
        items_file.write_text(json.dumps(list(range(n))))
        _set_cmd(
            cron, "xd", "gen",
            [_PY, "-m", "cronstable", "xcom", "push", "--key", "items",
             str(items_file)],
        )
        _set_cmd(cron, "xd", "work", [_PY, "-c", "pass"])
        run_key = await cron._dag.trigger_run("xd")
        # a fan-out wider than the claim quota defers the surplus to a prompt
        # re-service, then converges.
        body = await _drive(cron, "xd", run_key, max_rounds=200)
        assert body["state"] == dag.SUCCESS, _states(body)
        assert len(body["mapped"]["work"]["items"]) == n
    finally:
        await _teardown(cron)


async def test_empty_fanout_terminalises_after_claim(tmp_path):
    yaml = (
        "dags:\n  - name: ez\n    tasks:\n"
        "      - id: gen\n        command: 'x'\n"
        "      - id: work\n        command: 'x'\n        dependsOn:\n"
        "          - gen\n        expand:\n"
        "          fromTask: gen\n          key: items\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        _set_cmd(cron, "ez", "gen", [_PY, "-c", "pass"])  # publishes nothing
        _set_cmd(cron, "ez", "work", [_PY, "-c", "pass"])
        run_key = await cron._dag.trigger_run("ez")
        # gen succeeds without a list, so the mapped task expands to zero
        # instances and the run terminalises on the claim RMW.
        body = await _drive(cron, "ez", run_key)
        assert body["state"] == dag.SUCCESS
        assert body["mapped"]["work"]["items"] == []
    finally:
        await _teardown(cron)


# ===========================================================================
# Completion routing: unknown-dag no-op, flush/finish/queue/retry branches.
# ===========================================================================


async def test_on_task_finished_unknown_dag_is_noop(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        class _FakeRunning:
            dag_ref = dagrun._DagRef(
                dag_name="ghost", run_key="rk", run_id="rid",
                task_id="a", taskkey="a", proc="p", attempt=0,
            )
            fail_reason = None
            resource_usage = None
            retcode = 0

        await cron._dag.on_task_finished(_FakeRunning())
        assert not cron._dag._completion_buffer
    finally:
        await _teardown(cron)


async def test_flush_completions_propagates_cancel(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        ref = ("lin", "rk-fc")
        cron._dag._completion_buffer[ref] = [{"taskkey": "a", "taskId": "a"}]

        async def _cancel(r, entries):
            raise asyncio.CancelledError()

        monkeypatch.setattr(cron._dag, "_flush_run_completions", _cancel)
        with pytest.raises(asyncio.CancelledError):
            await cron._dag.flush_completions()
    finally:
        await _teardown(cron)


async def test_flush_run_completions_propagates_cancel(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        dagcfg = cron.cron_dags["lin"]
        run_key = "manual-frc"
        assert await cron._dag._create_doc(dagcfg, run_key, None, "manual")
        ref = ("lin", run_key)
        entry = {
            "taskkey": "a", "taskId": "a", "success": True, "exitCode": 0,
            "failReason": None, "proc": "tok", "attempt": 0, "poke": None,
            "resources": None,
        }

        async def _cancel(name, key, transform):
            raise asyncio.CancelledError()

        cron._dag._mutate = _cancel
        with pytest.raises(asyncio.CancelledError):
            await cron._dag._flush_run_completions(ref, [entry])
    finally:
        await _teardown(cron)


async def test_finish_task_queues_on_error_and_propagates_cancel(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        dagcfg = cron.cron_dags["lin"]
        run_key = "manual-ft"
        assert await cron._dag._create_doc(dagcfg, run_key, None, "manual")
        ref = ("lin", run_key)
        orig = cron._dag._mutate

        async def _boom(name, key, transform):
            raise RuntimeError("store stall")

        cron._dag._mutate = _boom
        await cron._dag._finish_task(
            dagcfg, ref, "a", "a", success=True, exit_code=0,
            fail_reason=None, proc="tok", attempt=0,
        )
        assert (ref, "a") in cron._dag._pending_completions

        async def _cancel(name, key, transform):
            raise asyncio.CancelledError()

        cron._dag._mutate = _cancel
        with pytest.raises(asyncio.CancelledError):
            await cron._dag._finish_task(
                dagcfg, ref, "a", "a", success=True, exit_code=0,
                fail_reason=None, proc="tok", attempt=0,
            )
        cron._dag._mutate = orig
        await _drain_pending(cron)
    finally:
        await _teardown(cron)


async def test_queue_completion_backs_off_on_repeat(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        d = cron._dag
        ref = ("lin", "rk-q")
        d._queue_completion(
            ref, "a", "a", success=True, exit_code=0, fail_reason=None,
            proc="tok", attempt=0, poke=None,
        )
        first = d._pending_completions[(ref, "a")]["delay"]
        d._queue_completion(
            ref, "a", "a", success=True, exit_code=0, fail_reason=None,
            proc="tok", attempt=0, poke=None,
        )
        second = d._pending_completions[(ref, "a")]["delay"]
        assert second == min(first * 2.0, dagrun.COMPLETION_RETRY_MAX_DELAY)
        assert second > first
    finally:
        await _teardown(cron)


async def test_retry_completions_skips_not_due_and_removed_dag(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        d = cron._dag
        # not yet due: left queued.
        not_due = (("lin", "rk"), "a")
        d._pending_completions[not_due] = {
            "ref": ("lin", "rk"), "taskkey": "a", "taskId": "a",
            "success": True, "exitCode": 0, "failReason": None, "proc": "tok",
            "attempt": 0, "poke": None, "resources": None, "delay": 5.0,
            "nextTryAt": dagrun._now() + 3600.0,
        }
        # due, but for a dag the reload removed: dropped.
        gone = (("ghost", "rk"), "a")
        d._pending_completions[gone] = {
            "ref": ("ghost", "rk"), "taskkey": "a", "taskId": "a",
            "success": True, "exitCode": 0, "failReason": None, "proc": "tok",
            "attempt": 0, "poke": None, "resources": None, "delay": 5.0,
            "nextTryAt": 0.0,
        }
        await d._retry_completions(dagrun._now())
        assert not_due in d._pending_completions
        assert gone not in d._pending_completions
    finally:
        await _teardown(cron)


# ===========================================================================
# reconcile_on_boot / approve / backfill / get_run degraded guards.
# ===========================================================================


async def test_reconcile_on_boot_no_backend_and_nonstr_runkey(
    tmp_path, monkeypatch
):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        backend = cron.state_backend
        cron.state_backend = None
        await cron._dag.reconcile_on_boot()  # no backend: early return
        cron.state_backend = backend

        async def _docs(ns):
            return [{"state": dag.RUNNING, "runKey": 123, "tasks": {}}]

        monkeypatch.setattr(cron.state_backend, "list_documents", _docs)
        cron._dag.forget()
        await cron._dag.reconcile_on_boot()  # non-str runKey skipped
        assert not cron._dag._owned
    finally:
        await _teardown(cron)


async def test_approve_unknown_dag(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        res = await cron._dag.approve(
            "ghost", "rk", "gate", approved=True, by="x"
        )
        assert res == {"ok": False, "reason": "no such dag"}
    finally:
        await _teardown(cron)


async def test_backfill_unscheduled_or_unknown_dag(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)  # lin has no schedule
    try:
        res = await cron._dag.backfill(
            "lin", "2026-01-01T00:00:00+00:00", "2026-01-01T01:00:00+00:00"
        )
        assert res == {"ok": False, "reason": "no such scheduled dag"}
        res2 = await cron._dag.backfill(
            "ghost", "2026-01-01T00:00:00+00:00", "2026-01-01T01:00:00+00:00"
        )
        assert res2["ok"] is False
    finally:
        await _teardown(cron)


async def test_get_run_unknown_dag_is_none(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        assert await cron._dag.get_run("ghost", "rk") is None
    finally:
        await _teardown(cron)


# ===========================================================================
# Rollup / xcom_for_run cancellation propagation (never swallowed).
# ===========================================================================


async def test_bulk_rollup_propagates_cancel(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        async def _cancel(ns):
            raise asyncio.CancelledError()

        monkeypatch.setattr(cron.state_backend, "list_documents", _cancel)
        ns = cron._dag._ns("xc")
        with pytest.raises(asyncio.CancelledError):
            await cron._dag._bulk_rollup(cron.state_backend, ns, "xc")
    finally:
        await _teardown(cron)


async def test_dag_run_rollup_propagates_cancel_on_keys_and_read(
    tmp_path, monkeypatch
):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        await _mint_run(cron, "r1")

        async def _cancel_keys(ns):
            raise asyncio.CancelledError()

        monkeypatch.setattr(
            cron.state_backend, "list_document_keys", _cancel_keys
        )
        with pytest.raises(asyncio.CancelledError):
            await cron._dag._dag_run_rollup(cron.state_backend, "xc")
        monkeypatch.undo()

        async def _cancel_read(ns, key):
            raise asyncio.CancelledError()

        monkeypatch.setattr(cron.state_backend, "read_document", _cancel_read)
        with pytest.raises(asyncio.CancelledError):
            await cron._dag._dag_run_rollup(cron.state_backend, "xc")
    finally:
        await _teardown(cron)


async def test_xcom_for_run_propagates_cancel_on_list_and_blob(
    tmp_path, monkeypatch
):
    cron = await _make_cron(tmp_path, _XC_YAML)
    try:
        run_id = await _mint_run(cron, "r1")
        scope = dag.xcom_scope("xc", str(run_id))
        await jobstate.artifact_put(cron.state_backend, scope, "a/k", b"hi")

        async def _cancel_list(*a, **k):
            raise asyncio.CancelledError()

        monkeypatch.setattr(jobstate, "artifact_list", _cancel_list)
        with pytest.raises(asyncio.CancelledError):
            await cron._dag.xcom_for_run("xc", "r1")
        monkeypatch.undo()

        async def _cancel_blob(digest):
            raise asyncio.CancelledError()

        monkeypatch.setattr(cron.state_backend, "get_blob", _cancel_blob)
        with pytest.raises(asyncio.CancelledError):
            await cron._dag.xcom_for_run("xc", "r1")
    finally:
        await _teardown(cron)


# ===========================================================================
# GC branches: no-backend, isolation, empty run key, prune-error swallow,
# removed-dag non-str run key + cancellation.
# ===========================================================================


async def test_gc_runs_no_backend_isolation_and_cancel(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        d = cron._dag
        backend = cron.state_backend
        cron.state_backend = None
        await d._gc_runs()  # no backend: early return
        cron.state_backend = backend

        async def _boom(backend_, name, dagcfg):
            raise RuntimeError("gc blew up")

        monkeypatch.setattr(d, "_gc_one_dag", _boom)
        await d._gc_runs()  # a per-dag failure is isolated
        monkeypatch.undo()

        async def _cancel(backend_, name, dagcfg):
            raise asyncio.CancelledError()

        monkeypatch.setattr(d, "_gc_one_dag", _cancel)
        with pytest.raises(asyncio.CancelledError):
            await d._gc_runs()
    finally:
        await _teardown(cron)


async def test_gc_one_dag_skips_empty_run_key(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _RETAIN_ONE)  # retainRuns 1
    try:
        dagcfg = cron.cron_dags["rt"]

        async def _docs(ns):
            return [
                {"runKey": "", "state": dag.SUCCESS, "createdAt": 1.0,
                 "tasks": {}},
                {"runKey": "keep", "state": dag.SUCCESS, "createdAt": 2.0,
                 "tasks": {}},
            ]

        monkeypatch.setattr(cron.state_backend, "list_documents", _docs)
        deleted = []

        async def _spy_delete(backend, name, run_key, run_id):
            deleted.append(run_key)

        monkeypatch.setattr(cron._dag, "_delete_run", _spy_delete)
        # excess is 1; the oldest terminal run has an empty run key and is
        # skipped rather than deleted.
        await cron._dag._gc_one_dag(cron.state_backend, "rt", dagcfg)
        assert deleted == []
    finally:
        await _teardown(cron)


async def test_delete_run_swallows_prune_error(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        dagcfg = cron.cron_dags["lin"]
        assert await cron._dag._create_doc(dagcfg, "del-k", None, "manual")

        async def _boom(stream, keep):
            raise RuntimeError("prune blew up")

        monkeypatch.setattr(cron.state_backend, "prune_records", _boom)
        # a run_id present means the XCom prune is attempted; its failure is
        # swallowed while the document itself is still deleted.
        await cron._dag._delete_run(
            cron.state_backend, "lin", "del-k", "some-run-id"
        )
        assert await cron._dag.get_run("lin", "del-k") is None
    finally:
        await _teardown(cron)


async def test_gc_removed_dags_nonstr_runkey_and_cancel(tmp_path, monkeypatch):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        del cron.cron_dags["lin"]  # gone from every live config

        async def _docs(ns):
            return [{"state": dag.SUCCESS, "runKey": 123, "tasks": {}}]

        monkeypatch.setattr(cron.state_backend, "list_documents", _docs)
        deleted = []

        async def _spy(backend, name, run_key, run_id):
            deleted.append(run_key)

        monkeypatch.setattr(cron._dag, "_delete_run", _spy)
        await cron._dag.gc_removed_dags(cron.state_backend, {"lin"}, grace=0.0)
        assert deleted == []  # non-str run key skipped
        monkeypatch.undo()

        async def _cancel(ns):
            raise asyncio.CancelledError()

        monkeypatch.setattr(cron.state_backend, "list_documents", _cancel)
        with pytest.raises(asyncio.CancelledError):
            await cron._dag.gc_removed_dags(
                cron.state_backend, {"lin"}, grace=0.0
            )
    finally:
        await _teardown(cron)


# ===========================================================================
# Lifecycle teardown: shutdown / forget cancel a live service task; _parse_iso.
# ===========================================================================


async def test_shutdown_cancels_live_service_task(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        async def _sleep():
            await asyncio.sleep(3600)

        task = asyncio.ensure_future(_sleep())
        cron._dag._service_task = task
        await cron._dag.shutdown()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert task.cancelled()
    finally:
        await _teardown(cron)


async def test_forget_cancels_live_service_task(tmp_path):
    cron = await _make_cron(tmp_path, _LINEAR)
    try:
        async def _sleep():
            await asyncio.sleep(3600)

        task = asyncio.ensure_future(_sleep())
        cron._dag._service_task = task
        cron._dag.forget()
        assert cron._dag._service_task is None
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert task.cancelled()
    finally:
        await _teardown(cron)


def test_parse_iso_edge_cases():
    assert dagrun._parse_iso(None) is None
    assert dagrun._parse_iso("") is None
    assert dagrun._parse_iso("not-a-date") is None
    dt = dagrun._parse_iso("2026-01-01T00:00:00")  # naive: read as UTC
    assert dt.tzinfo == datetime.timezone.utc
