"""DAG task-run reporting: onFailure/onSuccess fire for task instances.

A DAG task is a job invocation, so the report hooks its template carries
(set per-task or inherited from the ``defaults:`` block) fire on its runs.
The dispatch lives in ``Cron._handle_finished_dag_task`` (the reaper's DAG
branch), spawned as a tracked completion task exactly like a job's report
sequence. Job-level retry arming and onPermanentFailure stay out: a task's
attempts are graph-driven, so the per-task schema accepts report-only hooks.
"""

import json
import types

import pytest

from cronstable.config import ConfigError, parse_config_string
from cronstable.cron import Cron
from cronstable.job import RunningJob, report_config_enabled

from tests.test_job import _WebhookServer

# ---------------------------------------------------------------------------
# Schema: per-task report hooks
# ---------------------------------------------------------------------------

_DAG_WITH_DEFAULTS = """
defaults:
  onFailure:
    report:
      webhook:
        url:
          value: {url}

dags:
  - name: d
    tasks:
      - id: t1
        command: "true"
"""


def test_task_accepts_per_task_report_hooks_and_wins_over_defaults():
    yaml = """
defaults:
  onFailure:
    report:
      webhook:
        url:
          value: https://example.invalid/global

dags:
  - name: d
    tasks:
      - id: t1
        command: "true"
        onFailure:
          report:
            webhook:
              url:
                value: https://example.invalid/task
      - id: t2
        command: "true"
        onSuccess:
          report:
            webhook:
              url:
                value: https://example.invalid/ok
"""
    conf = parse_config_string(yaml, "")
    t1 = conf.dags[0].task_templates["t1"].onFailure["report"]
    assert t1["webhook"]["url"]["value"] == "https://example.invalid/task"
    t2 = conf.dags[0].task_templates["t2"]
    # t2 sets only onSuccess; its onFailure still inherits the global one
    assert (
        t2.onFailure["report"]["webhook"]["url"]["value"]
        == "https://example.invalid/global"
    )
    assert (
        t2.onSuccess["report"]["webhook"]["url"]["value"]
        == "https://example.invalid/ok"
    )


def test_task_rejects_job_level_retry_under_on_failure():
    # a task's attempts are the DAG node's `retries`; the job retry ladder
    # must not be accepted as live config on a task.
    yaml = """
dags:
  - name: d
    tasks:
      - id: t1
        command: "true"
        onFailure:
          retry:
            maximumRetries: 2
            initialDelay: 1
            maximumDelay: 10
            backoffMultiplier: 2
"""
    with pytest.raises(ConfigError):
        parse_config_string(yaml, "")


def test_report_config_enabled_probes_each_reporter():
    conf = parse_config_string(
        "jobs:\n  - name: a\n    command: 'x'\n    schedule: '@reboot'\n", ""
    )
    disabled = conf.jobs[0].onFailure["report"]
    assert not report_config_enabled(disabled)
    for enable in (
        lambda r: r["sentry"]["dsn"].__setitem__("fromEnvVar", "DSN"),
        lambda r: (
            r["mail"].__setitem__("to", "a@b"),
            r["mail"].__setitem__("from", "c@d"),
        ),
        lambda r: r["shell"].__setitem__("command", "notify.sh"),
        lambda r: r["webhook"]["url"].__setitem__("value", "https://x"),
    ):
        report = json.loads(json.dumps(disabled))
        enable(report)
        assert report_config_enabled(report)


# ---------------------------------------------------------------------------
# Dispatch: Cron._handle_finished_dag_task
# ---------------------------------------------------------------------------


def _dag_ref(dag_name="d", taskkey="t1"):
    return types.SimpleNamespace(
        dag_name=dag_name,
        run_key="2026-07-23T00:00",
        run_id="r1",
        task_id="t1",
        taskkey=taskkey,
        proc="p1",
        attempt=1,
        poke=None,
    )


def _task_job(cron, *, retcode):
    template = cron.cron_dags["d"].task_templates["t1"]
    job = RunningJob(template, None, dag_ref=_dag_ref())
    job.retcode = retcode
    return job


async def test_finished_dag_task_failure_hits_real_webhook_reporter():
    # zero mocks: the reaper's DAG branch spawns the report task, and the
    # defaults-inherited webhook config posts to a live local server.
    server = _WebhookServer()
    async with server as url:
        cron = Cron(None, config_yaml=_DAG_WITH_DEFAULTS.format(url=url))
        job = _task_job(cron, retcode=1)
        await cron._handle_finished_dag_task(job)
        assert cron._completion_tasks
        await cron._drain_completions()
    (request,) = server.requests
    payload = json.loads(request["body"])
    assert "d.t1" in payload["text"]


async def test_finished_dag_task_success_reports_via_on_success():
    yaml = _DAG_WITH_DEFAULTS.format(url="https://example.invalid/hook")
    cron = Cron(None, config_yaml=yaml.replace("onFailure", "onSuccess"))
    job = _task_job(cron, retcode=0)
    calls = []

    async def record_success():
        calls.append("success")

    async def record_failure():
        calls.append("failure")

    job.report_success = record_success
    job.report_failure = record_failure
    await cron._handle_finished_dag_task(job)
    await cron._drain_completions()
    assert calls == ["success"]


@pytest.mark.parametrize("flag", ["cancelled", "replaced"])
async def test_finished_dag_task_cancelled_or_replaced_not_reported(flag):
    cron = Cron(
        None,
        config_yaml=_DAG_WITH_DEFAULTS.format(url="https://example.invalid/h"),
    )
    job = _task_job(cron, retcode=1)
    setattr(job, flag, True)
    await cron._handle_finished_dag_task(job)
    assert cron._completion_tasks == set()


async def test_finished_dag_task_without_reporters_spawns_nothing():
    # the enabled-probe must keep the common unconfigured case at dict
    # lookups: no completion task at all, not a task that no-ops.
    cron = Cron(
        None,
        config_yaml=(
            "dags:\n  - name: d\n    tasks:\n"
            "      - id: t1\n        command: 'true'\n"
        ),
    )
    job = _task_job(cron, retcode=1)
    await cron._handle_finished_dag_task(job)
    assert cron._completion_tasks == set()


async def test_launch_failed_task_reports_failure(tmp_path):
    # a task whose command cannot spawn at all (bad argv[0]) still counts as
    # a failed attempt: start() latches start_failed, the reaper gives it the
    # conventional 127, and the onFailure reporter fires like any failure.
    from cronstable import dag

    from tests.test_state_dag_run import _drive, _make_cron, _teardown

    server = _WebhookServer()
    async with server as url:
        cron = await _make_cron(tmp_path, _DAG_WITH_DEFAULTS.format(url=url))
        try:
            tmpl = cron.cron_dags["d"].task_templates["t1"]
            tmpl.command = [str(tmp_path / "no-such-binary")]
            run_key = await cron._dag.trigger_run("d")
            body = await _drive(cron, "d", run_key)
            assert body["state"] == dag.FAILED
            assert body["tasks"]["t1"]["exitCode"] == 127
            await cron._drain_completions()
        finally:
            await _teardown(cron)
    assert server.requests, "launch-failed attempt fired no onFailure report"
    payload = json.loads(server.requests[0]["body"])
    assert "d.t1" in payload["text"]
