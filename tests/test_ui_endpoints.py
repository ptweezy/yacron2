"""Dashboard-facing endpoints and payloads added by the UI catch-up.

Covers the new/extended web surface: the durable-retry / slot / reboot fields
on ``/jobs``, the ``unknown`` bucket in run stats, the enriched ``/dags``
summary, the DAG XCom read, and the metadata-only state inspector
(``/state``, ``/state/documents``, ``/state/records``) with its redaction and
guards.  Handlers are called directly with a small mock request, the same
style as the existing ``test_cron.py`` web tests; the state/dag cases spin a
real :class:`~cronstable.state.FilesystemStateBackend` in a temp dir like
``test_state_dag_run.py``.
"""

import asyncio
import datetime
import json

import pytest
from aiohttp import web

from cronstable.cron import (
    Cron,
    JobRunInfo,
    _job_run_info_from_dict,
    _run_stats,
)
from cronstable.job import JobOutputStream, JobRetryState
from cronstable.resources import ResourceUsage
from cronstable.state import Lease

_UTC = datetime.timezone.utc


class Req:
    """A minimal stand-in for an aiohttp request."""

    def __init__(self, query=None, match=None, headers=None):
        self.query = query or {}
        self.match_info = match or {}
        self.headers = headers or {}


def _run(outcome, *, dur=1.0, exit_code=0):
    now = datetime.datetime.now(_UTC)
    return JobRunInfo(
        outcome=outcome,
        exit_code=exit_code,
        started_at=now - datetime.timedelta(seconds=dur),
        finished_at=now,
        fail_reason=None,
        output=JobOutputStream(),
    )


# ---------------------------------------------------------------------------
# _run_stats: the crash-reconciled `unknown` bucket
# ---------------------------------------------------------------------------


def test_run_stats_counts_unknown_separately():
    runs = [
        _run("success"),
        _run("success"),
        _run("failure"),
        _run("unknown"),
        _run("cancelled"),
    ]
    stats = _run_stats(runs)
    assert stats["total"] == 5
    assert stats["success"] == 2
    assert stats["failure"] == 1
    assert stats["unknown"] == 1
    assert stats["cancelled"] == 1
    # success_rate excludes cancellations AND unknowns (only success+failure)
    assert stats["success_rate"] == pytest.approx(2 / 3)


def test_run_stats_unknown_key_present_when_zero():
    stats = _run_stats([_run("success")])
    assert stats["unknown"] == 0


# ---------------------------------------------------------------------------
# _job_to_dict: durable-retry / slot / reboot visibility
# ---------------------------------------------------------------------------

_RETRY_JOB = """
jobs:
  - name: flaky
    command: 'false'
    schedule: "@reboot"
    onFailure:
      retry:
        maximumRetries: 5
        initialDelay: 8
        maximumDelay: 60
        backoffMultiplier: 2
"""


def _cron(yaml):
    cron = Cron(None, config_yaml=yaml)
    cron.web_config = {}
    return cron


def test_job_to_dict_no_phantom_retry_when_unarmed():
    # a retry ladder is created eagerly at launch with count 0; a run that has
    # not failed must NOT surface a retry block (no "attempt 0" phantom chip).
    cron = _cron(_RETRY_JOB)
    cron.retry_state["flaky"] = JobRetryState(8.0, 2.0, 60.0)  # count == 0
    d = cron._job_to_dict("flaky", cron.cron_jobs["flaky"])
    assert "retry" not in d


def test_job_to_dict_retry_block_when_armed():
    cron = _cron(_RETRY_JOB)
    st = JobRetryState(8.0, 2.0, 60.0)
    st.count = 2
    st.next_retry_at = datetime.datetime.now(_UTC) + datetime.timedelta(
        seconds=16
    )
    st.scheduled_delay = 16.0
    cron.retry_state["flaky"] = st
    d = cron._job_to_dict("flaky", cron.cron_jobs["flaky"])
    assert d["retry"]["attempt"] == 2
    assert d["retry"]["maxAttempts"] == 5
    assert d["retry"]["delaySeconds"] == 16.0
    assert d["retry"]["nextRetryAt"].endswith("+00:00")


def test_job_to_dict_reboot_pending_flag():
    cron = _cron(_RETRY_JOB)
    d = cron._job_to_dict("flaky", cron.cron_jobs["flaky"])
    assert "rebootPending" not in d
    cron._pending_reboot_jobs["flaky"] = cron.cron_jobs["flaky"]
    d = cron._job_to_dict("flaky", cron.cron_jobs["flaky"])
    assert d["rebootPending"] is True


_SLOT_JOB = """
jobs:
  - name: clustered
    command: echo hi
    schedule: "*/5 * * * *"
    concurrencyScope: cluster
    concurrencyPolicy: Forbid
  - name: local
    command: echo hi
    schedule: "*/5 * * * *"
"""


def test_job_to_dict_cluster_slot_block():
    cron = _cron(_SLOT_JOB)
    # non-cluster job carries no slot info
    local = cron._job_to_dict("local", cron.cron_jobs["local"])
    assert "slot" not in local and "concurrencyScope" not in local
    # cluster-scoped job, slot not held here
    d = cron._job_to_dict("clustered", cron.cron_jobs["clustered"])
    assert d["concurrencyScope"] == "cluster"
    assert d["slot"] == {"held": False, "holder": None, "refs": 0}
    # now hold the slot lease -- keyed by plain job name, as production does
    slot = cron._slot_name("clustered")
    cron._slot_leases["clustered"] = Lease(slot, "host-a#tok", 3, 9e18)
    cron._slot_refs["clustered"] = 2
    d = cron._job_to_dict("clustered", cron.cron_jobs["clustered"])
    assert d["slot"] == {"held": True, "holder": "host-a#tok", "refs": 2}


# ---------------------------------------------------------------------------
# _web_int_query clamping
# ---------------------------------------------------------------------------


def test_web_int_query_clamps_and_defaults():
    q = Cron._web_int_query
    assert q(Req(), "limit", default=50, lo=1, hi=500) == 50  # missing
    assert q(Req({"limit": "10"}), "limit", default=50, lo=1, hi=500) == 10
    assert q(Req({"limit": "9999"}), "limit", default=50, lo=1, hi=500) == 500
    assert q(Req({"limit": "0"}), "limit", default=50, lo=1, hi=500) == 1
    assert q(Req({"limit": "x"}), "limit", default=50, lo=1, hi=500) == 50


# ---------------------------------------------------------------------------
# State inspector + DAG enrichment (real backend)
# ---------------------------------------------------------------------------

# just the dags: section; _make_cron prepends a temp-path state section.
_STATE_DAG = """
dags:
  - name: etl
    schedule: "0 2 * * *"
    tasks:
      - id: a
        command: 'x'
      - id: b
        command: 'x'
        dependsOn:
          - a
"""


def _state_cfg(yaml):
    from cronstable.config import parse_config_string

    return parse_config_string(yaml, "").state_config


async def _make_cron(tmp_path, dags_yaml):
    cfg = (
        "state:\n  path: {}\n  jobApi:\n    enabled: true\n".format(tmp_path)
    ) + dags_yaml
    cron = Cron(None, config_yaml=cfg)
    cron.web_config = {}
    await cron.start_stop_state(_state_cfg(cfg))
    return cron


async def _teardown(cron):
    await cron._dag.shutdown()
    await cron._stop_job_api()
    if cron.state_backend is not None:
        await cron.state_backend.stop()


async def test_web_state_disabled_without_backend():
    cron = _cron(_RETRY_JOB)  # no state section
    resp = await cron._web_state(Req())
    assert json.loads(resp.text) == {"enabled": False}


async def test_web_state_inventory_and_node(tmp_path):
    cron = await _make_cron(tmp_path, _STATE_DAG)
    try:
        # arm a retry so node.retries has an entry
        st = JobRetryState(8.0, 2.0, 60.0)
        st.count = 1
        st.next_retry_at = datetime.datetime.now(_UTC)
        cron.retry_state["etl.a"] = st
        resp = await cron._web_state(Req())
        body = json.loads(resp.text)
        assert body["enabled"] is True
        assert body["enumerable"] is True
        assert "records" in body and "documents" in body
        assert body["view"]["backend"] == "filesystem"
        jobs = [r["job"] for r in body["node"]["retries"]]
        assert "etl.a" in jobs
    finally:
        await _teardown(cron)


async def test_web_state_documents_redacts_kv(tmp_path):
    from cronstable import jobstate

    cron = await _make_cron(tmp_path, _STATE_DAG)
    try:
        be = cron.state_backend
        await jobstate.kv_set(be, "myscope", "secret", {"pw": "hunter2"})
        resp = await cron._web_state_documents(Req(query={"ns": "kv/myscope"}))
        docs = json.loads(resp.text)["documents"]
        assert docs and "value" not in docs[0]
        assert docs[0]["valueType"] == "dict"
        assert docs[0]["valueSize"] > 0
        # a non kv/cursor/idem namespace is rejected
        with pytest.raises(web.HTTPBadRequest):
            await cron._web_state_documents(Req(query={"ns": "runs/etl"}))
    finally:
        await _teardown(cron)


async def test_web_state_records_forbids_logs(tmp_path):
    cron = await _make_cron(tmp_path, _STATE_DAG)
    try:
        with pytest.raises(web.HTTPForbidden):
            await cron._web_state_records(Req(query={"stream": "logs/etl.a"}))
        # a normal stream is allowed (empty result is fine)
        resp = await cron._web_state_records(
            Req(query={"stream": "runs/nobody"})
        )
        assert json.loads(resp.text)["records"] == []
    finally:
        await _teardown(cron)


async def test_web_list_dags_enriched(tmp_path):
    cron = await _make_cron(tmp_path, _STATE_DAG)
    try:
        resp = await cron._web_list_dags(Req())
        dags = json.loads(resp.text)
        etl = next(d for d in dags if d["name"] == "etl")
        assert etl["schedule"] == "0 2 * * *"  # grafted by the handler
        assert etl["scheduled"] is True
        ids = {t["id"]: t for t in etl["tasks"]}
        assert ids["b"]["dependsOn"] == ["a"]
        assert ids["a"]["triggerRule"] == "all_success"
        assert ids["a"]["mapped"] is False
    finally:
        await _teardown(cron)


async def test_dag_xcom_unknown_dag_is_none(tmp_path):
    cron = await _make_cron(tmp_path, _STATE_DAG)
    try:
        assert await cron._dag.xcom_for_run("ghost", "x") is None
    finally:
        await _teardown(cron)


# ---------------------------------------------------------------------------
# /jobs/{name}/resources + /node/history (the resource-chart endpoints)
# ---------------------------------------------------------------------------

_RES_JOB = """
jobs:
  - name: heavy
    command: echo hi
    schedule: "*/5 * * * *"
    monitorResources:
      interval: 0.5
      history: 50
"""


def _monitored_run(outcome, series):
    info = _run(outcome)
    info.resource_usage = ResourceUsage(
        cpu_user_seconds=1.0,
        cpu_system_seconds=0.5,
        max_rss_bytes=2048,
        samples=len(series),
        series=series,
    )
    return info


async def test_web_job_resources_payload():
    cron = _cron(_RES_JOB)
    series = [[1.0, 5.0, 1024], [2.0, 7.0, 2048]]
    cron.run_history["heavy"].append(_monitored_run("success", series))
    # an unmonitored run in the same history is filtered out of `runs`
    cron.run_history["heavy"].append(_run("failure"))
    resp = await cron._web_job_resources(Req(match={"name": "heavy"}))
    body = json.loads(resp.text)
    assert body["monitored"] is True
    assert body["interval"] == 0.5  # the configured sampling cadence
    assert body["live"] == []  # nothing running
    assert len(body["runs"]) == 1
    assert body["runs"][0]["resources"]["series"] == series


async def test_web_job_resources_runs_param_and_404():
    cron = _cron(_RES_JOB)
    for _ in range(5):
        cron.run_history["heavy"].append(
            _monitored_run("success", [[1.0, 1.0, 1]])
        )
    body = json.loads(
        (
            await cron._web_job_resources(
                Req(query={"runs": "2"}, match={"name": "heavy"})
            )
        ).text
    )
    assert len(body["runs"]) == 2
    body = json.loads(
        (
            await cron._web_job_resources(
                Req(query={"runs": "0"}, match={"name": "heavy"})
            )
        ).text
    )
    assert body["runs"] == []
    with pytest.raises(web.HTTPNotFound):
        await cron._web_job_resources(Req(match={"name": "ghost"}))


async def test_web_job_resources_unmonitored_job():
    cron = _cron(_RETRY_JOB)
    body = json.loads(
        (await cron._web_job_resources(Req(match={"name": "flaky"}))).text
    )
    assert body["monitored"] is False
    assert body["live"] == [] and body["runs"] == []


async def test_run_series_stays_out_of_polled_payloads():
    # the chart series rides ONLY the ledger record and the dedicated
    # resources endpoint; /jobs and /jobs/{name}/runs stay summary-sized.
    cron = _cron(_RES_JOB)
    info = _monitored_run("success", [[1.0, 5.0, 1024]])
    cron.run_history["heavy"].append(info)
    cron.last_run["heavy"] = info
    runs_body = json.loads(
        (await cron._web_job_runs(Req(match={"name": "heavy"}))).text
    )
    assert "series" not in runs_body["runs"][0]["resources"]
    d = cron._job_to_dict("heavy", cron.cron_jobs["heavy"])
    assert "series" not in d["last_run"]["resources"]
    # ...while the durable ledger record carries it (chart restart survival)
    assert info.to_dict(include_series=True)["resources"]["series"]


def test_run_record_series_round_trip():
    # the ledger record (to_dict include_series) rehydrates with its series
    info = _monitored_run("success", [[1.0, 5.0, 1024]])
    restored = _job_run_info_from_dict(info.to_dict(include_series=True))
    assert restored is not None
    assert restored.resource_usage is not None
    assert restored.resource_usage.series == [[1.0, 5.0, 1024]]
    # a summary-only record (the polled shape) rehydrates without one
    summary = _job_run_info_from_dict(info.to_dict())
    assert summary is not None
    assert summary.resource_usage is not None
    assert summary.resource_usage.series is None


async def test_web_node_history_endpoint():
    cron = _cron(_RETRY_JOB)
    body = json.loads((await cron._web_node_history(Req())).text)
    assert body["enabled"] is False
    assert body["points"] == []
    cron._node_sampler.start_history(interval=0.05, points=10)
    try:
        await asyncio.sleep(0.2)
        body = json.loads((await cron._web_node_history(Req())).text)
        assert body["enabled"] is True
        assert body["interval"] == 0.05
        assert body["points"]
        assert all(len(p) == 3 for p in body["points"])
    finally:
        await cron._node_sampler.stop_history()
