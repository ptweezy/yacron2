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


# ---------------------------------------------------------------------------
# dead-schedule surfacing: never_fires + schedule_findings
# ---------------------------------------------------------------------------

_DEAD_JOB = """
jobs:
  - name: parked
    command: echo hi
    schedule: "0 0 1 1 * 2020"
  - name: live
    command: echo hi
    schedule: "*/5 * * * *"
  - name: footgun
    command: echo hi
    schedule: "0 0 13 * 5"
"""


def test_job_to_dict_never_fires_and_findings():
    cron = _cron(_DEAD_JOB)
    dead = cron._job_to_dict("parked", cron.cron_jobs["parked"])
    assert dead["never_fires"] is True
    assert [f["code"] for f in dead["schedule_findings"]] == ["never-fires"]
    live = cron._job_to_dict("live", cron.cron_jobs["live"])
    assert live["never_fires"] is False
    assert live["schedule_findings"] == []
    # advisory findings ride along for live-but-suspect schedules too
    footgun = cron._job_to_dict("footgun", cron.cron_jobs["footgun"])
    assert footgun["never_fires"] is False
    codes = [f["code"] for f in footgun["schedule_findings"]]
    assert codes == ["day-fields-both-restricted"]
    # and the payload is JSON-serializable end to end
    json.dumps(footgun["schedule_findings"])


def test_status_payload_marks_dead_schedules():
    cron = _cron(_DEAD_JOB)
    rows = {row["job"]: row for row in cron.status_payload()}
    assert rows["parked"]["status"] == "scheduled"
    assert rows["parked"]["scheduled_in"] is None
    assert rows["parked"]["never_fires"] is True
    assert "never_fires" not in rows["live"]
    assert rows["live"]["scheduled_in"] > 0


def test_status_payload_running_dead_schedule_keeps_never_fires():
    # /status and /jobs must agree: a RUNNING job whose schedule has no
    # future occurrence keeps its never_fires flag (the two surfaces used
    # to drift for exactly this case).
    class _Run:
        proc = None

    cron = _cron(_DEAD_JOB)
    cron.running_jobs["parked"] = [_Run()]
    cron.running_jobs["live"] = [_Run()]
    rows = {row["job"]: row for row in cron.status_payload()}
    assert rows["parked"]["status"] == "running"
    assert rows["parked"]["never_fires"] is True
    assert rows["live"]["status"] == "running"
    assert "never_fires" not in rows["live"]


async def test_web_status_text_says_never_fires():
    cron = _cron(_DEAD_JOB)
    resp = await cron._web_get_status(Req())
    lines = resp.text.splitlines()
    parked = next(line for line in lines if line.startswith("parked:"))
    assert "never fires" in parked


def test_dead_schedule_never_enters_the_fire_index_but_warns(caplog):
    import logging as _logging

    cron = _cron(_DEAD_JOB)
    now = datetime.datetime.now(_UTC)
    with caplog.at_level(_logging.WARNING, logger="cronstable"):
        cron._ensure_seeded(now)
        cron._ensure_seeded(now)  # the warning latches: once, not per pass
    assert "parked" not in cron._next_fire
    assert "live" in cron._next_fire
    warned = [
        rec
        for rec in caplog.records
        if "NEVER fire" in rec.getMessage() and "'parked'" in rec.getMessage()
    ]
    assert len(warned) == 1


# ---------------------------------------------------------------------------
# GET /schedule/preview: the sandboxes' single source of truth
# ---------------------------------------------------------------------------


async def test_schedule_preview_valid_expression():
    cron = _cron(_RETRY_JOB)
    resp = await cron._web_schedule_preview(
        Req(query={"expr": "*/15 * * * *", "count": "3"})
    )
    body = json.loads(resp.text)
    assert body["valid"] is True
    assert body["normalized"] == "*/15 * * * *"
    assert body["description"] == "Every 15 minutes, every day"
    assert body["timezone"] == "UTC"
    assert len(body["fires"]) == 3
    assert body["never_fires"] is False
    assert body["lint"] == []
    # ISO instants, parseable and strictly increasing
    fires = [datetime.datetime.fromisoformat(f) for f in body["fires"]]
    assert fires == sorted(fires)


async def test_schedule_preview_lint_and_never_fires():
    cron = _cron(_RETRY_JOB)
    resp = await cron._web_schedule_preview(
        Req(query={"expr": "0 0 30 2 *"})
    )
    body = json.loads(resp.text)
    assert body["valid"] is True
    assert body["fires"] == []
    assert body["never_fires"] is True
    assert [f["code"] for f in body["lint"]] == ["never-fires"]


async def test_schedule_preview_timezone_carries_dst_notes():
    cron = _cron(_RETRY_JOB)
    resp = await cron._web_schedule_preview(
        Req(query={"expr": "30 2 * * *", "tz": "America/New_York"})
    )
    body = json.loads(resp.text)
    assert body["timezone"] == "America/New_York"
    assert "dst-skipped-time" in [f["code"] for f in body["lint"]]
    # fires come back in the requested frame
    first = datetime.datetime.fromisoformat(body["fires"][0])
    assert first.utcoffset() != datetime.timedelta(0)


async def test_schedule_preview_invalid_expression_and_reboot():
    cron = _cron(_RETRY_JOB)
    body = json.loads(
        (
            await cron._web_schedule_preview(
                Req(query={"expr": "0 */5 * * * ?"})
            )
        ).text
    )
    assert body["valid"] is False
    assert "Quartz" in body["error"]
    body = json.loads(
        (
            await cron._web_schedule_preview(Req(query={"expr": "@reboot"}))
        ).text
    )
    assert body["valid"] is True and body["reboot"] is True
    assert body["fires"] == []


async def test_schedule_preview_bad_requests_are_400s():
    cron = _cron(_RETRY_JOB)
    resp = await cron._web_schedule_preview(Req(query={}))
    assert resp.status == 400
    resp = await cron._web_schedule_preview(
        Req(query={"expr": "* * * * *", "tz": "Not/AZone"})
    )
    assert resp.status == 400
    assert "unknown timezone" in json.loads(resp.text)["error"]


# ---------------------------------------------------------------------------
# GET /schedule/why: the per-instant no-run explainer
# ---------------------------------------------------------------------------

_WHY_YAML = """
jobs:
  - name: weekday
    command: echo hi
    schedule: "0 9 * * mon,fri"
    utc: true
dags:
  - name: pipe
    schedule: "0 4 * * *"
    utc: true
    tasks:
      - id: a
        command: 'true'
"""


async def test_schedule_why_explains_a_miss_field_by_field():
    cron = _cron(_WHY_YAML)
    resp = await cron._web_schedule_why(
        Req(query={"job": "weekday", "at": "2026-07-14T09:00:00"})
    )
    body = json.loads(resp.text)
    assert body["matches"] is False
    assert body["failed"] == ["day-of-week"]
    dow = body["checks"][5]
    assert (dow["label"], dow["allowed"]) == ("Tuesday", "Monday and Friday")
    assert body["previous_fire"] == "2026-07-13T09:00:00+00:00"
    assert body["next_fire"] == "2026-07-17T09:00:00+00:00"
    assert body["timezone"] == "UTC"


async def test_schedule_why_resolves_dag_schedule_jobs():
    cron = _cron(_WHY_YAML)
    resp = await cron._web_schedule_why(
        Req(query={"job": "dag:pipe", "at": "2026-07-14T04:00:00"})
    )
    body = json.loads(resp.text)
    assert body["job"] == "dag:pipe"
    assert body["expression"] == "0 4 * * *"
    assert body["matches"] is True


async def test_schedule_why_bad_requests():
    cron = _cron(_WHY_YAML)
    resp = await cron._web_schedule_why(Req(query={"job": "weekday"}))
    assert resp.status == 400
    resp = await cron._web_schedule_why(
        Req(query={"job": "weekday", "at": "not-a-time"})
    )
    assert resp.status == 400
    assert "ISO 8601" in json.loads(resp.text)["error"]
    with pytest.raises(web.HTTPNotFound):
        await cron._web_schedule_why(
            Req(query={"job": "ghost", "at": "2026-07-14T09:00:00"})
        )


# ---------------------------------------------------------------------------
# fleet schedule analysis: /schedule/pressure, /duplicates, /suggest
# ---------------------------------------------------------------------------

_FLEET_YAML = """
jobs:
  - name: herd-a
    command: "true"
    schedule: "0 * * * *"
  - name: herd-b
    command: "true"
    schedule: "0 * * * *"
  - name: herd-c
    command: "true"
    schedule: "@hourly"
  - name: spread
    command: "true"
    schedule: "H * * * *"
  - name: parked
    command: "true"
    schedule: "0 0 * * *"
    enabled: false
  - name: boot
    command: "true"
    schedule: "@reboot"
"""


async def test_schedule_pressure_endpoint_counts_and_excludes():
    cron = _cron(_FLEET_YAML)
    resp = await cron._web_schedule_pressure(Req(query={}))
    assert resp.status == 200
    body = json.loads(resp.text)
    assert body["hours"] == 24
    assert body["timezone"] == "UTC"
    assert body["jobs"] == 4  # parked + boot excluded
    assert body["excluded"] == {"disabled": 1, "reboot": 1}
    assert body["by_minute_jobs"][0] == 3  # the herd (semantic @hourly too)
    assert len(body["grid"]) == 24 and len(body["grid"][0]) == 60
    # the H job fired somewhere: total fires exceed the herd's own
    assert body["total_fires"] > body["by_minute_fires"][0]
    resp = await cron._web_schedule_pressure(Req(query={"tz": "Nope/Zone"}))
    assert resp.status == 400


async def test_schedule_pressure_hours_are_clamped():
    cron = _cron(_FLEET_YAML)
    resp = await cron._web_schedule_pressure(Req(query={"hours": "9999"}))
    assert json.loads(resp.text)["hours"] == 168
    resp = await cron._web_schedule_pressure(Req(query={"hours": "junk"}))
    assert json.loads(resp.text)["hours"] == 24


async def test_schedule_duplicates_endpoint_groups_semantically():
    cron = _cron(_FLEET_YAML)
    resp = await cron._web_schedule_duplicates(Req())
    assert resp.status == 200
    body = json.loads(resp.text)
    assert body["jobs"] == 4
    assert len(body["groups"]) == 1
    group = body["groups"][0]
    assert group["count"] == 3
    assert group["jobs"] == ["herd-a", "herd-b", "herd-c"]
    assert group["timezone"] == "UTC"
    assert group["description"]


async def test_schedule_suggest_endpoint_and_validation():
    cron = _cron(_FLEET_YAML)
    resp = await cron._web_schedule_suggest(Req(query={}))
    assert resp.status == 200
    body = json.loads(resp.text)
    assert body["period"] == "hourly"
    assert body["minute"] != 0  # never lands on the herd
    assert body["hash_hint"] == "H * * * *"
    resp = await cron._web_schedule_suggest(Req(query={"period": "daily"}))
    assert json.loads(resp.text)["period"] == "daily"
    resp = await cron._web_schedule_suggest(Req(query={"period": "weekly"}))
    assert resp.status == 400
    resp = await cron._web_schedule_suggest(Req(query={"tz": "Nope/Zone"}))
    assert resp.status == 400


async def test_schedule_preview_seed_resolves_h():
    cron = _cron(_RETRY_JOB)
    resp = await cron._web_schedule_preview(
        Req(query={"expr": "H H * * *", "seed": "report-gen"})
    )
    body = json.loads(resp.text)
    assert body["valid"] is True
    assert body["seed"] == "report-gen"
    assert body["resolved"] == "43 9 * * *"  # pinned by test_cronexpr
    assert "hashed from the job name" in body["description"]
    assert "hashed-slot" in [f["code"] for f in body["lint"]]
    # without a seed the engine's own error explains what is missing
    resp = await cron._web_schedule_preview(Req(query={"expr": "H * * * *"}))
    body = json.loads(resp.text)
    assert body["valid"] is False
    assert "hash key" in body["error"]


def test_jobs_payload_ships_schedule_resolved_only_for_h():
    cron = _cron(_FLEET_YAML)
    jobs = {j["name"]: j for j in cron.jobs_payload()}
    assert jobs["spread"]["schedule"] == "H * * * *"
    resolved = jobs["spread"]["schedule_resolved"]
    assert resolved.endswith(" * * * *") and resolved[0].isdigit()
    assert "schedule_resolved" not in jobs["herd-a"]
    assert "hashed-slot" in [
        f["code"] for f in jobs["spread"]["schedule_findings"]
    ]


# ---------------------------------------------------------------------------
# the iCal feed: /calendar.ics and /jobs/{name}/calendar.ics
# ---------------------------------------------------------------------------

_CAL_YAML = """
jobs:
  - name: monthly-close
    command: "true"
    schedule: "30 1 LW * *"
    utc: true
  - name: third-friday-report
    command: "true"
    schedule: "0 2 * * 5#3"
    utc: true
  - name: parked
    command: "true"
    schedule: "0 0 * * *"
    enabled: false
  - name: boot
    command: "true"
    schedule: "@reboot"
"""

_CAL_START = datetime.datetime(2026, 7, 1, tzinfo=_UTC)
_CAL_NOW = datetime.datetime(2026, 7, 18, 12, 0, tzinfo=_UTC)


def test_calendar_payload_fleet_content_and_determinism():
    cron = _cron(_CAL_YAML)
    text = cron.calendar_payload(days=35, start=_CAL_START, now=_CAL_NOW)
    assert text.startswith("BEGIN:VCALENDAR\r\n")
    assert text.endswith("END:VCALENDAR\r\n")
    assert "\n" not in text.replace("\r\n", "")  # CRLF-only
    # July 2026: LW is Friday the 31st, the 3rd Friday is the 17th; both
    # engine-enumerated, both in UTC
    assert "DTSTART:20260731T013000Z" in text
    assert "DTSTART:20260717T020000Z" in text
    # disabled and @reboot jobs never become events
    assert "SUMMARY:parked" not in text
    assert "SUMMARY:boot" not in text
    # a regenerated feed is byte-identical (stable UIDs, pinned DTSTAMP),
    # so subscribed clients update in place instead of duplicating
    again = cron.calendar_payload(days=35, start=_CAL_START, now=_CAL_NOW)
    assert again == text


def test_calendar_payload_per_job_feed_and_unknown():
    cron = _cron(_CAL_YAML)
    text = cron.calendar_payload(
        "monthly-close", days=35, start=_CAL_START, now=_CAL_NOW
    )
    assert "X-WR-CALNAME:cronstable: monthly-close" in text
    assert text.count("BEGIN:VEVENT") == 1
    assert "SUMMARY:third-friday-report" not in text
    assert cron.calendar_payload("nope", start=_CAL_START) is None
    # a known job with no timetable renders as a valid, empty calendar
    boot = cron.calendar_payload("boot", start=_CAL_START, now=_CAL_NOW)
    assert boot.count("BEGIN:VEVENT") == 0
    assert boot.startswith("BEGIN:VCALENDAR\r\n")


def test_calendar_payload_uses_run_history_for_block_length():
    cron = _cron(_CAL_YAML)
    cron.run_history["monthly-close"] = [_run("success", dur=520.0)]
    text = cron.calendar_payload(
        "monthly-close", days=35, start=_CAL_START, now=_CAL_NOW
    )
    assert "DURATION:PT9M" in text
    assert "Typical runtime: 9m" in text.replace("\r\n ", "")


async def test_calendar_endpoint_headers_and_status():
    cron = _cron(_CAL_YAML)
    resp = await cron._web_calendar(Req(query={}))
    assert resp.status == 200
    assert resp.content_type == "text/calendar"
    assert resp.charset == "utf-8"
    assert (
        resp.headers["Content-Disposition"]
        == 'inline; filename="cronstable.ics"'
    )
    assert "BEGIN:VCALENDAR" in resp.text


async def test_job_calendar_endpoint_and_404():
    cron = _cron(_CAL_YAML)
    # 60 days always contains a next "third Friday", whatever today is
    resp = await cron._web_job_calendar(
        Req(query={"days": "60"}, match={"name": "third-friday-report"})
    )
    assert resp.status == 200
    assert "SUMMARY:third-friday-report" in resp.text
    assert resp.text.count("BEGIN:VEVENT") >= 1
    with pytest.raises(web.HTTPNotFound):
        await cron._web_job_calendar(Req(match={"name": "nope"}))


async def test_calendar_query_params_are_clamped():
    cron = _cron(_CAL_YAML)
    # junk and out-of-range values fall back / clamp instead of erroring
    resp = await cron._web_calendar(
        Req(query={"days": "junk", "per_job": "999999"})
    )
    assert resp.status == 200


# ---------------------------------------------------------------------------
# bearer auth: the .ics query-token carve-out
# ---------------------------------------------------------------------------


class _AuthReq:
    def __init__(self, path, headers=None, query=None):
        self.path = path
        self.headers = headers or {}
        self.query = query or {}


async def _run_auth(middleware, request):
    async def handler(_request):
        return "ok"

    return await middleware(request, handler)


async def test_auth_middleware_accepts_query_token_on_ics_only():
    mw = Cron._make_auth_middleware("sekrit", frozenset())
    # the normal path: bearer header
    assert (
        await _run_auth(
            mw,
            _AuthReq("/jobs", headers={"Authorization": "Bearer sekrit"}),
        )
        == "ok"
    )
    # calendar clients: ?token= on .ics paths
    assert (
        await _run_auth(
            mw, _AuthReq("/calendar.ics", query={"token": "sekrit"})
        )
        == "ok"
    )
    assert (
        await _run_auth(
            mw,
            _AuthReq(
                "/jobs/backup/calendar.ics", query={"token": "sekrit"}
            ),
        )
        == "ok"
    )
    # a wrong or missing query token still refuses
    with pytest.raises(web.HTTPUnauthorized):
        await _run_auth(
            mw, _AuthReq("/calendar.ics", query={"token": "wrong"})
        )
    with pytest.raises(web.HTTPUnauthorized):
        await _run_auth(mw, _AuthReq("/calendar.ics"))
    # the carve-out is for .ics ONLY: query tokens on API paths refuse,
    # keeping tokens out of URLs everywhere else
    with pytest.raises(web.HTTPUnauthorized):
        await _run_auth(mw, _AuthReq("/jobs", query={"token": "sekrit"}))
