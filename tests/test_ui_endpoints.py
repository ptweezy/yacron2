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
import types

import pytest
from aiohttp import web

from cronstable.config import ConfigError
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


# ---------------------------------------------------------------------------
# GET /jobs/{name}: single-job detail (was MCP-only)
# ---------------------------------------------------------------------------


async def test_web_get_job_returns_same_shape_as_list():
    cron = _cron(_DEAD_JOB)
    resp = await cron._web_get_job(Req(match={"name": "live"}))
    body = json.loads(resp.text)
    expected = cron._job_to_dict("live", cron.cron_jobs["live"])
    # identical per-job shape to an entry in GET /jobs (the live countdown
    # ticks between the two builds, so compare every key but that one).
    assert body.pop("scheduled_in") == pytest.approx(
        expected.pop("scheduled_in"), abs=1.0
    )
    assert body == expected
    assert body["name"] == "live"


async def test_web_get_job_unknown_is_404():
    cron = _cron(_DEAD_JOB)
    with pytest.raises(web.HTTPNotFound):
        await cron._web_get_job(Req(match={"name": "nope"}))


# ---------------------------------------------------------------------------
# GET /summary: one batched fleet overview for widgets
# ---------------------------------------------------------------------------

_SUMMARY_JOBS = """
jobs:
  - name: a
    command: echo hi
    schedule: "*/5 * * * *"
  - name: b
    command: echo hi
    schedule: "*/10 * * * *"
  - name: c
    command: echo hi
    schedule: "* * * * *"
    enabled: false
  - name: dead
    command: echo hi
    schedule: "0 0 1 1 * 2020"
"""


async def test_summary_payload_counts_and_next_fire():
    import cronstable.version

    cron = _cron(_SUMMARY_JOBS)
    cron._ensure_seeded(datetime.datetime.now(_UTC))
    # b is running (so it drops out of "next fire"); a's last run failed.
    cron.running_jobs["b"] = [types.SimpleNamespace(proc=None)]
    cron.last_run["a"] = _run("failure")

    summary = cron.summary_payload()
    assert summary["version"] == cronstable.version.version
    assert summary["node_name"] == cron._state_host
    assert summary["jobs"] == {
        "total": 4,
        "enabled": 3,  # a, b, dead (c is disabled)
        "disabled": 1,
        "running": 1,  # b
        "paused": 0,
        "failing": 1,  # a's last run failed
        "never_fires": 1,  # dead
    }
    # a fires every 5 min; b is running, c disabled, dead never fires -> a wins.
    assert summary["next_fire"]["job"] == "a"
    assert summary["next_fire"]["in"] >= 0
    assert summary["next_fire"]["at"].endswith("+00:00")
    # no cluster section configured
    assert summary["cluster"] == {"enabled": False}
    # no dags configured -> the dags block is omitted (lean)
    assert "dags" not in summary


async def test_summary_paused_counts_and_next_fire_skips_covered_fire():
    from cronstable.cron import PauseInfo

    now = datetime.datetime.now(_UTC)
    cron = _cron(_SUMMARY_JOBS)
    cron._ensure_seeded(now)
    # deterministic fire instants: a in ~250s, b in ~400s.
    cron._next_fire["a"] = now + datetime.timedelta(seconds=250)
    cron._next_fire["b"] = now + datetime.timedelta(seconds=400)

    def _pause(until_seconds):
        return PauseInfo(
            since=now,
            until=now + datetime.timedelta(seconds=until_seconds),
            note="",
            by="t",
            channel="api",
        )

    # the pause covers a's fire (300 > 250): that slot is skipped at the
    # gate, so a must not be reported as the fleet's next fire; b wins.
    cron._paused["a"] = _pause(300)
    summary = cron.summary_payload()
    assert summary["jobs"]["paused"] == 1
    assert summary["next_fire"]["job"] == "b"
    # the pause lifts before the fire (100 < 250): the fire happens, so a
    # still wins while still counting as paused right now.
    cron._paused["a"] = _pause(100)
    summary = cron.summary_payload()
    assert summary["jobs"]["paused"] == 1
    assert summary["next_fire"]["job"] == "a"


async def test_web_get_summary_endpoint_returns_json():
    cron = _cron(_SUMMARY_JOBS)
    resp = await cron._web_get_summary(Req())
    body = json.loads(resp.text)
    assert body["jobs"]["total"] == 4
    assert body["generated_at"].endswith("+00:00")


_NO_COUNTDOWN_JOBS = """
jobs:
  - name: parked
    command: echo hi
    schedule: "0 0 1 1 * 2020"
  - name: boot
    command: echo hi
    schedule: "@reboot"
"""


async def test_summary_next_fire_null_when_nothing_fires():
    cron = _cron(_NO_COUNTDOWN_JOBS)
    summary = cron.summary_payload()
    # parked never fires (past year) and @reboot has no countdown -> no soonest.
    assert summary["next_fire"] is None
    assert summary["jobs"]["never_fires"] == 1  # parked; @reboot is not "dead"


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


# ===========================================================================
# Direct-call payload builders behind the web endpoints: schedule explainers,
# fleet analysis, next-fire / never-fires helpers, the metadata-only state
# inspector, and a handful of small web helpers.  Driven with a tiny in-memory
# config and lightweight fakes -- no live cluster, lease, or network.
# ===========================================================================

# ---------------------------------------------------------------------------
# schedule_why_payload: the resolved-source + previous/next-fire tail
# ---------------------------------------------------------------------------

_WHY_PAYLOAD_YAML = """
jobs:
  - name: weekday
    command: echo hi
    schedule: "0 9 * * mon,fri"
    utc: true
  - name: hashed
    command: echo hi
    schedule: "H 9 * * *"
    utc: true
"""


def test_schedule_why_payload_reports_neighbouring_fires():
    cron = _cron(_WHY_PAYLOAD_YAML)
    # a Wednesday 09:00 UTC: no match, but real fires exist on both sides
    payload = cron.schedule_why_payload("weekday", "2026-07-15T09:00:00")
    assert payload is not None
    assert payload["expression"] == "0 9 * * mon,fri"
    assert payload["reboot"] is False
    assert payload["matches"] is False
    # previous fire = Mon 2026-07-13, next fire = Fri 2026-07-17 (both 09:00Z)
    assert payload["previous_fire"] == "2026-07-13T09:00:00+00:00"
    assert payload["next_fire"] == "2026-07-17T09:00:00+00:00"
    # a real (non-@reboot) schedule always carries the field-by-field checks
    assert payload["checks"]


def test_schedule_why_payload_surfaces_resolved_hash_source():
    cron = _cron(_WHY_PAYLOAD_YAML)
    payload = cron.schedule_why_payload("hashed", "2026-07-15T09:00:00")
    assert payload is not None
    # an H schedule resolves to a concrete minute; the payload exposes it
    assert payload["expression"] == "H 9 * * *"
    assert payload["resolved"] != "H 9 * * *"
    assert payload["resolved"].endswith(" 9 * * *")
    assert payload["resolved"].split()[0].isdigit()


def test_schedule_why_payload_unknown_job_is_none():
    cron = _cron(_WHY_PAYLOAD_YAML)
    assert cron.schedule_why_payload("ghost", "2026-07-15T09:00:00") is None


# ---------------------------------------------------------------------------
# fleet-analysis payloads built with their own entries snapshot
# ---------------------------------------------------------------------------

_FLEET_PAYLOAD_YAML = """
jobs:
  - name: herd-a
    command: "true"
    schedule: "0 * * * *"
  - name: herd-b
    command: "true"
    schedule: "0 * * * *"
  - name: parked
    command: "true"
    schedule: "0 0 * * *"
    enabled: false
  - name: boot
    command: "true"
    schedule: "@reboot"
"""


def test_schedule_pressure_payload_builds_own_entries():
    cron = _cron(_FLEET_PAYLOAD_YAML)
    payload = cron.schedule_pressure_payload(hours=24)
    assert payload["excluded"] == {"disabled": 1, "reboot": 1}
    assert len(payload["grid"]) == 24
    assert len(payload["grid"][0]) == 60
    # both herd jobs land on minute 0
    assert payload["by_minute_jobs"][0] == 2


def test_schedule_duplicates_payload_groups_the_herd():
    cron = _cron(_FLEET_PAYLOAD_YAML)
    payload = cron.schedule_duplicates_payload()
    assert payload["jobs"] == 2  # only the two enabled cron jobs
    assert len(payload["groups"]) == 1
    assert payload["groups"][0]["jobs"] == ["herd-a", "herd-b"]


def test_schedule_suggest_payload_avoids_the_herd():
    cron = _cron(_FLEET_PAYLOAD_YAML)
    payload = cron.schedule_suggest_payload(period="hourly")
    assert payload["period"] == "hourly"
    assert payload["minute"] != 0  # never lands on the busy slot


def test_schedule_suggest_payload_rejects_bad_period():
    cron = _cron(_FLEET_PAYLOAD_YAML)
    with pytest.raises(ValueError):
        cron.schedule_suggest_payload(period="weekly")


# ---------------------------------------------------------------------------
# _scheduled_in / _schedule_never_fires: index vs dead-latch, once seeded
# ---------------------------------------------------------------------------

_DEAD_YAML = """
jobs:
  - name: live
    command: echo hi
    schedule: "*/5 * * * *"
  - name: parked
    command: echo hi
    schedule: "0 0 1 1 * 2020"
  - name: off
    command: echo hi
    schedule: "*/5 * * * *"
    enabled: false
"""


def test_scheduled_in_reads_index_and_dead_latch():
    cron = _cron(_DEAD_YAML)
    now = datetime.datetime.now(_UTC)
    cron._ensure_seeded(now)
    # a live schedule is read from the fire index, not re-walked: */5 puts the
    # next fire within 300s. (A bare `>= 0.0` here asserted nothing: the value
    # is naturally positive just after seeding, so it never reached the
    # max(0.0, ...) clamp it looked like it was testing.)
    live = cron._scheduled_in("live", cron.cron_jobs["live"], False)
    assert live is not None and 0.0 <= live <= 300.0
    # a fire time already in the past clamps to zero rather than going
    # negative: this is the clamp branch.
    cron._next_fire["live"] = now - datetime.timedelta(seconds=90)
    assert cron._scheduled_in("live", cron.cron_jobs["live"], False) == 0.0
    cron._ensure_seeded(now)
    # a dead schedule sits in the dead-latch: no next run
    assert cron._scheduled_in("parked", cron.cron_jobs["parked"], False) is None
    # disabled / running jobs short-circuit to None
    assert cron._scheduled_in("off", cron.cron_jobs["off"], False) is None
    assert cron._scheduled_in("live", cron.cron_jobs["live"], True) is None


def test_schedule_never_fires_index_vs_latch():
    cron = _cron(_DEAD_YAML)
    # before seeding: enabled crontab job falls through to the engine search
    assert cron._schedule_never_fires("live", cron.cron_jobs["live"]) is False
    now = datetime.datetime.now(_UTC)
    cron._ensure_seeded(now)
    # after seeding the two probes decide it: index -> fires, latch -> never
    assert cron._schedule_never_fires("live", cron.cron_jobs["live"]) is False
    assert cron._schedule_never_fires("parked", cron.cron_jobs["parked"]) is True
    # a disabled job is never "never fires" (it simply does not schedule)
    assert cron._schedule_never_fires("off", cron.cron_jobs["off"]) is False


# ---------------------------------------------------------------------------
# _job_to_dict: the spread-distribution cluster-owner block
# ---------------------------------------------------------------------------

_CLUSTER_YAML = """
jobs:
  - name: leader
    command: echo hi
    schedule: "*/5 * * * *"
    clusterPolicy: Leader
  - name: prefer
    command: echo hi
    schedule: "*/5 * * * *"
    clusterPolicy: PreferLeader
"""


def test_job_to_dict_spread_owner_block():
    cron = _cron(_CLUSTER_YAML)
    cron._elect_leader_configured = True
    cron.cluster_manager = types.SimpleNamespace(
        distribution="spread",
        job_owner=lambda n: "node-A",
        available_job_owner=lambda n: "node-B",
    )
    leader = cron._job_to_dict("leader", cron.cron_jobs["leader"])
    assert leader["clusterPolicy"] == "Leader"
    assert leader["clusterOwner"] == "node-A"  # Leader -> job_owner
    prefer = cron._job_to_dict("prefer", cron.cron_jobs["prefer"])
    assert prefer["clusterPolicy"] == "PreferLeader"
    assert prefer["clusterOwner"] == "node-B"  # PreferLeader -> available_


# ---------------------------------------------------------------------------
# state inspector payloads, driven by small fake backends
# ---------------------------------------------------------------------------


class _FakeBackend:
    """A minimal state backend: only the inspector methods are exercised."""

    def __init__(self, *, inventory=None, documents=None, records=None):
        self._inventory = inventory
        self._documents = documents
        self._records = records

    def view_dict(self):
        return {"backend": "fake"}

    def stats(self):
        return {"ok": True}

    async def inventory(self):
        if isinstance(self._inventory, Exception):
            raise self._inventory
        return dict(self._inventory)

    async def list_documents(self, ns):
        if isinstance(self._documents, Exception):
            raise self._documents
        return list(self._documents)

    async def list_records(self, stream, limit, newest_first):
        if isinstance(self._records, Exception):
            raise self._records
        return list(self._records)


async def test_state_payload_degrades_when_inventory_fails():
    cron = _cron(_DEAD_YAML)
    cron.state_backend = _FakeBackend(inventory=RuntimeError("boom"))
    payload = await cron.state_payload()
    # the health-only fallback: enumerable False, empty maps, still enabled
    assert payload["enabled"] is True
    assert payload["enumerable"] is False
    assert payload["records"] == {} and payload["documents"] == {}
    assert payload["view"] == {"backend": "fake"}
    # this node's own memory state is always grafted on
    assert payload["node"]["host"] == cron._state_host


async def test_state_payload_grafts_node_retries_and_slots():
    from cronstable.job import JobRetryState

    cron = _cron(_DEAD_YAML)
    cron.state_backend = _FakeBackend(
        inventory={
            "view": {"backend": "fake"},
            "stats": {},
            "enumerable": True,
            "records": {},
            "documents": {},
            "leases": [],
        }
    )
    st = JobRetryState(8.0, 2.0, 60.0)
    st.count = 3
    st.next_retry_at = datetime.datetime.now(_UTC)
    st.scheduled_delay = 12.0
    cron.retry_state["live"] = st
    cron._slot_leases["live"] = Lease("slots/live", "host#tok", 1, 9e18)
    cron._slot_refs["live"] = 2
    payload = await cron.state_payload()
    retries = {r["job"]: r for r in payload["node"]["retries"]}
    assert retries["live"]["attempt"] == 3
    assert retries["live"]["delaySeconds"] == 12.0
    slots = {s["slot"]: s for s in payload["node"]["slots"]}
    assert slots["live"]["holder"] == "host#tok"
    assert slots["live"]["refs"] == 2


async def test_state_documents_payload_redacts_kv_values():
    cron = _cron(_DEAD_YAML)
    cron.state_backend = _FakeBackend(
        documents=[
            {"key": "a", "value": {"pw": "hunter2"}},
            # a non-JSON-serializable value: valueSize degrades to None
            {"key": "b", "value": {1, 2, 3}},
        ]
    )
    payload = await cron.state_documents_payload("kv/scope")
    doc = payload["documents"][0]
    assert "value" not in doc  # the secret is stripped
    assert doc["valueType"] == "dict"
    assert doc["valueSize"] > 0
    assert payload["namespace"] == "kv/scope"
    unserializable = payload["documents"][1]
    assert "value" not in unserializable
    assert unserializable["valueType"] == "set"
    assert unserializable["valueSize"] is None


async def test_state_documents_payload_passes_cursor_docs_verbatim():
    cron = _cron(_DEAD_YAML)
    cron.state_backend = _FakeBackend(
        documents=[{"key": "c", "value": "2026-07-01"}]
    )
    # a non-kv namespace is NOT redacted: the watermark rides through
    payload = await cron.state_documents_payload("cursor/scope")
    assert payload["documents"][0]["value"] == "2026-07-01"


async def test_state_documents_payload_rejects_and_degrades():
    cron = _cron(_DEAD_YAML)
    good = _FakeBackend(documents=RuntimeError("read failed"))
    cron.state_backend = good
    # a backend read error degrades to an empty list, not a raise
    payload = await cron.state_documents_payload("kv/scope")
    assert payload["documents"] == []
    # a bad namespace is a 400 ApiActionError
    from cronstable.cron import ApiActionError

    with pytest.raises(ApiActionError):
        await cron.state_documents_payload("runs/etl")
    # no store configured is a 404 ApiActionError
    cron.state_backend = None
    with pytest.raises(ApiActionError):
        await cron.state_documents_payload("kv/scope")


async def test_state_records_payload_guards_and_degrades():
    from cronstable.cron import ApiActionError

    cron = _cron(_DEAD_YAML)
    cron.state_backend = _FakeBackend(records=[{"seq": 1}, {"seq": 2}])
    payload = await cron.state_records_payload("runs/live", limit=10)
    assert payload["stream"] == "runs/live"
    assert payload["records"] == [{"seq": 1}, {"seq": 2}]
    # a log stream is forbidden (raw job output)
    with pytest.raises(ApiActionError):
        await cron.state_records_payload("logs/live")
    # an empty stream name is a 400
    with pytest.raises(ApiActionError):
        await cron.state_records_payload("")
    # a backend error degrades to []
    cron.state_backend = _FakeBackend(records=RuntimeError("boom"))
    degraded = await cron.state_records_payload("runs/live")
    assert degraded["records"] == []
    # no store is a 404
    cron.state_backend = None
    with pytest.raises(ApiActionError):
        await cron.state_records_payload("runs/live")


# ---------------------------------------------------------------------------
# job_resources_payload / job_runs_payload / job_trends_payload edges
# ---------------------------------------------------------------------------

_RES_YAML = """
jobs:
  - name: heavy
    command: echo hi
    schedule: "*/5 * * * *"
    monitorResources:
      interval: 0.5
      history: 50
"""


def test_job_resources_payload_collects_live_series():
    cron = _cron(_RES_YAML)

    class _Running:
        proc = types.SimpleNamespace(pid=4321)
        started_at = datetime.datetime.now(_UTC)

        def live_resource_series(self):
            return [[1.0, 5.0, 1024]]

        def live_resources(self):
            return {"cpu_percent": 10.0}

    cron.running_jobs["heavy"] = [_Running()]
    payload = cron.job_resources_payload("heavy", max_runs=20)
    assert payload["monitored"] is True
    assert payload["interval"] == 0.5
    assert len(payload["live"]) == 1
    live = payload["live"][0]
    assert live["pid"] == 4321
    assert live["series"] == [[1.0, 5.0, 1024]]
    assert live["current"] == {"cpu_percent": 10.0}


def test_job_resources_payload_skips_unsampled_instances():
    cron = _cron(_RES_YAML)

    class _Quiet:
        proc = None
        started_at = None

        def live_resource_series(self):
            return None

        def live_resources(self):
            return None

    cron.running_jobs["heavy"] = [_Quiet()]
    payload = cron.job_resources_payload("heavy", max_runs=20)
    assert payload["live"] == []  # the unsampled instance is filtered out


def test_job_resources_payload_unknown_job_is_none():
    cron = _cron(_RES_YAML)
    assert cron.job_resources_payload("ghost", max_runs=5) is None


def test_job_runs_payload_unknown_job_is_none():
    cron = _cron(_RES_YAML)
    assert cron.job_runs_payload("ghost") is None
    payload = cron.job_runs_payload("heavy")
    assert payload["name"] == "heavy"
    assert payload["runs"] == []
    assert payload["stats"]["total"] == 0


async def test_job_trends_payload_unknown_job_is_none():
    cron = _cron(_RES_YAML)
    assert await cron.job_trends_payload("ghost") is None


# ---------------------------------------------------------------------------
# small web helpers: _web_int_query, _web_json_body
# ---------------------------------------------------------------------------


def test_web_int_query_defaults_and_clamps():
    q = Cron._web_int_query
    assert q(Req(), "n", default=7, lo=1, hi=100) == 7  # missing -> default
    assert q(Req({"n": "20"}), "n", default=7, lo=1, hi=100) == 20
    assert q(Req({"n": "500"}), "n", default=7, lo=1, hi=100) == 100  # hi clamp
    assert q(Req({"n": "-3"}), "n", default=7, lo=1, hi=100) == 1  # lo clamp
    assert q(Req({"n": "bad"}), "n", default=7, lo=1, hi=100) == 7  # unparsable


class _BodyReq:
    def __init__(self, *, can_read, body=None, raises=None):
        self.can_read_body = can_read
        self._body = body
        self._raises = raises

    async def json(self):
        if self._raises is not None:
            raise self._raises
        return self._body


async def test_web_json_body_variants():
    body = Cron._web_json_body
    # no body to read -> empty dict
    assert await body(_BodyReq(can_read=False)) == {}
    # a JSON object round-trips
    assert await body(
        _BodyReq(can_read=True, body={"decision": "approve"})
    ) == {"decision": "approve"}
    # malformed JSON -> 400
    with pytest.raises(web.HTTPBadRequest):
        await body(_BodyReq(can_read=True, raises=ValueError("bad json")))
    # a non-object JSON body -> 400
    with pytest.raises(web.HTTPBadRequest):
        await body(_BodyReq(can_read=True, body=[1, 2, 3]))


# ---------------------------------------------------------------------------
# header builders merge operator-supplied web.headers
# ---------------------------------------------------------------------------


def test_security_and_sse_headers_merge_custom():
    cron = _cron(_RES_YAML)
    cron.web_config = {"headers": {"X-Custom": "yes"}}
    sec = cron._security_headers()
    assert sec["X-Custom"] == "yes"
    assert any(k.lower() == "content-security-policy" for k in sec)
    sse = cron._sse_headers()
    assert sse["Content-Type"] == "text/event-stream"
    assert sse["X-Custom"] == "yes"


# ---------------------------------------------------------------------------
# _resolve_web_token: the fromFile read-error path
# ---------------------------------------------------------------------------


def test_resolve_web_token_missing_file_fails_closed(tmp_path):
    missing = tmp_path / "nope" / "token"  # parent dir does not exist either
    with pytest.raises(ConfigError):
        Cron._resolve_web_token(
            {
                "authToken": {
                    "value": None,
                    "fromFile": str(missing),
                    "fromEnvVar": None,
                }
            }
        )


def test_resolve_web_token_reads_file(tmp_path):
    tok = tmp_path / "token"
    tok.write_text("s3cret\n")
    assert (
        Cron._resolve_web_token(
            {
                "authToken": {
                    "value": None,
                    "fromFile": str(tok),
                    "fromEnvVar": None,
                }
            }
        )
        == "s3cret"
    )


# ---------------------------------------------------------------------------
# _apply_socket_mode: non-unix early-out + chmod failure warning
# ---------------------------------------------------------------------------


def test_apply_socket_mode_ignores_non_unix(monkeypatch, caplog):
    # a TCP listen url is a no-op (returns without touching the filesystem).
    # Asserted by making any chmod fatal: the scheme guard is what has to keep
    # us out of it. Merely CALLING _apply_socket_mode proves nothing, since
    # the chmod that a missing guard would reach is itself wrapped in a
    # try/except that swallows the resulting OSError.
    import logging
    import os as _os

    def boom(*a, **kw):
        raise AssertionError("chmod attempted for a non-unix listen url")

    monkeypatch.setattr(_os, "chmod", boom)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        Cron._apply_socket_mode("http://127.0.0.1:8080", "600")
    assert caplog.records == []


def test_apply_socket_mode_warns_on_chmod_failure(caplog):
    import logging

    with caplog.at_level(logging.WARNING, logger="cronstable"):
        # a unix path that does not exist: os.chmod raises OSError, caught
        Cron._apply_socket_mode("unix:///no/such/socket.sock", "600")
    assert any("socketMode" in r.getMessage() for r in caplog.records)


def test_apply_socket_mode_warns_on_bad_mode(caplog):
    import logging

    with caplog.at_level(logging.WARNING, logger="cronstable"):
        # a non-octal mode raises ValueError from int(mode, 8), also caught
        Cron._apply_socket_mode("unix:///tmp/whatever.sock", "not-octal")
    assert any("socketMode" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# _artifact_scope_names: global + per-job + per-dag-task scopes
# ---------------------------------------------------------------------------

_SCOPE_YAML = """
state:
  path: {path}
jobs:
  - name: writer
    command: echo hi
    schedule: "*/5 * * * *"
    stateAllowedScopes:
      - shared-a
dags:
  - name: pipe
    schedule: "0 4 * * *"
    tasks:
      - id: t
        command: 'true'
        stateAllowedScopes:
          - shared-b
"""


# ---------------------------------------------------------------------------
# POST /jobs/{name}/pause + /resume: happy paths, 400 shapes, 404, idempotency
# ---------------------------------------------------------------------------

from cronstable.cron import (  # noqa: E402 - grouped with its test section
    PAUSE_BY_MAX,
    PAUSE_DEFAULT_SECONDS,
    PAUSE_MAX_SECONDS,
    PAUSE_NOTE_MAX,
)

_PAUSE_YAML = """
jobs:
  - name: p
    command: echo hi
    schedule: "* * * * *"
"""


class _PauseReq:
    """A minimal aiohttp request stand-in with a match slot and JSON body."""

    def __init__(self, name, body=None):
        self.match_info = {"name": name}
        self._body = body
        self.can_read_body = body is not None

    async def json(self):
        return self._body


async def test_web_pause_defaults_to_default_duration():
    cron = _cron(_PAUSE_YAML)
    resp = await cron._web_pause_job(_PauseReq("p"))
    assert resp.status == 200
    paused = json.loads(resp.body)["paused"]
    since = datetime.datetime.fromisoformat(paused["since"])
    until = datetime.datetime.fromisoformat(paused["until"])
    assert (until - since).total_seconds() == PAUSE_DEFAULT_SECONDS
    assert paused["channel"] == "api"
    assert paused["by"] == "api"
    assert paused["note"] == ""
    assert cron._pause_active("p") is not None


async def test_web_pause_with_duration_note_and_by():
    cron = _cron(_PAUSE_YAML)
    resp = await cron._web_pause_job(
        _PauseReq(
            "p", {"durationSeconds": 120, "note": "maint", "by": "parker"}
        )
    )
    paused = json.loads(resp.body)["paused"]
    since = datetime.datetime.fromisoformat(paused["since"])
    until = datetime.datetime.fromisoformat(paused["until"])
    assert (until - since).total_seconds() == 120
    assert paused["note"] == "maint"
    assert paused["by"] == "parker"


async def test_web_pause_with_until():
    cron = _cron(_PAUSE_YAML)
    until = datetime.datetime.now(_UTC) + datetime.timedelta(seconds=600)
    resp = await cron._web_pause_job(
        _PauseReq("p", {"until": until.isoformat()})
    )
    paused = json.loads(resp.body)["paused"]
    assert paused["until"] == until.isoformat()


async def test_web_pause_400_shapes():
    cron = _cron(_PAUSE_YAML)
    now = datetime.datetime.now(_UTC)
    future = (now + datetime.timedelta(seconds=600)).isoformat()
    past = (now - datetime.timedelta(seconds=600)).isoformat()
    far = (
        now + datetime.timedelta(seconds=PAUSE_MAX_SECONDS + 3600)
    ).isoformat()
    bad_bodies = [
        # both keys are exclusive
        {"durationSeconds": 60, "until": future},
        # past until
        {"until": past},
        # out of range
        {"durationSeconds": 0},
        {"durationSeconds": PAUSE_MAX_SECONDS + 1},
        {"until": far},
        # oversized audit fields
        {"note": "x" * (PAUSE_NOTE_MAX + 1)},
        {"by": "x" * (PAUSE_BY_MAX + 1)},
        # bad types (bool is an int subclass and must not read as 1 second)
        {"durationSeconds": "60"},
        {"durationSeconds": True},
        {"until": 12345},
        {"until": "not-a-timestamp"},
        {"note": 7},
        {"by": ["ops"]},
    ]
    for body in bad_bodies:
        with pytest.raises(web.HTTPBadRequest):
            await cron._web_pause_job(_PauseReq("p", body))
    assert cron._pause_active("p") is None  # nothing invalid stuck


async def test_web_pause_and_resume_unknown_job_404():
    cron = _cron(_PAUSE_YAML)
    with pytest.raises(web.HTTPNotFound):
        await cron._web_pause_job(_PauseReq("ghost"))
    with pytest.raises(web.HTTPNotFound):
        await cron._web_resume_job(_PauseReq("ghost"))


async def test_web_pause_is_idempotent_overwrite():
    cron = _cron(_PAUSE_YAML)
    resp = await cron._web_pause_job(_PauseReq("p", {"durationSeconds": 60}))
    first = json.loads(resp.body)["paused"]
    resp = await cron._web_pause_job(_PauseReq("p", {"durationSeconds": 7200}))
    second = json.loads(resp.body)["paused"]
    # a re-pause overwrites the window (no 409): the live record is the new one
    assert second["until"] > first["until"]
    live = cron._pause_active("p")
    assert live is not None
    assert live.until.isoformat() == second["until"]


async def test_web_resume_clears_and_is_noop_when_not_paused():
    cron = _cron(_PAUSE_YAML)
    await cron._web_pause_job(_PauseReq("p"))
    resp = await cron._web_resume_job(_PauseReq("p", {"by": "parker"}))
    assert resp.status == 200
    assert json.loads(resp.body) == {"paused": None}
    assert cron._pause_active("p") is None
    # resuming a job that is not paused is a 200 no-op, not a conflict
    resp = await cron._web_resume_job(_PauseReq("p"))
    assert json.loads(resp.body) == {"paused": None}


async def test_jobs_payload_carries_paused():
    cron = _cron(_PAUSE_YAML)
    (job,) = cron.jobs_payload()
    assert job["paused"] is None  # always present, null when not paused
    record = await cron.pause_job_by_name(
        "p", duration=300, note="maint", by="parker", channel="api"
    )
    (job,) = cron.jobs_payload()
    assert job["paused"] == record
    assert set(job["paused"]) == {"since", "until", "note", "by", "channel"}


async def test_schedule_why_notes_pause():
    cron = _cron(_PAUSE_YAML)
    await cron.pause_job_by_name("p", duration=300, note="maint", by="parker")
    payload = cron.schedule_why_payload("p", "2026-07-15T09:00:00")
    notes = [n for n in payload["notes"] if n["code"] == "paused"]
    assert len(notes) == 1
    assert "parker" in notes[0]["message"]
    assert "maint" in notes[0]["message"]


def test_artifact_scope_names_unions_all_sources(tmp_path):
    from cronstable.jobstate import GLOBAL_SCOPE

    cron = _cron(_SCOPE_YAML.format(path=str(tmp_path).replace("\\", "/")))
    scopes = cron._artifact_scope_names()
    assert GLOBAL_SCOPE in scopes  # the shared scope is always present
    assert "shared-a" in scopes  # from the job
    assert "shared-b" in scopes  # from the dag task template
