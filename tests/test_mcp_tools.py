"""The MCP server's tool/resource/prompt handlers against a real ``Cron``.

``test_mcp.py`` covers the protocol plumbing (initialize, visibility, the
HTTP transport, config); this file drives the individual tool handlers
through ``MCPHandler.handle_message``: the observe/act surface against a
stateless in-process :class:`~cronstable.cron.Cron`, and the dags/state
surface against a real ``FilesystemStateBackend`` + DAG scheduler, reusing
the ``test_state_dag_run.py`` harness (real task subprocesses via
``[sys.executable, ...]`` argv, a pump instead of the daemon's reaper).
"""

import datetime
import json
import sys

from cronstable import mcp as mcp_mod
from cronstable.config import _build_mcp_config, parse_config_string
from cronstable.cron import PAUSE_DEFAULT_SECONDS, Cron, JobRunInfo
from cronstable.job import JobOutputStream
from cronstable.mcp import MCPHandler
from cronstable.resources import ResourceUsage
from tests.test_state_dag_run import (
    _drive,
    _set_cmd,
    _teardown,
)
from tests.test_state_dag_run import (
    _make_cron as _make_state_cron,
)

_PY = sys.executable
_UTC = datetime.timezone.utc

_YAML = """
jobs:
  - name: hello
    command: echo hi
    schedule: "* * * * *"
  - name: nightly
    command: backup
    schedule: "0 3 * * *"
    enabled: false
  - name: heavy
    command: echo hi
    schedule: "*/5 * * * *"
    monitorResources:
      interval: 0.5
      history: 50
"""

_ALL_TOOLSETS = ["observe", "dags", "state", "act"]


def _handler(mcp=None, yaml=_YAML):
    cron = Cron(None, config_yaml=yaml)
    cron.web_config = {}
    cfg = _build_mcp_config(
        {
            "enabled": True,
            "readOnly": False,
            "toolsets": _ALL_TOOLSETS,
            **(mcp or {}),
        }
    )
    return MCPHandler(cron, cfg)


async def _req(handler, method, params=None, mid=1, notif=False):
    msg = {"jsonrpc": "2.0", "method": method}
    if not notif:
        msg["id"] = mid
    if params is not None:
        msg["params"] = params
    return await handler.handle_message(msg)


async def _call(handler, name, arguments=None):
    resp = await _req(
        handler, "tools/call", {"name": name, "arguments": arguments or {}}
    )
    assert "error" not in resp, resp
    return resp["result"]


def _run_info(outcome, *, dur=1.0, exit_code=0):
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
# module helpers: _dumps fallback, the Prometheus exposition parser
# ---------------------------------------------------------------------------


def test_dumps_falls_back_for_nonportable_values():
    # a non-finite float fails the fleet-portability gate of _json but must
    # not 500 a transient MCP response: the stdlib fallback encodes it.
    assert mcp_mod._dumps(float("inf")) == b"Infinity"
    assert mcp_mod._dumps({"v": 1}) == b'{"v":1}'


_EXPO = """\
# HELP cronstable_job_runs_total Total runs.
# TYPE cronstable_job_runs_total counter
cronstable_job_runs_total{job="hello"} 3
cronstable_job_runs_total{job="nightly"} 1
cronstable_up 1
{not a metric line}
process_cpu_seconds_total NaN
"""


def test_parse_prometheus_filters_and_caps():
    samples, total = mcp_mod._parse_prometheus(_EXPO, "runs_total", 1)
    assert total == 2  # both labeled samples matched...
    assert len(samples) == 1  # ...but the page is capped at limit
    assert samples[0] == {
        "name": "cronstable_job_runs_total",
        "labels": '{job="hello"}',
        "value": "3",
    }


def test_parse_prometheus_unfiltered_keeps_values_as_strings():
    samples, total = mcp_mod._parse_prometheus(_EXPO, None, 100)
    assert total == 4  # comment + malformed lines skipped
    by_name = {s["name"]: s for s in samples}
    assert by_name["cronstable_up"]["labels"] == ""
    # a NaN gauge round-trips untouched because values stay strings
    assert by_name["process_cpu_seconds_total"]["value"] == "NaN"


# ---------------------------------------------------------------------------
# handle_message protocol edges
# ---------------------------------------------------------------------------


async def test_missing_method_request_and_notification():
    h = _handler()
    resp = await h.handle_message({"jsonrpc": "2.0", "id": 1})
    assert resp["error"]["code"] == mcp_mod.INVALID_REQUEST
    assert resp["error"]["message"] == "missing method"
    assert await h.handle_message({"jsonrpc": "2.0"}) is None


async def test_invalid_params_request_and_notification():
    h = _handler()
    resp = await _req(h, "ping", params=[1])
    assert resp["error"]["code"] == mcp_mod.INVALID_PARAMS
    assert await _req(h, "ping", params=[1], notif=True) is None


async def test_tools_call_requires_string_name_and_object_arguments():
    h = _handler()
    resp = await _req(h, "tools/call", {"arguments": {}})
    assert resp["error"]["code"] == mcp_mod.INVALID_PARAMS
    resp = await _req(
        h, "tools/call", {"name": "cron_get_status", "arguments": [1]}
    )
    assert resp["error"]["code"] == mcp_mod.INVALID_PARAMS


async def test_mcp_error_in_notification_is_swallowed():
    h = _handler()
    # a notification whose handler raises MCPError (unknown tool) -> no reply
    resp = await _req(
        h, "tools/call", {"name": "ghost_tool", "arguments": {}}, notif=True
    )
    assert resp is None


async def test_internal_error_envelope_and_notification(monkeypatch):
    h = _handler()

    def boom():
        raise RuntimeError("kaput")

    monkeypatch.setattr(h._cron, "status_payload", boom)
    resp = await _req(
        h, "tools/call", {"name": "cron_get_status", "arguments": {}}
    )
    assert resp["error"]["code"] == mcp_mod.INTERNAL_ERROR
    assert resp["error"]["message"] == "internal error"  # no traceback leak
    resp = await _req(
        h,
        "tools/call",
        {"name": "cron_get_status", "arguments": {}},
        notif=True,
    )
    assert resp is None


# ---------------------------------------------------------------------------
# handle_http edges not covered by the transport tests
# ---------------------------------------------------------------------------


class FakeReq:
    def __init__(self, method="POST", headers=None, body=b""):
        self.method = method
        self.headers = headers or {}
        self._body = body
        self.content_length = len(body) if body else None

    async def read(self):
        return self._body


async def test_http_body_over_limit_after_read():
    h = _handler({"maxBodyBytes": 16})
    req = FakeReq(body=b"x" * 64)
    req.content_length = None  # chunked: only the read can see the size
    resp = await h.handle_http(req)
    assert resp.status == 413


async def test_http_empty_body_rejected():
    h = _handler()
    resp = await h.handle_http(FakeReq(body=b""))
    assert resp.status == 400
    assert b"empty request body" in resp.body


async def test_http_options_rejects_unlisted_and_missing_origin():
    h = _handler({"allowedOrigins": ["http://ok.example"]})
    resp = await h.handle_options(
        FakeReq(headers={"Origin": "http://evil.example"})
    )
    assert resp.status == 403
    resp = await h.handle_options(FakeReq())
    assert resp.status == 405


# ---------------------------------------------------------------------------
# initialize options / capability gating
# ---------------------------------------------------------------------------


async def test_initialize_custom_instructions():
    h = _handler({"instructions": "be careful"})
    resp = await _req(h, "initialize", {"protocolVersion": "2025-11-25"})
    assert resp["result"]["instructions"] == "be careful"


async def test_resources_and_prompts_can_be_disabled():
    cfg = _build_mcp_config({"enabled": True})
    cfg["resources"] = False
    cfg["prompts"] = False
    h = MCPHandler(_handler()._cron, cfg)
    caps = (await _req(h, "initialize", {}))["result"]["capabilities"]
    assert "resources" not in caps
    assert "prompts" not in caps
    resp = await _req(h, "resources/list")
    assert resp["error"]["code"] == mcp_mod.METHOD_NOT_FOUND
    resp = await _req(h, "prompts/list")
    assert resp["error"]["code"] == mcp_mod.METHOD_NOT_FOUND


# ---------------------------------------------------------------------------
# pagination fallbacks
# ---------------------------------------------------------------------------


async def test_page_and_limit_fall_back_on_junk():
    h = _handler()
    result = await _call(h, "cron_get_status", {"limit": "x", "offset": ["y"]})
    meta = result["structuredContent"]["page"]
    assert meta["offset"] == 0
    assert meta["limit"] == 200  # maxRows default
    assert meta["total"] == 3
    assert meta["nextOffset"] is None


async def test_page_next_offset():
    h = _handler()
    result = await _call(h, "cron_get_status", {"limit": 2})
    meta = result["structuredContent"]["page"]
    assert meta["returned"] == 2
    assert meta["nextOffset"] == 2


# ---------------------------------------------------------------------------
# observe tools against the stateless Cron
# ---------------------------------------------------------------------------


async def test_get_status_summary():
    h = _handler()
    result = await _call(h, "cron_get_status")
    assert "3 job(s)" in result["content"][0]["text"]
    names = {r["job"] for r in result["structuredContent"]["status"]}
    assert names == {"hello", "nightly", "heavy"}


async def test_list_jobs_filter_and_states():
    h = _handler()
    result = await _call(h, "cron_list_jobs", {"filter": "NIGHT"})
    rows = result["structuredContent"]["jobs"]
    assert [r["name"] for r in rows] == ["nightly"]
    result = await _call(h, "cron_list_jobs", {"state": "disabled"})
    rows = result["structuredContent"]["jobs"]
    assert [r["name"] for r in rows] == ["nightly"]
    result = await _call(h, "cron_list_jobs", {"state": "scheduled"})
    names = {r["name"] for r in result["structuredContent"]["jobs"]}
    assert names == {"hello", "heavy"}
    result = await _call(h, "cron_list_jobs", {"state": "running"})
    assert result["structuredContent"]["jobs"] == []


async def test_get_job_detail_and_not_found():
    h = _handler()
    result = await _call(h, "cron_get_job", {"name": "hello"})
    assert result["structuredContent"]["name"] == "hello"
    result = await _call(h, "cron_get_job", {"name": "ghost"})
    assert result["isError"] is True
    assert "cron_list_jobs" in result["content"][0]["text"]


async def test_get_job_requires_name():
    h = _handler()
    result = await _call(h, "cron_get_job", {})
    assert result["isError"] is True
    assert "required string argument" in result["content"][0]["text"]


async def test_list_runs_pages_from_the_end():
    h = _handler()
    for outcome in ("success", "failure", "success"):
        h._cron.run_history["hello"].append(_run_info(outcome))
    result = await _call(h, "cron_list_runs", {"name": "hello", "limit": 2})
    body = result["structuredContent"]
    assert body["totalRuns"] == 3
    assert body["returnedRuns"] == 2
    assert "3 run(s) retained, 2 returned" in result["content"][0]["text"]
    result = await _call(h, "cron_list_runs", {"name": "ghost"})
    assert result["isError"] is True


async def test_job_trends_found_and_not_found():
    h = _handler()
    result = await _call(h, "cron_get_job_trends", {"name": "hello"})
    assert result["structuredContent"] is not None
    assert "trends for job" in result["content"][0]["text"]
    result = await _call(h, "cron_get_job_trends", {"name": "ghost"})
    assert result["isError"] is True


async def test_job_resources_series_and_not_found():
    h = _handler()
    info = _run_info("success")
    info.resource_usage = ResourceUsage(
        cpu_user_seconds=1.0,
        cpu_system_seconds=0.5,
        max_rss_bytes=2048,
        samples=1,
        series=[[1.0, 5.0, 1024]],
    )
    h._cron.run_history["heavy"].append(info)
    result = await _call(h, "cron_get_job_resources", {"name": "heavy"})
    body = result["structuredContent"]
    assert body["monitored"] is True
    assert len(body["runs"]) == 1
    result = await _call(h, "cron_get_job_resources", {"name": "ghost"})
    assert result["isError"] is True


async def test_cluster_fleet_and_node_views():
    h = _handler()
    result = await _call(h, "cron_get_cluster")
    assert result["structuredContent"]["enabled"] is False
    assert "enabled=False" in result["content"][0]["text"]
    result = await _call(h, "cron_get_fleet")
    assert result["structuredContent"]["enabled"] is False
    result = await _call(h, "cron_get_node", {"history": True})
    assert "node" in result["content"][0]["text"]


async def test_query_metrics_match_and_bad_match():
    h = _handler()
    result = await _call(
        h, "cron_query_metrics", {"match": "cronstable", "limit": 5}
    )
    body = result["structuredContent"]
    assert body["match"] == "cronstable"
    assert body["returned"] <= 5
    assert all("cronstable" in s["name"] for s in body["samples"])
    result = await _call(h, "cron_query_metrics", {"match": 7})
    assert result["isError"] is True


async def test_get_version_tool():
    h = _handler()
    result = await _call(h, "cron_get_version")
    body = result["structuredContent"]
    assert body["jobs"] == 3
    assert body["job_set_id"]
    assert "3 job(s)" in result["content"][0]["text"]


async def test_tail_job_logs_and_not_found():
    h = _handler()
    result = await _call(
        h, "cron_tail_job_logs", {"name": "hello", "tail": 5, "cursor": "x"}
    )
    body = result["structuredContent"]
    assert body["name"] == "hello"
    assert body["lines"] == []
    result = await _call(h, "cron_tail_job_logs", {"name": "ghost"})
    assert result["isError"] is True


async def test_schedule_sandbox_argument_types():
    h = _handler()
    result = await _call(
        h, "cron_validate_schedule", {"expression": "* * * * *", "tz": 5}
    )
    assert result["isError"] is True
    assert "IANA timezone" in result["content"][0]["text"]
    result = await _call(
        h, "cron_explain_schedule", {"expression": "* * * * *", "seed": 5}
    )
    assert result["isError"] is True
    assert "job name" in result["content"][0]["text"]


async def test_schedule_pressure_and_suggest_argument_types():
    h = _handler()
    result = await _call(h, "cron_schedule_pressure", {"tz": 5})
    assert result["isError"] is True
    result = await _call(h, "cron_suggest_slot", {"tz": 5})
    assert result["isError"] is True
    result = await _call(h, "cron_suggest_slot", {"period": "weekly"})
    assert result["isError"] is True  # engine rejects unknown period


# ---------------------------------------------------------------------------
# act tools (job control)
# ---------------------------------------------------------------------------


async def test_run_job_success_and_confirm_gate(monkeypatch):
    h = _handler()
    launched = []

    async def fake_start(name):
        launched.append(name)

    monkeypatch.setattr(h._cron, "start_job_by_name", fake_start)
    result = await _call(h, "cron_run_job", {"name": "hello"})
    assert result["isError"] is True  # confirm missing
    assert launched == []
    result = await _call(h, "cron_run_job", {"name": "hello", "confirm": True})
    assert result["structuredContent"] == {"started": "hello"}
    assert launched == ["hello"]


async def test_run_job_disabled_surfaces_api_action_error():
    h = _handler()
    result = await _call(
        h, "cron_run_job", {"name": "nightly", "confirm": True}
    )
    assert result["isError"] is True
    assert "disabled" in result["content"][0]["text"]


class _FakeRunning:
    def __init__(self):
        self.cancelled = False
        self.proc = None


async def test_cancel_job_marks_all_instances():
    h = _handler()
    instances = [_FakeRunning(), _FakeRunning()]
    h._cron.running_jobs["hello"] = instances
    result = await _call(
        h, "cron_cancel_job", {"name": "hello", "confirm": True}
    )
    body = result["structuredContent"]
    assert body == {"cancelled": "hello", "instances": 2}
    assert all(inst.cancelled for inst in instances)


async def test_cancel_job_not_running_is_tool_error():
    h = _handler()
    result = await _call(
        h, "cron_cancel_job", {"name": "hello", "confirm": True}
    )
    assert result["isError"] is True
    assert "not running" in result["content"][0]["text"]


async def test_pause_job_confirm_gate_and_success():
    h = _handler()
    result = await _call(h, "cron_pause_job", {"name": "hello"})
    assert result["isError"] is True  # confirm missing
    assert "hello" not in h._cron._paused
    result = await _call(
        h,
        "cron_pause_job",
        {
            "name": "hello",
            "durationSeconds": 120,
            "note": "db migration",
            "confirm": True,
        },
    )
    body = result["structuredContent"]
    assert body["paused"] == "hello"
    assert body["until"] in result["content"][0]["text"]
    info = h._cron._paused["hello"]
    assert (info.by, info.channel) == ("mcp", "mcp")
    assert info.note == "db migration"
    assert (info.until - info.since).total_seconds() == 120


async def test_pause_job_default_duration():
    h = _handler()
    result = await _call(
        h, "cron_pause_job", {"name": "hello", "confirm": True}
    )
    assert result["structuredContent"]["paused"] == "hello"
    info = h._cron._paused["hello"]
    assert (
        info.until - info.since
    ).total_seconds() == PAUSE_DEFAULT_SECONDS


async def test_pause_job_unknown_and_bad_duration():
    h = _handler()
    result = await _call(
        h, "cron_pause_job", {"name": "ghost", "confirm": True}
    )
    assert result["isError"] is True
    assert "not found" in result["content"][0]["text"]
    result = await _call(
        h,
        "cron_pause_job",
        {"name": "hello", "durationSeconds": 0, "confirm": True},
    )
    assert result["isError"] is True
    assert "between 1 and" in result["content"][0]["text"]
    result = await _call(
        h,
        "cron_pause_job",
        {"name": "hello", "durationSeconds": "soon", "confirm": True},
    )
    assert result["isError"] is True
    assert "integer" in result["content"][0]["text"]
    assert "hello" not in h._cron._paused  # no rejected call took effect


async def test_resume_job_confirm_gate_clears_pause_and_noops():
    h = _handler()
    await _call(h, "cron_pause_job", {"name": "hello", "confirm": True})
    result = await _call(h, "cron_resume_job", {"name": "hello"})
    assert result["isError"] is True  # confirm missing
    assert "hello" in h._cron._paused
    result = await _call(
        h, "cron_resume_job", {"name": "hello", "confirm": True}
    )
    assert result["structuredContent"] == {"resumed": "hello"}
    assert "hello" not in h._cron._paused
    # resuming an unpaused job is still a success (idempotent no-op)
    result = await _call(
        h, "cron_resume_job", {"name": "hello", "confirm": True}
    )
    assert result["structuredContent"] == {"resumed": "hello"}
    result = await _call(
        h, "cron_resume_job", {"name": "ghost", "confirm": True}
    )
    assert result["isError"] is True
    assert "not found" in result["content"][0]["text"]


_SLA_YAML = _YAML + """\
  - name: watched
    command: echo hi
    schedule: "0 * * * *"
    sla:
      lateAfterSeconds: 60
"""


async def test_observe_payloads_carry_paused_and_sla():
    # the shared payload builder's new fields must reach the observe tools
    # untouched (no tool-side field filtering)
    h = _handler(yaml=_SLA_YAML)
    await _call(
        h,
        "cron_pause_job",
        {"name": "hello", "note": "window", "confirm": True},
    )
    result = await _call(h, "cron_get_job", {"name": "hello"})
    paused = result["structuredContent"]["paused"]
    assert (paused["by"], paused["channel"]) == ("mcp", "mcp")
    assert paused["note"] == "window"
    result = await _call(h, "cron_get_job", {"name": "watched"})
    sla = result["structuredContent"]["sla"]
    assert sla["thresholds"] == {"lateAfterSeconds": 60}
    assert sla["state"] == "ok"
    result = await _call(h, "cron_list_jobs")
    rows = {r["name"]: r for r in result["structuredContent"]["jobs"]}
    assert rows["hello"]["paused"] is not None
    assert rows["watched"]["paused"] is None
    assert "sla" in rows["watched"]


# ---------------------------------------------------------------------------
# resources/read
# ---------------------------------------------------------------------------


async def test_resources_read_requires_uri():
    h = _handler()
    resp = await _req(h, "resources/read", {"uri": 7})
    assert resp["error"]["code"] == mcp_mod.INVALID_PARAMS


async def test_resources_read_job_template_and_ghost():
    h = _handler()
    resp = await _req(h, "resources/read", {"uri": "cronstable://jobs/hello"})
    contents = resp["result"]["contents"][0]
    assert contents["mimeType"] == "application/json"
    assert json.loads(contents["text"])["name"] == "hello"
    resp = await _req(h, "resources/read", {"uri": "cronstable://jobs/ghost"})
    assert resp["error"]["code"] == mcp_mod.RESOURCE_NOT_FOUND


async def test_resources_read_fixed_resources():
    h = _handler()
    resp = await _req(h, "resources/read", {"uri": "cronstable://status"})
    body = json.loads(resp["result"]["contents"][0]["text"])
    assert {r["job"] for r in body["status"]} == {
        "hello",
        "nightly",
        "heavy",
    }
    resp = await _req(h, "resources/read", {"uri": "cronstable://cluster"})
    body = json.loads(resp["result"]["contents"][0]["text"])
    assert body["enabled"] is False


async def test_resources_read_state_ns_maps_action_error():
    # no state backend configured: the loader's ApiActionError must map to
    # the MCP resource-not-found protocol error, not a 500.
    h = _handler()
    resp = await _req(
        h, "resources/read", {"uri": "cronstable://state/kv/scope"}
    )
    assert resp["error"]["code"] == mcp_mod.RESOURCE_NOT_FOUND
    assert "state store" in resp["error"]["message"]


# ---------------------------------------------------------------------------
# prompts
# ---------------------------------------------------------------------------


async def test_prompt_renderers_fill_arguments():
    h = _handler()
    got = await _req(
        h,
        "prompts/get",
        {"name": "blast_radius", "arguments": {"target": "hello"}},
    )
    assert "'hello'" in got["result"]["messages"][0]["content"]["text"]
    got = await _req(h, "prompts/get", {"name": "fleet_health_summary"})
    text = got["result"]["messages"][0]["content"]["text"]
    assert "cron_get_fleet" in text
    got = await _req(
        h,
        "prompts/get",
        {
            "name": "why_did_dag_run_fail",
            "arguments": {"dag": "etl", "run_key": "r1"},
        },
    )
    text = got["result"]["messages"][0]["content"]["text"]
    assert "'etl'" in text and "'r1'" in text
    got = await _req(
        h,
        "prompts/get",
        {
            "name": "backfill_plan",
            "arguments": {"dag": "etl", "from": "a", "to": "b"},
        },
    )
    text = got["result"]["messages"][0]["content"]["text"]
    assert "backfill of dag 'etl' from a to b" in text


async def test_prompts_get_tolerates_non_object_arguments():
    h = _handler()
    got = await _req(
        h, "prompts/get", {"name": "blast_radius", "arguments": [1]}
    )
    # falls back to the placeholder when arguments are unusable
    assert "<target>" in got["result"]["messages"][0]["content"]["text"]


# ---------------------------------------------------------------------------
# summary formatters (direct payload-shape unit tests)
# ---------------------------------------------------------------------------


def test_preview_summary_shapes():
    assert mcp_mod._preview_summary(
        {"valid": False, "error": "boom"}
    ).startswith("INVALID: boom")
    assert "@reboot" in mcp_mod._preview_summary(
        {"valid": True, "reboot": True}
    )
    assert "never fires" in mcp_mod._preview_summary(
        {"valid": True, "description": "d", "never_fires": True, "lint": []}
    )
    text = mcp_mod._preview_summary(
        {
            "valid": True,
            "description": "every minute",
            "never_fires": False,
            "lint": [{"level": "warning"}, {"level": "note"}],
            "fires": ["2026-07-18T00:00:00+00:00"],
        }
    )
    assert "1 lint warning(s), 1 note(s)" in text
    assert "first fire 2026-07-18T00:00:00+00:00" in text
    # clean lint and no computed fires: the description stands alone
    bare = mcp_mod._preview_summary(
        {
            "valid": True,
            "description": "every minute",
            "never_fires": False,
            "lint": [],
            "fires": [],
        }
    )
    assert bare == "valid: every minute"


def test_why_summary_shapes():
    assert "@reboot" in mcp_mod._why_summary({"job": "j", "reboot": True})
    yes = mcp_mod._why_summary(
        {
            "job": "j",
            "reboot": False,
            "matches": True,
            "at_in_zone": "2026-07-18T09:00:00+02:00",
            "notes": [
                {"code": "day-and", "message": "AND rule"},
                {"code": "dst-gap", "message": "shifted"},
            ],
            "enabled": False,
            "checks": [],
        }
    )
    assert yes.startswith("YES")
    assert "BUT shifted" in yes
    assert "disabled" in yes
    yes_enabled = mcp_mod._why_summary(
        {
            "job": "j",
            "reboot": False,
            "matches": True,
            "at_in_zone": "x",
            "notes": [],
            "enabled": True,
            "checks": [],
        }
    )
    assert "cron_list_runs" in yes_enabled
    no = mcp_mod._why_summary(
        {
            "job": "j",
            "reboot": False,
            "matches": False,
            "notes": [],
            "enabled": False,
            "checks": [
                {
                    "field": "minute",
                    "label": "0",
                    "allowed": "0",
                    "matched": True,
                },
                {
                    "field": "day-of-week",
                    "label": "Tuesday",
                    "allowed": "Monday",
                    "matched": False,
                },
            ],
        }
    )
    assert no.startswith("NO: minute matched")
    assert "day-of-week Tuesday is not in Monday" in no
    assert "also disabled" in no
    nothing_matched = mcp_mod._why_summary(
        {
            "job": "j",
            "reboot": False,
            "matches": False,
            "notes": [],
            "enabled": True,
            "checks": [
                {
                    "field": "minute",
                    "label": "5",
                    "allowed": "0",
                    "matched": False,
                }
            ],
        }
    )
    assert nothing_matched.startswith("NO; minute 5 is not in 0")


def test_opt_int_rejects_junk():
    assert mcp_mod._opt_int(None) is None
    assert mcp_mod._opt_int("7") == 7
    assert mcp_mod._opt_int("x") is None


# ---------------------------------------------------------------------------
# dags/state tools against a real backend + scheduler
# ---------------------------------------------------------------------------

_GATE_DAG = (
    "dags:\n  - name: ap\n    tasks:\n"
    "      - id: a\n        command: 'x'\n"
    "      - id: gate\n        type: approval\n        dependsOn:\n"
    "          - a\n"
    "      - id: b\n        command: 'x'\n        dependsOn:\n"
    "          - gate\n"
    "  - name: bf\n    schedule: '0 * * * *'\n    tasks:\n"
    "      - id: a\n        command: 'x'\n"
)


async def _state_handler(tmp_path):
    cron = await _make_state_cron(tmp_path, _GATE_DAG)
    cron.web_config = {}
    _set_cmd(cron, "ap", "a", [_PY, "-c", "pass"])
    _set_cmd(cron, "ap", "b", [_PY, "-c", "pass"])
    _set_cmd(cron, "bf", "a", [_PY, "-c", "pass"])
    cfg = _build_mcp_config(
        {"enabled": True, "readOnly": False, "toolsets": _ALL_TOOLSETS}
    )
    return MCPHandler(cron, cfg), cron


async def test_dag_tools_full_flow(tmp_path):
    h, cron = await _state_handler(tmp_path)
    try:
        result = await _call(h, "cron_list_dags")
        names = {d["name"] for d in result["structuredContent"]["dags"]}
        assert names == {"ap", "bf"}

        # trigger: confirm gate first, then the real run
        result = await _call(h, "cron_trigger_dag", {"dag": "ap"})
        assert result["isError"] is True
        result = await _call(
            h, "cron_trigger_dag", {"dag": "ghost", "confirm": True}
        )
        assert result["isError"] is True
        result = await _call(
            h, "cron_trigger_dag", {"dag": "ap", "confirm": True}
        )
        run_key = result["structuredContent"]["runKey"]
        assert result["structuredContent"]["dag"] == "ap"

        # drive to the approval gate
        await _drive(cron, "ap", run_key)

        result = await _call(h, "cron_list_dag_runs", {"dag": "ap"})
        runs = result["structuredContent"]["runs"]
        assert [r["runKey"] for r in runs] == [run_key]
        result = await _call(h, "cron_list_dag_runs", {"dag": "ghost"})
        assert result["isError"] is True

        result = await _call(
            h, "cron_get_dag_run", {"dag": "ap", "run_key": run_key}
        )
        assert result["structuredContent"]["runKey"] == run_key
        result = await _call(
            h, "cron_get_dag_run", {"dag": "ap", "run_key": "nope"}
        )
        assert result["isError"] is True

        result = await _call(
            h, "cron_get_dag_xcom", {"dag": "ap", "run_key": run_key}
        )
        assert result["structuredContent"] is not None
        result = await _call(
            h, "cron_get_dag_xcom", {"dag": "ap", "run_key": "nope"}
        )
        assert result["isError"] is True

        result = await _call(
            h,
            "cron_tail_dag_task_logs",
            {"dag": "ap", "run_key": run_key, "taskkey": "a", "tail": 5},
        )
        assert result["structuredContent"]["dag"] == "ap"
        result = await _call(
            h,
            "cron_tail_dag_task_logs",
            {"dag": "ghost", "run_key": run_key, "taskkey": "a"},
        )
        assert result["isError"] is True

        # the approval gate: argument validation, then a real approval
        result = await _call(
            h,
            "cron_decide_gate",
            {
                "dag": "ap",
                "run_key": run_key,
                "taskkey": "gate",
                "decision": "maybe",
            },
        )
        assert result["isError"] is True
        result = await _call(
            h,
            "cron_decide_gate",
            {
                "dag": "ap",
                "run_key": run_key,
                "taskkey": "gate",
                "decision": "approve",
            },
        )
        assert result["isError"] is True  # confirm missing
        result = await _call(
            h,
            "cron_decide_gate",
            {
                "dag": "ap",
                "run_key": run_key,
                "taskkey": "nope",
                "decision": "approve",
                "confirm": True,
            },
        )
        assert result["isError"] is True  # unknown gate task
        result = await _call(
            h,
            "cron_decide_gate",
            {
                "dag": "ap",
                "run_key": run_key,
                "taskkey": "gate",
                "decision": "approve",
                "by": "alice",
                "confirm": True,
            },
        )
        assert "approved gate" in result["content"][0]["text"]

        # resources/read: dag detail template (found + ghost)
        resp = await _req(h, "resources/read", {"uri": "cronstable://dags/ap"})
        detail = json.loads(resp["result"]["contents"][0]["text"])
        assert detail["name"] == "ap"
        resp = await _req(
            h, "resources/read", {"uri": "cronstable://dags/ghost"}
        )
        assert resp["error"]["code"] == mcp_mod.RESOURCE_NOT_FOUND
    finally:
        await _teardown(cron)


async def test_backfill_tool_dry_run_default_and_real(tmp_path):
    h, cron = await _state_handler(tmp_path)
    try:
        result = await _call(
            h,
            "cron_backfill_dag",
            {"dag": "ghost", "from": "a", "to": "b"},
        )
        assert result["isError"] is True

        args = {
            "dag": "bf",
            "from": "2026-01-01T00:00:00+00:00",
            "to": "2026-01-01T01:30:00+00:00",
        }
        result = await _call(h, "cron_backfill_dag", args)
        body = result["structuredContent"]
        assert body["dryRun"] is True
        assert body["wouldExecute"] is False
        assert "DRY RUN" in result["content"][0]["text"]

        # a real backfill still requires confirm=true
        result = await _call(
            h, "cron_backfill_dag", {**args, "dry_run": False}
        )
        assert result["isError"] is True

        result = await _call(
            h,
            "cron_backfill_dag",
            {**args, "dry_run": False, "confirm": True},
        )
        assert result["structuredContent"]["ok"] is True
        assert result["structuredContent"]["created"] == 2

        # an unparseable range surfaces the engine's reason as a tool error
        result = await _call(
            h,
            "cron_backfill_dag",
            {
                "dag": "bf",
                "from": "bad",
                "to": "worse",
                "dry_run": False,
                "confirm": True,
            },
        )
        assert result["isError"] is True
    finally:
        await _teardown(cron)


async def test_inspect_state_forms(tmp_path):
    from cronstable import jobstate

    h, cron = await _state_handler(tmp_path)
    try:
        result = await _call(
            h, "cron_inspect_state", {"ns": "kv/x", "stream": "runs/y"}
        )
        assert result["isError"] is True

        await jobstate.kv_set(
            cron.state_backend, "scope", "k", {"pw": "hunter2"}
        )
        result = await _call(h, "cron_inspect_state", {"ns": "kv/scope"})
        docs = result["structuredContent"]["documents"]
        assert docs and "value" not in docs[0]

        result = await _call(
            h, "cron_inspect_state", {"stream": "runs/nobody", "limit": 5}
        )
        assert result["structuredContent"]["records"] == []

        result = await _call(h, "cron_inspect_state")
        overview = result["structuredContent"]
        assert overview["enabled"] is True
        assert "enabled=True" in result["content"][0]["text"]
    finally:
        await _teardown(cron)


async def test_resources_read_state_ns_with_backend(tmp_path):
    from cronstable import jobstate

    h, cron = await _state_handler(tmp_path)
    try:
        await jobstate.kv_set(cron.state_backend, "scope", "k", 1)
        resp = await _req(
            h, "resources/read", {"uri": "cronstable://state/kv/scope"}
        )
        body = json.loads(resp["result"]["contents"][0]["text"])
        assert body["namespace"] == "kv/scope"
        # a non-inspectable namespace maps ApiActionError -> resource error
        resp = await _req(
            h, "resources/read", {"uri": "cronstable://state/runs/x"}
        )
        assert resp["error"]["code"] == mcp_mod.RESOURCE_NOT_FOUND
    finally:
        await _teardown(cron)


# ---------------------------------------------------------------------------
# config plumbing kept honest (parse -> handler round trip)
# ---------------------------------------------------------------------------


def test_mcp_config_round_trip_through_parse():
    yaml = (
        "web:\n  listen:\n    - http://127.0.0.1:8080\n"
        "mcp:\n  enabled: true\n  readOnly: false\n"
        "  toolsets:\n    - observe\n    - act\n"
    )
    cfg = parse_config_string(yaml, "t.yaml").mcp_config
    assert cfg["enabled"] is True
    assert cfg["readOnly"] is False
    assert cfg["toolsets"] == ["observe", "act"]


async def test_tools_call_unknown_tool_is_invalid_params():
    h = _handler()
    resp = await _req(h, "tools/call", {"name": "ghost_tool", "arguments": {}})
    assert resp["error"]["code"] == mcp_mod.INVALID_PARAMS
    assert "unknown tool" in resp["error"]["message"]


async def test_ping_round_trips():
    h = _handler()
    resp = await _req(h, "ping")
    assert resp == {"jsonrpc": "2.0", "id": 1, "result": {}}


# ---------------------------------------------------------------------------
# fuzzing findings: falsy dry_run must preview, and non-finite numeric
# arguments must clamp/default instead of -32603
# ---------------------------------------------------------------------------


async def test_backfill_falsy_dry_run_values_still_preview(tmp_path):
    # `dry_run: null` is exactly how an MCP client or LLM encodes an
    # unspecified optional parameter; args.get("dry_run", True) applied the
    # default only when the key was ABSENT, so every present-but-falsy
    # value (null/[]/{}/""/0) fell through the preview gate into a REAL
    # backfill on a destructiveHint:true tool.  Only the literal boolean
    # false may execute.
    h, cron = await _state_handler(tmp_path)
    try:
        args = {
            "dag": "bf",
            "from": "2026-01-01T00:00:00+00:00",
            "to": "2026-01-01T01:30:00+00:00",
            "confirm": True,  # confirm alone must not defeat the preview
        }
        for falsy in (None, [], {}, "", 0):
            result = await _call(
                h, "cron_backfill_dag", {**args, "dry_run": falsy}
            )
            body = result["structuredContent"]
            assert body.get("dryRun") is True, (falsy, body)
            assert body.get("wouldExecute") is False
            assert "DRY RUN" in result["content"][0]["text"]
        # the documented real-run spelling still executes
        result = await _call(
            h, "cron_backfill_dag", {**args, "dry_run": False}
        )
        assert result["structuredContent"]["ok"] is True
    finally:
        await _teardown(cron)


async def test_numeric_arguments_survive_non_finite_json_numbers():
    # 1e999 is a well-formed RFC-8259 number the stdlib parser reads as
    # inf; int(inf) raises OverflowError, which none of the coercion
    # helpers caught -- turning a schema-valid argument into a -32603
    # protocol fault (with a server-side traceback) on 15 tool/argument
    # pairs, instead of the documented clamp-never-error behaviour.
    import json as stdjson

    inf = stdjson.loads(b'{"limit": 1e999}')["limit"]  # off-the-wire shape
    assert mcp_mod._opt_int(inf) is None
    assert mcp_mod._opt_int(float("nan")) is None
    assert mcp_mod._opt_int(float("-inf")) is None

    h = _handler()
    for args in (
        {"limit": inf},
        {"offset": inf, "limit": 5},
        {"limit": float("nan")},
    ):
        result = await _call(h, "cron_get_status", args)
        # a normal (possibly clamped) result, never a JSON-RPC error
        assert "structuredContent" in result, (args, result)
    # _call itself asserts no JSON-RPC error envelope: reaching a normal
    # tool result (even an isError one) is the fix for the tail/cursor pair
    await _call(h, "cron_tail_job_logs", {"job": "hello", "tail": inf})
