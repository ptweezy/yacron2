"""A hand-rolled Model Context Protocol (MCP) server for cronstable.

MCP (https://modelcontextprotocol.io) lets an AI agent -- Claude Desktop /
Code, Cursor, VS Code Copilot, and other MCP clients -- drive cronstable the
way an operator drives the dashboard: list and inspect jobs, DAGs, the
cluster/fleet and the durable state store (observe), and, when the operator
opts in, run/cancel a job or trigger/backfill/approve a DAG (act).

The protocol is JSON-RPC 2.0 over the "Streamable HTTP" transport
(https://modelcontextprotocol.io/specification/2025-11-25/basic/transports) --
a single ``POST /mcp`` endpoint.  cronstable already owns every building block
(a JSON dispatcher, aiohttp, a bearer-token middleware, the ``_json`` fast
path), so this server is a few hundred lines of pure Python with NO new
dependencies -- deliberately, rather than vendoring the official ``mcp`` SDK
and its Rust-compiled transitive tree (pydantic-core / cryptography / rpds-py),
which would break cronstable's multi-architecture, distroless packaging story.

The endpoint is served on the existing ``web.listen`` addresses and rides the
same bearer/mTLS/unix-socket auth (see :meth:`cronstable.cron.Cron.\
start_stop_web_app`).  The tools call the same in-process payload builders the
``_web_*`` REST handlers use, so there is one source of truth.  Local desktop
clients reach the server through the featherweight ``cronstable mcp`` stdio
bridge (:mod:`cronstable.mcpcli`), which forwards frames here over urllib.

Design profile: STATELESS (no ``Mcp-Session-Id``, no GET SSE stream), tools
only (no resources/prompts yet), pinned to protocol revision
``2025-11-25``.  Statelessness keeps a future migration to the session-less
2026 revision a near-no-op.
"""

import json as _stdlib_json
import logging
import re
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    cast,
)

from aiohttp import web

from cronstable import _json
from cronstable import version as _version
from cronstable.cron import ApiActionError

if TYPE_CHECKING:  # pragma: no cover - typing only, no import cost / no cycle
    from cronstable.cron import Cron

logger = logging.getLogger("cronstable.mcp")

# The MCP revision this server implements. Every response advertises it; a
# request carrying an unsupported MCP-Protocol-Version header is rejected.
PROTOCOL_VERSION = "2025-11-25"
# Revisions we can speak on the wire (all share the Streamable-HTTP framing).
# A client that negotiates one of these at initialize gets it echoed back.
SUPPORTED_PROTOCOL_VERSIONS = frozenset(
    {"2025-11-25", "2025-06-18", "2025-03-26"}
)

# JSON-RPC 2.0 error codes (protocol-level faults). Tool *execution* and
# input-validation failures do NOT use these -- they return a normal result
# with isError:true so the model can read and self-correct (MCP SEP-1303).
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603
# MCP-specific: a resources/read for a URI that does not resolve.
RESOURCE_NOT_FOUND = -32002

RESOURCE_MIME = "application/json"

ToolHandler = Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]
_MethodHandler = Callable[
    [Dict[str, Any]], Awaitable[Optional[Dict[str, Any]]]
]


class MCPError(Exception):
    """A JSON-RPC protocol-level fault (mapped to an ``error`` response)."""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class _ToolInputError(Exception):
    """A bad tool argument -> an ``isError`` result the model can correct."""


def _dumps(obj: Any) -> bytes:
    """Serialize a response body to JSON bytes.

    Prefers the orjson-accelerated :func:`cronstable._json.dumps_bytes`, but
    an MCP payload is a transient response (never a durable, cross-fleet
    record), so a non-finite float or other non-"portable" value should not
    500 the endpoint: fall back to the stdlib, which encodes it.
    """
    try:
        return _json.dumps_bytes(obj)
    except _json.UnsupportedValue:
        return _stdlib_json.dumps(obj, default=str).encode("utf-8")


# Prometheus exposition line: metric name, optional {labels}, then a value.
_METRIC_LINE_RE = re.compile(r"^([A-Za-z_:][\w:]*)(\{[^}]*\})?\s+(\S+)")


def _parse_prometheus(
    text: str, match: Optional[str], limit: int
) -> Tuple[List[Dict[str, Any]], int]:
    """Reduce a Prometheus exposition to a compact, filtered sample list.

    Returns ``(samples, total_matched)``; ``samples`` is capped at ``limit``.
    HELP/TYPE comment lines are skipped and values are kept as strings so a
    ``NaN`` / ``+Inf`` gauge round-trips untouched.
    """
    needle = match.lower() if match else None
    samples: List[Dict[str, Any]] = []
    total = 0
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = _METRIC_LINE_RE.match(line)
        if m is None:
            continue
        name = m.group(1)
        if needle is not None and needle not in name.lower():
            continue
        total += 1
        if len(samples) < limit:
            samples.append(
                {
                    "name": name,
                    "labels": m.group(2) or "",
                    "value": m.group(3),
                }
            )
    return samples, total


def _filter_metric_samples(
    samples: Iterator[Tuple[str, str, str]],
    match: Optional[str],
    limit: int,
) -> Tuple[List[Dict[str, Any]], int]:
    """Filter structured ``(name, label_block, value)`` samples by a
    case-insensitive name substring, capping the returned list at ``limit``
    while still counting every match.

    The model-level twin of :func:`_parse_prometheus` (same filter, same
    output shape), but fed the metric families directly via
    :meth:`prometheus...iter_samples`, so the metrics query skips rendering
    the whole exposition text only to regex it back apart.
    """
    needle = match.lower() if match else None
    out: List[Dict[str, Any]] = []
    total = 0
    for name, labels, value in samples:
        if needle is not None and needle not in name.lower():
            continue
        total += 1
        if len(out) < limit:
            out.append({"name": name, "labels": labels, "value": value})
    return out, total


class MCPHandler:
    """Serves MCP over Streamable HTTP for one running :class:`Cron`.

    Built inside ``start_stop_web_app`` so it always reflects the current
    config; a config reload discards and rebuilds it.
    """

    def __init__(self, cron: "Cron", config: Dict[str, Any]) -> None:
        self._cron = cron
        self._read_only: bool = config["readOnly"]
        self._toolsets = set(config["toolsets"])
        self._max_rows: int = config["maxRows"]
        self._max_body: int = config["maxBodyBytes"]
        self._allowed_origins = set(config["allowedOrigins"])
        self._resources_enabled: bool = config.get("resources", True)
        self._prompts_enabled: bool = config.get("prompts", True)
        self._instructions: Optional[str] = config.get("instructions")
        self._methods: Dict[str, _MethodHandler] = {
            "initialize": self._m_initialize,
            "notifications/initialized": self._m_noop,
            "notifications/cancelled": self._m_noop,
            "ping": self._m_ping,
            "tools/list": self._m_tools_list,
            "tools/call": self._m_tools_call,
        }
        self._tools = self._build_registry()
        self._tool_by_name = {t["name"]: t for t in self._tools}
        self._resources, self._templates = self._build_resources()
        self._resource_by_uri = {r["uri"]: r for r in self._resources}
        self._prompts = self._build_prompts()
        self._prompt_by_name = {p["name"]: p for p in self._prompts}
        if self._resources_enabled:
            self._methods["resources/list"] = self._m_resources_list
            self._methods["resources/templates/list"] = (
                self._m_resource_templates_list
            )
            self._methods["resources/read"] = self._m_resources_read
        if self._prompts_enabled:
            self._methods["prompts/list"] = self._m_prompts_list
            self._methods["prompts/get"] = self._m_prompts_get

    # -- capabilities / visibility ----------------------------------------

    def _capabilities(self) -> Dict[str, Any]:
        """Advertise ONLY what is actually registered.

        A server MUST NOT advertise a capability it does not implement (a
        conformant client would then call the method and get -32601). Tools
        are always present; resources/prompts appear only when enabled AND
        something is registered under the active toolsets.
        """
        caps: Dict[str, Any] = {"tools": {"listChanged": False}}
        if self._resources_enabled and (
            any(self._resource_visible(r) for r in self._resources)
            or any(self._resource_visible(t) for t in self._templates)
        ):
            caps["resources"] = {"listChanged": False}
        if self._prompts_enabled and any(
            self._prompt_visible(p) for p in self._prompts
        ):
            caps["prompts"] = {"listChanged": False}
        return caps

    def _resource_visible(self, entry: Dict[str, Any]) -> bool:
        return entry["toolset"] in self._toolsets

    def _prompt_visible(self, entry: Dict[str, Any]) -> bool:
        return entry["toolset"] in self._toolsets

    def _is_visible(self, tool: Dict[str, Any]) -> bool:
        """Whether ``tool`` is exposed under the current config.

        Its toolset must be enabled, and a mutating tool is stripped entirely
        while ``readOnly`` is on (readOnly wins over toolsets, GitHub-style).
        """
        if tool["toolset"] not in self._toolsets:
            return False
        if tool["mutating"] and self._read_only:
            return False
        return True

    # -- JSON-RPC method handlers -----------------------------------------

    async def _m_initialize(self, params: Dict[str, Any]) -> Dict[str, Any]:
        requested = params.get("protocolVersion")
        # echo the client's version when we can speak it, else offer ours.
        negotiated = (
            requested
            if requested in SUPPORTED_PROTOCOL_VERSIONS
            else PROTOCOL_VERSION
        )
        result: Dict[str, Any] = {
            "protocolVersion": negotiated,
            "capabilities": self._capabilities(),
            "serverInfo": {
                "name": "cronstable",
                "title": "cronstable",
                "version": _version.version,
            },
        }
        instructions = self._instructions or (
            "cronstable's MCP server. Read-only 'observe' tools describe "
            "jobs, DAGs, the cluster/fleet, metrics and durable state. "
            "Mutating tools (run/cancel/pause/resume a job, "
            "trigger/backfill/approve a DAG) require confirm=true and "
            "appear only when the operator disabled readOnly. Start with "
            "cron_get_status or cron_list_jobs. When authoring a schedule, "
            "verify it with cron_validate_schedule / cron_explain_schedule "
            "(the daemon's own engine) before proposing it; cron_why_no_run "
            "explains why a job's schedule did or did not fire at a given "
            "timestamp."
        )
        result["instructions"] = instructions
        return result

    async def _m_noop(
        self, params: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        return None

    async def _m_ping(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return {}

    async def _m_tools_list(self, params: Dict[str, Any]) -> Dict[str, Any]:
        tools = [
            {
                "name": t["name"],
                "title": t["title"],
                "description": t["description"],
                "inputSchema": t["inputSchema"],
                "annotations": t["annotations"],
            }
            for t in self._tools
            if self._is_visible(t)
        ]
        return {"tools": tools}

    async def _m_tools_call(self, params: Dict[str, Any]) -> Dict[str, Any]:
        name = params.get("name")
        if not isinstance(name, str):
            raise MCPError(INVALID_PARAMS, "tools/call requires a 'name'")
        tool = self._tool_by_name.get(name)
        if tool is None or not self._is_visible(tool):
            raise MCPError(INVALID_PARAMS, "unknown tool: {}".format(name))
        arguments = params.get("arguments", {})
        if not isinstance(arguments, dict):
            raise MCPError(INVALID_PARAMS, "'arguments' must be an object")
        handler = cast(ToolHandler, tool["handler"])
        try:
            return await handler(arguments)
        except _ToolInputError as ex:
            return _tool_error(str(ex))
        except ApiActionError as ex:
            # a client-facing action failure (unknown/disabled/not-running
            # job, bad dag): an isError result the model can act on, not a
            # transport fault.
            return _tool_error(ex.message)

    # -- top-level dispatch (transport-independent, unit-testable) --------

    async def handle_message(self, msg: Any) -> Optional[Dict[str, Any]]:
        """Dispatch one JSON-RPC message.

        Returns the response object for a request, or ``None`` for a
        notification (the caller then emits a 202).  The single seam tests
        drive directly.
        """
        if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0":
            return _error_envelope(
                _id_of(msg), INVALID_REQUEST, "invalid JSON-RPC 2.0 message"
            )
        is_notification = "id" not in msg
        msg_id = msg.get("id")
        method = msg.get("method")
        if not isinstance(method, str):
            if is_notification:
                return None
            return _error_envelope(msg_id, INVALID_REQUEST, "missing method")
        handler = self._methods.get(method)
        if handler is None:
            if is_notification:
                return None  # unknown notifications are ignored, per spec
            return _error_envelope(
                msg_id, METHOD_NOT_FOUND, "unknown method: {}".format(method)
            )
        params = msg.get("params") or {}
        if not isinstance(params, dict):
            if is_notification:
                return None
            return _error_envelope(msg_id, INVALID_PARAMS, "invalid params")
        try:
            result = await handler(params)
        except MCPError as ex:
            if is_notification:
                return None
            return _error_envelope(msg_id, ex.code, ex.message)
        except Exception:  # noqa: BLE001 - never leak a traceback to a client
            logger.exception("mcp: internal error handling %s", method)
            if is_notification:
                return None
            return _error_envelope(msg_id, INTERNAL_ERROR, "internal error")
        if is_notification:
            return None
        return {"jsonrpc": "2.0", "id": msg_id, "result": result}

    # -- HTTP (Streamable HTTP, stateless profile) ------------------------

    async def handle_http(self, request: web.Request) -> web.StreamResponse:
        origin = request.headers.get("Origin")
        # DNS-rebinding defense: a present Origin must be allow-listed. With
        # an empty allowedOrigins (non-browser clients only) any Origin is
        # refused -- a real MCP client over stdio/CLI sends none.
        if origin is not None and origin not in self._allowed_origins:
            return self._http_error(403, "Origin not allowed", origin)
        pv = request.headers.get("MCP-Protocol-Version")
        if pv is not None and pv not in SUPPORTED_PROTOCOL_VERSIONS:
            return self._http_error(
                400, "unsupported MCP-Protocol-Version", origin
            )
        accept = request.headers.get("Accept")
        # stateless mode only ever emits application/json; be lenient on a
        # missing Accept, but honor a present, incompatible one.
        if accept and "application/json" not in accept and "*/*" not in accept:
            return self._http_error(406, "Accept application/json", origin)
        if (
            request.content_length is not None
            and request.content_length > self._max_body
        ):
            return self._http_error(413, "request body too large", origin)
        raw = await request.read()
        if len(raw) > self._max_body:
            return self._http_error(413, "request body too large", origin)
        if not raw:
            return self._http_error(400, "empty request body", origin)
        try:
            msg = _json.loads(raw)
        except Exception:  # noqa: BLE001 - malformed JSON -> 400
            return self._http_error(400, "malformed JSON", origin)
        if isinstance(msg, list):
            # JSON-RPC batching was removed in MCP 2025-06-18.
            return self._http_error(400, "batching unsupported", origin)
        response = await self.handle_message(msg)
        if response is None:
            # a notification/response carries no reply.
            return self._plain(202, origin)
        return self._json_response(response, origin=origin)

    async def handle_http_get(
        self, request: web.Request
    ) -> web.StreamResponse:
        # stateless: no server->client SSE stream to open.
        origin = request.headers.get("Origin")
        resp = self._http_error(405, "method not allowed", origin)
        resp.headers["Allow"] = "POST, OPTIONS"
        return resp

    async def handle_options(self, request: web.Request) -> web.StreamResponse:
        origin = request.headers.get("Origin")
        if origin and origin in self._allowed_origins:
            headers = self._cors_headers(origin)
            headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
            return web.Response(status=204, headers=headers)
        return self._http_error(403 if origin else 405, "preflight", origin)

    # -- HTTP response helpers --------------------------------------------

    def _cors_headers(self, origin: Optional[str]) -> Dict[str, str]:
        if origin and origin in self._allowed_origins:
            # credentialed CORS may not use a wildcard; echo the exact origin.
            return {
                "Access-Control-Allow-Origin": origin,
                "Access-Control-Allow-Headers": (
                    "Authorization, Content-Type, MCP-Protocol-Version"
                ),
                "Access-Control-Expose-Headers": "MCP-Protocol-Version",
                "Vary": "Origin",
            }
        return {}

    def _json_response(
        self, obj: Dict[str, Any], *, origin: Optional[str]
    ) -> web.Response:
        headers = {"MCP-Protocol-Version": PROTOCOL_VERSION}
        headers.update(self._cors_headers(origin))
        return web.Response(
            body=_dumps(obj),
            status=200,
            content_type="application/json",
            charset="utf-8",
            headers=headers,
        )

    def _plain(self, status: int, origin: Optional[str]) -> web.Response:
        headers = {"MCP-Protocol-Version": PROTOCOL_VERSION}
        headers.update(self._cors_headers(origin))
        return web.Response(status=status, headers=headers)

    def _http_error(
        self, status: int, message: str, origin: Optional[str]
    ) -> web.Response:
        headers = {"MCP-Protocol-Version": PROTOCOL_VERSION}
        headers.update(self._cors_headers(origin))
        return web.Response(
            body=_dumps({"error": message}),
            status=status,
            content_type="application/json",
            charset="utf-8",
            headers=headers,
        )

    # -- pagination / argument helpers ------------------------------------

    def _clamp_limit(self, requested: Any) -> int:
        if requested is None:
            return self._max_rows
        try:
            n = int(requested)
        # OverflowError alongside the usual two: int(float("inf")) raises
        # it, and the stdlib JSON parser produces inf from the well-formed
        # literal 1e999 -- without it a schema-valid argument became a
        # -32603 protocol fault instead of the documented clamp.
        except (TypeError, ValueError, OverflowError):
            return self._max_rows
        return max(1, min(n, self._max_rows))

    def _page(
        self, items: List[Any], offset: Any, limit: Any
    ) -> Tuple[List[Any], Dict[str, Any]]:
        total = len(items)
        try:
            off = max(0, int(offset or 0))
        # OverflowError: see _clamp_limit.
        except (TypeError, ValueError, OverflowError):
            off = 0
        lim = self._clamp_limit(limit)
        page = items[off : off + lim]
        nxt = off + len(page)
        return page, {
            "offset": off,
            "limit": lim,
            "total": total,
            "returned": len(page),
            "nextOffset": nxt if nxt < total else None,
        }

    # -- tool registry -----------------------------------------------------

    def _build_registry(self) -> List[Dict[str, Any]]:
        obj = _obj_schema
        # (toolset, mutating, name, title, description, inputSchema, handler,
        #  destructive, idempotent)
        specs = [
            # ---- observe (read-only) ----
            (
                "observe",
                False,
                "cron_get_status",
                "Job status",
                "One-line status (running/disabled/scheduled) of every job.",
                obj({"offset": _INT, "limit": _INT}),
                self._t_get_status,
                False,
                True,
            ),
            (
                "observe",
                False,
                "cron_list_jobs",
                "List jobs",
                "List jobs with schedule, enabled/running state, next run and "
                "last outcome. Optional name substring `filter` and `state` "
                "(running/disabled/scheduled).",
                obj(
                    {
                        "filter": _STR,
                        "state": _enum(["running", "disabled", "scheduled"]),
                        "offset": _INT,
                        "limit": _INT,
                    }
                ),
                self._t_list_jobs,
                False,
                True,
            ),
            (
                "observe",
                False,
                "cron_get_job",
                "Get one job",
                "Full detail for one job (schedule, command, last run, live "
                "resources, retry/slot state).",
                obj({"name": _STR}, ["name"]),
                self._t_get_job,
                False,
                True,
            ),
            (
                "observe",
                False,
                "cron_list_runs",
                "Run history",
                "Retained run history + success/duration stats for one job "
                "(most recent `limit` runs).",
                obj({"name": _STR, "limit": _INT}, ["name"]),
                self._t_list_runs,
                False,
                True,
            ),
            (
                "observe",
                False,
                "cron_get_job_trends",
                "SLA trends",
                "Per-window (1h/24h/7d/30d/all) success-rate and duration "
                "aggregates over the durable run ledger for one job.",
                obj({"name": _STR}, ["name"]),
                self._t_get_job_trends,
                False,
                True,
            ),
            (
                "observe",
                False,
                "cron_get_job_resources",
                "Resource usage",
                "CPU/RSS time series for a job's live and recent runs "
                "(monitorResources jobs).",
                obj({"name": _STR, "runs": _INT}, ["name"]),
                self._t_get_job_resources,
                False,
                True,
            ),
            (
                "observe",
                False,
                "cron_get_cluster",
                "Cluster view",
                "This node's cluster/leadership view (peers, quorum, role, "
                "live load). enabled:false without a cluster section.",
                obj({}),
                self._t_get_cluster,
                False,
                True,
            ),
            (
                "observe",
                False,
                "cron_get_fleet",
                "Fleet view",
                "The cluster-wide jobs x nodes run matrix (single pane of "
                "glass). enabled:false without a cluster.",
                obj({}),
                self._t_get_fleet,
                False,
                True,
            ),
            (
                "observe",
                False,
                "cron_get_node",
                "Node resources",
                "This node's live whole-host CPU/memory (optionally with the "
                "retained history ring).",
                obj({"history": _BOOL}),
                self._t_get_node,
                False,
                True,
            ),
            (
                "observe",
                False,
                "cron_query_metrics",
                "Query metrics",
                "Parsed samples from the Prometheus /metrics exposition, "
                "optionally filtered by a metric-name substring `match`.",
                obj({"match": _STR, "limit": _INT}),
                self._t_query_metrics,
                False,
                True,
            ),
            (
                "observe",
                False,
                "cron_get_version",
                "Version",
                "Daemon version, job-set id and job count.",
                obj({}),
                self._t_get_version,
                False,
                True,
            ),
            (
                "observe",
                False,
                "cron_tail_job_logs",
                "Tail job logs",
                "Last retained stdout/stderr lines of a job, with a `cursor` "
                "to poll for newly appended lines (the poll form of the live "
                "log stream).",
                obj({"name": _STR, "tail": _INT, "cursor": _INT}, ["name"]),
                self._t_tail_job_logs,
                False,
                True,
            ),
            (
                "observe",
                False,
                "cron_schedule_pressure",
                "Schedule pressure",
                "The fleet's collision heatmap: every enabled schedule's "
                "fires over the next `hours` (default 24, max 168), bucketed "
                "by hour and minute in `tz` (default UTC). Answers 'how many "
                "jobs fire at :00?' and 'which minutes are empty?'.",
                obj({"hours": _INT, "tz": _STR}),
                self._t_schedule_pressure,
                False,
                True,
            ),
            (
                "observe",
                False,
                "cron_schedule_duplicates",
                "Duplicate schedules",
                "Groups of jobs whose schedules fire on the identical "
                "instants (semantic equality: */5 == 0-59/5, same timezone). "
                "Use to spot copy-pasted schedules worth spreading out.",
                obj({}),
                self._t_schedule_duplicates,
                False,
                True,
            ),
            (
                "observe",
                False,
                "cron_suggest_slot",
                "Suggest a slot",
                "The least-loaded slot for a new job, from the fleet's real "
                "fires over the next 24h: `period` 'hourly' picks a minute, "
                "'daily' a minute and hour; returns the cron expression, two "
                "runners-up, and the busiest slot for contrast.",
                obj(
                    {
                        "period": _enum(["hourly", "daily"]),
                        "tz": _STR,
                    }
                ),
                self._t_suggest_slot,
                False,
                True,
            ),
            (
                "observe",
                False,
                "cron_validate_schedule",
                "Validate a schedule",
                "Parse and lint a cron expression BEFORE it becomes a job: "
                "valid true/false with the engine's exact error (including "
                "wrong-field hints for Quartz-style forms), the "
                "plain-English description, the normalized form, advisory "
                "lint findings and the first upcoming fire, all from the "
                "daemon's own scheduling engine. The dialect includes L "
                "(last day), L-n (n days before it), nW / LW (nearest / "
                "last weekday) and Ln / d#n (last / nth weekday: L5 = last "
                "Friday, 5#3 = third Friday). `tz` (IANA zone the job will "
                "run in) enables the DST checks; `seed` (the prospective "
                "job name) resolves Jenkins-style H slots.",
                obj(
                    {"expression": _STR, "tz": _STR, "seed": _STR},
                    ["expression"],
                ),
                self._t_validate_schedule,
                False,
                True,
            ),
            (
                "observe",
                False,
                "cron_explain_schedule",
                "Explain a schedule",
                "Decode a cron expression into a plain-English description, "
                "its next `count` fires (default 5, max 60) as ISO instants "
                "in `tz` (default UTC; pass the job's zone), and advisory "
                "lint findings, so a schedule can be authored, verified "
                "against the daemon's own engine, and round-tripped to the "
                "user for confirmation. `seed` (a job name) resolves "
                "Jenkins-style H slots.",
                obj(
                    {
                        "expression": _STR,
                        "count": _INT,
                        "tz": _STR,
                        "seed": _STR,
                    },
                    ["expression"],
                ),
                self._t_explain_schedule,
                False,
                True,
            ),
            (
                "observe",
                False,
                "cron_why_no_run",
                "Why no run?",
                "Explain field-by-field why a job's schedule did or did not "
                "select a timestamp ('day-of-week Tuesday is not in Monday "
                "and Friday'), with the nearest real fire on each side and "
                "notes on this dialect's day-field AND rule and DST "
                "effects. `at` is ISO 8601; a naive timestamp reads as wall "
                "time in the job's own timezone. If the schedule DOES "
                "match, the answer points at execution history "
                "(cron_list_runs) instead.",
                obj({"name": _STR, "at": _STR}, ["name", "at"]),
                self._t_why_no_run,
                False,
                True,
            ),
            # ---- dags ----
            (
                "dags",
                False,
                "cron_list_dags",
                "List DAGs",
                "Configured orchestration DAGs with their tasks and "
                "dependencies.",
                obj({}),
                self._t_list_dags,
                False,
                True,
            ),
            (
                "dags",
                False,
                "cron_list_dag_runs",
                "List DAG runs",
                "Recent runs of one DAG with per-state task counts.",
                obj({"dag": _STR, "limit": _INT}, ["dag"]),
                self._t_list_dag_runs,
                False,
                True,
            ),
            (
                "dags",
                False,
                "cron_get_dag_run",
                "Get DAG run",
                "One DAG run's full document: task states, timing, decisions.",
                obj({"dag": _STR, "run_key": _STR}, ["dag", "run_key"]),
                self._t_get_dag_run,
                False,
                True,
            ),
            (
                "dags",
                False,
                "cron_get_dag_xcom",
                "DAG XCom",
                "The XCom values a DAG run's tasks published.",
                obj({"dag": _STR, "run_key": _STR}, ["dag", "run_key"]),
                self._t_get_dag_xcom,
                False,
                True,
            ),
            (
                "dags",
                False,
                "cron_tail_dag_task_logs",
                "Tail DAG task logs",
                "Last retained log lines of a currently-running DAG task "
                "instance, with a `cursor` to poll for more.",
                obj(
                    {
                        "dag": _STR,
                        "run_key": _STR,
                        "taskkey": _STR,
                        "tail": _INT,
                        "cursor": _INT,
                    },
                    ["dag", "run_key", "taskkey"],
                ),
                self._t_tail_dag_task_logs,
                False,
                True,
            ),
            # ---- state (read-only inspector) ----
            (
                "state",
                False,
                "cron_inspect_state",
                "Inspect state store",
                "Metadata-only view of the durable state store: overview "
                "(default), one namespace's documents (`ns` "
                "kv/|cursor/|idem/) or a stream's newest records (`stream`). "
                "KV values and "
                "secrets are redacted.",
                obj({"ns": _STR, "stream": _STR, "limit": _INT}),
                self._t_inspect_state,
                False,
                True,
            ),
            # ---- act (mutating job control; readOnly:false to expose) ----
            (
                "act",
                True,
                "cron_run_job",
                "Run job now",
                "Launch a job immediately (honours its concurrencyPolicy). "
                "Requires confirm=true.",
                obj({"name": _STR, "confirm": _BOOL}, ["name"]),
                self._t_run_job,
                False,
                False,
            ),
            (
                "act",
                True,
                "cron_cancel_job",
                "Cancel job",
                "Terminate a job's running instances (graceful, then kill). "
                "Requires confirm=true.",
                obj({"name": _STR, "confirm": _BOOL}, ["name"]),
                self._t_cancel_job,
                True,
                True,
            ),
            (
                "act",
                True,
                "cron_pause_job",
                "Pause job",
                "Skip a job's scheduled fires until the window expires "
                "(durationSeconds, default 3600) or the job is resumed; "
                "manual runs stay allowed. Requires confirm=true.",
                obj(
                    {
                        "name": _STR,
                        "durationSeconds": _INT,
                        "note": _STR,
                        "confirm": _BOOL,
                    },
                    ["name"],
                ),
                self._t_pause_job,
                False,
                True,
            ),
            (
                "act",
                True,
                "cron_resume_job",
                "Resume job",
                "Lift a job's pause so scheduled fires resume; a no-op when "
                "the job is not paused. Requires confirm=true.",
                obj({"name": _STR, "confirm": _BOOL}, ["name"]),
                self._t_resume_job,
                False,
                True,
            ),
            # ---- dag control (mutating; toolset dags + readOnly:false) ----
            (
                "dags",
                True,
                "cron_trigger_dag",
                "Trigger DAG",
                "Create and start a manual DAG run now. "
                "Requires confirm=true.",
                obj({"dag": _STR, "confirm": _BOOL}, ["dag"]),
                self._t_trigger_dag,
                False,
                False,
            ),
            (
                "dags",
                True,
                "cron_backfill_dag",
                "Backfill DAG",
                "Replay a scheduled DAG across an ISO date range. dry_run "
                "(default true) previews; a real backfill needs dry_run=false "
                "AND confirm=true.",
                obj(
                    {
                        "dag": _STR,
                        "from": _STR,
                        "to": _STR,
                        "dry_run": _BOOL,
                        "confirm": _BOOL,
                    },
                    ["dag", "from", "to"],
                ),
                self._t_backfill_dag,
                True,
                False,
            ),
            (
                "dags",
                True,
                "cron_decide_gate",
                "Decide approval gate",
                "Approve or reject a DAG approval gate. "
                "Requires confirm=true.",
                obj(
                    {
                        "dag": _STR,
                        "run_key": _STR,
                        "taskkey": _STR,
                        "decision": _enum(["approve", "reject"]),
                        "by": _STR,
                        "confirm": _BOOL,
                    },
                    ["dag", "run_key", "taskkey", "decision"],
                ),
                self._t_decide_gate,
                True,
                False,
            ),
        ]
        registry: List[Dict[str, Any]] = []
        for (
            toolset,
            mutating,
            name,
            title,
            desc,
            schema,
            handler,
            destructive,
            idempotent,
        ) in specs:
            registry.append(
                {
                    "toolset": toolset,
                    "mutating": mutating,
                    "name": name,
                    "title": title,
                    "description": desc,
                    "inputSchema": schema,
                    "handler": handler,
                    "annotations": {
                        "title": title,
                        "readOnlyHint": not mutating,
                        "destructiveHint": destructive,
                        "idempotentHint": idempotent,
                        # every tool acts on cronstable's own closed domain,
                        # never an unpredictable external system.
                        "openWorldHint": False,
                    },
                }
            )
        return registry

    # -- observe tool handlers --------------------------------------------

    async def _t_get_status(self, args: Dict[str, Any]) -> Dict[str, Any]:
        rows = self._cron.status_payload()
        page, meta = self._page(rows, args.get("offset"), args.get("limit"))
        running = sum(1 for r in page if r.get("status") == "running")
        summary = "{} job(s); {} running in this page".format(
            meta["total"], running
        )
        return _result({"status": page, "page": meta}, summary)

    async def _t_list_jobs(self, args: Dict[str, Any]) -> Dict[str, Any]:
        rows = self._cron.jobs_payload()
        flt = args.get("filter")
        if isinstance(flt, str) and flt:
            low = flt.lower()
            rows = [r for r in rows if low in r["name"].lower()]
        state = args.get("state")
        if state == "running":
            rows = [r for r in rows if r.get("running")]
        elif state == "disabled":
            rows = [r for r in rows if not r.get("enabled")]
        elif state == "scheduled":
            rows = [
                r for r in rows if r.get("enabled") and not r.get("running")
            ]
        page, meta = self._page(rows, args.get("offset"), args.get("limit"))
        return _result(
            {"jobs": page, "page": meta},
            "{} matching job(s); {} returned".format(
                meta["total"], meta["returned"]
            ),
        )

    async def _t_get_job(self, args: Dict[str, Any]) -> Dict[str, Any]:
        name = _req_str(args, "name")
        payload = self._cron.job_detail_payload(name)
        if payload is None:
            return _tool_error(
                "job not found: {!r}. Use cron_list_jobs to enumerate.".format(
                    name
                )
            )
        return _result(payload, "job {!r}".format(name))

    async def _t_list_runs(self, args: Dict[str, Any]) -> Dict[str, Any]:
        name = _req_str(args, "name")
        payload = self._cron.job_runs_payload(name)
        if payload is None:
            return _tool_error("job not found: {!r}".format(name))
        limit = self._clamp_limit(args.get("limit"))
        all_runs = payload["runs"]
        payload["runs"] = all_runs[-limit:]
        payload["totalRuns"] = len(all_runs)
        payload["returnedRuns"] = len(payload["runs"])
        return _result(
            payload,
            "job {!r}: {} run(s) retained, {} returned".format(
                name, len(all_runs), len(payload["runs"])
            ),
        )

    async def _t_get_job_trends(self, args: Dict[str, Any]) -> Dict[str, Any]:
        name = _req_str(args, "name")
        payload = await self._cron.job_trends_payload(name)
        if payload is None:
            return _tool_error("job not found: {!r}".format(name))
        return _result(payload, "trends for job {!r}".format(name))

    async def _t_get_job_resources(
        self, args: Dict[str, Any]
    ) -> Dict[str, Any]:
        name = _req_str(args, "name")
        max_runs = self._clamp_limit(args.get("runs"))
        payload = self._cron.job_resources_payload(name, max_runs)
        if payload is None:
            return _tool_error("job not found: {!r}".format(name))
        return _result(payload, "resource series for job {!r}".format(name))

    async def _t_get_cluster(self, args: Dict[str, Any]) -> Dict[str, Any]:
        payload = self._cron.cluster_payload()
        return _result(
            payload,
            "cluster enabled={}".format(payload.get("enabled")),
        )

    async def _t_get_fleet(self, args: Dict[str, Any]) -> Dict[str, Any]:
        payload = self._cron.fleet_payload()
        return _result(
            payload, "fleet enabled={}".format(payload.get("enabled"))
        )

    async def _t_get_node(self, args: Dict[str, Any]) -> Dict[str, Any]:
        payload = self._cron.node_payload(history=bool(args.get("history")))
        return _result(payload, "node {}".format(payload.get("node_name")))

    async def _t_query_metrics(self, args: Dict[str, Any]) -> Dict[str, Any]:
        match = args.get("match")
        if match is not None and not isinstance(match, str):
            raise _ToolInputError("`match` must be a string")
        limit = self._clamp_limit(args.get("limit"))
        samples, total = _filter_metric_samples(
            self._cron.metrics.iter_samples(self._cron), match, limit
        )
        return _result(
            {
                "samples": samples,
                "totalMatched": total,
                "returned": len(samples),
                "match": match,
            },
            "{} metric sample(s) matched, {} returned".format(
                total, len(samples)
            ),
        )

    async def _t_schedule_pressure(
        self, args: Dict[str, Any]
    ) -> Dict[str, Any]:
        # no clamp here: croninfo.schedule_pressure clamps hours to
        # [1, 168] authoritatively and echoes the clamped value back in
        # the payload; only the default is applied at this layer.
        hours = _opt_int(args.get("hours"))
        hours = 24 if hours is None else hours
        tz = args.get("tz")
        if tz is not None and not isinstance(tz, str):
            raise _ToolInputError("`tz` must be an IANA timezone string")
        try:
            # offloaded to the default executor (see the _async wrapper in
            # cron.py): the up-to-168h occurrence walk is pure CPU and must
            # not stall job dispatch on the scheduler's event loop.
            payload = await self._cron.schedule_pressure_payload_async(
                hours, tz or None
            )
        except ValueError as err:
            return _tool_error(str(err))
        busiest = payload["busiest_minute"]
        return _result(
            payload,
            "{} fire(s) from {} job(s) in the next {}h; busiest minute :{:02d}"
            " ({} job(s)), {} minute(s) empty".format(
                payload["total_fires"],
                payload["jobs"],
                payload["hours"],
                busiest["minute"],
                busiest["jobs"],
                len(payload["empty_minutes"]),
            ),
        )

    async def _t_schedule_duplicates(
        self, args: Dict[str, Any]
    ) -> Dict[str, Any]:
        # offloaded (see cron.py): the fleet walk must not block the loop.
        payload = await self._cron.schedule_duplicates_payload_async()
        groups = payload["groups"]
        biggest = (
            "; biggest: {} job(s) sharing '{}'".format(
                groups[0]["count"], groups[0]["expression"]
            )
            if groups
            else ""
        )
        return _result(
            payload,
            "{} duplicate group(s) across {} scheduled job(s){}".format(
                len(groups), payload["jobs"], biggest
            ),
        )

    async def _t_suggest_slot(self, args: Dict[str, Any]) -> Dict[str, Any]:
        period = args.get("period") or "hourly"
        tz = args.get("tz")
        if tz is not None and not isinstance(tz, str):
            raise _ToolInputError("`tz` must be an IANA timezone string")
        try:
            # offloaded (see cron.py): the 24h fleet walk must not block
            # the loop; the bad-period/timezone ValueError still surfaces
            # at the await.
            payload = await self._cron.schedule_suggest_payload_async(
                period, tz or None
            )
        except ValueError as err:
            return _tool_error(str(err))
        return _result(
            payload,
            "least-loaded {} slot: '{}' ({} fire(s) already there "
            "in 24h)".format(
                payload["period"],
                payload["expression"],
                payload["fires_in_window"],
            ),
        )

    @staticmethod
    def _preview_args(
        args: Dict[str, Any],
    ) -> Tuple[Optional[str], Optional[str]]:
        """The shared `tz`/`seed` arguments of the schedule sandboxes."""
        tz = args.get("tz")
        if tz is not None and not isinstance(tz, str):
            raise _ToolInputError("`tz` must be an IANA timezone string")
        seed = args.get("seed")
        if seed is not None and not isinstance(seed, str):
            raise _ToolInputError("`seed` must be a string (a job name)")
        return (tz or None), (seed or None)

    async def _t_validate_schedule(
        self, args: Dict[str, Any]
    ) -> Dict[str, Any]:
        expr = _req_str(args, "expression")
        tz, seed = self._preview_args(args)
        try:
            # count=1: the gate needs validity, lint and never_fires, not a
            # fire list; the single fire keeps never_fires truthful and
            # doubles as a confirmation of the first launch instant.
            payload = self._cron.schedule_preview_payload(
                expr, tz, count=1, seed=seed
            )
        except ValueError as err:
            return _tool_error(str(err))
        return _result(payload, _preview_summary(payload))

    async def _t_explain_schedule(
        self, args: Dict[str, Any]
    ) -> Dict[str, Any]:
        expr = _req_str(args, "expression")
        tz, seed = self._preview_args(args)
        count = _opt_int(args.get("count"))
        count = 5 if count is None else max(1, min(count, 60))
        try:
            payload = self._cron.schedule_preview_payload(
                expr, tz, count=count, seed=seed
            )
        except ValueError as err:
            return _tool_error(str(err))
        return _result(payload, _preview_summary(payload))

    async def _t_why_no_run(self, args: Dict[str, Any]) -> Dict[str, Any]:
        name = _req_str(args, "name")
        at = _req_str(args, "at")
        try:
            payload = self._cron.schedule_why_payload(name, at)
        except ValueError as err:
            return _tool_error(str(err))
        if payload is None:
            return _tool_error(
                "job not found: {!r}. Use cron_list_jobs to enumerate.".format(
                    name
                )
            )
        return _result(payload, _why_summary(payload))

    async def _t_get_version(self, args: Dict[str, Any]) -> Dict[str, Any]:
        data = {
            "version": _version.version,
            "job_set_id": self._cron.job_set_id(),
            "jobs": len(self._cron.cron_jobs),
        }
        return _result(
            data,
            "cronstable {} - {} job(s)".format(data["version"], data["jobs"]),
        )

    async def _t_tail_job_logs(self, args: Dict[str, Any]) -> Dict[str, Any]:
        name = _req_str(args, "name")
        tail = self._clamp_limit(args.get("tail"))
        payload = self._cron.job_logs_tail_payload(
            name, tail=tail, cursor=_opt_int(args.get("cursor"))
        )
        if payload is None:
            return _tool_error("job not found: {!r}".format(name))
        return _result(
            payload,
            "job {!r}: {} line(s) (cursor {})".format(
                name, len(payload["lines"]), payload["cursor"]
            ),
        )

    # -- dags tool handlers -----------------------------------------------

    async def _t_list_dags(self, args: Dict[str, Any]) -> Dict[str, Any]:
        dags = await self._cron.dags_payload()
        return _result({"dags": dags}, "{} DAG(s)".format(len(dags)))

    async def _t_list_dag_runs(self, args: Dict[str, Any]) -> Dict[str, Any]:
        dag = _req_str(args, "dag")
        limit = self._clamp_limit(args.get("limit"))
        runs = await self._cron._dag.list_runs(dag, limit=limit)
        if runs is None:
            return _tool_error("dag not found: {!r}".format(dag))
        return _result(
            {"dag": dag, "runs": runs},
            "dag {!r}: {} run(s)".format(dag, len(runs)),
        )

    async def _t_get_dag_run(self, args: Dict[str, Any]) -> Dict[str, Any]:
        dag = _req_str(args, "dag")
        run_key = _req_str(args, "run_key")
        body = await self._cron._dag.get_run(dag, run_key)
        if body is None:
            return _tool_error(
                "dag run not found: {!r}/{!r}".format(dag, run_key)
            )
        return _result(body, "dag {!r} run {!r}".format(dag, run_key))

    async def _t_get_dag_xcom(self, args: Dict[str, Any]) -> Dict[str, Any]:
        dag = _req_str(args, "dag")
        run_key = _req_str(args, "run_key")
        result = await self._cron._dag.xcom_for_run(dag, run_key)
        if result is None:
            return _tool_error(
                "dag run not found: {!r}/{!r}".format(dag, run_key)
            )
        return _result(
            result, "xcom for dag {!r} run {!r}".format(dag, run_key)
        )

    async def _t_tail_dag_task_logs(
        self, args: Dict[str, Any]
    ) -> Dict[str, Any]:
        dag = _req_str(args, "dag")
        run_key = _req_str(args, "run_key")
        taskkey = _req_str(args, "taskkey")
        tail = self._clamp_limit(args.get("tail"))
        payload = self._cron.dag_task_logs_tail_payload(
            dag,
            run_key,
            taskkey,
            tail=tail,
            cursor=_opt_int(args.get("cursor")),
        )
        if payload is None:
            return _tool_error("dag not found: {!r}".format(dag))
        return _result(
            payload,
            "dag {!r} task {!r}: {} line(s)".format(
                dag, taskkey, len(payload["lines"])
            ),
        )

    # -- state tool handler -----------------------------------------------

    async def _t_inspect_state(self, args: Dict[str, Any]) -> Dict[str, Any]:
        ns = args.get("ns")
        stream = args.get("stream")
        if ns is not None and stream is not None:
            raise _ToolInputError("pass at most one of `ns` or `stream`")
        if ns is not None:
            payload = await self._cron.state_documents_payload(str(ns))
            return _result(payload, "state documents in {!r}".format(ns))
        if stream is not None:
            limit = self._clamp_limit(args.get("limit"))
            payload = await self._cron.state_records_payload(
                str(stream), limit=limit
            )
            return _result(payload, "state records in {!r}".format(stream))
        overview = await self._cron.state_payload()
        return _result(
            overview,
            "state store enabled={}".format(overview.get("enabled")),
        )

    # -- act (mutating) tool handlers -------------------------------------

    async def _t_run_job(self, args: Dict[str, Any]) -> Dict[str, Any]:
        name = _req_str(args, "name")
        _require_confirm(args, "running")
        await self._cron.start_job_by_name(name)
        return _result({"started": name}, "started job {!r}".format(name))

    async def _t_cancel_job(self, args: Dict[str, Any]) -> Dict[str, Any]:
        name = _req_str(args, "name")
        _require_confirm(args, "cancelling")
        count = await self._cron.cancel_job_by_name(name)
        return _result(
            {"cancelled": name, "instances": count},
            "cancelled {} instance(s) of job {!r}".format(count, name),
        )

    async def _t_pause_job(self, args: Dict[str, Any]) -> Dict[str, Any]:
        name = _req_str(args, "name")
        _require_confirm(args, "pausing")
        duration: Optional[int] = None
        if args.get("durationSeconds") is not None:
            duration = _opt_int(args["durationSeconds"])
            if duration is None:
                raise _ToolInputError("durationSeconds must be an integer")
        note = args.get("note")
        if note is not None and not isinstance(note, str):
            raise _ToolInputError("note must be a string")
        record = await self._cron.pause_job_by_name(
            name, duration=duration, note=note or "", by="mcp", channel="mcp"
        )
        return _result(
            {"paused": name, "until": record["until"]},
            "paused job {!r} until {}".format(name, record["until"]),
        )

    async def _t_resume_job(self, args: Dict[str, Any]) -> Dict[str, Any]:
        name = _req_str(args, "name")
        _require_confirm(args, "resuming")
        await self._cron.resume_job_by_name(name, by="mcp", channel="mcp")
        return _result({"resumed": name}, "resumed job {!r}".format(name))

    async def _t_trigger_dag(self, args: Dict[str, Any]) -> Dict[str, Any]:
        dag = _req_str(args, "dag")
        _require_confirm(args, "triggering")
        run_key = await self._cron._dag.trigger_run(dag)
        if run_key is None:
            return _tool_error("dag not found: {!r}".format(dag))
        return _result(
            {"dag": dag, "runKey": run_key},
            "triggered dag {!r} (run {})".format(dag, run_key),
        )

    async def _t_backfill_dag(self, args: Dict[str, Any]) -> Dict[str, Any]:
        dag = _req_str(args, "dag")
        start = _req_str(args, "from")
        end = _req_str(args, "to")
        if dag not in self._cron.cron_dags:
            return _tool_error("dag not found: {!r}".format(dag))
        # dry_run defaults TRUE, tested by IDENTITY like _require_confirm:
        # only the literal boolean false may take the destructive branch.
        # ``args.get("dry_run", True)`` applied the default only when the
        # key was ABSENT, so a present-but-falsy value -- null (exactly how
        # an MCP client or LLM encodes "unspecified"), [], {}, "", 0 --
        # fell through the preview gate into a real backfill.
        if args.get("dry_run") is not False:
            return _result(
                {
                    "dag": dag,
                    "from": start,
                    "to": end,
                    "dryRun": True,
                    "wouldExecute": False,
                },
                "DRY RUN: would backfill dag {!r} from {} to {}. Call again "
                "with dry_run=false and confirm=true to execute.".format(
                    dag, start, end
                ),
            )
        _require_confirm(args, "backfilling")
        result = await self._cron._dag.backfill(dag, start, end)
        if not result.get("ok"):
            return _tool_error(str(result.get("reason")))
        return _result(
            result, "backfilled dag {!r} from {} to {}".format(dag, start, end)
        )

    async def _t_decide_gate(self, args: Dict[str, Any]) -> Dict[str, Any]:
        dag = _req_str(args, "dag")
        run_key = _req_str(args, "run_key")
        taskkey = _req_str(args, "taskkey")
        decision = args.get("decision")
        if decision not in ("approve", "reject"):
            raise _ToolInputError("decision must be 'approve' or 'reject'")
        _require_confirm(args, "deciding an approval gate")
        by = str(args.get("by") or "mcp")
        result = await self._cron._dag.approve(
            dag, run_key, taskkey, approved=(decision == "approve"), by=by
        )
        if not result.get("ok"):
            return _tool_error(str(result.get("reason")))
        return _result(
            result, "{}d gate {!r} on dag {!r}".format(decision, taskkey, dag)
        )

    # -- resources (URI-addressable read-only context) --------------------

    async def _m_resources_list(
        self, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        resources = [
            {
                "uri": r["uri"],
                "name": r["name"],
                "title": r["title"],
                "description": r["description"],
                "mimeType": RESOURCE_MIME,
            }
            for r in self._resources
            if self._resource_visible(r)
        ]
        return {"resources": resources}

    async def _m_resource_templates_list(
        self, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        templates = [
            {
                "uriTemplate": t["uriTemplate"],
                "name": t["name"],
                "title": t["title"],
                "description": t["description"],
                "mimeType": RESOURCE_MIME,
            }
            for t in self._templates
            if self._resource_visible(t)
        ]
        return {"resourceTemplates": templates}

    async def _m_resources_read(
        self, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        uri = params.get("uri")
        if not isinstance(uri, str):
            raise MCPError(INVALID_PARAMS, "resources/read requires a 'uri'")
        loader, args = self._match_resource(uri)
        if loader is None:
            raise MCPError(
                RESOURCE_NOT_FOUND, "resource not found: {}".format(uri)
            )
        try:
            data = await loader(*args)
        except ApiActionError as ex:
            raise MCPError(RESOURCE_NOT_FOUND, ex.message) from ex
        if data is None:
            raise MCPError(
                RESOURCE_NOT_FOUND, "resource not found: {}".format(uri)
            )
        return {
            "contents": [
                {
                    "uri": uri,
                    "mimeType": RESOURCE_MIME,
                    "text": _dumps(data).decode("utf-8"),
                }
            ]
        }

    def _match_resource(
        self, uri: str
    ) -> Tuple[Optional[Callable[..., Any]], Tuple[str, ...]]:
        """Resolve a URI to a loader + captured args, or ``(None, ())``."""
        fixed = self._resource_by_uri.get(uri)
        if fixed is not None and self._resource_visible(fixed):
            return fixed["loader"], ()
        for tmpl in self._templates:
            if not self._resource_visible(tmpl):
                continue
            m = tmpl["regex"].match(uri)
            if m is not None:
                return tmpl["loader"], m.groups()
        return None, ()

    def _build_resources(
        self,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        cron = self._cron

        async def version_data() -> Dict[str, Any]:
            return {
                "version": _version.version,
                "job_set_id": cron.job_set_id(),
                "jobs": len(cron.cron_jobs),
            }

        async def status_data() -> Dict[str, Any]:
            return {"status": cron.status_payload()}

        async def dag_detail(name: str) -> Optional[Dict[str, Any]]:
            for entry in await cron.dags_payload():
                if entry.get("name") == name:
                    return entry
            return None

        # fixed resources
        fixed = [
            (
                "cronstable://status",
                "status",
                "Job status",
                "Live status of every job.",
                "observe",
                status_data,
            ),
            (
                "cronstable://cluster",
                "cluster",
                "Cluster view",
                "This node's cluster/leadership view.",
                "observe",
                _async(cron.cluster_payload),
            ),
            (
                "cronstable://fleet",
                "fleet",
                "Fleet view",
                "The cluster-wide jobs x nodes matrix.",
                "observe",
                _async(cron.fleet_payload),
            ),
            (
                "cronstable://version",
                "version",
                "Version",
                "Daemon version, job-set id and job count.",
                "observe",
                version_data,
            ),
        ]
        resources = [
            {
                "uri": uri,
                "name": name,
                "title": title,
                "description": desc,
                "toolset": toolset,
                "loader": loader,
            }
            for uri, name, title, desc, toolset, loader in fixed
        ]
        # resource templates: (uriTemplate, regex, name, title, desc, toolset,
        #  loader(*groups))
        templates_spec = [
            (
                "cronstable://jobs/{name}",
                r"^cronstable://jobs/([^/]+)$",
                "job",
                "Job detail",
                "Full detail for one job.",
                "observe",
                _async1(cron.job_detail_payload),
            ),
            (
                "cronstable://jobs/{name}/runs",
                r"^cronstable://jobs/([^/]+)/runs$",
                "job-runs",
                "Job run history",
                "Retained run history + stats for one job.",
                "observe",
                _async1(cron.job_runs_payload),
            ),
            (
                "cronstable://dags/{name}",
                r"^cronstable://dags/([^/]+)$",
                "dag",
                "DAG detail",
                "One DAG's tasks and dependencies.",
                "dags",
                dag_detail,
            ),
            (
                "cronstable://dags/{name}/runs/{run_key}",
                r"^cronstable://dags/([^/]+)/runs/([^/]+)$",
                "dag-run",
                "DAG run",
                "One DAG run's full document.",
                "dags",
                cron._dag.get_run,
            ),
            (
                "cronstable://state/{ns}",
                r"^cronstable://state/(.+)$",
                "state-ns",
                "State namespace",
                "Redacted documents of a kv/|cursor/|idem/ namespace.",
                "state",
                cron.state_documents_payload,
            ),
        ]
        templates = [
            {
                "uriTemplate": tmpl,
                "regex": re.compile(rx),
                "name": name,
                "title": title,
                "description": desc,
                "toolset": toolset,
                "loader": loader,
            }
            for tmpl, rx, name, title, desc, toolset, loader in templates_spec
        ]
        return resources, templates

    # -- prompts (canned triage playbooks) --------------------------------

    async def _m_prompts_list(self, params: Dict[str, Any]) -> Dict[str, Any]:
        prompts = [
            {
                "name": p["name"],
                "title": p["title"],
                "description": p["description"],
                "arguments": p["arguments"],
            }
            for p in self._prompts
            if self._prompt_visible(p)
        ]
        return {"prompts": prompts}

    async def _m_prompts_get(self, params: Dict[str, Any]) -> Dict[str, Any]:
        name = params.get("name")
        prompt = self._prompt_by_name.get(name) if name else None
        if prompt is None or not self._prompt_visible(prompt):
            raise MCPError(INVALID_PARAMS, "unknown prompt: {}".format(name))
        args = params.get("arguments") or {}
        if not isinstance(args, dict):
            args = {}
        text = prompt["render"](args)
        return {
            "description": prompt["description"],
            "messages": [
                {
                    "role": "user",
                    "content": {"type": "text", "text": text},
                }
            ],
        }

    def _build_prompts(self) -> List[Dict[str, Any]]:
        def arg(name: str, desc: str, required: bool = True) -> Dict[str, Any]:
            return {"name": name, "description": desc, "required": required}

        def triage(a: Dict[str, Any]) -> str:
            job = a.get("job", "<job>")
            return (
                "Investigate why the cronstable job '{0}' is failing. Steps:\n"
                "1. cron_get_job(name='{0}') and cron_list_runs(name='{0}') "
                "for the recent outcomes.\n"
                "2. cron_get_job_trends(name='{0}') to see if this is new or "
                "chronic.\n"
                "3. cron_tail_job_logs(name='{0}') for the failing output.\n"
                "4. cron_get_node() / cron_get_cluster() to rule out host or "
                "quorum problems.\n"
                "Then give a root-cause hypothesis, the blast radius, and the "
                "safest next action (do NOT run or cancel anything without "
                "asking)."
            ).format(job)

        def dag_fail(a: Dict[str, Any]) -> str:
            return (
                "Diagnose the failed DAG run '{1}' of dag '{0}'. Use "
                "cron_get_dag_run(dag='{0}', run_key='{1}') to find the "
                "failed task(s), cron_tail_dag_task_logs(...) for their "
                "output, and cron_get_dag_xcom(dag='{0}', run_key='{1}') for "
                "the data they passed. Explain which task failed, why, and "
                "what downstream tasks were blocked."
            ).format(a.get("dag", "<dag>"), a.get("run_key", "<run_key>"))

        def blast(a: Dict[str, Any]) -> str:
            return (
                "Assess the blast radius of an incident involving '{0}'. Use "
                "cron_get_status and cron_get_fleet to find other affected "
                "jobs, cron_list_dags to see which DAGs depend on it, and "
                "cron_inspect_state to check for shared locks/cursors it "
                "holds. Summarize what else is at risk if it stays broken."
            ).format(a.get("target", "<target>"))

        def fleet(a: Dict[str, Any]) -> str:
            return (
                "Summarize overall cronstable health for a status update. Use "
                "cron_get_fleet, cron_get_cluster and cron_get_status to "
                "report: how many jobs are failing vs healthy, the cluster "
                "quorum/leadership state, and any node under resource "
                "pressure (cron_get_node). Lead with the single most "
                "important thing."
            )

        def backfill_plan(a: Dict[str, Any]) -> str:
            return (
                "Plan a backfill of dag '{0}' from {1} to {2}. First run "
                "cron_backfill_dag(dag='{0}', from='{1}', to='{2}') with its "
                "default dry_run to preview the range, confirm the DAG exists "
                "and the window is sane, then explain what a real backfill "
                "would do. Only propose the real run (dry_run=false, "
                "confirm=true) after the operator agrees."
            ).format(
                a.get("dag", "<dag>"),
                a.get("from", "<from>"),
                a.get("to", "<to>"),
            )

        return [
            {
                "name": "triage_job_failure",
                "title": "Triage a job failure",
                "description": "Root-cause a failing job from its runs, "
                "trends, logs and host health.",
                "arguments": [arg("job", "the failing job's name")],
                "toolset": "observe",
                "render": triage,
            },
            {
                "name": "blast_radius",
                "title": "Assess blast radius",
                "description": "Scope what else is at risk from a broken job "
                "or DAG.",
                "arguments": [arg("target", "a job or dag name")],
                "toolset": "observe",
                "render": blast,
            },
            {
                "name": "fleet_health_summary",
                "title": "Fleet health summary",
                "description": "A wallboard-style digest of cluster + fleet + "
                "job health.",
                "arguments": [],
                "toolset": "observe",
                "render": fleet,
            },
            {
                "name": "why_did_dag_run_fail",
                "title": "Diagnose a failed DAG run",
                "description": "Walk a failed DAG run's tasks, logs and XCom "
                "to find the cause.",
                "arguments": [
                    arg("dag", "the DAG name"),
                    arg("run_key", "the failed run key"),
                ],
                "toolset": "dags",
                "render": dag_fail,
            },
            {
                "name": "backfill_plan",
                "title": "Plan a DAG backfill",
                "description": "Preview and reason about a DAG backfill "
                "before proposing a real run.",
                "arguments": [
                    arg("dag", "the DAG name"),
                    arg("from", "ISO start date"),
                    arg("to", "ISO end date"),
                ],
                "toolset": "dags",
                "render": backfill_plan,
            },
        ]


# -- module-level helpers -------------------------------------------------


def _async(fn: Callable[[], Any]) -> Callable[[], Awaitable[Any]]:
    """Wrap a sync payload builder as a zero-arg coroutine for a resource."""

    async def loader() -> Any:
        return fn()

    return loader


def _async1(fn: Callable[[str], Any]) -> Callable[[str], Awaitable[Any]]:
    """Wrap a sync one-arg payload builder as a coroutine for a template."""

    async def loader(arg: str) -> Any:
        return fn(arg)

    return loader


# JSON Schema fragments (draft 2020-12, the MCP default dialect).
_STR = {"type": "string"}
_INT = {"type": "integer"}
_BOOL = {"type": "boolean"}


def _enum(values: List[str]) -> Dict[str, Any]:
    return {"type": "string", "enum": values}


def _obj_schema(
    properties: Dict[str, Any], required: Optional[List[str]] = None
) -> Dict[str, Any]:
    schema: Dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }
    if required:
        schema["required"] = required
    return schema


def _result(structured: Dict[str, Any], summary: str) -> Dict[str, Any]:
    """A successful tool result: a text summary plus the structured object.

    The ``text`` block mirrors ``structuredContent`` for clients that do not
    read structured output; modern clients parse the object.
    """
    return {
        "content": [{"type": "text", "text": summary}],
        "structuredContent": structured,
    }


def _tool_error(message: str) -> Dict[str, Any]:
    """A tool-execution failure (isError:true), readable by the model."""
    return {"content": [{"type": "text", "text": message}], "isError": True}


def _preview_summary(payload: Dict[str, Any]) -> str:
    """The one-line verdict of a validate/explain schedule payload."""
    if not payload.get("valid"):
        return "INVALID: {}".format(payload.get("error"))
    if payload.get("reboot"):
        return (
            "valid: @reboot runs once when the daemon starts, never on a "
            "timetable"
        )
    parts = ["valid: {}".format(payload["description"])]
    if payload.get("never_fires"):
        parts.append(
            "WARNING: no future occurrence, this schedule never fires"
        )
    else:
        warnings = sum(
            1 for f in payload["lint"] if f.get("level") == "warning"
        )
        notes = len(payload["lint"]) - warnings
        if warnings or notes:
            parts.append(
                "{} lint warning(s), {} note(s)".format(warnings, notes)
            )
        fires = payload.get("fires") or []
        if fires:
            parts.append("first fire {}".format(fires[0]))
    return "; ".join(parts)


def _why_summary(payload: Dict[str, Any]) -> str:
    """The one-line verdict of a cron_why_no_run payload."""
    name = payload["job"]
    if payload.get("reboot"):
        return (
            "job {!r} is @reboot: it runs once when the daemon starts and "
            "never fires on a timetable".format(name)
        )
    if payload["matches"]:
        text = "YES: the schedule of job {!r} selects {}".format(
            name, payload["at_in_zone"]
        )
        # a DST note rewrites the story ("fired at the shifted wall time"),
        # so it belongs in the one-line verdict, not only in the notes.
        for note in payload["notes"]:
            if note["code"].startswith("dst-"):
                text += "; BUT " + note["message"]
        if not payload["enabled"]:
            return text + ", but the job is disabled, so it did not launch"
        return text + (
            "; if no run is on record (cron_list_runs), look at execution, "
            "not the schedule: daemon downtime, concurrencyPolicy, or "
            "cluster leadership"
        )
    matched = [c["field"] for c in payload["checks"] if c["matched"]]
    failed = [
        "{} {} is not in {}".format(c["field"], c["label"], c["allowed"])
        for c in payload["checks"]
        if not c["matched"]
    ]
    text = "NO"
    if matched:
        text += ": {} matched".format(", ".join(matched))
    text += "; " + "; ".join(failed)
    if not payload["enabled"]:
        text += " (the job is also disabled)"
    return text


def _req_str(args: Dict[str, Any], key: str) -> str:
    value = args.get(key)
    if not isinstance(value, str) or not value:
        raise _ToolInputError(
            "missing or empty required string argument: {!r}".format(key)
        )
    return value


def _opt_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    # OverflowError: int(float("inf")) raises it, and stdlib json parses
    # the legal literal 1e999 to inf -- fall back to the default like any
    # other unusable value instead of surfacing a -32603 internal error.
    except (TypeError, ValueError, OverflowError):
        return None


def _require_confirm(args: Dict[str, Any], gerund: str) -> None:
    if args.get("confirm") is not True:
        raise _ToolInputError(
            "{} changes state; call again with confirm=true to proceed".format(
                gerund
            )
        )


def _id_of(msg: Any) -> Any:
    return msg.get("id") if isinstance(msg, dict) else None


def _error_envelope(msg_id: Any, code: int, message: str) -> Dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": msg_id,
        "error": {"code": code, "message": message},
    }
