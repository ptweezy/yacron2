# Design Document: A Model Context Protocol (MCP) Server for cronstable

**Status:** Implemented · **Author:** Principal Engineering · **Date:** 2026-07-08 · **Target spec:** MCP `2025-11-25`

> **This design shipped.** It was implemented as `cronstable/mcp.py` and
> `cronstable/mcpcli.py`; the user-facing documentation is the [MCP](MCP)
> page, and the shipped configuration fields are in the
> [`mcp` configuration reference](Configuration-Reference#mcp). The body of
> this document is preserved as written on 2026-07-08 and is not updated to
> match the implementation; where they differ, the implementation wins. The
> divergences:
>
> - **`--validate` shipped as `--check`** (§9): the stdio bridge's self-check
>   flag is `cronstable mcp --check`.
> - **No per-run job resource template** (§5.3): the proposed
>   `cronstable://jobs/{name}/runs/{run_id}` shipped as
>   `cronstable://jobs/{name}/runs` (the whole retained history, no
>   `run_id`). The DAG template kept `{run_key}`.
> - **Three additional config keys** (§7): the shipped `mcp:` block also
>   takes `allowUnauthenticated`, `resources`, and `prompts`.
> - **Stricter fail-closed rule** (§6/§7): there is no
>   bind-safe-listeners-and-warn mode for a mixed listen set. Startup raises
>   a `ConfigError` whenever any routable listener lacks a token;
>   `mcp.allowUnauthenticated: true` is the explicit override.
> - **Offset paging, not opaque cursors** (§5.1): list tools take
>   `offset`/`limit` and return a `nextOffset`; only the two log-tail tools
>   take a `cursor`, an integer position for polling newly appended lines.
> - **Resources and prompts are toolset-scoped** (§5.3/§5.4): the `dags`
>   resource templates and the `why_did_dag_run_fail` / `backfill_plan`
>   prompts require the `dags` toolset.

---

## 1. Executive summary

- **We are adding a first-party MCP server** so AI agents and operators (Claude Desktop/Code, Cursor, VS Code Copilot, ChatGPT connectors) can **observe, act on, and reason over** cronstable's jobs, DAGs, cluster/fleet, metrics, and durable state through the same surface humans use in the dashboard and REST API.
- **Headline recommendation: hand-roll a small, pure-Python MCP layer over the existing aiohttp apiserver, do NOT vendor the official `mcp` Python SDK.** The SDK's transitive tree (`starlette`, `pydantic`/`pydantic-core` [Rust], `cryptography` [Rust/C], `rpds-py` [Rust], `uvicorn`, `anyio`, `httpx`) is a categorical departure from cronstable's minimal-dependency, pure-Python, multi-arch/distroless story and cannot be avoided even via the SDK's low-level API. MCP is just JSON-RPC 2.0 over "Streamable HTTP", the exact shape cronstable already hand-rolled for its k8s/etcd clients and SSE log endpoints ([spec: transports](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)).
- **Two transports, one core:** embed a stateless `POST /mcp` route inside the existing `web` server (`Cron.start_stop_web_app` in `cronstable/cron.py`), reusing its listeners, bearer/mTLS/unix-socket auth, and reload lifecycle; and ship a featherweight `cronstable mcp` **stdio bridge** (a urllib frame-proxy, zero daemon imports, like `cronstable/jobcli.py`) for local desktop clients. Tool logic lives in exactly one place.
- **Safe-by-default, GitHub-style read/write separation:** `readOnly: true` by default strips every mutating tool; disableable **toolsets** (`observe`, `act`, `dags`, `state`) keep the tool count and context lean ([GitHub MCP server](https://github.com/github/github-mcp-server/blob/main/docs/server-configuration.md), [Grafana MCP](https://grafana.com/docs/grafana/latest/developer-resources/mcp/)). Mutating tools require an explicit `confirm`/`dry_run` argument and carry honest annotations, with server-side authorization as the real guard. Annotations are untrusted hints, not a security boundary ([tool annotations](https://blog.modelcontextprotocol.io/posts/2026-03-16-tool-annotations/)).
- **Auth reuses what cronstable already has, and fails closed.** The spec explicitly blesses cronstable's exact posture for a hardened local ops server: stdio, or HTTP restricted by (a) a bearer token and/or (b) a unix socket with filesystem-permission gating ([security best practices](https://modelcontextprotocol.io/docs/tutorials/security/security_best_practices)). **No OAuth is required** for the self-hosted case; full OAuth 2.1 Resource-Server work is deferred behind a clear "if you ever host a public multi-tenant endpoint" gate. Because cronstable's auth middleware only exists when a token resolves, the endpoint **fails closed**: with `mcp.enabled` set but no inherited token, cronstable refuses to serve `/mcp` on any non-loopback, non-socket listener (§6/§7).
- **SSE log streams map to a poll+cursor tool, not an infinite MCP stream**: no major client consumes an unbounded tool stream well. The existing dashboard SSE endpoints are untouched.
- **Design for statelessness now** (no `Mcp-Session-Id`, no `initialize` reliance for routing) so the imminent `2026-07-28` spec revision, which removes sessions and the handshake, is a near-no-op migration ([2026-07-28 RC](https://blog.modelcontextprotocol.io/posts/2026-07-28-release-candidate/)).
- **New/changed files are small and localized:** a new `cronstable/mcp.py` (protocol + tool registry), a new `cronstable/mcpcli.py` (stdio bridge), route/handler wiring in `cronstable/cron.py`, a `mcp:` schema in `cronstable/config.py`, a subcommand in `cronstable/__main__.py`, and `tests/test_mcp.py`.

---

## 2. What MCP is now (mid-2026)

MCP is an open JSON-RPC 2.0 protocol that lets an AI application (host/client) connect to external servers exposing **tools**, **resources**, and **prompts**. If your mental model is a year old, correct these points:

**Current authoritative revision is `2025-11-25`.** Revisions are `YYYY-MM-DD` date strings marking the last backwards-incompatible change; the line is `2024-11-05 → 2025-03-26 → 2025-06-18 → 2025-11-25 (current)` ([versioning](https://modelcontextprotocol.io/specification/versioning)). A **`2026-07-28` revision is a Release Candidate only** (locked 2026-05-21, publishes 2026-07-28, *after* today), so build to `2025-11-25` and treat `2026-07-28` as a forward-compatibility target ([RC](https://blog.modelcontextprotocol.io/posts/2026-07-28-release-candidate/)).

**Server primitives:**
- **Tools**: model-controlled actions. A tool has `name` (`[A-Za-z0-9_.-]`, 1–128 chars), `title`, `description`, `inputSchema` (a JSON Schema object, **default dialect 2020-12** as of `2025-11-25`), optional `outputSchema`, and `annotations`. Results carry an unstructured `content` array (`text`/`image`/`audio`/`resource_link`/embedded `resource`) and/or a machine-readable `structuredContent` object; you should mirror structured output into a `text` block for back-compat. **Two error channels:** JSON-RPC protocol errors (e.g. `-32601` unknown method) versus **Tool Execution Errors** returned as a normal result with `isError: true`, and since `2025-11-25`, *input-validation* failures belong in the latter so the model can self-correct ([tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)).
- **Resources**: application-controlled, URI-addressable read-only context (with **resource templates** for parameterized URIs). Cacheable, no side effects.
- **Prompts**: user-controlled workflow templates, typically surfaced as slash-commands.
- As of `2025-11-25`, tools/resources/prompts can all carry `icons`; `_meta` is a reserved reverse-DNS-keyed extension field.

**Behavioral annotations** (`readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`) are **hints for UX, not guarantees**. Clients must treat them as untrusted from an untrusted server ([annotations](https://blog.modelcontextprotocol.io/posts/2026-03-16-tool-annotations/)). Worst-case defaults a client assumes for an *unspecified* hint: `readOnlyHint=false`, `destructiveHint=true`, `idempotentHint=false`, `openWorldHint=true`. Note `openWorldHint` specifically distinguishes tools whose interaction domain is an **unpredictable, external open set** (e.g. web search) from tools operating on a **closed, well-defined domain** (e.g. a database or the server's own state). It is *not* a synonym for "reads external data" (§5.1).

**Client/interaction primitives**, sampling (now with tool-calling), **elicitation** (server requests structured user input; the standard human-in-the-loop primitive), roots, plus utilities: logging, completions, progress, cancellation, cursor-based pagination on list ops. Client support for these is **very uneven** (see §8); only **tools** are universal.

**Transports, exactly two:**
1. **stdio**: client launches the server as a subprocess; newline-delimited JSON-RPC on stdin/stdout. **stdout carries only MCP frames; all logging goes to stderr.**
2. **Streamable HTTP**: a **single endpoint** (e.g. `/mcp`) handling `POST` (one JSON-RPC message per request) and optionally `GET` (a server→client SSE stream). The server answers a `POST` request with **either** `application/json` (one object) **or** `text/event-stream` (SSE), and returns **`202 Accepted`** for a notification/response. **The old 2024-11-05 "HTTP+SSE" two-endpoint transport is deprecated** and being removed across the ecosystem in 2026. Do not implement it. **JSON-RPC batching was removed in `2025-06-18`.**

**HTTP rules that matter for us:** every post-initialize HTTP request MUST carry `MCP-Protocol-Version: <negotiated>` (missing ⇒ assume `2025-03-26`; unsupported ⇒ `400`). Clients MUST send `Accept: application/json, text/event-stream`. Servers MUST validate the `Origin` header and return **`403`** on a present-but-invalid Origin (DNS-rebinding defense), SHOULD bind local servers to `127.0.0.1`, and SHOULD authenticate. **Sessions (`Mcp-Session-Id`) are optional**; a stateless server assigns none, answers `application/json` to every POST and `405` to GET, and can sit behind a round-robin load balancer.

**Auth (HTTP only; stdio uses env credentials):** authorization is **OPTIONAL**. When used, the MCP server is an **OAuth 2.1 Resource Server only** (it validates tokens; a separate authorization server issues them), the `2025-03-26` model where the server was its own AS is gone. For a self-hosted local server the spec explicitly permits a **static bearer token and/or unix-socket IPC** instead of OAuth ([auth](https://modelcontextprotocol.io/specification/draft/basic/authorization), [security](https://modelcontextprotocol.io/docs/tutorials/security/security_best_practices)).

**Forward look (`2026-07-28` RC):** stateless protocol core (no `initialize` handshake, no `Mcp-Session-Id`; protocol version travels in `_meta`), an Extensions framework, Tasks graduating to a stateless extension for long-running work, and Sampling/Roots/Logging **deprecated**. None of this blocks us; a stateless design absorbs it.

---

## 3. Why MCP fits cronstable

cronstable already exposes a rich read/act surface over aiohttp (documented in [`wiki/HTTP-API.md`](HTTP-API)). MCP turns that surface into something an agent can drive with judgment. The value lands in three modes:

**Observe**, an agent can list jobs and their health (`GET /jobs`, `/status`), read run history/trends/resources (`/jobs/{name}/runs`, `/trends`, `/resources`), tail logs (`/jobs/{name}/logs`), read the single-pane cluster/fleet views (`/cluster`, `/fleet`, `/node`), parse Prometheus metrics (`/metrics`), inspect DAG runs (`/dags/...`), and browse the durable state store metadata (`/state`, `/state/documents`, `/state/records`).

**Act**, run or cancel a job (`POST /jobs/{name}/start|cancel`), trigger or backfill a DAG (`/dags/{name}/trigger|backfill`), and resolve an approval gate (`/dags/.../decision`). These are precisely the operator actions that benefit from an agent that has *already read the context*, and precisely the ones that need human-in-the-loop guardrails.

**Reason**, the real payoff. cronstable's data model (jobs ↔ DAG task dependencies ↔ shared locks/cursors/idempotency keys ↔ cluster leadership ↔ per-node load) is exactly the kind of correlated blast-radius reasoning agents are good at, if given the right tools. Canned **prompts** ("why is this job failing", "what's the blast radius of cancelling X", "summarize fleet health") chain the read tools into repeatable incident-triage playbooks. Because cronstable is a *cron replacement* that people wire jobs and DAGs around, "the agent can explain and safely operate the scheduler" is a headline capability, not a novelty.

Critically, cronstable is a good MCP citizen *architecturally*: it is a long-running daemon with a stable HTTP surface, honest structured JSON, existing auth, and existing SSE, so the MCP layer is a thin projection, not new subsystems.

---

## 4. Architecture decision: SDK vs hand-rolled, and transport

**This is the crux.** cronstable is aiohttp (not ASGI), minimal-dependency by design, shipped as PyInstaller single-file binaries across many exotic arches and as distroless/no-shell images, and security-hardened (non-root, read-only rootfs, all caps dropped, RuntimeDefault seccomp, strict CSP). Each of these weighs against the official SDK.

### 4.1 Recommendation: hand-roll a pure-Python MCP layer over aiohttp

**Rationale, the dependency tree is decisive.** cronstable's entire runtime graph today is pure-Python or ships wheels on every target: `strictyaml` (pure-Python, on `ruamel.yaml`), `aiohttp` (C-accelerated but wheels-everywhere with a pure fallback), `sentry-sdk`, `aiosmtplib`, `jinja2`, `tzdata` (all pure-Python), and `psutil` (C, but mainstream wheels + source build), see `pyproject.toml`. The team deliberately **hand-rolled the k8s and etcd clients over aiohttp** to avoid pulling `kubernetes`/`etcd3`/`grpc`. The official `mcp` SDK (currently `1.28.1`, requires Python ≥3.10) hard-depends transitively on **three Rust-compiled crates**, `pydantic-core` (via `pydantic`), `cryptography` (via `pyjwt[crypto]`), and `rpds-py` (via `jsonschema`), plus `starlette`, `uvicorn`, `anyio`, `httpx`, `httpx-sse`, `sse-starlette`, `python-multipart` ([mcp on PyPI](https://pypi.org/pypi/mcp/json)). Those Rust crates **do not publish wheels for riscv64 and are flaky on musl-armv7**, forcing per-target Rust+OpenSSL source builds and complicating the distroless image (bundling glibc/musl- and OpenSSL-matched `.so` files). **This cost is unavoidable even if we use only the SDK's low-level `mcp.server.Server`**, the import graph is the same. Adopting the SDK would single-handedly break the "zero-new-dep, architecture-portable core" invariant that motivates cronstable's whole packaging story.

Against that, the thing we'd be buying, spec compliance, is cheap here: **MCP is JSON-RPC 2.0 over a single POST endpoint with optional SSE**, and cronstable already owns every building block (JSON dispatch, bearer auth in `_make_auth_middleware`, aiohttp SSE in `_pump_output`/`_sse_send_line`, a JSON fast-path in `cronstable/_json.py` that prefers `orjson` when present). A minimal-but-complete server is a **JSON-RPC dispatcher over ~9 methods**, a few hundred pure-Python lines with **zero new dependencies**.

> **Standalone FastMCP (3.x) is also rejected**, it's a large superset (`fastmcp-slim` + integration extras) aimed at feature-rich standalone servers, even heavier than the official SDK for an embeddable daemon.

**Honest trade-offs of hand-rolling** (the alternative is the SDK):
- **We own spec compliance and must track spec changes.** This includes the two error-channel semantics (protocol error vs `isError`, and the `2025-11-25` SEP-1303 rule that input-validation failures are tool errors, not protocol errors) and JSON-Schema-2020-12 drift for our tool schemas, real, ongoing maintenance the SDK would otherwise absorb. Mitigation: pin to `2025-11-25`, generate/validate wire shapes against the canonical [`schema.ts`/`schema.json`](https://github.com/modelcontextprotocol/modelcontextprotocol/tree/main/schema) in tests (dev-only `jsonschema`, never a runtime dep), and keep the stateless profile small so there's less surface to drift. The protocol is stabilizing (the `2026-07-28` RC *simplifies* the HTTP core), so the maintenance slope is downhill.
- **We forgo SDK niceties** (auto-derived schemas from type hints, `Context` injection, structured-output plumbing). Mitigation: these are conveniences, not requirements; our tool set is curated and hand-written by design (Anthropic's own guidance is to *not* auto-generate 1:1 API wrappers, [writing tools for agents](https://www.anthropic.com/engineering/writing-tools-for-agents)).
- **No SDK-blessed transport.** But the SDK's transport is ASGI (`http_app()`), and cronstable is not ASGI; bridging it (e.g. `aiohttp-asgi`) risks buffering long-lived SSE and still drags the dep tree. Hand-rolling on aiohttp gives us native streaming and one shared port/route/auth.

### 4.2 Transports and how the endpoint mounts

**HTTP (primary, remote):** embed a **stateless Streamable HTTP** endpoint as a new route in `Cron.start_stop_web_app` (`cronstable/cron.py:2341`), alongside the existing `web.get/post` registrations. Because that method **rebuilds the route list on every reload**, the `MCPHandler` is constructed *there* (not once in `__init__`) so it always reflects the current config:

```python
# in start_stop_web_app, after building `routes` and (re)building self._mcp:
if self.mcp_config and self.mcp_config["enabled"]:
    self._mcp = MCPHandler(self, self.mcp_config)      # rebuilt on every reload
    routes.append(web.post("/mcp", self._mcp.handle_http))
    routes.append(web.get("/mcp", self._mcp.handle_http_get))    # -> 405 (stateless)
    routes.append(web.options("/mcp", self._mcp.handle_options))  # CORS preflight (if allowedOrigins)
```

- It **rides the existing listeners** (`web.listen`: `http://127.0.0.1:8080`, `unix:///run/cronstable/cronstable.sock`) via the unchanged `web_site_from_url`.
- It sits behind the **existing app-wide `_make_auth_middleware`** (`cron.py:3382`), so `web.authToken` protects `/mcp` identically, *when a token resolves*. When no token resolves, that middleware is **not installed at all** and every route is open; hence the fail-closed rule in §6/§7 that refuses a routable `/mcp` without a token. `/mcp` **must never** be added to `WEB_PUBLIC_PATHS` nor to the `metrics.public` exemption set (`cron.py:2363–2367`).
- It **restarts with the web app** on config reload, exactly like `metrics`/`nodeHistory`.
- The `Cron`-bound handler gives the tools in-process access to `self.cron_jobs`, `self.cron_dags`, `self.metrics`, and the internal helpers behind the `_web_*` handlers, **no HTTP hop, no second port.**

**Stateless profile mechanics** (what `handle_http` enforces):
- **Origin:** if present and not in `mcp.allowedOrigins`, return **`403`** (DNS-rebinding defense).
- **Accept:** stateless mode only ever emits `application/json`. Be lenient on a missing `Accept`; if `Accept` is present and does **not** include `application/json` (nor `*/*`), return **`406`**. (We never need `text/event-stream` acceptance in the MVP because we never upgrade a POST to SSE there.)
- **`MCP-Protocol-Version`:** if present and unsupported, **`400`**; if absent, assume `2025-03-26` for back-compat but negotiate `2025-11-25` at `initialize`.
- **Body size:** cap the request body at `mcp.maxBodyBytes` (default 1 MiB, aiohttp's `client_max_size`) and return **`413`** beyond it, tool arguments arrive from an LLM and must not be an unbounded-memory vector.
- Parse one JSON-RPC message. If it's a **notification/response**, return **`202`** empty. If it's a **request**, dispatch and return **one `application/json` object**. **No `Mcp-Session-Id`, no GET SSE stream, `GET /mcp` → `405`.** (Progress/log streaming via SSE is an optional later add-on, not part of the core.)

**stdio (local desktop clients):** a new `cronstable mcp` subcommand implemented in a new `cronstable/mcpcli.py`, modeled on `cronstable/jobcli.py`: it uses **stdlib `urllib` only (no aiohttp, no daemon import graph)**, reads newline-delimited JSON-RPC from stdin, forwards each frame to a running daemon's `POST /mcp` (adding `Authorization: Bearer` and `Accept: application/json`), and writes the reply to stdout. This keeps tool logic **in one place (the daemon)** and the entrypoint featherweight, inheriting the fast-start property of the other job-facing subcommands. It **requires a reachable running daemon**, the correct model for an ops tool; the URL/token come from `--url`/`--token` flags or env. **The stdio golden rule (stdout = frames only, logs → stderr) is trivial to honor in a thin proxy.**

- **Protocol-version sourcing (spelled out):** the bridge is a line-proxy that sees every frame, so it **passively parses the `initialize` result as it flows back, caches `result.protocolVersion`, and stamps `MCP-Protocol-Version` on every subsequent forwarded request**; before `initialize` completes (or when pinned via `--protocol-version`) it sends the build-time default `2025-11-25`. This resolves how a "dumb" proxy learns the negotiated value.
- **Known limitation (blocking urllib):** a synchronous line-proxy processes one frame at a time and has **no server→client channel**, so it cannot carry elicitation, sampling, or progress SSE. Those enhancements therefore apply only to the **direct HTTP(+SSE)** path; over the bridge, human-in-the-loop degrades to `confirm`/`dry_run` (the universal path anyway, §5.2/§8).

```
cronstable mcp --url http://127.0.0.1:8080 --token-env CRONSTABLE_WEB_TOKEN
```

**Stateless vs stateful:** **stateless.** cronstable has no per-connection MCP state worth keeping, stateless matches where the protocol is heading (`2026-07-28` drops sessions), and it avoids sticky-routing/session-store complexity behind a load balancer. If a future need for server→client progress on long backfills appears, add an SSE upgrade path on the same `POST /mcp` (return `text/event-stream` for that one call) without changing the endpoint shape.

### 4.3 Minimal-but-complete JSON-RPC method set

`cronstable/mcp.py` implements a dispatcher over these methods (shapes validated against `schema/2025-11-25/schema.json`):

| Method | Kind | Result shape (abbreviated) |
| --- | --- | --- |
| `initialize` | request | `{protocolVersion:"2025-11-25", capabilities:{…only for registered primitives…}, serverInfo:{name:"cronstable", version, title:"cronstable"}, instructions}` |
| `notifications/initialized` | notification (in) | *(no reply; return `None`)* |
| `ping` | request | `{}` |
| `tools/list` | request | `{tools:[{name,title,description,inputSchema,outputSchema?,annotations}], nextCursor?}` |
| `tools/call` | request | `{content:[{type:"text",text}], structuredContent?, isError?}` |
| `resources/list` | request | `{resources:[{uri,name,mimeType,...}], nextCursor?}` |
| `resources/templates/list` | request | `{resourceTemplates:[{uriTemplate,name,...}]}` |
| `resources/read` | request | `{contents:[{uri,mimeType,text}]}` |
| `prompts/list` | request | `{prompts:[{name,description,arguments:[{name,description,required}]}]}` |
| `prompts/get` | request | `{description, messages:[{role,content:{type:"text",text}}]}` |

**Capabilities are computed from what the running config actually registers, never hard-coded.** A server MUST NOT advertise a capability it doesn't implement: a conformant client that sees `resources`/`prompts` will call `resources/list`/`prompts/list` and, if unwired, get `-32601` ([lifecycle](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle)). Therefore a **tools-only** deployment advertises exactly `capabilities:{tools:{listChanged:false}}`; the `resources/*` and `prompts/*` methods (and their capability flags) appear only when the resource/prompt primitives are enabled. `listChanged` is `false` because our tool/resource/prompt sets are static per config.

Protocol faults use JSON-RPC error objects (`-32700` parse, `-32600` invalid request, `-32601` method not found, `-32602` invalid params, `-32603` internal). **Tool execution and input-validation failures return `{isError:true, content:[…]}`**, not JSON-RPC errors, so the model can self-correct (SEP-1303). Serialize all replies through `cronstable._json.dumps_bytes` (the orjson-accelerated serializer, which returns `bytes`) so orjson accelerates them when installed.

Proposed core signatures:

```python
# cronstable/mcp.py  (new; pure-Python, no new deps)
class MCPHandler:
    def __init__(self, cron: "Cron", config: dict) -> None: ...
    async def handle_message(self, msg: dict) -> Optional[dict]: ...      # JSON-RPC dispatch
    async def handle_http(self, request: web.Request) -> web.StreamResponse: ...
    async def handle_http_get(self, request: web.Request) -> web.StreamResponse: ...  # 405
    async def handle_options(self, request: web.Request) -> web.StreamResponse: ...   # CORS preflight
    def _capabilities(self) -> dict: ...                                 # gated by registered primitives
    def _list_tools(self) -> list[dict]: ...                             # honors readOnly + toolsets
    async def _call_tool(self, name: str, arguments: dict) -> dict: ...
```

Each tool implementation calls the *same internal helper the matching `_web_*` handler already uses*. Where a handler currently builds its payload inline (e.g. `_web_get_status`, `_web_list_jobs`), extract that body into a reusable method (e.g. `Cron.status_payload()`, `Cron.jobs_payload(filter, state, cursor, limit)`) that both the REST handler and the MCP tool call. This is a **contained but non-trivial** refactor, not a pure rename: several `_web_*` handlers read filters/cursors/limits off `request.query`, so the extraction must cleanly separate **HTTP-request parsing** (which stays in the handler) from **payload construction** (which moves into the shared method and takes plain arguments). Low risk given the existing `test_ui_endpoints.py`/`test_cron.py` coverage across ~10 handlers, but it warrants care.

---

## 5. Proposed capability catalog

Tools are grouped into **toolsets** (`observe`, `act`, `dags`, `state`). Default exposure is **`readOnly: true`** and **`toolsets: [observe]`**. `readOnly: true` strips every tool below the divider and **takes precedence** over `toolsets` (GitHub's pattern). Names are prefixed `cron_` for namespacing and tool-selection accuracy.

### 5.1 Read-only tools (`readOnlyHint: true`, `openWorldHint: false`)

These tools operate exclusively on cronstable's **own closed, well-defined domain**, its jobs, DAGs, cluster/fleet, metrics, and durable state. That is the textbook "closed domain" case for `openWorldHint: **false**` (contrast web search, which reaches an unpredictable external set). Setting `openWorldHint:true` here would wrongly invite clients to over-scrutinize benign local reads. No read tool below reaches a genuinely external, dynamic system, so all carry `openWorldHint:false`.

| Tool | Description | Maps to | Toolset | Key inputs |
| --- | --- | --- | --- | --- |
| `cron_list_jobs` | Compact rows (name, schedule, enabled, running, next-run, last outcome) for all jobs; filterable. | `GET /jobs` (`_web_list_jobs`) | observe | `filter?`, `state?` (`running`/`disabled`/`scheduled`), `cursor?`, `limit?` |
| `cron_get_job` | One job's status + schedule + recent-run summary. | `/jobs` + `/jobs/{name}/runs` | observe | `name` |
| `cron_list_runs` | Retained run history + aggregate stats for a job. | `GET /jobs/{name}/runs` (`_web_job_runs`) | observe | `name`, `cursor?`, `limit?`, `since?` |
| `cron_get_job_trends` | Per-window SLA stats over the durable ledger (1h/24h/7d/30d/all). | `GET /jobs/{name}/trends` | observe | `name` |
| `cron_get_job_resources` | CPU/RSS time series for a job's recent runs. | `GET /jobs/{name}/resources` | observe | `name`, `runs?` |
| `cron_tail_job_logs` | **Last N lines + opaque cursor** for polling more (SSE→poll mapping). | `GET /jobs/{name}/logs` (`_web_job_logs`) | observe | `name`, `tail?`, `cursor?` |
| `cron_get_status` | One-line status of every job. | `GET /status` (`_web_get_status`) | observe |, |
| `cron_get_cluster` | This node's cluster/leadership view. | `GET /cluster` (`_web_get_cluster`) | observe |, |
| `cron_get_fleet` | Cluster-wide per-node/per-job single-pane. | `GET /fleet` (`_web_get_fleet`) | observe |, |
| `cron_get_node` | Live node CPU/memory (+ optional history ring). | `GET /node` (+`/node/history`) | observe | `history?` |
| `cron_query_metrics` | **Parsed** series summary from the Prometheus exposition (not the raw dump). | `GET /metrics` (`_web_metrics`) | observe | `names?`/`match?` |
| `cron_get_version` | Daemon version + job-set id. | `GET /version`, `/job-set-id` | observe |, |
| `cron_list_dags` | Configured DAGs and their tasks/deps. | `GET /dags` (`_web_list_dags`) | dags |, |
| `cron_list_dag_runs` | Recent dag_runs with per-state task counts. | `GET /dags/{name}/runs` | dags | `dag`, `cursor?`, `limit?` |
| `cron_get_dag_run` | One run's full document (task states, timing, decisions). | `GET /dags/{name}/runs/{run_key}` | dags | `dag`, `run_key` |
| `cron_get_dag_xcom` | XCom outputs a run's tasks published. | `GET /dags/{name}/runs/{run_key}/xcom` | dags | `dag`, `run_key` |
| `cron_tail_dag_task_logs` | Last N lines + cursor for a running task instance. | `GET /dags/.../tasks/{taskkey}/logs` | dags | `dag`, `run_key`, `taskkey`, `tail?`, `cursor?` |
| `cron_inspect_state` | Metadata-only store inventory / namespace docs / stream records; **secret names only, values redacted**. | `GET /state`, `/state/documents`, `/state/records` | state | `ns?`, `stream?`, `cursor?`, `limit?` |

**Result shaping (all read tools):** return `structuredContent` (JSON) **plus** a compact human-readable `text` summary mirror; declare `outputSchema` where the shape is stable. Prefer human-readable fields over uuids; return counts + top-N + opaque cursor for large lists (never a full dump). Each tool's `limit` is **clamped to `mcp.maxRows`**: a caller asking for more is capped, with a `text` note saying so (no error). Offer `response_format: concise|detailed` on the heavy ones; and on truncation, include a `text` note steering the agent to a narrower query. Give actionable errors ("job not found: use `cron_list_jobs` to enumerate"), this is standard agent-tool hygiene ([writing tools for agents](https://www.anthropic.com/engineering/writing-tools-for-agents), [code execution with MCP](https://www.anthropic.com/engineering/code-execution-with-mcp)). *(Shipped divergence: list pagination is `offset`/`limit` with a `nextOffset` in results, not opaque cursors; only the log-tail tools take a `cursor`.)*

### 5.2 Mutating tools (`readOnlyHint: false`; require `readOnly: false`)

Every mutating tool takes an explicit `confirm: true` (and `dry_run` where a preview exists), carries honest annotations for client-side confirmation, **and re-checks the same server-side authorization as the REST API**. Human-in-the-loop is layered: (1) honest `destructiveHint` drives client confirmation prompts; (2) the mandatory `confirm`/`dry_run` argument means the model must take a deliberate second step; (3) where the client supports **elicitation** *and the transport can carry it*, additionally raise an elicitation form for the range/reason. This works whether or not the client supports elicitation, and note elicitation is reachable only on the **direct HTTP(+SSE)** path, never over the blocking stdio bridge (§4.2/§8), so the design never depends on it.

| Tool | Description | Maps to | Annotation | Key inputs |
| --- | --- | --- | --- | --- |
| `cron_run_job` | Launch a job now (honors `concurrencyPolicy`). | `POST /jobs/{name}/start` (`_web_start_job`) | `destructiveHint:false`, `idempotentHint:false` | `name`, `confirm` |
| `cron_cancel_job` | Terminate running instances (graceful→kill). | `POST /jobs/{name}/cancel` | `destructiveHint:true` | `name`, `confirm` |
| `cron_trigger_dag` | Create+start a manual DAG run now. | `POST /dags/{name}/trigger` | `destructiveHint:false` | `dag`, `confirm` |
| `cron_backfill_dag` | Replay a scheduled DAG across a range. **`dry_run: true` by default**; two-step (preview → confirm). | `POST /dags/{name}/backfill` | `destructiveHint:true` | `dag`, `from`, `to`, `dry_run=true`, `confirm` |
| `cron_decide_gate` | Approve/reject an approval gate. | `POST /dags/.../decision` | `destructiveHint:true` | `dag`, `run_key`, `taskkey`, `decision`, `by` |

**Deliberately excluded from mutating tools:** the job-facing durable-state *write* primitives (KV set/delete, cursor advance, lock acquire/release, XCom push, idempotency claim). Writing to a running job's cursor/lock from an agent can corrupt in-flight work; these stay **read-only** in MCP via `cron_inspect_state`. (Open question §11.)

### 5.3 Resources and resource templates

Application-controlled, cacheable read context for capable clients. **Mirror every critical read as a tool too** (§5.1) because resource support across clients remains uneven, resources are an optimization, never the only path.

- Fixed: `cronstable://status`, `cronstable://cluster`, `cronstable://fleet`, `cronstable://version`
- Templates: `cronstable://jobs/{name}`, `cronstable://jobs/{name}/runs/{run_id}`, `cronstable://dags/{name}`, `cronstable://dags/{name}/runs/{run_key}`, `cronstable://state/{ns}`

*(Shipped divergence: the job-runs template is `cronstable://jobs/{name}/runs`, no `{run_id}`; and resources are toolset-scoped, so the `dags` templates require the `dags` toolset.)*

### 5.4 Prompts (the "Reason" layer)

User-controlled slash-command playbooks that chain the read tools. Prompts are supported by Claude Desktop/Code/Cursor/VS Code, so they reach most operators. *(Shipped divergence: prompts are toolset-scoped; `why_did_dag_run_fail` and `backfill_plan` require the `dags` toolset.)*

| Prompt | Arguments | What it orchestrates |
| --- | --- | --- |
| `triage_job_failure` | `job` | Pull recent runs + trends, tail logs, correlate node/cluster health → root-cause narrative. |
| `why_did_dag_run_fail` | `dag`, `run_key` | Walk task states, failed-task logs, XCom, upstream deps. |
| `blast_radius` | `target` (job or dag) | Correlate downstream DAG deps + shared locks/cursors/idempotency keys to scope impact. |
| `fleet_health_summary` |, | Summarize `/fleet` + `/cluster` + per-node load into a wallboard digest. |
| `backfill_plan` | `dag`, `from`, `to` | Reason over a `dry_run` backfill before proposing `confirm`. |

---

## 6. Auth & security

**Reuse cronstable's existing model as-is for the self-hosted case.** The spec's stated minimum for a hardened local server is exactly what cronstable already ships: stdio, **or** HTTP restricted by (a) a bearer token and/or (b) a unix domain socket with restricted filesystem permissions ([security best practices](https://modelcontextprotocol.io/docs/tutorials/security/security_best_practices)). cronstable's `web.authToken` (bearer, resolved via `_resolve_web_token`, compared in constant time with `hmac.compare_digest`, **fail-closed** on an empty token), `web.socketMode` fs-gated unix sockets, and reverse-proxy-terminated mTLS **meet and exceed** that bar. **No OAuth is required to be spec-compliant**, because OAuth is OPTIONAL and stdio/IPC transports are exempt.

**Concrete rules for the `/mcp` route:**
- **Fail closed when unauthenticated on a routable listener (the load-bearing rule).** cronstable's app-wide auth middleware (`_make_auth_middleware`) is only installed when a token *resolves*; with no `web.authToken` there is **no middleware at all** and every route is open. Therefore, if `mcp.enabled` is true and no token is inherited, cronstable **refuses to expose `/mcp` on any listener that isn't loopback (`127.0.0.1`/`::1`) or a filesystem-gated unix socket**, startup raises a `ConfigError` when every listener is routable, or binds `/mcp` only on the safe listeners and hard-warns on a mixed listen set (§7). An unauthenticated `/mcp` on a routable address would expose full read, and, under `readOnly:false`, mutating, access. *(Shipped divergence: the bind-safe-and-warn option was dropped; any tokenless routable listener raises a `ConfigError` unless `mcp.allowUnauthenticated: true`.)*
- **Token on every request; `/mcp` is never public.** It inherits the app-wide auth middleware; do **not** add `/mcp` to `WEB_PUBLIC_PATHS` or the `metrics.public` exemption. Unauthenticated ⇒ `401`.
- **`cron_query_metrics` re-imposes auth even though `/metrics` may be public.** The `metrics.public` opt-out exists for scrapers that can't send credentials; the MCP tool must **not** become an auth-bypass back-door to metrics. It reads the metrics *through the authenticated MCP surface*, not by exempting itself.
- **Origin validation (new):** the REST API doesn't validate `Origin` today; the `/mcp` handler **must**, returning `403` on a present-but-invalid Origin, because MCP clients may be browser-based (DNS-rebinding defense). Configurable via `mcp.allowedOrigins` (empty ⇒ non-browser only).
- **CORS for browser clients (new).** `allowedOrigins` is only a rebinding *check*; a browser-hosted client also needs CORS to talk to `/mcp`. When `allowedOrigins` is **non-empty**, `/mcp` answers `OPTIONS` preflight and returns a **scoped** `Access-Control-Allow-Origin` (echoing only an allow-listed Origin, **never `*`**, credentialed CORS may not use a wildcard), with `Access-Control-Allow-Headers: Authorization, Content-Type, MCP-Protocol-Version` and `Access-Control-Expose-Headers: MCP-Protocol-Version`. Empty `allowedOrigins` ⇒ **no CORS headers** (non-browser clients only). This closes the "Origin-validated but not browser-reachable" half-spec.
- **Bounded request bodies (new).** Tool arguments arrive from an LLM; `/mcp` caps the request body at `mcp.maxBodyBytes` (default 1 MiB, aiohttp's `client_max_size`) and returns `413` beyond it, so a runaway or hostile client can't exhaust memory.
- **Localhost binding:** cronstable already supports `http://127.0.0.1` + unix sockets; keep the default guidance to bind loopback/socket and terminate TLS/mTLS in a reverse proxy (matching [`wiki/HTTP-API.md`](HTTP-API)).
- **No token passthrough:** cronstable *is* the resource; it must never forward a caller's bearer token to a downstream. (A hard MUST NOT.)
- **No session-as-auth:** we're stateless, so there are no sessions to hijack; if that ever changes, verify the token on every request, use CSPRNG session IDs, and bind them to identity.
- **Confused-deputy: not applicable**: cronstable is a single **direct** resource server, not an OAuth proxy/aggregator. Explicitly **stay out of the proxy pattern** to avoid that entire attack class.
- **Tool-definition stability (anti "rug-pull").** Tool names, schemas, and annotations are a **stable contract**: pinned in code, covered by the schema-contract test (§10), and changed only deliberately under review. We advertise `tools.listChanged:false` (the set is static per config), so clients aren't invited to silently re-fetch a mutated definition mid-session. A full signed/attested tool-definition scheme is out of scope and explicitly deferred (§11).
- **Annotations are not a control.** `destructiveHint`/`readOnlyHint` are honest UX hints; the real guard is (1) `readOnly: true` by default, (2) server-side authorization identical to the REST API on every tool, (3) mandatory `confirm`/`dry_run`, and (4) argument constraints (validate `from`/`to` ranges, job/dag names against the live set). Treat **all** tool inputs as untrusted (they come from an LLM).
- **Secret redaction:** `cron_inspect_state` mirrors the existing `/state/documents` redaction, KV values reduced to `valueSize`/`valueType`, archived-output `logs/` streams refused, run-scoped **secret names only, never values**.

**If/when cronstable hosts a public multi-tenant MCP endpoint** (beyond localhost/socket/mTLS on a trusted host), *then* add the OAuth 2.1 Resource-Server layer, and only then: implement RFC 9728 Protected Resource Metadata at `/.well-known/oauth-protected-resource`; return `401` with `WWW-Authenticate: Bearer resource_metadata=…` + a scope hint; validate every token's audience (RFC 8707) against cronstable's canonical URI and reject tokens not issued for it; and **stay Resource-Server-only**, delegating issuance to an external authorization server. Publish a minimal `scopes_supported` (e.g. `cron:read` baseline, `cron:write`/`cron:delete` elevated) and use `403 insufficient_scope` step-up rather than broad grants ([auth](https://modelcontextprotocol.io/specification/draft/basic/authorization)). This is deliberately out of scope for the initial implementation.

This posture keeps the MCP surface fully compatible with cronstable's hardened runtime (non-root, read-only rootfs, dropped caps, RuntimeDefault seccomp): the hand-rolled pure-Python handler adds no native code and no new browser surface (the `/mcp` route is a JSON endpoint, separate from the CSP-locked dashboard).

---

## 7. Configuration

A new top-level **`mcp:`** block, parsed by the strictyaml `CONFIG_SCHEMA` in `cronstable/config.py` in the same style as `web:`/`state:`. It **relates to `web:`** by riding the `web.listen` addresses and inheriting `web.authToken` and the web server's reload lifecycle, the HTTP endpoint is resolved and (re)started inside `Cron.start_stop_web_app` exactly like `metrics`/`nodeHistory`. The stdio bridge is config-independent (it takes `--url`/`--token` flags/env).

```yaml
web:
  listen:
    - http://127.0.0.1:8080
    - unix:///run/cronstable/cronstable.sock
  authToken:
    fromEnvVar: CRONSTABLE_WEB_TOKEN      # ALSO gates /mcp (inherited)
  socketMode: "0660"

mcp:
  enabled: true                  # bool, default false, opt-in; serves POST /mcp on web.listen
  readOnly: true                 # bool, default true, strips all mutating tools (precedence over toolsets)
  toolsets: [observe]            # seq[enum], default [observe]; choices: observe, act, dags, state
  allowedOrigins: []             # seq[str], default [], exact-match Origins (rebinding guard + CORS scope)
  instructions: null             # str|null, optional server `instructions` surfaced at initialize
  maxRows: 200                   # int, default 200, ceiling for every tool's `limit` before an opaque cursor
  maxBodyBytes: 1048576          # int, default 1 MiB, cap on the /mcp request body (413 beyond)
```

Proposed schema fragment (matching the existing idiom, `Map`/`Opt`/`Enum`/`Bool`/`Seq`/`EmptyNone`):

```python
Opt("mcp"): Map({
    Opt("enabled"): Bool(),
    Opt("readOnly"): Bool(),
    Opt("toolsets"): Seq(Enum(["observe", "act", "dags", "state"])),
    Opt("allowedOrigins"): Seq(Str()),
    Opt("instructions"): EmptyNone() | Str(),
    Opt("maxRows"): Int(),
    Opt("maxBodyBytes"): Int(),
}),
```

*(Shipped divergence: the final schema also takes `allowUnauthenticated`, `resources`, and `prompts`; see the [`mcp` configuration reference](Configuration-Reference#mcp).)*

Defaults are filled by a `_build_mcp_config` helper (mirroring how `jobApi` defaults come from `DEFAULT_JOB_API`). **Validation / fail-closed:**
- if `mcp.enabled` is true but `web.listen` is empty, raise a `ConfigError` (there is nowhere to serve `/mcp`);
- **if `mcp.enabled` is true and no `web.authToken` resolves, refuse to expose `/mcp` on any non-loopback, non-unix-socket listener**: raise a `ConfigError` when every listener is routable, or bind `/mcp` only on the loopback/socket listeners and hard-warn when the listen set is mixed. (The auth middleware does not exist without a token, so a routable `/mcp` would be unauthenticated, §6.) *(Shipped divergence: a `ConfigError` in both cases, with `mcp.allowUnauthenticated: true` as the escape hatch.)*;
- if `act` appears in `toolsets` while `readOnly: true`, log a warning that mutating tools remain suppressed (read-only wins);
- each tool's `limit` input is **clamped to `mcp.maxRows`**, a request above the cap is capped with a `text` note, not an error.

`enabled` defaults to **false** so the feature is strictly opt-in, consistent with cronstable's "stateless install pays nothing" ethos.

---

## 8. Client compatibility & scope guardrails

Client feature support is **very uneven and still moving**, design so anything critical is reachable via a **tool**, and re-check the live matrix at publish time rather than baking a snapshot into the design ([support matrix](https://mcp-availability.com/)):

- **Tools: universal** (VS Code Copilot, Cursor, Claude Code, Claude Desktop, Claude.ai, ChatGPT, Windsurf). **Everything load-bearing is a tool.**
- **Resources & prompts: still uneven, and expanding.** Most desktop clients (VS Code/Cursor/Claude Code/Claude Desktop) read them. ChatGPT was historically tools-only, but OpenAI has since advertised broader MCP support (Agents SDK, Responses API, ChatGPT desktop) and connector resource support has been growing, so **do not hard-code "ChatGPT = tools-only" as fact; verify against the live matrix before publishing.** Windsurf remains resources/prompts-only. The unevenness across clients is itself enough to justify the guardrail: ⇒ **Mirror every resource as a tool** (§5.3); ship prompts as a convenience, not a dependency.
- **Elicitation ~11%, roots ~8%, sampling ~12%.** ⇒ **Do not require elicitation** for human-in-the-loop; rely on `destructiveHint` + `confirm`/`dry_run`, and use elicitation only as an *enhancement* where present (and it is unreachable over the blocking stdio bridge regardless, §4.2). **Do not build on sampling or roots at all** (also being deprecated in `2026-07-28`).
- **Progress/logging:** support cursor-based pagination on lists; keep results lean. Do **not** adopt the deprecated `logging/setLevel` notification feature, log to stderr (stdio) and rely on cronstable's existing observability for the HTTP path.

**Scope guardrails baked in:** read-only default profile; `observe`-only default toolset; consolidated high-level tools (not 1:1 REST wrappers); compact results with cursors; honest annotations; server-side authz on every call.

---

## 9. Implementation plan

The server is built in four increments, each shippable on its own.

**Read-only core (stdio + HTTP), secure by default.** Ship the protocol core and the `observe` toolset, read-only.
- **New `cronstable/mcp.py`:** `MCPHandler` (JSON-RPC dispatch; `initialize`/`ping`/`tools/list`/`tools/call`; the §5.1 `observe` tools; **capabilities gated to `{tools:{listChanged:false}}`**; stateless `handle_http`/`handle_http_get`; error mapping; `_json.dumps_bytes`-based serialization). Include the security basics from day one: `Origin`→403, `Accept`→406, body-size→413, and the fail-closed token/listener check.
- **`cronstable/cron.py`:** (re)construct `self._mcp` and register `web.post("/mcp", …)`/`web.get("/mcp", …)` in `start_stop_web_app` **on each reload**; extract reusable payload builders from `_web_get_status`/`_web_list_jobs`/`_web_job_runs`/`_web_get_cluster`/`_web_get_fleet`/`_web_get_node`/`_web_metrics` (separating request-parsing from payload construction, §4.3); add the Origin check.
- **New `cronstable/mcpcli.py`:** the stdio frame-proxy (urllib, no aiohttp), with `initialize`-sniffed `MCP-Protocol-Version` header injection and a `--validate` self-check *(shipped as `--check`)*.
- **`cronstable/__main__.py`:** add the top-level `mcp` subparser in `_add_state_subcommands` (or a sibling `_add_mcp_subcommand`), routed like the other job-facing subcommands (lazy import, no `Cron`).
- **`cronstable/config.py`:** `mcp:` schema + `_build_mcp_config` + the fail-closed checks (empty-listen, token/routable-listener, maxRows clamp).
- **`tests/test_mcp.py`:** direct `MCPHandler.handle_message()` calls with the mock-`Req` style of `tests/test_ui_endpoints.py`.

**Mutating actions + HITL + gating.** Add the `act`/`dags` mutating tools (§5.2) with `confirm`/`dry_run` and honest annotations; implement `readOnly` and `toolsets` filtering in `_list_tools`/`_call_tool`; wire `cron_tail_job_logs`/`cron_tail_dag_task_logs` poll+cursor over the existing SSE buffers (`_pump_output` source). Extend tests to assert mutating tools are absent under `readOnly: true` and that `confirm` is enforced.

**Resources & prompts.** Add `resources/list`, `resources/templates/list`, `resources/read`, `prompts/list`, `prompts/get` and the `state` toolset (`cron_inspect_state` with redaction). **Extend `_capabilities()` to advertise `resources`/`prompts` only now that they're registered.** Optional: SSE upgrade on `POST /mcp` for progress on `cron_backfill_dag`.

**Hardening, docs, distribution.** Finalize `MCP-Protocol-Version` edge cases and scoped **CORS** for browser-hosted clients; add the optional OAuth Resource-Server path behind a config flag (only if a public endpoint is wanted); publish to the [official MCP Registry](https://github.com/modelcontextprotocol/registry) via `mcp-publisher` with a `server.json` declaring both a `packages[]` (PyPI, `uvx cronstable mcp`) and a `remotes[]` (`streamable-http` URL) entry, plus an `mcp-name:` line in `README.md` for ownership proof; add copy-paste client configs and the Inspector CI smoke test (§10). New [`wiki/MCP.md`](MCP); update [`wiki/HTTP-API.md`](HTTP-API) and [`wiki/Configuration-Reference.md`](Configuration-Reference).

---

## 10. Testing & DX

**Unit/contract tests (pytest, `asyncio_mode=auto`, matching the repo):** call `MCPHandler.handle_message()` directly with dict messages and a minimal mock request, the same direct-handler style as `tests/test_ui_endpoints.py` (`Req` stand-in) and `tests/test_cron.py`. Assert:
- `initialize` negotiates `2025-11-25` **and advertises only the capabilities that are registered** (a tools-only config exposes `tools` but **not** `resources`/`prompts`);
- `tools/list` matches expectations, honors `readOnly`/`toolsets`, and every read tool carries `readOnlyHint:true` + `openWorldHint:false`;
- `tools/call` returns `structuredContent` + a `text` mirror; per-tool `limit` is clamped to `maxRows`;
- unknown tool ⇒ JSON-RPC `-32601`; bad arguments ⇒ `isError:true` (not a protocol error); mutating tools require `confirm`; secrets are redacted in `cron_inspect_state`;
- **fail-closed auth:** `mcp.enabled` with no token on an all-routable listen set raises `ConfigError`;
- conformance: missing/incompatible `Accept`⇒`406`, oversize body⇒`413`, present-but-invalid `Origin`⇒`403`, `GET /mcp`⇒`405`, batching unsupported.

**Validate every tool's `inputSchema`/`outputSchema` against JSON Schema 2020-12 and validate representative payloads against the canonical `schema/2025-11-25/schema.json`** using a **dev-only** `jsonschema` (never a runtime dep).

**MCP Inspector (official dev/debug + CI gate)** ([Inspector](https://modelcontextprotocol.io/docs/tools/inspector)):
- HTTP: `npx @modelcontextprotocol/inspector --cli http://127.0.0.1:8080/mcp --transport http --method tools/list --header "Authorization: Bearer $TOKEN"`
- stdio: `npx @modelcontextprotocol/inspector --cli cronstable mcp --url http://127.0.0.1:8080 --method tools/list`
- Add a CI job (alongside `tox`) running `initialize` + `tools/list` + `resources/list` + `prompts/list` + a representative `tools/call`, gating on exit code.

**Static checks:** `cronstable/mcp.py` and `cronstable/mcpcli.py` must pass `ruff` and `mypy` at **line-length 79, py310** (repo config). **`mcpcli.py` must not import `aiohttp`/`strictyaml`/the `Cron` graph**, enforce with a test that imports it and asserts those modules are absent from `sys.modules`, preserving the fast-start contract of the job-facing subcommands.

**Client wiring (ship in `README.md`/[`wiki/MCP.md`](MCP))** ([Claude Code MCP](https://code.claude.com/docs/en/mcp)):
- Claude Code (HTTP): `claude mcp add --transport http cronstable https://cronstable.example/mcp --header "Authorization: Bearer $TOKEN"`
- Claude Code (stdio): `claude mcp add --transport stdio cronstable -- cronstable mcp --url http://127.0.0.1:8080`
- Claude Desktop: `claude_desktop_config.json` → `mcpServers.cronstable = {command:"cronstable", args:["mcp","--url","http://127.0.0.1:8080"]}` (absolute paths; any JSON error silently disables all servers).
- Cursor `~/.cursor/mcp.json` (`mcpServers` wrapper) and VS Code `.vscode/mcp.json` (`servers` wrapper, explicit `type`). Note that remote entries **need an explicit `type`** and a `url`-without-`type` entry is a config error.

---

## 11. Risks & open questions

**Risks / mitigations**
- **Unauthenticated `/mcp` on misconfiguration (the one real security gap).** Because the auth middleware only exists when a token resolves, an enabled-but-tokenless `/mcp` on a routable address would be wide open. Mitigation: the fail-closed token/listener check in §6/§7 (ConfigError on all-routable, bind-safe-only + hard-warn on mixed), covered by a dedicated test.
- **Spec churn (we own compliance).** Mitigation: pin `2025-11-25`, schema-validate in tests (incl. the SEP-1303 input-error-as-tool-error rule), stay stateless so the `2026-07-28` simplification is a near-no-op. The `2026-07-28` RC is not final as of today; treat its details (stateless core, `Mcp-Method`/`Mcp-Name` routing headers, missing-resource error code moving `-32002`→`-32602`) as provisional.
- **`/metrics` public-exemption bypass.** `cron_query_metrics` must re-impose auth; covered by an explicit test.
- **Agent misuse of mutating tools.** Read-only default + `confirm`/`dry_run` + server-side authz + argument allowlists; annotations are not relied on for safety.
- **stdio bridge needs a running daemon.** This is a real departure from the self-contained-subprocess model desktop clients assume when they *launch* a stdio server: the bridge only works where the daemon is already up and the URL/token are provisioned, and a first-run against no daemon simply errors. Acceptable for an ops tool, documented as intended, with an actionable message when the daemon URL is unreachable.
- **Refactor risk in `cron.py`.** Extracting payload builders touches ~10 hot handlers and must separate request-parsing from payload construction (not a pure rename); keep changes contained and covered by the existing `test_ui_endpoints.py`/`test_cron.py` suites.
- **Ongoing hand-roll maintenance.** JSON-RPC/2020-12 schema drift and error-channel semantics are ours to keep correct; the dev-only `jsonschema` contract test against canonical `schema.json` is the mitigation that keeps it tractable.

**Open questions**
1. **Multi-user vs single-operator.** Is the bearer token a single machine/operator credential, or must MCP act on behalf of distinct end users? Multi-user pushes toward per-user audience validation and (eventually) the OAuth RS path.
2. **Public/hosted endpoint?** If cronstable is ever exposed to browser-hosted agents (ChatGPT/Claude.ai remote connectors), OAuth 2.1 RS **and** the scoped-CORS work (§6) become necessary, worth the hand-rolled effort only if that audience matters.
3. **Expose durable-state *writes* as MCP tools?** Current recommendation is **no** (read-only via `cron_inspect_state`) to avoid corrupting running jobs' cursors/locks/XCom. Revisit if a concrete agent workflow needs it, gated behind `readOnly:false` + a dedicated `state-write` toolset.
4. **Long backfills: poll+cursor now, Tasks later?** Ship progress via a bounded SSE upgrade + cursor first (works everywhere); adopt the `2026-07-28` stateless **Tasks** extension once client support matures.
5. **Metrics semantics.** Should `cron_query_metrics` return a parsed series summary (recommended, lean) or the raw exposition? Confirm the parse covers the cluster health series that mirror `/cluster`.
6. **Tool-definition attestation.** We treat tool definitions as a reviewed, pinned contract with `listChanged:false`; do we ever need signed/attested definitions to fully close the rug-pull surface, or is deferral (§6) permanently acceptable for a single-tenant ops server?
7. **Empirical annotation behavior.** Which clients actually honor `destructiveHint` to prompt on `cron_cancel_job`/`cron_backfill_dag`? Test against Claude Desktop/Code, Cursor, and VS Code before relying on it as a UX affordance (never as a control).
8. **MCP Apps (sandboxed-iframe HTML widgets, `2026-07-28`).** Desirable for richer run/DAG visualization inside Claude.ai, but potentially in tension with cronstable's strict-CSP, minimal-surface stance, defer.


---

## Appendix A, Verified implementation anchors

All symbol/line references below were **verified against the working tree on 2026-07-08** (branch `main`). This turns the plan into a build spec: an implementer can jump straight to each insertion point.

### A.1 Insertion points (confirmed)

| Concern | Confirmed anchor | Action |
| --- | --- | --- |
| **Route registration** | `cron.py:2371`, the `routes = [ web.get("/version", …), … ]` list is built inside `start_stop_web_app` immediately after `middlewares` | Construct `self._mcp = MCPHandler(self, cfg)` **here** (so it rebuilds every reload) and append `web.post("/mcp", …)`, `web.get("/mcp", …)` (→405), `web.options("/mcp", …)` (CORS). |
| **Fail-closed mechanism** | `cron.py:2357-2370`, the auth middleware is appended **only when `_resolve_web_token()` is non-None** | This is the exact reason a tokenless `/mcp` on a routable listener is wide open, and why config must refuse it (§6/§7). |
| **Never-public sets** | `cron.py:251` `WEB_PUBLIC_PATHS = frozenset({"/"})`; `cron.py:2363-2367` `metrics.public` → `public.add("/metrics")` | Do **not** add `/mcp` to either set. |
| **Auth middleware** | `cron.py:3382` `_make_auth_middleware(token, public_paths)`; `cron.py:3352` `_resolve_web_token` | `/mcp` inherits it automatically when a token resolves. |
| **Read payload builders to extract** (split request-parsing from payload construction) | `_web_get_status:1472`, `_web_list_jobs:1802`, `_web_job_runs:2075`, `_web_get_cluster:1345`, `_web_get_fleet:1368`, `_web_get_node:1393`, `_web_metrics:1450` | Extract a plain-argument `*_payload(...)` method each; REST handler and MCP tool both call it. |
| **Mutating handler** | `_web_start_job:1541` (+ the cancel / dag trigger / backfill / decision handlers) | Same extract-and-share; MCP tool re-checks the same authz. |
| **Log-tail source** | `_pump_output:2265`, `_web_job_logs:2299`, `_sse_send_line:542` | Back `cron_tail_job_logs` poll+cursor over the same in-memory buffer (no new SSE). |
| **JSON serialization** | `_json.py:105/119` `dumps_bytes(obj, *, sort_keys=False) -> bytes`; `:113/125` `loads` | Use **`dumps_bytes`** for `web.Response(body=…, content_type="application/json")`, returns `bytes`, orjson-accelerated when present. There is no `dumps` (str) export. |
| **Config schema** | `config.py:673` `CONFIG_SCHEMA`; `:678` `Opt("web"): Map({ authToken:683, socketMode:691 })`; `:201` `DEFAULT_JOB_API`; `:232` `ConfigError` | Add `Opt("mcp"): Map({…})` as a sibling of `web`; add `_build_mcp_config` (merge-over-defaults, mirroring `DEFAULT_JOB_API`); raise `ConfigError` for the fail-closed cases. |
| **CLI subcommand** | `__main__.py:16` `_add_state_subcommands`; `:168` dispatch `if command == "state"`; `:175/192` `jobcli.dispatch` | Add an `mcp` subparser and `elif command == "mcp": sys.exit(mcpcli.dispatch(args))` (lazy import, no `Cron`). |

### A.2 Ordered task list

**Read-only core (stdio + HTTP), secure by default**
1. `config.py`: `mcp:` schema + `_build_mcp_config` + fail-closed validation (empty `web.listen`; no-token-on-routable-listener; `maxRows` clamp).
2. `cronstable/mcp.py` (new): `MCPHandler`, JSON-RPC dispatch (`initialize`/`ping`/`notifications/initialized`/`tools/list`/`tools/call`); `_capabilities()` gated to `{tools:{listChanged:false}}`; stateless `handle_http` (Origin→403, Accept→406, body→413, `GET`→405); error mapping (protocol `-32xxx` vs `isError:true`); `dumps_bytes` responses.
3. `cron.py`: extract the 7 read payload builders (A.1); construct `self._mcp` + register routes in `start_stop_web_app` (rebuild on reload); add the Origin check.
4. The `observe` tools (§5.1) over the shared builders, `structuredContent` + `text` mirror, `maxRows` clamp, opaque cursors, actionable errors.
5. `cronstable/mcpcli.py` (new): urllib stdio frame-proxy; `initialize`-sniffed `MCP-Protocol-Version`; **must not import** `aiohttp`/`strictyaml`/`Cron`.
6. `__main__.py`: `mcp` subparser + dispatch branch.
7. `tests/test_mcp.py`: direct `handle_message()` calls (the `Req` style from `test_ui_endpoints.py`); assert capability gating, `readOnlyHint:true`+`openWorldHint:false`, `-32601`, `406/413/403/405`, fail-closed `ConfigError`, and an import-isolation test for `mcpcli`.

**Actions + human-in-the-loop**
8. `mcp.py`: `readOnly` + `toolsets` filtering in `_list_tools`/`_call_tool`.
9. Mutating tools (§5.2) with `confirm`/`dry_run` + honest annotations, each re-checking the REST authz and validating job/dag names + `from`/`to` ranges against the live set; `cron_backfill_dag` defaults `dry_run:true`.
10. `cron_tail_job_logs` / `cron_tail_dag_task_logs` poll+cursor over the `_pump_output` buffer.
11. Tests: mutating tools **absent** under `readOnly:true`; `confirm` enforced; `dry_run` preview; authz re-check; secret redaction.

**Verification gate** (per `/verify` + MCP Inspector): `npx @modelcontextprotocol/inspector --cli http://127.0.0.1:8080/mcp --transport http --method tools/list --header "Authorization: Bearer $TOKEN"`, then a representative `tools/call`.
