# MCP Server (Model Context Protocol)

cronstable ships an optional [Model Context Protocol](https://modelcontextprotocol.io)
server, so an AI agent (Claude Desktop / Code, Cursor, VS Code Copilot,
ChatGPT connectors) can drive cronstable the way an operator drives the
[dashboard](Web-Dashboard): **observe** every job, DAG, the cluster/fleet,
metrics and the durable state store, and, when you opt in, **act** (run or
cancel a job, trigger / backfill / approve a DAG).

It is served two ways from the same code:

- **`POST /mcp`** on the existing [`web.listen`](HTTP-API) addresses, a
  stateless [Streamable HTTP](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)
  JSON-RPC endpoint that inherits the web API's `authToken` / unix-socket auth.
- **`cronstable mcp`**, a featherweight **stdio bridge** that desktop clients
  launch as a subprocess; it forwards frames to a running daemon's `/mcp`.

It is hand-rolled in pure Python with **no new dependencies** (the same
minimal-dependency stance as the rest of cronstable), targets MCP revision
`2025-11-25`, and exposes **tools**, **resources** and **prompts**.

## Enabling it

The server is **off by default**. Add an [`mcp`](Configuration-Reference#mcp)
section (it rides the [`web`](HTTP-API) listeners, so a `web` section is
required):

```yaml
web:
  listen:
    - http://127.0.0.1:8080
  authToken:
    fromEnvVar: CRONSTABLE_WEB_TOKEN   # also gates /mcp

mcp:
  enabled: true
```

That serves the **read-only** `observe` toolset. An agent can look but not
touch. To let it act, opt into more toolsets and turn off `readOnly`:

```yaml
mcp:
  enabled: true
  readOnly: false        # expose mutating tools
  toolsets:
    - observe
    - dags
    - act
    - state
```

See the [`mcp` configuration reference](Configuration-Reference#mcp) for every
field.

## What the agent can do

### Tools (model-controlled actions)

Grouped into **toolsets** you enable with `toolsets:`. `readOnly: true` (the
default) strips every mutating tool regardless of toolset.

| Toolset | Tools |
| --- | --- |
| `observe` (read) | `cron_get_status`, `cron_list_jobs`, `cron_get_job`, `cron_list_runs`, `cron_get_job_trends`, `cron_get_job_resources`, `cron_get_cluster`, `cron_get_fleet`, `cron_get_node`, `cron_query_metrics`, `cron_get_version`, `cron_tail_job_logs` |
| `dags` (read) | `cron_list_dags`, `cron_list_dag_runs`, `cron_get_dag_run`, `cron_get_dag_xcom`, `cron_tail_dag_task_logs` |
| `state` (read) | `cron_inspect_state` (store overview / a namespace's documents / a stream's records; KV values and secrets redacted) |
| `act` (**mutating**) | `cron_run_job`, `cron_cancel_job` |
| `dags` (**mutating**) | `cron_trigger_dag`, `cron_backfill_dag`, `cron_decide_gate` |

Mutating tools require an explicit `confirm: true` argument, carry honest
`destructiveHint` annotations, and re-check the same authorization as the REST
API. `cron_backfill_dag` defaults to `dry_run: true`. It previews the range
and only executes on `dry_run: false` **and** `confirm: true`.

### Resources (read-only context)

Enabled by default (`resources: true`). URI-addressable snapshots that clients
can attach as context, scoped by the same toolsets:

- Fixed: `cronstable://status`, `cronstable://cluster`, `cronstable://fleet`, `cronstable://version`
- Templates: `cronstable://jobs/{name}`, `cronstable://jobs/{name}/runs`, `cronstable://dags/{name}`, `cronstable://dags/{name}/runs/{run_key}`, `cronstable://state/{ns}`

Every critical read is *also* a tool, because client support for resources is
uneven. Resources are an optimization, never the only path.

### Prompts (canned triage playbooks)

Enabled by default (`prompts: true`). Slash-command workflows that chain the
read tools:

- `triage_job_failure(job)`: root-cause a failing job
- `why_did_dag_run_fail(dag, run_key)`: walk a failed DAG run
- `blast_radius(target)`: scope what else is at risk
- `fleet_health_summary()`: a wallboard-style digest
- `backfill_plan(dag, from, to)`: reason about a backfill before running it

## Wiring a client to it

### Claude Code

```shell
# remote (Streamable HTTP):
claude mcp add --transport http cronstable https://your-host/mcp \
  --header "Authorization: Bearer $CRONSTABLE_WEB_TOKEN"

# local (stdio bridge to a daemon on this host):
claude mcp add --transport stdio cronstable -- \
  cronstable mcp --url http://127.0.0.1:8080 --token-env CRONSTABLE_WEB_TOKEN
```

### Claude Desktop / Cursor / VS Code

Point the client's MCP config at the stdio bridge. For Claude Desktop
(`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "cronstable": {
      "command": "cronstable",
      "args": ["mcp", "--url", "http://127.0.0.1:8080",
               "--token-env", "CRONSTABLE_WEB_TOKEN"]
    }
  }
}
```

Cursor uses `~/.cursor/mcp.json` (`mcpServers` wrapper); VS Code uses
`.vscode/mcp.json` (`servers` wrapper, with an explicit `type`). Remote
Streamable-HTTP entries take a `url` and the `Authorization` header shown above.

### The stdio bridge

`cronstable mcp` reads newline-delimited JSON-RPC on stdin and forwards each
frame to `<url>/mcp`, writing replies to stdout (only frames go to stdout; logs
go to stderr). It needs a **reachable running daemon**, the right model for an
ops tool. Flags:

- `--url` (default `http://127.0.0.1:8080`): the daemon's web base URL
- `--token` / `--token-env`: the bearer token (defaults to the
  `CRONSTABLE_WEB_TOKEN` env var if set)
- `--check`: handshake the endpoint (`initialize` + `tools/list`) and exit,
  a quick "is it wired up?" test

```shell
$ cronstable mcp --url http://127.0.0.1:8080 --token-env CRONSTABLE_WEB_TOKEN --check
mcp check: ok - protocol 2025-11-25, 23 tool(s) at http://127.0.0.1:8080/mcp
```

## Security

The MCP surface fits cronstable's hardened posture and is safe by default:

- **Read-only by default.** `readOnly: true` strips every mutating tool; an
  agent gets look-but-don't-touch access until you opt in.
- **Inherits the web auth.** `/mcp` sits behind `web.authToken` exactly like
  the data routes. It is never public. If you enable `mcp` with **no** token
  on a routable (non-loopback, non-socket) listener, cronstable **fails
  closed**: it refuses to start (with no token there is no auth middleware at
  all, so `/mcp` would be wide open). Restrict `web.listen` to loopback /
  unix sockets, set `web.authToken`, or, only if the endpoint is protected by
  other means (an mTLS-terminating proxy), set `mcp.allowUnauthenticated:
  true`.
- **Origin + body defenses.** A present, non-allow-listed `Origin` is refused
  `403` (DNS-rebinding defense; browser clients go on `mcp.allowedOrigins`),
  and an oversized request body is refused `413`.
- **Human-in-the-loop for writes.** Mutating tools require `confirm: true`,
  backfills default to a dry-run preview, and every action re-checks the REST
  authorization. Tool annotations are honest hints. The real guards are the
  read-only default, the confirm gate, and server-side authorization.
- **Redaction.** `cron_inspect_state` mirrors the dashboard's metadata-only
  stance: KV values become a size/type summary and secret **names** are shown
  without values.

## Trying it

The [`example/mcp`](https://github.com/ptweezy/cronstable/tree/develop/example/mcp)
project boots a node with the MCP server enabled:

```shell
docker compose -f example/mcp/docker-compose.yml up
# then, in another shell, point a client (or the bridge) at it:
CRONSTABLE_WEB_TOKEN=dev-token \
  cronstable mcp --url http://127.0.0.1:8080 --check
```

## See also

- [`mcp` configuration reference](Configuration-Reference#mcp): every field.
- [HTTP Control API](HTTP-API): the REST endpoints the tools project, and the
  `POST /mcp` entry.
- The [MCP specification](https://modelcontextprotocol.io/specification/2025-11-25)
  and the [MCP Inspector](https://modelcontextprotocol.io/docs/tools/inspector)
  for debugging a server.
