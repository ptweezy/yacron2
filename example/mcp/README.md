# MCP demo

A single cronstable node with the [MCP server](https://github.com/ptweezy/cronstable/wiki/MCP)
enabled, so an AI agent (Claude Desktop / Code, Cursor, VS Code Copilot) can
observe and control the scheduler.

## Run it

```shell
docker compose -f example/mcp/docker-compose.yml up --build
```

- Dashboard: <http://localhost:8080/> (enter the token `dev-token` when
  prompted).
- MCP endpoint: `POST http://localhost:8080/mcp`, bearer token `dev-token`.

The [config](cronstable.yaml) enables `mcp` with `readOnly: false` and all
toolsets, so an agent can both observe **and** act (demo only; keep the default
`readOnly: true` in production). It runs a steady `heartbeat`, a `flaky-export`
that fails on purpose, a long `slow-report`, and an `on-demand-sync` that only
runs when asked.

## Check it without a client

From a checkout (the `cronstable` CLI includes the stdio bridge):

```shell
CRONSTABLE_WEB_TOKEN=dev-token \
  cronstable mcp --url http://127.0.0.1:8080 --check
# -> mcp check: ok - protocol 2025-11-25, 23 tool(s) at http://127.0.0.1:8080/mcp
```

## Wire up a client

**Claude Code** (remote HTTP):

```shell
claude mcp add --transport http cronstable http://127.0.0.1:8080/mcp \
  --header "Authorization: Bearer dev-token"
```

**Claude Desktop / Cursor / VS Code** (stdio bridge):

```json
{
  "mcpServers": {
    "cronstable": {
      "command": "cronstable",
      "args": ["mcp", "--url", "http://127.0.0.1:8080", "--token", "dev-token"]
    }
  }
}
```

## Things to ask the agent

- "What cronstable jobs are failing, and why?" → it uses `cron_get_status`,
  `cron_list_runs`, `cron_tail_job_logs`.
- Run the **`triage_job_failure`** prompt against `flaky-export`.
- "Run the on-demand-sync job now." → `cron_run_job` (with `confirm: true`).
- "Summarize overall health." → the **`fleet_health_summary`** prompt.

See the [MCP wiki page](https://github.com/ptweezy/cronstable/wiki/MCP) for the
full tool / resource / prompt catalog and security notes.
