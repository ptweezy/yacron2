# How cronstable compares

**cronstable is the only cron-family scheduler with a built-in [MCP server](https://github.com/ptweezy/cronstable/wiki/MCP)** — so an AI agent (Claude, Cursor, Copilot) can observe every job, DAG and node, and act on them when you opt in. Nothing else in the field ships one natively — not even Apache Airflow. And it lands alongside durable state, a real DAG engine, leader-elected clustering and a live dashboard, in a single hardened, dependency-free daemon.

**Legend:** ✅ native · 🟡 partial / limited · ➕ requires an add-on · — not available

| Capability | cronstable | yacron | supercronic | Ofelia | dkron | Cronicle | K8s CronJob | Airflow |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **🔹 AI & agent control** | | | | | | | | |
| MCP server for AI agents (observe + control) | ✅ | — | — | — | — | — | ➕ | ➕ |
| Agent triage playbooks & resources | ✅ | — | — | — | — | — | — | — |
| **🔹 Scheduling core** | | | | | | | | |
| YAML job configuration | ✅ | ✅ | — | — | — | — | ✅ | ➕ |
| Classic Vixie crontab files as-is | ✅ | — | ✅ | — | — | 🟡 | — | — |
| Arbitrary per-job timezones | ✅ | ✅ | ✅ | 🟡 | ✅ | ✅ | ✅ | ✅ |
| Concurrency policy + execution/kill timeouts | ✅ | ✅ | 🟡 | 🟡 | ✅ | ✅ | ✅ | ✅ |
| Retries with exponential backoff | ✅ | ✅ | — | — | 🟡 | 🟡 | ✅ | ✅ |
| Missed-run catch-up after downtime | ✅ | — | — | — | — | ✅ | 🟡 | ✅ |
| **🔹 Orchestration & workflows** | | | | | | | | |
| DAGs / dependency graphs | ✅ | — | — | — | 🟡 | 🟡 | ➕ | ✅ |
| Cross-task data hand-off (XCom) | ✅ | — | — | — | — | ✅ | ➕ | ✅ |
| Human approval gates | ✅ | — | — | — | — | — | ➕ | ✅ |
| Backfill / historical reruns | ✅ | — | — | — | — | 🟡 | — | ✅ |
| **🔹 Distributed & fault-tolerant** | | | | | | | | |
| Clustering + leader election (no double-run) | ✅ | — | — | — | ✅ | ✅ | ✅ | ✅ |
| Fenced exactly-once execution | ✅ | — | — | — | 🟡 | — | — | 🟡 |
| Durable state store for jobs (KV/locks/cursors/secrets) | ✅ | — | — | — | — | — | 🟡 | 🟡 |
| **🔹 Observability & control** | | | | | | | | |
| Live web dashboard (tail · run · cancel) | ✅ | — | — | — | 🟡 | ✅ | ➕ | ✅ |
| HTTP REST control API | ✅ | ✅ | — | — | ✅ | ✅ | ✅ | ✅ |
| Native Prometheus / statsd metrics | ✅ | ✅ | ✅ | — | ✅ | — | ➕ | ✅ |
| Per-job resource monitoring (CPU/peak mem) | ✅ | — | — | — | 🟡 | ✅ | ➕ | — |
| Failure reporting (mail · Sentry · Slack) | ✅ | ✅ | 🟡 | ✅ | 🟡 | ✅ | ➕ | ✅ |
| **🔹 Platform & deployment** | | | | | | | | |
| Native Windows + macOS + Linux | ✅ | — | — | 🟡 | ✅ | 🟡 | — | — |
| Self-contained multi-arch binaries | ✅ | 🟡 | ✅ | ✅ | ✅ | — | — | — |
| Hardened containers (non-root · read-only · distroless) | ✅ | — | — | — | — | — | 🟡 | 🟡 |
| Minimal runtime dependencies | ✅ | 🟡 | ✅ | ✅ | ✅ | — | — | — |
| **Native features (of 24)** | **24** | **7** | **5** | **3** | **8** | **9** | **6** | **13** |

### The part nobody else has

cronstable's MCP server is served at `POST /mcp` (Streamable HTTP) and via a `cronstable mcp` stdio bridge, hand-rolled in pure Python against MCP revision `2025-11-25` — **zero new dependencies**. It exposes **23 tools**, URI resources, and 5 triage-prompt playbooks, and is **safe by default**:

- **Read-only until you opt in** — mutating tools are stripped unless you enable them.
- **Human-in-the-loop writes** — every mutation needs `confirm: true`; backfills default to a dry-run preview.
- **Inherits your web auth** and *fails closed* with no token on a routable listener; secret values and KV data are redacted.

```shell
# wire it to Claude Code
claude mcp add --transport http cronstable https://your-host/mcp \
  --header "Authorization: Bearer $CRONSTABLE_WEB_TOKEN"
```

---

*Methodology: rows are cronstable's own capability set; competitor cells were checked against each project's official docs and adversarially re-verified. Airflow is included as the heavyweight-orchestrator reference — it matches cronstable's orchestration but carries no native MCP server (its AIP-91 proposal is still a draft; only third-party REST wrappers exist, hence ➕). "Classic crontab" is scored — for tools that accept a cron expression but do not ingest crontab files. Snapshot July 2026; verify against current docs before quoting.*
