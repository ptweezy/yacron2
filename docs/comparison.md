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
