# How cronstable compares

**cronstable is the only cron-family scheduler with a built-in [MCP server](https://github.com/ptweezy/cronstable/wiki/MCP)**. AI agents (Claude, Cursor, Copilot) can observe your cronstable state and act on them when you opt in. All alongside durable state, a real DAG engine, leader-elected clustering and a live dashboard, in a single hardened, dependency-free daemon.

**Legend:** ✅ native · 🟡 partial / limited · ➕ requires an add-on

| Capability | cronstable | yacron | supercronic | Ofelia | dkron | Cronicle | K8s CronJob | Airflow |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **🔹 AI & agent control** | | | | | | | | |
| MCP server for AI agents (observe + control) | ✅ | — | — | — | — | — | ➕ | ➕ |
| Agent triage playbooks & resources | ✅ | — | — | — | — | — | — | — |
| **🔹 Scheduling core** | | | | | | | | |
| YAML job configuration | ✅ | ✅ | — | — | — | — | ✅ | ➕ |
| Classic Vixie crontab files as-is | ✅ | — | ✅ | — | — | 🟡 | — | — |
| Sub-minute (second-level) schedules | ✅ | — | ✅ | ✅ | ✅ | — | — | 🟡 |
| Extended cron dialect (last-day · last-weekday · year) | ✅ | ✅ | ✅ | — | — | 🟡 | — | ✅ |
| `@reboot` once per OS boot | ✅ | 🟡 | — | — | — | — | — | — |
| Arbitrary per-job timezones | ✅ | ✅ | ✅ | 🟡 | ✅ | ✅ | ✅ | ✅ |
| Concurrency policy + execution/kill timeouts | ✅ | ✅ | 🟡 | 🟡 | ✅ | ✅ | ✅ | ✅ |
| Configurable failure conditions (stdout · stderr · exit) | ✅ | ✅ | — | — | 🟡 | 🟡 | 🟡 | 🟡 |
| Retries with exponential backoff | ✅ | ✅ | — | — | 🟡 | 🟡 | ✅ | ✅ |
| Depends-on-past gate | ✅ | — | — | — | — | — | — | ✅ |
| Missed-run catch-up after downtime | ✅ | — | — | — | — | ✅ | 🟡 | ✅ |
| **🔹 Orchestration & workflows** | | | | | | | | |
| DAGs / dependency graphs | ✅ | — | — | — | 🟡 | 🟡 | ➕ | ✅ |
| Cross-task data hand-off (XCom) | ✅ | — | — | — | — | ✅ | ➕ | ✅ |
| Dynamic task mapping (fan-out · fan-in) | ✅ | — | — | — | — | — | — | ✅ |
| Sensors (poll-until-true tasks) | ✅ | — | — | — | — | — | — | ✅ |
| Human approval gates | ✅ | — | — | — | — | — | ➕ | ✅ |
| Backfill / historical reruns | ✅ | — | — | — | — | 🟡 | — | ✅ |
| **🔹 Distributed & fault-tolerant** | | | | | | | | |
| Clustering + leader election (no double-run) | ✅ | — | — | — | ✅ | ✅ | ✅ | ✅ |
| Fenced exactly-once execution | ✅ | — | — | — | 🟡 | — | — | 🟡 |
| Cluster-wide concurrency scope | ✅ | — | — | — | 🟡 | 🟡 | ✅ | 🟡 |
| Crash-resume of in-flight runs | ✅ | — | — | — | 🟡 | 🟡 | ✅ | ✅ |
| Durable state store for jobs (KV/locks/cursors/secrets) | ✅ | — | — | — | — | — | 🟡 | 🟡 |
| **🔹 Observability & control** | | | | | | | | |
| Live web dashboard (tail · run · cancel) | ✅ | — | — | — | 🟡 | ✅ | ➕ | ✅ |
| HTTP REST control API | ✅ | ✅ | — | — | ✅ | ✅ | ✅ | ✅ |
| Native Prometheus / statsd metrics | ✅ | ✅ | ✅ | — | ✅ | — | ➕ | ✅ |
| Per-job resource monitoring (CPU/peak mem) | ✅ | — | — | — | 🟡 | ✅ | ➕ | — |
| Failure reporting (mail · Sentry · Slack) | ✅ | ✅ | 🟡 | ✅ | 🟡 | ✅ | ➕ | ✅ |
| Secret redaction in archived output | ✅ | — | — | — | — | — | ➕ | 🟡 |
| **🔹 Platform & deployment** | | | | | | | | |
| Native Windows + macOS + Linux | ✅ | — | — | 🟡 | ✅ | 🟡 | — | — |
| Self-contained multi-arch binaries | ✅ | 🟡 | ✅ | ✅ | ✅ | — | — | — |
| Hardened containers (non-root · read-only · distroless) | ✅ | — | — | — | — | — | 🟡 | 🟡 |
| Minimal runtime dependencies | ✅ | 🟡 | ✅ | ✅ | ✅ | — | — | — |
| State store backup · restore · migrate | ✅ | — | — | — | 🟡 | 🟡 | 🟡 | 🟡 |
| **Native features (of 35)** | **35** | **9** | **7** | **4** | **9** | **9** | **8** | **18** |
