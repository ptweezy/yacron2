# cronstable Wiki

cronstable is a cron replacement built on asyncio that runs natively on Linux, macOS, and Windows. Its "crontab" is written in YAML ([classic Vixie crontabs](Classic-Crontabs) are accepted as-is too), so jobs, schedules, and behavior are all declared in configuration: it reports job failures by email, Sentry, or shell command; retries failing jobs with exponential backoff; emits job metrics to statsd and serves them natively to Prometheus; and can expose an optional HTTP control API and a built-in [web dashboard](Web-Dashboard) to watch, run, cancel, and tail jobs live. An opt-in [durable state store](Durable-State) adds restart-surviving run history, missed-run catch-up, and state primitives for jobs; an optional [DAG engine](Orchestration-and-DAGs) runs dependency-ordered workflows on top of it; and an optional [MCP server](MCP) lets AI agents observe the daemon and, when you opt in, act on jobs and DAGs. When one instance is not enough, opt-in [clustering and leader election](Clustering-and-Leader-Election) lets several replicas run one config without double-running jobs, coordinated by mTLS gossip or fenced through a Kubernetes or etcd lease. It runs in the foreground, logs to stdout/stderr, and supports arbitrary timezones, which suits Docker, Kubernetes, and 12-factor deployments. cronstable is a fork of [gjcarneiro/yacron](https://github.com/gjcarneiro/yacron) (by Gustavo Carneiro), continuing development from version 0.19.

## Contents

### Getting Started

- [Installation](Installation): Install via Docker, pip, pipx, or the self-contained binary.
- [Command-Line Reference](CLI-Reference): The `cronstable` command and its flags, config file/directory loading, the `state` administration subcommand, the job-facing state commands, and the `mcp` and `tui` client subcommands.
- [Production and Container Deployment](Production-Deployment): Running hardened under non-root, read-only-root-filesystem Kubernetes/Docker.
- [Running on Windows](Running-on-Windows): Installing and running cronstable natively on Windows: config path, default shell, Ctrl-C shutdown, and unsupported features.

### Configuration

- [Configuration Reference](Configuration-Reference): The full YAML schema: top-level sections and per-job options.
- [Classic Crontabs](Classic-Crontabs): Running plain Vixie-style crontab files as-is, how entries map onto cronstable's standard defaults, and the documented deviations from cron.
- [Schedules and Timezones](Schedules-and-Timezones): Crontab strings, schedule objects, `@reboot`, UTC vs. local, and arbitrary timezones.
- [Business-Day Schedules](Business-Day-Schedules): The month-shaped day forms: `L-n` offsets from month-end, `nW` nearest-weekday, `LW` last weekday, and `d#n` nth-weekday (`5#3` = third Friday), with Quartz porting notes.
- [Schedule Linting](Schedule-Linting): Advisory findings for legal-but-suspect schedules: dead never-fires schedules, AND day semantics, uneven steps, skipped months, and DST anomalies.
- [Hashed Schedules (H)](Hashed-Schedules): Jenkins-style `H` fields that hash the job name to a stable slot, spreading a fleet across the hour while keeping every job's fire time predictable.
- [Commands and Environment](Commands-and-Environment): Shell vs. argv commands, environment variables, env files, and per-job user/group.
- [Output Capturing](Output-Capturing): Capturing stdout/stderr and customizing stream prefixes.
- [Includes, Defaults, and Multi-File Config](Includes-and-Defaults): Sharing settings via `defaults`, the `include` directive, and multi-file config directories.
- [Logging Configuration](Logging-Configuration): Customizing cronstable's own logging via the `logging` section.

### Job Behavior

- [Concurrency and Timeouts](Concurrency-and-Timeouts): `concurrencyPolicy`, `executionTimeout`, and `killTimeout`.
- [Failure Detection and Retries](Failure-Detection-and-Retries): `failsWhen` rules, `retry` with exponential backoff, and `onPermanentFailure`.
- [Resource Monitoring](Resource-Monitoring): Opt-in per-job `monitorResources` accounting: whole-process-tree CPU and peak-memory sampling, with per-run chart series, surfaced across the dashboards, HTTP API, metrics, and reports.
- [Durable State](Durable-State): The opt-in `state` section: a restart-surviving run ledger, missed-run catch-up, output archival, and the job-facing state primitives (key-value, cursors, locks, artifacts, idempotency keys, secrets).
- [Orchestration and DAGs](Orchestration-and-DAGs): The `dags:` section: dependency-ordered workflows of tasks on a schedule, with XCom, fan-out mapping, sensors, approval gates, and backfill, built on the durable state store.
- [Clustering and Leader Election](Clustering-and-Leader-Election): the `cluster` section: mTLS peer attestation, quorum-gated leader election, per-job `clusterPolicy`, and a pluggable `backend` (gossip / kubernetes / etcd) for best-effort or fenced exactly-once election.
- [Job-Set ID](Job-Set-ID): The deterministic, order-independent fingerprint of a running job set: what it covers, why it embeds no secret material, and where it surfaces (CLI, HTTP, logs, dashboards, metrics, cluster).

### Integrations

- [Reporting (Mail, Sentry, Shell, Webhook)](Reporting): `onFailure`/`onSuccess` reporting via email, Sentry, shell, and webhook (Slack-compatible), with jinja2 templating.
- [Metrics with statsd](Metrics-with-Statsd): Emitting start/stop/success/duration metrics over UDP to statsd.
- [Metrics with Prometheus](Metrics-with-Prometheus): The `/metrics` endpoint on the web API, with job, scheduler, and cluster metrics in Prometheus or OpenMetrics format.
- [HTTP Control API](HTTP-API): The optional REST interface: job status, on-demand start and cancel, run history, live log streaming, DAG and state-inspection routes, and cluster/fleet views.
- [Web Dashboard](Web-Dashboard): The built-in browser dashboard: live status and log tailing, run history and resource charts, timezone-aware schedule previews, incident tools, cluster and fleet views, and a wallboard/TV mode, in one self-contained page.
- [Terminal Dashboard](Terminal-Dashboard): `cronstable tui`, the dashboard's TUI sibling — the same board and the same keyboard shortcuts, in a terminal over SSH.
- [Calendar Export (iCal)](Calendar-Export): The scheduler's upcoming fires as subscribable `.ics` feeds (fleet-wide and per job) and the dashboard's seven-day week calendar, with token-in-URL auth for calendar clients.
- [Schedule Pressure](Schedule-Pressure): The fleet's forward-looking collision heatmap: every fire over the next 24 hours, bucketed hour by minute, on the API, the dashboard, and the wallboard.
- [Duplicate Schedule Detection](Duplicate-Schedule-Detection): Groups of jobs whose schedules fire on the identical instants, by the engine's own semantic equality.
- [Suggest a Slot](Suggest-a-Slot): The least-loaded minute or hour:minute for a new job, recommended from the fleet's real fires.
- [Why Didn't It Run?](Why-No-Run): Field-by-field explanations of why a job's schedule did or did not select a timestamp, with the nearest real fires and notes on the AND day rule and DST, over the API and MCP.
- [MCP Server (Model Context Protocol)](MCP): The optional MCP server: `POST /mcp` on the web listeners plus the `cronstable mcp` stdio bridge, with read-only-by-default toolsets so AI agents can observe and, when you opt in, act.

### Reference and Development

- [Architecture and Internals](Architecture-and-Internals): How the asyncio scheduler, jobs, and reporters fit together.
- [MCP Server Design](MCP-Server-Design): The design document behind the MCP server, preserved as written, with the shipped divergences flagged up front.
- [Contributing and Releasing](Contributing-and-Releasing): Development setup, the test/lint/type-check workflow, and the release process.
- [Performance Benchmarks](Performance-Benchmarks): The CI benchmark suite: every commit measured against the last release, a per-release diff chart, and a regression gate on publishing.
- [Migration from yacron](Migration-from-yacron): Moving from gjcarneiro/yacron to cronstable.
- [Troubleshooting and FAQ](Troubleshooting): Common problems, errors, and answers.
