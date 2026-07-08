# cronstable Wiki

cronstable is a cron replacement built on asyncio that runs natively on Linux, macOS, and Windows. Its "crontab" is written in YAML ([classic Vixie crontabs](Classic-Crontabs) are accepted as-is too), so jobs, schedules, and behavior are all declared in configuration: it reports job failures by email, Sentry, or shell command; retries failing jobs with exponential backoff; emits job metrics to statsd and serves them natively to Prometheus; and can expose an optional HTTP control API and a built-in [web dashboard](Web-Dashboard) to watch, run, cancel, and tail jobs live. When one instance is not enough, opt-in [clustering and leader election](Clustering-and-Leader-Election) lets several replicas run one config without double-running jobs, coordinated by mTLS gossip or fenced through a Kubernetes or etcd lease. It runs in the foreground, logs to stdout/stderr, and supports arbitrary timezones, which suits Docker, Kubernetes, and 12-factor deployments. cronstable is a fork of [gjcarneiro/yacron](https://github.com/gjcarneiro/yacron) (by Gustavo Carneiro), continuing development from version 0.19.

## Contents

### Getting Started

- [Installation](Installation): Install via Docker, pip, pipx, or the self-contained binary.
- [Command-Line Reference](CLI-Reference): The `cronstable` command, its flags, and config file/directory loading.
- [Production and Container Deployment](Production-Deployment): Running hardened under non-root, read-only-root-filesystem Kubernetes/Docker.
- [Running on Windows](Running-on-Windows): Installing and running cronstable natively on Windows: config path, default shell, Ctrl-C shutdown, and unsupported features.

### Configuration

- [Configuration Reference](Configuration-Reference): The full YAML schema: top-level sections and per-job options.
- [Classic Crontabs](Classic-Crontabs): Running plain Vixie-style crontab files as-is, how entries map onto cronstable's standard defaults, and the documented deviations from cron.
- [Schedules and Timezones](Schedules-and-Timezones): Crontab strings, schedule objects, `@reboot`, UTC vs. local, and arbitrary timezones.
- [Commands and Environment](Commands-and-Environment): Shell vs. argv commands, environment variables, env files, and per-job user/group.
- [Output Capturing](Output-Capturing): Capturing stdout/stderr and customizing stream prefixes.
- [Includes, Defaults, and Multi-File Config](Includes-and-Defaults): Sharing settings via `defaults`, the `include` directive, and multi-file config directories.
- [Logging Configuration](Logging-Configuration): Customizing cronstable's own logging via the `logging` section.

### Job Behavior

- [Concurrency and Timeouts](Concurrency-and-Timeouts): `concurrencyPolicy`, `executionTimeout`, and `killTimeout`.
- [Failure Detection and Retries](Failure-Detection-and-Retries): `failsWhen` rules, `retry` with exponential backoff, and `onPermanentFailure`.
- [Clustering and Leader Election](Clustering-and-Leader-Election): the `cluster` section: mTLS peer attestation, quorum-gated leader election, per-job `clusterPolicy`, and a pluggable `backend` (gossip / kubernetes / etcd) for best-effort or fenced exactly-once election.

### Integrations

- [Reporting (Mail, Sentry, Shell, Webhook)](Reporting): `onFailure`/`onSuccess` reporting via email, Sentry, shell, and webhook (Slack-compatible), with jinja2 templating.
- [Metrics with statsd](Metrics-with-Statsd): Emitting start/stop/success/duration metrics over UDP to statsd.
- [Metrics with Prometheus](Metrics-with-Prometheus): The `/metrics` endpoint on the web API, with job, scheduler, and cluster metrics in Prometheus or OpenMetrics format.
- [HTTP Control API](HTTP-API): The optional REST interface for status and on-demand job starts.
- [Web Dashboard](Web-Dashboard): The built-in browser dashboard: live status, live log tailing, run history, and timezone-aware schedule previews.

### Reference and Development

- [Architecture and Internals](Architecture-and-Internals): How the asyncio scheduler, jobs, and reporters fit together.
- [Contributing and Releasing](Contributing-and-Releasing): Development setup, the test/lint/type-check workflow, and the release process.
- [Migration from yacron](Migration-from-yacron): Moving from gjcarneiro/yacron to cronstable.
- [Troubleshooting and FAQ](Troubleshooting): Common problems, errors, and answers.
