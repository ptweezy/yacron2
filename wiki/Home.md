# yacron2 Wiki

yacron2 is a cron replacement built on asyncio for POSIX systems. Its "crontab" is written in YAML, so jobs, schedules, and behavior are all declared in configuration: it reports job failures by email, Sentry, or shell command; retries failing jobs with exponential backoff; emits job metrics to statsd; and can expose an optional HTTP control API to inspect status and trigger jobs on demand. It runs in the foreground, logs to stdout/stderr, and supports arbitrary timezones, which suits Docker, Kubernetes, and 12-factor deployments. yacron2 is a fork of [gjcarneiro/yacron](https://github.com/gjcarneiro/yacron) (by Gustavo Carneiro), continuing development from version 0.19.

## Contents

### Getting Started

- [Installation](Installation) — Install via Docker, pip, pipx, or the self-contained binary.
- [Command-Line Reference](CLI-Reference) — The `yacron2` command, its flags, and config file/directory loading.
- [Production and Container Deployment](Production-Deployment) — Running hardened under non-root, read-only-root-filesystem Kubernetes/Docker.

### Configuration

- [Configuration Reference](Configuration-Reference) — The full YAML schema: top-level sections and per-job options.
- [Schedules and Timezones](Schedules-and-Timezones) — Crontab strings, schedule objects, `@reboot`, UTC vs. local, and arbitrary timezones.
- [Commands and Environment](Commands-and-Environment) — Shell vs. argv commands, environment variables, env files, and per-job user/group.
- [Output Capturing](Output-Capturing) — Capturing stdout/stderr and customizing stream prefixes.
- [Includes, Defaults, and Multi-File Config](Includes-and-Defaults) — Sharing settings via `defaults`, the `include` directive, and multi-file config directories.
- [Logging Configuration](Logging-Configuration) — Customizing yacron2's own logging via the `logging` section.

### Job Behavior

- [Concurrency and Timeouts](Concurrency-and-Timeouts) — `concurrencyPolicy`, `executionTimeout`, and `killTimeout`.
- [Failure Detection and Retries](Failure-Detection-and-Retries) — `failsWhen` rules, `retry` with exponential backoff, and `onPermanentFailure`.

### Integrations

- [Reporting (Mail, Sentry, Shell)](Reporting) — `onFailure`/`onSuccess` reporting via email, Sentry, and shell, with jinja2 templating.
- [Metrics with statsd](Metrics-with-Statsd) — Emitting start/stop/success/duration metrics over UDP to statsd.
- [HTTP Control API](HTTP-API) — The optional REST interface for status and on-demand job starts.

### Reference and Development

- [Architecture and Internals](Architecture-and-Internals) — How the asyncio scheduler, jobs, and reporters fit together.
- [Contributing and Releasing](Contributing-and-Releasing) — Development setup, the test/lint/type-check workflow, and the release process.
- [Migration from yacron](Migration-from-yacron) — Moving from gjcarneiro/yacron to yacron2.
- [Troubleshooting and FAQ](Troubleshooting) — Common problems, errors, and answers.
