# Late-Run Detection (SLA Monitoring)

Cron's failure model only sees runs that happened; the runs that hurt most are the ones that did not. Per-job SLA monitoring watches for exactly that: a job that has gone too long without a success, a due slot that never started, or a run that is still going long past what it should take. Each job declares its thresholds in an `sla:` block, and a dedicated `onLate` reporting hook fires once when a threshold is breached, through the same four reporters as [failure reporting](Reporting).

The monitor lives in `cronstable/cron.py` (`Cron._sla_periodic`), evaluating every configured check once per wall-clock minute, entirely in memory: it needs no [state store](Durable-State), though one improves the staleness check across restarts. Breaches surface on every dashboard surface: the [HTTP API](HTTP-API#get-jobs) (`sla` on `GET /jobs`), the [web](Web-Dashboard) and [terminal](Terminal-Dashboard) dashboards (an **OVERDUE** badge), [Prometheus](Metrics-with-Prometheus#per-job), and the [MCP](MCP) observe tools.

One naming caution: `GET /jobs/{name}/trends` reports historical "SLA aggregates" (success rates and durations over the durable ledger). That surface describes runs that finished; this page's `sla:` block watches for runs that have not happened. They share the acronym and nothing else.

## Configuring it

```yaml
jobs:
  - name: nightly-etl
    command: python -m etl.run
    schedule: "0 4 * * *"
    sla:
      maxTimeSinceSuccessSeconds: 129600   # page when no success for 36h
      lateAfterSeconds: 900                # page when a due slot has not started within 15min
      maxRuntimeSeconds: 7200              # page when a run exceeds 2h
    onLate:
      report:
        webhook:
          url:
            fromEnvVar: SLACK_WEBHOOK_URL
```

### Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `sla.maxTimeSinceSuccessSeconds` | int or null | `null` (off) | Breach when this many seconds pass without a successful finish. Must be `> 0` when set. |
| `sla.lateAfterSeconds` | int or null | `null` (off) | Breach when a due scheduled slot has not started a run within this many seconds. Must be `> 0` when set. |
| `sla.maxRuntimeSeconds` | int or null | `null` (off) | Breach while any running instance has been running longer than this. Observes only; the run is never killed (use [`executionTimeout`](Concurrency-and-Timeouts) to enforce a limit). Must be `> 0` when set. |
| `onLate.report` | report block | reporter defaults | The [reporters](Reporting) fired once per breach: `mail`, `sentry`, `shell`, `webhook`, the same schema as `onFailure.report` with overdue-specific default templates. |

The three thresholds are independent; set any subset. Configuring an `onLate` reporter (a mail recipient, a sentry DSN, a shell command, a webhook URL) with all three thresholds unset is a load-time `ConfigError` (`onLate requires sla`): a reporter that can never fire is a misconfiguration, not a default. Both keys merge normally under a [`defaults:` block](Includes-and-Defaults) and, like the catch-up options, are excluded from the [job-set id](Job-Set-ID) fingerprint: alerting thresholds are not part of a job's identity.

## The three checks

Check names are the config keys minus their `Seconds` suffix: `maxTimeSinceSuccess`, `lateAfter`, `maxRuntime`. That one vocabulary appears everywhere a check is named: the metric `check` label, the payload's `check` field, and the `{{sla_check}}` template variable.

1. **`maxTimeSinceSuccess`**: breached when `now - last successful finish` exceeds the threshold. When no success is on record (a stateless daemon after a restart, or a job that has never succeeded), the reference is the daemon's start time, so a fresh boot ages into the breach rather than paging instantly. With a [durable run ledger](Durable-State) the real last success is rehydrated at boot, and paging soon after a restart for a genuinely stale job is the correct behavior.
2. **`lateAfter`**: a scheduled slot falls due, and no run of the job has started since it. Breached when `now - due` exceeds the threshold; any start (scheduled, catch-up, retry, or manual) clears it. Slots skipped because the job was [paused](Pausing-Jobs) are excused, and a restart baselines on the next due slot.
3. **`maxRuntime`**: breached while any currently-running instance has been running longer than the threshold, measured from the run's launch instant. Clears when the run ends. It never terminates anything.

A disabled or [paused](Pausing-Jobs) job is not evaluated at all, and under [leader election](Clustering-and-Leader-Election) only the node that owns the job evaluates it, so one breach pages once, not once per node.

## Breaches latch

Each `(job, check)` pair carries a latch. On the transition into breach, cronstable fires the `onLate` reporters once, sets `cronstable_job_late{job_name, check}` to `1`, increments `cronstable_job_sla_breaches_total`, and logs a warning naming the observed and threshold seconds. While the breach persists, nothing re-fires. On recovery the gauge clears and an info line is logged; recovery sends no report. The latch is in-memory, so after a daemon restart a still-breached check fires its report once more.

Reports are dispatched off the scheduler loop and ordered after the same job's in-flight completion reports, so a slow SMTP server can never stall scheduling.

## The onLate report

`onLate.report` takes the exact schema of the other [reporting hooks](Reporting), with defaults reworded for a breach (there is no run outcome to describe). The default mail subject is:

```text
Cron job '{{name}}' is overdue ({{sla_check}})
```

the default body names the check, the threshold, the observed value, and the last success (or `(none recorded)`); the default webhook body wraps the same text in the Slack-compatible `{"text": ...}` shape; and the default sentry fingerprint is `["cronstable", "sla", "{{ name }}"]`, so breaches group as their own Sentry issue per job instead of folding into run failures.

Templates receive the full standard [template variable set](Reporting#templating) (with the run-shaped fields empty: `success` is `false`, `fail_reason` is `sla: <check> breached`, `stdout`/`stderr`/`exit_code` are `null`) plus four breach variables, which the shell reporter also receives as environment variables:

| Template variable | Environment variable | Value |
| --- | --- | --- |
| `sla_check` | `CRONSTABLE_SLA_CHECK` | The check name: `maxTimeSinceSuccess`, `lateAfter`, or `maxRuntime`. |
| `threshold_seconds` | `CRONSTABLE_SLA_THRESHOLD_SECONDS` | The configured threshold. |
| `observed_seconds` | `CRONSTABLE_SLA_OBSERVED_SECONDS` | The measured value that breached it. |
| `last_success_at` | `CRONSTABLE_LAST_SUCCESS_AT` | ISO-8601 instant of the last known success, or `null` (empty string in the environment). |

## Where breaches show

- **`GET /jobs`** carries an `sla` object for every job with a configured check (and only those): `thresholds` (the non-null keys), `state` (`"ok"` or `"late"`), and `breaches`, a list of `{check, since, observed_seconds, threshold_seconds}` where `since` is when the monitor latched the breach and `observed_seconds` is re-measured at payload time, so dashboards show a moving number. See [HTTP API](HTTP-API#get-jobs).
- **[Prometheus](Metrics-with-Prometheus#per-job)**: `cronstable_job_late{job_name, check}` (0/1 per check) and `cronstable_job_sla_breaches_total{job_name, check}`, both emitted once the monitor first evaluates the job's checks.
- **The [web dashboard](Web-Dashboard)** shows an **OVERDUE** badge on late jobs (row chip, drawer, wallboard); the [terminal dashboard](Terminal-Dashboard) paints the same suffix.
- **[MCP](MCP)** observe tools (`cron_list_jobs`, `cron_get_job`) return the same `sla` object.

## The monitor cannot report its own death

`onLate` runs inside the cronstable daemon. A killed daemon, a hung host, or a partitioned node takes the monitor down with the jobs it watches, and no in-process check can page about that. Pair the `sla:` block with the external staleness alert documented on [Metrics with Prometheus](Metrics-with-Prometheus#example-alerts): a Prometheus server alerting on `time() - cronstable_job_last_success_timestamp_seconds` (and on the scrape itself going stale via `up == 0`) watches from outside the process, so the two layers cover each other. Use `onLate` for per-job thresholds with rich, job-aware notifications; keep the Prometheus rule as the backstop that still fires when the daemon itself is gone.

## See also

- [Pausing Jobs](Pausing-Jobs): pausing suppresses a job's SLA checks.
- [Reporting (Mail, Sentry, Shell, Webhook)](Reporting): the reporter options `onLate.report` accepts.
- [Metrics with Prometheus](Metrics-with-Prometheus): the metric families and the external staleness alert to pair with.
- [Failure Detection and Retries](Failure-Detection-and-Retries): the hooks for runs that happened and failed.
- [Hashed Schedules](Hashed-Schedules): stable `H` slots keep "was this run late?" answerable.
- [Configuration Reference](Configuration-Reference#sla-monitoring-and-the-onlate-hook): the schema and load-time validation.
