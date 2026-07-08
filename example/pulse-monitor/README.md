# Pulse — a real-time uptime / SLA monitor (second-level scheduling)

A small, self-contained cronstable project that shows why **second-level
scheduling** matters: it watches a latency-critical HTTP service and catches an
outage within **seconds**, not minutes.

## Why not just use a once-a-minute cron?

A classic five-field crontab checks at most once per minute. Between two checks
there is a blind spot of up to **60 seconds** — long enough to miss an entire
short outage, blow an availability SLO, or page late. For anything user-facing
and latency-critical (a payment API, an auth gateway, an edge cache) you want to
know in a few seconds.

cronstable can schedule at second granularity, so one small daemon can probe every
2–5 seconds, heartbeat every 10, and still roll up a summary once a minute.

## Run it

The "service under test" is cronstable's **own web API** (`GET /status`), so there
is nothing else to start — the monitor watches its own liveness endpoint.

```console
# in a container (publishes the dashboard on :8080)
docker compose -f docker-compose-pulse.yml up --build

# …or locally, from a checkout (needs python3, which the cronstable image has)
cronstable -c example/pulse-monitor/cronstable.yaml
```

Open <http://localhost:8080/> and watch the `liveness-probe` and `latency-slo`
rows: their **next-run countdowns tick down in seconds**, and their run history
fills in several times a minute. Open a probe's logs to tail its verdicts live.

Stop it:

```console
docker compose -f docker-compose-pulse.yml down
```

## The jobs

| Job | Cadence | Schedule spelling | What it does |
| --- | --- | --- | --- |
| `banner` | once, at start | `@reboot` | Prints the target and SLO so the dashboard has a first run to show. |
| `liveness-probe` | every **5 s** | object `second: "*/5"` | GETs the service; fails (and pages the on-call shell hook) if it is unreachable or non-200. `concurrencyPolicy: Forbid` + `executionTimeout` keep a hung probe from stacking. |
| `latency-slo` | every **2 s** | 7-field string `*/2 * * * * * *` | Measures round-trip time; a response slower than `PULSE_BUDGET_MS` writes to stderr, and `failsWhen.producesStderr` turns that into a failed run. |
| `heartbeat` | every **10 s** | object `second: "*/10"` | Emits a liveness pulse so a downstream dead-man's-switch can tell the monitor itself is alive. |
| `sla-rollup` | every **60 s** | 5-field `* * * * *` | A per-minute summary. It coexists with the second-level jobs and fires **exactly once per minute** — see below. |

The two spellings are equivalent; use whichever reads better:

```yaml
schedule: "*/5 * * * * * *"   # 7-field string: second minute hour dom month dow year
schedule:                     # …or the object form
  second: "*/5"
```

## Things to notice

- **Second-level countdowns.** In the dashboard, the probe rows count down in
  seconds. A plain cron dashboard can only ever count down in whole minutes.
- **Mixed cadence, no double-runs.** Because a second-level job makes the whole
  scheduler tick every second, a minute-level job *could* be tested 60 times a
  minute — but cronstable de-duplicates each job per scheduling slot, so
  `sla-rollup` still fires exactly once per minute. Sub-minute and per-minute
  jobs mix freely.
- **No cost when unused.** Delete the second-level jobs and cronstable goes back to
  waking once a minute; the per-second cadence only turns on while some enabled
  job actually needs it.

## See it catch an outage

Point the probes at a dead port and watch the rows go red within ~5 seconds and
the on-call "PAGE" line appear in `liveness-probe`'s report output:

```console
# container
docker compose -f docker-compose-pulse.yml run --rm \
  -e PULSE_TARGET=http://127.0.0.1:9/nope cronstable-pulse

# local
PULSE_TARGET=http://127.0.0.1:9/nope cronstable -c example/pulse-monitor/cronstable.yaml
```

To exercise the **latency** path instead, tighten the budget so normal
round-trips breach it: `PULSE_BUDGET_MS=1`.

## Point it at a real service

Set `PULSE_TARGET` to any URL you want to monitor and swap the on-call
`shell` report for a real [`webhook` or `mail` reporter](../../wiki/Reporting.md)
(Slack, PagerDuty, ntfy, …). Everything else stays the same.

See [Schedules and Timezones → Second-level schedules](../../wiki/Schedules-and-Timezones.md#second-level-schedules)
for the full reference.
