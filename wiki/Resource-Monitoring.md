# Resource Monitoring

cronstable can record what each job run actually *uses*. Opt-in per-job resource accounting (`monitorResources`) samples the run's **whole process tree** (children and shell-outs included) with [psutil](https://github.com/giampaolo/psutil) while it runs, and records the run's **total CPU time** (user + system), its **sampled peak resident memory**, and -- optionally -- a downsampled CPU%/RSS chart series. The numbers ride the run record everywhere a run already reports: the [web dashboard](Web-Dashboard), the [terminal dashboard](Terminal-Dashboard), the [HTTP API](HTTP-API), [Prometheus](Metrics-with-Prometheus), [statsd](Metrics-with-Statsd), failure [reports](Reporting), and the durable [run ledger](Durable-State).

It is observability only: monitoring never changes a run's success/failure verdict, never delays or crashes a job, and is off by default -- a per-run sampling task is spawned only when it is on. psutil is a core dependency (it ships wheels for the mainstream targets); if it is somehow unavailable, monitoring degrades to "no data" rather than failing anything.

## Enabling it

`monitorResources` is a per-job option; set it once under a [`defaults:` block](Includes-and-Defaults) to enable it fleet-wide:

```yaml
jobs:
  - name: nightly-model-refresh
    command: python -m models.refresh
    schedule: "0 4 * * *"
    monitorResources: true
```

The bool form samples with the defaults (a 1s cadence, a 240-point chart series per run). [DAG tasks](Orchestration-and-DAGs) accept the option too; see [DAG tasks](#dag-tasks) below.

### Options

The schema is `Bool() | Map({Opt("enabled"): Bool(), Opt("interval"): Float(), Opt("history"): Int()})` -- a bool shorthand or a map, every key optional:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | bool | `true` | Turn sampling on. Writing the map form at all opts in, so `enabled` exists to keep a tuned map in place while switched off. |
| `interval` | float | `1.0` | Seconds between process-tree samples. Minimum `0.1`; each sample walks the process table, so a sub-100ms cadence would busy-loop, and violating the bound is a load-time `ConfigError`. A shorter interval catches sharper RSS spikes at the cost of more wakeups; total CPU is cumulative and re-read every sample, so it converges regardless of the interval. |
| `history` | int | `240` | Chart-series points kept per run; `0` keeps the summary numbers only. Must be between `0` and `2000` (load-time `ConfigError` otherwise), bounding what one run adds to a durable ledger record. |

```yaml
monitorResources:
  interval: 0.5     # seconds between samples (default 1.0, minimum 0.1)
  history: 240      # chart points kept per run (default 240; 0 = summary only)
```

The option merges normally under `defaults:` and is **not** part of the [job-set id](Job-Set-ID) fingerprint -- turning monitoring on or off never changes a job's identity. The per-job option row and the numeric validation table also appear in the [Configuration Reference](Configuration-Reference#metrics).

## How sampling works

The monitor attaches to the launched child's pid immediately after launch, takes a first reading right away (so even a short run has a chance of a sample), then polls on the configured `interval`. One final opportunistic reading is taken when the run ends.

- **The whole tree is accounted.** Each sample reads the child and all of its descendants. CPU time is tracked per tree member, and a member that exits has its last reading banked before it is forgotten, so sequential children (`sh -c 'a; b'`) accumulate instead of plateauing. The only CPU that escapes accounting entirely is a child that spawns *and* exits within a single sampling gap.
- **Peak RSS is a sampled high-water mark.** The recorded maximum is the highest resident-set sum observed across samples, so a spike narrower than the sampling gap can be missed; a shorter `interval` narrows that window.
- **Best-effort, never fatal.** Every psutil interaction is guarded: a process that exits mid-sample, a platform that denies the read, or psutil raising anything at all simply yields whatever was captured so far. A run where nothing could be sampled carries no resource stats (`resources` is `null` in the API); it is never an error.
- **Sampled, so approximate for short runs.** A run that finishes between two samples is measured approximately; the long, heavy runs whose resource use actually matters are sampled many times and measured well.

A finished monitored run records a summary object -- `cpu_user_seconds`, `cpu_system_seconds`, `cpu_total_seconds`, `max_rss_bytes`, and `samples` (how many times the tree was successfully read). While the run is still going, the live readings are `cpu_seconds` (cumulative), `cpu_percent` (usage since the previous sample; can exceed 100 across multiple cores), and `rss_bytes` (the tree's current resident memory).

## The chart series

Alongside the summary numbers, each monitored run records a `[t, cpu%, rss]` point per sample -- the data behind the dashboard's Resources charts. Once a run exceeds its `history` cap, the series is downsampled in place: adjacent buckets merge with the **mean** CPU% but the **peak** RSS (the memory spikes people monitor for are never averaged away) and the effective resolution halves, so a run of any length stays within `history` points with uniform bucket widths -- a few KB even for a days-long run. `history: 0` disables the series and keeps the summary only.

The series is embedded in the durable run record's `resources.series`, so charts survive restarts, and is deliberately **excluded** from the polled `/jobs` and `/jobs/{name}/runs` payloads; it is served by the dedicated [`GET /jobs/{name}/resources`](HTTP-API#get-jobsnameresources) endpoint instead, which the dashboard fetches lazily when a job's Resources tab is opened, never on the poll loop.

## Where the numbers surface

| Surface | What appears |
| --- | --- |
| [Web Dashboard](Web-Dashboard) | Live CPU/memory chips on a running job's row and drawer; per-run CPU and peak-memory columns and stats in the drawer's History tab; the drawer's **Resources** tab charts the live instance, the recorded profile of recent runs, and per-run trend strips (an unmonitored job gets a pointer at the config instead of an empty chart). |
| [Terminal Dashboard](Terminal-Dashboard) | Live CPU/memory chips on monitored rows, a resources view in the job drawer, and a node resources panel. |
| [HTTP API](HTTP-API) | `running_resources` (live, summed over running instances) on [`GET /jobs`](HTTP-API#get-jobs); a per-run `resources` object plus windowed CPU/RSS aggregates on [`GET /jobs/{name}/runs`](HTTP-API#get-jobsnameruns); the chart-grade series on [`GET /jobs/{name}/resources`](HTTP-API#get-jobsnameresources). |
| [Prometheus](Metrics-with-Prometheus#per-job) | `cronstable_job_cpu_seconds_total{job_name, mode}`, `cronstable_job_peak_rss_bytes`, `cronstable_job_last_run_cpu_seconds`, and `cronstable_job_last_run_max_rss_bytes` -- emitted only once the job has a monitored run; the two cumulative families are made restart-durable by a `state:` store. |
| [statsd](Metrics-with-Statsd#on-stop) | A `<prefix>.cpu` timer and a `<prefix>.max_rss` gauge appended to a monitored run's stop datagram; an unmonitored job's datagram is unchanged. |
| [Reporting](Reporting) | `cpu_seconds` / `cpu_user_seconds` / `cpu_system_seconds` / `max_rss_bytes` [template variables](Reporting#templating) and `CRONSTABLE_CPU_SECONDS` / `CRONSTABLE_MAX_RSS_BYTES` in the [shell reporter's environment](Reporting#environment-variables), so a failure page can say how big the run was when it died. |
| [Durable State](Durable-State) | The run ledger record carries the `resources` object, chart series included, so stats and charts survive restarts; retention follows the ledger's `state.maxRunsPerJob` pruning. |
| [MCP](MCP) | The read-only `cron_get_job_resources` tool (`observe` toolset) returns the same live and per-run series as `GET /jobs/{name}/resources`. |

## DAG tasks

[DAG](Orchestration-and-DAGs) tasks accept `monitorResources` with the same schema, but surface the result differently: a finished task instance's summary usage lands in the `resources` object of its task record inside the durable `dag_run` document (returned by `GET /dags/{name}/runs/{run_key}`), and a task with a `statsd` sink sends the same `cpu` / `max_rss` lines. Task instances are ephemeral and do not appear in the per-job Prometheus families on `GET /metrics`.

## Node-level resources

Three adjacent surfaces report the **node's** load rather than a job's; none of them requires `monitorResources`:

- [`GET /node`](HTTP-API#get-node) samples the serving host's live CPU/memory (plus the daemon's own footprint) fresh per request and drives the dashboard header's node meter. It is container-aware: under a cgroup v2 limit the numbers describe the daemon's own slice, not the host.
- [`GET /node/history`](HTTP-API#get-nodehistory) serves a background-sampled CPU/memory ring (every 5s, keeping the last hour, by default) charted behind the header meter; it is tuned or disabled via [`web.nodeHistory`](Configuration-Reference#web).
- [`cluster.observability`](Configuration-Reference#observability-overlay) shares each node's whole-node CPU/memory across a [cluster](Clustering-and-Leader-Election), so the dashboard's cluster panel and fleet view show where the load actually is.

## Version notes

- Per-job resource monitoring (`monitorResources: true`), the dashboard/API/Prometheus/statsd/report surfaces, `GET /node`, and `cluster.observability` were added in 1.2.8, which also made psutil a core dependency (see `HISTORY.md`).
- The map form (`interval` / `history`), the per-run chart series, `GET /jobs/{name}/resources`, `GET /node/history` with `web.nodeHistory`, and the dashboard's Resources tab and node card were added in 1.2.9.

## See also

- [Configuration Reference](Configuration-Reference#metrics): the per-job option table and load-time numeric validation.
- [HTTP Control API](HTTP-API): the endpoints that carry the numbers, with response examples.
- [Metrics with Prometheus](Metrics-with-Prometheus): the resource metric families and their restart-durability.
- [Metrics with statsd](Metrics-with-Statsd): the exact wire format of the `cpu` / `max_rss` lines.
- [Reporting (Mail, Sentry, Shell, Webhook)](Reporting): the resource template and environment variables.
- [Durable State](Durable-State): the run ledger the summary and series persist into.
- [Orchestration and DAGs](Orchestration-and-DAGs): `monitorResources` on DAG tasks.
- [Clustering and Leader Election](Clustering-and-Leader-Election): the `cluster.observability` fleet-wide node stats.
