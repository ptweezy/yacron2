# The yacron2 Grand Tour — "Meridian"

**Every yacron2 feature, at once, in one coherent deployment.**

Meridian is a fictional global commerce platform. Its entire scheduled
back-office runs on a **three-node, mutual-TLS, leader-electing yacron2
cluster** (`distribution: spread`) that shares **one durable state store**. On
top of that fleet it runs, all together:

- a realistic **scheduled job set** (ingest → transform → load → report, plus
  monitoring and housekeeping),
- **durable-state** jobs (ETL cursors, exactly-once billing, a fleet mutex,
  artifacts, run-scoped secrets, a durable counter),
- two **orchestration DAGs** (a scheduled fan-out ETL and a manual, human-gated
  release),
- **second-level SLA probes**, and
- **all four failure reporters** wired to live sinks (Mailpit for mail, an echo
  server for webhooks, a statsd exporter for metrics, stdout for shell pages;
  Sentry is one env var away).

If you only run one example, run this one. For a gentler start see
[`example/demo`](../demo) (single node), [`example/dag`](../dag) (DAGs alone),
[`example/job-state`](../job-state) (state primitives alone), or the
[`acme-platform`](../acme-platform) cluster showcase this one extends.

## Run it

```console
docker compose -f docker-compose-grand-tour.yml up --build
```

The three nodes build the image from this repo, so the example works before the
state/DAG features ship in a published release. Then open:

| What | Where |
| --- | --- |
| Node dashboards | <http://localhost:8080/> (meridian-a), …8081, …8082 (b, c) |
| Prometheus metrics (per node) | <http://localhost:8080/metrics> |
| On-call inbox (Mailpit) | <http://localhost:8025/> |
| Job metrics (statsd → Prometheus) | <http://localhost:9102/metrics> |
| Webhook sink | `docker compose -f docker-compose-grand-tour.yml logs -f webhook-sink` |

Stop and wipe everything (including the throwaway certs and the state store):

```console
docker compose -f docker-compose-grand-tour.yml down -v
```

## How it all coordinates

There is no coordination service beyond the shared state volume:

- **Leader election** is the default `gossip` backend: each node serves a
  **mutual-TLS** `/peer` endpoint, attests that every peer holds the same
  [job-set id](../../README.md#job-set-id), and the quorum elects a leader.
  `distribution: spread` gives each `Leader`/`PreferLeader` job (and DAG) its
  own **owner** node via rendezvous hashing — watch the dashboard **Owner**
  column fan the pipeline across a, b and c.
- **Durable state** is a **shared** volume (`topology: shared`). Cursors,
  locks, idempotency keys, artifacts and **every `dag_run`** live on it and
  coordinate **fleet-wide**, so a task launches exactly once even though all
  three nodes run the identical config. Each node's `cluster:` section is
  generated at start by `node-entrypoint.sh`; the job set (`platform.yaml`) is
  mounted identically into all three.

## Feature map

Every `[feature: …]` tag in [`platform.yaml`](platform.yaml) points at the job
or task that demonstrates it. The big ones:

| Area | Feature | Where |
| --- | --- | --- |
| **Scheduling** | cron strings, object form, `@reboot`, `@daily`, `utc:false` | throughout |
| | **second-level** (object `second:` and 7-field string) | `pulse-liveness`, `pulse-latency` |
| | timezones (New York / London / Tokyo) + DST advisory | `finance-eod-close`, `eu-/apac-open-report` |
| **Execution** | string vs **argv-list** command, `environment`, **`env_file`** | `orders-ingest`, `pulse-*`, `backup-warehouse` |
| | `captureStdout/Stderr` off, custom + empty `streamPrefix` | `silent-cleanup`, `sla-rollup` |
| | `executionTimeout` + `killTimeout` (job is killed) | `slow-report-generator` |
| | `enabled: false`, `saveLimit` | `legacy-nightly-sync`, `audit-log-ship` |
| **Failure** | `failsWhen` (stderr-based failure) | `config-lint`, `pulse-latency` |
| | `retry` w/ backoff + `onPermanentFailure` | `webhook-dispatch` |
| | **mail** / **webhook** / **shell** / **sentry** reporters | `cert-expiry-check` / `db-health-orders` / `slow-report-generator` / `fraud-model-refresh` |
| **Concurrency** | `Forbid` (node) vs `Replace` vs **`concurrencyScope: cluster`** | `warehouse-sync` / `search-reindex` / `inventory-sync` |
| **Clustering** | mTLS attestation, quorum election, `distribution: spread` | whole cluster; `node-entrypoint.sh` + `gen-certs.sh` |
| | `clusterPolicy` Leader / PreferLeader / EveryNode | throughout |
| **Durable state** | **cursor** watermark | `incremental-orders-export` |
| | **idempotent** exactly-once | `charge-subscriptions` |
| | **lock** fleet mutex | `compact-warehouse` |
| | **artifact** + **secret** (`fromEnvVar` + `fromFile`) + **state KV** | `build-daily-report` |
| | durable **counter** | `platform-pulse-counter` |
| | missed-run **catch-up** + `onlyIfLastSucceeded` + `archiveOutput` | `daily-reconcile` |
| **Orchestration** | DAG **XCom** + **fan-out (`expand`)** + per-task retries + fan-in | `orders-etl` |
| | DAG **approval gate** + **sensor** + `triggerRule` + manual trigger | `release-train` |
| **Metrics/API** | **statsd** push, native **Prometheus** `/metrics`, HTTP control API | `etl-build-facts`, `queue-depth-probe`; every node |
| **Config** | **`include:`**, custom **`logging:`**, a **classic crontab** | `_defaults.yaml`, `legacy.crontab` |

## Self-driving dashboard demos (UTC, no clicks)

The failures are deterministic, driven by the wall-clock minute, so they fire on
their own:

| Time (UTC) | What happens | Feature |
| --- | --- | --- |
| **:05 & :35** | `slow-report-generator` is killed by its `executionTimeout` | timeout handling; shell report |
| **:15–:19** | the four `db-health-*` checks fail together (`exit 69`) | incident correlation; `db-health-orders` posts a **webhook** (see the sink logs) and **shell**-pages |
| **:25–:29** | `config-lint` "fails" only because it wrote to stderr | `failsWhen: producesStderr` |
| **:45–:47** | `cert-expiry-check` fails alone and **mails** on-call | mail report (see Mailpit) |
| **:50** | `fraud-model-refresh` fails | Sentry (no-op until `SENTRY_DSN` set) + mail |
| every 5th min | `webhook-dispatch` fails, retries, then mails on permanent failure | retry/backoff + `onPermanentFailure` |
| every **:00** | four `hourly-*` reports collide | Schedule tab thundering-herd warning |
| continuous | `warehouse-sync` (node `Forbid`), `inventory-sync` (**cluster** `Forbid`), `search-reindex` (`Replace` → Cancelled) | concurrency policies |
| every 3 min | the `orders-etl` DAG runs end to end | XCom + fan-out + fan-in |
| every few s | the `pulse-*` probes tick the next-run countdown **in seconds** | second-level scheduling |

## Things to try

1. **Watch the spread.** Open two dashboards side by side and compare each job's
   **Owner**. Stop a node
   (`docker compose -f docker-compose-grand-tour.yml stop meridian-a`) and watch
   its owned jobs and DAGs re-home to the survivors.
2. **Lose quorum.** Stop 2 of 3 nodes: `Leader` jobs stand down (dashboard shows
   *no quorum*), while `PreferLeader` jobs keep running.
3. **Drive the release DAG.** Trigger it, then approve the gate from **any**
   node (the decision is a compare-and-set on the shared run document):

   ```console
   curl -X POST http://localhost:8080/dags/release-train/trigger
   # -> {"dag":"release-train","runKey":"manual-…"}; then, with that runKey:
   curl -X POST \
     http://localhost:8080/dags/release-train/runs/<runKey>/tasks/approve/decision \
     -H 'Content-Type: application/json' -d '{"decision":"approve","by":"me"}'
   ```

   The `canary` **sensor** then polls the platform's own `/status` until it is
   healthy, and `publish` ships the tagged build.
4. **Crash-resume a DAG.** While an `orders-etl` run is mid-flight, stop the node
   advancing it; another node adopts the run within a lease TTL and finishes it
   from durable state — no task double-launches.
5. **Prove durability.** Note `platform-pulse-counter`'s count, then
   `restart` the fleet — the counter continues from where it left off (it lives
   in the shared store, not memory).
6. **Poke the control API.** `curl http://localhost:8080/status`,
   `/jobs`, `/cluster`, `/job-set-id`, `/metrics`, and
   `curl -X POST http://localhost:8080/jobs/run-schema-migration/start`.
7. **Compare single-leader vs spread.**
   `DISTRIBUTION=single-leader docker compose -f docker-compose-grand-tour.yml up -d`
   and watch every owned job collapse onto one node.

## Files

| File | Purpose |
| --- | --- |
| [`platform.yaml`](platform.yaml) | the annotated job set + DAGs + `state:` + `web:` (mounted identically into all three nodes) |
| [`_defaults.yaml`](_defaults.yaml) | shared `defaults:` + custom `logging:`, pulled in via `include:` |
| [`legacy.crontab`](legacy.crontab) | a classic Vixie crontab, loaded as-is from the same config dir |
| [`platform.env`](platform.env) | `env_file` for `backup-warehouse` |
| [`secrets/signing.key`](secrets/signing.key) | demo file for a `fromFile` run-scoped secret |
| [`gen-certs.sh`](gen-certs.sh) | mints the throwaway cluster CA + per-node leaf certs |
| [`node-entrypoint.sh`](node-entrypoint.sh) | generates each node's `cluster:` section and assembles the config dir |

> **Note.** These nodes mount a writable state volume, so they do not run with a
> read-only root filesystem here. The published image still supports the fully
> hardened, non-root, read-only-rootfs deployment (only the state mount needs to
> be writable); see [Production container deployment](../../README.md#production-container-deployment).
