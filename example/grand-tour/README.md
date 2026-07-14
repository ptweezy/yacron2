# The cronstable Grand Tour — "Meridian"

**Every cronstable feature, at once, in one coherent deployment.**

Meridian is a fictional global commerce platform. Its entire scheduled
back-office runs on a **nine-node, mutual-TLS, leader-electing cronstable cluster**
(`distribution: spread`) that shares **one durable state store**. On top of that
fleet it runs, all together:

- a realistic **scheduled job set** (ingest → transform → load → report, plus
  monitoring and housekeeping),
- **durable-state** jobs (ETL cursors, exactly-once billing, a fleet mutex and a
  fleet semaphore, artifacts, run-scoped secrets, durable counters),
- five **orchestration DAGs**, one per pattern (dynamic fan-out ETL, linear
  pipeline, static diamond, and two human-gated releases),
- **second-level SLA probes**, and
- **all four failure reporters** wired to live sinks (Mailpit for mail, an echo
  server for webhooks, a statsd exporter for metrics, stdout for shell pages),
  plus **success** reports; Sentry is one env var away.

Each example is deliberately **small**: every job shows exactly one feature, so
you can read it top to bottom and know what it proves.

If you only run one example, run this one. For a gentler start see
[`example/demo`](../demo) (single node), [`example/dag`](../dag) (DAGs alone),
[`example/job-state`](../job-state) (state primitives alone), or the
[`acme-platform`](../acme-platform) cluster showcase this one extends.

## Run it

```console
docker compose -f example/grand-tour/docker-compose.yml up --build
```

The nine nodes build the image from this repo, so the example works before the
state/DAG features ship in a published release. Then open:

| What | Where |
| --- | --- |
| Node dashboards | <http://localhost:8080/> (meridian-a), …8081…8088 (b–i) |
| Prometheus metrics (per node) | <http://localhost:8080/metrics> |
| On-call inbox (Mailpit) | <http://localhost:8025/> |
| Job metrics (statsd → Prometheus) | <http://localhost:9102/metrics> |
| Webhook sink | `docker compose -f example/grand-tour/docker-compose.yml logs -f webhook-sink` |

Nine full nodes plus the sinks (and the second-level probes ticking every 2s on
each node) is a real load on a laptop. Trim nodes or probes if it runs hot.

Stop and wipe everything (including the throwaway certs and the state store):

```console
docker compose -f example/grand-tour/docker-compose.yml down -v
```

## How it all coordinates

There is no coordination service beyond the shared state volume:

- **Leader election** defaults to the `gossip` backend: each node serves a
  **mutual-TLS** `/peer` endpoint, attests that every peer holds the same
  [job-set id](../../README.md#job-set-id), and the quorum elects a leader.
  `distribution: spread` gives each `Leader`/`PreferLeader` job (and DAG) its
  own **owner** node via rendezvous hashing — watch the dashboard **Owner**
  column fan the pipeline across the fleet. (Or swap in the **filesystem**
  backend, which elects over a lease file on the shared mount with no certs and
  no `/peer` endpoint — see *Things to try*.)
- **Durable state** is a **shared** volume (`topology: shared`). Cursors,
  locks, idempotency keys, artifacts and **every `dag_run`** live on it and
  coordinate **fleet-wide**, so a task launches exactly once even though all
  nine nodes run the identical config. Each node's `cluster:` section is
  generated at start by `node-entrypoint.sh`; the job set (`platform.yaml`) is
  mounted identically into all nine.

## Feature map

Every `[feature: …]` tag in [`platform.yaml`](platform.yaml) points at the job
or task that demonstrates it. The big ones:

| Area | Feature | Where |
| --- | --- | --- |
| **Scheduling** | cron strings, object form (named fields), `@reboot`, `@daily`, `utc:false` | throughout |
| | **second-level** (object `second:` and 7-field string) | `pulse-liveness`, `pulse-latency` |
| | the trailing **year** field | `fiscal-year-open` |
| | timezones (New York / London / Tokyo) + DST advisory | `finance-eod-close`, `eu-/apac-open-report` |
| **Execution** | string vs **argv-list** command, `environment`, **`env_file`** | `orders-ingest`, `pulse-*`, `backup-warehouse` |
| | `streamPrefix` **templating** (`{job_name}`/`{stream_name}`) + empty prefix | `orders-ingest`, `sla-rollup` |
| | `captureStdout/Stderr` off, **`maxLineLength`** | `silent-cleanup`, `log-rotate` |
| | `executionTimeout` + `killTimeout` (job is killed) | `slow-report-generator` |
| | `user`/`group` privilege drop (config only) | `drop-privileges` comment |
| | `enabled: false`, `saveLimit` | `legacy-nightly-sync`, `audit-log-ship` |
| **Failure** | `failsWhen` on **stderr / stdout / always** | `config-lint` / `format-check` / `alert-selftest` |
| | `retry` w/ backoff (**finite** → `onPermanentFailure`) | `webhook-dispatch` |
| | `retry` **forever** (`maximumRetries: -1`, self-heals) | `outbox-flush` |
| | **mail** / **webhook** / **shell** / **sentry** reporters | `cert-expiry-check` / `db-health-orders` / `slow-report-generator` / `fraud-model-refresh` |
| | reporter on **success** (`onSuccess`) | `ops-heartbeat-report` |
| | mail **SMTP auth + HTML**; webhook **method/headers/contentType** | `cert-expiry-check`; `db-health-orders` |
| **Concurrency** | `Forbid` (node) vs `Replace` vs **`concurrencyScope: cluster`** | `warehouse-sync` / `search-reindex` / `inventory-sync` |
| | fleet **mutex** vs fleet **semaphore** (`lock --permits N`) | `compact-warehouse` / `render-thumbnails` |
| **Clustering** | mTLS attestation, quorum election, `distribution: spread` | whole cluster; `node-entrypoint.sh` + `gen-certs.sh` |
| | **filesystem** backend (shared-mount lease, no service) | `BACKEND=filesystem` |
| | `clusterPolicy` Leader / PreferLeader / EveryNode | throughout |
| **Durable state** | **cursor** watermark, default + explicit **`--scope`** | `incremental-orders-export`, `dedup-orders` |
| | **idempotent** exactly-once | `charge-subscriptions` |
| | **artifact** put/**get**/**list** + **secret** + **KV** (string + `--json`) | `build-daily-report` |
| | durable **counter** | `platform-pulse-counter` |
| | catch-up **run-once** vs **run-all** + `startingDeadlineSeconds` + `catchupJitterSeconds` | `daily-reconcile` / `hourly-invoice-emit` |
| | `onlyIfLastSucceeded`, `archiveOutput` + `redactArchivedSecrets` | `daily-reconcile`, `build-daily-report` |
| | `deploymentId` namespacing, `onStoreUnavailable`, `gcGraceSeconds` | `state:` block |
| **Orchestration** | DAG **dynamic fan-out (`expand`)** + XCom + retries + fan-in + catch-up | `orders-etl` |
| | DAG **linear pipeline** + timezone | `nightly-close` |
| | DAG **static diamond** (explicit parallel) + `triggerRule: all_success` | `data-quality-gate` |
| | DAG **approval gate** (`onReject: skip`) + **sensor** + manual trigger | `release-train` |
| | DAG approval **`onReject: fail`** (failure cascade) | `hotfix-release` |
| **Metrics/API** | **statsd** push, native **Prometheus** `/metrics` (map form: public + custom buckets), `web.headers` | `etl-build-facts`, `queue-depth-probe`; every node |
| | opt-in bearer **`authToken`** | `web.authToken` (see *Things to try*) |
| **Config** | **`include:`**, custom **`logging:`**, a **classic crontab** | `_defaults.yaml`, `legacy.crontab` |

## Self-driving dashboard demos (UTC, no clicks)

The failures are deterministic, driven by the wall-clock minute, so they fire on
their own:

| Time (UTC) | What happens | Feature |
| --- | --- | --- |
| **:05 & :35** | `slow-report-generator` is killed by its `executionTimeout` | timeout handling; shell report |
| **:15–:19** | the four `db-health-*` checks fail together (`exit 69`) | incident correlation; `db-health-orders` posts a **webhook** and **shell**-pages |
| **:25–:29** | `config-lint` "fails" only because it wrote to stderr | `failsWhen: producesStderr` |
| **:35–:39** | `format-check` "fails" only because it wrote to stdout | `failsWhen: producesStdout` |
| **:45–:47** | `cert-expiry-check` fails alone and **mails** on-call (SMTP auth + HTML) | mail report (see Mailpit) |
| **:20 & :40** | `alert-selftest` force-fails and posts a webhook | `failsWhen: always`; alert self-test |
| **:50** | `fraud-model-refresh` fails | Sentry (no-op until `SENTRY_DSN` set) + mail |
| every 5th min | `webhook-dispatch` fails, retries, then mails on permanent failure | finite retry + `onPermanentFailure` |
| every 5 min | `ops-heartbeat-report` succeeds and posts a **success** webhook + mail | `onSuccess` reporter |
| every 5 min | `outbox-flush` fails a few times, then self-heals (never permanent) | `retry: maximumRetries: -1` |
| every **:00** | four `hourly-*` reports collide | Schedule tab thundering-herd warning |
| continuous | `warehouse-sync` (node `Forbid`), `inventory-sync` (**cluster** `Forbid`), `search-reindex` (`Replace` → Cancelled), `render-thumbnails` (2-permit **semaphore**) | concurrency policies |
| every 3 min | the `orders-etl` DAG runs end to end | XCom + fan-out + fan-in |
| every 15 min | the `data-quality-gate` diamond certifies (even UTC hours) or skips `certify` (odd hours, a check fails) | `triggerRule: all_success` cascade |
| every few s | the `pulse-*` probes tick the next-run countdown **in seconds** | second-level scheduling |

## Things to try

1. **Watch the spread.** Open two dashboards side by side and compare each job's
   **Owner**. Stop a node
   (`docker compose -f example/grand-tour/docker-compose.yml stop meridian-a`)
   and watch its owned jobs and DAGs re-home to the survivors.
2. **Lose quorum.** Stop 5 of 9 nodes: `Leader` jobs stand down (dashboard shows
   *no quorum*), while `PreferLeader` jobs keep running.
3. **Compare single-leader vs spread.**
   `DISTRIBUTION=single-leader docker compose -f example/grand-tour/docker-compose.yml up -d`
   and watch every owned job collapse onto one node.
4. **Swap the leadership backend.** Bring the fleet up with the shared-mount
   lease instead of the gossip mesh — no certs, no `/peer` endpoint, election is
   one file on the state volume (single-leader only):

   ```console
   docker compose -f example/grand-tour/docker-compose.yml down
   BACKEND=filesystem docker compose -f example/grand-tour/docker-compose.yml up
   ```

   Stop the leader and watch a follower adopt the lease file within a TTL.
5. **Turn on API auth.** Uncomment `web.authToken` in
   [`platform.yaml`](platform.yaml), then:

   ```console
   WEB_TOKEN=s3cret docker compose -f example/grand-tour/docker-compose.yml up -d
   curl -i http://localhost:8080/status                       # 401 Unauthorized
   curl -H 'Authorization: Bearer s3cret' http://localhost:8080/status   # 200
   curl -i http://localhost:8080/metrics                      # still public (200)
   ```

   The `pulse-*` probes and the release DAG's canary already present `$WEB_TOKEN`,
   so they keep working.
6. **Drive the release DAG.** Trigger it, then approve the gate from **any**
   node (the decision is a compare-and-set on the shared run document):

   ```console
   curl -X POST http://localhost:8080/dags/release-train/trigger
   # -> {"dag":"release-train","runKey":"manual-…"}; then, with that runKey:
   curl -X POST \
     http://localhost:8080/dags/release-train/runs/<runKey>/tasks/approve/decision \
     -H 'Content-Type: application/json' -d '{"decision":"approve","by":"me"}'
   ```

   The `canary` **sensor** then polls the platform's own `/status` until it is
   healthy, and `publish` ships the tagged build. Trigger `hotfix-release` the
   same way and **reject** its gate to watch the opposite policy — `onReject:
   fail` cascades the failure to `deploy` instead of skipping it.
7. **Backfill a DAG.** Deliberately replay `orders-etl` over a past window; each
   scheduled instant in the range gets a run (idempotent — re-running the same
   window is a no-op):

   ```console
   curl -X POST http://localhost:8080/dags/orders-etl/backfill \
     -H 'Content-Type: application/json' \
     -d '{"from":"2026-07-05T09:00:00Z","to":"2026-07-05T09:30:00Z"}'
   ```

8. **Crash-resume a DAG.** While an `orders-etl` run is mid-flight, stop the node
   advancing it; another node adopts the run within a lease TTL and finishes it
   from durable state — no task double-launches.
9. **Prove durability.** Note `platform-pulse-counter`'s count, then
   `restart` the fleet — the counter continues from where it left off (it lives
   in the shared store, not memory).
10. **Inspect / back up the store** with the offline state-admin CLI (point `-c`
    at the config dir the entrypoint assembled inside any node):

    ```console
    dc="docker compose -f example/grand-tour/docker-compose.yml"
    $dc exec meridian-a cronstable -c /tmp/cronstable.d state check         # inventory: record counts per prefix
    $dc exec meridian-a cronstable -c /tmp/cronstable.d state gc --dry-run  # preview reclaimable state
    $dc exec meridian-a cronstable -c /tmp/cronstable.d state backup -o /tmp/meridian-state.tar.gz
    $dc exec meridian-a cronstable -c /tmp/cronstable.d -v                  # --validate-config: "Configuration is valid."
    ```

    `check`, `gc --dry-run` and `backup` are safe while the daemon runs;
    `restore <file>` and `migrate --dest <path>` expect a stopped fleet.
11. **Poke the control API.** `curl http://localhost:8080/status`,
    `/jobs`, `/cluster`, `/job-set-id`, `/metrics`, and
    `curl -X POST http://localhost:8080/jobs/run-schema-migration/start`.

## Alternate leadership backends (config only)

Beyond `gossip` and `filesystem`, cronstable can elect over **etcd** or a
**Kubernetes** Lease. They need an external service / control plane, so they are
not wired into this compose file, but the `cluster:` config is a one-liner swap.
etcd (HTTP-only, no extra Python deps):

```yaml
cluster:
  backend: etcd
  nodeName: meridian-a
  etcd:
    endpoints: [http://etcd:2379]
    electionName: cronstable/leader
    ttl: 15
```

Kubernetes (a `coordination.k8s.io` Lease; run one Deployment, no per-node peer
list):

```yaml
cluster:
  backend: kubernetes
  nodeName: meridian-a          # usually the pod name (downward API)
  kubernetes:
    leaseName: cronstable-leader
    leaseNamespace: meridian
    clientLibrary: http         # hand-rolled HTTP client, no `kubernetes` dep
```

See [`example/kubernetes`](../kubernetes) and
[`example/etcd`](../etcd) for full deployments.

## Files

| File | Purpose |
| --- | --- |
| [`docker-compose.yml`](docker-compose.yml) | the nine nodes plus the Mailpit / statsd / webhook sinks; its header comments list the things to try |
| [`platform.yaml`](platform.yaml) | the annotated job set + DAGs + `state:` + `web:` (mounted identically into all nine nodes) |
| [`_defaults.yaml`](_defaults.yaml) | shared `defaults:` + custom `logging:`, pulled in via `include:` |
| [`legacy.crontab`](legacy.crontab) | a classic Vixie crontab, loaded as-is from the same config dir |
| [`platform.env`](platform.env) | `env_file` for `backup-warehouse` |
| [`secrets/signing.key`](secrets/signing.key) | demo file for a `fromFile` run-scoped secret |
| [`gen-certs.sh`](gen-certs.sh) | mints the throwaway cluster CA + per-node leaf certs (gossip backend) |
| [`node-entrypoint.sh`](node-entrypoint.sh) | generates each node's `cluster:` section (gossip **or** filesystem) and assembles the config dir |

> **Note.** These nodes mount a writable state volume, so they do not run with a
> read-only root filesystem here. The published image still supports the fully
> hardened, non-root, read-only-rootfs deployment (only the state mount needs to
> be writable); see [Production container deployment](../../README.md#production-container-deployment).
