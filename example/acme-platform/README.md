# ACME Orders — a full-fledged cronstable showcase

A **five-node mutual-TLS cluster** (leader election + `distribution: spread`)
running a realistic mini **data & ops platform** (ingest → transform → load →
report, plus monitoring and housekeeping). It is deliberately built to exercise
**everything cronstable can do** and to make every
[web dashboard](../../wiki/Web-Dashboard.md) feature demo itself. Two small
sinks make the reporting features real: **Mailpit** (SMTP) catches the on-call
mail, and a **statsd exporter** receives job metrics.

## Run it

This is the deep end. For a gentler start, try a single node
(`example/demo` / `docker-compose.yml`), a plainer 3-node cluster
(`docker-compose-cluster.yml`), or a larger CPU-spread 10-node cluster
(`docker-compose-cluster-large.yml`).

```console
docker compose -f docker-compose-acme.yml up --build
```

- Node dashboards: <http://localhost:8080/> (cronstable-a), …8081–8084 (b–e)
- On-call inbox (Mailpit): <http://localhost:8025/>
- Native Prometheus metrics (per node): <http://localhost:8080/metrics>
- Job metrics (statsd, re-exported as Prometheus): <http://localhost:9102/metrics>

Stop and wipe (including the throwaway certs):

```console
docker compose -f docker-compose-acme.yml down -v
```

## cronstable features exercised

| Feature | Where |
| --- | --- |
| Leader election + `distribution: spread` (per-job owners) | whole cluster; watch the dashboard **Owner** column |
| Mutual-TLS peer attestation, job-set-id agreement | generated `cluster.yaml` + `gen-certs.sh` |
| `clusterPolicy`: Leader / PreferLeader / EveryNode | throughout |
| `concurrencyPolicy`: Allow / **Forbid** / **Replace** | `warehouse-sync` (Forbid), `search-reindex` (Replace) |
| `onFailure.retry` with exponential backoff | `webhook-dispatch` |
| Failure **mail** reports (→ Mailpit) | `cert-expiry-check`, `webhook-dispatch` (on permanent failure) |
| Failure **shell** reports (pages logged to stdout) | `db-health-orders`, `slow-report-generator`, others |
| `failsWhen` output-based failure (fails on any stderr) | `config-lint` |
| `executionTimeout` + `killTimeout` (job is killed) | `slow-report-generator` |
| `environment` (inline) and `env_file` | `orders-ingest`; `backup-warehouse` (`acme.env`) |
| `timezone` (New York / London / Tokyo) + `@daily` macro | reporting jobs |
| `captureStdout`/`captureStderr` off | `silent-cleanup` |
| `saveLimit` (short history) | `audit-log-ship` |
| `statsd` metrics | `etl-build-facts`, `queue-depth-probe` |
| Native Prometheus `/metrics` endpoint (on by default with the web dashboard) | every node, e.g. <http://localhost:8080/metrics> |

## Dashboard demos (self-driving, UTC)

The failures are deterministic and driven by the wall-clock minute, so they fire
on their own:

| Time (UTC) | What happens | Feature |
| --- | --- | --- |
| **:05 & :35** | `slow-report-generator` is killed by its `executionTimeout` | timeout handling; a distinct `exit≈137/143` signature |
| **:15–:19** | the four `db-health-*` checks fail together (`exit 69`) | **incident cockpit** verdict + correlation (`×4 share exit=69`); **mitigate** console; **timeline** (`i`); **wallboard alarm** (`w`, then `a`) |
| **:25–:29** | `config-lint` "fails" only because it wrote to stderr | `failsWhen: producesStderr` |
| **:45–:47** | `cert-expiry-check` fails alone and **mails on-call** | single-job verdict; mail report (see Mailpit) |
| every 5th min | `webhook-dispatch` fails, retries, then mails on permanent failure | retry/backoff + `onPermanentFailure` |
| every **:00** | four `hourly-*` reports collide in one minute | Schedule tab **thundering-herd** warning |
| continuous | `warehouse-sync` overruns its minute (Forbid); `search-reindex` is replaced each minute (Replace → **Cancelled**) | overlap warning + Running/Cancelled states |
| daily 02:30 | `finance-eod-close` in `America/New_York` | Schedule tab **DST** advisory |

The `db-health-*` and other monitoring checks are **EveryNode**, so the incident
shows on every node's dashboard; the pipeline/reporting jobs are Leader/
PreferLeader and fan out under spread.

## Things to try

1. **Watch the spread**: open two dashboards side by side and compare each job's
   Owner; stop a node (`docker compose -f docker-compose-acme.yml stop cronstable-a`)
   and watch its owned jobs re-home to survivors.
2. **Lose quorum**: stop 3 of the 5 nodes; Leader jobs stand down (dashboard
   shows *no quorum*), while PreferLeader jobs keep running.
3. **Enable the Run Ledger** (Settings) and watch `etl-build-facts` earn a
   `slow` anomaly chip on its every-6th-minute spike.
4. **Open Mailpit** at :8025 during the `:45` window to see the on-call mail.
5. Compare `single-leader` vs `spread` (see the env override in the compose
   header).
