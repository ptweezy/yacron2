# History

cronstable is a fork of [yacron](https://github.com/gjcarneiro/yacron),
continuing from yacron 0.19.  The 1.0.x entries below document the fork; the
entries from 0.19.0 onward document the history of the original yacron
project, on which cronstable is based.

## 1.2.15 (2026-07-14)

A maintenance release: no functional changes to cronstable itself -- the
package, the CLI, and every shipped binary behave exactly as in 1.2.14.  It
consolidates the project's separate CI workflows into one gated pipeline,
hardens the release automation and updates the contributor
docs to match.

- **One CI/CD pipeline.** The former `build.yml`, `docker.yml`, and `tox.yml`
  are folded into a single `release.yml` that builds and tests the whole product
  on every push and pull request -- the tox lint/mypy/pytest matrix, the wheel +
  sdist, every self-contained binary (Linux glibc and musl across the full arch
  set, macOS arm64/amd64, Windows amd64/arm64), and all eight Docker images --
  and gates a release on all of it.  Nothing publishes on an ordinary commit; a
  release still ships the PyPI upload, the GitHub Release with every binary and a
  single `SHA256SUMS`, the container images, and the Homebrew tap update, but
  only after the entire build + test matrix is green.

- **Reliable release publishing.** The consolidated pipeline attaches every
  release asset in one shot as the GitHub Release is created (no separate attach
  step that could fail against an already-published, immutable release), and the
  Homebrew tap update is no longer best-effort -- if the tap cannot be updated
  (for example, a missing token) the release now fails loudly instead of
  finishing green with a stale formula.

- **Hardened release trigger.** A `1.3.0` was published in error before this
  release: the trigger substring-matched whole commit messages, so a commit
  *body* that merely discussed the bare `[release]` marker out-bumped the
  intended `[release:patch]`. `1.3.0` is withdrawn (yanked on PyPI, its GitHub
  release and container tags removed) and contains exactly what ships here as
  `1.2.15`. The trigger now scans only commit subject lines, only honors a
  marker at the very start of the subject, and when several commits carry one,
  the latest commit's marker wins.

- **Contributor docs.** `CONTRIBUTING.md` and the "Contributing and Releasing"
  wiki page are rewritten for the single-pipeline flow.

## 1.2.13 (2026-07-08)

cronstable becomes drivable by an AI agent.  A new, opt-in **MCP server**
exposes the scheduler over the [Model Context
Protocol](https://modelcontextprotocol.io), so Claude, Cursor, VS Code
Copilot, or any MCP client can **observe** every job, DAG, the cluster/fleet,
metrics, and the durable state store the way an operator reads the dashboard
-- and, when you opt in, **act** (run or cancel a job, trigger / backfill /
approve a DAG).  It is read-only by default, served two ways from one
implementation -- a `POST /mcp` endpoint on the existing web listeners and a
`cronstable mcp` stdio bridge for desktop clients -- and, like the rest of
cronstable, hand-rolled in pure Python with **no new dependencies**.  It stays
off unless an `mcp:` section sets `enabled: true`, so a plain install pays
nothing and behavior is unchanged without it.

- **The `mcp:` section and its two transports.** Configuration lives under a
  new optional top-level `mcp:` block that rides the `web:` listeners -- a
  `web` section is required, because there is nowhere else to serve it.
  `enabled: true` turns on a **stateless Streamable-HTTP** JSON-RPC 2.0
  endpoint at `POST /mcp`, pinned to MCP revision `2025-11-25` (no
  `Mcp-Session-Id`; `GET /mcp` is `405`), and exposes the `cronstable mcp`
  **stdio bridge** that desktop clients launch as a subprocess.  Both paths
  run the same server code.

- **Tools, grouped into opt-in toolsets.** The default `observe` toolset is
  read-only -- status, jobs, per-job runs / trends / resources, cluster,
  fleet, node load, a metrics query, version, and live log tails (twelve
  tools).  `dags` adds DAG, run, and XCom reads plus task-log tails; `state`
  adds a redacted durable-state inspector; `act` adds mutating job control
  (`cron_run_job`, `cron_cancel_job`), and `dags` gains DAG control
  (`cron_trigger_dag`, `cron_backfill_dag`, `cron_decide_gate`) once writes are
  enabled -- 23 tools with every toolset on and `readOnly: false`.  Mutating
  tools require an explicit `confirm: true`, carry honest `destructiveHint`
  annotations, re-check the same authorization as the REST route, and
  `cron_backfill_dag` previews as a dry run unless it is called with both
  `dry_run: false` **and** `confirm: true`.

- **Resources and prompts, both read-only and on by default.** Resources are
  URI-addressable read-only snapshots a client can attach as context --
  `cronstable://status`, `cronstable://cluster`, `cronstable://fleet`,
  `cronstable://version`, and templates for jobs, runs, DAGs, and state
  namespaces -- and every critical read is *also* a tool, because client
  support for resources is uneven.  Prompts are canned triage playbooks that
  chain the read tools: `triage_job_failure`, `why_did_dag_run_fail`,
  `blast_radius`, `fleet_health_summary`, and `backfill_plan`.  Both are scoped
  by the enabled toolsets (a DAG resource appears only with the `dags` toolset)
  and can be turned off (`resources: false`, `prompts: false`) for a
  tools-only client.

- **Safe by default.** `readOnly: true` is the default and strips every
  mutating tool regardless of toolset, so an agent gets look-but-don't-touch
  until you opt in.  `/mcp` inherits `web.authToken` exactly like the data
  routes and is never in the public set; enable it with no token on a routable
  (non-loopback, non-socket) listener and cronstable **fails closed at config
  load** (without a token the web app installs no auth middleware at all, so
  `/mcp` would be wide open) -- restrict `web.listen` to loopback/sockets, set
  a token, or set `mcp.allowUnauthenticated: true` when a proxy terminates
  auth.  A present, non-allow-listed `Origin` is refused `403` (a DNS-rebinding
  defense; browser clients go on `mcp.allowedOrigins`), an oversized body is
  refused `413` (`maxBodyBytes`, 1 MiB default), and any list tool's `limit` is
  capped at `maxRows` (200) with an opaque cursor for the remainder.
  `cron_inspect_state` mirrors the dashboard's metadata-only stance: KV values
  collapse to a size/type summary and secret *names* appear without their
  values.

- **One source of truth for REST and MCP.** The web layer's read handlers are
  refactored so each endpoint's data is produced by a reusable `*_payload`
  method -- `status_payload`, `jobs_payload`, `cluster_payload`,
  `fleet_payload`, `node_payload`, the per-job runs / resources / trends
  projections, `dags_payload`, the `state_*_payload` family, and new
  poll/cursor log-tail projections -- and both the REST routes and the MCP
  tools call those same methods, and the same `start_job_by_name` /
  `cancel_job_by_name` action paths, so `GET /jobs` and `cron_list_jobs` can
  never drift apart.  The web app now also rebuilds when only the `mcp` config
  changes, so flipping `readOnly` or adding a toolset takes effect on the next
  reload.

- **The `cronstable mcp` stdio bridge.** A featherweight stdlib client --
  imported lazily so it never pulls the daemon graph into CLI start-up --
  reads newline-delimited JSON-RPC on stdin and forwards each frame to a
  running daemon's `/mcp`, writing replies to stdout and logs to stderr.
  `--url`, `--token` / `--token-env` (defaulting to the `CRONSTABLE_WEB_TOKEN`
  env var), and a `--check` handshake that runs `initialize` + `tools/list`,
  prints the negotiated protocol and tool count, and exits.  It needs a
  reachable running daemon -- the right model for an ops tool.

- **Docs, an example, and a rebuilt README.** New wiki pages (MCP and
  MCP-Server-Design), an `mcp` row and full option table on
  Configuration-Reference, and a `POST /mcp` section on HTTP-API.
  `example/mcp` (with `docker-compose-mcp.yml`) boots a single node with the
  server on and every toolset enabled -- a steady `heartbeat`, an intentionally
  failing `flaky-export`, a long `slow-report`, and an on-demand
  `on-demand-sync` -- and walks through wiring Claude Code / Desktop, Cursor,
  and VS Code to it.  The README now leads with an animated dashboard reel
  (every frame a real running fleet, WebP with a GIF fallback) and a
  ten-theme / two-font showcase, adds a written tour of the dashboard's
  accessibility options (interface font, UI scale, color-vision-safe palettes,
  reduced motion), lists the MCP server among the features, moves the yacron
  fork attribution to the foot of the page, and -- at last -- says how to
  pronounce the name: *kraahn-stuh-bl, like constable*.

- Internal: the MCP server ships with a dedicated `tests/test_mcp.py` suite --
  the `initialize`/capability handshake, toolset and `readOnly` gating, the
  `confirm` and dry-run write guards, `maxRows` clamping and pagination, the
  fail-closed no-token check and the Origin / body-size / batching HTTP
  defenses, resource and prompt scoping, and the featherweight-bridge import
  cost -- and the release workflow's finalize job, which has no
  `actions/checkout` (so `gh release download/upload` could not infer the
  repository and died with "not a git repository"), now sets `GH_REPO`
  explicitly.

## 1.2.12 (2026-07-08)

The package finishes becoming **cronstable**.  Everything that was still
named `yacron2` -- the import package, the `python -m` entry point, the
console-script command, and the PyPI distribution -- is now `cronstable`, so
the name is consistent all the way from `pip install` through `import` to the
CLI.  The fork has shipped as cronstable in its repo and docs for a while;
this release moves the code's own identifiers to match, so nothing user-facing
still answers to the old name.

- **The `yacron2` package is renamed to `cronstable`.** The source tree moves
  from `yacron2/` to `cronstable/` and every intra-package import follows, so
  `import yacron2...` becomes `import cronstable...` and `python -m yacron2`
  becomes `python -m cronstable`.  The web assets, backends, and cluster
  modules move with it; no module contents change, only their home.

- **The CLI command and PyPI distribution are renamed.** The console script the
  wheel installs is now `cronstable` (it was `yacron2`), and the project
  publishes to PyPI as `cronstable` -- `pip install cronstable`.  The repo,
  Docker image, and container registry are `ptweezy/cronstable`.  Update any
  scripts, service/unit files, or `pip`/import references that still say
  `yacron2`; there is no compatibility shim, so the old names stop resolving.

- **Release-pipeline hardening.** The release workflow now pushes the version
  tag with a scoped `RELEASE_TOKEN` rather than the default `GITHUB_TOKEN` --
  GitHub refuses to let the Actions app push a tag whose commit touches
  `.github/workflows/` without the `workflows` scope it cannot be granted -- and
  the PyPI publish runs with `skip-existing`, so a release interrupted after the
  upload (before the tag and GitHub Release) can be retried without burning the
  version.

## 1.2.10 (2026-07-07)

cronstable now parses cron expressions itself.  This release retires the
third-party `crontab` (parse-crontab) library in favor of a small,
stdlib-only engine that lives in the tree -- so the scheduler owns the one
piece of syntax every job depends on, the dialect is documented and tested
where it is implemented, and the install carries one dependency fewer.
Compatibility is not aspirational: it is pinned by golden vectors recorded
from the old library across 180+ expressions and fixed instants (DST
transitions, leap days, month/year boundaries, the year cap, ambiguous
fall-back folds).  Nothing changes for existing configs -- the dialect, the
timezone model, and the strictly-future fire semantics are all preserved
vector-by-vector.

- **`cronstable/cronexpr.py`: the built-in cron engine.** A new `CronTab` class
  parses the crontab dialect cronstable has always accepted (5/6/7 fields,
  ranges, steps including bare-start `5/15`, lists, case-insensitive
  `jan`/`mon` names, `0`-`7` weekdays with `6-0` wrap, `L`-last-day and
  `L5`-last-Friday forms, `@`-nicknames) and answers the scheduler's two
  questions: `next()` (strictly future, DST-correct across UTC-offset
  changes, capped at the 2099 horizon so dead schedules drop from the index)
  and `test()`.  A stdlib-only leaf module -- `calendar` and `datetime`, no
  third-party imports.

- **The `crontab` dependency is dropped.** Removed from `pyproject.toml` and
  every import site (`config.py`, `cron.py`, `crontabs.py`, `dagrun.py`,
  `prometheus.py`); classic crontab-file loading and YAML `schedule` strings
  now share the same in-house engine, so both formats still accept identical
  expressions.

- **Golden-vector compatibility harness.** `tests/gen_cron_golden.py`
  records `next()`/`test()` answers from the original parse-crontab library
  into `tests/data/cron_golden.json`; `tests/test_cronexpr.py` replays them
  against the new engine so any behavioral drift fails the suite.  This is
  the proof that "behavior-compatible" is a fact, not a hope.

- **`mergedicts` reimplemented.** The config defaults-merge helper is
  rewritten with the identical semantics -- dicts merge recursively, an empty
  YAML section (`None`) never wipes a populated default, `environment` and
  `secrets` lists merge by key/name, sentry `fingerprint` replaces rather
  than appends, and all other lists concatenate -- and now returns a `dict`
  directly, retiring the `dict(mergedicts(...))` wrappers at every call site.

- **Dashboard accessibility pass.** The web UI gains an **interface font**
  toggle (the terminal monospace, or a proportional sans "reader mode" for
  easier reading, with logs and cron strings kept monospace either way),
  app-level **color-vision** palettes (deutan / tritan status-color remaps,
  status glyphs always distinct too), whole-UI **zoom**, and a
  **reduce-motion** switch, each reachable from Settings and the command
  palette and persisted across sessions.

- **Docs.** The "Schedules and Timezones" wiki page now documents the full
  dialect the engine owns -- the day-of-month-AND-day-of-week rule (Friday
  the 13th, deliberately kept over Vixie's OR), the `L` forms, the
  1970-2099 year horizon -- and Installation, Migration-from-yacron,
  Classic-Crontabs, Architecture-and-Internals, and the README are updated to
  point at the built-in engine instead of the removed library.

## 1.2.9 (2026-07-07)

Resource monitoring grows a time axis. 1.2.8 answered "what did that run
use?" with two numbers per run; this release records **how the run used it**
-- a per-run CPU/memory chart series with a user-tunable sampling cadence --
and puts proper charts in the dashboard: a live view of the running
instance, the recorded profile of any recent run, per-run trend strips, and
a whole-node history chart behind the header meter. Everything rides the
existing HTTP+JSON surface and the durable run ledger; nothing changes for
existing configs (`monitorResources: true` still means what it meant, with
the same 1s cadence).

- **`monitorResources` map form.** Alongside the bool, the option now takes
  a map: `enabled` (default true -- writing the map at all opts in),
  `interval` (seconds between process-tree samples, default `1.0`, floor
  `0.1`), and `history` (chart points kept per run, default `240`, `0` for
  summary-only, ceiling `2000`). Validated at load time with the other
  numeric ranges, merged normally under `defaults:`, and accepted by DAG
  tasks. Not fingerprinted, like the bool before it.

- **Per-run chart series.** Each monitored run now records a
  `[t, cpu%, rss]` point per sample, downsampled in place once it exceeds
  the `history` cap: adjacent buckets merge with *mean* CPU but *peak* RSS,
  so the memory spikes people monitor for survive downsampling, and a run
  of any length stays a few KB with uniform bucket widths. The series is
  embedded in the durable run record's `resources.series` -- charts survive
  restarts and are bounded by the existing `state.maxRunsPerJob` pruning --
  and is deliberately **excluded** from the polled `/jobs` and
  `/jobs/{name}/runs` payloads, which keep their summary-only shape.

- **`GET /jobs/{name}/resources`.** A lazy, chart-grade endpoint: the
  run-so-far series of every currently-running monitored instance (plus its
  live instantaneous readings) and the recorded series of recent finished
  monitored runs, capped by a `runs` query parameter. `monitored: false`
  with empty lists distinguishes "never opted in" from "no data yet".

- **`GET /node/history` + `web.nodeHistory`.** A background sampler records
  the node's CPU/memory (the same cgroup-aware percentages `GET /node`
  reports) into an in-memory ring -- every 5s, keeping the last hour, by
  default. It follows the web app's lifecycle, is on whenever the web API
  is on, and is tuned or disabled via `web.nodeHistory`
  (`interval`/`points`, or `false`). A gap wider than the cadence in the
  returned points means the daemon was down, not idle.

- **Dashboard: the Resources tab and the node card.** The job drawer gains
  a **Resources** tab: CPU and memory drawn as separate small-multiple
  charts (one honest 0-based axis each, never dual-axis) with a synced
  crosshair and tooltip, chips to flip between the live instance and recent
  runs, a refresh-cadence selector for the live view (1s-10s, persisted),
  and clickable per-run CPU-time / peak-memory trend strips. Clicking the
  header node meter opens a **node resources** card charting the retained
  node history. Chart inks are the theme-aware blue/amber pair
  (colorblind-safe and contrast-checked against every theme surface, light
  and dark), gaps in sampling break the line rather than lying across it,
  and unmonitored jobs get a pointer at the config instead of an empty
  chart.

- **Examples.** The grand tour (`_defaults.yaml` + `platform.yaml`) and the
  large-cluster demo now use the map forms -- a 0.5s sampling cadence with
  the default 240-point series, and a 2s x 1800-point node history ring --
  so their resource-heavy jobs light the new charts up out of the box.

## 1.2.8 (2026-07-07)

This release answers "what is this actually *using*?" Opt-in per-job
**resource monitoring** records every run's CPU time and peak memory and
carries the numbers everywhere a run already reports -- the dashboard, the
HTTP API, Prometheus, statsd, and failure reports -- while a new `GET /node`
endpoint and a `cluster.observability` block put live whole-node load beside
the jobs, on every node in the fleet. One footprint note: **psutil joins the
core dependencies**, the fork's first addition to the core install (it ships
wheels for the mainstream targets and builds from source elsewhere).
Behavior is unchanged without the new config: `monitorResources` is off by
default and the observability overlay is opt-in.

- **Per-job resource monitoring (`monitorResources: true`).** A
  psutil-backed sampler polls the run's whole process tree and records its
  total CPU time (user and system) and its sampled peak resident memory.
  Accounting is best-effort by design: a process that exits mid-sample, a
  platform that denies the read, or psutil failing outright simply yields
  whatever was captured so far -- monitoring never crashes a job, never
  delays it, and never changes its success/failure verdict. Peak RSS is a
  sampled high-water mark and per-member CPU is banked as the tree shrinks,
  so the long, heavy runs that matter are measured well; only a child that
  spawns *and* exits within a single sampling gap escapes entirely.

- **The numbers surface everywhere a run does.** The dashboard overview
  shows live CPU/memory chips on a running job, and the history tab adds
  per-run CPU and peak-memory columns and stats. The HTTP API carries
  `resources` on each run in the history, live `running_resources` on a
  running job, and windowed CPU/RSS aggregates in the job stats. Prometheus
  grows `cronstable_job_cpu_seconds_total{job_name, mode}` (user/system),
  `cronstable_job_peak_rss_bytes`, and last-run CPU/RSS gauges -- emitted only
  once a job has a monitored run, and persisted across restarts by the
  durable metrics snapshot. A monitored run's statsd stop datagram gains a
  `cpu` timer and a `max_rss` gauge (an unmonitored job's datagram is
  unchanged), and failure reports get `cpu_seconds` / `max_rss_bytes` (and
  friends) plus `CRONSTABLE_CPU_SECONDS` / `CRONSTABLE_MAX_RSS_BYTES` template
  variables.

- **`GET /node`: the node's own live load.** A new endpoint samples the
  serving host's CPU and memory fresh per request -- whole-host utilisation
  plus the daemon's own footprint -- and drives a node meter in the
  dashboard header. It is container-aware: under a cgroup v2 limit
  (Docker/Kubernetes limits, systemd slices) the numbers describe the
  daemon's *slice* -- the effective memory limit with reclaimable page cache
  excluded (the same accounting `docker stats` shows) and utilisation of the
  CPU quota -- with memory and CPU switching over independently. Unlimited
  cgroups, cgroup v1 hosts, and non-Linux platforms report whole-host
  numbers, and the response shape never changes.

- **`cluster.observability`: gossip as a secondary data plane.** Opt in and
  every node shares its whole-node CPU/memory across the cluster. Under
  `backend: gossip` the reading rides the election mesh as a small
  `X-Cronstable-Node-Stats` response header on full and `304` responses alike,
  so a sharing cluster's steady-state round still costs headers only. The
  lease backends (`kubernetes`/`etcd`/`filesystem`), which have no
  node-to-node channel of their own, can stand up a *second,
  election-inert* gossip mesh purely for observability data -- which also
  brings the fleet view to lease-backed clusters. The dashboard's cluster
  panel gains per-peer load meters and the fleet view puts each node's live
  load in its column header. Like the run summaries, node stats are
  best-effort display data: a malformed peer payload degrades to "no data",
  never poisoning the view or any decision.

- **Dashboard themes.** A new `carolina-light` theme joins the palette, and
  `carolina` replaces `amber` as the default.

- **Docs and examples.** The README is rebuilt around a sixty-second quick
  start, four tutorials (alerting and retries, durable restarts, a first
  DAG, two-replica leader election), and a screenshot tour of the
  dashboard; the screenshots themselves are now reproducible via a scripted
  pipeline under `docs/screenshots/` that captures a live grand-tour fleet.
  The grand tour gains resource-monitored CPU- and memory-heavy demo jobs
  and the observability overlay, and the new features are documented on the
  wiki's Configuration-Reference, HTTP-API, Clustering, Metrics, and
  Reporting pages.

## 1.2.7 (2026-07-06)

This release makes cronstable **stateful**. An opt-in durable state store lets
the scheduler remember across restarts -- retries that survive a daemon
restart, `@reboot` that really means once per boot, Prometheus counters that
do not reset -- and turns a shared directory into fleet-wide coordination:
cluster-scoped concurrency, cross-node retry takeover, and leader election
with nothing but a mount both nodes can reach. On top of the store sit a
state API handed to every job (key-value, cursors, fleet locks, idempotency
claims, artifacts, run-scoped secrets) and durable **DAG orchestration**
(dependencies, XCom, fan-out, sensors, approval gates, backfills) with
crash-resume. All of it is opt-in: without a `state:` block (and a `dags:`
block for pipelines) nothing changes -- no new behavior, no new files on
disk, and the zero-new-dependency, architecture-portable core install is
untouched.

- **A durable state store behind a single `state:` block.** `state.path`
  names a directory -- a local disk or a shared NFS/EFS-style mount -- and
  the daemon keeps everything under `<path>/<deploymentId>`: append-only
  JSON record streams, mutable documents, content-addressed blobs, and
  `flock`-guarded TTL leases with monotonic fence counters. The write
  discipline is crash-safe on POSIX and Windows alike (atomic
  temp-plus-rename with directory fsyncs; a record that cannot be parsed is
  quarantined, never trusted and never fatal), files are owner-only
  (`0o700`/`0o600` -- archived job output is exactly where secrets live),
  and archived output additionally passes through a conservative
  best-effort secret redactor before it is written. A store outage degrades
  the stateful features, never scheduling: durable writes are
  fire-and-forget, reads on scheduling paths are bounded and fall back,
  store calls run on abandonable worker threads so a hung hard mount cannot
  wedge the daemon or its shutdown, and `state.maxOpsPerSecond` throttles
  everything except lease renewals, which must never queue behind bulk
  work. The full model is documented on the wiki's Durable-State page.

- **Restart-surviving scheduling.** With a store configured, a pending
  retry re-arms after a daemon restart instead of vanishing; `@reboot`
  distinguishes a real boot from a mere restart (and, under election, runs
  once per fleet); Prometheus counters persist across restarts; the run
  ledger, optionally archived output (`archiveOutput`), and catch-up
  checkpoints are durable; and in-flight run records let a restarted daemon
  settle the runs that died with it, so failure handlers and retries fire
  for work a crash orphaned.

- **Garbage collection that can prove absence.** Every node periodically
  writes a manifest of the jobs, scopes, and dags it carries; GC deletes a
  stream only when no recent manifest references it *and* its newest record
  is older than `state.gcGraceSeconds` (default seven days), and it defers
  wholesale until the retained manifest history spans a full grace window
  -- "nobody has manifested yet" never reads as "nobody wants this".
  Artifact streams and payload blobs age out with their scope, run
  documents of removed dags are collected by the daemon that owned them,
  and of the lease files only the per-run DAG advance class is ever
  reclaimed: every other lease carries fences that persist in durable
  records, so it is never deleted at any age. A node that is merely down
  loses nothing.

- **Fleet HA through a shared directory.** A new `cluster.backend:
  filesystem` runs leader election over the same flock-and-lease machinery
  -- no gossip ports, no Kubernetes, no etcd -- and composes with
  everything clustering shipped in 1.2.1. `concurrencyScope: cluster` makes
  `concurrencyPolicy: Forbid`/`Replace` hold fleet-wide through per-job
  slot leases (a `Replace` fired anywhere cancels the run wherever it
  lives; a crashed holder's slot frees by TTL), a pending retry left by a
  dead node can be claimed and resumed by a survivor -- serialized on a
  claim lease and re-checked under it; the contract is at-least-once,
  honestly -- and `@reboot` under election survives leader failover in the
  safe direction: a takeover can delay a one-shot, never double-run it.

- **Every job gets a state API.** With `state.jobApi` (on by default once a
  store is configured) the daemon serves a loopback-only HTTP endpoint and
  injects its address and a per-run bearer token into each job's
  environment; the `cronstable` binary doubles as the client. `cronstable state
  get/set/delete/keys` is durable KV; `cronstable cursor` keeps resumable
  positions; `cronstable lock` gives fleet-wide mutexes and semaphores backed
  by the same TTL leases the cluster uses, with fencing tokens and a
  `lock run --` wrapper; `cronstable idempotent` makes run-once guards honest
  (exit `0` fresh, `5` duplicate, `1` transport or store error); `cronstable
  artifact` stores content-addressed payloads under configurable size caps.
  A job's `secrets:` block stages secrets over the endpoint for exactly one
  run -- resolved fresh, served only to that run, never in the environment
  and never in the durable store -- read back with `cronstable secret get`.
  Scopes default to the job's own name; `stateAllowedScopes` opens shared
  ones.

- **DAG orchestration.** A new `dags:` section defines multi-step pipelines
  on the job grammar: tasks with `dependsOn` and per-task retries, XCom
  hand-off between tasks (`cronstable xcom push/pull`), mapped fan-out over a
  pushed list (capped, launched in bounded batches), sensors that poke on
  an interval, and approval gates a human resolves from the dashboard or
  API. Dags run on cron schedules (the job schedule grammar, minus
  `@reboot`), manual triggers, and date-range backfills. A per-run advance
  lease makes exactly one node drive each run; when a driver dies, the
  lease lapses and a peer adopts the run mid-flight, reconciling exactly
  what was and was not still running. Run history is retained per-dag and
  collected under the same grace rules. See the wiki's
  Orchestration-and-DAGs page.

- **Dashboard and HTTP API.** The dashboard gains DAG cards with a run
  drawer and task graph (trigger, backfill, and approve/reject inline),
  cluster/HA chips, per-job durable run history, and a metadata-only state
  inspector -- streams, documents, leases, and blob inventory by name and
  size, never values. The HTTP API adds the matching `/dags/...` routes
  (runs, XCom, live task logs) and `/state` inventory routes, and the
  Prometheus endpoint grows state-store and DAG metric families. All routes
  are documented on the wiki's HTTP-API page.

- **Store administration.** `cronstable state backup` writes an owner-only
  `.tar.gz` of the whole store, safe against a live daemon; `state restore`
  merges it back atomically (fence-aware, refuses a non-empty store without
  `--force`, and is not safe while a daemon uses the store); `state
  migrate` copies a store across paths or mounts without a reader ever
  seeing a torn record; `state gc [--dry-run]` runs or previews a
  collection pass; `state check` verifies the store is usable and prints an
  inventory; `state migrate-schema` rewrites records of older known
  schemes.

- **Packaging and examples.** orjson joins uvloop in the `speedups` extra,
  accelerating the durable-state and cluster-gossip JSON paths through
  `cronstable._json`, whose stdlib fallback is behavior-identical -- the core
  install stays zero-new-dependency and architecture-portable, and the
  prebuilt binaries bundle orjson wherever a real wheel or verified source
  build exists, with a verify-or-strip step mirroring uvloop's. New
  examples: `example/job-state` (the CLI primitives), `example/dag` and
  `example/dag-cluster` (pipelines, single-node and fleet), and
  `example/grand-tour` (a docker-compose fleet exercising the whole
  feature set end to end).

## 1.2.6 (2026-07-03)

A toolchain and packaging release. There are **no behavior, API, or
configuration changes** and **no change to the core dependency footprint**: the
published wheel/sdist and the release binaries are built from the same sources
as before and install exactly the same runtime dependencies. The work adopts
[uv](https://docs.astral.sh/uv/) across the paths where it pays off -- CI, the
build/release pipeline, and the local dev loop -- refreshes the container base
images, and pins the CI action versions.

- **uv on the runner-native CI, build, and dev paths.** The dist build
  (`uv build`), the runner-native PyInstaller binaries (macOS, Windows, and the
  all-wheels Linux arches, each installed into a throwaway `uv venv` and frozen
  with `uv run`), the version probe (`uv run --no-project --with
  setuptools-scm`), and `twine check` (`uvx twine`) all run through uv now:
  parallel downloads and a shared global wheel cache make them markedly faster,
  and the results are behavior-identical. `UV_PYTHON_DOWNLOADS=never` keeps uv on
  the exact interpreter `setup-python` pinned rather than fetching a managed one,
  and `UV_HTTP_TIMEOUT` carries the same transient-network hardening the pip
  paths had.

- **The emulated foreign-arch binary legs stay on pip, on purpose.** The
  musl and glibc-extra binary jobs (armv7/armv6, ppc64le, s390x, riscv64, i686)
  build inside `docker run` containers under QEMU, where uv is not a fit: its
  official image is amd64/arm64 only and it has no musl ppc64le/s390x wheels, so
  pip remains the arch-portable choice there and keeps `PIP_RETRIES`/`PIP_TIMEOUT`
  hardening. The uvloop bundling and per-arch `--version` smoke test are
  unchanged on every leg.

- **uv in the local dev loop.** `tox.ini` now declares `requires = tox-uv`, so a
  plain `tox` auto-provisions its environments and installs dependencies with uv
  (much faster, behavior-identical); `tox-uv` is added to the `dev` extra and
  `requirements_dev.txt`. `CONTRIBUTING.md` documents the uv quickstart
  (`uv venv`, `uv pip install -e ".[dev]"`) alongside the unchanged stock
  `venv`+`pip` path, and notes the `tox --runner virtualenv` escape hatch for
  anyone who wants the legacy runner.

- **Refreshed container base images.** The Docker variant matrix moves to
  current bases: `ubuntu` 24.04 -> 26.04 (Python 3.12 -> 3.14), `rhel` UBI9 ->
  UBI10, `fedora` 41 -> 44 (3.13 -> 3.14), `opensuse` Leap 15.6 -> 16.0 (3.11 ->
  3.13), and `distroless` to Python 3.13. The Debian/Alpine images and every
  image tag stay as they were.

- Internal: every CI-consumed action is pinned to an exact version
  (`checkout@v7.0.0`, `setup-python@v6.3.0`, `setup-uv@v8.2.0`, the docker/*
  actions, etc.), and a `dependabot.yml` is added to keep those pins and the
  Python dev dependencies current.

## 1.2.5 (2026-07-03)

A performance and footprint release. There are **no behavior or configuration
changes**: every schedule fires exactly as before, the metrics endpoint renders
byte-for-byte identically, and the core install stays zero-new-dependency. The
work trims CPU on the daemon's hottest repeating paths -- the once-a-minute
config reload, every Prometheus scrape, and each cluster poll / gossip round /
lease renew -- lowers steady-state memory, and adds an optional faster event
loop.

- **Optional uvloop event loop (`speedups` extra).** `pip install
  cronstable[speedups]` swaps asyncio's selector loop for uvloop's faster
  libuv-based one, speeding every I/O path cronstable drives: cluster gossip and
  lease HTTP, the web dashboard, and the Prometheus scrape. It is entirely
  opt-in and best-effort -- `__main__` selects uvloop lazily on POSIX and falls
  back to stock asyncio, behavior unchanged, whenever it is absent or
  unimportable -- so it stays off the core install to keep the baseline
  architecture-portable. Windows always uses its Proactor loop (there is no
  uvloop build there, and the Proactor loop is required for subprocess support
  anyway). The prebuilt POSIX binaries now bundle uvloop wherever it builds: a
  wheel where one exists, an otherwise verified source build, with a start-up
  self-test (`verify_uvloop.py`) that uninstalls a miscompiled build (a real
  risk under QEMU emulation) before freezing so the binary cleanly runs on
  asyncio instead. An arch where uvloop cannot build ships the asyncio binary
  exactly as before.

- **The once-a-minute reload no longer reparses an unchanged config.** The
  scheduler rereads and reparses the config every minute so an on-disk edit is
  picked up promptly, but strictyaml is a slow pure-Python parser and reparsing
  an unchanged file was pure wasted work (in a worker thread, but still real CPU
  plus thread-pool churn). `reload_config` now compares a cheap `os.stat`
  fingerprint -- `(path, mtime_ns, size)` per file, plus the config directory's
  own mtime -- of exactly the files the last parse read (the top-level config,
  every transitively `include`d file, and each job's `env_file`) and skips the
  reparse entirely when nothing has changed, returning the already-loaded
  config. A genuine edit, a vanished file, or a new entry dropped into a config
  directory still reparses on the next pass.

- **Cheaper Prometheus scrapes.** The job-set fingerprint (`job_set_id`, queried
  on every scrape and every cluster poll / gossip round / lease renew) is a pure
  function of the loaded jobs, so it is now computed once per reload and
  memoized rather than re-deriving its per-job deepcopy / JSON / SHA-256 each
  time. The `job_next_run_timestamp` gauge reads the scheduler's authoritative
  next-fire index instead of re-walking every crontab and building two aware
  datetimes per job per scrape (falling back to a direct computation only in the
  brief start-up window before the index is seeded). The histogram `le` label
  strings are precomputed once from the bucket bounds rather than re-rendered for
  every bucket of every job on every scrape.

- **Lower steady-state memory and faster attribute access.** `JobConfig` -- one
  instance per configured job for the life of the process -- now declares
  `__slots__`, trimming its per-instance `__dict__` and speeding the attribute
  reads on the scheduling hot path. Fingerprint redaction is now copy-on-write
  instead of `deepcopy`, so the long immutable report templates (the sentry body,
  the webhook body) are shared by reference rather than duplicated on each
  fingerprint. The PyInstaller binaries are now built with `optimize=2`, which
  strips docstrings and (side-effect-free, internal-invariant) asserts from the
  frozen bytecode -- cronstable's modules are deliberately docstring-dense, so this
  shrinks the binary and lowers resident memory for the life of the daemon.

- **The idle scheduler no longer polls once a second.** The job reaper waited on
  its "any job running?" event with a one-second timeout, waking every second
  even when nothing was running. It now blocks on the event outright -- the wait
  condition can only change when a job launches or shutdown is signalled, both of
  which set the event (shutdown now does so explicitly, so the reaper exits
  promptly) -- so a fully idle daemon does no per-second work. A related fix reads
  the running-jobs map with `.get()` so a concurrency check can no longer leave a
  phantom empty entry that would spin the reaper hot at shutdown.

- Internal: the reload skip cache (change detection, the failed-parse and
  worker-thread paths), the memoized fingerprint, and the scrape reading the
  seeded next-fire index are covered by new tests; the uvloop bundling is gated
  behind a build-time verification step and the per-arch `--version` smoke test.

## 1.2.4 (2026-07-03)

This release re-implements the scheduler core added in 1.2.3 without changing
what it does: every schedule fires exactly when it did before, and there are no
configuration changes. The daemon no longer wakes on a fixed cadence and tests
every job against the clock; it keeps each job's next fire time in an index and
**sleeps until the soonest one is due**, servicing only the jobs whose moment
has arrived.

- **Per-wake cost scales with jobs *due*, not jobs *configured*.** The previous
  loop matched every enabled job against the clock on every tick -- and with a
  second-level job present that tick was once a second, so the whole job set was
  scanned every second. The scheduler now maintains a next-fire index (each
  job's next-fire instant, mirrored in a min-heap), sleeps until the earliest
  entry, and touches only the jobs actually due. An idle wake over a large fleet
  is an O(1) heap peek that runs zero cron matching; a wake with a cohort due
  matches only that cohort. A deployment running thousands of sparsely-scheduled
  jobs pays dramatically less per wake, and adding a second-level job no longer
  imposes a per-second scan of everything else.

- **Robust across wall-clock and NTP steps.** The sleep is realized against the
  event loop's monotonic clock, and firing compares the wall clock against
  fixed, forward-only next-fire instants. A clock step **backward** (an NTP
  correction or a manual set) now defers the pending fire instead of re-running
  a slot that already fired; a large step **forward**, a resume from suspend, or
  an RTC-less boot corrected far ahead resumes at the current slot in O(1)
  instead of enumerating and replaying every occurrence in the skipped span. The
  bounded catch-up for a genuinely overrun pass -- a slow config reload, a burst
  of simultaneous launches -- is retained, still capped at the ten-second
  `CATCHUP_LIMIT`, and now covers minute- and second-level jobs by the same
  path.

- **De-duplication is now structural.** A fired slot cannot fire twice because
  advancing the index moves a job's next fire strictly past the slot it just
  fired; the old per-slot "did this already run?" gate is gone (`_last_run_slot`
  is kept only for status/introspection). All the surrounding guarantees are
  preserved: a job fires exactly once per matching slot, a mid-period restart
  skips the period already under way (the index is seeded strictly-future at
  start-up), `@reboot` jobs run once at boot, a config reload landing on a job's
  own boundary minute does not skip that fire, and housekeeping (config reload,
  cluster and web upkeep, logging) still runs at most once a wall-clock minute.

- Internal: the next-fire index, monotonic sleep, clock-step handling, reload
  reconciliation, and a fleet-scale performance demonstration are covered by a
  new batch of scheduler tests; the wiki's "How the scheduler ticks" section is
  rewritten to describe the index.

## 1.2.3 (2026-07-02)

This release brings **second-level (sub-minute) scheduling**: a job can now
fire at second granularity, either through a new `second` field on the schedule
object or a full seven-field crontab string. The scheduler keeps its historical
once-a-minute cadence -- and its zero overhead -- until some enabled job
actually asks for seconds, at which point it ticks once a second, firing
second-level jobs on time while every minute-level job still fires exactly once
in its minute. The release also starts honoring the schedule object's `year`
key (previously accepted but silently dropped, a behavior change for the few
configs that set it), surfaces a malformed schedule as a named `ConfigError` at
reload instead of an anonymous traceback, teaches the web dashboard to parse,
describe, and preview five-, six-, and seven-field expressions, and ships two
runnable examples (`pulse-monitor` and its clustered sibling `pulse-cluster`)
built around second-level probing. Sub-minute scheduling is entirely opt-in;
see the upgrade notes below for the one behavior change that can affect an
existing deployment.

### Second-level (sub-minute) scheduling

- **New `second` field and seven-field crontab strings.** parse-crontab reads
  extra columns from the *ends* of a crontab line, so the field count selects
  the dialect: a five-field line has an implicit second of `0` and any year, a
  six-field line adds a trailing **year** column, and a seven-field line adds a
  leading **second** column too (`second minute hour dayOfMonth month dayOfWeek
  year`). So the object `second: "*/15"` and the seven-field string
  `"*/15 * * * * * *"` both fire every 15 seconds, while a six-field string pins
  a year and stays minute-granular. The `second` field takes the same syntax as
  any other (`*`, `*/5`, `0,30`, `10-20`); `second: "*"` fires every second.
  Second-level scheduling is a YAML feature: classic crontab files stay
  five-field and minute-granular.

- **Adaptive cadence, zero cost when unused.** The scheduler ticks once a second
  only while some *enabled* job pins a second (`Cron._needs_subminute()`);
  otherwise it keeps the historical once-a-minute cadence, aligned to the top of
  each UTC minute, byte-for-byte as before. A disabled second-level job never
  forces the per-second cadence. The cadence is re-evaluated every tick, so a
  reload that adds or removes a second-level job switches modes on that same
  tick.

- **Exactly once per slot; mixed cadences.** Each pass reads the clock once and
  tests every job against a single scheduling "slot" truncated to that job's own
  resolution -- the whole second for a second-level job, the top of the minute
  otherwise. Launches are de-duplicated per slot (`_last_run_slot`), so a
  minute-level job now tested up to 60 times in its due minute still fires
  exactly once, and a second-level job fires once per matching second even if two
  ticks land in the same second. A leader-gated job is evaluated once per slot
  regardless of which node runs it. Sub-minute and per-minute jobs mix freely in
  one config; `concurrencyPolicy` still governs overlap as before.

- **Catch-up for overrun seconds, bounded.** In sub-minute mode, if a pass runs
  long -- many simultaneous launches, or the once-a-minute config reload -- and
  the clock crosses one or more whole seconds before the next pass, the skipped
  seconds are serviced after the fact, so a second-level job due in the gap still
  fires (once) rather than being silently dropped. The replay is bounded by a
  ten-second `CATCHUP_LIMIT`: a larger gap is treated as a stall, suspend, or
  clock jump and resumed past with a warning, never replayed as a burst of
  backdated launches (matching cron's no-catch-up-after-an-outage behavior).
  Minute-level jobs need no catch-up: their minute-truncated slot already
  absorbs any sub-minute overrun.

- **No spurious run at a mid-period restart.** On startup the de-dup map is
  seeded with the in-progress slot for every scheduled job, so a job whose
  minute (or second) is already under way does not fire immediately on the first
  tick; it first fires at the next matching boundary, exactly as in minute-only
  mode. Without this, merely having any second-level job present would have made
  every minute-level job fire about a second after a mid-minute restart.
  `@reboot` jobs are unaffected and still fire once at startup.

- **Concurrent launches within a slot.** When several jobs are due in the same
  slot, `spawn_jobs` now launches them concurrently instead of one at a time.
  With N jobs sharing a slot the old serial form cost N times a subprocess spawn
  -- the dominant source of same-second overrun -- which now collapses to about
  a single spawn. The single-job case (the norm) still takes a direct await and
  is byte-identical to before, and the de-dup and cluster-gate decisions are
  still made sequentially, so only the per-job "Starting"/"spawned" log lines may
  now interleave.

- **Config reload moved off the event loop.** The once-a-minute reload now runs
  its disk read and full reparse in a worker thread (`reload_config`), so a slow
  parse no longer freezes the event loop -- web API, cluster gossip, job-output
  pumping -- for its whole duration. The parsed job set is still applied on the
  loop thread and *before* jobs are serviced, so the cluster leader-gate is
  always current for the tick. Housekeeping (config reload, cluster and web
  (re)start, logging config) is gated to run at most once per wall-clock minute
  even while the loop ticks per second; in pure minute-tick mode it runs every
  iteration, exactly as before.

### The `year` schedule key is now honored

- **`year` restricts the schedule to specific years.** Earlier releases accepted
  a `year` key on the schedule object but built only a five-field crontab string
  from it, silently dropping `year` so it had no effect -- a job with an
  object-form `year` ran every year. It is now emitted as parse-crontab's
  trailing year column and honored, so `year: "2017"` really does pin the
  schedule to 2017. (String schedules were always passed to parse-crontab
  verbatim, so a six-field string already honored its year; only the object form
  changes.) This is a behavior change -- see the upgrade notes below.

- Honoring `year` changes that job's job-set fingerprint, so during a rolling
  upgrade of a cluster the old and new binaries compute different `job_set_id`s
  for the identical config and will not treat each other as agreed peers until
  every node is upgraded -- the same transient, self-healing drift as any config
  rollout, and leader election stays at-most-once throughout. Jobs that do *not*
  use object-form `year` are unaffected: their fingerprint is byte-for-byte
  identical to before.

### Schedule parsing, errors, and fingerprints

- **A malformed schedule now fails the reload with a named error.**
  parse-crontab's `ValueError` on a bad field (an out-of-range value, the wrong
  field count) is caught and re-raised as `ConfigError("invalid schedule
  '...': ...")`, naming the offending expression, so a bad schedule fails config
  load or reload cleanly with a message the reload loop can log, rather than
  surfacing as an anonymous traceback.

- **One object-to-crontab builder, shared everywhere.** A single
  `schedule_object_to_crontab` helper now renders the object form to a crontab
  line -- five fields normally, six or seven when `year`/`second` are used -- and
  is shared by parsing, the fingerprint, and the dashboard's schedule label, so
  those three can never disagree on the mapping. The object form still collapses
  to the exact five-field line as before when neither `second` nor `year` is set,
  so its fingerprint is unchanged. Whether a schedule counts as second-level is
  derived from the *actual rendered field count* (seven), not mere key presence,
  so a blank `second:` value that renders an empty column does not force the
  whole scheduler onto the per-second cadence.

### Web dashboard

- **Cron parsing, description, and preview understand five-, six-, and
  seven-field expressions.** The client-side cron engine normalizes any of the
  three widths (implicit second `0` and any year for five fields, a trailing year
  for six, a leading second for seven), computes next-fire times at second
  resolution with year restriction (and parse-crontab's 2099 year ceiling), and
  renders wall-clock times with a seconds component where the schedule has one.
  Plain-English descriptions gain "Every second", "Every N seconds", "At
  second(s) ...", and an "in {year}" clause -- and deliberately do *not* lead
  with a per-second cadence phrase when a coarser field is restricted, so a
  schedule like `* 30 * * * * *` is not described as firing every second.

- **Cron sandbox covers the new widths.** The palette's schedule sandbox
  validates 5-, 6-, and 7-field expressions (its error copy and field-breakout
  labels updated to match, labelling the leading second and trailing year
  columns correctly), and its next-fire preview shows seconds.

- **Clicking the wordmark spins the logo.** The "cronstable" wordmark now triggers
  the same mark-spin animation as clicking the mark glyph.

### Examples and documentation

- **`example/pulse-monitor`** -- a small, runnable real-time uptime / SLA monitor
  built entirely on second-level scheduling: it probes a latency-critical service
  every few seconds, heartbeats every ten, and rolls up a summary once a minute
  (which still fires exactly once per minute alongside the per-second probes). It
  watches cronstable's own `/status` endpoint, so `docker compose -f
  docker-compose-pulse.yml up` needs nothing else running.

- **`example/pulse-cluster`** -- the clustered sibling: a three-node,
  mutual-TLS, leader-electing cluster that splits the monitoring work the way a
  real fleet should -- `liveness-probe` runs on every node (independent vantage
  points catch a partition outage), while `latency-slo` and the summary run on
  the leader only. A one-shot service mints throwaway certs, and an optional
  `distribution: spread` fans the leader jobs across nodes by rendezvous hashing.
  `docker compose -f docker-compose-pulse-cluster.yml up`.

- The wiki and README are updated throughout: a new "Second-level schedules"
  reference and field-count table in Schedules and Timezones, the `year` key
  documented as honored (with an upgrade note), a Troubleshooting entry on the
  common "six fields is a year, not seconds" mistake, the Configuration Reference
  `schedule` row, and the Web Dashboard sandbox notes.

- Internal: second-level scheduling ships with a matching batch of tests
  (`test_cron.py`, `test_config.py`, `test_fingerprint.py`) covering the object
  and string spellings, the adaptive cadence and its zero-overhead minute path,
  per-slot de-duplication, bounded catch-up, the mid-period-restart seeding, the
  `year` fingerprint change, and the malformed-schedule error path.

### Upgrade notes

- **Object-form `year` is now honored (breaking).** A schedule object that sets
  `year` previously had no effect and now restricts the job to that year, so a
  past year stops the job firing. This is the only change that can affect an
  existing deployment, and only one that uses object-form `year`; to keep the old
  "runs every year" behavior, remove the `year` key. During a rolling cluster
  upgrade such a job's fingerprint changes, so mixed-version nodes will not agree
  on its `job_set_id` until all are upgraded (transient and self-healing; leader
  election stays at-most-once). All other schedules -- crontab strings, and
  object schedules without `year`/`second` -- behave and fingerprint exactly as
  before.

- **Seven-field crontab strings now fire at second granularity.** A seven-field
  string earlier fired at most once a minute, because the scheduler zeroed the
  seconds column and only woke per minute; such schedules were effectively
  meaningless. They now fire on the seconds they specify. This is unlikely to
  surprise, but audit any seven-field strings already in your configs.

## 1.2.2 (2026-07-02)

- **New webhook reporter: native Slack/Discord/Teams/ntfy notifications.** A
  fourth reporter joins sentry/mail/shell in every `report` block: `webhook`
  sends an HTTP request (POST by default) to a configured URL with a
  jinja2-templated body. The default body is a `{"text": ...}` JSON payload
  carrying the same subject-plus-body text as the default mail/sentry
  templates, JSON-encoded with jinja2's `tojson` filter so quotes, newlines,
  and non-ASCII job output always produce valid JSON -- point `url` at a
  Slack, Mattermost, or Teams incoming webhook and it works with no further
  configuration. `method`, `contentType`, `headers`, `body`, and `timeout`
  cover everything else (Discord's `{"content": ...}` shape, ntfy's
  plain-text body and header-driven priority, or your own endpoint). The URL
  resolves like the sentry DSN (`value` / `fromFile` / `fromEnvVar`) and is
  treated as a secret throughout: it is never logged, and the job-set
  fingerprint redacts the inline URL value and all header values (which
  commonly carry `Authorization` tokens). No new dependency -- outbound
  delivery rides the core aiohttp. Note: because every job's effective
  config gains the new default block, job-set ids change on upgrade;
  replicas must be on the same version to compare ids, as before.

- **Unchanged peers now answer gossip polls with a bodyless `304`.** Every
  `/peer` response carries a strong `ETag` (a content hash of the payload),
  and each polling node echoes the tag of the last full body a peer served
  it back as `If-None-Match`, so a peer whose state has not changed since
  then skips re-sending the full O(members + jobs) JSON: a converged, idle
  cluster's steady-state round costs headers rather than bodies. This is a
  transport optimization, not a protocol change. A `304` is still a fresh,
  mutually-authenticated round trip, and because the tag is content-derived,
  a match proves the peer's payload is exactly the one the poller already
  holds, so the poller replays its cached observation and every gate (mutual
  agreement, conflict detection, the `cluster.driftAfter` debounce) advances
  exactly as if the identical body had been re-sent. The one live field, a
  job's seconds-to-next-fire countdown, is hashed as the absolute next-fire
  time instead, so the tag stays stable between fires and rolls exactly when
  a schedule fires. Mixed fleets degrade safely during a rolling upgrade: an
  older peer ignores `If-None-Match` and keeps serving full bodies, a
  tagless response stops the poller from sending the header at all, an
  unsolicited `304` is recorded as a failed poll, and an over-long or
  non-printable tag is never stored or echoed.

- **Fleet-view countdowns are aged, not frozen.** With `304` rounds
  refreshing a peer's liveness without re-shipping its job summaries, a
  stored snapshot can now legitimately outlive many polling rounds, so
  `GET /fleet` re-derives each peer job's advertised `scheduled_in`
  countdown from the snapshot's age (an elapsed duration measured on the
  local clock alone, so peer clock offsets never leak in) instead of serving
  the value the snapshot arrived with, clamping at zero. The fire itself
  rolls the peer's `ETag`, so the next poll ships a full body carrying the
  real successor value.

- **`/peer` bodies that do go out are gzip-compressed** once they reach
  1 KiB (below that, the per-request CPU spend outweighs the few bytes
  saved). The polling side already advertised gzip support, and the existing
  response-size cap applies to the *decompressed* payload, so compression
  does not weaken it.

- **Dashboard: the version and job-set id header chips copy their value on
  click.** Header text is chrome and is no longer text-selectable; the two
  values worth grabbing hand themselves out instead. Clicking the version
  chip copies the version, and clicking the job-set chip copies the full
  job-set id even though the header shows only a short prefix; both
  tooltips say so. The command palette carries the same two copies, so both
  values stay reachable from the keyboard.

- **Dashboard: the quick "power-on sweep" flash between boots is gone.** The
  full POST boot screen still replays once its cooldown elapses, but the
  visits in between now start the app directly instead of playing a
  full-screen power-on animation first.

- Internal: the conditional exchange ships with a matching batch of cluster
  tests (tag stability across countdown ticks and rollover on a fire, the
  `304` replay path, unsolicited-`304` and unusable-tag rejection, countdown
  aging, and an end-to-end mutual-TLS `304`-plus-gzip round), and the wiki's
  Architecture and Internals page documents the exchange.

## 1.2.1 (2026-07-02)

This is the largest cronstable release since the fork. Its headline feature is
clustering: several replicas can now verify they hold the same job set, elect
a leader so each scheduled job runs on one node instead of every node, spread
jobs across the fleet, and fail over when a node dies, coordinating either
peer-to-peer over mutual TLS or through a Kubernetes or etcd lease. The
release also adds native Prometheus metrics, accepts classic crontab files as
configuration, and grows the web dashboard into a small operations console.
Clustering is entirely opt-in; see the upgrade notes below for the few
behavior changes that apply to existing deployments.

### Clustering and leader election

- **New optional top-level `cluster:` section.** Give every replica the same
  static peer list and a dedicated mutual-TLS listener (`cluster.listen`,
  `cluster.tls.{ca,cert,key}`, `cluster.peers`), and each node polls every
  peer once per round (`cluster.interval`, default 30 seconds), comparing
  job-set ids (see 1.1.8) to attest that all replicas hold an identical job
  set. Peers are reported as `agreed`, `syncing`, `drifted` (a mismatch that
  persists for `cluster.driftAfter` consecutive rounds, default 3),
  `unreachable`, `untrusted` (TLS verification failed), or `conflict`. On its
  own this is observe-only (every node still runs every job), which makes it
  a safe first rollout step.
- **Leader election (`cluster.electLeader: true`).** The leader is the lowest
  `cluster.nodeName` (default: the hostname) among the agreeing nodes this
  node can see, and only when they form a strict majority of the declared
  cluster size; below quorum a node stands down and runs nothing. Two
  disjoint majorities cannot exist, so a clean partition produces at most one
  leader within about one polling interval. Conflicts fail closed: a
  duplicate `nodeName`, a cluster-size disagreement (say, a rolling resize
  from 3 to 5 nodes), or a coordination-policy divergence parks `Leader` jobs
  until the fleet reconverges, and each is logged loudly and shown on the
  dashboard. A freshly started or reconfigured node holds its jobs until it
  has polled every configured peer at least once, so a blank-view node cannot
  elect itself. Two-node election is refused at config load (it is strictly
  worse than a single replica); even cluster sizes log a warning.
- **A per-job `clusterPolicy`** (also settable under `defaults:`) decides
  what election means for each job: `Leader` (the default: only the elected
  leader runs it, and it is skipped when in doubt), `PreferLeader` (never
  skip: the lowest reachable agreeing node runs it even without quorum,
  accepting a rare double-run, so reserve it for idempotent jobs), and
  `EveryNode` (all nodes run it, for node-local housekeeping). Manual runs
  via `POST /jobs/{name}/start` are never gated. Automatic retries re-check
  the gate before every relaunch and abandon a pending retry when ownership
  has demonstrably moved to another node; `@reboot` jobs under election are
  deferred until the cluster converges and then run once, on the owner.
- **Load-balanced jobs: `cluster.distribution: spread`.** Instead of one
  leader running every `Leader`/`PreferLeader` job, each such job is
  assigned an owning node by rendezvous (highest-random-weight) hashing over
  the quorate agreeing members, spreading work across the fleet. A
  membership change moves only the departing or joining node's share of
  jobs. The same quorum gating applies, and `GET /jobs` reports each job's
  current `clusterOwner`.
- **Mutual TLS is the entire trust boundary.** A peer must present a
  certificate chaining to the configured CA and matching the host it was
  reached at, so the CA should be dedicated to cronstable nodes and nothing
  else. Certificates rotated in place are detected and reloaded without a
  restart. The peer endpoint is served only on the cluster listener, never
  on the web API listener.
- **The failure semantics are documented, not hand-waved.** The built-in
  backend is best-effort by design: there are narrow, self-healing windows
  where a `Leader` job can be skipped or a `PreferLeader` job can
  double-run, and the new wiki page enumerates them. When you need fenced
  exactly-once execution, use one of the lease backends below.
- `cronstable --validate-config` validates the entire `cluster` section (peer
  list, TLS material, lease timing invariants) without starting anything.

### Kubernetes and etcd lease backends

- **`cluster.backend: gossip | kubernetes | etcd`** picks the coordination
  mechanism (default `gossip`, the peer-to-peer mode above). The lease
  backends replace the peer quorum with a fenced, expiring lease in an
  external store: exactly one node holds the lease at a time, so `Leader`
  jobs are exactly-once while the store is reachable. With a lease backend,
  election is always on, there is no peer list or cluster mTLS to manage,
  and `distribution: spread` is rejected at config load (a single lease
  cannot express per-job ownership).
- **Kubernetes** (`cluster.kubernetes.*`): replicas campaign for a
  `coordination.k8s.io/v1` Lease object using the client-go leader-election
  algorithm (`leaseName` defaults to `cronstable-leader`;
  `leaseDurationSeconds`/`renewDeadlineSeconds`/`retryPeriodSeconds` default
  to 15/10/2, with the client-go timing invariants enforced at config load).
  In-cluster ServiceAccount token, CA, namespace, and API server are
  detected automatically; a `kubeconfig` and an explicit `apiServer` (https
  only) are supported. The backend talks to the API server over the bundled
  aiohttp by default; installing the new optional extra
  (`pip install cronstable[kubernetes]`) switches to the official Kubernetes
  client (`clientLibrary: auto|http|library`). The stored holder identity
  embeds a per-process token, so two pods that accidentally share a name
  cannot both hold the lease. `example/kubernetes/` ships a ready-to-apply
  Deployment with the minimal ServiceAccount, Role, and RoleBinding.
- **etcd** (`cluster.etcd.*`): replicas campaign for a lease-bound key
  (`electionName` defaults to `cronstable/leader`) through etcd's v3 JSON/HTTP
  gRPC gateway, using a create-if-absent transaction fenced by the lease id
  (`ttl` defaults to 15 seconds, minimum 3). Multiple `endpoints` fail over
  in order; optional `username`/`password` (literal, `fromFile`, or
  `fromEnvVar`) and client TLS are supported, and credentials are refused
  unless every endpoint is `https://`. `example/etcd/` ships a compose demo
  with an etcd and two cronstable replicas.
- **Failover is fast and clock-safe on both.** All fencing runs on the
  monotonic clock with a one-second skew margin (no wall-clock or cross-node
  clock-sync assumptions): a holder whose renewals stall demotes itself
  locally before the store-side lease can expire, without a network call. A
  graceful shutdown releases the lease explicitly (Kubernetes clears
  `holderIdentity`, etcd revokes its lease) so a survivor takes over
  immediately rather than waiting out the TTL. If the store becomes
  unreachable, `Leader` jobs fail closed and `PreferLeader` jobs keep
  running. `@reboot` bookkeeping is persisted in the store, scoped to the
  job-set id, so a `Leader` `@reboot` job runs once per job configuration
  rather than once per fleet restart.
- **No new runtime dependencies.** Both lease backends are plain HTTP over
  the existing aiohttp core (no etcd or gRPC client; the Kubernetes client
  is optional), so all packaged architectures keep working.

### Prometheus metrics

- **Native Prometheus metrics at `GET /metrics`**, served by the existing
  web listener whenever the web API is enabled; there is no exporter
  sidecar and no new dependency (the exposition is generated in-process).
  Both the classic text format and OpenMetrics 1.0 are served, negotiated
  via the `Accept` header.
- Per-job series cover run outcomes (`cronstable_job_runs_total` labeled by
  `job_name` and `status`), retries, permanent failures, failures to start,
  a duration histogram with configurable buckets
  (`web.metrics.durationBuckets`), last success/failure/run timestamps and
  exit code, and live state (enabled, running, next run). Daemon series
  cover the version, start time, job-set id, job counts, and config-reload
  health. Cluster series cover size, quorum, leadership, per-peer status
  counts, conflicts, and leader/quorum transition counters; the wiki
  suggests alerting on `sum(cronstable_cluster_is_leader) > 1` and on losing
  `cronstable_cluster_quorate`. Metrics are recorded at the same point as the
  run history, so `/metrics` and `/jobs/{name}/runs` never disagree.
- `web.authToken`, when configured, protects `/metrics` like every other
  endpoint; `web.metrics.public: true` exempts just this endpoint for a
  scraper, and `web.metrics: false` removes it entirely.

### Classic crontab files

- **Classic (Vixie) crontabs are now accepted as configuration.** A file
  with a `.crontab` or `.cron` extension, or named exactly `crontab`, is
  parsed as a crontab wherever configuration is loaded: passed to `-c`,
  dropped into a config directory alongside `*.yaml` files, or pulled in
  via `include:`; a neutral-named file given to `-c` or `include:` is
  content-sniffed. Supported syntax: five-field entries (the same field
  dialect as YAML `schedule` strings), `@keywords` (including `@reboot` and
  `@midnight`), position-sensitive `VAR=value` environment lines, comments,
  and the `\%` escape. `SHELL` and `CRON_TZ` assignments are honored as the
  job's `shell` and `timezone`.
- Each entry becomes a normal cronstable job named `<file>:<line>`,
  indistinguishable downstream: it shows up on the dashboard and HTTP API,
  participates in the job-set id and clustering, and can be run or
  cancelled on demand. cronstable's defaults apply rather than cron's (UTC
  unless `CRON_TZ` says otherwise; any stderr output marks the run as
  failed). Deviations are deliberate and loud: an unescaped `%` is a
  load-time error instead of silently not feeding stdin, `MAILTO` is
  exported to the job but sends no mail, and the six-column system-crontab
  user field is not supported. Per-entry knobs (retries, reporting,
  timeouts) still require migrating that line to YAML; the new wiki page
  shows how.

### Web dashboard

- **A cluster operations view.** A cluster panel shows this node's role
  (leader, follower, or standing down and why), quorum state, and a
  per-peer table with status dots and a per-peer history timeline; lease
  backends show the lease holder and expiry instead. A page-level alert bar
  calls out a conflict or lost quorum without scrolling, and a fleet view
  (backed by the new `GET /fleet`) renders a jobs-by-nodes matrix of every
  node's last outcome per job, with a failing-only filter.
- **Incident tooling.** A verdict bar summarizes active failures and, when
  several jobs fail together, says whether they look like one cause or
  independent ones; an incident timeline (press `i`) orders every job's
  most recent run; a mitigate console bulk-starts or bulk-cancels jobs and
  copies a Markdown incident summary; and a merged multi-tail console
  streams up to four jobs' logs into one color-coded view.
- **Wallboard and zen mode.** A full-screen TV mode (press `w`, or open
  `#tv`) shows worst-first job tiles with a staleness watchdog that shows
  `NO SIGNAL` rather than a false all-green; when everything is healthy and
  idle for a while it drifts into a zen screensaver in which every job
  pulses at its real next fire time.
- **More ways to read the schedule.** An activity heatmap (a 6h/24h/7d
  punchcard per job), a cron sandbox that validates a cron expression and
  previews its next 12 fire times alongside live jobs sharing it, a column
  picker adding Owner/Policy/TZ/Next-at/Rate columns, and an opt-in,
  browser-local run ledger (IndexedDB) that keeps run history beyond the
  daemon's in-memory cap and flags unusually slow runs against each job's
  own duration baseline.
- **Finishing touches.** A fourth theme, `carolina` (a Carolina-Blue CRT
  phosphor), joins amber/green/modern; optional audible cues with a volume
  setting and an escalating failure alarm (press `a` to acknowledge); and
  an optional boot self-test splash on load.

### HTTP API

- New endpoints: `GET /cluster` (this node's cluster view: backend, quorum,
  leader, conflicts, per-peer detail) and `GET /fleet` (fleet-wide per-job
  run summaries carried on the gossip round; observability only), plus
  `GET /metrics` above. `GET /jobs` gains `clusterPolicy` and, under
  `distribution: spread`, each job's `clusterOwner`. The HTTP API wiki page
  now documents every endpoint with full response shapes.
- Configured `web.headers` are now applied to every successful response,
  including the new endpoints, and to the `409 Conflict` bodies of
  `start`/`cancel`.
- A web-section configuration error found during a reload now leaves the
  web API down with a clear log line while the rest of the reload (jobs,
  cluster, logging) still applies.

### Reliability

- **A failed job launch can no longer crash the scheduler.** Spawning a job
  now guards against any `OSError` (file-descriptor exhaustion, fork or
  memory limits, permission errors), not just a missing executable: the run
  is recorded as an ordinary start failure with exit code 127 (and counted
  in `cronstable_job_start_failures_total`) instead of the error propagating
  out of the scheduling loop and taking the daemon down.

### Upgrade notes

- `/metrics` is served by default wherever the web API is enabled. It sits
  behind `web.authToken` like every other endpoint when a token is
  configured; set `web.metrics: false` to remove it.
- The job-set id changes once on upgrade: `clusterPolicy` is now part of
  every job's fingerprint, so an unchanged configuration hashes to a
  different `v1:` id than 1.1.8 through 1.1.11 reported. Compare ids only
  between nodes running the same cronstable version; a mixed-version fleet
  reads as drift until the rollout completes.
- Config directories now load crontab-named files (`*.crontab`, `*.cron`,
  `crontab`) that earlier releases silently ignored.

### Packaging

- New `cronstable.backends` subpackage, and a new optional extra:
  `pip install cronstable[kubernetes]` installs the official Kubernetes client
  for `clientLibrary: library`. A core install gains no new runtime
  dependency and the supported Python range is unchanged. The PyPI metadata
  gains prometheus/metrics/monitoring keywords and an updated description.

### Documentation and examples

- Three new wiki pages: Clustering and Leader Election (a full operations
  guide: both backend families, every failure window, sizing, rollout, and
  troubleshooting), Metrics with Prometheus, and Classic Crontabs, plus
  major updates to the HTTP API, Web Dashboard, Production Deployment,
  Architecture and Internals, and Troubleshooting pages. The README gains
  matching sections.
- New runnable demos: `docker-compose-cluster.yml` (a three-node mutual-TLS
  cluster with scripted try-it failover scenarios),
  `docker-compose-cluster-large.yml` (ten nodes with `distribution: spread`
  and CPU-heavy jobs, for watching work fan out), `docker-compose-acme.yml`
  (a five-node simulated data platform with a mail sink, a statsd exporter,
  and deterministic scripted incidents that exercise the dashboard's
  incident tooling), `docker-compose-zen.yml` (one calm node for the zen
  screensaver), `example/etcd/` and `example/kubernetes/` for the lease
  backends, and `example/crontab/` for classic crontabs.

### Internal

- The test suite roughly doubles, with new suites for the cluster manager,
  both lease backends, the leadership abstraction, the Prometheus
  registry/exposition, crontab parsing, and backend config validation, plus
  large additions to the scheduler tests. The `dev` extra adds
  `cryptography` for the cluster mTLS tests (skipped on Windows ARM64,
  which has no wheel). A new `.gitattributes` pins `*.sh` to LF line
  endings so the bind-mounted cluster demos work from a Windows clone.

## 1.1.11 (2026-06-29)

- **Coverage is now published to [Codecov](https://codecov.io/gh/ptweezy/cronstable).**
  Every CI matrix cell uploads its own `coverage.xml` under an
  `<os>-py<version>` flag, and Codecov merges them into one combined number, so
  POSIX-only paths that Windows skips (privilege drop, `user`/`group`
  resolution) still count toward the published total instead of dragging it down
  to the lowest single row. tox now also writes the report it consumes
  (`pytest --cov-report=xml`). The hard pass/fail gate stays with tox's
  `--cov-fail-under=82` (see 1.1.10): Codecov's own project and patch status
  checks are configured as *informational* only, so they annotate pull requests
  without ever blocking them. The upload runs even on failed or cancelled jobs
  and keeps `fail_ci_if_error: false`, so a Codecov outage never reds the build,
  and flag `carryforward` keeps the combined number stable when a matrix row is
  skipped on a given run. The README gains a matching coverage badge.

## 1.1.10 (2026-06-24)

- **Numeric `user`/`group` is read as a uid/gid, not a login name.** In the
  config schema the `user`/`group` type was a `Str() | Int()` union, and
  strictyaml matched the always-accepting `Str()` first, so `user: 1000`
  arrived as the *string* `"1000"` and was looked up as a login *name*
  (`getpwnam("1000")`) rather than used as uid 1000. The union is now
  `Int() | Str()`, so a bare number is treated as the uid/gid it looks like; a
  non-numeric name (`user: www-data`) still falls through to `Str()`. (POSIX
  only; per-job `user`/`group` remains rejected with a configuration error on
  Windows.)

- **More resilient container builds.** Every image build, across the default
  Debian image and all seven distro variants (`-alpine`, `-ubuntu`, `-rhel`,
  `-fedora`, `-opensuse`, `-amazonlinux`, `-distroless`), now wraps its
  package-manager and `pip` network steps in a retry-with-backoff helper,
  alongside each manager's native knobs (`apt`'s `Acquire::Retries`, `dnf`'s
  `--setopt=retries`, an explicit `zypper refresh` retry, and a longer
  `pip --timeout`), so a transient mirror or package-index hiccup retries
  instead of failing the whole build. The build and test CI workflows get the
  same hardening via `PIP_RETRIES`/`PIP_TIMEOUT`, with `build.yml` forwarding
  them into its emulated cross-architecture binary builds via `docker run -e`.

- **The `-distroless` image now builds for `amd64`/`arm64` only.** The
  `gcr.io/distroless/python3-debian12` base publishes no `ppc64le` or `s390x`
  manifest, so requesting those arches aborted the distroless release with
  "no match for platform in manifest". The RPM-based variants (`-rhel`,
  `-fedora`, `-opensuse`) still cover the wider arch set.

- The README status badges also gain brand new colors
  (and logos on the PyPI/Python badges). yay

- Internal: branch coverage is now measured and gated in CI (tox runs
  `pytest --cov-fail-under=82`), backed by substantially expanded unit tests for
  config and user/group validation, config reload and graceful shutdown, the
  job runner, and the job-set-id fingerprint.

## 1.1.9 (2026-06-23)

- **More prebuilt container images.** Alongside the default Debian-based image,
  every release now also publishes the same build on seven more bases, each
  tagged with a `-<distro>` suffix: `-alpine`, `-ubuntu`, `-rhel` (Red Hat
  UBI 9), `-fedora`, `-opensuse` (Leap), `-amazonlinux` (2023) and
  `-distroless`, plus an explicit `-debian` alias for the default. Pick the base
  that matches your host userland or image-provenance policy; behavior is
  identical, since cronstable is a pure-Python app (Python >= 3.10) and each image
  uses its distro's native interpreter. The Debian image still owns the bare
  `latest`/`<version>` tags and the widest architecture coverage. See
  [Distro variants](README.md#distro-variants).

## 1.1.8 (2026-06-23)

- **Job-set id.** cronstable can now emit a *job-set id*: an order-independent
  hash of every job's effective configuration. Two instances deployed from the
  same configuration produce the same id, so replicas can confirm they hold an
  identical set of jobs. It is taken over the merged, effective config (so
  reordering jobs, or moving
  a setting into `defaults`, doesn't change it), normalizes equivalent schedule
  spellings, fingerprints `user`/`group` as configured rather than as a
  host-specific resolved uid/gid, and embeds no secret material (inline
  reporting secrets are redacted, and only `environment` variable names are
  hashed, not their values). Get it from
  the CLI (`cronstable --job-set-id`, prints and exits), the web API
  (`GET /job-set-id`, also `application/json`), and the dashboard header; it is
  logged once at startup and again whenever a config reload changes it. The
  scheme is versioned (a `v1:` prefix) so ids are only compared within a scheme.

## 1.1.7 (2026-06-23)

- **Windows support.** cronstable now runs natively on Windows, in addition to
  Linux and macOS. The core was made portable: the POSIX-only `grp`/`pwd`
  imports are now lazy and guarded, Ctrl-C / Ctrl-Break shutdown is wired up
  without the POSIX-only event-loop signal handlers, and subprocess argv is
  encoded per platform. `pip install cronstable` works on Windows, and every
  release now also ships self-contained binaries `cronstable-windows-amd64.exe`
  and `cronstable-windows-arm64.exe` (Python not required on the target).
  - On Windows a string `command` with no explicit `shell` runs through the
    native command processor (`%ComSpec%`, i.e. `cmd.exe`), mirroring the
    `/bin/sh` default on POSIX. Set `shell:` or pass `command` as a list for
    anything else.
  - The default config location (`-c`) is `%APPDATA%\cronstable` on Windows
    (`/etc/cronstable.d` is unchanged on POSIX).
  - Two features remain POSIX-only and are reported clearly on Windows: per-job
    `user`/`group` switching (rejected with a configuration error) and
    `unix://` web listeners (skipped with a warning; use an `http://` listener).
- CI now runs the test suite on Windows (x64 and ARM64) as well as Linux, and
  the per-commit build plus every release build the Windows binaries.
- Update the README `Platforms` badge to include Windows.

## 1.1.6 (2026-06-22)

- Add self-contained binaries for two more Linux architectures, bringing
  every release to eight Linux architectures plus macOS: 64-bit RISC-V
  (`riscv64`) in both glibc and musl flavors (`cronstable-linux-riscv64` and
  `cronstable-linux-riscv64-musl`) and 32-bit ARMv6 (`armv6`, e.g. Raspberry
  Pi Zero / Pi 1) in musl only (`cronstable-linux-armv6-musl`). As with the
  other binaries, Python is not required on the target system. Neither arch
  has a native GitHub runner, so they build inside a container via
  `docker run --platform` under QEMU emulation.
  - `armv6` is musl-only because the Debian/glibc base image ships no
    32-bit ARMv6 variant (only ARMv5/ARMv7), so there is no glibc `armv6`
    binary and the container image does not cover it.
  - Some dependencies ship no prebuilt wheel for these arches
    (`multidict`/`frozenlist`/`ruamel.yaml.clib` on `riscv64`; the entire
    C-extension stack on `armv6`), so they compile from source during the
    build.
- The published container image now also covers `linux/riscv64` (alongside
  `linux/amd64`, `linux/arm64`, `linux/386`, `linux/arm/v7`, `linux/ppc64le`
  and `linux/s390x`), and is build-checked at that full arch set on every
  commit.
- Update the README `Architectures` badge to list the new targets
  (`amd64`, `arm64`, `armv7`, `armv6`, `i686`, `ppc64le`, `s390x`,
  `riscv64`).


## 1.1.5 (2026-06-22)

This is a documentation release; there are no changes to the `cronstable`
package itself.

### Documentation

- README changes
- Add an `Architectures` badge to the README summarizing the binary and
  container targets (`amd64`, `arm64`, `i686`, `armv7`, `ppc64le`,
  `s390x`).

### Release automation

- Default the manual (`workflow_dispatch`) release to a `patch` bump and
  list `patch` first in the bump options, since patch releases are the
  common case.


## 1.1.4 (2026-06-22)

- Add self-contained binaries for two more Linux architectures to every
  release, in both glibc and musl flavors: little-endian POWER (`ppc64le`)
  and IBM Z (`s390x`) (`cronstable-linux-ppc64le`, `cronstable-linux-s390x`, and
  their `-musl` variants) alongside the existing `amd64`, `arm64`, `i686`
  and `armv7` builds. As with the other binaries, Python is not required on
  the target system. Neither arch has a native GitHub runner, so they build
  inside a container via `docker run --platform` under QEMU emulation; both
  have prebuilt manylinux and musllinux wheels for the aiohttp dependency
  stack, so nothing compiles from source.
- The published container image now covers them too: the multi-arch image
  adds `linux/ppc64le` and `linux/s390x` (to `linux/amd64`, `linux/arm64`,
  `linux/386` and `linux/arm/v7`), and is build-checked at that full arch
  coverage on every commit.


## 1.1.3 (2026-06-22)

- Add self-contained binaries for two more Linux architectures to every
  release, in both glibc and musl flavors: 32-bit x86 (`cronstable-linux-i686`
  and `cronstable-linux-i686-musl`) and 32-bit ARM (`cronstable-linux-armv7` and
  `cronstable-linux-armv7-musl`), alongside the existing 64-bit `amd64` and
  `arm64` builds. As with the other binaries, Python is not required on the
  target system. The 32-bit binaries are built inside a 32-bit container
  (`i686` natively on the x86-64 runner, `armv7` under QEMU emulation).
- The published container image now covers those architectures too: the
  multi-arch image is built for `linux/amd64`, `linux/arm64`, `linux/386`
  and `linux/arm/v7`, and is build-checked at that full arch coverage on
  every commit.


## 1.1.2 (2026-06-21)

This is a documentation release; there are no changes to the `cronstable`
package itself.

### Documentation

- Add a project wiki (under `wiki/`) covering installation, the
  configuration reference, the HTTP API, the web dashboard, schedules and
  timezones, reporting, statsd metrics, output capturing, concurrency and
  timeouts, failure detection and retries, includes and defaults, logging,
  the CLI, architecture and internals, production deployment, migration
  from yacron, contributing/releasing, and troubleshooting.
- Showcase the web dashboard near the top of the README with annotated
  screenshots of the overview, live log tail, run history, schedule
  preview, command palette, keyboard-shortcut reference, and the
  green-phosphor and flat modern themes, linking the dashboard tour in the
  wiki.
- Slim the README's web-server section to an "Enabling the web dashboard"
  pointer to that showcase and the wiki, removing the duplicated feature
  list.


## 1.1.1 (2026-06-21)

### Features

- Add a built-in web dashboard, served at the root path (`/`) of any
  `http://` listener. It shows each job's latest status with a live
  countdown to the next run and a trend sparkline, tails job logs live
  (with in-log search, ANSI-color rendering, optional timestamps, a
  line-wrap toggle, and a download button), runs or cancels jobs on
  demand, and reports each job's run history, success rate, and a
  plain-English schedule with a preview of upcoming run times. It is
  keyboard-first (`?` for shortcuts, `Ctrl-K`/`⌘K` command palette, `/`
  to filter), with configurable themes, a compact density mode, polling
  interval, and optional desktop failure notifications, all remembered
  in the browser.
- Cancel running jobs over the REST API with `POST /jobs/{name}/cancel`,
  using the same graceful SIGTERM-then-SIGKILL sequence (honoring
  `killTimeout`) as elsewhere. A cancelled run is recorded with a
  `cancelled` outcome and is neither reported nor retried; the endpoint
  returns `409 Conflict` if the job is not running and `404 Not Found`
  for an unknown job.
- `GET /jobs` now returns detailed per-job information: schedule,
  timezone, enabled/running state, time until the next run, a summary of
  the most recent finished run, and a compact recent-outcome history.
- Read a job's retained run history and aggregate statistics (success
  rate and average/min/max duration) with `GET /jobs/{name}/runs`.
- Tail a job's captured output live over Server-Sent Events with
  `GET /jobs/{name}/logs`, replaying the most recent run's buffered
  output before streaming new lines.
- Add a `web.ui` option; set `ui: false` to expose only the REST API and
  disable the dashboard.
- Keep run history and live logs in memory only, so the dashboard does
  not change cronstable's read-only-filesystem deployment story; history
  resets when cronstable restarts.
- Ship a `docker-compose.yml` and a demo crontab for trying the
  dashboard against a set of varied example jobs.

### Security

- Serve the dashboard with a strict `Content-Security-Policy` and
  additional hardening headers (`X-Content-Type-Options`,
  `X-Frame-Options`, `Referrer-Policy`); each can be overridden via
  `web.headers` while unset defaults are still applied.
- When bearer-token authentication (`web.authToken`) is enabled, the
  dashboard page loads without a token and then prompts for one, storing
  it only in that browser tab; every data request it makes is
  authenticated with that token.


## 1.0.16 (2026-06-21)

- Publish container images to Docker Hub as `docker.io/ptweezy/cronstable`
  on every release, in addition to GHCR. The two registries carry the
  same multi-arch (`linux/amd64` + `linux/arm64`) image, so you can
  pull from whichever you prefer.
- Document the Docker Hub images in the README and add a quick-start
  `docker run` example and a Docker Hub badge.
- Harden the release workflow so Docker Hub publishing is enabled only
  when both `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` are configured.


## 1.0.15 (2026-06-21)

- Lower the minimum required Python version from 3.13 to 3.10;
  cronstable now supports Python 3.10, 3.11, 3.12, 3.13, and 3.14.
- Add PyPI trove classifiers for Python 3.10, 3.11, and 3.12 so the
  expanded support is reflected on the package page.
- Expand the test matrix (`tox` and CI) to run across all five
  supported interpreters (3.10â€“3.14).
- Type-check with `mypy` against Python 3.10 so stdlib APIs that are
  unavailable on the lowest supported interpreter are caught at lint
  time rather than at runtime.


## 1.0.14 (2026-06-21)

Since 1.0.13, the net changes are entirely build/CI hardening (a new `build.yml`, an `arm64` addition to `docker.yml`). Here's the changelog body:

- Add a per-commit build-verification workflow that builds the wheel,
  `sdist`, and self-contained PyInstaller binaries for Linux (both
  glibc and musl/Alpine, on `amd64` and `arm64`) and macOS (`amd64`
  and `arm64`) on every push, without publishing, so a broken build or
  bundle is caught at commit time instead of only at release.
- Build-verify the Docker image for both `linux/amd64` and
  `linux/arm64` on every commit, catching arm64-only breakage (such as
  a dependency with no arm64 wheel) that the previous amd64-only check
  would miss.

## 1.0.13 (2026-06-20)

### Improvements

- Update the bundled Python runtime in the standalone binaries to
  `3.13.14` (from `3.13.5`), picking up the latest upstream bug and
  security fixes.
- Expand the PyPI package metadata with additional keywords, trove
  classifiers, and project links (`Documentation`, `Source`,
  `Changelog`, `Issues`, and `Container`) for easier discovery.

### Documentation

- Tidy up `README.md`, trimming redundant badges and condensing the
  macOS code-signing notes.


## 1.0.12 (2026-06-20)

- Update the GitHub Actions used to build and publish Docker images
  (`docker/metadata-action`, `docker/login-action`,
  `docker/setup-qemu-action`, `docker/setup-buildx-action`, and
  `docker/build-push-action`) to their latest major versions.
- Update the release workflow's `actions/upload-artifact`,
  `actions/download-artifact`, and `softprops/action-gh-release`
  actions to their latest major versions.


## 1.0.11 (2026-06-20)

- The macOS binaries are now Developer ID code-signed and notarized by
  Apple, so Gatekeeper accepts them and they run without first clearing
  the quarantine attribute (`xattr -d com.apple.quarantine` is no longer
  needed).


## 1.0.10 (2026-06-20)

- Release binaries now include macOS builds for both Apple Silicon
  (`cronstable-macos-arm64`) and Intel (`cronstable-macos-amd64`),
  alongside the existing Linux glibc and musl binaries. As with the
  Linux binaries, Python is not required on the target machine.
- Document clearing the macOS Gatekeeper quarantine with
  `xattr -d com.apple.quarantine` before first running the macOS
  binaries, which are unsigned and unnotarized.
- Fix a typo in the README fork attribution.


## 1.0.9 (2026-06-20)

### Documentation

- Document that the standalone binary is self-extracting: on each
  start it unpacks its embedded Python runtime into a temporary
  directory, so it requires a temp directory that is both writable
  and executable.
- Add guidance for running the binary under a read-only root
  filesystem â€” mount a small `rw,exec` tmpfs at `/tmp` (Docker's
  `--tmpfs` defaults to `noexec`, which fails), use a Kubernetes
  `emptyDir`, or point `TMPDIR` at a writable, executable directory.
- Clarify that this temp-directory requirement is unique to the
  standalone binary: the published container image and `pip`/`pipx`
  installs run cronstable as a normal Python package and need no
  writable temp directory.

### Container image

- The official multi-arch (`linux/amd64` + `linux/arm64`) container
  image is now built and published to GHCR automatically as part of
  every release, and is build-checked on every commit so a broken
  `Dockerfile` fails fast.


## 1.0.8 (2026-06-20)

- Add self-contained musl binaries to every release for Alpine and
  other musl-based systems: `cronstable-linux-amd64-musl` and
  `cronstable-linux-arm64-musl`, alongside the existing glibc
  `cronstable-linux-amd64` and `cronstable-linux-arm64` builds. Python is
  not required on the target system.
- Build the release binaries with Python 3.14.


## 1.0.7 (2026-06-20)

- GitHub Releases now use the curated `HISTORY.md` section for the
  release as the body of the release notes. The matching `## X.Y.Z`
  entry is extracted and shown above GitHub's auto-generated "What's
  Changed" list and changelog compare link, so each release page leads
  with the human-written changelog instead of only auto-generated
  notes.


## 1.0.6 (2026-06-20)

- Release binaries are now published for both `linux/amd64` and
  `linux/arm64`. Every GitHub Release attaches a self-contained
  `cronstable-linux-amd64` and `cronstable-linux-arm64` executable, each built
  natively on its target architecture (previously only a single binary
  was provided).
- The downloadable binaries embed Python, so none is required on the
  target system, and run on any Linux host with glibc 2.39 or newer
  (e.g. Ubuntu 24.04) matching the CPU architecture.
- Each binary is smoke-tested with `--version` and built before
  publishing


## 1.0.5 (2026-06-20)

- docker builds

## 1.0.4 (2026-06-19)

### Reliability fixes

- Config reload failures no longer risk crashing the scheduler: if
  re-reading the configuration fails (for example, a YAML error introduced
  while cronstable is running), the previously-loaded jobs keep running
  instead of the main loop failing on an unset `config` reference.
- A job whose command cannot be launched (for example, the executable does
  not exist) is now reported as an ordinary failure with exit code `127`,
  instead of raising `RuntimeError("process is not running")` and being
  logged as an internal "please report this as a bug" error.
- statsd reporting is now strictly best-effort: a failure to send the
  `job_started`/`job_stopped` metrics (for example, an unresolvable statsd
  host) is logged as a warning instead of propagating out of job
  start/stop.
- The mail reporter now always closes its SMTP connection, even when
  `STARTTLS`, login, or sending fails, so a misbehaving mail server can no
  longer leak one connection per report.
- Sentry and e-mail reporting no longer raise `KeyError` when the DSN or
  password is configured with `fromEnvVar` but the environment variable is
  unset; cronstable logs an error and skips that report instead.

### Configuration

- The Sentry `fingerprint` setting now replaces rather than appends when
  merging `defaults`: a job (or `defaults` block) that defines its own
  `fingerprint` overrides the default entirely, so custom Sentry issue
  grouping works as configured (previously the three default entries were
  silently prepended).
- `include` cycles are now detected and rejected with a clear `ConfigError`
  ("include cycle detected") instead of recursing until a `RecursionError`.
- Jobs loaded from a configuration directory are now processed in sorted
  filename order, so job ordering and "first config found" messages are
  deterministic rather than dependent on the filesystem's directory order.
- Environment files (`env_file`) are now read as UTF-8.

### Security

- The web API's `Authorization` check now treats the `Bearer` auth scheme
  as case-insensitive (per RFC 7235), while still comparing the token
  itself in constant time.
- The mail reporter no longer logs the name of the configured password
  environment variable.

### Internal

- Refactored `JobConfig` construction into focused helper methods and
  switched `send_to_statsd` to `asyncio.get_running_loop()`; no behavioral
  change.
- Added a `.github/CODEOWNERS` file.

## 1.0.3 (2026-06-19)

This is a tooling and documentation release; there are no changes to the
`cronstable` package itself.

### Release automation & CI

- Added an opt-in, marker-driven `release` GitHub Actions workflow: a push to
  `main` whose commit message carries a release marker on its own line
  (`[release]` / `[release:major|minor|patch]`), or a manual run, gates on
  `tox`, builds at the next version, publishes to PyPI via Trusted Publishing
  (OIDC), and only after a successful publish tags the commit and cuts a GitHub
  Release.
- Hardened the release trigger to match a whole-line marker, so a `[release]`
  mention inside prose never triggers a publish, and added a local `commit-msg`
  hook (`scripts/gen_changelog_entry.py`) that drafts a changelog entry for
  release commits.
- Set least-privilege `permissions: contents: read` defaults on the `tox` and
  `release` workflows.

### Docs

- Added `CONTRIBUTING.md` documenting the development setup, the
  test/lint/type-check workflow, and the release process, and linked it from the
  README.
- Converted the changelog from reStructuredText to Markdown (`HISTORY.rst` ->
  `HISTORY.md`) and pointed the changelog generator, the `commit-msg` hook, and
  `CONTRIBUTING.md` at the Markdown changelog.

### Packaging

- Promote the PyPI `Development Status` classifier from `4 - Beta` to
  `5 - Production/Stable` to reflect the stable 1.0 release series. No code
  changes.

## 1.0.1 (2026-06-19)

### Security & behavior fixes

- The web API now fails closed when `web.authToken` is configured but
  resolves to an empty token (an unset `fromEnvVar`, or an empty/missing
  `fromFile`): cronstable raises a `ConfigError` and refuses to start the
  HTTP server, instead of silently serving the control API without
  authentication.
- The web API now honors `enabled: false`. `POST /jobs/<name>/start`
  returns `409 Conflict` for a disabled job rather than launching it, and
  `GET /status` reports such jobs as `disabled` instead of an
  inapplicable `scheduled (in N seconds)`.
- Invalid `web.listen` URLs (an unsupported scheme, or an `http` url
  missing host/port) are now logged as a warning and skipped, instead of being
  surfaced as an internal "please report this as a bug" error; a bind failure
  (`OSError`) on one address likewise no longer aborts the whole config
  update. The "started listening" message is logged only after the bind
  actually succeeds.
- `concurrencyPolicy: Replace` no longer reports the replaced (cancelled)
  job instance as a failure and no longer schedules retries for it; the forced
  termination is treated as a replacement, not a job failure.

### Cleanups

- Removed a dead Windows event-loop branch from `main()` (cronstable is
  POSIX-only because it imports `grp`/`pwd` at load time).
- `naturaltime` no longer relies on an `assert` for control flow (which
  would be stripped under `python -O`).
- The concurrency-policy test was rewritten to be deterministic (it was
  previously an `xfail` that could never exercise a second job instance).

## 1.0.0 (2026-06-19)

### About this release

- cronstable 1.0.0 is the first release of the cronstable fork, based on
  gjcarneiro/yacron 0.19. It carries forward all of upstream yacron's
  functionality and adds modernized packaging, a Python 3.13+ runtime, new
  web-API authentication, and a set of security and correctness fixes.
- The project, package, command, config directory, and reporter environment
  variables have all been renamed from `yacron` to `cronstable` (see Breaking
  changes for migration steps).

### Breaking changes

- The installed command and PyPI distribution are renamed `yacron` ->
  `cronstable` (install with `pip install cronstable`; run `cronstable`). The
  Python import package is now `cronstable` and the entry point is
  `cronstable.__main__:main`.
- The default config directory changed from `/etc/yacron.d` to
  `/etc/cronstable.d`; operators relying on the default path must move their
  config directory.
- Minimum Python is now 3.13 (`requires-python >=3.13`); only Python 3.13 and
  3.14 are supported. Python 3.7 through 3.12 are no longer supported.
- Reporter shell environment variables were renamed `YACRON_*` ->
  `CRONSTABLE_*` (e.g. `CRONSTABLE_JOB_NAME`, `CRONSTABLE_RETCODE`). Existing
  `onFailure`/`onSuccess` shell scripts must be updated.
- mail `validate_certs` now defaults to `True`, so SMTP TLS certificate
  validation is enabled unless explicitly disabled. Delivery to servers with
  self-signed/invalid certificates that previously worked silently will now
  fail unless `validate_certs: false` is set.
- Privilege drop now drops/sets supplementary groups (`os.initgroups` /
  `os.setgroups`) before `setuid`, fixing a privilege-escalation bug where
  root's supplementary group memberships leaked into the child. A numeric
  `user` without an explicit `group` now derives its primary gid from the
  passwd database instead of silently keeping yacron's gid 0.
- `defaults.environment` now merges by key instead of concatenating: a job
  overriding a default variable yields a single entry. Configs relying on the
  old duplicate-key concatenation behave differently.
- Dependency pins changed: `crontab` jumped from `==0.22.8` to `>=1,<2`
  (major version change), `strictyaml` to `>=1.7,<2`, `aiohttp` to
  `>=3.10,<4`, `aiosmtplib` to `>=3,<6` (v2+ login API), `sentry-sdk`
  to `>=2,<3`. `pytz` and the direct `ruamel.yaml` pin were dropped;
  `tzdata>=2024.1` was added.

### Features & behavior

- New `web.authToken` option adds opt-in bearer-token authentication to the
  HTTP API (literal `value`, `fromFile`, or `fromEnvVar`); when set, an
  aiohttp middleware requires `Authorization: Bearer <token>` on every route,
  compares it in constant time (`hmac.compare_digest`), and returns 401
  otherwise.
- New `web.socketMode` option sets octal permissions on `unix://` listen
  sockets, logging a warning rather than failing on invalid values; non-unix
  schemes are ignored.
- Job stderr is now written to the process's stderr instead of stdout, so
  operators separating cronstable's own stdout/stderr streams get correctly routed
  output.
- Config now validates numeric ranges at load time and raises a clear
  `ConfigError` for invalid values (`saveLimit>=0`, `maxLineLength>0`,
  `killTimeout>=0`, `executionTimeout>0`, and `onFailure.retry`
  constraints) instead of failing obscurely at runtime.
- Multi-file config directories now aggregate jobs, defaults, and logging
  across all files instead of using only the last file's settings. Duplicate
  `web` or `logging` blocks across the directory raise a `ConfigError`,
  an empty/all-skipped directory yields an empty config (no
  `UnboundLocalError`), and a missing/unreadable single config file now
  raises a clear `ConfigError`.
- Logging configuration is now re-applied on reload when it changes and is only
  marked applied on success, so a logging section fixed after an error or
  changed at runtime is picked up without a restart.
- Scheduling a retry for a job that was removed from the configuration
  mid-retry no longer crashes; the stale retry state is cleared and the retry
  is skipped.
- Job stop metrics (statsd `job_stopped`) are now emitted exactly once per
  run; a guard makes `_on_stop` idempotent, preventing duplicate metrics when
  `cancel` races `wait` (e.g. `concurrencyPolicy=Replace`).
- Non-UTF-8 job output no longer crashes the stream reader (output is decoded
  with `errors='replace'`).
- A job with an empty environment list now gets its environment assigned
  correctly (previously left `None`).
- Email reports now set an RFC 5322 `Date` header
  (`email.utils.format_datetime`), encode HTML bodies with the correct
  charset/transfer-encoding (`set_content` subtype `html`), and call
  `smtp.login` positionally for aiosmtplib v2+ compatibility.
- The Sentry client is now initialized once per `(dsn, environment)` and
  cached instead of on every report, and uses `sentry_sdk.new_scope()`
  (replacing the deprecated `push_scope()`).
- Report templates (sentry/mail body, subject, fingerprint) are now compiled
  and cached via an `lru_cache`, and the three report blocks (`onFailure`,
  `onPermanentFailure`, `onSuccess`) deep-copy their defaults so they no
  longer alias one shared mutable object.
- The shell reporter now logs a nonzero reporter exit code via `logger.error`
  (clean message) instead of `logger.exception` (which logged a bogus
  `NoneType: None` traceback).
- statsd UDP errors are now logged with their detail (`UDP error received:
  %s`) instead of being dropped due to a missing format placeholder.

### Python & runtime

- Timezone handling migrated from third-party `pytz` to the standard-library
  `zoneinfo`; invalid timezones now raise `ConfigError`.
- Added `tzdata>=2024.1` so `zoneinfo` can resolve timezones on
  slim/minimal container images that don't ship the system tz database.
- The asyncio event loop is now created with `asyncio.new_event_loop()`
  instead of the deprecated `asyncio.get_event_loop()` (carried from
  upstream).
- Internal logger and argparse program name updated to `cronstable`; CLI
  error/version output now reads `cronstable`.

### Packaging & build

- Migrated from legacy `setup.py`/`setup.cfg` to a PEP 621
  `pyproject.toml` using the setuptools build backend (`setuptools>=77`,
  `setuptools_scm>=8`); `setup.py` and `setup.cfg` were removed.
- Versioning continues via setuptools_scm, now configured under
  `[tool.setuptools_scm]` writing `cronstable/version.py`.
- Adopted a PEP 639 SPDX license expression (`license = "MIT"`) with
  `license-files`, and updated the LICENSE with a `Copyright (c) 2026, the
  cronstable developers` line alongside the original 2019 copyright.
- Added a `[project.optional-dependencies]` `dev` extra (mypy,
  mypy-extensions, pytest, pytest-asyncio, pytest-cov, ruff, tox) and trimmed
  `requirements_dev.txt` to match (dropped flake8, types-pytz, and stale
  pins; added ruff).
- Consolidated mypy and pytest configuration into `pyproject.toml` and bumped
  the black/ruff target-version to `py313`.
- `MANIFEST.in` and packaging metadata updated for the `README.rst` ->
  `README.md` switch.

### CI & tooling

- Removed Travis CI configuration (`.travis.yml`).
- Switched linting from black + flake8 to ruff (`ruff check` + `ruff
  format`) with bugbear/mccabe/pycodestyle/pyflakes/import-sorting rules and a
  mccabe complexity limit; added a bandit config and a
  `.pre-commit-config.yaml` running bandit and ruff hooks (carried from
  upstream).
- Modernized the GitHub Actions tox workflow: bumped `actions/checkout` (v3
  -> v7) and `actions/setup-python` (v3 -> v6.2.0), renamed the lint job, and
  trimmed the test matrix to Python 3.13 and 3.14.
- Modernized `tox.ini` (envlist `py313, py314, lint, mypy`), removed the
  Travis mapping section, added `skip_install` to the lint/mypy envs, dropped
  `types-pytz` from the mypy env, and pointed lint/mypy commands at the
  `cronstable` package.
- Bumped pre-commit hook revisions (ruff-pre-commit and bandit).

### Docs & examples

- Converted the README from reStructuredText to Markdown (`README.rst` ->
  `README.md`) and rebranded it to cronstable, with a new intro noting it is a
  fork of gjcarneiro/yacron continuing from 0.19. The content is otherwise the
  same as upstream 0.19, not a rewrite; install docs now require Python >=
  3.13, the prebuilt binary targets glibc 2.39 / Ubuntu 24.04, and releases
  come from github.com/ptweezy/cronstable.
- `HISTORY.rst` gained a fork-attribution preamble; older entries are
  retained as upstream yacron history.
- Modernized the Docker example: base image `python:3.14-slim` with `pip
  install cronstable` (replacing ubuntu:xenial + virtualenv), config copied into
  `/etc/cronstable.d`, and `ENTRYPOINT ['cronstable']`.
- Updated the Kubernetes example to the `apps/v1` Deployment API with the
  now-required selector, rebranded `yacrondemo` -> `cronstabledemo`.
- Rebranded the ad-hoc example config directory, example tab file, PyInstaller
  spec/launcher, and listen socket paths (`/tmp/yacron.sock` ->
  `/tmp/cronstable.sock`) to cronstable.

### Credits (trailing upstream changes)

- `web.headers` option to control HTTP response headers on all web
  endpoints, by Gustavo Carneiro (gjcarneiro), commit bde0f0b; merged upstream
  but never released in yacron 0.19.0.
- Python 3.14 compatibility, including `asyncio.new_event_loop()` and
  modern-Python lint/format fixes, by Gustavo J. A. M. Carneiro (gjcarneiro),
  commit 27a32bc (#100).
- Switch from black/flake8 to ruff, plus bandit and pre-commit configuration,
  by Gustavo Carneiro (gjcarneiro), commits c656fa6 and 4f7936a.
- Removal of Travis CI and modernization of the Python/PyInstaller version
  matrices, by upstream yacron (gjcarneiro), commits d9b1ca6, 8d28816, 4e6892a,
  2941dcf.
- README logging example fix adding `datefmt: '%Y-%m-%d %H:%M:%S'` to the
  custom-logging formatter, by andreas-wittig, commit 931b186.

## 0.19.0 (2023-03-11)

- Add ability to configure yacron's own logging (#81 #82 #83, gjcarneiro, bdamian)
- Add config value for SMTP(validate_certs=False) (David Batley)

## 0.18.0 (2023-01-01)

- fixes "Job is always executed immediately on yacron start" (#67)
- add an `enabled` option in jobs (#73)
- give a better error message when no configuration file is provided or exists (#72)

## 0.17.0 (2022-06-26)

- Support Additional Shell Report Vars (RJ Garcia)
- Shell reporter: handle long lines truncatation (Hannes Hergeth)
- exe: undo pyinstaller LD_LIBRARY_PATH changes in subprocesses (#68, Gustavo Carneiro)

## 0.16.0 (2021-12-05)

- make the capture max line length configurable and change the default
  from 64K to 16M (#56)
- Add config option to change prefix of subprocess stream lines (#58, eelkeh)

## 0.15.1 (2021-11-19)

- Fix a bug in the --validate option (#57, Leonid Repin)

## 0.15.0 (2021-11-10)

- Allow emails to be html formatted
- Fix an error when reading cmd output with huge lines (#56)

## 0.14.0 (2021-10-04)

- Sentry: increase the size of messages before getting truncated #54
- Sentry: allow specifying the environment option #53
- Minor fixes

## 0.13.1 (2021-08-10)

- unicode fixes for the exe binary version

## 0.13.0 (2021-06-28)

- Add ability for one config file to include another one #38
- Add shell command reporting ability (Hannes Hergeth, #50)

## 0.12.2 (2021-05-31)

- constrain ruamel.yaml to version 0.17.4 or below, later versions are buggy

## 0.12.1 (2021-05-30)

- blacklist ruamel.yaml version 0.17.5 in requirements #47

## 0.12.0 (2021-04-22)

- web: don't crash when receiving a web request without Accept header (#45)
- add env_file configuration option (Alessandro Romani, #43)
- email: add missing Date header (#39)

## 0.11.2 (2020-11-29)

- Add back a self contained binary, this time based on PyInstaller

## 0.11.1 (2020-07-29)

- Fix email reporting when multiple recipients given

## 0.11.0 (2020-07-20)

- reporting: add a failure reason line at the top of sentry/email (#36)
- mail: new tls, startls, username, and password options (#21)
- allow jobs to run as a different user (#18)
- Support timezone schedule (#26)

## 0.10.1 (2020-06-02)

- Minor bugfixes

## 0.10.0 (2019-11-03)

- HTTP remote interface, allowing to get job status and start jobs on demand
- Simple Linux binary including all dependencies (built using PyOxidizer)

## 0.10.0b2 (2019-10-26)

- Build Linux binary inside Docker Ubuntu 16.04, so that it is compatible with
  older glibc systems

## 0.10.0b1 (2019-10-13)

- Build a standalone Linux binary, using PyOxidizer
- Switch from raven to sentry-sdk

## 0.9.0 (2019-04-03)

- Added an option to just check if the yaml file is valid without running the scheduler.
- Fix missing `body` in the schema for sentry config

## 0.8.1 (2018-10-16)

- Fix a bug handling `@reboot` in schedule (#22)

## 0.8.0 (2018-05-14)

- Sentry: add new `extra` and `level` options.

## 0.7.0 (2018-03-21)

- Added the `utc` option and document that times are utc by default (#17);
- If an email body is empty, skip sending it;
- Added docker and k8s example.

## 0.6.0 (2017-11-24)

- Add custom Sentry fingerprint support
- Ability to send job metrics to statsd (thanks bofm)
- `always` flag to consider any cron job that exits to be failed
  (thanks evanjardineskinner)
- `maximumRetries` can now be `-1` to never stop retrying (evanjardineskinner)
- `schedule` can be the string `@reboot` to always run that cron job on startup
  (evanjardineskinner)
- `saveLimit` can be set to zero (evanjardineskinner)

## 0.5.0

- Templating support for reports
- Remove deprecated smtp_host/smtp_port

## 0.4.3 (2017-09-13)

- Bug fixes

## 0.4.2 (2017-09-07)

- Bug fixes

## 0.4.1 (2017-08-03)

- More polished handling of configuration errors;
- Unit tests;
- Bug fixes.

## 0.4.0 (2017-07-24)

- New option `executionTimeout`, to terminate jobs that get stuck;
- If a job doesn't terminate gracefully kill it.  New option `killTimeout`
  controls how much time to wait for graceful termination before killing it;
- Switch parsing to strictyaml, for more user friendly parsing validation error
  messages.
