# Architecture and Internals

Internal design reference for developers reading or extending yacron2. It maps
the modules, describes the single-threaded asyncio event loop, the scheduler
main loop and hot reload, the running-job lifecycle, the retry state machine,
concurrency handling, and signal-driven shutdown. It references functions by
name rather than repeating the option reference; see
[Configuration Reference](Configuration-Reference) for option semantics.

## Module map

yacron2 is an asyncio Python daemon that runs natively on Linux, macOS, and
Windows. All OS-specific behavior is isolated in `yacron2/platform.py`
(`DEFAULT_SHELL`, `DEFAULT_CONFIG_PATH`, `supports_unix_sockets`, `encode_argv`,
`install_shutdown_handlers`, plus the `IS_WINDOWS` flag). `grp`/`pwd` and per-job
user/group switching remain POSIX-only and are runtime-gated on `IS_WINDOWS`
rather than blocking import. See [Running on Windows](Running-on-Windows) for the
operator-facing walkthrough.

| Module | Responsibility |
| --- | --- |
| `yacron2/__main__.py` | CLI entry point. Argument parsing, basic logging setup, event-loop creation, shutdown-handler registration (delegated to `platform.install_shutdown_handlers`), and process exit codes. |
| `yacron2/platform.py` | The single home for all per-OS branches: `DEFAULT_SHELL`, `DEFAULT_CONFIG_PATH`, `supports_unix_sockets`, `encode_argv`, `install_shutdown_handlers`, and the `IS_WINDOWS` flag. The rest of the codebase reads the same on every platform. |
| `yacron2/config.py` | strictyaml `CONFIG_SCHEMA`, `DEFAULT_CONFIG`, `_REPORT_DEFAULTS`; config loading from a file or directory; include handling and dict merging (`mergedicts`); `JobConfig` parsing/validation; `Yacron2Config` dataclass. |
| `yacron2/cron.py` | `Cron` class: scheduler main loop (`Cron.run`), hot reload (`update_config`), the aiohttp web app (`start_stop_web_app` and handlers), due-job spawning (`spawn_jobs` / `job_should_run` / `launch_scheduled_job` / `maybe_launch_job`), the job reaper (`_wait_for_running_jobs`), and retry orchestration (`handle_job_failure` / `schedule_retry_job` / `cancel_job_retries`). |
| `yacron2/job.py` | `RunningJob` lifecycle (subprocess launch, privilege drop, wait, stream capture), `StreamReader`, the `Reporter` implementations (`SentryReporter`, `MailReporter`, `ShellReporter`, `WebhookReporter`), and `JobRetryState`. |
| `yacron2/fingerprint.py` | The order-independent **job-set id**: `canonical_job` (the host-independent, effective per-job representation) and the versioned hashing (`SCHEME_VERSION`). Consumed by `cron.py` (the `/job-set-id` endpoint and startup/reload logging) and by `cluster.py` (peer comparison). |
| `yacron2/leadership.py` | The pluggable-backend seam: the `LeadershipBackend` ABC every leader-gating call in `cron.py` goes through (`start`/`stop`/`is_leader`/`leader_name`/`is_quorate`/`view_dict` plus the defaulted per-job, conflict, `@reboot`, and never-skip `available_*` families), the `LeaseBackend` shared base for the single-holder lease backends, and the `make_backend` factory that builds the one named by `cluster.backend` (`gossip` -> `cluster.ClusterManager`, `kubernetes` -> `backends.kubernetes.KubernetesBackend`, `etcd` -> `backends.etcd.EtcdBackend`) via deferred imports. |
| `yacron2/backends/kubernetes.py` | `KubernetesBackend` (a `LeaseBackend`): a `coordination.k8s.io/v1` `Lease` driven over either the official `kubernetes` client or a hand-rolled apiserver REST transport (`cluster.kubernetes.clientLibrary` chooses `auto`/`library`/`http`). |
| `yacron2/backends/etcd.py` | `EtcdBackend` (a `LeaseBackend`): a lease-backed key/election against etcd's v3 gRPC-gateway JSON/HTTP API, a single fully-portable transport with no optional client library. |
| `yacron2/backends/__init__.py` | Shared backend helpers, notably the pure `select_transport(client_library, native_available, backend)` used by the kubernetes backend to pick its transport (`auto` prefers the native client, `library` requires it, `http` forces the hand-rolled path). |
| `yacron2/cluster.py` | The `gossip` backend: `ClusterManager` (the mTLS `/peer` listener and periodic peer-poll loop) is one concrete `LeadershipBackend`, plus the pure `ClusterView` state machine (per-peer status + drift debounce), the pure `quorum_size`/`elect_leader`/`elect_available_leader` functions, and (for `distribution: spread`) the pure rendezvous-hashing `elect_job_owner`/`elect_available_job_owner`. Imports `config` and `fingerprint`; no dependency on `cron.py`. See [Clustering and Leader Election](Clustering-and-Leader-Election). |
| `yacron2/statsd.py` | `StatsdJobMetricWriter` and the UDP `StatsdClientProtocol` used to emit best-effort statsd metrics. |
| `yacron2/prometheus.py` | `PrometheusMetrics` accumulators plus the hand-rolled text/OpenMetrics exposition renderer behind `GET /metrics`. |
| `yacron2/version.py` | Generated version string (`version`), served by the web `/version` endpoint and printed by `--version`. |

The dependency direction is `__main__` -> `cron` -> (`config`, `job`,
`fingerprint`, `leadership`) -> (`statsd`, `config`). `cron.py` imports
`make_backend` from `leadership`, so the leadership seam fans out as
`cron` -> `leadership` -> {`cluster` (gossip), `backends.kubernetes`,
`backends.etcd`}; those backend modules are imported lazily by `make_backend`,
so `backends/` never enters the import graph unless `cluster.backend` selects a
lease backend. `cluster.py` depends on `config` and `fingerprint` only; both
lease backends depend on `config` and `leadership`; `config.py` has no
dependency on `cron.py` or `job.py`.
`prometheus.py` is a leaf module like `statsd.py`: it imports only
`yacron2.version` at module scope, `cron.py` imports it, and its renderer
late-imports `yacron2.cron` helpers at scrape time to break the cycle.
`platform.py` is a leaf module with no yacron2 dependencies, imported by
`__main__`, `config`, `cron`, and `job` wherever per-OS behavior is needed.

## The event loop

`main()` in `__main__.py` creates a fresh event loop with
`asyncio.new_event_loop()` and runs `main_loop(loop)` inside a `try/finally`
that always calls `loop.close()`. The daemon runs on Windows too; the event loop
is created the same way on every OS, and the only difference is shutdown-handler
wiring via `platform.install_shutdown_handlers` (Windows uses the Proactor loop,
which lacks `add_signal_handler`).

`main_loop` parses arguments (`-c/--config`, `-l/--log-level`,
`-v/--validate-config`, `--version`), calls `logging.basicConfig` at the chosen
level, and constructs `Cron(args.config)`. Construction calls
`Cron.update_config()` once eagerly, so a `ConfigError` at startup is caught in
`main_loop` and turned into exit code 1. With `--validate-config`, a successful
construction logs `"Configuration is valid."` and exits 0 without starting the
loop. With `--version`, the version is printed and the process exits 0 before
any config is loaded.

The whole daemon is single-threaded: one event loop drives the scheduler loop,
the reaper task, all running-job subprocess waits, the reporters, and the
aiohttp web server. Concurrency is cooperative via `await`; there are no worker
threads and no locks (the code comments note that `asyncio` being
single-threaded is what makes certain flag-based guards safe without locking).

Shutdown-handler registration is delegated to `platform.install_shutdown_handlers`,
which returns a cleanup function called in the `finally`. On POSIX it registers
`SIGINT` and `SIGTERM` with `loop.add_signal_handler(...)` bound to
`cron.signal_shutdown`. On Windows `loop.add_signal_handler` raises
`NotImplementedError`, so it falls back to `signal.signal` for `SIGINT` (Ctrl-C)
and `SIGBREAK` (Ctrl-Break / console close), marshalling onto the loop thread
with `call_soon_threadsafe` and ticking a ~0.25s heartbeat timer so the handler
runs while the loop is blocked in IOCP. `loop.run_until_complete(cron.run())`
blocks until the main loop returns. See [Running on Windows](Running-on-Windows).

## `Cron.run`: the scheduler main loop

`Cron.run` first starts the reaper task:

```text
self._wait_for_running_jobs_task = asyncio.create_task(self._wait_for_running_jobs())
```

It then enters `while not self._stop_event.is_set():`. Each iteration performs,
in order:

1. **Hot reload.** `config = self.update_config()` re-reads the configuration
   from disk and rebuilds `self.cron_jobs` (an `OrderedDict` of name ->
   `JobConfig`). On `ConfigError`, the error is logged and `self.cron_jobs` is
   left unchanged, because `update_config` only assigns `self.cron_jobs` on a
   successful parse; the loop keeps running the previously loaded jobs. `config`
   is initialized to `None` at the top of each iteration so that a failed parse
   does not dereference an unbound config later in the body. Any other
   exception is logged as `"please report this as a bug (1)"`.

2. **Cluster start/stop.** `await self.start_stop_cluster(config.cluster_config)`
   runs inside the same `try` as `update_config`, immediately after it, and
   reconciles the leadership backend (`self.cluster_manager`, typed
   `Optional[LeadershipBackend]`, so it may be the gossip `ClusterManager` or a
   `KubernetesBackend`/`EtcdBackend`) against the (possibly changed) `cluster`
   config, mirroring the web-app reconcile: the backend is stopped when the
   section is removed or changed, and (re)started when present. It also records
   `_elect_leader_configured` up front so the leader gate can fail closed even
   when the backend is absent or failed to start. A start failure (bad
   cert/credential files, a bad listen address, a port in use, or a lease store
   the backend cannot reach or authenticate to) is logged and the reload
   continues. The id the backend reports tracks reloads on its own (it calls
   `self.job_set_id` each round), so only a change to the cluster section itself
   needs a restart. (See "Cluster manager" and
   [Clustering and Leader Election](Clustering-and-Leader-Election).)

3. **Web app start/stop.** `await self.start_stop_web_app(config.web_config)`
   reconciles the running aiohttp server against the (possibly changed) web
   config. It runs only when the config parsed, and *after* the cluster
   reconcile, under its **own** error handling: a `ConfigError` (e.g. an
   `authToken` that resolves empty) logs `"Error in the web configuration, so
   not starting the web API"` and leaves only the web API down -- the rest of
   the new config (jobs, cluster, logging) is still applied, so a web
   misconfiguration can never skip the cluster gate (which would fail *open*).
   Any other exception here is logged as `"please report this as a bug (4)"`.
   (See "Web control app".)

4. **Logging config.** If the reloaded config has a non-`None`
   `logging_config` that differs from `applied_logging_config`,
   `logging.config.dictConfig(...)` is applied. It is recorded as applied only
   on success, so a logging section that was broken on a previous reload is
   re-tried and picked up once fixed, without a restart. A failure logs an
   error pointing at the Python `logging.config` dictionary-schema docs and the
   offending config.

5. **Spawn due jobs.** `await self.spawn_jobs(startup)`.

6. **Sleep to the next minute boundary.** `next_sleep_interval()` computes the
   seconds until the next minute boundary in UTC as `now.replace(second=0) +
   WAKEUP_INTERVAL` (where `WAKEUP_INTERVAL` is one minute). Because
   `replace(second=0)` clears only the seconds field, the target retains `now`'s
   sub-second component, so the wake-up lands at the same fractional offset past
   the next minute rather than exactly `:00.000000`. The loop then does
   `await asyncio.wait_for(self._stop_event.wait(), sleep_interval)`, so a
   shutdown signal wakes it immediately; a `TimeoutError` (the normal case)
   means the minute elapsed.

`startup` is `True` only on the first iteration and is set to `False`
immediately after the first `spawn_jobs`. This drives the `@reboot` startup
pass (see below).

### Startup pass (`@reboot`)

`spawn_jobs(startup)` iterates `self.cron_jobs.values()` and calls
`launch_scheduled_job(job)` for every job where both `job_should_run(startup,
job)` **and** `self._cluster_allows(job)` are true (and first logs any
leadership transition via `_log_cluster_role`). `_cluster_allows` is always
`True` unless `cluster.electLeader` is configured; then it consults the job's
`clusterPolicy` against the elected-leader state: `EveryNode` always runs;
`Leader`/`PreferLeader` fail closed when no backend is running (except
`PreferLeader`, which is never-skip and runs anyway). Otherwise, under the
default `distribution: single-leader` it gates on `is_available_leader()` for
`PreferLeader` and, for `Leader`, checks `has_conflict()` **first** (fail closed
if true, see below) and then `is_leader()`. Under `distribution: spread` the
same shape applies to the per-job variants (`is_available_job_owner(name)` for
`PreferLeader`, then `has_conflict()`/`is_job_owner(name)` for `Leader`), so
leader-gated work fans out across the quorate nodes by rendezvous hashing. The
`has_conflict()` fail-closed-first case stands `Leader` jobs down whenever a
quorate peer makes the election unsafe (a duplicate `nodeName`, a cluster-size
disagreement, or a coordination-policy conflict); `PreferLeader` is left running
since it already accepts double-runs. A backend read is wrapped in a `try`, and
any exception fails the gate closed (skip this cycle) so a backend bug cannot
kill the unguarded `spawn_jobs` path. Manual (API) triggers and retries go
through `maybe_launch_job` and are **not** gated (though `schedule_retry_job`
re-checks the gate before relaunching a scheduled-job retry). See
[Clustering and Leader Election](Clustering-and-Leader-Election#per-job-policy).
`job_should_run`:

- returns `False` for any disabled job (`job.enabled` is `False`);
- when `startup` is `True`, returns `True` only for jobs whose schedule is the
  string `"@reboot"`, and `False` for all others;
- when `startup` is `False`, evaluates `CronTab` schedules with
  `crontab.test(get_now(job.timezone).replace(second=0))` and returns `True`
  when the current minute matches. Non-`CronTab` schedules (i.e. `"@reboot"`)
  return `False` on non-startup iterations, so a `@reboot` job runs once at
  daemon start and never on the regular cadence.

As `README.md` puts it, `@reboot` "will only run the job when yacron2 is
initially executed."

### Shutdown sequence

When `_stop_event` is set the `while` loop exits and `Cron.run` logs
`"Shutting down (after currently running jobs finish)..."`, then:

1. Drains pending retries: while `self.retry_state` is non-empty, it
   `cancel_job_retries(name)` for every entry concurrently via
   `asyncio.gather`.
2. If a leadership backend is running, logs `"Stopping cluster manager"` and
   `await self.cluster_manager.stop()` (which releases leadership best-effort
   for fast failover: gossip cancels the poll loop and tears down the mTLS
   `/peer` listener; a lease backend cancels its renew loop and releases its
   lease). Leadership is released *before* the running-job drain below: the
   drain is unbounded, and holding leadership through it would stall every
   `Leader` job cluster-wide until the slowest local job finishes. The
   trade-off is confined to the still-draining jobs: the new owner may start
   one of those while it finishes here -- the same overlap a crash produces.
   (Retries were all cancelled in step 1, so no retry task consults the
   stopped manager.)
3. `await self._wait_for_running_jobs_task` awaits the reaper, which only
   returns once `self.running_jobs` is empty and the stop event is set.
4. If a web server is running, logs `"Stopping http server"` and
   `await self.web_runner.cleanup()`.

`signal_shutdown` simply calls `self._stop_event.set()`; it is invoked from the
platform shutdown handler (`SIGINT`/`SIGTERM` on POSIX, Ctrl-C/Ctrl-Break on
Windows; see "The event loop"). Note that `handle_job_failure` early-returns
when `_stop_event.is_set()`, so jobs that finish during shutdown are not reported
as failures and do not schedule new retries.

## Configuration hot reload

`update_config` is the single point of reload. When `config_arg` is `None`
(the unit-test path) it returns an empty `Yacron2Config`. Otherwise it calls
`parse_config(self.config_arg)`, which dispatches on whether the argument is a
directory (`_parse_config_dir`) or a single file (`parse_config_file`). On
success it overwrites `self.cron_jobs` with a fresh `OrderedDict` keyed by job
name and returns the full `Yacron2Config` (jobs, web config, job defaults,
logging config). Because reload happens once per minute at the top of the loop,
config edits take effect within a minute without a restart. Schedule parsing,
include merging, and defaults application all happen inside `parse_config*`; see
[Includes, Defaults, and Multi-File Config](Includes-and-Defaults).

## Web control app

`start_stop_web_app(web_config)` reconciles a single `aiohttp` `AppRunner`:

- If a runner exists and the new `web_config` is `None` or differs from the
  currently applied `self.web_config`, the old server is cleaned up and
  `self.web_runner` is reset to `None`.
- If a `web_config` with a non-empty `listen` list exists and no runner is
  running, a new `web.Application` is built. When `authToken` is configured,
  `_resolve_web_token` resolves the bearer token from exactly one source
  (`value`, `fromFile`, or `fromEnvVar`) and raises `ConfigError` if it
  resolves to empty (fail-closed); `_make_auth_middleware` then enforces a
  case-insensitive `Bearer` scheme and a constant-time `hmac.compare_digest`
  token comparison. Routes are `GET /version`, `GET /status`, and
  `POST /jobs/{name}/start`. Each `listen` URL is turned into a site by
  `web_site_from_url` (TCP for `http://host:port`, Unix socket for
  `unix://path`); a malformed URL or a bind error logs a warning and skips that
  address rather than aborting the reload. `socketMode` is applied to
  `unix://` sockets via `_apply_socket_mode`.

On Windows, `unix://` listeners are unsupported (the Proactor loop lacks
`create_unix_server` / aiohttp's `UnixSite`), so such a URL is skipped (gated via
`platform.supports_unix_sockets`) with the warning `"Ignoring web listen url
<url>: unix-socket listeners are not supported on this platform"`; use an
`http://` listener instead. Since `socketMode` only ever applies to unix sockets,
it is irrelevant on Windows. See [Running on Windows](Running-on-Windows).

See [HTTP Control API](HTTP-API) for the request/response contract.

## Cluster manager

Leader election sits behind a pluggable-backend seam. The
scheduler never talks to a concrete cluster implementation directly: it only ever
asks *am I allowed to run this job?* through a handful of methods on whatever
object `cluster.backend` selected. That seam is the `LeadershipBackend` ABC in
`yacron2/leadership.py`, and `make_backend(cluster_config, get_job_set_id)` is the
factory that builds the chosen one (via deferred imports, so a lease backend
never enters the import graph for the common gossip case):

- **`gossip`** (default) -> `cluster.ClusterManager`, the original mTLS,
  no-shared-state, best-effort quorum election (detailed below). Zero new
  dependencies.
- **`kubernetes`** -> `backends.kubernetes.KubernetesBackend`, a
  `coordination.k8s.io/v1` `Lease`. Fenced, exactly-once while the lease store is
  reachable.
- **`etcd`** -> `backends.etcd.EtcdBackend`, a lease-backed key/election against
  an etcd cluster, same fenced guarantee.

The `LeadershipBackend` surface is split three ways so a new lease backend stays
tiny: the *core abstract* methods every backend implements (`start`, `stop`,
`is_leader`, `leader_name`, `is_quorate`, `view_dict`); *defaulted* bodies a
single-holder lease backend inherits unchanged (per-job ownership collapses to
the leader, there are no gossip-style conflicts, the cluster is logically size 1,
the view is never mid-convergence, TLS rotation does not apply); and the
*never-skip* `available_*` family, defaulted to the locked lease semantics (a
node that currently cannot reach the store runs a `PreferLeader` job anyway,
while a node that can see the holder defers; `Leader` stays fail-closed). The
`@reboot` "already ran" defaults (`reboot_ran` false, `mark_reboot_ran` a no-op)
are the one pair `LeaseBackend` **replaces** rather than inherits: it persists
the ran-set in the lease store (a Lease annotation / etcd sibling key under
`REBOOT_RAN_KEY`), scoped to the job-set id, so a *failover* holder does not
re-run a one-shot (see
[Clustering and Leader Election](Clustering-and-Leader-Election)). Gossip
overrides every defaulted method with its richer behaviour, so the gossip path
is byte-identical to before the seam existed. The two lease backends share
`LeaseBackend`, which pins `distribution` to `"single-leader"` and provides the
common lease-shaped `view_dict()`; both talk to their store over plain HTTP via
the core `aiohttp` dependency (no grpc/protobuf wheels), keeping the wide
architecture coverage intact. See
[Clustering and Leader Election](Clustering-and-Leader-Election) for the
operator-facing model and the lease backends' fencing.

`start_stop_cluster(cluster_config)` reconciles the single backend held in
`self.cluster_manager` (typed `Optional[LeadershipBackend]`), mirroring the
web-app reconcile: the backend is stopped when the `cluster` section is removed
or differs from the running one, and (re)started (via `make_backend` inside a
`try`) when present and none is running. A start failure is caught as `OSError`,
`ssl.SSLError`, `ValueError`, `ConfigError`, `aiohttp.ClientError`, or
`asyncio.TimeoutError`, logged, and the reload continues (jobs keep running).
`OSError`/`ssl.SSLError`/`ValueError` cover the gossip case (bad cert files, a bad
listen address, a port already in use); `ConfigError`, `aiohttp.ClientError`, and
`asyncio.TimeoutError` additionally cover a lease backend that cannot reach or
authenticate to its store at `start()` (a `ClientResponseError` on a rejected
token is an `aiohttp.ClientError`, not an `OSError`), so these operational
misconfigurations are logged rather than escaping to the run loop's generic
"please report this as a bug" handler. `_elect_leader_configured` is set first, so
the leader gate is correct even if the backend is absent.

### The gossip backend: `ClusterManager`

The gossip concrete (`ClusterManager` in `yacron2/cluster.py`) owns two things:

- **The mTLS `/peer` listener**: its own `aiohttp` `AppRunner` on the `cluster`
  `listen` address, with a server SSL context that *requires* a CA-signed client
  cert (`ssl.CERT_REQUIRED`). Its `GET /peer` returns the full attestation
  payload (see "The peer attestation payload" below). It also accepts
  `POST /reboot-ran` (the eager `@reboot` push). This listener is entirely
  separate from the public web app.
- **The poll loop**: every `interval` seconds it polls each peer's `/peer` over
  mTLS (a client SSL context with `check_hostname=True`, so the peer cert's SAN
  must match the configured host), and feeds each observation into the pure
  `ClusterView`. TLS/cert failures classify the peer as `untrusted`; connect or
  timeout failures as `unreachable`. The exchange is conditional: every `/peer`
  response carries a strong `ETag` (a content hash of the payload, with the
  live per-job countdown normalised to the absolute next-fire time so the tag
  is stable between fires), the poller echoes it back as `If-None-Match`, and
  an unchanged peer answers with a bodyless `304` -- the poller then replays
  its cached observation with a fresh timestamp, so a converged, idle
  cluster's steady-state round costs headers rather than the full
  O(members + jobs) JSON. A `304` is still a fresh, mutually-authenticated
  round trip, so agreement, conflict detection, and the drift debounce advance
  exactly as if the identical body had been re-sent. Full bodies large enough
  to be worth it are gzip-compressed.

`ClusterView` is pure (no I/O): it holds the per-peer table and the rules that
update it (the `agreed`/`syncing`/`drifted`/`unreachable`/`untrusted`/`self`
state machine and the `driftAfter` debounce), which keeps the logic trivially
testable. Leader election is likewise pure: `quorum_size`, `elect_leader`
(quorum-gated, returns `None` for a minority), and `elect_available_leader` (no
quorum gate, for `PreferLeader`) take a node name, the agreeing peer names, and
the cluster size, and the manager wraps them as `is_leader()` /
`is_available_leader()` / `leader_name()`. The `/cluster` web endpoint serialises
`view_dict()`.

Under `distribution: spread` the single leader is replaced by per-job ownership:
the equally pure `elect_job_owner` (quorum-gated) and `elect_available_job_owner`
pick the owner of a given job via rendezvous (highest-random-weight) hashing
(`_hrw_score` / `_hrw_owner`), and the manager exposes `is_job_owner(name)` /
`is_available_job_owner(name)`. `Cron._cluster_allows` branches on
`manager.distribution` to call the leader or the per-job variant; the choice is
purely about *which node* runs a job, so the quorum gate and the guarantee are
unchanged. Because the owner is a deterministic function of the job name and the
agreeing member set, all quorate nodes agree, and a membership change only
reassigns the affected jobs.

### The peer attestation payload

A gossip node's `GET /peer` handler (`ClusterManager._handle_peer`) returns a
single JSON object that a polling peer feeds into its `ClusterView` and its
election. The full body:

```jsonc
{
  // this node's stable, human-readable identity (the configured nodeName,
  // defaulting to the hostname). The election's key: elect_leader picks the
  // minimum live name, so two nodes sharing this is a conflict_names conflict.
  "node_name": "yacron-a",
  // the order-independent job-set fingerprint (fingerprint.canonical_job);
  // peers with a different value are "drifted" (running a different config).
  "job_set_id": "3f2a...c9",
  // the fingerprint scheme version (fingerprint.SCHEME_VERSION), so a peer on a
  // newer hashing scheme is recognised rather than silently treated as drifted.
  "scheme_version": "v1",
  // a per-process-boot random token: lets a poller distinguish a genuine
  // restart of this node from a second node fraudulently reusing node_name,
  // and dedupes a node reachable at two addresses.
  "instance_id": "b17e...",
  // this node's declared cluster size (len(peers)+1). The election's safety
  // assumes every node shares one N; a peer declaring a different one is a
  // size_conflict (conflicting_sizes) and fails Leader closed.
  "cluster_size": 3,
  // this node's own per-peer observations (itself first, always agreeing), each
  // tagged with node_name, instance_id, and whether it is currently seen
  // agreed, so the poller can confirm the edge is two-way (mutual agreement)
  // and spot a duplicate nodeName transitively (one name, two instance_ids).
  "members": [
    { "node_name": "yacron-a", "instance_id": "b17e...", "agreed": true },
    { "node_name": "yacron-b", "instance_id": "6c40...", "agreed": true }
  ],
  // the peers this node MUTUALLY agrees with (witnessed two-way edges): the
  // poller uses this as sound evidence that a transitively-reached node is
  // itself quorate, driving bridge-discovery deferral.
  "mutual_agreeing": ["yacron-b", "yacron-c"],
  // @reboot one-shots already run in the cluster (this node's, plus any it
  // learned from agreed peers), so a poller can retire its matching deferred
  // job without re-running it. Capped to MAX_ADVERTISED_REBOOT_JOBS.
  "ran_reboot_jobs": ["nightly-migrate"],
  // this node's coordination policy: neither is in the job-set fingerprint, so
  // a peer declaring a different distribution or elect_leader is a
  // policy_conflict (conflicting_policies) and fails Leader closed.
  "distribution": "single-leader",
  "elect_leader": true,
  // the nodes THIS node can confirm are themselves quorate (its eligible
  // candidates): stronger than mutual_agreeing. A poller folds these into its
  // spread Leader-path owner set, so it only ever defers a job to a node
  // vouched able to run it.
  "quorate_vouched": ["yacron-a", "yacron-b", "yacron-c"]
}
```

The `distribution`, `elect_leader`, and `cluster_size` fields make coordination
policy and cluster size part of the attested payload precisely so a disagreement
becomes a detectable conflict rather than a silent split-brain: a differing
`distribution`/`elect_leader` surfaces as `policy_conflict: true`, a differing
`cluster_size` as `size_conflict: true`, and both (like a duplicate `node_name`)
stand `Leader` jobs down through the umbrella `conflict` flag.

**Threat model.** The listener requires a CA-signed client cert
(`ssl.CERT_REQUIRED`) and the poller pins the peer cert's SAN
(`check_hostname=True`), so only a node holding a cert your CA issued can join the
gossip. But mTLS authenticates the *transport*, not the *claims*: every field
above is self-asserted, so a validly-signed node can lie about its own state
(advertise a different `job_set_id`, claim `quorate_vouched` peers it cannot
reach, or reuse another node's `node_name`). This is why the election is
best-effort and why the conflict gates fail `Leader` closed on any disagreement
rather than trusting a single peer's word. The `instance_id` blunts the crudest
fabrication (a second node reusing a `node_name` shows a different `instance_id`,
surfacing the duplicate). Responses are size-capped (`ran_reboot_jobs` truncated
to `MAX_ADVERTISED_REBOOT_JOBS`, the whole body bounded by
`MAX_PEER_RESPONSE_BYTES`) so a peer cannot exhaust a poller's memory with an
inflated payload, and poll failures are bounded by timeouts (classified
`unreachable`) so a slow or hung peer cannot exhaust file descriptors or wedge the
loop. See
[Clustering and Leader Election](Clustering-and-Leader-Election) for the
operator-facing trust boundary.

## The reaper task: `_wait_for_running_jobs`

A single long-lived task started by `Cron.run` owns all completion handling. It
maintains a local `wait_tasks` map from `RunningJob` to an `asyncio.Task`
wrapping `job.wait()`, and runs while `self.running_jobs` is non-empty or the
stop event is not yet set. Each cycle:

1. For every `RunningJob` in `self.running_jobs` that does not yet have a wait
   task, it creates one with `asyncio.create_task(job.wait())`.
2. If there are no wait tasks, it waits up to 1 second on the
   `self._jobs_running` event (set by `maybe_launch_job` whenever a job is
   spawned) and continues, avoiding a busy loop.
3. Otherwise it clears `_jobs_running` and `await asyncio.wait(...,
   timeout=1.0, return_when=FIRST_COMPLETED)`. Completed jobs are removed from
   `wait_tasks`; `task.result()` is read (an unexpected exception is logged as
   `"please report this as a bug (2)"`), then each finished job is passed to
   `_handle_finished_job`.

`CancelledError` is re-raised; any other unexpected exception in the loop is
logged as `"please report this as a bug (3)"` followed by a one-second sleep.

`_handle_finished_job` removes the job from `self.running_jobs[name]` (deleting
the key when the list becomes empty), then:

- If `job.replaced` is `True`, the run was a deliberate `concurrencyPolicy:
  Replace` termination: it is logged as replaced and **not** reported and does
  not trigger retries.
- Otherwise it inspects `job.fail_reason`. `None` -> `handle_job_success`;
  non-`None` -> `handle_job_failure`.

## RunningJob lifecycle

`maybe_launch_job` constructs `RunningJob(job, self.retry_state.get(job.name))`,
calls `await running_job.start()`, appends it to `self.running_jobs[job.name]`,
and sets `self._jobs_running`. The lifecycle inside `RunningJob`:

1. **`start()`** chooses the spawn function from the command form:
   `create_subprocess_exec` for a list command, or when `shell` is set the
   command is run as `[shell, "-c", command]` via `exec`; a string command with
   no `shell` uses `create_subprocess_shell`. The default `shell` is
   platform-specific (via `platform.DEFAULT_SHELL`): POSIX defaults to `/bin/sh`,
   so a string command runs as `["/bin/sh", "-c", command]`; on Windows the
   default is empty, so a string command with no shell goes through
   `create_subprocess_shell` to the native command processor `%ComSpec%`
   (cmd.exe). A list command bypasses the shell on every platform. It assembles
   `env` (only when the job has `environment` entries, layering them over
   `os.environ` after `fixup_pyinstaller_env`), sets `preexec_fn=self._demote`
   when a uid/gid is configured, requests `stdout`/`stderr` PIPEs per
   `captureStdout`/`captureStderr`, sets the stream buffer `limit` to
   `maxLineLength`, and records `execution_deadline = time.perf_counter() +
   executionTimeout` when a timeout is set. Arguments are encoded via
   `platform.encode_argv` before spawning (UTF-8 bytes on POSIX; passed as `str`
   unchanged on Windows for `CreateProcessW`). The `preexec_fn=self._demote` path
   is POSIX-only: per-job user/group switching is not supported on Windows, where
   a job with `user` or `group` set is rejected up front with the configuration
   error `"Job <name>: changing user/group is not supported on Windows"` (gated on
   `IS_WINDOWS` in `config.py`), so `_demote`/`preexec_fn` never applies there. If
   the spawn raises `SubprocessError`, `UnicodeEncodeError`, or
   `FileNotFoundError`, the error is logged, `self.start_failed = True` is set, and
   `start()` returns without a process. On success `_on_start()` emits the statsd
   `job_started` metric (best-effort; `OSError` is caught) and a `StreamReader`
   task is started for each captured stream. See
   [Running on Windows](Running-on-Windows).

2. **`_demote()`** runs in the child (still privileged) and drops privileges in
   the security-critical order: supplementary groups first
   (`os.initgroups(username, gid)` when both are known, else `os.setgroups([])`),
   then `os.setgid(gid)`, then `os.setuid(uid)`. Any failure raises
   `RuntimeError`. (uid/gid resolution and the "must be root" check happen
   earlier in `JobConfig._resolve_user_group`.) This whole privilege-drop path is
   POSIX-only: there is no setuid/setgid model on Windows; on Windows
   `user`/`group` config is rejected up front with a configuration error (see
   `start()` above), so `_demote` is never reached.

3. **`wait()`** is what the reaper awaits. If there is no process but
   `start_failed` is set, it synthesizes `retcode = 127` (conventional
   "command not found"), reads streams, and returns, so a failed launch is a
   normal job failure, not a reaper bug. With no deadline it awaits
   `proc.wait()` and then `_on_stop()`. With a deadline it awaits
   `asyncio.wait_for(proc.wait(), timeout)`; on `TimeoutError` it logs the
   `executionTimeout`, sets `retcode = -100`, and calls `cancel()`. Finally it
   reads the stream buffers.

4. **`cancel()`** sends `SIGTERM` (`proc.terminate()`), waits up to
   `killTimeout` for graceful exit, and `proc.kill()`s (`SIGKILL`) on timeout;
   it then calls `_on_stop()`. The signal mapping is POSIX-specific: on POSIX
   `terminate()` = `SIGTERM` (graceful/trappable) and `kill()` = `SIGKILL`
   (forceful), a real escalation. On Windows there are no POSIX signals: both
   `terminate()` and `kill()` call `TerminateProcess` (immediate, ungraceful, the
   child is not notified to clean up), so the terminate -> kill escalation is
   effectively moot, though `killTimeout` still bounds the wait. `_on_stop()` is
   idempotent (guarded by `self._stopped`) because `cancel()` and `wait()` can
   both reach it for one run (e.g. under `Replace`); it emits the statsd
   `job_stopped` metric once.

5. **Failure classification.** `fail_reason` is a property evaluated against
   `failsWhen`: `always`, then `nonzeroReturn` (`retcode != 0`), then
   `producesStdout`/`producesStderr` (true when captured output is non-empty
   *or* lines were discarded). `failed` is `fail_reason is not None`.

6. **Reporting.** `report_failure`, `report_permanent_failure`, and
   `report_success` each delegate to `_report_common`, which runs all three
   `REPORTERS` (`SentryReporter`, `MailReporter`, `ShellReporter`, `WebhookReporter`) concurrently
   with `asyncio.gather(..., return_exceptions=True)`; an exception from any one
   reporter is logged and does not stop the others. Each reporter reads the
   relevant sub-key of the report config (`onFailure["report"]`,
   `onPermanentFailure["report"]`, or `onSuccess["report"]`) and self-disables
   when its required fields are unset (e.g. mail with no `to`/`from`, sentry
   with no resolvable `dsn`, shell with no `command`). Templates render against
   `template_vars`. See [Reporting (Mail, Sentry, Shell, Webhook)](Reporting).

### StreamReader

Each captured stream gets a `StreamReader` whose `_read` task loops on
`stream.readline()`, decodes UTF-8 with `errors="replace"`, and for live output
prefixes each line with `streamPrefix` (formatted with `job_name`/`stream_name`)
and writes it to the daemon's own `sys.stdout`/`sys.stderr` via `_emit` (bytes
write with an ASCII-replacement fallback). For retention it keeps the first
`saveLimit // 2` lines in `save_top` and the last `saveLimit - saveLimit // 2`
lines in a bounded `save_bottom` deque, incrementing `discarded_lines` for every
evicted or never-saved line. A `ValueError` from an over-long line (exceeding
the buffer `limit`) logs a warning and is skipped. `join()` awaits the reader
task and returns the assembled text (top lines, an optional
`"[.... N lines discarded ...]"` marker, then bottom lines) plus the discard
count. See [Output Capturing](Output-Capturing).

## Retry state machine

Retries are driven by `JobRetryState` (in `job.py`), the
`Cron.retry_state` map (name -> `JobRetryState`), and the
`schedule_retry_job` coroutine.

`JobRetryState` holds `delay` (current, initialized to `initialDelay`),
`multiplier` (`backoffMultiplier`), `max_delay` (`maximumDelay`), a `count` of
retries performed, an optional `task` (the pending `schedule_retry_job` task),
and a `cancelled` flag. `next_delay()` returns the current delay, then advances
it to `min(delay * multiplier, max_delay)` and increments `count` (exponential
backoff capped at `maximumDelay`).

Flow:

- **Arming.** `launch_scheduled_job` first `cancel_job_retries(name)` (asserting
  the name is then absent from `retry_state`), and if `maximumRetries` is
  truthy creates a `JobRetryState(initialDelay, backoffMultiplier,
  maximumDelay)` and stores it under the job name before `maybe_launch_job`. The
  `RunningJob` is given this state via the `retry_state` constructor argument.
- **On failure** (`handle_job_failure`, only when not shutting down): the job's
  output is logged, `report_failure()` runs, then the retry state is examined.
  If there is no state or it is `cancelled`, `report_permanent_failure()` runs
  and the flow stops. Otherwise any prior pending retry `task` is awaited (if
  done) or cancelled, and the cap is checked: when
  `state.count >= maximumRetries and maximumRetries != -1`, retries are
  cancelled (`cancel_job_retries`) and `report_permanent_failure()` runs. A
  `maximumRetries` of `-1` means retry forever (the documented sentinel,
  enforced by `JobConfig._validate_numeric_ranges`). Otherwise a new retry is
  scheduled: `state.task = asyncio.create_task(schedule_retry_job(name,
  state.next_delay(), state.count))`.
- **`schedule_retry_job(name, delay, retry_num)`** logs the planned retry,
  `await asyncio.sleep(delay)`, then re-fetches the job from `self.cron_jobs`.
  If the job has disappeared from the (possibly reloaded) config, it pops the
  stale retry state and returns; otherwise it calls `maybe_launch_job(job)`.
- **On success** (`handle_job_success`): `cancel_job_retries(name)` clears any
  pending retry, then `report_success()` runs.
- **`cancel_job_retries(name)`** pops the state (no-op if absent), sets
  `cancelled = True`, and awaits or cancels the pending `task`.

See [Failure Detection and Retries](Failure-Detection-and-Retries) for the
operator-facing options.

## Concurrency handling and the `replaced` flag

`self.running_jobs` is a `defaultdict(list)` mapping job name to the list of
currently running `RunningJob` instances, so multiple concurrent runs of one
job are tracked. `maybe_launch_job` consults `concurrencyPolicy` when an
instance is already running:

- **`Allow`**: launch another instance (the list grows).
- **`Forbid`**: return without launching.
- **`Replace`**: for each currently running instance, set
  `running_job.replaced = True` *before* `await running_job.cancel()`, so the
  reaper recognizes the forced termination as a replacement rather than a
  failure (`_handle_finished_job` skips reporting/retries for replaced runs),
  then a fresh instance is launched.

The `replaced` flag is the single mechanism distinguishing an
operator/scheduler-initiated replacement from an actual job failure. See
[Concurrency and Timeouts](Concurrency-and-Timeouts).

## statsd metrics

When a job has a `statsd` config, `RunningJob` builds a
`StatsdJobMetricWriter`. `_on_start` sends `{prefix}.start:1|g`; `_on_stop`
sends `{prefix}.stop:1|g`, `{prefix}.success:{0|1}|g`, and
`{prefix}.duration:{ms}|ms|@0.1`, where success is `0 if self.job.failed else 1`
and duration is wall time between start and stop in milliseconds. Each
`send_to_statsd` call is one fire-and-forget UDP datagram, so there are two
datagrams per run: `job_started` sends the single `.start` line, and
`job_stopped` sends the three stop metrics concatenated (newline-delimited) into
one datagram. `job_stopped` also early-returns without sending if `start_time`
is `None` (i.e. `job_started` never ran). `send_to_statsd` opens a datagram
endpoint with `StatsdClientProtocol`, which sends in `connection_made` and is
immediately closed. Send failures are caught as `OSError` at the `RunningJob`
call sites and logged as warnings so telemetry never crashes the scheduler. See
[Metrics with statsd](Metrics-with-Statsd).

## Prometheus metrics

The pull-side sibling of statsd is the `PrometheusMetrics` registry in
`yacron2/prometheus.py`, owned by the `Cron` object rather than the web app, so
counters survive web-app restarts and cluster-manager rebuilds across reloads
(they reset only on process restart; `update_config` prunes series for jobs
removed from the config). Cumulative state is recorded by synchronous in-memory
hooks in `cron.py`: `_record_run` (run outcomes and the duration histogram),
`schedule_retry_job` (retries actually launched), the permanent-failure
branches, `update_config` (reload success/failure), and the leadership- and
quorum-transition latches. Gauges are not stored at all: `Cron._web_metrics`
calls the renderer, which computes them at scrape time from `cron_jobs`,
`running_jobs`, `last_run`, and `cluster_manager.view_dict()`. If a backend
read fails during a scrape, the cluster block degrades to
`yacron2_cluster_enabled` alone instead of failing the whole scrape. See
[Metrics with Prometheus](Metrics-with-Prometheus).

## Concurrency model summary

- One process, one thread, one event loop.
- One long-lived scheduler coroutine (`Cron.run`) and one long-lived reaper
  coroutine (`_wait_for_running_jobs`).
- One short-lived `wait()` task per running job, plus one
  `schedule_retry_job` task per armed-and-pending retry.
- Reporters run concurrently per job but failures are isolated.
- Shutdown is cooperative: a signal sets `_stop_event`, the scheduler stops
  spawning, pending retries are cancelled, and the reaper drains in-flight jobs
  before the process exits. The trigger is platform-specific:
  `SIGINT`/`SIGTERM` on POSIX vs Ctrl-C/Ctrl-Break (`SIGINT`/`SIGBREAK`) on
  Windows, both routed through `platform.install_shutdown_handlers` into
  `signal_shutdown` -> `_stop_event.set()`. But the finish-running-jobs-first
  behavior is identical on every OS.

For the broader operational picture see
[Production and Container Deployment](Production-Deployment); for the CLI surface
see [Command-Line Reference](CLI-Reference).
