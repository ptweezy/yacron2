# Configuration Reference

The canonical, exhaustive reference for the cronstable YAML configuration. It
documents the top-level structure and every per-job option, with the exact
strictyaml type, default, and load-time validation rule taken from
`cronstable/config.py`. Deep topics (schedules, reporting, the HTTP API, metrics,
logging, includes) have dedicated pages linked from each section.

## Configuration source

A configuration is either a single YAML file or a directory of `*.yml`/`*.yaml`
files, selected with the `-c` flag (see [Command-Line Reference](CLI-Reference)).
The document is parsed and validated against a fixed strictyaml schema
(`CONFIG_SCHEMA`); an unknown key, a wrong type, or a malformed value is a hard
`ConfigError` at load time. An empty document is valid.

Classic (Vixie-style) crontab files are accepted alongside YAML, recognised by
name (`*.crontab`, `*.cron`, or a file named `crontab`): each entry is lowered
to an ordinary job definition and merged over the same `DEFAULT_CONFIG`
defaults documented below, so internally it is configured to cronstable's
standard behavior rather than an emulation of cron's. A crontab can only
define jobs; every other section on this page (and any per-job option beyond
schedule, command, shell, timezone, and environment) is YAML-only. See
[Classic Crontabs](Classic-Crontabs).

In the option tables below, "Required" means the strictyaml key is mandatory
(not wrapped in `Opt(...)`); every other key is optional and falls back to the
default shown. Per-job defaults come from `DEFAULT_CONFIG`; a `defaults:` block
and any included files override `DEFAULT_CONFIG`, and an individual job overrides
both. See [Includes, Defaults, and Multi-File Config](Includes-and-Defaults) for
the precise merge order.

## Top-level structure

```yaml
defaults: { ... }   # optional: per-job defaults for this file
jobs:               # optional: list of job definitions
  - name: ...
    command: ...
    schedule: ...
dags:               # optional: durable orchestration DAGs (needs state)
  - name: ...
    tasks: [ ... ]
include: [ ... ]    # optional: list of other config files to merge
web: { ... }        # optional: HTTP control API
mcp: { ... }        # optional: Model Context Protocol server for AI agents
cluster: { ... }    # optional: mTLS peer attestation / leader election
state: { ... }      # optional: durable state store (history, catch-up, retries)
logging: { ... }    # optional: Python logging dictConfig
```

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `defaults` | `Map` of the per-job common options | No | Default values inherited by every job in the same file. May contain any per-job option except `name`, `command`, and `schedule`. See [Includes, Defaults, and Multi-File Config](Includes-and-Defaults). |
| `jobs` | `Seq(Map)` of job definitions | No | The list of cron jobs. Each entry is validated against the per-job schema below. |
| `dags` | `Seq(Map)` of DAG definitions | No | Durable orchestration workflows: each DAG is a graph of tasks with `dependsOn` edges, run on a schedule. Requires a `state` section with `jobApi` enabled. See [Orchestration and DAGs](Orchestration-and-DAGs). |
| `include` | `Seq(Str)` | No | Paths (relative to the including file) of other config files to parse and merge. Include cycles raise a `ConfigError`. See [Includes, Defaults, and Multi-File Config](Includes-and-Defaults). |
| `web` | `Map` | No | Enables the HTTP control API. See [HTTP Control API](HTTP-API). |
| `mcp` | `Map` | No | Enables the Model Context Protocol server (`POST /mcp` on the web listeners, plus the `cronstable mcp` stdio bridge) so AI agents can observe and, opt-in, control jobs/DAGs. Requires a `web` section. Off by default. See [`mcp`](#mcp) below and [MCP](MCP). |
| `cluster` | `Map` | No | Enables mutual-TLS peer attestation and optional leader election across replicas. See [Clustering and Leader Election](Clustering-and-Leader-Election). |
| `state` | `Map` | No | Enables the opt-in durable state store: restart-durable run history, missed-run catch-up, restart-surviving retries, and once-per-boot `@reboot` runs. Without it cronstable is stateless (everything in memory, exactly as before). See [Durable State](Durable-State). |
| `logging` | `Map` (Python `logging.config` dictConfig) | No | Custom logging configuration. See [Logging Configuration](Logging-Configuration). |

### `web`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `listen` | `Seq(Str)` | required | Listen URLs, e.g. `http://127.0.0.1:8080` or `unix:///tmp/cronstable.sock`. `http://` listeners work everywhere; `unix://` listeners are not supported on Windows (the Proactor loop lacks `create_unix_server`) and are skipped with the warning `Ignoring web listen url <url>: unix-socket listeners are not supported on this platform`. Use an `http://` listener instead. See [Running on Windows](Running-on-Windows). |
| `headers` | `MapPattern(Str, Str)` | none | Extra HTTP response headers applied to all endpoints. |
| `allowedOrigins` | `Seq(Str)` | `[]` | Extra exact-match browser `Origin`s allowed to call the **mutating** endpoints (`POST /jobs/{name}/start`, `/jobs/{name}/cancel`, `/dags/{name}/trigger`, `/dags/{name}/backfill`, task decisions). Cross-site browser requests to those endpoints are refused `403` as a CSRF/DNS-rebinding defense; same-origin requests (the served dashboard) and clients that send no `Origin` (curl, monitoring) always pass, and `/mcp` keeps enforcing its own `mcp.allowedOrigins`. Setting `headers` with `Access-Control-Allow-Origin: <origin>` implicitly allow-lists that origin; `Access-Control-Allow-Origin: "*"` disables the check (logged loudly). |
| `authToken` | `Map` with `value` / `fromFile` / `fromEnvVar` (each `EmptyNone() \| Str`) | none | Opt-in bearer-token auth. When set but resolving empty, cronstable refuses to start. |
| `socketMode` | `Str` | none | Octal permissions applied to a `unix://` listen socket. Only ever applies to unix sockets, so it is irrelevant on Windows (where `unix://` listeners are unsupported). |
| `ui` | `Bool` | `true` | Serve the browser dashboard at `/`. `ui: false` omits the dashboard page while every JSON endpoint keeps working, for an API-only deployment. See [Web Dashboard](Web-Dashboard). |
| `metrics` | `Bool \| Map` with `enabled` / `public` (each `Bool`) and `durationBuckets` (`Seq(Float)`) | enabled | The Prometheus `GET /metrics` endpoint, served by default whenever the web API is on. `metrics: false` (bool shorthand) disables it; the map form sets `enabled` (default `true`), `public` (default `false`; exempts only `/metrics` from `authToken`), and `durationBuckets` (histogram bounds in seconds; must be finite, positive, and strictly increasing, else a `ConfigError`). See [Metrics with Prometheus](Metrics-with-Prometheus). |
| `nodeHistory` | `Bool \| Map` with `enabled` (`Bool`), `interval` (`Float`) and `points` (`Int`) | enabled | Background node CPU/memory sampling for the dashboard's node history chart, served by `GET /node/history`. On by default whenever the web API is on; `nodeHistory: false` disables the sampling task. The map form sets `interval` (seconds between samples, default `5.0`, minimum `1.0`) and `points` (ring size, default `720` — the last hour at the default cadence; 10–50000). The ring is in-memory only and follows the web app's lifecycle. |

`listen` is the only required key. Full behavior, authentication, and endpoint
semantics are documented in [HTTP Control API](HTTP-API).

### `mcp`

Opt-in [Model Context Protocol](https://modelcontextprotocol.io) server, so an
AI agent (Claude Desktop/Code, Cursor, VS Code Copilot, …) can drive cronstable
the way an operator drives the dashboard. It is served as a stateless
Streamable-HTTP endpoint at **`POST /mcp`** on the existing `web.listen`
addresses (inheriting the same `authToken` / unix-socket auth) and over the
featherweight **`cronstable mcp`** stdio bridge for desktop clients. Every field
is optional; the server is **off unless `enabled: true`**, and requires a `web`
section (there is nowhere to serve it otherwise). Tools operate on the same data
as the REST API, so there is one source of truth.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | `Bool` | `false` | Serve `POST /mcp` and expose the `cronstable mcp` stdio bridge. |
| `readOnly` | `Bool` | `true` | Strip every mutating tool (run/cancel a job, trigger/backfill a DAG, decide a gate). On by default: agents get read-only access unless the operator opts into control. Takes precedence over `toolsets` (`act` stays suppressed while true). |
| `toolsets` | `Seq(Enum)` of `observe` / `act` / `dags` / `state` | `[observe]` | Which tool groups to expose. `observe` = read-only job/cluster/metrics views; `dags` = DAG introspection (+ control when `readOnly:false`); `state` = durable-state inspector (redacted); `act` = mutating job control (only when `readOnly:false`). |
| `allowedOrigins` | `Seq(Str)` | `[]` | Exact-match browser `Origin`s allowed to call `/mcp`. Empty serves non-browser clients only, so a present `Origin` not on the list is refused `403` (a DNS-rebinding defense). A non-empty list additionally answers CORS preflight with a scoped `Access-Control-Allow-Origin`. |
| `allowUnauthenticated` | `Bool` | `false` | Serve `/mcp` on a routable (non-loopback, non-socket) listener even with no `web.authToken`. Fail-closed default: with no token the web app has no auth middleware at all, so an enabled `/mcp` on a routable address raises a `ConfigError` at load. Set true only when the endpoint is protected by other means (an mTLS-terminating proxy, a network policy). |
| `resources` | `Bool` | `true` | Expose MCP **resources**: URI-addressable read-only context like `cronstable://status` and `cronstable://jobs/{name}` that mirrors the read tools for clients that consume resources. Their scope follows `toolsets` (a `cronstable://dags/{name}` resource is served only when the `dags` toolset is on). |
| `prompts` | `Bool` | `true` | Expose MCP **prompts**: canned triage playbooks (`triage_job_failure`, `why_did_dag_run_fail`, `blast_radius`, `fleet_health_summary`, `backfill_plan`) that chain the read tools into repeatable workflows. Like resources, their scope follows `toolsets`: `why_did_dag_run_fail` and `backfill_plan` are served only when the `dags` toolset is on, so the default `[observe]` exposes the other three. |
| `instructions` | `EmptyNone() \| Str` | none | Optional free-text server `instructions` surfaced to the client at `initialize`. |
| `maxRows` | `Int` | `200` | Ceiling on any list tool's `limit`; a larger request is capped (never an error) and an opaque `nextOffset` is offered for the rest. Must be `>= 1` (a `ConfigError` at load). |
| `maxBodyBytes` | `Int` | `1048576` | Cap on a single `/mcp` request body (tool arguments arrive from an LLM); an oversized POST is refused `413`. Must be `>= 1` (a `ConfigError` at load). |

```yaml
web:
  listen:
    - http://127.0.0.1:8080
  authToken:
    fromEnvVar: CRONSTABLE_WEB_TOKEN   # also gates /mcp

mcp:
  enabled: true
  readOnly: false        # allow mutating tools
  toolsets:              # jobs + DAG reads/control
    - observe
    - dags
    - act
```

Wire a client to it with, e.g., `claude mcp add --transport http cronstable
https://host/mcp --header "Authorization: Bearer $TOKEN"`, or over stdio with
`claude mcp add cronstable -- cronstable mcp --url http://127.0.0.1:8080`.

This section is only the config schema. The full tool catalog, the prompts and
resources, client setup recipes, and the stdio bridge's flags are documented in
[MCP](MCP).

### `cluster`

Optional. Gates scheduled jobs on a **leadership backend** so several replicas
can run from one config without double-running jobs. `cluster.backend` chooses
how: the default **`gossip`** backend attests, over mutual TLS, that a static
list of peers is running the same job set and runs a best-effort quorum
election; the **`kubernetes`** and **`etcd`** backends use a coordination store
(a `Lease` / a lease-bound key) for a fenced, exactly-once election; the
**`filesystem`** backend elects through a fenced TTL lease on a shared POSIX
mount, with no coordination service at all (its safety additionally rests on
synchronized clocks; see its table below). There must be exactly one `cluster`
block across the whole configuration; a duplicate in an included file or a
second config-directory file raises a `ConfigError`. Defaults come from
`DEFAULT_CLUSTER` (plus `DEFAULT_K8S` / `DEFAULT_ETCD` / `DEFAULT_FILESYSTEM`
for the lease backends) and are applied only when a `cluster` section is
present.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `backend` | `Enum(["gossip", "kubernetes", "etcd", "filesystem"])` | `gossip` | Which leadership backend gates jobs. `gossip` (default) is the embedded mTLS best-effort election; `kubernetes`/`etcd`/`filesystem` are fenced lease backends. `kubernetes`/`etcd` talk to their store over plain HTTP via the core `aiohttp` dependency, and `filesystem` needs only a shared POSIX mount, so none of them adds a runtime dependency. |

**Gossip backend** (`backend: gossip`). `listen`, `tls`, and `peers` are
required **only for this backend**:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `listen` | `Str` | required (gossip) | `host:port` the mTLS `/peer` listener binds to (e.g. `0.0.0.0:8443`). Served only here, never on the public `web` API. |
| `tls.ca` | `Str` | required (gossip) | Path to the cluster CA (trust anchor for peer certificates). |
| `tls.cert` | `Str` | required (gossip) | Path to this node's certificate (used both to serve `/peer` and to authenticate as a client). Its SAN must match the host other nodes use to reach it. |
| `tls.key` | `Str` | required (gossip) | Path to this node's private key. |
| `peers` | `Seq(Map({"host": Str}))` | required (gossip) | Every **other** member as `host:port`. Cluster size is `len(peers) + 1`. |
| `nodeName` | `Str` | system hostname | Stable, human-readable identity for this node; the leader is the lowest `nodeName` among agreeing members. **Must be unique across the cluster**: a duplicate is detected at runtime (status `conflict`) and pauses `Leader` jobs until resolved. The hostname default is already unique per host. (Also used as the lease backends' default `identity` / etcd key value.) |
| `interval` | `Int` | `30` | Seconds between peer-attestation rounds. Must be `> 0`. |
| `driftAfter` | `Int` | `3` | Consecutive reachable-but-mismatched rounds before a peer is reported `drifted` (debounce). Must be `>= 1`. |
| `connectTimeout` | `Int` | `10` | Seconds per request (also the HTTP timeout for the lease backends). Must be `> 0`. |
| `electLeader` | `Bool` | `false` | When true, only the quorum-gated elected leader runs *scheduled* jobs (manual API triggers and retries are unaffected). Off by default, so a gossip `cluster` section is observe-only until opted in. The lease backends imply `electLeader: true` (configuring one is opting into leadership). |
| `distribution` | `Enum(["single-leader", "spread"])` | `single-leader` | How leader-gated jobs spread across the quorate cluster. `single-leader`: one elected leader runs every `Leader` job. `spread`: per-job ownership via rendezvous hashing, so the work fans out across the quorate nodes (same quorum gate, same guarantee). Inert without `electLeader` (warns if set anyway). With a lease backend (`kubernetes`/`etcd`/`filesystem`) a non-default `distribution` is a **hard `ConfigError` at load** (a single lease holder cannot be a per-job owner), not a silent fallback. See [Clustering and Leader Election](Clustering-and-Leader-Election#distribution-one-leader-or-spread-the-load). |

Gossip load-time validation (in addition to the numeric ranges above): every
address -- `listen` and each `peers[].host` -- must be `host:port` with a
numeric port in `1`-`65535`, and an IPv6 host must be written **bracketed**
(`[2001:db8::1]:8900`); a bare IPv6 literal is a `ConfigError` at load
(`looks like a bare IPv6 address; write it as [ipv6]:port`), because splitting
it at the last colon would silently mis-read the host and, for a peer, drop it
from quorum with no error. The same address checks apply to the
[observability overlay](#observability-overlay)'s `listen`/`peers`, which are
built through the same code path. With `electLeader: true`, a **2-node**
cluster (one peer) is rejected outright with a `ConfigError` (a quorum of 2
needs both up, strictly worse than one replica); an **even** cluster size
**greater than 2** is allowed but logs a warning (an odd count is best for a
clean majority).

**Kubernetes backend** (`backend: kubernetes`), under `cluster.kubernetes`. A
`coordination.k8s.io/v1` `Lease` is the fence. Defaults from `DEFAULT_K8S`:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `leaseName` | `Str` | `cronstable-leader` | Name of the `Lease` object the replicas contend for. Must be a valid RFC1123 subdomain (lowercase alphanumerics, `-` and `.`; `<=253` chars), checked at load; it is spliced into the apiserver URL path, so a stray `/`, `?`, `#`, or space is rejected. |
| `leaseNamespace` | `Str` or null | null → in-cluster namespace | Namespace of the `Lease`; defaults to the pod's own namespace (the service-account namespace file). When set, must be a valid RFC1123 label (lowercase alphanumerics and `-`; `<=63` chars), checked at load. |
| `leaseDurationSeconds` | `Int` | `15` | How long a renewal keeps the lease valid. Must be `> renewDeadlineSeconds`. |
| `renewDeadlineSeconds` | `Int` | `10` | Per-round renew/observe deadline: a round that exceeds it is abandoned and retried next round, so a stuck apiserver call cannot run out the full lease. Must be `> 0` and `< leaseDurationSeconds`. |
| `retryPeriodSeconds` | `Int` | `2` | Seconds between renew/observe rounds. Must be `> 0` and `< renewDeadlineSeconds` (a holder must be able to attempt a renew before its own deadline). Additionally, `renewDeadlineSeconds + retryPeriodSeconds < leaseDurationSeconds` is enforced at load, so the worst-case interval between two successful refreshes still fits inside the lease. |
| `identity` | `Str` or null | null → `nodeName` | The human-readable holder for this node (shown in the dashboard / `GET /cluster`). cronstable appends a **per-process token** to the `holderIdentity` it actually writes (`<identity>#<token>`), so two nodes sharing an `identity`/`nodeName` still write distinct holders and cannot both believe they hold the `Lease`. See [Node identity](Clustering-and-Leader-Election#node-identity-for-the-lease-backends). |
| `kubeconfig` | `Str` or null | null → in-cluster | Path to a kubeconfig for out-of-cluster / local testing; otherwise the in-cluster service-account credentials are used. On the hand-rolled HTTP transport a kubeconfig user that relies on an `exec` credential plugin or an `auth-provider` raises a `ConfigError` (those must be executed, which only the native client can do); use `clientLibrary: library` (`cronstable[kubernetes]`) or a kubeconfig with a static token / client certificate instead. `insecure-skip-tls-verify` is honored (the apiserver certificate is not validated) but logs a warning. |
| `apiServer` | `Str` or null | null | Override the apiserver URL (else the in-cluster `KUBERNETES_SERVICE_*` env or the kubeconfig). When set, must be an `https://` URL: a non-https value is a `ConfigError` at load, since the ServiceAccount bearer token must not travel in cleartext. |
| `clientLibrary` | `Enum(["auto", "http", "library"])` | `auto` | Transport selection. `auto` uses the official `kubernetes` client when it is importable (install `cronstable[kubernetes]`) and otherwise falls back to a hand-rolled apiserver REST transport over `aiohttp`; `library` requires the native client (a `ConfigError` if absent); `http` forces the hand-rolled transport. |

**etcd backend** (`backend: etcd`), under `cluster.etcd`. A lease-bound key is
the fence; the backend uses etcd's v3 gRPC-gateway JSON/HTTP API directly (no
native client). Defaults from `DEFAULT_ETCD`:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `endpoints` | `Seq(Str)` | `["http://127.0.0.1:2379"]` | etcd client URLs, tried in order for failover. Each must be `http(s)://host[:port]`; the port is optional (defaults to the scheme's port, e.g. `443` behind an https ingress) and only an explicitly out-of-range port is rejected. Credentials embedded in the URL are refused. |
| `electionName` | `Str` | `cronstable/leader` | The etcd key contended for; its value is the holder's `nodeName`. There is **no separate `identity` key** for etcd (the holder identity is always `cluster.nodeName`), but leadership is fenced on the **bound lease id**, not this string, so a duplicate `nodeName` cannot make two nodes both lead. See [Node identity](Clustering-and-Leader-Election#node-identity-for-the-lease-backends). |
| `ttl` | `Int` | `15` | Lease time-to-live, seconds. Must be `>= 3`: the leader holds the key only until `ttl` minus a 1s clock-skew margin, so a smaller `ttl` would make a fresh winner treat its own lease as already expired (no `Leader` job would ever run). The keepalive cadence is ~`ttl/3` against the **effective** ttl, which etcd may grant smaller than requested (a smaller granted TTL narrows the fence window). |
| `username` | `Str` or null | null | etcd auth username (omit for an auth-less cluster). Pair it with a resolvable `password`. The auth token is re-fetched automatically when it expires (re-auth on a `401`). |
| `password` | `Map` with `value` / `fromFile` / `fromEnvVar` (each `EmptyNone() \| Str`) | unset | etcd auth password source, resolved like `web.authToken` from exactly one of `value` / `fromFile` / `fromEnvVar`; a configured-but-empty source fails closed. |
| `tls.ca` / `tls.cert` / `tls.key` | `Str` or null | null | Optional client TLS for `https://` endpoints. `tls.cert` and `tls.key` are all-or-nothing (a client certificate needs its private key), enforced at load. |

etcd load-time guards: any TLS material (`tls.ca`/`tls.cert`/`tls.key`) requires
at least one `https://` endpoint (otherwise it would be silently ignored and
traffic sent in cleartext, a `ConfigError`). Likewise a `username` or resolved
`password` requires **every** endpoint to be `https://`, so the credentials and
bearer token are never POSTed in cleartext; a `username` without a resolvable
`password` is also rejected.

One more etcd check is advisory rather than fatal: a small `ttl` also shrinks
the per-request timeout each renew POST gets (roughly
`(ttl - max(1, ttl/3) - 1s) / 5`), and when that budget falls below ~1 second
-- any integer `ttl` from `3` to `8` -- load emits a one-time startup advisory.
If a single round-trip to etcd is slower than the budget (e.g. a cross-AZ or
cross-region endpoint), every renew round times out, the node treats a
reachable etcd as unreachable, and `Leader` jobs fail closed. It is the
operator's explicit `ttl` choice and a local, low-latency etcd is fine, so
cronstable warns rather than rejects; raise `ttl` otherwise.

**Filesystem backend** (`backend: filesystem`), under `cluster.filesystem`. The
flock-guarded, fence-counted TTL lease of the durable state store's filesystem
backend is the fence, taken over a shared POSIX mount (Amazon EFS (NFSv4) / S3
Files) -- no coordination service; the mount is the store. Defaults from
`DEFAULT_FILESYSTEM`:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `path` | `Str` | required | Directory the election lease lives in -- normally a shared mount. Must be present and non-empty: a missing, blank, or whitespace-only value is a `ConfigError` at load (`cluster.filesystem.path is required and must be non-empty`). Sharing the [`state`](#state) store's directory (same mount, same `deploymentId`) is legal and recommended when both are configured: the namespaces are disjoint, and one mount stays the whole coordination surface. |
| `electionName` | `Str` | `cluster/leader` | Name of the lease the replicas contend for. Must be non-empty: a blank or whitespace-only value is a `ConfigError` at load (`cluster.filesystem.electionName must be non-empty`). |
| `ttl` | `Int` or `Float` | `15` | Lease time-to-live, seconds. Must be `>= 3`, for the same reason as etcd's floor: the leader holds the lease only until `ttl` minus a clock-skew margin and renews every `max(1s, ttl/3)`, so a smaller `ttl` would make a node that wins the election immediately treat its own lease as expired (no `Leader` job would ever run). |
| `deploymentId` | `Str` or null | null → namespace `default` | Stable namespace prefix inside the store, same semantics as `state.deploymentId`. Use the **same** value as `state.deploymentId` when sharing a mount with the state store. |
| `topology` | `Enum(["auto", "single-node", "shared"])` | `auto` | Whether the mount's locks may be trusted across hosts; same semantics and probe as `state.topology`. `auto` probes `/proc/mounts` for a shared network mount; Windows and macOS cannot probe, so there `auto` resolves to `single-node` (the election then only excludes processes on this host, with a startup warning) unless overridden with an explicit `shared`. |

The lease-backend family rules apply exactly as for `kubernetes`/`etcd`:
`electLeader` is forced on (configuring the backend is opting into
leadership), `distribution: spread` is a hard `ConfigError`, a store block
belonging to a different backend (say an `etcd:` block under
`backend: filesystem`) is a `ConfigError` rather than silently ignored, and
gossip-only keys (`listen`, `tls`, `peers`, `interval`, `driftAfter`) draw an
emit-once startup advisory.

One filesystem-backend guard is deferred to startup, because it needs the
live mount: `start()` probes lock fidelity (two descriptors of one file must
actually contend on a non-blocking exclusive lock; on Linux an NFS mount
carrying `nolock` or `local_lock=flock`/`local_lock=all` is additionally
refused, since those honour locks host-locally) and **hard-refuses** a store
whose locks are no-ops, verbatim
`cluster.backend filesystem: refusing to elect over <path>: <reason>`. A
refused start leaves the cluster manager unbuilt, so `Leader` jobs fail
closed -- the safe direction. Both checks run on one host, so on platforms
without `/proc/mounts` (Windows, macOS) the residual risk rests on the
operator's `topology: shared` assertion. Unlike the `kubernetes`/`etcd`
fences, election safety here also rests on wall clocks (two leaders need
inter-host clock skew above roughly 2 seconds): run NTP on every node, the
same requirement [Durable State](Durable-State) documents for shared mounts.

Because the cluster schema has many load-time rejections (the ordering rules,
the RFC1123 and https guards, the credential-over-plaintext refusals, and the
lease-family rules above), check a cluster config before deploying with
`cronstable --validate-config`, which runs the full load path and prints the
first `ConfigError` without starting the scheduler. See
[Command-Line Reference](CLI-Reference).

Full behavior, the trust model, quorum math, the lease backends' guarantees,
and per-job `clusterPolicy` are documented in
[Clustering and Leader Election](Clustering-and-Leader-Election).

#### Observability overlay

`cluster.observability` shares fleet data — each node's live CPU/memory (see
[`GET /node`](HTTP-API#get-node)) and per-job run summaries — across the cluster
for the dashboard's [fleet view](Web-Dashboard#fleet-view-every-nodes-runs-in-one-pane),
**independent of which backend owns election**. It exists because the fleet view
rides node-to-node gossip, which the lease backends do not have: it lets a
`kubernetes`/`etcd`/`filesystem` cluster stand up a *second*, election-inert
gossip mesh purely to carry that data.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `shareNodeStats` | `Bool` | `true` | Gossip this node's whole-node CPU/memory for the fleet view. Set `false` to run the overlay mesh for job summaries only. |
| `listen`, `tls`, `peers` | as `backend: gossip` | — | The overlay gossip transport. **Required with a lease backend** (the overlay is its own mesh); **rejected with `backend: gossip`** (redundant — the election mesh already carries fleet data). |
| `nodeName`, `interval`, `driftAfter`, `connectTimeout` | as `backend: gossip` | gossip defaults | Optional overlay mesh tuning. **Lease backends only** (they tune the dedicated overlay mesh); **rejected with `backend: gossip`**, where node stats ride the election mesh and the cluster-level keys of the same names already apply. |

Two shapes:

- **`backend: gossip`** — the election mesh already exchanges `/peer` bodies, so
  `observability` is just an opt-in marker: `observability: { shareNodeStats: true }`
  adds node CPU/memory to what that mesh already gossips. `listen`/`tls`/`peers`
  here are a `ConfigError` (redundant), and so are the overlay tuning keys
  `nodeName`/`interval`/`driftAfter`/`connectTimeout` (there is no overlay mesh
  to tune; the stats gossip at `cluster.interval`, so set the cluster-level keys
  instead). The stats ride each `/peer` response as an `X-Cronstable-Node-Stats`
  header, never in the body, so sharing live load keeps the mesh's idle `304`
  optimisation intact.
- **A lease backend** (`kubernetes`/`etcd`/`filesystem`) — election stays with the
  lease store; `observability` stands up a dedicated gossip mesh (its own
  `listen`/`tls`/`peers`, all required) that **never elects** (it holds no
  leadership and gates no jobs), purely to carry fleet data.

```yaml
cluster:
  backend: kubernetes            # election via a coordination.k8s.io Lease
  kubernetes:
    leaseName: cronstable-leader
  observability:                 # a gossip mesh JUST for the fleet view
    listen: "0.0.0.0:8140"
    tls: { ca: /tls/ca.pem, cert: /tls/node.pem, key: /tls/node.key }
    peers:
      - host: node-b:8140
      - host: node-c:8140
```

Requires [`psutil`](https://github.com/giampaolo/psutil) (a core dependency) for
the CPU/memory numbers; a node that cannot read its own load simply shares none.
Node stats are best-effort observability: a malformed or hostile peer payload
degrades to "no data for that node", never poisoning the view.

### `state`

Optional. Enables the **durable state store**: restart-durable run history,
missed-run catch-up, restart-surviving retries, once-per-boot `@reboot`
dedupe, restart-durable Prometheus job counters, and durable output archival.
cronstable is stateless by default: absent this section everything stays in
memory exactly as before. The store is a directory of immutable JSON records
behind a single filesystem backend -- a local path gives single-node
durability, while a shared Amazon EFS (NFSv4) / S3 Files mount gives the same
durability and coordination fleet-wide (the same code either way; the mount
decides the reach). There must be exactly one `state` block across the whole
configuration; a duplicate in an included file or a second config-directory
file raises a `ConfigError`. Defaults come from `DEFAULT_STATE` and are
applied only when a `state` section is present. (The
[Web Dashboard](Web-Dashboard)'s browser-side IndexedDB run ledger is a
separate, client-local feature, unrelated to this store.)

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `path` | `Str` | required | Directory the store lives in. A local path gives single-node durability; a shared Amazon EFS (NFSv4) / S3 Files mount gives the same durability fleet-wide. Must be non-empty: a blank or whitespace-only value is a `ConfigError` at load (`state.path is required and must be non-empty`). |
| `topology` | `Enum(["auto", "single-node", "shared"])` | `auto` | Whether the store may offer cross-node coordination. `auto` probes `/proc/mounts` for a shared network mount (NFS/EFS/S3 Files) and otherwise assumes `single-node`; Windows and macOS cannot probe, so there `auto` resolves to `single-node` unless overridden with `shared`. |
| `deploymentId` | `Str` | none (namespace `default`) | Stable namespace prefix so several deployments can share one store/bucket without colliding or cross-reading. Unset means the `default` namespace. |
| `maxRunsPerJob` | `Int` | `1000` | Durable finished-run retention per job (the durable analogue of the in-memory history ring); the ledger is pruned to this after each append. `<= 0` disables pruning (unbounded; rely on an external lifecycle rule). Durable retention is larger than the in-memory window on purpose. |
| `onStoreUnavailable` | `Enum(["degrade", "fail-closed"])` | `degrade` | What the stateful features do while the store is configured but unavailable (down, unreadable, hung). `degrade`: durable-truth gates fail open to the in-memory state and failed writes are dropped with a warning (counted in `cronstable_state_dropped_writes_total`). `fail-closed`: prefer not running over possibly running wrong -- the `onlyIfLastSucceeded` gate blocks, a due durable retry defers until the store answers, and an unverifiable `@reboot` boot marker skips the boot run. Plain scheduled fires are **never** gated on the store under either policy. |
| `gcGraceSeconds` | `Int` | `604800` (7 days) | Age past which durable state belonging to a job that no recent manifest references (no node's loaded config under this `deploymentId` has mentioned it for this long) is garbage collected. `<= 0` disables automatic GC. Values between `1` and `86399` are a `ConfigError` at load: a grace below the manifest cadence would make live peers' manifests look stale and collect their state. |
| `maxOpsPerSecond` | `Int` or `Float` | `0` | Token-bucket cap on store operations per second (burst of one second's tokens), for request-rate/cost control on mounts that bill per request; throttled ops queue and are counted. `0` disables throttling. Must be `>= 0` (a negative value is a `ConfigError` at load). Lease (coordination) operations bypass the bucket: a lease renew queued behind bulk writes could overshoot its TTL and double-run the very job the lease exists to fence. |
| `slotTtlSeconds` | `Int` or `Float` | `30` | TTL, in seconds, of the per-job concurrency slot lease taken for `concurrencyScope: cluster` jobs; the running holder renews it at a third of this, and a crashed holder's slot frees itself after at most this long. Must be `>= 5` (a `ConfigError` at load, `state.slotTtlSeconds must be >= 5`): the renew cadence needs headroom, and below ~5s one slow renew on a network mount expires a healthy holder's slot and invites the cross-node double-run the lease exists to fence. |
| `jobApi` | `Map` | *(see below)* | The [job-facing state endpoint](Durable-State#job-facing-state): a loopback HTTP server the daemon injects into every job's environment, backing the `cronstable state\|cursor\|lock\|artifact\|idempotent\|secret` commands. A nested block (merged over its defaults, so a partial block keeps the rest). |

The `state.jobApi` sub-keys:

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `enabled` | `Bool` | `true` | Run the loopback endpoint and inject its address/token into every job. `false` keeps the durable scheduler features but exposes nothing to jobs. |
| `listen` | `Str` | *(ephemeral)* | Override the bind, as an `http://host:port` URL (a `unix://` URL is a `ConfigError`: the job CLI speaks TCP only). Unset binds an OS-assigned ephemeral port on `127.0.0.1`. An explicit port must be an integer in `0`-`65535` (a `ConfigError` otherwise; `0` or omitting the port keeps the ephemeral bind), and a non-loopback host is a `ConfigError` unless `allowNonLoopbackBind` is also `true`. |
| `maxValueBytes` | `Int` | `1048576` | Cap (bytes) on one KV / cursor value; a larger set is refused (HTTP 413). Must be `>= 0`. |
| `maxArtifactBytes` | `Int` | `67108864` | Cap (bytes) on one artifact payload; a larger put is refused (HTTP 413). Must be `>= 0`. |
| `lockTtlSeconds` | `Int` or `Float` | `30` | TTL of a job mutex/semaphore lease, renewed by the daemon at a third of this. Must be `>= 5` (a `ConfigError`, `state.jobApi.lockTtlSeconds must be >= 5`), for the same reason as `slotTtlSeconds`. |
| `allowNonLoopbackBind` | `Bool` | `false` | Explicit opt-in for a non-loopback `listen` host. Without it, a non-loopback host is a `ConfigError`: the endpoint serves per-run bearer tokens and staged job secrets over plaintext HTTP, so exposing it beyond this host needs a deliberate choice (and should be paired with a reverse proxy adding TLS/auth). |

`path` is the only required key. Full behavior -- the store layout and
durability model, restart-surviving retries, missed-run catch-up, `@reboot`
dedupe, the SLA trends endpoint, garbage collection, the state metrics, and
the `cronstable state` CLI subcommands (backup, restore, migrate, gc, check,
migrate-schema) --
is documented in [Durable State](Durable-State). The per-job knobs that build
on this store are under [Durable state and catch-up](#durable-state-and-catch-up)
below.

### `dags`

A list of durable orchestration DAGs. Requires a `state` section with
`jobApi.enabled` (the default). Full guide: [Orchestration and DAGs](Orchestration-and-DAGs).

Per-DAG keys:

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `name` | `Str` | required | Unique DAG name. |
| `tasks` | `Seq(Map)` | required | The task nodes (at least one). |
| `schedule` | `Str` or `Map` | none | Same grammar as a job's `schedule`, except it must parse to a cron expression: `@reboot` is a `ConfigError` (`DAG schedules must be cron expressions; @reboot is not supported for dags`), while `@daily`/`@hourly`-style aliases still work. Omit for a manual-only DAG. |
| `timezone` / `utc` | `Str` / `Bool` | as jobs | Schedule time base, as jobs. |
| `onMissed` | `skip` / `run-once` / `run-all` | `skip` | Missed-run catch-up on restart, as jobs. |
| `startingDeadlineSeconds` | `Int` | none | Bound how old a missed run may be to replay. |
| `catchupJitterSeconds` | `Int` | `0` | Spread boot-time catch-up. |
| `clusterPolicy` | `Leader` / `PreferLeader` / `EveryNode` | `Leader` | Which node schedules the DAG under leader election. |
| `enabled` | `Bool` | `true` | Disable without deleting. |
| `retainRuns` | `Int` | `50` | Keep the newest N terminal runs (must be ≥ 1). |

Per-task keys:

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `id` | `Str` | required | Unique task id within the DAG. |
| `command` | `Str` or `Seq(Str)` | required (not for `approval`) | The command to run. |
| `type` | `task` / `sensor` / `approval` | `task` | Node kind. |
| `dependsOn` | `Seq(Str)` | `[]` | Upstream task ids. |
| `triggerRule` | `all_success` / `all_done` | `all_success` | When the task becomes ready. |
| `retries` | `Int` | `0` | Per-task retry attempts (DAG-owned). Must be `>= 0`: the job-level `-1` retry-forever sentinel is a `ConfigError` here. |
| `retryDelaySeconds` | `Int`/`Float` | `0` | Delay between attempts. |
| `expand` | `Map{fromTask, key}` | none | Dynamic mapping: fan out over an upstream's XCom list (a direct, non-mapped dependency). |
| `pokeIntervalSeconds` | `Int`/`Float` | `30` | Sensor: seconds between pokes. |
| `pokeTimeoutSeconds` | `Int`/`Float` | `3600` | Sensor: give up after this long. |
| `pokeJitterSeconds` | `Int`/`Float` | `0` | Sensor: jitter added to each poke. |
| `onReject` | `fail` / `skip` | `fail` | Approval gate: what a rejection does. |

Plus the shared launch fields a job takes: `shell`, `environment`,
`captureStdout` / `captureStderr`, `monitorResources`, `saveLimit`,
`maxLineLength`, `streamPrefix`, `failsWhen`, `executionTimeout`,
`killTimeout`, `statsd`, `user` / `group`, `env_file`, `secrets`,
`stateAllowedScopes`. Where a task's `monitorResources` numbers surface is
covered under [Metrics](#metrics) below.

The graph is validated at load: unknown/duplicate ids, a cycle, a self-edge, or
an `expand.fromTask` that is not a direct non-mapped dependency are config
errors.

### `logging`

A standard Python `logging.config` dictionary-schema. `version` (`Int`) is
required; `incremental`, `disable_existing_loggers`, `formatters`, `filters`,
`handlers`, `loggers`, and `root` are optional. See
[Logging Configuration](Logging-Configuration).

## Per-job options

Every key below comes from `_job_schema_dict` (jobs) / `_job_defaults_common`
(`defaults` and the per-file defaults). Defaults are from `DEFAULT_CONFIG`.
`name`, `command`, and `schedule` are required on a job; all other keys are
optional. The three keys `name`, `command`, and `schedule` are **not** allowed
in a `defaults` block (only the common keys are).

### Identity and command

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `name` | `Str` | required | Job identifier. Used in logs, the stream prefix, reports, statsd, and the HTTP API. |
| `command` | `Str` or `Seq(Str)` | required | A shell command string (run via `shell`) or an argv list (run directly, no shell). The shell used for a string `command` is platform-specific (`/bin/sh` on POSIX vs cmd.exe via `%ComSpec%` on Windows when `shell` is left empty); an argv list bypasses the shell on every platform. See [Commands and Environment](Commands-and-Environment) and [Running on Windows](Running-on-Windows). |
| `schedule` | `Str` or `Map` | required | A crontab string (5, 6, or 7 fields), the literal `@reboot`, or a mapping with `second`, `minute`, `hour`, `dayOfMonth`, `month`, `year`, `dayOfWeek` (each `Str`, all optional). The mapping is assembled into a crontab: 5 fields normally, 6 when `year` is set, 7 when `second` is set (second/year emitted only when used, the rest default to `*`). A `second` schedules at second granularity; `year` restricts to specific years. See [Schedules and Timezones](Schedules-and-Timezones). |
| `shell` | `Str` | `/bin/sh` (POSIX) / empty (Windows) | Shell used to run a string `command`. Ignored when `command` is a list. The default is platform-specific: on POSIX a string `command` runs as `["/bin/sh", "-c", command]`; on Windows the default is empty, which routes a string `command` through the native command processor `%ComSpec%` (cmd.exe) via `asyncio.create_subprocess_shell`. For PowerShell or another interpreter set `shell:` explicitly, or pass `command` as a list to bypass the shell entirely (on every platform). The `shell` field itself works on all OSes. See [Running on Windows](Running-on-Windows). |
| `enabled` | `Bool` | `true` | When `false`, the job is parsed and validated but never scheduled or runnable. |

### Output capturing

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `captureStdout` | `Bool` | `false` | Capture the process's stdout for failure detection and reports. When false, the job's stdout passes through to cronstable's stdout. |
| `captureStderr` | `Bool` | `true` | Capture the process's stderr for failure detection and reports. When false, the job's stderr passes through to cronstable's stderr. |
| `saveLimit` | `Int` | `4096` | Maximum number of captured lines retained per stream (split into the first half and the last half; lines in between are discarded and counted). `0` disables retention. |
| `maxLineLength` | `Int` | `16777216` (16 MiB) | Maximum bytes buffered per line by the stream reader. A longer line is skipped with a warning. |
| `streamPrefix` | `Str` | `[{job_name} {stream_name}] ` | Format string prefixed to every emitted output line. Supports `{job_name}` and `{stream_name}` placeholders; set to `""` to disable. |

See [Output Capturing](Output-Capturing) for buffering, truncation, and the
captured-output handoff to reporters.

### Scheduling time base

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `utc` | `Bool` | `true` | When true, the schedule is interpreted in UTC. When false (and no `timezone`), local time is used. |
| `timezone` | `Str` | none | IANA timezone name (e.g. `America/Los_Angeles`) overriding `utc`. An unknown name raises a `ConfigError`. |

See [Schedules and Timezones](Schedules-and-Timezones).

### Concurrency and timeouts

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `concurrencyPolicy` | `Enum(["Allow", "Forbid", "Replace"])` | `Allow` | Behavior when a scheduled run overlaps a still-running instance. `Allow`: run concurrently. `Forbid`: skip the new run. `Replace`: cancel the running instance and start the new one. |
| `concurrencyScope` | `Enum(["node", "cluster"])` | `node` | How far `concurrencyPolicy` reaches. `node` (default): an overlap is another instance in this cronstable process, exactly as before. `cluster`: `Forbid`/`Replace` also exclude instances of the job on other nodes sharing the [`state`](#state) store, via a per-job TTL slot lease (`slots/<job name>`) in the state store -- it works with or without a `cluster` section. Requires a `state` section somewhere in the assembled config: without one, load fails with a `ConfigError` naming the offending job(s). `cluster` with `concurrencyPolicy: Allow` is likewise a `ConfigError` at load (Allow places no bound on concurrent instances, so widening its scope gates nothing). Enforcement is best-effort at-least-once, not exactly-once; `state.slotTtlSeconds` and `state.onStoreUnavailable` govern the edges. Part of the [job-set id](Clustering-and-Leader-Election#the-job-set-id-foundation) **only when set to `cluster`** (existing configs keep their digests; replicas disagreeing on it show as drift). See [Concurrency and Timeouts](Concurrency-and-Timeouts#concurrency-across-a-cluster). |
| `clusterPolicy` | `Enum(["Leader", "PreferLeader", "EveryNode"])` | `Leader` | Where this job runs under cluster leader election. **Inert unless `cluster.electLeader` is set** (without election every job runs on every instance). `Leader`: only the quorum-gated leader runs it (at-most-once; may skip). `PreferLeader`: the lowest reachable agreeing node runs it, ignoring quorum (never skips; may double-run across a partition). `EveryNode`: every node runs it, independent of cluster health. Part of the [job-set id](Clustering-and-Leader-Election#the-job-set-id-foundation). See [Clustering and Leader Election](Clustering-and-Leader-Election#per-job-policy). |
| `executionTimeout` | `Float` | none | Seconds after which a still-running run is terminated. Unset means no timeout. Must be `> 0` when set. Termination signals the job's whole process group/tree, so the timeout bounds the run's work -- descendants included -- not just its root process; the platform-specific escalation is under `killTimeout` below. See [Running on Windows](Running-on-Windows). |
| `killTimeout` | `Float` | `30` | Seconds to wait after the graceful terminate before the forced kill. Must be `>= 0`. A job spawns in its own session/process group (`start_new_session` on POSIX), and cancellation takes the whole group down, not just the process cronstable spawned. On POSIX the graceful step is a `SIGTERM` to the **group** (trappable); once the direct child exits or `killTimeout` elapses, the group is **unconditionally** `SIGKILL`ed -- the leader exiting says nothing about descendants still holding the job's output pipes, and an already-empty group makes that a no-op. On Windows there is no graceful group signal: the graceful step remains an immediate `TerminateProcess` of the direct child (ungraceful, no notification), and the forced step walks the live process tree with `taskkill /F /T`; a descendant already orphaned when `taskkill` runs is missed, which is why the post-kill stream drain is separately bounded. See [Running on Windows](Running-on-Windows). |

See [Concurrency and Timeouts](Concurrency-and-Timeouts).

### Failure detection

`failsWhen` determines when a completed run is treated as a failure. In the
strictyaml schema only `producesStdout` is a required key inside `failsWhen`;
the others are optional. The `DEFAULT_CONFIG` defaults are:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `failsWhen.producesStdout` | `Bool` | `false` | Fail if any stdout was captured. |
| `failsWhen.producesStderr` | `Bool` | `true` | Fail if any stderr was captured. |
| `failsWhen.nonzeroReturn` | `Bool` | `true` | Fail if the process exits with a non-zero return code. |
| `failsWhen.always` | `Bool` | `false` | Fail whenever the process exits, regardless of output or return code. |

```yaml
jobs:
  - name: example
    command: echo "hi"
    schedule: "* * * * *"
    failsWhen:
      producesStdout: false
      producesStderr: true
      nonzeroReturn: true
      always: false
```

See [Failure Detection and Retries](Failure-Detection-and-Retries).

### Retries and reporting hooks

Three lifecycle hooks each carry a `report` block (mail, sentry, shell,
webhook); `onFailure` additionally carries a `retry` block. The `report` blocks
all share the same `_report_schema` and the same `_REPORT_DEFAULTS` (deep-copied
so the three blocks do not alias one another).

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `onFailure.retry.maximumRetries` | `Int` | `0` | Max retry attempts after a failure. `0` disables retries; `-1` retries forever. Must be `>= -1`. |
| `onFailure.retry.initialDelay` | `Float` | `1` | Seconds before the first retry. Must be `>= 0`. |
| `onFailure.retry.maximumDelay` | `Float` | `300` | Upper bound on the backoff delay. Must be `> 0`. |
| `onFailure.retry.backoffMultiplier` | `Float` | `2` | Multiplier applied to the delay between retries (exponential backoff). Must be `> 0`. |
| `onFailure.report` | `_report_schema` (`mail`/`sentry`/`shell`/`webhook`) | defaults below | Reporters fired on every detected failure (including each failed attempt). |
| `onPermanentFailure.report` | `_report_schema` | defaults below | Reporters fired only after all retries are exhausted. |
| `onSuccess.report` | `_report_schema` | defaults below | Reporters fired on a successful run. |

Inside `onFailure.retry`, all four keys (`maximumRetries`, `initialDelay`,
`maximumDelay`, `backoffMultiplier`) are required by the strictyaml schema once
a `retry` block is present. See
[Failure Detection and Retries](Failure-Detection-and-Retries).

The `report` blocks are covered in full on [Reporting (Mail, Sentry, Shell, Webhook)](Reporting);
their schema and `_REPORT_DEFAULTS` are summarized here.

#### `report.mail`

`from` and `to` are required keys (each `EmptyNone() | Str`). Defaults from
`_REPORT_DEFAULTS["mail"]`:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `from` | `EmptyNone() \| Str` | `None` | Sender address (required key). |
| `to` | `EmptyNone() \| Str` | `None` | Recipient address(es) (required key). |
| `smtpHost` | `Str` | `None` | SMTP server host. |
| `smtpPort` | `Int` | `25` | SMTP server port. |
| `subject` | `Str` | jinja2 default subject template | Email subject (jinja2). |
| `body` | `Str` | jinja2 default body template | Email body (jinja2). |
| `username` | `Str` | `None` | SMTP login username (enables login with `password`). |
| `password` | `Map` with `value`/`fromFile`/`fromEnvVar` (each `EmptyNone() \| Str`) | all `None` | SMTP login password source. |
| `tls` | `Bool` | `false` | Use implicit TLS. |
| `starttls` | `Bool` | `false` | Use STARTTLS. |
| `validate_certs` | `Bool` | `true` | Validate TLS certificates. Defaults to `true` in cronstable (a breaking change from upstream). |
| `html` | `Bool` | `false` | Send the body as HTML. |

#### `report.sentry`

Defaults from `_REPORT_DEFAULTS["sentry"]`:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `dsn` | `Map` with `value`/`fromFile`/`fromEnvVar` (each `EmptyNone() \| Str`) | all `None` | Sentry DSN source. |
| `fingerprint` | `Seq(Str)` | `["cronstable", "{{ environment.HOSTNAME }}", "{{ name }}"]` | Issue-grouping fingerprint (jinja2 per entry). Replaces, never appends, on merge. |
| `level` | `Str` | unset (effective `error`) | Sentry event level. When unset, events are captured at level `error`. |
| `extra` | `MapPattern(Str, Str \| Int \| Bool)` | unset | Extra structured context. |
| `body` | `Str` | default subject + body templates | Event message (jinja2). |
| `environment` | `Str` | `None` | Sentry environment. |
| `maxStringLength` | `Int` | `8192` | Max string length before Sentry truncation. |

#### `report.shell`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `shell` | `Str` | `/bin/sh` (POSIX) / empty (Windows) | Shell used to run the reporter command. The default is platform-specific, same as the per-job `shell` field: on Windows the default is empty (the reporter command runs via cmd.exe through `%ComSpec%`). Set `shell:` explicitly for another interpreter, or pass `command` as a list. See [Running on Windows](Running-on-Windows). |
| `command` | `Str` or `Seq(Str)` | `None` | Reporter command (required key). Receives `CRONSTABLE_*` environment variables. |
| `timeout` | `Float` | `60` | Hard bound, in seconds, on the reporter command; on expiry its whole process group is killed. Reports run inline on the daemon's job-completion loop, so a reporter that never exits would otherwise stall completion handling for every job. |

#### `report.webhook`

Defaults from `_REPORT_DEFAULTS["webhook"]`:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `url` | `Map` with `value`/`fromFile`/`fromEnvVar` (each `EmptyNone() \| Str`) | all `None` | Webhook URL source (treated as a secret; never logged). No URL means webhook reporting is disabled. |
| `method` | `Str` | `POST` | HTTP method. |
| `contentType` | `Str` | `application/json` | `Content-Type` header value. |
| `headers` | `MapPattern(Str, Str)` | `{}` | Extra request headers, sent verbatim (not templated). |
| `body` | `Str` | default webhook body template | Request body (jinja2). The default is a Slack-compatible `{"text": ...}` JSON payload of the default subject + body text. |
| `timeout` | `Float` | `10` | Total request timeout, seconds. |

### Durable state and catch-up

These options build on the top-level [`state`](#state) store and only take
full effect when a `state` section is configured; without one they parse and
validate normally but change nothing. The one exception is
`onlyIfLastSucceeded`, which also works without a `state` section from the
in-memory history alone (the gate then resets on restart). See
[Durable State](Durable-State) for the full semantics.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `onMissed` | `Enum(["skip", "run-once", "run-all"])` | `skip` | Missed-run catch-up after downtime, computed from the durable last-run watermark. `skip` (classic behavior): occurrences missed while down are not run. `run-once`: fire once at boot, coalescing all missed slots. `run-all`: replay each missed occurrence. Inert without a `state` section. |
| `startingDeadlineSeconds` | `Int` or null | none | Only occurrences missed within this many seconds are caught up; unset means no deadline. Bounds `run-all` to a recent window so a long outage cannot stampede (like the Kubernetes CronJob field of the same name). Also invalidates a persisted retry ladder older than the deadline. Must be `> 0` when set. Only meaningful with a `state` section. |
| `catchupJitterSeconds` | `Int` | `0` | Spread the boot-time catch-up launches of different jobs over `[0, N)` seconds, deterministic per job name, so a fleet of jobs does not all fire at once on restart. `0` fires them together. Must be `>= 0`. Only meaningful with a `state` section. |
| `onlyIfLastSucceeded` | `Bool` | `false` | Depends-on-past gate: skip a scheduled fire when the job's most recent finished run did not succeed, or when a previous instance is still running (unless `concurrencyPolicy: Replace`). The last real outcome is the newest of the in-memory history and the durable run ledger; cancelled and skipped runs are ignored; retries, catch-up backfills, and manual API triggers deliberately bypass the gate. Works without a `state` section from the in-memory history alone (resetting on restart); with one, the gate's memory survives restarts. |
| `archiveOutput` | `Bool` | `false` | Persist each finished run's captured output durably to the state store (the job's `logs/` stream). Encryption at rest is the mount's job (EFS/S3 SSE, an encrypted volume). Inert without a `state` section (a startup warning notes it archives nothing). |
| `redactArchivedSecrets` | `Bool` | `true` | Scrub recognisable secrets (tokens, passwords, keys, auth URLs) from archived output before it is written. Applies only when `archiveOutput` is set, so it too has no effect without a `state` section. |

### Environment

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `environment` | `Seq(Map({"key": Str, "value": Str}))` | `[]` | Environment variables set for the process. Both `key` and `value` are required per entry. Merged by key with `defaults` and with `env_file` (config values win). |
| `env_file` | `Str` | none | Path to a `KEY=VALUE` file; blank lines and `#` comments are ignored. Variables in `environment` override file values. A read error or a line without `=` raises a `ConfigError`. |
| `secrets` | `Seq(Map({"name": Str, "value"/"fromFile"/"fromEnvVar": Str}))` | `[]` | Run-scoped secrets staged for the job over the [job-facing state endpoint](Durable-State#run-scoped-secrets) rather than placed in the environment, so they never show in `/proc/<pid>/environ`. Each needs a `name` and exactly one source (a nameless or sourceless entry is a `ConfigError`; a same-named entry merges last-wins, like `environment`). The job reads one with `cronstable secret get NAME`. Requires a `state` section with `jobApi` enabled, else load fails naming the offending job(s). |
| `stateAllowedScopes` | `Seq(Str)` | `[]` | Extra scope names (besides the job's own name and `global`) this job's `cronstable state\|cursor\|lock\|artifact` calls may explicitly name via `--scope`. Naming any other scope -- most dangerously another job's own name, which IS that job's private scope -- is refused (`403`). See [Scopes](Durable-State#scopes). |

```yaml
jobs:
  - name: example
    command: env
    schedule: "* * * * *"
    env_file: .env
    environment:
      - key: PATH
        value: /bin:/usr/bin
```

See [Commands and Environment](Commands-and-Environment).

### Privilege switching

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `user` | `Str` or `Int` | none | User name or numeric uid the process runs as. A numeric uid derives its primary gid and login name from the passwd database. An unknown name raises a `ConfigError`. |
| `group` | `Str` or `Int` | none | Group name or numeric gid the process runs as. If only `user` is set, the group defaults to that user's primary group. An unknown name raises a `ConfigError`. |

This section is POSIX-only (the setuid/setgid model). On POSIX, setting `user`
or `group` requires cronstable to run as root (euid 0); otherwise a `ConfigError`
is raised. Privilege switching is **not supported on Windows**: a job with
`user` or `group` set raises a configuration error, verbatim
`Job <name>: changing user/group is not supported on Windows`. See
[Production and Container Deployment](Production-Deployment) and
[Running on Windows](Running-on-Windows).

### Metrics

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `statsd` | `Map({"prefix": Str, "host": Str, "port": Int})` | none | When set, emit start/stop/success/duration metrics over UDP. All three keys are required. |
| `monitorResources` | `Bool` or `Map` | `false` | Sample each run's CPU time and peak resident memory (RSS) by polling the job's process tree while it runs. Observability only: the numbers ride the run record into the dashboard, `GET /metrics` and statsd, but never change a run's success/failure verdict. Off by default; a per-instance sampling task is spawned only when it is on. Set it under `defaults:` to enable it fleet-wide. The map form tunes it: `enabled` (`Bool`, default `true`), `interval` (`Float` seconds between samples, default `1.0`), `history` (`Int` chart-series points kept per run, default `240`; `0` keeps summary numbers only). Full feature guide: [resource monitoring](Resource-Monitoring). |

See [Metrics with statsd](Metrics-with-Statsd). Prometheus metrics are not
configured per job: the `GET /metrics` endpoint is global, tuned under
`web.metrics` in the `web` section above. See
[Metrics with Prometheus](Metrics-with-Prometheus).

**Resource accounting (`monitorResources`).** With it on, a run is sampled by
[psutil](https://github.com/giampaolo/psutil) (a core dependency) over its
whole process tree, so a job that shells out to child processes is accounted
too. The result (total user/system CPU seconds and the peak RSS observed)
appears in the dashboard run history and stats, in the durable run record's
`resources` object (so it survives a restart), and as the metrics listed in
[Metrics with Prometheus](Metrics-with-Prometheus). Report templates and the
shell reporter also receive `cpu_seconds` / `max_rss_bytes`
(`CRONSTABLE_CPU_SECONDS` / `CRONSTABLE_MAX_RSS_BYTES`). This section covers
the config surface; the full feature guide is
[resource monitoring](Resource-Monitoring).

DAG tasks accept `monitorResources` too, but surface the result differently:
a finished task instance's usage is recorded in the `resources` object of its
task record inside the durable `dag_run` document (returned by
`GET /dags/<name>/runs/<run_key>`), and sent to the task's statsd sink when
one is configured. DAG task instances are ephemeral and do not appear in the
per-job Prometheus families on `GET /metrics`.

The numbers are **sampled**,
so a run that finishes between two samples is measured approximately; the long,
heavy runs whose resource use actually matters are sampled many times. It is
best-effort: if psutil cannot read a process (already exited, permission
denied) the run simply carries no resource stats, and monitoring never fails a
job.

**Sampling cadence and chart series.** The map form controls how monitoring
behaves:

```yaml
monitorResources:
  interval: 0.5     # seconds between samples (default 1.0, minimum 0.1)
  history: 240      # chart points kept per run (default 240; 0 = summary only)
```

`interval` sets the process-tree polling cadence: shorter intervals catch
sharper RSS spikes at the cost of more wakeups (each sample walks the process
table, hence the 0.1s floor). Alongside the summary numbers, each monitored
run records a **downsampled CPU%/RSS time series** for the dashboard's
Resources tab: one point per sample until `history` points accumulate, after
which adjacent points merge (mean CPU%, peak RSS — spikes are never averaged
away) and the effective resolution halves, so a run of any length stays within
`history` points (at most 2000). The series is embedded in the durable run
record's `resources.series`, so charts survive restarts and are bounded by the
same `state.maxRunsPerJob` retention as run records; it is served by
`GET /jobs/<name>/resources` and deliberately excluded from the polled
`/jobs` and `/jobs/<name>/runs` payloads.

The dashboard's node-level CPU/memory history chart is configured separately,
under `web.nodeHistory` (see the `web` section): `interval` (`Float` seconds,
default `5.0`, minimum `1.0`) and `points` (`Int` ring size, default `720` —
an hour at the default cadence), or `nodeHistory: false` to disable the
background node sampler entirely.

## Load-time numeric validation

strictyaml enforces only the type (`Int`/`Float`). After type validation,
`JobConfig._validate_numeric_ranges` enforces value ranges (plus one
cross-field rule) and raises a `ConfigError` (prefixed `Job <name>:`) on
violation. These checks run at load time, not at run time. New in the cronstable
fork.

| Rule | Condition |
| --- | --- |
| `saveLimit >= 0` | always |
| `maxLineLength > 0` | always |
| `killTimeout >= 0` | always |
| `concurrencyPolicy` is `Forbid` or `Replace` | only when `concurrencyScope: cluster` (widening `Allow` gates nothing, so it is rejected rather than ignored) |
| `executionTimeout > 0` | only when `executionTimeout` is set |
| `catchupJitterSeconds >= 0` | always |
| `startingDeadlineSeconds > 0` | only when `startingDeadlineSeconds` is set |
| `onFailure.retry.maximumRetries >= -1` | only when a `retry` block is present |
| `onFailure.retry.initialDelay >= 0` | only when a `retry` block is present |
| `onFailure.retry.maximumDelay > 0` | only when a `retry` block is present |
| `onFailure.retry.backoffMultiplier > 0` | only when a `retry` block is present |
| `monitorResources.interval >= 0.1` | always (a sub-100ms cadence would busy-loop the process-table walk) |
| `0 <= monitorResources.history <= 2000` | always (bounds what one run adds to a durable ledger record) |

## Minimal valid example

```yaml
defaults:
  shell: /bin/bash   # POSIX path; on Windows omit shell (uses cmd.exe) or set a Windows interpreter
  utc: false

jobs:
  - name: nightly-backup
    command: /usr/local/bin/backup.sh
    schedule: "0 3 * * *"
    captureStdout: true
    captureStderr: true
    executionTimeout: 3600
    onFailure:
      retry:
        maximumRetries: 3
        initialDelay: 5
        maximumDelay: 60
        backoffMultiplier: 2
```
