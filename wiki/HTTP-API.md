# HTTP Control API

cronstable exposes an optional [aiohttp](https://docs.aiohttp.org/) REST control API,
enabled by adding a top-level `web` section to the configuration. It serves
endpoints for querying the daemon version, inspecting job status, starting and
cancelling jobs on demand, reading per-job run history, tailing captured job
output live, exposing [Prometheus metrics](Metrics-with-Prometheus), and (when
a `cluster` section is configured) reporting the cluster and fleet views. This page documents the configuration schema, every endpoint,
bearer-token authentication, Unix-socket permissions, and lifecycle behavior.

The interface is inherited from upstream yacron; `web.authToken` and
`web.socketMode` are cronstable additions.

> **Looking for the browser UI?** The same HTTP interface also serves the
> built-in **[Web Dashboard](Web-Dashboard)** at `/` on every `http://` listener
> (enabled by default; disable it with `ui: false`). This page documents the REST
> endpoints; the [Web Dashboard](Web-Dashboard) page is the visual tour.

## Enabling the API

Add a `web` section with at least one `listen` URL:

```yaml
web:
  listen:
    - http://127.0.0.1:8080
    - unix:///tmp/cronstable.sock
```

> **Windows:** `unix://` listeners are not supported on Windows. On Windows
> such a listen URL is skipped with the warning `Ignoring web listen url
> <url>: unix-socket listeners are not supported on this platform` (aiohttp's
> `UnixSite` needs `create_unix_server`, which the Windows Proactor loop
> lacks); use an `http://` listener instead. The `http://` listener and the
> entire HTTP control API otherwise behave identically on Windows. See
> [Running on Windows](Running-on-Windows).

The server is created only when `web.listen` is non-empty. There must be exactly one
`web` block across the whole configuration: a duplicate `web` block in an included file
or a second file in a config directory raises a `ConfigError`. See
[Includes, Defaults, and Multi-File Config](Includes-and-Defaults).

## Configuration reference

The `web` section is parsed by the strictyaml `CONFIG_SCHEMA` in `cronstable/config.py`.
`listen` is required; the rest are optional (strictyaml `Opt(...)`).

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `listen` | sequence of strings | (required) | List of URLs to bind. Each is `http://host:port` or `unix:///path`. An empty list disables the server. |
| `headers` | map of string→string | (none) | Extra HTTP headers added to every `200` success response (all routes, including `/cluster` and `/job-set-id`) and to the 409 conflict body, but not the 404 or 401. |
| `allowedOrigins` | sequence of strings | `[]` | Extra exact-match browser `Origin`s allowed to call the mutating `POST` endpoints (see [Cross-site request defense](#cross-site-request-defense)). |
| `authToken` | map (`value`/`fromFile`/`fromEnvVar`) | (none) | When set, requires bearer-token authentication on all routes (see [Authentication](#authentication)). |
| `socketMode` | string (octal) | (none) | File mode applied via `chmod` to `unix://` listen sockets (see [Unix socket permissions](#unix-socket-permissions)). Applies only to `unix://` sockets, so it is irrelevant on Windows (where unix-socket listeners are unsupported and skipped with a warning). |
| `ui` | bool | `true` | Serve the [Web Dashboard](Web-Dashboard) page at `/` (see [`GET /`](#get--the-dashboard-page)); `ui: false` exposes only the REST endpoints. |
| `metrics` | bool or map | `true` | Serve the Prometheus exposition at `/metrics`; the map form tunes buckets or exempts the endpoint from `authToken` (see [`GET /metrics`](#get-metrics)). |
| `nodeHistory` | bool or map | `true` | Background node CPU/memory sampling that feeds [`GET /node/history`](#get-nodehistory); the map form tunes cadence and window size (see [`web.nodeHistory`](Configuration-Reference#web)). |

### `listen` URL forms

| Scheme | Form | Requirements |
| --- | --- | --- |
| `http` | `http://host:port` | Both host and port are required. An `http` URL missing either is logged as a warning (`Ignoring web listen url ...: http url needs host and port`) and skipped. |
| `unix` | `unix:///path/to/socket` | Binds an `aiohttp` `UnixSite` at the given filesystem path. POSIX-only: on Windows `UnixSite` is unavailable (no `create_unix_server` on the Proactor loop), so such a URL is skipped with the warning `Ignoring web listen url <url>: unix-socket listeners are not supported on this platform`. Use an `http://` listener instead. |

Any other scheme is logged (`scheme ... not supported`) and skipped. Binding maps to
`web.TCPSite` for `http` and `web.UnixSite` for `unix` (`web_site_from_url` in
`cronstable/cron.py`).

`https` is not a recognized scheme. To serve the API over TLS, bind to a loopback
`http` address or a `unix` socket (POSIX-only; on Windows use a loopback `http`
address) and terminate TLS in a reverse proxy.

## Endpoints

All routes are registered in `start_stop_web_app`:

| Method | Path | Handler | Success status |
| --- | --- | --- | --- |
| `GET` | `/version` | `_web_get_version` | `200` |
| `GET` | `/job-set-id` | `_web_job_set_id` | `200` |
| `GET` | `/cluster` | `_web_get_cluster` | `200` |
| `GET` | `/fleet` | `_web_get_fleet` | `200` |
| `GET` | `/node` | `_web_get_node` | `200` |
| `GET` | `/node/history` | `_web_node_history` | `200` |
| `GET` | `/status` | `_web_get_status` | `200` |
| `GET` | `/schedule/preview` | `_web_schedule_preview` | `200` (`400` for a missing `expr` or unknown `tz`) |
| `GET` | `/schedule/pressure` | `_web_schedule_pressure` | `200` (`400` for an unknown `tz`) |
| `GET` | `/schedule/duplicates` | `_web_schedule_duplicates` | `200` |
| `GET` | `/schedule/suggest` | `_web_schedule_suggest` | `200` (`400` for a bad `period` or unknown `tz`) |
| `GET` | `/schedule/why` | `_web_schedule_why` | `200` (`400` for a missing `job`/`at` or an unparseable `at`; `404` for an unknown job) |
| `GET` | `/calendar.ics` | `_web_calendar` | `200` (`text/calendar`) |
| `GET` | `/jobs` | `_web_list_jobs` | `200` |
| `GET` | `/jobs/{name}/runs` | `_web_job_runs` | `200` |
| `GET` | `/jobs/{name}/calendar.ics` | `_web_job_calendar` | `200` (`text/calendar`; `404` for an unknown job) |
| `GET` | `/jobs/{name}/resources` | `_web_job_resources` | `200` |
| `GET` | `/jobs/{name}/trends` | `_web_job_trends` | `200` |
| `POST` | `/jobs/{name}/start` | `_web_start_job` | `200` |
| `POST` | `/jobs/{name}/cancel` | `_web_cancel_job` | `200` |
| `GET` | `/jobs/{name}/logs` | `_web_job_logs` | `200` (SSE stream) |
| `GET` | `/dags` | `_web_list_dags` | `200` |
| `GET` | `/dags/{name}/runs` | `_web_dag_runs` | `200` |
| `GET` | `/dags/{name}/runs/{run_key}` | `_web_dag_run` | `200` |
| `GET` | `/dags/{name}/runs/{run_key}/xcom` | `_web_dag_xcom` | `200` |
| `GET` | `/dags/{name}/runs/{run_key}/tasks/{taskkey}/logs` | `_web_dag_task_logs` | `200` (SSE stream) |
| `POST` | `/dags/{name}/trigger` | `_web_dag_trigger` | `200` |
| `POST` | `/dags/{name}/backfill` | `_web_dag_backfill` | `200` |
| `POST` | `/dags/{name}/runs/{run_key}/tasks/{taskkey}/decision` | `_web_dag_decision` | `200` |
| `GET` | `/state` | `_web_state` | `200` |
| `GET` | `/state/documents` | `_web_state_documents` | `200` |
| `GET` | `/state/records` | `_web_state_records` | `200` |
| `POST` | `/mcp` | `MCPHandler.handle_http` | `200` (JSON-RPC response; `202` for a notification; omitted unless `mcp.enabled`) |
| `GET` | `/mcp` | `MCPHandler.handle_http_get` | `405` (always, with `Allow: POST, OPTIONS`; omitted unless `mcp.enabled`) |
| `OPTIONS` | `/mcp` | `MCPHandler.handle_options` | `204` (CORS preflight for an allow-listed `Origin`; omitted unless `mcp.enabled`) |
| `GET` | `/metrics` | `_web_metrics` | `200` (Prometheus exposition; omitted when `metrics: false`) |
| `GET` | `/` | `_web_index` | `200` (dashboard page; omitted when `ui: false`) |

The `/dags/...` routes are documented under [DAG endpoints](#dag-endpoints),
the `/state...` routes under
[State inspector endpoints](#state-inspector-endpoints), and the `/mcp`
routes under [`POST /mcp`](#post-mcp-the-mcp-server).

The configured `headers` map is applied to every `200` success response across
all routes (including `/cluster` and `/job-set-id`) and to the `409` conflict
bodies of `/jobs/{name}/start` and `/jobs/{name}/cancel`. The `404` (unknown
job) and `401` (authentication failure) responses are raised without it.

> The same interface serves the **[Web Dashboard](Web-Dashboard)** at `/`; that
> page is the visual tour of the UI these endpoints feed.

### `GET /version`

Returns the cronstable version as `text/plain` (the value of `cronstable.version.version`).

```shell
$ http get http://127.0.0.1:8080/version
HTTP/1.1 200 OK
Content-Type: text/plain; charset=utf-8

1.0.13
```

### `GET /status`

Returns the status of every configured job. The response format depends on the
request's `Accept` header:

- If `Accept` is exactly `application/json`, the response is a JSON array
  (`application/json`).
- Otherwise the response is `text/plain`, one job per line.

Each job has one of three statuses, determined in this order:

| Status | When | Fields |
| --- | --- | --- |
| `running` | One or more instances are currently running. | `job`, `status`, `pid` (list of the PIDs of running instances whose subprocess has been started, i.e. `runjob.proc is not None`). |
| `disabled` | The job is not running and `enabled: false`. | `job`, `status`. |
| `scheduled` | The job is not running and is enabled. | `job`, `status`, `scheduled_in`. |

For `scheduled` jobs, `scheduled_in` is the number of seconds until the next run
(a float, computed from the job's crontab in the job's timezone). For an `@reboot`
schedule, `scheduled_in` is the literal string `"@reboot"`.

A `scheduled` job whose crontab has **no future occurrence** (a fixed past
year, an impossible date) reports `scheduled_in: null` plus `never_fires:
true`, and the text form says `never fires (schedule has no future
occurrence)`; see [Schedule Linting](Schedule-Linting).

The `disabled` status is reported honestly instead of an inapplicable
`scheduled (in N seconds)`.

Text form:

```shell
$ http get http://127.0.0.1:8080/status
HTTP/1.1 200 OK
Content-Type: text/plain; charset=utf-8

test-01: scheduled (in 14 seconds)
test-02: running (pid: 12345)
test-03: disabled
```

In the text form, `scheduled_in` is rendered as a human-readable relative time
(`in N seconds` / `minutes` / `hours` / `days`), running jobs show
`running (pid: <comma-separated pids>)`, and disabled jobs show `disabled`.

JSON form:

```shell
$ http get http://127.0.0.1:8080/status Accept:application/json
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

[
    {"job": "test-01", "status": "scheduled", "scheduled_in": 6.16588},
    {"job": "test-02", "status": "running", "pid": [12345]},
    {"job": "test-03", "status": "disabled"}
]
```

### `GET /schedule/preview`

Parses, describes, previews and lints one cron expression with the daemon's
own engine, the single source of truth behind the dashboards' sandboxes, so
a preview can never disagree with what the scheduler will actually do.

Query parameters:

| Parameter | Meaning |
| --- | --- |
| `expr` | **Required.** The expression to decode (URL-encoded). `400` when missing or blank. |
| `tz` | Optional IANA zone for the preview frame and the DST lint checks (default `UTC`). `400` for an unknown name. |
| `count` | Optional number of upcoming fires to return, clamped to 1–60 (default 12). |
| `seed` | Optional hash key (a job name, real or prospective) that resolves [`H` items](Hashed-Schedules). Without it an `H` expression comes back `valid: false` with the engine's own error; with it the response echoes `seed` and adds `resolved`, the expression with every `H` replaced by its hashed values. |

For an expression the engine accepts, the response carries `valid: true`,
the whitespace-`normalized` form, the plain-English `description`, the next
`fires` as ISO-8601 instants in the requested frame, `never_fires` (no
remaining occurrence), and the [schedule linter's](Schedule-Linting)
findings. For a rejected expression it carries `valid: false` and the
parser's `error` (including the Quartz dialect hints). `@reboot` returns
`valid: true, reboot: true` with no fires. The `cron_validate_schedule`
and `cron_explain_schedule` [MCP tools](MCP) serve this same payload to
agents.

```shell
$ http get "http://127.0.0.1:8080/schedule/preview?expr=*/7 * * * *&count=2"
{
    "expression": "*/7 * * * *",
    "timezone": "UTC",
    "valid": true,
    "reboot": false,
    "normalized": "*/7 * * * *",
    "description": "At minutes 00, 07, 14, 21, 28, 35, 42, 49, 56 past every hour, every day",
    "fires": ["2026-07-18T15:42:00+00:00", "2026-07-18T15:49:00+00:00"],
    "never_fires": false,
    "lint": [
        {
            "code": "uneven-step",
            "level": "warning",
            "message": "'*/7' in the minute field: 7 does not divide the field's span of 60, so one interval at the wrap is only 4 minutes"
        }
    ]
}
```

### `GET /schedule/pressure`

The fleet's forward-looking collision heatmap: every enabled schedule's
fires over the next `hours` (1 to 168, default 24), enumerated with the
scheduler's own engine and bucketed by civil hour and minute in `tz`
(default `UTC`; `400` for an unknown name). The payload carries the 24x60
`grid`, the 60-bin `by_minute_fires`/`by_minute_jobs` histograms,
`by_hour`, the `busiest_minute` headline, `empty_minutes`, `top_cells`
(each naming up to ten jobs), and an `excluded` count of disabled and
`@reboot` jobs. Full field reference on the
[Schedule Pressure](Schedule-Pressure) page.

### `GET /schedule/duplicates`

Groups of jobs whose schedules fire on the identical instants, by the
engine's semantic equality (`*/5` equals `0-59/5`; grouping includes the
resolved timezone). Each group carries the most common source
`expression`, a plain-English `description`, `timezone`, `count`, and the
member `jobs`, sorted largest group first. See
[Duplicate Schedule Detection](Duplicate-Schedule-Detection).

### `GET /schedule/suggest`

The least-loaded slot for a new job, scored on the same 24-hour fire walk
as `/schedule/pressure`. `period` is `hourly` (pick a minute) or `daily`
(pick a minute and hour; `400` otherwise), `tz` frames the daily pick.
Returns the winning `expression`, its `fires_in_window`, the `busiest`
slot for contrast, two `alternatives`, and a `hash_hint` naming the
[`H` spelling](Hashed-Schedules) that spreads jobs without this endpoint.
See [Suggest a Slot](Suggest-a-Slot).

### `GET /schedule/why`

Explains field by field why one job's schedule did or did not select one
timestamp, decomposing the same match test the scheduler runs.

Query parameters:

| Parameter | Meaning |
| --- | --- |
| `job` | **Required.** The job name; a DAG's synthetic `dag:<name>` schedule job resolves too. `404` for an unknown name. |
| `at` | **Required.** An ISO-8601 timestamp. With a UTC offset (`2026-07-14T09:00:00+02:00`, trailing `Z` accepted) it converts into the job's resolved timezone; a naive timestamp reads as wall time there. `400` when missing or unparseable. |

The response carries one `checks` row per cron field (`field`, the
probed `value` with its human `label`, the field's accepted values as
prose in `allowed`, and `matched`), the overall `matches` verdict with
the `failed` field names in field order, and the nearest real
`previous_fire` / `next_fire` around the probe, computed with the
scheduler's own occurrence walk in the job's zone. `notes` flags the
semantics that make an answer genuinely surprising: `day-fields-and-rule`
(both day fields restricted, exactly one matched, so classic Vixie cron
would have fired; see [Schedule Linting](Schedule-Linting)) and
`dst-skipped-time` / `dst-repeated-time` for a matching wall time a DST
transition skips or repeats. An [`H` schedule](Hashed-Schedules) reports
its `resolved` spelling and checks against the resolved slots. `@reboot`
jobs answer `reboot: true` with no checks; a disabled job still explains
its timetable and reports `enabled: false`. The `cron_why_no_run`
[MCP tool](MCP) serves the same payload to agents. See
[Why Didn't It Run?](Why-No-Run) for a walkthrough.

### `GET /calendar.ics` and `GET /jobs/{name}/calendar.ics`

The upcoming fires as a standard iCalendar (RFC 5545) feed, fleet-wide or
per job, `Content-Type: text/calendar`: one `VEVENT` per fire, enumerated by
the scheduler's own engine in each job's resolved timezone and emitted as
UTC instants with stable UIDs, so subscribed calendar apps update in place.
Query parameters `days` (window, default 14, clamped 1 to 60) and `per_job`
(event cap per job, default 100, clamped 1 to 1000) are clamped rather than
erroring. Disabled and `@reboot` jobs carry no events; an unknown job name
on the per-job route is a `404`.

With [`web.authToken`](#authentication) set, the `.ics`
paths (only) also accept the token as a `token` query parameter, because
calendar clients cannot send a bearer header. Full event anatomy, privacy
rationale, and subscription notes: [Calendar Export](Calendar-Export).

```shell
curl "http://localhost:8080/calendar.ics?days=30"
curl http://localhost:8080/jobs/nightly-backup/calendar.ics
```

### `GET /cluster`

Returns this node's [cluster](Clustering-and-Leader-Election) view as JSON.
When no `cluster` section is configured, it returns
`{"enabled": false, "peers": []}`. When a cluster section is present it returns
`enabled: true` plus a `backend` field naming the active leadership backend
(`gossip`, `kubernetes`, `etcd`, or `filesystem`) and the node's view: its
`node_name` and [`job_set_id`](Job-Set-ID), the computed `cluster_size` and `quorum`, whether
`elect_leader` is on, the `distribution` mode (`single-leader` or `spread`),
whether any conflict is pausing `Leader` jobs (`conflict`, the umbrella flag),
whether this node is `quorate`, the elected `leader`
(`null` when this node is not quorate, and always `null` in `spread` mode) and
`is_leader` (always `false` in `spread` mode), and a `peers` array (each with
`host`, `status`, `node_name`, `job_set_id`, `last_seen`, `last_error`,
`mismatch_streak`, and `node_stats`). Under `distribution: spread`, per-job
owners instead appear as a `clusterOwner` field on each leader-gated job in
`GET /jobs`.

A top-level `node_stats` object carries **this** node's own live CPU/memory
(the same shape as [`GET /node`](#get-node)'s `resources`), always present (it
is local and free). Each peer's `node_stats` is its last-shared load — `null`
unless the cluster shares node stats via
[`cluster.observability`](Configuration-Reference#observability-overlay). The
dashboard's cluster panel renders this node's load in the summary and a per-peer
**Load** column when peers share.

The umbrella `conflict` flag is set by any of three triggers, each with its own
detail list, and all three stand `Leader` jobs down: a duplicate `nodeName` (the
offending names in `conflict_names`); an agreeing peer declaring a different
cluster size (`size_conflict: true`, the divergent sizes in `conflicting_sizes`);
and a quorate peer advertising a different `distribution` or `elect_leader`
setting, a coordination-policy conflict surfaced as `policy_conflict: true` with
the differing descriptors in `conflicting_policies`.

The **lease backends** (`kubernetes` / `etcd` / `filesystem`) have no static
peer set, so their view is lease-shaped: `backend` names the backend, `peers`
is empty, `cluster_size`/`quorum` are `1`, `elect_leader` is `true`,
`distribution` is
`single-leader`, all three conflict flags (`conflict`, `size_conflict`,
`policy_conflict`) are always `false`, and an extra `lease` block carries the
backend-specific detail. This is an endpoint-reference view; the field
semantics (what `quorate` means for a lease backend, the `lease` block contents,
and the `expiry` rules below) are documented once in
[Clustering and Leader Election](Clustering-and-Leader-Election#observing-the-cluster).
For `kubernetes` the block is `{name, namespace, identity, holder, expiry}`,
for `etcd` `{electionName, identity, holder, leaseId, expiry}`, and for
`filesystem` `{path, electionName, identity, holder, fence, expiry}`. `fence`
is the store's monotonic takeover counter: it bumps every time the lease
changes hands (a renew by the same holder keeps it). For `kubernetes` and
`etcd` the `expiry` is populated only while **this** node holds the lease: a
follower reports `expiry: null` (for `kubernetes` it is the local lease
deadline while this node leads; for `etcd` the current lease deadline). For
`filesystem` it is the written expiry of the last lease this node observed,
follower included. `quorate` is whether the node has a fresh successful read
of the lease store (stale → `Leader` fails closed; the never-skip
`PreferLeader` default then runs the job anyway).

```shell
$ http get http://127.0.0.1:8080/cluster Accept:application/json
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

{
    "enabled": true,
    "backend": "gossip",
    "node_name": "node-a",
    "job_set_id": "v1:…",
    "cluster_size": 3,
    "quorum": 2,
    "elect_leader": true,
    "distribution": "single-leader",
    "conflict": false,
    "conflict_names": [],
    "size_conflict": false,
    "conflicting_sizes": [],
    "policy_conflict": false,
    "conflicting_policies": [],
    "quorate": true,
    "leader": "node-a",
    "is_leader": true,
    "peers": [
        {"host": "cronstable-b.internal:8443", "status": "agreed", "node_name": "node-b", "job_set_id": "v1:…", "last_seen": "2026-06-23T19:00:00+00:00", "last_error": null, "mismatch_streak": 0}
    ]
}
```

A lease backend (here `kubernetes`) returns the lease-shaped view instead:

```jsonc
{
    "enabled": true,
    "backend": "kubernetes",
    "node_name": "cronstable-0",
    "job_set_id": "v1:…",
    "cluster_size": 1,
    "quorum": 1,
    "elect_leader": true,
    "distribution": "single-leader",
    "conflict": false, "conflict_names": [],
    "size_conflict": false, "conflicting_sizes": [],
    "policy_conflict": false, "conflicting_policies": [],
    "quorate": true,
    "leader": "cronstable-0",
    "is_leader": true,
    "peers": [],
    "lease": {"name": "cronstable-leader", "namespace": "default",
              "identity": "cronstable-0", "holder": "cronstable-0",
              "expiry": "2026-06-24T19:00:14.000000Z"}
}
```

The per-peer `status` values (`agreed`, `syncing`, `drifted`, `unreachable`,
`untrusted`, `self`, `conflict`, `unknown`) are documented in
[Clustering and Leader Election](Clustering-and-Leader-Election#per-peer-status).

> The separate `GET /peer` attestation endpoint is **not** part of this web API.
> It is served only on the cluster's own mutual-TLS `listen` address (default
> port `8443`), never on the public `web` listeners. When node stats are
> shared, each `/peer` response carries the node's live load as an
> `X-Cronstable-Node-Stats` response header (on `200` and `304` responses alike,
> never in the body), so sharing preserves that exchange's conditional `304`
> optimisation. See
> [Clustering and Leader Election](Clustering-and-Leader-Election).

### `GET /fleet`

Returns the cluster-wide per-job run view that backs the dashboard's
[fleet view](Web-Dashboard#fleet-view-every-nodes-runs-in-one-pane): one entry
per node, each carrying that node's per-job run summaries. It is answered
entirely from state this node already holds. Every gossip node piggybacks a
compact summary of its own jobs (running / enabled / seconds to next fire /
last finished run) on its mutual-TLS `/peer` response, so the summaries arrive
with the peer polls this node is already making; serving `/fleet` triggers no
peer traffic. Any node can therefore serve the whole fleet's picture, at most
one gossip `interval` stale per peer.

When no `cluster` section is configured, or the backend is a lease backend
(`kubernetes` / `etcd` / `filesystem`, which carry only a lease and know
nothing about what other nodes run), it returns
`{"enabled": false, "nodes": []}`.

For the gossip backend it returns `enabled: true`, the serving node's
`node_name`, the `distribution` and `elect_leader` policy, the gossip
`interval` in seconds (the peer-data freshness bound), and a `nodes` array.
The serving node is always first (`self: true`, status `self`, `as_of` stamped
at request time); each configured peer follows with the `status` and `host`
from the peer table and the summaries absorbed from its last successful poll
(`as_of` = `last_seen`). Self-listings are skipped and two addresses that
answer as the same process are deduplicated.

Per node: `jobs` maps each job name to
`{running, enabled, scheduled_in, last}`, where `last` is
`{outcome, finished_at, duration, exit_code}` or `null` for a job that has not
run since that node started. `jobs: null` (as opposed to `{}`) means no
snapshot is held for that node at all: it was never reached, or it runs an
older build that does not gossip summaries. Each node also carries `node_stats`
— its whole-node CPU/memory (the same shape as [`GET /node`](#get-node)'s
`resources`) — when the cluster shares node load via
[`cluster.observability`](Configuration-Reference#observability-overlay);
`null` when that node shares none. `truncated: true` flags a node
with more jobs than the per-payload cap (512), whose advertised set is the
sorted-name prefix. A briefly unreachable peer keeps its last-known summaries
with the old `as_of` rather than being blanked, so stale data is visibly stale
instead of silently missing.

```shell
$ http get http://127.0.0.1:8080/fleet Accept:application/json
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

{
    "enabled": true,
    "backend": "gossip",
    "node_name": "node-a",
    "distribution": "spread",
    "elect_leader": true,
    "interval": 30,
    "nodes": [
        {"node_name": "node-a", "host": null, "self": true, "status": "self",
         "as_of": "2026-06-23T19:00:02+00:00", "truncated": false,
         "jobs": {"backup": {"running": false, "enabled": true, "scheduled_in": 1042.5,
                             "last": {"outcome": "success", "finished_at": "2026-06-23T18:00:01+00:00",
                                      "duration": 12.4, "exit_code": 0}}}},
        {"node_name": "node-b", "host": "cronstable-b.internal:8443", "self": false, "status": "agreed",
         "as_of": "2026-06-23T18:59:45+00:00", "truncated": false,
         "jobs": {"backup": {"running": true, "enabled": true, "scheduled_in": null, "last": null}}}
    ]
}
```

The summaries are observability data only: they never feed leader election or
any run/skip decision, and a malformed or hostile peer payload degrades to
"no data for that node" rather than poisoning the view.

### `GET /node`

The serving node's **live** CPU and memory, sampled fresh per request (this is
what drives the dashboard header's node meter). Returns the node identity and a
`resources` object:

```shell
$ http get http://127.0.0.1:8080/node
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

{
    "node_name": "node-a",
    "resources": {
        "cpu_percent": 37.2,
        "cpu_count": 8,
        "mem_percent": 61.4,
        "mem_used_bytes": 5284167680,
        "mem_total_bytes": 8589934592,
        "proc_rss_bytes": 58720256,
        "proc_cpu_percent": 1.1
    }
}
```

`node_name` is the cluster node name when clustered, else the hostname.
`cpu_percent` / `mem_percent` are whole-host utilisation; `proc_rss_bytes` /
`proc_cpu_percent` are the cronstable daemon's own footprint (best-effort, may be
absent if the platform denies the per-process read). `resources` is `null` when
host sampling is unavailable (psutil could not read the host), and the
dashboard then hides the meter. CPU percentages are measured since the previous
sample, so the first request after startup reads a priming `0`.

**Containers.** When the daemon runs under a cgroup v2 limit (Docker/Kubernetes
memory or CPU limits, or a systemd slice with `MemoryMax`/`CPUQuota`), these
numbers describe **its own slice** rather than the whole host: `mem_total_bytes`
is the effective memory limit, `mem_used_bytes` is the slice's usage with
reclaimable page cache excluded (the same accounting `docker stats` shows),
`cpu_count` is the CPU quota rounded up, and `cpu_percent` is utilisation of
that quota. Memory and CPU switch over independently — a container with only a
memory limit still reports host-wide CPU. Unlimited cgroups, cgroup v1 hosts,
and non-Linux platforms report whole-host numbers as before; the response shape
never changes.

### `POST /jobs/{name}/start`

Launches the named job immediately, regardless of its schedule. `{name}` is the
job's `name`.

| Condition | Response |
| --- | --- |
| No job with that name. | `404 Not Found`. |
| The job exists but has `enabled: false`. | `409 Conflict`, body `job '<name>' is disabled`. |
| Otherwise. | `200 OK`, empty body; the job is launched via the normal launch path. |

The `409` is deliberate: a disabled job behaves as if it is not there,
so the API refuses to launch it manually rather than overriding the config.

Manual launch goes through `maybe_launch_job`, so the job's `concurrencyPolicy`
applies. If an instance is already running, `Allow` starts another, `Forbid` does
not start a new one (the `200` still returns), and `Replace` cancels the running
instance(s) first. See [Concurrency and Timeouts](Concurrency-and-Timeouts).

```shell
$ http post http://127.0.0.1:8080/jobs/test-02/start
HTTP/1.1 200 OK
Content-Length: 0
```

### `POST /jobs/{name}/cancel`

Terminates every currently-running instance of the named job, using the same
graceful terminate-then-kill sequence cronstable uses elsewhere (honoring the
job's `killTimeout`; see [Concurrency and Timeouts](Concurrency-and-Timeouts)).
Instances are cancelled concurrently, so a job with several running instances
costs at most one `killTimeout`, not one per instance.

| Condition | Response |
| --- | --- |
| No job with that name. | `404 Not Found`. |
| The job exists but no instance is running. | `409 Conflict`, body `job '<name>' is not running`. |
| Otherwise. | `200 OK`, empty body; all running instances are cancelled. |

A run cancelled this way is recorded in the job's history with the outcome
`cancelled`. Cancellation is a deliberate operator action, not a job failure,
so it is **not** reported (`onFailure` does not fire) and does **not** trigger
retries.

```shell
$ http post http://127.0.0.1:8080/jobs/test-03/cancel
HTTP/1.1 200 OK
```

### `GET /jobs`

Returns a JSON array describing every job: its schedule and timezone, whether
it is enabled and running, the time until its next scheduled run, a summary of
its most recent finished run, and a compact tail of recent outcomes. This is
the endpoint the [Web Dashboard](Web-Dashboard) polls.

| Field | Meaning |
| --- | --- |
| `name`, `enabled`, `schedule`, `command` | The job's name and `enabled` flag, its schedule as a crontab string, and its command (argv lists are joined for display). |
| `captureStdout`, `captureStderr` | Which output streams the job captures, and therefore which are available from `/jobs/{name}/logs`. |
| `utc`, `timezone` | The schedule's reference frame: `utc` (default `true`) and the IANA `timezone` name, or `null`. |
| `running`, `pids` | Whether any instance is currently running, and the PIDs of running instances whose subprocess has started. |
| `running_resources` | Present only while a [`monitorResources`](Resource-Monitoring) job has a running instance: the **live** CPU/memory of the running instance(s), summed — `{cpu_percent, cpu_seconds, rss_bytes, instances}`. Omitted otherwise. `cpu_percent` is usage since the last sample and can exceed 100 across cores. |
| `scheduled_in` | Seconds until the next scheduled run (a float), or `null` when not applicable (disabled, currently running, or a one-off `@reboot` schedule). |
| `never_fires` | `true` when the job is enabled but its crontab has no future occurrence (a fixed past year, an impossible date), distinguishing the dead-schedule `null` above from the running/disabled ones. See [Schedule Linting](Schedule-Linting). |
| `schedule_findings` | The [schedule linter's](Schedule-Linting) advisory findings for this job's crontab, each `{code, level, message}` (empty for a clean schedule). Computed once at config load, in the job's own timezone. |
| `schedule_resolved` | Present only for [`H` hashed schedules](Hashed-Schedules): the plain expression the `H` items resolved to for this job, so clients can compute previews while displaying the `H` the user wrote. |
| `last_run` | The most recent finished run (`outcome`, `exit_code`, `started_at`, `finished_at`, `duration`, `fail_reason`), or `null` if the job has not run yet. |
| `history` | Compact oldest-first tail of recent runs (`outcome` and `duration` only), sized for the dashboard's inline sparkline. Full per-run detail comes from `/jobs/{name}/runs`. |
| `clusterPolicy`, `clusterOwner` | Present only when leader election is configured: the job's [cluster policy](Clustering-and-Leader-Election#per-job-policy), and, under `distribution: spread` for leader-gated jobs, the node that currently owns the job (`null` when there is no quorum). |

```shell
$ http get http://127.0.0.1:8080/jobs
[
    {
        "name": "test-01",
        "enabled": true,
        "schedule": "*/5 * * * *",
        "command": "echo foobar",
        "captureStdout": true,
        "captureStderr": true,
        "utc": true,
        "timezone": "UTC",
        "running": false,
        "pids": [],
        "scheduled_in": 42.1,
        "never_fires": false,
        "schedule_findings": [],
        "last_run": {
            "outcome": "success",
            "exit_code": 0,
            "started_at": "2026-06-21T12:00:00+00:00",
            "finished_at": "2026-06-21T12:00:01+00:00",
            "duration": 1.02,
            "fail_reason": null
        },
        "history": [
            {"outcome": "success", "duration": 0.98},
            {"outcome": "failure", "duration": 1.21},
            {"outcome": "success", "duration": 1.02}
        ]
    }
]
```

### `GET /jobs/{name}/runs`

Returns the job's retained run history (oldest first, bounded, and held in
memory -- though with a [durable state store](Durable-State) configured it is
rehydrated from the durable run ledger after a restart) together with
aggregate statistics. Returns `404 Not Found` for an unknown job.

Each entry in `runs` carries the same fields as `last_run` in `GET /jobs`
(`outcome`, `exit_code`, `started_at`, `finished_at`, `duration`,
`fail_reason`, and `resources`). `resources` is `null` unless the job opted
into [`monitorResources`](Resource-Monitoring), in which case it is
`{cpu_user_seconds, cpu_system_seconds, cpu_total_seconds, max_rss_bytes,
samples}` for that run. Besides `success`, `failure`, and `cancelled`, `outcome` can
be `unknown`: a crash-reconciled run, recorded when the daemon exited or lost
the [state store](Durable-State) mid-run so no completion was ever written.
It is a non-verdict: excluded from `success_rate`, counted only in `total`,
with no `started_at` or `duration` (`fail_reason` explains the interruption).
`stats` summarizes them:

| `stats` field | Meaning |
| --- | --- |
| `total`, `success`, `failure`, `cancelled` | Counts by outcome over the retained history. |
| `success_rate` | Success rate over runs that ran to completion. Cancellations are user-initiated, not a verdict on the job, so they are excluded; `null` when no run has completed. |
| `avg_duration`, `min_duration`, `max_duration`, `last_duration` | Duration aggregates in seconds, over runs with a recorded duration; `null` when there are none. |
| `avg_cpu_seconds`, `max_cpu_seconds`, `last_cpu_seconds` | CPU-time aggregates over the [`monitorResources`](Resource-Monitoring) runs in the window; `null` when none were monitored. |
| `avg_rss_bytes`, `max_rss_bytes`, `last_rss_bytes` | Peak-RSS aggregates (bytes) over the monitored runs; `null` when none were monitored. |

```shell
$ http get http://127.0.0.1:8080/jobs/test-01/runs
{
    "name": "test-01",
    "runs": [
        {
            "outcome": "success",
            "exit_code": 0,
            "started_at": "2026-06-21T12:00:00+00:00",
            "finished_at": "2026-06-21T12:00:01+00:00",
            "duration": 1.02,
            "fail_reason": null
        }
    ],
    "stats": {
        "total": 1,
        "success": 1,
        "failure": 0,
        "cancelled": 0,
        "success_rate": 1.0,
        "avg_duration": 1.02,
        "min_duration": 1.02,
        "max_duration": 1.02,
        "last_duration": 1.02
    }
}
```

### `GET /jobs/{name}/resources`

Chart-grade CPU/RSS time series for one job — the heavyweight sibling of the
summary numbers that ride `GET /jobs` and `GET /jobs/{name}/runs`. The
dashboard fetches it lazily when a job's **Resources** tab is opened, never on
the poll loop. The sampler behind these series is documented on
[resource monitoring](Resource-Monitoring). Returns `404 Not Found` for an
unknown job.

```shell
$ http get http://127.0.0.1:8080/jobs/test-01/resources runs==5
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

{
    "name": "test-01",
    "monitored": true,
    "interval": 1.0,
    "live": [
        {
            "started_at": "2026-07-07T12:00:00+00:00",
            "pid": 4242,
            "current": {"cpu_seconds": 8.4, "cpu_percent": 96.2, "rss_bytes": 21430272},
            "series": [[1751889600.0, 0.0, 20971520], [1751889601.0, 98.1, 21430272]]
        }
    ],
    "runs": [
        {
            "outcome": "success",
            "started_at": "2026-07-07T11:00:00+00:00",
            "finished_at": "2026-07-07T11:00:42+00:00",
            "duration": 42.1,
            "exit_code": 0,
            "fail_reason": null,
            "resources": {
                "cpu_user_seconds": 39.2,
                "cpu_system_seconds": 1.1,
                "cpu_total_seconds": 40.3,
                "max_rss_bytes": 22020096,
                "samples": 42,
                "series": [[1751886000.0, 0.0, 20971520], [1751886001.0, 97.4, 22020096]]
            }
        }
    ]
}
```

A `series` is a list of `[t, cpu_percent, rss_bytes]` points, oldest first,
with `t` in epoch seconds. Points are recorded every
[`monitorResources.interval`](Resource-Monitoring) seconds and
downsampled in place once a run exceeds its configured `history` cap (mean
CPU%, **peak** RSS per merged bucket, so spikes survive), so a series is
bounded no matter how long the run. `live` carries the run-so-far series of
each currently-running monitored instance plus its `current` instantaneous
readings; `runs` the recorded series of recent finished **monitored** runs
(oldest first, unmonitored runs are omitted), capped by the `runs` query
parameter (default 20, clamped to the retained history). With a
[durable state store](Durable-State), run series survive restarts inside the
run ledger records. `monitored: false` with empty lists means the job never
opted into `monitorResources` — distinguishable from "monitored but no data
yet".

### `GET /node/history`

The serving node's retained CPU/memory history — the time-series companion to
[`GET /node`](#get-node)'s live snapshot, driving the dashboard's node chart
(click the header meter). Sampled in the background per
[`web.nodeHistory`](Configuration-Reference#web) (every 5s, last hour, by
default), independent of whether anyone is polling.

```shell
$ http get http://127.0.0.1:8080/node/history
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

{
    "node_name": "node-a",
    "enabled": true,
    "interval": 5.0,
    "points": [[1751889600.0, 37.2, 61.4], [1751889605.0, 35.9, 61.5]]
}
```

`points` is oldest-first `[t, cpu_percent, mem_percent]` (epoch seconds; the
same cgroup-aware percentages `GET /node` reports). A gap between consecutive
points much wider than `interval` means the daemon was down, not idle. The
ring is in-memory only and resets on restart. `enabled: false` (with empty
`points`) means the sampler is off — `web.nodeHistory: false`, or psutil
cannot read this host.

### `GET /jobs/{name}/trends`

The long-horizon sibling of `GET /jobs/{name}/runs`: the same `stats` object,
computed per time window over the [durable run ledger](Durable-State), which
survives restarts and -- on a shared mount -- merges every node's runs.
Returns `404 Not Found` for an unknown job.

The response carries the job `name`, a `source` field, a `generated_at`
timestamp, and a `windows` map with keys `1h`, `24h`, `7d`, `30d`, and `all`,
each holding the stats object documented under
[`GET /jobs/{name}/runs`](#get-jobsnameruns) for the runs that finished inside
that window:

```shell
$ http get http://127.0.0.1:8080/jobs/test-01/trends
{
    "name": "test-01",
    "source": "durable",
    "generated_at": "2026-07-04T12:00:00+00:00",
    "windows": {
        "1h":  { "total": 4, "success": 4, "...": "..." },
        "24h": { "total": 96, "success": 95, "...": "..." },
        "7d":  { "total": 672, "success": 668, "...": "..." },
        "30d": { "...": "..." },
        "all": { "...": "..." }
    }
}
```

`source` is `"durable"` when the aggregates were computed over the durable
ledger (the horizon is then bounded by `state.maxRunsPerJob` retention, and
by the 5000 newest records per request on an unbounded-retention store) and
`"memory"` when the endpoint degraded to the in-memory run history -- because
no `state:` section is configured, or the store could not be read in time.
The endpoint always answers rather than erroring on store trouble. See
[Durable State](Durable-State#sla-trends-over-the-ledger) for the ledger this
reads.

### `GET /jobs/{name}/logs`

A [Server-Sent Events](https://developer.mozilla.org/docs/Web/API/Server-sent_events)
stream of a job's captured output. Returns `404 Not Found` for an unknown job.
The stream begins with the lines already buffered (from the most recent running
instance, or else the last finished run's retained output), then follows a
running job's new lines live, and finishes with an `end` event:

- Each line arrives as an `event: line` whose `data` is a JSON object
  `{"stream": "stdout"|"stderr", "line": "..."}`.
- When the run finishes, the server sends `event: end` with data `{}`. If the
  job has no captured output at all yet, the stream ends immediately with
  `event: end` and data `{"reason": "no-output"}`.
- During quiet periods the server writes an SSE comment (`: ping`) every 15
  seconds as a keep-alive, which also detects disconnected clients.

Only the streams a job captures (`captureStdout` / `captureStderr`) appear
here; see [Output Capturing](Output-Capturing). The response carries
`X-Accel-Buffering: no` so reverse proxies (e.g. nginx) do not buffer the
stream.

```shell
$ curl -N http://127.0.0.1:8080/jobs/test-01/logs
event: line
data: {"stream": "stdout", "line": "foobar"}

event: end
data: {}
```

### DAG endpoints

The orchestration DAGs (the [`dags:`](Orchestration-and-DAGs) section) are
introspected and controlled here. All are token-gated like the job endpoints.

#### `GET /dags`

The configured DAGs and their tasks:

```json
[{"name": "nightly-etl", "enabled": true, "scheduled": true,
  "tasks": [{"id": "extract", "type": "task", "dependsOn": []},
            {"id": "load", "type": "task", "dependsOn": ["extract"]}]}]
```

#### `GET /dags/{name}/runs`

Recent dag_runs (newest first), each with its state and a per-state task count:

```json
{"dag": "nightly-etl", "runs": [
  {"runKey": "2026-07-04T02:00:00_00:00", "runId": "…", "state": "success",
   "kind": "scheduled", "logicalDate": "2026-07-04T02:00:00+00:00",
   "taskStates": {"success": 3}}]}
```

The `limit` query parameter caps the number of runs returned (default 50,
max 500); a missing or unparseable value falls back to the default rather
than erroring. `404` if the DAG is not configured.

#### `GET /dags/{name}/runs/{run_key}`

One run's full durable document -- every task's state, attempt, timing, XCom
expansion (`mapped`), and approval decisions. `404` if the run is unknown.

#### `POST /dags/{name}/trigger`

Create and start a manual run now; returns `{"dag": …, "runKey": …}`. `404`
if the DAG is not configured; a run that could not be durably recorded (the
state backend is unavailable) surfaces as a `500` error rather than a
`runKey` for a run that does not exist.

#### `POST /dags/{name}/backfill`

Replay a scheduled DAG across a historical range. Body:
`{"from": "<ISO>", "to": "<ISO>"}`. Idempotent (create-if-absent per date) and
bounded. Returns `{"ok": true, "created": <N>}`; `400` on a bad range.

#### `POST /dags/{name}/runs/{run_key}/tasks/{taskkey}/decision`

Approve or reject an [approval gate](Orchestration-and-DAGs#approval-gates).
Body: `{"decision": "approve"|"reject", "by": "<who>"}`. `200` on success,
`400` on a bad decision value, `409` if the task is not awaiting a decision.

#### `GET /dags/{name}/runs/{run_key}/xcom`

The XCom outputs the run's tasks published, as a flat list of entries (task,
key, sha256, size, timestamp) with small text values inlined and larger ones
metadata-only; `truncated` flags a run with more entries than the cap. `404`
if the DAG or run is unknown.

#### `GET /dags/{name}/runs/{run_key}/tasks/{taskkey}/logs`

An SSE stream of a *running* task instance's live captured output, in the
same event shape as [`GET /jobs/{name}/logs`](#get-jobsnamelogs). A finished
instance's buffer is not retained, so the stream then ends immediately with
`event: end` and `{"reason": "no-output"}`. `404` if the DAG is not
configured.

### State inspector endpoints

The dashboard's [durable state](Durable-State) inspector is fed by three
**metadata-only** routes: record payloads, KV values, and archived output
never cross this surface. All are token-gated like the job endpoints.

#### `GET /state`

Store health and topology plus an inventory: per-prefix stream and document
counts, capped scope lists, active leases, the quarantine count, and this
node's live retry ladders and held concurrency slots. Returns
`{"enabled": false}` when no `state:` section is configured; an unreadable
store degrades to health-only rather than erroring.

#### `GET /state/documents?ns=<namespace>`

The documents of one `kv/`, `cursor/`, or `idem/` namespace (`400` for any
other namespace). KV values are redacted to a `valueSize` / `valueType`
summary; cursor watermarks and idempotency claim metadata are returned
verbatim. Unlike `GET /state`, which degrades to `{"enabled": false}` on a
stateless install, this route (and `/state/records`) returns `404`
(`state store is not configured`) when no `state:` section is configured.

#### `GET /state/records?stream=<stream>&limit=<n>`

The newest records of one stream, newest first (default 100, max 500).
Archived-output `logs/` streams are refused with `403`: they carry raw job
output, which the metadata-only stance keeps off this surface. A missing or
empty `stream` parameter is a `400`; a stateless install is a `404`
(`state store is not configured`), as for `/state/documents`.

### `GET /job-set-id`

Returns this instance's job-set id: the order-independent fingerprint of every
job's effective configuration that replicas compare to confirm they hold the
same set of jobs (see [job-set id](Job-Set-ID)).
The response is `text/plain` by default; with `Accept: application/json` it is
a JSON object that also carries the job count.

```shell
$ http get http://127.0.0.1:8080/job-set-id
v1:b834d7565aee0da50cd017f666651a5ba3b2e6b161daf0cb6e430f23f51ce90b

$ http get http://127.0.0.1:8080/job-set-id Accept:application/json
{"job_set_id": "v1:b834d7…51ce90b", "jobs": 3}
```

### `GET /metrics`

Exposes cronstable's native [Prometheus](Metrics-with-Prometheus) metrics: daemon
info, per-job run counters and duration histograms, live per-job gauges, and
(when a `cluster` section is configured) the cluster health series that mirror
`GET /cluster`. The exposition is generated by cronstable itself, with no
exporter sidecar and no extra dependency. This section covers the endpoint
mechanics; the full metric reference lives in
[Metrics with Prometheus](Metrics-with-Prometheus).

The response format depends on the request's `Accept` header:

- By default the response is the classic Prometheus text format
  (`text/plain; version=0.0.4`).
- If `Accept` advertises `application/openmetrics-text`, the response is
  OpenMetrics 1.0 (terminated by `# EOF`), as modern Prometheus servers
  request.

The configured `web.headers` are applied to the response as on every other
route, except `Content-Type`, which this endpoint owns: the exposition
format's contract always wins over an operator-configured header.

The endpoint is enabled by default whenever the web API is on. The
`web.metrics` option tunes or disables it, accepting either a boolean
shorthand (`metrics: false` disables the endpoint) or a map:

| Sub-option | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | bool | `true` | Serve `GET /metrics`. |
| `public` | bool | `false` | Exempt `/metrics` (and only `/metrics`) from `web.authToken` bearer-token authentication (see [Authentication](#authentication)). |
| `durationBuckets` | sequence of floats | `0.1, 0.5, 1, 5, 15, 60, 300, 900, 3600` | Upper bounds (seconds) of the `cronstable_job_duration_seconds` histogram. Bounds must be finite, positive, and strictly increasing; anything else raises a `ConfigError`. |

The metric registry is owned by the daemon rather than the web app, so
counters survive config reloads (including ones that restart the web server)
and reset only when the process restarts; series for jobs removed by a reload
are pruned.

```shell
$ curl http://127.0.0.1:8080/metrics
# HELP cronstable_info cronstable build information.
# TYPE cronstable_info gauge
cronstable_info{version="1.0.13"} 1
# HELP cronstable_jobs Number of configured jobs by enablement state.
# TYPE cronstable_jobs gauge
cronstable_jobs{state="enabled"} 2
cronstable_jobs{state="disabled"} 1
# HELP cronstable_job_runs_total Finished job runs by outcome, as recorded in the run history.
# TYPE cronstable_job_runs_total counter
cronstable_job_runs_total{job_name="test-01",status="success"} 12
cronstable_job_runs_total{job_name="test-01",status="failure"} 1
cronstable_job_runs_total{job_name="test-01",status="cancelled"} 0
...
```

### `GET /` (the dashboard page)

Serves the single-page [Web Dashboard](Web-Dashboard). Set `ui: false` in the
`web` section to disable the page and expose only the REST endpoints. The page
is served with secure default headers (a strict Content-Security-Policy,
anti-clickjacking, and nosniff) with any operator `web.headers` merged on top,
so a deliberately-set operator header wins. When `web.authToken` is enabled,
the page itself loads without a token (it holds no data); it prompts for the
token in the browser and authenticates every data request with it (see
[Authentication](#authentication)).

### `POST /mcp` (the MCP server)

When a [`mcp`](Configuration-Reference#mcp) section sets `enabled: true`, an
opt-in [Model Context Protocol](https://modelcontextprotocol.io) server is
served at `POST /mcp` on the same listeners, letting an AI agent observe (and,
when `readOnly: false`, control) jobs, DAGs, the cluster/fleet, metrics and the
durable state store. It is a **stateless Streamable-HTTP** JSON-RPC 2.0
endpoint (no `Mcp-Session-Id`; `GET /mcp` returns `405`), pinned to MCP
revision `2025-11-25`. It inherits `web.authToken` exactly like the data
routes (it is **never** in the public set, so it always requires the bearer
token when one is configured), and additionally validates the `Origin` header
(`403` on a present, non-allow-listed origin) and caps the request body
(`413`). If `mcp.enabled` is set with no `web.authToken` on a routable
listener, cronstable **fails closed** at config load (raises a `ConfigError`)
unless `mcp.allowUnauthenticated: true`. Desktop MCP clients reach it over the
`cronstable mcp` stdio bridge. Full tool catalog: [MCP](MCP); configuration:
[`mcp`](Configuration-Reference#mcp).

## Response headers

The `web.headers` map (merged upstream but never released in yacron 0.19) is a
string→string map applied to every `200` success
response across all routes (`/version`, `/status`, `/cluster`, `/job-set-id`,
the job routes, and the `200` of `/jobs/{name}/start`) and to the `409`
conflict bodies of `/jobs/{name}/start` and `/jobs/{name}/cancel`. It is not
applied to the `404` (unknown job) or `401` (authentication failure) responses,
which are raised without the configured headers. Example:

```yaml
web:
  listen:
    - http://127.0.0.1:8080
  headers:
    X-Frame-Options: DENY
    Cache-Control: no-store
```

## Authentication

By default the API is unauthenticated; anyone who can reach a `listen` address can
call every endpoint. Restrict access at the network or socket level, or enable
bearer-token authentication with `web.authToken`.

`authToken` resolves the token from exactly one source, in this precedence order
(`_resolve_web_token`):

| Sub-option | Type | Description |
| --- | --- | --- |
| `value` | string or null | Literal token value. |
| `fromFile` | string or null | Path to a file; the token is the file contents with surrounding whitespace stripped. |
| `fromEnvVar` | string or null | Name of an environment variable holding the token. |

When `authToken` is set, an aiohttp middleware (`_make_auth_middleware`) requires
`Authorization: Bearer <token>` on every route:

- The auth scheme is compared case-insensitively (`Bearer`, `bearer`, etc.) per
  RFC 7235.
- The presented token is compared against the configured token in constant time via
  `hmac.compare_digest`.
- A missing/malformed `Authorization` header, a wrong scheme, or a non-matching
  token returns `401 Unauthorized`.

The one configurable exception: setting `web.metrics.public: true` exempts
`/metrics` (and only `/metrics`) from the bearer token, for scrapers that
cannot send credentials; every other route stays gated (see
[`GET /metrics`](#get-metrics)).

One built-in carve-out: paths ending in `.ics` (the
[calendar feeds](Calendar-Export)) also accept the same token as a `token`
query parameter, compared in the same constant time, because calendar
clients subscribing to a feed cannot attach a bearer header. Every other
path refuses query tokens, keeping the token out of URLs, access logs, and
referrers where a header will do.

```yaml
web:
  listen:
    - http://127.0.0.1:8080
  authToken:
    fromEnvVar: CRONSTABLE_WEB_TOKEN
```

```shell
$ http get http://127.0.0.1:8080/status "Authorization:Bearer s3cr3t"
$ curl -H "Authorization: Bearer s3cr3t" http://127.0.0.1:8080/status
```

### Cross-site request defense

Independently of `authToken`, an always-on middleware refuses **cross-site
browser requests to the mutating endpoints** (`POST /jobs/{name}/start`,
`POST /jobs/{name}/cancel`, `POST /dags/{name}/trigger`,
`POST /dags/{name}/backfill`, and the task decision route). Those POSTs are
CORS "simple requests" -- the browser sends them without a preflight -- so
without this gate any web page an operator happens to visit could fire them
at a localhost-bound daemon (classic CSRF, and the DNS-rebinding variant).
The rule, per request:

- Requests without an `Origin` header pass untouched: curl, monitoring
  agents, and other non-browser clients are unaffected (every current
  browser sends `Origin` on cross-site POSTs, which is the attack this
  defends against).
- An `Origin` whose authority matches the request's own `Host` passes: the
  served dashboard keeps working, including behind a TLS-terminating reverse
  proxy (the scheme is deliberately not compared, only hostname and port).
- An `Origin` on `web.allowedOrigins` (exact match) passes -- for a trusted
  dashboard served from another origin.
- Anything else, including `Origin: null`, is refused `403`.

`GET`/`HEAD`/`OPTIONS` are never gated (no read here mutates, and the
browser's same-origin policy already hides their responses cross-site), and
`/mcp` is exempt because it enforces its own `mcp.allowedOrigins` list with
CORS preflight support (see [MCP](MCP)).

Two escape hatches for deliberate cross-origin deployments: a specific
origin in a `web.headers` `Access-Control-Allow-Origin` header is treated as
allow-listed, and `Access-Control-Allow-Origin: "*"` disables the gate
entirely (logged as a warning at startup).

Note the gate complements, not replaces, `authToken`: with a bearer token
configured a cross-site page could not authenticate anyway; without one, the
gate is what keeps a browsing operator's localhost daemon from being driven
by arbitrary web pages. Anyone who can reach the listen address directly
(off-browser) is still governed only by `authToken` and network policy. One
honest residual: a DNS-rebinding page served on the daemon's *own port* (so
`Origin` and `Host` genuinely agree after the rebind) is indistinguishable
from the real dashboard by header comparison alone -- cross-port and
cross-host rebinds are refused, and `authToken` closes that residual
completely.

### Fail-closed behavior

If `authToken` is configured but resolves to an empty token, cronstable raises a
`ConfigError` and refuses to start the web server, rather than silently serving
the control API with no authentication. This happens when:

- `value`, `fromFile`, and `fromEnvVar` are all empty/absent;
- `fromEnvVar` names a variable that is unset (resolves to `""`);
- `fromFile` points to a file that is empty or contains only whitespace.

If `fromFile` cannot be read (`OSError`), cronstable also raises a `ConfigError`
(`web.authToken.fromFile could not be read: ...`).

## Unix socket permissions

> **Windows:** this entire feature is POSIX-only. It depends on `unix://`
> sockets (which Windows does not support) and on `chmod`. On Windows
> `socketMode` has no effect and `unix://` listen URLs are skipped with a
> warning; use an `http://` listener. See [Running on Windows](Running-on-Windows).

`web.socketMode` is an octal-string file mode applied with `chmod`
to each `unix://` listen socket after it starts (`_apply_socket_mode`):

```yaml
web:
  listen:
    - unix:///run/cronstable/cronstable.sock
  socketMode: "0660"
```

- The mode is parsed as base-8 (`int(socketMode, 8)`).
- It is applied only to `unix://` sockets; non-`unix` addresses are ignored.
- An invalid mode (not an octal integer, raising `ValueError`) or a `chmod`
  failure (`OSError`) is logged as a warning
  (`web: could not set socketMode <mode> on <path>: ...`) and does not abort
  startup.

When using a Unix socket on a read-only-root container, point the socket at a small
writable volume. See [Production and Container Deployment](Production-Deployment).

## Lifecycle and reload behavior

The control API lifecycle is driven by `start_stop_web_app`, called on each config
reload from the scheduler loop:

- If a server is running and the new `web` config is absent or differs from the
  running one, the running server is stopped (`web_runner.cleanup()`) before any new
  one is started. A change to any `web` field (including `headers`, `authToken`, or
  `socketMode`) thus triggers a restart of the server on reload.
- The server is (re)started only when `web` is present, `listen` is non-empty, and no
  server is currently running.
- Each `listen` address is bound independently. A bad URL (`ValueError`) or a bind
  failure (`OSError`, e.g. address already in use) on one address is logged as a
  warning (`web: could not listen on <addr>: ...`) and skipped; the remaining
  addresses still bind, and the config update is not aborted.
- The `web: started listening on <addr>` log line is emitted only after the bind
  succeeds.
- On shutdown, the running server is stopped after currently running jobs finish.
  On Windows, graceful shutdown is triggered with Ctrl-C or Ctrl-Break (rather
  than `SIGTERM` as on POSIX); cronstable still finishes currently-running jobs
  before stopping the server, identical to POSIX behavior. See
  [Running on Windows](Running-on-Windows).

A `ConfigError` raised while resolving `authToken` (empty token or unreadable
`fromFile`) propagates out of `start_stop_web_app` and is caught by the reload
loop's dedicated web-app handler, which logs `Error in the web configuration, so
not starting the web API` and leaves the web API down until a later reload fixes
it. The rest of the new configuration (jobs, cluster, logging) is still applied:
the web app starts under its own error handling, after the cluster manager, so a
web misconfiguration cannot skip the cluster gate (which would otherwise fail
open and run every `Leader` job on every node).

## Job-facing state endpoints (loopback)

Separate from the `web` control API above, cronstable can run a second,
**loopback-only** HTTP server that hands its [durable state store](Durable-State)
to the *jobs it runs*. It binds `127.0.0.1` on an OS-assigned ephemeral port (or
the address in `state.jobApi.listen`), and the daemon injects its base URL and a
per-run bearer token into every job's environment, so a job's command line can
read and write durable state, coordinate through a fleet-wide lock, or fetch a
run-scoped secret. The `cronstable state|cursor|lock|artifact|idempotent|secret`
[CLI commands](CLI-Reference#job-facing-state-commands) are thin clients of this
endpoint; the same primitives are also reachable offline against the store
directly, so this server is a front-end, not a second source of truth. The
surface is served by `cronstable/jobapi.py`; the primitives themselves live in
`cronstable/jobstate.py`.

### Enabling the endpoint

The endpoint is stood up only when the configuration has a `state:` section with
`jobApi.enabled` (default `true`). A stateless install, or one with a `state:`
section but `jobApi.enabled: false`, never starts it and injects nothing.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | bool | `true` | Serve the loopback endpoint and inject the `CRONSTABLE_*` environment into every job. |
| `listen` | string | (ephemeral) | Address to bind, as `host:port` or `http://host:port`. When omitted it binds `127.0.0.1` on an OS-assigned port; an explicit port must be in `0`-`65535` (`0` = OS-assigned), and a non-loopback host needs `allowNonLoopbackBind`. |
| `maxValueBytes` | int | `1048576` (1 MiB) | Reject a KV or cursor value larger than this many bytes (JSON-encoded) with `413`. `0` means no limit. |
| `maxArtifactBytes` | int | `67108864` (64 MiB) | Reject an artifact payload larger than this many bytes with `413`. `0` means no limit. |
| `lockTtlSeconds` | float | `30` | Default lease TTL for a job lock (floored at `5`); the daemon renews it at a third of the TTL while the job holds it. |
| `allowNonLoopbackBind` | bool | `false` | Explicit opt-in for a non-loopback `listen` host; without it such a host is a `ConfigError` (the endpoint serves per-run tokens and staged secrets over plaintext HTTP). |

The server's own transport-level body cap is derived from these limits (the
larger of the two, plus envelope headroom), so an oversized request body is
refused with `413` rather than silently truncated; setting either limit to
`0` lifts the transport cap too -- genuinely unlimited.

### Injected environment

On launch the daemon injects these variables into every job's process. All are
strings; an unknown scheduled time is the empty string rather than absent, so a
job can test the variable instead of guessing whether it was set.

| Variable | Meaning |
| --- | --- |
| `CRONSTABLE_STATE_URL` | The loopback base URL, e.g. `http://127.0.0.1:54321`. |
| `CRONSTABLE_STATE_TOKEN` | A per-run bearer token, revoked the instant the run ends. |
| `CRONSTABLE_RUN_ID` | A unique id for this run. |
| `CRONSTABLE_JOB_NAME` | The job's name (also its default scope). |
| `CRONSTABLE_ATTEMPT` | The retry attempt number (`0` on the first fire). |
| `CRONSTABLE_SCHEDULED_AT` | The scheduled fire time (ISO-8601), or empty. |
| `CRONSTABLE_HOST` | The host name. |

### Authentication and errors

Every request must carry `Authorization: Bearer $CRONSTABLE_STATE_TOKEN`. The token
is matched in constant time against the live run set, so a missing, malformed,
forged, or stale token returns `401 Unauthorized` before any state is touched.
Other outcomes:

| Status | When | Body |
| --- | --- | --- |
| `400` | A caller error: a missing required field, or a body that is not a JSON object. | `{"error": "..."}` |
| `409` | A cursor advanced with a value not comparable to its stored one (a type clash). | `{"error": "..."}` |
| `410` | An artifact record survives but its payload blob was garbage collected. | `{"error": "..."}` |
| `413` | A value or artifact larger than the configured `maxValueBytes` / `maxArtifactBytes`. | `{"error": "..."}` |
| `404` | A `get` for a key, cursor, artifact, or secret that is not set. | (empty) |
| `503` | The state store is unavailable or a backend call timed out. | `{"error": "..."}` |

### Scopes

Every KV, cursor, idempotency, and artifact call acts in a *scope*: a namespace
that defaults to the calling job's own name (`defaultScope` in `GET /v1/run`), so
one job cannot read another's state by accident. Omit `scope` for that private
namespace, or pass `scope=global` (any shared name works) for deliberate
cross-job coordination.

### Routes

All routes are under `/v1/` and registered in `JobStateAPI._routes`:

| Method | Path | Body / query | Success response |
| --- | --- | --- | --- |
| `GET` | `/v1/run` | -- | `{runId, job, attempt, scheduledAt, host, defaultScope}` |
| `GET` | `/v1/kv/get` | `?scope=&key=` | `{value, updatedAt}`, or `404` |
| `POST` | `/v1/kv/set` | `{scope?, key, value}` | `{ok, updatedAt}` |
| `POST` | `/v1/kv/delete` | `{scope?, key}` | `{existed}` |
| `GET` | `/v1/kv/list` | `?scope=` | `{scope, keys: [{key, value, updatedAt}]}` |
| `GET` | `/v1/cursor/get` | `?scope=&name=` | `{value, updatedAt}`, or `404` |
| `POST` | `/v1/cursor/advance` | `{scope?, name, value, force?}` | `{value, advanced}` |
| `POST` | `/v1/idempotency/claim` | `{scope?, key, ttl?}` | `{fresh, claimedAt}` |
| `POST` | `/v1/idempotency/release` | `{scope?, key}` | `{released}` |
| `POST` | `/v1/artifact/put` | `?scope=&name=`, raw body | `{sha256, size}` |
| `GET` | `/v1/artifact/get` | `?scope=&name=` | raw bytes plus `X-Cronstable-Sha256` / `X-Cronstable-Size`, or `404` |
| `GET` | `/v1/artifact/list` | `?scope=` | `{scope, artifacts: [{name, sha256, size, at}]}` |
| `POST` | `/v1/lock/acquire` | `{scope?, name, permits?, ttl?, wait?, blockSeconds?}` | `{acquired, token?, slot?, fence?, ttl?}` |
| `POST` | `/v1/lock/release` | `{token}` | `{released}` |
| `GET` | `/v1/secret/get` | `?name=` | `{value}`, or `404` |
| `GET` | `/v1/secret/list` | -- | `{names: [...]}` |

`cursor/advance` is monotonic by default: the stored value only ever moves to
`max(current, value)`, so a replayed or out-of-order batch cannot walk a
watermark backwards, and two nodes racing to advance the same cursor converge on
the larger value. `advanced` is `false` when the given value was not greater than
the current one (a no-op that is not even a write), and `force: true` sets the
value unconditionally (a deliberate rewind). `idempotency/claim` is a fleet-wide
create-if-absent: the first caller gets `fresh: true`, every later caller
`fresh: false`, so a retried run can tell "already did this" from "first time"; a
positive `ttl` bounds the dedupe window (`0` is a permanent claim).

The lock is a fleet-wide mutex (or a semaphore, with `permits > 1`) held as a TTL
lease that the daemon renews for as long as the job holds it and releases the
instant the job releases it or the run ends -- so a job that crashes or forgets
to unlock never leaks a lock. `permits` outside `1`-`1024` is rejected up
front with a `400`: the acquire pass probes permits sequentially, so the cap
keeps a fully-contended pass bounded. `wait: true` retries for up to `blockSeconds`
before giving up; without it the call makes a single pass over the permits and
returns `{acquired: false}` when they are all taken. Like every cronstable
coordination primitive the lock is at-least-once, not exactly-once (the `fence`
token in the reply is there for a job that needs true fencing). Run-scoped
**secrets** are staged in memory by the daemon per run and vanish when the run
ends; they never touch the store, so only the daemon holds them, and there is no
scope on the secret routes -- a run sees only its own.

```shell
# Inside a job, using the injected env directly.
$ curl -s -H "Authorization: Bearer $CRONSTABLE_STATE_TOKEN" \
    "$CRONSTABLE_STATE_URL/v1/run"
{"runId": "…", "job": "nightly-etl", "attempt": 0, "scheduledAt": "2026-07-04T02:00:00+00:00", "host": "node-a", "defaultScope": "nightly-etl"}

# Advance the job's private ETL cursor to the last row processed.
$ curl -s -H "Authorization: Bearer $CRONSTABLE_STATE_TOKEN" \
    -H 'Content-Type: application/json' \
    -d '{"name": "rows", "value": 20482}' \
    "$CRONSTABLE_STATE_URL/v1/cursor/advance"
{"value": 20482, "advanced": true}
```

In practice a job reaches for the
[job-facing CLI](CLI-Reference#job-facing-state-commands) rather than raw `curl`;
those commands read the injected environment for you.

## See also

- [Web Dashboard](Web-Dashboard): the built-in browser UI served by this interface.
- [Metrics with Prometheus](Metrics-with-Prometheus): the full metric reference behind `GET /metrics` and Prometheus scrape configuration.
- [Clustering and Leader Election](Clustering-and-Leader-Election): the `GET /cluster` view and the separate mTLS `/peer` endpoint.
- [Running on Windows](Running-on-Windows): `unix://` listeners and `socketMode`
  behave differently on Windows.
- [Durable State](Durable-State): the store the [job-facing state endpoints](#job-facing-state-endpoints-loopback) expose to the jobs the daemon runs.
- [Configuration Reference](Configuration-Reference)
- [CLI Reference](CLI-Reference)
- [Concurrency and Timeouts](Concurrency-and-Timeouts)
- [Production and Container Deployment](Production-Deployment)
- [Architecture and Internals](Architecture-and-Internals)
