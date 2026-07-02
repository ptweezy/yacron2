# Configuration Reference

The canonical, exhaustive reference for the yacron2 YAML configuration. It
documents the top-level structure and every per-job option, with the exact
strictyaml type, default, and load-time validation rule taken from
`yacron2/config.py`. Deep topics (schedules, reporting, the HTTP API, metrics,
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
defaults documented below, so internally it is configured to yacron2's
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
include: [ ... ]    # optional: list of other config files to merge
web: { ... }        # optional: HTTP control API
cluster: { ... }    # optional: mTLS peer attestation / leader election
logging: { ... }    # optional: Python logging dictConfig
```

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `defaults` | `Map` of the per-job common options | No | Default values inherited by every job in the same file. May contain any per-job option except `name`, `command`, and `schedule`. See [Includes, Defaults, and Multi-File Config](Includes-and-Defaults). |
| `jobs` | `Seq(Map)` of job definitions | No | The list of cron jobs. Each entry is validated against the per-job schema below. |
| `include` | `Seq(Str)` | No | Paths (relative to the including file) of other config files to parse and merge. Include cycles raise a `ConfigError`. See [Includes, Defaults, and Multi-File Config](Includes-and-Defaults). |
| `web` | `Map` | No | Enables the HTTP control API. See [HTTP Control API](HTTP-API). |
| `cluster` | `Map` | No | Enables mutual-TLS peer attestation and optional leader election across replicas. See [Clustering and Leader Election](Clustering-and-Leader-Election). |
| `logging` | `Map` (Python `logging.config` dictConfig) | No | Custom logging configuration. See [Logging Configuration](Logging-Configuration). |

### `web`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `listen` | `Seq(Str)` | required | Listen URLs, e.g. `http://127.0.0.1:8080` or `unix:///tmp/yacron2.sock`. `http://` listeners work everywhere; `unix://` listeners are not supported on Windows (the Proactor loop lacks `create_unix_server`) and are skipped with the warning `Ignoring web listen url <url>: unix-socket listeners are not supported on this platform`. Use an `http://` listener instead. See [Running on Windows](Running-on-Windows). |
| `headers` | `MapPattern(Str, Str)` | none | Extra HTTP response headers applied to all endpoints. |
| `authToken` | `Map` with `value` / `fromFile` / `fromEnvVar` (each `EmptyNone() \| Str`) | none | Opt-in bearer-token auth. When set but resolving empty, yacron2 refuses to start. |
| `socketMode` | `Str` | none | Octal permissions applied to a `unix://` listen socket. Only ever applies to unix sockets, so it is irrelevant on Windows (where `unix://` listeners are unsupported). |
| `metrics` | `Bool \| Map` with `enabled` / `public` (each `Bool`) and `durationBuckets` (`Seq(Float)`) | enabled | The Prometheus `GET /metrics` endpoint, served by default whenever the web API is on. `metrics: false` (bool shorthand) disables it; the map form sets `enabled` (default `true`), `public` (default `false`; exempts only `/metrics` from `authToken`), and `durationBuckets` (histogram bounds in seconds; must be finite, positive, and strictly increasing, else a `ConfigError`). See [Metrics with Prometheus](Metrics-with-Prometheus). |

`listen` is the only required key. Full behavior, authentication, and endpoint
semantics are documented in [HTTP Control API](HTTP-API).

### `cluster`

Optional. Gates scheduled jobs on a **leadership backend** so several replicas
can run from one config without double-running jobs. `cluster.backend` chooses
how: the default **`gossip`** backend attests, over mutual TLS, that a static
list of peers is running the same job set and runs a best-effort quorum
election; the **`kubernetes`** and **`etcd`** backends use a coordination store
(a `Lease` / a lease-bound key) for a fenced, exactly-once election. There must
be exactly one `cluster` block across the whole configuration; a duplicate in an
included file or a second config-directory file raises a `ConfigError`. Defaults
come from `DEFAULT_CLUSTER` (plus `DEFAULT_K8S` / `DEFAULT_ETCD` for the lease
backends) and are applied only when a `cluster` section is present.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `backend` | `Enum(["gossip", "kubernetes", "etcd"])` | `gossip` | Which leadership backend gates jobs. `gossip` (default) is the embedded mTLS best-effort election; `kubernetes`/`etcd` are fenced lease backends. The lease backends talk to their store over plain HTTP via the core `aiohttp` dependency, so they add no runtime dependency. |

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
| `distribution` | `Enum(["single-leader", "spread"])` | `single-leader` | How leader-gated jobs spread across the quorate cluster. `single-leader`: one elected leader runs every `Leader` job. `spread`: per-job ownership via rendezvous hashing, so the work fans out across the quorate nodes (same quorum gate, same guarantee). Inert without `electLeader` (warns if set anyway). With `backend: kubernetes`/`etcd` a non-default `distribution` is a **hard `ConfigError` at load** (a single lease holder cannot be a per-job owner), not a silent fallback. See [Clustering and Leader Election](Clustering-and-Leader-Election#distribution-one-leader-or-spread-the-load). |

Gossip load-time validation (in addition to the numeric ranges above): with
`electLeader: true`, a **2-node** cluster (one peer) is rejected outright with a
`ConfigError` (a quorum of 2 needs both up, strictly worse than one replica);
an **even** cluster size **greater than 2** is allowed but logs a warning (an
odd count is best for a clean majority).

**Kubernetes backend** (`backend: kubernetes`), under `cluster.kubernetes`. A
`coordination.k8s.io/v1` `Lease` is the fence. Defaults from `DEFAULT_K8S`:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `leaseName` | `Str` | `yacron2-leader` | Name of the `Lease` object the replicas contend for. Must be a valid RFC1123 subdomain (lowercase alphanumerics, `-` and `.`; `<=253` chars), checked at load; it is spliced into the apiserver URL path, so a stray `/`, `?`, `#`, or space is rejected. |
| `leaseNamespace` | `Str` or null | null → in-cluster namespace | Namespace of the `Lease`; defaults to the pod's own namespace (the service-account namespace file). When set, must be a valid RFC1123 label (lowercase alphanumerics and `-`; `<=63` chars), checked at load. |
| `leaseDurationSeconds` | `Int` | `15` | How long a renewal keeps the lease valid. Must be `> renewDeadlineSeconds`. |
| `renewDeadlineSeconds` | `Int` | `10` | Per-round renew/observe deadline: a round that exceeds it is abandoned and retried next round, so a stuck apiserver call cannot run out the full lease. Must be `> 0` and `< leaseDurationSeconds`. |
| `retryPeriodSeconds` | `Int` | `2` | Seconds between renew/observe rounds. Must be `> 0` and `< renewDeadlineSeconds` (a holder must be able to attempt a renew before its own deadline). Additionally, `renewDeadlineSeconds + retryPeriodSeconds < leaseDurationSeconds` is enforced at load, so the worst-case interval between two successful refreshes still fits inside the lease. |
| `identity` | `Str` or null | null → `nodeName` | The human-readable holder for this node (shown in the dashboard / `GET /cluster`). yacron2 appends a **per-process token** to the `holderIdentity` it actually writes (`<identity>#<token>`), so two nodes sharing an `identity`/`nodeName` still write distinct holders and cannot both believe they hold the `Lease`. See [Node identity](Clustering-and-Leader-Election#node-identity-for-the-lease-backends). |
| `kubeconfig` | `Str` or null | null → in-cluster | Path to a kubeconfig for out-of-cluster / local testing; otherwise the in-cluster service-account credentials are used. On the hand-rolled HTTP transport a kubeconfig user that relies on an `exec` credential plugin or an `auth-provider` raises a `ConfigError` (those must be executed, which only the native client can do); use `clientLibrary: library` (`yacron2[kubernetes]`) or a kubeconfig with a static token / client certificate instead. `insecure-skip-tls-verify` is honored (the apiserver certificate is not validated) but logs a warning. |
| `apiServer` | `Str` or null | null | Override the apiserver URL (else the in-cluster `KUBERNETES_SERVICE_*` env or the kubeconfig). When set, must be an `https://` URL: a non-https value is a `ConfigError` at load, since the ServiceAccount bearer token must not travel in cleartext. |
| `clientLibrary` | `Enum(["auto", "http", "library"])` | `auto` | Transport selection. `auto` uses the official `kubernetes` client when it is importable (install `yacron2[kubernetes]`) and otherwise falls back to a hand-rolled apiserver REST transport over `aiohttp`; `library` requires the native client (a `ConfigError` if absent); `http` forces the hand-rolled transport. |

**etcd backend** (`backend: etcd`), under `cluster.etcd`. A lease-bound key is
the fence; the backend uses etcd's v3 gRPC-gateway JSON/HTTP API directly (no
native client). Defaults from `DEFAULT_ETCD`:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `endpoints` | `Seq(Str)` | `["http://127.0.0.1:2379"]` | etcd client URLs, tried in order for failover. Each must be `http(s)://host[:port]`; the port is optional (defaults to the scheme's port, e.g. `443` behind an https ingress) and only an explicitly out-of-range port is rejected. Credentials embedded in the URL are refused. |
| `electionName` | `Str` | `yacron2/leader` | The etcd key contended for; its value is the holder's `nodeName`. There is **no separate `identity` key** for etcd (the holder identity is always `cluster.nodeName`), but leadership is fenced on the **bound lease id**, not this string, so a duplicate `nodeName` cannot make two nodes both lead. See [Node identity](Clustering-and-Leader-Election#node-identity-for-the-lease-backends). |
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

Because the cluster schema has many load-time rejections (the ordering rules,
the RFC1123 and https guards, the credential-over-plaintext refusals above),
check a cluster config before deploying with `yacron2 --validate-config`, which
runs the full load path and prints the first `ConfigError` without starting the
scheduler. See [Command-Line Reference](CLI-Reference).

Full behavior, the trust model, quorum math, the lease backends' guarantees,
and per-job `clusterPolicy` are documented in
[Clustering and Leader Election](Clustering-and-Leader-Election).

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
| `schedule` | `Str` or `Map` | required | A crontab string, the literal `@reboot`, or a mapping with `minute`, `hour`, `dayOfMonth`, `month`, `year`, `dayOfWeek` (each `Str`, all optional). The mapping is assembled into a 5-field crontab; the five used fields default to `*`. `year` is accepted by the schema but ignored (the parser builds only a 5-field crontab). See [Schedules and Timezones](Schedules-and-Timezones). |
| `shell` | `Str` | `/bin/sh` (POSIX) / empty (Windows) | Shell used to run a string `command`. Ignored when `command` is a list. The default is platform-specific: on POSIX a string `command` runs as `["/bin/sh", "-c", command]`; on Windows the default is empty, which routes a string `command` through the native command processor `%ComSpec%` (cmd.exe) via `asyncio.create_subprocess_shell`. For PowerShell or another interpreter set `shell:` explicitly, or pass `command` as a list to bypass the shell entirely (on every platform). The `shell` field itself works on all OSes. See [Running on Windows](Running-on-Windows). |
| `enabled` | `Bool` | `true` | When `false`, the job is parsed and validated but never scheduled or runnable. |

### Output capturing

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `captureStdout` | `Bool` | `false` | Capture the process's stdout for failure detection and reports. When false, the job's stdout passes through to yacron2's stdout. |
| `captureStderr` | `Bool` | `true` | Capture the process's stderr for failure detection and reports. When false, the job's stderr passes through to yacron2's stderr. |
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
| `clusterPolicy` | `Enum(["Leader", "PreferLeader", "EveryNode"])` | `Leader` | Where this job runs under cluster leader election. **Inert unless `cluster.electLeader` is set** (without election every job runs on every instance). `Leader`: only the quorum-gated leader runs it (at-most-once; may skip). `PreferLeader`: the lowest reachable agreeing node runs it, ignoring quorum (never skips; may double-run across a partition). `EveryNode`: every node runs it, independent of cluster health. Part of the [job-set id](Clustering-and-Leader-Election#the-job-set-id-foundation). See [Clustering and Leader Election](Clustering-and-Leader-Election#per-job-policy). |
| `executionTimeout` | `Float` | none | Seconds after which a still-running process is terminated. Unset means no timeout. Must be `> 0` when set. The "terminated" action differs by platform (graceful SIGTERM->SIGKILL escalation on POSIX vs an immediate `TerminateProcess` on Windows); see `killTimeout` below and [Running on Windows](Running-on-Windows). |
| `killTimeout` | `Float` | `30` | Seconds to wait after SIGTERM before sending SIGKILL when terminating a job. Must be `>= 0`. The SIGTERM-then-SIGKILL escalation is POSIX-specific: there `terminate()` sends SIGTERM (graceful, trappable) and `kill()` sends SIGKILL, a real escalation. On Windows there are no POSIX signals, so both `terminate()` and `kill()` call `TerminateProcess` (an immediate, ungraceful stop that does not notify the child), so the escalation is effectively moot; `killTimeout` still bounds the wait but the outcome is the same hard kill. See [Running on Windows](Running-on-Windows). |

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
| `validate_certs` | `Bool` | `true` | Validate TLS certificates. Defaults to `true` in yacron2 (a breaking change from upstream). |
| `html` | `Bool` | `false` | Send the body as HTML. |

#### `report.sentry`

Defaults from `_REPORT_DEFAULTS["sentry"]`:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `dsn` | `Map` with `value`/`fromFile`/`fromEnvVar` (each `EmptyNone() \| Str`) | all `None` | Sentry DSN source. |
| `fingerprint` | `Seq(Str)` | `["yacron2", "{{ environment.HOSTNAME }}", "{{ name }}"]` | Issue-grouping fingerprint (jinja2 per entry). Replaces, never appends, on merge. |
| `level` | `Str` | unset (effective `error`) | Sentry event level. When unset, events are captured at level `error`. |
| `extra` | `MapPattern(Str, Str \| Int \| Bool)` | unset | Extra structured context. |
| `body` | `Str` | default subject + body templates | Event message (jinja2). |
| `environment` | `Str` | `None` | Sentry environment. |
| `maxStringLength` | `Int` | `8192` | Max string length before Sentry truncation. |

#### `report.shell`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `shell` | `Str` | `/bin/sh` (POSIX) / empty (Windows) | Shell used to run the reporter command. The default is platform-specific, same as the per-job `shell` field: on Windows the default is empty (the reporter command runs via cmd.exe through `%ComSpec%`). Set `shell:` explicitly for another interpreter, or pass `command` as a list. See [Running on Windows](Running-on-Windows). |
| `command` | `Str` or `Seq(Str)` | `None` | Reporter command (required key). Receives `YACRON2_*` environment variables. |

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

### Environment

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `environment` | `Seq(Map({"key": Str, "value": Str}))` | `[]` | Environment variables set for the process. Both `key` and `value` are required per entry. Merged by key with `defaults` and with `env_file` (config values win). |
| `env_file` | `Str` | none | Path to a `KEY=VALUE` file; blank lines and `#` comments are ignored. Variables in `environment` override file values. A read error or a line without `=` raises a `ConfigError`. |

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
or `group` requires yacron2 to run as root (euid 0); otherwise a `ConfigError`
is raised. Privilege switching is **not supported on Windows**: a job with
`user` or `group` set raises a configuration error, verbatim
`Job <name>: changing user/group is not supported on Windows`. See
[Production and Container Deployment](Production-Deployment) and
[Running on Windows](Running-on-Windows).

### Metrics

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `statsd` | `Map({"prefix": Str, "host": Str, "port": Int})` | none | When set, emit start/stop/success/duration metrics over UDP. All three keys are required. |

See [Metrics with statsd](Metrics-with-Statsd). Prometheus metrics are not
configured per job: the `GET /metrics` endpoint is global, tuned under
`web.metrics` in the `web` section above. See
[Metrics with Prometheus](Metrics-with-Prometheus).

## Load-time numeric validation

strictyaml enforces only the type (`Int`/`Float`). After type validation,
`JobConfig._validate_numeric_ranges` enforces value ranges and raises a
`ConfigError` (prefixed `Job <name>:`) on violation. These checks run at load
time, not at run time. New in the yacron2 fork.

| Rule | Condition |
| --- | --- |
| `saveLimit >= 0` | always |
| `maxLineLength > 0` | always |
| `killTimeout >= 0` | always |
| `executionTimeout > 0` | only when `executionTimeout` is set |
| `onFailure.retry.maximumRetries >= -1` | only when a `retry` block is present |
| `onFailure.retry.initialDelay >= 0` | only when a `retry` block is present |
| `onFailure.retry.maximumDelay > 0` | only when a `retry` block is present |
| `onFailure.retry.backoffMultiplier > 0` | only when a `retry` block is present |

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
