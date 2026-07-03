# Troubleshooting and FAQ

A problem -> cause -> fix reference for common yacron2 failures, grounded in the
source. Each entry names the exact config option, default, or code path involved.
For full option semantics see the [Configuration Reference](Configuration-Reference);
for deployment specifics see [Production and Container Deployment](Production-Deployment).

## Startup and configuration loading

### "configuration file not found"

**Symptom.** yacron2 exits immediately with:

```text
yacron2 error: configuration file not found, please provide one with the --config option
```

followed by the argument help, and exit code `1`.

**Cause.** `__main__.py` defaults `-c`/`--config` to a platform-specific
`CONFIG_DEFAULT = platform.DEFAULT_CONFIG_PATH`: `/etc/yacron2.d` on POSIX, or
`%APPDATA%\yacron2` on Windows (for example `C:\Users\<you>\AppData\Roaming\yacron2`,
falling back to the user profile `~` if `APPDATA` is unset). When that default is in
effect *and* the path does not exist
(`args.config == CONFIG_DEFAULT and not os.path.exists(args.config)`), yacron2
prints the error and exits before constructing the scheduler. The not-found special
case keys off the *platform default value*, not the literal string `/etc/yacron2.d`,
so on Windows it fires when `-c` is left at `%APPDATA%\yacron2` and that path does not
exist. See [Running on Windows](Running-on-Windows).

**Fix.** Create `/etc/yacron2.d/` and place `*.yaml`/`*.yml` files in it, or pass an
explicit path with `-c FILE-OR-DIR` (a single file or a directory). On Windows the
default location to create and populate is `%APPDATA%\yacron2` rather than
`/etc/yacron2.d`; see [Running on Windows](Running-on-Windows). Note the error
text is only emitted for the *default* path; an explicit `-c` pointing at a missing
file instead surfaces as a `ConfigError` (see below). yacron2's default is
`/etc/yacron2.d` rather than yacron's `/etc/yacron.d`; if you upgraded from yacron,
move your config directory. See [Migration from yacron](Migration-from-yacron) and the
[Command-Line Reference](CLI-Reference).

### "Configuration error" / a missing or unreadable explicit config file

**Symptom.** `Configuration error: <message>` is logged and yacron2 exits `1`.

**Cause.** Any `ConfigError` raised during initial parse aborts startup
(`__main__.py` wraps `Cron(args.config)` and exits on `ConfigError`). When `-c`
points at a single file that is missing or unreadable, `parse_config` catches the
`OSError` and re-raises it as a clean `ConfigError` with the OS message.

**Fix.** Correct the path/permissions or the YAML. Run `yacron2 -v -c <path>` to
validate without starting the scheduler; on success it logs `Configuration is valid.`
and exits `0`. See [Includes, Defaults, and Multi-File Config](Includes-and-Defaults).

### Reload errors do not crash a running daemon

**Symptom.** After editing a live config, the log shows
`Error in configuration file(s), so not updating any of the config.:` (followed by
the parse error) but jobs keep running with the old config.

**Cause.** This is by design. The scheduler re-reads the config each wakeup. If
`update_config()` raises `ConfigError`, the loop logs it and keeps the
previously-loaded `cron_jobs` (the assignment only happens on a successful parse).
This applies only to reloads; a parse failure at *initial* startup still exits `1`.

**Fix.** Fix the YAML; the next wakeup picks it up. A `logging` section that was
broken and is later corrected is also re-applied on reload without a restart
(logging config is only marked applied on success).

## Standalone binary under a read-only root filesystem

### Binary aborts at startup: "Could not create temporary directory" / "Operation not permitted"

**Symptom.** The downloaded standalone binary aborts at startup with
`Could not create temporary directory`, or
`Error loading shared library …: Operation not permitted`.

**Cause.** The standalone binary is a self-extracting PyInstaller executable: on each
start it unpacks its embedded Python runtime into a temporary directory and loads
shared libraries from there, so it needs a temp directory that is both **writable**
and **executable**. Under a read-only root filesystem (a hardened container), `/tmp`
is read-only too, and the unpack/exec fails. This requirement is unique to the
standalone binary. The published container image and `pip`/`pipx` installs run
yacron2 as an ordinary Python package and never self-extract.

**Fix.** Provide a small writable, executable temp mount. With Docker, note that
`--tmpfs` defaults to `noexec`, so you must request `exec` explicitly:

```shell
docker run --rm --read-only \
  --tmpfs /tmp:rw,exec,nosuid,nodev,size=64m \
  -v "$PWD/yacron2tab.yaml:/etc/yacron2.d/yacron2tab.yaml:ro" \
  your-image-with-the-binary -c /etc/yacron2.d
```

On Kubernetes mount an `emptyDir` at `/tmp` (writable and executable by default;
`medium: Memory` for a tmpfs). Alternatively point the binary at any other writable,
executable directory with `TMPDIR=/path`. See [Installation](Installation) and
[Production and Container Deployment](Production-Deployment).

## Per-job user/group switching

### "yacron2 is not running as superuser"

**Symptom.** Startup (or reload) fails with:

```text
Job <name> wants to change user or group, but yacron2 is not running as superuser
```

**Cause.** A job sets `user` and/or `group`. `_resolve_user_group` raises this
`ConfigError` whenever `self.uid` or `self.gid` is set and `os.geteuid() != 0`.
Dropping to another user requires the daemon itself to start as root.

**Windows note.** Per-job `user`/`group` switching is POSIX-only (Windows has no
setuid/setgid model). On Windows a job with `user` or `group` set raises a config
error *before* the superuser/`geteuid` check is ever reached, verbatim:
`Job <name>: changing user/group is not supported on Windows`. The "not running as
superuser", "User not found", and "Group not found" errors and the numeric-uid
passwd-database behavior below are therefore all POSIX-only (they require the
`pwd`/`grp` databases and `os.geteuid()`). The fix on Windows is to remove the
`user`/`group` fields. See [Running on Windows](Running-on-Windows).

**Fix.** Run the daemon as root if you need per-job privilege drop, or remove the
`user`/`group` fields from the job. Related `ConfigError`s from the same code path:

- `User not found: '<user>'`: a string `user` is not in the passwd database
  (`getpwnam` raised `KeyError`).
- `Group not found: '<group>'`: a string `group` is not in the group database.

A numeric `user` without an explicit `group` derives its primary gid (and login name,
used for supplementary groups) from the passwd database; if the uid is not in the
database, the gid is left unset and supplementary groups are cleared. See
[Commands and Environment](Commands-and-Environment).

## Web control API

### API serves without authentication / refuses to start

**Symptom (intended hardening).** With `web.authToken` configured, yacron2 either
logs `web: requiring bearer-token authentication` and requires
`Authorization: Bearer <token>` on every route, or raises:

```text
web.authToken is configured but resolved to an empty token; refusing to start the web API without authentication
```

**Cause.** `_resolve_web_token` fails closed. If `authToken` is present but resolves
to an empty string (an unset `fromEnvVar`, an empty/missing `fromFile`, or an empty
`value`), it raises `ConfigError` rather than silently serving the control API
unauthenticated. A `fromFile` that cannot be read raises
`web.authToken.fromFile could not be read: …`. If `authToken` is entirely absent, the
API listens without auth (this is the default).

**Fix.** Ensure exactly one of `value`/`fromFile`/`fromEnvVar` resolves to a
non-empty secret. Precedence is `value`, then `fromFile`, then `fromEnvVar`. The
`Bearer` scheme is matched case-insensitively and the token is compared in constant
time (`hmac.compare_digest`); a wrong/absent token returns `401`. See
[HTTP Control API](HTTP-API).

### Web listen URL is ignored

**Symptom.** A `web.listen` entry never accepts connections; the log shows
`web: could not listen on <url>: <error>` or
`Ignoring web listen url <url>: …`.

**Cause.** Per-address failures are warned-and-skipped, not fatal. A malformed
`http://` URL (missing host or port) or an unsupported scheme raises `ValueError`
internally and is skipped; a bind `OSError` (port in use, permission, bad socket
path) is likewise skipped. Only `http://` and `unix://` schemes are supported. The
`web: started listening on <url>` message is logged only after the bind succeeds.

**Windows note.** `unix://` listeners are *not* supported on Windows (the Proactor
event loop lacks `create_unix_server`). Such a listen URL is skipped with the
verbatim warning
`Ignoring web listen url <url>: unix-socket listeners are not supported on this platform`;
use an `http://` listener instead. See [Running on Windows](Running-on-Windows).

**Fix.** Use a supported scheme with host and port (`http://127.0.0.1:8080`) or a
`unix://` path, and resolve the bind error. For a `unix://` socket on a read-only
root filesystem, point it at a writable volume and optionally set `web.socketMode`
(octal string) for permissions. This `unix://`/`web.socketMode` guidance is
POSIX-only: on Windows unix sockets are unavailable and `socketMode` is irrelevant
(it only ever applies to unix sockets); on Windows use an `http://` listener. See
[Running on Windows](Running-on-Windows).

### Duplicate `web` or `logging` across a config directory

**Symptom.** Startup fails with `Multiple 'web' configurations found: first in …, now
in …` (or the same for `logging`).

**Cause.** When `-c` is a directory, `_parse_config_dir` aggregates across files but
allows at most one `web` block and one `logging` block total; a second one raises
`ConfigError`. (Within `include` chains the equivalent errors are `multiple web
configs` / `multiple logging configs`.)

**Fix.** Keep `web` and `logging` in a single file. See
[Includes, Defaults, and Multi-File Config](Includes-and-Defaults) and
[Logging Configuration](Logging-Configuration).

## Scheduling

### A job runs immediately on startup (or never runs at startup)

**Symptom.** A job fires the moment yacron2 starts, or you expected one to fire at
startup and it did not.

**Cause.** At startup (`startup=True`), `job_should_run` returns `True` *only* for
jobs whose schedule is the literal string `@reboot`; all CronTab-scheduled jobs return
`False` and wait for their next matching minute. yacron2 wakes aligned to the start of
each minute and runs a CronTab job when `crontab.test(now.replace(second=0))` matches.

**Fix.** Use `schedule: "@reboot"` for run-on-start behavior; use a normal crontab
expression or schedule object otherwise. See
[Schedules and Timezones](Schedules-and-Timezones).

### A disabled job never runs

**Symptom.** A job with `enabled: false` is skipped entirely; `GET /status` reports it
as `disabled`; `POST /jobs/<name>/start` returns `409 Conflict`
(`job '<name>' is disabled`).

**Cause.** `enabled` defaults to `true`. `enabled: false` jobs
are treated "as if they aren't there" apart from config validation:
`job_should_run` short-circuits to `False`, and the web API refuses to launch them.

**Fix.** Set `enabled: true` (or remove the field) to run the job.

### A `second` schedule does not fire more than once a minute

**Symptom.** A schedule with a `second` field (or a seven-field crontab string) only
seems to run once a minute.

**Cause.** Second-level scheduling requires a **seven-field** crontab string
(`second minute hour dayOfMonth month dayOfWeek year`) or the object `second:` key.
A common mistake is writing a **six-field** string like `"*/15 * * * * *"` expecting
"every 15 seconds". A six-field line has no seconds column: its *leading* field is
still the **minute**, and the extra *trailing* sixth field is the **year** (`*` =
any year). So `"*/15 * * * * *"` actually runs every 15 **minutes** (at second 0),
not every 15 seconds. Add the seventh field (`"*/15 * * * * * *"`, whose leading
field is the second), or use the object form (`second: "*/15"`).

**Fix.** Use the object `second:` key, or a full seven-field string. See
[Second-level schedules](Schedules-and-Timezones#second-level-schedules). Note that
second-level scheduling is a YAML feature; [classic crontab files](Classic-Crontabs)
stay five-field and minute-granular.

### Unknown timezone

**Symptom.** Startup fails with `unknown timezone: <value>`.

**Cause.** `_resolve_timezone` calls `ZoneInfo(timezone)`; a
`ZoneInfoNotFoundError`/`ValueError` is re-raised as `ConfigError`. On slim images
that lack the system tz database, yacron2 depends on the bundled `tzdata` package to
resolve names.

**Fix.** Use a valid IANA name (for example `America/Los_Angeles`). The `timezone`
option takes precedence over `utc`; with neither set, scheduling
uses local time only when `utc: false` (the default `utc` is `true`, i.e. UTC). See
[Schedules and Timezones](Schedules-and-Timezones).

### "include cycle detected"

**Symptom.** Startup fails with `include cycle detected at <path>`.

**Cause.** `parse_config_file` tracks visited absolute paths in a per-top-level-parse
`_seen` set; a file that includes itself directly or transitively raises this
`ConfigError` instead of recursing to `RecursionError`. Two independent files
including a common file are *not* flagged (the set is scoped per top-level parse).

**Fix.** Break the include cycle. Remember that a top-level `defaults` block does not
retro-apply to jobs pulled in via `include`; included jobs arrive fully constructed
with only their own file's defaults. See
[Includes, Defaults, and Multi-File Config](Includes-and-Defaults).

### Jobs in a config directory are not loaded

**Symptom.** Some files in the `-c` directory are silently ignored.

**Cause.** `_parse_config_dir` skips a directory entry when the base name's first
character is `_` or `.` (so `_inc.yaml` and dotfiles are excluded), or when its name
is neither YAML (`.yml`/`.yaml` extension) nor a classic crontab (`.crontab`/`.cron`
extension, or a file named `crontab`; see [Classic Crontabs](Classic-Crontabs)).
Entries are processed in sorted filename order.

**Fix.** Name loadable configs with a non-`_`/non-`.` leading character and a
recognised name (`.yml`/`.yaml` for YAML, `.crontab`/`.cron`/`crontab` for classic
crontabs). Files meant only to be `include`d (conventionally `_*.yaml`) are
intentionally skipped as top-level configs and pulled in by the file that includes
them.

## Failure detection and output

### A job is marked failed on nonzero exit or any stderr

**Symptom.** A job that "worked" is reported as failed; the log shows a `fail_reason`
such as `failsWhen=nonzeroReturn and retcode=<n>` or
`failsWhen=producesStderr and stderr is not empty`.

**Cause.** `fail_reason` is computed from `failsWhen`. The defaults are:

| failsWhen key   | Type | Default | Effect when `true`                                  |
| --------------- | ---- | ------- | --------------------------------------------------- |
| `producesStdout`| Bool | `false` | Any captured stdout marks the job failed            |
| `producesStderr`| Bool | `true`  | Any captured stderr marks the job failed            |
| `nonzeroReturn` | Bool | `true`  | A nonzero exit code marks the job failed            |
| `always`        | Bool | `false` | The job is always considered failed when it exits   |

A job whose command cannot be launched at all (for example, the executable does not
exist) is reported as an ordinary failure with exit code `127`, not an internal error.
Note `producesStderr`/`producesStdout` only apply when the corresponding stream is
captured, and they also fire when output was *discarded* (`saveLimit: 0` still counts
discarded lines as output).

**Fix.** Adjust `failsWhen`. To stop stderr from marking a job failed, set
`producesStderr: false`. Note `producesStdout` is the only required key in the
`failsWhen` map (strictyaml-required); the other three are optional and take the
defaults above. See [Failure Detection and Retries](Failure-Detection-and-Retries).

### stderr capture vs. routing

**Symptom.** A job's output does not appear where expected, or is not in failure
reports.

**Cause.** Defaults are `captureStderr: true`, `captureStdout: false`. A stream that
is *not* captured is passed through to yacron2's own stdout/stderr (job stderr to
yacron2 stderr); only captured streams are saved and available to `failsWhen` and to
report templates. Captured lines are prefixed per `streamPrefix`
(default `"[{job_name} {stream_name}] "`).

**Fix.** Enable `captureStdout: true` / keep `captureStderr: true` for the streams you
need in reports or in `producesStdout`/`producesStderr` checks. See
[Output Capturing](Output-Capturing).

### "ignored a very long line"

**Symptom.** Log warning `job <name>: ignored a very long line`, and that line is
absent from captured output.

**Cause.** Captured streams use an asyncio reader limited to `maxLineLength`
(default `16 * 1024 * 1024` = 16 MiB). A line longer than the limit raises
`ValueError` in the reader, which logs the warning and skips that line.

**Fix.** Raise `maxLineLength` (must be `> 0`; `_validate_numeric_ranges` rejects
non-positive values with a `ConfigError`). Distinct from saved-output truncation:
`saveLimit` (default `4096` lines, `0` disables saving) caps how many lines are kept,
inserting a `[.... N lines discarded ...]` marker. See
[Output Capturing](Output-Capturing).

### Invalid numeric option values

**Symptom.** Startup fails with `Job <name>: <field> must be …`.

**Cause.** `_validate_numeric_ranges` enforces ranges strictyaml's type check cannot:
`saveLimit >= 0`, `maxLineLength > 0`, `killTimeout >= 0`, `executionTimeout > 0` when
set, `onFailure.retry.maximumRetries >= -1` (`-1` = retry forever),
`initialDelay >= 0`, `maximumDelay > 0`, `backoffMultiplier > 0`.

**Fix.** Use values within those ranges. See
[Concurrency and Timeouts](Concurrency-and-Timeouts) and
[Failure Detection and Retries](Failure-Detection-and-Retries).

## Reporting

### SMTP TLS certificate failures after upgrading from yacron

**Symptom.** Mail that delivered fine under yacron now fails with a TLS/certificate
validation error.

**Cause.** yacron2's mail `validate_certs` default is `True` (a change from yacron;
`_REPORT_DEFAULTS["mail"]["validate_certs"] = True`). The mail reporter passes
`validate_certs=mail["validate_certs"]` to `aiosmtplib.SMTP`, so delivery to servers
with self-signed or otherwise invalid certificates that previously worked silently now
fails.

**Fix.** Fix the server certificate, or set `validate_certs: false` on the `mail`
report block to restore the old behavior. Related mail TLS keys: `tls` (default
`false`, implicit TLS), `starttls` (default `false`). See
[Reporting (Mail, Sentry, Shell, Webhook)](Reporting). Reporter exceptions are logged
(`Problem reporting job <name> failure`) and never crash the scheduler; reporters run
concurrently with `return_exceptions=True`.

### A report (Sentry / mail / shell / webhook) is silently skipped

**Symptom.** No report is sent and there is no exception.

**Cause.** Each reporter early-returns when not configured: Sentry returns unless a
DSN resolves (and logs `sentry: dsn env var '<name>' is not set; not reporting` when
`fromEnvVar` is unset); mail returns unless both `to` and `from` are set (and logs
`mail: password env var is not set; not sending email` when a `fromEnvVar` password is
unset); the shell reporter returns when `command` is `None`; the webhook reporter
returns unless a `url` resolves (and logs `webhook: url env var '<name>' is not set;
not reporting` when `fromEnvVar` is unset). A successful-job mail with
an empty rendered body is also skipped.

**Fix.** Provide the required fields. See [Reporting (Mail, Sentry, Shell, Webhook)](Reporting).

## Metrics

### No statsd metrics arrive

**Symptom.** Expected metrics never reach statsd. yacron2 emits four per job:
`start`, `stop`, and `success` as gauges (`|g`) and `duration` as a timer
(`|ms|@0.1`), prefixed by the job's `statsd.prefix`. At most a log warning
`Job <name>: failed to send statsd … metric` or an error
`UDP error received: <exc>`.

**Cause.** statsd is best-effort: metrics go out over fire-and-forget UDP, and an
`OSError` on send (for example an unresolvable host) is logged as a warning rather than
propagated. If the `statsd` block is absent, no statsd metrics are emitted at
all (the [Prometheus endpoint](Metrics-with-Prometheus) is independent of it).

**Fix.** Configure the job's `statsd` block (`host`, `port`, `prefix`, all required)
and verify the host resolves and the UDP path is open. See
[Metrics with statsd](Metrics-with-Statsd).

### Prometheus scrapes return 401

**Symptom.** Prometheus marks the yacron2 target down with
`server returned HTTP status 401 Unauthorized`; `curl http://host:port/metrics`
returns `401`.

**Cause.** `web.authToken` is configured, and `GET /metrics` requires the bearer
token like every other data endpoint. The only exemption is
`web.metrics.public: true`, which exempts `/metrics` (and only `/metrics`) from
authentication.

**Fix.** Send the token from the scrape configuration (an `authorization` block
with `type: Bearer` and `credentials`), or set `web.metrics.public: true` if the
scraper cannot send credentials -- everything else stays gated. See
[Metrics with Prometheus](Metrics-with-Prometheus).

### `/metrics` returns 404

**Symptom.** `GET /metrics` returns `404 Not Found`, or nothing listens on the
expected port at all.

**Cause.** The endpoint is served by the web API and is on by default whenever
the web API is enabled, so a `404` means it was disabled explicitly
(`web.metrics: false`, or `web.metrics.enabled: false` in the map form). If the
connection is refused instead, there is no `web` section (or no working
`web.listen` entry), so no web API -- and no `/metrics` -- is served.

**Fix.** Remove the `metrics: false` / `enabled: false` override (`web.metrics`
left unset means enabled), and make sure a `web.listen` entry binds
successfully. See [Metrics with Prometheus](Metrics-with-Prometheus).

## Clustering

These cover the optional `cluster` section (peer attestation, leader election,
and the lease backends). For the full model see
[Clustering and Leader Election](Clustering-and-Leader-Election).

### `quorate: false` and `Leader` jobs stop running

**Symptom.** `GET /cluster` reports `"quorate": false` and `Leader`-policy jobs
stop firing on every replica (`PreferLeader` jobs keep running).

**Cause.** This node cannot see a quorum (a strict majority) of the cluster, so
it deliberately **stands down** rather than risk a second leader. On the
`gossip` backend that means a minority-side network partition or too many peers
`unreachable`/`untrusted`. On a **lease** backend (`kubernetes`/`etcd`) it means
the coordination store is unreachable or the last read has gone stale (past one
`leaseDurationSeconds`/`ttl`); `quorate` there means "has a fresh read of the
store", not "sees a majority". Either way `Leader` **fails closed** (skips) while
`PreferLeader` keeps running (it may double-run during the outage).

**Fix.** Restore a quorum: heal the partition or bring failed peers back so a
majority is mutually reachable (gossip), or restore reachability to the apiserver
/ etcd (lease backends). See
[Why the quorum gate is safe](Clustering-and-Leader-Election#why-the-quorum-gate-is-safe)
and, for the lease backends, [Failure modes](Clustering-and-Leader-Election#failure-modes).

### Duplicate `nodeName` conflict (`conflict: true` with `conflict_names`)

**Symptom.** `GET /cluster` shows `"conflict": true` with the offending name in
`"conflict_names"`, one or more peers show status `conflict`, a banner appears in
the dashboard cluster panel, and an `ERROR` log line reports the duplicate.
`Leader` jobs stand down.

**Cause.** Two processes are running with the **same `nodeName`** (distinguished
by their random per-process instance ids). Because each would elect itself as the
lowest name in its own view, both could lead: a silent double-run. yacron2
detects this and **fails `Leader` jobs closed** until it clears (`PreferLeader`
and `EveryNode` are not gated).

**Fix.** Give every node a distinct `nodeName` (or distinct hostnames, since the
default `nodeName` is the system hostname). The gate is self-healing: it clears
automatically once the duplicate is renamed. See
[Unique node names](Clustering-and-Leader-Election#unique-node-names).

### Coordination-policy conflict (`policy_conflict: true` with `conflicting_policies`)

**Symptom.** `GET /cluster` shows `"policy_conflict": true` with the differing
descriptors in `"conflicting_policies"` (and the umbrella `"conflict": true`), a
banner appears in the dashboard cluster panel, and `Leader` jobs stand down.

**Cause.** A quorate peer is advertising a different `distribution` or
`elect_leader` setting than this node. Because those are cluster-wide coordination
settings (not part of the job-set id, so they do not surface as drift), a mismatch
would let nodes coordinate differently and double-run. It is the **third** trigger
of the umbrella `conflict` flag, alongside a duplicate `nodeName`
(`conflict_names`) and a cluster-size disagreement (`size_conflict` /
`conflicting_sizes`); all three stand `Leader` jobs down.

**Fix.** Align `electLeader` and `distribution` across every node and roll the
change out uniformly (one node at a time). The gate clears automatically once the
cluster reconverges on one policy. See
[Distribution: one leader, or spread the load](Clustering-and-Leader-Election#distribution-one-leader-or-spread-the-load)
and [Consistent cluster size](Clustering-and-Leader-Election#consistent-cluster-size).

### A peer shows `untrusted`

**Symptom.** A peer's status on `GET /cluster` is `untrusted` (with a TLS error in
`last_error`), and the `peer … is untrusted` `WARNING` appears in the logs. That
peer never counts toward agreement, so quorum can drop.

**Cause (gossip only).** The peer's certificate did not verify: it does not chain
to the configured `tls.ca`, or its SAN/hostname does not match the address it was
reached at (standard TLS hostname pinning, e.g. the cert at
`yacron-b.internal:8443` must carry `yacron-b.internal` as a SAN). This most often
follows a botched CA roll (the overlap step was skipped) or a node whose cert
refresh lagged.

**Fix.** Restore trust overlap (distribute a CA bundle covering whichever CA the
`untrusted` peers were issued from) or finish rolling the lagging nodes; ensure
each node's cert SAN matches its peer-list host. Each node reloads within ~1
minute and peers return to `agreed`. See
[Cluster peer attestation](Clustering-and-Leader-Election#cluster-peer-attestation)
and [Cluster certificate operations](Production-Deployment#cluster-certificate-operations).

### `electLeader` on a 2-node cluster refuses to start

**Symptom.** Startup fails with a `ConfigError`:

```text
cluster.electLeader needs a fault-tolerant cluster, but this config declares
only 2 nodes (1 peer). A quorum of 2 requires both nodes up for either to run,
so it is strictly worse than a single replica. …
```

**Cause.** With `electLeader` and a 2-node cluster the quorum is 2, so **both**
nodes must be up for either to run: lower availability than a single replica and
no failover. yacron2 refuses it outright rather than silently degrade. (A 2-node
cluster is fine for attestation-only, without `electLeader`.)

**Fix.** Use 3 or more nodes (an odd count is best), or run a single replica
without `electLeader`. See
[Sizing the cluster](Clustering-and-Leader-Election#sizing-the-cluster).

### Even-size warning with `electLeader`

**Symptom.** Startup logs a warning that an even cluster size tolerates no more
failures than the next-lower odd size.

**Cause.** For `size > 2` an even size (4, 6, 10, …) needs the same quorum as the
odd size below it, so the extra node only adds something that can fail; its
`P(runs)` is equal-or-worse, never better. This is a non-fatal advisory (unlike
the 2-node case above, which is rejected).

**Fix.** Prefer an odd size: shrink by one for the same tolerance with one fewer
node, or grow by one to tolerate an extra failure. See
[Sizing the cluster](Clustering-and-Leader-Election#sizing-the-cluster).

## Concurrency and termination

### Overlapping runs, skipped runs, or a job killed mid-run

**Symptom.** Two instances of a job run at once, the next run is skipped while one is
in progress, or a running instance is terminated when the next is due.

**Cause.** `concurrencyPolicy` (default `Allow`) governs overlap: `Allow` runs
concurrently; `Forbid` skips the new run while one is still running; `Replace` cancels
the running instance (marking it `replaced` so it is not reported as a failure or
retried) and starts a new one.

**Fix.** Set `concurrencyPolicy` to the desired policy. See
[Concurrency and Timeouts](Concurrency-and-Timeouts).

### A timed-out job is killed forcefully

**Symptom.** Log shows `Job <name> exceeded its executionTimeout …, cancelling it…`
and possibly `Job <name> did not gracefully terminate after <n> seconds, killing it…`.

**Cause.** `executionTimeout` (default unset/`None`) cancels a job still running after
N seconds (recorded internally as retcode `-100`). Cancellation sends `SIGTERM`, then
`SIGKILL` if the process is still alive after `killTimeout` seconds (default `30`).

**Windows note.** The `SIGTERM`-then-`SIGKILL` escalation is POSIX behavior
(`terminate()` = `SIGTERM`, graceful and trappable; `kill()` = `SIGKILL`, forceful).
Windows has no POSIX signals: both `terminate()` and `kill()` call `TerminateProcess`,
an immediate ungraceful stop. The child is not notified to clean up, so the
terminate->kill escalation is effectively moot (`killTimeout` still bounds the wait,
but the outcome is the same hard kill). See [Running on Windows](Running-on-Windows).

**Fix.** Raise `executionTimeout`, or give the process more graceful-shutdown time via
`killTimeout`. See [Concurrency and Timeouts](Concurrency-and-Timeouts).

## Reference: exit codes used internally

| Code   | Meaning                                                              |
| ------ | ------------------------------------------------------------------- |
| `127`  | Command could not be launched (e.g. executable not found)           |
| `-100` | Job cancelled because it exceeded `executionTimeout`                |

These appear in logs and in report template `exit_code`. See
[Architecture and Internals](Architecture-and-Internals) for the scheduler and
job-lifecycle details, and [Logging Configuration](Logging-Configuration) for raising
the log level (`-l DEBUG`) when diagnosing scheduling decisions.
