# Command-Line Reference

This page documents the `yacron2` command and every argument it accepts, the
`yacron2 state` administration subcommands, the job-facing state commands a
running job uses (`state get|set|delete|keys`, `cursor`, `lock`, `artifact`,
`idempotent`, `secret`), the runtime model (foreground execution, signal
handling, exit codes), and common invocations. Behavior is taken from
`yacron2/__main__.py`, `yacron2/state_admin.py`, and `yacron2/jobcli.py`.

## Synopsis

```
yacron2 [-c FILE-OR-DIR] [-l LOG_LEVEL] [-v] [--job-set-id] [--version]
yacron2 state ACTION [options] [-c FILE-OR-DIR]
yacron2 state get|set|delete|keys ...  [--scope NAME | --global]
yacron2 cursor|lock|artifact|idempotent|secret ...  [--scope NAME | --global]
```

Without a subcommand, `yacron2` is the scheduler daemon described below. With
the `state` subcommand it is an offline administration tool for the durable
state store; see [The `state` subcommand](#the-state-subcommand). The
`state get|set|delete|keys`, `cursor`, `lock`, `artifact`, `idempotent`, and
`secret` commands are a different surface: a *running job* uses them to reach the
daemon's store through its loopback endpoint; see
[Job-facing state commands](#job-facing-state-commands).

`yacron2` runs as a single foreground process. It does not daemonize, does not
fork, and does not write a PID file. Diagnostics go to stdout/stderr via the
standard library `logging` module. To run it as a service, place it under a
process supervisor (systemd, a container runtime, etc.); see
[Production and Container Deployment](Production-Deployment).

## Arguments

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `-c`, `--config` | path (file or directory) | platform default[^cfgdefault] | Configuration file, or a directory containing configuration files. When a directory, every `*.yml`/`*.yaml` file, plus every classic crontab (`*.crontab`, `*.cron`, or a file named `crontab`), is loaded (entries whose name starts with `_` or `.` are skipped). See [Includes, Defaults, and Multi-File Config](Includes-and-Defaults) and [Classic Crontabs](Classic-Crontabs). |
| `-l`, `--log-level` | string | `INFO` | Root log level. Passed to `logging.basicConfig(level=getattr(logging, LOG_LEVEL))`, so the value must name an attribute of the `logging` module (e.g. `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`). |
| `-v`, `--validate-config` | flag | off | Parse and validate the configuration, then exit. Exits `0` if valid, `1` on a configuration error. Does not start the scheduler or web server. |
| `--job-set-id` | flag | off | Parse the configuration, print the [job-set id](Clustering-and-Leader-Election#the-job-set-id-foundation) (an order-independent hash of every job's effective configuration) to stdout, and exit `0`. Identical across instances running the same set of jobs. Exits `1` on a configuration error. |
| `--version` | flag | off | Print the yacron2 version to stdout and exit `0`. |
| `-h`, `--help` | flag | — | Print usage (argparse builtin) and exit `0`. |

The only other command-line surface is the `state` subcommand,
[documented below](#the-state-subcommand), which administers the durable state
store. Job schedules, commands, environment, reporting, and the web API are
configured entirely in YAML, not on the command line; see the
[Configuration Reference](Configuration-Reference).

[^cfgdefault]: The default config path is platform-specific (`DEFAULT_CONFIG_PATH`
    in `yacron2/platform.py`): `/etc/yacron2.d` on POSIX, and `%APPDATA%\yacron2`
    (e.g. `C:\Users\<you>\AppData\Roaming\yacron2`, falling back to the user
    profile `~` if `APPDATA` is unset) on Windows. See
    [Running on Windows](Running-on-Windows).

### `-c` / `--config`

The argument may be a single file or a directory:

- **File:** parsed directly. YAML by default; a classic crontab when the name
  says so (`*.crontab`, `*.cron`, or a file named `crontab`, e.g. a
  `crontab -l > crontab` export) or, for a file with a neutral name such as
  `-c /var/spool/cron/crontabs/root`, when the content unmistakably is one
  (see [Classic Crontabs](Classic-Crontabs); the six-field *system* crontab
  format of `/etc/crontab` is not supported). An I/O error (for example, the
  file does not exist) is reported as a configuration error and exits `1`.
- **Directory:** each non-hidden `*.yml`/`*.yaml` or crontab-named entry is
  parsed in name-sorted order. An empty directory (or one whose files are all
  skipped) yields an empty configuration with no jobs rather than an error.

#### Default-path special case

The default is the platform default config path (`DEFAULT_CONFIG_PATH` from
`yacron2/platform.py`): `/etc/yacron2.d` on POSIX, `%APPDATA%\yacron2` on
Windows. The special case is triggered by the condition
`args.config == DEFAULT_CONFIG_PATH and not os.path.exists(args.config)`: if the
config argument equals the platform default and that path does not exist,
yacron2 prints the following to stderr, prints the usage help, and exits `1`:

```
yacron2 error: configuration file not found, please provide one with the --config option
```

Because the check compares the argument value (not whether `-c` was supplied),
it fires both when `-c` is omitted and when you pass `-c` set to the platform
default explicitly (`-c /etc/yacron2.d` on POSIX, `-c %APPDATA%\yacron2` on
Windows). For any other non-existent path passed with `-c`, you instead get the
generic configuration-error path (a logged `Configuration error: ...` and exit
`1`).

### `-l` / `--log-level`

The log level is applied with `logging.basicConfig` before the configuration is
loaded, so it governs yacron2's own startup and runtime logging. The value is
resolved with `getattr(logging, args.log_level)`; an unknown name (e.g. a
lowercase or misspelled level) raises `AttributeError` and the process aborts
with a traceback rather than a clean error. Use a canonical level name such as
`DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`.

A `logging:` section in the configuration can reconfigure logging after startup
via `logging.config.dictConfig`; see [Logging Configuration](Logging-Configuration).

### `-v` / `--validate-config`

Validation works by constructing the scheduler from the resolved config
(`Cron(config)`), which parses and schema-checks every file. On success it logs
`Configuration is valid.` and exits `0`. On any `ConfigError` (schema violation,
unknown timezone, invalid numeric range, missing user/group, include cycle,
multiple `web`/`logging` sections, etc.) it logs `Configuration error: <detail>`
and exits `1`. The scheduler loop and web server are never started in this mode.

The default-path special case above still applies: it is checked before
`Cron(config)` is constructed, so validating while the config argument equals
the platform default (`DEFAULT_CONFIG_PATH`) and that path is absent exits `1`
with the not-found message rather than the `Configuration error: ...` message.

### `--job-set-id`

Constructs the scheduler from the resolved config exactly like
`--validate-config`, then prints the job-set id to stdout and exits `0`: an
order-independent hash of every job's effective configuration, identical
across instances running the same set of jobs regardless of file order or
how the jobs are split across files. This is the same value served by the
[`GET /job-set-id`](HTTP-API) endpoint and compared between cluster peers; see
[Clustering and Leader Election](Clustering-and-Leader-Election#the-job-set-id-foundation).

Because the config is fully parsed first, a configuration error exits `1`, and
the [default-path special case](#default-path-special-case) applies just as it
does for `--validate-config`.

### `--version`

Prints the version string (e.g. `1.0.13`) to stdout and exits `0`. This check
runs before the config is touched, so `--version` succeeds even when no
configuration exists.

## The `state` subcommand

```
yacron2 state ACTION [options] [-c FILE-OR-DIR]
```

`yacron2 state` administers the durable state store defined by the
configuration's `state:` section (the daemon-side store on disk or on a shared
mount -- not the [Web Dashboard](Web-Dashboard)'s browser-side IndexedDB run
ledger, which is a separate, purely client-side feature). Every action works
offline, straight from the configuration, with no running daemon required.
Actions that read or copy *out of* the store (`backup`, `check`,
`migrate-schema`) and the `gc` pass stay safe against a *running* daemon,
because records are immutable and copies/reads never lock; a backup taken
mid-write is a point-in-time-ish snapshot rather than an exact one.
Restoring or migrating *into* a store a daemon is actively using is **not**
safe (see [`state restore`](#state-restore) / [`state migrate`](#state-migrate)).

Each action accepts its own `-c`/`--config`, with the same meaning and default
as the daemon flag, so both positions work: `yacron2 -c /etc/yacron2.d state gc`
and `yacron2 state gc -c /etc/yacron2.d` are equivalent. (`-c` between `state`
and the action name is not accepted.) If the resolved configuration has no
`state:` section, or cannot be read, the action prints
`yacron2 state error: <detail>` to stdout and exits `1`; the
[default-path special case](#default-path-special-case) does not apply here.

| Action | Description |
| --- | --- |
| `backup` | Write a `.tar.gz` backup of the store. |
| `restore` | Restore a backup into the store. |
| `migrate` | Copy the store to another path or mount (local disk <-> S3 Files / EFS). |
| `gc` | Garbage-collect state of unreferenced jobs. |
| `check` | Verify the store is usable and print an inventory. |
| `migrate-schema` | Rewrite records of older known record schemes. |

### `state backup`

```
yacron2 state backup -o FILE.tar.gz [-c FILE-OR-DIR]
```

Writes a gzipped tar of the store's namespace to `-o`/`--output` (required).
The archive carries the full store: the immutable records (`records/`), the
mutable documents (`docs/` -- KV entries, cursors, idempotency claims, and
dag_run documents), the content-addressed artifact payloads (`blobs/`), and
the lease files (`leases/` -- a lease file is the only home of its fence
counter, so dropping it would re-issue fence values). Deliberately *not*
carried: `tmp/` (transient write debris) and `quarantine/` (poison records;
forensics stay with the source store). The archive is created owner-only
(mode `0600`): it flattens captured job output, KV values, and artifact
payloads into a single file. Against a live daemon, a file that disappears
mid-backup (a prune, a lease rewrite) is skipped, by design. Exits `1` when
the store directory does not exist (`nothing to back up`).

### `state restore`

```
yacron2 state restore FILE.tar.gz [--force] [-c FILE-OR-DIR]
```

Extracts a backup archive into the configured store. It refuses to restore
into a store that already contains data and exits `1`; pass `--force` to
merge the archive into it. Restoring is **not** safe while a daemon uses the
store -- stop the daemon first. Archive members are sanitised: only plain
files that extract strictly inside the store are honored (no absolute paths,
no `..` escapes, no symlinks or devices), and each file lands with mode
`0600` via a temp sibling plus atomic replace, so a concurrent reader never
sees a torn record. When merging into a populated store, `.lock` side-files
are skipped (a live daemon may hold an OS lock on that very inode), and a
lease file replaces the current one only when its archived fence counter is
provably not older -- a fence-max merge; regressing a fence would re-issue
fence values already handed out. The kept-lease count is reported.

### `state migrate`

```
yacron2 state migrate --dest PATH [--dest-deployment-id ID] [--force]
                      [-c FILE-OR-DIR]
```

Copies the store to another path or mount. A local directory and an Amazon
S3 Files / EFS mount share one on-disk layout, so migration in either
direction is a faithful file copy. `--dest` (required) is the destination
`state.path`; `--dest-deployment-id` selects a different namespace at the
destination (default: keep the current one). Each file lands via a temp
sibling plus atomic rename, so a reader of the *destination* never observes a
torn record -- important when cutting over to a shared mount that other nodes
already watch. Refused with exit `1`: migrating a store onto (or into) itself,
and a destination namespace that already holds records or leases unless
`--force` is given -- overwriting a live destination's lease files would
regress their fence counters under any daemon already using that store.
After a successful copy, point `state.path` (and `deploymentId`, if you
changed it) at the new location to cut over.

### `state gc`

```
yacron2 state gc [--dry-run] [-c FILE-OR-DIR]
```

Runs one manual garbage-collection pass with the same rules as the daemon's
automatic periodic pass: it removes the streams of jobs (and artifact
scopes) that no recent manifest references and whose newest record is older
than `state.gcGraceSeconds`, plus counter and manifest streams of
unmanifested hosts, provably dead lease files, crashed write-temp files, and
quarantined records older than the grace, then sweeps artifact payload
blobs no surviving record references. It prints what was removed (or, with
`--dry-run`, what would be), the kept-stream count, and the reclaimed
orphan-blob count -- or the reason the blob sweep stood down (an
unenumerable artifact stream or an unreadable record keeps every blob). Run
documents of removed DAGs are left to the running daemon's own pass, which
alone knows what it owns. Like the automatic pass, it defers (exit `0`,
with a message) until
the store's manifest history spans one full grace window -- a store that
cannot yet prove absence deletes nothing. When GC is disabled
(`gcGraceSeconds` <= 0) the command reports that there is nothing to collect
and exits `1`.

### `state check`

```
yacron2 state check [-c FILE-OR-DIR]
```

Verifies the store is usable -- starting the backend probes writability --
and prints an inventory: the store path, backend, namespace, topology,
shared-locking mode, the number of streams and records (broken down by stream
prefix, e.g. `runs`, `logs`, `retries`), and the quarantined-record count. A
store that cannot be started or probed exits `1`.

### `state migrate-schema`

```
yacron2 state migrate-schema [--dry-run] [-c FILE-OR-DIR]
```

Rewrites records written under *older known* record-scheme versions to the
current one, and reports how many records were converted, already current,
unknown, unreadable, or failed. `v1` is the only scheme so far, so today this
reports and converts nothing; it becomes useful only after a future scheme
bump. Records with unknown versions are left in place for the daemon's usual
quarantine-on-read handling. `--dry-run` counts without rewriting.

### `state` exit codes

Every action exits `0` on success and `1` on any error: a missing or invalid
configuration, no `state:` section, an I/O failure, or a refusal (restoring
into a non-empty store without `--force`, migrating a store onto itself, GC
with `gcGraceSeconds` disabled). Errors print `yacron2 state error: <detail>`.
`yacron2 state` with no action prints a pointer to `yacron2 state --help` and
exits `2`, the same code argparse itself uses for usage errors (an unknown
option, or a missing required one such as `backup` without `-o`).

## Job-facing state commands

Alongside the offline `state` admin actions above, yacron2 ships a family of
**job-facing** state commands -- `state get|set|delete|keys`, `cursor`, `lock`,
`artifact`, `idempotent`, and `secret` -- that a *running job's* command line
uses to reach the daemon's durable store. They are thin clients of the
[loopback state endpoint](HTTP-API#job-facing-state-endpoints-loopback) the
daemon injects into every job's environment: each reads the injected
`YACRON2_STATE_URL` / `YACRON2_STATE_TOKEN` and speaks HTTP over the standard
library (no aiohttp, no event loop), so it starts instantly and needs no config
file. Behavior is taken from `yacron2/jobcli.py`.

These are meant to run **inside a job**, not from an operator shell: outside a
job the injected environment is absent and every command exits `1` with `not
running inside a yacron2 job: YACRON2_STATE_URL is not set`. They require a
`state:` section with `jobApi.enabled` (the default); see the
[endpoint reference](HTTP-API#job-facing-state-endpoints-loopback) and
[Durable State](Durable-State).

> The `state get|set|delete|keys` job actions **coexist** with the offline
> `state backup|restore|migrate|gc|check|migrate-schema`
> [admin actions](#the-state-subcommand) under the one `yacron2 state` command;
> the action name selects which handler runs. The admin actions operate offline
> from `-c`; these job actions act through the running daemon and take no `-c`.

### Scope and exit codes

Every KV, cursor, artifact, lock, and idempotency command acts in a *scope*: a
namespace that defaults to the calling job's own name, so one job cannot read
another's state by accident. Two mutually exclusive flags override it:

| Flag | Meaning |
| --- | --- |
| `--scope NAME` | Act in the named scope. |
| `--global` | Act in the shared `global` scope (deliberate cross-job coordination). |

(`secret` takes neither flag: a run's secrets are always its own.)

The commands share one exit-code convention, made for shell branching:

| Code | Meaning |
| --- | --- |
| `0` | Success (or, for `idempotent`, the claim was fresh). |
| `1` | An error (a transport or store failure). |
| `2` | Usage error (argparse; e.g. a command invoked with no action). |
| `3` | A `lock acquire` / `lock run` did not get the lock. |
| `4` | The looked-up key, cursor, artifact, or secret does not exist. |
| `5` | The `idempotent` key was already claimed -- a duplicate. |

### `state get|set|delete|keys` (durable key/value)

```
yacron2 state get KEY [--scope NAME | --global]
yacron2 state set KEY VALUE [--json] [--scope NAME | --global]
yacron2 state delete KEY [--scope NAME | --global]
yacron2 state keys [--scope NAME | --global]
```

Durable, restart-surviving key/value storage. `get` prints the value (exit `4`
if the key is absent); `set` stores it (as a string by default, or as a parsed
JSON document with `--json`); `delete` removes it (exit `4` if it did not exist);
`keys` prints one key per line for the scope.

### `cursor get|advance` (ETL watermark)

```
yacron2 cursor get NAME [--scope NAME | --global]
yacron2 cursor advance NAME VALUE [--force] [--scope NAME | --global]
```

A monotonic marker an incremental job advances and never sees regress. `advance`
moves the cursor to `VALUE` only when it is greater than the stored value (a
numeric `VALUE` compares numerically, otherwise it compares as a string), so a
replayed or out-of-order batch cannot walk it backwards; `--force` sets it
unconditionally (a deliberate rewind). `get` prints the current value (exit `4`
if the cursor is unset). Both print the resulting value.

### `lock acquire|release|run` (distributed mutex/semaphore)

```
yacron2 lock acquire NAME [--permits N] [--wait --timeout S] [--ttl S] [--scope NAME | --global]
yacron2 lock run NAME [--permits N] [--wait --timeout S] [--ttl S] [--scope NAME | --global] -- COMMAND...
yacron2 lock release TOKEN
```

A fleet-wide mutex (or a semaphore, with `--permits N`) held as a daemon-renewed
lease. `acquire` takes the lock and prints its hold token (exit `3` if it could
not, unless `--wait` blocks up to `--timeout` seconds for a free permit);
`release TOKEN` frees a lock by the token `acquire` printed (it takes no scope
flags). `run` is the safe form: it holds the lock while running `COMMAND...`
(everything after `--`), exits with the command's own exit code, and always
releases the lock afterward -- even if the command fails or is signalled.
`--ttl` overrides the lease TTL (default `state.jobApi.lockTtlSeconds`). The
daemon also releases any lock a run still holds when the run ends, so a crash
never leaks one.

### `artifact put|get|list` (named blob store)

```
yacron2 artifact put NAME [FILE] [--scope NAME | --global]
yacron2 artifact get NAME [-o FILE] [--scope NAME | --global]
yacron2 artifact list [--scope NAME | --global]
```

Small named blobs published by one run and read back by a later run or a peer
node. `put` publishes from `FILE` (or from stdin when `FILE` is omitted or `-`)
and prints the payload's `sha256`; `get` writes the newest blob for `NAME` to
`-o FILE` (or stdout when omitted or `-`, and exits `4` if the name was never
published); `list` prints one artifact name per line.

### `idempotent` (run-once guard)

```
yacron2 idempotent KEY [--ttl S] [--release] [--scope NAME | --global]
```

A fleet-wide create-if-absent claim: the first caller to claim `KEY` exits `0`
(fresh -- do the work), every later caller exits `5` (a duplicate -- skip it),
made for a shell guard around an at-most-once side effect. A transport or
store error exits `1` instead, distinct from the duplicate code, so an outage
is detectable rather than reading as "already done". `--ttl S` expires the
claim after `S` seconds (`0`, the default, is a permanent claim); `--release`
drops the claim instead of making it, so `KEY` can be claimed fresh again.

### `secret get|list` (run-scoped secrets)

```
yacron2 secret get NAME
yacron2 secret list
```

Read a secret the daemon staged in memory for this run (resolved fresh per run,
never written to the store, dropped when the run ends). `get` prints the value
(exit `4` if no secret of that name is staged); `list` prints one staged name
per line. There are no scope flags: a run sees only its own secrets.

### `xcom push|pull|list` (DAG cross-task data)

```
yacron2 xcom push --key KEY [FILE]
yacron2 xcom pull --task TASK --key KEY [--map-index I] [-o FILE]
yacron2 xcom list
```

Pass data between the tasks of a [DAG run](Orchestration-and-DAGs#xcom-passing-data-between-tasks).
Only meaningful inside a task the DAG scheduler launched (the daemon injects the
run's XCom scope and this task's id); outside one, the commands print a clean
error and exit non-zero.

- `push` publishes this task's output under `KEY` (from `FILE`, or stdin).
- `pull` reads an upstream task's output; `--map-index` selects one instance of
  a [mapped](Orchestration-and-DAGs#fan-out-dynamic-mapping) upstream. Writes to
  `-o FILE` or stdout; exit `4` if that key was never published.
- `list` prints the XCom keys published in this run.

### Job command examples

These run inside a job, where the daemon has injected `YACRON2_STATE_URL` and
`YACRON2_STATE_TOKEN`.

Advance an ETL watermark from the highest id this run processed, so the next run
resumes from where it stopped:

```shell
last=$(yacron2 cursor get rows 2>/dev/null || echo 0)
process-rows --since "$last" --emit-max-id > max_id
yacron2 cursor advance rows "$(cat max_id)"
```

Hold a fleet-wide mutex while a critical section runs, releasing it
automatically when the command finishes (or crashes):

```shell
yacron2 lock run db-migrate --wait --timeout 300 -- ./apply-migrations.sh
```

Guard an at-most-once side effect so a retried or duplicated run sends today's
invoices only once, fleet-wide:

```shell
if yacron2 idempotent "invoice-$(date +%F)"; then
    send-invoices
else
    echo "invoices already sent for today; skipping"
fi
```

Hand a build artifact from one job to a shared scope another job reads:

```shell
yacron2 artifact put report.pdf ./out/report.pdf --global
# ...then, in a later job:
yacron2 artifact get report.pdf -o ./report.pdf --global
```

## Runtime model

When started normally (no `--version`, no `--validate-config`, no
`--job-set-id`, no `state` subcommand, with a usable config), yacron2:

1. Configures logging from `-l`.
2. Resolves and parses the configuration (`-c`), exiting `1` on error.
3. Installs shutdown handlers. On POSIX these are bound to `SIGINT` and
   `SIGTERM` on the event loop; on Windows yacron2 instead uses `signal.signal`
   for `SIGINT` (Ctrl-C) and `SIGBREAK` (Ctrl-Break) plus a heartbeat timer,
   because the Proactor loop has no `add_signal_handler`.
4. Runs the asyncio scheduler loop in the foreground until shutdown.

The scheduler re-reads the configuration on every loop iteration, so editing the
config files takes effect without a restart. A configuration that becomes
invalid after a successful start is logged and ignored; the previously loaded
jobs keep running. See [Architecture and Internals](Architecture-and-Internals).

### Signal handling and graceful shutdown

`SIGINT` (Ctrl-C) and `SIGTERM` are both bound to the same graceful-shutdown
path: they set an internal stop event. The scheduler loop notices the event,
stops scheduling new job runs, logs `Shutting down (after currently running
jobs finish)...`, and then yacron2:

1. Cancels all pending retry timers.
2. Waits for currently running jobs to finish.
3. Stops the HTTP control server if it is running (logged as
   `Stopping http server`).

yacron2 does not force-kill its own running jobs on shutdown. Individual jobs
have their own kill behavior (`killTimeout`) when they are stopped; see
[Concurrency and Timeouts](Concurrency-and-Timeouts). Sending a second signal
does not change the shutdown sequence; if you need an immediate stop, kill the
process with `SIGKILL` (POSIX-only; there is no Windows equivalent, so use Task
Manager or `taskkill /F` there).

On Windows, press Ctrl-C or Ctrl-Break (`SIGINT`/`SIGBREAK`) to trigger the same
graceful shutdown: it finishes the currently-running jobs first, exactly as
`SIGTERM` does on POSIX. The wiring differs only internally: `signal.signal`
plus a heartbeat timer, because the Proactor loop lacks `add_signal_handler`.
See [Running on Windows](Running-on-Windows).

### Exit codes

| Code | Condition |
| --- | --- |
| `0` | `--version` printed; `--validate-config` succeeded; `--job-set-id` printed; `--help`; a `state` action succeeded; or normal shutdown after a signal. |
| `1` | Configuration error (parse/schema/validation failure or unreadable config); the default `-c` path (platform-specific: `/etc/yacron2.d` on POSIX, `%APPDATA%\yacron2` on Windows) does not exist and no `-c` was given; or a `state` action failed (see [`state` exit codes](#state-exit-codes)). |
| `2` | Usage error (argparse builtin): unknown option or missing required option (e.g. `state backup` without `-o`); or `yacron2 state` invoked with no action. |

A traceback (non-zero, not the clean `1` path) results from an invalid
`--log-level` value, since the level is resolved before error handling is in
place.

## Examples

Run with a single config file in the foreground:

```shell
yacron2 -c /tmp/my-crontab.yaml
```

Run against a config directory (the conventional container entrypoint):

```shell
yacron2 -c /etc/yacron2.d
```

On Windows the config path uses Windows paths and the default is
`%APPDATA%\yacron2` rather than `/etc/yacron2.d`:

```bat
yacron2.exe -c %APPDATA%\yacron2
```

See [Running on Windows](Running-on-Windows) for Windows-specific CLI behavior
(default config path, default shell, Ctrl-C / Ctrl-Break shutdown).

Validate a config and exit (suitable for CI or a container healthcheck/preflight):

```shell
yacron2 -v -c /etc/yacron2.d
```

Increase log verbosity:

```shell
yacron2 -l DEBUG -c /tmp/my-crontab.yaml
```

Print the version:

```shell
yacron2 --version
```

Back up the durable state store defined by a config (the `-c` may equally go
before `state`):

```shell
yacron2 state backup -o /backups/yacron2-state.tar.gz -c /etc/yacron2.d
```

For installation and packaging details (pip, PyInstaller binary, Docker), see
[Installation](Installation). For deploying yacron2 as a long-running service,
see [Production and Container Deployment](Production-Deployment). For
Windows-specific CLI behavior (default config path, default shell, Ctrl-C /
Ctrl-Break shutdown), see [Running on Windows](Running-on-Windows).
