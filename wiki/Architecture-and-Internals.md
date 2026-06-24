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
| `yacron2/job.py` | `RunningJob` lifecycle (subprocess launch, privilege drop, wait, stream capture), `StreamReader`, the `Reporter` implementations (`SentryReporter`, `MailReporter`, `ShellReporter`), and `JobRetryState`. |
| `yacron2/statsd.py` | `StatsdJobMetricWriter` and the UDP `StatsdClientProtocol` used to emit best-effort statsd metrics. |
| `yacron2/version.py` | Generated version string (`version`), served by the web `/version` endpoint and printed by `--version`. |

The dependency direction is `__main__` -> `cron` -> (`config`, `job`) ->
(`statsd`, `config`). `config.py` has no dependency on `cron.py` or `job.py`.
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

## `Cron.run` — the scheduler main loop

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

2. **Web app start/stop.** `await self.start_stop_web_app(config.web_config)`
   reconciles the running aiohttp server against the (possibly changed) web
   config. (See "Web control app".)

3. **Logging config.** If the reloaded config has a non-`None`
   `logging_config` that differs from `applied_logging_config`,
   `logging.config.dictConfig(...)` is applied. It is recorded as applied only
   on success, so a logging section that was broken on a previous reload is
   re-tried and picked up once fixed, without a restart. A failure logs an
   error pointing at the Python `logging.config` dictionary-schema docs and the
   offending config.

4. **Spawn due jobs.** `await self.spawn_jobs(startup)`.

5. **Sleep to the next minute boundary.** `next_sleep_interval()` computes the
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
`launch_scheduled_job(job)` for every job where `job_should_run(startup, job)`
is true. `job_should_run`:

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
2. `await self._wait_for_running_jobs_task` — awaits the reaper, which only
   returns once `self.running_jobs` is empty and the stop event is set.
3. If a web server is running, logs `"Stopping http server"` and
   `await self.web_runner.cleanup()`.

`signal_shutdown` simply calls `self._stop_event.set()`; it is invoked from the
platform shutdown handler (`SIGINT`/`SIGTERM` on POSIX, Ctrl-C/Ctrl-Break on
Windows — see "The event loop"). Note that `handle_job_failure` early-returns
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

## The reaper task — `_wait_for_running_jobs`

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
   POSIX-only — there is no setuid/setgid model on Windows; on Windows
   `user`/`group` config is rejected up front with a configuration error (see
   `start()` above), so `_demote` is never reached.

3. **`wait()`** is what the reaper awaits. If there is no process but
   `start_failed` is set, it synthesizes `retcode = 127` (conventional
   "command not found"), reads streams, and returns — so a failed launch is a
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
   `REPORTERS` (`SentryReporter`, `MailReporter`, `ShellReporter`) concurrently
   with `asyncio.gather(..., return_exceptions=True)`; an exception from any one
   reporter is logged and does not stop the others. Each reporter reads the
   relevant sub-key of the report config (`onFailure["report"]`,
   `onPermanentFailure["report"]`, or `onSuccess["report"]`) and self-disables
   when its required fields are unset (e.g. mail with no `to`/`from`, sentry
   with no resolvable `dsn`, shell with no `command`). Templates render against
   `template_vars`. See [Reporting (Mail, Sentry, Shell)](Reporting).

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
task and returns the assembled text — top lines, an optional
`"[.... N lines discarded ...]"` marker, then bottom lines — plus the discard
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

- **`Allow`** — launch another instance (the list grows).
- **`Forbid`** — return without launching.
- **`Replace`** — for each currently running instance, set
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

## Concurrency model summary

- One process, one thread, one event loop.
- One long-lived scheduler coroutine (`Cron.run`) and one long-lived reaper
  coroutine (`_wait_for_running_jobs`).
- One short-lived `wait()` task per running job, plus one
  `schedule_retry_job` task per armed-and-pending retry.
- Reporters run concurrently per job but failures are isolated.
- Shutdown is cooperative: a signal sets `_stop_event`, the scheduler stops
  spawning, pending retries are cancelled, and the reaper drains in-flight jobs
  before the process exits. The trigger is platform-specific —
  `SIGINT`/`SIGTERM` on POSIX vs Ctrl-C/Ctrl-Break (`SIGINT`/`SIGBREAK`) on
  Windows, both routed through `platform.install_shutdown_handlers` into
  `signal_shutdown` -> `_stop_event.set()` — but the finish-running-jobs-first
  behavior is identical on every OS.

For the broader operational picture see
[Production and Container Deployment](Production-Deployment); for the CLI surface
see [Command-Line Reference](CLI-Reference).
