# Command-Line Reference

This page documents the `yacron2` command and every argument it accepts, the
runtime model (foreground execution, signal handling, exit codes), and common
invocations. Behavior is taken from `yacron2/__main__.py`.

## Synopsis

```
yacron2 [-c FILE-OR-DIR] [-l LOG_LEVEL] [-v] [--version]
```

`yacron2` runs as a single foreground process. It does not daemonize, does not
fork, and does not write a PID file. Diagnostics go to stdout/stderr via the
standard library `logging` module. To run it as a service, place it under a
process supervisor (systemd, a container runtime, etc.); see
[Production and Container Deployment](Production-Deployment).

## Arguments

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `-c`, `--config` | path (file or directory) | platform default[^cfgdefault] | Configuration file, or a directory containing configuration files. When a directory, every `*.yml`/`*.yaml` file in it is loaded (entries whose name starts with `_` or `.` are skipped). See [Includes, Defaults, and Multi-File Config](Includes-and-Defaults). |
| `-l`, `--log-level` | string | `INFO` | Root log level. Passed to `logging.basicConfig(level=getattr(logging, LOG_LEVEL))`, so the value must name an attribute of the `logging` module (e.g. `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`). |
| `-v`, `--validate-config` | flag | off | Parse and validate the configuration, then exit. Exits `0` if valid, `1` on a configuration error. Does not start the scheduler or web server. |
| `--version` | flag | off | Print the yacron2 version to stdout and exit `0`. |
| `-h`, `--help` | flag | — | Print usage (argparse builtin) and exit `0`. |

There are no other arguments. Job schedules, commands, environment, reporting,
and the web API are configured entirely in YAML, not on the command line; see
the [Configuration Reference](Configuration-Reference).

[^cfgdefault]: The default config path is platform-specific (`DEFAULT_CONFIG_PATH`
    in `yacron2/platform.py`): `/etc/yacron2.d` on POSIX, and `%APPDATA%\yacron2`
    (e.g. `C:\Users\<you>\AppData\Roaming\yacron2`, falling back to the user
    profile `~` if `APPDATA` is unset) on Windows. See
    [Running on Windows](Running-on-Windows).

### `-c` / `--config`

The argument may be a single YAML file or a directory:

- **File:** parsed directly. An I/O error (for example, the file does not exist)
  is reported as a configuration error and exits `1`.
- **Directory:** each non-hidden `*.yml`/`*.yaml` entry is parsed in
  name-sorted order. An empty directory (or one whose files are all skipped)
  yields an empty configuration with no jobs rather than an error.

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

### `--version`

Prints the version string (e.g. `1.0.13`) to stdout and exits `0`. This check
runs before the config is touched, so `--version` succeeds even when no
configuration exists.

## Runtime model

When started normally (no `--version`, no `--validate-config`, with a usable
config), yacron2:

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
process with `SIGKILL` (POSIX-only; there is no Windows equivalent — use Task
Manager or `taskkill /F` there).

On Windows, press Ctrl-C or Ctrl-Break (`SIGINT`/`SIGBREAK`) to trigger the same
graceful shutdown: it finishes the currently-running jobs first, exactly as
`SIGTERM` does on POSIX. The wiring differs only internally — `signal.signal`
plus a heartbeat timer, because the Proactor loop lacks `add_signal_handler`.
See [Running on Windows](Running-on-Windows).

### Exit codes

| Code | Condition |
| --- | --- |
| `0` | `--version` printed; `--validate-config` succeeded; `--help`; or normal shutdown after a signal. |
| `1` | Configuration error (parse/schema/validation failure or unreadable config); or the default `-c` path (platform-specific: `/etc/yacron2.d` on POSIX, `%APPDATA%\yacron2` on Windows) does not exist and no `-c` was given. |

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

For installation and packaging details (pip, PyInstaller binary, Docker), see
[Installation](Installation). For deploying yacron2 as a long-running service,
see [Production and Container Deployment](Production-Deployment). For
Windows-specific CLI behavior (default config path, default shell, Ctrl-C /
Ctrl-Break shutdown), see [Running on Windows](Running-on-Windows).
