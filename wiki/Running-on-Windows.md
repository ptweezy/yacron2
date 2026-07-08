# Running on Windows

cronstable runs natively on Windows, alongside Linux and macOS. This page is the
canonical reference for the handful of behaviors that differ on Windows: how
to install it, where it looks for configuration, how a string `command` is fed
to a shell, how to stop the daemon, how a job is terminated, the two
POSIX-only features that are reported (never silently dropped) on Windows, and
what file-lock coordination over a shared mount can and cannot verify.
Everything not listed here behaves exactly as it does on POSIX, so the rest of
this wiki applies unchanged.

All of the OS-specific behavior is isolated in a single module
(`cronstable/platform.py`); the scheduler, job runner, config loader, and entry
point read the same on every platform.

## Supported platforms and architectures

cronstable supports Windows on two CPU architectures: `amd64` (x64) and `arm64`
(ARM64). You can install it either as a normal Python package or as a
self-contained executable.

| Architecture | pip / pipx | Standalone binary |
| --- | --- | --- |
| `amd64` (x64) | `pip install cronstable` | `cronstable-windows-amd64.exe` |
| `arm64` (ARM64) | `pip install cronstable` | `cronstable-windows-arm64.exe` |

The full test suite runs on Windows (both x64 and ARM64) in CI on every commit,
and every release builds both Windows binaries. See
[Contributing and Releasing](Contributing-and-Releasing) for the build and
release workflow.

## Installation

There are two ways to install cronstable on Windows.

### pip / pipx

`pip install cronstable` works on Windows just as it does on POSIX, installing the
`cronstable` console script into your environment. A supported Python (3.10 or
newer) must be present. See [Installation](Installation) for the Python and
dependency requirements that apply on every platform.

```shell
pip install cronstable
cronstable --version
```

### Standalone binary (no Python required)

Every release attaches self-contained executables
(`cronstable-windows-amd64.exe` (x64) and `cronstable-windows-arm64.exe` (ARM64)) on
the [releases page](https://github.com/ptweezy/cronstable/releases). Python is
**not** required on the target system; the interpreter is embedded in the
executable. Download the asset for your architecture, then run it:

```shell
cronstable-windows-amd64.exe --version
```

The binaries are built natively on Windows runners (the ARM64 binary on a
`windows-11-arm` runner). As on every platform, the standalone binary is a
self-extracting executable; for the writable-and-executable temp-directory
detail (which matters only under unusual locked-down filesystems) see
[Installation](Installation).

There is no Windows container image; the published Docker image is Linux-only.
See [Installation](Installation) for the Linux image and its supported
architectures.

## Default configuration location

When `-c`/`--config` is omitted, the directory cronstable looks in is
platform-specific:

| Platform | Default `-c` path |
| --- | --- |
| POSIX | `/etc/cronstable.d` |
| Windows | `%APPDATA%\cronstable` (e.g. `C:\Users\<you>\AppData\Roaming\cronstable`) |

On Windows the default is `%APPDATA%\cronstable`, the Windows analog of
`/etc/cronstable.d`. If `APPDATA` is somehow unset (rare, for example a bare
service account with no roaming profile), cronstable falls back to the user
profile directory (`~`, i.e. `os.path.expanduser("~")`) and uses
`<profile>\cronstable`.

You can point `-c` anywhere (a single YAML file or a directory of `*.yaml` /
`*.yml` files) exactly as on POSIX:

```shell
cronstable -c C:\path\to\cronstable.yaml
```

### "Configuration file not found" applies to this path

cronstable has a special-case exit for a missing **default** config path: when the
`-c` argument is left at the platform default and that path does not exist,
cronstable prints the following to stderr, prints the usage help, and exits `1`:

```text
cronstable error: configuration file not found, please provide one with the --config option
```

This check keys off the **platform default value**, not the literal string
`/etc/cronstable.d`. On Windows it therefore fires when `-c` resolves to
`%APPDATA%\cronstable` (whether you omit `-c` or pass that path explicitly) and
the directory does not exist. For any *other* non-existent path you pass with
`-c`, you instead get the generic configuration-error path (a logged
`Configuration error: ...` and exit `1`). See the
[Command-Line Reference](CLI-Reference) for the full argument and exit-code
reference, and [Troubleshooting and FAQ](Troubleshooting) for the
problem/cause/fix entry.

## Default shell and running commands

How a string `command` is handed to a shell is platform-specific. The `shell`
field itself works on every OS; only its default differs:

| Platform | Default `shell` | A string `command` runs as |
| --- | --- | --- |
| POSIX | `/bin/sh` | `["/bin/sh", "-c", command]` |
| Windows | empty | `command` through the native command processor `%ComSpec%` (cmd.exe) |

On Windows the default `shell` is empty. An empty `shell` routes a string
`command` through the native command processor (`%ComSpec%`, i.e. `cmd.exe`)
via `asyncio.create_subprocess_shell`, the closest equivalent to the POSIX
`/bin/sh -c` path. A bare string command therefore runs under `cmd.exe` by
default:

```yaml
jobs:
  - name: hello
    command: echo Hello from cmd.exe
    schedule: "*/5 * * * *"
    captureStdout: true
```

### Using PowerShell or another interpreter

To run a command under PowerShell, or any interpreter other than `cmd.exe`, you
have two options. Set `shell:` explicitly:

```yaml
jobs:
  - name: powershell-shell
    command: Get-Date
    shell: powershell
    schedule: "*/5 * * * *"
    captureStdout: true
```

…or pass `command` as a **list**, which bypasses the shell entirely on every
platform (the argv is taken verbatim: no word splitting, globbing, quoting, or
variable expansion is performed):

```yaml
jobs:
  - name: powershell-list
    command:
      - powershell
      - -Command
      - Get-Date
    schedule: "*/5 * * * *"
    captureStdout: true
```

For the full shell-vs.-list semantics (including how `defaults.shell` is
inherited and how launch failures are handled), see
[Commands and Environment](Commands-and-Environment).

## Graceful shutdown

To stop cronstable on Windows, press `Ctrl-C` (or `Ctrl-Break`). As on POSIX, this
is a *graceful* shutdown: cronstable stops scheduling new runs and finishes the
currently running jobs first, exactly as `SIGTERM` does on POSIX. It does not
force-kill its own running jobs on shutdown.

Internally, POSIX wires `SIGINT`/`SIGTERM` onto the asyncio event loop. The
Windows Proactor loop has no `add_signal_handler`, so on Windows cronstable instead
installs `signal.signal` handlers for `SIGINT` (Ctrl-C) and `SIGBREAK`
(Ctrl-Break / console close) and runs a lightweight heartbeat timer so the
interpreter observes the pending handler promptly even while the loop is blocked
in I/O. The user-visible behavior is identical to POSIX. For the shutdown
sequence in detail, see
[Signal handling and graceful shutdown](CLI-Reference#signal-handling-and-graceful-shutdown)
in the [Command-Line Reference](CLI-Reference).

## Job termination semantics

When cronstable stops a job (because its `executionTimeout` expired, because of
`concurrencyPolicy: Replace`, or because of a cancel request through the
[HTTP Control API](HTTP-API)) it calls `proc.terminate()`, waits up to
`killTimeout` seconds, then escalates to `proc.kill()`. The meaning of those two
calls differs by platform:

| Platform | `terminate()` | `kill()` | Escalation |
| --- | --- | --- | --- |
| POSIX | `SIGTERM` (graceful, trappable) | `SIGKILL` (forceful) | Real: a child can trap `SIGTERM` to clean up before `SIGKILL`. |
| Windows | `TerminateProcess` | `TerminateProcess` | Moot: both calls are the same immediate, ungraceful stop. |

On Windows there are no POSIX signals: both `terminate()` and `kill()` map to
`TerminateProcess`, an immediate, ungraceful stop. The child is **not** notified
to clean up, so the `terminate()` → `kill()` escalation is effectively moot.
`killTimeout` still bounds how long cronstable waits between the two calls, but the
outcome is the same hard kill either way. A job cannot trap a "please shut down"
signal on Windows the way it can trap `SIGTERM` on POSIX.

For the full cancellation sequence, the `-100` timeout return code, and how
`concurrencyPolicy: Replace` cancels the outgoing instance, see
[Concurrency and Timeouts](Concurrency-and-Timeouts).

## Features not supported on Windows

Two POSIX-specific features cannot work on Windows. Neither is silently dropped:
each is reported clearly.

### Per-job `user` / `group` switching

Windows has no `setuid`/`setgid` model, so a job cannot drop to another user or
group. A job with `user` or `group` set raises a configuration error at config
load, verbatim:

```text
Job <name>: changing user/group is not supported on Windows
```

Remove the `user`/`group` fields from the job to run it on Windows. For the
POSIX semantics of these fields (resolution rules, the root requirement, and the
demotion ordering), see
[Commands and Environment](Commands-and-Environment).

### `unix://` web listeners

aiohttp's `UnixSite` needs `create_unix_server`, which the Windows Proactor
event loop does not provide, so `unix://` web listeners cannot be bound. Such a
`web.listen` URL is skipped (not fatal) with a warning, verbatim:

```text
Ignoring web listen url <url>: unix-socket listeners are not supported on this platform
```

Use an `http://` listener instead; it (and the entire HTTP control API and
[Web Dashboard](Web-Dashboard)) behaves identically on Windows:

```yaml
web:
  listen:
    - http://127.0.0.1:8080
```

Because `web.socketMode` only ever applies to `unix://` sockets, it is
irrelevant on Windows. See the [HTTP Control API](HTTP-API) for the listener
configuration and [Web Dashboard](Web-Dashboard) for the browser UI.

Note that this limitation is specific to `unix://` **web** listeners. Gossip
clustering (the mTLS peer listener) does work on Windows: `cluster.listen` binds
a TCP `host:port`, not a unix socket, so the Proactor unix-socket restriction
does not apply. See [Clustering and Leader Election](Clustering-and-Leader-Election).

## Shared-mount coordination

The [durable state store](Durable-State) and the `filesystem` leadership
backend ([Clustering and Leader Election](Clustering-and-Leader-Election))
coordinate through advisory file locks. On Windows the lock primitive is an
`msvcrt.locking` byte-range lock rather than the POSIX `fcntl.flock`; between
processes on the *same* Windows host it excludes exactly as on POSIX, so
leases, leader election, and `concurrencyScope: cluster` slots all work fully
when the coordinating processes share one machine. What Windows cannot do is
*verify* cross-host reach: there is no `/proc/mounts` to probe, so
`topology: auto` resolves to `single-node`, and the lock-fidelity probe
(which runs on one host) cannot detect a mount whose locks are real locally
but never reach the file server.

Left at `topology: auto`, the state store logs an info line telling you to
set `state.topology: shared`, and the filesystem election backend warns at
startup that its locks only exclude local processes, verbatim (`<path>`
filled in):

```text
cluster: the filesystem election store at <path> resolved topology 'single-node', so its locks only exclude processes on THIS host (Windows/macOS cannot probe the mount); if the directory really is a shared network mount, set cluster.filesystem.topology: shared
```

Coordinating *across* Windows hosts over a shared mount therefore requires
both an explicit assertion (`state.topology: shared` and/or
`cluster.filesystem.topology: shared`) **and** a mount that truly honours
byte-range locks across hosts. cronstable cannot check the second half on
Windows, so with `topology: shared` asserted the election still logs a loud
startup advisory, verbatim, and the residual risk rests on your assertion:

```text
cluster: filesystem election on a Windows shared mount: cross-host lock fidelity cannot be verified on this platform (no /proc/mounts); the election is safe only if the mount honours byte-range locks across hosts
```

The same limit applies to `concurrencyScope: cluster`: over a mount that does
not propagate locks between hosts, a "cluster-wide" claim only guards
processes on the same host. Asserting `shared` over a mount that fakes its
locks is how you get two leaders or overlapping `Forbid` runs, so verify the
mount's lock semantics before trusting it. See [Durable State](Durable-State)
and [Clustering and Leader Election](Clustering-and-Leader-Election) for the
full coordination semantics and guarantees.

One neighbouring durable-state mechanism needs no caveat: crash
reconciliation's same-host pid-liveness check (an in-flight run left open by
a previous daemon is not declared dead while its recorded pid still exists,
because a daemon crash does not kill the job processes it spawned) works
fully on Windows, via `OpenProcess` in place of the POSIX `kill(pid, 0)`
probe.

## Everything else behaves identically

Apart from the differences above, cronstable behaves the same on Windows as on
POSIX. The YAML crontab, schedules and timezones, environment variables and env
files, output capturing, concurrency, failure detection and retries, reporting
(mail / Sentry / shell / webhook), statsd metrics, the Prometheus `/metrics` endpoint,
the HTTP control API, and the web dashboard all work exactly as documented
elsewhere in this wiki:

- [Schedules and Timezones](Schedules-and-Timezones)
- [Commands and Environment](Commands-and-Environment)
- [Output Capturing](Output-Capturing)
- [Concurrency and Timeouts](Concurrency-and-Timeouts)
- [Failure Detection and Retries](Failure-Detection-and-Retries)
- [Reporting (Mail, Sentry, Shell, Webhook)](Reporting)
- [Metrics with statsd](Metrics-with-Statsd) and
  [Metrics with Prometheus](Metrics-with-Prometheus)
- [HTTP Control API](HTTP-API) and [Web Dashboard](Web-Dashboard)

See [Installation](Installation) and the
[Command-Line Reference](CLI-Reference) to get started, and
[Troubleshooting and FAQ](Troubleshooting) if something does not behave as
expected.
