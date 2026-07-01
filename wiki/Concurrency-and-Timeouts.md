# Concurrency and Timeouts

This page documents how yacron2 handles overlapping runs of the same job
(`concurrencyPolicy`) and how it bounds the duration of a single run
(`executionTimeout`, `killTimeout`). These options are per-job (settable in
`defaults`) and govern only one launch of a job; they have no effect across
different jobs.

## Overview

A job is identified by its `name`. yacron2 tracks, per name, a list of
currently-running instances. When a scheduled time arrives (or a manual start
is requested through the [HTTP Control API](HTTP-API)), yacron2 checks whether
any instance of that job is already running and consults `concurrencyPolicy`
before launching a new one. Independently, each running instance carries a
deadline derived from `executionTimeout`; on expiry it is cancelled, and
`killTimeout` controls the SIGTERM-then-SIGKILL escalation used during any
cancellation.

The SIGTERM-then-SIGKILL escalation is the POSIX behavior. On Windows there
are no POSIX signals, so both steps call `TerminateProcess` (an immediate,
ungraceful stop); `killTimeout` still bounds the wait, but the
terminate-then-kill escalation is effectively moot because the outcome is the
same hard kill. See [Running on Windows](Running-on-Windows).

## Option summary

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `concurrencyPolicy` | enum: `Allow`, `Forbid`, `Replace` | `Allow` | Behavior when a launch is requested while another instance of the same job is still running. |
| `executionTimeout` | float (seconds, `> 0` when set) | none (`null`) | Maximum wall-clock duration of a single run. On expiry the run is cancelled and assigned return code `-100`. |
| `killTimeout` | float (seconds, `>= 0`) | `30` | When a run is cancelled, seconds to wait after SIGTERM before sending SIGKILL (POSIX); on Windows both calls map to `TerminateProcess`, so `killTimeout` only bounds the wait before the same hard kill. See [Running on Windows](Running-on-Windows). |

Types are from the strictyaml schema (`concurrencyPolicy` is
`Enum(["Allow", "Forbid", "Replace"])`; `executionTimeout` and `killTimeout`
are `Float()`). Defaults are from `DEFAULT_CONFIG`. All three options are
optional (`Opt(...)` in the schema). Numeric ranges are enforced after parsing:
`killTimeout >= 0` and, when set, `executionTimeout > 0`; a violating value
raises a `ConfigError` at config load.

See the [Configuration Reference](Configuration-Reference) for where these
options sit in the document and how `defaults` apply.

## Concurrency policy

When `maybe_launch_job` is asked to start a job and one or more instances of
that name are already running, it logs a warning
(`Job <name>: still running and concurrencyPolicy is <policy>`) and then acts
according to `concurrencyPolicy`:

### Allow (default)

The new instance is started immediately alongside the existing one(s). Multiple
instances of the same job can run concurrently with no bound on their number.
Each instance is tracked and reaped independently.

```yaml
jobs:
  - name: ingest
    command: ./ingest.sh
    schedule: "* * * * *"
    concurrencyPolicy: Allow
```

### Forbid

If any instance is still running, the new launch is skipped entirely; no new
process is started. The already-running instance continues unaffected. This
applies equally to scheduled launches and to retry-triggered launches.

```yaml
jobs:
  - name: ingest
    command: ./ingest.sh
    schedule: "* * * * *"
    concurrencyPolicy: Forbid
```

### Replace

Every currently-running instance of the job is cancelled, then a new instance
is started. Before cancelling, the scheduler sets `replaced = True` on each
outgoing instance. This flag changes how the finished run is reaped:

- The replaced run is **not** treated as a failure: `_handle_finished_job`
  returns early when `replaced` is set, logging
  `Job <name> was replaced by a newer instance`.
- Because it is not a failure, it is **not reported** (no Mail/Sentry/Shell
  reporters fire for it) and it does **not** trigger
  [retries](Failure-Detection-and-Retries). `cancel()` itself does not set a
  return code, so whatever value the run's own `wait()` task happened to record
  (the signal-derived code, or `-100` had its own `executionTimeout` expired
  first) is irrelevant: the reaper short-circuits on `replaced` before
  inspecting it.

Cancellation of the outgoing instance uses the same SIGTERM/`killTimeout`/SIGKILL
escalation described under [Cancellation and killTimeout](#cancellation-and-killtimeout).
`maybe_launch_job` awaits each `cancel()` before starting the replacement, so
the new instance is launched only after the old one has terminated.

```yaml
jobs:
  - name: sync
    command: ./sync.sh
    schedule: "* * * * *"
    concurrencyPolicy: Replace
    killTimeout: 10
```

## Execution timeout

`executionTimeout` bounds the wall-clock duration of a single run. It is unset
by default (`null`), meaning a run may take arbitrarily long.

### Deadline mechanism

When a run starts, if `executionTimeout` is set, `RunningJob.start` records an
absolute deadline using a monotonic clock:

```
execution_deadline = time.perf_counter() + executionTimeout
```

`time.perf_counter()` is used (not wall-clock time), so the deadline is immune
to system clock adjustments while the job runs.

When the run is awaited (`RunningJob.wait`):

- If no deadline is set, yacron2 waits indefinitely for the process to exit.
- If a deadline is set, the remaining time is computed as
  `execution_deadline - time.perf_counter()`. If that remaining time is `> 0`,
  the process exit is awaited under `asyncio.wait_for(..., timeout)`; if it is
  already `<= 0`, the timeout path is taken immediately.

On timeout (the remaining time elapses, or was non-positive), yacron2:

1. Logs `Job <name> exceeded its executionTimeout of <N> seconds, cancelling
   it...`.
2. Sets the run's return code to `-100`.
3. Calls `cancel()` to terminate the process (see below).

A `-100` return code is therefore the marker of a timeout-induced termination.
For a normal (non-replaced) run, `retcode = -100` is non-zero, so a job with
the default `failsWhen.nonzeroReturn` treats the timeout as a failure, which is
then reported and may be retried. See
[Failure Detection and Retries](Failure-Detection-and-Retries) for what happens
after a timeout-induced failure. (When the timed-out run was a `Replace`
victim, the `replaced` flag suppresses failure handling regardless of the
`-100` code.)

```yaml
jobs:
  - name: maybe-hangs
    command: |
      echo "starting..."
      sleep 2
      echo "all done."
    schedule:
      minute: "*"
    captureStderr: true
    executionTimeout: 1   # seconds; cancel the run if still alive after 1s
```

## Cancellation and killTimeout

Cancellation (`RunningJob.cancel`) is invoked both by an `executionTimeout`
expiry and by `concurrencyPolicy: Replace`. The sequence is:

1. If the process is still running (`returncode is None`), send SIGTERM via
   `proc.terminate()`. A `ProcessLookupError` (process already gone) is
   ignored.
2. Wait up to `killTimeout` seconds for the process to exit, using
   `asyncio.wait_for(proc.wait(), killTimeout)`.
3. If it has not exited by then, log `Job <name> did not gracefully terminate
   after <N> seconds, killing it...` and send SIGKILL via `proc.kill()`.

`proc.terminate()` = SIGTERM and `proc.kill()` = SIGKILL only on POSIX (a real
escalation; a child can trap SIGTERM to clean up). On Windows both
`terminate()` and `kill()` call `TerminateProcess`, an immediate ungraceful
stop in which the child is *not* notified to clean up, so the escalation is
effectively moot: `killTimeout` still bounds the wait, but the result is the
same hard kill. See [Running on Windows](Running-on-Windows).

`killTimeout` defaults to `30` seconds and must be `>= 0`. A value of `0` is
valid and means SIGKILL is sent almost immediately after SIGTERM (the
`asyncio.wait_for` with a zero timeout gives the process essentially no grace
period). The SIGTERM/SIGKILL escalation is POSIX-specific: on Windows both
`terminate()` and `kill()` map to `TerminateProcess`, an immediate hard kill in
which the child is not notified, so the escalation is moot: `killTimeout` still
bounds the wait, but the outcome is the same hard kill.

`killTimeout` gives a job time to flush buffers and clean up after being asked
to stop; raise it for jobs that need longer to shut down, lower it for jobs
that may ignore SIGTERM and must be force-killed quickly. This grace and the
"ignore SIGTERM" guidance apply only on POSIX; on Windows `TerminateProcess`
gives the child no chance to flush or clean up and a job cannot trap or ignore
the stop, so `killTimeout` effectively only delays the (identical) hard kill.
See [Running on Windows](Running-on-Windows).

```yaml
jobs:
  - name: ignores-sigterm
    command: |
      trap "echo '(ignoring SIGTERM)'" TERM
      echo "starting..."
      sleep 10
      echo "all done."
    schedule:
      minute: "*"
    captureStderr: true
    executionTimeout: 1
    killTimeout: 0.5   # SIGKILL 0.5s after the (ignored) SIGTERM
```

This example demonstrates POSIX-only behavior (a shell trapping SIGTERM). On
Windows there is no signal to trap; the job would be hard-killed via
`TerminateProcess` regardless, so the trap and the SIGTERM/SIGKILL timing it
illustrates do not apply. See [Running on Windows](Running-on-Windows).

## Scope and interaction

- **Per run.** `executionTimeout` and `killTimeout` apply to a single instance
  of a job. The deadline is established at that instance's `start` and is not
  shared across instances. With `concurrencyPolicy: Allow`, each concurrent
  instance has its own independent deadline.
- **Replace + timeout.** A `Replace` victim is cancelled regardless of its own
  `executionTimeout`; its termination is governed by `killTimeout` and is not
  reported as a failure (the `replaced` flag).
- **Manual starts.** Launches via the [HTTP Control API](HTTP-API)
  (`POST /jobs/{name}/start`) go through the same `maybe_launch_job` path and
  thus honor `concurrencyPolicy`.
- **start-up failures vs. timeouts.** A `-100` return code specifically denotes
  a timeout-induced cancellation. A command that could not be launched at all
  (e.g. not found) is assigned `127` instead, on the normal failure path; see
  [Commands and Environment](Commands-and-Environment) and
  [Failure Detection and Retries](Failure-Detection-and-Retries).
