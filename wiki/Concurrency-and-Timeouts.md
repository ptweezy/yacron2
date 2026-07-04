# Concurrency and Timeouts

This page documents how yacron2 handles overlapping runs of the same job
(`concurrencyPolicy`, and how far that policy reaches: `concurrencyScope`)
and how it bounds the duration of a single run (`executionTimeout`,
`killTimeout`). These options are per-job (settable in `defaults`) and govern
only one launch of a job; they have no effect across different jobs. By
default they also reach no further than one daemon process;
`concurrencyScope` can widen `Forbid` and `Replace` to a whole fleet sharing
a [durable state store](Durable-State).

**On this page:**
[Overview](#overview) ·
[Option summary](#option-summary) ·
[Concurrency policy](#concurrency-policy) ·
[Concurrency across a cluster](#concurrency-across-a-cluster) ·
[Execution timeout](#execution-timeout) ·
[Cancellation and killTimeout](#cancellation-and-killtimeout) ·
[Scope and interaction](#scope-and-interaction)

## Overview

A job is identified by its `name`. yacron2 tracks, per name, a list of
currently-running instances. When a scheduled time arrives (or a manual start
is requested through the [HTTP Control API](HTTP-API)), yacron2 checks whether
any instance of that job is already running and consults `concurrencyPolicy`
before launching a new one. That tracking is local to one daemon process: by
default (`concurrencyScope: node`) the local list is the whole story, while
`concurrencyScope: cluster` makes `Forbid` and `Replace` additionally consult
a slot lease in the shared [state store](Durable-State), extending their
reach to instances on other nodes (see
[Concurrency across a cluster](#concurrency-across-a-cluster); the local
check always runs first, and the cluster gate is additive). Independently,
each running instance carries a deadline derived from `executionTimeout`; on
expiry it is cancelled, and `killTimeout` controls the
SIGTERM-then-SIGKILL escalation used during any cancellation.

The SIGTERM-then-SIGKILL escalation is the POSIX behavior. On Windows there
are no POSIX signals, so both steps call `TerminateProcess` (an immediate,
ungraceful stop); `killTimeout` still bounds the wait, but the
terminate-then-kill escalation is effectively moot because the outcome is the
same hard kill. See [Running on Windows](Running-on-Windows).

## Option summary

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `concurrencyPolicy` | enum: `Allow`, `Forbid`, `Replace` | `Allow` | Behavior when a launch is requested while another instance of the same job is still running. |
| `concurrencyScope` | enum: `node`, `cluster` | `node` | How far `concurrencyPolicy` reaches: `node` considers only this process's running instances; `cluster` makes `Forbid`/`Replace` also exclude instances on other nodes sharing the [`state` store](Durable-State). See [Concurrency across a cluster](#concurrency-across-a-cluster). |
| `executionTimeout` | float (seconds, `> 0` when set) | none (`null`) | Maximum wall-clock duration of a single run. On expiry the run is cancelled and assigned return code `-100`. |
| `killTimeout` | float (seconds, `>= 0`) | `30` | When a run is cancelled, seconds to wait after SIGTERM before sending SIGKILL (POSIX); on Windows both calls map to `TerminateProcess`, so `killTimeout` only bounds the wait before the same hard kill. See [Running on Windows](Running-on-Windows). |

Types are from the strictyaml schema (`concurrencyPolicy` is
`Enum(["Allow", "Forbid", "Replace"])`, `concurrencyScope` is
`Enum(["node", "cluster"])`; `executionTimeout` and `killTimeout`
are `Float()`). Defaults are from `DEFAULT_CONFIG`. All four options are
optional (`Opt(...)` in the schema). Numeric ranges are enforced after parsing:
`killTimeout >= 0` and, when set, `executionTimeout > 0`; a violating value
raises a `ConfigError` at config load. Two `concurrencyScope: cluster`
combinations are likewise refused at load rather than left silently inert;
see [Concurrency across a cluster](#concurrency-across-a-cluster).

See the [Configuration Reference](Configuration-Reference) for where these
options sit in the document and how `defaults` apply.

## Concurrency policy

When `maybe_launch_job` is asked to start a job and one or more instances of
that name are already running in this process, it logs a warning
(`Job <name>: still running and concurrencyPolicy is <policy>`) and then acts
according to `concurrencyPolicy`. This local check always runs first; for a
`concurrencyScope: cluster` job, a launch that clears it must then also claim
the job's cluster slot (see
[Concurrency across a cluster](#concurrency-across-a-cluster)).

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
applies equally to scheduled launches and to retry-triggered launches. The
instances considered are this process's own; with `concurrencyScope: cluster`
an instance running on another node that shares the state store also forbids
the launch (see [Concurrency across a cluster](#concurrency-across-a-cluster)).

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
- Because it is not a failure, it is **not reported** (no Mail/Sentry/Shell/Webhook
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

This inline cancel-then-launch applies to instances running in this process.
With `concurrencyScope: cluster`, an instance on another node is replaced by
asking its node to cancel it, and the replacement launch is deferred until
that node yields; see
[Replace across the cluster](#replace-across-the-cluster).

```yaml
jobs:
  - name: sync
    command: ./sync.sh
    schedule: "* * * * *"
    concurrencyPolicy: Replace
    killTimeout: 10
```

## Concurrency across a cluster

`concurrencyPolicy` on its own reaches only the instances tracked by one
daemon process. A fleet of nodes sharing a [durable state store](Durable-State)
can widen `Forbid` and `Replace` to the whole fleet with the per-job
`concurrencyScope` option (settable in `defaults`; see the
[Configuration Reference](Configuration-Reference)):

```yaml
state:
  path: /mnt/shared/yacron2-state   # the same store on every node
  topology: shared

jobs:
  - name: ingest
    command: ./ingest.sh
    schedule: "* * * * *"
    concurrencyPolicy: Forbid
    concurrencyScope: cluster
```

`concurrencyScope: node` (the default) is the classic behavior described
above: only this process's running instances are considered.
`concurrencyScope: cluster` makes `Forbid` and `Replace` also exclude
instances of the job on other nodes sharing the `state` store. The local
check still runs first and is unchanged; the cluster gate is additive. It
works with or without a `cluster:` section -- the shared store, not leader
election, is what coordinates the nodes.

### Requirements

Two combinations are refused at config load rather than left silently inert:

- `concurrencyScope: cluster` requires a `state` section somewhere in the
  final assembled config (a config directory may keep `state` and jobs in
  different files). Without one, parsing fails with ``concurrencyScope:
  cluster requires a `state` section (the shared store is what coordinates
  the nodes), but none is configured; offending job(s): ...``, naming every
  offending job.
- `concurrencyScope: cluster` with `concurrencyPolicy: Allow` raises
  `Job <name>: concurrencyScope: cluster has no effect with
  concurrencyPolicy: Allow (the default); set Forbid or Replace, or drop
  concurrencyScope`. `Allow` places no bound on concurrent instances, so
  there is nothing for the cluster to gate.

### The slot lease

Every launch of a cluster-scoped job -- scheduled, retry, catch-up backfill,
deferred `@reboot`, or a manual API start -- first claims a TTL **slot lease**
named `slots/<job name>` in the state store (the cron `state` store, not the
leadership store). The claim is a single choke point in `maybe_launch_job`,
and each store operation in it is bounded (10 seconds), so a hung mount
cannot stall the scheduler pass.

- The lease TTL is `state.slotTtlSeconds` (default `30`; a value below 5
  raises `state.slotTtlSeconds must be >= 5`). While the job runs here, the
  holder renews the lease every third of the TTL, so a node that crashes
  mid-run stops renewing and its slot frees itself after at most one TTL.
- Lease operations bypass the `state.maxOpsPerSecond` token bucket: a renew
  queued behind bulk writes could overshoot its TTL and double-run the very
  job the lease fences.
- The slot is released when the job's **last** local instance finishes
  (claims are refcounted, so overlapping instances share one lease). A
  release that fails logs `state: failed to release the concurrency slot for
  <name> (...); it frees by TTL` -- TTL expiry is always the fallback.
- The lease is held under a process-unique identity (log messages show the
  node's display name), so a restarted daemon can never adopt its
  predecessor's slot.

Manual starts through the [HTTP Control API](HTTP-API) go through the same
gate, consistent with the local behavior: a manual start was already subject
to a node-local `Forbid`.

### Forbid across the cluster

If the slot is held by a live instance on another node, the launch is
skipped with a warning naming the holder:
`Job <name> skipped: its cluster concurrency slot is held by <node>
(concurrencyPolicy: Forbid, concurrencyScope: cluster)`. Nothing further
happens for that occurrence, exactly like a local `Forbid` skip.

### Replace across the cluster

`Replace` cannot signal a process on another machine, so it asks the holder
to yield instead of killing it directly:

1. The requester appends an immutable **cancel record** to the
   `slots/<job name>` stream, targeted at the holder's exact lease fence, and
   logs `Job <name>: cluster Replace: asking the current slot holder (<node>)
   to yield; the launch is re-attempted when the slot frees`. Fence targeting
   makes a stale request inert: a takeover always bumps the fence.
2. The holder's renew task observes the cancel within about a third of the
   slot TTL and logs `Job <name>: node <host> requested this instance be
   replaced (concurrencyPolicy: Replace, concurrencyScope: cluster);
   cancelling`. Its instances are marked replaced -- the same not-a-failure
   treatment as a local `Replace`: no reports, no retries -- then cancelled,
   and the finish path releases the slot.
3. The requester waits in a **background pursuit task**, never inline on the
   scheduler pass (waiting a holder out takes up to two slot TTLs, which
   would stall every other due job). When the slot frees -- release or TTL
   expiry -- the launch is re-attempted through every normal gate, logging
   `Job <name>: launched after the previous cluster slot holder yielded
   (concurrencyPolicy: Replace)` on success.
4. The pursuit is bounded at **twice the slot TTL**. A holder that never
   yields forfeits this launch: `Job <name>: the foreign holder (<node>) did
   not yield its cluster concurrency slot within <N>s; skipping this launch
   (no-run over double-run)`.

### When the store cannot answer

A store that is down, hung, or whose denied claim cannot even be confirmed
by a follow-up read leaves the gate unanswerable, and
[`state.onStoreUnavailable`](Durable-State#when-the-store-is-unavailable-onstoreunavailable)
decides:

- `degrade` (the default) launches anyway, enforcing `concurrencyPolicy` on
  this node only for that run: `Job <name>: cannot claim its cluster
  concurrency slot (...); enforcing concurrencyPolicy on this node only for
  this run (onStoreUnavailable: degrade)`.
- `fail-closed` skips the launch: `Job <name> skipped: cannot claim its
  cluster concurrency slot (...) and onStoreUnavailable is fail-closed`.

A store whose file locks are demonstrably no-ops (some FUSE filesystems
grant two exclusive locks on one file) is caught by a lock-fidelity probe,
run once per backend and latched; its claims are then treated per
`onStoreUnavailable`, with an error logged once: `state: the store's file
locks cannot be trusted for cluster-wide concurrency (...);
concurrencyScope: cluster claims degrade per onStoreUnavailable`.

### What the cluster gate does and does not guarantee

The contract is **at-least-once**, not exactly-once. The gate closes the
routine overlap windows, but these remain open by design:

- **A holder that loses its slot keeps running.** yacron2 never kills work
  over a store blip. If a store outage outlasts the slot TTL and another
  node takes the slot over, the original holder logs `Job <name>: its
  cluster concurrency slot was taken over by <node> while it is still
  running here (a store outage outlasted the slot TTL?); the run continues
  -- the overlap is the documented at-least-once trade`. A `Forbid` peer
  that then wins the slot overlaps the still-running original.
- **`degrade` trades the gate for availability.** While the store cannot
  answer, only node-local enforcement applies to launches made under
  `onStoreUnavailable: degrade`.
- **Windows locks are same-host only.** On Windows the store's file locks
  have no cross-host reach, so a cluster slot claim there only fences
  daemons on the same host (`topology: auto` resolves to `single-node` on
  Windows and logs an advisory; see
  [One backend, two topologies](Durable-State#one-backend-two-topologies)).
- **Replace can give up.** The pursuit abandons the launch after twice the
  slot TTL rather than risk a double run -- the bias is no-run over
  double-run.
- **Slot expiry compares wall clocks across hosts**, so the shared-mount
  clock discipline in [Durable State](Durable-State) applies: run NTP on
  every node.

Winning a slot whose previous holder went silent also reconciles that
holder's interrupted run into the durable ledger as an `unknown` outcome
(an expired slot proves the holder stopped renewing, not that its process
died); see [Durable State](Durable-State) for the in-flight records behind
that.

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
  thus honor `concurrencyPolicy` -- including, for
  `concurrencyScope: cluster` jobs, the cluster slot gate.
- **Node first, cluster second.** `concurrencyScope: cluster` never changes
  the per-node behavior documented above; the local check runs first, and
  the [cluster gate](#concurrency-across-a-cluster) is an additional gate
  behind it.
- **start-up failures vs. timeouts.** A `-100` return code specifically denotes
  a timeout-induced cancellation. A command that could not be launched at all
  (e.g. not found) is assigned `127` instead, on the normal failure path; see
  [Commands and Environment](Commands-and-Environment) and
  [Failure Detection and Retries](Failure-Detection-and-Retries).
