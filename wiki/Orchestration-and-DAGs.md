# Orchestration and DAGs

cronstable schedules independent jobs. A **DAG** (the optional `dags:` section)
adds the other axis: a durable, dependency-ordered **workflow** of tasks, run
on a schedule, that survives restarts and coordinates across a fleet exactly
the way the rest of [Durable State](Durable-State) does. It is a small
orchestration engine built entirely on the pieces that already exist -- there
is **no new coordination service, no new backend, no client library**:

- a **dag_run** (one execution of a DAG) is a single mutable *document* in the
  [state store](Durable-State), holding every task's state;
- a **task** is an ordinary job invocation -- the same command/shell/env/
  timeout machinery, launched the same way, with the same
  [loopback state endpoint](Durable-State#job-facing-state) injected,
  so a task can call `cronstable xcom|artifact|state|lock|...`;
- **cross-task data** (XCom) rides the artifact store, scoped per dag_run;
- the scheduler advances each run under a single **lease**, so across a fleet a
  task is never launched twice and a run is never double-advanced.

> **Opt-in and store-backed.** DAGs require a `state` section with the loopback
> endpoint (`state.jobApi.enabled`, on by default). Without `dags:` none of
> this exists; adding it changes nothing about plain scheduled jobs.

**On this page:** [A first DAG](#a-first-dag) ·
[Tasks and dependencies](#tasks-and-dependencies) ·
[The task state machine](#the-task-state-machine) ·
[XCom: passing data between tasks](#xcom-passing-data-between-tasks) ·
[Fan-out: dynamic mapping](#fan-out-dynamic-mapping) ·
[Sensors](#sensors) · [Approval gates](#approval-gates) ·
[Scheduling, catch-up, and backfill](#scheduling-catch-up-and-backfill) ·
[Crash-resume and the fleet](#crash-resume-and-the-fleet) ·
[Retention and GC](#retention-and-gc) ·
[Inspecting and controlling runs](#inspecting-and-controlling-runs)

## A first DAG

```yaml
state:
  path: /var/lib/cronstable/state      # DAGs need a state store + jobApi
dags:
  - name: nightly-etl
    schedule: "0 2 * * *"
    tasks:
      - id: extract
        command: "echo '[1,2,3]' | cronstable xcom push --key ids"
      - id: transform
        dependsOn: [extract]
        command: "cronstable xcom pull --task extract --key ids"
      - id: load
        dependsOn: [transform]
        command: "echo loading"
```

At 02:00 the daemon creates a dag_run and advances it: `extract` runs first,
then `transform` (once `extract` succeeds), then `load`. Every transition is
durable, so a restart resumes the run from exactly where it was.

## Tasks and dependencies

Each task has an `id` (unique within the DAG) and, except for an approval gate,
a `command` (the same string-or-list command a job takes). Edges are declared
with `dependsOn:` -- a list of upstream task ids. The graph must be **acyclic**
and every dependency must resolve; a cycle or a dangling edge is a config error
at load, never a runtime hang.

A task's readiness is governed by its `triggerRule`:

| triggerRule | the task runs when… |
| --- | --- |
| `all_success` (default) | every upstream succeeded (an upstream failure makes it `upstream_failed`; an upstream skip cascades a `skipped`) |
| `all_done` | every upstream reached a terminal state, regardless of outcome |

Per-task launch fields mirror a job: `shell`, `environment`, `captureStdout` /
`captureStderr`, `executionTimeout`, `killTimeout`, `user` / `group`,
`failsWhen`, run-scoped `secrets`, `monitorResources`, and the rest of the
shared launch keys; the complete list, with types and defaults, is in the
[configuration reference](Configuration-Reference#dags). Because a task **is**
a job invocation, its launch fields inherit the file's
[`defaults:` block](Includes-and-Defaults#the-defaults-section) the same way a
job does: a global `shell`, `environment`, `monitorResources`, run-scoped
`secrets`, or reporter block covers DAG tasks too, and the task's own value
wins on any key it sets. A task's `onFailure` / `onSuccess` reporters (set
per-task or inherited) fire on each of its runs, every failed attempt
included; per-task the two hooks accept a `report` block only, since a task's
retries come from the node's `retries` field, not a job-level
`onFailure.retry` ladder (an inherited one is ignored for tasks). Only the
**launch** fields inherit; the DAG-node
fields that shape the graph (`dependsOn`, `triggerRule`, `retries`,
`retryDelaySeconds`, `expand`, `onReject`, the poke settings) are never touched
by a `defaults:` block. The DAG's own schedule frame is separate too: the
synthetic trigger job that fires the DAG on schedule stays on the built-in
defaults, so a global `onSuccess`/`onFailure` reporter does not alert on every
DAG tick. A monitored task
instance's sampled CPU time and peak RSS land in the `resources` object of
its task record in the `dag_run` document, and in the task's statsd sink if
one is configured; task instances do not appear in the per-job Prometheus
families. Per-task **retries** are DAG-owned
(independent of a job's `onFailure.retry`):

```yaml
      - id: load
        command: "..."
        retries: 3                # up to 3 retries -> 4 attempts
        retryDelaySeconds: 30     # wait between attempts
```

## The task state machine

Each task instance moves through:

```text
pending ─▶ running ─▶ success
                   ├▶ up_for_retry ─▶ running (after retryDelaySeconds)
                   └▶ failed              (retries exhausted)
pending ─▶ upstream_failed   (an upstream failed, all_success)
pending ─▶ skipped           (an upstream was skipped, all_success)
```

A dag_run is `success` once every task is terminal and none failed; `failed`
if any task ended `failed` or `upstream_failed`. `skipped` is not a failure.

## XCom: passing data between tasks

A task publishes a small output under a key; a downstream task reads it. XCom
is a thin, task-keyed convention over the [artifact store](Durable-State),
scoped to the dag_run, driven by the `cronstable xcom` CLI the daemon makes
reachable in every task:

```bash
# in an upstream task:
echo '{"rows": 42}' | cronstable xcom push --key summary
cronstable xcom push --key summary producer_output_file.json    # or from a file

# in a downstream task:
cronstable xcom pull --task upstream_id --key summary            # -> stdout
cronstable xcom pull --task upstream_id --key summary -o out.json
cronstable xcom list                                            # keys in this run
```

Outputs are content-addressed and versioned (newest wins by key). The daemon
injects the run's identity so the CLI needs no arguments beyond the key:
`CRONSTABLE_DAG_NAME`, `CRONSTABLE_DAG_RUN_ID`, `CRONSTABLE_DAG_TASK`,
`CRONSTABLE_DAG_TASKKEY`, `CRONSTABLE_DAG_MAP_INDEX`, `CRONSTABLE_DAG_MAP_ITEM`,
`CRONSTABLE_DAG_XCOM_SCOPE`.

## Fan-out: dynamic mapping

A task can **expand** into N parallel instances, one per item of an upstream's
XCom list (Airflow's `.expand()`):

```yaml
      - id: list-work
        command: "echo '[\"a\",\"b\",\"c\"]' | cronstable xcom push --key items"
      - id: process
        dependsOn: [list-work]
        expand:
          fromTask: list-work      # a direct, non-mapped dependency
          key: items               # its XCom list
        command: "echo processing $CRONSTABLE_DAG_MAP_ITEM (#$CRONSTABLE_DAG_MAP_INDEX)"
```

When `list-work` succeeds, the scheduler reads its `items` list and materialises
`process#0`, `process#1`, `process#2`, each with its own state, retries and
XCom, and its item in `$CRONSTABLE_DAG_MAP_ITEM`. A downstream task that
`dependsOn: [process]` waits for **all** the mapped instances (fan-in). An
empty list resolves the mapped task to `success` immediately.

The expanded item set is recorded **once** in the dag_run and never recomputed,
so a crash-resumed run reconstructs the identical set of mapped instances
rather than re-deriving it from a possibly-changed upstream output.

Because the expansion is permanent, the read that derives it is **strict**
about store trouble. A store that cannot answer -- an I/O error or timeout on
a shared mount, a record only a newer node's schema can read -- leaves the
fan-out **unknown**: the task stays unexpanded and the scheduler retries the
read on a later pass, regardless of
[`onStoreUnavailable`](Durable-State#when-the-store-is-unavailable-onstoreunavailable)
(this deliberately overrides the store's usual
[skip-on-read-error](Durable-State#the-store-model) rule -- one blip must not
freeze the task into a permanently empty, vacuously successful fan-out). The
empty fan-out is reserved for a *definitive* answer: the upstream finished
without publishing the key; the published value is not a usable JSON list
(invalid JSON, not a list, or carrying a non-portable value); or the record
survives but its payload blob is gone (`410` -- possible only through external
interference with the store, such as a partial restore, since GC never sweeps
a blob a surviving record references). A warning names each mapping-to-empty
that indicates a problem.

A fan-out is capped at **1000 items**: a larger XCom list fails the mapped
task with an explanatory reason instead of materialising the flood (its
`all_success` downstream sees `upstream_failed`). A single scheduler pass
also launches at most 32 instances at a time, so a large fan-out ramps up in
bounded bursts rather than one subprocess stampede.

## Sensors

A `type: sensor` task polls an external condition on a bounded, jittered,
durable schedule instead of running once. Its command's exit code is the
verdict: **0 = condition met** (the task succeeds); non-zero = not yet, poke
again after `pokeIntervalSeconds` (± `pokeJitterSeconds`) until
`pokeTimeoutSeconds` elapses, after which the sensor fails.

```yaml
      - id: wait-for-file
        type: sensor
        command: "test -f /data/$(date +%F).ready"
        pokeIntervalSeconds: 60
        pokeTimeoutSeconds: 7200
        pokeJitterSeconds: 10
```

The poke schedule (`nextPokeAt`, `pokeCount`) is durable, so a restart resumes
polling on time rather than restarting the timeout window.

## Approval gates

A `type: approval` task blocks the graph until a human or an API call decides
it. It runs no command. Approve or reject it over the
[control API](HTTP-API#dag-endpoints) (or the [dashboard](Web-Dashboard)):

```bash
curl -X POST .../dags/nightly-etl/runs/<run_key>/tasks/publish-gate/decision \
     -H 'Content-Type: application/json' \
     -d '{"decision": "approve", "by": "alice"}'
```

`approve` succeeds the gate and the graph proceeds; `reject` fails it (or, with
`onReject: skip`, marks it `skipped`, cascading `skipped` to its `all_success`
downstream). The decision (`by`, timestamp) is recorded durably.

A gate that begins waiting can page you: configure the
[`notify:` block](Reporting#daemon-event-notifications-notify) with the
`approval_waiting` event to have cronstable fire a reporter (webhook, mail, …)
the first time each gate parks awaiting a decision. A whole DAG run reaching
`failed` similarly fires the `dag_failure` event.

## Scheduling, catch-up, and backfill

A scheduled DAG reuses the job [schedule grammar](Schedules-and-Timezones)
with one restriction: the schedule must parse to a cron expression, so
`@reboot` is rejected at config load (`DAG schedules must be cron
expressions; @reboot is not supported for dags`), while `@daily` /
`@hourly`-style aliases still work. It follows the
[catch-up discipline](Durable-State#missed-run-catch-up): `onMissed`
(`skip` / `run-once` / `run-all`) and `startingDeadlineSeconds` bound how many
missed logical dates a restart replays, capped like a job's catch-up. A DAG
with no `schedule` is manual-only.

**Backfill** replays a DAG across a historical range on demand -- a deliberate
operation that ignores the automatic deadline but is still bounded and
idempotent (each date's run is create-if-absent, so re-running a backfill never
duplicates runs):

```bash
curl -X POST .../dags/nightly-etl/backfill \
     -d '{"from": "2026-01-01T00:00:00+00:00", "to": "2026-01-07T00:00:00+00:00"}'
```

## Crash-resume and the fleet

The durable per-task state -- not memory -- is the source of truth. A dag_run
is advanced only by the node holding that run's **advance lease** (a TTL lease
on the shared store, renewed while the run is active), so across a fleet only
one node ever advances a given run and a task never double-launches. The claim
that flips a task `pending → running` is a single atomic compare-and-set on the
run document, a correctness backstop underneath the lease.

If a node crashes, its lease lapses and a peer adopts the run, reconciling from
the durable state: a task recorded `running` whose process is gone (a dead pid,
or a foreign owner proven dead by the lease lapse) is retried if attempts
remain, else failed; a sensor mid-poke is re-poked; an approval gate keeps
waiting. This mirrors the job-level
[crash reconciliation](Durable-State#in-flight-runs-and-crash-reconciliation)
seam. Like every cronstable coordination primitive it is **at-least-once**,
not exactly-once: a task whose process outlives a crashed daemon may run again
on resume, so a task that must be exactly-once should guard its side effect
with an [idempotency key](Durable-State#idempotency-keys).

## Retention and GC

A dag_run document is durable and, while its DAG is configured, is **not**
swept by the record garbage collector. Instead each DAG keeps its newest
`retainRuns` **terminal** runs (default 50) and prunes the rest, along with
their XCom, on a periodic DAG-owned pass. A DAG *removed from every config*
ages out like a removed job: once it has been absent from every config and
recent manifest for a full `state.gcGraceSeconds`, the daemon's
[GC pass](Durable-State#garbage-collection-and-manifests) deletes its
terminal run documents (an active run is never touched, so a re-added DAG
resumes it) and its aged XCom streams. Artifact payload blobs are
content-addressed; a blob any surviving record still references is never
swept, so a retained run's XCom can never dangle -- only blobs no surviving
record references, and older than the grace, are reclaimed.

## Inspecting and controlling runs

Over the [HTTP control API](HTTP-API#dag-endpoints):

- `GET /dags` — the configured DAGs and their tasks
- `GET /dags/{name}/runs` — recent runs and their per-task state counts
- `GET /dags/{name}/runs/{run_key}` — one run's full document
- `POST /dags/{name}/trigger` — start a manual run now
- `POST /dags/{name}/backfill` — replay a date range
- `POST /dags/{name}/runs/{run_key}/tasks/{taskkey}/decision` — approve/reject a gate

The [Web Dashboard](Web-Dashboard) drives the same endpoints from a DAG
orchestration UI -- a DAG card and a per-DAG drawer (runs, tasks, graph, XCom,
logs) with trigger, backfill, and approval decisions; its page documents that
UI.

See [example/dag/](https://github.com/ptweezy/cronstable/tree/develop/example/dag)
for a complete configuration exercising every node type.
