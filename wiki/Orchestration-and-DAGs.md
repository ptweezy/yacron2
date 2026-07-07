# Orchestration and DAGs

yacron2 schedules independent jobs. A **DAG** (the optional `dags:` section)
adds the other axis: a durable, dependency-ordered **workflow** of tasks, run
on a schedule, that survives restarts and coordinates across a fleet exactly
the way the rest of [Durable State](Durable-State) does. It is a small
orchestration engine built entirely on the pieces that already exist -- there
is **no new coordination service, no new backend, no client library**:

- a **dag_run** (one execution of a DAG) is a single mutable *document* in the
  [state store](Durable-State), holding every task's state;
- a **task** is an ordinary job invocation -- the same command/shell/env/
  timeout machinery, launched the same way, with the same
  [loopback state endpoint](Durable-State#state-as-a-job-primitive) injected,
  so a task can call `yacron2 xcom|artifact|state|lock|...`;
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
  path: /var/lib/yacron2/state      # DAGs need a state store + jobApi
dags:
  - name: nightly-etl
    schedule: "0 2 * * *"
    tasks:
      - id: extract
        command: "echo '[1,2,3]' | yacron2 xcom push --key ids"
      - id: transform
        dependsOn: [extract]
        command: "yacron2 xcom pull --task extract --key ids"
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
`failsWhen`, run-scoped `secrets`, and `monitorResources` (a monitored task
instance's sampled CPU time and peak RSS land in the `resources` object of its
task record in the `dag_run` document, and in the task's statsd sink if one is
configured; task instances do not appear in the per-job Prometheus families).
Per-task **retries** are DAG-owned
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
scoped to the dag_run, driven by the `yacron2 xcom` CLI the daemon makes
reachable in every task:

```bash
# in an upstream task:
echo '{"rows": 42}' | yacron2 xcom push --key summary
yacron2 xcom push --key summary producer_output_file.json    # or from a file

# in a downstream task:
yacron2 xcom pull --task upstream_id --key summary            # -> stdout
yacron2 xcom pull --task upstream_id --key summary -o out.json
yacron2 xcom list                                            # keys in this run
```

Outputs are content-addressed and versioned (newest wins by key). The daemon
injects the run's identity so the CLI needs no arguments beyond the key:
`YACRON2_DAG_NAME`, `YACRON2_DAG_RUN_ID`, `YACRON2_DAG_TASK`,
`YACRON2_DAG_TASKKEY`, `YACRON2_DAG_MAP_INDEX`, `YACRON2_DAG_MAP_ITEM`,
`YACRON2_DAG_XCOM_SCOPE`.

## Fan-out: dynamic mapping

A task can **expand** into N parallel instances, one per item of an upstream's
XCom list (Airflow's `.expand()`):

```yaml
      - id: list-work
        command: "echo '[\"a\",\"b\",\"c\"]' | yacron2 xcom push --key items"
      - id: process
        dependsOn: [list-work]
        expand:
          fromTask: list-work      # a direct, non-mapped dependency
          key: items               # its XCom list
        command: "echo processing $YACRON2_DAG_MAP_ITEM (#$YACRON2_DAG_MAP_INDEX)"
```

When `list-work` succeeds, the scheduler reads its `items` list and materialises
`process#0`, `process#1`, `process#2`, each with its own state, retries and
XCom, and its item in `$YACRON2_DAG_MAP_ITEM`. A downstream task that
`dependsOn: [process]` waits for **all** the mapped instances (fan-in). An
empty list resolves the mapped task to `success` immediately.

The expanded item set is recorded **once** in the dag_run and never recomputed,
so a crash-resumed run reconstructs the identical set of mapped instances
rather than re-deriving it from a possibly-changed upstream output.

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
[control API](HTTP-API#dag-endpoints) (or the dashboard):

```bash
curl -X POST .../dags/nightly-etl/runs/<run_key>/tasks/publish-gate/decision \
     -H 'Content-Type: application/json' \
     -d '{"decision": "approve", "by": "alice"}'
```

`approve` succeeds the gate and the graph proceeds; `reject` fails it (or, with
`onReject: skip`, marks it `skipped`, cascading `skipped` to its `all_success`
downstream). The decision (`by`, timestamp) is recorded durably.

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
[crash reconciliation](Durable-State#crash-reconciliation) seam. Like every
yacron2 coordination primitive it is **at-least-once**, not exactly-once: a
task whose process outlives a crashed daemon may run again on resume, so a task
that must be exactly-once should guard its side effect with an
[idempotency key](Durable-State#idempotency-keys).

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

See [example/dag/](https://github.com/ptweezy/yacron2/tree/develop/example/dag)
for a complete configuration exercising every node type.
