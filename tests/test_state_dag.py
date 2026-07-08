"""The durable DAG state machine (pure logic in cronstable.dag).

These tests drive :mod:`cronstable.dag` directly: the transforms are pure
``transform(body) -> (new_body, result)`` callables, so a tiny in-test executor
stands in for the cron driver (apply the claim transform, "launch" each intent,
mark it finished with a scripted outcome, repeat).  No backend, no clock, no
subprocess -- the whole graph engine is exercised against plain dicts.

Style matches the other state test files: bare ``def`` tests, module seams
driven with explicit values, no frozen wall clock (``now`` is an explicit
argument everywhere).  Backend + cron wiring lives in test_state_dag_run.py.
"""

import asyncio
import json
import sys

import pytest

import cronstable.__main__
from cronstable import dag, jobcli
from cronstable.config import (
    ConfigError,
    _validate_cross_sections,
    parse_config_string,
)
from cronstable.dag import DagSpec, ExpandSpec, TaskSpec

_STATE = "state:\n  path: /tmp/x\n"


def _dagcfg(dags_yaml, state=_STATE):
    return parse_config_string(state + dags_yaml, "")


def _xsect(dags_yaml, state=_STATE):
    _validate_cross_sections(_dagcfg(dags_yaml, state))


def _spec(*tasks):
    return DagSpec.build("d", list(tasks))


def _body(spec, now=0.0):
    return dag.new_run_body(
        dag="d",
        run_key="rk",
        run_id="rid",
        logical_date=None,
        kind="scheduled",
        now=now,
        spec=spec,
    )


def _apply(transform, body):
    new, result = transform(body)
    if dag.is_keep(new):
        return body, result
    return new, result


class _Executor:
    """Drives a spec+body to a fixed point like the real advance loop.

    ``outcomes`` maps a task id (or ``id#i``) to ``True`` (success) / ``False``
    (failure); ``xcom`` maps a task id to the list it "published" so a mapped
    downstream can expand.  Approval gates and un-scripted tasks are left
    parked (the run stops making progress), which the test then inspects.
    """

    def __init__(self, spec, outcomes=None, xcom=None):
        self.spec = spec
        self.outcomes = outcomes or {}
        self.xcom = xcom or {}
        self.now = 100.0
        self.launched = []

    def _expansions(self, body):
        out = {}
        for tid, from_task, _key in dag.tasks_awaiting_expansion(
            self.spec, body
        ):
            out[tid] = self.xcom.get(from_task)
        return out

    def step(self, body):
        self.now += 1.0
        transform = dag.plan_and_claim(
            self.spec, self.now, "proc-A", "host-A", self._expansions(body)
        )
        body, result = _apply(transform, body)
        for intent in result.launches:
            self.launched.append(intent.taskkey)
            body = self._finish(body, intent)
        return body, result

    def _finish(self, body, intent):
        # simulate set_task_pid then completion
        body, _ = _apply(
            dag.set_task_pid(intent.taskkey, "proc-A", 4321, self.now), body
        )
        key = intent.taskkey
        success = self.outcomes.get(key, self.outcomes.get(intent.task_id))
        if success is None:
            return body  # unscripted: leave running (e.g. approval/sensor)
        task = self.spec.by_id[intent.task_id]
        body, _ = _apply(
            dag.mark_task_finished(
                key,
                success=bool(success),
                exit_code=0 if success else 1,
                fail_reason=None if success else "boom",
                now=self.now,
                task=task,
            ),
            body,
        )
        return body

    def run(self, body, max_steps=50):
        for _ in range(max_steps):
            body, result = self.step(body)
            if dag.is_terminal_run(body):
                return body
            if not result.changed and not result.launches:
                return body  # fixed point (parked on approval/sensor)
        raise AssertionError("did not converge")


def _state(body, key):
    return body["tasks"][key]["state"]


# --------------------------------------------------------------------------
# Graph validation
# --------------------------------------------------------------------------


def test_validate_ok_linear():
    spec = _spec(
        TaskSpec("a"),
        TaskSpec("b", depends_on=("a",)),
        TaskSpec("c", depends_on=("b",)),
    )
    dag.validate_graph(spec)  # no raise


def test_validate_unknown_dep():
    spec = _spec(TaskSpec("a", depends_on=("nope",)))
    with pytest.raises(dag.DagValidationError, match="unknown task 'nope'"):
        dag.validate_graph(spec)


def test_validate_cycle():
    spec = _spec(
        TaskSpec("a", depends_on=("c",)),
        TaskSpec("b", depends_on=("a",)),
        TaskSpec("c", depends_on=("b",)),
    )
    with pytest.raises(dag.DagValidationError, match="cycle"):
        dag.validate_graph(spec)


def test_validate_duplicate_id():
    spec = _spec(TaskSpec("a"), TaskSpec("a"))
    with pytest.raises(dag.DagValidationError, match="duplicate"):
        dag.validate_graph(spec)


def test_validate_expand_needs_direct_dep():
    spec = _spec(
        TaskSpec("a"),
        TaskSpec("b", depends_on=("a",)),
        TaskSpec(
            "c",
            depends_on=("b",),
            expand=ExpandSpec(from_task="a", key="items"),
        ),
    )
    with pytest.raises(dag.DagValidationError, match="direct dependsOn"):
        dag.validate_graph(spec)


def test_validate_expand_of_sensor_rejected():
    spec = _spec(
        TaskSpec("a"),
        TaskSpec(
            "s",
            type=dag.SENSOR,
            depends_on=("a",),
            expand=ExpandSpec(from_task="a", key="k"),
        ),
    )
    with pytest.raises(dag.DagValidationError, match="only a plain task"):
        dag.validate_graph(spec)


def test_validate_chained_mapping_rejected():
    spec = _spec(
        TaskSpec("a"),
        TaskSpec(
            "b", depends_on=("a",),
            expand=ExpandSpec(from_task="a", key="k"),
        ),
        TaskSpec(
            "c", depends_on=("b",),
            expand=ExpandSpec(from_task="b", key="k"),
        ),
    )
    with pytest.raises(dag.DagValidationError, match="itself mapped"):
        dag.validate_graph(spec)


# --------------------------------------------------------------------------
# Linear progression + terminal run
# --------------------------------------------------------------------------


def test_linear_all_success():
    spec = _spec(
        TaskSpec("a"),
        TaskSpec("b", depends_on=("a",)),
        TaskSpec("c", depends_on=("b",)),
    )
    ex = _Executor(spec, outcomes={"a": True, "b": True, "c": True})
    body = ex.run(_body(spec))
    assert body["state"] == dag.SUCCESS
    assert ex.launched == ["a", "b", "c"]  # strict dependency order


def test_upstream_failure_propagates():
    spec = _spec(
        TaskSpec("a"),
        TaskSpec("b", depends_on=("a",)),
        TaskSpec("c", depends_on=("b",)),
    )
    ex = _Executor(spec, outcomes={"a": False})
    body = ex.run(_body(spec))
    assert body["state"] == dag.FAILED
    assert _state(body, "a") == dag.FAILED
    assert _state(body, "b") == dag.UPSTREAM_FAILED
    assert _state(body, "c") == dag.UPSTREAM_FAILED
    assert "b" not in ex.launched  # never launched a doomed downstream


def test_all_done_runs_despite_failure():
    spec = _spec(
        TaskSpec("a"),
        TaskSpec("b", depends_on=("a",), trigger_rule=dag.ALL_DONE),
    )
    ex = _Executor(spec, outcomes={"a": False, "b": True})
    body = ex.run(_body(spec))
    assert _state(body, "a") == dag.FAILED
    assert _state(body, "b") == dag.SUCCESS
    # run is FAILED because a task failed, even though b ran and succeeded
    assert body["state"] == dag.FAILED


def test_diamond_fan_in():
    spec = _spec(
        TaskSpec("root"),
        TaskSpec("left", depends_on=("root",)),
        TaskSpec("right", depends_on=("root",)),
        TaskSpec("join", depends_on=("left", "right")),
    )
    ex = _Executor(
        spec,
        outcomes=dict.fromkeys(("root", "left", "right", "join"), True),
    )
    body = ex.run(_body(spec))
    assert body["state"] == dag.SUCCESS
    assert ex.launched[0] == "root"
    assert ex.launched[-1] == "join"
    assert set(ex.launched[1:3]) == {"left", "right"}


# --------------------------------------------------------------------------
# Retry
# --------------------------------------------------------------------------


def test_task_retries_then_succeeds():
    spec = _spec(TaskSpec("a", max_attempts=3, retry_delay=0.0))
    body = _body(spec)
    now = 10.0
    # first claim + fail -> up_for_retry
    body, res = _apply(
        dag.plan_and_claim(spec, now, "p", "h", {}), body
    )
    assert res.launches[0].task_id == "a"
    task = spec.by_id["a"]
    body, _ = _apply(
        dag.mark_task_finished(
            "a", success=False, exit_code=1, fail_reason="x",
            now=now, task=task,
        ),
        body,
    )
    assert _state(body, "a") == dag.UP_FOR_RETRY
    assert body["tasks"]["a"]["attempt"] == 1
    # next advance re-claims (retry delay elapsed)
    body, res = _apply(
        dag.plan_and_claim(spec, now + 1, "p", "h", {}), body
    )
    assert [i.task_id for i in res.launches] == ["a"]
    assert _state(body, "a") == dag.RUNNING
    # succeed the retry
    body, _ = _apply(
        dag.mark_task_finished(
            "a", success=True, exit_code=0, fail_reason=None,
            now=now + 2, task=task,
        ),
        body,
    )
    assert _state(body, "a") == dag.SUCCESS


def test_task_exhausts_retries():
    spec = _spec(TaskSpec("a", max_attempts=2))
    ex = _Executor(spec, outcomes={"a": False})
    body = ex.run(_body(spec))
    assert _state(body, "a") == dag.FAILED
    assert body["tasks"]["a"]["attempt"] == 2
    assert ex.launched.count("a") == 2  # initial + one retry


def test_completion_is_fenced_to_the_claiming_proc_and_attempt():
    # H3/H4: a superseded attempt's late completion (a partitioned/evicted
    # former owner whose subprocess outlived its lease) must NOT terminalise
    # the instance another node has since reconciled and re-claimed.
    spec = _spec(TaskSpec("a", max_attempts=3, retry_delay=0.0))
    body = _body(spec)
    # node A claims attempt 0 under proc token "proc-A" and records its pid.
    body, res = _apply(
        dag.plan_and_claim(spec, 10.0, "proc-A", "host-A", {}), body
    )
    assert res.launches[0].task_id == "a"
    body, _ = _apply(dag.set_task_pid("a", "proc-A", 111, 10.0), body)
    # node A partitions; node B reconciles the crashed attempt (its proc is
    # foreign and its pid is not alive here) -> up_for_retry, attempt 1.
    body, _ = _apply(
        dag.reconcile_crashed(
            spec, 40.0, "proc-B", "host-B", lambda pid: False
        ),
        body,
    )
    assert _state(body, "a") == dag.UP_FOR_RETRY
    assert body["tasks"]["a"]["attempt"] == 1
    # node B re-claims attempt 1 under proc token "proc-B" and launches it.
    body, _ = _apply(
        dag.plan_and_claim(spec, 41.0, "proc-B", "host-B", {}), body
    )
    assert _state(body, "a") == dag.RUNNING
    assert body["tasks"]["a"]["proc"] == "proc-B"
    task = spec.by_id["a"]
    # node A's OLD attempt-0 subprocess now finishes; its completion carries
    # the stale (proc-A, attempt 0) identity -> it must be a NO-OP.
    body, changed = _apply(
        dag.mark_task_finished(
            "a", success=True, exit_code=0, fail_reason=None, now=45.0,
            task=task, expected_proc="proc-A", expected_attempt=0,
        ),
        body,
    )
    assert changed is False
    assert _state(body, "a") == dag.RUNNING  # live attempt-1 untouched
    assert body["tasks"]["a"]["proc"] == "proc-B"
    # node B's real completion (matching fence) DOES apply.
    body, changed = _apply(
        dag.mark_task_finished(
            "a", success=True, exit_code=0, fail_reason=None, now=46.0,
            task=task, expected_proc="proc-B", expected_attempt=1,
        ),
        body,
    )
    assert changed is True
    assert _state(body, "a") == dag.SUCCESS


def test_completion_without_fence_still_applies_backward_compat():
    # expected_proc/expected_attempt default to None -> no fence, so existing
    # callers/tests that omit them keep working.
    spec = _spec(TaskSpec("a", max_attempts=1))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    body, changed = _apply(
        dag.mark_task_finished(
            "a", success=True, exit_code=0, fail_reason=None,
            now=2.0, task=spec.by_id["a"],
        ),
        body,
    )
    assert changed is True
    assert _state(body, "a") == dag.SUCCESS


def test_retry_delay_defers_reclaim():
    spec = _spec(TaskSpec("a", max_attempts=2, retry_delay=100.0))
    body = _body(spec)
    task = spec.by_id["a"]
    body, _ = _apply(dag.plan_and_claim(spec, 10.0, "p", "h", {}), body)
    body, _ = _apply(
        dag.mark_task_finished(
            "a", success=False, exit_code=1, fail_reason="x",
            now=10.0, task=task,
        ),
        body,
    )
    # before the delay elapses: no re-claim
    body, res = _apply(dag.plan_and_claim(spec, 50.0, "p", "h", {}), body)
    assert res.launches == []
    assert _state(body, "a") == dag.UP_FOR_RETRY
    # after: re-claim
    body, res = _apply(dag.plan_and_claim(spec, 200.0, "p", "h", {}), body)
    assert [i.task_id for i in res.launches] == ["a"]


# --------------------------------------------------------------------------
# Fan-out / dynamic mapping
# --------------------------------------------------------------------------


def test_fan_out_expands_and_joins():
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec(
            "work",
            depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
        TaskSpec("collect", depends_on=("work",)),
    )
    ex = _Executor(
        spec,
        outcomes={
            "gen": True, "collect": True,
            "work#0": True, "work#1": True, "work#2": True,
        },
        xcom={"gen": ["x", "y", "z"]},
    )
    body = ex.run(_body(spec))
    assert body["state"] == dag.SUCCESS
    assert body["mapped"]["work"]["items"] == ["x", "y", "z"]
    assert {"work#0", "work#1", "work#2"}.issubset(set(ex.launched))
    # each instance carried its own item
    assert body["tasks"]["work#1"]["mapItem"] == "y"
    assert ex.launched[-1] == "collect"


def test_fan_out_empty_list_resolves_success():
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec(
            "work", depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
        TaskSpec("collect", depends_on=("work",)),
    )
    ex = _Executor(
        spec,
        outcomes={"gen": True, "collect": True},
        xcom={"gen": []},
    )
    body = ex.run(_body(spec))
    assert body["state"] == dag.SUCCESS
    assert body["mapped"]["work"]["items"] == []
    # collect still ran (empty map counts as success upstream)
    assert "collect" in ex.launched


def test_fan_out_one_instance_fails_fails_join():
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec(
            "work", depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
        TaskSpec("collect", depends_on=("work",)),
    )
    ex = _Executor(
        spec,
        outcomes={"gen": True, "work#0": True, "work#1": False},
        xcom={"gen": ["a", "b"]},
    )
    body = ex.run(_body(spec))
    assert body["state"] == dag.FAILED
    assert dag.effective_state(spec, body, "work") == dag.UPSTREAM_FAILED
    assert _state(body, "collect") == dag.UPSTREAM_FAILED


def test_mapped_all_done_source_fails_terminalises():
    # regression: a mapped task with trigger_rule=all_done whose expand source
    # FAILS must terminalise (it can never fan out), not hang the run forever.
    spec = _spec(
        TaskSpec("u"),
        TaskSpec(
            "m",
            depends_on=("u",),
            trigger_rule=dag.ALL_DONE,
            expand=ExpandSpec(from_task="u", key="items"),
        ),
    )
    ex = _Executor(spec, outcomes={"u": False})
    body = ex.run(_body(spec))
    assert dag.is_terminal_run(body)
    assert body["state"] == dag.FAILED
    assert dag.effective_state(spec, body, "m") == dag.UPSTREAM_FAILED


def test_mapped_group_waits_for_all_instances():
    # regression: the fan-in barrier must hold -- the group is not terminal
    # (so a downstream cannot launch) until EVERY instance is terminal, even
    # if one has already failed.
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec(
            "w", depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
    )
    body = _body(spec)
    body["mapped"]["w"] = {"items": ["a", "b"], "expandedAt": 1.0}
    body["tasks"]["w#0"] = {"id": "w", "state": dag.FAILED}
    body["tasks"]["w#1"] = {"id": "w", "state": dag.RUNNING}
    assert dag._mapped_group_state(body, "w") == dag.RUNNING  # barrier
    body["tasks"]["w#1"]["state"] = dag.SUCCESS
    assert dag._mapped_group_state(body, "w") == dag.UPSTREAM_FAILED


def test_reconcile_protects_own_proc_without_pid():
    # regression: a task claimed by THIS process whose pid was never recorded
    # (set_pid failed / timed out) must NOT be reconciled by the same process
    # -- the proc token, set at claim time, protects the live task.
    spec = _spec(TaskSpec("a"))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "me", "h", {}), body)
    assert body["tasks"]["a"]["proc"] == "me"
    assert body["tasks"]["a"]["pid"] is None
    body, n = _apply(
        dag.reconcile_crashed(spec, 2.0, "me", "h", lambda pid: False), body
    )
    assert n == 0
    assert _state(body, "a") == dag.RUNNING


def test_added_task_does_not_block_terminalise():
    # regression: a reload that adds a task must not wedge an in-flight run
    # created under the older spec (the added task has no entry in this run).
    spec1 = _spec(TaskSpec("a"))
    body = _body(spec1)
    body, _ = _apply(dag.plan_and_claim(spec1, 1.0, "p", "h", {}), body)
    body, _ = _apply(
        dag.mark_task_finished(
            "a", success=True, exit_code=0, fail_reason=None,
            now=1.0, task=spec1.by_id["a"],
        ),
        body,
    )
    spec2 = _spec(TaskSpec("a"), TaskSpec("b", depends_on=("a",)))
    body, _ = _apply(dag.plan_and_claim(spec2, 2.0, "p", "h", {}), body)
    assert dag.is_terminal_run(body)
    assert body["state"] == dag.SUCCESS
    assert "b" not in body["tasks"]  # never materialised into this run


def test_fan_out_deterministic_on_replan():
    # the mapped item set is recorded once and never recomputed, even if the
    # upstream xcom "changes" underneath a later pass.
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec(
            "work", depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
    )
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    body, _ = _apply(
        dag.mark_task_finished(
            "gen", success=True, exit_code=0, fail_reason=None,
            now=1.0, task=spec.by_id["gen"],
        ),
        body,
    )
    body, _ = _apply(
        dag.plan_and_claim(spec, 2.0, "p", "h", {"work": ["a", "b"]}), body
    )
    assert body["mapped"]["work"]["items"] == ["a", "b"]
    # a later pass offering a different list must NOT re-expand
    body, _ = _apply(
        dag.plan_and_claim(spec, 3.0, "p", "h", {"work": ["a", "b", "c"]}),
        body,
    )
    assert body["mapped"]["work"]["items"] == ["a", "b"]


# --------------------------------------------------------------------------
# Sensors
# --------------------------------------------------------------------------


def test_sensor_pokes_until_success():
    spec = _spec(
        TaskSpec("s", type=dag.SENSOR, poke_interval=10.0, poke_timeout=1e9),
    )
    body = _body(spec)
    task = spec.by_id["s"]
    # first poke
    body, res = _apply(dag.plan_and_claim(spec, 100.0, "p", "h", {}), body)
    assert [i.is_sensor for i in res.launches] == [True]
    # poke returns "not yet" (nonzero) -> reschedule
    body, _ = _apply(
        dag.mark_task_finished(
            "s", success=False, exit_code=1, fail_reason=None,
            now=100.0, task=task,
        ),
        body,
    )
    assert _state(body, "s") == dag.RUNNING
    assert body["tasks"]["s"]["nextPokeAt"] == 110.0
    # not due yet
    body, res = _apply(dag.plan_and_claim(spec, 105.0, "p", "h", {}), body)
    assert res.launches == []
    # due: re-poke
    body, res = _apply(dag.plan_and_claim(spec, 111.0, "p", "h", {}), body)
    assert len(res.launches) == 1
    body, _ = _apply(
        dag.mark_task_finished(
            "s", success=True, exit_code=0, fail_reason=None,
            now=111.0, task=task,
        ),
        body,
    )
    assert _state(body, "s") == dag.SUCCESS


def test_sensor_times_out():
    spec = _spec(
        TaskSpec("s", type=dag.SENSOR, poke_interval=10.0, poke_timeout=25.0),
    )
    body = _body(spec)
    task = spec.by_id["s"]
    body, _ = _apply(dag.plan_and_claim(spec, 100.0, "p", "h", {}), body)
    body, _ = _apply(
        dag.mark_task_finished(
            "s", success=False, exit_code=1, fail_reason=None,
            now=100.0, task=task,
        ),
        body,
    )
    # far past the timeout window (firstPokeAt=100, timeout=25 -> 125)
    body, res = _apply(dag.plan_and_claim(spec, 200.0, "p", "h", {}), body)
    assert _state(body, "s") == dag.FAILED
    assert res.launches == []


# --------------------------------------------------------------------------
# Approval gates
# --------------------------------------------------------------------------


def test_approval_blocks_then_approves():
    spec = _spec(
        TaskSpec("a"),
        TaskSpec("gate", type=dag.APPROVAL, depends_on=("a",)),
        TaskSpec("b", depends_on=("gate",)),
    )
    ex = _Executor(spec, outcomes={"a": True, "b": True})
    body = ex.run(_body(spec))
    # parked awaiting approval; b not launched
    assert _state(body, "gate") == dag.RUNNING
    assert body["tasks"]["gate"]["awaitingApproval"] is True
    assert "b" not in ex.launched
    # approve
    body, result = _apply(
        dag.apply_approval(
            "gate", approved=True, by="alice", now=500.0,
            on_reject=dag.FAILED,
        ),
        body,
    )
    assert result["ok"] is True
    assert _state(body, "gate") == dag.SUCCESS
    # resume: b now runs to completion
    body = ex.run(body)
    assert body["state"] == dag.SUCCESS
    assert "b" in ex.launched


def test_approval_reject_skip_cascades():
    spec = _spec(
        TaskSpec("gate", type=dag.APPROVAL, on_reject=dag.SKIPPED),
        TaskSpec("b", depends_on=("gate",)),
    )
    body = _body(spec)
    # claim the gate (awaiting)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    assert body["tasks"]["gate"]["awaitingApproval"] is True
    body, result = _apply(
        dag.apply_approval(
            "gate", approved=False, by="bob", now=2.0, on_reject=dag.SKIPPED,
        ),
        body,
    )
    assert _state(body, "gate") == dag.SKIPPED
    # downstream cascades to skipped under all_success
    body, _ = _apply(dag.plan_and_claim(spec, 3.0, "p", "h", {}), body)
    assert _state(body, "b") == dag.SKIPPED
    assert dag.is_terminal_run(body)
    assert body["state"] == dag.SUCCESS  # skipped is not a failure


def test_double_approval_is_noop():
    spec = _spec(TaskSpec("gate", type=dag.APPROVAL))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    body, r1 = _apply(
        dag.apply_approval(
            "gate", approved=True, by="a", now=2.0, on_reject=dag.FAILED
        ),
        body,
    )
    assert r1["ok"] is True
    body, r2 = _apply(
        dag.apply_approval(
            "gate", approved=False, by="b", now=3.0, on_reject=dag.FAILED
        ),
        body,
    )
    assert r2["ok"] is False  # already decided
    assert _state(body, "gate") == dag.SUCCESS


# --------------------------------------------------------------------------
# Crash reconciliation
# --------------------------------------------------------------------------


def test_reconcile_dead_task_retries():
    spec = _spec(TaskSpec("a", max_attempts=2))
    body = _body(spec)
    # claim + record a pid from a now-dead prior process
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "old-proc", "h", {}), body)
    body, _ = _apply(dag.set_task_pid("a", "old-proc", 999, 1.0), body)
    assert _state(body, "a") == dag.RUNNING
    # a new process reconciles: pid 999 is dead
    body, n = _apply(
        dag.reconcile_crashed(
            spec, 10.0, "new-proc", "h", lambda pid: False
        ),
        body,
    )
    assert n == 1
    assert _state(body, "a") == dag.UP_FOR_RETRY
    assert body["tasks"]["a"]["attempt"] == 1


def test_reconcile_leaves_live_child():
    spec = _spec(TaskSpec("a"))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "old-proc", "h", {}), body)
    body, _ = _apply(dag.set_task_pid("a", "old-proc", 999, 1.0), body)
    # same host, pid still alive -> the child outlived the daemon; leave it
    body, n = _apply(
        dag.reconcile_crashed(spec, 10.0, "new-proc", "h", lambda pid: True),
        body,
    )
    assert n == 0
    assert _state(body, "a") == dag.RUNNING


def test_reconcile_leaves_own_process():
    spec = _spec(TaskSpec("a"))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "proc-A", "h", {}), body)
    body, _ = _apply(dag.set_task_pid("a", "proc-A", 5, 1.0), body)
    # our own token: never reconcile (even if pid_alive says dead)
    body, n = _apply(
        dag.reconcile_crashed(spec, 2.0, "proc-A", "h", lambda pid: False),
        body,
    )
    assert n == 0
    assert _state(body, "a") == dag.RUNNING


def test_reconcile_claimed_but_never_launched():
    spec = _spec(TaskSpec("a", max_attempts=1))
    body = _body(spec)
    # a prior process claimed `a` (its proc token persisted at claim time) but
    # crashed before recording the pid; a fresh process must recover it.
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "old-proc", "h", {}), body)
    assert body["tasks"]["a"]["proc"] == "old-proc"
    assert body["tasks"]["a"]["pid"] is None
    body, n = _apply(
        dag.reconcile_crashed(spec, 5.0, "new-proc", "h", lambda pid: True),
        body,
    )
    assert n == 1
    assert _state(body, "a") == dag.FAILED  # no attempts left -> terminal


def test_reconcile_leaves_sensor_between_pokes(monkeypatch):
    # a sensor idling between pokes has proc cleared;
    # reconciliation (which runs
    # at the top of every advance) must NOT touch it, or it would re-poke every
    # pass and defeat the poke schedule.
    spec = _spec(
        TaskSpec("s", type=dag.SENSOR, poke_interval=30.0, poke_timeout=1e9),
    )
    body = _body(spec)
    task = spec.by_id["s"]
    body, _ = _apply(dag.plan_and_claim(spec, 100.0, "p", "h", {}), body)
    body, _ = _apply(
        dag.mark_task_finished(
            "s", success=False, exit_code=1, fail_reason=None,
            now=100.0, task=task,
        ),
        body,
    )
    assert body["tasks"]["s"]["proc"] is None
    assert body["tasks"]["s"]["nextPokeAt"] == 130.0
    # reconcile with a fresh proc: the idle sensor is left exactly as-is.
    body, n = _apply(
        dag.reconcile_crashed(spec, 105.0, "q", "h", lambda pid: False),
        body,
    )
    assert n == 0
    assert body["tasks"]["s"]["nextPokeAt"] == 130.0  # schedule preserved


def test_reconcile_recovers_crashed_sensor_poke():
    # a sensor whose poke crashed mid-flight (proc set, pid dead) IS recovered.
    spec = _spec(TaskSpec("s", type=dag.SENSOR, poke_timeout=1e9))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "old", "h", {}), body)
    body, _ = _apply(dag.set_task_pid("s", "old", 999, 1.0), body)
    body, n = _apply(
        dag.reconcile_crashed(spec, 9.0, "new", "h", lambda pid: False),
        body,
    )
    assert n == 1
    assert _state(body, "s") == dag.RUNNING  # re-poke, not fail
    assert body["tasks"]["s"]["proc"] is None
    assert body["tasks"]["s"]["nextPokeAt"] == 9.0


def test_reconcile_skips_approval_gate():
    spec = _spec(TaskSpec("gate", type=dag.APPROVAL))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "old-proc", "h", {}), body)
    assert body["tasks"]["gate"]["awaitingApproval"] is True
    body, n = _apply(
        dag.reconcile_crashed(spec, 5.0, "new-proc", "h", lambda pid: False),
        body,
    )
    assert n == 0  # a gate awaiting a human is not a crash victim
    assert body["tasks"]["gate"]["awaitingApproval"] is True


# --------------------------------------------------------------------------
# Key helpers
# --------------------------------------------------------------------------


def test_xcom_scheme():
    assert dag.xcom_scope("etl", "rid1") == "dagxcom/etl/rid1"
    assert dag.xcom_name("work#2", "out") == "work#2/out"
    assert dag.task_display_key("t", None) == "t"
    assert dag.task_display_key("t", 3) == "t#3"


def test_run_key_sanitised():
    key = dag.run_key_for_logical("2026-07-04T02:00:00+00:00")
    assert "/" not in key and " " not in key
    # deterministic
    assert key == dag.run_key_for_logical("2026-07-04T02:00:00+00:00")


# --------------------------------------------------------------------------
# `cronstable xcom` CLI (the HTTP seam monkeypatched, like the phase-5 CLI
# tests)
# --------------------------------------------------------------------------


class _ExitError(Exception):
    pass


class _FakeHTTP:
    def __init__(self, responses=None):
        self.responses = responses or {}
        self.calls = []

    def __call__(self, method, path, *, query=None, json_body=None, data=None):
        self.calls.append(
            {"method": method, "path": path, "query": query, "data": data}
        )
        status, body = self.responses.get(path, (200, {}))
        payload = (
            body if isinstance(body, bytes) else json.dumps(body).encode()
        )
        return status, {}, payload


def _xcom_cli(monkeypatch, argv, http=None, stdin=b""):
    monkeypatch.setenv("CRONSTABLE_STATE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("CRONSTABLE_STATE_TOKEN", "tok")
    monkeypatch.setenv("CRONSTABLE_DAG_XCOM_SCOPE", "dagxcom/d/rid")
    monkeypatch.setenv("CRONSTABLE_DAG_TASKKEY", "gen")
    if http is not None:
        monkeypatch.setattr(jobcli, "_http", http)

    class _Buf:
        def __init__(self):
            self.buffer = self

        def read(self):
            return stdin

    monkeypatch.setattr(sys, "stdin", _Buf())
    loop = asyncio.new_event_loop()
    try:
        monkeypatch.setattr(sys, "argv", ["cronstable"] + argv)
        monkeypatch.setattr(
            sys, "exit", lambda code=0: (_ for _ in ()).throw(_ExitError(code))
        )
        with pytest.raises(_ExitError) as ex:
            cronstable.__main__.main_loop(loop)
        return ex.value.args[0]
    finally:
        loop.close()


def test_xcom_push_targets_own_taskkey(monkeypatch):
    http = _FakeHTTP({"/v1/artifact/put": (200, {"sha256": "ab", "size": 2})})
    code = _xcom_cli(
        monkeypatch, ["xcom", "push", "--key", "out"], http=http, stdin=b"hi"
    )
    assert code == 0
    call = http.calls[0]
    assert call["path"] == "/v1/artifact/put"
    assert call["query"] == {"scope": "dagxcom/d/rid", "name": "gen/out"}
    assert call["data"] == b"hi"


def test_xcom_pull_reads_upstream(monkeypatch, capsysbinary):
    http = _FakeHTTP({"/v1/artifact/get": (200, b"payload")})
    code = _xcom_cli(
        monkeypatch,
        ["xcom", "pull", "--task", "up", "--key", "out"],
        http=http,
    )
    assert code == 0
    assert http.calls[0]["query"]["name"] == "up/out"
    assert capsysbinary.readouterr().out == b"payload"


def test_xcom_pull_map_index(monkeypatch):
    http = _FakeHTTP({"/v1/artifact/get": (200, b"x")})
    _xcom_cli(
        monkeypatch,
        ["xcom", "pull", "--task", "up", "--key", "out", "--map-index", "2"],
        http=http,
    )
    assert http.calls[0]["query"]["name"] == "up#2/out"


def test_xcom_pull_missing_is_exit_4(monkeypatch):
    http = _FakeHTTP({"/v1/artifact/get": (404, {})})
    code = _xcom_cli(
        monkeypatch,
        ["xcom", "pull", "--task", "up", "--key", "gone"],
        http=http,
    )
    assert code == jobcli.EXIT_NOT_FOUND


def test_xcom_outside_dag_errors(monkeypatch):
    # no CRONSTABLE_DAG_XCOM_SCOPE -> a clean error, not a traceback
    monkeypatch.delenv("CRONSTABLE_DAG_XCOM_SCOPE", raising=False)
    monkeypatch.setenv("CRONSTABLE_STATE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("CRONSTABLE_STATE_TOKEN", "tok")
    monkeypatch.setattr(sys, "argv", ["cronstable", "xcom", "list"])
    monkeypatch.setattr(
        sys, "exit", lambda code=0: (_ for _ in ()).throw(_ExitError(code))
    )
    loop = asyncio.new_event_loop()
    try:
        with pytest.raises(_ExitError) as ex:
            cronstable.__main__.main_loop(loop)
        assert ex.value.args[0] == jobcli.EXIT_ERROR
    finally:
        loop.close()


# --------------------------------------------------------------------------
# Config parsing + cross-section validation
# --------------------------------------------------------------------------


_ETL = """
dags:
  - name: etl
    schedule: '0 2 * * *'
    onMissed: run-all
    retainRuns: 7
    tasks:
      - id: extract
        command: 'echo x'
      - id: load
        command: 'echo y'
        dependsOn:
          - extract
        retries: 3
        retryDelaySeconds: 5
"""


def test_dag_parsed():
    cfg = _dagcfg(_ETL)
    (d,) = cfg.dags
    assert d.name == "etl"
    assert d.retain_runs == 7
    assert d.schedule_job is not None
    assert d.schedule_job.onMissed == "run-all"
    load = d.task_templates["load"]
    assert load.command == "echo y"
    spec = {t.id: t.spec for t in d.tasks}
    assert spec["load"].max_attempts == 4  # retries: 3 -> 4 attempts
    assert spec["load"].retry_delay == 5.0
    assert spec["load"].depends_on == ("extract",)


def test_dag_manual_only_no_schedule():
    cfg = _dagcfg(
        "dags:\n  - name: m\n    tasks:\n"
        "      - id: t\n        command: 'echo'\n"
    )
    assert cfg.dags[0].schedule_job is None


def test_dag_requires_state():
    with pytest.raises(ConfigError, match="dags require a `state` section"):
        _validate_cross_sections(
            parse_config_string(
                "dags:\n  - name: d\n    tasks:\n"
                "      - id: t\n        command: 'echo'\n",
                "",
            )
        )


def test_dag_requires_jobapi_enabled():
    with pytest.raises(ConfigError, match="loopback endpoint"):
        _xsect(
            "dags:\n  - name: d\n    tasks:\n"
            "      - id: t\n        command: 'echo'\n",
            state="state:\n  path: /x\n  jobApi:\n    enabled: false\n",
        )


def test_dag_duplicate_name_rejected():
    with pytest.raises(ConfigError, match="duplicate dag name"):
        _xsect(
            "dags:\n"
            "  - name: d\n    tasks:\n      - id: a\n        command: 'e'\n"
            "  - name: d\n    tasks:\n      - id: b\n        command: 'e'\n"
        )


def test_dag_cycle_is_config_error():
    with pytest.raises(ConfigError, match="cycle"):
        _dagcfg(
            "dags:\n  - name: d\n    tasks:\n"
            "      - id: a\n        command: 'e'\n        dependsOn:\n"
            "          - b\n"
            "      - id: b\n        command: 'e'\n        dependsOn:\n"
            "          - a\n"
        )


def test_dag_unknown_dep_is_config_error():
    with pytest.raises(ConfigError, match="unknown task"):
        _dagcfg(
            "dags:\n  - name: d\n    tasks:\n"
            "      - id: a\n        command: 'e'\n        dependsOn:\n"
            "          - ghost\n"
        )


def test_dag_task_needs_command():
    with pytest.raises(ConfigError, match="needs a command"):
        _dagcfg("dags:\n  - name: d\n    tasks:\n      - id: a\n")


def test_dag_approval_needs_no_command():
    cfg = _dagcfg(
        "dags:\n  - name: d\n    tasks:\n"
        "      - id: gate\n        type: approval\n"
    )
    assert cfg.dags[0].tasks[0].type == "approval"


def test_dag_expand_must_be_direct_dep():
    with pytest.raises(ConfigError, match="direct dependsOn"):
        _dagcfg(
            "dags:\n  - name: d\n    tasks:\n"
            "      - id: a\n        command: 'e'\n"
            "      - id: b\n        command: 'e'\n        dependsOn:\n"
            "          - a\n"
            "      - id: c\n        command: 'e'\n        dependsOn:\n"
            "          - b\n        expand:\n"
            "          fromTask: a\n          key: items\n"
        )


def test_dag_retain_runs_floor():
    with pytest.raises(ConfigError, match="retainRuns must be >= 1"):
        _dagcfg(
            "dags:\n  - name: d\n    retainRuns: 0\n    tasks:\n"
            "      - id: a\n        command: 'e'\n"
        )


def test_dag_task_id_charset_rejected():
    # '#' / '/' would alias a mapped instance key or an XCom name
    with pytest.raises(ConfigError, match="may not contain"):
        _dagcfg(
            "dags:\n  - name: d\n    tasks:\n"
            "      - id: 'a/b'\n        command: 'e'\n"
        )
    with pytest.raises(dag.DagValidationError, match="may not contain"):
        dag.validate_graph(DagSpec.build("d", [TaskSpec("a#0")]))


# --------------------------------------------------------------------------
# Adversarial-review regressions
# --------------------------------------------------------------------------


def test_set_task_pid_fenced_to_claiming_proc():
    # A stale pid write from a superseded former owner must NOT clobber the
    # live claim's proc/pid -- doing so would fence out the real completion.
    spec = _spec(TaskSpec("a"))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "proc-B", "h", {}), body)
    assert body["tasks"]["a"]["proc"] == "proc-B"  # stamped at claim
    # a long-superseded former owner "proc-A" tries to record its pid
    body, changed = _apply(dag.set_task_pid("a", "proc-A", 999, 2.0), body)
    assert changed is False  # dropped: the entry is proc-B's claim now
    assert body["tasks"]["a"]["proc"] == "proc-B"  # unclobbered
    assert body["tasks"]["a"]["pid"] is None
    # the live owner's own pid write still applies
    body, changed = _apply(dag.set_task_pid("a", "proc-B", 4321, 3.0), body)
    assert changed is True
    assert body["tasks"]["a"]["pid"] == 4321


def test_set_task_pid_fenced_to_attempt():
    # A pid write stamped for a stale attempt is dropped even when the proc
    # token matches (a same-node reclaim after a retry bumps the attempt).
    spec = _spec(TaskSpec("a", max_attempts=3))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "proc-A", "h", {}), body)
    body["tasks"]["a"]["attempt"] = 1  # a newer attempt is now the live one
    body, changed = _apply(
        dag.set_task_pid("a", "proc-A", 7, 2.0, attempt=0), body
    )
    assert changed is False
    assert body["tasks"]["a"]["pid"] is None
    body, changed = _apply(
        dag.set_task_pid("a", "proc-A", 8, 3.0, attempt=1), body
    )
    assert changed is True
    assert body["tasks"]["a"]["pid"] == 8


def test_duplicate_depends_on_is_not_a_cycle():
    # regression: a repeated dependsOn entry is one edge; counting it twice
    # left a phantom indegree and a false 'cycle' rejection of an acyclic
    # graph.
    spec = _spec(TaskSpec("a"), TaskSpec("b", depends_on=("a", "a")))
    dag.validate_graph(spec)  # no raise
    ex = _Executor(spec, outcomes={"a": True, "b": True})
    body = ex.run(_body(spec))
    assert body["state"] == dag.SUCCESS


def test_dag_duplicate_dependson_config_accepted():
    # the same graph through the YAML path: dependsOn: [a, a] must load.
    cfg = _dagcfg(
        "dags:\n  - name: d\n    tasks:\n"
        "      - id: a\n        command: 'e'\n"
        "      - id: b\n        command: 'e'\n        dependsOn:\n"
        "          - a\n          - a\n"
    )
    assert cfg.dags[0].name == "d"


def test_sensor_repoke_clears_stale_due_instant():
    # regression: poke N>=2 must clear nextPokeAt at claim time -- a stale
    # past due-instant on an in-flight poke reads as a due wake and busy-spun
    # the driver loop for the poke's whole duration.
    spec = _spec(
        TaskSpec("s", type=dag.SENSOR, poke_interval=10.0, poke_timeout=1e9),
    )
    body = _body(spec)
    task = spec.by_id["s"]
    body, _ = _apply(dag.plan_and_claim(spec, 100.0, "p", "h", {}), body)
    body, _ = _apply(
        dag.mark_task_finished(
            "s",
            success=False,
            exit_code=1,
            fail_reason=None,
            now=100.0,
            task=task,
        ),
        body,
    )
    assert body["tasks"]["s"]["nextPokeAt"] == 110.0
    # poke 2 claimed at its due instant: the in-flight poke owns the schedule
    body, res = _apply(dag.plan_and_claim(spec, 111.0, "p", "h", {}), body)
    assert len(res.launches) == 1
    assert body["tasks"]["s"]["proc"] == "p"
    assert body["tasks"]["s"]["nextPokeAt"] is None
    # its completion re-sets the schedule
    body, _ = _apply(
        dag.mark_task_finished(
            "s",
            success=False,
            exit_code=1,
            fail_reason=None,
            now=112.0,
            task=task,
        ),
        body,
    )
    assert body["tasks"]["s"]["nextPokeAt"] == 122.0


def test_sensor_completion_poke_fence():
    # regression: a delayed re-apply of poke N's completion (its mutate timed
    # out but actually landed) carries the SAME proc token and attempt as the
    # live poke N+1 -- a re-poke claim re-stamps proc and never bumps attempt,
    # so only the poke number tells them apart.  A stale poke fence must
    # no-op; the matching one must apply.
    spec = _spec(
        TaskSpec("s", type=dag.SENSOR, poke_interval=10.0, poke_timeout=1e9),
    )
    body = _body(spec)
    task = spec.by_id["s"]
    # poke 0 claimed, its completion lands (pokeCount -> 1)
    body, res = _apply(dag.plan_and_claim(spec, 100.0, "p", "h", {}), body)
    assert [i.poke_number for i in res.launches] == [0]
    body, applied = _apply(
        dag.mark_task_finished(
            "s",
            success=False,
            exit_code=1,
            fail_reason=None,
            now=100.0,
            task=task,
            expected_proc="p",
            expected_attempt=0,
            expected_poke=0,
        ),
        body,
    )
    assert applied is True
    assert body["tasks"]["s"]["pokeCount"] == 1
    # poke 1 claimed: same proc token, same attempt, only pokeCount differs
    body, res = _apply(dag.plan_and_claim(spec, 111.0, "p", "h", {}), body)
    assert [i.poke_number for i in res.launches] == [1]
    assert body["tasks"]["s"]["proc"] == "p"
    # a stale re-apply of poke 0's completion must NOT touch the live poke
    body, applied = _apply(
        dag.mark_task_finished(
            "s",
            success=False,
            exit_code=1,
            fail_reason=None,
            now=112.0,
            task=task,
            expected_proc="p",
            expected_attempt=0,
            expected_poke=0,
        ),
        body,
    )
    assert applied is False
    entry = body["tasks"]["s"]
    assert entry["proc"] == "p"  # the live poke keeps its claim
    assert entry["pokeCount"] == 1
    assert entry["nextPokeAt"] is None  # in-flight poke owns the schedule
    # the live poke's own completion (matching poke fence) applies
    body, applied = _apply(
        dag.mark_task_finished(
            "s",
            success=True,
            exit_code=0,
            fail_reason=None,
            now=113.0,
            task=task,
            expected_proc="p",
            expected_attempt=0,
            expected_poke=1,
        ),
        body,
    )
    assert applied is True
    assert _state(body, "s") == dag.SUCCESS


def test_mapped_fanout_item_cap_fails_task_cleanly():
    # regression: an oversized XCom list must FAIL the mapped task with a
    # clear reason instead of materialising thousands of instances into the
    # run document and stampeding the host.
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec(
            "work",
            depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
        TaskSpec("collect", depends_on=("work",)),
    )
    items = list(range(dag.MAX_MAPPED_ITEMS + 1))
    ex = _Executor(spec, outcomes={"gen": True}, xcom={"gen": items})
    body = ex.run(_body(spec))
    assert body["state"] == dag.FAILED
    assert _state(body, "work") == dag.FAILED
    assert "exceeds the cap" in body["tasks"]["work"]["failReason"]
    assert "work" not in body["mapped"]  # the flood was never materialised
    assert "work#0" not in body["tasks"]
    assert _state(body, "collect") == dag.UPSTREAM_FAILED


def test_claims_are_batched_per_pass(monkeypatch):
    # regression: one advance pass must not claim (and so launch) an
    # unbounded batch; the remainder stays claimable, the result is marked
    # deferred, and later passes drain it.
    monkeypatch.setattr(dag, "MAX_CLAIMS_PER_PASS", 2)
    spec = _spec(*[TaskSpec("t{}".format(i)) for i in range(5)])
    body = _body(spec)
    body, res = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    assert len(res.launches) == 2
    assert res.deferred is True
    body, res = _apply(dag.plan_and_claim(spec, 2.0, "p", "h", {}), body)
    assert len(res.launches) == 2
    assert res.deferred is True
    body, res = _apply(dag.plan_and_claim(spec, 3.0, "p", "h", {}), body)
    assert len(res.launches) == 1
    assert res.deferred is False
    assert all(_state(body, "t{}".format(i)) == dag.RUNNING for i in range(5))


def test_reload_added_dependency_does_not_wedge_run():
    # A run is created for A -> B (all_success). A config reload then adds task
    # C and repoints B at [A, C]. C is absent from the already-created run
    # document (creation materialises only the then-current tasks); it must not
    # gate B, or the run would wait on C forever and never terminalise.
    old = _spec(TaskSpec("a"), TaskSpec("b", depends_on=("a",)))
    body = _body(old)
    body["tasks"]["a"]["state"] = dag.SUCCESS  # A already ran this run
    body["tasks"]["a"]["finishedAt"] = 5.0
    reloaded = _spec(
        TaskSpec("a"),
        TaskSpec("c"),
        TaskSpec("b", depends_on=("a", "c")),
    )
    # B is ready despite C's absence, and the run drives to a terminal state.
    assert dag._deps_verdict(reloaded, body, reloaded.by_id["b"]) == "ready"
    ex = _Executor(reloaded, outcomes={"b": True})
    body = ex.run(body)
    assert "b" in ex.launched  # B actually ran
    assert _state(body, "b") == dag.SUCCESS
    assert dag.is_terminal_run(body)
    assert body["state"] == dag.SUCCESS  # C's absence did not wedge it


# --------------------------------------------------------------------------
# Resource accounting on the task record (monitorResources)
# --------------------------------------------------------------------------


def test_finished_task_records_resources():
    # a monitored instance's sampled usage rides mark_task_finished into the
    # task record, and a later attempt's completion overwrites it.
    from cronstable.resources import ResourceUsage

    spec = _spec(TaskSpec("a", max_attempts=2, retry_delay=0.0))
    body = _body(spec)
    task = spec.by_id["a"]
    now = 10.0
    usage1 = ResourceUsage(1.5, 0.5, 1024, 3).to_dict()
    body, res = _apply(dag.plan_and_claim(spec, now, "p", "h", {}), body)
    assert res.launches[0].task_id == "a"
    body, _ = _apply(
        dag.mark_task_finished(
            "a",
            success=False,
            exit_code=1,
            fail_reason="x",
            now=now,
            task=task,
            resources=usage1,
        ),
        body,
    )
    assert body["tasks"]["a"]["resources"] == usage1
    # retry succeeds with different usage: the record carries the latest
    body, _ = _apply(dag.plan_and_claim(spec, now + 1, "p", "h", {}), body)
    usage2 = ResourceUsage(9.0, 1.0, 4096, 8).to_dict()
    body, _ = _apply(
        dag.mark_task_finished(
            "a",
            success=True,
            exit_code=0,
            fail_reason=None,
            now=now + 2,
            task=task,
            resources=usage2,
        ),
        body,
    )
    assert _state(body, "a") == dag.SUCCESS
    assert body["tasks"]["a"]["resources"] == usage2
    # the stored dict round-trips through the tolerant parser
    parsed = ResourceUsage.from_dict(body["tasks"]["a"]["resources"])
    assert parsed is not None and parsed.max_rss_bytes == 4096


def test_unmonitored_task_keeps_resources_none():
    # monitoring off (or nothing captured) -> resources stays None, and a
    # sensor's succeeding poke records its usage.
    from cronstable.resources import ResourceUsage

    spec = _spec(TaskSpec("a"))
    ex = _Executor(spec, outcomes={"a": True})
    body = ex.run(_body(spec))
    assert body["tasks"]["a"]["resources"] is None
    spec = _spec(TaskSpec("s", type=dag.SENSOR))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    usage = ResourceUsage(0.2, 0.1, 512, 1).to_dict()
    body, _ = _apply(
        dag.mark_task_finished(
            "s",
            success=True,
            exit_code=0,
            fail_reason=None,
            now=2.0,
            task=spec.by_id["s"],
            resources=usage,
        ),
        body,
    )
    assert _state(body, "s") == dag.SUCCESS
    assert body["tasks"]["s"]["resources"] == usage


def test_task_record_without_resources_field_still_parses():
    # backward compat: a pre-feature dag_run document has no "resources" key
    # on its task entries; completing and reading it must not care.
    from cronstable.resources import ResourceUsage

    spec = _spec(TaskSpec("a"))
    body = _body(spec)
    for entry in body["tasks"].values():
        entry.pop("resources", None)  # simulate an old document
    ex = _Executor(spec, outcomes={"a": True})
    body = ex.run(body)
    assert _state(body, "a") == dag.SUCCESS
    assert body["tasks"]["a"].get("resources") is None
    # a malformed stored value parses to None instead of raising
    assert ResourceUsage.from_dict(body["tasks"]["a"].get("resources")) is None
    assert ResourceUsage.from_dict("garbage") is None
    assert ResourceUsage.from_dict({"cpu_user_seconds": "nan?"}) is None
