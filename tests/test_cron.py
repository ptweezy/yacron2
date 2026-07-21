import asyncio
import datetime
import os
import signal
import time
from collections import OrderedDict
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import cronstable.cron
from cronstable import platform
from cronstable.config import ConfigError, JobConfig, parse_config_string
from cronstable.job import JobOutputStream, JobRetryState, RunningJob
from tests._commands import cmd_hang, cmd_print, cmd_sleep, yaml_command


async def _noop():
    # awaitable stand-in for a monkeypatched async launch_scheduled_job
    return None


@pytest.fixture(autouse=True)
def fixed_current_time(monkeypatch):
    FIXED_TIME = datetime.datetime(
        year=1999, month=12, day=31, hour=12, minute=0, second=0
    )

    def get_now(timezone):
        now = FIXED_TIME
        if timezone is not None:
            if now.tzinfo is None:
                now = now.replace(tzinfo=timezone)
            else:
                now = now.astimezone(timezone)
        return now

    monkeypatch.setattr("cronstable.cron.get_now", get_now)


@pytest.fixture()
def tracing_running_job(monkeypatch):
    TracingRunningJob._TRACE = asyncio.Queue()
    monkeypatch.setattr(cronstable.cron, "RunningJob", TracingRunningJob)
    yield TracingRunningJob
    TracingRunningJob._TRACE = asyncio.Queue()


class TracingRunningJob(RunningJob):
    _TRACE = asyncio.Queue()

    def __init__(self, config: JobConfig, retry_state, **kwargs) -> None:
        super().__init__(config, retry_state, **kwargs)
        self._TRACE.put_nowait((time.perf_counter(), "create", self))

    async def start(self) -> None:
        self._TRACE.put_nowait((time.perf_counter(), "start", self))
        await super().start()
        self._TRACE.put_nowait((time.perf_counter(), "started", self))

    async def wait(self) -> None:
        self._TRACE.put_nowait((time.perf_counter(), "wait", self))
        await super().wait()
        self._TRACE.put_nowait((time.perf_counter(), "waited", self))

    async def cancel(self) -> None:
        self._TRACE.put_nowait((time.perf_counter(), "cancel", self))
        await super().cancel()
        self._TRACE.put_nowait((time.perf_counter(), "cancelled", self))

    async def report_failure(self):
        self._TRACE.put_nowait((time.perf_counter(), "report_failure", self))
        await super().report_failure()

    async def report_permanent_failure(self):
        self._TRACE.put_nowait(
            (time.perf_counter(), "report_permanent_failure", self)
        )
        await super().report_permanent_failure()

    async def report_success(self):
        self._TRACE.put_nowait((time.perf_counter(), "report_success", self))
        await super().report_success()


JOB_THAT_SUCCEEDS = (
    "jobs:\n  - name: test\n"
    + yaml_command(cmd_print(out="foobar"))
    + '\n    schedule: "@reboot"\n'
)

JOB_THAT_FAILS = (
    "jobs:\n  - name: test\n"
    + yaml_command(cmd_print(out="foobar", code=2))
    + '\n    schedule: "@reboot"\n'
)


@pytest.mark.parametrize(
    "config_yaml, expected_events",
    [
        (
            JOB_THAT_SUCCEEDS,
            ["create", "start", "started", "wait", "waited", "report_success"],
        ),
        (
            JOB_THAT_FAILS,
            [
                "create",
                "start",
                "started",
                "wait",
                "waited",
                "report_failure",
                "report_permanent_failure",
            ],
        ),
    ],
)
@pytest.mark.asyncio
async def test_simple(tracing_running_job, config_yaml, expected_events):
    cron = cronstable.cron.Cron(None, config_yaml=config_yaml)

    events = []

    async def wait_and_quit():
        the_job = None
        while True:
            ts, event, job = await tracing_running_job._TRACE.get()
            print(ts, event)
            if the_job is None:
                job = the_job
            else:
                assert job is the_job
            events.append(event)
            if event in {"report_success", "report_permanent_failure"}:
                break
        cron.signal_shutdown()

    await asyncio.gather(wait_and_quit(), cron.run())
    assert events == expected_events


RETRYING_JOB_THAT_FAILS = (
    "jobs:\n  - name: test\n"
    + yaml_command(cmd_print(out="foobar", code=2))
    + """
    schedule: "@reboot"
    onFailure:
      retry:
        maximumRetries: 2
        initialDelay: 0.1
        maximumDelay: 1
        backoffMultiplier: 2
"""
)


@pytest.mark.asyncio
async def test_fail_retry(tracing_running_job):
    cron = cronstable.cron.Cron(None, config_yaml=RETRYING_JOB_THAT_FAILS)

    events = []

    async def wait_and_quit():
        known_jobs = {}
        while True:
            ts, event, job = await tracing_running_job._TRACE.get()
            try:
                jobnum = known_jobs[job]
            except KeyError:
                if known_jobs:
                    jobnum = max(known_jobs.values()) + 1
                else:
                    jobnum = 1
                known_jobs[job] = jobnum
            print(ts, event, jobnum)
            events.append((jobnum, event))
            if jobnum == 3 and event == "report_permanent_failure":
                break
        cron.signal_shutdown()

    await asyncio.gather(wait_and_quit(), cron.run())
    assert events == [
        # initial attempt
        (1, "create"),
        (1, "start"),
        (1, "started"),
        (1, "wait"),
        (1, "waited"),
        (1, "report_failure"),
        # first retry
        (2, "create"),
        (2, "start"),
        (2, "started"),
        (2, "wait"),
        (2, "waited"),
        (2, "report_failure"),
        # second retry
        (3, "create"),
        (3, "start"),
        (3, "started"),
        (3, "wait"),
        (3, "waited"),
        (3, "report_failure"),
        (3, "report_permanent_failure"),
    ]


JOB_THAT_HANGS = (
    "jobs:\n  - name: test\n"
    + yaml_command(cmd_hang("starting...", 10))
    + """
    schedule: "@reboot"
    captureStdout: true
    executionTimeout: 0.25
    killTimeout: 0.25
"""
)


@pytest.mark.asyncio
async def test_execution_timeout(tracing_running_job):
    cron = cronstable.cron.Cron(None, config_yaml=JOB_THAT_HANGS)

    events = []
    jobs_stdout = {}

    async def wait_and_quit():
        known_jobs = {}
        while True:
            ts, event, job = await tracing_running_job._TRACE.get()
            try:
                jobnum = known_jobs[job]
            except KeyError:
                if known_jobs:
                    jobnum = max(known_jobs.values()) + 1
                else:
                    jobnum = 1
                known_jobs[job] = jobnum
            print(ts, event, jobnum)
            events.append((jobnum, event))
            if jobnum == 1 and event == "report_permanent_failure":
                jobs_stdout[jobnum] = job.stdout
                break
        cron.signal_shutdown()

    await asyncio.gather(wait_and_quit(), cron.run())
    assert events == [
        # initial attempt
        (1, "create"),
        (1, "start"),
        (1, "started"),
        (1, "wait"),
        (1, "cancel"),
        (1, "cancelled"),
        (1, "waited"),
        (1, "report_failure"),
        (1, "report_permanent_failure"),
    ]
    assert jobs_stdout[1] == "starting...\n"


CONCURRENT_JOB = (
    "jobs:\n  - name: test\n"
    + yaml_command(cmd_sleep(30))
    + """
    schedule: "@reboot"
    captureStdout: true
    concurrencyPolicy: {policy}
"""
)


@pytest.mark.parametrize("policy", ["Allow", "Forbid", "Replace"])
@pytest.mark.asyncio
async def test_concurrency_policy(policy):
    # Launch the same long-running job twice and assert the second launch is
    # handled per the policy. Driven directly (no wall-clock dependence) so it
    # is deterministic.
    cron = cronstable.cron.Cron(
        None, config_yaml=CONCURRENT_JOB.format(policy=policy)
    )
    job = cron.cron_jobs["test"]
    try:
        await cron.maybe_launch_job(job)  # first instance
        first = cron.running_jobs["test"][0]
        assert first.proc.returncode is None

        await cron.maybe_launch_job(job)  # second launch, subject to policy
        running = cron.running_jobs["test"]

        if policy == "Allow":
            assert len(running) == 2
            assert all(rj.proc.returncode is None for rj in running)
            assert first.replaced is False
        elif policy == "Forbid":
            # second launch skipped; the original instance is untouched
            assert running == [first]
            assert first.proc.returncode is None
            assert first.replaced is False
        else:  # Replace
            assert first.replaced is True
            # the first instance was actually terminated...
            assert first.proc.returncode is not None
            # ...and exactly one fresh instance is now running
            others = [rj for rj in running if rj is not first]
            assert len(others) == 1
            assert others[0].proc.returncode is None
    finally:
        for rj in list(cron.running_jobs.get("test", [])):
            if rj.proc is not None and rj.proc.returncode is None:
                await rj.cancel()


FAILED_SPAWN_REPLACE_JOB = (
    "jobs:\n  - name: test\n"
    + yaml_command(["cronstable-no-such-binary-xyz"])
    + """
    schedule: "@reboot"
    concurrencyPolicy: Replace
"""
)


@pytest.mark.asyncio
async def test_replace_policy_survives_failed_spawn():
    # A spawn failure registers the instance with proc=None (start_failed);
    # the next fire's Replace branch then cancels whatever running_jobs holds.
    # cancel() raising RuntimeError("process is not running") there used to
    # escape maybe_launch_job -- which spawn_jobs runs OUTSIDE run()'s
    # try/except -- and kill the whole scheduler on the second fire after a
    # bad deploy. (The cluster slot-renewer cancels through the same method,
    # so this guards that path too.)
    cron = cronstable.cron.Cron(None, config_yaml=FAILED_SPAWN_REPLACE_JOB)
    job = cron.cron_jobs["test"]

    await cron.maybe_launch_job(job)
    first = cron.running_jobs["test"][0]
    assert first.proc is None
    assert first.start_failed is True

    await cron.maybe_launch_job(job)  # Replace branch: must not raise
    assert first.replaced is True

    # the reaper still completes the never-spawned instance normally
    await first.wait()
    assert first.retcode == 127


@pytest.mark.asyncio
async def test_handle_finished_job_skips_replaced(monkeypatch):
    # a job cancelled to make way for a replacement must not be reported as a
    # success or failure (and must not trigger retries).
    from types import SimpleNamespace

    cron = cronstable.cron.Cron(None)
    calls = []

    async def fake_failure(job):
        calls.append(("failure", job))

    async def fake_success(job):
        calls.append(("success", job))

    monkeypatch.setattr(cron, "handle_job_failure", fake_failure)
    monkeypatch.setattr(cron, "handle_job_success", fake_success)

    job = SimpleNamespace(
        config=SimpleNamespace(name="test", concurrencyScope="node"),
        replaced=True,
        cancelled=False,
        fail_reason="failsWhen=nonzeroReturn and retcode=-15",
        retcode=-15,
        stdout=None,
        stderr=None,
        started_at=None,
        output=JobOutputStream(),
    )
    cron.running_jobs["test"].append(job)
    await cron._handle_finished_job(job)

    assert calls == []  # replaced -> neither reported
    assert "test" not in cron.running_jobs  # still cleaned up
    assert "test" not in cron.last_run  # replaced runs aren't recorded
    assert "test" not in cron.run_history  # nor added to history


@pytest.mark.asyncio
async def test_handle_finished_job_reports_normal_failure(monkeypatch):
    from types import SimpleNamespace

    cron = cronstable.cron.Cron(None)
    calls = []

    async def fake_failure(job):
        calls.append(("failure", job))

    async def fake_success(job):
        calls.append(("success", job))

    monkeypatch.setattr(cron, "handle_job_failure", fake_failure)
    monkeypatch.setattr(cron, "handle_job_success", fake_success)

    job = SimpleNamespace(
        config=SimpleNamespace(name="test", concurrencyScope="node"),
        replaced=False,
        cancelled=False,
        start_failed=False,
        fail_reason="failsWhen=nonzeroReturn and retcode=2",
        retcode=2,
        stdout=None,
        stderr=None,
        started_at=None,
        output=JobOutputStream(),
    )
    cron.running_jobs["test"].append(job)
    await cron._handle_finished_job(job)
    # the report+retry-arm sequence runs as a spawned per-job task now
    await cron._drain_completions()

    assert calls == [("failure", job)]
    # the finished run is recorded for the web UI
    assert cron.last_run["test"].outcome == "failure"
    assert cron.last_run["test"].exit_code == 2
    # ...and appended to the bounded run history
    assert [r.outcome for r in cron.run_history["test"]] == ["failure"]


@pytest.mark.asyncio
async def test_reaper_finishes_whole_batch_when_one_job_raises(
    monkeypatch, caplog
):
    # Regression: one job failing to finish must not take the rest of the
    # reaper's batch with it, nor strand the DAG-task completions that batch
    # has already buffered.
    #
    # Reachable in production: _handle_finished_job awaits
    # _job_api.finish_run, which touches the state backend (locks.release_all)
    # and can raise JobStateError("state backend is unavailable", 503). That
    # call used to be unguarded, with flush_completions after it inside the
    # same try, so a single raise (a) abandoned the jobs the batch had not
    # reached yet and (b) skipped the flush, leaving the completions buffered
    # by the jobs already handled RUNNING in their dag_run until some
    # unrelated later job reached the next flush; nothing else drains
    # _completion_buffer (_retry_completions only sees _pending_completions).
    # Batching is what turned a per-job failure into a cross-job one.
    import logging
    from types import SimpleNamespace

    from cronstable.jobstate import JobStateError

    cron = cronstable.cron.Cron(None)
    # shutdown already signalled, so the reaper returns as soon as the running
    # set drains: one batch is all this test needs.
    cron._stop_event.set()

    class FakeRunningJob:
        # only what the reaper touches: a wait() for an already-exited
        # process and a name for the log line. A class rather than a
        # SimpleNamespace because the reaper keys its wait-task map (and its
        # done set) by job, and SimpleNamespace is unhashable.
        def __init__(self, name):
            self.config = SimpleNamespace(name=name)

        async def wait(self):
            return None

    for name in ("t1", "t2", "t3"):
        cron.running_jobs[name] = [FakeRunningJob(name)]

    ref = ("dag", "run-key")
    handled = []

    async def fake_handle_finished_job(job):
        # mirrors the real handler's order: the instance leaves running_jobs
        # first, then finish_run (which is what 503s), then the completion is
        # buffered for the batch flush.
        cron.running_jobs.pop(job.config.name, None)
        handled.append(job.config.name)
        if len(handled) == 2:
            # the second job of the batch to be handled fails. done_jobs is a
            # set, so which job that is is not fixed; failing on the second
            # one guarantees both a completion already buffered (to strand)
            # and a job not yet reached (to abandon), whatever the order.
            raise JobStateError("state backend is unavailable", status=503)
        cron._dag._completion_buffer.setdefault(ref, []).append(
            {"taskkey": job.config.name}
        )

    recorded = []

    async def fake_flush_run_completions(run_ref, entries):
        recorded.extend((run_ref, entry["taskkey"]) for entry in entries)

    monkeypatch.setattr(cron, "_handle_finished_job", fake_handle_finished_job)
    monkeypatch.setattr(
        cron._dag, "_flush_run_completions", fake_flush_run_completions
    )

    reaper = asyncio.create_task(cron._wait_for_running_jobs())
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        try:
            # the fixed reaper awaits no timer: it drains the batch, flushes
            # and returns. Only a regression is still running here, parked in
            # the whole-loop handler's 1-second back-off.
            await asyncio.wait_for(reaper, timeout=0.5)
        except asyncio.TimeoutError:
            reaper.cancel()
            await asyncio.gather(reaper, return_exceptions=True)

    # (a) the raise did not abandon the rest of the batch
    assert sorted(handled) == ["t1", "t2", "t3"]
    # (b) ...nor skip the flush: every completion buffered around it was
    # recorded (the raiser never got as far as buffering its own), leaving
    # the buffer drained rather than stranded.
    assert sorted(key for _, key in recorded) == sorted(
        name for name in handled if name != handled[1]
    )
    assert all(run_ref == ref for run_ref, _ in recorded)
    assert cron._dag._completion_buffer == {}
    # and it stayed a per-job event: the whole-loop handler (whose back-off
    # stalls the reaper for a second) never saw it.
    messages = [r.message for r in caplog.records]
    assert any("bug (6)" in m for m in messages)
    assert not any("bug (3)" in m for m in messages)


@pytest.mark.asyncio
async def test_reaper_flushes_completions_even_when_the_batch_unwinds(
    monkeypatch,
):
    # The companion to the test above, pinning the OTHER half of the fix.
    # That one is satisfied by the per-job try/except alone: it swallows the
    # JobStateError before anything can escape the batch loop, so it would
    # still pass with the try/finally removed. This one uses CancelledError,
    # which the per-job guard deliberately re-raises, so the flush is reached
    # only because it sits in a finally. Without it, the completions the
    # batch had already buffered are lost on the way out.
    from types import SimpleNamespace

    cron = cronstable.cron.Cron(None)
    cron._stop_event.set()

    class FakeRunningJob:
        def __init__(self, name):
            self.config = SimpleNamespace(name=name)

        async def wait(self):
            return None

    for name in ("t1", "t2"):
        cron.running_jobs[name] = [FakeRunningJob(name)]

    ref = ("dag", "run-key")
    handled = []

    async def fake_handle_finished_job(job):
        cron.running_jobs.pop(job.config.name, None)
        handled.append(job.config.name)
        if len(handled) == 2:
            # escapes the per-job guard by design
            raise asyncio.CancelledError()
        cron._dag._completion_buffer.setdefault(ref, []).append(
            {"taskkey": job.config.name}
        )

    recorded = []

    async def fake_flush_run_completions(run_ref, entries):
        recorded.extend((run_ref, entry["taskkey"]) for entry in entries)

    monkeypatch.setattr(cron, "_handle_finished_job", fake_handle_finished_job)
    monkeypatch.setattr(
        cron._dag, "_flush_run_completions", fake_flush_run_completions
    )

    reaper = asyncio.create_task(cron._wait_for_running_jobs())
    # the cancellation propagates out of the reaper, which is correct: what
    # matters is that the buffered completion was flushed on the way.
    await asyncio.gather(reaper, return_exceptions=True)

    assert recorded == [(ref, handled[0])]
    assert cron._dag._completion_buffer == {}


def test_simple_config_file(tracing_running_job):
    config_arg = str(Path(__file__).parent / "testconfig.yaml")
    cronstable.cron.Cron(config_arg)


RETRYING_JOB_THAT_FAILS2 = (
    "jobs:\n  - name: test\n"
    + yaml_command(cmd_print(out="foobar", code=2))
    + """
    schedule: "@reboot"
    onFailure:
      retry:
        maximumRetries: 1
        initialDelay: 0.4
        maximumDelay: 1
        backoffMultiplier: 1
"""
)


@pytest.mark.asyncio
async def test_concurrency_and_backoff(monkeypatch, tracing_running_job):  # noqa: C901
    # This test runs against the REAL wall clock (get_now maps perf_counter
    # 1:1 onto simulated time from START_TIME), spawning two real subprocesses:
    # the @reboot job fails, then its single retry fires initialDelay (0.4s)
    # later. numjobs must reach 2 before the wait_and_quit loop stops at
    # STOP_TIME. Keep this window WIDE (2s): the retry's launch is anchored to
    # when the FIRST job's subprocess finishes failing, and Windows/CI process
    # spawn latency is both large and variable (100-300ms). A tight window
    # (the retry delay nearly filling it) leaves only tens of ms of slack, so a
    # slow spawn pushes the retry's launch past STOP_TIME and the second job is
    # never counted -- a load-sensitive flake seen on the windows-latest CI
    # runners (assert 1 == 2). Do not shrink it back.
    START_TIME = datetime.datetime(
        year=1999,
        month=12,
        day=31,
        hour=12,
        minute=0,
        second=59,
        microsecond=750000,
    )
    STOP_TIME = datetime.datetime(
        year=1999,
        month=12,
        day=31,
        hour=12,
        minute=1,
        second=1,
        microsecond=750000,
    )

    t0 = time.perf_counter()

    def get_now(timezone):
        now = START_TIME + datetime.timedelta(
            seconds=(time.perf_counter() - t0)
        )
        if timezone is not None:
            if now.tzinfo is None:
                now = now.replace(tzinfo=timezone)
            else:
                now = now.astimezone(timezone)
        return now

    def get_reltime(ts):
        return START_TIME + datetime.timedelta(seconds=(ts - t0))

    monkeypatch.setattr("cronstable.cron.get_now", get_now)

    cron = cronstable.cron.Cron(None, config_yaml=RETRYING_JOB_THAT_FAILS2)

    events = []
    numjobs = 0

    async def wait_and_quit():
        nonlocal numjobs
        known_jobs = {}
        pending_jobs = set()
        running_jobs = set()
        while get_now(None) < STOP_TIME:
            try:
                ts, event, job = await asyncio.wait_for(
                    tracing_running_job._TRACE.get(), 0.1
                )
            except asyncio.TimeoutError:
                continue
            try:
                jobnum = known_jobs[job]
            except KeyError:
                if known_jobs:
                    jobnum = max(known_jobs.values()) + 1
                else:
                    jobnum = 1
                known_jobs[job] = jobnum
                pending_jobs.add(jobnum)
                running_jobs.add(jobnum)
                numjobs += 1
            print(get_reltime(ts), event, jobnum)
            events.append((jobnum, event))
            if event in {"report_success", "report_permanent_failure"}:
                pending_jobs.discard(jobnum)
            if event in {
                "report_success",
                "report_permanent_failure",
                "cancelled",
            }:
                running_jobs.discard(jobnum)
        cron.signal_shutdown()

    await asyncio.gather(wait_and_quit(), cron.run())
    import pprint

    pprint.pprint(events)
    assert numjobs == 2


@pytest.mark.parametrize(
    "value_in, out",
    [
        (10, "in 10 seconds"),
        (305.0, "in 5 minutes"),
        (5000.0, "in 83 minutes"),
        (50000.0, "in 13 hours"),
        (500000.0, "in 5 days"),
    ],
)
def test_naturaltime(value_in, out):
    got_out = cronstable.cron.naturaltime(value_in)
    assert got_out == out


@pytest.mark.asyncio
async def test_schedule_retry_job_disappeared():
    # a job removed from config while a retry is pending must not raise
    # UnboundLocalError; the retry is simply skipped.
    cron = cronstable.cron.Cron(None)
    await cron.schedule_retry_job("nonexistent", 0.0, 0)
    assert "nonexistent" not in cron.retry_state


@pytest.mark.asyncio
async def test_schedule_retry_job_abandoned_when_no_longer_owner():
    # H1 regression: a retry can outlive the leadership it started under (a
    # partition / quorum loss moved ownership while it slept). It must re-check
    # the gate and abandon rather than relaunch -- relaunching here while the
    # new owner also runs it on its next tick is the split-brain double-run
    # the abstraction exists to prevent. Abandonment requires ANOTHER node to
    # be positively identified as the owner (a quorate, conflict-free view);
    # a transient denial defers instead (see the blip test below).
    import types

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    cron.cluster_manager = types.SimpleNamespace(
        distribution="single-leader",
        is_leader=lambda: False,  # leadership moved away...
        is_quorate=lambda: True,  # ...under a trustworthy quorate view
        is_available_leader=lambda: False,  # another node positively owns it
        has_conflict=lambda: False,
        view_settled=lambda: True,  # a converged view, not the settle hold
    )
    job = types.SimpleNamespace(name="j", clusterPolicy="Leader")
    cron.cron_jobs["j"] = job
    state = JobRetryState(0.1, 2, 1)
    cron.retry_state["j"] = state  # a pending retry
    await cron.schedule_retry_job("j", 0.0, 0)
    # abandoned (not relaunched) and the stale retry state cleared
    assert "j" not in cron.retry_state
    assert "j" not in cron.running_jobs
    # ...and the escaped-state relaunch path is closed (see the test below)
    assert state.cancelled is True


@pytest.mark.asyncio
async def test_schedule_retry_job_survives_transient_gate_blip(monkeypatch):
    # A retry waking during a TRANSIENT fail-closed condition (lost quorum, a
    # nodeName/size/policy conflict, a backend read error) must NOT abandon
    # the chain: this node may still be the rightful owner, and for the
    # wiki's keep-alive pattern (@reboot + maximumRetries: -1 + Leader) there
    # is no next scheduled firing -- reboot_ran was recorded before the first
    # launch, so an abandonment during a one-interval blip would mean no node
    # ever restarts the process. The retry defers and re-checks the gate,
    # relaunching once the blip clears.
    import types

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    quorate = False  # a one-interval quorum blip

    cron.cluster_manager = types.SimpleNamespace(
        distribution="single-leader",
        is_leader=lambda: quorate,  # this node leads again once quorum returns
        is_quorate=lambda: quorate,
        is_available_leader=lambda: True,  # nobody else positively owns it
        has_conflict=lambda: False,
        view_settled=lambda: True,
    )
    launched = []
    monkeypatch.setattr(
        cron,
        "maybe_launch_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = types.SimpleNamespace(name="j", clusterPolicy="Leader")
    cron.cron_jobs["j"] = job
    state = JobRetryState(0.01, 1, 0.01)
    cron.retry_state["j"] = state
    task = asyncio.create_task(cron.schedule_retry_job("j", 0.01, 1))
    await asyncio.sleep(0.1)  # the retry wakes mid-blip and defers
    assert launched == []  # not relaunched while the gate is closed...
    assert "j" in cron.retry_state  # ...but the chain survives the blip
    assert state.cancelled is False
    quorate = True  # blip over: this node is the owner again
    await asyncio.wait_for(task, timeout=5)
    assert launched == ["j"]  # the kept retry relaunched the job


@pytest.mark.asyncio
async def test_schedule_retry_job_defers_during_unsettled_view(
    monkeypatch, caplog
):
    # Reviewer regression on the abandon/defer split: a QUORATE but not yet
    # SETTLED view (a freshly rebuilt gossip manager whose current-build
    # agreeing peers have not all re-attested its new instance_id; quorum only
    # needs a majority, the settle hold waits for every such peer) holds
    # is_available_leader() False even on the rightful owner. That hold is a
    # transient fail-closed denial, not a positive ownership move: abandoning
    # there would end an @reboot keep-alive (maximumRetries: -1) chain
    # cluster-wide, since reboot_ran was recorded before the first launch.
    # The retry must defer and relaunch once the view settles.
    import types

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    settled = False  # the ~2-interval re-attestation window

    cron.cluster_manager = types.SimpleNamespace(
        distribution="single-leader",
        is_leader=lambda: False,
        is_quorate=lambda: True,  # quorate the whole time
        is_available_leader=lambda: settled,  # held closed while unsettled
        has_conflict=lambda: False,
        view_settled=lambda: settled,
    )
    # while unsettled, the gate denial must read as transient, never a move
    job = types.SimpleNamespace(name="j", clusterPolicy="PreferLeader")
    assert cron._cluster_owner_moved(job) is False
    launched = []
    monkeypatch.setattr(
        cron,
        "maybe_launch_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    monkeypatch.setattr(cronstable.cron, "RETRY_GATE_RECHECK_FLOOR", 0.01)
    cron.cron_jobs["j"] = job
    state = JobRetryState(0.01, 1, 0.01)
    cron.retry_state["j"] = state
    import logging

    with caplog.at_level(logging.DEBUG, logger="cronstable"):
        task = asyncio.create_task(cron.schedule_retry_job("j", 0.01, 1))
        await asyncio.sleep(0.1)  # the retry wakes mid-hold and defers
        assert launched == []  # not relaunched while the gates are held...
        assert "j" in cron.retry_state  # ...but the chain was NOT abandoned
        assert state.cancelled is False
        settled = True  # peers re-attested: the hold lifts, this node owns it
        await asyncio.wait_for(task, timeout=5)
    assert launched == ["j"]  # the kept retry relaunched the job
    # log cadence: the deferral is announced once at INFO; the re-checks that
    # follow (about one per second at the recheck floor) repeat only at DEBUG,
    # so a long gate-closed outage cannot spam the log.
    deferred = [r for r in caplog.records if "deferred" in r.message]
    assert len(deferred) > 1  # the loop really re-checked several times
    assert [r.levelno for r in deferred].count(logging.INFO) == 1
    assert all(r.levelno in (logging.INFO, logging.DEBUG) for r in deferred)
    # (a settled view where the gate STAYS False is the genuine move case,
    # covered by test_schedule_retry_job_abandoned_when_no_longer_owner)


@pytest.mark.asyncio
async def test_retry_abandonment_cancels_state_and_records(caplog):
    # The ownership-move abandonment must (a) set state.cancelled BEFORE
    # dropping the state: a RunningJob launched while the retry sat pending
    # (a manual API start, a concurrencyPolicy Allow overlap) captured this
    # same JobRetryState, and its own later failure would otherwise re-arm a
    # retry on the untracked state -- which cancel_job_retries can never find
    # or cancel, so the orphan would relaunch the job after a later success;
    # and (b) end the sequence loudly: a WARNING naming the actual cause plus
    # a run-history record, not one INFO line.
    import logging
    import types

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    cron.cluster_manager = types.SimpleNamespace(
        distribution="single-leader",
        is_leader=lambda: False,
        is_quorate=lambda: True,
        is_available_leader=lambda: False,  # another node positively owns it
        has_conflict=lambda: False,
        view_settled=lambda: True,
    )
    job = types.SimpleNamespace(name="j", clusterPolicy="Leader")
    cron.cron_jobs["j"] = job
    state = JobRetryState(0.1, 2, 1)
    cron.retry_state["j"] = state
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron.schedule_retry_job("j", 0.0, 1)
    assert "j" not in cron.retry_state
    assert state.cancelled is True  # kills the rogue-relaunch path
    warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "abandoned" in r.message
    ]
    assert len(warnings) == 1
    assert "moved ownership" in warnings[0].message
    # the sequence's end is visible in the run history / dashboard
    assert cron.last_run["j"].outcome == "cancelled"
    assert "ownership moved" in cron.last_run["j"].fail_reason
    assert [r.outcome for r in cron.run_history["j"]] == ["cancelled"]

    # rogue-relaunch closure: a concurrent RunningJob that captured this same
    # state must now end its own failing run permanently instead of re-arming
    # a retry on the untracked state.
    reported = []

    async def _report_failure():
        reported.append("failure")

    async def _report_permanent_failure():
        reported.append("permanent_failure")

    running = types.SimpleNamespace(
        config=types.SimpleNamespace(
            name="j",
            onFailure={
                "retry": {
                    "maximumRetries": -1,
                    "initialDelay": 0.1,
                    "maximumDelay": 1,
                    "backoffMultiplier": 2,
                }
            },
        ),
        stdout=None,
        stderr=None,
        retry_state=state,
        report_failure=_report_failure,
        report_permanent_failure=_report_permanent_failure,
    )
    await cron.handle_job_failure(running)
    assert reported == ["failure", "permanent_failure"]
    assert "j" not in cron.retry_state  # no orphan retry was re-armed


def test_cluster_allows_fails_closed_on_backend_error():
    # crash-safety: a backend read that raises must not escape _cluster_allows
    # (spawn_jobs runs outside the run loop's try/except, so it would kill the
    # scheduler); the gate fails closed instead.
    import types

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    def boom():
        raise RuntimeError("backend bug")

    cron.cluster_manager = types.SimpleNamespace(
        distribution="single-leader",
        is_leader=boom,
        is_available_leader=boom,
        has_conflict=lambda: False,
    )
    leader = types.SimpleNamespace(clusterPolicy="Leader", name="j")
    prefer = types.SimpleNamespace(clusterPolicy="PreferLeader", name="j")
    assert cron._cluster_allows(leader) is False
    assert cron._cluster_allows(prefer) is False
    # EveryNode never touches the backend, so it still runs
    every = types.SimpleNamespace(clusterPolicy="EveryNode", name="j")
    assert cron._cluster_allows(every) is True


def test_resolve_web_token_value():
    auth = {"value": "secret", "fromFile": None, "fromEnvVar": None}
    token = cronstable.cron.Cron._resolve_web_token({"authToken": auth})
    assert token == "secret"


def test_resolve_web_token_envvar(monkeypatch):
    monkeypatch.setenv("CRONSTABLE_TEST_WEB_TOKEN", "envsecret")
    token = cronstable.cron.Cron._resolve_web_token(
        {
            "authToken": {
                "value": None,
                "fromFile": None,
                "fromEnvVar": "CRONSTABLE_TEST_WEB_TOKEN",
            }
        }
    )
    assert token == "envsecret"


def test_resolve_web_token_absent():
    assert cronstable.cron.Cron._resolve_web_token({"listen": []}) is None


def test_resolve_web_token_missing_envvar_fails_closed(monkeypatch):
    # authToken configured but the env var is unset: must raise rather than
    # silently leaving the web API unauthenticated.
    monkeypatch.delenv("CRONSTABLE_TEST_WEB_TOKEN", raising=False)
    with pytest.raises(cronstable.config.ConfigError):
        cronstable.cron.Cron._resolve_web_token(
            {
                "authToken": {
                    "value": None,
                    "fromFile": None,
                    "fromEnvVar": "CRONSTABLE_TEST_WEB_TOKEN",
                }
            }
        )


def test_resolve_web_token_empty_value_fails_closed():
    with pytest.raises(cronstable.config.ConfigError):
        cronstable.cron.Cron._resolve_web_token(
            {
                "authToken": {
                    "value": None,
                    "fromFile": None,
                    "fromEnvVar": None,
                }
            }
        )


def test_resolve_web_token_empty_file_fails_closed(tmp_path):
    empty = tmp_path / "token"
    empty.write_text("   \n")
    with pytest.raises(cronstable.config.ConfigError):
        cronstable.cron.Cron._resolve_web_token(
            {
                "authToken": {
                    "value": None,
                    "fromFile": str(empty),
                    "fromEnvVar": None,
                }
            }
        )


@pytest.mark.asyncio
async def test_auth_middleware():
    from aiohttp import web

    middleware = cronstable.cron.Cron._make_auth_middleware("secret")

    async def handler(request):
        return web.Response(text="ok")

    class FakeRequest:
        def __init__(self, auth):
            self.headers = {"Authorization": auth} if auth else {}
            # the middleware consults these on non-bearer requests (the
            # .ics query-token carve-out); a real request always has them
            self.path = "/jobs"
            self.query = {}

    resp = await middleware(FakeRequest("Bearer secret"), handler)
    assert resp.text == "ok"

    with pytest.raises(web.HTTPUnauthorized):
        await middleware(FakeRequest("Bearer wrong"), handler)
    with pytest.raises(web.HTTPUnauthorized):
        await middleware(FakeRequest(None), handler)
    # a non-ASCII token must be a clean 401: compare_digest raises TypeError
    # (-> 500 + traceback) on any non-ASCII str operand.
    with pytest.raises(web.HTTPUnauthorized):
        await middleware(FakeRequest("Bearer café"), handler)
    # surrogates (raw header bytes that never decoded) cannot even encode --
    # and can never match a real token; still a 401, not a 500.
    with pytest.raises(web.HTTPUnauthorized):
        await middleware(FakeRequest("Bearer t\udc80k"), handler)


def test_origin_matches_host():
    m = cronstable.cron._origin_matches_host
    assert m("http://localhost:8021", "localhost:8021")
    # default ports: a browser omits them in BOTH headers of a same-origin
    # request, so both sides normalize from the Origin's scheme
    assert m("https://cron.example.com", "cron.example.com")
    assert m("http://cron.example.com", "cron.example.com")
    # hostname case-insensitivity (urlparse lowercases both sides)
    assert m("HTTP://LOCALHOST:8021", "LocalHost:8021")
    # bracketed IPv6 authority
    assert m("http://[::1]:8021", "[::1]:8021")
    # scheme deliberately ignored: a TLS-terminating proxy shows the daemon
    # plain http while the browser's Origin says https
    assert m("https://localhost:8021", "localhost:8021")
    assert not m("http://localhost:9999", "localhost:8021")
    assert not m("http://evil.example", "localhost:8021")
    # "null" (sandboxed iframe / redirect chain) and garbage fail closed
    assert not m("null", "localhost:8021")
    assert not m("garbage", "localhost:8021")
    assert not m("http://localhost:8021", None)
    assert not m("http://localhost:8021", "")
    # a malformed port on either side can never match (urlparse defers the
    # ValueError to .port; the helper must swallow it, not 500)
    assert not m("http://localhost:notaport", "localhost:8021")
    assert not m("http://localhost:8021", "localhost:notaport")


@pytest.mark.asyncio
async def test_origin_middleware_blocks_cross_site_mutations():
    from aiohttp import web

    middleware = cronstable.cron.Cron._make_origin_middleware(
        frozenset({"https://dash.example"})
    )

    async def handler(request):
        return web.Response(text="ok")

    class FakeRequest:
        def __init__(
            self,
            method="POST",
            origin=None,
            host="localhost:8021",
            path="/jobs/x/start",
        ):
            self.method = method
            self.headers = {} if origin is None else {"Origin": origin}
            self.host = host
            self.path = path

    # non-browser clients (curl, monitoring) send no Origin: unaffected
    resp = await middleware(FakeRequest(), handler)
    assert resp.text == "ok"
    # the served dashboard: same-origin POST passes
    resp = await middleware(
        FakeRequest(origin="http://localhost:8021"), handler
    )
    assert resp.text == "ok"
    # operator-trusted extra origin (web.allowedOrigins) passes
    resp = await middleware(
        FakeRequest(origin="https://dash.example"), handler
    )
    assert resp.text == "ok"
    # any other page the operator happens to visit: refused before the
    # handler runs -- the CSRF this middleware exists to stop
    with pytest.raises(web.HTTPForbidden):
        await middleware(FakeRequest(origin="https://evil.example"), handler)
    # "null" Origin fails closed
    with pytest.raises(web.HTTPForbidden):
        await middleware(FakeRequest(origin="null"), handler)
    # safe methods pass untouched (reads mutate nothing; the browser's
    # same-origin policy already hides their responses cross-site)
    resp = await middleware(
        FakeRequest(method="GET", origin="https://evil.example"), handler
    )
    assert resp.text == "ok"
    # /mcp enforces its own allow-list (mcp.allowedOrigins) and must stay
    # reachable for origins allow-listed THERE: exempt from this gate
    resp = await middleware(
        FakeRequest(origin="https://evil.example", path="/mcp"), handler
    )
    assert resp.text == "ok"


@pytest.mark.asyncio
async def test_web_app_origin_gate_end_to_end():
    # the gate is wired into the real app even with NO authToken configured
    # (the default posture the CSRF gate exists for): a cross-site POST is
    # refused before the handler, while same-origin and Origin-less requests
    # reach it (409 -- the job is disabled -- proves the handler answered).
    import aiohttp

    cron = cronstable.cron.Cron(None, config_yaml=DISABLED_JOB)
    await cron.start_stop_web_app({"listen": ["http://127.0.0.1:0"]})
    try:
        port = cron.web_runner.addresses[0][1]
        base = "http://127.0.0.1:{}".format(port)
        async with aiohttp.ClientSession() as session:
            async with session.post(
                base + "/jobs/test/start",
                headers={"Origin": "https://evil.example"},
            ) as resp:
                assert resp.status == 403
            async with session.post(
                base + "/jobs/test/start", headers={"Origin": base}
            ) as resp:
                assert resp.status == 409
            async with session.post(base + "/jobs/test/start") as resp:
                assert resp.status == 409
    finally:
        await cron.start_stop_web_app(None)
        # let the Proactor's connection transports finish closing before the
        # loop is torn down (the aiohttp-documented Windows grace period);
        # otherwise their GC-time repr can raise "I/O operation on closed
        # pipe" as a PytestUnraisableExceptionWarning.
        await asyncio.sleep(0.25)


def test_web_site_from_url_bad_scheme():
    with pytest.raises(ValueError):
        cronstable.cron.web_site_from_url(None, "ftp://localhost:1234")


def test_web_site_from_url_malformed_http():
    # missing host/port must raise ValueError (a skippable bad entry), not
    # AssertionError (which would be reported as an internal cronstable bug).
    with pytest.raises(ValueError):
        cronstable.cron.web_site_from_url(None, "http://")


@pytest.mark.asyncio
async def test_start_web_app_ignores_bad_listen_urls():
    # an unusable listen url is skipped, not surfaced as an exception
    cron = cronstable.cron.Cron(None)
    bad_config = {"listen": ["ftp://localhost:1234", "http://"]}
    try:
        await cron.start_stop_web_app(bad_config)  # must not raise
    finally:
        await cron.start_stop_web_app(None)  # tear down the runner


DISABLED_JOB = """
jobs:
  - name: test
    command: echo hi
    schedule: "* * * * *"
    enabled: false
"""


@pytest.mark.asyncio
async def test_web_start_disabled_job_refused():
    from aiohttp import web

    cron = cronstable.cron.Cron(None, config_yaml=DISABLED_JOB)
    cron.web_config = {}

    class Req:
        match_info = {"name": "test"}
        headers: dict = {}

    with pytest.raises(web.HTTPConflict):
        await cron._web_start_job(Req())
    # the disabled job must not have been launched
    assert not cron.running_jobs


@pytest.mark.asyncio
async def test_web_status_reports_disabled():
    import json

    cron = cronstable.cron.Cron(None, config_yaml=DISABLED_JOB)
    cron.web_config = {}

    class Req:
        headers = {"Accept": "application/json"}

    resp = await cron._web_get_status(Req())
    data = json.loads(resp.text)
    assert data[0]["status"] == "disabled"


TWO_JOBS = """
jobs:
  - name: alpha
    command: echo alpha
    schedule: "*/5 * * * *"
    captureStdout: true
  - name: beta
    command:
      - echo
      - beta
    schedule: "@reboot"
    enabled: false
"""


@pytest.mark.asyncio
async def test_web_list_jobs():
    import json

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    class Req:
        headers: dict = {}

    resp = await cron._web_list_jobs(Req())
    data = json.loads(resp.text)
    assert [j["name"] for j in data] == ["alpha", "beta"]
    assert resp.headers.get("ETag")  # content ETag present for caches

    alpha = data[0]
    assert alpha["enabled"] is True
    assert alpha["schedule"] == "*/5 * * * *"
    assert alpha["command"] == "echo alpha"
    assert alpha["captureStdout"] is True
    assert alpha["running"] is False
    assert alpha["scheduled_in"] is not None  # next run computed
    assert alpha["last_run"] is None  # never run yet

    beta = data[1]
    assert beta["enabled"] is False
    assert beta["command"] == "echo beta"  # argv list joined for display
    assert beta["scheduled_in"] is None  # disabled -> no next run


@pytest.mark.asyncio
async def test_web_list_jobs_etag_304_and_invalidation():
    """GET /jobs serves a content ETag, 304s a matching conditional poll,
    keeps the tag stable while only the countdown moves, and moves it when
    job state changes."""
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    def req(inm=None):
        class Req:
            headers = {} if inm is None else {"If-None-Match": inm}

        return Req()

    first = await cron._web_list_jobs(req())
    etag = first.headers["ETag"]
    assert first.status == 200 and etag

    # a second poll with no change re-serves 200 with the SAME tag: the
    # relative countdown is not part of it (it is derived from the absolute
    # next-fire), so an idle poll is byte-identical.
    again = await cron._web_list_jobs(req())
    assert again.status == 200
    assert again.headers["ETag"] == etag

    # a conditional poll carrying that tag is told nothing changed.
    not_modified = await cron._web_list_jobs(req(etag))
    assert not_modified.status == 304
    assert not_modified.body in (None, b"")
    assert not_modified.headers["ETag"] == etag

    # a real state change (advancing a job's next fire) moves the tag, so
    # the same conditional poll now gets a fresh body instead of a 304.
    when = cron._next_fire.get("alpha")
    cron._next_fire["alpha"] = (
        when or DT(2000, 1, 1, tzinfo=UTC)
    ) + datetime.timedelta(hours=1)
    changed = await cron._web_list_jobs(req(etag))
    assert changed.status == 200
    assert changed.headers["ETag"] != etag


@pytest.mark.asyncio
async def test_web_job_set_id():
    import json

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    class Req:
        headers: dict = {}

    resp = await cron._web_job_set_id(Req())
    assert resp.text == cron.job_set_id()
    # the id always carries the live scheme label (see cronstable.fingerprint;
    # the golden-value tests pin the actual version)
    from cronstable.fingerprint import SCHEME_VERSION

    assert resp.text.startswith(SCHEME_VERSION + ":")

    class JsonReq:
        headers = {"Accept": "application/json"}

    resp = await cron._web_job_set_id(JsonReq())
    data = json.loads(resp.text)
    assert data["job_set_id"] == cron.job_set_id()
    assert data["jobs"] == 2


def test_job_set_id_logged_only_on_change(caplog):
    import logging

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    with caplog.at_level(logging.INFO, logger="cronstable"):
        cron._log_job_set_id()
        cron._log_job_set_id()  # unchanged: must not log again
    logged = [r.message for r in caplog.records if "Job set id" in r.message]
    assert len(logged) == 1
    assert cron.job_set_id() in logged[0]


@pytest.mark.asyncio
async def test_web_list_jobs_includes_last_run():
    import json

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}
    cron.last_run["alpha"] = cronstable.cron.JobRunInfo(
        outcome="failure",
        exit_code=2,
        started_at=DT(1999, 12, 31, 12, 0, 0, tzinfo=UTC),
        finished_at=DT(1999, 12, 31, 12, 0, 5, tzinfo=UTC),
        fail_reason="failsWhen=nonzeroReturn and retcode=2",
        output=JobOutputStream(),
    )

    class Req:
        headers: dict = {}

    resp = await cron._web_list_jobs(Req())
    data = json.loads(resp.text)
    last = data[0]["last_run"]
    assert last["outcome"] == "failure"
    assert last["exit_code"] == 2
    assert last["duration"] == 5.0
    assert last["fail_reason"].startswith("failsWhen")


def _mk_run(outcome, exit_code=0, dur=1.0):
    start = DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC)
    return cronstable.cron.JobRunInfo(
        outcome=outcome,
        exit_code=exit_code,
        started_at=start,
        finished_at=start + datetime.timedelta(seconds=dur),
        fail_reason=None if outcome == "success" else "boom",
        output=JobOutputStream(),
    )


def test_job_run_info_resources_round_trip():
    from cronstable.resources import ResourceUsage

    run = _mk_run("success")
    run.resource_usage = ResourceUsage(1.0, 0.5, 9000, 4)
    d = run.to_dict()
    assert d["resources"]["cpu_total_seconds"] == 1.5
    assert d["resources"]["max_rss_bytes"] == 9000
    # rehydrate from the ledger record
    restored = cronstable.cron._job_run_info_from_dict(d)
    assert restored is not None
    assert restored.resource_usage == run.resource_usage


def test_job_run_info_round_trip_without_resources():
    d = _mk_run("success").to_dict()
    assert d["resources"] is None
    restored = cronstable.cron._job_run_info_from_dict(d)
    assert restored is not None
    assert restored.resource_usage is None


def test_run_stats_cpu_and_memory_aggregates():
    from cronstable.resources import ResourceUsage

    runs = []
    for cpu, rss in ((1.0, 1000), (3.0, 5000)):
        r = _mk_run("success")
        r.resource_usage = ResourceUsage(cpu, 0.0, rss, 1)
        runs.append(r)
    # one unmonitored run in the window: it must not skew the averages
    runs.append(_mk_run("success"))
    stats = cronstable.cron._run_stats(runs)
    assert stats["avg_cpu_seconds"] == 2.0
    assert stats["max_cpu_seconds"] == 3.0
    assert stats["max_rss_bytes"] == 5000
    # the last run was unmonitored -> last_* are None
    assert stats["last_cpu_seconds"] is None
    assert stats["last_rss_bytes"] is None


def test_run_stats_no_monitored_runs_leaves_resource_fields_none():
    stats = cronstable.cron._run_stats(
        [_mk_run("success"), _mk_run("failure")]
    )
    assert stats["avg_cpu_seconds"] is None
    assert stats["max_rss_bytes"] is None
    assert stats["last_cpu_seconds"] is None


def test_record_run_caps_history():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    limit = cronstable.cron.RUN_HISTORY_LIMIT
    for i in range(limit + 10):
        cron._record_run("alpha", _mk_run("success", exit_code=i))
    hist = cron.run_history["alpha"]
    assert len(hist) == limit  # bounded ring buffer
    # oldest entries evicted; newest retained and ordered oldest-first
    assert hist[0].exit_code == 10
    assert hist[-1].exit_code == limit + 9
    # last_run mirrors the most recent recorded run
    assert cron.last_run["alpha"].exit_code == limit + 9


class _FakeMesh:
    """A stand-in leadership backend capturing provider installs/lifecycle."""

    def __init__(self, config):
        self.config = config
        self.job_summaries_provider = None
        self.node_stats_provider = None
        self.started = False
        self.stopped = False

    def set_job_summaries_provider(self, p):
        self.job_summaries_provider = p

    def set_node_stats_provider(self, p, share=True):
        self.node_stats_provider = p
        self.node_stats_share = share

    def tls_files_changed(self):
        return False

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True


def test_fleet_backend_prefers_observability_mesh():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.cluster_manager = object()
    assert cron._fleet_backend() is cron.cluster_manager
    mesh = object()
    cron.observability_mesh = mesh
    assert cron._fleet_backend() is mesh


@pytest.mark.asyncio
async def test_start_stop_observability_builds_mesh_and_installs_providers(
    monkeypatch,
):
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    built = []
    monkeypatch.setattr(
        cronstable.cron,
        "make_backend",
        lambda cfg, jsid: built.append(_FakeMesh(cfg)) or built[-1],
    )
    cluster_config = {
        "observabilityMesh": {"backend": "gossip", "marker": 1},
        "shareNodeStats": True,
    }
    await cron.start_stop_observability(cluster_config)
    assert cron.observability_mesh is built[0]
    assert built[0].started is True
    # both fleet providers installed on the overlay mesh, node stats SHARED
    assert built[0].job_summaries_provider == cron.fleet_job_summaries
    assert built[0].node_stats_provider == cron.node_resource_snapshot
    assert built[0].node_stats_share is True
    # a reload dropping the observability section tears the mesh down
    await cron.start_stop_observability({"observabilityMesh": None})
    assert cron.observability_mesh is None
    assert built[0].stopped is True


@pytest.mark.asyncio
async def test_start_stop_observability_respects_share_opt_out(monkeypatch):
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    made = []
    monkeypatch.setattr(
        cronstable.cron,
        "make_backend",
        lambda cfg, jsid: made.append(_FakeMesh(cfg)) or made[-1],
    )
    # mesh configured (for job summaries) but shareNodeStats off
    await cron.start_stop_observability(
        {"observabilityMesh": {"backend": "gossip"}, "shareNodeStats": False}
    )
    assert made[0].job_summaries_provider == cron.fleet_job_summaries
    # the provider is still installed (for the overlay's own self readout) but
    # NOT gossiped to peers
    assert made[0].node_stats_provider == cron.node_resource_snapshot
    assert made[0].node_stats_share is False


@pytest.mark.asyncio
async def test_start_stop_observability_reconciles_share_on_kept_mesh(
    monkeypatch,
):
    # shareNodeStats lives on the CLUSTER config, not on the resolved mesh
    # config the keep/rebuild comparison sees, so a toggle keeps the running
    # mesh: the latched share flag must be re-reconciled every reload, or
    # toggling off would keep gossiping CPU/memory until an unrelated restart
    # and toggling on would never start.
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    made = []
    monkeypatch.setattr(
        cronstable.cron,
        "make_backend",
        lambda cfg, jsid: made.append(_FakeMesh(cfg)) or made[-1],
    )
    mesh_cfg = {"backend": "gossip", "marker": 1}
    await cron.start_stop_observability(
        {"observabilityMesh": mesh_cfg, "shareNodeStats": True}
    )
    assert made[0].node_stats_share is True
    # toggle OFF: the mesh config is unchanged, so the mesh is KEPT...
    await cron.start_stop_observability(
        {"observabilityMesh": mesh_cfg, "shareNodeStats": False}
    )
    assert cron.observability_mesh is made[0]
    assert made[0].stopped is False
    # ...and the running mesh sees the new share value
    assert made[0].node_stats_share is False
    # toggling back ON reaches the kept mesh too
    await cron.start_stop_observability(
        {"observabilityMesh": mesh_cfg, "shareNodeStats": True}
    )
    assert cron.observability_mesh is made[0]
    assert made[0].node_stats_share is True


@pytest.mark.asyncio
async def test_start_stop_observability_none_is_noop():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    await cron.start_stop_observability(None)
    assert cron.observability_mesh is None
    await cron.start_stop_observability({"observabilityMesh": None})
    assert cron.observability_mesh is None


@pytest.mark.asyncio
async def test_web_get_cluster_injects_local_node_stats():
    import json

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    class FakeMgr:
        def view_dict(self):
            return {"backend": "gossip", "peers": []}

    cron.cluster_manager = FakeMgr()

    class Req:
        pass

    resp = await cron._web_get_cluster(Req())
    data = json.loads(resp.text)
    # this node's own live load is always injected (local, free)
    assert data["node_stats"] is not None
    assert "cpu_percent" in data["node_stats"]


@pytest.mark.asyncio
async def test_web_get_node_returns_resources():
    import json

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    class Req:
        pass

    resp = await cron._web_get_node(Req())
    data = json.loads(resp.text)
    assert data["node_name"]
    # psutil is a core dep, so the node snapshot is populated in tests
    assert data["resources"] is not None
    assert "cpu_percent" in data["resources"]
    assert "mem_percent" in data["resources"]


@pytest.mark.asyncio
async def test_job_to_dict_includes_live_running_resources():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    job = cron.cron_jobs["alpha"]

    class FakeRunning:
        proc = None

        def live_resources(self):
            return {"cpu_percent": 40.0, "cpu_seconds": 2.0, "rss_bytes": 1000}

    cron.running_jobs["alpha"] = [FakeRunning(), FakeRunning()]
    d = cron._job_to_dict("alpha", job)
    # summed across the two running instances
    assert d["running_resources"] == {
        "cpu_percent": 80.0,
        "cpu_seconds": 4.0,
        "rss_bytes": 2000,
        "instances": 2,
    }


@pytest.mark.asyncio
async def test_job_to_dict_omits_running_resources_when_unmonitored():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    job = cron.cron_jobs["alpha"]

    class FakeRunning:
        proc = None

        def live_resources(self):
            return None  # unmonitored / no sample yet

    cron.running_jobs["alpha"] = [FakeRunning()]
    d = cron._job_to_dict("alpha", job)
    assert "running_resources" not in d


@pytest.mark.asyncio
async def test_web_list_jobs_includes_history_and_timezone():
    import json

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}
    for outcome in ("success", "failure", "success"):
        cron._record_run("alpha", _mk_run(outcome))

    class Req:
        headers: dict = {}

    resp = await cron._web_list_jobs(Req())
    data = json.loads(resp.text)
    alpha = data[0]
    # inline compact history (oldest first) for the table sparkline
    assert [h["outcome"] for h in alpha["history"]] == [
        "success",
        "failure",
        "success",
    ]
    # schedule reference frame exposed for client-side next-run computation;
    # a utc:true job (the default) resolves to the "UTC" zone
    assert alpha["utc"] is True
    assert alpha["timezone"] == "UTC"
    # a job that never ran reports an empty (not missing) history
    assert data[1]["history"] == []


@pytest.mark.asyncio
async def test_web_job_runs_endpoint_returns_runs_and_stats():
    import json

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}
    cron._record_run("alpha", _mk_run("success", dur=2.0))
    cron._record_run("alpha", _mk_run("failure", dur=4.0))
    cron._record_run("alpha", _mk_run("success", dur=6.0))
    cron._record_run("alpha", _mk_run("cancelled", dur=1.0))

    class Req:
        match_info = {"name": "alpha"}

    resp = await cron._web_job_runs(Req())
    body = json.loads(resp.text)
    assert body["name"] == "alpha"
    assert [r["outcome"] for r in body["runs"]] == [
        "success",
        "failure",
        "success",
        "cancelled",
    ]
    stats = body["stats"]
    assert stats["total"] == 4
    assert stats["success"] == 2
    assert stats["failure"] == 1
    assert stats["cancelled"] == 1
    # success rate excludes cancellations: 2 success / (2 success + 1 failure)
    assert stats["success_rate"] == pytest.approx(2 / 3)
    assert stats["avg_duration"] == pytest.approx((2 + 4 + 6 + 1) / 4)
    assert stats["min_duration"] == 1.0
    assert stats["max_duration"] == 6.0
    assert stats["last_duration"] == 1.0


@pytest.mark.asyncio
async def test_web_job_runs_unknown_job_404():
    from aiohttp import web

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    class Req:
        match_info = {"name": "nope"}

    with pytest.raises(web.HTTPNotFound):
        await cron._web_job_runs(Req())


@pytest.mark.asyncio
async def test_web_job_runs_empty_history():
    import json

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    class Req:
        match_info = {"name": "alpha"}

    resp = await cron._web_job_runs(Req())
    body = json.loads(resp.text)
    assert body["runs"] == []
    assert body["stats"]["total"] == 0
    assert body["stats"]["success_rate"] is None
    assert body["stats"]["avg_duration"] is None


@pytest.mark.asyncio
async def test_web_cancel_unknown_job_404():
    from aiohttp import web

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    class Req:
        match_info = {"name": "nope"}

    with pytest.raises(web.HTTPNotFound):
        await cron._web_cancel_job(Req())


@pytest.mark.asyncio
async def test_web_cancel_not_running_409():
    from aiohttp import web

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    class Req:
        match_info = {"name": "alpha"}
        headers: dict = {}

    with pytest.raises(web.HTTPConflict):
        await cron._web_cancel_job(Req())


@pytest.mark.asyncio
async def test_handle_finished_job_records_cancelled(monkeypatch):
    # a run cancelled by the user is recorded as "cancelled" but, like a
    # replacement, must not be reported as success/failure or retried.
    from types import SimpleNamespace

    cron = cronstable.cron.Cron(None)
    calls = []

    async def fake_failure(job):
        calls.append(("failure", job))

    async def fake_success(job):
        calls.append(("success", job))

    monkeypatch.setattr(cron, "handle_job_failure", fake_failure)
    monkeypatch.setattr(cron, "handle_job_success", fake_success)

    job = SimpleNamespace(
        config=SimpleNamespace(name="test", concurrencyScope="node"),
        replaced=False,
        cancelled=True,
        fail_reason=None,
        retcode=-15,
        stdout=None,
        stderr=None,
        started_at=None,
        output=JobOutputStream(),
    )
    cron.running_jobs["test"].append(job)
    await cron._handle_finished_job(job)

    assert calls == []  # neither reported
    assert "test" not in cron.running_jobs  # cleaned up
    assert cron.last_run["test"].outcome == "cancelled"
    assert cron.last_run["test"].exit_code == -15
    assert [r.outcome for r in cron.run_history["test"]] == ["cancelled"]


@pytest.mark.asyncio
async def test_web_cancel_running_job_terminates_and_records():
    # end-to-end: launch a real long-running job, cancel it via the endpoint,
    # and confirm it is actually terminated and recorded as "cancelled".
    cron = cronstable.cron.Cron(
        None, config_yaml=CONCURRENT_JOB.format(policy="Allow")
    )
    cron.web_config = {}
    job = cron.cron_jobs["test"]
    await cron.maybe_launch_job(job)
    rj = cron.running_jobs["test"][0]
    assert rj.proc.returncode is None

    class Req:
        match_info = {"name": "test"}
        headers: dict = {}

    resp = await cron._web_cancel_job(Req())
    assert resp.status == 200
    assert rj.cancelled is True
    assert rj.proc.returncode is not None  # process actually terminated

    # the reaper would normally do this once the process exits; drive it here
    await cron._handle_finished_job(rj)
    assert "test" not in cron.running_jobs
    assert cron.last_run["test"].outcome == "cancelled"
    assert [r.outcome for r in cron.run_history["test"]] == ["cancelled"]


@pytest.mark.asyncio
async def test_web_index_served():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    class Req:
        pass

    resp = await cron._web_index(Req())
    assert resp.content_type == "text/html"
    assert "cronstable" in resp.text
    assert "<html" in resp.text.lower()


@pytest.mark.asyncio
async def test_web_index_sets_security_headers():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    class Req:
        pass

    resp = await cron._web_index(Req())
    csp = resp.headers["Content-Security-Policy"]
    # self-contained app: no external connections, so the CSP confines any
    # injected script to this origin and blocks framing of the action controls
    assert "connect-src 'self'" in csp
    assert "frame-ancestors 'none'" in csp
    assert "object-src 'none'" in csp
    assert resp.headers["X-Content-Type-Options"] == "nosniff"
    assert resp.headers["X-Frame-Options"] == "DENY"
    assert resp.headers["Referrer-Policy"] == "no-referrer"


@pytest.mark.asyncio
async def test_web_index_security_headers_overridable():
    # an operator-configured web.headers value wins over the secure default,
    # while defaults the operator didn't set are still applied.
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {"headers": {"X-Frame-Options": "SAMEORIGIN"}}

    class Req:
        pass

    resp = await cron._web_index(Req())
    assert resp.headers["X-Frame-Options"] == "SAMEORIGIN"  # operator override
    assert resp.headers["X-Content-Type-Options"] == "nosniff"  # default kept


@pytest.mark.asyncio
async def test_auth_middleware_public_path():
    from aiohttp import web

    middleware = cronstable.cron.Cron._make_auth_middleware(
        "secret", cronstable.cron.WEB_PUBLIC_PATHS
    )

    async def handler(request):
        return web.Response(text="ok")

    class FakeRequest:
        def __init__(self, path, auth=None):
            self.path = path
            self.headers = {"Authorization": auth} if auth else {}

    # the UI page is reachable without a token...
    resp = await middleware(FakeRequest("/"), handler)
    assert resp.text == "ok"
    # ...but data endpoints still require it
    with pytest.raises(web.HTTPUnauthorized):
        await middleware(FakeRequest("/jobs"), handler)


@pytest.mark.asyncio
async def test_web_job_logs_streams_last_run():
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}
    out = JobOutputStream()
    out.publish("stdout", "hello world\n")
    out.publish("stderr", "uh oh\n")
    out.close()
    cron.last_run["alpha"] = cronstable.cron.JobRunInfo(
        outcome="success",
        exit_code=0,
        started_at=None,
        finished_at=DT(1999, 12, 31, 12, 0, 0, tzinfo=UTC),
        fail_reason=None,
        output=out,
    )
    app = web.Application()
    app.router.add_get("/jobs/{name}/logs", cron._web_job_logs)
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/jobs/alpha/logs")
        assert resp.status == 200
        assert resp.content_type == "text/event-stream"
        body = await resp.text()
    # buffered lines of the last run are replayed, then the stream ends
    assert "event: line" in body
    assert "hello world" in body
    assert "uh oh" in body
    assert "event: end" in body


@pytest.mark.asyncio
async def test_web_job_logs_no_output():
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}
    app = web.Application()
    app.router.add_get("/jobs/{name}/logs", cron._web_job_logs)
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/jobs/alpha/logs")  # never run
        assert resp.status == 200
        body = await resp.text()
    assert "no-output" in body


@pytest.mark.asyncio
async def test_web_job_logs_unknown_job():
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}
    app = web.Application()
    app.router.add_get("/jobs/{name}/logs", cron._web_job_logs)
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/jobs/nope/logs")
        assert resp.status == 404


DT = datetime.datetime
UTC = datetime.timezone.utc
LONDON = ZoneInfo("Europe/London")


@pytest.mark.parametrize(
    "schedule, timezone, utc, now, startup, enabled, result",
    [
        (
            "* * * * *",
            "",
            "",
            DT(2020, 7, 20, 14, 59, 1, tzinfo=UTC),
            False,
            "",
            True,
        ),
        (
            "59 14 * * *",
            "",
            "",
            DT(2020, 7, 20, 14, 59, 1, tzinfo=UTC),
            False,
            "",
            True,
        ),
        (
            "59 14 * * *",
            "",
            "",
            DT(2020, 7, 20, 14, 59, 1, tzinfo=UTC),
            True,  # startup
            "",
            False,
        ),
        (
            "49 14 * * *",
            "",
            "",
            DT(2020, 7, 20, 14, 59, 1, tzinfo=UTC),
            False,
            "",
            False,
        ),
        (
            "59 14 * * *",
            "",
            "utc: true",
            DT(2020, 7, 20, 14, 59, 1, tzinfo=UTC),
            False,
            "",
            True,
        ),
        (
            "59 14 * * *",
            "",
            "utc: true",  # London is UTC+1 during DST
            DT(2020, 7, 20, 14, 59, 1, tzinfo=UTC).astimezone(LONDON),
            False,
            "",
            True,
        ),
        (
            "59 14 * * *",
            "",
            "utc: false",  # London is UTC+1 during DST
            DT(2020, 7, 20, 14, 59, 1, tzinfo=UTC).astimezone(LONDON),
            False,
            "",
            False,
        ),
        (
            "1 8 * * *",
            "timezone: America/Los_Angeles",
            "",
            DT(2020, 7, 20, 15, 1, 1, tzinfo=UTC),
            False,
            "",
            True,
        ),
        (
            "1 8 * * *",
            "timezone: Europe/London",
            "",
            DT(2020, 7, 20, 15, 1, 1, tzinfo=UTC),
            False,
            "",
            False,
        ),
        (
            "@reboot",
            "",
            "",
            DT(2020, 7, 20, 15, 1, 1, tzinfo=UTC),
            False,
            "",
            False,
        ),
        (
            "@reboot",
            "",
            "",
            DT(2020, 7, 20, 15, 1, 1, tzinfo=UTC),
            True,
            "",
            True,
        ),
        # enabled: false
        (
            "* * * * *",
            "",
            "",
            DT(2020, 7, 20, 14, 59, 1, tzinfo=UTC),
            False,
            "enabled: false",
            False,
        ),
    ],
)
def test_job_should_run(
    monkeypatch, schedule, timezone, utc, now, startup, enabled, result
):
    def get_now(timezone):
        print("timezone: ", timezone)
        retval = now
        if timezone is not None:
            retval = retval.astimezone(timezone)
        print("now: ", retval)
        return retval

    monkeypatch.setattr("cronstable.cron.get_now", get_now)

    config_yaml = f"""
jobs:
  - name: test
    command: |
      echo "foobar"
    schedule: "{schedule}"
    {timezone}
    {utc}
    {enabled}
                            """
    print(config_yaml)
    cron = cronstable.cron.Cron(None, config_yaml=config_yaml)
    job = list(cron.cron_jobs.values())[0]
    assert cron.job_should_run(startup, job) == result


@pytest.mark.asyncio
async def test_run_survives_config_error(tmp_path, monkeypatch):
    # If the reparse raises (e.g. the config became invalid on reload), run()
    # must log it and keep running the previously-loaded jobs, not crash with
    # UnboundLocalError when the housekeeping block later inspects `config`.
    # The reparse now runs off the event loop (reload_config ->
    # run_in_executor(parse_config)), so make parse_config itself fail after a
    # clean load at construction.
    cfg = tmp_path / "c.yaml"
    cfg.write_text(TWO_JOBS)
    cron = cronstable.cron.Cron(str(cfg))
    assert set(cron.cron_jobs) == {"alpha", "beta"}
    monkeypatch.setattr("cronstable.cron.next_sleep_interval", lambda *a: 0.01)

    def boom(*args, **kwargs):
        raise ConfigError("boom")

    # reload_config now skips the reparse when the file is unchanged on disk,
    # so touch it (a real "config edited to something invalid on reload"
    # scenario bumps mtime) to defeat the skip; the failed parse never records
    # a new fingerprint, so every subsequent tick still sees the change and
    # retries.
    cfg.write_text(TWO_JOBS + "\n# edited\n")
    monkeypatch.setattr("cronstable.cron.parse_config_with_sources", boom)

    task = asyncio.create_task(cron.run())
    try:
        # the reparse fails on every housekeeping tick, but the daemon must
        # stay up (no UnboundLocalError, no escape) and keep the jobs it had.
        await asyncio.sleep(0.1)
        assert not task.done()
        assert set(cron.cron_jobs) == {"alpha", "beta"}  # unchanged
        # the failed reload flips the standard "config broken on disk" signal
        # (cronstable_config_last_reload_successful) even though the parse ran
        # off the loop, in a worker thread.
        assert cron.metrics._last_reload_ok is False
    finally:
        cron.signal_shutdown()
        await asyncio.wait_for(task, timeout=5)


def test_cluster_allows_per_policy():
    import types

    cron = cronstable.cron.Cron(None)

    def job(policy):
        return types.SimpleNamespace(clusterPolicy=policy)

    # election not configured: every policy runs here (today's behavior)
    for p in ("Leader", "PreferLeader", "EveryNode"):
        assert cron._cluster_allows(job(p)) is True

    cron._elect_leader_configured = True

    # no manager running (e.g. failed to start): EveryNode jobs are immune and
    # still run; Leader fails CLOSED so we don't risk every replica firing; but
    # PreferLeader is never-skip -- a node with no manager is the "store
    # unreachable" case its contract already accepts a double-run for, so it
    # must still run rather than drop to at-most-zero fleet-wide (F14).
    cron.cluster_manager = None
    assert cron._cluster_allows(job("EveryNode")) is True
    assert cron._cluster_allows(job("Leader")) is False
    assert cron._cluster_allows(job("PreferLeader")) is True

    class _Mgr:
        distribution = "single-leader"

        def __init__(self, leader, avail):
            self._leader, self._avail = leader, avail

        def is_leader(self):
            return self._leader

        def is_available_leader(self):
            return self._avail

        def has_conflict(self):
            return False

    # available leader but not quorum leader (e.g. a minority partition):
    # Leader skips, PreferLeader runs, EveryNode runs.
    cron.cluster_manager = _Mgr(leader=False, avail=True)
    assert cron._cluster_allows(job("Leader")) is False
    assert cron._cluster_allows(job("PreferLeader")) is True
    assert cron._cluster_allows(job("EveryNode")) is True

    # the quorum leader: everything runs here
    cron.cluster_manager = _Mgr(leader=True, avail=True)
    assert cron._cluster_allows(job("Leader")) is True
    assert cron._cluster_allows(job("PreferLeader")) is True


def test_cluster_allows_spread_distribution():
    import types

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    def job(policy, name="j"):
        return types.SimpleNamespace(clusterPolicy=policy, name=name)

    # spread mode consults per-job ownership instead of one leader:
    # is_job_owner is keyed on job name, is_available_job_owner ignores quorum.
    class _SpreadMgr:
        distribution = "spread"

        def is_job_owner(self, name):
            return name == "mine"

        def is_available_job_owner(self, name):
            return name == "mine-avail"

        def has_conflict(self):
            return False

    cron.cluster_manager = _SpreadMgr()
    # Leader: runs only on the per-job owner
    assert cron._cluster_allows(job("Leader", "mine")) is True
    assert cron._cluster_allows(job("Leader", "other")) is False
    # PreferLeader: runs on the reachable owner (no quorum gate)
    assert cron._cluster_allows(job("PreferLeader", "mine-avail")) is True
    assert cron._cluster_allows(job("PreferLeader", "other")) is False
    # EveryNode: always runs, regardless of distribution
    assert cron._cluster_allows(job("EveryNode", "other")) is True


def test_cluster_role_logged_on_transition(caplog):
    import logging

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    class _Mgr:
        distribution = "single-leader"

        def __init__(self, leader, quorate=None):
            self._leader = leader
            # in single-leader mode the leader is by definition quorate; a
            # follower may be quorate without leading (default to leader state)
            self._quorate = leader if quorate is None else quorate

        def is_leader(self):
            return self._leader

        def is_quorate(self):
            return self._quorate

        def conflict_names(self):
            return []

        def conflicting_sizes(self):
            return []

        def conflicting_policies(self):
            return []

    cron.cluster_manager = _Mgr(True)
    with caplog.at_level(logging.INFO, logger="cronstable"):
        cron._log_cluster_role()
        cron._log_cluster_role()  # unchanged: no second log
        cron.cluster_manager = _Mgr(False)
        cron._log_cluster_role()
    msgs = [r.message for r in caplog.records if "leadership" in r.message]
    assert msgs == [
        "cluster: this node acquired scheduled-job leadership",
        "cluster: this node lost scheduled-job leadership",
    ]


def test_cluster_quorum_logged_on_follower_single_leader(caplog):
    # C1 regression: a follower (never leader) that loses quorum must still log
    # it -- in single-leader mode only the ex-leader's is_leader() flips, so
    # without this a whole cluster dropping below quorum leaves followers
    # silent.
    import logging

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    class _Follower:
        distribution = "single-leader"

        def __init__(self, quorate):
            self._quorate = quorate

        def is_leader(self):
            return False  # this node never leads (a higher-priority node does)

        def is_quorate(self):
            return self._quorate

        def conflict_names(self):
            return []

        def conflicting_sizes(self):
            return []

        def conflicting_policies(self):
            return []

    cron.cluster_manager = _Follower(True)
    with caplog.at_level(logging.INFO, logger="cronstable"):
        cron._log_cluster_role()  # joins quorum as a follower
        cron.cluster_manager = _Follower(False)
        cron._log_cluster_role()  # loses quorum -> must log here
    msgs = [r.message for r in caplog.records if "quorum" in r.message]
    assert msgs == [
        "cluster: this node joined quorum",
        "cluster: this node left quorum; no majority reachable, so Leader "
        "jobs cannot run until one is",
    ]
    # and a follower never logs a leadership line (it never led)
    assert not [r for r in caplog.records if "leadership" in r.message]


def test_cluster_role_logged_spread_quorum(caplog):
    import logging

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    class _SpreadMgr:
        distribution = "spread"

        def __init__(self, quorate):
            self._quorate = quorate

        def is_quorate(self):
            return self._quorate

        def conflict_names(self):
            return []

        def conflicting_sizes(self):
            return []

        def conflicting_policies(self):
            return []

    cron.cluster_manager = _SpreadMgr(True)
    with caplog.at_level(logging.INFO, logger="cronstable"):
        cron._log_cluster_role()
        cron._log_cluster_role()  # unchanged: no second log
        cron.cluster_manager = _SpreadMgr(False)
        cron._log_cluster_role()
    msgs = [r.message for r in caplog.records if "quorum" in r.message]
    assert msgs == [
        "cluster: this node joined quorum; per-job ownership active",
        "cluster: this node left quorum; per-job ownership suspended",
    ]


# ---------------------------------------------------------------------------
# duplicate-nodeName conflict gate + @reboot deferral
# ---------------------------------------------------------------------------


def test_cluster_allows_leader_stands_down_on_conflict():
    import types

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    def job(policy, name="j"):
        return types.SimpleNamespace(clusterPolicy=policy, name=name)

    class _Mgr:
        distribution = "single-leader"

        def __init__(self, conflict):
            self._conflict = conflict

        def has_conflict(self):
            return self._conflict

        def is_leader(self):
            return True

        def is_available_leader(self):
            return True

    # a duplicate nodeName fails Leader closed; PreferLeader still runs (its
    # contract already tolerates double-runs), EveryNode is unaffected.
    cron.cluster_manager = _Mgr(conflict=True)
    assert cron._cluster_allows(job("Leader")) is False
    assert cron._cluster_allows(job("PreferLeader")) is True
    assert cron._cluster_allows(job("EveryNode")) is True
    # once it clears, Leader runs again
    cron.cluster_manager = _Mgr(conflict=False)
    assert cron._cluster_allows(job("Leader")) is True


def test_cluster_allows_spread_leader_stands_down_on_conflict():
    import types

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    def job(policy, name="j"):
        return types.SimpleNamespace(clusterPolicy=policy, name=name)

    class _SpreadMgr:
        distribution = "spread"

        def __init__(self, conflict):
            self._conflict = conflict

        def has_conflict(self):
            return self._conflict

        def is_job_owner(self, name):
            return True

        def is_available_job_owner(self, name):
            return True

    cron.cluster_manager = _SpreadMgr(conflict=True)
    assert cron._cluster_allows(job("Leader")) is False
    assert cron._cluster_allows(job("PreferLeader")) is True  # ungated
    cron.cluster_manager = _SpreadMgr(conflict=False)
    assert cron._cluster_allows(job("Leader")) is True


def test_cluster_conflict_logged_on_transition(caplog):
    import logging

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    class _Mgr:
        distribution = "single-leader"

        def __init__(self, conflict):
            self._conflict = conflict

        def conflict_names(self):
            return ["dup"] if self._conflict else []

        def conflicting_sizes(self):
            return []

        def conflicting_policies(self):
            return []

        def is_leader(self):
            return False

        def is_quorate(self):
            return True

    cron.cluster_manager = _Mgr(True)
    with caplog.at_level(logging.INFO, logger="cronstable"):
        cron._log_cluster_role()
        cron._log_cluster_role()  # unchanged: no second log
        cron.cluster_manager = _Mgr(False)
        cron._log_cluster_role()
    msgs = [r.message for r in caplog.records]
    assert sum("duplicate nodeName detected" in m for m in msgs) == 1
    assert sum("conflict resolved" in m for m in msgs) == 1


def test_cluster_size_conflict_logged_on_transition(caplog):
    import logging

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    class _Mgr:
        distribution = "single-leader"

        def __init__(self, conflict):
            self._conflict = conflict

        def conflict_names(self):
            return []

        def conflicting_sizes(self):
            return [5] if self._conflict else []

        def conflicting_policies(self):
            return []

        def cluster_size(self):
            return 3

        def is_leader(self):
            return False

        def is_quorate(self):
            return True

    cron.cluster_manager = _Mgr(True)
    with caplog.at_level(logging.INFO, logger="cronstable"):
        cron._log_cluster_role()
        cron._log_cluster_role()  # unchanged: no second log
        cron.cluster_manager = _Mgr(False)
        cron._log_cluster_role()
    msgs = [r.message for r in caplog.records]
    detected = "agreeing peers declare 5 but we declare 3"
    assert sum(detected in m for m in msgs) == 1
    assert sum("cluster-size disagreement resolved" in m for m in msgs) == 1


def test_cluster_policy_conflict_logged_on_transition(caplog):
    # a coordination-policy divergence (a peer running a different distribution
    # / electLeader) stands Leader jobs down cluster-wide; it must leave a
    # breadcrumb just like a duplicate-name or size conflict, once per change.
    import logging

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    class _Mgr:
        distribution = "single-leader"

        def __init__(self, conflict):
            self._conflict = conflict

        def conflict_names(self):
            return []

        def conflicting_sizes(self):
            return []

        def conflicting_policies(self):
            return (
                ["distribution 'spread' != 'single-leader'"]
                if self._conflict
                else []
            )

        def is_leader(self):
            return False

        def is_quorate(self):
            return True

    cron.cluster_manager = _Mgr(True)
    with caplog.at_level(logging.INFO, logger="cronstable"):
        cron._log_cluster_role()
        cron._log_cluster_role()  # unchanged: no second log
        cron.cluster_manager = _Mgr(False)
        cron._log_cluster_role()
    msgs = [r.message for r in caplog.records]
    assert sum("coordination-policy divergence --" in m for m in msgs) == 1
    assert (
        sum("coordination-policy divergence resolved" in m for m in msgs) == 1
    )
    assert any("distribution 'spread' != 'single-leader'" in m for m in msgs)


def test_is_deferrable_reboot():
    import types

    from cronstable.cronexpr import CronTab

    cron = cronstable.cron.Cron(None)

    def job(policy, sched):
        return types.SimpleNamespace(clusterPolicy=policy, schedule=sched)

    # not deferrable until election is configured
    assert cron._is_deferrable_reboot(job("Leader", "@reboot")) is False
    cron._elect_leader_configured = True
    assert cron._is_deferrable_reboot(job("Leader", "@reboot")) is True
    assert cron._is_deferrable_reboot(job("PreferLeader", "@reboot")) is True
    # EveryNode @reboot is meant to run on every node at boot -> not deferred
    assert cron._is_deferrable_reboot(job("EveryNode", "@reboot")) is False
    # a real cron schedule (not @reboot) is never a deferrable reboot
    assert (
        cron._is_deferrable_reboot(job("Leader", CronTab("* * * * *")))
        is False
    )


def _reboot_job(name="boot", policy="Leader", enabled=True):
    import types

    return types.SimpleNamespace(
        name=name, clusterPolicy=policy, schedule="@reboot", enabled=enabled
    )


def _reboot_mgr(
    *, leader=None, conflict=False, node="node-a", available=None, ran=()
):
    ran_set = set(ran)

    class _Mgr:
        node_name = node
        distribution = "single-leader"

        def has_conflict(self):
            return conflict

        def leader_name(self):
            return leader

        def is_leader(self):
            # mirrors the real seam: leader iff the elected name is ours
            return self.leader_name() == self.node_name

        def available_leader_name(self):
            # quorum-free owner used by PreferLeader; an isolated node leads
            # its own reachable set, so default to self.
            return node if available is None else available

        def is_available_leader(self):
            return self.available_leader_name() == self.node_name

        def reboot_ran(self, name):
            return name in ran_set

        async def mark_reboot_ran(self, name):
            ran_set.add(name)

    return _Mgr()


@pytest.mark.asyncio
async def test_deferred_reboot_runs_on_owner(monkeypatch):
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job()
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    mgr = _reboot_mgr(leader="node-a")  # we are the owner
    cron.cluster_manager = mgr
    await cron._process_pending_reboots()
    assert launched == ["boot"]
    assert "boot" not in cron._pending_reboot_jobs
    # running it records + advertises the run, so peers won't re-run it
    assert mgr.reboot_ran("boot") is True


@pytest.mark.asyncio
async def test_deferred_reboot_paused_owner_keeps_it_pending(monkeypatch):
    # A pause defers a deferred @reboot one-shot's boot run instead of
    # forfeiting it: the cluster's once-per-boot token must not be spent on
    # a run the launcher's pause gate would only skip.
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job()
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    cron._paused["boot"] = cronstable.cron.PauseInfo(
        since=datetime.datetime.now(UTC),
        until=datetime.datetime.now(UTC) + datetime.timedelta(hours=1),
        note="",
        by="op",
        channel="api",
    )
    mgr = _reboot_mgr(leader="node-a")  # we are the owner
    cron.cluster_manager = mgr
    await cron._process_pending_reboots()
    assert launched == []
    assert "boot" in cron._pending_reboot_jobs  # still owed
    assert mgr.reboot_ran("boot") is False  # token not burnt
    # the pause lifts -> the boot run happens, exactly once
    cron._paused.pop("boot")
    await cron._process_pending_reboots()
    assert launched == ["boot"]
    assert "boot" not in cron._pending_reboot_jobs
    assert mgr.reboot_ran("boot") is True


@pytest.mark.asyncio
async def test_deferred_reboot_disabled_on_owner_is_not_run(monkeypatch):
    # A deferred @reboot Leader/PreferLeader job DISABLED via a reload while it
    # sat pending must be retired without running, even on the elected owner --
    # the same way job_should_run and the manual web trigger refuse a disabled
    # job. Otherwise an operator-disabled init/migration one-shot still runs
    # once cluster-wide on convergence.
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job(enabled=False)  # disabled on reload while pending
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    cron.cluster_manager = _reboot_mgr(leader="node-a")  # we are the owner
    await cron._process_pending_reboots()
    assert launched == []  # disabled -> not run...
    assert "boot" not in cron._pending_reboot_jobs  # ...and retired, not stuck


@pytest.mark.asyncio
async def test_deferred_reboot_disabled_no_manager_preferleader(monkeypatch):
    # The never-skip mgr-is-None PreferLeader branch must also refuse a job
    # disabled on reload (it otherwise runs every such one-shot here).
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    cron.cluster_manager = None
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job(policy="PreferLeader", enabled=False)
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    await cron._process_pending_reboots()
    assert launched == []
    assert "boot" not in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_deferred_reboot_disabled_after_election_removed(monkeypatch):
    # The election-removed branch (no longer gated) must also refuse a disabled
    # job rather than running it once on the way out.
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = False  # election turned off on reload
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job(enabled=False)
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    await cron._process_pending_reboots()
    assert launched == []
    assert "boot" not in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_deferred_reboot_records_before_launch(monkeypatch):
    # At-most-once crash safety: the deferred-@reboot owner MUST record
    # intent-to-run (mark_reboot_ran, which eagerly gossips/persists) BEFORE
    # spawning the job. A crash in a launch->record window would leave no
    # peer/store aware it ran, so a failover owner would re-run a Leader
    # one-shot (a double-run). Pin the RELATIVE ORDER, not just the end state,
    # so swapping the two production lines (launch then record) fails here.
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    events = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: events.append("launch") or _noop(),
    )
    mgr = _reboot_mgr(leader="node-a")  # we are the owner
    orig_mark = mgr.mark_reboot_ran

    async def _recording_mark(name):
        events.append("record")
        await orig_mark(name)

    mgr.mark_reboot_ran = _recording_mark
    job = _reboot_job()
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    cron.cluster_manager = mgr
    await cron._process_pending_reboots()
    assert events == ["record", "launch"]


@pytest.mark.asyncio
async def test_deferred_reboot_leader_runs_when_identity_differs(monkeypatch):
    # H3 regression: a lease backend reports leader_name() as the holder's
    # display *identity* (e.g. cluster.kubernetes.identity), which may
    # legitimately differ from node_name. The deferred-@reboot gate must
    # self-recognise the holder via the is_leader() boolean, NOT by comparing
    # that identity string to node_name -- otherwise a one-shot Leader @reboot
    # job never runs on ANY node (the holder's identity != its node_name, and
    # every follower's leader_name() is that identity too).
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )

    class _LeaseMgr:
        node_name = "pod-a"
        distribution = "single-leader"

        def has_conflict(self):
            return False

        def is_leader(self):
            return True  # this node holds the lease

        def leader_name(self):
            return "my-app"  # display identity, != node_name -- the trap

        def reboot_ran(self, name):
            return False

        async def mark_reboot_ran(self, name):
            pass

    job = _reboot_job()
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    cron.cluster_manager = _LeaseMgr()
    await cron._process_pending_reboots()
    assert launched == ["boot"]
    assert "boot" not in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_deferred_reboot_retired_on_ack_without_rerun(monkeypatch):
    # the gossip-ack: once the cluster reports the job already ran, a node
    # retires it WITHOUT running -- even if this node is now the elected owner.
    # This is what stops a re-run when leadership lands on a node that still
    # held the one-shot pending.
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job()
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    # we are the owner now, but the cluster already ran it -> do NOT re-run
    cron.cluster_manager = _reboot_mgr(leader="node-a", ran={"boot"})
    await cron._process_pending_reboots()
    assert launched == []
    assert "boot" not in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_deferred_reboot_kept_when_other_owns(monkeypatch):
    # #8: a non-owner must NOT drop the one-shot just because some other node
    # currently looks like the owner -- that node may itself be unable to run
    # it (reachable from us but not quorate from its own view), and dropping
    # would lose the boot job forever; we keep waiting instead.
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job()
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    cron.cluster_manager = _reboot_mgr(leader="node-b")  # someone else owns it
    await cron._process_pending_reboots()
    assert launched == []  # did not run here...
    assert "boot" in cron._pending_reboot_jobs  # ...and keeps waiting


@pytest.mark.asyncio
async def test_deferred_reboot_leader_runs_after_owner_lands_here(monkeypatch):
    # #8 (continued): because we kept waiting above instead of dropping, the
    # one-shot still runs when leadership later lands on this node -- so a
    # deferred boot job is never silently lost.
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job()
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    cron.cluster_manager = _reboot_mgr(leader="node-b")  # not us yet
    await cron._process_pending_reboots()
    assert launched == [] and "boot" in cron._pending_reboot_jobs
    cron.cluster_manager = _reboot_mgr(leader="node-a")  # now we are leader
    await cron._process_pending_reboots()
    assert launched == ["boot"] and "boot" not in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_deferred_reboot_preferleader_runs_without_quorum(monkeypatch):
    # #9: a PreferLeader @reboot must run even with no quorum (its contract is
    # to never skip while a node is up). The gate (_cluster_allows) uses the
    # quorum-free is_available_leader(), true on an isolated/minority node.
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job(policy="PreferLeader")
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    # no quorum (leader_name is None) but the availability owner is us
    cron.cluster_manager = _reboot_mgr(leader=None, available="node-a")
    await cron._process_pending_reboots()
    assert launched == ["boot"]
    assert "boot" not in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_deferred_reboot_preferleader_runs_when_no_manager(monkeypatch):
    # H1 regression: election configured but the backend never started (store
    # unreachable / bad creds -> cluster_manager is None). A deferred
    # PreferLeader @reboot must STILL run here -- its contract is never-skip,
    # exactly the store-unreachable case it exists to survive. Previously the
    # mgr-is-None branch returned early for ALL jobs, dropping it forever.
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    cron.cluster_manager = None  # backend failed to start
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job(policy="PreferLeader")
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    await cron._process_pending_reboots()
    assert launched == ["boot"]
    assert "boot" not in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_deferred_reboot_leader_pending_no_manager(monkeypatch):
    # H1 (cont.): a Leader @reboot in the SAME no-manager state must NOT run --
    # it stays fail-closed and pending, re-evaluated once a manager comes up.
    # Asymmetric with PreferLeader above, mirroring _cluster_allows.
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    cron.cluster_manager = None
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job(policy="Leader")
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    await cron._process_pending_reboots()
    assert launched == []
    assert "boot" in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_deferred_reboot_preferleader_waits_for_available_owner(
    monkeypatch,
):
    # the quorum-free availability owner can still be another node (a lower
    # name we mutually agree with); that node runs it, so we keep waiting.
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job(policy="PreferLeader")
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    cron.cluster_manager = _reboot_mgr(leader=None, available="node-b")
    await cron._process_pending_reboots()
    assert launched == []
    assert "boot" in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_deferred_reboot_waits_without_quorum(monkeypatch):
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job()
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    cron.cluster_manager = _reboot_mgr(leader=None)  # no quorum yet
    await cron._process_pending_reboots()
    assert launched == []
    assert "boot" in cron._pending_reboot_jobs  # keep waiting


@pytest.mark.asyncio
async def test_deferred_reboot_waits_on_conflict(monkeypatch):
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job()
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    # owner is undecided during a conflict even though leader_name() is us
    cron.cluster_manager = _reboot_mgr(leader="node-a", conflict=True)
    await cron._process_pending_reboots()
    assert launched == []
    assert "boot" in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_deferred_reboot_runs_when_election_disabled(monkeypatch):
    # election removed on a reload: nothing gates these anymore -> run here
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = False
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job()
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    await cron._process_pending_reboots()
    assert launched == ["boot"]
    assert not cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_deferred_reboot_kept_when_absent_election_disabled(monkeypatch):
    # #4 (election-disabled path): the same never-lose rule holds when election
    # was turned off on a reload -- a momentarily-absent name is kept pending,
    # not popped, and runs the current job once the name returns.
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = False
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job) or _noop(),
    )
    stale = _reboot_job()
    cron._pending_reboot_jobs["boot"] = stale
    cron.cron_jobs.pop("boot", None)  # absent right now
    await cron._process_pending_reboots()
    assert launched == []
    assert "boot" in cron._pending_reboot_jobs  # kept, not lost
    # name returns -> runs the CURRENT job (not the stale snapshot)
    current = _reboot_job()
    cron.cron_jobs["boot"] = current
    await cron._process_pending_reboots()
    assert launched == [current]
    assert launched[0] is not stale
    assert not cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_deferred_reboot_election_disabled_skips_non_reboot_reuse(
    monkeypatch,
):
    # #4 (election-off path): if a deferred name was reused for a non-@reboot
    # job by the time election is turned off, the stale one-shot is retired
    # WITHOUT running here -- the reused job schedules itself normally. Only a
    # name still mapping to an @reboot job runs on the election-off drain path,
    # mirroring the gated path's _is_deferrable_reboot retirement.
    import types

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = False
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    cron._pending_reboot_jobs["boot"] = _reboot_job()  # stale @reboot one-shot
    # the name now maps to a normally-scheduled job (reused)
    cron.cron_jobs["boot"] = types.SimpleNamespace(
        name="boot", clusterPolicy="Leader", schedule="0 * * * *"
    )
    await cron._process_pending_reboots()
    assert launched == []  # the reused non-@reboot job is not run here
    assert "boot" not in cron._pending_reboot_jobs  # stale entry retired


@pytest.mark.asyncio
async def test_deferred_reboot_kept_on_transient_absence(monkeypatch):
    # #4: @reboot only defers at startup, so if a name momentarily vanishes
    # from cron_jobs mid-reload (templating glitch, transient remove-then-
    # re-add) before the cluster converges, it must NOT be dropped -- dropping
    # would lose the one-shot forever and break the never-lose property. It
    # stays pending while absent and runs once the name returns and we own it.
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job()
    cron._pending_reboot_jobs["boot"] = job
    # the name is transiently absent from cron_jobs (cron.cron_jobs is empty)
    cron.cluster_manager = _reboot_mgr(leader="node-a")  # we would own it
    await cron._process_pending_reboots()
    assert launched == []  # did not run while absent...
    assert "boot" in cron._pending_reboot_jobs  # ...and was NOT dropped
    # the name comes back on a later reload; now it runs (we are the owner)
    cron.cron_jobs["boot"] = job
    await cron._process_pending_reboots()
    assert launched == ["boot"]
    assert "boot" not in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_deferred_reboot_absent_job_never_runs(monkeypatch):
    # a deliberately-removed @reboot job that never returns must never run,
    # even though we keep it pending: the launch is gated on presence.
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job()
    cron._pending_reboot_jobs["boot"] = job  # pending, but absent from config
    cron.cluster_manager = _reboot_mgr(leader="node-a")  # we would own it
    for _ in range(3):
        await cron._process_pending_reboots()
    assert launched == []  # removed-and-gone -> never runs


@pytest.mark.asyncio
async def test_deferred_reboot_runs_current_config_on_name_reuse(monkeypatch):
    # #4 name-reuse edge: if a name is removed and later re-added for a
    # DIFFERENT @reboot job, the owner runs the CURRENT cron_jobs[name], never
    # the stale JobConfig captured at boot.
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job) or _noop(),
    )
    stale = _reboot_job()  # captured at startup, then the name was reused
    fresh = _reboot_job()  # a different object with the same name
    cron._pending_reboot_jobs["boot"] = stale
    cron.cron_jobs["boot"] = fresh
    cron.cluster_manager = _reboot_mgr(leader="node-a")  # we are the owner
    await cron._process_pending_reboots()
    assert launched == [fresh]  # the live config, not the stale captured one
    assert launched[0] is not stale
    assert "boot" not in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_deferred_reboot_retired_when_name_reused_non_deferrable(
    monkeypatch,
):
    # #4 name-reuse edge: if a name is reused for a job that is no longer a
    # deferrable @reboot (e.g. EveryNode, or a real schedule), the stale
    # pending entry is retired WITHOUT running through the owner path -- the
    # new job is left to its own scheduling.
    import types

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    cron._pending_reboot_jobs["boot"] = _reboot_job()  # stale @reboot Leader
    # the name now belongs to an EveryNode @reboot job (not deferrable)
    cron.cron_jobs["boot"] = types.SimpleNamespace(
        name="boot", clusterPolicy="EveryNode", schedule="@reboot"
    )
    cron.cluster_manager = _reboot_mgr(leader="node-a")  # we would own it
    await cron._process_pending_reboots()
    assert launched == []  # the owner path did not run the reused name
    assert "boot" not in cron._pending_reboot_jobs  # stale entry retired


@pytest.mark.asyncio
async def test_spawn_jobs_defers_reboot_leader_at_startup(monkeypatch):
    config = parse_config_string(
        "jobs:\n  - name: boot\n    command: echo hi\n"
        '    schedule: "@reboot"\n    clusterPolicy: Leader\n',
        "",
    )
    cron = cronstable.cron.Cron(None)
    cron.cron_jobs = OrderedDict((j.name, j) for j in config.jobs)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: launched.append(job.name) or _noop(),
    )

    class _Mgr:
        node_name = "node-a"
        distribution = "single-leader"

        def conflict_names(self):
            return []

        def conflicting_sizes(self):
            return []

        def conflicting_policies(self):
            return []

        def is_leader(self):
            return False

        def is_quorate(self):
            return False  # no quorum at the startup instant

        def has_conflict(self):
            return False

        def leader_name(self):
            return None  # no quorum at the startup instant

        def reboot_ran(self, name):
            return False

    cron.cluster_manager = _Mgr()
    await cron.spawn_jobs(startup=True)
    assert launched == []  # deferred, not run at boot
    assert "boot" in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_web_start_deferred_reboot_retires_pending_and_marks_ran(
    monkeypatch,
):
    # The Run button (POST /jobs/{name}/start) used to launch a job still
    # pending as a deferred @reboot one-shot WITHOUT retiring the pending
    # entry or recording the run, so once the cluster converged
    # _process_pending_reboots saw reboot_ran(name) False and ran the
    # one-shot a second time -- possibly on another node, since the manual
    # run was never gossiped/persisted. A manual start IS the boot run: it
    # must retire the entry and mark it ran on the manager.
    cron = cronstable.cron.Cron(None)
    cron.web_config = {}
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron,
        "maybe_launch_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job()
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    # not converged yet (no quorum): exactly the window in which the
    # dashboard shows the job as pending and an operator clicks Run.
    mgr = _reboot_mgr(leader=None)
    cron.cluster_manager = mgr

    class Req:
        match_info = {"name": "boot"}
        headers: dict = {}

    resp = await cron._web_start_job(Req())
    assert resp.status == 200
    assert launched == ["boot"]  # the manual run happened
    assert "boot" not in cron._pending_reboot_jobs  # retired locally...
    assert mgr.reboot_ran("boot") is True  # ...and recorded cluster-wide
    # convergence later must not re-run the one-shot: nothing is pending here
    # and a peer still holding it pending stands down on the recorded run.
    await cron._process_pending_reboots()
    assert launched == ["boot"]


@pytest.mark.asyncio
async def test_web_start_deferred_reboot_without_manager(monkeypatch):
    # the same manual start with no manager running (backend failed to start)
    # must still retire the pending entry -- the local re-run protection --
    # and launch, without tripping on the absent manager.
    cron = cronstable.cron.Cron(None)
    cron.web_config = {}
    cron._elect_leader_configured = True
    cron.cluster_manager = None
    launched = []
    monkeypatch.setattr(
        cron,
        "maybe_launch_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job()
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job

    class Req:
        match_info = {"name": "boot"}
        headers: dict = {}

    resp = await cron._web_start_job(Req())
    assert resp.status == 200
    assert launched == ["boot"]
    assert "boot" not in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_web_start_deferred_reboot_concurrent_requests(monkeypatch):
    # Reviewer race: two concurrent POST /jobs/{name}/start for the SAME
    # still-pending @reboot name can both pass the pending check before the
    # awaited mark_reboot_ran yields (the gossip push awaits peers). The
    # loser must not 500 on a KeyError retiring an entry the winner already
    # retired: the entry is retired exactly once, and BOTH manual starts
    # still launch -- exactly as two manual starts of any other job would.
    cron = cronstable.cron.Cron(None)
    cron.web_config = {}
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron,
        "maybe_launch_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = _reboot_job()
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    mgr = _reboot_mgr(leader=None)
    orig_mark = mgr.mark_reboot_ran

    async def _slow_mark(name):
        # model the real gossip push: the record awaits peers, yielding to
        # the event loop while the pending entry is still present
        await asyncio.sleep(0.01)
        await orig_mark(name)

    mgr.mark_reboot_ran = _slow_mark
    cron.cluster_manager = mgr

    class Req:
        match_info = {"name": "boot"}
        headers: dict = {}

    r1, r2 = await asyncio.gather(
        cron._web_start_job(Req()), cron._web_start_job(Req())
    )
    assert (r1.status, r2.status) == (200, 200)  # the loser must not 500
    assert launched == ["boot", "boot"]  # both operator actions ran the job
    assert "boot" not in cron._pending_reboot_jobs  # retired exactly once
    assert mgr.reboot_ran("boot") is True  # ...and recorded cluster-wide


@pytest.mark.asyncio
async def test_cluster_start_survives_bad_cert_files(caplog):
    # #6: a missing/unreadable cert file is an operational misconfiguration --
    # start_stop_cluster must log it and keep running (no manager), NOT let the
    # exception escape to the run loop's generic "please report this as a bug"
    # handler. ClusterManager is constructed inside the try for exactly this.
    import logging

    yaml = (
        "jobs:\n  - name: a\n    command: echo a\n"
        '    schedule: "* * * * *"\n'
        "cluster:\n"
        '  listen: "127.0.0.1:18443"\n'
        "  tls:\n"
        "    ca: /nonexistent/ca.pem\n"
        "    cert: /nonexistent/cert.pem\n"
        "    key: /nonexistent/key.pem\n"
        "  peers:\n"
        "    - host: b:8443\n"
        "    - host: c:8443\n"
        "  electLeader: true\n"
    )
    config = parse_config_string(yaml, "")
    cron = cronstable.cron.Cron(None)
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await cron.start_stop_cluster(config.cluster_config)  # must not raise
    assert cron.cluster_manager is None
    # election intent is tracked regardless, so the Leader gate fails closed
    assert cron._elect_leader_configured is True
    assert any("failed to start" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_cluster_restarts_on_in_place_cert_rotation(caplog):
    # an in-place cert rotation leaves the config bytes identical, so the
    # restart-on-config-change check alone never fires; the manager must also
    # restart on the TLS-file-change signal -- but only once the new material
    # is actually loadable (#6), so a half-written cert mid-rotation cannot
    # wedge it. Loadable case: the rotation restart proceeds and the old
    # manager is stopped.
    import logging

    yaml = (
        "jobs:\n  - name: a\n    command: echo a\n"
        '    schedule: "* * * * *"\n'
        "cluster:\n"
        '  listen: "127.0.0.1:18443"\n'
        "  tls:\n"
        "    ca: /nonexistent/ca.pem\n"
        "    cert: /nonexistent/cert.pem\n"
        "    key: /nonexistent/key.pem\n"
        "  peers:\n"
        "    - host: b:8443\n"
        "    - host: c:8443\n"
        "  electLeader: true\n"
    )
    cfg = parse_config_string(yaml, "").cluster_config

    class _FakeMgr:
        def __init__(self, config):
            self.config = config
            self.stopped = False

        def tls_files_changed(self):
            return True

        def tls_files_loadable(self):
            return True  # new material loads cleanly -> proceed with restart

        async def stop(self):
            self.stopped = True

    cron = cronstable.cron.Cron(None)
    fake = _FakeMgr(cfg)
    cron.cluster_manager = fake
    # same config object -> the config-change branch is skipped; only the
    # TLS-change signal can trigger the restart.
    with caplog.at_level(logging.INFO, logger="cronstable"):
        await cron.start_stop_cluster(cfg)
    assert fake.stopped is True
    assert any(
        "TLS certificate files changed" in r.message for r in caplog.records
    )
    # reconstruction uses the (here deliberately bad) cert paths and fails
    # closed, so no new manager replaces the stopped one.
    assert cron.cluster_manager is None


@pytest.mark.asyncio
async def test_cluster_cert_rotation_keeps_manager_when_unloadable(caplog):
    # #6: a half-written / briefly-absent cert observed mid-rotation must NOT
    # tear the manager down. The rotation signal fires (tls_files_changed) but
    # the new material is not yet loadable, so the running manager is kept
    # (still serving the valid old cert) and we retry next reload -- Leader /
    # PreferLeader stay up the whole time instead of failing closed for ~1
    # reload while the rebuild fails on the same bad files.
    import logging

    yaml = (
        "jobs:\n  - name: a\n    command: echo a\n"
        '    schedule: "* * * * *"\n'
        "cluster:\n"
        '  listen: "127.0.0.1:18443"\n'
        "  tls:\n"
        "    ca: /nonexistent/ca.pem\n"
        "    cert: /nonexistent/cert.pem\n"
        "    key: /nonexistent/key.pem\n"
        "  peers:\n"
        "    - host: b:8443\n"
        "    - host: c:8443\n"
        "  electLeader: true\n"
    )
    cfg = parse_config_string(yaml, "").cluster_config

    class _FakeMgr:
        def __init__(self, config):
            self.config = config
            self.stopped = False

        def tls_files_changed(self):
            return True

        def tls_files_loadable(self):
            return False  # half-written rotation: cannot load yet

        def set_node_stats_provider(self, provider, share=True):
            # the kept-manager path re-reconciles the share flag every reload
            self.node_stats_share = share

        async def stop(self):
            self.stopped = True

    cron = cronstable.cron.Cron(None)
    fake = _FakeMgr(cfg)
    cron.cluster_manager = fake
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron.start_stop_cluster(cfg)
    # the old manager is kept and was never stopped or replaced
    assert fake.stopped is False
    assert cron.cluster_manager is fake
    assert any("not yet loadable" in r.message for r in caplog.records)


def _config_change_yamls():
    yaml_a = (
        "jobs:\n  - name: a\n    command: echo a\n"
        '    schedule: "* * * * *"\n'
        "cluster:\n"
        '  listen: "127.0.0.1:18443"\n'
        "  tls:\n"
        "    ca: /nonexistent/ca.pem\n"
        "    cert: /nonexistent/cert.pem\n"
        "    key: /nonexistent/key.pem\n"
        "  peers:\n"
        "    - host: b:8443\n"
        "    - host: c:8443\n"
        "  electLeader: true\n"
    )
    # a DIFFERENT peer set -> cluster_config != mgr.config -> config change
    yaml_b = yaml_a.replace("host: c:8443", "host: d:8443")
    return (
        parse_config_string(yaml_a, "").cluster_config,
        parse_config_string(yaml_b, "").cluster_config,
    )


class _ConfigChangeFakeMgr:
    def __init__(self, config):
        self.config = config
        self.stopped = False

    def tls_files_changed(self):
        return False  # config changed; the TLS-rotation path is moot here

    def tls_files_loadable(self):  # pragma: no cover - not reached
        return False

    def set_node_stats_provider(self, provider, share=True):
        # the kept-manager path re-reconciles the share flag every reload
        self.node_stats_share = share

    async def stop(self):
        self.stopped = True


@pytest.mark.asyncio
async def test_cluster_config_change_keeps_manager_when_new_tls_unloadable(
    caplog,
):
    import logging

    # RELOAD-TLS-COMBINED: a genuine config change (different peer set) that
    # coincides with an in-flight cert rotation (new TLS material not yet
    # loadable) must NOT tear the old manager down and then fail to rebuild --
    # which would wedge Leader/PreferLeader closed for up to a reload. The
    # pre-teardown dry-run keeps the running manager (still serving the valid
    # old cert) and retries next reload. The certs here are absent (the
    # mid-rotation case), so gossip_tls_loadable(cfg_b) is False.
    cfg_a, cfg_b = _config_change_yamls()
    cron = cronstable.cron.Cron(None)
    fake = _ConfigChangeFakeMgr(cfg_a)
    cron.cluster_manager = fake
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron.start_stop_cluster(cfg_b)
    assert fake.stopped is False  # kept, not torn down
    assert cron.cluster_manager is fake
    assert any("not yet loadable" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_cluster_config_change_tears_down_when_new_tls_loadable(
    monkeypatch,
):
    # the dry-run gate is specific to UNLOADABLE new TLS: when the new config's
    # TLS loads cleanly, a config change still tears the old manager down (the
    # operator changed config; the old manager no longer applies), and
    # reconstruction then fails closed on the (here deliberately absent) certs.
    cfg_a, cfg_b = _config_change_yamls()
    monkeypatch.setattr(
        "cronstable.cluster.gossip_tls_loadable", lambda cfg: True
    )
    cron = cronstable.cron.Cron(None)
    fake = _ConfigChangeFakeMgr(cfg_a)
    cron.cluster_manager = fake
    await cron.start_stop_cluster(cfg_b)
    assert fake.stopped is True  # config change tears down
    assert cron.cluster_manager is None  # reconstruction fails closed


def _observability_toggle_yamls():
    yaml_off = (
        "jobs:\n  - name: a\n    command: echo a\n"
        '    schedule: "* * * * *"\n'
        "cluster:\n"
        '  listen: "127.0.0.1:18443"\n'
        "  tls:\n"
        "    ca: /nonexistent/ca.pem\n"
        "    cert: /nonexistent/cert.pem\n"
        "    key: /nonexistent/key.pem\n"
        "  peers:\n"
        "    - host: b:8443\n"
        "    - host: c:8443\n"
        "  electLeader: true\n"
    )
    yaml_on = yaml_off + "  observability:\n    shareNodeStats: true\n"
    return (
        parse_config_string(yaml_off, "").cluster_config,
        parse_config_string(yaml_on, "").cluster_config,
    )


@pytest.mark.asyncio
async def test_cluster_observability_only_change_keeps_manager_reconciles():
    # An observability-only edit (shareNodeStats toggled; the election
    # section untouched) must NOT restart the election manager -- on a lease
    # backend that would drop the leadership lease and pause Leader jobs
    # fleet-wide for an election-inert change. Instead the kept manager's
    # LATCHED share flag is re-reconciled to the new config every reload, so
    # the toggle actually reaches the running gossip mesh.
    cfg_off, cfg_on = _observability_toggle_yamls()
    cron = cronstable.cron.Cron(None)
    fake = _ConfigChangeFakeMgr(cfg_off)
    cron.cluster_manager = fake
    # toggle ON: manager kept, share flag reconciled to True
    await cron.start_stop_cluster(cfg_on)
    assert fake.stopped is False
    assert cron.cluster_manager is fake
    assert fake.node_stats_share is True
    # toggle back OFF: still kept, flag reconciled to False
    await cron.start_stop_cluster(cfg_off)
    assert fake.stopped is False
    assert cron.cluster_manager is fake
    assert fake.node_stats_share is False
    # (a genuine election-relevant change still restarting is covered by
    # test_cluster_config_change_tears_down_when_new_tls_loadable)


# ---------------------------------------------------------------------------
# Web server integration.
#
# Every other web test calls a handler coroutine directly with a hand-rolled
# fake request, so routing, the auth middleware, and the bind/serve path are
# never exercised together. These drive the real server start_stop_web_app
# stands up, over real HTTP, so a dropped route or an inverted ui/auth gate (a
# data endpoint served unauthenticated) is caught.
# ---------------------------------------------------------------------------

_WEB_ONE_JOB = """
jobs:
  - name: alpha
    command: echo alpha
    schedule: "*/5 * * * *"
"""


@pytest.mark.asyncio
async def test_web_app_enforces_auth_when_token_configured():
    import aiohttp

    cron = cronstable.cron.Cron(None, config_yaml=_WEB_ONE_JOB)
    await cron.start_stop_web_app(
        {
            "listen": ["http://127.0.0.1:0"],
            "authToken": {"value": "secret"},
            "ui": False,
        }
    )
    try:
        port = cron.web_runner.addresses[0][1]
        base = "http://127.0.0.1:{}".format(port)
        async with aiohttp.ClientSession() as session:
            # no credentials -> rejected
            async with session.get(base + "/jobs") as resp:
                assert resp.status == 401
            # wrong token -> rejected
            async with session.get(
                base + "/jobs", headers={"Authorization": "Bearer nope"}
            ) as resp:
                assert resp.status == 401
            # correct token -> the real jobs payload is served
            async with session.get(
                base + "/jobs", headers={"Authorization": "Bearer secret"}
            ) as resp:
                assert resp.status == 200
                data = await resp.json()
                assert [j["name"] for j in data] == ["alpha"]
    finally:
        await cron.start_stop_web_app(None)
    # clearing the config fully stops the server
    assert cron.web_runner is None


@pytest.mark.asyncio
async def test_web_app_ui_path_public_but_data_paths_require_auth():
    import aiohttp

    cron = cronstable.cron.Cron(None, config_yaml=_WEB_ONE_JOB)
    await cron.start_stop_web_app(
        {
            "listen": ["http://127.0.0.1:0"],
            "authToken": {"value": "secret"},
            "ui": True,
        }
    )
    try:
        port = cron.web_runner.addresses[0][1]
        base = "http://127.0.0.1:{}".format(port)
        async with aiohttp.ClientSession() as session:
            # the UI page holds no data, so it is reachable without a token
            async with session.get(base + "/") as resp:
                assert resp.status == 200
                assert "text/html" in resp.headers["Content-Type"]
            # a data endpoint still requires the token even with the UI enabled
            async with session.get(base + "/jobs") as resp:
                assert resp.status == 401
    finally:
        await cron.start_stop_web_app(None)


@pytest.mark.asyncio
async def test_web_app_restarts_on_config_change(monkeypatch):
    # changing the web config replaces the running server with a new one;
    # clearing it stops the server entirely. web_site_from_url is faked so no
    # real socket is bound and the transition logic is tested in isolation.
    started = []

    class FakeSite:
        def __init__(self, url):
            self.url = url

        async def start(self):
            started.append(self.url)

    monkeypatch.setattr(
        cronstable.cron,
        "web_site_from_url",
        lambda runner, url, ssl_context=None: FakeSite(url),
    )

    cron = cronstable.cron.Cron(None)
    await cron.start_stop_web_app({"listen": ["http://host-a:8000"]})
    runner1 = cron.web_runner
    assert runner1 is not None
    assert started == ["http://host-a:8000"]

    # a different config: the old runner is replaced and the new site started
    await cron.start_stop_web_app({"listen": ["http://host-b:9000"]})
    assert cron.web_runner is not None
    assert cron.web_runner is not runner1
    assert started == ["http://host-a:8000", "http://host-b:9000"]

    # clearing the config stops the server
    await cron.start_stop_web_app(None)
    assert cron.web_runner is None


# ---------------------------------------------------------------------------
# Daemon lifecycle: config hot-reload, graceful shutdown drain, real signal.
#
# These drive the actual run() loop end-to-end. Reload through run() (vs the
# existing tests all use config_arg=None, so the job set never changes) and
# the retry-drain-on-shutdown path were untested; a regression in either breaks
# headline daemon behavior silently.
# ---------------------------------------------------------------------------


async def _wait_until(pred, tries=300, interval=0.01):
    # Poll a predicate instead of sleeping a fixed time, so the tests stay fast
    # and do not flake under CI load. Bounded so a never-true predicate fails
    # cleanly instead of hanging.
    for _ in range(tries):
        if pred():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met within {} tries".format(tries))


# schedules fire at midnight; the suite's clock is fixed at noon, so these jobs
# never actually spawn -- the test exercises reload, not execution.
_RELOAD_V1 = """
jobs:
  - name: alpha
    command: echo alpha
    schedule: "0 0 * * *"
  - name: beta
    command: echo beta
    schedule: "0 0 * * *"
"""

_RELOAD_V2 = """
jobs:
  - name: alpha
    command: echo alpha
    schedule: "0 0 * * *"
  - name: gamma
    command: echo gamma
    schedule: "0 0 * * *"
"""


@pytest.mark.asyncio
async def test_run_reloads_changed_config(tmp_path, monkeypatch):
    # tiny sleep so the reload loop iterates quickly instead of waiting out the
    # real ~60s to the next minute boundary.
    # accept the subminute flag arg the loop now passes to next_sleep_interval
    monkeypatch.setattr("cronstable.cron.next_sleep_interval", lambda *a: 0.02)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_RELOAD_V1)

    cron = cronstable.cron.Cron(str(cfg))
    assert set(cron.cron_jobs) == {"alpha", "beta"}
    id1 = cron.job_set_id()

    task = asyncio.create_task(cron.run())
    try:
        # let the loop load v1 at least once, then change the file on disk
        await _wait_until(lambda: cron._logged_job_set_id is not None)
        cfg.write_text(_RELOAD_V2)
        # the running daemon must pick up the new job set on its own
        await _wait_until(lambda: set(cron.cron_jobs) == {"alpha", "gamma"})
    finally:
        cron.signal_shutdown()
        await asyncio.wait_for(task, timeout=5)

    assert set(cron.cron_jobs) == {"alpha", "gamma"}
    assert cron.job_set_id() != id1


_RETRY_DRAIN_JOB = (
    "jobs:\n  - name: test\n"
    + yaml_command(cmd_print(out="x", code=2))
    + """
    schedule: "@reboot"
    onFailure:
      retry:
        maximumRetries: 5
        initialDelay: 30
        maximumDelay: 30
        backoffMultiplier: 1
"""
)


@pytest.mark.asyncio
async def test_run_drains_pending_retry_on_shutdown():
    # the @reboot job fails at once and schedules a retry with a long delay,
    # so a pending (sleeping) retry task sits in retry_state when we shut down.
    cron = cronstable.cron.Cron(None, config_yaml=_RETRY_DRAIN_JOB)

    task = asyncio.create_task(cron.run())
    try:
        await _wait_until(lambda: bool(cron.retry_state))
    finally:
        cron.signal_shutdown()
        await asyncio.wait_for(task, timeout=5)

    # graceful shutdown must cancel and drain the pending retry, not orphan a
    # task or leave retry_state populated.
    assert cron.retry_state == {}


_GATED_CLUSTER_BAD_WEB_TOKEN = """
jobs:
  - name: gated
    command: echo hi
    schedule: "0 0 * * *"
    clusterPolicy: Leader
web:
  listen:
    - http://127.0.0.1:0
  authToken:
    fromEnvVar: CRONSTABLE_TEST_MISSING_TOKEN
cluster:
  listen: "127.0.0.1:18443"
  tls:
    ca: /nonexistent/ca.pem
    cert: /nonexistent/cert.pem
    key: /nonexistent/key.pem
  peers:
    - host: b:8443
    - host: c:8443
  electLeader: true
"""


@pytest.mark.asyncio
async def test_web_config_error_does_not_disengage_cluster_gate(
    tmp_path, monkeypatch, caplog
):
    # start_stop_web_app and start_stop_cluster used to share one try/except
    # ConfigError, web first: a web misconfiguration raising ConfigError (an
    # authToken resolving empty -- a deploy forgetting the env var) skipped
    # start_stop_cluster on EVERY iteration, left _elect_leader_configured
    # False, and ran every Leader job ungated on every node -- the gate
    # failed OPEN on an unrelated web error. The cluster gate must engage
    # (fail CLOSED) regardless of the web app's fate, and the daemon must
    # keep running with the web API down.
    import logging

    monkeypatch.delenv("CRONSTABLE_TEST_MISSING_TOKEN", raising=False)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_GATED_CLUSTER_BAD_WEB_TOKEN)
    cron = cronstable.cron.Cron(str(cfg))

    with caplog.at_level(logging.ERROR, logger="cronstable"):
        task = asyncio.create_task(cron.run())
        try:
            await _wait_until(lambda: cron._elect_leader_configured)
            assert not task.done()  # the daemon keeps running
        finally:
            cron.signal_shutdown()
            await asyncio.wait_for(task, timeout=5)

    assert cron.web_runner is None  # the web API stayed down (fail closed)
    assert any("web.authToken" in r.message for r in caplog.records)
    # the manager itself failed to start (bad certs) but the gate still
    # engaged, so the Leader job fails CLOSED instead of running everywhere
    assert cron.cluster_manager is None
    assert cron._cluster_allows(cron.cron_jobs["gated"]) is False


@pytest.mark.asyncio
async def test_shutdown_stops_cluster_manager_before_job_drain():
    # run() used to stop the cluster manager only AFTER awaiting all running
    # jobs, so a draining leader kept its gossip liveness / lease renewal
    # alive for the whole (unbounded) drain and every Leader job cluster-wide
    # stalled until the slowest local job finished. Leadership must be
    # released after retries are cancelled but BEFORE the drain, so failover
    # proceeds while the jobs finish.
    cron = cronstable.cron.Cron(
        None, config_yaml=CONCURRENT_JOB.format(policy="Allow")
    )
    events = []

    class _Mgr:
        async def stop(self):
            running = cron.running_jobs.get("test") or []
            alive = any(
                rj.proc is not None and rj.proc.returncode is None
                for rj in running
            )
            events.append(("cluster-stopped", alive))
            # leadership released; now let the drain finish by terminating
            # the still-running job (marked cancelled so the reaper records
            # a deliberate cancellation, not a failure).
            for rj in running:
                rj.cancelled = True
                await rj.cancel()

    cron.cluster_manager = _Mgr()
    await cron.maybe_launch_job(cron.cron_jobs["test"])
    assert cron.running_jobs["test"][0].proc.returncode is None
    # stop before the loop's first iteration: run() goes straight to the
    # shutdown sequence with a job still running and a manager installed.
    cron.signal_shutdown()
    await asyncio.wait_for(cron.run(), timeout=10)
    # the manager was stopped while the job was still draining...
    assert events == [("cluster-stopped", True)]
    assert cron.cluster_manager is None
    assert not cron.running_jobs  # ...and the drain then completed


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="POSIX signal delivery (SIGTERM)"
)
def test_sigterm_triggers_graceful_shutdown():
    # End-to-end of the systemd/`docker stop` path: a real SIGTERM, routed
    # through the installed handler, must drive run() to a clean return. Uses a
    # dedicated loop (like the platform handler roundtrip test) so the handler
    # owns the signal and it does not reach the default disposition.
    loop = asyncio.new_event_loop()
    try:
        cron = cronstable.cron.Cron(
            None
        )  # no jobs: run() idles until signalled
        remove = platform.install_shutdown_handlers(loop, cron.signal_shutdown)
        try:
            loop.call_later(0.05, lambda: os.kill(os.getpid(), signal.SIGTERM))
            loop.run_until_complete(asyncio.wait_for(cron.run(), timeout=5))
            assert cron._stop_event.is_set()
        finally:
            remove()
    finally:
        loop.close()


@pytest.mark.asyncio
async def test_fleet_job_summaries_snapshot():
    # the compact per-job snapshot gossiped to peers for the fleet view:
    # lean fixed-shape entries only -- notably no fail_reason (arbitrary
    # operator text) and no command line, which stay on this node's own API.
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    out = JobOutputStream()
    out.close()
    cron.last_run["alpha"] = cronstable.cron.JobRunInfo(
        outcome="failure",
        exit_code=3,
        started_at=DT(1999, 12, 31, 11, 59, 58, tzinfo=UTC),
        finished_at=DT(1999, 12, 31, 12, 0, 0, tzinfo=UTC),
        fail_reason="boom",
        output=out,
    )
    summaries = cron.fleet_job_summaries()
    assert set(summaries) == {"alpha", "beta"}
    alpha = summaries["alpha"]
    assert alpha["running"] is False
    assert alpha["enabled"] is True
    assert isinstance(alpha["scheduled_in"], float)
    assert alpha["last"] == {
        "outcome": "failure",
        "finished_at": "1999-12-31T12:00:00+00:00",
        "duration": 2.0,
        "exit_code": 3,
    }
    assert "fail_reason" not in alpha["last"]
    # beta is disabled (and an @reboot one-shot): no next fire, no last run
    beta = summaries["beta"]
    assert beta == {
        "running": False,
        "enabled": False,
        "scheduled_in": None,
        "last": None,
    }
    # a running instance flips the flag and suppresses the next-fire estimate
    cron.running_jobs["alpha"] = ["sentinel"]
    alpha = cron.fleet_job_summaries()["alpha"]
    assert alpha["running"] is True
    assert alpha["scheduled_in"] is None


# =====================================================================
#  second-level (sub-minute) scheduling
# =====================================================================

_SECONDS_JOB = """
jobs:
  - name: sec
    command: echo sec
    schedule: "*/15 * * * * * *"
  - name: min
    command: echo min
    schedule: "* * * * *"
"""


def _set_now(monkeypatch, holder):
    # a controllable clock: holder["now"] is a naive datetime, localised to the
    # requested timezone exactly as the real fixed_current_time fixture does.
    def get_now(timezone):
        now = holder["now"]
        if timezone is not None:
            now = (
                now.replace(tzinfo=timezone)
                if now.tzinfo is None
                else now.astimezone(timezone)
            )
        return now

    monkeypatch.setattr("cronstable.cron.get_now", get_now)


def test_schedule_slot_resolution(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 0, 0, 4, 500000)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SECONDS_JOB)
    sec = cron.cron_jobs["sec"]
    minute = cron.cron_jobs["min"]
    # a second-level job truncates to the whole second (microseconds zeroed)
    assert cronstable.cron.schedule_slot(sec) == DT(
        2020, 1, 1, 0, 0, 4, tzinfo=UTC
    )
    # a minute-level job truncates to the top of the minute, as always
    assert cronstable.cron.schedule_slot(minute) == DT(
        2020, 1, 1, 0, 0, 0, tzinfo=UTC
    )


def test_needs_subminute():
    # an enabled second-level job makes the scheduler tick per-second
    cron = cronstable.cron.Cron(None, config_yaml=_SECONDS_JOB)
    assert cron._needs_subminute() is True
    # minute-only config does not
    cron2 = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    assert cron2._needs_subminute() is False
    # a DISABLED second-level job must not force per-second ticking
    disabled = """
jobs:
  - name: sec
    command: echo sec
    schedule: "*/15 * * * * * *"
    enabled: false
"""
    cron3 = cronstable.cron.Cron(None, config_yaml=disabled)
    assert cron3._needs_subminute() is False


@pytest.mark.parametrize(
    "second, should_run",
    [(0, True), (1, False), (14, False), (15, True), (45, True), (46, False)],
)
def test_job_should_run_at_seconds(monkeypatch, second, should_run):
    holder = {"now": DT(2020, 1, 1, 0, 0, second)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SECONDS_JOB)
    job = cron.cron_jobs["sec"]  # "*/15 * * * * * *"
    assert cron.job_should_run(False, job) is should_run


def test_next_sleep_interval_modes(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 30, 30, 500000)}
    _set_now(monkeypatch, holder)
    # minute mode snaps to the next minute (preserving the sub-second offset,
    # exactly as the historical behaviour did): from :30.5 that is 30.0s away.
    assert cronstable.cron.next_sleep_interval(False) == pytest.approx(30.0)
    # sub-minute mode snaps to the next whole-second boundary: :30.5 -> :31.0
    assert cronstable.cron.next_sleep_interval(True) == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_spawn_jobs_subminute_dedup(monkeypatch):
    # A minute-level job fires exactly once per minute and a second-level job
    # exactly once per matching second, even when the loop wakes more than once
    # in a second (a duplicate tick). The forward-only next-fire index de-dupes
    # structurally: once a slot has fired the job's next fire has already
    # advanced past it. Start-up seeds strictly-future, so the second and
    # minute in progress at start-up are skipped, not fired for a partial run.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SECONDS_JOB)

    launched = []

    async def fake_launch(job):
        launched.append((holder["now"].second, holder["now"].minute, job.name))

    monkeypatch.setattr(cron, "launch_scheduled_job", fake_launch)

    async def tick(minute, second, *, startup=False):
        holder["now"] = DT(2020, 1, 1, 0, minute, second)
        await cron._service_slots(startup)

    await tick(0, 0, startup=True)  # start-up: seed strictly-future, fire none
    await tick(0, 15)  # sec fires (min not due until :01:00)
    await tick(0, 15)  # duplicate tick in the same second: de-duped
    await tick(0, 30)  # sec only
    await tick(0, 45)  # sec only
    await tick(1, 0)  # new minute: sec + min both fire

    sec_fires = [(m, s) for (s, m, n) in launched if n == "sec"]
    min_fires = [(m, s) for (s, m, n) in launched if n == "min"]
    assert sec_fires == [(0, 15), (0, 30), (0, 45), (1, 0)]
    assert min_fires == [(1, 0)]  # once, despite the ticks through the minute


_EVERY_SECOND_AND_MINUTE = """
jobs:
  - name: tick
    command: echo tick
    schedule: "* * * * * * *"
  - name: noon
    command: echo noon
    schedule: "0 12 * * *"
"""


def _drive_cron(monkeypatch, holder, config_yaml):
    """A Cron wired to the controllable clock, recording (name, slot-second)
    at each launch by reading the de-dup slot spawn_jobs just set."""
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=config_yaml)
    launched = []

    async def fake_launch(job):
        launched.append((job.name, cron._last_run_slot[job.name].second))

    monkeypatch.setattr(cron, "launch_scheduled_job", fake_launch)
    return cron, launched


def _seed_due(cron, *names):
    """Make the named jobs due *now* by seeding the next-fire index at the
    current (frozen) clock, so a direct spawn_jobs(False) call services them.
    Mirrors what a real pass does once the loop has been running -- the loop's
    start-up seeding is strictly-future, which deliberately skips the current
    slot."""
    now = cronstable.cron.get_now(datetime.timezone.utc)
    for name in names:
        cron._set_next_fire(name, now)


@pytest.mark.asyncio
async def test_service_slots_catches_up_overrun_seconds(monkeypatch):
    # A pass that overruns by a couple of seconds (the clock jumps forward
    # between passes) must not silently drop the seconds it skipped: the next
    # pass services each skipped whole-second slot too.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    cron, launched = _drive_cron(monkeypatch, holder, _EVERY_SECOND_AND_MINUTE)

    await cron._service_slots(startup=True)  # startup at :00 seeds, fires none
    assert launched == []
    holder["now"] = DT(2020, 1, 1, 0, 0, 3)  # the :00 pass overran to :03
    await cron._service_slots(startup=False)

    # every skipped second :01, :02, :03 is serviced (the every-second job
    # fires once for each), rather than only :03.
    assert [s for (n, s) in launched if n == "tick"] == [1, 2, 3]


@pytest.mark.asyncio
async def test_service_slots_bounds_catchup_after_long_gap(monkeypatch):
    # A gap larger than CATCHUP_LIMIT is a stall/suspend, not tick overhead:
    # resume at the current second instead of replaying a burst.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    cron, launched = _drive_cron(monkeypatch, holder, _EVERY_SECOND_AND_MINUTE)

    await cron._service_slots(startup=True)
    gap = int(cronstable.cron.CATCHUP_LIMIT.total_seconds()) + 5
    holder["now"] = DT(2020, 1, 1, 0, 0, gap)
    await cron._service_slots(startup=False)

    # only the current second fires -- no backdated storm of the skipped ones
    assert [s for (n, s) in launched if n == "tick"] == [gap]


@pytest.mark.asyncio
async def test_startup_seeding_skips_in_progress_minute(monkeypatch):
    # Restarting partway through a minute must not fire a minute-level job for
    # the minute already under way, even though a second-level job is present
    # (which forces per-second ticking). Regression: without startup seeding
    # the minute job fired ~1s after a mid-minute restart.
    holder = {"now": DT(2020, 1, 1, 0, 5, 30)}
    cron, launched = _drive_cron(monkeypatch, holder, _SECONDS_JOB)

    await cron._service_slots(startup=True)  # restart at 00:05:30
    holder["now"] = DT(2020, 1, 1, 0, 5, 31)
    await cron._service_slots(startup=False)
    # the in-progress minute is skipped -- "min" must not have fired
    assert "min" not in [n for (n, s) in launched]

    holder["now"] = DT(2020, 1, 1, 0, 6, 0)  # next minute boundary
    await cron._service_slots(startup=False)
    assert ("min", 0) in launched  # now it fires, once, at the fresh boundary


@pytest.mark.asyncio
async def test_single_slot_job_fires_once_across_boundary(monkeypatch):
    # A single-slot job (noon) serviced tick-by-tick across the minute boundary
    # fires exactly once. Regression for the two-clock-read TOCTOU: the due
    # test and the de-dup key are now one and the same read, so the boundary
    # cannot double-launch it.
    holder = {"now": DT(2020, 1, 1, 11, 59, 58)}
    cron, launched = _drive_cron(monkeypatch, holder, _EVERY_SECOND_AND_MINUTE)

    await cron._service_slots(startup=True)
    for sec in (59, 0, 1, 2):
        minute = 59 if sec == 59 else 0
        hour = 11 if sec == 59 else 12
        holder["now"] = DT(2020, 1, 1, hour, minute, sec)
        await cron._service_slots(startup=False)

    noon_fires = [s for (n, s) in launched if n == "noon"]
    assert noon_fires == [0]  # exactly one launch, at second 0 of 12:00


# =====================================================================
#  concurrent launches + off-loop config reparse
# =====================================================================

_THREE_DUE = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
  - name: b
    command: echo b
    schedule: "* * * * *"
  - name: c
    command: echo c
    schedule: "* * * * *"
"""


@pytest.mark.asyncio
async def test_spawn_jobs_launches_concurrently(monkeypatch):
    # Jobs due in the same slot are launched concurrently, not one at a time:
    # all three enter their (blocking) launch before any of them completes, so
    # a slot's wall time is ~one spawn-time instead of N x spawn-time. Under
    # the old sequential loop only the first launch would ever start (it blocks
    # on `release`), so `started` would never fire and this would time out.
    cron = cronstable.cron.Cron(None, config_yaml=_THREE_DUE)

    order = []
    started = asyncio.Event()
    release = asyncio.Event()

    async def fake_launch(job):
        order.append(job.name)
        if len(order) == 3:
            started.set()
        await release.wait()

    monkeypatch.setattr(cron, "launch_scheduled_job", fake_launch)

    _seed_due(cron, "a", "b", "c")  # all three due this pass
    task = asyncio.create_task(cron.spawn_jobs(False))
    try:
        # all three launches are in flight at once (would hang if sequential)
        await asyncio.wait_for(started.wait(), timeout=2)
        assert order == ["a", "b", "c"]  # scheduled in config order
    finally:
        release.set()
        await asyncio.wait_for(task, timeout=2)


@pytest.mark.asyncio
async def test_single_due_job_still_launches(monkeypatch):
    # The len == 1 fast path (await directly, no gather) still launches the one
    # due job, so the optimisation does not regress the common single-job slot.
    # Only "alpha" is due here ("beta" is a disabled @reboot one-shot).
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    launched = []

    async def fake_launch(job):
        launched.append(job.name)

    monkeypatch.setattr(cron, "launch_scheduled_job", fake_launch)
    # alpha is "*/5 * * * *"; the frozen clock is 12:00, a multiple of 5.
    # Seed it due so this single-pass call services it (beta is a disabled
    # @reboot one-shot and never enters the next-fire index).
    _seed_due(cron, "alpha")
    await cron.spawn_jobs(False)
    assert launched == ["alpha"]


@pytest.mark.asyncio
async def test_reload_runs_off_event_loop(tmp_path, monkeypatch):
    # The once-a-minute reparse is offloaded to a worker thread so a slow disk
    # read + parse cannot freeze the event loop (and stall the scheduling
    # tick). Prove every run-loop reparse executes off the event-loop thread.
    import itertools
    import threading

    cfg = tmp_path / "c.yaml"
    cfg.write_text(TWO_JOBS)
    # construction parses once, synchronously, on this (the loop) thread
    cron = cronstable.cron.Cron(str(cfg))
    main_thread = threading.get_ident()

    seen = []
    real_parse = cronstable.cron.parse_config_with_sources

    def recording_parse(arg):
        seen.append(threading.get_ident())
        return real_parse(arg)

    monkeypatch.setattr(
        "cronstable.cron.parse_config_with_sources", recording_parse
    )
    monkeypatch.setattr("cronstable.cron.next_sleep_interval", lambda *a: 0.01)
    # Force every housekeeping pass to reparse by defeating the
    # unchanged-config skip cache: an ever-incrementing signature never equals
    # the stored one, so reload_config always treats the config as changed and
    # offloads the parse. This test is about WHERE the reparse runs (a worker
    # thread), not about the skip cache's change detection -- which
    # test_run_reloads_changed_config already covers via a real on-disk edit.
    # Driving the reparse this way keeps the test off filesystem timing
    # entirely: relying on real size/mtime changes to trigger successive
    # reparses races the parse->record window (reload_config re-stats the file
    # when recording the parse result, so a second rapid rewrite lands inside
    # that window and is absorbed into the record -- the next reparse never
    # fires). That race is benign in production (reloads are ~60s apart) but is
    # deterministic under this test's 10ms ticks on Windows / Python <= 3.12,
    # whose coarse ~15.6ms asyncio timer lands every rewrite inside the window
    # -- which hung this test in CI.
    _sig_counter = itertools.count()
    monkeypatch.setattr(
        cron, "_config_signature", lambda files: next(_sig_counter)
    )

    task = asyncio.create_task(cron.run())
    try:
        await _wait_until(lambda: len(seen) >= 2)  # a couple of reload ticks
    finally:
        cron.signal_shutdown()
        await asyncio.wait_for(task, timeout=5)

    assert len(seen) >= 2  # the loop reparsed on each on-disk change
    assert all(t != main_thread for t in seen)  # ...always off the loop thread


_LEADER_REBOOT_BAD_CLUSTER = """
jobs:
  - name: boot
    command: echo boot
    schedule: "@reboot"
    clusterPolicy: Leader
cluster:
  listen: "127.0.0.1:18444"
  tls:
    ca: /nonexistent/ca.pem
    cert: /nonexistent/cert.pem
    key: /nonexistent/key.pem
  peers:
    - host: b:8443
    - host: c:8443
  electLeader: true
"""


@pytest.mark.asyncio
async def test_startup_gates_reboot_before_servicing(tmp_path, monkeypatch):
    # Housekeeping (which sets the cluster gate _elect_leader_configured via
    # start_stop_cluster) must run BEFORE the first spawn_jobs, so a Leader
    # @reboot job is deferred to the elected owner rather than run ungated on
    # every node. This must hold even though the reparse is now offloaded to a
    # worker thread (reload_config): it is still awaited and applied before
    # _service_slots. Here the manager fails to start (bad certs), so the
    # Leader one-shot must stay deferred and fail closed -- never launched.
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_LEADER_REBOOT_BAD_CLUSTER)
    cron = cronstable.cron.Cron(str(cfg))

    launched = []

    async def fake_launch(job):
        launched.append(job.name)

    monkeypatch.setattr(cron, "launch_scheduled_job", fake_launch)
    monkeypatch.setattr("cronstable.cron.next_sleep_interval", lambda *a: 0.01)

    task = asyncio.create_task(cron.run())
    try:
        await _wait_until(lambda: cron._elect_leader_configured)
    finally:
        cron.signal_shutdown()
        await asyncio.wait_for(task, timeout=5)

    assert cron.cluster_manager is None  # bad certs -> no manager
    assert "boot" in cron._pending_reboot_jobs  # deferred, not run ungated
    assert launched == []  # never launched anywhere


# =====================================================================
#  next-fire index + monotonic-sleep behaviour, and a perf demonstration
# =====================================================================

_ONE_MINUTE_JOB = """
jobs:
  - name: m
    command: echo m
    schedule: "* * * * *"
"""

_NOON_DAILY = """
jobs:
  - name: noon
    command: echo noon
    schedule: "0 12 * * *"
"""

_TZ_JOBS = """
jobs:
  - name: utc
    command: echo utc
    schedule: "*/10 * * * *"
  - name: local
    command: echo local
    schedule: "*/10 * * * *"
    utc: false
  - name: la
    command: echo la
    schedule: "*/10 * * * *"
    timezone: America/Los_Angeles
"""

_RELOAD_BEFORE = """
jobs:
  - name: keep
    command: echo keep
    schedule: "* * * * *"
  - name: drop
    command: echo drop
    schedule: "* * * * *"
"""

_RELOAD_AFTER = """
jobs:
  - name: keep
    command: echo keep
    schedule: "* * * * *"
  - name: added
    command: echo added
    schedule: "*/5 * * * *"
"""


def test_compute_next_fire_is_now_plus_delay_utc(monkeypatch):
    # The index instant is exactly now + the cron engine's delay-to-next-match,
    # the same formula the dashboard countdown and the Prometheus next-run
    # gauge use, so all three agree.
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_MINUTE_JOB)
    now = cronstable.cron.get_now(UTC)
    job = cron.cron_jobs["m"]
    delay = job.schedule.next(now=now, default_utc=True)
    assert cron._compute_next_fire(job, now) == now + datetime.timedelta(
        seconds=delay
    )
    # strictly-future: the in-progress minute (:00) is skipped for :01
    assert cron._compute_next_fire(job, now) == DT(
        2020, 1, 1, 0, 1, tzinfo=UTC
    )


def test_compute_next_fire_lands_on_a_matching_slot(monkeypatch):
    # Whatever the timezone, the computed next-fire instant, rendered back into
    # the job's own frame, satisfies the cron expression -- so the heap fires
    # the job exactly when the old test()-based tick would have matched. Uses a
    # */10 schedule so the next fire is minutes away (no DST boundary crossed).
    holder = {"now": DT(2020, 6, 1, 12, 34, 56)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_TZ_JOBS)
    now = cronstable.cron.get_now(UTC)
    for name, job in cron.cron_jobs.items():
        fire = cron._compute_next_fire(job, now)
        assert fire is not None and fire.tzinfo is not None
        assert fire > now
        if job.timezone is not None:
            frame = fire.astimezone(job.timezone)
        else:
            frame = fire.astimezone().replace(tzinfo=None)
        assert job.schedule.test(frame.replace(microsecond=0)), name


def test_sleep_interval_uses_soonest_fire(monkeypatch):
    # The loop sleeps until the soonest job's next fire, not a fixed tick.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(
        None, config_yaml=_SECONDS_JOB
    )  # sec */15 + min
    cron._ensure_seeded(cronstable.cron.get_now(UTC))
    # soonest is the */15 job at :15 -> 15s away (the minute job is 60s away)
    assert cron._sleep_interval() == pytest.approx(15.0, abs=0.05)


def test_sleep_interval_capped_by_housekeeping(monkeypatch):
    # A sparse job hours away still wakes the loop within the next wall-minute,
    # so config reload / cluster upkeep stays ~once a minute.
    holder = {"now": DT(2020, 1, 1, 3, 0, 15)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(
        None, config_yaml=_NOON_DAILY
    )  # next fire 12:00
    cron._ensure_seeded(cronstable.cron.get_now(UTC))
    # capped at the next minute boundary (03:01:00), i.e. 45s
    assert cron._sleep_interval() == pytest.approx(45.0, abs=0.05)


def test_sleep_interval_no_jobs_uses_housekeeping(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 3, 0, 15)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None)  # nothing scheduled
    assert cron._peek_soonest_fire() is None
    assert cron._sleep_interval() == pytest.approx(45.0, abs=0.05)


@pytest.mark.asyncio
async def test_backward_clock_step_does_not_refire(monkeypatch):
    # The heart of the clock-step immunity: next-fire advances forward-only, so
    # an NTP/clock step BACKWARD defers the next fire rather than re-firing an
    # already-fired slot. The old tick+test scheduler re-matched the earlier
    # second and fired it a second time.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    cron, launched = _drive_cron(monkeypatch, holder, _EVERY_SECOND_AND_MINUTE)
    await cron._service_slots(startup=True)  # seed tick->:01, noon->12:00

    holder["now"] = DT(2020, 1, 1, 0, 0, 5)
    await cron._service_slots(startup=False)  # tick fires :01..:05
    assert [s for (n, s) in launched if n == "tick"] == [1, 2, 3, 4, 5]

    launched.clear()
    # the wall clock jumps BACK 3 seconds
    holder["now"] = DT(2020, 1, 1, 0, 0, 2)
    await cron._service_slots(startup=False)
    assert [s for (n, s) in launched if n == "tick"] == []  # no re-fire

    # ...and it resumes cleanly once the clock passes the pending fire again
    holder["now"] = DT(2020, 1, 1, 0, 0, 6)
    await cron._service_slots(startup=False)
    assert [s for (n, s) in launched if n == "tick"] == [6]


_EVERY_15MIN = """
jobs:
  - name: j15
    command: echo j15
    schedule: "*/15 * * * *"
"""


@pytest.mark.asyncio
async def test_long_gap_sparse_job_fires_only_if_current_slot_matches(
    monkeypatch,
):
    # After a gap beyond CATCHUP_LIMIT, a sparse job resumes EXACTLY where the
    # old tick would: it fires only if NOW's own slot matches the schedule, not
    # a stale most-recent occurrence. Regression: an earlier draft fired the
    # most recent missed slot (00:30), backdating a launch the old scheduler
    # never made.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    cron, launched = _drive_cron(monkeypatch, holder, _EVERY_15MIN)
    await cron._service_slots(startup=True)  # seed j15 -> 00:15:00

    # froze ~37 minutes, resuming at 00:37 -- NOT a */15 slot
    holder["now"] = DT(2020, 1, 1, 0, 37, 0)
    await cron._service_slots(startup=False)
    assert launched == []  # nothing backdated (00:37 is not a */15 slot)

    # ...and it resyncs: the next real fire at 00:45 still happens
    holder["now"] = DT(2020, 1, 1, 0, 45, 0)
    await cron._service_slots(startup=False)
    assert [n for (n, s) in launched] == ["j15"]


@pytest.mark.asyncio
async def test_long_gap_resumes_at_matching_current_slot(monkeypatch):
    # The mirror of the above: when the resume instant DOES land on a matching
    # slot, the job fires once there (the frequently-scheduled / on-boundary
    # case), matching the old "fire the current slot" behaviour.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    cron, launched = _drive_cron(monkeypatch, holder, _EVERY_15MIN)
    await cron._service_slots(startup=True)  # seed j15 -> 00:15:00
    holder["now"] = DT(2020, 1, 1, 0, 45, 0)  # froze to a */15 boundary
    await cron._service_slots(startup=False)
    assert [(n, s) for (n, s) in launched] == [("j15", 0)]  # once, at 00:45


@pytest.mark.asyncio
async def test_large_forward_jump_does_not_enumerate_window(monkeypatch):
    # A large forward clock jump / long suspend must NOT walk the missed window
    # occurrence-by-occurrence: for a per-second job an 8h gap is ~28,800
    # occurrences (and an RTC-less 1970->now boot is billions), which would
    # block the event loop and exhaust memory. The long-gap branch resumes at
    # the current slot with O(1) crontab work. Regression guard for the
    # review's
    # high-severity finding.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    cron, launched = _drive_cron(monkeypatch, holder, _EVERY_SECOND_AND_MINUTE)
    await cron._service_slots(startup=True)  # seed tick -> 00:00:01
    counts = _count_crontab_calls(monkeypatch)

    holder["now"] = DT(2020, 1, 1, 8, 0, 0)  # jump forward 8 hours
    await cron._service_slots(startup=False)

    # O(1), not ~28,800 (one crontab.next per second of the gap)
    assert counts["next"] <= 3
    # ...and the per-second job still fires once, at the current second
    assert [s for (n, s) in launched if n == "tick"] == [0]


_LOCAL_MINUTE_JOB = """
jobs:
  - name: loc
    command: echo loc
    schedule: "* * * * *"
    utc: false
"""


@pytest.mark.asyncio
async def test_last_run_slot_is_aware_utc_in_both_advance_branches(
    monkeypatch,
):
    # _last_run_slot must never mix naive and aware datetimes. The normal
    # catch-up branch records the aware-UTC next-fire instant; the long-gap
    # branch records schedule_slot(), which is NAIVE local for a utc:false /
    # no-timezone job, so it converts back to UTC before recording. Regression
    # for the review finding: an earlier draft stored the naive slot, leaving
    # _last_run_slot[name] after a long-gap resume mutually incomparable with
    # the value the normal branch stores (a TypeError on any ordering).
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    cron, launched = _drive_cron(monkeypatch, holder, _LOCAL_MINUTE_JOB)
    await cron._service_slots(startup=True)  # seed loc -> next minute boundary

    # normal branch: a short overrun within CATCHUP_LIMIT
    holder["now"] = DT(2020, 1, 1, 0, 1, 2)
    await cron._service_slots(startup=False)
    normal = cron._last_run_slot["loc"]
    assert normal.tzinfo == UTC

    # long-gap branch: a gap beyond CATCHUP_LIMIT resumes at the current slot
    holder["now"] = DT(2020, 1, 1, 0, 30, 0)
    await cron._service_slots(startup=False)
    longgap = cron._last_run_slot["loc"]
    assert longgap.tzinfo == UTC
    # both values are now comparable (a naive one would raise TypeError here)
    assert longgap > normal


def test_reload_utc_to_timezone_utc_preserves_next_fire(tmp_path, monkeypatch):
    # utc:true and an explicit `timezone: UTC` fire on identical instants, so a
    # reconfiguration between them must NOT be treated as a schedule change (an
    # object-identity timezone compare made datetime.timezone.utc != ZoneInfo
    # ("UTC") and forced a reseed that could skip a boundary fire). Regression
    # guard for the review's _same_schedule finding.
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_ONE_MINUTE_JOB)  # utc:true by default
    cron = cronstable.cron.Cron(str(cfg))
    assert cron._next_fire["m"] == DT(2020, 1, 1, 0, 1, tzinfo=UTC)

    # reload AT the boundary with an explicit `timezone: UTC` added
    holder["now"] = DT(2020, 1, 1, 0, 1, 0)
    cfg.write_text(_ONE_MINUTE_JOB.rstrip() + "\n    timezone: UTC\n")
    cron.update_config()
    # kept at 00:01:00 (a spurious reseed would jump it to 00:02:00)
    assert cron._next_fire["m"] == DT(2020, 1, 1, 0, 1, tzinfo=UTC)


@pytest.mark.asyncio
async def test_minute_job_missed_minutes_fires_once(monkeypatch):
    # A minute-level job whose scheduler froze across several minutes fires
    # ONCE on resume (no backdated storm), matching cron's outage semantics --
    # the per-job catch-up bound (CATCHUP_LIMIT) unifies this with sub-minute
    # catch-up.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    cron, launched = _drive_cron(monkeypatch, holder, _ONE_MINUTE_JOB)
    await cron._service_slots(startup=True)  # seed m -> 00:01:00
    holder["now"] = DT(2020, 1, 1, 0, 5, 30)  # froze ~4.5 minutes
    await cron._service_slots(startup=False)
    assert [n for (n, s) in launched] == ["m"]  # once, not five times


def test_reload_preserves_unchanged_next_fire(tmp_path, monkeypatch):
    # A reload that does NOT change a job's schedule keeps its next-fire, so a
    # reload landing on the job's own boundary minute never recomputes a
    # strictly-future fire and skips that fire.
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_ONE_MINUTE_JOB)
    cron = cronstable.cron.Cron(str(cfg))
    assert cron._next_fire["m"] == DT(2020, 1, 1, 0, 1, tzinfo=UTC)

    # reload AT the boundary minute, same schedule
    holder["now"] = DT(2020, 1, 1, 0, 1, 0)
    cron.update_config()
    # kept at 00:01:00 (a reseed would have jumped it to 00:02:00, dropping the
    # fire due this very minute)
    assert cron._next_fire["m"] == DT(2020, 1, 1, 0, 1, tzinfo=UTC)


def test_reload_reseeds_changed_schedule(tmp_path, monkeypatch):
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_ONE_MINUTE_JOB)
    cron = cronstable.cron.Cron(str(cfg))
    assert cron._next_fire["m"] == DT(2020, 1, 1, 0, 1, tzinfo=UTC)

    cfg.write_text(_ONE_MINUTE_JOB.replace('"* * * * *"', '"*/5 * * * *"'))
    cron.update_config()
    # reseeded strictly-future for the NEW schedule
    assert cron._next_fire["m"] == DT(2020, 1, 1, 0, 5, tzinfo=UTC)


def test_reload_refreshes_index(tmp_path, monkeypatch):
    # One reload exercises all three reconciliations: an unchanged job keeps
    # its fire, a removed job leaves the index, a new job is seeded
    # strictly-future.
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_RELOAD_BEFORE)
    cron = cronstable.cron.Cron(str(cfg))
    keep_before = cron._next_fire["keep"]
    assert set(cron._next_fire) == {"keep", "drop"}

    cfg.write_text(_RELOAD_AFTER)
    cron.update_config()
    assert set(cron._next_fire) == {"keep", "added"}  # drop gone, added in
    assert cron._next_fire["keep"] == keep_before  # unchanged kept
    assert cron._next_fire["added"] == DT(2020, 1, 1, 0, 5, tzinfo=UTC)


def test_reload_drops_disabled_job(tmp_path, monkeypatch):
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_ONE_MINUTE_JOB)
    cron = cronstable.cron.Cron(str(cfg))
    assert "m" in cron._next_fire

    cfg.write_text(_ONE_MINUTE_JOB.rstrip() + "\n    enabled: false\n")
    cron.update_config()
    assert "m" not in cron._next_fire  # disabled -> not scheduled


def test_reload_prunes_finished_run_maps(tmp_path, monkeypatch):
    # _apply_reload prunes _last_run_slot and the metric series of removed
    # jobs; last_run and run_history must go with them. A removed job's
    # display data is unreachable (every payload guards on cron_jobs
    # membership first), so keeping it is a pure leak -- worst under classic
    # crontabs, whose <file>:<line> job names are reminted by every line
    # added or removed above them.
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_RELOAD_BEFORE)
    cron = cronstable.cron.Cron(str(cfg))
    for name in ("keep", "drop"):
        info = cronstable.cron.JobRunInfo(
            outcome="success",
            exit_code=0,
            started_at=DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC),
            finished_at=DT(2020, 1, 1, 0, 0, 1, tzinfo=UTC),
            fail_reason=None,
            output=JobOutputStream(),
        )
        cron.last_run[name] = info
        cron.run_history[name].append(info)

    cfg.write_text(_RELOAD_AFTER)
    cron.update_config()
    assert "drop" not in cron.last_run  # removed job's data pruned
    assert "drop" not in cron.run_history
    assert "keep" in cron.last_run  # surviving job's data kept
    assert len(cron.run_history["keep"]) == 1


def _count_crontab_calls(monkeypatch):
    import cronstable.cronexpr as crontab_mod

    counts = {"test": 0, "next": 0}
    orig_test = crontab_mod.CronTab.test
    orig_next = crontab_mod.CronTab.next

    def counting_test(self, entry):
        counts["test"] += 1
        return orig_test(self, entry)

    def counting_next(self, *a, **k):
        counts["next"] += 1
        return orig_next(self, *a, **k)

    monkeypatch.setattr(crontab_mod.CronTab, "test", counting_test)
    monkeypatch.setattr(crontab_mod.CronTab, "next", counting_next)
    return counts


@pytest.mark.asyncio
async def test_perf_wake_is_o_due_not_o_all(monkeypatch, capsys):
    # PERFORMANCE DEMONSTRATION. The next-fire index turns a wake from
    # O(all jobs) into O(due jobs): over a large fleet, a wake where nothing is
    # due performs ZERO crontab matches (a heap peek), and a wake with a cohort
    # due matches only that cohort -- independent of fleet size. The old
    # tick+test loop called CronTab.test once per job per tick, i.e. O(all).
    N = 2000
    jobs = "\n".join(
        "  - name: j{0}\n    command: echo {0}\n"
        '    schedule: "{1} * * * *"'.format(i, i % 60)
        for i in range(N)
    )
    holder = {"now": DT(2020, 1, 1, 0, 30, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml="jobs:\n" + jobs)

    async def fake_launch(job):
        pass

    monkeypatch.setattr(cron, "launch_scheduled_job", fake_launch)

    # one-time seeding (startup) is O(all); every wake AFTER it is O(due)
    await cron._service_slots(startup=True)
    assert len(cron._next_fire) == N

    counts = _count_crontab_calls(monkeypatch)

    # a wake where nothing is due: no crontab work at all
    holder["now"] = DT(2020, 1, 1, 0, 30, 1)
    await cron._service_slots(startup=False)
    assert counts["test"] == 0  # the O(all) per-tick scan primitive is gone
    assert counts["next"] == 0  # the heap said nothing is due -> no work

    # a wake where the minute-31 cohort (~N/60 jobs) is due
    holder["now"] = DT(2020, 1, 1, 0, 31, 0)
    await cron._service_slots(startup=False)
    due = sum(1 for i in range(N) if i % 60 == 31)
    assert counts["test"] == 0  # still never scans-and-tests the whole fleet
    assert counts["next"] == due  # exactly one advance per due job -> O(due)

    # wall-time: an idle heap wake over N jobs vs the old O(all) test() scan
    idle_reps = 50
    t0 = time.perf_counter()
    for _ in range(idle_reps):
        await cron._service_slots(startup=False)  # nothing due now
    new_idle = (time.perf_counter() - t0) / idle_reps

    sample = next(iter(cron.cron_jobs.values()))
    slot = cronstable.cron.schedule_slot(sample, holder["now"])
    scan_reps = 50
    t0 = time.perf_counter()
    for _ in range(scan_reps):
        for job in cron.cron_jobs.values():  # what the old tick did every wake
            job.schedule.test(slot)
    old_scan = (time.perf_counter() - t0) / scan_reps

    with capsys.disabled():
        print(
            "\n[perf] fleet={0} jobs | idle heap wake {1:.1f}us "
            "vs old O(all) test scan {2:.0f}us  (~{3:.0f}x faster)".format(
                N,
                new_idle * 1e6,
                old_scan * 1e6,
                old_scan / max(new_idle, 1e-12),
            )
        )
    # the idle wake must be dramatically cheaper than scanning every job
    assert new_idle < old_scan


# ---------------------------------------------------------------------------
# runtime pause/resume: the scheduler-side core
# ---------------------------------------------------------------------------

_PAUSABLE_JOB = """
jobs:
  - name: p
    command: echo hi
    schedule: "* * * * *"
"""


def _launch_recorder(monkeypatch, cron):
    launched = []

    async def fake(job, *, with_retries=True):
        launched.append(job.name)
        return True

    monkeypatch.setattr(cron, "maybe_launch_job", fake)
    return launched


@pytest.mark.asyncio
async def test_pause_gate_skips_fire_and_writes_skipped_row(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_PAUSABLE_JOB)
    launched = _launch_recorder(monkeypatch, cron)
    await cron.pause_job_by_name("p", note="maint", by="op")
    await cron.launch_scheduled_job(cron.cron_jobs["p"])
    assert launched == []  # the due fire was skipped, not launched
    info = cron.last_run["p"]
    assert info.outcome == "skipped"
    assert info.skip_reason == "paused"
    assert info.started_at is None
    assert info.exit_code is None
    # finished_at is SET (the skip instant): this is what advances the
    # derived catch-up watermark across the pause window.
    assert info.finished_at == DT(2020, 1, 1, 0, 0, 30, tzinfo=UTC)
    assert info.output.closed is True
    # the synthetic row round-trips through the ledger record shape
    restored = cronstable.cron._job_run_info_from_dict(info.to_dict())
    assert restored is not None
    assert restored.outcome == "skipped"
    assert restored.skip_reason == "paused"


@pytest.mark.asyncio
async def test_pause_expiry_resumes_firing(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_PAUSABLE_JOB)
    launched = _launch_recorder(monkeypatch, cron)
    await cron.pause_job_by_name("p", duration=60)
    assert cron._pause_active("p") is not None
    # ... the window ends; expiry is reader-enforced, no resume call needed
    holder["now"] = DT(2020, 1, 1, 0, 2, 0)
    assert cron._pause_active("p") is None
    await cron.launch_scheduled_job(cron.cron_jobs["p"])
    assert launched == ["p"]
    assert "p" not in cron.last_run  # no skipped row for a launched fire


def test_pause_periodic_sweeps_expired_entries(monkeypatch, caplog):
    import logging

    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_PAUSABLE_JOB)
    cron._paused["p"] = cronstable.cron.PauseInfo(
        since=DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC),
        until=DT(2020, 1, 1, 0, 0, 10, tzinfo=UTC),
        note="",
        by="op",
        channel="api",
    )
    with caplog.at_level(logging.INFO, logger="cronstable"):
        cron._pause_periodic()
    assert "p" not in cron._paused
    assert any("pause expired" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_manual_start_allowed_while_paused(monkeypatch):
    cron = cronstable.cron.Cron(None, config_yaml=_PAUSABLE_JOB)
    launched = _launch_recorder(monkeypatch, cron)
    await cron.pause_job_by_name("p")
    # a pause skips SCHEDULED fires only: the operator asking by hand is the
    # operator overriding their own pause (unlike the disabled 409).
    await cron.start_job_by_name("p")
    assert launched == ["p"]


@pytest.mark.asyncio
async def test_retry_defers_across_pause_and_fires_after_resume(monkeypatch):
    monkeypatch.setattr(cronstable.cron, "RETRY_GATE_RECHECK_FLOOR", 0.02)
    cron = cronstable.cron.Cron(None, config_yaml=_PAUSABLE_JOB)
    launched = _launch_recorder(monkeypatch, cron)
    await cron.pause_job_by_name("p")
    state = JobRetryState(0.01, 2, 60)
    state.next_delay()
    cron.retry_state["p"] = state
    task = asyncio.create_task(cron.schedule_retry_job("p", 0.0, 1))
    state.task = task
    # the due attempt DEFERS while paused: neither launched nor cancelled
    await asyncio.sleep(0.2)
    assert launched == []
    assert "p" in cron.retry_state
    await cron.resume_job_by_name("p")
    await asyncio.wait_for(task, timeout=5)
    assert launched == ["p"]


def test_reload_keeps_pause_and_prunes_removed_jobs(tmp_path, monkeypatch):
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_RELOAD_BEFORE)
    cron = cronstable.cron.Cron(str(cfg))
    for name in ("keep", "drop"):
        cron._paused[name] = cronstable.cron.PauseInfo(
            since=DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC),
            until=DT(2020, 1, 1, 6, 0, 0, tzinfo=UTC),
            note="",
            by="op",
            channel="api",
        )
    cfg.write_text(_RELOAD_AFTER)
    cron.update_config()
    # pause survives the reload for the surviving job (a config edit does
    # not clear it: no digest check, unlike retries)...
    assert cron._pause_active("keep") is not None
    # ...and the paused job STAYS in the fire heap, so its slots keep being
    # skipped and the watermark keeps advancing.
    assert "keep" in cron._next_fire
    # the removed job's entry is pruned, not leaked
    assert "drop" not in cron._paused


@pytest.mark.asyncio
async def test_catch_up_defers_a_paused_job_instead_of_latching_it(
    monkeypatch,
):
    # A pause is transient and excuses only the slots inside its own window,
    # so catch-up must DEFER a paused job (like a transient cluster denial),
    # never latch it done: latching forfeits a backlog owed from before the
    # pause began, and catch-up is one-shot per process.
    holder = {"now": DT(2020, 1, 1, 0, 10, 0)}
    _set_now(monkeypatch, holder)
    yaml = (
        "jobs:\n  - name: p\n    command: echo hi\n"
        '    schedule: "* * * * *"\n    onMissed: run-all\n'
    )
    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    await cron.pause_job_by_name("p")
    unresolved = await cron._evaluate_catch_up(
        DT(2020, 1, 1, 0, 10, 0, tzinfo=UTC)
    )
    assert unresolved is True
    assert "p" not in cron._catchup_done
    assert not cron._catchup_tasks
    # once the pause lifts the job is evaluated for real
    await cron.resume_job_by_name("p")
    unresolved = await cron._evaluate_catch_up(
        DT(2020, 1, 1, 0, 10, 0, tzinfo=UTC)
    )
    assert unresolved is False
    assert "p" in cron._catchup_done


@pytest.mark.asyncio
async def test_origin_middleware_covers_pause_and_resume_routes():
    from aiohttp import web

    middleware = cronstable.cron.Cron._make_origin_middleware(frozenset())

    async def handler(request):
        return web.Response(text="ok")

    class FakeRequest:
        def __init__(self, path):
            self.method = "POST"
            self.headers = {"Origin": "https://evil.example"}
            self.host = "localhost:8021"
            self.path = path

    # the new mutating routes get the same CSRF/origin gate as start/cancel
    for path in ("/jobs/x/pause", "/jobs/x/resume"):
        with pytest.raises(web.HTTPForbidden):
            await middleware(FakeRequest(path), handler)


# ---------------------------------------------------------------------------
# per-job SLA monitor (_sla_periodic) and onLate dispatch
# ---------------------------------------------------------------------------

_SLA_STALE_JOB = """
jobs:
  - name: s
    command: echo hi
    schedule: "* * * * *"
    sla:
      maxTimeSinceSuccessSeconds: 3600
"""

_SLA_LATE_JOB = """
jobs:
  - name: s
    command: echo hi
    schedule: "* * * * *"
    sla:
      lateAfterSeconds: 120
"""

_SLA_RUNTIME_JOB = """
jobs:
  - name: s
    command: echo hi
    schedule: "* * * * *"
    sla:
      maxRuntimeSeconds: 600
"""

_SLA_EXEMPT_JOBS = """
jobs:
  - name: pd
    command: echo hi
    schedule: "* * * * *"
    sla:
      maxTimeSinceSuccessSeconds: 60
  - name: dd
    command: echo hi
    schedule: "* * * * *"
    enabled: false
    sla:
      maxTimeSinceSuccessSeconds: 60
"""

STALE = cronstable.cron.SLA_CHECK_STALE
LATE = cronstable.cron.SLA_CHECK_LATE
RUNTIME = cronstable.cron.SLA_CHECK_RUNTIME


def _sla_report_recorder(monkeypatch):
    reports = []

    async def fake(ctx, report_config):
        reports.append((ctx, report_config))

    monkeypatch.setattr(cronstable.cron, "report_sla_breach", fake)
    return reports


@pytest.mark.asyncio
async def test_pause_and_sla_pass_survives_a_broken_config(monkeypatch):
    # The pause sweep and the SLA monitor must NOT share run()'s reload
    # try/except: a broken config file on disk raises out of reload_config,
    # which would skip every later statement in that block. Going quiet about
    # jobs that stopped running is the exact failure late-run detection
    # exists to report, so the pass is guarded on its own.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 11, 30, 0, tzinfo=UTC)

    # a reload that raises must not stop the pass from latching the breach
    def boom():
        raise cronstable.config.ConfigError("bad yaml on disk")

    monkeypatch.setattr(cron, "reload_config", boom)
    holder["now"] = DT(2020, 1, 1, 13, 30, 0)
    cron._pause_and_sla_periodic()
    assert cron._sla_state[("s", STALE)] == DT(
        2020, 1, 1, 13, 30, 0, tzinfo=UTC
    )
    assert cron.metrics._job("s").sla_late == {STALE: 1}


@pytest.mark.asyncio
async def test_sla_stale_check_breaches_and_clears(monkeypatch, caplog):
    import logging

    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    reports = _sla_report_recorder(monkeypatch)

    # within threshold: no latch, but the series exist at 0 from the first
    # evaluation (so increase() has a baseline before the first breach)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 11, 30, 0, tzinfo=UTC)
    cron._sla_periodic()
    assert ("s", STALE) not in cron._sla_state
    assert cron.metrics._job("s").sla_late == {STALE: 0}
    assert cron.metrics._job("s").sla_breaches == {STALE: 0}

    # the success ages past the threshold: latch, gauge, counter, warning
    holder["now"] = DT(2020, 1, 1, 13, 30, 0)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        cron._sla_periodic()
    assert cron._sla_state[("s", STALE)] == DT(2020, 1, 1, 13, 30, 0,
                                               tzinfo=UTC)
    assert cron.metrics._job("s").sla_late == {STALE: 1}
    assert cron.metrics._job("s").sla_breaches == {STALE: 1}
    assert any("SLA check" in r.getMessage() for r in caplog.records)

    # a recorded success (any path: _record_run feeds the tracker) clears it
    caplog.clear()
    info = cronstable.cron.JobRunInfo(
        outcome="success",
        exit_code=0,
        started_at=DT(2020, 1, 1, 13, 31, 0, tzinfo=UTC),
        finished_at=DT(2020, 1, 1, 13, 31, 5, tzinfo=UTC),
        fail_reason=None,
        output=JobOutputStream(),
    )
    cron._record_run("s", info)
    assert cron._sla_last_success["s"] == info.finished_at
    holder["now"] = DT(2020, 1, 1, 13, 32, 0)
    with caplog.at_level(logging.INFO, logger="cronstable"):
        cron._sla_periodic()
    assert ("s", STALE) not in cron._sla_state
    assert cron.metrics._job("s").sla_late == {STALE: 0}
    assert any("recovered" in r.getMessage() for r in caplog.records)
    # the breach fired onLate exactly once, on the ok-to-breached transition
    await cron._drain_completions()
    assert len(reports) == 1


@pytest.mark.asyncio
async def test_sla_late_after_check_breaches_and_clears(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_LATE_JOB)
    _sla_report_recorder(monkeypatch)

    due = DT(2020, 1, 1, 11, 55, 0, tzinfo=UTC)
    cron._sla_due["s"] = due
    # a start BEFORE the due slot does not excuse it
    cron._sla_last_start["s"] = DT(2020, 1, 1, 11, 50, 0, tzinfo=UTC)
    cron._sla_periodic()
    assert ("s", LATE) in cron._sla_state
    assert cron.metrics._job("s").sla_late == {LATE: 1}

    # any actual launch at/after the due slot clears it on the next pass
    cron._sla_last_start["s"] = DT(2020, 1, 1, 12, 0, 30, tzinfo=UTC)
    holder["now"] = DT(2020, 1, 1, 12, 1, 0)
    cron._sla_periodic()
    assert ("s", LATE) not in cron._sla_state
    assert cron.metrics._job("s").sla_late == {LATE: 0}
    await cron._drain_completions()


@pytest.mark.asyncio
async def test_sla_late_after_within_grace_is_not_breached(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 1, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_LATE_JOB)
    _sla_report_recorder(monkeypatch)
    # 60s past the slot, threshold 120: within the grace window
    cron._sla_due["s"] = DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC)
    cron._sla_periodic()
    assert ("s", LATE) not in cron._sla_state


@pytest.mark.asyncio
async def test_sla_max_runtime_check_breaches_and_clears(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_RUNTIME_JOB)
    reports = _sla_report_recorder(monkeypatch)

    class _FakeRun:
        started_at = DT(2020, 1, 1, 11, 30, 0, tzinfo=UTC)

    cron.running_jobs["s"] = [_FakeRun()]
    cron._sla_periodic()
    assert ("s", RUNTIME) in cron._sla_state
    assert cron.metrics._job("s").sla_late == {RUNTIME: 1}
    await cron._drain_completions()
    (ctx, _), = reports
    assert ctx.sla_check == RUNTIME
    assert ctx.observed_seconds == 1800.0
    assert ctx.threshold_seconds == 600

    # the run ends: the check observes nothing running and clears (the
    # monitor never kills anything)
    cron.running_jobs["s"] = []
    cron._sla_periodic()
    assert ("s", RUNTIME) not in cron._sla_state
    assert cron.metrics._job("s").sla_late == {RUNTIME: 0}
    await cron._drain_completions()
    assert len(reports) == 1  # the clear reports nothing


@pytest.mark.asyncio
async def test_sla_latch_fires_onlate_exactly_once(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    reports = _sla_report_recorder(monkeypatch)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC)

    cron._sla_periodic()
    holder["now"] = DT(2020, 1, 1, 12, 1, 0)
    cron._sla_periodic()  # still breached: the latch holds, no re-report
    await cron._drain_completions()

    assert len(reports) == 1
    ctx, report_config = reports[0]
    assert ctx.config is cron.cron_jobs["s"]
    assert ctx.sla_check == STALE
    assert ctx.threshold_seconds == 3600
    assert ctx.observed_seconds == 7200.0
    assert ctx.last_success_at == "2020-01-01T10:00:00+00:00"
    assert report_config is cron.cron_jobs["s"].onLate["report"]
    # latched once: the counter shows one incident, not one per pass
    assert cron.metrics._job("s").sla_breaches == {STALE: 1}


@pytest.mark.asyncio
async def test_sla_paused_and_disabled_jobs_are_exempt(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_EXEMPT_JOBS)
    reports = _sla_report_recorder(monkeypatch)
    await cron.pause_job_by_name("pd", duration=7200)

    # both jobs are far past the 60s threshold (no success on record and
    # the process is an hour old), but paused/disabled are excused
    holder["now"] = DT(2020, 1, 1, 13, 0, 0)
    cron._sla_periodic()
    assert not cron._sla_state
    await cron._drain_completions()
    assert reports == []

    # resume: the same condition latches again, proving it was real. The
    # hour it spent paused is credited against the staleness measurement
    # (see test_sla_pause_time_is_credited_against_staleness), so the clock
    # has to move past the threshold once more before it can page.
    await cron.resume_job_by_name("pd")
    cron._sla_periodic()
    assert not cron._sla_state
    holder["now"] = DT(2020, 1, 1, 13, 30, 0)
    cron._sla_periodic()
    assert ("pd", STALE) in cron._sla_state
    assert ("dd", STALE) not in cron._sla_state
    await cron._drain_completions()
    assert len(reports) == 1


@pytest.mark.asyncio
async def test_sla_cluster_gate_is_per_job(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 13, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC)

    monkeypatch.setattr(cron, "_cluster_allows", lambda job: False)
    cron._sla_periodic()
    assert not cron._sla_state  # not the owner: not evaluated, no page

    monkeypatch.setattr(cron, "_cluster_allows", lambda job: True)
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    await cron._drain_completions()


@pytest.mark.asyncio
async def test_sla_restart_baseline_without_a_store(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)

    # stateless boot, no success ever known: baselined on process start,
    # so the check does NOT page instantly...
    cron._sla_periodic()
    assert not cron._sla_state

    # ...nor within the threshold...
    holder["now"] = DT(2020, 1, 1, 12, 30, 0)
    cron._sla_periodic()
    assert not cron._sla_state

    # ...but it ages into the breach like a normal miss
    holder["now"] = DT(2020, 1, 1, 13, 30, 0)
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    await cron._drain_completions()


@pytest.mark.asyncio
async def test_sla_warmed_last_success_drives_the_check(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    reports = _sla_report_recorder(monkeypatch)

    # a durable ledger rehydrate warmed the real last success (see
    # _rehydrate_from_state): a genuinely stale job pages right after
    # boot, process-start grace does not apply
    cron._sla_last_success["s"] = DT(2020, 1, 1, 9, 0, 0, tzinfo=UTC)
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    await cron._drain_completions()
    (ctx, _), = reports
    assert ctx.last_success_at == "2020-01-01T09:00:00+00:00"


@pytest.mark.asyncio
async def test_sla_due_slot_excused_while_paused(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_LATE_JOB)

    async def fake_launch(job):
        return None

    monkeypatch.setattr(cron, "launch_scheduled_job", fake_launch)
    job = cron.cron_jobs["s"]

    await cron.pause_job_by_name("s")
    slot = DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC)
    await cron._launch_plan([(job, [slot])])
    # the pause-skipped slot is excused from lateAfter, but the slot
    # bookkeeping itself still advances
    assert "s" not in cron._sla_due
    assert cron._last_run_slot["s"] == slot

    await cron.resume_job_by_name("s")
    slot2 = DT(2020, 1, 1, 12, 1, 0, tzinfo=UTC)
    await cron._launch_plan([(job, [slot2])])
    assert cron._sla_due["s"] == slot2


@pytest.mark.asyncio
async def test_sla_last_start_set_on_actual_launch(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)

    class FakeRunningJob:
        def __init__(self, config, retry_state, **kwargs):
            self.config = config
            self.started_at = None

        async def start(self):
            pass

    monkeypatch.setattr(cronstable.cron, "RunningJob", FakeRunningJob)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_LATE_JOB)
    assert "s" not in cron._sla_last_start
    assert await cron.maybe_launch_job(cron.cron_jobs["s"]) is True
    assert cron._sla_last_start["s"] == DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC)


def test_sla_payload_shape(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 30, 0)}
    _set_now(monkeypatch, holder)
    yaml = (
        "jobs:\n"
        "  - name: s\n    command: echo hi\n"
        '    schedule: "* * * * *"\n'
        "    sla:\n"
        "      maxTimeSinceSuccessSeconds: 3600\n"
        "      lateAfterSeconds: 120\n"
        "  - name: plain\n    command: echo hi\n"
        '    schedule: "* * * * *"\n'
    )
    cron = cronstable.cron.Cron(None, config_yaml=yaml)

    # no sla block: no "sla" key at all
    plain = cron._job_to_dict("plain", cron.cron_jobs["plain"])
    assert "sla" not in plain

    # configured, nothing latched: thresholds only carry the non-null keys
    payload = cron._job_to_dict("s", cron.cron_jobs["s"])
    assert payload["sla"] == {
        "thresholds": {
            "maxTimeSinceSuccessSeconds": 3600,
            "lateAfterSeconds": 120,
        },
        "state": "ok",
        "breaches": [],
    }

    # one latched breach: state flips and the entry carries the latch
    # instant with a LIVE observed value (measured at payload time)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC)
    cron._sla_state[("s", STALE)] = DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC)
    payload = cron._job_to_dict("s", cron.cron_jobs["s"])
    assert payload["sla"]["state"] == "late"
    assert payload["sla"]["breaches"] == [
        {
            "check": STALE,
            "since": "2020-01-01T12:00:00+00:00",
            "observed_seconds": 9000.0,
            "threshold_seconds": 3600,
        }
    ]


@pytest.mark.asyncio
async def test_sla_dropped_check_latch_is_cleared(monkeypatch):
    # a reload can drop one check while keeping the sla block; its stale
    # latch must clear instead of showing late forever
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    cron._sla_state[("s", RUNTIME)] = DT(2020, 1, 1, 11, 0, 0, tzinfo=UTC)
    cron._sla_periodic()
    assert ("s", RUNTIME) not in cron._sla_state
    assert cron.metrics._job("s").sla_late[RUNTIME] == 0
    await cron._drain_completions()


def test_reload_prunes_sla_trackers(tmp_path, monkeypatch):
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_RELOAD_BEFORE)
    cron = cronstable.cron.Cron(str(cfg))
    at = DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC)
    for name in ("keep", "drop"):
        cron._sla_last_success[name] = at
        cron._sla_due[name] = at
        cron._sla_last_start[name] = at
        cron._sla_state[(name, STALE)] = at
    cfg.write_text(_RELOAD_AFTER)
    cron.update_config()
    # the surviving job's trackers survive the edit (history is history)...
    assert cron._sla_last_success["keep"] == at
    assert cron._sla_due["keep"] == at
    assert cron._sla_last_start["keep"] == at
    assert ("keep", STALE) in cron._sla_state
    # ...and the removed job's are pruned, latch included
    assert "drop" not in cron._sla_last_success
    assert "drop" not in cron._sla_due
    assert "drop" not in cron._sla_last_start
    assert ("drop", STALE) not in cron._sla_state


# ---------------------------------------------------------------------------
# SLA: exemption clears the latch, and false lateAfter pages
# ---------------------------------------------------------------------------

_SLA_CLUSTER_LATE_JOB = """
jobs:
  - name: s
    command: echo hi
    schedule: "* * * * *"
    concurrencyScope: cluster
    concurrencyPolicy: Forbid
    sla:
      lateAfterSeconds: 120
"""

_SLA_FORBID_LATE_JOB = """
jobs:
  - name: s
    command: echo hi
    schedule: "*/10 * * * *"
    concurrencyPolicy: Forbid
    sla:
      lateAfterSeconds: 300
"""


@pytest.mark.asyncio
async def test_sla_pause_clears_a_latch_taken_before_the_pause(monkeypatch):
    # regression (#17/#33/#38): a job that latched a breach and is THEN
    # paused was skipped whole by the monitor, so cronstable_job_late, the
    # /jobs sla block and the OVERDUE chip stayed pinned at breached for the
    # entire pause window, for a job the operator deliberately silenced.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC)

    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    assert cron.metrics._job("s").sla_late == {STALE: 1}

    # the pause drops the latch on the API call itself, not a minute later
    await cron.pause_job_by_name("s", duration=86400)
    assert not cron._sla_state
    assert cron.metrics._job("s").sla_late == {STALE: 0}
    payload = cron._job_to_dict("s", cron.cron_jobs["s"])
    assert payload["sla"]["state"] == "ok"
    assert payload["paused"] is not None

    # and the monitor keeps it clear for the whole window
    holder["now"] = DT(2020, 1, 1, 23, 0, 0)
    cron._sla_periodic()
    assert not cron._sla_state
    assert cron.metrics._job("s").sla_late == {STALE: 0}
    await cron._drain_completions()


@pytest.mark.asyncio
async def test_sla_latch_clears_when_disabled_or_not_owned(monkeypatch):
    # regression (#17): the same freeze through the other two exemptions.
    # Disabling a job, or losing it to another node under election, must
    # drop its latch rather than leave the gauge asserting a live breach.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC)
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state

    cron.cron_jobs["s"].enabled = False
    cron._sla_periodic()
    assert not cron._sla_state
    assert cron.metrics._job("s").sla_late == {STALE: 0}

    # re-enabled and still breaching: it re-latches and pages once
    cron.cron_jobs["s"].enabled = True
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state

    # losing ownership excuses it the same way, and drops the lateAfter
    # reference with it so regaining ownership cannot page for a slot the
    # owner of the day ran on time
    cron._sla_due["s"] = DT(2020, 1, 1, 11, 0, 0, tzinfo=UTC)
    monkeypatch.setattr(cron, "_cluster_allows", lambda job: False)
    cron._sla_periodic()
    assert not cron._sla_state
    assert cron.metrics._job("s").sla_late == {STALE: 0}
    assert "s" not in cron._sla_due
    await cron._drain_completions()


@pytest.mark.asyncio
async def test_sla_due_is_only_recorded_by_the_owning_node(monkeypatch):
    # regression (#18): _launch_plan recorded the due slot BEFORE the
    # ownership gate, so a follower accumulated due slots it never launched
    # and had no matching _sla_last_start. The first leader failover then
    # paged a false lateAfter breach on the incoming owner, for slots the
    # dead leader had run on time.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_LATE_JOB)
    reports = _sla_report_recorder(monkeypatch)
    launched = []

    async def fake_launch(job):
        launched.append(job.name)

    monkeypatch.setattr(cron, "launch_scheduled_job", fake_launch)
    monkeypatch.setattr(cron, "_cluster_allows", lambda job: False)

    job = cron.cron_jobs["s"]
    for minute in (55, 56, 57, 58, 59):
        slot = DT(2020, 1, 1, 11, minute, 0, tzinfo=UTC)
        await cron._launch_plan([(job, [slot])])
    assert launched == []
    # the follower launched nothing, so it owes nothing: no due reference
    assert "s" not in cron._sla_due
    # ...but the slot bookkeeping the status payload reads still advances
    assert cron._last_run_slot["s"] == DT(2020, 1, 1, 11, 59, 0, tzinfo=UTC)

    # this node wins the election three minutes later: nothing to page for
    monkeypatch.setattr(cron, "_cluster_allows", lambda job: True)
    holder["now"] = DT(2020, 1, 1, 12, 3, 0)
    cron._sla_periodic()
    assert ("s", LATE) not in cron._sla_state
    assert cron.metrics._job("s").sla_late == {LATE: 0}
    await cron._drain_completions()
    assert reports == []


@pytest.mark.asyncio
async def test_sla_due_excused_when_a_peer_holds_the_cluster_slot(monkeypatch):
    # regression (#16): a node that records the slot as due and is then
    # denied the cluster concurrency slot by a LIVE peer never launches, so
    # its _sla_last_start never advances and lateAfter latches on every node
    # that lost the race, for a job the fleet is running normally.
    import cronstable.state as state_mod

    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_CLUSTER_LATE_JOB)
    reports = _sla_report_recorder(monkeypatch)

    class _PeerHeldBackend:
        async def read_lease(self, name):
            return state_mod.Lease(
                name=name, holder="peer#1", fence=1, expires_at=1e12
            )

    async def no_reason():
        return None

    async def denied(backend, lease_name):
        return None

    cron.state_backend = _PeerHeldBackend()
    cron._state_configured = True
    monkeypatch.setattr(cron, "_slot_fidelity_reason", no_reason)
    monkeypatch.setattr(cron, "_acquire_slot_lease", denied)

    cron._sla_due["s"] = DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC)
    assert await cron.maybe_launch_job(cron.cron_jobs["s"]) is False
    assert "s" not in cron._sla_due

    holder["now"] = DT(2020, 1, 1, 12, 5, 0)
    cron._sla_periodic()
    assert ("s", LATE) not in cron._sla_state
    await cron._drain_completions()
    assert reports == []


@pytest.mark.asyncio
async def test_sla_late_after_excused_while_an_instance_runs(monkeypatch):
    # regression (#23): a slot Forbid dropped because the previous instance is
    # STILL RUNNING is not a late slot, so one healthy long run must not page
    # lateAfter (which, with maxRuntime also set, paged it twice). The excuse
    # is now RECORDED by maybe_launch_job's Forbid drop (it pops the due slot),
    # not only inferred live from running_jobs -- see the residual test below.
    holder = {"now": DT(2020, 1, 1, 12, 16, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_FORBID_LATE_JOB)
    reports = _sla_report_recorder(monkeypatch)

    class _FakeRun:
        started_at = DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC)

    # the 12:10 slot fires while the 12:00 instance still runs: _launch_plan
    # records it as due, and the Forbid drop in maybe_launch_job pops it
    cron.running_jobs["s"] = [_FakeRun()]
    cron._sla_due["s"] = DT(2020, 1, 1, 12, 10, 0, tzinfo=UTC)
    cron._sla_last_start["s"] = DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC)
    assert await cron.maybe_launch_job(cron.cron_jobs["s"]) is False
    assert "s" not in cron._sla_due
    cron._sla_periodic()
    assert ("s", LATE) not in cron._sla_state
    assert cron.metrics._job("s").sla_late == {LATE: 0}
    await cron._drain_completions()
    assert reports == []

    # the live running_jobs guard still excuses a due slot recorded for a
    # round while an instance is running (belt-and-braces alongside the pop)
    cron._sla_due["s"] = DT(2020, 1, 1, 12, 10, 0, tzinfo=UTC)
    cron._sla_periodic()
    assert ("s", LATE) not in cron._sla_state
    await cron._drain_completions()
    assert reports == []


@pytest.mark.asyncio
async def test_sla_late_after_forbid_drop_survives_the_run_ending(monkeypatch):
    # regression (#23 residual): the running_jobs guard only excused the
    # dropped slot WHILE the instance was alive; once the run ended and the
    # reaper emptied running_jobs, a stale _sla_due from the Forbid-dropped
    # slot latched lateAfter and dispatched onLate, in the window between the
    # run finishing and the next slot launching. Recording the excuse (popping
    # _sla_due in maybe_launch_job's Forbid drop) makes it survive the ending.
    holder = {"now": DT(2020, 1, 1, 12, 16, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_FORBID_LATE_JOB)
    reports = _sla_report_recorder(monkeypatch)

    class _FakeRun:
        started_at = DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC)

    # the 12:00 run is still going; the 12:10 slot fires and is Forbid-dropped
    cron.running_jobs["s"] = [_FakeRun()]
    cron._sla_due["s"] = DT(2020, 1, 1, 12, 10, 0, tzinfo=UTC)
    cron._sla_last_start["s"] = DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC)
    assert await cron.maybe_launch_job(cron.cron_jobs["s"]) is False

    # the run ends and the reaper empties running_jobs; without the pop the
    # stale 12:10 due latches lateAfter here (observed 420s > 300s)
    cron.running_jobs["s"] = []
    holder["now"] = DT(2020, 1, 1, 12, 17, 0)
    cron._sla_periodic()
    assert ("s", LATE) not in cron._sla_state
    await cron._drain_completions()
    assert reports == []

    # a genuinely unserved slot -- recorded for a round with no running
    # instance and never launched -- still latches lateAfter normally
    cron._sla_due["s"] = DT(2020, 1, 1, 12, 20, 0, tzinfo=UTC)
    holder["now"] = DT(2020, 1, 1, 12, 26, 0)
    cron._sla_periodic()
    assert ("s", LATE) in cron._sla_state
    await cron._drain_completions()
    assert len(reports) == 1


# ---------------------------------------------------------------------------
# SLA: the maxTimeSinceSuccess staleness baseline
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sla_first_seen_baselines_a_reload_added_job(
    tmp_path, monkeypatch
):
    # regression (#19): a job ADDED by a reload was baselined on process
    # start, so on a long-running daemon it paged maxTimeSinceSuccess on the
    # very tick it appeared, before it had any chance to run.
    holder = {"now": DT(2020, 1, 1, 9, 0, 0)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(
        "jobs:\n  - name: other\n    command: echo hi\n"
        '    schedule: "* * * * *"\n'
    )
    cron = cronstable.cron.Cron(str(cfg))
    _sla_report_recorder(monkeypatch)

    # a week of uptime, then the operator adds a job with a 25h threshold
    holder["now"] = DT(2020, 1, 8, 9, 0, 0)
    cfg.write_text(
        "jobs:\n  - name: other\n    command: echo hi\n"
        '    schedule: "* * * * *"\n'
        "  - name: db-vacuum\n    command: echo hi\n"
        '    schedule: "0 3 * * *"\n'
        "    sla:\n      maxTimeSinceSuccessSeconds: 90000\n"
    )
    cron.update_config()
    assert cron._sla_first_seen["db-vacuum"] == DT(
        2020, 1, 8, 9, 0, 0, tzinfo=UTC
    )
    cron._sla_periodic()
    assert not cron._sla_state

    # it ages into the breach from when it appeared, like any other job
    holder["now"] = DT(2020, 1, 9, 10, 30, 0)
    cron._sla_periodic()
    assert ("db-vacuum", STALE) in cron._sla_state
    await cron._drain_completions()


def test_reload_prunes_sla_first_seen_and_pause_windows(tmp_path, monkeypatch):
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_RELOAD_BEFORE)
    cron = cronstable.cron.Cron(str(cfg))
    at = DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC)
    assert set(cron._sla_first_seen) == {"keep", "drop"}
    for name in ("keep", "drop"):
        cron._sla_pause_windows[name] = [(at, at)]
    holder["now"] = DT(2020, 1, 1, 0, 1, 30)
    cfg.write_text(_RELOAD_AFTER)
    cron.update_config()
    # the removed job's entries are pruned, and the job the reload ADDED gets
    # its own first-seen baseline rather than inheriting the process start
    assert set(cron._sla_first_seen) == {"keep", "added"}
    assert cron._sla_first_seen["keep"] == DT(2020, 1, 1, 0, 0, 30, tzinfo=UTC)
    assert cron._sla_first_seen["added"] == DT(
        2020, 1, 1, 0, 1, 30, tzinfo=UTC
    )
    assert set(cron._sla_pause_windows) == {"keep"}


_SLA_DISABLED_STALE_JOB = (
    "jobs:\n  - name: db-vacuum\n    command: echo hi\n"
    '    schedule: "0 3 * * *"\n'
    "    enabled: false\n"
    "    sla:\n      maxTimeSinceSuccessSeconds: 90000\n"
)

_SLA_ENABLED_STALE_JOB = (
    "jobs:\n  - name: db-vacuum\n    command: echo hi\n"
    '    schedule: "0 3 * * *"\n'
    "    sla:\n      maxTimeSinceSuccessSeconds: 90000\n"
)


@pytest.mark.asyncio
async def test_sla_reenabled_after_a_disabled_span_gets_a_fresh_baseline(
    tmp_path, monkeypatch
):
    # regression (#19 residual): a job present at boot with enabled: false and
    # re-enabled by a later reload kept the process-start baseline it entered
    # with, so it paged maxTimeSinceSuccess instantly for the whole disabled
    # span before it had any chance to run. A disabled job cannot run, so its
    # staleness baseline must roll forward while it is switched off -- the same
    # credit a pause banks -- and it should age in only AFTER re-enabling.
    holder = {"now": DT(2020, 1, 1, 9, 0, 0)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_SLA_DISABLED_STALE_JOB)
    cron = cronstable.cron.Cron(str(cfg))
    reports = _sla_report_recorder(monkeypatch)
    assert cron._sla_first_seen["db-vacuum"] == DT(
        2020, 1, 1, 9, 0, 0, tzinfo=UTC
    )

    # a week switched off: each housekeeping tick rolls the baseline forward,
    # and a disabled job never pages
    for day in range(2, 9):
        holder["now"] = DT(2020, 1, day, 9, 0, 0)
        cron._sla_periodic()
        assert not cron._sla_state
    assert cron._sla_first_seen["db-vacuum"] == DT(
        2020, 1, 8, 9, 0, 0, tzinfo=UTC
    )

    # the operator re-enables it; the first tick after must NOT page for the
    # week it was deliberately off
    holder["now"] = DT(2020, 1, 8, 9, 1, 0)
    cfg.write_text(_SLA_ENABLED_STALE_JOB)
    cron.update_config()
    assert cron.cron_jobs["db-vacuum"].enabled is True
    cron._sla_periodic()
    assert ("db-vacuum", STALE) not in cron._sla_state
    await cron._drain_completions()
    assert reports == []

    # ...but it ages into the breach a full threshold (25h) after re-enabling,
    # from when it was turned on rather than from process start
    holder["now"] = DT(2020, 1, 9, 10, 30, 0)
    cron._sla_periodic()
    assert ("db-vacuum", STALE) in cron._sla_state
    await cron._drain_completions()
    assert len(reports) == 1


@pytest.mark.asyncio
async def test_sla_reenabled_after_a_disabled_span_credits_a_prior_success(
    tmp_path, monkeypatch
):
    # regression (#19 residual, the previously-succeeded arm): a job that HAD
    # recorded a success, then sat disabled longer than
    # maxTimeSinceSuccessSeconds, then re-enabled, paged the whole disabled
    # span instantly on the first tick. The _sla_first_seen roll-forward only
    # reaches the never-succeeded arm; _sla_stale_reference here returns the
    # week-old _sla_last_success. The disabled span must be banked as a
    # staleness credit (the same #22 pause-credit machinery) so a job the
    # operator deliberately switched off does not page for that span.
    holder = {"now": DT(2020, 1, 1, 9, 0, 0)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_SLA_DISABLED_STALE_JOB)
    cron = cronstable.cron.Cron(str(cfg))
    reports = _sla_report_recorder(monkeypatch)
    # the job succeeded once, an hour before the first disabled housekeeping
    # tick records the disabled-span start
    cron._sla_last_success["db-vacuum"] = DT(2020, 1, 1, 8, 0, 0, tzinfo=UTC)

    # a week switched off: a disabled job never pages, and the disabled-span
    # start is banked from the first disabled tick (2020-01-01 09:00)
    for day in range(1, 9):
        holder["now"] = DT(2020, 1, day, 9, 0, 0)
        cron._sla_periodic()
        assert not cron._sla_state
    assert cron._sla_disabled_since["db-vacuum"] == DT(
        2020, 1, 1, 9, 0, 0, tzinfo=UTC
    )

    # the operator re-enables it; the first tick must NOT page for the week it
    # was off: the disabled span is banked as a credit against the old success
    holder["now"] = DT(2020, 1, 8, 9, 1, 0)
    cfg.write_text(_SLA_ENABLED_STALE_JOB)
    cron.update_config()
    assert cron.cron_jobs["db-vacuum"].enabled is True
    cron._sla_periodic()
    assert ("db-vacuum", STALE) not in cron._sla_state
    await cron._drain_completions()
    assert reports == []
    # the span was banked and the tracker cleared at the transition
    assert "db-vacuum" not in cron._sla_disabled_since
    assert cron._sla_pause_windows["db-vacuum"] == [
        (
            DT(2020, 1, 1, 9, 0, 0, tzinfo=UTC),
            DT(2020, 1, 8, 9, 1, 0, tzinfo=UTC),
        )
    ]

    # ...but it still ages into the breach a full threshold (25h) after
    # re-enabling, measured from re-enable rather than the old success
    holder["now"] = DT(2020, 1, 9, 10, 30, 0)
    cron._sla_periodic()
    assert ("db-vacuum", STALE) in cron._sla_state
    await cron._drain_completions()
    assert len(reports) == 1


def test_sla_bank_pause_coalesces_out_of_order_overlapping_windows():
    # regression (#19 residual, the overlap arm): a job can be disabled first
    # (older since) and paused later (newer since), and _pause_periodic banks
    # the pause BEFORE _sla_periodic banks the older disabled span, so windows
    # reach _sla_bank_pause out of `since` order. The old merge only extended
    # the newest span's END, so the earlier disabled stretch was dropped and
    # the job paged the whole switched-off span on re-enable. The banked spans
    # must be the true disjoint union regardless of arrival order.
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC)

    def pause(since, until):
        return cronstable.cron.PauseInfo(
            since=since, until=until, note="", by="", channel="test"
        )

    # the later pause is banked first (newer since), then the older disabled
    # span that overlaps it: [10:00,10:05] then [09:00,10:02]
    cron._sla_bank_pause(
        "s",
        pause(DT(2020, 1, 8, 10, 0, tzinfo=UTC), DT(2020, 1, 8, 10, 5, tzinfo=UTC)),
        DT(2020, 1, 8, 10, 5, tzinfo=UTC),
    )
    cron._sla_bank_pause(
        "s",
        pause(DT(2020, 1, 8, 9, 0, tzinfo=UTC), DT(2020, 1, 8, 10, 2, tzinfo=UTC)),
        DT(2020, 1, 8, 10, 2, tzinfo=UTC),
    )
    # one coalesced span covering the whole union, not just the pause
    assert cron._sla_pause_windows["s"] == [
        (DT(2020, 1, 8, 9, 0, tzinfo=UTC), DT(2020, 1, 8, 10, 5, tzinfo=UTC))
    ]
    # and the credit is the full 65 minutes, counted once (no double-count of
    # the shared 10:00..10:02 stretch, no loss of the 09:00..10:00 stretch)
    credit = cron._sla_paused_seconds(
        "s",
        DT(2020, 1, 8, 8, 0, tzinfo=UTC),
        DT(2020, 1, 8, 11, 0, tzinfo=UTC),
    )
    assert credit == 65 * 60

    # a third window overlapping TWO existing spans coalesces them all
    cron._sla_bank_pause(
        "s",
        pause(DT(2020, 1, 8, 8, 30, tzinfo=UTC), DT(2020, 1, 8, 12, 0, tzinfo=UTC)),
        DT(2020, 1, 8, 12, 0, tzinfo=UTC),
    )
    assert cron._sla_pause_windows["s"] == [
        (DT(2020, 1, 8, 8, 30, tzinfo=UTC), DT(2020, 1, 8, 12, 0, tzinfo=UTC))
    ]


@pytest.mark.asyncio
async def test_sla_pause_time_is_credited_against_staleness(monkeypatch):
    # regression (#22): the staleness clock ran at full rate across a pause,
    # so an unattended job paged the first pass after the window expired,
    # for time the operator had declared it should not run.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC)

    # paused for four hours, well past the 1h threshold
    await cron.pause_job_by_name("s", duration=14400)
    holder["now"] = DT(2020, 1, 1, 4, 0, 1)
    cron._pause_and_sla_periodic()
    assert "s" not in cron._paused  # the window was swept
    assert not cron._sla_state  # ...and did not page as it lifted

    # the credit is exactly the window: half a threshold later, still quiet
    holder["now"] = DT(2020, 1, 1, 4, 30, 1)
    cron._sla_periodic()
    assert not cron._sla_state

    # a full threshold after the resume it pages, as a stale job should
    holder["now"] = DT(2020, 1, 1, 5, 0, 30)
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    await cron._drain_completions()


@pytest.mark.asyncio
async def test_sla_pause_credit_never_counts_a_window_twice(monkeypatch):
    # regression (#22): repeated and OVERLAPPING pauses must each be
    # credited once. Re-pausing a paused job replaces the window, so the
    # stretch the two share would otherwise be banked by both.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC)

    # pause 00:00 to 02:00, then re-pause at 01:00 for another two hours
    await cron.pause_job_by_name("s", duration=7200)
    holder["now"] = DT(2020, 1, 1, 1, 0, 0)
    await cron.pause_job_by_name("s", duration=7200)
    holder["now"] = DT(2020, 1, 1, 3, 0, 0)
    await cron.resume_job_by_name("s")
    # 00:00 to 03:00 held once, not 2h + 2h
    assert cron._sla_pause_windows["s"] == [
        (
            DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC),
            DT(2020, 1, 1, 3, 0, 0, tzinfo=UTC),
        )
    ]

    # a second, disjoint pause banks its own span
    holder["now"] = DT(2020, 1, 1, 3, 30, 0)
    await cron.pause_job_by_name("s", duration=1800)
    holder["now"] = DT(2020, 1, 1, 4, 0, 0)
    await cron.resume_job_by_name("s")
    assert len(cron._sla_pause_windows["s"]) == 2

    # 4h elapsed, 3.5h of it paused: half an hour of real staleness
    now = DT(2020, 1, 1, 4, 0, 0, tzinfo=UTC)
    obs = cron._sla_observations("s", cron.cron_jobs["s"], now)
    assert obs[STALE] == (3600, 1800.0, False)

    # and the credit retires once a success moves the reference past it
    info = cronstable.cron.JobRunInfo(
        outcome="success",
        exit_code=0,
        started_at=now,
        finished_at=now,
        fail_reason=None,
        output=JobOutputStream(),
    )
    cron._record_run("s", info)
    holder["now"] = DT(2020, 1, 1, 5, 30, 0)
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    await cron._drain_completions()


@pytest.mark.asyncio
async def test_sla_pause_credit_covers_a_window_spanning_a_restart(
    monkeypatch,
):
    # regression (#22): a pause rehydrated from the store after a restart
    # carries its original `since`, so the part of the window that elapsed
    # before the restart is credited too.
    holder = {"now": DT(2020, 1, 1, 6, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    # the ledger warm supplies a last success from before the pause began
    cron._sla_last_success["s"] = DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC)
    # ...and the pause refresh a window that started five hours ago
    cron._paused["s"] = cronstable.cron.PauseInfo(
        since=DT(2020, 1, 1, 1, 0, 0, tzinfo=UTC),
        until=DT(2020, 1, 1, 6, 30, 0, tzinfo=UTC),
        note="",
        by="op",
        channel="api",
    )
    holder["now"] = DT(2020, 1, 1, 6, 30, 0)
    cron._pause_and_sla_periodic()
    # 6.5h since the success, 5.5h of it paused: an hour of real staleness,
    # exactly at the threshold rather than six times past it
    assert "s" not in cron._paused
    assert not cron._sla_state
    holder["now"] = DT(2020, 1, 1, 7, 0, 30)
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    await cron._drain_completions()


# ---------------------------------------------------------------------------
# SLA: warming the staleness reference from the durable ledger
# ---------------------------------------------------------------------------


class _RecordBackend:
    """A ledger stub for the rehydrate warm-up: run records, nothing else."""

    def __init__(self, records):
        self._records = list(records)
        self.reads = []

    async def list_records(self, stream, limit=None, newest_first=False):
        self.reads.append((stream, limit))
        if not stream.startswith("runs/"):
            return []
        recs = sorted(
            self._records, key=lambda r: r["_seq"], reverse=newest_first
        )
        return recs[: limit or len(recs)]

    async def list_stream_names(self, prefix):
        return []


def _run_record(seq, outcome, finished_at):
    return {
        "_seq": seq,
        "outcome": outcome,
        "exit_code": 0 if outcome == "success" else 1,
        "started_at": finished_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "fail_reason": None,
    }


async def _warm_ledger(cron, records):
    backend = _RecordBackend(records)
    cron.state_backend = backend
    cron._state_rehydrated = False
    await cron._rehydrate_from_state()
    return backend


@pytest.mark.asyncio
async def test_sla_warm_takes_the_newest_success_by_finished_at(monkeypatch):
    # regression (#21): the warm walked the ledger by APPEND position, but
    # record files are named on write time and run-record writes are
    # unserialized, so the last-appended success can be older than one
    # appended before it. The reference must be the newest by finished_at.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    await _warm_ledger(
        cron,
        [
            _run_record(1, "success", DT(2020, 1, 1, 10, 5, 0, tzinfo=UTC)),
            _run_record(2, "success", DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC)),
        ],
    )
    assert cron._sla_last_success["s"] == DT(2020, 1, 1, 10, 5, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_sla_warm_widens_past_a_success_free_window(monkeypatch):
    # regression (#20): a job failing more often than the warm window is
    # wide has no success among the newest RUN_HISTORY_LIMIT records, and
    # the reference was left unset, re-baselining maxTimeSinceSuccess on
    # process start: every restart bought a genuinely stale job another
    # silent threshold.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    success_at = DT(2019, 12, 29, 12, 0, 0, tzinfo=UTC)
    records = [_run_record(0, "success", success_at)]
    records += [
        _run_record(
            n,
            "failure",
            DT(2019, 12, 30, 0, 0, 0, tzinfo=UTC)
            + datetime.timedelta(minutes=5 * n),
        )
        for n in range(1, 61)
    ]
    backend = await _warm_ledger(cron, records)
    # the restart does not buy it another silent threshold
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    assert cron._sla_last_success["s"] == success_at
    # exactly one deep re-read, on top of the ordinary warm read
    assert len([r for r in backend.reads if r[0] == "runs/s"]) == 2
    await cron._drain_completions()


@pytest.mark.asyncio
async def test_sla_warm_floors_on_the_oldest_record_without_a_success(
    monkeypatch,
):
    # regression (#20): with no success anywhere in the ledger the oldest
    # record still bounds the staleness from below (the true last success is
    # at or before it), which beats resetting the clock to process start.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    oldest = DT(2019, 12, 31, 0, 0, 0, tzinfo=UTC)
    records = [
        _run_record(n, "failure", oldest + datetime.timedelta(hours=n))
        for n in range(6)
    ]
    await _warm_ledger(cron, records)
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    assert cron._sla_last_success["s"] == oldest
    await cron._drain_completions()


def _mem_run(outcome, finished_at):
    """An in-memory JobRunInfo, for pre-seeding run_history before a warm."""
    return cronstable.cron.JobRunInfo(
        outcome=outcome,
        exit_code=0 if outcome == "success" else 1,
        started_at=finished_at,
        finished_at=finished_at,
        fail_reason=None,
        output=JobOutputStream(),
    )


@pytest.mark.asyncio
async def test_job_trends_payload_caches_within_ttl_and_busts_on_run():
    cron = cronstable.cron.Cron(None, config_yaml=_ONLY_IF_LAST_JOB)  # job "s"
    backend = _RecordBackend(
        [_run_record(1, "success", DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC))]
    )
    cron.state_backend = backend

    first = await cron.job_trends_payload("s")
    assert first is not None
    reads = len(backend.reads)
    assert reads >= 1

    # a second poll inside the TTL is served from cache: no new ledger read,
    # and the very same payload object comes back.
    again = await cron.job_trends_payload("s")
    assert len(backend.reads) == reads
    assert again is first

    # a locally finished run must bust the cache so the next poll re-reads;
    # detach the backend across _record_run so its fire-and-forget ledger
    # persist does not leave a pending task in the test.
    cron.state_backend = None
    cron._record_run("s", _mem_run("failure", DT(2020, 1, 1, 12, 5, tzinfo=UTC)))
    cron.state_backend = backend
    fresh = await cron.job_trends_payload("s")
    assert len(backend.reads) > reads
    assert fresh is not first

    # an unknown job never touches the cache or the backend.
    reads2 = len(backend.reads)
    assert await cron.job_trends_payload("nope") is None
    assert len(backend.reads) == reads2


def test_apply_reload_prunes_trends_cache_for_removed_jobs():
    # _trends_cache is busted per job by _record_run, but a job the reload
    # REMOVED (or a classic-crontab name reminted when a line shifts) never
    # runs again under that name, so without a reload-time prune its entry
    # would orphan forever -- a slow leak under name churn. It must be pruned
    # exactly like every other per-job map in _apply_reload.
    two = "jobs:\n" + "".join(
        "  - name: {n}\n    command: echo {n}\n    schedule: '* * * * *'\n".format(n=n)
        for n in ("keep", "gone")
    )
    one = "jobs:\n  - name: keep\n    command: echo keep\n    schedule: '* * * * *'\n"
    cron = cronstable.cron.Cron(None, config_yaml=two)
    cron._trends_cache["keep"] = (1e18, {"name": "keep"})
    cron._trends_cache["gone"] = (1e18, {"name": "gone"})
    cron._apply_reload(cronstable.config.parse_config_string(one, "t.yaml"))
    assert "keep" in cron._trends_cache
    assert "gone" not in cron._trends_cache


@pytest.mark.asyncio
async def test_sla_warm_seeds_reference_past_the_in_memory_history_guard(
    monkeypatch,
):
    # regression (#20 residual): the staleness seed lived INSIDE the two
    # early-continue guards that skip a job already carrying in-memory history.
    # A job that only FAILED during a state outage has non-empty run_history
    # when the store finally comes up, so the pre-await guard skipped seeding,
    # _sla_last_success stayed unset, and _sla_stale_reference fell back to
    # process start: a 3-day-stale job reported ~0s observed and stayed silent.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    # a failure recorded while the store was down: the pre-await guard fires
    cron.run_history["s"].append(
        _mem_run("failure", DT(2020, 1, 1, 11, 0, 0, tzinfo=UTC))
    )
    backend = _RecordBackend(
        [_run_record(1, "success", DT(2019, 12, 29, 12, 0, 0, tzinfo=UTC))]
    )
    cron.state_backend = backend
    cron._state_rehydrated = False
    await cron._rehydrate_from_state()
    # the guard no longer hides the real last success behind the live history
    assert cron._sla_last_success.get("s") == DT(
        2019, 12, 29, 12, 0, 0, tzinfo=UTC
    )
    # ...so the 3-day-stale job pages instead of re-baselining on process start
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    await cron._drain_completions()


@pytest.mark.asyncio
async def test_sla_warm_seeds_reference_when_a_run_lands_during_the_read(
    monkeypatch,
):
    # regression (#20 residual): the SECOND guard (a run finishing while the
    # rehydrate read awaited) skipped the seed the same way. The reference must
    # still be seeded from the live in-memory history before that continue.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)

    class _AppendDuringRead(_RecordBackend):
        # a run for "s" finishes (a failure) while the first runs/s read is in
        # flight, populating run_history exactly as the post-await guard tests.
        def __init__(self, records, cron):
            super().__init__(records)
            self._cron = cron
            self._fired = False

        async def list_records(
            self, stream, limit=None, newest_first=False
        ):
            recs = await super().list_records(stream, limit, newest_first)
            if stream == "runs/s" and not self._fired:
                self._fired = True
                self._cron.run_history["s"].append(
                    _mem_run(
                        "failure", DT(2020, 1, 1, 11, 0, 0, tzinfo=UTC)
                    )
                )
            return recs

    backend = _AppendDuringRead(
        [_run_record(1, "success", DT(2019, 12, 29, 12, 0, 0, tzinfo=UTC))],
        cron,
    )
    cron.state_backend = backend
    cron._state_rehydrated = False
    # run_history["s"] is empty at the loop's pre-await guard, so guard 1 does
    # not fire; the read side-effect trips guard 2 after the await
    assert not cron.run_history.get("s")
    await cron._rehydrate_from_state()
    assert cron._sla_last_success.get("s") == DT(
        2019, 12, 29, 12, 0, 0, tzinfo=UTC
    )
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    await cron._drain_completions()


_ONLY_IF_LAST_JOB = """
jobs:
  - name: s
    command: echo hi
    schedule: "* * * * *"
    onlyIfLastSucceeded: true
"""


@pytest.mark.asyncio
async def test_sla_warm_last_real_outcome_is_newest_by_finished_at(monkeypatch):
    # regression (#21 residual): the onlyIfLastSucceeded memo (_last_real_outcome)
    # was seeded by a positional walk of the warmed ring (the last-APPENDED
    # real outcome). Record files are named on WRITE time and run-record writes
    # are unserialized, so a success appended AFTER a newer failure would seed
    # a success as the last real outcome and reopen the gate this memo holds.
    # It must be the newest real outcome by finished_at.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_ONLY_IF_LAST_JOB)
    _sla_report_recorder(monkeypatch)
    # seq2 (the success) is appended AFTER seq1 (the failure) but finished
    # EARLIER, the write-order inversion the finding established
    await _warm_ledger(
        cron,
        [
            _run_record(1, "failure", DT(2020, 1, 1, 10, 5, 0, tzinfo=UTC)),
            _run_record(2, "success", DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC)),
        ],
    )
    assert cron._last_real_outcome["s"] == (
        DT(2020, 1, 1, 10, 5, 0, tzinfo=UTC),
        "failure",
    )


@pytest.mark.asyncio
async def test_depends_on_past_folds_all_ledger_records_by_finished_at(
    monkeypatch,
):
    # regression (#21 residual, the peer / shared-mount arm): the ledger arm
    # of _depends_on_past_ok took the FIRST real record newest-by-SEQUENCE
    # then broke, so an out-of-order write (a peer on the shared mount, or two
    # concurrencyPolicy: Allow runs racing) that landed a newer success ahead
    # of the true-newest failure cleared the gate on the stale success. The
    # memo cannot cover it (only THIS node's runs update the memo). The arm
    # must fold ALL records and pick the max by finished_at.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_ONLY_IF_LAST_JOB)
    # this node saw only its own success@10:00; that seeds the local memo
    await _warm_ledger(
        cron,
        [_run_record(1, "success", DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC))],
    )
    assert cron._last_real_outcome["s"] == (
        DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC),
        "success",
    )
    # a peer then appended to the shared ledger out of order: the TRUE newest
    # real outcome is a failure@10:20, but a later-sequence success@10:10 sits
    # ahead of it in newest-by-sequence order.
    cron.state_backend = _RecordBackend(
        [
            _run_record(1, "success", DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC)),
            _run_record(2, "failure", DT(2020, 1, 1, 10, 20, 0, tzinfo=UTC)),
            _run_record(3, "success", DT(2020, 1, 1, 10, 10, 0, tzinfo=UTC)),
        ]
    )
    # the gate must BLOCK: the newest real outcome by finished_at is a failure
    assert await cron._depends_on_past_ok(cron.cron_jobs["s"]) is False


@pytest.mark.asyncio
async def test_depends_on_past_picks_newest_in_memory_run_by_finished_at(
    monkeypatch,
):
    # regression (#21 residual, the in-memory arm): the ring walk took the
    # first real outcome by reversed() list position then broke. Two
    # concurrencyPolicy: Allow runs whose unserialized record writes land out
    # of order put an older success LAST in the ring behind a newer failure,
    # so the positional walk cleared the gate on the stale success. With no
    # backend this arm decides alone; it must pick the max by finished_at.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_ONLY_IF_LAST_JOB)
    assert cron.state_backend is None
    # the failure finished LATER (10:20) but the success was appended AFTER it
    # (10:10), so the success sits last in the ring
    cron.run_history["s"].append(
        _mem_run("failure", DT(2020, 1, 1, 10, 20, 0, tzinfo=UTC))
    )
    cron.run_history["s"].append(
        _mem_run("success", DT(2020, 1, 1, 10, 10, 0, tzinfo=UTC))
    )
    assert await cron._depends_on_past_ok(cron.cron_jobs["s"]) is False


# ---------------------------------------------------------------------------
# SLA: report ordering against real run completions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sla_report_never_blocks_a_run_completion(monkeypatch):
    # regression (#4): the onLate report installed itself as the job's
    # _completion_tail, which _queue_job_completion blocks on, so a slow
    # onLate reporter delayed the finished run's failure report AND its
    # retry arming. maxRuntime makes that the ordinary case: it breaches
    # while the run is still executing.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_RUNTIME_JOB)
    gate = asyncio.Event()
    handled = []

    async def hanging_report(ctx, report_config):
        await gate.wait()

    async def fake_failure(job):
        handled.append(job)

    monkeypatch.setattr(cronstable.cron, "report_sla_breach", hanging_report)
    monkeypatch.setattr(cron, "handle_job_failure", fake_failure)

    class _FakeRun:
        started_at = DT(2020, 1, 1, 11, 30, 0, tzinfo=UTC)
        config = cron.cron_jobs["s"]

    cron.running_jobs["s"] = [_FakeRun()]
    cron._sla_periodic()
    assert ("s", RUNTIME) in cron._sla_state

    # that same run now finishes while the reporter is still hung
    cron._queue_job_completion(_FakeRun(), failed=True)
    for _ in range(20):
        await asyncio.sleep(0)
    assert len(handled) == 1

    # the report waits on its own tail; the completion tail stays owned by
    # real completions
    assert "s" in cron._sla_report_tail
    gate.set()
    await cron._drain_completions()
    assert cron._sla_report_tail == {}




# ---------------------------------------------------------------------------
# Web endpoints, the run loop, config-signature, and schedule helpers.
# ---------------------------------------------------------------------------


def test_webloop_origin_matches_host_no_hostname():
    # a Host header that parses to no hostname (a bare ":port") can never be a
    # same-origin match; fail closed.
    assert (
        cronstable.cron._origin_matches_host("http://example.com", ":8080")
        is False
    )
    # a real same-origin pair still matches, for contrast.
    assert (
        cronstable.cron._origin_matches_host(
            "http://a.example:80", "a.example"
        )
        is True
    )


def test_webloop_http_for_action_error_with_headers():
    from aiohttp import web

    ex = cronstable.cron.ApiActionError("nope", status=409)
    resp = cronstable.cron._http_for_action_error(ex, headers={"X-Test": "1"})
    assert isinstance(resp, web.HTTPConflict)
    assert resp.headers.get("X-Test") == "1"
    # and the headerless path maps the status to the matching exception type.
    assert isinstance(
        cronstable.cron._http_for_action_error(
            cronstable.cron.ApiActionError("x", status=404)
        ),
        web.HTTPNotFound,
    )


def test_webloop_fold_manifest_ignores_mistyped_fields():
    names, hosts, scopes, dags = set(), set(), set(), set()
    # every mistyped field contributes nothing (an older node's record simply
    # advertises less).
    cronstable.cron._fold_manifest(
        {"jobs": "a", "host": 123, "scopes": "s", "dags": None},
        names,
        hosts,
        scopes,
        dags,
    )
    assert (names, hosts, scopes, dags) == (set(), set(), set(), set())
    # a well-formed record still folds in.
    cronstable.cron._fold_manifest(
        {"jobs": ["j"], "host": "h", "scopes": ["sc"], "dags": ["d"]},
        names,
        hosts,
        scopes,
        dags,
    )
    assert names == {"j"}
    assert hosts == {"h"}
    assert scopes == {"sc"}
    assert dags == {"d"}


def test_webloop_load_index_html_disk_fallback(monkeypatch):
    import importlib

    # force the importlib.resources lookup to fail so the on-disk fallback path
    # is exercised; clear the lru_cache on the way in and out so neither this
    # test nor its neighbours see a stale cached value.
    cronstable.cron.load_index_html.cache_clear()

    def boom(*a, **k):
        raise ModuleNotFoundError("no package data")

    monkeypatch.setattr(importlib.resources, "files", boom)
    try:
        html = cronstable.cron.load_index_html()
    finally:
        cronstable.cron.load_index_html.cache_clear()
    assert "<" in html and len(html) > 0


def test_webloop_schedule_str_object_form():
    yaml = """
jobs:
  - name: obj
    command: echo hi
    schedule:
      minute: "*/5"
"""
    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    job = cron.cron_jobs["obj"]
    # an object-form schedule is rebuilt into a crontab line via the shared
    # builder.
    s = cronstable.cron.schedule_str(job)
    assert isinstance(s, str) and s


def test_webloop_web_site_from_url_unix_unsupported(monkeypatch):
    monkeypatch.setattr(
        cronstable.cron.platform, "supports_unix_sockets", lambda: False
    )
    # a unix-socket listener on a platform that cannot serve one is a skippable
    # bad-config entry (ValueError), not a crash.
    with pytest.raises(ValueError):
        cronstable.cron.web_site_from_url(None, "unix:///tmp/whatever.sock")


def test_webloop_config_signature_missing_file_and_dir(tmp_path):
    cron = cronstable.cron.Cron(None)
    # a vanished file collapses to the (path, None, None) sentinel so a deletion
    # still registers as a change.
    missing = str(tmp_path / "gone.yaml")
    sig = cron._config_signature(frozenset([missing]))
    assert sig == ((missing, None, None),)
    # a directory config source folds its own mtime in as well.
    cron.config_arg = str(tmp_path)
    sig2 = cron._config_signature(frozenset())
    assert any(part[0] == "\0dir" for part in sig2)


def test_webloop_config_signature_dir_stat_error(monkeypatch):
    # a directory config source whose own stat fails still records a sentinel
    # (the dir-vanished-mid-scan branch).
    cron = cronstable.cron.Cron(None)
    cron.config_arg = "some-dir"
    monkeypatch.setattr(cronstable.cron.os.path, "isdir", lambda p: True)

    def raising_stat(path, *a, **k):
        raise OSError("boom")

    monkeypatch.setattr(cronstable.cron.os, "stat", raising_stat)
    sig = cron._config_signature(frozenset())
    assert ("\0dir", None) in sig


def test_webloop_update_config_no_source_returns_empty():
    cron = cronstable.cron.Cron(None)
    cfg = cron.update_config()
    assert cfg.jobs == []
    assert cfg.web_config is None


_SUBMINUTE_NOFIRE = """
jobs:
  - name: sec
    command: echo sec
    schedule: "5/15 * * * * * *"
"""


@pytest.mark.asyncio
async def test_webloop_run_skips_subminute_housekeeping(monkeypatch):
    # a second-level job forces per-second ticking, so after the first pass the
    # once-a-minute housekeeping is SKIPPED on subsequent same-minute ticks (the
    # frozen clock keeps now_minute constant). The seconds (5/15) never include
    # the frozen :00, so the job itself never actually spawns.
    monkeypatch.setattr(
        "cronstable.cron.next_sleep_interval", lambda *a: 0.001
    )
    cron = cronstable.cron.Cron(None, config_yaml=_SUBMINUTE_NOFIRE)
    assert cron._needs_subminute() is True
    task = asyncio.create_task(cron.run())
    try:
        await _wait_until(lambda: cron._last_housekeeping_minute is not None)
        # let it iterate several more times within the same frozen minute
        await asyncio.sleep(0.05)
        assert not task.done()
    finally:
        cron.signal_shutdown()
        await asyncio.wait_for(task, timeout=5)


@pytest.mark.asyncio
async def test_webloop_run_shutdown_teardown(tmp_path, monkeypatch):
    from tests.test_state import _state_cfg

    # drive run()'s graceful-shutdown teardown across the observability overlay,
    # the slot renewers / catch-up / slot-pursuit task pools, the state-backend
    # block (with an empty pending-write set) and the web runner cleanup.
    monkeypatch.setattr("cronstable.cron.next_sleep_interval", lambda *a: 30)
    cron = cronstable.cron.Cron(None)
    task = asyncio.create_task(cron.run())

    renewer = catchup = pursuit = None
    stopped = {"mesh": False}

    try:
        # let the loop finish its first housekeeping pass and park on the long
        # sleep, then inject the teardown-path fixtures.
        await asyncio.sleep(0.2)

        await cron.start_stop_state(
            _state_cfg("state:\n  path: {}\n".format(tmp_path))
        )
        assert cron.state_backend is not None

        # drain any writes the backend startup queued, then neutralise
        # _track_state_write so the final counter snapshot does not repopulate
        # the pending set: the shutdown flush must see it EMPTY.
        pend = list(cron._pending_state_writes)
        if pend:
            await asyncio.gather(*pend, return_exceptions=True)
        monkeypatch.setattr(
            cron, "_track_state_write", lambda coro: coro.close()
        )

        class Mesh:
            async def stop(self):
                stopped["mesh"] = True

        cron.observability_mesh = Mesh()

        class Runner:
            def __init__(self):
                self.cleaned = False

            async def cleanup(self):
                self.cleaned = True

        cron.web_runner = Runner()

        renewer = asyncio.create_task(asyncio.sleep(100))
        cron._slot_renewers["s"] = renewer
        catchup = asyncio.create_task(asyncio.sleep(100))
        cron._catchup_tasks.add(catchup)
        pursuit = asyncio.create_task(asyncio.sleep(100))
        cron._slot_pursuits["p"] = pursuit
    finally:
        cron.signal_shutdown()
        await asyncio.wait_for(task, timeout=5)

    await asyncio.gather(
        renewer, catchup, pursuit, return_exceptions=True
    )
    assert stopped["mesh"] is True
    assert cron.observability_mesh is None
    assert cron.web_runner.cleaned is True
    assert renewer.cancelled()
    assert catchup.cancelled()
    assert pursuit.cancelled()
    assert cron._slot_renewers == {}


_LOGGING_CFG = """
jobs:
  - name: a
    command: echo hi
    schedule: "0 0 * * *"
logging:
    version: 1
"""


@pytest.mark.asyncio
async def test_webloop_run_applies_logging_config(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "cronstable.cron.next_sleep_interval", lambda *a: 0.01
    )
    applied = []
    monkeypatch.setattr(
        "cronstable.cron.logging.config.dictConfig",
        lambda cfg: applied.append(cfg),
    )
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_LOGGING_CFG)
    cron = cronstable.cron.Cron(str(cfg))
    task = asyncio.create_task(cron.run())
    try:
        await _wait_until(lambda: bool(applied))
    finally:
        cron.signal_shutdown()
        await asyncio.wait_for(task, timeout=5)
    assert applied[0] == {"version": 1}


@pytest.mark.asyncio
async def test_webloop_run_survives_logging_config_error(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "cronstable.cron.next_sleep_interval", lambda *a: 0.01
    )
    attempts = []

    def boom(cfg):
        attempts.append(cfg)
        raise ValueError("bad logging config")

    monkeypatch.setattr("cronstable.cron.logging.config.dictConfig", boom)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_LOGGING_CFG)
    cron = cronstable.cron.Cron(str(cfg))
    task = asyncio.create_task(cron.run())
    try:
        await _wait_until(lambda: len(attempts) >= 1)
        # a broken logging section is logged and the daemon keeps running.
        assert not task.done()
    finally:
        cron.signal_shutdown()
        await asyncio.wait_for(task, timeout=5)


@pytest.mark.asyncio
async def test_webloop_web_get_version():
    import cronstable.version

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    class Req:
        headers: dict = {}

    resp = await cron._web_get_version(Req())
    assert resp.text == cronstable.version.version


@pytest.mark.asyncio
async def test_webloop_web_status_text_running_and_disabled():
    from types import SimpleNamespace

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}
    cron.running_jobs["alpha"] = [
        SimpleNamespace(proc=SimpleNamespace(pid=4321))
    ]

    class Req:
        headers: dict = {}  # no Accept header -> plain-text renderer

    resp = await cron._web_get_status(Req())
    assert "alpha: running (pid: 4321)" in resp.text
    assert "beta: disabled" in resp.text


@pytest.mark.asyncio
async def test_webloop_schedule_why_reboot_with_pause(monkeypatch):
    from types import SimpleNamespace

    yaml = """
jobs:
  - name: boot
    command: echo hi
    schedule: "@reboot"
"""
    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    pause = SimpleNamespace(
        until=DT(2020, 1, 1, tzinfo=UTC), by="op", note=None
    )
    monkeypatch.setattr(cron, "_pause_active", lambda name: pause)
    payload = cron.schedule_why_payload("boot", "2020-01-01T00:00:00")
    assert payload is not None
    assert payload["reboot"] is True
    # the active pause is surfaced as a note on the @reboot payload.
    assert any(n["code"] == "paused" for n in payload["notes"])


@pytest.mark.asyncio
async def test_webloop_schedule_why_no_previous_fire():
    yaml = """
jobs:
  - name: yr
    command: echo hi
    schedule: "0 0 1 1 * * 2035"
"""
    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    payload = cron.schedule_why_payload("yr", "2020-06-15T12:00:00")
    assert payload is not None
    # a future-year schedule has no previous fire before the probe.
    assert payload["previous_fire"] is None
    assert payload["next_fire"] is not None


@pytest.mark.asyncio
async def test_webloop_schedule_why_previous_fire():
    yaml = """
jobs:
  - name: m
    command: echo hi
    schedule: "* * * * *"
"""
    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    payload = cron.schedule_why_payload("m", "2020-06-15T12:00:30")
    assert payload is not None
    assert payload["previous_fire"] is not None


def test_webloop_schedule_entries_includes_dag():
    yaml = """
dags:
  - name: sch
    schedule: "*/5 * * * *"
    tasks:
      - id: a
        command: x
  - name: nosched
    tasks:
      - id: a
        command: x
"""
    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    entries = cron._schedule_entries()
    # a DAG's schedule rides along as its synthetic dag:<name> entry; a DAG with
    # no schedule (nosched) contributes nothing.
    names = [e.name for e in entries]
    assert "dag:sch" in names
    assert "dag:nosched" not in names


@pytest.mark.asyncio
async def test_webloop_web_dag_run_and_xcom(monkeypatch):
    import json as _json

    from aiohttp import web

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    class Req:
        match_info = {"name": "d", "run_key": "rk"}

    async def none_run(name, run_key):
        return None

    monkeypatch.setattr(cron._dag, "get_run", none_run)
    with pytest.raises(web.HTTPNotFound):
        await cron._web_dag_run(Req())

    async def some_run(name, run_key):
        return {"state": "ok"}

    monkeypatch.setattr(cron._dag, "get_run", some_run)
    resp = await cron._web_dag_run(Req())
    assert _json.loads(resp.text)["state"] == "ok"

    async def none_xcom(name, run_key):
        return None

    monkeypatch.setattr(cron._dag, "xcom_for_run", none_xcom)
    with pytest.raises(web.HTTPNotFound):
        await cron._web_dag_xcom(Req())

    async def some_xcom(name, run_key):
        return {"a": 1}

    monkeypatch.setattr(cron._dag, "xcom_for_run", some_xcom)
    resp = await cron._web_dag_xcom(Req())
    assert _json.loads(resp.text)["a"] == 1


@pytest.mark.asyncio
async def test_webloop_web_dag_backfill_errors(monkeypatch):
    import json as _json

    from aiohttp import web

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    class Req:
        can_read_body = True

        def __init__(self, body):
            self.match_info = {"name": "d"}
            self._body = body

        async def json(self):
            return self._body

    # non-string from/to -> 400
    with pytest.raises(web.HTTPBadRequest):
        await cron._web_dag_backfill(Req({"from": 1, "to": 2}))

    async def bad_backfill(name, start, end):
        return {"ok": False, "reason": "nope"}

    monkeypatch.setattr(cron._dag, "backfill", bad_backfill)
    with pytest.raises(web.HTTPBadRequest):
        await cron._web_dag_backfill(
            Req({"from": "2020-01-01", "to": "2020-01-02"})
        )

    async def ok_backfill(name, start, end):
        return {"ok": True, "runs": 2}

    monkeypatch.setattr(cron._dag, "backfill", ok_backfill)
    resp = await cron._web_dag_backfill(
        Req({"from": "2020-01-01", "to": "2020-01-02"})
    )
    assert _json.loads(resp.text)["runs"] == 2


@pytest.mark.asyncio
async def test_webloop_state_payloads_propagate_cancel():
    cron = cronstable.cron.Cron(None)

    class CancelBackend:
        async def inventory(self):
            raise asyncio.CancelledError()

        def view_dict(self):
            return {}

        def stats(self):
            return {}

        async def list_documents(self, ns):
            raise asyncio.CancelledError()

        async def list_records(self, stream, limit, newest_first):
            raise asyncio.CancelledError()

    cron.state_backend = CancelBackend()
    # a cancellation must propagate, never be swallowed by the degrade-to-empty
    # guard.
    with pytest.raises(asyncio.CancelledError):
        await cron.state_payload()
    with pytest.raises(asyncio.CancelledError):
        await cron.state_documents_payload("kv/x")
    with pytest.raises(asyncio.CancelledError):
        await cron.state_records_payload("s")


def test_webloop_tail_payload_with_cursor():
    out = JobOutputStream()
    for i in range(5):
        out.publish("stdout", "line{}\n".format(i))
    payload = cronstable.cron.Cron._tail_payload(out, 10, 2)
    # with a cursor, the lines AFTER that offset are returned (not the tail).
    assert payload["cursor"] == 5
    assert payload["truncated"] is False
    assert len(payload["lines"]) == 3


@pytest.mark.asyncio
async def test_webloop_pump_output_handles_disconnect():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}
    out = JobOutputStream()
    out.publish("stdout", "x\n")
    out.close()

    class FakeResp:
        async def write(self, data):
            raise ConnectionResetError()

    # a client that vanishes mid-write is swallowed; nothing escapes.
    await cron._pump_output(FakeResp(), out)


@pytest.mark.asyncio
async def test_webloop_web_job_logs_live_running():
    from types import SimpleNamespace

    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}
    out = JobOutputStream()
    # a currently-running instance exposes its live output buffer.
    cron.running_jobs["alpha"] = [SimpleNamespace(output=out)]

    app = web.Application()
    app.router.add_get("/jobs/{name}/logs", cron._web_job_logs)
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/jobs/alpha/logs")
        assert resp.status == 200
        # let the handler subscribe and park, then publish a live line so it is
        # delivered over the queue (not just the replay buffer), and end it.
        await asyncio.sleep(0.05)
        out.publish("stdout", "live line\n")
        out.close()
        body = await resp.text()
    assert "live line" in body
    assert "event: end" in body


_DAG_LOGS_YAML = """
dags:
  - name: lin
    tasks:
      - id: a
        command: x
"""


@pytest.mark.asyncio
async def test_webloop_web_dag_task_logs_unknown_dag():
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    cron = cronstable.cron.Cron(None, config_yaml=_DAG_LOGS_YAML)
    cron.web_config = {}
    app = web.Application()
    app.router.add_get(
        "/dags/{name}/runs/{run_key}/tasks/{taskkey}/logs",
        cron._web_dag_task_logs,
    )
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/dags/nope/runs/rk/tasks/a/logs")
        assert resp.status == 404


@pytest.mark.asyncio
async def test_webloop_web_dag_task_logs_no_output():
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    cron = cronstable.cron.Cron(None, config_yaml=_DAG_LOGS_YAML)
    cron.web_config = {}
    app = web.Application()
    app.router.add_get(
        "/dags/{name}/runs/{run_key}/tasks/{taskkey}/logs",
        cron._web_dag_task_logs,
    )
    async with TestClient(TestServer(app)) as client:
        # no running instance -> no reachable buffer.
        resp = await client.get("/dags/lin/runs/rk/tasks/a/logs")
        assert resp.status == 200
        body = await resp.text()
    assert "no-output" in body


@pytest.mark.asyncio
async def test_webloop_web_dag_task_logs_live_running():
    from types import SimpleNamespace

    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    cron = cronstable.cron.Cron(None, config_yaml=_DAG_LOGS_YAML)
    cron.web_config = {}
    out = JobOutputStream()
    dref = SimpleNamespace(run_key="rk", taskkey="a")
    # a running instance under the template name "<dag>.<task_id>" whose dag_ref
    # matches this run + instance key exposes its live buffer; a non-matching
    # sibling instance is skipped first (the loop-continue branch).
    cron.running_jobs["lin.a"] = [
        SimpleNamespace(
            output=JobOutputStream(),
            dag_ref=SimpleNamespace(run_key="other", taskkey="a"),
        ),
        SimpleNamespace(output=out, dag_ref=dref),
    ]

    app = web.Application()
    app.router.add_get(
        "/dags/{name}/runs/{run_key}/tasks/{taskkey}/logs",
        cron._web_dag_task_logs,
    )
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/dags/lin/runs/rk/tasks/a/logs")
        assert resp.status == 200
        await asyncio.sleep(0.05)
        out.publish("stdout", "task line\n")
        out.close()
        body = await resp.text()
    assert "task line" in body
    assert "event: end" in body




# ==========================================================================
# The cron.py start/stop lifecycle and
# durable-state garbage-collection paths.  Targets start_stop_web_app,
# start_stop_cluster, start_stop_observability, start_stop_state, the job-API
# seams (_start_job_api / _stop_job_api), _persist_manifest, _live_pause_keep,
# and the three GC helpers (_collect_state_garbage / _gc_dag_state /
# _sweep_orphan_artifact_blobs).  Most of these are degrade-and-survive
# branches reached by driving a lifecycle transition or by monkeypatching a
# backend method to raise, then asserting the observable side effect.
# ==========================================================================

_LIFECYCLE_JOB = (
    "jobs:\n  - name: j\n    command: 'true'\n    schedule: '* * * * *'\n"
)


class _LifecycleFakeSite:
    """A web site whose start() never binds a real socket (isolation)."""

    def __init__(self, url):
        self.url = url

    async def start(self):
        return None


def _lifecycle_state_config(tmp_path):
    from tests.test_state import _state_cfg

    return _state_cfg("state:\n  path: " + str(tmp_path))


async def _lifecycle_start_state(tmp_path):
    cron = cronstable.cron.Cron(None, config_yaml=_LIFECYCLE_JOB)
    await cron.start_stop_state(_lifecycle_state_config(tmp_path))
    assert cron.state_backend is not None
    return cron


async def _lifecycle_stop_state(cron):
    if cron.state_backend is not None:
        await cron.state_backend.stop()
        cron.state_backend = None


async def _lifecycle_seed_anchor_frozen(cron):
    # Manifests whose timestamps are relative to the FROZEN clock the autouse
    # fixture installs (get_now -> 1999-12-31), so a GC pass can prove absence
    # even though tests/test_cron freezes time.  One manifest older than the
    # 3600s grace (the history-depth guard) plus one recent, both advertising
    # scopes/dags so the pass manages artifact streams instead of deferring.
    now = cronstable.cron.get_now(datetime.timezone.utc)
    backend = cron.state_backend
    await backend.append_record(
        "manifests/old-host",
        {
            "jobSetId": "v1:old",
            "host": "old-host",
            "jobs": [],
            "scopes": [],
            "dags": [],
            "at": (now - datetime.timedelta(seconds=7200)).isoformat(),
        },
    )
    await backend.append_record(
        "manifests/other-host",
        {
            "jobSetId": "v1:other",
            "host": "other-host",
            "jobs": [],
            "scopes": [],
            "dags": [],
            "at": now.isoformat(),
        },
    )


# --- start_stop_web_app ----------------------------------------------------


async def test_lifecycle_web_app_wildcard_acao_and_socket_mode(monkeypatch, caplog):
    # A wildcard Access-Control-Allow-Origin header disables the cross-site
    # Origin gate (loudly), and socketMode drives the post-listen apply hook.
    # web_site_from_url is faked so no real socket is bound.
    import logging

    monkeypatch.setattr(
        cronstable.cron,
        "web_site_from_url",
        lambda runner, url, ssl_context=None: _LifecycleFakeSite(url),
    )
    cron = cronstable.cron.Cron(None, config_yaml=_LIFECYCLE_JOB)
    try:
        with caplog.at_level(logging.WARNING, logger="cronstable"):
            await cron.start_stop_web_app(
                {
                    "listen": ["http://127.0.0.1:1"],
                    "headers": {"Access-Control-Allow-Origin": "*"},
                    "socketMode": "0660",
                }
            )
        assert cron.web_runner is not None
        assert any(
            "Access-Control-Allow-Origin" in r.message
            for r in caplog.records
        )
    finally:
        await cron.start_stop_web_app(None)
    assert cron.web_runner is None


async def test_lifecycle_web_app_specific_acao_folded_into_allowlist(monkeypatch):
    # A specific (non-wildcard) ACAO response header is folded into the
    # cross-site allow-list so a deliberate cross-origin dashboard survives.
    monkeypatch.setattr(
        cronstable.cron,
        "web_site_from_url",
        lambda runner, url, ssl_context=None: _LifecycleFakeSite(url),
    )
    cron = cronstable.cron.Cron(None, config_yaml=_LIFECYCLE_JOB)
    try:
        await cron.start_stop_web_app(
            {
                "listen": ["http://127.0.0.1:1"],
                "headers": {
                    "Access-Control-Allow-Origin": "https://dash.example"
                },
            }
        )
        assert cron.web_runner is not None
    finally:
        await cron.start_stop_web_app(None)


async def test_lifecycle_web_app_mounts_mcp_endpoint(monkeypatch):
    # An enabled MCP config wires the POST/GET/OPTIONS /mcp routes and builds
    # the handler against the current config.
    from cronstable.config import _build_mcp_config

    monkeypatch.setattr(
        cronstable.cron,
        "web_site_from_url",
        lambda runner, url, ssl_context=None: _LifecycleFakeSite(url),
    )
    cron = cronstable.cron.Cron(None, config_yaml=_LIFECYCLE_JOB)
    mcp_config = _build_mcp_config({"enabled": True})
    try:
        await cron.start_stop_web_app(
            {"listen": ["http://127.0.0.1:1"]}, mcp_config
        )
        assert cron.web_runner is not None
        assert cron._mcp is not None
    finally:
        await cron.start_stop_web_app(None)


# --- start_stop_cluster ----------------------------------------------------


async def test_lifecycle_cluster_build_installs_providers_and_warns(
    monkeypatch, caplog
):
    # A fresh cluster build installs both fleet providers and starts the
    # manager, and an even cluster size emits a (once-per-(re)start) advisory.
    import logging

    from cronstable.config import parse_config_string

    yaml = (
        "jobs:\n  - name: a\n    command: echo a\n"
        '    schedule: "* * * * *"\n'
        "cluster:\n"
        '  listen: "127.0.0.1:18443"\n'
        "  tls:\n"
        "    ca: /nonexistent/ca.pem\n"
        "    cert: /nonexistent/cert.pem\n"
        "    key: /nonexistent/key.pem\n"
        "  peers:\n"
        "    - host: b:8443\n"
        "    - host: c:8443\n"
        "    - host: d:8443\n"
        "  electLeader: true\n"
    )
    cfg = parse_config_string(yaml, "").cluster_config
    made = []
    monkeypatch.setattr(
        cronstable.cron,
        "make_backend",
        lambda c, jsid: made.append(_FakeMesh(c)) or made[-1],
    )
    cron = cronstable.cron.Cron(None, config_yaml=_LIFECYCLE_JOB)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron.start_stop_cluster(cfg)
    assert cron.cluster_manager is made[0]
    assert made[0].started is True
    assert made[0].job_summaries_provider == cron.fleet_job_summaries
    assert made[0].node_stats_provider == cron.node_resource_snapshot
    assert cron._elect_leader_configured is True
    assert any("even cluster size" in r.message for r in caplog.records)


async def test_lifecycle_cluster_reload_logs_leader_and_quorum_loss(caplog):
    # Removing the cluster section stops the running manager; if this node
    # held leadership/quorum, the ex-leader logs the transition here (before
    # the flags reset) rather than going silent about why it stopped.
    import logging

    class _Mgr:
        def __init__(self):
            self.config = {"backend": "gossip"}
            self.stopped = False

        async def stop(self):
            self.stopped = True

    cron = cronstable.cron.Cron(None, config_yaml=_LIFECYCLE_JOB)
    mgr = _Mgr()
    cron.cluster_manager = mgr
    cron._was_leader = True
    cron._was_quorate = True
    with caplog.at_level(logging.INFO, logger="cronstable"):
        await cron.start_stop_cluster(None)
    assert mgr.stopped is True
    assert cron.cluster_manager is None
    assert cron._was_leader is False and cron._was_quorate is False
    assert any(
        "lost scheduled-job leadership" in r.message for r in caplog.records
    )
    assert any("left quorum" in r.message for r in caplog.records)


# --- start_stop_observability ----------------------------------------------


async def test_lifecycle_observability_keeps_mesh_when_new_tls_unloadable(
    monkeypatch, caplog
):
    # A TLS-file rotation signals a rebuild, but make-before-break is
    # infeasible for gossip: while the new material is not yet loadable the
    # running overlay is kept (serving the valid old cert) and the share flag
    # is still re-reconciled on it.
    import logging

    monkeypatch.setattr(
        "cronstable.cluster.gossip_tls_loadable", lambda cfg: False
    )
    mesh_cfg = {"backend": "gossip", "marker": 7}

    class _Mesh:
        def __init__(self, config):
            self.config = config
            self.stopped = False
            self.share = None

        def tls_files_changed(self):
            return True

        def set_node_stats_provider(self, provider, share=True):
            self.share = share

        async def stop(self):
            self.stopped = True

    cron = cronstable.cron.Cron(None, config_yaml=_LIFECYCLE_JOB)
    mesh = _Mesh(mesh_cfg)
    cron.observability_mesh = mesh
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron.start_stop_observability(
            {"observabilityMesh": mesh_cfg, "shareNodeStats": True}
        )
    assert cron.observability_mesh is mesh
    assert mesh.stopped is False
    assert mesh.share is True
    assert any("not yet loadable" in r.message for r in caplog.records)


async def test_lifecycle_observability_start_failure_swallowed(
    monkeypatch, caplog
):
    # A misconfigured overlay whose start() raises must be logged and
    # swallowed: durability/observability being broken never stops jobs.
    import logging

    class _FailMesh:
        def __init__(self, config):
            self.config = config

        def set_job_summaries_provider(self, p):
            pass

        def set_node_stats_provider(self, p, share=True):
            pass

        async def start(self):
            raise OSError("overlay bind failed")

    monkeypatch.setattr(
        cronstable.cron, "make_backend", lambda c, jsid: _FailMesh(c)
    )
    cron = cronstable.cron.Cron(None, config_yaml=_LIFECYCLE_JOB)
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await cron.start_stop_observability(
            {
                "observabilityMesh": {"backend": "gossip"},
                "shareNodeStats": False,
            }
        )
    assert cron.observability_mesh is None
    assert any("failed to start" in r.message for r in caplog.records)


# --- start_stop_state teardown ---------------------------------------------


async def test_lifecycle_state_teardown_cancels_slot_and_retry_tasks(tmp_path):
    # Removing the state section tears the backend down and cancels every
    # per-store background task (slot renewers, Replace pursuits, the
    # cross-node retry-claim scan): they belong to the old store generation.
    cron = await _lifecycle_start_state(tmp_path)

    async def _idle():
        await asyncio.sleep(3600)

    renewer = asyncio.ensure_future(_idle())
    pursuit = asyncio.ensure_future(_idle())
    claim = asyncio.ensure_future(_idle())
    cron._slot_renewers["j"] = renewer
    cron._slot_pursuits["j"] = pursuit
    cron._retry_claim_task = claim
    await asyncio.sleep(0)  # let the tasks reach their await points

    await cron.start_stop_state(None)

    assert cron.state_backend is None
    assert cron._slot_renewers == {}
    assert cron._slot_pursuits == {}
    assert cron._retry_claim_task is None
    await asyncio.sleep(0)
    assert renewer.cancelled()
    assert pursuit.cancelled()
    assert claim.cancelled()


# --- _start_job_api / _stop_job_api ----------------------------------------


async def test_lifecycle_start_job_api_swallows_start_failure(
    monkeypatch, tmp_path, caplog
):
    # The loopback job-state API is best-effort: a start failure is logged and
    # swallowed (jobs run without the endpoint), leaving _job_api unset.
    import logging

    import cronstable.jobapi

    class _FailApi:
        def __init__(self, *a, **k):
            pass

        async def start(self):
            raise OSError("loopback bind failed")

        async def stop(self):
            return None

    monkeypatch.setattr(cronstable.jobapi, "JobStateAPI", _FailApi)
    cron = cronstable.cron.Cron(None, config_yaml=_LIFECYCLE_JOB)
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await cron._start_job_api(_lifecycle_state_config(tmp_path))
    assert cron._job_api is None
    assert any("job API failed to start" in r.message for r in caplog.records)


async def test_lifecycle_stop_job_api_noop_when_absent():
    # No API running: stop is a clean no-op.
    cron = cronstable.cron.Cron(None, config_yaml=_LIFECYCLE_JOB)
    assert cron._job_api is None
    await cron._stop_job_api()
    assert cron._job_api is None


async def test_lifecycle_stop_job_api_warns_on_unclean_stop(caplog):
    # A stop that raises is logged as an unclean shutdown, and the handle is
    # cleared regardless so the generation cannot leak.
    import logging

    class _Api:
        async def stop(self):
            raise OSError("did not close")

    cron = cronstable.cron.Cron(None, config_yaml=_LIFECYCLE_JOB)
    cron._job_api = _Api()
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._stop_job_api()
    assert cron._job_api is None
    assert any("did not stop cleanly" in r.message for r in caplog.records)


# --- _persist_manifest -----------------------------------------------------


async def test_lifecycle_persist_manifest_noop_without_backend():
    # No backend: the manifest write is a no-op.
    cron = cronstable.cron.Cron(None, config_yaml=_LIFECYCLE_JOB)
    assert cron.state_backend is None
    await cron._persist_manifest()


async def test_lifecycle_persist_manifest_swallows_append_error(
    tmp_path, caplog
):
    # A failed manifest append is counted as a dropped write and logged, not
    # raised (it runs as a fire-and-forget background task).
    import logging

    cron = await _lifecycle_start_state(tmp_path)
    try:

        async def _boom(*a, **k):
            raise OSError("append failed")

        cron.state_backend.append_record = _boom
        with caplog.at_level(logging.WARNING, logger="cronstable"):
            await cron._persist_manifest()
        assert any(
            "failed to record the job manifest" in r.message
            for r in caplog.records
        )
    finally:
        await _lifecycle_stop_state(cron)


# --- _live_pause_keep ------------------------------------------------------


async def test_lifecycle_live_pause_keep_keeps_all_on_enumerate_error(
    tmp_path, caplog
):
    # If the pause-stream listing cannot be enumerated, every kept job is kept
    # unconditionally: GC never eats a live pause on doubt.
    import logging

    cron = await _lifecycle_start_state(tmp_path)
    try:

        async def _boom(*a, **k):
            raise OSError("cannot list")

        cron.state_backend.list_stream_names = _boom
        now = cronstable.cron.get_now(datetime.timezone.utc)
        with caplog.at_level(logging.WARNING, logger="cronstable"):
            keep = await cron._live_pause_keep(
                cron.state_backend, {"j"}, now
            )
        assert keep == {"j"}
        assert any(
            "not reclaiming dead pause streams" in r.message
            for r in caplog.records
        )
    finally:
        await _lifecycle_stop_state(cron)


async def test_lifecycle_live_pause_keep_skips_removed_and_unreadable_streams(
    tmp_path, caplog
):
    # A pause stream for a job not in the keep set is skipped (its name was
    # already collected), and a kept job's unreadable pause stream is kept on
    # doubt rather than dropped.
    import logging

    cron = await _lifecycle_start_state(tmp_path)
    try:
        backend = cron.state_backend
        await backend.append_record(
            cronstable.cron.PAUSE_STREAM_PREFIX + "removed",
            {"until": "2099-01-01T00:00:00+00:00"},
        )
        await backend.append_record(
            cronstable.cron.PAUSE_STREAM_PREFIX + "keeper",
            {"until": "2099-01-01T00:00:00+00:00"},
        )

        async def _boom(*a, **k):
            raise OSError("cannot read")

        backend.list_records = _boom
        now = cronstable.cron.get_now(datetime.timezone.utc)
        with caplog.at_level(logging.WARNING, logger="cronstable"):
            keep = await cron._live_pause_keep(backend, {"keeper"}, now)
        assert "keeper" in keep
        assert any(
            "keeping the pause stream of keeper" in r.message
            for r in caplog.records
        )
    finally:
        await _lifecycle_stop_state(cron)


# --- _collect_state_garbage ------------------------------------------------


async def test_lifecycle_collect_garbage_noop_without_grace():
    # gcGraceSeconds unset (0) or no backend: the whole pass is a no-op.
    cron = cronstable.cron.Cron(None, config_yaml=_LIFECYCLE_JOB)
    cron._state_gc_grace = 0.0
    await cron._collect_state_garbage()


async def test_lifecycle_collect_garbage_degrades_on_enumerate_error(
    tmp_path, caplog
):
    # Cannot enumerate the manifest streams: collect nothing this pass.
    import logging

    cron = await _lifecycle_start_state(tmp_path)
    try:
        cron._state_gc_grace = 3600.0

        async def _boom(*a, **k):
            raise OSError("cannot enumerate")

        cron.state_backend.list_stream_names = _boom
        with caplog.at_level(logging.WARNING, logger="cronstable"):
            await cron._collect_state_garbage()
        assert any(
            "cannot enumerate the manifest streams" in r.message
            for r in caplog.records
        )
    finally:
        await _lifecycle_stop_state(cron)


async def test_lifecycle_collect_garbage_caps_manifest_hosts(
    tmp_path, monkeypatch, caplog
):
    # More manifest host streams than the cap: warn and read only the first
    # cap-many this pass (a churning fleet with never-reused host identities).
    import logging

    from tests.test_cron_state_hardening import _seed_gc_anchor

    cron = await _lifecycle_start_state(tmp_path)
    try:
        await _seed_gc_anchor(cron)
        monkeypatch.setattr(cronstable.cron, "MANIFEST_HOSTS_CAP", 1)
        cron._state_gc_grace = 3600.0
        with caplog.at_level(logging.WARNING, logger="cronstable"):
            await cron._collect_state_garbage()
        assert any(
            "reading only the first" in r.message for r in caplog.records
        )
    finally:
        await _lifecycle_stop_state(cron)


async def test_lifecycle_collect_garbage_degrades_on_manifest_read_error(
    tmp_path, caplog
):
    # The streams enumerate but a record read fails: collect nothing.
    import logging

    from tests.test_cron_state_hardening import _seed_gc_anchor

    cron = await _lifecycle_start_state(tmp_path)
    try:
        await _seed_gc_anchor(cron)
        cron._state_gc_grace = 3600.0

        async def _boom(*a, **k):
            raise OSError("cannot read")

        cron.state_backend.list_records = _boom
        with caplog.at_level(logging.WARNING, logger="cronstable"):
            await cron._collect_state_garbage()
        assert any(
            "cannot read the manifest streams" in r.message
            for r in caplog.records
        )
    finally:
        await _lifecycle_stop_state(cron)


async def test_lifecycle_collect_garbage_degrades_on_collect_failure(
    tmp_path, caplog
):
    # The pass reaches the backend collect step (history spans grace, scopes
    # advertised) and that step raises: degrade to "collected nothing".
    import logging

    cron = await _lifecycle_start_state(tmp_path)
    try:
        await _lifecycle_seed_anchor_frozen(cron)
        cron._state_gc_grace = 3600.0

        async def _boom(*a, **k):
            raise OSError("collect failed")

        cron.state_backend.collect_garbage = _boom
        with caplog.at_level(logging.WARNING, logger="cronstable"):
            await cron._collect_state_garbage()
        assert any(
            "garbage collection failed" in r.message for r in caplog.records
        )
    finally:
        await _lifecycle_stop_state(cron)


# --- _gc_dag_state ---------------------------------------------------------


async def test_lifecycle_gc_dag_state_degrades_on_namespace_error(
    tmp_path, caplog
):
    # Cannot enumerate the dag-run namespaces: leave artifact streams wholly
    # unmanaged this pass (the keep map is untouched).
    import logging

    cron = await _lifecycle_start_state(tmp_path)
    try:
        backend = cron.state_backend

        async def _boom(*a, **k):
            raise OSError("cannot enumerate namespaces")

        backend.list_document_namespaces = _boom
        keep = {}
        with caplog.at_level(logging.WARNING, logger="cronstable"):
            await cron._gc_dag_state(backend, keep, set(), set(), 3600.0)
        assert "artifacts/" not in keep
        assert any(
            "cannot enumerate the dag-run namespaces" in r.message
            for r in caplog.records
        )
    finally:
        await _lifecycle_stop_state(cron)


async def test_lifecycle_gc_dag_state_defers_when_namespace_incomplete(
    tmp_path, caplog
):
    # A dag-run namespace exists whose name cannot be recovered: its runs'
    # XCom scopes cannot be protected, so artifacts stay unmanaged this pass.
    import logging

    cron = await _lifecycle_start_state(tmp_path)
    try:
        backend = cron.state_backend

        async def _incomplete(*a, **k):
            return ([], False)

        backend.list_document_namespaces = _incomplete
        keep = {}
        with caplog.at_level(logging.WARNING, logger="cronstable"):
            await cron._gc_dag_state(backend, keep, set(), set(), 3600.0)
        assert "artifacts/" not in keep
        assert any("cannot be recovered" in r.message for r in caplog.records)
    finally:
        await _lifecycle_stop_state(cron)


async def test_lifecycle_gc_dag_state_degrades_on_document_read_error(
    tmp_path, caplog
):
    # The namespaces enumerate but a run document read fails: unmanaged.
    import logging

    from cronstable.dag import DAG_RUN_NS_PREFIX

    cron = await _lifecycle_start_state(tmp_path)
    try:
        backend = cron.state_backend

        async def _ns(*a, **k):
            return ([DAG_RUN_NS_PREFIX + "d"], True)

        async def _boom(*a, **k):
            raise OSError("cannot read documents")

        backend.list_document_namespaces = _ns
        backend.list_documents = _boom
        keep = {}
        with caplog.at_level(logging.WARNING, logger="cronstable"):
            # live_dags carries "d" so gc_removed_dags is not invoked and the
            # test targets only the document-read degrade branch.
            await cron._gc_dag_state(backend, keep, set(), {"d"}, 3600.0)
        assert "artifacts/" not in keep
        assert any(
            "cannot read the dag-run documents" in r.message
            for r in caplog.records
        )
    finally:
        await _lifecycle_stop_state(cron)


# --- _sweep_orphan_artifact_blobs ------------------------------------------


async def test_lifecycle_sweep_blobs_degrades_on_audit_error(tmp_path, caplog):
    # Cannot enumerate the artifact streams: skip the sweep this pass.
    import logging

    cron = await _lifecycle_start_state(tmp_path)
    try:
        backend = cron.state_backend

        async def _boom(*a, **k):
            raise OSError("cannot audit")

        backend.list_stream_names_audit = _boom
        with caplog.at_level(logging.WARNING, logger="cronstable"):
            await cron._sweep_orphan_artifact_blobs(backend, 3600.0)
        assert any(
            "cannot enumerate" in r.message and "artifact streams" in r.message
            for r in caplog.records
        )
    finally:
        await _lifecycle_stop_state(cron)


async def test_lifecycle_sweep_blobs_degrades_on_sweep_error(tmp_path, caplog):
    # The reference set builds but the blob sweep itself raises: skip, biased
    # to keep, so a live payload is never deleted on doubt.
    import logging

    cron = await _lifecycle_start_state(tmp_path)
    try:
        backend = cron.state_backend

        async def _boom(*a, **k):
            raise OSError("cannot sweep")

        backend.sweep_orphan_blobs = _boom
        with caplog.at_level(logging.WARNING, logger="cronstable"):
            await cron._sweep_orphan_artifact_blobs(backend, 3600.0)
        assert any(
            "skipping the orphan-blob sweep" in r.message
            and "cannot be ruled" in r.message
            for r in caplog.records
        )
    finally:
        await _lifecycle_stop_state(cron)


# --- cancellation must propagate (never swallowed as a degrade) ------------
#
# Every GC/state degrade block re-raises asyncio.CancelledError ahead of its
# broad "log and survive" except, so a shutdown cancel is honoured rather than
# mistaken for a store error.  These drive that re-raise on each block.


async def _lifecycle_cancel(*a, **k):
    raise asyncio.CancelledError()


async def test_lifecycle_live_pause_keep_propagates_enumerate_cancel(tmp_path):
    cron = await _lifecycle_start_state(tmp_path)
    try:
        cron.state_backend.list_stream_names = _lifecycle_cancel
        now = cronstable.cron.get_now(datetime.timezone.utc)
        with pytest.raises(asyncio.CancelledError):
            await cron._live_pause_keep(cron.state_backend, {"j"}, now)
    finally:
        await _lifecycle_stop_state(cron)


async def test_lifecycle_live_pause_keep_propagates_read_cancel(tmp_path):
    cron = await _lifecycle_start_state(tmp_path)
    try:
        backend = cron.state_backend
        await backend.append_record(
            cronstable.cron.PAUSE_STREAM_PREFIX + "keeper",
            {"until": "2099-01-01T00:00:00+00:00"},
        )
        backend.list_records = _lifecycle_cancel
        now = cronstable.cron.get_now(datetime.timezone.utc)
        with pytest.raises(asyncio.CancelledError):
            await cron._live_pause_keep(backend, {"keeper"}, now)
    finally:
        await _lifecycle_stop_state(cron)


async def test_lifecycle_collect_garbage_propagates_enumerate_cancel(tmp_path):
    cron = await _lifecycle_start_state(tmp_path)
    try:
        cron._state_gc_grace = 3600.0
        cron.state_backend.list_stream_names = _lifecycle_cancel
        with pytest.raises(asyncio.CancelledError):
            await cron._collect_state_garbage()
    finally:
        await _lifecycle_stop_state(cron)


async def test_lifecycle_collect_garbage_propagates_read_cancel(tmp_path):
    from tests.test_cron_state_hardening import _seed_gc_anchor

    cron = await _lifecycle_start_state(tmp_path)
    try:
        await _seed_gc_anchor(cron)
        cron._state_gc_grace = 3600.0
        cron.state_backend.list_records = _lifecycle_cancel
        with pytest.raises(asyncio.CancelledError):
            await cron._collect_state_garbage()
    finally:
        await _lifecycle_stop_state(cron)


async def test_lifecycle_gc_dag_state_propagates_namespace_cancel(tmp_path):
    cron = await _lifecycle_start_state(tmp_path)
    try:
        cron.state_backend.list_document_namespaces = _lifecycle_cancel
        with pytest.raises(asyncio.CancelledError):
            await cron._gc_dag_state(
                cron.state_backend, {}, set(), set(), 3600.0
            )
    finally:
        await _lifecycle_stop_state(cron)


async def test_lifecycle_gc_dag_state_propagates_document_cancel(tmp_path):
    from cronstable.dag import DAG_RUN_NS_PREFIX

    cron = await _lifecycle_start_state(tmp_path)
    try:
        backend = cron.state_backend

        async def _ns(*a, **k):
            return ([DAG_RUN_NS_PREFIX + "d"], True)

        backend.list_document_namespaces = _ns
        backend.list_documents = _lifecycle_cancel
        with pytest.raises(asyncio.CancelledError):
            await cron._gc_dag_state(backend, {}, set(), {"d"}, 3600.0)
    finally:
        await _lifecycle_stop_state(cron)


async def test_lifecycle_sweep_blobs_propagates_audit_cancel(tmp_path):
    cron = await _lifecycle_start_state(tmp_path)
    try:
        cron.state_backend.list_stream_names_audit = _lifecycle_cancel
        with pytest.raises(asyncio.CancelledError):
            await cron._sweep_orphan_artifact_blobs(
                cron.state_backend, 3600.0
            )
    finally:
        await _lifecycle_stop_state(cron)


async def test_lifecycle_sweep_blobs_propagates_sweep_cancel(tmp_path):
    cron = await _lifecycle_start_state(tmp_path)
    try:
        cron.state_backend.sweep_orphan_blobs = _lifecycle_cancel
        with pytest.raises(asyncio.CancelledError):
            await cron._sweep_orphan_artifact_blobs(
                cron.state_backend, 3600.0
            )
    finally:
        await _lifecycle_stop_state(cron)


async def test_lifecycle_collect_garbage_propagates_collect_cancel(tmp_path):
    # Reach the backend collect step (history spans grace, scopes advertised)
    # and cancel there: the re-raise must win over the broad degrade except.
    cron = await _lifecycle_start_state(tmp_path)
    try:
        await _lifecycle_seed_anchor_frozen(cron)
        cron._state_gc_grace = 3600.0
        cron.state_backend.collect_garbage = _lifecycle_cancel
        with pytest.raises(asyncio.CancelledError):
            await cron._collect_state_garbage()
    finally:
        await _lifecycle_stop_state(cron)



# =====================================================================
#  Scheduling, catch-up, and reboot paths in cronstable/cron.py
# =====================================================================


def test_catchup_smoke_sanity():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    assert cron is not None


from tests.test_state import (  # noqa: E402
    _count_launcher,
    _cron_with_watermark,
    _NOW,
    _state_cfg,
)

_CATCHUP_REBOOT_YAML = """
jobs:
  - name: boot
    command: echo hi
    schedule: "@reboot"
"""


def _catchup_pause(hours_from=1):
    # a pause window live against the frozen 1999-12-31 12:00 clock.
    return cronstable.cron.PauseInfo(
        since=DT(1999, 12, 31, 11, 0, 0, tzinfo=UTC),
        until=DT(1999, 12, 31, 12 + hours_from, 0, 0, tzinfo=UTC),
        note="",
        by="op",
        channel="api",
    )


async def _catchup_reboot_cron(tmp_path):
    cron = cronstable.cron.Cron(None, config_yaml=_CATCHUP_REBOOT_YAML)
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    return cron


# --- _peek_soonest_fire / _sleep_interval / _due_names ---------------


def test_catchup_peek_soonest_fire_discards_stale_top():
    # a heap entry the next-fire index has since superseded is popped from the
    # top; the next live entry is returned.
    cron = cronstable.cron.Cron(None)
    now = cronstable.cron.get_now(UTC)
    w1 = now + datetime.timedelta(seconds=10)
    w2 = now + datetime.timedelta(seconds=20)
    cron._set_next_fire("a", w1)  # heap holds (w1, a)
    cron._next_fire["a"] = w2  # supersede without touching the heap
    cron._set_next_fire("a", w2)  # push the live (w2, a)
    assert cron._peek_soonest_fire() == w2  # (w1, a) discarded as stale


def test_catchup_sleep_interval_capped_by_dag_wake(monkeypatch):
    # a due DAG wake pulls the sleep below the once-a-minute housekeeping cap.
    cron = cronstable.cron.Cron(None)
    monkeypatch.setattr(cron._dag, "next_wake_delay", lambda: 0.3)
    assert cron._sleep_interval() == pytest.approx(0.3)


def test_catchup_due_names_dedupes_duplicate_live_entries():
    # a name that somehow holds two live heap entries for the same instant is
    # returned exactly once.
    cron = cronstable.cron.Cron(None)
    when = cronstable.cron.get_now(UTC)
    cron._set_next_fire("a", when)
    cron._set_next_fire("a", when)  # a second live entry for the same slot
    assert cron._due_names(when) == ["a"]


# --- _pause_excusal_window -------------------------------------------


async def test_catchup_pause_excusal_window_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_PAUSABLE_JOB)
    assert await cron._pause_excusal_window("p") is None


async def test_catchup_pause_excusal_window_store_error_degrades(tmp_path, caplog):
    import logging

    cron = await _cron_with_watermark(tmp_path, None, onmissed="run-all")

    async def boom(*a, **k):
        raise RuntimeError("store down")

    cron.state_backend.list_records = boom  # type: ignore[method-assign]
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        assert await cron._pause_excusal_window("j") is None
    assert any("pause stream" in r.getMessage() for r in caplog.records)


# --- _checkpoint_catchup ---------------------------------------------


async def test_catchup_checkpoint_catchup_no_backend_is_noop():
    cron = cronstable.cron.Cron(None, config_yaml=_PAUSABLE_JOB)
    # no state backend -> returns without touching anything (no raise).
    await cron._checkpoint_catchup("p", "open", "wm")


async def test_catchup_checkpoint_catchup_write_error_is_best_effort(
    tmp_path, caplog
):
    import logging

    cron = await _cron_with_watermark(tmp_path, None, onmissed="run-all")

    async def boom(*a, **k):
        raise RuntimeError("append failed")

    cron.state_backend.append_record = boom  # type: ignore[method-assign]
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._checkpoint_catchup("j", "open", "wm")  # swallowed
    assert any(
        "could not checkpoint" in r.getMessage() for r in caplog.records
    )


# --- _catch_up orchestration edges -----------------------------------


async def test_catchup_catch_up_defers_before_retry_interval(tmp_path):
    cron = await _cron_with_watermark(tmp_path, None, onmissed="run-all")
    cron._catchup_next_retry = asyncio.get_running_loop().time() + 1000
    await cron._catch_up(_NOW)
    assert cron._caught_up is False  # bailed out before evaluating
    assert cron._catchup_tasks == set()


async def test_catchup_catch_up_no_state_warns_archive_and_gate(caplog):
    import logging

    yaml = (
        "jobs:\n  - name: j\n    command: 'true'\n    schedule: '* * * * *'\n"
        "    archiveOutput: true\n    onlyIfLastSucceeded: true\n"
    )
    cron = cronstable.cron.Cron(None, config_yaml=yaml)  # no state backend
    with caplog.at_level(logging.INFO, logger="cronstable"):
        await cron._catch_up(_NOW)
    assert cron._caught_up is True
    msgs = [r.getMessage() for r in caplog.records]
    assert any("archiveOutput" in m for m in msgs)
    assert any("onlyIfLastSucceeded" in m for m in msgs)


async def test_catchup_catch_up_reraises_cancelled(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )

    async def boom(now):
        raise asyncio.CancelledError()

    cron._evaluate_catch_up = boom  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await cron._catch_up(_NOW)


async def test_catchup_catch_up_defers_on_unexpected_error(tmp_path, caplog):
    import logging

    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )

    async def boom(now):
        raise RuntimeError("kaboom")

    cron._evaluate_catch_up = boom  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await cron._catch_up(_NOW)
    assert cron._caught_up is False  # unresolved -> will retry
    assert cron._catchup_next_retry > 0
    assert any("evaluating" in r.getMessage() for r in caplog.records)


# --- _evaluate_catch_up edges ----------------------------------------


async def test_catchup_evaluate_catch_up_skips_already_done(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    cron._catchup_done.add("j")  # already resolved on an earlier pass
    assert await cron._evaluate_catch_up(_NOW) is False  # nothing pending


async def test_catchup_evaluate_catch_up_pins_pre_pause_watermark(tmp_path):
    # a paused job with no open checkpoint pins the pre-pause watermark (an
    # `open` checkpoint) and defers rather than latching.
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    cron._paused["j"] = _catchup_pause()
    assert await cron._evaluate_catch_up(_NOW) is True  # deferred
    recs = await cron.state_backend.list_records(cron._catchup_stream("j"))
    assert recs and recs[0]["kind"] == "open"


async def test_catchup_evaluate_catch_up_pause_pin_error_defers(tmp_path, caplog):
    import logging

    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    cron._paused["j"] = _catchup_pause()

    async def boom(name):
        raise RuntimeError("store down")

    cron._pending_catchup_watermark = boom  # type: ignore[method-assign]
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        assert await cron._evaluate_catch_up(_NOW) is True
    assert any("pin the pre-pause" in r.getMessage() for r in caplog.records)


async def test_catchup_evaluate_catch_up_reraises_cancelled(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )

    async def boom(job, now):
        raise asyncio.CancelledError()

    cron._missed_occurrences = boom  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await cron._evaluate_catch_up(_NOW)


# --- _run_catch_up edges ---------------------------------------------


async def test_catchup_run_catch_up_reread_error_drops(tmp_path, caplog):
    import logging

    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]

    async def boom(job, now):
        raise RuntimeError("watermark read failed")

    cron._missed_occurrences = boom  # type: ignore[method-assign]
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._run_catch_up(cron.cron_jobs["j"], 3, 0.0, _NOW)
    assert calls == []
    assert any("re-read" in r.getMessage() for r in caplog.records)


async def test_catchup_run_catch_up_reraises_cancelled(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )

    async def boom(job, now):
        raise asyncio.CancelledError()

    cron._missed_occurrences = boom  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await cron._run_catch_up(cron.cron_jobs["j"], 3, 0.0, _NOW)


async def test_catchup_run_catch_up_nothing_owed_closes_cycle(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]

    async def zero(job, now):
        return 0, "2026-07-01T10:00:00+00:00"

    cron._missed_occurrences = zero  # type: ignore[method-assign]
    await cron._run_catch_up(cron.cron_jobs["j"], 3, 0.0, _NOW)
    assert calls == []
    recs = await cron.state_backend.list_records(cron._catchup_stream("j"))
    assert recs and recs[0]["kind"] == "close"


async def test_catchup_run_catch_up_bails_when_idle_wait_signals_stop(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]

    async def two(job, now):
        return 2, "wm"

    async def idle_false(name, *, max_wait=None):
        return False  # shutdown signalled while draining

    cron._missed_occurrences = two  # type: ignore[method-assign]
    cron._wait_job_idle = idle_false  # type: ignore[method-assign]
    await cron._run_catch_up(cron.cron_jobs["j"], 2, 0.0, _NOW)
    assert calls == []


async def test_catchup_run_catch_up_ownership_moves_mid_backfill(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]

    async def two(job, now):
        return 2, "wm"

    async def idle_true(name, *, max_wait=None):
        return True

    seen = {"n": 0}

    def allows(job):
        seen["n"] += 1
        return seen["n"] <= 1  # owner after jitter, then ownership moves

    cron._missed_occurrences = two  # type: ignore[method-assign]
    cron._wait_job_idle = idle_true  # type: ignore[method-assign]
    cron._cluster_allows = allows  # type: ignore[method-assign]
    await cron._run_catch_up(cron.cron_jobs["j"], 2, 0.0, _NOW)
    assert calls == []  # left to the new owner before any launch


async def test_catchup_run_catch_up_paused_mid_backfill(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]

    async def two(job, now):
        return 2, "wm"

    async def idle_true(name, *, max_wait=None):
        return True

    cron._missed_occurrences = two  # type: ignore[method-assign]
    cron._wait_job_idle = idle_true  # type: ignore[method-assign]
    cron._paused["j"] = _catchup_pause()
    await cron._run_catch_up(cron.cron_jobs["j"], 2, 0.0, _NOW)
    assert calls == []  # dropped without closing the checkpoint


async def test_catchup_run_catch_up_final_drain_signals_stop(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]

    async def one(job, now):
        return 1, "wm"

    idle = {"n": 0}

    async def idle_cb(name, *, max_wait=None):
        idle["n"] += 1
        return idle["n"] == 1  # go for the loop, stop on the final drain

    cron._missed_occurrences = one  # type: ignore[method-assign]
    cron._wait_job_idle = idle_cb  # type: ignore[method-assign]
    await cron._run_catch_up(cron.cron_jobs["j"], 1, 0.0, _NOW)
    assert calls == ["j"]  # the one launch happened
    recs = await cron.state_backend.list_records(cron._catchup_stream("j"))
    assert not any(r.get("kind") == "close" for r in recs)  # not closed


async def test_catchup_run_catch_up_outer_error_never_kills_loop(
    tmp_path, caplog
):
    import logging

    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]

    async def two(job, now):
        return 2, "wm"

    async def idle_boom(name, *, max_wait=None):
        raise RuntimeError("unexpected")

    cron._missed_occurrences = two  # type: ignore[method-assign]
    cron._wait_job_idle = idle_boom  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await cron._run_catch_up(cron.cron_jobs["j"], 2, 0.0, _NOW)  # no raise
    assert calls == []
    assert any("backfilling" in r.getMessage() for r in caplog.records)


# --- _wait_job_idle --------------------------------------------------


async def test_catchup_wait_job_idle_returns_false_on_stop():
    cron = cronstable.cron.Cron(None, config_yaml=_PAUSABLE_JOB)
    cron.running_jobs["p"] = ["sentinel"]  # still busy
    cron._stop_event.set()  # shutdown while waiting
    assert await cron._wait_job_idle("p") is False


# --- _defer_paused_reboot / _process_paused_reboots ------------------


def test_catchup_defer_paused_reboot_is_idempotent():
    cron = cronstable.cron.Cron(None)
    cron._defer_paused_reboot("boot")
    cron._defer_paused_reboot("boot")  # already held -> early return
    assert cron._paused_reboot_jobs == {"boot"}


async def test_catchup_process_paused_reboots_absent_stays_owed(monkeypatch):
    cron = cronstable.cron.Cron(None)
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda j: launched.append(j.name) or _noop(),
    )
    cron._paused_reboot_jobs.add("ghost")  # name not in cron_jobs
    await cron._process_paused_reboots()
    assert "ghost" in cron._paused_reboot_jobs  # transiently absent -> owed
    assert launched == []


async def test_catchup_process_paused_reboots_retires_non_reboot(monkeypatch):
    cron = cronstable.cron.Cron(None)
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda j: launched.append(j.name) or _noop(),
    )
    cron.cron_jobs["boot"] = _reboot_job(enabled=False)  # disabled on reload
    cron._paused_reboot_jobs.add("boot")
    await cron._process_paused_reboots()
    assert "boot" not in cron._paused_reboot_jobs  # retired without running
    assert launched == []


async def test_catchup_process_paused_reboots_still_paused_keeps_owed(monkeypatch):
    cron = cronstable.cron.Cron(None)
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda j: launched.append(j.name) or _noop(),
    )
    cron.cron_jobs["boot"] = _reboot_job()
    cron._paused_reboot_jobs.add("boot")
    cron._paused["boot"] = _catchup_pause()  # pause has not lifted yet
    await cron._process_paused_reboots()
    assert "boot" in cron._paused_reboot_jobs  # still deferred
    assert launched == []


async def test_catchup_process_paused_reboots_ownership_moved_keeps_owed(
    monkeypatch,
):
    cron = cronstable.cron.Cron(None)
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda j: launched.append(j.name) or _noop(),
    )
    cron.cron_jobs["boot"] = _reboot_job()
    cron._paused_reboot_jobs.add("boot")
    cron._cluster_allows = lambda job: False  # type: ignore[method-assign]
    await cron._process_paused_reboots()
    assert "boot" in cron._paused_reboot_jobs  # ownership moved -> still owed
    assert launched == []


# --- _spawn_due_jobs dead schedule / _launch_plan --------------------


async def test_catchup_spawn_due_jobs_latches_dead_schedule(monkeypatch, caplog):
    import logging

    yaml = (
        "jobs:\n  - name: dead\n    command: echo x\n"
        "    schedule: '0 0 30 2 *'\n"  # February 30th: never fires again
    )
    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    monkeypatch.setattr(cron, "launch_scheduled_job", lambda j: _noop())
    now = cronstable.cron.get_now(UTC)
    cron._set_next_fire("dead", now)  # force it due this pass
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._spawn_due_jobs(now)
    assert "dead" in cron._dead_schedules
    assert "dead" not in cron._next_fire
    assert any("NEVER fire again" in r.getMessage() for r in caplog.records)


async def test_catchup_launch_plan_skips_shallow_jobs_in_later_rounds(monkeypatch):
    cron = cronstable.cron.Cron(None, config_yaml=_THREE_DUE)
    launched = []

    async def fake(job):
        launched.append(job.name)

    monkeypatch.setattr(cron, "launch_scheduled_job", fake)
    now = cronstable.cron.get_now(UTC)
    later = now + datetime.timedelta(minutes=1)
    plan = [
        (cron.cron_jobs["a"], [now, later]),  # two catch-up rounds
        (cron.cron_jobs["b"], [now]),  # only one -> skipped in round 2
    ]
    await cron._launch_plan(plan)
    # round 0: a, b concurrently; round 1: a alone (b past its fire list)
    assert launched == ["a", "b", "a"]


# --- _reboot_marker_covers / _reboot_boot_gate -----------------------


async def test_catchup_reboot_marker_covers_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_CATCHUP_REBOOT_YAML)
    assert await cron._reboot_marker_covers(cron.cron_jobs["boot"]) is False


async def test_catchup_reboot_marker_covers_ignores_foreign_host(tmp_path):
    cron = await _catchup_reboot_cron(tmp_path)
    await cron.state_backend.append_record(
        cron._reboot_stream("boot"),
        {"host": "some-other-host", "jobDigest": "x", "bootId": "y"},
    )
    # only this host's markers decide; a foreign one is skipped -> not covered.
    assert await cron._reboot_marker_covers(cron.cron_jobs["boot"]) is False


async def test_catchup_reboot_gate_sick_runs_without_dedupe(tmp_path, caplog):
    import logging

    cron = await _catchup_reboot_cron(tmp_path)
    cron._reboot_gate_sick = True  # a prior op timed out this pass
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        assert await cron._reboot_boot_gate(cron.cron_jobs["boot"]) is True
    assert any("without boot-marker" in r.getMessage() for r in caplog.records)


async def test_catchup_reboot_gate_reraises_cancelled_read(tmp_path):
    cron = await _catchup_reboot_cron(tmp_path)

    async def boom(job):
        raise asyncio.CancelledError()

    cron._reboot_marker_covers = boom  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await cron._reboot_boot_gate(cron.cron_jobs["boot"])


async def test_catchup_reboot_gate_read_timeout_marks_sick_then_runs(tmp_path):
    cron = await _catchup_reboot_cron(tmp_path)

    async def boom(job):
        raise asyncio.TimeoutError()

    cron._reboot_marker_covers = boom  # type: ignore[method-assign]
    # default degrade policy: a read timeout latches sick and runs the job.
    assert await cron._reboot_boot_gate(cron.cron_jobs["boot"]) is True
    assert cron._reboot_gate_sick is True


async def test_catchup_reboot_gate_reraises_cancelled_write(tmp_path):
    cron = await _catchup_reboot_cron(tmp_path)

    async def not_covered(job):
        return False

    async def boom(*a, **k):
        raise asyncio.CancelledError()

    cron._reboot_marker_covers = not_covered  # type: ignore[method-assign]
    cron.state_backend.append_record = boom  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await cron._reboot_boot_gate(cron.cron_jobs["boot"])


async def test_catchup_reboot_gate_write_timeout_fail_closed_recheck_visible(
    tmp_path,
):
    cron = await _catchup_reboot_cron(tmp_path)
    cron._state_on_unavailable = "fail-closed"
    seen = {"n": 0}

    async def marker(job):
        seen["n"] += 1
        return seen["n"] > 1  # absent at the gate, visible on the re-check

    async def boom(*a, **k):
        raise asyncio.TimeoutError()

    cron._reboot_marker_covers = marker  # type: ignore[method-assign]
    cron.state_backend.append_record = boom  # type: ignore[method-assign]
    # the abandoned append landed late: the re-check sees it -> launch.
    assert await cron._reboot_boot_gate(cron.cron_jobs["boot"]) is True
    assert cron._reboot_gate_sick is True


async def test_catchup_reboot_gate_write_timeout_fail_closed_recheck_absent(
    tmp_path, caplog
):
    import logging

    cron = await _catchup_reboot_cron(tmp_path)
    cron._state_on_unavailable = "fail-closed"
    seen = {"n": 0}

    async def marker(job):
        seen["n"] += 1
        if seen["n"] == 1:
            return False  # absent at the gate
        raise RuntimeError("still unknown")  # re-check cannot decide

    async def boom(*a, **k):
        raise asyncio.TimeoutError()

    cron._reboot_marker_covers = marker  # type: ignore[method-assign]
    cron.state_backend.append_record = boom  # type: ignore[method-assign]
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        assert await cron._reboot_boot_gate(cron.cron_jobs["boot"]) is False
    assert any("cannot record" in r.getMessage() for r in caplog.records)


# --- _process_pending_reboots edges ----------------------------------


async def test_catchup_pending_reboots_election_removed_paused_keeps_owed(
    monkeypatch,
):
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = False  # election turned off on reload
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda j: launched.append(j.name) or _noop(),
    )
    job = _reboot_job()
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    cron._paused["boot"] = _catchup_pause()
    await cron._process_pending_reboots()
    assert launched == []
    assert "boot" in cron._pending_reboot_jobs  # pause defers, keeps it owed


async def test_catchup_pending_reboots_no_manager_absent_kept():
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    cron.cluster_manager = None  # manager never came up
    cron._pending_reboot_jobs["ghost"] = _reboot_job("ghost")  # not in jobs
    await cron._process_pending_reboots()
    assert "ghost" in cron._pending_reboot_jobs  # never-lose


async def test_catchup_pending_reboots_no_manager_paused_keeps_owed(monkeypatch):
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    cron.cluster_manager = None
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda j: launched.append(j.name) or _noop(),
    )
    job = _reboot_job(policy="PreferLeader")
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    cron._paused["boot"] = _catchup_pause()
    await cron._process_pending_reboots()
    assert launched == []
    assert "boot" in cron._pending_reboot_jobs  # deferred by the pause


async def test_catchup_pending_reboots_reboot_ran_error_keeps_owed(caplog):
    import logging

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    class _RaisingMgr:
        node_name = "node-a"
        distribution = "single-leader"

        def reboot_ran(self, name):
            raise RuntimeError("backend read failed")

    cron.cluster_manager = _RaisingMgr()
    job = _reboot_job()
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await cron._process_pending_reboots()
    assert "boot" in cron._pending_reboot_jobs  # kept pending on read error
    assert any("already ran" in r.getMessage() for r in caplog.records)


# --- CancelledError re-raise paths (defensive) -----------------------


async def test_catchup_pause_excusal_window_reraises_cancelled(tmp_path):
    cron = await _cron_with_watermark(tmp_path, None, onmissed="run-all")

    async def boom(*a, **k):
        raise asyncio.CancelledError()

    cron.state_backend.list_records = boom  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await cron._pause_excusal_window("j")


async def test_catchup_evaluate_catch_up_pause_pin_reraises_cancelled(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    cron._paused["j"] = _catchup_pause()

    async def boom(name):
        raise asyncio.CancelledError()

    cron._pending_catchup_watermark = boom  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await cron._evaluate_catch_up(_NOW)


async def test_catchup_reboot_gate_write_timeout_recheck_reraises_cancelled(
    tmp_path,
):
    cron = await _catchup_reboot_cron(tmp_path)
    cron._state_on_unavailable = "fail-closed"
    seen = {"n": 0}

    async def marker(job):
        seen["n"] += 1
        if seen["n"] == 1:
            return False  # absent at the gate
        raise asyncio.CancelledError()  # cancelled during the re-check

    async def boom(*a, **k):
        raise asyncio.TimeoutError()

    cron._reboot_marker_covers = marker  # type: ignore[method-assign]
    cron.state_backend.append_record = boom  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await cron._reboot_boot_gate(cron.cron_jobs["boot"])




# --- CancelledError re-raise paths (defensive) -----------------------


async def test_catchup_pause_excusal_window_reraises_cancelled(tmp_path):
    cron = await _cron_with_watermark(tmp_path, None, onmissed="run-all")

    async def boom(*a, **k):
        raise asyncio.CancelledError()

    cron.state_backend.list_records = boom  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await cron._pause_excusal_window("j")


async def test_catchup_evaluate_catch_up_pause_pin_reraises_cancelled(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    cron._paused["j"] = _catchup_pause()

    async def boom(name):
        raise asyncio.CancelledError()

    cron._pending_catchup_watermark = boom  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await cron._evaluate_catch_up(_NOW)


async def test_catchup_reboot_gate_write_timeout_recheck_reraises_cancelled(
    tmp_path,
):
    cron = await _catchup_reboot_cron(tmp_path)
    cron._state_on_unavailable = "fail-closed"
    seen = {"n": 0}

    async def marker(job):
        seen["n"] += 1
        if seen["n"] == 1:
            return False  # absent at the gate
        raise asyncio.CancelledError()  # cancelled during the re-check

    async def boom(*a, **k):
        raise asyncio.TimeoutError()

    cron._reboot_marker_covers = marker  # type: ignore[method-assign]
    cron.state_backend.append_record = boom  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await cron._reboot_boot_gate(cron.cron_jobs["boot"])




# ---------------------------------------------------------------------------
# Cluster concurrency slot leasing.
#   _log_cluster_role error swallow, maybe_launch_job cluster start-failure
#   cleanup, _prepare_job_api_run secret staging, _slot_fidelity_reason,
#   _acquire_slot_lease, _claim_cluster_slot, _spawn_slot_pursuit,
#   _pursue_replace_slot, _slot_renewer, and the release paths.
# ---------------------------------------------------------------------------

import cronstable.state as _slotlease_state

_SLOTLEASE_REAL_SLEEP = asyncio.sleep


async def _slotlease_fast_sleep(_delay=0, *args, **kwargs):
    # collapse the slot renewer / pursuit poll waits (floored at 1.0s) so the
    # loops iterate instantly; loop.time() still advances a hair each pass.
    await _SLOTLEASE_REAL_SLEEP(0)


async def _slotlease_cancel(*tasks):
    for task in tasks:
        if task is None:
            continue
        task.cancel()
        try:
            await task
        except BaseException:
            pass


def _slotlease_lease(name="slots/s", holder="peer#1", fence=1, expires_at=1e12):
    return _slotlease_state.Lease(
        name=name, holder=holder, fence=fence, expires_at=expires_at
    )


_SLOTLEASE_CLUSTER_FORBID = """
jobs:
  - name: s
    command: echo hi
    schedule: "* * * * *"
    concurrencyScope: cluster
    concurrencyPolicy: Forbid
"""

_SLOTLEASE_CLUSTER_REPLACE = """
jobs:
  - name: s
    command: echo hi
    schedule: "* * * * *"
    concurrencyScope: cluster
    concurrencyPolicy: Replace
"""


class _SlotleaseBackend:
    """A single-shot fake state backend for the slot-leasing paths."""

    def __init__(
        self,
        *,
        acquire=None,
        read=None,
        renew=None,
        acquire_exc=None,
        read_exc=None,
        renew_exc=None,
        release_exc=None,
        append_exc=None,
        records=None,
    ):
        self._acquire = acquire
        self._read = read
        self._renew = renew
        self.acquire_exc = acquire_exc
        self.read_exc = read_exc
        self.renew_exc = renew_exc
        self.release_exc = release_exc
        self.append_exc = append_exc
        self.records = records if records is not None else []
        self.released = []
        self.appended = []

    async def acquire_lease(self, name, holder, ttl):
        if self.acquire_exc is not None:
            raise self.acquire_exc
        return self._acquire

    async def read_lease(self, name):
        if self.read_exc is not None:
            raise self.read_exc
        return self._read

    async def renew_lease(self, lease, ttl):
        if self.renew_exc is not None:
            raise self.renew_exc
        return self._renew

    async def release_lease(self, lease):
        if self.release_exc is not None:
            raise self.release_exc
        self.released.append(lease)

    async def append_record(
        self, stream, data, *, prune_keep=None, prune_latest_by=None
    ):
        if self.append_exc is not None:
            raise self.append_exc
        self.appended.append((stream, data))
        return "rid"

    async def list_records(self, stream, *, limit=None, newest_first=False):
        return list(self.records)


# --- _log_cluster_role: swallow a backend read error (7441-7442) -----------


def test_slotlease_log_cluster_role_swallows_backend_error(caplog):
    import logging

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    class _Boom:
        def conflict_names(self):
            raise RuntimeError("store unreachable")

    cron.cluster_manager = _Boom()
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        cron._log_cluster_role()  # must not raise
    assert any(
        "error while logging cluster role" in r.message for r in caplog.records
    )


# --- _prepare_job_api_run: stage secrets, skip an unresolvable one ----------


def test_slotlease_prepare_job_api_run_skips_unresolvable_secret(
    monkeypatch, caplog
):
    import logging
    import types

    cron = cronstable.cron.Cron(None)
    registered = []

    class _Api:
        base_url = "http://127.0.0.1:65500"
        cacert = None

        def register_run(self, ctx):
            registered.append(ctx)

    cron._job_api = _Api()
    monkeypatch.delenv("SLOTLEASE_UNSET_SECRET", raising=False)
    job = types.SimpleNamespace(
        name="s",
        secrets=[
            {"name": "good", "value": "v1"},
            {"name": "bad", "fromEnvVar": "SLOTLEASE_UNSET_SECRET"},
        ],
        stateAllowedScopes=[],
    )
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        token, env = cron._prepare_job_api_run(job, None)
    assert token is not None
    assert registered and registered[0].secrets == {"good": "v1"}
    assert any(
        "could not stage secret" in r.message for r in caplog.records
    )
    assert "CRONSTABLE_STATE_URL" in env or env  # env was built


# --- _slot_fidelity_reason -------------------------------------------------


@pytest.mark.asyncio
async def test_slotlease_slot_fidelity_reason_no_backend():
    cron = cronstable.cron.Cron(None)
    cron.state_backend = None
    assert await cron._slot_fidelity_reason() is None


@pytest.mark.asyncio
async def test_slotlease_slot_fidelity_reason_probe_error_is_inconclusive():
    cron = cronstable.cron.Cron(None)

    class _B:
        async def verify_locking(self):
            raise RuntimeError("probe blip")

    cron.state_backend = _B()
    cron._slot_fidelity = None
    assert await cron._slot_fidelity_reason() is None
    assert cron._slot_fidelity is None  # nothing latched; retried next claim


@pytest.mark.asyncio
async def test_slotlease_slot_fidelity_reason_cancelled_propagates():
    cron = cronstable.cron.Cron(None)

    class _B:
        async def verify_locking(self):
            raise asyncio.CancelledError

    cron.state_backend = _B()
    cron._slot_fidelity = None
    with pytest.raises(asyncio.CancelledError):
        await cron._slot_fidelity_reason()


@pytest.mark.asyncio
async def test_slotlease_slot_fidelity_reason_latches_and_logs(caplog):
    import logging

    cron = cronstable.cron.Cron(None)

    class _B:
        async def verify_locking(self):
            return "locks are advisory only"

    cron.state_backend = _B()
    cron._slot_fidelity = None
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        assert await cron._slot_fidelity_reason() == "locks are advisory only"
    assert cron._slot_fidelity == "locks are advisory only"
    assert any(
        "file locks cannot be trusted" in r.message for r in caplog.records
    )


# --- _acquire_slot_lease: map timeout/error to None, re-raise cancel --------


@pytest.mark.asyncio
async def test_slotlease_acquire_slot_lease_maps_failures_to_none():
    cron = cronstable.cron.Cron(None)
    timed_out = _SlotleaseBackend(acquire_exc=asyncio.TimeoutError())
    errored = _SlotleaseBackend(acquire_exc=RuntimeError("flock ENOLCK"))
    assert await cron._acquire_slot_lease(timed_out, "slots/s") is None
    assert await cron._acquire_slot_lease(errored, "slots/s") is None


@pytest.mark.asyncio
async def test_slotlease_acquire_slot_lease_cancel_propagates():
    cron = cronstable.cron.Cron(None)
    cancelling = _SlotleaseBackend(acquire_exc=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await cron._acquire_slot_lease(cancelling, "slots/s")


# --- _claim_cluster_slot ---------------------------------------------------


def _slotlease_cluster_cron(policy_yaml=_SLOTLEASE_CLUSTER_FORBID, monkeypatch=None):
    cron = cronstable.cron.Cron(None, config_yaml=policy_yaml)
    cron._state_configured = True
    cron._slot_fidelity = ""  # verified: locks fence, skip the probe
    if monkeypatch is not None:
        async def _noop_reconcile(job):
            return None

        monkeypatch.setattr(
            cron, "_reconcile_takeover_inflight", _noop_reconcile
        )
    return cron


@pytest.mark.asyncio
async def test_slotlease_claim_returns_true_when_state_not_configured():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron._state_configured = False
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is True


@pytest.mark.asyncio
async def test_slotlease_claim_degrades_when_backend_is_none():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron._state_configured = True
    cron.state_backend = None
    cron._state_on_unavailable = "degrade"
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is True
    assert cron._slot_refs["s"] == 1  # node-local enforcement refcount


@pytest.mark.asyncio
async def test_slotlease_claim_fails_closed_when_backend_is_none():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron._state_configured = True
    cron.state_backend = None
    cron._state_on_unavailable = "fail-closed"
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is False
    assert "s" not in cron._slot_refs


@pytest.mark.asyncio
async def test_slotlease_claim_degrades_when_locks_cannot_fence(monkeypatch):
    cron = _slotlease_cluster_cron(monkeypatch=monkeypatch)
    cron.state_backend = _SlotleaseBackend()

    async def _bad_fidelity():
        return "locks are advisory only"

    monkeypatch.setattr(cron, "_slot_fidelity_reason", _bad_fidelity)
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is True
    assert cron._slot_refs["s"] == 1


@pytest.mark.asyncio
async def test_slotlease_claim_adopts_live_local_lease(monkeypatch):
    cron = _slotlease_cluster_cron(monkeypatch=monkeypatch)
    cron.state_backend = _SlotleaseBackend()
    live = asyncio.create_task(_SLOTLEASE_REAL_SLEEP(30))
    cron._slot_leases["s"] = _slotlease_lease(holder=cron._slot_holder())
    cron._slot_renewers["s"] = live
    cron._slot_refs["s"] = 1
    try:
        assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is True
        assert cron._slot_refs["s"] == 2  # adopted the live lease
    finally:
        await _slotlease_cancel(live)


@pytest.mark.asyncio
async def test_slotlease_claim_forbid_when_peer_holds_slot(monkeypatch):
    cron = _slotlease_cluster_cron(monkeypatch=monkeypatch)
    cron.state_backend = _SlotleaseBackend(acquire=None, read=_slotlease_lease())
    seen = []
    monkeypatch.setattr(cron, "_sla_peer_owns_slot", lambda name: seen.append(name))
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is False
    assert seen == ["s"]


@pytest.mark.asyncio
async def test_slotlease_claim_replace_spawns_pursuit(monkeypatch):
    cron = _slotlease_cluster_cron(_SLOTLEASE_CLUSTER_REPLACE, monkeypatch=monkeypatch)
    cron.state_backend = _SlotleaseBackend(acquire=None, read=_slotlease_lease())
    spawned = []

    async def _fake_pursue(job, observed):
        spawned.append((job.name, observed))

    monkeypatch.setattr(cron, "_pursue_replace_slot", _fake_pursue)
    monkeypatch.setattr(cron, "_sla_peer_owns_slot", lambda name: None)
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is False
    pursuit = cron._slot_pursuits.get("s")
    if pursuit is not None:
        await pursuit
    assert spawned and spawned[0][0] == "s"


@pytest.mark.asyncio
async def test_slotlease_claim_adopts_own_late_acquire(monkeypatch):
    cron = _slotlease_cluster_cron(monkeypatch=monkeypatch)
    # acquire timed out (None) but the read shows OUR holder landed the write.
    cron.state_backend = _SlotleaseBackend(
        acquire=None, read=_slotlease_lease(holder=cron._slot_holder())
    )
    try:
        assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is True
        assert cron._slot_leases["s"].holder == cron._slot_holder()
        assert cron._slot_refs["s"] == 1
    finally:
        await _slotlease_cancel(cron._slot_renewers.get("s"))


@pytest.mark.asyncio
async def test_slotlease_claim_expired_unreclaimed_falls_to_policy(monkeypatch):
    cron = _slotlease_cluster_cron(monkeypatch=monkeypatch)
    # a foreign lease whose TTL already lapsed: treated as unanswered, so the
    # degrade policy grants a node-local run.
    cron.state_backend = _SlotleaseBackend(
        acquire=None, read=_slotlease_lease(expires_at=1.0)
    )
    cron._state_on_unavailable = "degrade"
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is True
    assert cron._slot_refs["s"] == 1


@pytest.mark.asyncio
async def test_slotlease_claim_read_timeout_is_unanswered(monkeypatch):
    cron = _slotlease_cluster_cron(monkeypatch=monkeypatch)
    cron.state_backend = _SlotleaseBackend(
        acquire=None, read_exc=asyncio.TimeoutError()
    )
    cron._state_on_unavailable = "fail-closed"
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is False


@pytest.mark.asyncio
async def test_slotlease_claim_read_error_is_unanswered(monkeypatch):
    cron = _slotlease_cluster_cron(monkeypatch=monkeypatch)
    cron.state_backend = _SlotleaseBackend(
        acquire=None, read_exc=RuntimeError("EIO")
    )
    cron._state_on_unavailable = "fail-closed"
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is False


@pytest.mark.asyncio
async def test_slotlease_claim_success_cancels_stale_renewer(monkeypatch):
    cron = _slotlease_cluster_cron(monkeypatch=monkeypatch)
    cron.state_backend = _SlotleaseBackend(acquire=_slotlease_lease(holder=cron._slot_holder()))
    # a live renewer with no recorded lease (the adoption branch is skipped):
    # the fresh acquire must cancel it and install a replacement.
    stale = asyncio.create_task(_SLOTLEASE_REAL_SLEEP(30))
    cron._slot_renewers["s"] = stale
    try:
        assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is True
        new = cron._slot_renewers["s"]
        assert new is not stale  # replaced by a fresh renewer
        await _slotlease_cancel(stale, new)
        assert stale.cancelled()
    finally:
        await _slotlease_cancel(stale, cron._slot_renewers.get("s"))


# --- _spawn_slot_pursuit: single-flight ------------------------------------


@pytest.mark.asyncio
async def test_slotlease_spawn_slot_pursuit_is_single_flight():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    job = cron.cron_jobs["s"]
    existing = asyncio.create_task(_SLOTLEASE_REAL_SLEEP(30))
    cron._slot_pursuits["s"] = existing
    try:
        cron._spawn_slot_pursuit(job, _slotlease_lease())
        assert cron._slot_pursuits["s"] is existing  # not replaced
    finally:
        await _slotlease_cancel(existing)


# --- _pursue_replace_slot --------------------------------------------------


@pytest.mark.asyncio
async def test_slotlease_pursue_replace_no_backend_returns():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    cron.state_backend = None
    await cron._pursue_replace_slot(cron.cron_jobs["s"], _slotlease_lease())


@pytest.mark.asyncio
async def test_slotlease_pursue_replace_append_failure_gives_up(caplog):
    import logging

    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    cron.state_backend = _SlotleaseBackend(append_exc=RuntimeError("no write"))
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._pursue_replace_slot(cron.cron_jobs["s"], _slotlease_lease())
    assert any(
        "could not record the cluster Replace cancel" in r.message
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_slotlease_pursue_replace_stops_on_shutdown():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    backend = _SlotleaseBackend(read=_slotlease_lease())
    cron.state_backend = backend
    cron._stop_event.set()
    await cron._pursue_replace_slot(cron.cron_jobs["s"], _slotlease_lease())
    assert backend.appended  # the cancel request was recorded before stopping


@pytest.mark.asyncio
async def test_slotlease_pursue_replace_relaunches_when_slot_frees(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    # append ok, then the slot reads back free (holder released) -> relaunch.
    cron.state_backend = _SlotleaseBackend(read=None)
    relaunched = []

    async def _fake_launch(job, **kwargs):
        relaunched.append(job.name)
        return True

    monkeypatch.setattr(cron, "maybe_launch_job", _fake_launch)
    await cron._pursue_replace_slot(cron.cron_jobs["s"], _slotlease_lease())
    assert relaunched == ["s"]


@pytest.mark.asyncio
async def test_slotlease_pursue_replace_read_error_is_ignored(monkeypatch, caplog):
    import logging

    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    # append ok; read raises (kept as observed = still foreign held), the
    # deadline (2 * ttl == 0) then trips and the launch is skipped.
    cron.state_backend = _SlotleaseBackend(read_exc=RuntimeError("blip"))
    cron._slot_ttl = 0.0
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._pursue_replace_slot(cron.cron_jobs["s"], _slotlease_lease())
    assert any("did not yield" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_slotlease_pursue_replace_read_cancel_propagates(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    cron.state_backend = _SlotleaseBackend(read_exc=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await cron._pursue_replace_slot(cron.cron_jobs["s"], _slotlease_lease())


# --- _slot_renewer ---------------------------------------------------------


@pytest.mark.asyncio
async def test_slotlease_slot_renewer_returns_when_lease_gone(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron.state_backend = _SlotleaseBackend()
    # no lease recorded -> the renewer stands down on its first cycle.
    await asyncio.wait_for(cron._slot_renewer("s"), timeout=5)


@pytest.mark.asyncio
async def test_slotlease_slot_renewer_retires_when_superseded(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron.state_backend = _SlotleaseBackend(renew=_slotlease_lease(holder="me#x"))
    cron._slot_leases["s"] = _slotlease_lease(holder=cron._slot_holder(), fence=5)
    # _slot_renewers has NO entry for "s": the renewer sees it was retired
    # mid-renew and stands down without touching _slot_leases.
    await asyncio.wait_for(cron._slot_renewer("s"), timeout=5)
    assert "s" in cron._slot_leases  # left for the finish path to release


class _SlotleaseRenewBackend:
    """Stateful renewer backend driven by a per-call script."""

    def __init__(self, cron, *, list_script, renew_script, read_script):
        self.cron = cron
        self.list_script = list(list_script)
        self.renew_script = list(renew_script)
        self.read_script = list(read_script)
        self.n = 0

    async def list_records(self, stream, *, limit=None, newest_first=False):
        self.n += 1
        item = self.list_script[min(self.n - 1, len(self.list_script) - 1)]
        if isinstance(item, Exception):
            raise item
        return item

    async def renew_lease(self, lease, ttl):
        item = self.renew_script[min(self.n - 1, len(self.renew_script) - 1)]
        if isinstance(item, Exception):
            raise item
        return item

    async def read_lease(self, name):
        item = self.read_script[min(self.n - 1, len(self.read_script) - 1)]
        if isinstance(item, Exception):
            raise item
        return item


@pytest.mark.asyncio
async def test_slotlease_slot_renewer_list_error_then_taken_over(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    mine = _slotlease_lease(holder=cron._slot_holder(), fence=5)
    cron._slot_leases["s"] = mine
    backend = _SlotleaseRenewBackend(
        cron,
        # cycle 1: list raises -> recs=[]; renew succeeds -> stored, continue.
        # cycle 2: list ok empty; renew denied (None) -> read shows a peer
        #          took the slot over -> pop + return.
        list_script=[RuntimeError("list blip"), []],
        renew_script=[_slotlease_lease(holder=cron._slot_holder(), fence=6), None],
        read_script=[None, _slotlease_lease(holder="peer#9", fence=9)],
    )
    cron.state_backend = backend
    task = asyncio.create_task(cron._slot_renewer("s"))
    cron._slot_renewers["s"] = task
    await asyncio.wait_for(task, timeout=5)
    assert "s" not in cron._slot_leases  # dropped on the takeover


@pytest.mark.asyncio
async def test_slotlease_slot_renewer_renew_timeout_then_error_then_taken_over(
    monkeypatch,
):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    mine = _slotlease_lease(holder=cron._slot_holder(), fence=5)
    cron._slot_leases["s"] = mine
    backend = _SlotleaseRenewBackend(
        cron,
        list_script=[[], [], []],
        # cycle 1: renew times out -> continue; cycle 2: renew errors ->
        # warn+continue; cycle 3: renew denied -> read shows takeover -> return.
        renew_script=[asyncio.TimeoutError(), RuntimeError("EIO"), None],
        read_script=[None, None, _slotlease_lease(holder="peer#3", fence=7)],
    )
    cron.state_backend = backend
    task = asyncio.create_task(cron._slot_renewer("s"))
    cron._slot_renewers["s"] = task
    await asyncio.wait_for(task, timeout=5)
    assert "s" not in cron._slot_leases


@pytest.mark.asyncio
async def test_slotlease_slot_renewer_replace_request_cancels_instance(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    mine = _slotlease_lease(holder=cron._slot_holder(), fence=5)
    cron._slot_leases["s"] = mine

    class _FakeRun:
        def __init__(self):
            self.replaced = False
            self.cancelled = False

        async def cancel(self):
            self.cancelled = True

    run = _FakeRun()
    cron.running_jobs["s"] = [run]
    backend = _SlotleaseRenewBackend(
        cron,
        # cycle 1: a cancel record aimed at our fence -> cancel the instance;
        #          renew denied -> read shows our own lease -> keep going.
        # cycle 2: no record; renew denied -> read shows takeover -> return.
        list_script=[
            [{"kind": "cancel", "fence": 5, "by": "peerZ"}],
            [],
        ],
        renew_script=[None, None],
        read_script=[
            _slotlease_lease(holder=cron._slot_holder(), fence=5),
            _slotlease_lease(holder="peer#4", fence=8),
        ],
    )
    cron.state_backend = backend
    task = asyncio.create_task(cron._slot_renewer("s"))
    cron._slot_renewers["s"] = task
    await asyncio.wait_for(task, timeout=5)
    assert run.replaced is True and run.cancelled is True


# --- release paths ---------------------------------------------------------


@pytest.mark.asyncio
async def test_slotlease_release_cluster_slot_decrements_refcount():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron._slot_refs["s"] = 2
    await cron._release_cluster_slot(cron.cron_jobs["s"])
    assert cron._slot_refs["s"] == 1  # still one user; lease kept


@pytest.mark.asyncio
async def test_slotlease_release_cluster_slot_kept_while_instance_runs():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron._slot_refs["s"] = 1
    cron._slot_leases["s"] = _slotlease_lease(holder=cron._slot_holder())
    cron.running_jobs["s"] = [object()]  # a spawning instance still present
    await cron._release_cluster_slot(cron.cron_jobs["s"])
    assert "s" in cron._slot_leases  # not released out from under the run
    assert "s" not in cron._slot_refs


@pytest.mark.asyncio
async def test_slotlease_release_cluster_slot_releases_lease():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    lease = _slotlease_lease(holder=cron._slot_holder())
    backend = _SlotleaseBackend()
    cron.state_backend = backend
    cron._slot_refs["s"] = 1
    cron._slot_leases["s"] = lease
    renewer = asyncio.create_task(_SLOTLEASE_REAL_SLEEP(30))
    cron._slot_renewers["s"] = renewer
    await cron._release_cluster_slot(cron.cron_jobs["s"])
    for task in list(cron._pending_state_writes):
        await task
    assert renewer.cancelled() or renewer.done()
    assert backend.released == [lease]


@pytest.mark.asyncio
async def test_slotlease_release_cluster_slot_phantom_cleanup():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    backend = _SlotleaseBackend(read=None)  # no lease on disk -> nothing to free
    cron.state_backend = backend
    cron._slot_refs["s"] = 1  # a degraded launch left a ref but no lease
    await cron._release_cluster_slot(cron.cron_jobs["s"])
    for task in list(cron._pending_state_writes):
        await task
    assert backend.released == []


@pytest.mark.asyncio
async def test_slotlease_release_slot_lease_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron.state_backend = None
    await cron._release_slot_lease("s", _slotlease_lease())  # returns, no error


@pytest.mark.asyncio
async def test_slotlease_release_slot_lease_skips_when_reclaimed():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    backend = _SlotleaseBackend()
    cron.state_backend = backend
    cron._slot_leases["s"] = _slotlease_lease(holder=cron._slot_holder())
    await cron._release_slot_lease("s", _slotlease_lease())
    assert backend.released == []  # a fresh claim adopted the on-disk lease


@pytest.mark.asyncio
async def test_slotlease_release_slot_lease_warns_on_error(caplog):
    import logging

    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron.state_backend = _SlotleaseBackend(release_exc=RuntimeError("EROFS"))
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._release_slot_lease("s", _slotlease_lease())
    assert any(
        "failed to release the concurrency slot" in r.message
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_slotlease_release_phantom_slot_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron.state_backend = None
    await cron._release_phantom_slot("s")  # returns, no error


@pytest.mark.asyncio
async def test_slotlease_release_phantom_slot_releases_own_lease():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    mine = _slotlease_lease(holder=cron._slot_holder())
    backend = _SlotleaseBackend(read=mine)
    cron.state_backend = backend
    await cron._release_phantom_slot("s")
    assert backend.released == [mine]


@pytest.mark.asyncio
async def test_slotlease_release_phantom_slot_swallows_error():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron.state_backend = _SlotleaseBackend(read_exc=RuntimeError("EIO"))
    await cron._release_phantom_slot("s")  # best-effort; no raise


# --- maybe_launch_job: cluster start-failure hands the slot back -----------


@pytest.mark.asyncio
async def test_slotlease_maybe_launch_job_releases_slot_on_start_failure(
    monkeypatch,
):
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    job = cron.cron_jobs["s"]

    async def _claim(j):
        return True

    monkeypatch.setattr(cron, "_claim_cluster_slot", _claim)
    released = []

    async def _release(j):
        released.append(j.name)

    monkeypatch.setattr(cron, "_release_cluster_slot", _release)
    finished = []

    class _Api:
        async def finish_run(self, token):
            finished.append(token)

    cron._job_api = _Api()
    monkeypatch.setattr(
        cron,
        "_prepare_job_api_run",
        lambda j, rs: ("tok123", {"CRONSTABLE_RUN_ID": "rid"}),
    )

    class _BoomRun:
        def __init__(self, *args, **kwargs):
            pass

        async def start(self):
            raise RuntimeError("spawn failed")

    monkeypatch.setattr(cronstable.cron, "RunningJob", _BoomRun)
    with pytest.raises(RuntimeError):
        await cron.maybe_launch_job(job)
    assert released == ["s"]
    assert finished == ["tok123"]


# --- cancellation propagates through every store call (never swallowed) -----


@pytest.mark.asyncio
async def test_slotlease_claim_read_lease_cancel_propagates(monkeypatch):
    cron = _slotlease_cluster_cron(monkeypatch=monkeypatch)
    cron.state_backend = _SlotleaseBackend(
        acquire=None, read_exc=asyncio.CancelledError()
    )
    with pytest.raises(asyncio.CancelledError):
        await cron._claim_cluster_slot(cron.cron_jobs["s"])


@pytest.mark.asyncio
async def test_slotlease_pursue_replace_append_cancel_propagates():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    cron.state_backend = _SlotleaseBackend(append_exc=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await cron._pursue_replace_slot(cron.cron_jobs["s"], _slotlease_lease())


@pytest.mark.asyncio
async def test_slotlease_slot_renewer_list_cancel_propagates(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron._slot_leases["s"] = _slotlease_lease(holder=cron._slot_holder())

    class _B:
        async def list_records(self, stream, *, limit=None, newest_first=False):
            raise asyncio.CancelledError

    cron.state_backend = _B()
    with pytest.raises(asyncio.CancelledError):
        await cron._slot_renewer("s")


@pytest.mark.asyncio
async def test_slotlease_slot_renewer_renew_cancel_propagates(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron._slot_leases["s"] = _slotlease_lease(holder=cron._slot_holder())

    class _B:
        async def list_records(self, stream, *, limit=None, newest_first=False):
            return []

        async def renew_lease(self, lease, ttl):
            raise asyncio.CancelledError

    cron.state_backend = _B()
    with pytest.raises(asyncio.CancelledError):
        await cron._slot_renewer("s")


@pytest.mark.asyncio
async def test_slotlease_slot_renewer_readback_error_then_takeover(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron._slot_leases["s"] = _slotlease_lease(holder=cron._slot_holder(), fence=5)
    backend = _SlotleaseRenewBackend(
        cron,
        list_script=[[], []],
        # renew denied both cycles; cycle 1 read-back errors -> continue,
        # cycle 2 read-back shows a takeover -> pop + return.
        renew_script=[None, None],
        read_script=[RuntimeError("blip"), _slotlease_lease(holder="peer#2", fence=8)],
    )
    cron.state_backend = backend
    task = asyncio.create_task(cron._slot_renewer("s"))
    cron._slot_renewers["s"] = task
    await asyncio.wait_for(task, timeout=5)
    assert "s" not in cron._slot_leases


@pytest.mark.asyncio
async def test_slotlease_slot_renewer_readback_cancel_propagates(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron._slot_leases["s"] = _slotlease_lease(holder=cron._slot_holder(), fence=5)

    class _B:
        async def list_records(self, stream, *, limit=None, newest_first=False):
            return []

        async def renew_lease(self, lease, ttl):
            return None  # denied -> falls through to the read-back

        async def read_lease(self, name):
            raise asyncio.CancelledError

    cron.state_backend = _B()
    task = asyncio.create_task(cron._slot_renewer("s"))
    cron._slot_renewers["s"] = task
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=5)


@pytest.mark.asyncio
async def test_slotlease_release_slot_lease_cancel_propagates():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron.state_backend = _SlotleaseBackend(release_exc=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await cron._release_slot_lease("s", _slotlease_lease())


@pytest.mark.asyncio
async def test_slotlease_release_phantom_slot_skips_when_claim_present():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    backend = _SlotleaseBackend(read=_slotlease_lease(holder=cron._slot_holder()))
    cron.state_backend = backend
    cron._slot_leases["s"] = _slotlease_lease(holder=cron._slot_holder())
    await cron._release_phantom_slot("s")
    assert backend.released == []  # a live claim owns the slot; not a phantom


@pytest.mark.asyncio
async def test_slotlease_release_phantom_slot_cancel_propagates():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron.state_backend = _SlotleaseBackend(read_exc=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await cron._release_phantom_slot("s")


# Non-cluster start-failure cleanup and the slot pursuit poll loop.

_SLOTLEASE_NODE_JOB = """
jobs:
  - name: s
    command: echo hi
    schedule: "* * * * *"
"""


@pytest.mark.asyncio
async def test_slotlease_maybe_launch_node_scope_start_failure_finishes_run(
    monkeypatch,
):
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_NODE_JOB)
    job = cron.cron_jobs["s"]
    released = []

    async def _release(j):
        released.append(j.name)

    monkeypatch.setattr(cron, "_release_cluster_slot", _release)
    finished = []

    class _Api:
        async def finish_run(self, token):
            finished.append(token)

    cron._job_api = _Api()
    monkeypatch.setattr(cron, "_prepare_job_api_run", lambda j, rs: ("tokN", {}))

    class _BoomRun:
        def __init__(self, *args, **kwargs):
            pass

        async def start(self):
            raise RuntimeError("spawn failed")

    monkeypatch.setattr(cronstable.cron, "RunningJob", _BoomRun)
    with pytest.raises(RuntimeError):
        await cron.maybe_launch_job(job)
    assert released == []  # node scope: no cluster slot to hand back
    assert finished == ["tokN"]  # but the job-API run registration is dropped


@pytest.mark.asyncio
async def test_slotlease_maybe_launch_start_failure_without_job_api(monkeypatch):
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_NODE_JOB)
    job = cron.cron_jobs["s"]
    monkeypatch.setattr(cron, "_prepare_job_api_run", lambda j, rs: (None, {}))
    cron._job_api = None

    class _BoomRun:
        def __init__(self, *args, **kwargs):
            pass

        async def start(self):
            raise RuntimeError("spawn failed")

    monkeypatch.setattr(cronstable.cron, "RunningJob", _BoomRun)
    with pytest.raises(RuntimeError):
        await cron.maybe_launch_job(job)


@pytest.mark.asyncio
async def test_slotlease_pursue_replace_polls_until_slot_frees(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    cron._slot_ttl = 100.0  # deadline is far off, so the poll loop iterates
    reads = [_slotlease_lease(), None]  # foreign held, then the holder yields

    class _B:
        async def append_record(self, stream, data, *, prune_keep=None,
                                 prune_latest_by=None):
            return "rid"

        async def read_lease(self, name):
            return reads.pop(0) if reads else None

    cron.state_backend = _B()
    relaunched = []

    async def _launch(job, **kwargs):
        relaunched.append(job.name)
        return True

    monkeypatch.setattr(cron, "maybe_launch_job", _launch)
    await cron._pursue_replace_slot(cron.cron_jobs["s"], _slotlease_lease())
    assert relaunched == ["s"]



# ===================== rehydrate additions =====================
#
# The durable-state plumbing in cronstable/cron.py:
# the inflight open/close persistence, crash reconciliation, run-record and
# counter-snapshot writes, the rehydrate/reconcile boot paths, retry re-arming
# and validation, and the completion/failure handlers. The degrade branches
# (backend torn down, store error, timeout, cancellation) are exercised
# alongside the real happy-path behaviour so the in-memory maps are asserted.

from cronstable.cron import JobRunInfo as _JRI5
from cronstable.cron import _job_run_info_from_dict as _from_dict5
from cronstable.fingerprint import job_digest as _job_digest5
from tests.test_state import _UTC as _UTC5
from tests.test_state import _state_cfg as _scfg5

_ONE_JOB_REHYDRATE = (
    "jobs:\n  - name: j\n    command: 'true'\n    schedule: '* * * * *'\n"
)

_RUNALL_REHYDRATE = (
    "jobs:\n  - name: j\n    command: 'true'\n    schedule: '* * * * *'\n"
    "    onMissed: run-all\n"
)

_DEP_REHYDRATE = (
    "jobs:\n  - name: j\n    command: 'true'\n    schedule: '* * * * *'\n"
    "    onlyIfLastSucceeded: true\n"
)

_RETRY_REHYDRATE = (
    "jobs:\n  - name: j\n    command: 'true'\n    schedule: '* * * * *'\n"
    "    onFailure:\n"
    "      retry:\n"
    "        maximumRetries: 2\n"
    "        initialDelay: 0.1\n"
    "        maximumDelay: 1\n"
    "        backoffMultiplier: 2\n"
)

_REBOOT_RETRY_REHYDRATE = (
    "jobs:\n  - name: j\n    command: 'true'\n    schedule: '@reboot'\n"
    "    onFailure:\n"
    "      retry:\n"
    "        maximumRetries: 2\n"
    "        initialDelay: 0.1\n"
    "        maximumDelay: 1\n"
    "        backoffMultiplier: 2\n"
)


def _rehydrate_cfg(tmp_path):
    return _scfg5("state:\n  path: " + str(tmp_path))


async def _rehydrate_state_cron(tmp_path, yaml=_ONE_JOB_REHYDRATE):
    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    await cron.start_stop_state(_rehydrate_cfg(tmp_path))
    return cron


async def _raise_oserror5(*args, **kwargs):
    raise OSError("state store went away")


async def _raise_cancelled5(*args, **kwargs):
    raise asyncio.CancelledError


async def _raise_timeout5(*args, **kwargs):
    raise asyncio.TimeoutError


def _mem_run5(outcome, minute):
    dt = datetime.datetime(2026, 7, 1, 10, minute, 0, tzinfo=_UTC5)
    return _JRI5(
        outcome=outcome,
        exit_code=0 if outcome == "success" else 1,
        started_at=dt,
        finished_at=dt,
        fail_reason=None,
        output=JobOutputStream(),
    )


async def _rehydrate_seed_pending_retry(
    cron, *, attempt=1, not_before="2026-07-01T10:00:00+00:00", host=None
):
    job = cron.cron_jobs["j"]
    await cron.state_backend.append_record(
        cron._retry_stream("j"),
        {
            "kind": "pending",
            "attempt": attempt,
            "notBefore": not_before,
            "host": host if host is not None else cron._state_host,
            "jobDigest": _job_digest5(job),
            "at": not_before,
        },
    )


class _FakeRun5:
    """A minimal RunningJob stand-in carrying just a config with a name."""

    def __init__(self, config, *, state_token=None):
        self.config = config
        self.state_token = state_token


# --- inflight open/closed persistence --------------------------------------


async def test_rehydrate_persist_inflight_open_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB_REHYDRATE)
    assert cron.state_backend is None
    await cron._persist_inflight_open(cron.cron_jobs["j"], object())


async def test_rehydrate_persist_inflight_open_degrades_on_error(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.append_record = _raise_oserror5

    class _RJ:
        proc = None

    await cron._persist_inflight_open(cron.cron_jobs["j"], _RJ())  # no raise


async def test_rehydrate_persist_inflight_closed_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB_REHYDRATE)
    await cron._persist_inflight_closed("j")


async def test_rehydrate_persist_inflight_closed_degrades_on_error(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.append_record = _raise_oserror5
    await cron._persist_inflight_closed("j")  # no raise


# --- inflight reconciliation ------------------------------------------------


async def test_rehydrate_reconcile_inflight_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB_REHYDRATE)
    await cron._reconcile_inflight()


async def test_rehydrate_reconcile_inflight_skips_running(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.running_jobs["j"].append(object())
    await cron._reconcile_inflight()  # the only job is running -> skipped
    assert "j" not in cron.last_run


async def test_rehydrate_reconcile_inflight_timeout_breaks(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.list_records = _raise_timeout5
    await cron._reconcile_inflight()  # a hung store aborts the whole pass
    assert "j" not in cron.last_run


async def test_rehydrate_reconcile_inflight_error_continues(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.list_records = _raise_oserror5
    await cron._reconcile_inflight()  # a store error skips the job, no crash
    assert "j" not in cron.last_run


async def test_rehydrate_reconcile_inflight_cancelled_propagates(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.list_records = _raise_cancelled5
    with pytest.raises(asyncio.CancelledError):
        await cron._reconcile_inflight()


# --- takeover reconciliation ------------------------------------------------


async def test_rehydrate_reconcile_takeover_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB_REHYDRATE)
    await cron._reconcile_takeover_inflight(cron.cron_jobs["j"])


async def test_rehydrate_reconcile_takeover_error_returns(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.list_records = _raise_oserror5
    await cron._reconcile_takeover_inflight(cron.cron_jobs["j"])  # no raise


async def test_rehydrate_reconcile_takeover_cancelled_propagates(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.list_records = _raise_cancelled5
    with pytest.raises(asyncio.CancelledError):
        await cron._reconcile_takeover_inflight(cron.cron_jobs["j"])


async def test_rehydrate_reconcile_takeover_skips_own_live_run(tmp_path):
    # a record this very process wrote (same host AND proc token) is our own
    # live run: the takeover must stand down and reconcile nothing.
    cron = await _rehydrate_state_cron(tmp_path)
    await cron.state_backend.append_record(
        cron._inflight_stream("j"),
        {
            "kind": "open",
            "host": cron._state_host,
            "proc": cron._proc_token,
            "pid": None,
            "startedAt": "2026-07-01T10:00:00+00:00",
        },
    )
    await cron._reconcile_takeover_inflight(cron.cron_jobs["j"])
    assert "j" not in cron.last_run


async def test_rehydrate_reconcile_takeover_closes_foreign_orphan(tmp_path):
    # a foreign host's open record is judged purely by fence supersession:
    # the takeover closes it and surfaces a synthetic unknown-outcome run.
    cron = await _rehydrate_state_cron(tmp_path)
    await cron.state_backend.append_record(
        cron._inflight_stream("j"),
        {
            "kind": "open",
            "host": "other-host",
            "proc": "deadbeef",
            "pid": None,
            "startedAt": "2026-07-01T10:00:00+00:00",
        },
    )
    await cron._reconcile_takeover_inflight(cron.cron_jobs["j"])
    assert cron.last_run["j"].outcome == "unknown"
    assert cron._last_completed_at["j"].isoformat() == (
        "2026-07-01T10:00:00+00:00"
    )
    await asyncio.gather(*list(cron._pending_state_writes))


async def test_rehydrate_reconcile_open_record_defaults_missing_started(tmp_path):
    # a record with no startedAt string falls back to "now" for the
    # interruption instant rather than crashing the reconcile.
    cron = await _rehydrate_state_cron(tmp_path)
    await cron.state_backend.append_record(
        cron._inflight_stream("j"),
        {"kind": "open", "host": "other-host", "proc": "deadbeef", "pid": None},
    )
    await cron._reconcile_takeover_inflight(cron.cron_jobs["j"])
    assert cron.last_run["j"].outcome == "unknown"
    await asyncio.gather(*list(cron._pending_state_writes))


async def test_rehydrate_reconcile_open_record_runall_leaves_watermark(tmp_path):
    # under onMissed run-all the interrupted slot is still owed to catch-up,
    # so the synthetic row carries interruptedAt (no finished_at) and the
    # rehydrated info's outcome is still unknown.
    cron = await _rehydrate_state_cron(tmp_path, _RUNALL_REHYDRATE)
    await cron.state_backend.append_record(
        cron._inflight_stream("j"),
        {
            "kind": "open",
            "host": "other-host",
            "proc": "deadbeef",
            "pid": None,
            "startedAt": "2026-07-01T10:00:00+00:00",
        },
    )
    await cron._reconcile_takeover_inflight(cron.cron_jobs["j"])
    assert cron.last_run["j"].outcome == "unknown"
    await asyncio.gather(*list(cron._pending_state_writes))
    (rec,) = await cron.state_backend.list_records(cron._run_stream("j"))
    assert rec.get("finished_at") is None
    assert rec["interruptedAt"] == "2026-07-01T10:00:00+00:00"


async def test_rehydrate_persist_reconciled_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB_REHYDRATE)
    await cron._persist_reconciled_record("j", {"outcome": "unknown"})


async def test_rehydrate_persist_reconciled_degrades_on_error(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.append_record = _raise_oserror5
    await cron._persist_reconciled_record("j", {"outcome": "unknown"})


# --- run record / counter snapshot / archive: no-backend guards -------------


async def test_rehydrate_persist_run_record_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB_REHYDRATE)
    await cron._persist_run_record("j", _mem_run5("success", 0))


async def test_rehydrate_persist_counter_snapshot_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB_REHYDRATE)
    await cron._persist_counter_snapshot()


async def test_rehydrate_persist_counter_snapshot_unseeded(tmp_path):
    # the seed gate: a run finishing before _rehydrate_counters ran must not
    # write a snapshot the seed would then double-ingest.
    cron = await _rehydrate_state_cron(tmp_path)
    cron._counters_seeded = False
    await cron._persist_counter_snapshot()
    assert await cron.state_backend.list_records(cron._counters_stream()) == []


async def test_rehydrate_archive_output_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB_REHYDRATE)
    await cron._archive_output(cron.cron_jobs["j"], _mem_run5("success", 0))


# --- SLA last-success warm scan ---------------------------------------------


async def test_rehydrate_warm_last_success_cancelled(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.list_records = _raise_cancelled5
    with pytest.raises(asyncio.CancelledError):
        await cron._warm_last_success_beyond_history("j", [])


async def test_rehydrate_warm_last_success_error_falls_back_to_oldest(tmp_path):
    # the deeper re-read errors: the reference falls back to the oldest
    # finished_at seen in the warmed history (a lower bound on staleness).
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.list_records = _raise_oserror5
    history = [_mem_run5("failure", 5), _mem_run5("failure", 2)]
    await cron._warm_last_success_beyond_history("j", history)
    assert cron._sla_last_success["j"] == datetime.datetime(
        2026, 7, 1, 10, 2, 0, tzinfo=_UTC5
    )


async def test_rehydrate_warm_last_success_finds_deeper_success(tmp_path):
    # a poison record (no finished_at) is skipped; a real deeper success is
    # taken as the staleness reference.
    cron = await _rehydrate_state_cron(tmp_path)
    await cron.state_backend.append_record(
        cron._run_stream("j"), {"outcome": "success"}
    )
    await cron.state_backend.append_record(
        cron._run_stream("j"),
        {"outcome": "success", "finished_at": "2026-07-01T09:00:00+00:00"},
    )
    await cron._warm_last_success_beyond_history("j", [])
    assert cron._sla_last_success["j"].isoformat() == (
        "2026-07-01T09:00:00+00:00"
    )


# --- rehydrate-from-state degrade branches ----------------------------------


async def test_rehydrate_rehydrate_from_state_timeout_breaks(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron._state_rehydrated = False

    async def _list(stream, **kw):
        if stream.startswith("runs/"):
            raise asyncio.TimeoutError
        return []

    cron.state_backend.list_records = _list
    await cron._rehydrate_from_state()  # a hung store aborts the warm-up
    assert not cron.run_history.get("j")


async def test_rehydrate_rehydrate_from_state_oserror_continues(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron._state_rehydrated = False

    async def _list(stream, **kw):
        if stream.startswith("runs/"):
            raise OSError("boom")
        return []

    cron.state_backend.list_records = _list
    await cron._rehydrate_from_state()  # a store error skips this job
    assert not cron.run_history.get("j")


async def test_rehydrate_rehydrate_from_state_warms_history(tmp_path):
    # the happy path: seeded run records warm run_history and last_run, and
    # the real onlyIfLastSucceeded / last-completed memos are seeded too.
    cron = await _rehydrate_state_cron(tmp_path)
    cron._state_rehydrated = False
    await cron.state_backend.append_record(
        cron._run_stream("j"),
        {
            "outcome": "failure",
            "exit_code": 1,
            "finished_at": "2026-07-01T09:00:00+00:00",
            "ranAt": "2026-07-01T09:00:00+00:00",
        },
    )
    await cron.state_backend.append_record(
        cron._run_stream("j"),
        {
            "outcome": "success",
            "exit_code": 0,
            "finished_at": "2026-07-01T09:05:00+00:00",
            "ranAt": "2026-07-01T09:05:00+00:00",
        },
    )
    await cron._rehydrate_from_state()
    assert len(cron.run_history["j"]) == 2
    assert cron.last_run["j"].outcome == "success"
    assert cron._last_real_outcome["j"][1] == "success"
    assert cron._last_completed_at["j"].isoformat() == (
        "2026-07-01T09:05:00+00:00"
    )


# --- rehydrate counters -----------------------------------------------------


async def test_rehydrate_rehydrate_counters_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB_REHYDRATE)
    await cron._rehydrate_counters()


async def test_rehydrate_rehydrate_counters_cancelled(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron._counters_seeded = False
    cron.state_backend.list_records = _raise_cancelled5
    with pytest.raises(asyncio.CancelledError):
        await cron._rehydrate_counters()


async def test_rehydrate_rehydrate_counters_error_forfeits_seed(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron._counters_seeded = False
    cron.state_backend.list_records = _raise_oserror5
    await cron._rehydrate_counters()  # the seed is forfeited, latch still set
    assert cron._counters_seeded is True


# --- rehydrate retries ------------------------------------------------------


async def test_rehydrate_rehydrate_retries_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB_REHYDRATE)
    await cron._rehydrate_retries()


async def test_rehydrate_rehydrate_retries_skips_live_ladder(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path, _RETRY_REHYDRATE)
    cron.retry_state["j"] = JobRetryState(1.0, 2.0, 10.0)
    await cron._rehydrate_retries()  # live in-memory ladder outranks ledger
    assert cron.retry_state["j"].task is None


async def test_rehydrate_rehydrate_retries_timeout_breaks(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path, _RETRY_REHYDRATE)

    async def _list(stream, **kw):
        if stream.startswith("retries/"):
            raise asyncio.TimeoutError
        return []

    cron.state_backend.list_records = _list
    await cron._rehydrate_retries()  # hung store aborts the re-arm pass
    assert "j" not in cron.retry_state


async def test_rehydrate_rehydrate_retries_cancelled_propagates(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path, _RETRY_REHYDRATE)

    async def _list(stream, **kw):
        if stream.startswith("retries/"):
            raise asyncio.CancelledError
        return []

    cron.state_backend.list_records = _list
    with pytest.raises(asyncio.CancelledError):
        await cron._rehydrate_retries()


async def test_rehydrate_rehydrate_retries_error_continues(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path, _RETRY_REHYDRATE)

    async def _list(stream, **kw):
        if stream.startswith("retries/"):
            raise OSError("boom")
        return []

    cron.state_backend.list_records = _list
    await cron._rehydrate_retries()  # a store error skips the job
    assert "j" not in cron.retry_state


async def test_rehydrate_rehydrate_retries_durable_lookup_error(tmp_path):
    # the superseded-by-run memo seed errors: durable_at stays None (guard
    # left open) and the invalid pending record is then settled, not re-armed.
    cron = await _rehydrate_state_cron(tmp_path, _RETRY_REHYDRATE)
    await _rehydrate_seed_pending_retry(cron, attempt=0)  # invalid -> no re-arm
    cron.durable_last_completed_at = _raise_oserror5
    await cron._rehydrate_retries()
    assert "j" not in cron.retry_state


async def test_rehydrate_rehydrate_retries_durable_lookup_cancelled(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path, _RETRY_REHYDRATE)
    await _rehydrate_seed_pending_retry(cron, attempt=0)
    cron.durable_last_completed_at = _raise_cancelled5
    with pytest.raises(asyncio.CancelledError):
        await cron._rehydrate_retries()


async def test_rehydrate_rehydrate_retries_reboot_marker_error(tmp_path):
    # an @reboot pending whose boot-marker probe errors reads as not-covered:
    # the stale ladder is settled (superseded-by-reboot), never re-armed.
    cron = await _rehydrate_state_cron(tmp_path, _REBOOT_RETRY_REHYDRATE)
    await _rehydrate_seed_pending_retry(cron, attempt=1)
    cron._reboot_marker_covers = _raise_oserror5
    await cron._rehydrate_retries()
    assert "j" not in cron.retry_state


async def test_rehydrate_rehydrate_retries_reboot_marker_cancelled(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path, _REBOOT_RETRY_REHYDRATE)
    await _rehydrate_seed_pending_retry(cron, attempt=1)
    cron._reboot_marker_covers = _raise_cancelled5
    with pytest.raises(asyncio.CancelledError):
        await cron._rehydrate_retries()


# --- _validate_pending_retry verdicts ---------------------------------------


def test_rehydrate_validate_pending_retry_invalid_record(tmp_path):
    cron = cronstable.cron.Cron(None, config_yaml=_RETRY_REHYDRATE)
    job = cron.cron_jobs["j"]
    assert cron._validate_pending_retry("j", job, {"attempt": 0}) is None


def test_rehydrate_validate_pending_retry_config_changed(tmp_path):
    cron = cronstable.cron.Cron(None, config_yaml=_RETRY_REHYDRATE)
    job = cron.cron_jobs["j"]
    rec = {
        "attempt": 1,
        "notBefore": "1999-01-01T00:00:00+00:00",
        "jobDigest": "stale-digest",
    }
    assert cron._validate_pending_retry("j", job, rec) is None


def test_rehydrate_validate_pending_retry_ok(tmp_path):
    cron = cronstable.cron.Cron(None, config_yaml=_RETRY_REHYDRATE)
    job = cron.cron_jobs["j"]
    rec = {
        "attempt": 1,
        "notBefore": "1999-01-01T00:00:00+00:00",
        "jobDigest": _job_digest5(job),
    }
    validated = cron._validate_pending_retry("j", job, rec)
    assert validated is not None
    attempt, not_before = validated
    assert attempt == 1
    assert not_before == datetime.datetime(1999, 1, 1, tzinfo=_UTC5)


# --- depends-on-past cancellation propagation -------------------------------


async def test_rehydrate_depends_on_past_cancelled_propagates(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path, _DEP_REHYDRATE)
    cron.state_backend.list_records = _raise_cancelled5
    with pytest.raises(asyncio.CancelledError):
        await cron._depends_on_past_ok(cron.cron_jobs["j"])


# --- completion sequencing --------------------------------------------------


async def test_rehydrate_queue_completion_chains_behind_prev():
    # the second completion for one job waits on the first: the serial
    # per-job retry-arm ordering the reaper used to give inline.
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB_REHYDRATE)
    job = _FakeRun5(cron.cron_jobs["j"])
    gate = asyncio.Event()
    calls = []

    async def _slow(j):
        calls.append("slow-start")
        await gate.wait()
        calls.append("slow-end")

    async def _fast(j):
        calls.append("fast")

    cron.handle_job_success = _slow
    cron._queue_job_completion(job, failed=False)
    for _ in range(5):
        await asyncio.sleep(0)
    cron.handle_job_success = _fast
    cron._queue_job_completion(job, failed=False)  # chains behind the slow one
    for _ in range(5):
        await asyncio.sleep(0)
    assert calls == ["slow-start"]  # the fast one is blocked on its prev
    gate.set()
    await cron._drain_completions()
    assert calls == ["slow-start", "slow-end", "fast"]


async def test_rehydrate_queue_completion_reraises_cancelled():
    # a cancellation inside the sequenced handler propagates (it is not
    # swallowed by the defensive except), ending the task cancelled.
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB_REHYDRATE)
    job = _FakeRun5(cron.cron_jobs["j"])
    cron.handle_job_failure = _raise_cancelled5
    cron._queue_job_completion(job, failed=True)
    await cron._drain_completions()
    assert cron._completion_tasks == set()


# --- finished DAG task reaping ----------------------------------------------


async def test_rehydrate_handle_finished_dag_task_survives_dag_error():
    # a DAG scheduler error while recording a task completion is logged, never
    # allowed to kill the reaper; the task is still removed from running_jobs.
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB_REHYDRATE)
    rj = _FakeRun5(cron.cron_jobs["j"], state_token=None)
    cron.running_jobs["j"].append(rj)

    async def _boom(job):
        raise RuntimeError("dag exploded")

    cron._dag.on_task_finished = _boom
    await cron._handle_finished_dag_task(rj)  # no raise
    assert "j" not in cron.running_jobs


# --- handle_job_failure: stderr log + live retry-task cancel ----------------


async def test_rehydrate_handle_job_failure_logs_stderr_and_cancels_task():
    # a failing run with captured stderr logs it, then an armed-but-live
    # retry task is cancelled before the exhausted ladder is settled.
    cron = cronstable.cron.Cron(None, config_yaml=_RETRY_REHYDRATE)

    async def _sleeper():
        await asyncio.sleep(100)

    state = JobRetryState(0.1, 2.0, 1.0)
    state.count = 5  # already past maximumRetries (2): exhausted branch
    state.task = asyncio.create_task(_sleeper())

    class _FailJob5:
        def __init__(self, config, retry_state):
            self.config = config
            self.retry_state = retry_state
            self.stdout = ""
            self.stderr = "an error happened"

        async def report_failure(self):
            return None

        async def report_permanent_failure(self):
            return None

    job = _FailJob5(cron.cron_jobs["j"], state)
    await cron.handle_job_failure(job)
    # the live task was cancelled; let the cancellation settle, then confirm.
    try:
        await state.task
    except asyncio.CancelledError:
        pass
    assert state.task.cancelled()




# ===================================================================
# Job start/pause/resume, SLA, and the cross-node retry claim machinery
# (start_job_by_name, pause/resume, pause-store refresh, SLA banking/
# observations/report, and the cross-node retry claim/consume machinery)
# ===================================================================

_RETRYCLAIM_RETRY_JOB = """
jobs:
  - name: j
    command: ls
    schedule: "0 0 * * *"
    onFailure:
      retry:
        maximumRetries: 3
        initialDelay: 1
        maximumDelay: 60
        backoffMultiplier: 2
"""

_RETRYCLAIM_RETRY_JOB_DEADLINE = """
jobs:
  - name: j
    command: ls
    schedule: "0 0 * * *"
    startingDeadlineSeconds: 60
    onFailure:
      retry:
        maximumRetries: 3
        initialDelay: 1
        maximumDelay: 60
        backoffMultiplier: 2
"""

_RETRYCLAIM_RETRY_JOB_NO_RETRY = """
jobs:
  - name: j
    command: ls
    schedule: "0 0 * * *"
"""


async def _retryclaim_stateful(tmp_path, yaml, extra=""):
    from tests.test_state import _state_cfg

    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    cfg = _state_cfg("state:\n  path: {}\n{}".format(tmp_path, extra))
    await cron.start_stop_state(cfg)
    assert cron.state_backend is not None
    return cron


async def _retryclaim_stop(cron):
    from tests.test_state import _drain_state_writes

    await _drain_state_writes(cron)
    if cron.state_backend is not None:
        await cron.state_backend.stop()
        cron.state_backend = None


def _retryclaim_foreign(cron, job, host="node-a", secs_stale=120):
    from cronstable.fingerprint import job_digest

    now = cronstable.cron.get_now(datetime.timezone.utc)
    stale = now - datetime.timedelta(seconds=secs_stale)
    return {
        "kind": "pending",
        "attempt": 1,
        "notBefore": stale.isoformat(),
        "jobDigest": job_digest(job),
        "host": host,
        "at": stale.isoformat(),
    }


# --- start_job_by_name ----------------------------------------------------


@pytest.mark.asyncio
async def test_retryclaim_start_job_unknown_raises_404():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    with pytest.raises(cronstable.cron.ApiActionError) as ei:
        await cron.start_job_by_name("ghost")
    assert ei.value.status == 404


@pytest.mark.asyncio
async def test_retryclaim_start_job_counts_as_pause_deferred_boot_run(monkeypatch):
    # a manual start of a job whose boot run a pause deferred IS the boot run:
    # the paused-reboot entry is retired and the durable boot marker written.
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron._paused_reboot_jobs.add("alpha")
    cron._state_configured = True
    gated = []

    async def _gate(job):
        gated.append(job.name)
        return True

    monkeypatch.setattr(cron, "_reboot_boot_gate", _gate)
    launched = []
    monkeypatch.setattr(
        cron, "maybe_launch_job", lambda j: launched.append(j.name) or _noop()
    )
    await cron.start_job_by_name("alpha")
    assert "alpha" not in cron._paused_reboot_jobs
    assert gated == ["alpha"]
    assert launched == ["alpha"]


# --- pause_job_by_name / _refresh_pauses_from_store / _pause_info ----------


@pytest.mark.asyncio
async def test_retryclaim_pause_job_naive_until_gets_utc():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    naive = DT(1999, 12, 31, 13, 0, 0)  # naive, one hour past the frozen now
    await cron.pause_job_by_name("alpha", until=naive)
    got = cron._paused["alpha"].until
    assert got.tzinfo is not None
    assert got == DT(1999, 12, 31, 13, 0, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_retryclaim_refresh_pauses_no_backend_returns():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    assert cron.state_backend is None
    # returns immediately with no backend (no raise)
    await cron._refresh_pauses_from_store()


def test_retryclaim_pause_info_from_record_variants():
    f = cronstable.cron.Cron._pause_info_from_record
    assert f(None) is None
    assert f({"kind": "resumed"}) is None
    # a paused record with an unparseable `until` reads as not paused
    assert f({"kind": "paused", "until": "not-a-date"}) is None
    info = f(
        {
            "kind": "paused",
            "until": "1999-12-31T13:00:00+00:00",
            "since": None,
            "note": 5,
            "by": None,
            "channel": 7,
        }
    )
    assert info is not None
    # non-string audit fields normalise to ""; a missing since defaults to until
    assert info.note == "" and info.by == "" and info.channel == ""
    assert info.since == info.until


@pytest.mark.asyncio
async def test_retryclaim_refresh_pauses_from_store_skip_removed_and_replace(
    tmp_path,
):
    cron = await _retryclaim_stateful(tmp_path, TWO_JOBS)
    try:
        now = cronstable.cron.get_now(datetime.timezone.utc)
        until1 = now + datetime.timedelta(hours=1)
        until2 = now + datetime.timedelta(hours=2)
        # a stream for a job not in the config: the sweep skips it entirely
        await cron.state_backend.append_record(
            "paused/ghost",
            {
                "kind": "paused",
                "since": now.isoformat(),
                "until": until1.isoformat(),
                "note": "",
                "by": "",
                "channel": "",
                "at": now.isoformat(),
                "host": "h",
            },
        )
        # alpha already paused in memory with a DIFFERENT window: the store's
        # newer window replaces it and banks the one it superseded.
        cron._paused["alpha"] = cronstable.cron.PauseInfo(
            since=now - datetime.timedelta(minutes=30),
            until=until1,
            note="",
            by="",
            channel="",
        )
        await cron.state_backend.append_record(
            "paused/alpha",
            {
                "kind": "paused",
                "since": now.isoformat(),
                "until": until2.isoformat(),
                "note": "",
                "by": "",
                "channel": "",
                "at": now.isoformat(),
                "host": "h",
            },
        )
        await cron._refresh_pauses_from_store()
        assert "ghost" not in cron._paused  # removed-job stream skipped
        assert cron._paused["alpha"].until == until2  # window replaced
    finally:
        await _retryclaim_stop(cron)


# --- SLA banking / observations / report ----------------------------------


def test_retryclaim_sla_bank_pause_clamps_ended_at_to_until():
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    now = cronstable.cron.get_now(datetime.timezone.utc)
    cron._sla_last_success["s"] = now  # pin the staleness reference at `now`
    was = cronstable.cron.PauseInfo(
        since=now,
        until=now + datetime.timedelta(hours=1),
        note="",
        by="",
        channel="",
    )
    # ended_at AFTER `until`: it is clamped down to `until` before banking
    cron._sla_bank_pause("s", was, now + datetime.timedelta(hours=2))
    spans = cron._sla_pause_windows.get("s")
    assert spans is not None
    assert spans[-1][1] == now + datetime.timedelta(hours=1)


def test_retryclaim_sla_observations_skips_runjob_without_started_at():
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_RUNTIME_JOB)
    now = cronstable.cron.get_now(datetime.timezone.utc)

    class _R:
        started_at = None

    cron.running_jobs["s"] = [_R()]
    obs = cron._sla_observations("s", cron.cron_jobs["s"], now)
    threshold, observed, breached = obs[RUNTIME]
    assert observed == 0.0 and breached is False


@pytest.mark.asyncio
async def test_retryclaim_queue_sla_report_waits_for_earlier_tail(monkeypatch):
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    reports = _sla_report_recorder(monkeypatch)
    gate = asyncio.Event()

    async def _blocked():
        await gate.wait()

    prev = asyncio.create_task(_blocked())
    cron._completion_tail["s"] = prev  # an in-flight completion report
    cron._queue_sla_report(cron.cron_jobs["s"], STALE, 3600, 4000.0)
    task = cron._sla_report_tail["s"]
    await asyncio.sleep(0)
    assert not task.done()  # ordered behind the earlier tail
    assert reports == []
    gate.set()
    await asyncio.wait_for(task, timeout=5)
    assert len(reports) == 1
    prev.cancel()


@pytest.mark.asyncio
async def test_retryclaim_queue_sla_report_reraises_cancelled(monkeypatch):
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)

    async def _cancel(ctx, cfg):
        raise asyncio.CancelledError()

    monkeypatch.setattr(cronstable.cron, "report_sla_breach", _cancel)
    cron._queue_sla_report(cron.cron_jobs["s"], STALE, 3600, 4000.0)
    task = cron._sla_report_tail["s"]
    with pytest.raises(asyncio.CancelledError):
        await task


# --- web resume validation ------------------------------------------------


@pytest.mark.asyncio
async def test_retryclaim_web_resume_job_rejects_nonstring_by():
    from aiohttp import web

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    class Req:
        can_read_body = True
        match_info = {"name": "alpha"}

        async def json(self):
            return {"by": 123}

    with pytest.raises(web.HTTPBadRequest):
        await cron._web_resume_job(Req())


# --- schedule_retry_job gate/pause returns --------------------------------


@pytest.mark.asyncio
async def test_retryclaim_schedule_retry_paused_returns_when_state_gone():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    now = cronstable.cron.get_now(datetime.timezone.utc)
    cron._paused["alpha"] = cronstable.cron.PauseInfo(
        since=now,
        until=now + datetime.timedelta(hours=1),
        note="",
        by="",
        channel="",
    )
    # no retry_state entry: the paused branch sees state None and returns
    await cron.schedule_retry_job("alpha", 0.0, 1)
    assert "alpha" not in cron.retry_state


@pytest.mark.asyncio
async def test_retryclaim_schedule_retry_transient_gate_returns_when_cancelled(
    monkeypatch,
):
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron._elect_leader_configured = True
    cron.cluster_manager = None  # Leader fails closed; no positive owner
    state = JobRetryState(0.01, 1, 0.01)
    state.cancelled = True
    cron.retry_state["alpha"] = state
    launched = []
    monkeypatch.setattr(
        cron, "maybe_launch_job", lambda j: launched.append(j.name) or _noop()
    )
    await cron.schedule_retry_job("alpha", 0.0, 1)
    assert launched == []  # returned at the cancelled-state guard, no launch


# --- retry write plumbing -------------------------------------------------


def test_retryclaim_note_retry_write_dropped_warns_when_state_configured(caplog):
    import logging

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron._state_configured = True
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        cron._note_retry_write_dropped("alpha", "pending")
    assert any(
        "dropping retry-ladder record" in r.getMessage() for r in caplog.records
    )


@pytest.mark.asyncio
async def test_retryclaim_queue_retry_write_orders_behind_prev_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    gate = asyncio.Event()

    async def _blocked():
        await gate.wait()

    prev = asyncio.create_task(_blocked())
    cron._retry_write_tail["alpha"] = prev
    task = cron._queue_retry_write("alpha", {"kind": "settled"})
    await asyncio.sleep(0)
    assert not task.done()  # ordered behind the in-flight previous write
    gate.set()
    # the append runs with no backend: it notes the drop and returns cleanly
    await asyncio.wait_for(task, timeout=5)


@pytest.mark.asyncio
async def test_retryclaim_append_retry_record_survives_backend_error(
    tmp_path, caplog
):
    import logging

    cron = await _retryclaim_stateful(tmp_path, TWO_JOBS)
    try:

        async def _boom(*a, **k):
            raise OSError("disk gone")

        cron.state_backend.append_record = _boom
        with caplog.at_level(logging.WARNING, logger="cronstable"):
            await cron._append_retry_record("alpha", {"kind": "settled"})
        assert any(
            "failed to persist retry state" in r.getMessage()
            for r in caplog.records
        )
    finally:
        await _retryclaim_stop(cron)


@pytest.mark.asyncio
async def test_retryclaim_append_pause_record_defers_without_backend():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron._state_configured = True  # a store is configured but torn down
    assert cron.state_backend is None
    await cron._append_pause_record(
        "alpha", {"kind": "paused", "until": "1999-12-31T13:00:00+00:00"}
    )
    assert "alpha" in cron._pause_pending_writes


# --- _retry_consume_ok ----------------------------------------------------


@pytest.mark.asyncio
async def test_retryclaim_retry_consume_ok_tolerates_slow_prev_tail(monkeypatch):
    import types

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    appended = []

    async def _append(stream, record, **k):
        appended.append(record)

    cron.state_backend = types.SimpleNamespace(append_record=_append)
    slow = asyncio.create_task(asyncio.sleep(10))
    cron._retry_write_tail["alpha"] = slow
    monkeypatch.setattr(cronstable.cron, "STATE_OP_TIMEOUT", 0.02)
    ok = await cron._retry_consume_ok("alpha", 1, quiet=True)
    assert ok is True  # the settle wrote once the prev-wait timed out
    assert appended and appended[0]["reason"] == "launched"
    slow.cancel()


@pytest.mark.asyncio
async def test_retryclaim_retry_consume_ok_reraises_cancelled():
    import types

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)

    async def _append(stream, record, **k):
        raise asyncio.CancelledError()

    cron.state_backend = types.SimpleNamespace(append_record=_append)
    with pytest.raises(asyncio.CancelledError):
        await cron._retry_consume_ok("alpha", 1, quiet=True)


# --- _acquire_retry_claim -------------------------------------------------


@pytest.mark.asyncio
async def test_retryclaim_acquire_retry_claim_timeout_returns_none(monkeypatch):
    import types

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)

    async def _slow(*a, **k):
        await asyncio.sleep(10)

    backend = types.SimpleNamespace(acquire_lease=_slow)
    monkeypatch.setattr(cronstable.cron, "STATE_OP_TIMEOUT", 0.02)
    got = await cron._acquire_retry_claim(
        backend, cron.cron_jobs["j"], 1, quiet=True
    )
    assert got is None


@pytest.mark.asyncio
async def test_retryclaim_acquire_retry_claim_error_returns_none(caplog):
    import logging
    import types

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)

    async def _boom(*a, **k):
        raise OSError("no locks")

    backend = types.SimpleNamespace(acquire_lease=_boom)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        got = await cron._acquire_retry_claim(
            backend, cron.cron_jobs["j"], 1, quiet=False
        )
    assert got is None
    assert any(
        "retry-claim store call raised" in r.getMessage()
        for r in caplog.records
    )


# --- _retry_consume_decision (cross-node) ---------------------------------


@pytest.mark.asyncio
async def test_retryclaim_consume_decision_eligible_but_no_backend(monkeypatch):
    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)
    assert cron.state_backend is None
    decision = await cron._retry_consume_decision(
        cron.cron_jobs["j"], 1, quiet=True
    )
    assert decision == "launch"  # degrades to the classic consume_ok


@pytest.mark.asyncio
async def test_retryclaim_consume_decision_aborts_on_foreign_record(monkeypatch):
    import types

    from cronstable.state import Lease

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)
    released = []
    lease = Lease(
        name="l", holder=cron._slot_holder(), fence=1, expires_at=9e18
    )

    async def _acq(*a, **k):
        return lease

    async def _list(*a, **k):
        return [{"host": "another-node", "kind": "pending"}]

    async def _rel(lz):
        released.append(lz)
        raise OSError("release failed")  # swallowed by the finally guard

    cron.state_backend = types.SimpleNamespace(
        acquire_lease=_acq, list_records=_list, release_lease=_rel
    )
    decision = await cron._retry_consume_decision(
        cron.cron_jobs["j"], 1, quiet=True
    )
    assert decision == "abort"  # a foreign newest record moved the ladder
    assert released == [lease]


@pytest.mark.asyncio
async def test_retryclaim_consume_decision_defers_for_live_claimer(monkeypatch):
    import types

    from cronstable.state import Lease

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)

    async def _acq_none(*a, **k):
        return None

    async def _read(name):
        return Lease(name=name, holder="rival#1", fence=1, expires_at=9e18)

    cron.state_backend = types.SimpleNamespace(
        acquire_lease=_acq_none, read_lease=_read
    )
    decision = await cron._retry_consume_decision(
        cron.cron_jobs["j"], 1, quiet=True
    )
    assert decision == "defer"  # a live claimer holds the lease


@pytest.mark.asyncio
async def test_retryclaim_consume_decision_read_timeout_fail_closed_defers(
    monkeypatch,
):
    import types

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    cron._state_on_unavailable = "fail-closed"
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)

    async def _acq_none(*a, **k):
        return None

    async def _read_timeout(name):
        raise asyncio.TimeoutError()

    cron.state_backend = types.SimpleNamespace(
        acquire_lease=_acq_none, read_lease=_read_timeout
    )
    decision = await cron._retry_consume_decision(
        cron.cron_jobs["j"], 1, quiet=True
    )
    assert decision == "defer"  # cannot serialize + fail-closed


@pytest.mark.asyncio
async def test_retryclaim_consume_decision_read_cancelled_propagates(monkeypatch):
    import types

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)

    async def _acq_none(*a, **k):
        return None

    async def _read_cancel(name):
        raise asyncio.CancelledError()

    cron.state_backend = types.SimpleNamespace(
        acquire_lease=_acq_none, read_lease=_read_cancel
    )
    with pytest.raises(asyncio.CancelledError):
        await cron._retry_consume_decision(cron.cron_jobs["j"], 1, quiet=True)


@pytest.mark.asyncio
async def test_retryclaim_consume_decision_adopts_late_lease_and_launches(
    monkeypatch,
):
    import types

    from cronstable.state import Lease

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)
    released = []
    own = Lease(
        name="l", holder=cron._slot_holder(), fence=1, expires_at=9e18
    )

    async def _acq_none(*a, **k):
        return None

    async def _read(name):
        return own  # our own late-landing acquire, observed on read-back

    async def _list_boom(*a, **k):
        raise OSError("read fail")  # degrade -> recs=[]

    async def _append(stream, record, **k):
        pass

    async def _rel(lz):
        released.append(lz)

    cron.state_backend = types.SimpleNamespace(
        acquire_lease=_acq_none,
        read_lease=_read,
        list_records=_list_boom,
        release_lease=_rel,
        append_record=_append,
    )
    decision = await cron._retry_consume_decision(
        cron.cron_jobs["j"], 1, quiet=True
    )
    assert decision == "launch"
    assert released == [own]  # the adopted lease is released


@pytest.mark.asyncio
async def test_retryclaim_consume_decision_list_error_fail_closed_defers(
    monkeypatch,
):
    import types

    from cronstable.state import Lease

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    cron._state_on_unavailable = "fail-closed"
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)
    lease = Lease(
        name="l", holder=cron._slot_holder(), fence=1, expires_at=9e18
    )

    async def _acq(*a, **k):
        return lease

    async def _list_boom(*a, **k):
        raise OSError("read fail")

    async def _rel(lz):
        pass

    cron.state_backend = types.SimpleNamespace(
        acquire_lease=_acq, list_records=_list_boom, release_lease=_rel
    )
    decision = await cron._retry_consume_decision(
        cron.cron_jobs["j"], 1, quiet=True
    )
    assert decision == "defer"  # unreadable ladder + fail-closed


# --- _retry_claim_scan ----------------------------------------------------


@pytest.mark.asyncio
async def test_retryclaim_retry_claim_scan_inactive_returns():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    # cross-node resume inactive (no backend) -> returns without scanning
    await cron._retry_claim_scan()


@pytest.mark.asyncio
async def test_retryclaim_retry_claim_scan_logs_and_continues_on_error(
    monkeypatch, caplog
):
    import logging

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    monkeypatch.setattr(cron, "_retry_resume_active", lambda: True)

    async def _boom(name, job):
        raise RuntimeError("scan bug")

    monkeypatch.setattr(cron, "_maybe_claim_retry", _boom)
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await cron._retry_claim_scan()
    assert any("scanning job" in r.getMessage() for r in caplog.records)


# --- _maybe_claim_retry ---------------------------------------------------


@pytest.mark.asyncio
async def test_retryclaim_maybe_claim_retry_guards(monkeypatch):
    import types

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    job = cron.cron_jobs["j"]
    # no backend -> returns
    await cron._maybe_claim_retry("j", job)

    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)
    cron.state_backend = types.SimpleNamespace()  # non-None sentinel
    # a running instance outranks a claim
    cron.running_jobs["j"] = ["run"]
    await cron._maybe_claim_retry("j", job)
    cron.running_jobs["j"] = []
    # a live local ladder (count > 0) outranks
    st = JobRetryState(1, 2, 60)
    st.next_delay()
    cron.retry_state["j"] = st
    await cron._maybe_claim_retry("j", job)
    cron.retry_state.pop("j")
    # the cluster does not currently allow this node to run it
    monkeypatch.setattr(cron, "_cluster_allows", lambda job: False)
    await cron._maybe_claim_retry("j", job)
    assert "j" not in cron.retry_state


@pytest.mark.asyncio
async def test_retryclaim_maybe_claim_retry_disabled_or_no_retries(monkeypatch):
    import types

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB_NO_RETRY)
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)
    cron.state_backend = types.SimpleNamespace()
    # maximumRetries defaults to 0 for a job with no onFailure.retry block
    await cron._maybe_claim_retry("j", cron.cron_jobs["j"])
    assert "j" not in cron.retry_state


@pytest.mark.asyncio
async def test_retryclaim_maybe_claim_retry_list_error_returns(monkeypatch):
    import types

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)
    monkeypatch.setattr(cron, "_cluster_allows", lambda job: True)

    async def _list_boom(*a, **k):
        raise OSError("read fail")

    cron.state_backend = types.SimpleNamespace(list_records=_list_boom)
    await cron._maybe_claim_retry("j", cron.cron_jobs["j"])
    assert "j" not in cron.retry_state


@pytest.mark.asyncio
async def test_retryclaim_maybe_claim_retry_acquire_timeout_returns(monkeypatch):
    import types

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)
    monkeypatch.setattr(cron, "_cluster_allows", lambda job: True)
    cron._state_host = "node-b"
    job = cron.cron_jobs["j"]
    foreign = _retryclaim_foreign(cron, job)

    async def _list_ok(*a, **k):
        return [foreign]

    async def _acq_slow(*a, **k):
        await asyncio.sleep(10)

    cron.state_backend = types.SimpleNamespace(
        list_records=_list_ok, acquire_lease=_acq_slow
    )
    monkeypatch.setattr(cronstable.cron, "STATE_OP_TIMEOUT", 0.02)
    await cron._maybe_claim_retry("j", job)  # acquire times out -> no claim
    assert "j" not in cron.retry_state


@pytest.mark.asyncio
async def test_retryclaim_maybe_claim_retry_release_error_swallowed(monkeypatch):
    import types

    from cronstable.state import Lease

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)
    monkeypatch.setattr(cron, "_cluster_allows", lambda job: True)
    cron._state_host = "node-b"
    job = cron.cron_jobs["j"]
    foreign = _retryclaim_foreign(cron, job)

    async def _list_ok(*a, **k):
        return [foreign]

    async def _acq(*a, **k):
        return Lease(name="l", holder="x", fence=1, expires_at=9e18)

    async def _rel_boom(lz):
        raise OSError("release failed")

    async def _claim_false(*a, **k):
        return False

    cron.state_backend = types.SimpleNamespace(
        list_records=_list_ok, acquire_lease=_acq, release_lease=_rel_boom
    )
    monkeypatch.setattr(cron, "_claim_retry_under_lease", _claim_false)
    await cron._maybe_claim_retry("j", job)  # release error is swallowed
    assert "j" not in cron.retry_state


@pytest.mark.asyncio
async def test_retryclaim_maybe_claim_retry_claims_and_arms(tmp_path, monkeypatch):
    import types

    from cronstable.fingerprint import job_digest

    cron = await _retryclaim_stateful(
        tmp_path, _RETRYCLAIM_RETRY_JOB, extra="  topology: shared\n"
    )
    try:
        cron._elect_leader_configured = True
        cron.cluster_manager = types.SimpleNamespace(
            distribution="single-leader",
            is_leader=lambda: True,
            is_quorate=lambda: True,
            has_conflict=lambda: False,
            view_settled=lambda: True,
            is_available_leader=lambda: True,
        )
        assert cron._retry_resume_active() is True
        cron._state_host = "node-b"
        job = cron.cron_jobs["j"]
        foreign = _retryclaim_foreign(cron, job, host="node-a")
        await cron.state_backend.append_record("retries/j", foreign)
        armed = []

        async def _fake_sched(name, delay, attempt):
            armed.append((name, delay, attempt))

        monkeypatch.setattr(cron, "schedule_retry_job", _fake_sched)
        await cron._maybe_claim_retry("j", job)
        assert "j" in cron.retry_state  # claimed and armed a local ladder
        # the ladder arms via asyncio.create_task; let it run once so the
        # scheduling call lands before we assert on it.
        await cron.retry_state["j"].task
        assert armed and armed[0][0] == "j" and armed[0][2] == 1
        from tests.test_state import _drain_state_writes

        await _drain_state_writes(cron)
        recs = await cron.state_backend.list_records(
            "retries/j", limit=1, newest_first=True
        )
        assert recs[0]["host"] == "node-b"
        assert recs[0]["claimedFrom"] == "node-a"
        assert recs[0]["jobDigest"] == job_digest(job)
    finally:
        await _retryclaim_stop(cron)


# --- _claim_retry_under_lease ---------------------------------------------


@pytest.mark.asyncio
async def test_retryclaim_claim_under_lease_no_backend_false():
    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    now = cronstable.cron.get_now(datetime.timezone.utc)
    ok = await cron._claim_retry_under_lease(
        "j", cron.cron_jobs["j"], {}, 1, now
    )
    assert ok is False


@pytest.mark.asyncio
async def test_retryclaim_claim_under_lease_recheck_mismatch_false(tmp_path):
    cron = await _retryclaim_stateful(tmp_path, _RETRYCLAIM_RETRY_JOB)
    try:
        cron._state_host = "node-b"
        job = cron.cron_jobs["j"]
        foreign = _retryclaim_foreign(cron, job, host="node-a")
        await cron.state_backend.append_record("retries/j", foreign)
        now = cronstable.cron.get_now(datetime.timezone.utc)
        # the record we "saw" differs from what is now newest -> declined
        stale_view = dict(foreign, attempt=2)
        ok = await cron._claim_retry_under_lease(
            "j", job, stale_view, 2, now
        )
        assert ok is False
    finally:
        await _retryclaim_stop(cron)


@pytest.mark.asyncio
async def test_retryclaim_claim_under_lease_list_error_false(tmp_path, monkeypatch):
    cron = await _retryclaim_stateful(tmp_path, _RETRYCLAIM_RETRY_JOB)
    try:
        job = cron.cron_jobs["j"]
        now = cronstable.cron.get_now(datetime.timezone.utc)

        async def _boom(*a, **k):
            raise OSError("read fail")

        cron.state_backend.list_records = _boom
        ok = await cron._claim_retry_under_lease("j", job, {}, 1, now)
        assert ok is False
    finally:
        await _retryclaim_stop(cron)


@pytest.mark.asyncio
async def test_retryclaim_claim_under_lease_durable_read_error_false(
    tmp_path, monkeypatch
):
    cron = await _retryclaim_stateful(tmp_path, _RETRYCLAIM_RETRY_JOB)
    try:
        cron._state_host = "node-b"
        job = cron.cron_jobs["j"]
        foreign = _retryclaim_foreign(cron, job, host="node-a")
        await cron.state_backend.append_record("retries/j", foreign)
        now = cronstable.cron.get_now(datetime.timezone.utc)

        async def _boom(name):
            raise OSError("ledger read fail")

        monkeypatch.setattr(cron, "durable_last_completed_at", _boom)
        ok = await cron._claim_retry_under_lease(
            "j", job, foreign, 1, foreign_notbefore(foreign)
        )
        assert ok is False
    finally:
        await _retryclaim_stop(cron)


@pytest.mark.asyncio
async def test_retryclaim_claim_under_lease_superseded_by_run(tmp_path, monkeypatch):
    cron = await _retryclaim_stateful(tmp_path, _RETRYCLAIM_RETRY_JOB)
    try:
        cron._state_host = "node-b"
        job = cron.cron_jobs["j"]
        foreign = _retryclaim_foreign(cron, job, host="node-a")
        await cron.state_backend.append_record("retries/j", foreign)
        now = cronstable.cron.get_now(datetime.timezone.utc)
        later = (now + datetime.timedelta(minutes=1)).isoformat()

        async def _durable(name):
            return later  # a run finished AFTER the ladder was armed

        monkeypatch.setattr(cron, "durable_last_completed_at", _durable)
        ok = await cron._claim_retry_under_lease(
            "j", job, foreign, 1, foreign_notbefore(foreign)
        )
        assert ok is False
        from tests.test_state import _drain_state_writes

        await _drain_state_writes(cron)
        recs = await cron.state_backend.list_records(
            "retries/j", limit=1, newest_first=True
        )
        assert recs[0]["kind"] == "settled"
        assert recs[0]["reason"] == "superseded-by-run"
    finally:
        await _retryclaim_stop(cron)


def foreign_notbefore(rec):
    return datetime.datetime.fromisoformat(rec["notBefore"])


# --- _retry_record_claimable ----------------------------------------------


def test_retryclaim_retry_record_claimable_variants():
    from cronstable.fingerprint import job_digest

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    job = cron.cron_jobs["j"]
    f = cron._retry_record_claimable
    now = cronstable.cron.get_now(datetime.timezone.utc)
    stale = (now - datetime.timedelta(seconds=120)).isoformat()
    dig = job_digest(job)
    # not a pending/handoff record
    assert f("j", job, {"kind": "settled"}) is None
    # a bool attempt (bool is an int subclass) is rejected
    assert (
        f("j", job, {"kind": "pending", "attempt": True, "notBefore": stale})
        is None
    )
    # attempt beyond maximumRetries
    assert (
        f(
            "j",
            job,
            {
                "kind": "pending",
                "attempt": 99,
                "notBefore": stale,
                "jobDigest": dig,
                "host": "node-a",
            },
        )
        is None
    )
    # our own pending is rehydration's business, not the scan's
    assert (
        f(
            "j",
            job,
            {
                "kind": "pending",
                "attempt": 1,
                "notBefore": stale,
                "jobDigest": dig,
                "host": cron._state_host,
            },
        )
        is None
    )
    # a foreign pending still within the staleness grace: too fresh to claim
    fresh = (now - datetime.timedelta(seconds=1)).isoformat()
    assert (
        f(
            "j",
            job,
            {
                "kind": "pending",
                "attempt": 1,
                "notBefore": fresh,
                "jobDigest": dig,
                "host": "node-a",
                "at": fresh,
            },
        )
        is None
    )
    # a handoff record is immediately claimable (no grace)
    claim = f(
        "j",
        job,
        {
            "kind": "handoff",
            "attempt": 2,
            "notBefore": stale,
            "jobDigest": dig,
            "fromHost": "node-a",
            "at": stale,
        },
    )
    assert claim is not None and claim[0] == 2


def test_retryclaim_retry_record_claimable_deadline_exceeded():
    from cronstable.fingerprint import job_digest

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB_DEADLINE)
    job = cron.cron_jobs["j"]
    now = cronstable.cron.get_now(datetime.timezone.utc)
    # notBefore is older than startingDeadlineSeconds (60): past its deadline
    old = (now - datetime.timedelta(seconds=600)).isoformat()
    rec = {
        "kind": "pending",
        "attempt": 1,
        "notBefore": old,
        "jobDigest": job_digest(job),
        "host": "node-a",
        "at": old,
    }
    assert cron._retry_record_claimable("j", job, rec) is None


# --- _cluster_owner_moved -------------------------------------------------


def test_retryclaim_cluster_owner_moved_variants():
    import types

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    job = cron.cron_jobs["j"]
    # a nodeName conflict: nobody positively owns it -> transient, not a move
    cron.cluster_manager = types.SimpleNamespace(
        has_conflict=lambda: True,
        is_quorate=lambda: True,
        view_settled=lambda: True,
        distribution="single-leader",
        is_available_leader=lambda: False,
    )
    assert cron._cluster_owner_moved(job) is False
    # spread distribution consults the per-job availability owner
    cron.cluster_manager = types.SimpleNamespace(
        has_conflict=lambda: False,
        is_quorate=lambda: True,
        view_settled=lambda: True,
        distribution="spread",
        is_available_job_owner=lambda n: False,
    )
    assert cron._cluster_owner_moved(job) is True
    # a raising manager is a transient fail-closed condition, never a move
    def _boom():
        raise RuntimeError("mgr bug")

    cron.cluster_manager = types.SimpleNamespace(
        has_conflict=_boom,
        is_quorate=lambda: True,
        view_settled=lambda: True,
        distribution="single-leader",
        is_available_leader=lambda: False,
    )
    assert cron._cluster_owner_moved(job) is False


# --- _reap_retry_task -----------------------------------------------------


@pytest.mark.asyncio
async def test_retryclaim_reap_retry_task_ignores_cancelled():
    async def _forever():
        await asyncio.sleep(100)

    task = asyncio.create_task(_forever())
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    # a cancelled retry task is retrieved without logging or re-raising
    cronstable.cron.Cron._reap_retry_task("j", task)


@pytest.mark.asyncio
async def test_retryclaim_reap_retry_task_logs_exception(caplog):
    import logging

    async def _die():
        raise RuntimeError("retry boom")

    task = asyncio.create_task(_die())
    await asyncio.wait({task})
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        cronstable.cron.Cron._reap_retry_task("j", task)
    assert any("retry task died" in r.getMessage() for r in caplog.records)
