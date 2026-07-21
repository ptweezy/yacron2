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
        pass

    resp = await cron._web_list_jobs(Req())
    data = json.loads(resp.text)
    assert [j["name"] for j in data] == ["alpha", "beta"]

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
        pass

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
        pass

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
        lambda runner, url: FakeSite(url),
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
