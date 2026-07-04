import asyncio
import datetime
import os
import signal
import time
from collections import OrderedDict
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import yacron2.cron
from tests._commands import cmd_hang, cmd_print, cmd_sleep, yaml_command
from yacron2 import platform
from yacron2.config import ConfigError, JobConfig, parse_config_string
from yacron2.job import JobOutputStream, JobRetryState, RunningJob


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

    monkeypatch.setattr("yacron2.cron.get_now", get_now)


@pytest.fixture()
def tracing_running_job(monkeypatch):
    TracingRunningJob._TRACE = asyncio.Queue()
    monkeypatch.setattr(yacron2.cron, "RunningJob", TracingRunningJob)
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
    cron = yacron2.cron.Cron(None, config_yaml=config_yaml)

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
    cron = yacron2.cron.Cron(None, config_yaml=RETRYING_JOB_THAT_FAILS)

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
    cron = yacron2.cron.Cron(None, config_yaml=JOB_THAT_HANGS)

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
    cron = yacron2.cron.Cron(
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


@pytest.mark.asyncio
async def test_handle_finished_job_skips_replaced(monkeypatch):
    # a job cancelled to make way for a replacement must not be reported as a
    # success or failure (and must not trigger retries).
    from types import SimpleNamespace

    cron = yacron2.cron.Cron(None)
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

    cron = yacron2.cron.Cron(None)
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

    assert calls == [("failure", job)]
    # the finished run is recorded for the web UI
    assert cron.last_run["test"].outcome == "failure"
    assert cron.last_run["test"].exit_code == 2
    # ...and appended to the bounded run history
    assert [r.outcome for r in cron.run_history["test"]] == ["failure"]


def test_simple_config_file(tracing_running_job):
    config_arg = str(Path(__file__).parent / "testconfig.yaml")
    yacron2.cron.Cron(config_arg)


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

    monkeypatch.setattr("yacron2.cron.get_now", get_now)

    cron = yacron2.cron.Cron(None, config_yaml=RETRYING_JOB_THAT_FAILS2)

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
    got_out = yacron2.cron.naturaltime(value_in)
    assert got_out == out


@pytest.mark.asyncio
async def test_schedule_retry_job_disappeared():
    # a job removed from config while a retry is pending must not raise
    # UnboundLocalError; the retry is simply skipped.
    cron = yacron2.cron.Cron(None)
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

    cron = yacron2.cron.Cron(None)
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

    cron = yacron2.cron.Cron(None)
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
        cron, "maybe_launch_job",
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

    cron = yacron2.cron.Cron(None)
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
        cron, "maybe_launch_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    monkeypatch.setattr(yacron2.cron, "RETRY_GATE_RECHECK_FLOOR", 0.01)
    cron.cron_jobs["j"] = job
    state = JobRetryState(0.01, 1, 0.01)
    cron.retry_state["j"] = state
    import logging

    with caplog.at_level(logging.DEBUG, logger="yacron2"):
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
    assert all(
        r.levelno in (logging.INFO, logging.DEBUG) for r in deferred
    )
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

    cron = yacron2.cron.Cron(None)
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
    with caplog.at_level(logging.WARNING, logger="yacron2"):
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

    cron = yacron2.cron.Cron(None)
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
    token = yacron2.cron.Cron._resolve_web_token({"authToken": auth})
    assert token == "secret"


def test_resolve_web_token_envvar(monkeypatch):
    monkeypatch.setenv("YACRON2_TEST_WEB_TOKEN", "envsecret")
    token = yacron2.cron.Cron._resolve_web_token(
        {
            "authToken": {
                "value": None,
                "fromFile": None,
                "fromEnvVar": "YACRON2_TEST_WEB_TOKEN",
            }
        }
    )
    assert token == "envsecret"


def test_resolve_web_token_absent():
    assert yacron2.cron.Cron._resolve_web_token({"listen": []}) is None


def test_resolve_web_token_missing_envvar_fails_closed(monkeypatch):
    # authToken configured but the env var is unset: must raise rather than
    # silently leaving the web API unauthenticated.
    monkeypatch.delenv("YACRON2_TEST_WEB_TOKEN", raising=False)
    with pytest.raises(yacron2.config.ConfigError):
        yacron2.cron.Cron._resolve_web_token(
            {
                "authToken": {
                    "value": None,
                    "fromFile": None,
                    "fromEnvVar": "YACRON2_TEST_WEB_TOKEN",
                }
            }
        )


def test_resolve_web_token_empty_value_fails_closed():
    with pytest.raises(yacron2.config.ConfigError):
        yacron2.cron.Cron._resolve_web_token(
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
    with pytest.raises(yacron2.config.ConfigError):
        yacron2.cron.Cron._resolve_web_token(
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

    middleware = yacron2.cron.Cron._make_auth_middleware("secret")

    async def handler(request):
        return web.Response(text="ok")

    class FakeRequest:
        def __init__(self, auth):
            self.headers = {"Authorization": auth} if auth else {}

    resp = await middleware(FakeRequest("Bearer secret"), handler)
    assert resp.text == "ok"

    with pytest.raises(web.HTTPUnauthorized):
        await middleware(FakeRequest("Bearer wrong"), handler)
    with pytest.raises(web.HTTPUnauthorized):
        await middleware(FakeRequest(None), handler)


def test_web_site_from_url_bad_scheme():
    with pytest.raises(ValueError):
        yacron2.cron.web_site_from_url(None, "ftp://localhost:1234")


def test_web_site_from_url_malformed_http():
    # missing host/port must raise ValueError (a skippable bad entry), not
    # AssertionError (which would be reported as an internal yacron2 bug).
    with pytest.raises(ValueError):
        yacron2.cron.web_site_from_url(None, "http://")


@pytest.mark.asyncio
async def test_start_web_app_ignores_bad_listen_urls():
    # an unusable listen url is skipped, not surfaced as an exception
    cron = yacron2.cron.Cron(None)
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

    cron = yacron2.cron.Cron(None, config_yaml=DISABLED_JOB)
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

    cron = yacron2.cron.Cron(None, config_yaml=DISABLED_JOB)
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

    cron = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
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

    cron = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    class Req:
        headers: dict = {}

    resp = await cron._web_job_set_id(Req())
    assert resp.text == cron.job_set_id()
    # the id always carries the live scheme label (see yacron2.fingerprint;
    # the golden-value tests pin the actual version)
    from yacron2.fingerprint import SCHEME_VERSION

    assert resp.text.startswith(SCHEME_VERSION + ":")

    class JsonReq:
        headers = {"Accept": "application/json"}

    resp = await cron._web_job_set_id(JsonReq())
    data = json.loads(resp.text)
    assert data["job_set_id"] == cron.job_set_id()
    assert data["jobs"] == 2


def test_job_set_id_logged_only_on_change(caplog):
    import logging

    cron = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
    with caplog.at_level(logging.INFO, logger="yacron2"):
        cron._log_job_set_id()
        cron._log_job_set_id()  # unchanged: must not log again
    logged = [r.message for r in caplog.records if "Job set id" in r.message]
    assert len(logged) == 1
    assert cron.job_set_id() in logged[0]


@pytest.mark.asyncio
async def test_web_list_jobs_includes_last_run():
    import json

    cron = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}
    cron.last_run["alpha"] = yacron2.cron.JobRunInfo(
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
    return yacron2.cron.JobRunInfo(
        outcome=outcome,
        exit_code=exit_code,
        started_at=start,
        finished_at=start + datetime.timedelta(seconds=dur),
        fail_reason=None if outcome == "success" else "boom",
        output=JobOutputStream(),
    )


def test_record_run_caps_history():
    cron = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
    limit = yacron2.cron.RUN_HISTORY_LIMIT
    for i in range(limit + 10):
        cron._record_run("alpha", _mk_run("success", exit_code=i))
    hist = cron.run_history["alpha"]
    assert len(hist) == limit  # bounded ring buffer
    # oldest entries evicted; newest retained and ordered oldest-first
    assert hist[0].exit_code == 10
    assert hist[-1].exit_code == limit + 9
    # last_run mirrors the most recent recorded run
    assert cron.last_run["alpha"].exit_code == limit + 9


@pytest.mark.asyncio
async def test_web_list_jobs_includes_history_and_timezone():
    import json

    cron = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
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

    cron = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
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

    cron = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    class Req:
        match_info = {"name": "nope"}

    with pytest.raises(web.HTTPNotFound):
        await cron._web_job_runs(Req())


@pytest.mark.asyncio
async def test_web_job_runs_empty_history():
    import json

    cron = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
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

    cron = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    class Req:
        match_info = {"name": "nope"}

    with pytest.raises(web.HTTPNotFound):
        await cron._web_cancel_job(Req())


@pytest.mark.asyncio
async def test_web_cancel_not_running_409():
    from aiohttp import web

    cron = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
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

    cron = yacron2.cron.Cron(None)
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
    cron = yacron2.cron.Cron(
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
    cron = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    class Req:
        pass

    resp = await cron._web_index(Req())
    assert resp.content_type == "text/html"
    assert "yacron2" in resp.text
    assert "<html" in resp.text.lower()


@pytest.mark.asyncio
async def test_web_index_sets_security_headers():
    cron = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
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
    cron = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {"headers": {"X-Frame-Options": "SAMEORIGIN"}}

    class Req:
        pass

    resp = await cron._web_index(Req())
    assert resp.headers["X-Frame-Options"] == "SAMEORIGIN"  # operator override
    assert resp.headers["X-Content-Type-Options"] == "nosniff"  # default kept


@pytest.mark.asyncio
async def test_auth_middleware_public_path():
    from aiohttp import web

    middleware = yacron2.cron.Cron._make_auth_middleware(
        "secret", yacron2.cron.WEB_PUBLIC_PATHS
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

    cron = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}
    out = JobOutputStream()
    out.publish("stdout", "hello world\n")
    out.publish("stderr", "uh oh\n")
    out.close()
    cron.last_run["alpha"] = yacron2.cron.JobRunInfo(
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

    cron = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
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

    cron = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
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

    monkeypatch.setattr("yacron2.cron.get_now", get_now)

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
    cron = yacron2.cron.Cron(None, config_yaml=config_yaml)
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
    cron = yacron2.cron.Cron(str(cfg))
    assert set(cron.cron_jobs) == {"alpha", "beta"}
    monkeypatch.setattr("yacron2.cron.next_sleep_interval", lambda *a: 0.01)

    def boom(*args, **kwargs):
        raise ConfigError("boom")

    # reload_config now skips the reparse when the file is unchanged on disk,
    # so touch it (a real "config edited to something invalid on reload"
    # scenario bumps mtime) to defeat the skip; the failed parse never records
    # a new fingerprint, so every subsequent tick still sees the change and
    # retries.
    cfg.write_text(TWO_JOBS + "\n# edited\n")
    monkeypatch.setattr("yacron2.cron.parse_config_with_sources", boom)

    task = asyncio.create_task(cron.run())
    try:
        # the reparse fails on every housekeeping tick, but the daemon must
        # stay up (no UnboundLocalError, no escape) and keep the jobs it had.
        await asyncio.sleep(0.1)
        assert not task.done()
        assert set(cron.cron_jobs) == {"alpha", "beta"}  # unchanged
        # the failed reload flips the standard "config broken on disk" signal
        # (yacron2_config_last_reload_successful) even though the parse ran off
        # the loop, in a worker thread.
        assert cron.metrics._last_reload_ok is False
    finally:
        cron.signal_shutdown()
        await asyncio.wait_for(task, timeout=5)


def test_cluster_allows_per_policy():
    import types

    cron = yacron2.cron.Cron(None)

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

    cron = yacron2.cron.Cron(None)
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

    cron = yacron2.cron.Cron(None)
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
    with caplog.at_level(logging.INFO, logger="yacron2"):
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

    cron = yacron2.cron.Cron(None)
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
    with caplog.at_level(logging.INFO, logger="yacron2"):
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

    cron = yacron2.cron.Cron(None)
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
    with caplog.at_level(logging.INFO, logger="yacron2"):
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

    cron = yacron2.cron.Cron(None)
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

    cron = yacron2.cron.Cron(None)
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

    cron = yacron2.cron.Cron(None)
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
    with caplog.at_level(logging.INFO, logger="yacron2"):
        cron._log_cluster_role()
        cron._log_cluster_role()  # unchanged: no second log
        cron.cluster_manager = _Mgr(False)
        cron._log_cluster_role()
    msgs = [r.message for r in caplog.records]
    assert sum("duplicate nodeName detected" in m for m in msgs) == 1
    assert sum("conflict resolved" in m for m in msgs) == 1


def test_cluster_size_conflict_logged_on_transition(caplog):
    import logging

    cron = yacron2.cron.Cron(None)
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
    with caplog.at_level(logging.INFO, logger="yacron2"):
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

    cron = yacron2.cron.Cron(None)
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
    with caplog.at_level(logging.INFO, logger="yacron2"):
        cron._log_cluster_role()
        cron._log_cluster_role()  # unchanged: no second log
        cron.cluster_manager = _Mgr(False)
        cron._log_cluster_role()
    msgs = [r.message for r in caplog.records]
    assert sum("coordination-policy divergence --" in m for m in msgs) == 1
    assert sum(
        "coordination-policy divergence resolved" in m for m in msgs
    ) == 1
    assert any(
        "distribution 'spread' != 'single-leader'" in m for m in msgs
    )


def test_is_deferrable_reboot():
    import types

    from crontab import CronTab

    cron = yacron2.cron.Cron(None)

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
    assert cron._is_deferrable_reboot(job("Leader", CronTab("* * * * *"))) \
        is False


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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
async def test_deferred_reboot_disabled_on_owner_is_not_run(monkeypatch):
    # A deferred @reboot Leader/PreferLeader job DISABLED via a reload while it
    # sat pending must be retired without running, even on the elected owner --
    # the same way job_should_run and the manual web trigger refuse a disabled
    # job. Otherwise an operator-disabled init/migration one-shot still runs
    # once cluster-wide on convergence.
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = True
    cron.cluster_manager = None
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = False  # election turned off on reload
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = True
    events = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = True
    cron.cluster_manager = None  # backend failed to start
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = True
    cron.cluster_manager = None
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = False
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = False
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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

    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = False
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
        'jobs:\n  - name: boot\n    command: echo hi\n'
        '    schedule: "@reboot"\n    clusterPolicy: Leader\n',
        "",
    )
    cron = yacron2.cron.Cron(None)
    cron.cron_jobs = OrderedDict((j.name, j) for j in config.jobs)
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron, "launch_scheduled_job",
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
    cron = yacron2.cron.Cron(None)
    cron.web_config = {}
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron, "maybe_launch_job",
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
    cron = yacron2.cron.Cron(None)
    cron.web_config = {}
    cron._elect_leader_configured = True
    cron.cluster_manager = None
    launched = []
    monkeypatch.setattr(
        cron, "maybe_launch_job",
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
    cron = yacron2.cron.Cron(None)
    cron.web_config = {}
    cron._elect_leader_configured = True
    launched = []
    monkeypatch.setattr(
        cron, "maybe_launch_job",
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
    cron = yacron2.cron.Cron(None)
    with caplog.at_level(logging.ERROR, logger="yacron2"):
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

    cron = yacron2.cron.Cron(None)
    fake = _FakeMgr(cfg)
    cron.cluster_manager = fake
    # same config object -> the config-change branch is skipped; only the
    # TLS-change signal can trigger the restart.
    with caplog.at_level(logging.INFO, logger="yacron2"):
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

        async def stop(self):
            self.stopped = True

    cron = yacron2.cron.Cron(None)
    fake = _FakeMgr(cfg)
    cron.cluster_manager = fake
    with caplog.at_level(logging.WARNING, logger="yacron2"):
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
    cron = yacron2.cron.Cron(None)
    fake = _ConfigChangeFakeMgr(cfg_a)
    cron.cluster_manager = fake
    with caplog.at_level(logging.WARNING, logger="yacron2"):
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
        "yacron2.cluster.gossip_tls_loadable", lambda cfg: True
    )
    cron = yacron2.cron.Cron(None)
    fake = _ConfigChangeFakeMgr(cfg_a)
    cron.cluster_manager = fake
    await cron.start_stop_cluster(cfg_b)
    assert fake.stopped is True  # config change tears down
    assert cron.cluster_manager is None  # reconstruction fails closed


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

    cron = yacron2.cron.Cron(None, config_yaml=_WEB_ONE_JOB)
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

    cron = yacron2.cron.Cron(None, config_yaml=_WEB_ONE_JOB)
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
        yacron2.cron,
        "web_site_from_url",
        lambda runner, url: FakeSite(url),
    )

    cron = yacron2.cron.Cron(None)
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
    monkeypatch.setattr("yacron2.cron.next_sleep_interval", lambda *a: 0.02)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_RELOAD_V1)

    cron = yacron2.cron.Cron(str(cfg))
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
    cron = yacron2.cron.Cron(None, config_yaml=_RETRY_DRAIN_JOB)

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
    fromEnvVar: YACRON2_TEST_MISSING_TOKEN
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

    monkeypatch.delenv("YACRON2_TEST_MISSING_TOKEN", raising=False)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_GATED_CLUSTER_BAD_WEB_TOKEN)
    cron = yacron2.cron.Cron(str(cfg))

    with caplog.at_level(logging.ERROR, logger="yacron2"):
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
    cron = yacron2.cron.Cron(
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
        cron = yacron2.cron.Cron(None)  # no jobs: run() idles until signalled
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
    cron = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
    out = JobOutputStream()
    out.close()
    cron.last_run["alpha"] = yacron2.cron.JobRunInfo(
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

    monkeypatch.setattr("yacron2.cron.get_now", get_now)


def test_schedule_slot_resolution(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 0, 0, 4, 500000)}
    _set_now(monkeypatch, holder)
    cron = yacron2.cron.Cron(None, config_yaml=_SECONDS_JOB)
    sec = cron.cron_jobs["sec"]
    minute = cron.cron_jobs["min"]
    # a second-level job truncates to the whole second (microseconds zeroed)
    assert yacron2.cron.schedule_slot(sec) == DT(
        2020, 1, 1, 0, 0, 4, tzinfo=UTC
    )
    # a minute-level job truncates to the top of the minute, as always
    assert yacron2.cron.schedule_slot(minute) == DT(
        2020, 1, 1, 0, 0, 0, tzinfo=UTC
    )


def test_needs_subminute():
    # an enabled second-level job makes the scheduler tick per-second
    cron = yacron2.cron.Cron(None, config_yaml=_SECONDS_JOB)
    assert cron._needs_subminute() is True
    # minute-only config does not
    cron2 = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
    assert cron2._needs_subminute() is False
    # a DISABLED second-level job must not force per-second ticking
    disabled = """
jobs:
  - name: sec
    command: echo sec
    schedule: "*/15 * * * * * *"
    enabled: false
"""
    cron3 = yacron2.cron.Cron(None, config_yaml=disabled)
    assert cron3._needs_subminute() is False


@pytest.mark.parametrize(
    "second, should_run",
    [(0, True), (1, False), (14, False), (15, True), (45, True), (46, False)],
)
def test_job_should_run_at_seconds(monkeypatch, second, should_run):
    holder = {"now": DT(2020, 1, 1, 0, 0, second)}
    _set_now(monkeypatch, holder)
    cron = yacron2.cron.Cron(None, config_yaml=_SECONDS_JOB)
    job = cron.cron_jobs["sec"]  # "*/15 * * * * * *"
    assert cron.job_should_run(False, job) is should_run


def test_next_sleep_interval_modes(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 30, 30, 500000)}
    _set_now(monkeypatch, holder)
    # minute mode snaps to the next minute (preserving the sub-second offset,
    # exactly as the historical behaviour did): from :30.5 that is 30.0s away.
    assert yacron2.cron.next_sleep_interval(False) == pytest.approx(30.0)
    # sub-minute mode snaps to the next whole-second boundary: :30.5 -> :31.0
    assert yacron2.cron.next_sleep_interval(True) == pytest.approx(0.5)


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
    cron = yacron2.cron.Cron(None, config_yaml=_SECONDS_JOB)

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
    cron = yacron2.cron.Cron(None, config_yaml=config_yaml)
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
    now = yacron2.cron.get_now(datetime.timezone.utc)
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
    gap = int(yacron2.cron.CATCHUP_LIMIT.total_seconds()) + 5
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
    cron = yacron2.cron.Cron(None, config_yaml=_THREE_DUE)

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
    cron = yacron2.cron.Cron(None, config_yaml=TWO_JOBS)
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
    cron = yacron2.cron.Cron(str(cfg))
    main_thread = threading.get_ident()

    seen = []
    real_parse = yacron2.cron.parse_config_with_sources

    def recording_parse(arg):
        seen.append(threading.get_ident())
        return real_parse(arg)

    monkeypatch.setattr(
        "yacron2.cron.parse_config_with_sources", recording_parse
    )
    monkeypatch.setattr("yacron2.cron.next_sleep_interval", lambda *a: 0.01)
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
    cron = yacron2.cron.Cron(str(cfg))

    launched = []

    async def fake_launch(job):
        launched.append(job.name)

    monkeypatch.setattr(cron, "launch_scheduled_job", fake_launch)
    monkeypatch.setattr("yacron2.cron.next_sleep_interval", lambda *a: 0.01)

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
    # The index instant is exactly now + parse-crontab's delay-to-next-match,
    # the same formula the dashboard countdown and the Prometheus next-run
    # gauge use, so all three agree.
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cron = yacron2.cron.Cron(None, config_yaml=_ONE_MINUTE_JOB)
    now = yacron2.cron.get_now(UTC)
    job = cron.cron_jobs["m"]
    delay = job.schedule.next(now=now, default_utc=True)
    assert cron._compute_next_fire(job, now) == now + datetime.timedelta(
        seconds=delay
    )
    # strictly-future: the in-progress minute (:00) is skipped for :01
    assert cron._compute_next_fire(job, now) == DT(2020, 1, 1, 0, 1, tzinfo=UTC)


def test_compute_next_fire_lands_on_a_matching_slot(monkeypatch):
    # Whatever the timezone, the computed next-fire instant, rendered back into
    # the job's own frame, satisfies the cron expression -- so the heap fires
    # the job exactly when the old test()-based tick would have matched. Uses a
    # */10 schedule so the next fire is minutes away (no DST boundary crossed).
    holder = {"now": DT(2020, 6, 1, 12, 34, 56)}
    _set_now(monkeypatch, holder)
    cron = yacron2.cron.Cron(None, config_yaml=_TZ_JOBS)
    now = yacron2.cron.get_now(UTC)
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
    cron = yacron2.cron.Cron(None, config_yaml=_SECONDS_JOB)  # sec */15 + min
    cron._ensure_seeded(yacron2.cron.get_now(UTC))
    # soonest is the */15 job at :15 -> 15s away (the minute job is 60s away)
    assert cron._sleep_interval() == pytest.approx(15.0, abs=0.05)


def test_sleep_interval_capped_by_housekeeping(monkeypatch):
    # A sparse job hours away still wakes the loop within the next wall-minute,
    # so config reload / cluster upkeep stays ~once a minute.
    holder = {"now": DT(2020, 1, 1, 3, 0, 15)}
    _set_now(monkeypatch, holder)
    cron = yacron2.cron.Cron(None, config_yaml=_NOON_DAILY)  # next fire 12:00
    cron._ensure_seeded(yacron2.cron.get_now(UTC))
    # capped at the next minute boundary (03:01:00), i.e. 45s
    assert cron._sleep_interval() == pytest.approx(45.0, abs=0.05)


def test_sleep_interval_no_jobs_uses_housekeeping(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 3, 0, 15)}
    _set_now(monkeypatch, holder)
    cron = yacron2.cron.Cron(None)  # nothing scheduled
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
    # the current slot with O(1) crontab work. Regression guard for the review's
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
async def test_last_run_slot_is_aware_utc_in_both_advance_branches(monkeypatch):
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
    cron = yacron2.cron.Cron(str(cfg))
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
    cron = yacron2.cron.Cron(str(cfg))
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
    cron = yacron2.cron.Cron(str(cfg))
    assert cron._next_fire["m"] == DT(2020, 1, 1, 0, 1, tzinfo=UTC)

    cfg.write_text(_ONE_MINUTE_JOB.replace('"* * * * *"', '"*/5 * * * *"'))
    cron.update_config()
    # reseeded strictly-future for the NEW schedule
    assert cron._next_fire["m"] == DT(2020, 1, 1, 0, 5, tzinfo=UTC)


def test_reload_refreshes_index(tmp_path, monkeypatch):
    # One reload exercises all three reconciliations: an unchanged job keeps its
    # fire, a removed job leaves the index, a new job is seeded strictly-future.
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_RELOAD_BEFORE)
    cron = yacron2.cron.Cron(str(cfg))
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
    cron = yacron2.cron.Cron(str(cfg))
    assert "m" in cron._next_fire

    cfg.write_text(_ONE_MINUTE_JOB.rstrip() + "\n    enabled: false\n")
    cron.update_config()
    assert "m" not in cron._next_fire  # disabled -> not scheduled


def _count_crontab_calls(monkeypatch):
    import crontab as crontab_mod

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
    cron = yacron2.cron.Cron(None, config_yaml="jobs:\n" + jobs)

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
    slot = yacron2.cron.schedule_slot(sample, holder["now"])
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
