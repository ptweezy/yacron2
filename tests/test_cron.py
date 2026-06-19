import asyncio
import datetime
import time
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import yacron2.cron
from yacron2.config import ConfigError, JobConfig
from yacron2.job import RunningJob


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

    def __init__(self, config: JobConfig, retry_state) -> None:
        super().__init__(config, retry_state)
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


JOB_THAT_SUCCEEDS = """
jobs:
  - name: test
    command: |
      echo "foobar"
    schedule: "@reboot"
"""

JOB_THAT_FAILS = """
jobs:
  - name: test
    command: |
      echo "foobar"
      exit 2
    schedule: "@reboot"
"""


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


RETRYING_JOB_THAT_FAILS = """
jobs:
  - name: test
    command: |
      echo "foobar"
      exit 2
    schedule: "@reboot"
    onFailure:
      retry:
        maximumRetries: 2
        initialDelay: 0.1
        maximumDelay: 1
        backoffMultiplier: 2
"""


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


JOB_THAT_HANGS = """
jobs:
  - name: test
    command: |
      trap "echo '(ignoring SIGTERM)'" TERM
      echo "starting..."
      sleep 10
      echo "all done."
    schedule: "@reboot"
    captureStdout: true
    executionTimeout: 0.25
    killTimeout: 0.25
"""


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


CONCURRENT_JOB = """
jobs:
  - name: test
    command: |
      sleep 30
    schedule: "@reboot"
    captureStdout: true
    concurrencyPolicy: {policy}
"""


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
        config=SimpleNamespace(name="test"),
        replaced=True,
        fail_reason="failsWhen=nonzeroReturn and retcode=-15",
        retcode=-15,
        stdout=None,
        stderr=None,
    )
    cron.running_jobs["test"].append(job)
    await cron._handle_finished_job(job)

    assert calls == []  # replaced -> neither reported
    assert "test" not in cron.running_jobs  # still cleaned up


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
        config=SimpleNamespace(name="test"),
        replaced=False,
        fail_reason="failsWhen=nonzeroReturn and retcode=2",
        retcode=2,
        stdout=None,
        stderr=None,
    )
    cron.running_jobs["test"].append(job)
    await cron._handle_finished_job(job)

    assert calls == [("failure", job)]


def test_simple_config_file(tracing_running_job):
    config_arg = str(Path(__file__).parent / "testconfig.yaml")
    yacron2.cron.Cron(config_arg)


RETRYING_JOB_THAT_FAILS2 = """
jobs:
  - name: test
    command: |
      echo "foobar"
      exit 2
    schedule: "@reboot"
    onFailure:
      retry:
        maximumRetries: 1
        initialDelay: 0.4
        maximumDelay: 1
        backoffMultiplier: 1
"""


@pytest.mark.asyncio
async def test_concurrency_and_backoff(monkeypatch, tracing_running_job):  # noqa: C901
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
        second=00,
        microsecond=250000,
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
async def test_run_survives_config_error(monkeypatch):
    # If update_config() raises (e.g. the config became invalid on reload),
    # run() must log it and keep running the previously-loaded jobs, not crash
    # with UnboundLocalError when it later inspects `config`.
    cron = yacron2.cron.Cron(None)

    def failing_update_config():
        # stop the loop after this (failing) iteration, then fail
        cron.signal_shutdown()
        raise ConfigError("boom")

    monkeypatch.setattr(cron, "update_config", failing_update_config)

    # completes without raising (UnboundLocalError before the fix)
    await asyncio.wait_for(cron.run(), timeout=5)
