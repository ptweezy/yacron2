import asyncio
import logging
import os
import signal
import tempfile
import time
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import aiosmtplib
import pytest
from sentry_sdk.utils import Dsn

import cronstable.config
import cronstable.job
import cronstable.platform
import cronstable.statsd
from cronstable.platform import DEFAULT_SHELL, IS_WINDOWS
from tests._commands import (
    PYTHON,
    cmd_print,
    cmd_print_sleep_print,
    cmd_sleep,
    cmd_spawn_helper_then_sleep,
    cmd_write_env,
    yaml_command,
)


def _argv(*parts):
    """Expected subprocess argv for this platform (str on Windows, bytes on
    POSIX) -- mirrors cronstable.platform.encode_argv."""
    return tuple(parts) if IS_WINDOWS else tuple(p.encode() for p in parts)


@pytest.mark.asyncio
async def test_stream_reader_join_timeout_keeps_partial_output():
    # The read loop only ends at EOF, i.e. once EVERY write-end of the pipe is
    # closed -- including one a descendant of the job inherited and never
    # closes. join() must be able to give up on that and keep what it read:
    # the lines already collected live here, not in the pipe.
    fake_stream = asyncio.StreamReader()
    reader = cronstable.job.StreamReader("test", "stdout", fake_stream, "", 10)
    fake_stream.feed_data(b"line1\n")  # note: no feed_eof() -- ever
    output, discarded = await asyncio.wait_for(reader.join(0.2), 5)
    assert output == "line1\n"
    assert discarded == 0


@pytest.mark.parametrize(
    "save_limit, input_lines, output, expected_failure",
    [
        (
            10,
            b"line1\nline2\nline3\nline4\n",
            "line1\nline2\nline3\nline4\n",
            True,
        ),
        (
            1,
            b"line1\nline2\nline3\nline4\n",
            "   [.... 3 lines discarded ...]\nline4\n",
            True,
        ),
        (
            2,
            b"line1\nline2\nline3\nline4\n",
            "line1\n   [.... 2 lines discarded ...]\nline4\n",
            True,
        ),
        (0, b"line1\nline2\nline3\nline4\n", "", True),
        (0, b"", "", False),
    ],
)
@pytest.mark.asyncio
async def test_stream_reader(
    save_limit, input_lines, output, expected_failure
):
    fake_stream = asyncio.StreamReader()
    reader = cronstable.job.StreamReader(
        "cronjob-1", "stderr", fake_stream, "", save_limit
    )

    conf = cronstable.config.parse_config_string(
        """
jobs:
  - name: test
    command: foo
    schedule: "* * * * *"
    captureStderr: true
""",
        "",
    )
    job_config = conf.jobs[0]
    job = cronstable.job.RunningJob(job_config, None)

    async def producer(fake_stream):
        fake_stream.feed_data(input_lines)
        fake_stream.feed_eof()

    job._stderr_reader = reader
    job.retcode = 0

    await asyncio.gather(producer(fake_stream), job._read_job_streams())

    out = job.stderr

    assert (out, job.failed) == (output, expected_failure)


@pytest.mark.asyncio
async def test_stream_reader_long_line():
    fake_stream = asyncio.StreamReader()
    reader = cronstable.job.StreamReader(
        "cronjob-1", "stderr", fake_stream, "", 500
    )

    conf = cronstable.config.parse_config_string(
        """
jobs:
  - name: test
    command: foo
    schedule: "* * * * *"
    captureStderr: true
""",
        "",
    )
    job_config = conf.jobs[0]
    job = cronstable.job.RunningJob(job_config, None)

    async def producer(fake_stream):
        fake_stream.feed_data(b"one line\n")
        fake_stream.feed_data(b"long line:" + b"1234567890" * 10_000)
        fake_stream.feed_data(b"\n")
        fake_stream.feed_data(b"another line\n")
        fake_stream.feed_eof()

    job._stderr_reader = reader
    job.retcode = 0

    await asyncio.gather(producer(fake_stream), job._read_job_streams())

    out = job.stderr
    assert out == "one line\nanother line\n"


@pytest.mark.asyncio
async def test_job_output_stream_subscribe_then_publish():
    out = cronstable.job.JobOutputStream()
    queue = out.subscribe()
    out.publish("stdout", "hello\n")
    out.publish("stderr", "oops\n")
    assert queue.get_nowait() == ("stdout", "hello\n")
    assert queue.get_nowait() == ("stderr", "oops\n")
    # the ring buffer retains lines for late viewers
    assert list(out.lines) == [("stdout", "hello\n"), ("stderr", "oops\n")]


@pytest.mark.asyncio
async def test_job_output_stream_close_delivers_sentinel():
    out = cronstable.job.JobOutputStream()
    queue = out.subscribe()
    out.publish("stdout", "line\n")
    out.close()
    assert queue.get_nowait() == ("stdout", "line\n")
    assert queue.get_nowait() is None  # end-of-stream sentinel


@pytest.mark.asyncio
async def test_job_output_stream_late_subscriber_gets_sentinel():
    # subscribing after the run finished must not block forever: the new
    # subscriber receives the end sentinel immediately, after the buffer.
    out = cronstable.job.JobOutputStream()
    out.publish("stdout", "done\n")
    out.close()
    queue = out.subscribe()
    assert queue.get_nowait() is None
    assert list(out.lines) == [("stdout", "done\n")]


@pytest.mark.asyncio
async def test_job_output_stream_ring_buffer_bounds():
    out = cronstable.job.JobOutputStream(limit=3)
    for i in range(5):
        out.publish("stdout", f"line {i}\n")
    # only the most recent `limit` lines are retained
    assert list(out.lines) == [
        ("stdout", "line 2\n"),
        ("stdout", "line 3\n"),
        ("stdout", "line 4\n"),
    ]


@pytest.mark.asyncio
async def test_job_output_stream_subscriber_queue_drops_oldest_when_full(
    monkeypatch,
):
    # A stalled subscriber (never draining its queue) must not grow without
    # bound: the queue is capped and overflow drops the OLDEST line so the
    # viewer keeps receiving the newest output.
    monkeypatch.setattr(cronstable.job, "LIVE_LOG_SUBSCRIBER_QUEUE_LIMIT", 3)
    out = cronstable.job.JobOutputStream()
    queue = out.subscribe()
    for i in range(5):
        out.publish("stdout", f"line {i}\n")
    assert queue.qsize() == 3
    assert out.dropped == 2
    # oldest two (line 0, line 1) were evicted; newest three remain in order
    assert queue.get_nowait() == ("stdout", "line 2\n")
    assert queue.get_nowait() == ("stdout", "line 3\n")
    assert queue.get_nowait() == ("stdout", "line 4\n")


@pytest.mark.asyncio
async def test_job_output_stream_sentinel_delivered_to_saturated_queue(
    monkeypatch,
):
    # close()'s end sentinel must reach even a subscriber whose bounded queue is
    # already full, or that reader loop would never terminate.
    monkeypatch.setattr(cronstable.job, "LIVE_LOG_SUBSCRIBER_QUEUE_LIMIT", 2)
    out = cronstable.job.JobOutputStream()
    queue = out.subscribe()
    out.publish("stdout", "a\n")
    out.publish("stdout", "b\n")
    out.close()
    drained = []
    while not queue.empty():
        drained.append(queue.get_nowait())
    assert drained[-1] is None  # sentinel present despite the earlier overflow


@pytest.mark.asyncio
async def test_stream_reader_publishes_to_output():
    # the on_line hook wires StreamReader output into a JobOutputStream so the
    # web UI can tail lines live as the job produces them.
    out = cronstable.job.JobOutputStream()
    fake_stream = asyncio.StreamReader()
    reader = cronstable.job.StreamReader(
        "cronjob-1", "stdout", fake_stream, "", 100, on_line=out.publish
    )
    fake_stream.feed_data(b"first\n")
    fake_stream.feed_data(b"second\n")
    fake_stream.feed_eof()
    await reader.join()
    assert list(out.lines) == [("stdout", "first\n"), ("stdout", "second\n")]


A_JOB = """
jobs:
  - name: test
    command: ls
    schedule: "* * * * *"
    onSuccess:
      report:
        mail:
          from: example@foo.com
          to: example@bar.com
          smtpHost: smtp1
          smtpPort: 1025
          subject: >
            Cron job '{{name}}' {% if success %}completed{%
            else %}failed{% endif %}
          password:
            value: foobar
          username: thisisme
          tls: false
          starttls: true
          body: |
            {% if stdout and stderr -%}
            STDOUT:
            ---
            {{stdout}}
            ---
            STDERR:
            {{stderr}}
            {% elif stdout -%}
            {{stdout}}
            {% elif stderr -%}
            {{stderr}}
            {% else -%}
            (no output was captured)
            {% endif %}
"""


@pytest.mark.parametrize(
    "success, stdout, stderr, subject, body",
    [
        (
            True,
            "out",
            "err",
            "Cron job 'test' completed",
            "STDOUT:\n---\nout\n---\nSTDERR:\nerr\n",
        ),
        (
            False,
            "out",
            "err",
            "Cron job 'test' failed",
            "STDOUT:\n---\nout\n---\nSTDERR:\nerr\n",
        ),
        (
            False,
            None,
            None,
            "Cron job 'test' failed",
            "(no output was captured)\n",
        ),
        (False, None, "err", "Cron job 'test' failed", "err\n"),
        (False, "out", None, "Cron job 'test' failed", "out\n"),
    ],
)
@pytest.mark.asyncio
async def test_report_mail(success, stdout, stderr, subject, body):
    conf = cronstable.config.parse_config_string(A_JOB, "")
    job_config = conf.jobs[0]
    print(job_config.onSuccess["report"])
    job = Mock(
        config=job_config,
        stdout=stdout,
        stderr=stderr,
        template_vars={
            "name": job_config.name,
            "success": success,
            "stdout": stdout,
            "stderr": stderr,
        },
    )

    mail = cronstable.job.MailReporter()

    connect_calls = []
    start_tls_calls = []
    login_calls = []
    messages_sent = []

    async def connect(self):
        connect_calls.append(self)

    async def starttls(self):
        start_tls_calls.append(self)

    async def login(self, username, password):
        login_calls.append((username, password))

    async def send_message(self, message):
        messages_sent.append(message)

    real_init = aiosmtplib.SMTP.__init__
    smtp_init_args = None

    def init(self, *args, **kwargs):
        nonlocal smtp_init_args
        smtp_init_args = args, kwargs
        real_init(self, *args, **kwargs)

    with (
        patch("aiosmtplib.SMTP.__init__", init),
        patch("aiosmtplib.SMTP.connect", connect),
        patch("aiosmtplib.SMTP.send_message", send_message),
        patch("aiosmtplib.SMTP.login", login),
        patch("aiosmtplib.SMTP.starttls", starttls),
    ):
        await mail.report(success, job, job_config.onSuccess["report"])

    assert smtp_init_args == (
        (),
        {
            "hostname": "smtp1",
            "port": 1025,
            "use_tls": False,
            "validate_certs": True,
        },
    )
    assert len(connect_calls) == 1
    assert len(start_tls_calls) == 1
    assert login_calls == [("thisisme", "foobar")]
    assert len(messages_sent) == 1
    message = messages_sent[0]
    assert message["From"] == "example@foo.com"
    assert message["To"] == "example@bar.com"
    assert message["Subject"] == subject
    assert message.get_payload() == body


@pytest.mark.parametrize(
    "success, dsn_from, body, extra, expected_dsn, fingerprint, "
    "level_in, level_out",
    [
        (
            True,
            "value",
            "Cron job 'test' completed\n\n(job failed because reasons)"
            "\n\nSTDOUT:\n---\nout\n---\nSTDERR:\nerr\n",
            {
                "job": "test",
                "exit_code": 0,
                "command": "ls",
                "shell": DEFAULT_SHELL,
                "success": True,
            },
            "http://xxx:yyy@sentry/1",
            ["test"],
            "warning",
            "warning",
        ),
        (
            False,
            "file",
            "Cron job 'test' failed\n\n(job failed because reasons)"
            "\n\nSTDOUT:\n---\nout\n---\nSTDERR:\nerr\n",
            {
                "job": "test",
                "exit_code": 0,
                "command": "ls",
                "shell": DEFAULT_SHELL,
                "success": False,
            },
            "http://xxx:yyy@sentry/2",
            ["test"],
            None,
            "error",
        ),
        (
            False,
            "envvar",
            "Cron job 'test' failed\n\n(job failed because reasons)"
            "\n\nSTDOUT:\n---\nout\n---\nSTDERR:\nerr\n",
            {
                "job": "test",
                "exit_code": 0,
                "command": "ls",
                "shell": DEFAULT_SHELL,
                "success": False,
            },
            "http://xxx:yyy@sentry/3",
            ["test"],
            None,
            "error",
        ),
    ],
)
@pytest.mark.asyncio
async def test_report_sentry(  # noqa: C901
    success,
    dsn_from,
    body,
    extra,
    expected_dsn,
    fingerprint,
    level_in,
    level_out,
    tmpdir,
    monkeypatch,
):
    conf = cronstable.config.parse_config_string(A_JOB, "")
    job_config = conf.jobs[0]

    p = tmpdir.join("sentry-secret-dsn")
    p.write("http://xxx:yyy@sentry/2")

    monkeypatch.setenv("TEST_SENTRY_DSN", "http://xxx:yyy@sentry/3")

    if dsn_from == "value":
        job_config.onSuccess["report"]["sentry"] = {
            "dsn": {
                "value": "http://xxx:yyy@sentry/1",
                "fromFile": None,
                "fromEnvVar": None,
            }
        }
    elif dsn_from == "file":
        job_config.onSuccess["report"]["sentry"] = {
            "dsn": {"value": None, "fromFile": str(p), "fromEnvVar": None}
        }
    elif dsn_from == "envvar":
        job_config.onSuccess["report"]["sentry"] = {
            "dsn": {
                "value": None,
                "fromFile": None,
                "fromEnvVar": "TEST_SENTRY_DSN",
            }
        }
    else:
        raise AssertionError

    job_config.onSuccess["report"]["sentry"]["body"] = (
        cronstable.config.DEFAULT_CONFIG["onFailure"]["report"]["sentry"][
            "body"
        ]
    )

    job_config.onSuccess["report"]["sentry"]["fingerprint"] = ["{{ name }}"]

    if level_in is not None:
        job_config.onSuccess["report"]["sentry"]["level"] = level_in

    job = Mock(
        config=job_config,
        stdout="out",
        stderr="err",
        retcode=0,
        template_vars={
            "fail_reason": "reasons",
            "name": job_config.name,
            "success": success,
            "stdout": "out",
            "stderr": "err",
        },
    )

    transports = []

    class FakeSentryTransport:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            self.messages_sent = []
            # sentry-sdk 2.x reads transport.parsed_dsn off the client; the
            # options dict is passed positionally as make_transport(options).
            options = args[0] if args else kwargs.get("options", {})
            dsn = options.get("dsn")
            self.parsed_dsn = Dsn(dsn) if dsn else None

        # sentry-sdk 2.x delivers events as envelopes, not bare events.
        def capture_envelope(self, envelope):
            event = envelope.get_event()
            if event is not None:
                self.messages_sent.append(event)

        def capture_event(self, event_opt):
            self.messages_sent.append(event_opt)

        def record_lost_event(self, *args, **kwargs):
            pass

        def is_healthy(self):
            return True

        def kill(self):
            pass

        def flush(self, *args, **kwargs):
            pass

    def make_transport(*args, **kwargs):
        transport = FakeSentryTransport(*args, **kwargs)
        transports.append(transport)
        return transport

    monkeypatch.setattr("sentry_sdk.client.make_transport", make_transport)

    sentry = cronstable.job.SentryReporter()
    await sentry.report(success, job, job_config.onSuccess["report"])
    for transport in transports:
        assert transport.args[0].get("dsn") == expected_dsn

    messages_sent = [
        msg for transport in transports for msg in transport.messages_sent
    ]

    assert len(messages_sent) == 1
    msg = messages_sent[0]
    msg1 = {
        key: msg[key] for key in {"message", "level", "fingerprint", "extra"}
    }
    msg1["extra"].pop("sys.argv", "")

    assert msg1 == {
        "message": body,
        "level": level_out,
        "fingerprint": fingerprint,
        "extra": extra,
    }


@pytest.mark.parametrize(
    "command, expected_output",
    [
        (
            'echo "foobar" && exit 123',
            'test - echo "foobar" && exit 123 - * * * * * - Error code 123',
        ),
        (
            "\n      - bad-cmd\n      - arg",
            "test - bad-cmd arg - * * * * * - Error code 123",
        ),
    ],
)
@pytest.mark.asyncio
async def test_report_shell(command, expected_output):
    stdout, stderr = None, None
    with tempfile.TemporaryDirectory() as tmp:
        out_file_path = os.path.join(tmp, "unit_test_file")
        reporter = yaml_command(cmd_write_env(out_file_path), indent=10)

        conf = cronstable.config.parse_config_string(
            f"""
jobs:
  - name: test
    command: {command}
    schedule: "* * * * *"
    onFailure:
      report:
        shell:
{reporter}
""",
            "",
        )
        job_config = conf.jobs[0]

        job = Mock(
            config=job_config,
            stdout=stdout,
            stderr=stderr,
            template_vars={
                "name": job_config.name,
                "success": False,
                "stdout": stdout,
                "stderr": stderr,
            },
            retcode=123,
            fail_reason="",
            failed=True,
            run_id="run-mock",
            started_at=datetime(2026, 7, 22, 3, 0, 0, tzinfo=timezone.utc),
        )

        shell_reporter = cronstable.job.ShellReporter()

        await shell_reporter.report(False, job, job_config.onFailure["report"])

        assert os.path.isfile(out_file_path)
        with open(out_file_path, "r") as file:
            data = file.read()
        assert data.strip() == expected_output


def _webhook_job_config(url_yaml: str, extra: str = "") -> str:
    return f"""
jobs:
  - name: test
    command: ls
    schedule: "* * * * *"
    onFailure:
      report:
        webhook:
          url:
{url_yaml}
{extra}
"""


def _webhook_job(job_config, success=False, stdout="out", stderr="err"):
    fail_reason = None if success else "reasons"
    return Mock(
        config=job_config,
        stdout=stdout,
        stderr=stderr,
        template_vars={
            "name": job_config.name,
            "success": success,
            "fail_reason": fail_reason,
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": 0 if success else 123,
            "command": job_config.command,
            "shell": job_config.shell,
            "environment": None,
        },
    )


class _WebhookServer:
    """A local aiohttp server capturing every request the reporter makes."""

    def __init__(self, status: int = 200) -> None:
        from aiohttp import web

        self.requests = []
        self.status = status
        app = web.Application()
        app.router.add_route("*", "/hook", self._handler)
        self._app = app
        self._server = None

    async def _handler(self, request):
        from aiohttp import web

        self.requests.append(
            {
                "method": request.method,
                "headers": dict(request.headers),
                "body": await request.text(),
            }
        )
        return web.Response(status=self.status, text="a response body")

    async def __aenter__(self) -> str:
        from aiohttp.test_utils import TestServer

        self._server = TestServer(self._app)
        await self._server.start_server()
        return str(self._server.make_url("/hook"))

    async def __aexit__(self, *exc) -> None:
        await self._server.close()


@pytest.mark.parametrize(
    "success, expected_subject",
    [
        (False, "Cron job 'test' failed"),
        (True, "Cron job 'test' completed"),
    ],
)
@pytest.mark.asyncio
async def test_report_webhook(success, expected_subject):
    import json

    server = _WebhookServer()
    async with server as url:
        conf = cronstable.config.parse_config_string(
            _webhook_job_config(
                f"            value: {url}",
                extra=("          headers:\n            X-Custom: yes-hello"),
            ),
            "",
        )
        job_config = conf.jobs[0]
        job = _webhook_job(job_config, success=success)

        reporter = cronstable.job.WebhookReporter()
        await reporter.report(success, job, job_config.onFailure["report"])

    (request,) = server.requests
    assert request["method"] == "POST"
    assert request["headers"]["Content-Type"] == "application/json"
    assert request["headers"]["X-Custom"] == "yes-hello"
    # the default body template must render valid JSON in the {"text": ...}
    # shape Slack-compatible webhooks accept
    payload = json.loads(request["body"])
    assert set(payload.keys()) == {"text"}
    assert payload["text"].startswith(expected_subject)
    assert "out" in payload["text"]
    assert "err" in payload["text"]
    if not success:
        assert "(job failed because reasons)" in payload["text"]


@pytest.mark.parametrize("url_source", ["fromFile", "fromEnvVar"])
@pytest.mark.asyncio
async def test_report_webhook_url_sources(url_source, monkeypatch, tmp_path):
    server = _WebhookServer()
    async with server as url:
        if url_source == "fromFile":
            url_file = tmp_path / "hook-url"
            url_file.write_text(url + "\n")
            url_yaml = f"            fromFile: {url_file}"
        else:
            monkeypatch.setenv("TEST_WEBHOOK_URL", url)
            url_yaml = "            fromEnvVar: TEST_WEBHOOK_URL"
        conf = cronstable.config.parse_config_string(
            _webhook_job_config(url_yaml), ""
        )
        job_config = conf.jobs[0]
        job = _webhook_job(job_config)

        await cronstable.job.WebhookReporter().report(
            False, job, job_config.onFailure["report"]
        )

    assert len(server.requests) == 1


@pytest.mark.asyncio
async def test_report_webhook_disabled():
    # with no url source configured (the default), the reporter must return
    # early without opening any HTTP session
    conf = cronstable.config.parse_config_string(
        """
jobs:
  - name: test
    command: ls
    schedule: "* * * * *"
""",
        "",
    )
    job_config = conf.jobs[0]
    job = _webhook_job(job_config)

    def no_session(*args, **kwargs):
        raise AssertionError("ClientSession must not be created")

    with patch("aiohttp.ClientSession", no_session):
        await cronstable.job.WebhookReporter().report(
            False, job, job_config.onFailure["report"]
        )


@pytest.mark.asyncio
async def test_report_webhook_env_var_not_set(monkeypatch, caplog):
    monkeypatch.delenv("TEST_WEBHOOK_URL", raising=False)
    conf = cronstable.config.parse_config_string(
        _webhook_job_config("            fromEnvVar: TEST_WEBHOOK_URL"), ""
    )
    job_config = conf.jobs[0]
    job = _webhook_job(job_config)

    def no_session(*args, **kwargs):
        raise AssertionError("ClientSession must not be created")

    with patch("aiohttp.ClientSession", no_session):
        with caplog.at_level(logging.ERROR, logger="cronstable"):
            await cronstable.job.WebhookReporter().report(
                False, job, job_config.onFailure["report"]
            )
    assert any("url env var" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_report_webhook_http_error(caplog):
    # a non-2xx response is logged at ERROR (with the response text) but must
    # not raise out of the reporter
    server = _WebhookServer(status=500)
    async with server as url:
        conf = cronstable.config.parse_config_string(
            _webhook_job_config(f"            value: {url}"), ""
        )
        job_config = conf.jobs[0]
        job = _webhook_job(job_config)

        with caplog.at_level(logging.ERROR, logger="cronstable"):
            await cronstable.job.WebhookReporter().report(
                False, job, job_config.onFailure["report"]
            )

    assert len(server.requests) == 1
    assert any(
        "HTTP 500" in rec.getMessage()
        and "a response body" in rec.getMessage()
        for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_report_webhook_custom_method_and_body():
    server = _WebhookServer()
    async with server as url:
        conf = cronstable.config.parse_config_string(
            _webhook_job_config(
                f"            value: {url}",
                extra=(
                    "          method: PUT\n"
                    "          contentType: text/plain\n"
                    '          body: "job {{ name }}: rc={{ exit_code }}"'
                ),
            ),
            "",
        )
        job_config = conf.jobs[0]
        job = _webhook_job(job_config)

        await cronstable.job.WebhookReporter().report(
            False, job, job_config.onFailure["report"]
        )

    (request,) = server.requests
    assert request["method"] == "PUT"
    assert request["headers"]["Content-Type"] == "text/plain"
    assert request["body"] == "job test: rc=123"


@pytest.mark.parametrize(
    "shell, command, expected_type, expected_args",
    [
        ("", "Civ 6", "shell", _argv("Civ 6")),
        ("", ["echo", "hello"], "exec", _argv("echo", "hello")),
        ("bash", 'echo "hello"', "exec", _argv("bash", "-c", 'echo "hello"')),
    ],
)
@pytest.mark.asyncio
async def test_job_run(
    monkeypatch, shell, command, expected_type, expected_args
):
    shell_commands = []
    exec_commands = []

    async def create_subprocess_common(*args, **kwargs):
        stdout = asyncio.StreamReader()
        stderr = asyncio.StreamReader()
        stdout.feed_data(b"out\n")
        stdout.feed_eof()
        stderr.feed_data(b"err\n")
        stderr.feed_eof()
        proc = Mock(stdout=stdout, stderr=stderr)

        async def wait():
            return

        proc.wait = wait
        return proc

    async def create_subprocess_shell(*args, **kwargs):
        shell_commands.append((args, kwargs))
        return await create_subprocess_common(*args, **kwargs)

    async def create_subprocess_exec(*args, **kwargs):
        exec_commands.append((args, kwargs))
        return await create_subprocess_common(*args, **kwargs)

    monkeypatch.setattr(
        "asyncio.create_subprocess_exec", create_subprocess_exec
    )
    monkeypatch.setattr(
        "asyncio.create_subprocess_shell", create_subprocess_shell
    )

    if isinstance(command, list):
        command_snippet = "\n".join(
            ["    command:"] + ["      - " + arg for arg in command]
        )
    else:
        command_snippet = "    command: " + command

    conf = cronstable.config.parse_config_string(
        """
jobs:
  - name: test
{command}
    schedule: "* * * * *"
    shell: {shell}
    captureStderr: true
    captureStdout: true
    environment:
      - key: FOO
        value: bar
""".format(command=command_snippet, shell=shell),
        "",
    )
    job_config = conf.jobs[0]

    job = cronstable.job.RunningJob(job_config, None)

    await job.start()
    await job.wait()

    if shell_commands:
        run_type = "shell"
        assert len(shell_commands) == 1
        args, kwargs = shell_commands[0]
    elif exec_commands:
        run_type = "exec"
        assert len(exec_commands) == 1
        args, kwargs = exec_commands[0]
    else:
        raise AssertionError

    assert kwargs["env"]["FOO"] == "bar"
    assert run_type == expected_type
    assert args == expected_args


@pytest.mark.asyncio
async def test_monitor_resources_populates_usage():
    # a monitored job records CPU time + peak RSS on the RunningJob, which the
    # reaper then folds into the run record / metrics.
    conf = cronstable.config.parse_config_string(
        "jobs:\n  - name: test\n"
        + yaml_command(cmd_sleep(0.4))
        + """
    monitorResources: true
    schedule: "* * * * *"
""",
        "",
    )
    job = cronstable.job.RunningJob(conf.jobs[0], None)
    # the monitor takes an immediate first sample when its task first runs
    # (during the wait below), so even this sub-second run is measured once.
    await job.start()
    await job.wait()
    assert job.resource_usage is not None
    assert job.resource_usage.samples >= 1
    assert job.resource_usage.max_rss_bytes > 0
    # exposed to report templates as well
    assert (
        job.template_vars["max_rss_bytes"] == job.resource_usage.max_rss_bytes
    )


@pytest.mark.asyncio
async def test_monitor_resources_off_by_default():
    conf = cronstable.config.parse_config_string(
        "jobs:\n  - name: test\n"
        + yaml_command(cmd_print(out="hi"))
        + """
    schedule: "* * * * *"
""",
        "",
    )
    job = cronstable.job.RunningJob(conf.jobs[0], None)
    assert job.config.monitorResources is False
    await job.start()
    await job.wait()
    assert job.resource_usage is None
    assert job.template_vars["cpu_seconds"] is None


@pytest.mark.asyncio
async def test_template_vars_carry_run_context(monkeypatch):
    # a report payload should identify the run: host, schedule, start instant,
    # and the durable-ledger run id (all new alongside the run's outcome).
    conf = cronstable.config.parse_config_string(
        "jobs:\n  - name: test\n"
        + yaml_command(cmd_print(out="hi"))
        + """
    schedule: "*/5 * * * *"
""",
        "",
    )
    job = cronstable.job.RunningJob(conf.jobs[0], None, run_id="run-xyz")
    # a sentinel host, so the assertion pins WHERE the value comes from
    # (comparing against report_hostname()'s own expression proves nothing).
    monkeypatch.setenv("HOSTNAME", "host-sentinel-1")
    tv = job.template_vars
    # host, schedule and the ledger id are known before the run starts.
    assert tv["host"] == "host-sentinel-1"
    assert tv["schedule"] == "*/5 * * * *"
    assert tv["run_id"] == "run-xyz"
    # nothing has started yet, so there is no start instant.
    assert tv["started_at"] is None

    await job.start()
    await job.wait()
    started = job.template_vars["started_at"]
    assert started is not None
    # surfaced as ISO-8601, matching the run-history field of the same name.
    assert started == job.started_at.isoformat()


@pytest.mark.asyncio
async def test_template_vars_schedule_renders_object_form():
    # schedule_unparsed is Union[str, dict]; an object schedule must render to
    # its crontab line here just as it does for the shell reporter's
    # CRONSTABLE_JOB_SCHEDULE, so every report payload agrees.
    conf = cronstable.config.parse_config_string(
        "jobs:\n"
        "  - name: objsched\n"
        "    command: echo hi\n"
        "    schedule:\n"
        '      minute: "*/5"\n',
        "",
    )
    job = cronstable.job.RunningJob(conf.jobs[0], None)
    assert job.template_vars["schedule"] == "*/5 * * * *"


@pytest.mark.asyncio
async def test_execution_timeout():
    conf = cronstable.config.parse_config_string(
        "jobs:\n  - name: test\n"
        + yaml_command(cmd_print_sleep_print("hello", 1, "world"))
        + """
    executionTimeout: 0.25
    schedule: "* * * * *"
    captureStderr: false
    captureStdout: true
""",
        "",
    )
    job_config = conf.jobs[0]
    job = cronstable.job.RunningJob(job_config, None)
    await job.start()
    await job.wait()
    assert job.stdout == "hello\n"


def _spawner_yaml(name="test", execution_timeout=0.25, kill_timeout=1):
    return (
        "jobs:\n  - name: {}\n".format(name)
        + yaml_command(cmd_spawn_helper_then_sleep(30))
        + """
    executionTimeout: {}
    killTimeout: {}
    schedule: "* * * * *"
    captureStderr: false
    captureStdout: true
""".format(execution_timeout, kill_timeout)
    )


async def _await_reaped(pid, timeout=10.0):
    """Wait for ``pid`` to disappear; return whether it did."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if cronstable.platform.pid_alive(pid) is not True:
            return True
        await asyncio.sleep(0.05)
    return cronstable.platform.pid_alive(pid) is not True


@pytest.mark.skipif(
    IS_WINDOWS, reason="process groups (and killpg) are POSIX-only"
)
@pytest.mark.asyncio
async def test_execution_timeout_kills_the_whole_process_group():
    # A job that leaves a helper behind (`sh -c 'helper & main'`) hits its
    # executionTimeout. Terminating only the process we spawned kills the
    # shell but not the helper, which still holds the job's stdout write-end
    # open -- so the pipe never reaches EOF and wait() never returns: the run
    # is stranded in running_jobs forever, and under concurrencyPolicy: Forbid
    # the job never runs again. Killing the whole process group reaps the
    # helper too, so the pipe closes and the run completes.
    conf = cronstable.config.parse_config_string(_spawner_yaml(), "")
    job = cronstable.job.RunningJob(conf.jobs[0], None)
    await job.start()
    # before the fix this hangs forever, not for `timeout` seconds.
    await asyncio.wait_for(job.wait(), 20)
    assert job.retcode == -100  # cancelled by executionTimeout
    helper_pid = int(job.stdout.strip())
    # the helper went down WITH the group rather than outliving the job --
    # executionTimeout bounds the run's work, not just its root process.
    assert await _await_reaped(helper_pid), (
        "the helper outlived the group kill"
    )


@pytest.mark.asyncio
async def test_killed_job_with_an_escaped_descendant_still_finishes(
    monkeypatch,
):
    # Defense in depth for the same wedge: where the group kill cannot reach a
    # descendant -- it made its own session, or Windows lost the orphan from
    # the process tree -- the job's pipe still never reaches EOF. The drain
    # must be bounded anyway, so the run always leaves running_jobs; the output
    # captured before the kill is kept.
    async def never_reaches_the_group(pid, *, force):
        return False  # simulate a descendant outside the group's reach

    monkeypatch.setattr(
        cronstable.platform, "kill_process_group", never_reaches_the_group
    )
    monkeypatch.setattr(cronstable.job, "KILLED_STREAM_DRAIN_TIMEOUT", 1.0)
    conf = cronstable.config.parse_config_string(_spawner_yaml(), "")
    job = cronstable.job.RunningJob(conf.jobs[0], None)
    await job.start()
    # without the bound this hangs on the surviving helper's pipe forever.
    await asyncio.wait_for(job.wait(), 20)
    assert job.retcode == -100
    helper_pid = int(job.stdout.strip())  # partial output kept, not discarded
    # the helper really did survive (that is what made the drain hang), so
    # this test must not leak it into the rest of the run.
    os.kill(helper_pid, signal.SIGKILL if not IS_WINDOWS else signal.SIGTERM)


@pytest.mark.asyncio
async def test_untouched_job_drain_is_not_bounded(monkeypatch):
    # The bound is only for a run we killed: a job left to exit on its own owns
    # its lifetime, and its output is not ours to cut short. Assert the gate,
    # not the timeout -- an unbounded join is the absence of a deadline.
    conf = cronstable.config.parse_config_string(
        "jobs:\n  - name: test\n"
        + yaml_command(cmd_print(out="hi"))
        + '\n    schedule: "* * * * *"\n',
        "",
    )
    job = cronstable.job.RunningJob(conf.jobs[0], None)
    joined = []
    real_join = cronstable.job.StreamReader.join

    async def spy(self, timeout=None):
        joined.append(timeout)
        return await real_join(self, timeout)

    monkeypatch.setattr(cronstable.job.StreamReader, "join", spy)
    await job.start()
    await job.wait()
    assert job._terminated is False
    assert joined and all(t is None for t in joined)


@pytest.mark.asyncio
async def test_error1():
    conf = cronstable.config.parse_config_string(
        "jobs:\n  - name: test\n"
        + yaml_command(cmd_sleep(5))
        + '\n    schedule: "* * * * *"\n',
        "",
    )
    job_config = conf.jobs[0]
    job = cronstable.job.RunningJob(job_config, None)

    await job.start()
    with pytest.raises(RuntimeError):
        await job.start()
    await job.cancel()


@pytest.mark.asyncio
async def test_error2():
    conf = cronstable.config.parse_config_string(
        "jobs:\n  - name: test\n"
        + yaml_command(cmd_sleep(5))
        + '\n    schedule: "* * * * *"\n',
        "",
    )
    job_config = conf.jobs[0]
    job = cronstable.job.RunningJob(job_config, None)

    with pytest.raises(RuntimeError):
        await job.wait()


@pytest.mark.asyncio
async def test_error3():
    # cancel() with no process is a NO-OP, not a RuntimeError: callers cancel
    # whatever running_jobs holds (a failed spawn registers with proc=None),
    # and several of those paths run outside run()'s try/except -- a raise
    # here used to take the whole scheduler down (see
    # test_cancel_with_no_process_is_noop for the spawn-failed variant).
    conf = cronstable.config.parse_config_string(
        "jobs:\n  - name: test\n"
        + yaml_command(cmd_sleep(5))
        + '\n    schedule: "* * * * *"\n',
        "",
    )
    job_config = conf.jobs[0]
    job = cronstable.job.RunningJob(job_config, None)

    await job.cancel()  # never started: must not raise
    assert job.proc is None


@pytest.mark.parametrize(
    "command", [cmd_print(out="hello"), cmd_print(code=1)]
)
@pytest.mark.asyncio
async def test_statsd(command):
    loop = asyncio.get_event_loop()
    received = []

    async def run():
        class UDPServerProtocol:
            def connection_made(self, transport):
                self.transport = transport

            def datagram_received(self, data, addr):
                print("Statsd UDP packet received:", data)
                message = data.decode()
                received.extend(m for m in message.split("\n") if m)

            def connection_lost(*_):
                pass

        listen = loop.create_datagram_endpoint(
            UDPServerProtocol, local_addr=("127.0.0.1", 0)
        )
        transport, protocol = await listen

        host, port = transport.get_extra_info("sockname")
        print("Listening UDP on %s:%s" % (host, port))

        conf = cronstable.config.parse_config_string(
            "jobs:\n  - name: test\n"
            + yaml_command(command)
            + """
    schedule: "* * * * *"
    statsd:
      host: 127.0.0.1
      port: {port}
      prefix: the.prefix
""".format(port=port),
            "",
        )
        job_config = conf.jobs[0]

        job = cronstable.job.RunningJob(job_config, None)

        await job.start()
        await job.wait()
        await asyncio.sleep(0.05)
        transport.close()
        await asyncio.sleep(0.05)
        return job

    job = await run()

    assert received
    assert len(received) == 4
    assert "the.prefix.start" in received[0]
    assert any("the.prefix.stop" in r for r in received[1:])
    success = 0 if job.failed else 1
    assert any("the.prefix.success:%i" % success in r for r in received[1:])
    assert any("the.prefix.duration" in r for r in received[1:])


@pytest.mark.asyncio
async def test_statsd_resource_metrics():
    # with monitorResources on, job_stopped also ships cpu + max_rss gauges.
    loop = asyncio.get_event_loop()
    received = []

    class UDPServerProtocol:
        def connection_made(self, transport):
            self.transport = transport

        def datagram_received(self, data, addr):
            received.extend(m for m in data.decode().split("\n") if m)

        def connection_lost(*_):
            pass

    transport, _ = await loop.create_datagram_endpoint(
        UDPServerProtocol, local_addr=("127.0.0.1", 0)
    )
    _host, port = transport.get_extra_info("sockname")
    conf = cronstable.config.parse_config_string(
        "jobs:\n  - name: test\n"
        + yaml_command(cmd_sleep(0.3))
        + """
    schedule: "* * * * *"
    monitorResources: true
    statsd:
      host: 127.0.0.1
      port: {port}
      prefix: the.prefix
""".format(port=port),
        "",
    )
    job = cronstable.job.RunningJob(conf.jobs[0], None)
    await job.start()
    await job.wait()
    await asyncio.sleep(0.05)
    transport.close()
    await asyncio.sleep(0.05)

    assert job.resource_usage is not None
    assert any("the.prefix.cpu:" in r for r in received)
    assert any("the.prefix.max_rss:" in r for r in received)


@pytest.mark.asyncio
async def test_start_failure_reported_not_raised():
    # A command that cannot be launched (e.g. it does not exist) must be
    # treated as a normal job failure with exit code 127, not raise
    # RuntimeError (which the reaper logs as "please report this as a bug").
    conf = cronstable.config.parse_config_string(
        """
jobs:
  - name: test
    command:
      - /this/command/definitely/does/not/exist
    schedule: "* * * * *"
""",
        "",
    )
    job = cronstable.job.RunningJob(conf.jobs[0], None)

    await job.start()
    assert job.proc is None
    assert job.start_failed

    # must not raise; routed through normal failure handling instead
    await job.wait()
    assert job.retcode == 127
    assert job.failed


@pytest.mark.asyncio
async def test_start_failure_bare_oserror_reported_not_raised(monkeypatch):
    # REBOOT-LAUNCH-OSERROR: a bare OSError from create_subprocess_exec (fd /
    # process exhaustion -- EMFILE/ENFILE/ENOMEM/EAGAIN -- or EPERM/EACCES) is
    # NOT a SubprocessError and NOT FileNotFoundError, so before the catch was
    # broadened it propagated out of the unguarded spawn_jobs /
    # _process_pending_reboots path and killed the whole scheduler. It must now
    # be treated as a normal start failure (start_failed set), not raised.
    conf = cronstable.config.parse_config_string(
        """
jobs:
  - name: test
    command:
      - /bin/true
    schedule: "* * * * *"
""",
        "",
    )

    async def boom(*args, **kwargs):
        raise OSError(24, "Too many open files")

    monkeypatch.setattr("asyncio.create_subprocess_exec", boom)
    job = cronstable.job.RunningJob(conf.jobs[0], None)

    await job.start()  # must NOT raise
    assert job.proc is None
    assert job.start_failed

    await job.wait()
    assert job.retcode == 127
    assert job.failed


@pytest.mark.asyncio
async def test_start_failure_log_does_not_leak_the_child_environment(
    monkeypatch, caplog
):
    # The spawn kwargs carry a full os.environ copy plus the CRONSTABLE_*
    # loopback credentials, and the spawn-failure handler formats kwargs into
    # an ERROR record. Logging it verbatim published every environment secret
    # the daemon holds to journald/syslog and anything shipping from them.
    conf = cronstable.config.parse_config_string(
        """
jobs:
  - name: test
    command:
      - /bin/true
    schedule: "* * * * *"
    environment:
      - key: JOB_VAR
        value: job-value
""",
        "",
    )
    monkeypatch.setenv("SPAWN_LEAK_CANARY", "canary-from-os-environ")

    async def boom(*args, **kwargs):
        raise OSError(24, "Too many open files")

    monkeypatch.setattr("asyncio.create_subprocess_exec", boom)
    job = cronstable.job.RunningJob(conf.jobs[0], None)
    job.extra_env = {"CRONSTABLE_STATE_TOKEN": "tok-canary-deadbeef"}

    with caplog.at_level(logging.DEBUG, logger="cronstable"):
        await job.start()
    assert job.start_failed

    blob = caplog.text
    assert "canary-from-os-environ" not in blob
    assert "tok-canary-deadbeef" not in blob
    assert "job-value" not in blob
    # the diagnostic itself survives: the failure, the job, and the fact that
    # a custom environment was in play
    assert "Error launching subprocess of job test" in blob
    assert "vars, values omitted" in blob


def test_loggable_spawn_kwargs_passes_through_without_env():
    # no env in kwargs (the common case: a job with no `environment:` and no
    # control-channel injection) is returned untouched, same object.
    kwargs = {"limit": 4096}
    assert cronstable.job.loggable_spawn_kwargs(kwargs) is kwargs


def test_loggable_spawn_kwargs_leaves_other_keys_alone():
    kwargs = {"env": {"A": "secret-a", "B": "secret-b"}, "limit": 4096}
    out = cronstable.job.loggable_spawn_kwargs(kwargs)
    assert out["limit"] == 4096
    assert out["env"] == "<2 vars, values omitted>"
    # the caller's dict is not mutated: it is still the real env handed to the
    # subprocess
    assert kwargs["env"] == {"A": "secret-a", "B": "secret-b"}


@pytest.mark.asyncio
async def test_statsd_failure_does_not_crash(monkeypatch):
    # statsd is best-effort: a send error (e.g. an unresolvable host) must be
    # swallowed and not propagate out of start()/wait() to crash the scheduler.
    async def boom(*args, **kwargs):
        raise OSError("statsd unreachable")

    monkeypatch.setattr(cronstable.statsd, "send_to_statsd", boom)

    conf = cronstable.config.parse_config_string(
        "jobs:\n  - name: test\n"
        + yaml_command(cmd_print())
        + """
    schedule: "* * * * *"
    statsd:
      host: 127.0.0.1
      port: 9999
      prefix: the.prefix
""",
        "",
    )
    job = cronstable.job.RunningJob(conf.jobs[0], None)

    await job.start()  # _on_start must swallow the OSError
    await job.wait()  # _on_stop must swallow the OSError
    assert job.retcode == 0


@pytest.mark.asyncio
async def test_report_mail_closes_connection_on_error():
    # if sending fails, the SMTP connection must still be closed (no leak).
    conf = cronstable.config.parse_config_string(A_JOB, "")
    job_config = conf.jobs[0]
    job = Mock(
        config=job_config,
        stdout="out",
        stderr="err",
        template_vars={
            "name": job_config.name,
            "success": False,
            "stdout": "out",
            "stderr": "err",
        },
    )

    mail = cronstable.job.MailReporter()
    close_calls = []

    async def connect(self):
        pass

    async def starttls(self):
        pass

    async def login(self, username, password):
        pass

    async def send_message(self, message):
        raise RuntimeError("smtp boom")

    def close(self):
        close_calls.append(self)

    with (
        patch("aiosmtplib.SMTP.connect", connect),
        patch("aiosmtplib.SMTP.starttls", starttls),
        patch("aiosmtplib.SMTP.login", login),
        patch("aiosmtplib.SMTP.send_message", send_message),
        patch("aiosmtplib.SMTP.close", close),
    ):
        with pytest.raises(RuntimeError):
            await mail.report(False, job, job_config.onSuccess["report"])

    assert len(close_calls) == 1


# ---------------------------------------------------------------------------
# Privilege drop (_demote) -- POSIX only.
#
# The child drops supplementary groups BEFORE setuid (the classic "forgot
# setgroups() before setuid()" privilege-escalation bug). A refactor that
# reorders these syscalls, drops the setgroups([]) fallback, swaps initgroups
# for a plain setgid, or stops wrapping OSError, would re-open that hole
# or run the job as the wrong account. There is no test that catches it today,
# yet _demote runs on every POSIX deploy that uses user/group. These lock the
# exact syscall order, both group branches, and the error wrapping.
# ---------------------------------------------------------------------------

_SIMPLE_JOB = """
jobs:
  - name: t
    command: echo hi
    schedule: "* * * * *"
"""


def _make_job_with_ids(uid=None, gid=None, username=None):
    conf = cronstable.config.parse_config_string(_SIMPLE_JOB, "")
    job = cronstable.job.RunningJob(conf.jobs[0], None)
    job.config.uid = uid
    job.config.gid = gid
    job.config.username = username
    return job


def _record_priv_syscalls(monkeypatch, calls, failing=None):
    # Replace the four privilege-drop syscalls with recorders. The one named in
    # `failing` raises OSError instead, exercising the error-wrap branches.
    def make(name):
        def fake(*args):
            if name == failing:
                raise OSError("denied")
            calls.append((name, args))

        return fake

    for name in ("initgroups", "setgroups", "setgid", "setuid"):
        monkeypatch.setattr(os, name, make(name))


@pytest.mark.skipif(IS_WINDOWS, reason="privilege drop is POSIX-only")
def test_demote_drops_groups_before_setuid(monkeypatch):
    calls = []
    _record_priv_syscalls(monkeypatch, calls)

    job = _make_job_with_ids(uid=1000, gid=1000, username="svc")
    job._demote()

    # supplementary groups MUST be set before the gid, which MUST be set
    # before the uid: once the uid drops to non-root, setgid/setgroups fail.
    assert [name for name, _ in calls] == ["initgroups", "setgid", "setuid"]
    # a known user+gid uses initgroups (the user's groups), not setgroups([])
    assert calls[0] == ("initgroups", ("svc", 1000))
    assert calls[1] == ("setgid", (1000,))
    assert calls[2] == ("setuid", (1000,))


@pytest.mark.skipif(IS_WINDOWS, reason="privilege drop is POSIX-only")
def test_demote_clears_groups_when_no_username(monkeypatch):
    calls = []
    _record_priv_syscalls(monkeypatch, calls)

    # numeric uid with no resolved username: the user's own groups cannot be
    # enumerated, so ALL supplementary groups are dropped, never kept.
    job = _make_job_with_ids(uid=1000, gid=1000, username=None)
    job._demote()

    assert [name for name, _ in calls] == ["setgroups", "setgid", "setuid"]
    assert calls[0] == ("setgroups", ([],))


@pytest.mark.skipif(IS_WINDOWS, reason="privilege drop is POSIX-only")
@pytest.mark.parametrize(
    "failing_call, prefix",
    [
        ("initgroups", "setgroups/initgroups:"),
        ("setgid", "setgid:"),
        ("setuid", "setuid:"),
    ],
)
def test_demote_wraps_oserror(monkeypatch, failing_call, prefix):
    # every privilege-drop syscall that fails must surface as a RuntimeError
    # with a clear prefix, not a bare OSError the reaper would mislabel as an
    # internal cronstable bug.
    calls = []
    _record_priv_syscalls(monkeypatch, calls, failing=failing_call)

    job = _make_job_with_ids(uid=1000, gid=1000, username="svc")
    with pytest.raises(RuntimeError) as exc:
        job._demote()
    assert str(exc.value).startswith(prefix)


@pytest.mark.skipif(IS_WINDOWS, reason="preexec_fn is POSIX-only")
@pytest.mark.asyncio
async def test_start_wires_preexec_fn_only_when_demoting(monkeypatch):
    captured = []

    async def fake_exec(*args, **kwargs):
        captured.append(kwargs)
        proc = Mock(stdout=None, stderr=None)

        async def wait():
            return 0

        proc.wait = wait
        return proc

    monkeypatch.setattr("asyncio.create_subprocess_exec", fake_exec)

    conf = cronstable.config.parse_config_string(
        """
jobs:
  - name: t
    command:
      - echo
      - hi
    schedule: "* * * * *"
    captureStdout: false
    captureStderr: false
""",
        "",
    )

    # with a uid to drop to, start() must wire preexec_fn -> _demote (bound
    # methods compare equal, not identical, so use ==).
    job = cronstable.job.RunningJob(conf.jobs[0], None)
    job.config.uid = 1000
    await job.start()
    assert captured[-1].get("preexec_fn") == job._demote

    # with neither uid nor gid, preexec_fn must NOT be passed: it is needless
    # overhead on every spawn and an outright error on some platforms.
    job2 = cronstable.job.RunningJob(conf.jobs[0], None)
    job2.config.uid = None
    job2.config.gid = None
    await job2.start()
    assert "preexec_fn" not in captured[-1]


# ---------------------------------------------------------------------------
# Shell-reporter CRONSTABLE_* env contract.
#
# Users' alerting scripts read these exact variable names; a rename or typo,
# or an inverted truncation flag, breaks them silently with the suite green.
# The pre-existing shell-reporter test reads back only 4 of 10 variables and
# never exercises truncation. These lock the full name set, the values, and the
# 16 KiB truncation behavior.
# ---------------------------------------------------------------------------

GOLDEN_SHELL_ENV_KEYS = frozenset(
    {
        "CRONSTABLE_FAIL_REASON",
        "CRONSTABLE_JOB_NAME",
        "CRONSTABLE_JOB_COMMAND",
        "CRONSTABLE_JOB_SCHEDULE",
        "CRONSTABLE_FAILED",
        "CRONSTABLE_RETCODE",
        "CRONSTABLE_STDERR",
        "CRONSTABLE_STDOUT",
        "CRONSTABLE_STDERR_TRUNCATED",
        "CRONSTABLE_STDOUT_TRUNCATED",
        # resource accounting: always exported (empty when the run was not
        # monitored), see ShellReporter.report / cronstable.resources.
        "CRONSTABLE_CPU_SECONDS",
        "CRONSTABLE_MAX_RSS_BYTES",
        # run context: the daemon host (always set), plus the run's ledger id
        # and start instant (empty on an onLate dispatch, which has no run).
        "CRONSTABLE_HOST",
        "CRONSTABLE_RUN_ID",
        "CRONSTABLE_STARTED_AT",
        # SLA breach detail: always exported, empty on run-completion
        # reports (only an onLate dispatch's SlaBreachContext carries
        # sla_vars).
        "CRONSTABLE_SLA_CHECK",
        "CRONSTABLE_SLA_THRESHOLD_SECONDS",
        "CRONSTABLE_SLA_OBSERVED_SECONDS",
        "CRONSTABLE_LAST_SUCCESS_AT",
    }
)

_MAX_ARG = 1024 * 16  # mirrors ShellReporter.max_length_arg

_SHELL_REPORTER_JOB = """
jobs:
  - name: test
    command: echo the-command
    schedule: "*/5 * * * *"
    onFailure:
      report:
        shell:
          command: "true"
"""


async def _capture_shell_reporter_env(
    monkeypatch, *, stdout, stderr, retcode=7, fail_reason="boom", failed=True
):
    # Drop any ambient CRONSTABLE_* so the exact-set assertion is deterministic
    # regardless of how the suite was launched.
    for key in [k for k in os.environ if k.startswith("CRONSTABLE_")]:
        monkeypatch.delenv(key, raising=False)

    captured = {}

    async def fake_create(*args, **kwargs):
        captured["env"] = kwargs["env"]
        proc = Mock()

        async def wait():
            return 0

        proc.wait = wait
        return proc

    # patch both spawn paths so the capture is robust to shell-vs-exec routing
    monkeypatch.setattr("asyncio.create_subprocess_shell", fake_create)
    monkeypatch.setattr("asyncio.create_subprocess_exec", fake_create)

    conf = cronstable.config.parse_config_string(_SHELL_REPORTER_JOB, "")
    job_config = conf.jobs[0]
    job = Mock(
        config=job_config,
        stdout=stdout,
        stderr=stderr,
        retcode=retcode,
        fail_reason=fail_reason,
        failed=failed,
        resource_usage=None,
        run_id="run-abc123",
        started_at=datetime(2026, 7, 22, 3, 0, 0, tzinfo=timezone.utc),
    )
    await cronstable.job.ShellReporter().report(
        False, job, job_config.onFailure["report"]
    )
    return captured["env"]


@pytest.mark.asyncio
async def test_report_shell_full_env_contract(monkeypatch):
    # sentinel host: pins that CRONSTABLE_HOST is sourced from the daemon's
    # HOSTNAME rather than restating report_hostname()'s own expression.
    monkeypatch.setenv("HOSTNAME", "host-sentinel-3")
    env = await _capture_shell_reporter_env(
        monkeypatch, stdout="out", stderr="err"
    )

    # exactly these CRONSTABLE_* variables are exported, no more and no fewer:
    # a dropped, renamed, or added variable fails here.
    assert {
        k for k in env if k.startswith("CRONSTABLE_")
    } == GOLDEN_SHELL_ENV_KEYS

    assert env["CRONSTABLE_JOB_NAME"] == "test"
    assert env["CRONSTABLE_JOB_COMMAND"] == "echo the-command"
    assert env["CRONSTABLE_JOB_SCHEDULE"] == "*/5 * * * *"
    assert env["CRONSTABLE_FAILED"] == "1"
    assert env["CRONSTABLE_RETCODE"] == "7"
    # unmonitored run: resource vars present but empty
    assert env["CRONSTABLE_CPU_SECONDS"] == ""
    assert env["CRONSTABLE_MAX_RSS_BYTES"] == ""
    # run context: host is the daemon's, run id and start instant come off the
    # (mocked) run.
    assert env["CRONSTABLE_HOST"] == "host-sentinel-3"
    assert env["CRONSTABLE_RUN_ID"] == "run-abc123"
    assert env["CRONSTABLE_STARTED_AT"] == "2026-07-22T03:00:00+00:00"
    assert env["CRONSTABLE_FAIL_REASON"] == "boom"
    assert env["CRONSTABLE_STDOUT"] == "out"
    assert env["CRONSTABLE_STDERR"] == "err"
    assert env["CRONSTABLE_STDOUT_TRUNCATED"] == "0"
    assert env["CRONSTABLE_STDERR_TRUNCATED"] == "0"
    # a run-completion report carries no SLA context (and the Mock's auto
    # sla_vars attribute is not a dict): present but empty
    assert env["CRONSTABLE_SLA_CHECK"] == ""
    assert env["CRONSTABLE_SLA_THRESHOLD_SECONDS"] == ""
    assert env["CRONSTABLE_SLA_OBSERVED_SECONDS"] == ""
    assert env["CRONSTABLE_LAST_SUCCESS_AT"] == ""


@pytest.mark.asyncio
async def test_report_shell_env_when_succeeded(monkeypatch):
    # FAILED tracks job.failed; a success exports "0" and an empty FAIL_REASON
    # (job.fail_reason is None -> ""), and None stdout/stderr collapse to "".
    env = await _capture_shell_reporter_env(
        monkeypatch,
        stdout=None,
        stderr=None,
        retcode=0,
        fail_reason=None,
        failed=False,
    )
    assert env["CRONSTABLE_FAILED"] == "0"
    assert env["CRONSTABLE_RETCODE"] == "0"
    assert env["CRONSTABLE_FAIL_REASON"] == ""
    assert env["CRONSTABLE_STDOUT"] == ""
    assert env["CRONSTABLE_STDERR"] == ""


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "out_len, err_len, exp_out_trunc, exp_err_trunc, exp_out_len, exp_err_len",
    [
        # both small: nothing truncated
        (100, 100, "0", "0", 100, 100),
        # stdout alone exceeds the per-arg limit: only stdout is truncated
        (_MAX_ARG + 5000, 100, "1", "0", _MAX_ARG, 100),
        # stderr alone exceeds the limit: only stderr is truncated
        (100, _MAX_ARG + 5000, "0", "1", 100, _MAX_ARG),
    ],
)
async def test_report_shell_truncates_large_output(
    monkeypatch,
    out_len,
    err_len,
    exp_out_trunc,
    exp_err_trunc,
    exp_out_len,
    exp_err_len,
):
    env = await _capture_shell_reporter_env(
        monkeypatch, stdout="o" * out_len, stderr="e" * err_len
    )
    assert env["CRONSTABLE_STDOUT_TRUNCATED"] == exp_out_trunc
    assert env["CRONSTABLE_STDERR_TRUNCATED"] == exp_err_trunc
    assert len(env["CRONSTABLE_STDOUT"]) == exp_out_len
    assert len(env["CRONSTABLE_STDERR"]) == exp_err_len


# --- cancel() on a run that never spawned -----------------------------------


@pytest.mark.asyncio
async def test_cancel_with_no_process_is_noop():
    # A job whose command fails to spawn registers with proc=None and
    # start_failed (see start()). The Replace branch of maybe_launch_job and
    # the cluster slot-renewer then await cancel() on whatever running_jobs
    # holds -- both OUTSIDE run()'s try/except -- so cancel() raising
    # RuntimeError("process is not running") here used to take down the whole
    # scheduler. It must be a no-op, and the reaper's wait() must still
    # complete the run afterwards.
    conf = cronstable.config.parse_config_string(
        "jobs:\n  - name: test\n"
        + yaml_command(["cronstable-no-such-binary-xyz"])
        + """
    schedule: "* * * * *"
""",
        "",
    )
    job = cronstable.job.RunningJob(conf.jobs[0], None)
    await job.start()
    assert job.proc is None
    assert job.start_failed is True

    await job.cancel()  # must not raise
    await job.cancel()  # idempotent

    # the reaper path still pairs the finish: conventional 127 exit
    await job.wait()
    assert job.retcode == 127


# --- shell reporter timeout --------------------------------------------------


_SHELL_REPORTER_HANG_JOB = (
    """
jobs:
  - name: test
    command: echo the-command
    schedule: "*/5 * * * *"
    onFailure:
      report:
        shell:
"""
    + yaml_command(cmd_sleep(120), indent=10)
    + """
          timeout: 0.5
"""
)


@pytest.mark.asyncio
async def test_report_shell_hanging_reporter_is_killed():
    # report() runs INLINE on the reaper -- the daemon's only job-completion
    # loop -- so a reporter command that never exits used to freeze completion
    # handling for every job daemon-wide. The configured timeout must kill the
    # reporter's process group and let report() return.
    conf = cronstable.config.parse_config_string(_SHELL_REPORTER_HANG_JOB, "")
    job_config = conf.jobs[0]
    assert job_config.onFailure["report"]["shell"]["timeout"] == 0.5
    job = Mock(
        config=job_config,
        stdout="",
        stderr="",
        retcode=1,
        fail_reason="boom",
        failed=True,
        resource_usage=None,
    )
    started = time.monotonic()
    await asyncio.wait_for(
        cronstable.job.ShellReporter().report(
            False, job, job_config.onFailure["report"]
        ),
        60,
    )
    # returned because the 0.5s timeout killed the 120s sleeper, not because
    # the sleeper finished (generous bound: process spawn + kill + reap).
    assert time.monotonic() - started < 60


def test_report_shell_timeout_defaults_to_60():
    # the bound must exist even when the config never mentions it -- an
    # unbounded default is exactly the reaper-wedging posture this guards.
    conf = cronstable.config.parse_config_string(_SHELL_REPORTER_JOB, "")
    report = conf.jobs[0].onFailure["report"]
    assert report["shell"]["timeout"] == 60


@pytest.mark.asyncio
async def test_report_shell_combined_over_limit_is_not_truncated(monkeypatch):
    # DOCUMENTS CURRENT BEHAVIOR (and a latent gap): when stdout and stderr are
    # each under the 16 KiB per-arg limit but whose SUM exceeds it, the code
    # flags args_too_long internally, yet the [:16 KiB] slice shortens neither
    # value, so neither *_TRUNCATED flag is set and the combined env block is
    # still ~2x the limit. If this is ever tightened, update this test
    # deliberately.
    each = 10000  # 2 * 10000 = 20000 > 16384, but each value < 16384
    env = await _capture_shell_reporter_env(
        monkeypatch, stdout="o" * each, stderr="e" * each
    )
    assert env["CRONSTABLE_STDOUT_TRUNCATED"] == "0"
    assert env["CRONSTABLE_STDERR_TRUNCATED"] == "0"
    assert len(env["CRONSTABLE_STDOUT"]) == each
    assert len(env["CRONSTABLE_STDERR"]) == each


# ===========================================================================
# Reporter / stream / cancel path behaviors: reporter secret-source and
# error/timeout branches, StreamReader passthrough-emit fallbacks,
# JobOutputStream teardown edges, RunningJob.cancel of a live process, the
# live-resource accessors, fail_reason, and _report_common exception handling.
# ===========================================================================

_SIMPLE_JOB_TEST = """
jobs:
  - name: test
    command: echo hi
    schedule: "* * * * *"
"""

_MAIL_JOB = """
jobs:
  - name: test
    command: ls
    schedule: "* * * * *"
    onFailure:
      report:
        mail:
          from: from@example.com
          to: to@example.com
          smtpHost: smtp.example.com
          smtpPort: 1025
          username: theuser
"""

_SHELL_JOB = """
jobs:
  - name: test
    command: echo the-command
    schedule: "* * * * *"
    onFailure:
      report:
        shell:
          command: "true"
"""


def _fresh_job():
    conf = cronstable.config.parse_config_string(_SIMPLE_JOB_TEST, "")
    return cronstable.job.RunningJob(conf.jobs[0], None)


def _mail_report_config():
    conf = cronstable.config.parse_config_string(_MAIL_JOB, "")
    return conf.jobs[0], conf.jobs[0].onFailure["report"]


def _mail_job_mock(job_config, success=False, stdout="out", stderr="err"):
    return Mock(
        config=job_config,
        stdout=stdout,
        stderr=stderr,
        template_vars={
            "name": job_config.name,
            "success": success,
            "fail_reason": None if success else "reasons",
            "stdout": stdout,
            "stderr": stderr,
        },
    )


async def _run_mail_capturing(report, job, success=False):
    """Drive MailReporter with the SMTP conversation stubbed out."""
    login_calls = []
    messages = []

    async def connect(self):
        pass

    async def starttls(self):
        pass

    async def login(self, username, password):
        login_calls.append((username, password))

    async def send_message(self, message):
        messages.append(message)

    def close(self):
        pass

    with (
        patch("aiosmtplib.SMTP.connect", connect),
        patch("aiosmtplib.SMTP.starttls", starttls),
        patch("aiosmtplib.SMTP.login", login),
        patch("aiosmtplib.SMTP.send_message", send_message),
        patch("aiosmtplib.SMTP.close", close),
    ):
        await cronstable.job.MailReporter().report(success, job, report)
    return login_calls, messages


# ---------------------------------------------------------------------------
# StreamReader passthrough-emit fallbacks
# ---------------------------------------------------------------------------


def test_stream_reader_emit_falls_back_to_ascii_on_unicode_error():
    # _emit writes bytes to the console; when the console's buffer refuses the
    # bytes (UnicodeEncodeError), it must fall back to an ascii-replaced text
    # write rather than let the reader task die.
    class FakeBuffer:
        def write(self, data):
            raise UnicodeEncodeError("utf-8", "x", 0, 1, "nope")

    class FakeStream:
        def __init__(self):
            self.buffer = FakeBuffer()
            self.text_written = []
            self.flushed = 0

        def write(self, text):
            self.text_written.append(text)

        def flush(self):
            self.flushed += 1

    fs = FakeStream()
    cronstable.job.StreamReader._emit(fs, "hello\n")

    assert fs.text_written == ["hello\n"]  # ascii-replaced text path taken
    assert fs.flushed == 1


async def test_flush_emit_buffer_survives_broken_daemon_stream(caplog):
    # If the daemon's own stdout is a dead pipe, the batched passthrough flush
    # must swallow the OSError and log a single warning -- the job's capture is
    # unaffected, so the reader keeps going.
    fake = asyncio.StreamReader()
    fake.feed_eof()
    reader = cronstable.job.StreamReader("j", "stdout", fake, "", 10)

    def boom(out, text):
        raise OSError("dead pipe")

    reader._emit = boom  # instance attr: plain function, not bound
    reader._emit_buffer = ["line\n"]

    with caplog.at_level(logging.WARNING, logger="cronstable"):
        reader._flush_emit_buffer()

    assert reader._emit_buffer == []  # buffer was drained despite the failure
    assert any(
        "could not mirror" in rec.getMessage() for rec in caplog.records
    )
    await reader.join()  # let the (already EOF) read task settle


# ---------------------------------------------------------------------------
# JobOutputStream teardown edges
# ---------------------------------------------------------------------------


def test_output_stream_unsubscribe_unknown_queue_is_noop():
    out = cronstable.job.JobOutputStream()
    queue = out.subscribe()
    out.unsubscribe(queue)
    # unsubscribing a queue that is no longer registered must not raise
    out.unsubscribe(queue)
    # a queue that was never registered at all is equally a no-op
    out.unsubscribe(asyncio.Queue())
    # the subscriber really is gone: a publish reaches no one
    out.publish("stdout", "x\n")
    assert queue.empty()


def test_output_stream_close_is_idempotent():
    out = cronstable.job.JobOutputStream()
    queue = out.subscribe()
    out.close()
    out.close()  # second close short-circuits, no second sentinel enqueued
    assert queue.get_nowait() is None
    assert queue.empty()


# ---------------------------------------------------------------------------
# SentryReporter
# ---------------------------------------------------------------------------


async def test_sentry_report_env_var_unset_is_skipped(monkeypatch, caplog):
    monkeypatch.delenv("TEST_SENTRY_UNSET", raising=False)
    conf = cronstable.config.parse_config_string(
        """
jobs:
  - name: test
    command: ls
    schedule: "* * * * *"
    onFailure:
      report:
        sentry:
          dsn:
            fromEnvVar: TEST_SENTRY_UNSET
""",
        "",
    )
    report = conf.jobs[0].onFailure["report"]
    job = Mock(config=conf.jobs[0])

    # sentry_sdk must never be initialized down this path -- the early return
    # fires before the (expensive) import.
    def boom(*args, **kwargs):
        raise AssertionError("sentry_sdk.init must not be called")

    monkeypatch.setattr("sentry_sdk.init", boom)

    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await cronstable.job.SentryReporter().report(True, job, report)

    assert any("dsn env var" in rec.getMessage() for rec in caplog.records)


async def test_sentry_report_applies_environment_and_max_string_length(
    monkeypatch,
):
    import sentry_sdk.utils

    conf = cronstable.config.parse_config_string(
        """
jobs:
  - name: test
    command: ls
    schedule: "* * * * *"
    onFailure:
      report:
        sentry:
          dsn:
            value: http://xxx:yyy@sentry/9
          environment: staging
          maxStringLength: 4096
""",
        "",
    )
    job_config = conf.jobs[0]
    report = job_config.onFailure["report"]
    job = Mock(
        config=job_config,
        stdout="out",
        stderr="err",
        retcode=0,
        template_vars={
            "name": job_config.name,
            "success": False,
            "fail_reason": "reasons",
            "stdout": "out",
            "stderr": "err",
            "environment": {},
        },
    )

    transports = []

    class FakeSentryTransport:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            self.messages_sent = []
            options = args[0] if args else kwargs.get("options", {})
            dsn = options.get("dsn")
            self.parsed_dsn = Dsn(dsn) if dsn else None

        def capture_envelope(self, envelope):
            event = envelope.get_event()
            if event is not None:
                self.messages_sent.append(event)

        def capture_event(self, event_opt):
            self.messages_sent.append(event_opt)

        def record_lost_event(self, *args, **kwargs):
            pass

        def is_healthy(self):
            return True

        def kill(self):
            pass

        def flush(self, *args, **kwargs):
            pass

    def make_transport(*args, **kwargs):
        transport = FakeSentryTransport(*args, **kwargs)
        transports.append(transport)
        return transport

    monkeypatch.setattr("sentry_sdk.client.make_transport", make_transport)

    await cronstable.job.SentryReporter().report(False, job, report)

    # maxStringLength was pushed into the sentry-sdk global
    assert sentry_sdk.utils.MAX_STRING_LENGTH == 4096
    # environment reached sentry_sdk.init -> the client options
    assert transports
    assert transports[-1].args[0].get("environment") == "staging"
    messages = [m for t in transports for m in t.messages_sent]
    assert len(messages) == 1
    msg = messages[0]
    assert msg["level"] == "error"  # default level
    assert msg["extra"]["job"] == "test"
    assert msg["extra"]["success"] is False


# ---------------------------------------------------------------------------
# MailReporter secret sources + skip/timeout branches
# ---------------------------------------------------------------------------


async def test_mail_report_password_from_file(tmp_path):
    pw = tmp_path / "smtp-pass"
    pw.write_text("filesecret\n")
    job_config, report = _mail_report_config()
    report["mail"]["password"] = {
        "value": None,
        "fromFile": str(pw),
        "fromEnvVar": None,
    }
    job = _mail_job_mock(job_config)

    login_calls, messages = await _run_mail_capturing(report, job)

    # the password read from the file (trailing newline stripped) is used
    assert login_calls == [("theuser", "filesecret")]
    assert len(messages) == 1


async def test_mail_report_html_body():
    job_config, report = _mail_report_config()
    report["mail"]["password"] = {
        "value": "pw",
        "fromFile": None,
        "fromEnvVar": None,
    }
    report["mail"]["html"] = True
    report["mail"]["body"] = "<b>hi</b>"
    job = _mail_job_mock(job_config)

    _login_calls, messages = await _run_mail_capturing(report, job)

    assert len(messages) == 1
    # html=True routes through set_content(..., subtype="html")
    assert messages[0].get_content_type() == "text/html"


async def test_mail_report_password_env_unset_is_skipped(monkeypatch, caplog):
    monkeypatch.delenv("TEST_SMTP_PW", raising=False)
    job_config, report = _mail_report_config()
    report["mail"]["password"] = {
        "value": None,
        "fromFile": None,
        "fromEnvVar": "TEST_SMTP_PW",
    }
    job = _mail_job_mock(job_config)

    def boom(*args, **kwargs):
        raise AssertionError("SMTP must not be constructed")

    with caplog.at_level(logging.ERROR, logger="cronstable"):
        with patch("aiosmtplib.SMTP", boom):
            await cronstable.job.MailReporter().report(False, job, report)

    assert any(
        "password env var is not set" in rec.getMessage()
        for rec in caplog.records
    )


async def test_mail_report_skips_empty_body_on_success():
    job_config, report = _mail_report_config()
    # a body that renders to nothing on success: the reporter must skip sending
    report["mail"]["body"] = "{% if not success %}only-on-failure{% endif %}"
    job = _mail_job_mock(job_config, success=True)

    def boom(*args, **kwargs):
        raise AssertionError("SMTP must not be constructed for an empty body")

    with patch("aiosmtplib.SMTP", boom):
        await cronstable.job.MailReporter().report(True, job, report)


async def test_mail_report_times_out(monkeypatch, caplog):
    monkeypatch.setattr(cronstable.job, "MAIL_REPORT_TIMEOUT", 0.1)
    job_config, report = _mail_report_config()
    job = _mail_job_mock(job_config)

    async def slow_connect(self):
        await asyncio.sleep(5)

    with caplog.at_level(logging.ERROR, logger="cronstable"):
        with patch("aiosmtplib.SMTP.connect", slow_connect):
            # must return (not raise) once the overall bound trips
            await asyncio.wait_for(
                cronstable.job.MailReporter().report(False, job, report), 10
            )

    assert any(
        "did not complete within" in rec.getMessage() for rec in caplog.records
    )


# ---------------------------------------------------------------------------
# ShellReporter: exec-with-shell, plain-shell, spawn error, timeout
# ---------------------------------------------------------------------------


def _shell_job_mock(job_config):
    return Mock(
        config=job_config,
        stdout="out",
        stderr="err",
        retcode=1,
        fail_reason="boom",
        failed=True,
        resource_usage=None,
        # a real RunningJob always carries these; set them so the mock encodes
        # to strings in the reporter's child env rather than raw Mock objects.
        run_id="run-mock",
        started_at=datetime(2026, 7, 22, 3, 0, 0, tzinfo=timezone.utc),
    )


async def test_shell_report_exec_with_explicit_shell(tmp_path):
    # an explicit `shell` on a string command routes through the exec branch as
    # [shell, "-c", command]; using the test interpreter as the shell keeps
    # this deterministic on every platform.
    marker = tmp_path / "ran"
    conf = cronstable.config.parse_config_string(_SHELL_JOB, "")
    report = conf.jobs[0].onFailure["report"]
    report["shell"]["shell"] = PYTHON
    report["shell"]["command"] = (
        "import pathlib; pathlib.Path({}).write_text('ok')".format(
            repr(str(marker))
        )
    )
    job = _shell_job_mock(conf.jobs[0])

    await cronstable.job.ShellReporter().report(
        False, job, conf.jobs[0].onFailure["report"]
    )

    assert marker.read_text() == "ok"  # the exec branch really ran the command


async def test_shell_report_plain_shell_nonzero_is_logged(caplog):
    # with no explicit shell, a string command runs through the plain-shell
    # branch (create_subprocess_shell); a nonzero exit is logged, not raised.
    # `exit 3` is valid in both POSIX sh and Windows cmd.
    conf = cronstable.config.parse_config_string(_SHELL_JOB, "")
    report = conf.jobs[0].onFailure["report"]
    report["shell"]["shell"] = ""
    report["shell"]["command"] = "exit 3"
    job = _shell_job_mock(conf.jobs[0])

    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await cronstable.job.ShellReporter().report(
            False, job, conf.jobs[0].onFailure["report"]
        )

    assert any("return code 3" in rec.getMessage() for rec in caplog.records)


async def test_shell_report_spawn_error_is_logged(caplog):
    # a missing reporter binary raises FileNotFoundError (an OSError) at spawn;
    # the reporter logs it and returns rather than crashing the reaper.
    conf = cronstable.config.parse_config_string(_SHELL_JOB, "")
    report = conf.jobs[0].onFailure["report"]
    report["shell"]["command"] = ["cronstable-no-such-reporter-binary-xyz"]
    job = _shell_job_mock(conf.jobs[0])

    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await cronstable.job.ShellReporter().report(
            False, job, conf.jobs[0].onFailure["report"]
        )

    assert any(
        "Error executing shell reporter" in rec.getMessage()
        for rec in caplog.records
    )


async def test_shell_report_timeout_kills_hanging_reporter(caplog):
    # a reporter command that never exits is killed once its timeout trips, and
    # report() returns instead of wedging the reaper.
    conf = cronstable.config.parse_config_string(_SHELL_JOB, "")
    report = conf.jobs[0].onFailure["report"]
    report["shell"]["shell"] = PYTHON
    report["shell"]["command"] = "import time; time.sleep(120)"
    report["shell"]["timeout"] = 0.5
    job = _shell_job_mock(conf.jobs[0])

    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await asyncio.wait_for(
            cronstable.job.ShellReporter().report(
                False, job, conf.jobs[0].onFailure["report"]
            ),
            30,
        )

    assert any(
        "did not finish within" in rec.getMessage() for rec in caplog.records
    )


async def test_shell_report_timeout_direct_kill_fallback(monkeypatch, caplog):
    # when the reporter times out and its process group cannot be signalled
    # (kill_process_group returns False), the reporter falls back to killing
    # the direct child.
    async def never_signalled(pid, *, force):
        return False

    monkeypatch.setattr(
        cronstable.platform, "kill_process_group", never_signalled
    )
    # Spy on the fallback itself. Asserting on the "did not finish within" log
    # line cannot test this: job.py emits it BEFORE the kill_process_group
    # call the patch above replaces, so such an assertion holds even with the
    # whole fallback branch deleted. It is also exactly what the sibling test
    # above asserts with no patch at all, which is what gave this one away.
    killed = []
    real_kill = asyncio.subprocess.Process.kill

    def spy_kill(self):
        killed.append(self.pid)
        return real_kill(self)

    monkeypatch.setattr(asyncio.subprocess.Process, "kill", spy_kill)

    conf = cronstable.config.parse_config_string(_SHELL_JOB, "")
    report = conf.jobs[0].onFailure["report"]
    report["shell"]["shell"] = PYTHON
    report["shell"]["command"] = "import time; time.sleep(120)"
    report["shell"]["timeout"] = 0.5
    job = _shell_job_mock(conf.jobs[0])

    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await asyncio.wait_for(
            cronstable.job.ShellReporter().report(
                False, job, conf.jobs[0].onFailure["report"]
            ),
            30,
        )

    assert killed, "direct-kill fallback did not run"
    assert any(
        "did not finish within" in rec.getMessage() for rec in caplog.records
    )


# ---------------------------------------------------------------------------
# RunningJob.cancel of a live process
# ---------------------------------------------------------------------------


async def test_cancel_terminates_a_running_job():
    conf = cronstable.config.parse_config_string(
        "jobs:\n  - name: test\n"
        + yaml_command(cmd_sleep(30))
        + '\n    schedule: "* * * * *"\n',
        "",
    )
    job = cronstable.job.RunningJob(conf.jobs[0], None)
    await job.start()
    assert job.proc is not None
    assert job.proc.returncode is None  # still running

    await asyncio.wait_for(job.cancel(), 20)

    # the child was signalled and reaped, so it is no longer running
    assert job.proc.returncode is not None
    assert job._terminated is True
    # draining afterwards completes the run without error
    await asyncio.wait_for(job.wait(), 20)


async def test_cancel_falls_back_to_direct_kill(monkeypatch):
    # where the process group cannot be signalled at all, cancel() must fall
    # back to the direct child: a graceful terminate (guarded against a
    # ProcessLookupError on an already-dead pid), then, after killTimeout, a
    # hard kill. Drive it with a stubbed process so the timeout/fallback
    # branches fire deterministically on every platform.
    async def never_signalled(pid, *, force):
        return False

    monkeypatch.setattr(
        cronstable.platform, "kill_process_group", never_signalled
    )
    job = _fresh_job()
    job.config.killTimeout = 0.3

    terminate_calls = []
    kill_calls = []

    proc = Mock()
    proc.pid = 4321
    proc.returncode = None

    def terminate():
        terminate_calls.append(True)
        raise ProcessLookupError()  # already reaped: must be swallowed

    def kill():
        kill_calls.append(True)

    async def wait():
        await asyncio.sleep(30)  # never returns before killTimeout

    proc.terminate = terminate
    proc.kill = kill
    proc.wait = wait
    job.proc = proc

    await asyncio.wait_for(job.cancel(), 10)

    assert terminate_calls == [True]  # graceful terminate attempted
    assert kill_calls == [True]  # then hard-killed after the timeout
    assert job._terminated is True


# ---------------------------------------------------------------------------
# live_resources / live_resource_series
# ---------------------------------------------------------------------------


def test_live_resources_none_without_monitor():
    job = _fresh_job()
    assert job._resource_monitor is None
    assert job.live_resources() is None
    assert job.live_resource_series() is None


def test_live_resources_delegate_to_monitor():
    job = _fresh_job()
    monitor = Mock()
    monitor.snapshot.return_value = {"cpu_seconds": 1.5}
    monitor.series.return_value = [[0.0, 1.0], [1.0, 2.0]]
    job._resource_monitor = monitor

    assert job.live_resources() == {"cpu_seconds": 1.5}
    assert job.live_resource_series() == [[0.0, 1.0], [1.0, 2.0]]


# ---------------------------------------------------------------------------
# fail_reason branches
# ---------------------------------------------------------------------------


def _set_fails_when(
    job, always=False, nonzero=False, stdout=False, stderr=False
):
    job.config.failsWhen = {
        "always": always,
        "nonzeroReturn": nonzero,
        "producesStdout": stdout,
        "producesStderr": stderr,
    }


def test_fail_reason_always():
    job = _fresh_job()
    _set_fails_when(job, always=True)
    assert job.fail_reason == "failsWhen=always"
    assert job.failed is True


def test_fail_reason_nonzero_return():
    job = _fresh_job()
    _set_fails_when(job, nonzero=True)
    job.retcode = 5
    assert job.fail_reason == ("failsWhen=nonzeroReturn and retcode=5")


def test_fail_reason_produces_stdout():
    job = _fresh_job()
    _set_fails_when(job, stdout=True)
    job.retcode = 0
    job.stdout = "some output\n"
    assert job.fail_reason == (
        "failsWhen=producesStdout and stdout is not empty"
    )


def test_fail_reason_produces_stdout_via_discarded_count():
    # even when stdout was all discarded (over saveLimit), the discard count
    # still counts as "produced output".
    job = _fresh_job()
    _set_fails_when(job, stdout=True)
    job.retcode = 0
    job.stdout = None
    job.stdout_discarded = 3
    assert job.fail_reason == (
        "failsWhen=producesStdout and stdout is not empty"
    )


def test_fail_reason_produces_stderr():
    job = _fresh_job()
    _set_fails_when(job, stderr=True)
    job.retcode = 0
    job.stderr = "an error\n"
    assert job.fail_reason == (
        "failsWhen=producesStderr and stderr is not empty"
    )


def test_fail_reason_none_when_no_condition_met():
    job = _fresh_job()
    _set_fails_when(job)  # all conditions off
    job.retcode = 0
    assert job.fail_reason is None
    assert job.failed is False


# ---------------------------------------------------------------------------
# _report_common exception handling
# ---------------------------------------------------------------------------


async def test_report_common_logs_reporter_exceptions(caplog):
    job = _fresh_job()

    class BoomReporter:
        async def report(self, success, job, config):
            raise RuntimeError("kaboom")

    class OkReporter:
        def __init__(self):
            self.calls = 0

        async def report(self, success, job, config):
            self.calls += 1

    ok = OkReporter()
    # shadow the class-level REPORTERS with our fakes for this instance
    job.REPORTERS = [BoomReporter(), ok]

    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await job._report_common({}, False)

    # the raising reporter is logged, and gather still runs the other reporter
    assert ok.calls == 1
    assert any(
        "Problem reporting job" in rec.getMessage() for rec in caplog.records
    )


# ---------------------------------------------------------------------------
# fuzzing findings: unspawnable argv must not kill the scheduler, and the
# object-form schedule must not disable the shell reporter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_failure_embedded_nul_reported_not_raised():
    # create_subprocess_exec raises ValueError('embedded null byte') for a
    # NUL in an argument -- not a SubprocessError, not an OSError -- and it
    # used to escape start() through the unguarded spawn_jobs path and kill
    # the daemon.  The crontab front end now refuses NULs at parse time, so
    # build the config directly: any future unspawnable argv must land as
    # an ordinary start failure.
    job_config = cronstable.config.JobConfig(
        cronstable.config.mergedicts(
            cronstable.config.DEFAULT_CONFIG,
            {
                "name": "nul",
                "command": "echo\x00hi",
                "schedule": "* * * * *",
            },
        )
    )
    job = cronstable.job.RunningJob(job_config, None)

    await job.start()  # must NOT raise
    assert job.proc is None
    assert job.start_failed

    await job.wait()
    assert job.retcode == 127
    assert job.failed


async def test_shell_report_runs_for_object_form_schedule(tmp_path):
    # schedule_unparsed is Union[str, dict]; the dict used to be placed
    # verbatim into the reporter's child env, dying in os.fsencode at spawn
    # -- so onFailure/onSuccess shell reports never executed for ANY job
    # whose schedule: was written in the object form.  The env value is now
    # the rendered crontab line, same as every other consumer.
    marker = tmp_path / "reporter-ran"
    conf = cronstable.config.parse_config_string(
        "jobs:\n"
        "  - name: objsched\n"
        "    command: echo hi\n"
        "    schedule:\n"
        '      minute: "*/5"\n'
        "    onFailure:\n"
        "      report:\n"
        "        shell:\n"
        '          command: "true"\n',
        "",
    )
    report = conf.jobs[0].onFailure["report"]
    report["shell"]["shell"] = PYTHON
    report["shell"]["command"] = (
        "import os, pathlib; pathlib.Path({}).write_text("
        "os.environ['CRONSTABLE_JOB_SCHEDULE'])".format(repr(str(marker)))
    )
    job = _shell_job_mock(conf.jobs[0])

    await cronstable.job.ShellReporter().report(False, job, report)

    # the reporter really ran, and saw the rendered 5-field line
    assert marker.read_text() == "*/5 * * * *"


# ---------------------------------------------------------------------------
# SLA breach reporting: SlaBreachContext + report_sla_breach (the onLate
# hook). The context must satisfy all four reporters without a RunningJob.
# ---------------------------------------------------------------------------


_SLA_MAIL_JOB = """
jobs:
  - name: test
    command: ls
    schedule: "* * * * *"
    sla:
      lateAfterSeconds: 120
    onLate:
      report:
        mail:
          from: example@foo.com
          to: example@bar.com
          smtpHost: smtp1
          smtpPort: 1025
"""

_SLA_SHELL_JOB = """
jobs:
  - name: test
    command: echo the-command
    schedule: "*/5 * * * *"
    sla:
      lateAfterSeconds: 120
    onLate:
      report:
        shell:
          command: "true"
"""

_SLA_PLAIN_JOB = """
jobs:
  - name: test
    command: ls
    schedule: "* * * * *"
    sla:
      maxRuntimeSeconds: 60
"""


def _breach_ctx(job_config, check="lateAfter"):
    return cronstable.job.SlaBreachContext(
        job_config,
        check=check,
        threshold_seconds=120,
        observed_seconds=300.5,
        last_success_at="2020-01-01T10:00:00+00:00",
    )


def test_sla_breach_context_full_template_var_contract(monkeypatch):
    # the full standard key set with None/False fills, so operator templates
    # written for onFailure render unchanged on onLate, plus the breach vars.
    # A sentinel host pins where `host` comes from (asserting against
    # report_hostname()'s own expression would prove nothing). Set before the
    # context is built: SlaBreachContext captures env at construction.
    monkeypatch.setenv("HOSTNAME", "host-sentinel-2")
    conf = cronstable.config.parse_config_string(_SLA_MAIL_JOB, "")
    ctx = _breach_ctx(conf.jobs[0])
    tv = ctx.template_vars
    assert set(tv) == {
        "name",
        "success",
        "fail_reason",
        "stdout",
        "stderr",
        "exit_code",
        "command",
        "shell",
        "environment",
        "host",
        "schedule",
        "started_at",
        "run_id",
        "cpu_seconds",
        "cpu_user_seconds",
        "cpu_system_seconds",
        "max_rss_bytes",
        "sla_check",
        "threshold_seconds",
        "observed_seconds",
        "last_success_at",
    }
    assert tv["name"] == "test"
    assert tv["success"] is False
    assert tv["fail_reason"] == "sla: lateAfter breached"
    assert tv["stdout"] is None
    assert tv["stderr"] is None
    assert tv["exit_code"] is None
    assert tv["cpu_seconds"] is None
    assert tv["max_rss_bytes"] is None
    # a breach describes a job that did NOT run: no start instant, no run id.
    assert tv["started_at"] is None
    assert tv["run_id"] is None
    assert tv["host"] == "host-sentinel-2"
    assert tv["schedule"] == "* * * * *"
    assert tv["sla_check"] == "lateAfter"
    assert tv["threshold_seconds"] == 120
    assert tv["observed_seconds"] == 300.5
    assert tv["last_success_at"] == "2020-01-01T10:00:00+00:00"
    # HOSTNAME rides env so the default sentry fingerprint's
    # {{ environment.HOSTNAME }} line keeps its host dimension.
    assert tv["environment"]["HOSTNAME"] == "host-sentinel-2"
    # the explicit run-shaped fills the reporters read directly
    assert ctx.failed is True
    assert ctx.retcode is None
    assert ctx.resource_usage is None
    assert ctx.stdout_discarded == 0
    assert ctx.stderr_discarded == 0


@pytest.mark.asyncio
async def test_sla_breach_mail_report_renders_late_templates():
    # the onLate defaults swap the completed/failed wording for the overdue
    # templates; success=False means the empty-body suppression cannot bite.
    conf = cronstable.config.parse_config_string(_SLA_MAIL_JOB, "")
    job_config = conf.jobs[0]
    ctx = _breach_ctx(job_config)

    messages_sent = []

    async def connect(self):
        pass

    async def send_message(self, message):
        messages_sent.append(message)

    with (
        patch("aiosmtplib.SMTP.connect", connect),
        patch("aiosmtplib.SMTP.send_message", send_message),
    ):
        await cronstable.job.MailReporter().report(
            False, ctx, job_config.onLate["report"]
        )

    assert len(messages_sent) == 1
    message = messages_sent[0]
    assert message["Subject"] == "Cron job 'test' is overdue (lateAfter)"
    body = message.get_payload()
    assert "SLA check: lateAfter" in body
    assert "Threshold: 120 seconds" in body
    assert "Observed: 300.5 seconds" in body
    assert "Last success: 2020-01-01T10:00:00+00:00" in body


@pytest.mark.asyncio
async def test_sla_breach_mail_body_without_last_success():
    conf = cronstable.config.parse_config_string(_SLA_MAIL_JOB, "")
    job_config = conf.jobs[0]
    ctx = cronstable.job.SlaBreachContext(
        job_config,
        check="maxTimeSinceSuccess",
        threshold_seconds=3600,
        observed_seconds=7200.0,
        last_success_at=None,
    )

    messages_sent = []

    async def connect(self):
        pass

    async def send_message(self, message):
        messages_sent.append(message)

    with (
        patch("aiosmtplib.SMTP.connect", connect),
        patch("aiosmtplib.SMTP.send_message", send_message),
    ):
        await cronstable.job.MailReporter().report(
            False, ctx, job_config.onLate["report"]
        )

    (message,) = messages_sent
    assert message["Subject"] == (
        "Cron job 'test' is overdue (maxTimeSinceSuccess)"
    )
    assert "Last success: (none recorded)" in message.get_payload()


@pytest.mark.asyncio
async def test_sla_breach_shell_report_exports_sla_env(monkeypatch):
    for key in [k for k in os.environ if k.startswith("CRONSTABLE_")]:
        monkeypatch.delenv(key, raising=False)

    captured = {}

    async def fake_create(*args, **kwargs):
        captured["env"] = kwargs["env"]
        proc = Mock()

        async def wait():
            return 0

        proc.wait = wait
        return proc

    monkeypatch.setattr("asyncio.create_subprocess_shell", fake_create)
    monkeypatch.setattr("asyncio.create_subprocess_exec", fake_create)

    conf = cronstable.config.parse_config_string(_SLA_SHELL_JOB, "")
    job_config = conf.jobs[0]
    ctx = _breach_ctx(job_config)
    await cronstable.job.ShellReporter().report(
        False, ctx, job_config.onLate["report"]
    )

    env = captured["env"]
    assert env["CRONSTABLE_SLA_CHECK"] == "lateAfter"
    assert env["CRONSTABLE_SLA_THRESHOLD_SECONDS"] == "120"
    assert env["CRONSTABLE_SLA_OBSERVED_SECONDS"] == "300.5"
    assert env["CRONSTABLE_LAST_SUCCESS_AT"] == "2020-01-01T10:00:00+00:00"
    # the run-shaped variables render the breach context faithfully: failed
    # with the sla fail_reason, no retcode ("None" per str(None)), no output
    assert env["CRONSTABLE_FAILED"] == "1"
    assert env["CRONSTABLE_RETCODE"] == "None"
    assert env["CRONSTABLE_FAIL_REASON"] == "sla: lateAfter breached"
    assert env["CRONSTABLE_STDOUT"] == ""
    assert env["CRONSTABLE_STDERR"] == ""
    assert env["CRONSTABLE_CPU_SECONDS"] == ""


@pytest.mark.asyncio
async def test_sla_breach_webhook_report_default_late_body():
    import json

    server = _WebhookServer()
    async with server as url:
        conf = cronstable.config.parse_config_string(
            f"""
jobs:
  - name: test
    command: ls
    schedule: "* * * * *"
    sla:
      lateAfterSeconds: 120
    onLate:
      report:
        webhook:
          url:
            value: {url}
""",
            "",
        )
        job_config = conf.jobs[0]
        ctx = _breach_ctx(job_config)
        await cronstable.job.WebhookReporter().report(
            False, ctx, job_config.onLate["report"]
        )

    (request,) = server.requests
    assert request["method"] == "POST"
    # the default onLate webhook body renders the overdue subject + breach
    # detail as valid JSON in the Slack-compatible {"text": ...} shape
    payload = json.loads(request["body"])
    assert set(payload.keys()) == {"text"}
    assert payload["text"].startswith("Cron job 'test' is overdue (lateAfter)")
    assert "SLA check: lateAfter" in payload["text"]
    assert "Threshold: 120 seconds" in payload["text"]


_SLA_SENTRY_JOB = """
jobs:
  - name: test
    command: ls
    schedule: "* * * * *"
    sla:
      lateAfterSeconds: 120
    onLate:
      report:
        sentry:
          dsn:
            value: http://xxx:yyy@sentry/1
"""


@pytest.mark.asyncio
async def test_sla_breach_sentry_report_uses_sla_fingerprint(monkeypatch):
    # the dsn must be declared in YAML: a parsed job's unoverridden report
    # subtrees alias DEFAULT_CONFIG, so an in-place dsn write here would
    # poison every config parsed later in the process
    conf = cronstable.config.parse_config_string(_SLA_SENTRY_JOB, "")
    job_config = conf.jobs[0]
    ctx = _breach_ctx(job_config)

    transports = []

    class FakeSentryTransport:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            self.messages_sent = []
            options = args[0] if args else kwargs.get("options", {})
            dsn = options.get("dsn")
            self.parsed_dsn = Dsn(dsn) if dsn else None

        def capture_envelope(self, envelope):
            event = envelope.get_event()
            if event is not None:
                self.messages_sent.append(event)

        def capture_event(self, event_opt):
            self.messages_sent.append(event_opt)

        def record_lost_event(self, *args, **kwargs):
            pass

        def is_healthy(self):
            return True

        def kill(self):
            pass

        def flush(self, *args, **kwargs):
            pass

    def make_transport(*args, **kwargs):
        transport = FakeSentryTransport(*args, **kwargs)
        transports.append(transport)
        return transport

    monkeypatch.setattr("sentry_sdk.client.make_transport", make_transport)

    await cronstable.job.SentryReporter().report(
        False, ctx, job_config.onLate["report"]
    )

    messages_sent = [
        msg for transport in transports for msg in transport.messages_sent
    ]
    assert len(messages_sent) == 1
    msg = messages_sent[0]
    # breaches group under their own default fingerprint, never folded into
    # this job's run failures, and success=False defaults the level to error
    assert msg["fingerprint"] == ["cronstable", "sla", "test"]
    assert msg["level"] == "error"
    assert msg["extra"]["job"] == "test"
    assert msg["extra"]["exit_code"] is None
    assert msg["extra"]["success"] is False


@pytest.mark.asyncio
async def test_report_sla_breach_runs_all_four_real_reporters():
    # every real reporter accepts the context and early-returns on its null
    # default config: the whole default onLate block is a safe no-op.
    conf = cronstable.config.parse_config_string(_SLA_PLAIN_JOB, "")
    job_config = conf.jobs[0]
    ctx = _breach_ctx(job_config, check="maxRuntime")
    await cronstable.job.report_sla_breach(ctx, job_config.onLate["report"])


@pytest.mark.asyncio
async def test_report_sla_breach_gathers_and_logs_exceptions(
    monkeypatch, caplog
):
    conf = cronstable.config.parse_config_string(_SLA_PLAIN_JOB, "")
    ctx = _breach_ctx(conf.jobs[0], check="maxRuntime")

    calls = []

    class BoomReporter:
        async def report(self, success, job, config):
            raise RuntimeError("kaboom")

    class OkReporter:
        async def report(self, success, job, config):
            calls.append((success, job, config))

    monkeypatch.setattr(
        cronstable.job.RunningJob, "REPORTERS", [BoomReporter(), OkReporter()]
    )
    report_config = {"marker": True}
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await cronstable.job.report_sla_breach(ctx, report_config)

    # success=False throughout, the config passed along verbatim, and the
    # raising reporter logged without stopping the others
    assert calls == [(False, ctx, report_config)]
    assert any(
        "Problem reporting job" in rec.getMessage() for rec in caplog.records
    )
