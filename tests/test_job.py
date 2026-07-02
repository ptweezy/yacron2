import asyncio
import logging
import os
import tempfile
from unittest.mock import Mock, patch

import aiosmtplib
import pytest
from sentry_sdk.utils import Dsn

import yacron2.config
import yacron2.job
import yacron2.statsd
from tests._commands import (
    cmd_print,
    cmd_print_sleep_print,
    cmd_sleep,
    cmd_write_env,
    yaml_command,
)
from yacron2.platform import DEFAULT_SHELL, IS_WINDOWS


def _argv(*parts):
    """Expected subprocess argv for this platform (str on Windows, bytes on
    POSIX) -- mirrors yacron2.platform.encode_argv."""
    return tuple(parts) if IS_WINDOWS else tuple(p.encode() for p in parts)


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
    reader = yacron2.job.StreamReader(
        "cronjob-1", "stderr", fake_stream, "", save_limit
    )

    conf = yacron2.config.parse_config_string(
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
    job = yacron2.job.RunningJob(job_config, None)

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
    reader = yacron2.job.StreamReader(
        "cronjob-1", "stderr", fake_stream, "", 500
    )

    conf = yacron2.config.parse_config_string(
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
    job = yacron2.job.RunningJob(job_config, None)

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
    out = yacron2.job.JobOutputStream()
    queue = out.subscribe()
    out.publish("stdout", "hello\n")
    out.publish("stderr", "oops\n")
    assert queue.get_nowait() == ("stdout", "hello\n")
    assert queue.get_nowait() == ("stderr", "oops\n")
    # the ring buffer retains lines for late viewers
    assert list(out.lines) == [("stdout", "hello\n"), ("stderr", "oops\n")]


@pytest.mark.asyncio
async def test_job_output_stream_close_delivers_sentinel():
    out = yacron2.job.JobOutputStream()
    queue = out.subscribe()
    out.publish("stdout", "line\n")
    out.close()
    assert queue.get_nowait() == ("stdout", "line\n")
    assert queue.get_nowait() is None  # end-of-stream sentinel


@pytest.mark.asyncio
async def test_job_output_stream_late_subscriber_gets_sentinel():
    # subscribing after the run finished must not block forever: the new
    # subscriber receives the end sentinel immediately, after the buffer.
    out = yacron2.job.JobOutputStream()
    out.publish("stdout", "done\n")
    out.close()
    queue = out.subscribe()
    assert queue.get_nowait() is None
    assert list(out.lines) == [("stdout", "done\n")]


@pytest.mark.asyncio
async def test_job_output_stream_ring_buffer_bounds():
    out = yacron2.job.JobOutputStream(limit=3)
    for i in range(5):
        out.publish("stdout", f"line {i}\n")
    # only the most recent `limit` lines are retained
    assert list(out.lines) == [
        ("stdout", "line 2\n"),
        ("stdout", "line 3\n"),
        ("stdout", "line 4\n"),
    ]


@pytest.mark.asyncio
async def test_stream_reader_publishes_to_output():
    # the on_line hook wires StreamReader output into a JobOutputStream so the
    # web UI can tail lines live as the job produces them.
    out = yacron2.job.JobOutputStream()
    fake_stream = asyncio.StreamReader()
    reader = yacron2.job.StreamReader(
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
    conf = yacron2.config.parse_config_string(A_JOB, "")
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

    mail = yacron2.job.MailReporter()

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
    conf = yacron2.config.parse_config_string(A_JOB, "")
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
        yacron2.config.DEFAULT_CONFIG["onFailure"]["report"]["sentry"]["body"]
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

    sentry = yacron2.job.SentryReporter()
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

        conf = yacron2.config.parse_config_string(
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
        )

        shell_reporter = yacron2.job.ShellReporter()

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
        conf = yacron2.config.parse_config_string(
            _webhook_job_config(
                f"            value: {url}",
                extra=(
                    "          headers:\n"
                    "            X-Custom: yes-hello"
                ),
            ),
            "",
        )
        job_config = conf.jobs[0]
        job = _webhook_job(job_config, success=success)

        reporter = yacron2.job.WebhookReporter()
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
        conf = yacron2.config.parse_config_string(
            _webhook_job_config(url_yaml), ""
        )
        job_config = conf.jobs[0]
        job = _webhook_job(job_config)

        await yacron2.job.WebhookReporter().report(
            False, job, job_config.onFailure["report"]
        )

    assert len(server.requests) == 1


@pytest.mark.asyncio
async def test_report_webhook_disabled():
    # with no url source configured (the default), the reporter must return
    # early without opening any HTTP session
    conf = yacron2.config.parse_config_string(
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
        await yacron2.job.WebhookReporter().report(
            False, job, job_config.onFailure["report"]
        )


@pytest.mark.asyncio
async def test_report_webhook_env_var_not_set(monkeypatch, caplog):
    monkeypatch.delenv("TEST_WEBHOOK_URL", raising=False)
    conf = yacron2.config.parse_config_string(
        _webhook_job_config("            fromEnvVar: TEST_WEBHOOK_URL"), ""
    )
    job_config = conf.jobs[0]
    job = _webhook_job(job_config)

    def no_session(*args, **kwargs):
        raise AssertionError("ClientSession must not be created")

    with patch("aiohttp.ClientSession", no_session):
        with caplog.at_level(logging.ERROR, logger="yacron2"):
            await yacron2.job.WebhookReporter().report(
                False, job, job_config.onFailure["report"]
            )
    assert any(
        "url env var" in rec.message for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_report_webhook_http_error(caplog):
    # a non-2xx response is logged at ERROR (with the response text) but must
    # not raise out of the reporter
    server = _WebhookServer(status=500)
    async with server as url:
        conf = yacron2.config.parse_config_string(
            _webhook_job_config(f"            value: {url}"), ""
        )
        job_config = conf.jobs[0]
        job = _webhook_job(job_config)

        with caplog.at_level(logging.ERROR, logger="yacron2"):
            await yacron2.job.WebhookReporter().report(
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
        conf = yacron2.config.parse_config_string(
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

        await yacron2.job.WebhookReporter().report(
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

    conf = yacron2.config.parse_config_string(
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

    job = yacron2.job.RunningJob(job_config, None)

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
async def test_execution_timeout():
    conf = yacron2.config.parse_config_string(
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
    job = yacron2.job.RunningJob(job_config, None)
    await job.start()
    await job.wait()
    assert job.stdout == "hello\n"


@pytest.mark.asyncio
async def test_error1():
    conf = yacron2.config.parse_config_string(
        "jobs:\n  - name: test\n"
        + yaml_command(cmd_sleep(5))
        + '\n    schedule: "* * * * *"\n',
        "",
    )
    job_config = conf.jobs[0]
    job = yacron2.job.RunningJob(job_config, None)

    await job.start()
    with pytest.raises(RuntimeError):
        await job.start()
    await job.cancel()


@pytest.mark.asyncio
async def test_error2():
    conf = yacron2.config.parse_config_string(
        "jobs:\n  - name: test\n"
        + yaml_command(cmd_sleep(5))
        + '\n    schedule: "* * * * *"\n',
        "",
    )
    job_config = conf.jobs[0]
    job = yacron2.job.RunningJob(job_config, None)

    with pytest.raises(RuntimeError):
        await job.wait()


@pytest.mark.asyncio
async def test_error3():
    conf = yacron2.config.parse_config_string(
        "jobs:\n  - name: test\n"
        + yaml_command(cmd_sleep(5))
        + '\n    schedule: "* * * * *"\n',
        "",
    )
    job_config = conf.jobs[0]
    job = yacron2.job.RunningJob(job_config, None)

    with pytest.raises(RuntimeError):
        await job.cancel()


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

        conf = yacron2.config.parse_config_string(
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

        job = yacron2.job.RunningJob(job_config, None)

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
async def test_start_failure_reported_not_raised():
    # A command that cannot be launched (e.g. it does not exist) must be
    # treated as a normal job failure with exit code 127, not raise
    # RuntimeError (which the reaper logs as "please report this as a bug").
    conf = yacron2.config.parse_config_string(
        """
jobs:
  - name: test
    command:
      - /this/command/definitely/does/not/exist
    schedule: "* * * * *"
""",
        "",
    )
    job = yacron2.job.RunningJob(conf.jobs[0], None)

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
    conf = yacron2.config.parse_config_string(
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
    job = yacron2.job.RunningJob(conf.jobs[0], None)

    await job.start()  # must NOT raise
    assert job.proc is None
    assert job.start_failed

    await job.wait()
    assert job.retcode == 127
    assert job.failed


@pytest.mark.asyncio
async def test_statsd_failure_does_not_crash(monkeypatch):
    # statsd is best-effort: a send error (e.g. an unresolvable host) must be
    # swallowed and not propagate out of start()/wait() to crash the scheduler.
    async def boom(*args, **kwargs):
        raise OSError("statsd unreachable")

    monkeypatch.setattr(yacron2.statsd, "send_to_statsd", boom)

    conf = yacron2.config.parse_config_string(
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
    job = yacron2.job.RunningJob(conf.jobs[0], None)

    await job.start()  # _on_start must swallow the OSError
    await job.wait()  # _on_stop must swallow the OSError
    assert job.retcode == 0


@pytest.mark.asyncio
async def test_report_mail_closes_connection_on_error():
    # if sending fails, the SMTP connection must still be closed (no leak).
    conf = yacron2.config.parse_config_string(A_JOB, "")
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

    mail = yacron2.job.MailReporter()
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
    conf = yacron2.config.parse_config_string(_SIMPLE_JOB, "")
    job = yacron2.job.RunningJob(conf.jobs[0], None)
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
    # internal yacron2 bug.
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

    conf = yacron2.config.parse_config_string(
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
    job = yacron2.job.RunningJob(conf.jobs[0], None)
    job.config.uid = 1000
    await job.start()
    assert captured[-1].get("preexec_fn") == job._demote

    # with neither uid nor gid, preexec_fn must NOT be passed: it is needless
    # overhead on every spawn and an outright error on some platforms.
    job2 = yacron2.job.RunningJob(conf.jobs[0], None)
    job2.config.uid = None
    job2.config.gid = None
    await job2.start()
    assert "preexec_fn" not in captured[-1]


# ---------------------------------------------------------------------------
# Shell-reporter YACRON2_* env contract.
#
# Users' alerting scripts read these exact variable names; a rename or typo,
# or an inverted truncation flag, breaks them silently with the suite green.
# The pre-existing shell-reporter test reads back only 4 of 10 variables and
# never exercises truncation. These lock the full name set, the values, and the
# 16 KiB truncation behavior.
# ---------------------------------------------------------------------------

GOLDEN_SHELL_ENV_KEYS = frozenset(
    {
        "YACRON2_FAIL_REASON",
        "YACRON2_JOB_NAME",
        "YACRON2_JOB_COMMAND",
        "YACRON2_JOB_SCHEDULE",
        "YACRON2_FAILED",
        "YACRON2_RETCODE",
        "YACRON2_STDERR",
        "YACRON2_STDOUT",
        "YACRON2_STDERR_TRUNCATED",
        "YACRON2_STDOUT_TRUNCATED",
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
    # Drop any ambient YACRON2_* so the exact-set assertion is deterministic
    # regardless of how the suite was launched.
    for key in [k for k in os.environ if k.startswith("YACRON2_")]:
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

    conf = yacron2.config.parse_config_string(_SHELL_REPORTER_JOB, "")
    job_config = conf.jobs[0]
    job = Mock(
        config=job_config,
        stdout=stdout,
        stderr=stderr,
        retcode=retcode,
        fail_reason=fail_reason,
        failed=failed,
    )
    await yacron2.job.ShellReporter().report(
        False, job, job_config.onFailure["report"]
    )
    return captured["env"]


@pytest.mark.asyncio
async def test_report_shell_full_env_contract(monkeypatch):
    env = await _capture_shell_reporter_env(
        monkeypatch, stdout="out", stderr="err"
    )

    # exactly these YACRON2_* variables are exported, no more and no fewer: a
    # dropped, renamed, or added variable fails here.
    assert {
        k for k in env if k.startswith("YACRON2_")
    } == GOLDEN_SHELL_ENV_KEYS

    assert env["YACRON2_JOB_NAME"] == "test"
    assert env["YACRON2_JOB_COMMAND"] == "echo the-command"
    assert env["YACRON2_JOB_SCHEDULE"] == "*/5 * * * *"
    assert env["YACRON2_FAILED"] == "1"
    assert env["YACRON2_RETCODE"] == "7"
    assert env["YACRON2_FAIL_REASON"] == "boom"
    assert env["YACRON2_STDOUT"] == "out"
    assert env["YACRON2_STDERR"] == "err"
    assert env["YACRON2_STDOUT_TRUNCATED"] == "0"
    assert env["YACRON2_STDERR_TRUNCATED"] == "0"


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
    assert env["YACRON2_FAILED"] == "0"
    assert env["YACRON2_RETCODE"] == "0"
    assert env["YACRON2_FAIL_REASON"] == ""
    assert env["YACRON2_STDOUT"] == ""
    assert env["YACRON2_STDERR"] == ""


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
    assert env["YACRON2_STDOUT_TRUNCATED"] == exp_out_trunc
    assert env["YACRON2_STDERR_TRUNCATED"] == exp_err_trunc
    assert len(env["YACRON2_STDOUT"]) == exp_out_len
    assert len(env["YACRON2_STDERR"]) == exp_err_len


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
    assert env["YACRON2_STDOUT_TRUNCATED"] == "0"
    assert env["YACRON2_STDERR_TRUNCATED"] == "0"
    assert len(env["YACRON2_STDOUT"]) == each
    assert len(env["YACRON2_STDERR"]) == each
