import asyncio
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
