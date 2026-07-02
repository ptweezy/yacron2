import asyncio
import asyncio.subprocess
import logging
import os
import subprocess
import sys
import time
from collections import deque
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import format_datetime
from functools import lru_cache
from socket import gethostname
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

import aiohttp
import aiosmtplib
import jinja2
import sentry_sdk
import sentry_sdk.utils

from yacron2 import platform
from yacron2.config import JobConfig
from yacron2.statsd import StatsdJobMetricWriter

logger = logging.getLogger("yacron2")


@lru_cache(maxsize=None)
def _compiled_template(source: str) -> jinja2.Template:
    # Template source strings come from config and are constant for the life
    # of the process; compile each distinct one once and reuse it.
    return jinja2.Template(source)


if "HOSTNAME" not in os.environ:
    os.environ["HOSTNAME"] = gethostname()


def fixup_pyinstaller_env(env: Dict[str, str]) -> None:
    # check for pyinstaller env, fix clobbered env vars
    # https://github.com/gjcarneiro/yacron/issues/68
    # These are the dynamic-loader paths PyInstaller rewrites on POSIX; the
    # Windows bootloader doesn't touch them, so there's nothing to restore.
    if getattr(sys, "frozen", False) and not platform.IS_WINDOWS:
        for env_var in "LD_LIBRARY_PATH", "LIBPATH":
            env[env_var] = env.get(f"{env_var}_ORIG", "")


# How many of the most recent output lines a JobOutputStream retains for the
# live web log tail. Independent of saveLimit (which bounds the text kept for
# failure reports); this only bounds the in-memory buffer the UI streams from.
LIVE_LOG_LIMIT = 1000


class JobOutputStream:
    """In-memory, broadcastable view of a job run's captured output.

    Lines are appended as the job produces them (see ``StreamReader``) and
    pushed to any live subscribers — the web UI's log tail. A bounded ring
    buffer of the most recent lines is retained so a viewer that connects
    mid-run, or just after the run finished, still sees recent context.

    Nothing is ever written to disk: this lives only for as long as the run's
    record is kept in memory, preserving yacron2's read-only-filesystem
    deployment story.
    """

    def __init__(self, limit: int = LIVE_LOG_LIMIT) -> None:
        # each item is (stream_name, line) with stream_name "stdout"/"stderr"
        self.lines: Deque[Tuple[str, str]] = deque(maxlen=limit)
        self._subscribers: List["asyncio.Queue"] = []
        self.closed = False

    def publish(self, stream_name: str, line: str) -> None:
        item = (stream_name, line)
        self.lines.append(item)
        for queue in self._subscribers:
            queue.put_nowait(item)

    def subscribe(self) -> "asyncio.Queue":
        queue: asyncio.Queue = asyncio.Queue()
        self._subscribers.append(queue)
        if self.closed:
            # the run already finished: deliver the end sentinel immediately so
            # a late subscriber's read loop terminates after the buffered
            # snapshot instead of blocking on a stream that will never produce
            # another line.
            queue.put_nowait(None)
        return queue

    def unsubscribe(self, queue: "asyncio.Queue") -> None:
        try:
            self._subscribers.remove(queue)
        except ValueError:
            pass

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        # None is the end-of-stream sentinel for subscriber read loops.
        for queue in self._subscribers:
            queue.put_nowait(None)


class StreamReader:
    def __init__(
        self,
        job_name: str,
        stream_name: str,
        stream: asyncio.StreamReader,
        stream_prefix: str,
        save_limit: int,
        on_line: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        self.save_top: List[str] = []
        self.save_bottom: Deque[str] = deque()
        self.job_name = job_name
        self.save_limit = save_limit
        self.stream_name = stream_name
        self.stream_prefix = stream_prefix
        # called with (stream_name, line) for each line read, so a live viewer
        # (the web UI) can tail output as the job produces it.
        self.on_line = on_line
        self._reader = asyncio.create_task(self._read(stream))
        self.discarded_lines = 0

    @staticmethod
    def _emit(out_stream, out_line: str) -> None:
        # Write bytes so we control the encoding; fall back to ASCII with
        # replacement when the console encoding can't represent the text.
        try:
            out_stream.buffer.write(out_line.encode())
        except UnicodeEncodeError:
            safe = out_line.encode("ascii", "replace").decode("ascii")
            out_stream.write(safe)
        out_stream.flush()

    async def _read(self, stream):
        prefix = self.stream_prefix.format(
            job_name=self.job_name, stream_name=self.stream_name
        )
        limit_top = self.save_limit // 2
        limit_bottom = self.save_limit - limit_top
        while True:
            try:
                # errors="replace" so a job emitting non-UTF-8 bytes does not
                # crash the reader task with UnicodeDecodeError.
                line = (await stream.readline()).decode(
                    "utf-8", errors="replace"
                )
            except ValueError:
                logger.warning(
                    "job %s: ignored a very long line", self.job_name
                )
                continue
            if not line:
                return
            if self.on_line is not None:
                self.on_line(self.stream_name, line)
            out_line = prefix + line
            if self.stream_name == "stdout":
                self._emit(sys.stdout, out_line)
            elif self.stream_name == "stderr":
                self._emit(sys.stderr, out_line)
            if self.save_limit > 0:
                if len(self.save_top) < limit_top:
                    self.save_top.append(line)
                else:
                    # deque(maxlen) would evict silently; track discards
                    # explicitly to preserve the "N lines discarded" count.
                    if len(self.save_bottom) == limit_bottom:
                        self.save_bottom.popleft()
                        self.discarded_lines += 1
                    self.save_bottom.append(line)
            else:
                self.discarded_lines += 1

    async def join(self) -> Tuple[str, int]:
        await self._reader
        if self.save_bottom:
            middle = (
                [
                    "   [.... {} lines discarded ...]\n".format(
                        self.discarded_lines
                    )
                ]
                if self.discarded_lines
                else []
            )
            output = "".join(self.save_top + middle + list(self.save_bottom))
        else:
            output = "".join(self.save_top)
        return output, self.discarded_lines


class Reporter:
    async def report(
        self, success: bool, job: "RunningJob", config: Dict[str, Any]
    ) -> None:
        raise NotImplementedError  # pragma: no cover


class SentryReporter(Reporter):
    def __init__(self) -> None:
        # Remember the last (dsn, environment) we initialized the global
        # Sentry client with, so we don't rebuild the client/transport on
        # every single report.
        self._inited_key: Optional[Tuple[str, Optional[str]]] = None

    async def report(
        self, success: bool, job: "RunningJob", config: Dict[str, Any]
    ) -> None:
        config = config["sentry"]
        if config["dsn"]["value"]:
            dsn = config["dsn"]["value"]
        elif config["dsn"]["fromFile"]:
            with open(config["dsn"]["fromFile"], "rt") as dsn_file:
                dsn = dsn_file.read().strip()
        elif config["dsn"]["fromEnvVar"]:
            env_var = config["dsn"]["fromEnvVar"]
            dsn = os.environ.get(env_var, "")
            if not dsn:
                logger.error(
                    "sentry: dsn env var %r is not set; not reporting",
                    env_var,
                )
                return
        else:
            return  # sentry disabled: early return

        template = _compiled_template(config["body"])
        body = template.render(job.template_vars)

        fingerprint = []
        for line in config["fingerprint"]:
            fingerprint.append(
                _compiled_template(line).render(job.template_vars)
            )

        kwargs = {}
        if config.get("maxStringLength"):
            sentry_sdk.utils.MAX_STRING_LENGTH = (  # type:ignore
                config["maxStringLength"]
            )
        if config.get("environment"):
            kwargs["environment"] = config["environment"]
        init_key = (dsn, kwargs.get("environment"))
        if init_key != self._inited_key:
            sentry_sdk.init(dsn=dsn, **kwargs)
            self._inited_key = init_key
        extra = {
            "job": job.config.name,
            "exit_code": job.retcode,
            "command": job.config.command,
            "shell": job.config.shell,
            "success": success,
        }
        extra.update(config.get("extra", {}))
        logger.debug(
            "sentry: fingerprint=%r; extra=%r' body:\n%s",
            fingerprint,
            extra,
            body,
        )
        with sentry_sdk.new_scope() as scope:
            for key, val in extra.items():
                scope.set_extra(key, val)
            scope.fingerprint = fingerprint
            sentry_sdk.capture_message(
                body, level=config.get("level", "error")
            )


class MailReporter(Reporter):
    async def report(
        self, success: bool, job: "RunningJob", config: Dict[str, Any]
    ) -> None:
        mail = config["mail"]
        if not (mail["to"] and mail["from"]):
            return  # email reporting disabled
        smtp_host = mail["smtpHost"]
        smtp_port = mail["smtpPort"]

        password = None  # type: Optional[str]
        username = None  # type: Optional[str]

        if mail["password"]["value"]:
            password = mail["password"]["value"]
        elif mail["password"]["fromFile"]:
            with open(mail["password"]["fromFile"], "rt") as pass_file:
                password = pass_file.read().strip()
        elif mail["password"]["fromEnvVar"]:
            env_var = mail["password"]["fromEnvVar"]
            password = os.environ.get(env_var)
            if not password:
                # The env var *name* is config-derived and tied to a secret,
                # so we don't echo it to the logs.
                logger.error(
                    "mail: password env var is not set; not sending email"
                )
                return
        else:
            password = None
        username = mail.get("username")

        tmpl_vars = job.template_vars
        body_tmpl = _compiled_template(mail["body"])
        body = body_tmpl.render(tmpl_vars)
        if success and not body.strip():
            logger.debug("body is empty, not sending email")
            return
        subject_tmpl = _compiled_template(mail["subject"])
        subject = subject_tmpl.render(tmpl_vars)

        logger.debug("smtp: host=%r, port=%r", smtp_host, smtp_port)
        message = EmailMessage()
        message["From"] = mail["from"]
        message["To"] = mail["to"].strip()
        message["Subject"] = subject.strip()
        # RFC 5322 date, e.g. "Wed, 18 Jun 2026 12:34:56 +0000" (not ISO-8601).
        message["Date"] = format_datetime(datetime.now(timezone.utc))
        if mail["html"]:
            # set_content handles charset + transfer-encoding so non-ASCII
            # HTML bodies are sent correctly (set_payload would not).
            message.set_content(body, subtype="html")
        else:
            message.set_content(body)
        smtp = aiosmtplib.SMTP(
            hostname=smtp_host,
            port=smtp_port,
            use_tls=mail["tls"],
            validate_certs=mail["validate_certs"],
        )
        await smtp.connect()
        # close() (sync, idempotent) guarantees the socket is released even if
        # starttls/login/send raises, so a failing SMTP server can't leak a
        # connection per report.
        try:
            if mail["starttls"]:
                await smtp.starttls()
            if username and password:
                # aiosmtplib >=2 takes username/password as positional args.
                await smtp.login(username, password)
            await smtp.send_message(message)
        finally:
            smtp.close()


class ShellReporter(Reporter):
    async def report(
        self, success: bool, job: "RunningJob", config: Dict[str, Any]
    ) -> None:
        shell_config = config["shell"]

        if shell_config["command"] is None:
            return

        if isinstance(shell_config["command"], list):
            create = asyncio.create_subprocess_exec  # type: Any
            cmd = shell_config["command"]
        else:
            if shell_config["shell"]:
                create = asyncio.create_subprocess_exec
                cmd = [shell_config["shell"], "-c", shell_config["command"]]
            else:
                create = asyncio.create_subprocess_shell
                cmd = [shell_config["command"]]

        # pass the necessary information as env variables

        # We have to be a bit careful because job.stderr and job.stdout
        # can potentially be very large. On Linux there are limits
        # both on the individual as well as combined length of the arguments.
        std_err_str = job.stderr if job.stderr is not None else ""
        std_out_str = job.stdout if job.stdout is not None else ""
        # this is an arbitrary safe lower limit
        max_length_arg = 1024 * 16
        args_too_long = (
            len(std_err_str) > max_length_arg
            or len(std_out_str) > max_length_arg
            or len(std_err_str) + len(std_out_str) > max_length_arg
        )
        std_err_str_safe = (
            std_err_str if not args_too_long else std_err_str[:max_length_arg]
        )
        std_out_str_safe = (
            std_out_str if not args_too_long else std_out_str[:max_length_arg]
        )

        env = {
            **os.environ,
            "YACRON2_FAIL_REASON": (
                job.fail_reason if job.fail_reason is not None else ""
            ),
            "YACRON2_JOB_NAME": job.config.name,
            "YACRON2_JOB_COMMAND": (
                job.config.command
                if not isinstance(job.config.command, list)
                else " ".join(job.config.command)
            ),
            "YACRON2_JOB_SCHEDULE": job.config.schedule_unparsed,
            "YACRON2_FAILED": "1" if job.failed else "0",
            "YACRON2_RETCODE": str(job.retcode),
            "YACRON2_STDERR": std_err_str_safe,
            "YACRON2_STDOUT": std_out_str_safe,
            "YACRON2_STDERR_TRUNCATED": (
                "1" if len(std_err_str_safe) != len(std_err_str) else "0"
            ),
            "YACRON2_STDOUT_TRUNCATED": (
                "1" if len(std_out_str_safe) != len(std_out_str) else "0"
            ),
        }

        logger.debug("Executing shell report cmd: %s", cmd)
        try:
            proc = await create(*cmd, env=env)
        except subprocess.SubprocessError:
            logger.exception(
                "Error executing shell reporter of job %s", job.config.name
            )
            return

        retcode = await proc.wait()
        if retcode != 0:
            # not in an except block: a nonzero exit is not an exception, so
            # logger.exception would log a bogus "NoneType: None" traceback.
            logger.error(
                "Error executing shell reporter of job %s with return code %s",
                job.config.name,
                retcode,
            )


class WebhookReporter(Reporter):
    async def report(
        self, success: bool, job: "RunningJob", config: Dict[str, Any]
    ) -> None:
        webhook = config["webhook"]

        url_config = webhook["url"]
        if url_config["value"]:
            url = url_config["value"]
        elif url_config["fromFile"]:
            with open(url_config["fromFile"], "rt") as url_file:
                url = url_file.read().strip()
        elif url_config["fromEnvVar"]:
            env_var = url_config["fromEnvVar"]
            url = os.environ.get(env_var, "")
            if not url:
                logger.error(
                    "webhook: url env var %r is not set; not reporting",
                    env_var,
                )
                return
        else:
            return  # webhook disabled: early return

        template = _compiled_template(webhook["body"])
        body = template.render(job.template_vars)

        headers = {"Content-Type": webhook["contentType"]}
        headers.update(webhook["headers"])

        timeout = aiohttp.ClientTimeout(total=webhook["timeout"])
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.request(
                webhook["method"],
                url,
                data=body.encode("utf-8"),
                headers=headers,
            ) as resp:
                if resp.status >= 400:
                    # never log the URL: Slack/Discord-style webhook URLs
                    # embed a secret token.
                    logger.error(
                        "webhook reporter of job %s: server returned"
                        " HTTP %s: %s",
                        job.config.name,
                        resp.status,
                        (await resp.text())[:1024],
                    )
                else:
                    logger.debug(
                        "webhook reporter of job %s: HTTP %s",
                        job.config.name,
                        resp.status,
                    )


class JobRetryState:
    def __init__(
        self, initial_delay: float, multiplier: float, max_delay: float
    ) -> None:
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.delay = initial_delay
        self.count = 0  # number of times retried
        self.task = None  # type: Optional[asyncio.Task]
        self.cancelled = False

    def next_delay(self) -> float:
        delay = self.delay
        self.delay = min(delay * self.multiplier, self.max_delay)
        self.count += 1
        return delay


class RunningJob:
    REPORTERS = [
        SentryReporter(),
        MailReporter(),
        ShellReporter(),
        WebhookReporter(),
    ]  # type: List[Reporter]

    def __init__(
        self, config: JobConfig, retry_state: Optional[JobRetryState]
    ) -> None:
        self.config = config
        self.proc = None  # type: Optional[asyncio.subprocess.Process]
        self.retcode = None  # type: Optional[int]
        # wall-clock instant this run started, for the web UI's run history;
        # set in start() so even a failed launch carries a timestamp.
        self.started_at = None  # type: Optional[datetime]
        # live, broadcastable view of this run's captured output (web UI tail)
        self.output = JobOutputStream()
        self._stderr_reader = None  # type: Optional[StreamReader]
        self._stdout_reader = None  # type: Optional[StreamReader]
        self.stderr = None  # type: Optional[str]
        self.stdout = None  # type: Optional[str]
        self.stderr_discarded = 0
        self.stdout_discarded = 0
        self.execution_deadline = None  # type: Optional[float]
        self.retry_state = retry_state
        self.env = None  # type: Optional[Dict[str, str]]
        # set when the subprocess could not be launched at all (e.g. the
        # command does not exist). Lets wait() treat it as a normal job
        # failure instead of raising RuntimeError("process is not running").
        self.start_failed = False
        # guards against _on_stop running twice (cancel() racing wait())
        self._stopped = False
        # set by the scheduler when this run is deliberately cancelled to make
        # way for a newer instance (concurrencyPolicy=Replace). Such a forced
        # termination is not a job failure and must not be reported or retried.
        self.replaced = False
        # set when a user explicitly cancels this run from the web UI. Like
        # `replaced` it is not reported or retried, but unlike `replaced` it is
        # recorded in the run history (shown as "cancelled" in the dashboard).
        self.cancelled = False

        statsd_config = self.config.statsd
        if statsd_config is not None:
            self.statsd_writer = StatsdJobMetricWriter(
                host=statsd_config["host"],
                port=statsd_config["port"],
                prefix=statsd_config["prefix"],
                job=self,
            )  # type: Optional[StatsdJobMetricWriter]
        else:
            self.statsd_writer = None

    async def _on_start(self) -> None:
        if self.statsd_writer:
            # statsd is best-effort telemetry; a send failure (e.g. an
            # unresolvable host) must never propagate out of job launch and
            # crash the scheduler loop.
            try:
                await self.statsd_writer.job_started()
            except OSError:
                logger.warning(
                    "Job %s: failed to send statsd job_started metric",
                    self.config.name,
                    exc_info=True,
                )

    async def _on_stop(self) -> None:
        # idempotent: cancel() and the wait() task can both reach here for a
        # single run (e.g. concurrencyPolicy=Replace), but stop metrics must
        # only be emitted once. Safe without locking because asyncio is
        # single-threaded and there is no await before the flag is set.
        if self._stopped:
            return
        self._stopped = True
        if self.statsd_writer:
            try:
                await self.statsd_writer.job_stopped()
            except OSError:
                logger.warning(
                    "Job %s: failed to send statsd job_stopped metric",
                    self.config.name,
                    exc_info=True,
                )

    async def start(self) -> None:
        if self.proc is not None:
            raise RuntimeError("process already running")
        self.started_at = datetime.now(timezone.utc)
        kwargs = {}  # type: Dict[str, Any]
        if isinstance(self.config.command, list):
            create = asyncio.create_subprocess_exec  # type: Any
            cmd = self.config.command
        else:
            if self.config.shell:
                create = asyncio.create_subprocess_exec
                cmd = [self.config.shell, "-c", self.config.command]
            else:
                create = asyncio.create_subprocess_shell
                cmd = [self.config.command]
        if self.config.environment:
            env = dict(os.environ)
            fixup_pyinstaller_env(env)
            for envvar in self.config.environment:
                env[envvar["key"]] = envvar["value"]
            self.env = env
            kwargs["env"] = env
        if self.config.uid is not None or self.config.gid is not None:
            # POSIX only: uid/gid are always None on Windows (the config layer
            # rejects user/group there), so preexec_fn is never wired up on a
            # platform that doesn't support it.
            kwargs["preexec_fn"] = self._demote
        logger.debug("%s: will execute argv %r", self.config.name, cmd)
        if self.config.captureStderr:
            kwargs["stderr"] = asyncio.subprocess.PIPE
        if self.config.captureStdout:
            kwargs["stdout"] = asyncio.subprocess.PIPE
        if self.config.executionTimeout:
            self.execution_deadline = (
                time.perf_counter() + self.config.executionTimeout
            )
        if self.config.captureStderr or self.config.captureStdout:
            kwargs["limit"] = self.config.maxLineLength

        try:
            # POSIX wants UTF-8 bytes argv (locale-independent); Windows wants
            # str (CreateProcessW rejects bytes). See platform.encode_argv.
            args = platform.encode_argv(cmd)
            logger.debug("subprocess: args=%r, kwargs=%r", args, kwargs)
            self.proc = await create(*args, **kwargs)
        except (
            subprocess.SubprocessError,
            UnicodeEncodeError,
            # OSError covers FileNotFoundError (bad argv[0]) AND the resource-
            # exhaustion / permission cases create_subprocess_exec can raise --
            # EMFILE/ENFILE (fd exhaustion), ENOMEM, EPERM/EACCES, EAGAIN (fork
            # limit). These are NOT SubprocessError subclasses, so without
            # OSError they propagate out of launch_scheduled_job through the
            # unguarded spawn_jobs / _process_pending_reboots path and kill the
            # whole scheduler. Catching here sets start_failed so the reaper
            # retries, instead of bringing the daemon down on a transient
            # spawn-time resource spike.
            OSError,
        ):
            logger.exception(
                "Error launching subprocess of job %s, cmd=%r, kwargs=%s "
                "(system encoding: %s)",
                self.config.name,
                cmd,
                kwargs,
                sys.getdefaultencoding(),
            )
            self.start_failed = True
            return

        await self._on_start()

        if self.config.captureStderr:
            assert self.proc.stderr is not None
            self._stderr_reader = StreamReader(
                self.config.name,
                "stderr",
                self.proc.stderr,
                self.config.streamPrefix,
                self.config.saveLimit,
                on_line=self.output.publish,
            )
        if self.config.captureStdout:
            assert self.proc.stdout is not None
            self._stdout_reader = StreamReader(
                self.config.name,
                "stdout",
                self.proc.stdout,
                self.config.streamPrefix,
                self.config.saveLimit,
                on_line=self.output.publish,
            )

    def _demote(self):
        # Runs in the child (preexec_fn) while still privileged. Order matters:
        # set/clear supplementary groups, then the primary gid, then the uid.
        # Dropping supplementary groups BEFORE setuid is essential — otherwise
        # the child keeps root's supplementary group memberships (the classic
        # "forgot setgroups() before setuid()" privilege-escalation bug).
        gid = self.config.gid
        uid = self.config.uid
        username = self.config.username
        try:
            if username is not None and gid is not None:
                # gives the target user exactly their own supplementary groups
                os.initgroups(username, gid)
            else:
                # unknown user/gid: drop all supplementary groups
                os.setgroups([])
        except OSError as ex:
            raise RuntimeError("setgroups/initgroups: {}".format(ex)) from ex
        if gid is not None:
            logger.debug("Changing to gid %r ...", gid)
            try:
                os.setgid(gid)
            except OSError as ex:
                raise RuntimeError("setgid: {}".format(ex)) from ex
        if uid is not None:
            logger.debug("Changing to uid %r ...", uid)
            try:
                os.setuid(uid)
            except OSError as ex:
                raise RuntimeError("setuid: {}".format(ex)) from ex

    async def wait(self) -> None:
        if self.proc is None:
            if self.start_failed:
                # The command never launched (e.g. it does not exist). Report
                # it as a normal failure (conventional "command not found"
                # exit code 127) rather than raising RuntimeError, which the
                # reaper would log as "please report this as a bug".
                self.retcode = 127
                await self._read_job_streams()
                return
            raise RuntimeError("process is not running")
        if self.execution_deadline is None:
            self.retcode = await self.proc.wait()
            await self._on_stop()
        else:
            timeout = self.execution_deadline - time.perf_counter()
            try:
                if timeout > 0:
                    self.retcode = await asyncio.wait_for(
                        self.proc.wait(), timeout
                    )
                    await self._on_stop()
                else:
                    raise asyncio.TimeoutError
            except asyncio.TimeoutError:
                logger.info(
                    "Job %s exceeded its executionTimeout of "
                    "%.1f seconds, cancelling it...",
                    self.config.name,
                    self.config.executionTimeout,
                )
                self.retcode = -100
                await self.cancel()
        await self._read_job_streams()

    async def _read_job_streams(self):
        if self._stderr_reader:
            (
                self.stderr,
                self.stderr_discarded,
            ) = await self._stderr_reader.join()
        if self._stdout_reader:
            (
                self.stdout,
                self.stdout_discarded,
            ) = await self._stdout_reader.join()
        # signal end-of-output to any live web log subscribers; their read
        # loops terminate on the sentinel this delivers.
        self.output.close()

    @property
    def failed(self) -> bool:
        return self.fail_reason is not None

    @property
    def fail_reason(self) -> Optional[str]:
        if self.config.failsWhen["always"]:
            return "failsWhen=always"
        if self.config.failsWhen["nonzeroReturn"] and self.retcode != 0:
            return "failsWhen=nonzeroReturn and retcode={}".format(
                self.retcode
            )
        if self.config.failsWhen["producesStdout"] and (
            self.stdout or self.stdout_discarded
        ):
            return "failsWhen=producesStdout and stdout is not empty"
        if self.config.failsWhen["producesStderr"] and (
            self.stderr or self.stderr_discarded
        ):
            return "failsWhen=producesStderr and stderr is not empty"
        return None

    async def cancel(self) -> None:
        if self.proc is None:
            raise RuntimeError("process is not running")
        if self.proc.returncode is None:
            try:
                self.proc.terminate()
            except ProcessLookupError:
                pass
        try:
            await asyncio.wait_for(self.proc.wait(), self.config.killTimeout)
        except asyncio.TimeoutError:
            logger.warning(
                "Job %s did not gracefully terminate after "
                "%.1f seconds, killing it...",
                self.config.name,
                self.config.killTimeout,
            )
            # The process may already be gone: on Python <=3.11
            # asyncio.wait_for can spuriously time out even though
            # proc.wait() completed (the timeout race fixed in 3.12),
            # leaving the transport closed with the returncode already
            # set. kill() would then raise ProcessLookupError on the
            # dead transport, so re-check and guard it like terminate().
            if self.proc.returncode is None:
                try:
                    self.proc.kill()
                except ProcessLookupError:
                    pass
        await self._on_stop()

    async def report_failure(self):
        logger.info("Cron job %s: reporting failure", self.config.name)
        await self._report_common(self.config.onFailure["report"], False)

    async def report_permanent_failure(self):
        logger.info(
            "Cron job %s: reporting permanent failure", self.config.name
        )
        await self._report_common(
            self.config.onPermanentFailure["report"], False
        )

    async def report_success(self):
        logger.info("Cron job %s: reporting success", self.config.name)
        await self._report_common(self.config.onSuccess["report"], True)

    async def _report_common(self, report_config: dict, success: bool) -> None:
        results = await asyncio.gather(
            *[
                reporter.report(success, self, report_config)
                for reporter in self.REPORTERS
            ],
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, Exception):
                logger.error(
                    "Problem reporting job %s failure: %s",
                    self.config.name,
                    result,
                    exc_info=result,
                )

    @property
    def template_vars(self) -> dict:
        fail_reason = self.fail_reason
        return {
            "name": self.config.name,
            "success": fail_reason is None,
            "fail_reason": fail_reason,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "exit_code": self.retcode,
            "command": self.config.command,
            "shell": self.config.shell,
            "environment": self.env,
        }
