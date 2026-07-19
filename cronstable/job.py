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
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Deque,
    Dict,
    List,
    Optional,
    Tuple,
)

import aiohttp

from cronstable import platform
from cronstable.config import JobConfig
from cronstable.resources import ResourceMonitor, ResourceUsage
from cronstable.statsd import StatsdJobMetricWriter

if TYPE_CHECKING:
    # jinja2/sentry_sdk/aiosmtplib are imported lazily inside the reporters
    # that use them (_compiled_template / SentryReporter / MailReporter): they
    # cost ~40-170ms to import and pull a lot into RSS, and a job that never
    # reports through those channels should pay for none of it. This block
    # runs only under the type checker (to resolve the jinja2.Template
    # annotation); at runtime TYPE_CHECKING is False and it is skipped.
    import jinja2

logger = logging.getLogger("cronstable")


@lru_cache(maxsize=None)
def _compiled_template(source: str) -> "jinja2.Template":
    # Template source strings come from config and are constant for the life
    # of the process; compile each distinct one once and reuse it. jinja2 is
    # imported here (not at module top) so a daemon whose jobs never render a
    # report template never pays its import cost; the lru_cache means the
    # import statement is only reached on the first distinct template anyway.
    import jinja2

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


def loggable_spawn_kwargs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Return ``kwargs`` with the child environment reduced to a summary.

    The spawn kwargs carry ``env``: a full copy of the daemon's own
    :data:`os.environ` (whatever the operator exported to cronstable, such as
    cloud keys, database URLs, or a systemd ``EnvironmentFile``) plus the
    job's configured variables plus the ``CRONSTABLE_*`` control-channel vars,
    whose token is a live bearer credential for the loopback state API.
    Formatting that dict into a log record publishes all of it to
    journald/syslog and any shipper behind them, at whatever level the record
    was emitted.

    :func:`cronstable.redact.redact_secrets` deliberately does not help here:
    it is scoped to archived job output and is pattern-based, so it would miss
    any variable whose name it doesn't recognise.  Names alone are also not
    safe to log (a variable can be named after the secret it holds), so the
    value is replaced wholesale by a count, which is what the surviving
    diagnostics (a bad ``argv[0]``, a bad encoding, a resource-exhaustion
    errno) actually need: whether a custom environment was in play, not what
    was in it.  ``preexec_fn`` and the stream/limit entries are left alone;
    none of them carries user data.
    """
    if "env" not in kwargs:
        return kwargs
    redacted = dict(kwargs)
    redacted["env"] = "<{} vars, values omitted>".format(len(kwargs["env"]))
    return redacted


# How many of the most recent output lines a JobOutputStream retains for the
# live web log tail. Independent of saveLimit (which bounds the text kept for
# failure reports); this only bounds the in-memory buffer the UI streams from.
LIVE_LOG_LIMIT = 1000

# Hard cap on the lines held in one subscriber's delivery queue. A live tail
# that keeps up drains this to near-empty each loop; the cap only bites when a
# subscriber stalls (a backgrounded tab, a full/slow TCP window) while its job
# is a firehose. Without it the queue grows to the run's ENTIRE output per
# stalled subscriber (the LIVE_LOG_LIMIT ring bounds the shared buffer, not the
# per-subscriber queue), so one paused tab on a chatty job could pin hundreds
# of MB. On overflow the OLDEST queued line is dropped so the viewer keeps
# receiving the newest output; the live tail is best-effort, and a reconnect
# re-snapshots the ring buffer. Generous headroom over the 1000-line ring so a
# briefly-slow client loses nothing.
LIVE_LOG_SUBSCRIBER_QUEUE_LIMIT = 8192

# How long a forcibly-terminated run waits for its stdout/stderr to reach EOF
# before the readers are cancelled and whatever they captured is kept (see
# RunningJob._read_job_streams). Only ever reached when a descendant escaped
# the process-group kill, so it costs nothing on a healthy run; a fixed bound
# rather than killTimeout, which is legitimately configured to 0 (kill at
# once) by jobs that would then lose output they had already produced.
KILLED_STREAM_DRAIN_TIMEOUT = 30.0

# Overall bound on one mail report's SMTP conversation (connect, STARTTLS,
# login, send). aiosmtplib's own default is 60 seconds PER OPERATION, so a
# black-holed or tar-pitting SMTP server could hold a report for several
# minutes with no explicit bound; the report runs inside the job's completion
# sequence, so that would also hold up the same job's retry arming. Generous
# for any healthy server; on expiry the report is logged as failed and the
# socket released.
MAIL_REPORT_TIMEOUT = 60.0


class JobOutputStream:
    """In-memory, broadcastable view of a job run's captured output.

    Lines are appended as the job produces them (see ``StreamReader``) and
    pushed to any live subscribers — the web UI's log tail. A bounded ring
    buffer of the most recent lines is retained so a viewer that connects
    mid-run, or just after the run finished, still sees recent context.

    Nothing is ever written to disk: this lives only for as long as the run's
    record is kept in memory, preserving cronstable's read-only-filesystem
    deployment story.
    """

    def __init__(self, limit: int = LIVE_LOG_LIMIT) -> None:
        # each item is (stream_name, line) with stream_name "stdout"/"stderr"
        self.lines: Deque[Tuple[str, str]] = deque(maxlen=limit)
        self._subscribers: List["asyncio.Queue"] = []
        self.closed = False
        # total lines ever published: `published - len(lines)` is how many
        # the ring evicted, so a consumer archiving the buffer (see
        # Cron._archive_output) can record the truncation instead of
        # presenting the tail as the whole output.
        self.published = 0
        # lines a stalled subscriber's bounded queue overflowed and dropped;
        # observability only (the live tail is best-effort).
        self.dropped = 0

    @staticmethod
    def _offer(queue: "asyncio.Queue", item: Any) -> bool:
        """Enqueue for one subscriber, dropping its oldest line if full.

        Returns True when an existing item had to be evicted to make room.
        publish() runs synchronously on the event-loop thread, so no consumer
        coroutine interleaves here and the get_nowait/put_nowait pair is race
        free. Dropping the OLDEST keeps the newest output flowing to a viewer
        that has fallen behind, and guarantees room for the end-of-stream
        sentinel even when the queue is saturated.
        """
        try:
            queue.put_nowait(item)
            return False
        except asyncio.QueueFull:
            try:
                queue.get_nowait()
            except (
                asyncio.QueueEmpty
            ):  # pragma: no cover - full implies non-empty
                pass
            queue.put_nowait(item)
            return True

    def publish(self, stream_name: str, line: str) -> None:
        item = (stream_name, line)
        self.published += 1
        self.lines.append(item)
        for queue in self._subscribers:
            if self._offer(queue, item):
                self.dropped += 1

    def subscribe(self) -> "asyncio.Queue":
        queue: asyncio.Queue = asyncio.Queue(
            maxsize=LIVE_LOG_SUBSCRIBER_QUEUE_LIMIT
        )
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
        # None is the end-of-stream sentinel for subscriber read loops. Route
        # it through _offer so a saturated queue still receives it (dropping an
        # oldest line to make room) and the reader loop terminates.
        for queue in self._subscribers:
            self._offer(queue, None)


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
        # lines awaiting one batched passthrough write to the daemon's own
        # stdout/stderr; flushed once per drained read (see _queue_emit).
        self._emit_buffer: List[str] = []
        self._emit_scheduled = False
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

    def _flush_emit_buffer(self) -> None:
        self._emit_scheduled = False
        if not self._emit_buffer:
            return
        text = "".join(self._emit_buffer)
        self._emit_buffer.clear()
        out = sys.stdout if self.stream_name == "stdout" else sys.stderr
        try:
            self._emit(out, text)
        except (OSError, ValueError):
            # The daemon's own stdout/stderr is broken or closed (a dead
            # pipe consumer). The passthrough copy is best-effort; the
            # capture buffers and live-tail publish above are unaffected,
            # so log once per batch and keep reading the job's output.
            logger.warning(
                "job %s: could not mirror %s to the daemon's own stream",
                self.job_name,
                self.stream_name,
                exc_info=True,
            )

    def _queue_emit(self, out_line: str) -> None:
        # One write+flush per DRAINED READ, not per line: readline() completes
        # without suspending while earlier reads left complete lines buffered,
        # so a flush scheduled with call_soon runs only once the read loop
        # actually blocks for new data, by which point every line of the
        # burst is in the buffer and goes out as a single write. Per line the
        # old inline emit cost two blocking syscalls ON THE EVENT LOOP THREAD,
        # and with the daemon's stdout pipe full it stalled the entire loop
        # once per line.
        self._emit_buffer.append(out_line)
        if not self._emit_scheduled:
            self._emit_scheduled = True
            asyncio.get_running_loop().call_soon(self._flush_emit_buffer)

    async def _read(self, stream):
        prefix = self.stream_prefix.format(
            job_name=self.job_name, stream_name=self.stream_name
        )
        limit_top = self.save_limit // 2
        limit_bottom = self.save_limit - limit_top
        passthrough = self.stream_name in ("stdout", "stderr")
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
                # EOF: push out whatever the last drain accumulated (the
                # already-scheduled callback then finds an empty buffer).
                self._flush_emit_buffer()
                return
            if self.on_line is not None:
                self.on_line(self.stream_name, line)
            if passthrough:
                self._queue_emit(prefix + line)
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

    async def join(self, timeout: Optional[float] = None) -> Tuple[str, int]:
        """Drain to end-of-file; return ``(output, discarded_lines)``.

        ``timeout`` bounds the wait. The read loop only ends on EOF, which
        arrives when *every* write-end of the pipe is closed -- including any
        a descendant of the job inherited -- so a caller that has just killed
        the job passes a bound rather than trusting the pipe to close (see
        RunningJob._read_job_streams). On expiry the read loop is cancelled
        and the output captured so far is returned: the lines already read are
        held here, not in the pipe, so nothing collected is lost.
        """
        if timeout is None:
            await self._reader
        else:
            try:
                await asyncio.wait_for(self._reader, timeout)
            except asyncio.TimeoutError:
                logger.warning(
                    "job %s: %s did not reach end-of-file within %.1f seconds "
                    "of the job being killed -- a descendant that outlived it "
                    "still holds the pipe open; keeping the output captured "
                    "so far",
                    self.job_name,
                    self.stream_name,
                    timeout,
                )
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

        # Imported here, past the disabled/no-DSN early returns, so the ~130ms
        # sentry_sdk import (and its RSS) is paid only when a job actually
        # reports to Sentry, not by every daemon at startup.
        import sentry_sdk
        import sentry_sdk.utils

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
        # Imported here, past the reporting-disabled early returns, so a daemon
        # that never sends a mail report never pays the aiosmtplib import cost.
        import aiosmtplib

        smtp = aiosmtplib.SMTP(
            hostname=smtp_host,
            port=smtp_port,
            use_tls=mail["tls"],
            validate_certs=mail["validate_certs"],
        )
        # One overall bound on the whole conversation: aiosmtplib only bounds
        # each individual operation (60s default), so without this a
        # black-holed server could hold the report (and the job's completion
        # sequence behind it) for several minutes.
        try:
            await asyncio.wait_for(
                self._converse(smtp, mail, username, password, message),
                MAIL_REPORT_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.error(
                "mail: report for job %s did not complete within %.0f "
                "seconds; giving up on it",
                job.config.name,
                MAIL_REPORT_TIMEOUT,
            )

    @staticmethod
    async def _converse(
        smtp: Any,
        mail: Dict[str, Any],
        username: Optional[str],
        password: Optional[str],
        message: EmailMessage,
    ) -> None:
        await smtp.connect()
        # close() (sync, idempotent) guarantees the socket is released even if
        # starttls/login/send raises (including the CancelledError a
        # wait_for timeout injects), so a failing SMTP server can't leak a
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
            "CRONSTABLE_FAIL_REASON": (
                job.fail_reason if job.fail_reason is not None else ""
            ),
            "CRONSTABLE_JOB_NAME": job.config.name,
            "CRONSTABLE_JOB_COMMAND": (
                job.config.command
                if not isinstance(job.config.command, list)
                else " ".join(job.config.command)
            ),
            "CRONSTABLE_JOB_SCHEDULE": job.config.schedule_unparsed,
            "CRONSTABLE_FAILED": "1" if job.failed else "0",
            "CRONSTABLE_RETCODE": str(job.retcode),
            "CRONSTABLE_STDERR": std_err_str_safe,
            "CRONSTABLE_STDOUT": std_out_str_safe,
            "CRONSTABLE_STDERR_TRUNCATED": (
                "1" if len(std_err_str_safe) != len(std_err_str) else "0"
            ),
            "CRONSTABLE_STDOUT_TRUNCATED": (
                "1" if len(std_out_str_safe) != len(std_out_str) else "0"
            ),
        }
        # resource accounting, when the run was monitored; empty otherwise so
        # the reporter command can test for presence.
        usage = job.resource_usage
        env["CRONSTABLE_CPU_SECONDS"] = (
            repr(usage.cpu_total_seconds) if usage is not None else ""
        )
        env["CRONSTABLE_MAX_RSS_BYTES"] = (
            str(usage.max_rss_bytes) if usage is not None else ""
        )

        logger.debug("Executing shell report cmd: %s", cmd)
        # Same process-group isolation as the job itself, so the timeout kill
        # below reaches the reporter's descendants as a unit (see
        # platform.new_process_group_kwargs).
        kwargs = platform.new_process_group_kwargs()
        try:
            proc = await create(*cmd, env=env, **kwargs)
        # OSError for the same reason RunningJob.start catches it: a missing
        # reporter binary (FileNotFoundError) or a spawn-time resource failure
        # (EMFILE/ENOMEM/EAGAIN) is not a SubprocessError subclass, and a
        # reporting problem must be logged, never propagated.
        except (subprocess.SubprocessError, OSError):
            logger.exception(
                "Error executing shell reporter of job %s", job.config.name
            )
            return

        # Bounded: report() runs INLINE on the reaper, the daemon's single
        # job-completion loop, so a reporter that never exits (curl with no
        # --max-time, a script reading stdin) would otherwise freeze
        # completion handling for EVERY job daemon-wide -- Forbid jobs stop
        # firing, shutdown never finishes. On expiry the reporter's whole
        # process group is killed and the run's handling proceeds.
        timeout = shell_config.get("timeout") or 60
        try:
            retcode = await asyncio.wait_for(proc.wait(), timeout)
        except asyncio.TimeoutError:
            logger.error(
                "Shell reporter of job %s did not finish within %.1f "
                "seconds; killing it",
                job.config.name,
                timeout,
            )
            if not await platform.kill_process_group(proc.pid, force=True):
                # group already gone or unsignallable: fall back to the
                # direct child, guarded like RunningJob.cancel.
                if proc.returncode is None:
                    try:
                        proc.kill()
                    except ProcessLookupError:
                        pass
            # reap the killed child so it does not linger as a zombie. The
            # direct child is dead after the kill above, so this returns at
            # once; the extra bound only guarantees the reaper can never be
            # wedged here no matter what.
            try:
                await asyncio.wait_for(proc.wait(), 10)
            except asyncio.TimeoutError:  # pragma: no cover - defensive
                logger.error(
                    "Shell reporter of job %s could not be reaped after "
                    "being killed",
                    job.config.name,
                )
            return
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
        # the absolute instant the currently-armed retry will fire, and the
        # delay it is sleeping out. Set by the scheduler when a retry is armed
        # (Cron.schedule_retry_job) so the dashboard can render a live
        # "attempt N/M · next retry in Xs" countdown from GET /jobs; None while
        # no retry is pending.
        self.next_retry_at = None  # type: Optional[datetime]
        self.scheduled_delay = None  # type: Optional[float]
        # the instant this ladder's current attempt was ARMED (its pending
        # first written). Copied into a cross-node HANDOFF record's
        # ``armedAt`` so the new owner's superseded-by-run guard anchors on
        # the original arm time, not the hand-off instant -- otherwise a run
        # the new owner already completed BETWEEN arming and hand-off would
        # look "older" than the record and be re-run (a double-fire).
        self.armed_at = None  # type: Optional[datetime]

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
        self,
        config: JobConfig,
        retry_state: Optional[JobRetryState],
        *,
        extra_env: Optional[Dict[str, str]] = None,
        state_token: Optional[str] = None,
        run_id: Optional[str] = None,
        dag_ref: Optional[Any] = None,
    ) -> None:
        self.config = config
        # when set, this RunningJob is one DAG task instance rather
        # than a scheduled job; the reaper routes its completion to the DAG
        # scheduler (cronstable.dagrun) instead of the normal
        # record/retry path.
        # An opaque marker carrying (dag, run_key, taskkey, ...) the scheduler
        # needs to move the graph forward.
        self.dag_ref = dag_ref
        # environment the daemon injects on top of the job's own
        # (the loopback state-API URL + a per-run bearer token + run context).
        # Applied unconditionally in start(), after config.environment, so the
        # control-channel vars are present on every job and win over a same-
        # named user override. state_token is the loopback token the daemon
        # revokes when this run finishes (see Cron._handle_finished_job); it is
        # also carried in extra_env, but kept here for a direct, unambiguous
        # cleanup handle. run_id identifies this run in the durable ledger.
        self.extra_env = extra_env or {}
        self.state_token = state_token
        self.run_id = run_id
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
        # per-run CPU/memory accounting (opt-in via config.monitorResources).
        # _resource_monitor samples the process tree while the job runs;
        # resource_usage holds the finished result (None when monitoring is
        # off, unavailable, or the run was too short to sample). Finalized in
        # _on_stop, before the statsd emission that reports it.
        self._resource_monitor: Optional[ResourceMonitor] = None
        self.resource_usage: Optional[ResourceUsage] = None
        # set when the subprocess could not be launched at all (e.g. the
        # command does not exist). Lets wait() treat it as a normal job
        # failure instead of raising RuntimeError("process is not running").
        self.start_failed = False
        # guards against _on_stop running twice (cancel() racing wait())
        self._stopped = False
        # set by cancel(): this run was forcibly terminated (executionTimeout,
        # Replace, a user cancel) rather than left to exit on its own. Read by
        # _read_job_streams, which then bounds its wait for pipe EOF instead of
        # trusting a killed process tree to close its output.
        self._terminated = False
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
        # Finalize resource accounting before statsd reports it. _on_stop is
        # the single choke point every completion path funnels through (normal
        # exit, executionTimeout, cancel/replace), and it is idempotent, so
        # stopping the monitor here captures usage exactly once no matter how
        # the run ended. Errors are swallowed inside stop(); guard anyway so a
        # monitor bug can never break job completion.
        if self._resource_monitor is not None:
            try:
                self.resource_usage = await self._resource_monitor.stop()
            except Exception:  # noqa: BLE001 - accounting must never be fatal
                logger.warning(
                    "Job %s: failed to finalize resource monitoring",
                    self.config.name,
                    exc_info=True,
                )
            finally:
                self._resource_monitor = None
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
        # Isolate the job in its own process group, so cancel() can take its
        # whole descendant tree down as a unit rather than only the process we
        # spawned -- see cronstable.platform.new_process_group_kwargs.
        kwargs = platform.new_process_group_kwargs()  # type: Dict[str, Any]
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
        if self.config.environment or self.extra_env:
            env = dict(os.environ)
            fixup_pyinstaller_env(env)
            for envvar in self.config.environment:
                env[envvar["key"]] = envvar["value"]
            # The daemon-injected control-channel vars go last, so a job's own
            # environment cannot shadow the loopback URL/token it needs to
            # reach the state API (CRONSTABLE_* is reserved for cronstable's
            # use).
            env.update(self.extra_env)
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
            logger.debug(
                "subprocess: args=%r, kwargs=%r",
                args,
                loggable_spawn_kwargs(kwargs),
            )
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
                loggable_spawn_kwargs(kwargs),
                sys.getdefaultencoding(),
            )
            self.start_failed = True
            return

        await self._on_start()

        if self.config.monitorResources and self.proc.pid is not None:
            # Begin sampling the child's process tree. Best-effort: if psutil
            # cannot attach (already exited, permission denied) the monitor
            # stays inert and resource_usage ends up None. Started here, right
            # after launch, so a long run is sampled from as early as possible.
            self._resource_monitor = ResourceMonitor(
                self.proc.pid,
                job_name=self.config.name,
                interval=self.config.monitorResourcesInterval,
                history=self.config.monitorResourcesHistory,
            )
            self._resource_monitor.start()

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

    def live_resources(self) -> Optional[Dict[str, Any]]:
        """Current live CPU/memory of this running instance, or ``None``.

        Read by the scheduler while the job is still running (the dashboard's
        live per-job readout). ``None`` when the run is not monitored, the
        monitor could not attach, or no sample has landed yet.
        """
        if self._resource_monitor is None:
            return None
        return self._resource_monitor.snapshot()

    def live_resource_series(self) -> Optional[List[List[float]]]:
        """The run-so-far CPU/RSS chart series, or ``None``.

        Kept separate from :meth:`live_resources` so the polled /jobs payload
        stays lean; only the dedicated resources endpoint asks for the series.
        """
        if self._resource_monitor is None:
            return None
        return self._resource_monitor.series()

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
        # The readers end on pipe EOF, which needs EVERY write-end closed --
        # including any a descendant of the job inherited. cancel() takes the
        # job's whole process group down, so on a killed run EOF normally
        # follows at once; but a descendant that escaped the group (it called
        # setsid itself, or Windows could not reach it once orphaned) would
        # hold the pipe open indefinitely. This await is what the reaper is
        # parked on, and it has no outer bound, so that would strand the run in
        # running_jobs forever. Bound the drain on a killed run: the slot is
        # then always released, at the cost of the output we never saw anyway.
        # An untouched run is left unbounded -- it owns its own lifetime, and
        # its output is not ours to cut short.
        timeout = KILLED_STREAM_DRAIN_TIMEOUT if self._terminated else None
        if self._stderr_reader:
            (
                self.stderr,
                self.stderr_discarded,
            ) = await self._stderr_reader.join(timeout)
        if self._stdout_reader:
            (
                self.stdout,
                self.stdout_discarded,
            ) = await self._stdout_reader.join(timeout)
        # signal end-of-output to any live web log subscribers; their read
        # loops terminate on the sentinel this delivers.
        self.output.close()
        # Close our end of the subprocess pipes now that both readers have been
        # joined above. A run that reached EOF normally already had its
        # transport closed by asyncio, so this is a no-op; but a KILLED run
        # whose descendant escaped the group (see the bounded drain above)
        # never reaches EOF, so without this its stdout/stderr pipe transport
        # lingers unclosed until garbage collection -- leaking the read-end fd
        # in a long-lived daemon, and, under the test harness, surfacing as a
        # ProactorEventLoop "unclosed transport" finalizer error ("Event loop
        # is closed") once the per-test loop is torn down. Closing here runs
        # the transport's connection-lost on the live loop instead. close() is
        # idempotent and, after the joins above, can lose no captured output.
        transport = getattr(self.proc, "_transport", None)
        if transport is not None:
            transport.close()

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
        """Terminate this run and everything it spawned.

        Signals the job's whole process group, not just the process we
        spawned: a job's descendants (``sh -c 'helper & main'``) inherit its
        stdout/stderr write-ends, so a helper that outlives a killed shell
        holds the pipe open forever -- the run never finishes draining, never
        leaves ``running_jobs``, and under ``concurrencyPolicy: Forbid`` the
        job never runs again. Killing the group also makes ``executionTimeout``
        mean what it says: a bound on the run's work, not on its root process.

        A run with no process (the spawn failed, so it registered with
        ``proc=None`` and ``start_failed``) is a NO-OP, not an error: callers
        cancel whatever ``running_jobs`` holds (the Replace branch of
        ``maybe_launch_job``, the cluster slot-renewer), and several of those
        paths run outside ``run()``'s try/except -- a raise here would escape
        them and take down the whole scheduler over a job that never even
        launched. The reaper still completes such a run through ``wait()``'s
        ``start_failed`` path, so nothing is left stranded.
        """
        if self.proc is None:
            logger.info(
                "Job %s: cancel is a no-op, no process was ever spawned "
                "(start_failed=%s)",
                self.config.name,
                self.start_failed,
            )
            return
        self._terminated = True
        # Graceful first: SIGTERM the group. This reaches the descendants even
        # once the leader itself has exited, which is exactly the case that
        # wedges the run. Where the group cannot be signalled (Windows has no
        # graceful kill, and no group to aim one at) fall back to the direct
        # child, as before.
        if not await platform.kill_process_group(self.proc.pid, force=False):
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
        # Unconditionally, whether or not the leader made its killTimeout: it
        # exiting says nothing about the descendants sharing its group, and
        # those are what hold the pipes open. A group that is already empty
        # reports back as "not signalled" and this is a no-op.
        if not await platform.kill_process_group(self.proc.pid, force=True):
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
        usage = self.resource_usage
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
            # resource accounting for report templates; all None when the run
            # was not monitored (monitorResources off / unavailable).
            "cpu_seconds": usage.cpu_total_seconds if usage else None,
            "cpu_user_seconds": usage.cpu_user_seconds if usage else None,
            "cpu_system_seconds": usage.cpu_system_seconds if usage else None,
            "max_rss_bytes": usage.max_rss_bytes if usage else None,
        }
