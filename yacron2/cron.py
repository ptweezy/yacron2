import asyncio
import asyncio.subprocess
import datetime
import heapq
import hmac
import importlib.resources
import json
import logging
import logging.config
import os
import ssl
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass
from functools import lru_cache
from typing import (  # noqa
    Any,
    Awaitable,
    Deque,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)
from urllib.parse import urlparse

import aiohttp
from aiohttp import web
from crontab import CronTab  # noqa

import yacron2.version
from yacron2 import platform
from yacron2.config import (
    ClusterConfig,
    ConfigError,
    JobConfig,
    JobDefaults,
    LoggingConfig,
    WebConfig,
    Yacron2Config,
    cluster_config_warnings,
    parse_config,
    parse_config_string,
    schedule_object_to_crontab,
)
from yacron2.fingerprint import job_set_id
from yacron2.job import JobOutputStream, JobRetryState, RunningJob
from yacron2.leadership import LeadershipBackend, make_backend
from yacron2.prometheus import (
    CONTENT_TYPE_OPENMETRICS,
    CONTENT_TYPE_TEXT,
    PrometheusMetrics,
    resolve_metrics_config,
)

logger = logging.getLogger("yacron2")
WAKEUP_INTERVAL = datetime.timedelta(minutes=1)
# The furthest back the scheduler will retroactively service a job after a slow
# pass or a forward clock jump (see Cron._advance): if a job's soonest missed
# fire is no more than this behind, every missed occurrence in the window is
# replayed so a frequently-scheduled job is not silently dropped by tick
# overhead (a long config reload, many simultaneous launches); a larger gap is
# a stall/suspend/clock jump, which we resume past by firing only the most
# recent occurrence -- matching cron's no-catch-up-after-an-outage behaviour,
# so a long freeze cannot unleash a burst of backdated launches.
CATCHUP_LIMIT = datetime.timedelta(seconds=10)
# How many finished runs to retain per job for the web UI's history/stats view.
# In-memory only (like the rest of the run record), and bounded so a frequently
# scheduled job cannot grow memory without limit.
RUN_HISTORY_LIMIT = 50
# How many compact run summaries to embed per job in the /jobs payload — enough
# for the dashboard's inline sparkline without shipping the full detailed
# history on every poll. The full history is available from /jobs/{name}/runs.
JOBS_INLINE_HISTORY = 20
# Floor (seconds) for the gate re-check interval of a deferred fail-closed
# retry: the cluster gate can stay closed for a while, and a job configured
# with a tiny/zero backoff delay must not hot-loop the scheduler (and spam the
# log) while it waits. See schedule_retry_job.
RETRY_GATE_RECHECK_FLOOR = 1.0
# requests served without bearer-token auth even when authToken is configured.
# Only the UI page itself (which carries no data and no secrets) is public; the
# browser then authenticates every data request with the token the user enters.
WEB_PUBLIC_PATHS = frozenset({"/"})

# Defense-in-depth security headers for the dashboard HTML document. The
# page is fully self-contained (one inline <script>, inline styles, no
# external assets) and only ever talks to its own origin, so this CSP is
# deliberately strict:
#   - 'unsafe-inline' for script/style is unavoidable (everything is inlined),
#     but connect-src 'self' confines any hypothetical injected script to this
#     origin — it cannot exfiltrate to an attacker's server;
#   - frame-ancestors 'none' (plus X-Frame-Options) blocks clickjacking of the
#     run/cancel controls; base-uri/form-action 'none' close those vectors.
# Operators can override any of these via the web.headers config option, which
# is merged on top of these defaults (see _security_headers).
WEB_SECURITY_HEADERS = {
    "Content-Security-Policy": (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; "
        "connect-src 'self'; "
        "base-uri 'none'; "
        "form-action 'none'; "
        "frame-ancestors 'none'; "
        "object-src 'none'"
    ),
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
}


@dataclass
class JobRunInfo:
    """In-memory summary of a job's most recent finished run (web UI history).

    Retains the run's output stream so the UI can replay the last run's logs
    after the job is no longer running. Never persisted to disk.
    """

    outcome: str  # "success" | "failure"
    exit_code: Optional[int]
    started_at: Optional[datetime.datetime]
    finished_at: datetime.datetime
    fail_reason: Optional[str]
    output: JobOutputStream

    @property
    def duration(self) -> Optional[float]:
        if self.started_at is None:
            return None
        return (self.finished_at - self.started_at).total_seconds()

    def to_dict(self) -> Dict[str, Any]:
        """JSON-serializable summary (everything except the output stream)."""
        return {
            "outcome": self.outcome,
            "exit_code": self.exit_code,
            "started_at": (
                self.started_at.isoformat()
                if self.started_at is not None
                else None
            ),
            "finished_at": self.finished_at.isoformat(),
            "duration": self.duration,
            "fail_reason": self.fail_reason,
        }


def _run_stats(runs: List[JobRunInfo]) -> Dict[str, Any]:
    """Aggregate stats over a job's retained run history, for the web UI."""
    total = len(runs)
    success = sum(1 for r in runs if r.outcome == "success")
    failure = sum(1 for r in runs if r.outcome == "failure")
    cancelled = sum(1 for r in runs if r.outcome == "cancelled")
    durations = [r.duration for r in runs if r.duration is not None]
    return {
        "total": total,
        "success": success,
        "failure": failure,
        "cancelled": cancelled,
        # success rate over runs that ran to completion (excludes
        # cancellations: user-initiated, not a verdict on the job itself).
        "success_rate": (
            success / (success + failure) if (success + failure) else None
        ),
        "avg_duration": (
            (sum(durations) / len(durations)) if durations else None
        ),
        "min_duration": min(durations) if durations else None,
        "max_duration": max(durations) if durations else None,
        "last_duration": runs[-1].duration if runs else None,
    }


@lru_cache(maxsize=1)
def load_index_html() -> str:
    """Return the bundled single-page web UI, cached after first load.

    Read from package data so it works identically for pip installs and the
    PyInstaller binary; falls back to a path relative to this module if the
    importlib.resources lookup is unavailable.
    """
    try:
        return (
            importlib.resources.files("yacron2.web")
            .joinpath("index.html")
            .read_text(encoding="utf-8")
        )
    except (FileNotFoundError, ModuleNotFoundError, OSError):
        here = os.path.dirname(os.path.abspath(__file__))
        with open(
            os.path.join(here, "web", "index.html"), encoding="utf-8"
        ) as fobj:
            return fobj.read()


def schedule_str(job: JobConfig) -> str:
    """Human-readable schedule for the web UI (the original config form)."""
    unparsed = job.schedule_unparsed
    if isinstance(unparsed, str):
        return unparsed
    # the object form: rebuild the crontab line (5 fields normally, or 6/7 when
    # a year/second column is used) via the shared builder so it matches what
    # the scheduler and fingerprint compute.
    return schedule_object_to_crontab(unparsed)


def command_str(command: Union[str, List[str]]) -> str:
    return command if isinstance(command, str) else " ".join(command)


async def _sse_send_line(
    resp: web.StreamResponse, stream_name: str, line: str
) -> None:
    payload = json.dumps({"stream": stream_name, "line": line.rstrip("\n")})
    await resp.write(("event: line\ndata: " + payload + "\n\n").encode())


def naturaltime(seconds: float) -> str:
    # only ever used to describe a future instant ("in N seconds")
    if seconds < 120:
        return "in {} second{}".format(
            int(seconds), "s" if seconds >= 2 else ""
        )
    minutes = seconds / 60
    if minutes < 120:
        return "in {} minute{}".format(
            int(minutes), "s" if minutes >= 2 else ""
        )
    hours = minutes / 60
    if hours < 48:
        return "in {} hour{}".format(int(hours), "s" if hours >= 2 else "")
    days = hours / 24
    return "in {} day{}".format(int(days), "s" if days >= 2 else "")


def get_now(timezone: Optional[datetime.tzinfo]) -> datetime.datetime:
    return datetime.datetime.now(timezone)


def next_sleep_interval(subminute: bool = False) -> float:
    """Seconds to sleep until the next scheduling tick.

    Minute mode (``subminute`` False, the default and the historical behaviour)
    snaps to the top of the next minute.  When any enabled job pins specific
    seconds the scheduler switches to ``subminute`` mode and snaps to the next
    whole-second boundary instead, so a second-level schedule can fire on time.
    """
    now = get_now(datetime.timezone.utc)
    if subminute:
        target = now.replace(microsecond=0) + datetime.timedelta(seconds=1)
    else:
        target = now.replace(second=0) + WAKEUP_INTERVAL
    return (target - now).total_seconds()


def schedule_slot(
    job: JobConfig, now: Optional[datetime.datetime] = None
) -> datetime.datetime:
    """The scheduling instant to test ``job`` against on this tick.

    Truncated to the job's own resolution -- the whole second for a
    second-level job (``has_seconds``), otherwise the top of the minute, which
    reproduces the historical minute-tick behaviour exactly.  Used both to
    decide whether the job is due (:meth:`Cron.job_should_run`) and to
    de-duplicate launches (:meth:`Cron.spawn_jobs`): microseconds are always
    zeroed so two ticks within one slot compare equal and the job fires once.

    ``now`` is the pass instant supplied by :meth:`Cron._service_slots` (a
    timezone-aware UTC datetime).  Passing it means the whole pass reads the
    clock ONCE: the same instant decides "due" and is recorded for de-dup, so
    the two cannot straddle a slot boundary and double-launch a single-slot
    job -- and a whole-second slot the previous pass overran can be serviced
    after the fact.  It is rendered into the job's own frame first: an explicit
    timezone via ``astimezone``, or local time (naive, matching
    ``get_now(None)``) for a job without one.  ``now`` omitted keeps the old
    per-job fresh read.
    """
    if now is None:
        now = get_now(job.timezone)
    elif job.timezone is not None:
        now = now.astimezone(job.timezone)
    else:
        # no explicit timezone -> local wall clock, naive, exactly as
        # get_now(None) (datetime.now(None)) would have returned.
        now = now.astimezone().replace(tzinfo=None)
    if job.has_seconds:
        return now.replace(microsecond=0)
    return now.replace(second=0, microsecond=0)


def web_site_from_url(runner: web.AppRunner, url: str) -> web.BaseSite:
    parsed = urlparse(url)
    if parsed.scheme == "http":
        if parsed.hostname is None or parsed.port is None:
            # raise ValueError (not AssertionError) so a malformed http url is
            # treated as a skippable bad-config entry, not an internal bug.
            logger.warning(
                "Ignoring web listen url %s: http url needs host and port", url
            )
            raise ValueError(url)
        return web.TCPSite(runner, parsed.hostname, parsed.port)
    elif parsed.scheme == "unix":
        if not platform.supports_unix_sockets():
            # asyncio's Windows Proactor loop can't serve a unix socket; skip
            # this listener (a skippable bad-config entry) rather than crash.
            logger.warning(
                "Ignoring web listen url %s: unix-socket listeners are not "
                "supported on this platform",
                url,
            )
            raise ValueError(url)
        return web.UnixSite(runner, parsed.path)
    else:
        logger.warning(
            "Ignoring web listen url %s: scheme %r not supported",
            url,
            parsed.scheme,
        )
        raise ValueError(url)


class Cron:
    def __init__(
        self, config_arg: Optional[str], *, config_yaml: Optional[str] = None
    ) -> None:
        # Prometheus accumulators (GET /metrics). Owned here -- not by the
        # web app -- so counters survive web-app restarts and cluster-manager
        # rebuilds; created before update_config so the first parse result is
        # already recorded. See yacron2.prometheus.
        self.metrics = PrometheusMetrics()
        # list of cron jobs we /want/ to run
        self.cron_jobs = OrderedDict()  # type: Dict[str, JobConfig]
        # list of cron jobs already running
        # name -> list of RunningJob
        self.running_jobs = defaultdict(list)  # type: Dict[str, List[RunningJob]]
        # name -> the last scheduling slot (a UTC datetime) we launched the job
        # in, retained for status/introspection.  Pruned on reload; the
        # forward-only next-fire index below is what actually de-duplicates
        # launches, so this no longer gates firing. See _launch_plan.
        self._last_run_slot = {}  # type: Dict[str, datetime.datetime]
        # The next-fire index: name -> the aware-UTC instant the job next
        # fires, for every enabled CronTab job (a @reboot/string schedule or a
        # disabled job is absent). _fire_heap is a min-heap of (when, name)
        # over the same data, to find the soonest fire in O(1) and pop the due
        # jobs in O(due log n); it may hold STALE entries (a name reseeded or
        # removed on reload), validated against _next_fire lazily on pop. This
        # replaces scanning every job with crontab.test each tick: the loop
        # sleeps until the soonest fire and only touches jobs actually due.
        self._next_fire = {}  # type: Dict[str, datetime.datetime]
        self._fire_heap = []  # type: List[tuple[datetime.datetime, str]]
        # wall-clock minute of the last housekeeping pass (config reload,
        # cluster/web (re)start, logging). Gates that work to once per minute
        # even while a second-level job wakes the loop far more often. run().
        self._last_housekeeping_minute: Optional[datetime.datetime] = None
        self.config_arg = config_arg
        if config_arg is not None:
            self.update_config()
        if config_yaml is not None:
            # config_yaml is for unit testing
            config = parse_config_string(config_yaml, "")
            self.cron_jobs = OrderedDict(
                (job.name, job) for job in config.jobs
            )

        self._wait_for_running_jobs_task = None  # type: Optional[asyncio.Task]
        self._stop_event = asyncio.Event()
        self._jobs_running = asyncio.Event()
        self.retry_state = {}  # type: Dict[str, JobRetryState]
        # name -> most recent finished run, for the web UI (in-memory only)
        self.last_run = {}  # type: Dict[str, JobRunInfo]
        # name -> bounded history of recent finished runs, oldest first, for
        # the web UI's history/stats view (in-memory only, like last_run)
        self.run_history = defaultdict(lambda: deque(maxlen=RUN_HISTORY_LIMIT))  # type: Dict[str, Deque[JobRunInfo]]
        self.web_runner = None  # type: Optional[web.AppRunner]
        self.web_config = None  # type: Optional[WebConfig]
        # the leadership backend, when a cluster section is configured
        self.cluster_manager: Optional[LeadershipBackend] = None
        # last job-set id we logged, so reloads only log it again on change
        self._logged_job_set_id = None  # type: Optional[str]
        # whether the loaded config asks us to gate jobs on leader election;
        # tracked separately from cluster_manager so the gate can fail closed
        # even when the manager failed to start.
        self._elect_leader_configured = False
        # last leadership state we logged, so we only log on transition
        self._was_leader = False
        # last quorum-membership state we logged; tracked on every node so a
        # follower losing quorum logs it too (not just the ex-leader)
        self._was_quorate = False
        # last duplicate-nodeName state we logged, so we only log on transition
        self._was_conflict = False
        # last cluster-size-disagreement state we logged (same rationale)
        self._was_size_conflict = False
        # last coordination-policy-divergence state we logged (same rationale)
        self._was_policy_conflict = False
        # @reboot Leader/PreferLeader jobs that could not start at boot because
        # the cluster had not yet elected an owner; run once on convergence.
        # name -> JobConfig; see _process_pending_reboots.
        self._pending_reboot_jobs = {}  # type: Dict[str, JobConfig]

    async def run(self) -> None:
        self._wait_for_running_jobs_task = asyncio.create_task(
            self._wait_for_running_jobs()
        )

        startup = True
        applied_logging_config: Optional[LoggingConfig] = None
        while not self._stop_event.is_set():
            # Housekeeping -- reloading the config from disk, (re)starting the
            # cluster manager and web app, applying logging config -- runs at
            # most once per wall-clock minute. When a sub-minute schedule makes
            # the loop tick every second (see the sleep below), rereading and
            # reparsing the config 60 times a minute would be pointless IO/CPU,
            # so gate it: config-reload latency stays ~1 minute, exactly as in
            # the minute-tick era. In pure minute-tick mode (no second-level
            # job) `not self._needs_subminute()` forces housekeeping every
            # iteration, so behaviour there is byte-identical to before -- and
            # a frozen-clock test still reloads every loop.
            now_minute = get_now(datetime.timezone.utc).replace(
                second=0, microsecond=0
            )
            # None when housekeeping is skipped this tick, or until the reload
            # succeeds; on failure we keep running the previously loaded jobs
            # (reload_config only swaps self.cron_jobs on a clean parse) and
            # must not dereference an unbound config below.
            config: Optional[Yacron2Config] = None
            if (
                startup
                or not self._needs_subminute()
                or now_minute != self._last_housekeeping_minute
            ):
                self._last_housekeeping_minute = now_minute
                try:
                    # reload_config runs the disk read + full reparse OFF the
                    # event loop (in a worker thread), so a slow parse no
                    # longer freezes the whole loop -- web API, cluster gossip,
                    # job output pumping -- for its duration once a minute. The
                    # parsed job set is applied here, on the loop thread, and
                    # BEFORE _service_slots below, so the cluster gate is in
                    # place before the first spawn_jobs (a reload that enables
                    # electLeader must gate its Leader jobs on that same tick,
                    # not one tick late).
                    config = await self.reload_config()
                    self._log_job_set_id()
                    await self.start_stop_cluster(config.cluster_config)
                except ConfigError as err:
                    logger.error(
                        "Error in configuration file(s), so not updating "
                        "any of the config.:\n%s",
                        str(err),
                    )
                except Exception:  # pragma: nocover
                    logger.exception("please report this as a bug (1)")
                if config is not None:
                    # The web app starts AFTER the cluster and under its OWN
                    # error handling: a web misconfiguration raising a
                    # ConfigError (an authToken that resolves empty) used to
                    # share the try/except above and skip start_stop_cluster
                    # entirely, so _elect_leader_configured stayed False and
                    # every node ran every Leader job ungated -- the gate
                    # failed OPEN on an unrelated web error, on startup and on
                    # every reload iteration. The cluster gate must engage
                    # regardless of the web app's fate.
                    try:
                        await self.start_stop_web_app(config.web_config)
                    except ConfigError as err:
                        logger.error(
                            "Error in the web configuration, so not starting "
                            "the web API:\n%s",
                            str(err),
                        )
                    except Exception:  # pragma: nocover
                        logger.exception("please report this as a bug (4)")
                if (
                    config is not None
                    and config.logging_config is not None
                    and config.logging_config != applied_logging_config
                ):
                    try:
                        logging.config.dictConfig(config.logging_config)
                    except Exception as ex:
                        logger.error(
                            "Error while configuring logging: %s\n"
                            "Check for correct format at "
                            "https://docs.python.org/3/library/logging.config"
                            ".html#dictionary-schema-details\n%s",
                            ex,
                            config.logging_config,
                        )
                    else:
                        # only mark applied on success, and re-apply when the
                        # config changes, so a fixed-after-error logging
                        # section is picked up on reload without a restart.
                        applied_logging_config = config.logging_config
            # Service the due job(s). _service_slots re-reads the clock AFTER
            # the (possibly slow) housekeeping above, so a fire the reload
            # pushed past is still serviced instead of silently dropped.
            await self._service_slots(startup)
            startup = False
            # Sleep until the soonest job's next fire (or the next housekeeping
            # minute, whichever is first). asyncio.wait_for schedules its
            # timeout against loop.time() -- the event loop's MONOTONIC clock
            # -- so the wait length is derived from the wall clock but realized
            # monotonically: a wall-clock/NTP step during the sleep cannot
            # stretch or collapse it, and (because firing compares the wall
            # clock against the fixed, forward-only next-fire instants in the
            # heap) a step is absorbed cleanly on the next wake rather than
            # re-firing already-fired slots or unleashing a catch-up storm.
            sleep_interval = self._sleep_interval()
            logger.debug("Will sleep for %.1f seconds", sleep_interval)
            try:
                await asyncio.wait_for(self._stop_event.wait(), sleep_interval)
            except asyncio.TimeoutError:
                pass

        logger.info("Shutting down (after currently running jobs finish)...")
        while self.retry_state:
            cancel_all = [
                self.cancel_job_retries(name) for name in self.retry_state
            ]
            await asyncio.gather(*cancel_all)
        # Release leadership BEFORE waiting out the running-job drain: the
        # drain is unbounded (it waits for every running job, no deadline),
        # and keeping the gossip listener / lease renew loop alive through it
        # would hold leadership on a node that no longer schedules anything,
        # stalling every Leader job cluster-wide until the slowest local job
        # finishes -- instead of the documented release-on-graceful-stop
        # failover. Retries were all cancelled above, so no retry task is
        # left to consult the stopped manager. The cost is confined to the
        # jobs still draining: the new owner may start one of those while it
        # finishes here (the same overlap a crash produces), rather than the
        # whole Leader-gated job set standing still.
        if self.cluster_manager is not None:
            logger.info("Stopping cluster manager")
            await self.cluster_manager.stop()
            self.cluster_manager = None
        await self._wait_for_running_jobs_task

        if self.web_runner is not None:
            logger.info("Stopping http server")
            await self.web_runner.cleanup()

    def signal_shutdown(self) -> None:
        logger.debug("Signalling shutdown")
        self._stop_event.set()

    @staticmethod
    def _empty_config() -> Yacron2Config:
        """The config used when no config source is set (config_arg is None).

        Empty job set, no web/cluster/logging, so applying it is a no-op that
        leaves any test-injected cron_jobs (config_yaml) untouched. Kept as a
        factory rather than a constant because JobDefaults({}) is mutable.
        """
        return Yacron2Config(
            jobs=[],
            web_config=None,
            job_defaults=JobDefaults({}),
            logging_config=None,
        )

    def update_config(self) -> Yacron2Config:
        """Reload the config from disk and apply it, synchronously.

        Used at construction (where there is no running event loop to offload
        to) and by tests. The run loop instead calls :meth:`reload_config`,
        which does the same work but runs the disk read + reparse off the event
        loop; both paths share the pure parse (:func:`parse_config`) and
        :meth:`_apply_reload`.
        """
        if self.config_arg is None:
            return self._empty_config()
        try:
            config = parse_config(self.config_arg)
        except ConfigError:
            # feeds yacron2_config_last_reload_successful, the standard
            # "config broken on disk" alert signal.
            self.metrics.config_parse(False)
            raise
        return self._apply_reload(config)

    async def reload_config(self) -> Yacron2Config:
        """Like :meth:`update_config`, but runs the disk read + full reparse
        OFF the event loop, in a worker thread.

        :func:`parse_config` is a synchronous file read and full reparse; run
        inline on the scheduling tick it froze the entire event loop -- web
        API, cluster gossip, job output pumping -- for its whole duration, once
        a minute. Offloading just the parse keeps the loop responsive; applying
        the result (which mutates shared scheduler state) stays on the loop
        thread via :meth:`_apply_reload`, so there is no cross-thread access to
        ``self``. The caller applies this BEFORE servicing due slots, so the
        cluster gate is always current for the tick.
        """
        if self.config_arg is None:
            return self._empty_config()
        loop = asyncio.get_running_loop()
        try:
            config = await loop.run_in_executor(
                None, parse_config, self.config_arg
            )
        except ConfigError:
            # feeds yacron2_config_last_reload_successful, the standard
            # "config broken on disk" alert signal. The parse ran in the worker
            # thread (parse_config does not touch metrics), so record the
            # failure here, back on the loop thread.
            self.metrics.config_parse(False)
            raise
        return self._apply_reload(config)

    def _apply_reload(self, config: Yacron2Config) -> Yacron2Config:
        """Swap in a freshly parsed config's job set (event-loop thread only).

        Records the successful reload, installs the new jobs and prunes the
        per-job maps of jobs the reload removed. Kept separate from the parse
        itself so the parse can run in a worker thread (see :meth:`run`) while
        this mutation of shared scheduler state stays on the loop thread.
        """
        self.metrics.config_parse(True)
        old_jobs = self.cron_jobs
        self.cron_jobs = OrderedDict((job.name, job) for job in config.jobs)
        # Drop metric series for jobs removed from the config, so a renamed
        # job does not leave a stale twin behind forever. A removed job with
        # an instance still running keeps its accumulator until the run
        # finishes: pruning it mid-run would let the finishing run recreate
        # the series from zero (a phantom counter reset); the reload after
        # the run ends prunes it for good.
        self.metrics.prune(set(self.cron_jobs) | set(self.running_jobs))
        # Drop last-run slots for jobs no longer in the config, so churning job
        # names cannot grow this map without bound. A removed-but-still-running
        # job keeps its slot until it finishes and the next reload prunes it,
        # matching how the metrics accumulators above are pruned.
        keep = set(self.cron_jobs) | set(self.running_jobs)
        self._last_run_slot = {
            name: slot
            for name, slot in self._last_run_slot.items()
            if name in keep
        }
        # Bring the next-fire index in step with the new job set: drop removed
        # / now-unscheduled jobs, reseed jobs whose schedule changed, and keep
        # the existing next-fire for jobs whose schedule is unchanged (a reseed
        # would recompute a STRICTLY-future fire and could skip a fire
        # that coincides with this reload's minute boundary).
        self._refresh_schedule(get_now(datetime.timezone.utc), old_jobs)
        return config

    def job_set_id(self) -> str:
        """Order-independent fingerprint of the currently-loaded job set.

        Two yacron2 instances return the same value iff they hold the same set
        of jobs (same effective config, any order); see yacron2.fingerprint.
        """
        return job_set_id(self.cron_jobs.values())

    def _log_job_set_id(self) -> None:
        """Log the job-set id at startup and whenever a reload changes it."""
        current = self.job_set_id()
        if current != self._logged_job_set_id:
            logger.info(
                "Job set id: %s (%d job%s)",
                current,
                len(self.cron_jobs),
                "" if len(self.cron_jobs) == 1 else "s",
            )
            self._logged_job_set_id = current

    async def _web_get_version(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        return web.Response(
            text=yacron2.version.version,
            headers=self.web_config.get("headers", None),
        )

    async def _web_job_set_id(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        job_set = self.job_set_id()
        headers = self.web_config.get("headers", None)
        if request.headers.get("Accept") == "application/json":
            return web.json_response(
                {"job_set_id": job_set, "jobs": len(self.cron_jobs)},
                headers=headers,
            )
        return web.Response(text=job_set, headers=headers)

    async def _web_get_cluster(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        headers = self.web_config.get("headers", None)
        if self.cluster_manager is None:
            return web.json_response(
                {"enabled": False, "peers": []}, headers=headers
            )
        payload = dict(self.cluster_manager.view_dict())
        payload["enabled"] = True
        return web.json_response(payload, headers=headers)

    async def _web_get_fleet(self, request: web.Request) -> web.Response:
        """The cluster-wide per-job run view (the dashboard's fleet view).

        Merged entirely from state this node already holds: its own scheduler
        snapshot plus the per-job summaries each peer piggybacked on the
        gossip exchanges this node has already made (see
        :meth:`yacron2.cluster.ClusterManager.fleet_view`) -- serving this
        endpoint triggers no peer traffic.  ``enabled: false`` when there is
        no cluster, or the backend has no node-to-node channel to have
        carried summaries (the lease backends); the dashboard then hides its
        fleet view.
        """
        assert self.web_config is not None
        headers = self.web_config.get("headers", None)
        mgr = self.cluster_manager
        fleet = mgr.fleet_view() if mgr is not None else None
        if fleet is None:
            return web.json_response(
                {"enabled": False, "nodes": []}, headers=headers
            )
        return web.json_response(fleet, headers=headers)

    async def _web_metrics(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        accept = request.headers.get("Accept", "")
        openmetrics = "application/openmetrics-text" in accept
        body = self.metrics.render(self, openmetrics=openmetrics)
        headers = {}  # type: Dict[str, str]
        custom = self.web_config.get("headers", None)
        if custom:
            headers.update(custom)
            # Unlike the other handlers, the Content-Type is the endpoint's
            # contract (scrapers parse it for the format version), so it
            # wins over an operator-configured web.headers Content-Type --
            # in ANY spelling: header names are case-insensitive on the
            # wire but this dict is not, and a case-variant leftover would
            # be emitted as a second, conflicting Content-Type header.
            for key in [k for k in headers if k.lower() == "content-type"]:
                del headers[key]
        headers["Content-Type"] = (
            CONTENT_TYPE_OPENMETRICS if openmetrics else CONTENT_TYPE_TEXT
        )
        return web.Response(body=body.encode("utf-8"), headers=headers)

    async def _web_get_status(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        out = []
        for name, job in self.cron_jobs.items():
            running = self.running_jobs.get(name, None)
            if running:
                out.append(
                    {
                        "job": name,
                        "status": "running",
                        "pid": [
                            runjob.proc.pid
                            for runjob in running
                            if runjob.proc is not None
                        ],
                    }
                )
            elif not job.enabled:
                # disabled jobs never run on schedule; report that honestly
                # instead of an inapplicable "scheduled (in N seconds)".
                out.append({"job": name, "status": "disabled"})
            else:
                crontab = job.schedule  # type: Union[CronTab, str]
                now = get_now(job.timezone)
                out.append(
                    {
                        "job": name,
                        "status": "scheduled",
                        "scheduled_in": (
                            crontab.next(now=now, default_utc=job.utc)
                            if isinstance(crontab, CronTab)
                            else str(crontab)
                        ),
                    }
                )
        if request.headers.get("Accept") == "application/json":
            return web.json_response(
                out, headers=self.web_config.get("headers", None)
            )
        else:
            lines = []
            for jobstat in out:  # type: Dict[str, Any]
                if jobstat["status"] == "running":
                    status = "running (pid: {pid})".format(
                        pid=", ".join(str(pid) for pid in jobstat["pid"])
                    )
                elif jobstat["status"] == "disabled":
                    status = "disabled"
                else:
                    status = "scheduled ({})".format(
                        (
                            jobstat["scheduled_in"]
                            if isinstance(jobstat["scheduled_in"], str)
                            else naturaltime(jobstat["scheduled_in"])
                        )
                    )
                lines.append(
                    "{name}: {status}".format(
                        name=jobstat["job"], status=status
                    )
                )
            return web.Response(
                text="\n".join(lines),
                headers=self.web_config.get("headers", None),
            )

    async def _web_start_job(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        name = request.match_info["name"]
        try:
            job = self.cron_jobs[name]
        except KeyError as ex:
            raise web.HTTPNotFound() from ex
        if not job.enabled:
            # a disabled job behaves "as if it isn't there"; refuse to launch
            # it manually rather than silently overriding the config.
            raise web.HTTPConflict(
                text="job {!r} is disabled".format(name),
                headers=self.web_config.get("headers", None),
            )
        # A manual start of a job still pending as a deferred @reboot one-shot
        # IS its boot run: retire the pending entry and record the run with
        # the cluster (when a manager is up), or _process_pending_reboots
        # would find reboot_ran(name) False after convergence and run the
        # one-shot a second time -- possibly on another node, since the
        # manual run was never gossiped/persisted as ran. Recording BEFORE
        # spawning mirrors the deferred-launch path's at-most-once ordering.
        if name in self._pending_reboot_jobs:
            mgr = self.cluster_manager
            if mgr is not None:
                await mgr.mark_reboot_ran(name)
            # pop, not del: a concurrent manual start of the same name can
            # retire the entry while the await above yields (the gossip push
            # awaits peers), and the loser of that race must not 500 on a
            # KeyError -- the entry is retired (and logged) exactly once,
            # mark_reboot_ran is idempotent, and both requests still launch
            # below, exactly as two manual starts of any other job would.
            if self._pending_reboot_jobs.pop(name, None) is not None:
                logger.info(
                    "cluster: manual start of deferred @reboot job %s counts "
                    "as its boot run; retiring the pending entry",
                    name,
                )
        await self.maybe_launch_job(job)
        return web.Response(headers=self.web_config.get("headers", None))

    async def _web_cancel_job(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        name = request.match_info["name"]
        if name not in self.cron_jobs:
            raise web.HTTPNotFound()
        running = list(self.running_jobs.get(name) or [])
        if not running:
            # nothing to cancel: report a conflict rather than a silent no-op
            # so the dashboard can tell the user the job was not running.
            raise web.HTTPConflict(
                text="job {!r} is not running".format(name),
                headers=self.web_config.get("headers", None),
            )
        for runjob in running:
            # mark before cancelling so the reaper records this as a deliberate
            # "cancelled" run rather than a job failure (no report, no retry).
            runjob.cancelled = True
        # cancel instances concurrently: a job with several running instances
        # then costs at most one killTimeout, not one per instance.
        await asyncio.gather(
            *(rj.cancel() for rj in running if rj.proc is not None)
        )
        return web.Response(headers=self.web_config.get("headers", None))

    def _security_headers(self) -> Dict[str, str]:
        """Security headers for the dashboard HTML page.

        Secure defaults (CSP, anti-clickjacking, nosniff) with any operator
        ``web.headers`` merged on top, so an operator who deliberately sets one
        of these (e.g. a relaxed CSP or framing policy) still wins.
        """
        assert self.web_config is not None
        headers = dict(WEB_SECURITY_HEADERS)
        custom = self.web_config.get("headers", None)
        if custom:
            headers.update(custom)
        return headers

    async def _web_index(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        return web.Response(
            text=load_index_html(),
            content_type="text/html",
            headers=self._security_headers(),
        )

    def _scheduled_in(self, job: JobConfig, running: bool) -> Optional[float]:
        """Seconds until the job's next scheduled run.

        ``None`` when not applicable: disabled, currently running, or a
        one-off ``@reboot`` schedule (a string, not a crontab).
        """
        if not job.enabled or running:
            return None
        crontab = job.schedule  # type: Union[CronTab, str]
        if not isinstance(crontab, CronTab):
            return None
        now = get_now(job.timezone)
        seconds: Optional[float] = crontab.next(now=now, default_utc=job.utc)
        return seconds

    def fleet_job_summaries(self) -> Dict[str, Any]:
        """Compact per-job snapshot gossiped to peers for the fleet view.

        Installed on the leadership backend as its job-summaries provider
        (see :meth:`start_stop_cluster`); the gossip backend piggybacks it on
        every ``/peer`` response, which is how the dashboard's fleet view can
        show runs happening on other nodes.  Deliberately lean -- one small
        fixed-shape entry per job -- because it travels in a byte-capped
        gossip payload: no command line, no ``fail_reason`` (arbitrary-length
        operator text), no run history.  Those stay on the owning node's own
        API.
        """
        out: Dict[str, Any] = {}
        for name, job in self.cron_jobs.items():
            running = bool(self.running_jobs.get(name))
            last = self.last_run.get(name)
            out[name] = {
                "running": running,
                "enabled": job.enabled,
                "scheduled_in": self._scheduled_in(job, running),
                "last": (
                    None
                    if last is None
                    else {
                        "outcome": last.outcome,
                        "finished_at": last.finished_at.isoformat(),
                        "duration": last.duration,
                        "exit_code": last.exit_code,
                    }
                ),
            }
        return out

    def _job_to_dict(self, name: str, job: JobConfig) -> Dict[str, Any]:
        running = self.running_jobs.get(name) or []
        # next scheduled run, in seconds; None when not applicable (disabled,
        # currently running, or a one-off @reboot schedule).
        scheduled_in = self._scheduled_in(job, bool(running))

        last = self.last_run.get(name)
        last_run = last.to_dict() if last is not None else None

        history = self.run_history.get(name)
        # compact, oldest-first tail of recent runs for the inline sparkline:
        # only outcome + duration are needed there, so the per-poll payload
        # stays small. Full per-run detail comes from /jobs/{name}/runs.
        recent = (
            [
                {"outcome": r.outcome, "duration": r.duration}
                for r in list(history)[-JOBS_INLINE_HISTORY:]
            ]
            if history
            else []
        )

        result = {
            "name": name,
            "enabled": job.enabled,
            "schedule": schedule_str(job),
            "command": command_str(job.command),
            "captureStdout": job.captureStdout,
            "captureStderr": job.captureStderr,
            # the schedule's reference frame, so the dashboard can compute and
            # label upcoming run times (utc=True is the default; timezone, when
            # set, is an IANA name like "America/Los_Angeles").
            "utc": job.utc,
            "timezone": (
                str(job.timezone) if job.timezone is not None else None
            ),
            "running": bool(running),
            "pids": [
                runjob.proc.pid
                for runjob in running
                if runjob.proc is not None
            ],
            "scheduled_in": scheduled_in,
            "last_run": last_run,
            "history": recent,
        }  # type: Dict[str, Any]
        # only relevant when leader election is on, so omit it otherwise to
        # keep the per-poll payload lean for the common single-instance case.
        if self._elect_leader_configured:
            result["clusterPolicy"] = job.clusterPolicy
            # Under spread distribution each leader-gated job has its own
            # owner, so surface it for the dashboard (None = no quorum)
            # and EveryNode has no single owner.
            mgr = self.cluster_manager
            if (
                mgr is not None
                and mgr.distribution == "spread"
                and job.clusterPolicy != "EveryNode"
            ):
                result["clusterOwner"] = (
                    mgr.available_job_owner(job.name)
                    if job.clusterPolicy == "PreferLeader"
                    else mgr.job_owner(job.name)
                )
        return result

    async def _web_list_jobs(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        out = [
            self._job_to_dict(name, job)
            for name, job in self.cron_jobs.items()
        ]
        return web.json_response(
            out, headers=self.web_config.get("headers", None)
        )

    async def _web_job_runs(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        name = request.match_info["name"]
        if name not in self.cron_jobs:
            raise web.HTTPNotFound()
        runs = list(self.run_history.get(name) or [])
        return web.json_response(
            {
                "name": name,
                "runs": [r.to_dict() for r in runs],  # oldest first
                "stats": _run_stats(runs),
            },
            headers=self.web_config.get("headers", None),
        )

    def _job_output(self, name: str) -> Optional[JobOutputStream]:
        # the live output of the most recent running instance, else the last
        # finished run's retained output, else nothing captured yet.
        running = self.running_jobs.get(name) or []
        if running:
            return running[-1].output
        last = self.last_run.get(name)
        return last.output if last is not None else None

    async def _web_job_logs(self, request: web.Request) -> web.StreamResponse:
        assert self.web_config is not None
        name = request.match_info["name"]
        if name not in self.cron_jobs:
            raise web.HTTPNotFound()

        headers = {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            # tell reverse proxies (nginx) not to buffer the event stream
            "X-Accel-Buffering": "no",
        }
        custom = self.web_config.get("headers", None)
        if custom:
            headers.update(custom)
        resp = web.StreamResponse(headers=headers)
        await resp.prepare(request)

        output = self._job_output(name)
        if output is None:
            await resp.write(b'event: end\ndata: {"reason": "no-output"}\n\n')
            return resp

        # Subscribe first, then snapshot the buffer: there is no await between
        # the two, so no line can slip through the gap. The snapshot holds
        # everything captured before now; the queue receives only lines
        # published after — together, no duplicates and no gaps.
        queue = output.subscribe()
        try:
            for stream_name, line in list(output.lines):
                await _sse_send_line(resp, stream_name, line)
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=15)
                except asyncio.TimeoutError:
                    # SSE comment as keep-alive (also detects disconnects)
                    await resp.write(b": ping\n\n")
                    continue
                if item is None:  # end-of-output sentinel
                    break
                stream_name, line = item
                await _sse_send_line(resp, stream_name, line)
            await resp.write(b"event: end\ndata: {}\n\n")
        except (ConnectionResetError, asyncio.CancelledError):
            # client navigated away / closed the tab: nothing to do
            pass
        finally:
            output.unsubscribe(queue)
        return resp

    async def start_stop_web_app(self, web_config: Optional[WebConfig]):
        if self.web_runner is not None and (
            web_config is None or web_config != self.web_config
        ):
            # assert self.web_runner is not None
            logger.info("Stopping http server")
            await self.web_runner.cleanup()
            self.web_runner = None

        if (
            web_config is not None
            and web_config["listen"]
            and self.web_runner is None
        ):
            ui_enabled = web_config.get("ui", True)
            metrics_config = resolve_metrics_config(web_config)
            middlewares = []
            token = self._resolve_web_token(web_config)
            if token is not None:
                logger.info("web: requiring bearer-token authentication")
                # the UI page is served unauthenticated (it holds no data); the
                # browser then sends the token on every data request.
                public = set(WEB_PUBLIC_PATHS) if ui_enabled else set()
                if metrics_config is not None and metrics_config["public"]:
                    # deliberate operator opt-out for scrapers that cannot
                    # send a bearer token; everything else stays gated.
                    public.add("/metrics")
                middlewares.append(
                    self._make_auth_middleware(token, frozenset(public))
                )
            app = web.Application(middlewares=middlewares)
            routes = [
                web.get("/version", self._web_get_version),
                web.get("/job-set-id", self._web_job_set_id),
                web.get("/cluster", self._web_get_cluster),
                web.get("/fleet", self._web_get_fleet),
                web.get("/status", self._web_get_status),
                web.get("/jobs", self._web_list_jobs),
                web.get("/jobs/{name}/runs", self._web_job_runs),
                web.post("/jobs/{name}/start", self._web_start_job),
                web.post("/jobs/{name}/cancel", self._web_cancel_job),
                web.get("/jobs/{name}/logs", self._web_job_logs),
            ]
            if metrics_config is not None:
                # buckets apply from here on; a changed bucket set restarts
                # the histograms (an ordinary counter reset to Prometheus).
                self.metrics.set_duration_buckets(
                    metrics_config["durationBuckets"]
                )
                routes.append(web.get("/metrics", self._web_metrics))
            if ui_enabled:
                routes.append(web.get("/", self._web_index))
            app.add_routes(routes)
            self.web_runner = web.AppRunner(app)
            await self.web_runner.setup()
            socket_mode = web_config.get("socketMode")
            for addr in web_config["listen"]:
                try:
                    site = web_site_from_url(self.web_runner, addr)
                    await site.start()
                except (ValueError, OSError) as ex:
                    # bad scheme/url (ValueError) or bind failure (OSError):
                    # skip this address rather than aborting the whole config
                    # update or reporting it as an internal bug.
                    logger.warning("web: could not listen on %s: %s", addr, ex)
                    continue
                logger.info("web: started listening on %s", addr)
                if socket_mode:
                    self._apply_socket_mode(addr, socket_mode)
            self.web_config = web_config

    async def start_stop_cluster(
        self, cluster_config: Optional[ClusterConfig]
    ) -> None:
        # Track the election intent up front so the leader gate can fail closed
        # even if the manager (below) is absent or fails to start.
        self._elect_leader_configured = bool(
            cluster_config and cluster_config.get("electLeader")
        )
        # Restart the manager when the cluster section is removed or changed,
        # mirroring start_stop_web_app. The id it reports tracks config reloads
        # on its own (it calls self.job_set_id each round), so only a change to
        # the cluster section itself (peers/tls/listen) needs a restart -- plus
        # an in-place TLS cert rotation, which leaves the config bytes
        # identical but the on-disk material new (cert-manager / Vault / a
        # Kubernetes secret refresh); without this the cluster keeps serving
        # the old cert until it expires and then loses quorum fleet-wide.
        mgr = self.cluster_manager
        if mgr is not None:
            if cluster_config is None or cluster_config != mgr.config:
                reason = "configuration changed"
            elif mgr.tls_files_changed():
                reason = "TLS certificate files changed"
            else:
                reason = None
            if (
                reason == "TLS certificate files changed"
                and not mgr.tls_files_loadable()
            ):
                # A cert rotation restarts the manager only to swap in the NEW
                # on-disk material, so validate it BEFORE tearing the old one
                # down: cert-manager / Vault / a Kubernetes secret refresh are
                # not atomic across all three files, so a reload can observe a
                # half-written or briefly-absent cert.  If the new material is
                # not yet loadable, keep the running manager -- still serving
                # the valid old cert -- and retry next reload, rather than
                # stopping it and then failing to rebuild, which would wedge
                # Leader / PreferLeader closed for up to a reload.  (Make-
                # before-break is infeasible for gossip: the new manager binds
                # the same listen port the old one still holds.)  Only this
                # reason is gated; a genuine configuration change tears the old
                # manager down regardless and lets start fail closed as before.
                # The etcd backend also reaches here (it tracks client-TLS
                # rotation and overrides tls_files_loadable to dry-run the new
                # ca/cert/key), so it gets the same make-before-break. The
                # kubernetes backend reports tls_files_changed but inherits the
                # always-true tls_files_loadable default, so it skips the gate
                # and rebuilds straight away. A backend with neither (plain
                # http, no tracked files) never reaches here at all.
                logger.warning(
                    "cluster: TLS certificate files changed but the new "
                    "material is not yet loadable (a partial/half-written "
                    "rotation?); keeping the running manager and retrying "
                    "next reload"
                )
                reason = None
            # local import to keep cluster.py out of the import graph until a
            # running manager is actually being reconfigured (mirrors
            # make_backend's deferred imports); the helper returns True
            # at once for a non-gossip new config, so this is gossip-only.
            from yacron2.cluster import gossip_tls_loadable

            if (
                reason == "configuration changed"
                and cluster_config is not None
                and not gossip_tls_loadable(cluster_config)
            ):
                # A genuine config change (peers/listen) tears the old manager
                # down regardless -- but if it coincides with an in-flight cert
                # rotation (half-written/absent cert files), the rebuild's
                # ClusterManager.__init__ would raise on the bad material and
                # leave NO manager, wedging Leader/PreferLeader closed up to
                # a reload. The cert-only path above does not cover
                # this combined case. Dry-run the NEW config's gossip TLS first
                # (the incoming paths, which a config edit may have repointed):
                # if it cannot load now, keep the running manager -- still
                # serving the valid old cert -- retry next reload, accepting
                # that the peers/listen change also waits one reload (a stale-
                # but-functional cluster beats no manager). Non-gossip backends
                # and tls-less configs always pass, so this is gossip-only.
                logger.warning(
                    "cluster: configuration changed but the new TLS material "
                    "is not yet loadable (a config edit racing a cert "
                    "rotation?); keeping the running manager and retrying "
                    "next reload"
                )
                reason = None
            if reason is not None:
                logger.info("cluster: %s, stopping", reason)
                # Record losing leadership/quorum HERE if we held it: the flag
                # resets below would otherwise suppress the transition log in
                # _emit_cluster_role_logs, leaving the ex-leader's own log
                # silent about why it stopped Leader jobs (until/unless a
                # replacement manager comes up and re-logs). Only fires when
                # election was on (the flags are only ever set then).
                if self._was_leader:
                    # a real leadership loss (the rebuilt manager re-elects
                    # from scratch), so it counts as a transition too
                    self.metrics.cluster_leader_transition()
                    logger.info(
                        "cluster: this node lost scheduled-job leadership "
                        "(leadership manager stopped for reload)"
                    )
                if self._was_quorate:
                    self.metrics.cluster_quorum_transition()
                    logger.info(
                        "cluster: this node left quorum (leadership manager "
                        "stopped for reload); Leader jobs cannot run until it "
                        "is rebuilt"
                    )
                await mgr.stop()
                self.cluster_manager = None
                # The transition flags track the OLD manager's last-logged
                # state; reset them so the first _log_cluster_role against the
                # replacement (or against no manager) reflects a clean
                # transition rather than suppressing or duplicating a log line.
                self._was_leader = False
                self._was_quorate = False
                self._was_conflict = False
                self._was_size_conflict = False
                self._was_policy_conflict = False
        if cluster_config is not None and self.cluster_manager is None:
            # Emit non-fatal advisories here (only when a manager is actually
            # (re)started) rather than at parse time, which runs every reload
            # and would repeat the same warning every minute.
            for warning in cluster_config_warnings(cluster_config):
                logger.warning("%s", warning)
            try:
                # Construct INSIDE the try: a backend's __init__/start can
                # raise on an operational misconfiguration -- the gossip
                # manager builds the TLS contexts (loading the CA/cert/key
                # files: OSError/ssl.SSLError) and start() parses listen
                # (ValueError) and binds the port (OSError); a lease backend's
                # start() may fail to load in-cluster/kubeconfig credentials
                # or build a client TLS context (ConfigError/OSError/SSLError).
                # All are misconfigurations we log and keep running through,
                # not bugs -- so they must not escape to the run loop's generic
                # "please report this as a bug" handler.
                manager = make_backend(cluster_config, self.job_set_id)
                # Install the fleet-view summaries provider BEFORE start():
                # start() runs a full poll round up front, during which peers
                # may already be polling us back, and their very first
                # absorbed snapshot should carry our jobs rather than an
                # empty block. No-op for the lease backends.
                manager.set_job_summaries_provider(self.fleet_job_summaries)
                await manager.start()
            except (
                OSError,
                ssl.SSLError,
                ValueError,
                ConfigError,
                aiohttp.ClientError,
                asyncio.TimeoutError,
            ) as ex:
                # bad cert/credential files / bad listen address / port already
                # in use / unreachable setup: log and keep running jobs rather
                # than aborting the reload. (A backend cleans up its own
                # half-started state on failure.) aiohttp.ClientError /
                # asyncio.TimeoutError cover a lease backend that cannot reach
                # or authenticate to its store at start() -- an operational
                # misconfiguration to log, not the generic "report a bug" path
                # (a ClientResponseError on a rejected token is not OSError).
                logger.error("cluster: failed to start: %s", ex)
                return
            self.cluster_manager = manager

    @staticmethod
    def _resolve_web_token(web_config: WebConfig) -> Optional[str]:
        auth = web_config.get("authToken")
        if not auth:
            return None
        # authToken is configured: resolve it from exactly one source and fail
        # closed (ConfigError) if it cannot be resolved to a non-empty secret.
        # Otherwise a misconfigured source (unset env var, empty/missing file)
        # would silently leave the web API listening with no authentication.
        if auth.get("value"):
            token = str(auth["value"])
        elif auth.get("fromFile"):
            try:
                with open(auth["fromFile"], "rt") as token_file:
                    token = token_file.read().strip()
            except OSError as ex:
                raise ConfigError(
                    "web.authToken.fromFile could not be read: {}".format(ex)
                ) from ex
        elif auth.get("fromEnvVar"):
            token = os.environ.get(auth["fromEnvVar"], "")
        else:
            token = ""
        if not token:
            raise ConfigError(
                "web.authToken is configured but resolved to an empty token; "
                "refusing to start the web API without authentication"
            )
        return token

    @staticmethod
    def _make_auth_middleware(
        token: str, public_paths: "frozenset[str]" = frozenset()
    ):
        @web.middleware
        async def auth_middleware(request, handler):
            if public_paths and request.path in public_paths:
                return await handler(request)
            header = request.headers.get("Authorization", "")
            scheme, _, presented = header.partition(" ")
            # RFC 7235: the auth scheme is case-insensitive (Bearer/bearer).
            # Compare only the token, in constant time, to avoid leaking it via
            # timing (the scheme is not secret).
            if scheme.lower() != "bearer" or not hmac.compare_digest(
                presented, token
            ):
                raise web.HTTPUnauthorized()
            return await handler(request)

        return auth_middleware

    @staticmethod
    def _apply_socket_mode(addr: str, socket_mode: str) -> None:
        parsed = urlparse(addr)
        if parsed.scheme != "unix":
            return
        try:
            os.chmod(parsed.path, int(socket_mode, 8))
        except (OSError, ValueError) as ex:
            logger.warning(
                "web: could not set socketMode %r on %s: %s",
                socket_mode,
                parsed.path,
                ex,
            )

    def _needs_subminute(self) -> bool:
        """Whether any enabled job fires at second granularity.

        Gates the once-per-minute housekeeping in :meth:`run`: while a
        second-level job wakes the loop far more often than once a minute,
        rereading and reparsing the config on every wake would be pointless
        IO/CPU, so housekeeping still runs at most once per wall-clock minute.
        Only enabled jobs count: a disabled second-level job never runs.
        """
        return any(
            job.has_seconds for job in self.cron_jobs.values() if job.enabled
        )

    # ---- next-fire index ------------------------------------------------
    #
    # Instead of testing every job against the clock on every tick, each
    # enabled CronTab job carries its next fire instant (aware UTC) in
    # ``_next_fire``, mirrored into the ``_fire_heap`` min-heap.  The loop
    # sleeps until the soonest entry and only touches the jobs actually due,
    # turning the per-wake cost from O(all jobs) into O(due jobs).  Firing
    # compares the wall clock against these fixed, forward-only instants, which
    # is what makes the cadence immune to clock steps (see :meth:`run`).

    def _compute_next_fire(
        self, job: JobConfig, after: datetime.datetime
    ) -> Optional[datetime.datetime]:
        """The aware-UTC instant ``job`` next fires strictly after ``after``.

        Render ``after`` into the job's own frame (its timezone, or the
        system-local zone when it has none) and ask parse-crontab for the delay
        to the next match.  The frame is kept timezone-AWARE in both cases, so
        parse-crontab computes the delay as a real duration -- correcting for
        any utcoffset (DST) change across the interval -- and adding it back to
        the UTC ``after`` yields the correct UTC fire instant.  A naive local
        frame would defeat that correction: parse-crontab would return a civil
        wall-clock delta that, added to a UTC instant, lands an hour off across
        a spring-forward/fall-back (the same wall time the old per-tick
        ``crontab.test`` matched correctly).  ``None`` when the schedule has no
        further occurrence (a fixed past year), so the job drops out of the
        index.
        """
        crontab = job.schedule
        assert isinstance(crontab, CronTab)
        if job.timezone is not None:
            frame = after.astimezone(job.timezone)  # type: datetime.datetime
        else:
            # no explicit timezone -> the system-local wall clock, but kept
            # AWARE (not .replace(tzinfo=None)) so parse-crontab applies its
            # DST correction. default_utc is inert for an aware `now`.
            frame = after.astimezone()
        delay = crontab.next(now=frame, default_utc=job.utc)
        if delay is None:
            return None
        return after + datetime.timedelta(seconds=delay)

    def _set_next_fire(self, name: str, when: datetime.datetime) -> None:
        """Record ``name``'s next fire and mirror it into the heap."""
        self._next_fire[name] = when
        heapq.heappush(self._fire_heap, (when, name))

    def _ensure_seeded(self, now: datetime.datetime) -> None:
        """Seed the index for any enabled CronTab job missing from it.

        Seeds strictly-future (the next boundary after ``now``), so a job just
        added on a reload -- or every job at start-up -- skips the in-progress
        slot rather than firing once for the partial period already under way.
        """
        for name, job in self.cron_jobs.items():
            if name in self._next_fire:
                continue
            if job.enabled and isinstance(job.schedule, CronTab):
                nxt = self._compute_next_fire(job, now)
                if nxt is not None:
                    self._set_next_fire(name, nxt)

    @staticmethod
    def _same_schedule(a: JobConfig, b: JobConfig) -> bool:
        """Whether two job configs fire on the same wall-clock instants.

        Compares the schedule and the RESOLVED timezone (which already folds
        in ``utc``: ``utc: true`` -> UTC, ``utc: false`` with no timezone ->
        local, an explicit ``timezone`` -> that zone with ``utc`` inert).  The
        timezone is compared by its canonical string so ``datetime.timezone``
        ``.utc`` and ``ZoneInfo("UTC")`` -- distinct objects that fire
        identically -- are treated as equal (an object-identity compare would
        force a needless reseed that could skip a fire on the reload boundary).
        The raw ``utc`` field is deliberately NOT compared: it is fully carried
        by the resolved timezone and has no further effect on the fire instants
        (:meth:`_compute_next_fire` reads an aware frame, so ``default_utc`` is
        inert), so comparing it would only cause spurious reseeds.
        """
        return a.schedule == b.schedule and str(a.timezone) == str(b.timezone)

    def _refresh_schedule(
        self, now: datetime.datetime, old_jobs: Dict[str, JobConfig]
    ) -> None:
        """Reconcile the next-fire index with a reloaded job set.

        Keeps the existing next-fire for a job whose schedule is unchanged (so
        a reload never recomputes a strictly-future fire and skips a fire that
        coincides with the reload's own minute boundary), drops a job that is
        gone / disabled / no longer a CronTab schedule / has a changed
        schedule, then reseeds anything now missing (changed schedules and
        newly added jobs).  Stale heap entries left behind by a drop are
        discarded lazily on pop (see :meth:`_due_names`).
        """
        for name in list(self._next_fire):
            job = self.cron_jobs.get(name)
            old = old_jobs.get(name)
            if (
                job is None
                or not job.enabled
                or not isinstance(job.schedule, CronTab)
                or old is None
                or not self._same_schedule(old, job)
            ):
                del self._next_fire[name]
        self._ensure_seeded(now)

    def _peek_soonest_fire(self) -> Optional[datetime.datetime]:
        """The soonest valid next-fire instant, or ``None`` if nothing is
        scheduled.  Discards stale heap entries from the top as it goes."""
        heap = self._fire_heap
        while heap:
            when, name = heap[0]
            if self._next_fire.get(name) == when:
                return when
            heapq.heappop(heap)  # stale: superseded or removed
        return None

    def _sleep_interval(self) -> float:
        """Seconds to sleep until the next wake.

        The soonest job's next fire, capped by the next housekeeping boundary
        (the next wall-clock minute) so config reloads and cluster/web upkeep
        stay ~once a minute even when no job is due for a while.  Never
        negative; a fire already due returns 0 and is serviced next pass.  The
        housekeeping cap goes through :func:`next_sleep_interval`, so a test
        can still patch that one function to spin the loop fast.
        """
        housekeeping = next_sleep_interval(False)
        soonest = self._peek_soonest_fire()
        if soonest is None:
            return housekeeping
        now = get_now(datetime.timezone.utc)
        delta = (soonest - now).total_seconds()
        return max(0.0, min(housekeeping, delta))

    def _due_names(self, now: datetime.datetime) -> List[str]:
        """Names of every job whose next fire is at or before ``now``.

        Pops the matching heap entries (validated against ``_next_fire``, so
        stale ones are discarded and a name that somehow holds two live entries
        for the same instant is returned once).  The popped names' next-fire
        entries are left in place for :meth:`_advance` to read the fired slot
        and push the replacement.
        """
        heap = self._fire_heap
        due = []  # type: List[str]
        seen = set()  # type: set[str]
        while heap:
            when, name = heap[0]
            if when > now:
                break
            heapq.heappop(heap)
            if name in seen:
                continue
            if self._next_fire.get(name) == when:
                due.append(name)
                seen.add(name)
        return due

    def _advance(
        self,
        job: JobConfig,
        fire_slot: datetime.datetime,
        now: datetime.datetime,
    ) -> Tuple[List[datetime.datetime], Optional[datetime.datetime]]:
        """The slots a due job launches this pass, plus its new next-fire.

        When ``fire_slot`` (its current next-fire, known ``<= now``) is within
        :data:`CATCHUP_LIMIT` of ``now``, walk forward occurrence by occurrence
        while still ``<= now`` -- replaying each missed slot so a frequently
        scheduled job overrun by a slow pass is not silently dropped.  This
        walk is bounded: at most ``CATCHUP_LIMIT`` occurrences even for a
        per-second job.

        A larger gap is a stall/suspend/forward-clock-jump, NOT tick overhead,
        and is handled WITHOUT walking the window: enumerating it would iterate
        once per missed occurrence -- millions of times for a per-second job
        across a multi-hour gap, unbounded for an RTC-less boot corrected
        forward by years -- blocking the event loop and exhausting memory only
        to discard the result.  Instead the job resumes exactly where the old
        per-tick scheduler would, in O(1): fire the current slot only if now
        itself matches (a per-second job fires once at the current second; a
        sparse job -- ``*/15``, hourly, daily -- whose current slot does not
        match fires nothing), then resync to the next occurrence after ``now``.
        This is cron's no-catch-up-after-an-outage rule.
        """
        if now - fire_slot >= CATCHUP_LIMIT:
            logger.warning(
                "job %s: the scheduler fell behind by %.0fs (a slow pass, "
                "stall, suspend, or clock change); resuming at the current "
                "slot instead of replaying the interval",
                job.name,
                (now - fire_slot).total_seconds(),
            )
            # Resume at the current slot, firing only if it matches -- the same
            # decision the old tick made via schedule_slot + crontab.test --
            # and resync to the first occurrence after now (no enumeration).
            crontab = job.schedule
            assert isinstance(crontab, CronTab)
            now_slot = schedule_slot(job, now)
            # Record the fired slot as an aware-UTC instant, matching the
            # normal branch below (whose fires are the aware-UTC next-fire
            # entries), so _last_run_slot never mixes naive and aware values.
            # schedule_slot renders now into the job's OWN frame -- naive local
            # for a job with no timezone -- which is what crontab.test must
            # match against; the recorded slot is then converted back to UTC.
            # astimezone(utc) reads a naive slot as local (as schedule_slot
            # produced it) and is a no-op for an already-UTC one.
            fires = (
                [now_slot.astimezone(datetime.timezone.utc)]
                if crontab.test(now_slot)
                else []
            )
            return fires, self._compute_next_fire(job, now)
        fires = [fire_slot]
        nxt = self._compute_next_fire(job, fire_slot)
        while nxt is not None and nxt <= now:
            fires.append(nxt)
            nxt = self._compute_next_fire(job, nxt)
        return fires, nxt

    async def _service_slots(self, startup: bool) -> None:
        """Service the jobs due on this pass.

        Reads the clock once (AFTER any slow housekeeping, so a fire the reload
        pushed past is still serviced instead of dropped) and hands that
        instant to :meth:`spawn_jobs`.  On the start-up pass it also seeds the
        next-fire index for every scheduled job, so their first fire is the
        boundary strictly after start-up (the skip-the-partial-period start).
        """
        now = get_now(datetime.timezone.utc)
        if startup:
            self._ensure_seeded(now)
        await self.spawn_jobs(startup, now)

    async def spawn_jobs(
        self, startup: bool, now: Optional[datetime.datetime] = None
    ) -> None:
        """Launch the jobs due on this pass.

        At start-up only ``@reboot`` jobs are due; scheduled (CronTab) jobs
        never fire at start-up (they are seeded strictly-future in the
        next-fire index).  On a normal pass the due jobs come from that index
        (:meth:`_due_names`), each advanced past its fired slot with bounded
        catch-up (:meth:`_advance`), so a job fires at most once per slot with
        no per-tick scan or ``crontab.test`` over the whole job set.

        ``now`` is the pass instant (aware UTC) from :meth:`_service_slots`; a
        direct caller may omit it for a fresh read.
        """
        if now is None:
            now = get_now(datetime.timezone.utc)
        self._log_cluster_role()
        if startup:
            await self._spawn_reboot_jobs()
        else:
            await self._spawn_due_jobs(now)
        await self._process_pending_reboots()

    async def _spawn_reboot_jobs(self) -> None:
        """Launch ``@reboot`` jobs at start-up, in config order.

        A ``@reboot`` Leader/PreferLeader job under election is deferred until
        the cluster elects an owner (:meth:`_process_pending_reboots`) rather
        than run now, when ownership is unknown: running it now would either
        skip it forever (Leader sees no quorum) or run it on every node
        (PreferLeader sees only itself).  ``EveryNode`` @reboot is not
        deferred: it is meant to run on every node at boot.
        """
        to_launch = []  # type: List[JobConfig]
        for job in self.cron_jobs.values():
            # job_should_run(startup=True) is True only for an enabled @reboot
            # job, so everything below concerns @reboot one-shots.
            if not self.job_should_run(True, job, None):
                continue
            if self._is_deferrable_reboot(job):
                self._pending_reboot_jobs[job.name] = job
                logger.info(
                    "cluster: deferring @reboot job %s until the cluster "
                    "elects an owner",
                    job.name,
                )
                continue
            if self._cluster_allows(job):
                to_launch.append(job)
        await self._launch_concurrently(to_launch)

    async def _spawn_due_jobs(self, now: datetime.datetime) -> None:
        """Launch the jobs whose next fire has arrived, in config order.

        Due jobs come from the next-fire index; each is advanced past its fired
        slot (with bounded catch-up) BEFORE any launch awaits, so the index is
        already current if the launches yield.
        """
        due = set(self._due_names(now))
        if not due:
            return
        # Build the launch plan in config order. Each due job contributes its
        # list of fire slots (usually one; more only when a slow pass or a
        # forward clock jump missed whole occurrences within CATCHUP_LIMIT).
        plan = []  # type: List[Tuple[JobConfig, List[datetime.datetime]]]
        for name, job in self.cron_jobs.items():
            if name not in due:
                continue
            fires, new_next = self._advance(job, self._next_fire[name], now)
            if new_next is not None:
                self._set_next_fire(name, new_next)
            else:
                # no further occurrence (a fixed past year now behind us):
                # drop it from the index so it is not revisited.
                self._next_fire.pop(name, None)
            plan.append((job, fires))
        await self._launch_plan(plan)

    async def _launch_plan(
        self, plan: List[Tuple[JobConfig, List[datetime.datetime]]]
    ) -> None:
        """Launch a due-job plan.

        One round per catch-up depth: within a round the due jobs launch
        concurrently in config order (so N jobs due in the same slot cost about
        one spawn-time, not N), while a single job's own catch-up replays run
        in successive rounds -- i.e. sequentially -- so its concurrencyPolicy
        still applies between them.  The common case (every job has exactly one
        fire) is a single round.
        """
        rounds = max((len(fires) for _, fires in plan), default=0)
        for r in range(rounds):
            to_launch = []  # type: List[JobConfig]
            for job, fires in plan:
                if r >= len(fires):
                    continue
                # record the slot this fire is for (status/introspection), then
                # gate on the cluster -- recorded whether or not this node runs
                # it, mirroring the old per-slot bookkeeping.
                self._last_run_slot[job.name] = fires[r]
                if self._cluster_allows(job):
                    to_launch.append(job)
            await self._launch_concurrently(to_launch)

    async def _launch_concurrently(self, to_launch: List[JobConfig]) -> None:
        """Launch every job in ``to_launch`` concurrently, in config order.

        Each launch awaits a subprocess spawn, so gathering collapses N due
        jobs from N x spawn-time to about a single spawn-time.  The launches
        are independent (each touches only its own name's running_jobs /
        retry_state entry).  The single-job case (the norm) takes a direct
        await so it is byte-identical to the pre-gather form; empty is a no-op.
        """
        if len(to_launch) == 1:
            await self.launch_scheduled_job(to_launch[0])
        elif to_launch:
            await asyncio.gather(
                *(self.launch_scheduled_job(job) for job in to_launch)
            )

    def _is_deferrable_reboot(self, job: JobConfig) -> bool:
        """Whether ``job`` is an @reboot job whose start must wait for the
        cluster to elect an owner (a ``Leader``/``PreferLeader`` job under
        ``electLeader``)."""
        return (
            self._elect_leader_configured
            and isinstance(job.schedule, str)
            and job.schedule == "@reboot"
            and job.clusterPolicy in ("Leader", "PreferLeader")
        )

    async def _process_pending_reboots(self) -> None:
        """Run each deferred @reboot job once the cluster has elected an owner.

        Each pending job is retired in one of two ways: this node runs it
        because it is the elected owner, or we learn (via the cluster's
        ``reboot_ran`` gossip) that it already ran somewhere and stand down
        without re-running.  We deliberately do *not* drop a job merely because
        some *other* node currently *looks like* the owner: that node may be
        unable to run it (reachable from us but not quorate from its own view),
        and dropping on speculation would lose the one-shot forever.  Dropping
        only on positive confirmation keeps the never-lose property while
        avoiding a re-run when leadership later moves to a node that still held
        the job pending.  If election was turned off on a reload, the
        quorum/owner gating is gone, so a still-pending one-shot runs here as
        long as its name still maps to an ``@reboot`` job (a name reused for a
        non-``@reboot`` job is retired without running, as on the gated path).

        A name that is momentarily *absent* from ``cron_jobs`` (a templating
        glitch or a remove-then-re-add seen mid-reload, before the cluster has
        converged) is **kept pending**, not dropped: dropping on a transient
        absence would lose the one-shot forever and break the never-lose
        property.  The launch is always gated on the name being present *and*
        still a deferrable @reboot, so a genuinely-removed job never runs, and
        we run the *current* ``cron_jobs[name]`` (never the stale config we
        captured at boot) so a name later reused for a different job runs the
        live definition.  If a name is reused for a job that is no longer a
        deferrable @reboot (e.g. it became ``EveryNode`` or a real schedule),
        the pending entry is retired and the new job is left to its own
        scheduling.
        """
        if not self._pending_reboot_jobs:
            return
        if not self._elect_leader_configured:
            # election removed on reload: the quorum/owner gating is gone, so
            # run the *current* job for any present name still defining an
            # @reboot one-shot. A momentarily-absent name is kept pending (not
            # popped) for the same never-lose reason as the gated path below;
            # a name reused for a non-@reboot job is retired without running
            # (its new definition schedules itself), mirroring that path.
            for name in list(self._pending_reboot_jobs):
                job = self.cron_jobs.get(name)
                if job is None:
                    continue  # transiently absent -> keep pending, re-check
                del self._pending_reboot_jobs[name]
                # a job disabled (enabled: false) on the reload that also
                # removed election is retired without running, the same way
                # job_should_run and the manual web trigger refuse a disabled
                # job rather than running it once on convergence. (enabled is
                # checked last so a name reused for a non-@reboot job -- which
                # need not carry the attribute -- short-circuits on the
                # schedule check, as _is_deferrable_reboot does on the gated
                # paths.)
                if (
                    isinstance(job.schedule, str)
                    and job.schedule == "@reboot"
                    and job.enabled
                ):
                    await self.launch_scheduled_job(job)
            return
        mgr = self.cluster_manager
        if mgr is None:
            # Election wanted but no manager (store unreachable / backend
            # failed to start). A Leader @reboot one-shot stays fail-closed --
            # keep it pending and re-evaluate once a manager comes up. But a
            # PreferLeader one-shot's contract is NEVER-SKIP: it must run
            # somewhere even when the store is unreachable (accepting a
            # possible double-run), exactly the asymmetry _cluster_allows
            # applies to *scheduled* PreferLeader jobs in this same mgr-is-None
            # case. Not mirroring it here would drop the one-shot forever on a
            # persistent start failure -- the very store-unreachable case
            # PreferLeader exists to survive. There is no store to record the
            # run to, so a later-starting manager may not see it ran; that is
            # the documented PreferLeader double-run cost, strictly better than
            # never running.
            for name in list(self._pending_reboot_jobs):
                if name not in self.cron_jobs:
                    continue  # transiently absent -> keep pending (never-lose)
                job = self.cron_jobs[name]
                if not self._is_deferrable_reboot(job) or not job.enabled:
                    # name reused for a non-deferrable job, or the job was
                    # disabled (enabled: false) on a reload while it sat
                    # pending -> retire it without running, as job_should_run
                    # refuses a disabled job on the normal scheduled path.
                    del self._pending_reboot_jobs[name]
                    continue
                if job.clusterPolicy == "PreferLeader":
                    del self._pending_reboot_jobs[name]
                    logger.info(
                        "cluster: running deferred @reboot PreferLeader job "
                        "%s (no leadership manager; never-skip semantics)",
                        name,
                    )
                    await self.launch_scheduled_job(job)
                # Leader one-shots: keep pending, fail closed, re-check next
                # wakeup once a manager is available.
            return
        for name in list(self._pending_reboot_jobs):
            if name not in self.cron_jobs:
                # Absent right now -- but @reboot only defers at startup, so a
                # name that vanishes mid-reload (templating glitch, transient
                # remove-then-re-add) could otherwise be lost forever. Keep it
                # pending and re-evaluate next wakeup; the launch below is
                # gated on presence, so a genuinely-removed job never runs.
                continue
            job = self.cron_jobs[name]
            if not self._is_deferrable_reboot(job) or not job.enabled:
                # the name was reused for a job that is no longer a deferrable
                # @reboot (now EveryNode, or a real schedule), or the job was
                # disabled (enabled: false) on a reload while it sat pending:
                # retire the stale entry without running it, mirroring
                # job_should_run, which refuses a disabled job.
                del self._pending_reboot_jobs[name]
                continue
            try:
                already_ran = mgr.reboot_ran(name)
            except Exception:
                # A backend read must not escape: _process_pending_reboots is
                # called from spawn_jobs, OUTSIDE run()'s try/except, so a
                # raise here would kill the whole scheduler. Treat it as "not
                # known to have run" and keep the job pending (never-lose);
                # re-evaluate next wakeup. Mirrors the _cluster_allows guard.
                logger.exception(
                    "cluster: error checking whether @reboot job %s already "
                    "ran; keeping it pending",
                    name,
                )
                continue
            if already_ran:
                # positive confirmation it already ran in the cluster -> retire
                # it without re-running (this is what prevents a re-run when
                # leadership later lands on a node that still held it pending).
                del self._pending_reboot_jobs[name]
                logger.info(
                    "cluster: deferred @reboot job %s already ran in the "
                    "cluster; standing down here",
                    name,
                )
                continue
            # Gate on the SAME boolean owner check as a scheduled job
            # (_cluster_allows), not a name comparison: a lease backend's
            # leader_name() reports the holder's display *identity*
            # (cluster.kubernetes.identity), which may legitimately differ from
            # node_name -- comparing those two would make the holder fail to
            # recognise itself, so the one-shot would never run on any node.
            # is_leader()/is_available_leader()/is_job_owner() self-recognise
            # correctly regardless of an identity != nodeName mismatch.
            if self._cluster_allows(job):
                del self._pending_reboot_jobs[name]
                logger.info(
                    "cluster: running deferred @reboot job %s (this node is "
                    "the elected owner)",
                    name,
                )
                # Record (and eagerly gossip / persist) intent-to-run BEFORE
                # spawning, not after: a crash in the launch->record window
                # would otherwise leave no peer/store aware it ran, so the
                # failover owner re-runs it. Recording first means the worst
                # case is a recorded-but-not-actually-run one-shot (at-most-
                # once preserved), not a double-run.
                await mgr.mark_reboot_ran(name)
                await self.launch_scheduled_job(job)
            # else: another node is (or will be) the owner, or the cluster has
            # not converged yet (no quorum / a nodeName conflict) -> keep the
            # job pending and re-evaluate next wakeup. Never drop a one-shot on
            # another node's behalf: see this method's docstring.

    def _cluster_allows(self, job: JobConfig) -> bool:
        """Whether this node may run *scheduled* ``job`` this cycle.

        Always true unless leader election is enabled
        (``cluster.electLeader``); then it depends on the job's
        ``clusterPolicy``:

        * ``EveryNode`` — run on every replica, independent of cluster state
          (so these jobs keep firing even if the manager failed to start);
        * ``Leader`` (default) — only the quorum-gated elected owner runs it
          (at-most-once; skips when there is no quorum);
        * ``PreferLeader`` — the reachable agreeing owner runs it, ignoring
          quorum (never skips while a node is up, but may double-run across a
          partition).

        Under the default ``distribution: single-leader`` the "owner" is the
        one cluster-wide elected leader (so all ``Leader`` jobs run on that
        node); under ``distribution: spread`` it is a *per-job*
        rendezvous-hashed owner, so leader-gated work fans out across the
        quorate nodes.  Both keep the same quorum gate and the same guarantee.

        When election is configured but no manager is running, ``Leader`` fails
        *closed* (return False) so a broken cluster does not make every replica
        fire, while ``PreferLeader`` (never-skip) runs anyway -- a node with no
        manager is the "store unreachable" case its contract already accepts a
        double-run for, so skipping it would drop the job to at-most-zero.
        Manual (API) triggers go through
        ``maybe_launch_job`` directly and are deliberately *not* gated (an
        explicit operator action runs where it is invoked).  Scheduled-job
        *retries* re-check this gate in ``schedule_retry_job`` before
        relaunching: a retry that outlives the leadership it began under is
        abandoned once another node positively owns the job (rather than
        double-running across a failover), but merely deferred and re-checked
        on a transient fail-closed denial (see ``_cluster_owner_moved``).

        A detected conflict (``mgr.has_conflict()`` — a duplicate ``nodeName``,
        an agreeing peer declaring a different cluster size ``N``, or an
        agreeing peer running a different coordination policy
        (``distribution`` / ``electLeader``)) additionally makes ``Leader``
        jobs fail closed: the quorum election is unsafe while two nodes share a
        name, disagree on ``N`` (either lets two nodes each elect themselves),
        or pick owners by different rules (which would double-run or drop a
        ``Leader`` job), so skipping is the at-most-once-preserving choice.
        ``PreferLeader`` is left running — it already accepts double-runs as
        the price of never skipping.
        """
        if not self._elect_leader_configured:
            return True
        if job.clusterPolicy == "EveryNode":
            return True
        mgr = self.cluster_manager
        if mgr is None:
            # Election is configured but no manager is running (it failed to
            # start, or a reload tore the old one down and the rebuild raised).
            # That is precisely the "store/quorum unreachable" condition, so
            # honour each policy's contract: Leader fails CLOSED
            # (at-most-once), but PreferLeader is never-skip -- it must run
            # anyway (accepting a possible double-run) rather than be silently
            # skipped on every replica, which for a fleet-wide start failure
            # would drop the job to at-most-ZERO, defeating the whole point of
            # PreferLeader.
            return bool(job.clusterPolicy == "PreferLeader")
        try:
            if mgr.distribution == "spread":
                if job.clusterPolicy == "PreferLeader":
                    return mgr.is_available_job_owner(job.name)
                if mgr.has_conflict():
                    return False  # "Leader": fail closed on duplicate nodeName
                return mgr.is_job_owner(job.name)  # "Leader"
            if job.clusterPolicy == "PreferLeader":
                return mgr.is_available_leader()
            if mgr.has_conflict():
                return False  # "Leader": fail closed on a duplicate nodeName
            return mgr.is_leader()  # "Leader"
        except Exception:
            # A backend read should never raise, but a bug in one (a bad gossip
            # payload reaching election, a KeyError in rendezvous hashing) must
            # not escape: spawn_jobs runs OUTSIDE the run loop's try/except, so
            # an exception here would kill the whole scheduler -- including the
            # EveryNode jobs meant to survive a broken manager. Fail closed
            # (skip this leader-gated job) and keep scheduling.
            logger.exception(
                "cluster: error evaluating the leader gate for job %s; "
                "failing closed (skipping it this cycle)",
                job.name,
            )
            return False

    def _log_cluster_role(self) -> None:
        """Log this node's run-eligibility transitions (once per change).

        Quorum membership is logged in both modes, so any node -- leader or
        follower -- records losing (and regaining) quorum, the gate that
        decides whether the cluster can run leader-gated work at all.
        Single-leader mode additionally logs this node acquiring or losing the
        one scheduled-job leadership.
        """
        if not self._elect_leader_configured:
            return
        # spawn_jobs (this method's only caller) runs OUTSIDE run()'s
        # try/except, so an exception from any backend read below would kill
        # the whole scheduler -- the failure mode _cluster_allows is hardened
        # against, but this method runs one step earlier on the same unguarded
        # path. It only logs, so swallow any backend-read error and keep
        # scheduling (the run/skip decision stays fail-closed in
        # _cluster_allows).
        try:
            self._emit_cluster_role_logs()
        except Exception:
            logger.exception(
                "cluster: error while logging cluster role; continuing"
            )

    def _emit_cluster_role_logs(self) -> None:
        mgr = self.cluster_manager
        if mgr is not None:
            # A duplicate nodeName is a misconfiguration that pauses Leader
            # jobs cluster-wide; log it loudly (and the recovery), once per
            # transition, so an operator notices and fixes the names.
            conflict = mgr.conflict_names()
            if bool(conflict) != self._was_conflict:
                if conflict:
                    logger.error(
                        "cluster: duplicate nodeName detected (%s) -- Leader "
                        "jobs will stand down until every node has a unique "
                        "cluster.nodeName",
                        ", ".join(conflict),
                    )
                else:
                    logger.info(
                        "cluster: nodeName conflict resolved; Leader jobs may "
                        "run again"
                    )
                self._was_conflict = bool(conflict)
            # A cluster-size disagreement (divergent peer lists) breaks the
            # quorum proof exactly as a duplicate nodeName does; log it just as
            # loudly so an operator reconciles cluster.peers (e.g. an in-flight
            # resize that has not finished rolling out).
            size_conflict = mgr.conflicting_sizes()
            if bool(size_conflict) != self._was_size_conflict:
                if size_conflict:
                    logger.error(
                        "cluster: cluster-size disagreement -- agreeing peers "
                        "declare %s but we declare %d; Leader jobs will stand "
                        "down until every node's cluster.peers agree on the "
                        "member set",
                        ", ".join(str(s) for s in size_conflict),
                        mgr.cluster_size(),
                    )
                else:
                    logger.info(
                        "cluster: cluster-size disagreement resolved; Leader "
                        "jobs may run again"
                    )
                self._was_size_conflict = bool(size_conflict)
            # A coordination-policy divergence (an agreeing peer running a
            # different distribution / electLeader) selects a different owner
            # and would double-run or drop Leader jobs, so it fails the Leader
            # gate closed exactly like the two conflicts above -- but unlike
            # them it would otherwise stand Leader jobs down cluster-wide with
            # nothing in the log. Surface it just as loudly, once per change.
            policy_conflict = mgr.conflicting_policies()
            if bool(policy_conflict) != self._was_policy_conflict:
                if policy_conflict:
                    logger.error(
                        "cluster: coordination-policy divergence -- agreeing "
                        "peers declare %s; Leader jobs will stand down until "
                        "every node's cluster.distribution and "
                        "cluster.electLeader agree",
                        "; ".join(policy_conflict),
                    )
                else:
                    logger.info(
                        "cluster: coordination-policy divergence resolved; "
                        "Leader jobs may run again"
                    )
                self._was_policy_conflict = bool(policy_conflict)
        # Quorum membership is logged on *every* node, in both modes, so a
        # follower that drops below quorum -- i.e. the whole cluster losing the
        # ability to elect a leader -- leaves a breadcrumb in its own log, not
        # only the ex-leader's (in single-leader mode only the ex-leader's
        # is_leader() flips, so without this a follower logs nothing on quorum
        # loss).
        spread = mgr is not None and mgr.distribution == "spread"
        quorate = mgr is not None and mgr.is_quorate()
        if quorate != self._was_quorate:
            self.metrics.cluster_quorum_transition()
            if spread and quorate:
                logger.info(
                    "cluster: this node joined quorum; "
                    "per-job ownership active"
                )
            elif spread:
                logger.info(
                    "cluster: this node left quorum; per-job ownership "
                    "suspended"
                )
            elif quorate:
                logger.info("cluster: this node joined quorum")
            else:
                logger.info(
                    "cluster: this node left quorum; no majority reachable, "
                    "so Leader jobs cannot run until one is"
                )
            self._was_quorate = quorate
        if spread:
            return  # no single leader in spread mode
        leader = mgr is not None and mgr.is_leader()
        if leader != self._was_leader:
            self.metrics.cluster_leader_transition()
            logger.info(
                "cluster: this node %s scheduled-job leadership",
                "acquired" if leader else "lost",
            )
            self._was_leader = leader

    @staticmethod
    def job_should_run(
        startup: bool,
        job: JobConfig,
        slot: Optional[datetime.datetime] = None,
    ) -> bool:
        if not job.enabled:
            logger.debug(
                "Job %s (%s) is disabled in the config",
                job.name,
                job.schedule_unparsed,
            )
            return False
        if startup:
            if isinstance(job.schedule, str) and job.schedule == "@reboot":
                logger.debug(
                    "Job %s (%s) is scheduled for startup (@reboot)",
                    job.name,
                    job.schedule_unparsed,
                )
                return True
            else:
                return False
        if isinstance(job.schedule, CronTab):
            crontab = job.schedule  # type: CronTab
            # schedule_slot truncates to the job's resolution: the top of the
            # minute for a minute-level job (identical to the old
            # replace(second=0)), or the whole second for a second-level one.
            # `slot` is the pass instant precomputed by spawn_jobs so the
            # due-test and the de-dup key are one and the same read; None means
            # a direct caller wants a fresh read.
            if slot is None:
                slot = schedule_slot(job)
            if crontab.test(slot):
                logger.debug(
                    "Job %s (%s) is scheduled for now",
                    job.name,
                    job.schedule_unparsed,
                )
                return True
            else:
                logger.debug(
                    "Job %s (%s) not scheduled for now",
                    job.name,
                    job.schedule_unparsed,
                )
                return False
        else:
            return False

    async def launch_scheduled_job(self, job: JobConfig) -> None:
        await self.cancel_job_retries(job.name)
        assert job.name not in self.retry_state

        retry = job.onFailure["retry"]
        logger.debug("Job %s retry config: %s", job.name, retry)
        if retry["maximumRetries"]:
            retry_state = JobRetryState(
                retry["initialDelay"],
                retry["backoffMultiplier"],
                retry["maximumDelay"],
            )
            self.retry_state[job.name] = retry_state

        await self.maybe_launch_job(job)

    async def maybe_launch_job(self, job: JobConfig) -> bool:
        """Launch ``job`` unless concurrencyPolicy forbids it.

        Returns whether a new instance was actually launched (False only
        for the ``Forbid`` skip), so a caller accounting for launches --
        the retry metric -- does not count a swallowed one.
        """
        if self.running_jobs[job.name]:
            logger.warning(
                "Job %s: still running and concurrencyPolicy is %s",
                job.name,
                job.concurrencyPolicy,
            )
            if job.concurrencyPolicy == "Allow":
                pass
            elif job.concurrencyPolicy == "Forbid":
                return False
            elif job.concurrencyPolicy == "Replace":
                for running_job in self.running_jobs[job.name]:
                    # mark before cancelling so the reaper treats the forced
                    # termination as a replacement, not a job failure.
                    running_job.replaced = True
                    await running_job.cancel()
            else:
                raise AssertionError  # pragma: no cover
        logger.info("Starting job %s", job.name)
        running_job = RunningJob(job, self.retry_state.get(job.name))
        await running_job.start()
        self.running_jobs[job.name].append(running_job)
        logger.info("Job %s spawned", job.name)
        self._jobs_running.set()
        return True

    # continually watches for the running jobs, clean them up when they exit
    async def _wait_for_running_jobs(self) -> None:
        # job -> wait task
        wait_tasks = {}  # type: Dict[RunningJob, asyncio.Task]
        while self.running_jobs or not self._stop_event.is_set():
            try:
                for jobs in self.running_jobs.values():
                    for job in jobs:
                        if job not in wait_tasks:
                            wait_tasks[job] = asyncio.create_task(job.wait())
                if not wait_tasks:
                    try:
                        await asyncio.wait_for(self._jobs_running.wait(), 1)
                    except asyncio.TimeoutError:
                        pass
                    continue
                self._jobs_running.clear()
                # wait for at least one task with timeout
                done_tasks, _ = await asyncio.wait(
                    wait_tasks.values(),
                    timeout=1.0,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                done_jobs = set()
                for job, task in list(wait_tasks.items()):
                    if task in done_tasks:
                        done_jobs.add(job)
                for job in done_jobs:
                    task = wait_tasks.pop(job)
                    try:
                        task.result()
                    except Exception:  # pragma: no cover
                        logger.exception(
                            "Unexpected error while waiting on job %s; "
                            "please report this as a bug (2)",
                            job.config.name,
                        )
                    await self._handle_finished_job(job)
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover
                logger.exception("please report this as a bug (3)")
                await asyncio.sleep(1)

    def _record_run(self, name: str, info: JobRunInfo) -> None:
        # the latest finished run (for status/log replay) plus the bounded
        # history (for the dashboard's history/stats view); in-memory only.
        self.last_run[name] = info
        self.run_history[name].append(info)
        # every recorded run also feeds the Prometheus counters/histogram,
        # so /metrics and the run-history API always agree on outcomes.
        self.metrics.job_run_recorded(name, info.outcome, info.duration)

    async def _handle_finished_job(self, job: RunningJob) -> None:
        jobs_list = self.running_jobs[job.config.name]
        jobs_list.remove(job)
        if not jobs_list:
            del self.running_jobs[job.config.name]

        if job.replaced:
            # deliberately cancelled to make way for a newer instance
            # (concurrencyPolicy=Replace); not a failure, so don't report it
            # or trigger retries.
            logger.info(
                "Job %s was replaced by a newer instance", job.config.name
            )
            return

        if job.cancelled:
            # explicitly cancelled by a user via the web UI: record it (as
            # "cancelled" in the dashboard) but, like a replacement, do not
            # report it as a failure or schedule retries.
            logger.info("Job %s was cancelled via the web UI", job.config.name)
            self._record_run(
                job.config.name,
                JobRunInfo(
                    outcome="cancelled",
                    exit_code=job.retcode,
                    started_at=job.started_at,
                    finished_at=get_now(datetime.timezone.utc),
                    fail_reason="cancelled via web UI",
                    output=job.output,
                ),
            )
            await self.cancel_job_retries(job.config.name)
            return

        if job.start_failed:
            # counted separately from ordinary failures: a command that
            # cannot launch at all (recorded below as a failure with the
            # conventional exit code 127) is usually a deploy/config bug,
            # not a job bug.
            self.metrics.job_start_failed(job.config.name)

        fail_reason = job.fail_reason
        logger.info(
            "Job %s exit code %s; has stdout: %s, "
            "has stderr: %s; fail_reason: %r",
            job.config.name,
            job.retcode,
            str(bool(job.stdout)).lower(),
            str(bool(job.stderr)).lower(),
            fail_reason,
        )
        # record this run for the web UI's "latest status / latest logs" view
        self._record_run(
            job.config.name,
            JobRunInfo(
                outcome="failure" if fail_reason is not None else "success",
                exit_code=job.retcode,
                started_at=job.started_at,
                finished_at=get_now(datetime.timezone.utc),
                fail_reason=fail_reason,
                output=job.output,
            ),
        )
        if fail_reason is not None:
            await self.handle_job_failure(job)
        else:
            await self.handle_job_success(job)

    async def handle_job_failure(self, job: RunningJob) -> None:
        if self._stop_event.is_set():
            return
        if job.stdout:
            logger.info(
                "Job %s STDOUT:\n%s", job.config.name, job.stdout.rstrip()
            )
        if job.stderr:
            logger.info(
                "Job %s STDERR:\n%s", job.config.name, job.stderr.rstrip()
            )
        await job.report_failure()

        # Handle retries...
        state = job.retry_state
        if state is None or state.cancelled:
            self.metrics.job_permanent_failure(job.config.name)
            await job.report_permanent_failure()
            return

        logger.debug(
            "Job %s has been retried %i times", job.config.name, state.count
        )
        if state.task is not None:
            if state.task.done():
                await state.task
            else:
                state.task.cancel()
        retry = job.config.onFailure["retry"]
        if (
            state.count >= retry["maximumRetries"]
            and retry["maximumRetries"] != -1
        ):
            await self.cancel_job_retries(job.config.name)
            self.metrics.job_permanent_failure(job.config.name)
            await job.report_permanent_failure()
        else:
            retry_delay = state.next_delay()
            state.task = asyncio.create_task(
                self.schedule_retry_job(
                    job.config.name, retry_delay, state.count
                )
            )

    async def schedule_retry_job(
        self, job_name: str, delay: float, retry_num: int
    ) -> None:
        logger.info(
            "Cron job %s scheduled to be retried (#%i) in %.1f seconds",
            job_name,
            retry_num,
            delay,
        )
        await asyncio.sleep(delay)
        deferrals = 0
        while True:
            try:
                job = self.cron_jobs[job_name]
            except KeyError:
                logger.warning(
                    "Cron job %s was scheduled for retry, but "
                    "disappeared from the configuration",
                    job_name,
                )
                # clear the now-stale retry state and stop; falling through
                # here would call maybe_launch_job(job) with an unbound 'job'.
                self.retry_state.pop(job_name, None)
                return
            # Re-check the leadership gate before relaunching: a retry can
            # outlive the leadership it started under (a partition / quorum
            # loss / reload moved ownership while we slept), and
            # maybe_launch_job does NOT gate. Relaunching unconditionally
            # would run a Leader-policy job here while the new owner also
            # runs it on its next tick -- the exact double-run the
            # abstraction promises to prevent.
            if self._cluster_allows(job):
                break
            if self._cluster_owner_moved(job):
                # ownership genuinely moved: end this node's retry sequence
                # (the new owner picks up the job's future scheduled firings,
                # not this failed attempt; see _abandon_retry for the
                # @reboot-one-shot caveat).
                self._abandon_retry(job_name, retry_num)
                return
            # A transient fail-closed denial (lost quorum, a nodeName/size/
            # policy conflict, a backend read error, no manager): this node
            # may well still be the rightful owner, and ending the sequence
            # here would end it EVERYWHERE for an @reboot keep-alive job
            # (maximumRetries: -1) -- reboot_ran was recorded before the
            # first launch, so no other node ever restarts it. Keep the
            # retry alive and re-check the gate after another delay.
            state = self.retry_state.get(job_name)
            if state is None or state.cancelled or self._stop_event.is_set():
                # the sequence ended (success / cancellation / shutdown)
                # while we deliberated: nothing left to keep alive.
                return
            recheck = max(delay, RETRY_GATE_RECHECK_FLOOR)
            # first deferral at INFO (the operator-visible event), repeats at
            # DEBUG: a long gate-closed outage with a tiny initialDelay would
            # otherwise emit this line about once per second for its whole
            # duration (the RETRY_GATE_RECHECK_FLOOR cadence).
            log = logger.info if deferrals == 0 else logger.debug
            deferrals += 1
            log(
                "Cron job %s retry (#%i) deferred: the cluster does not "
                "currently allow this node to run it and no other node "
                "positively owns it; re-checking in %.1f seconds",
                job_name,
                retry_num,
                recheck,
            )
            await asyncio.sleep(recheck)
        # counted on the launch result (not where the retry is armed) so the
        # counter reports retries actually launched -- net of cancellations,
        # abandonments, and a concurrencyPolicy=Forbid skip.
        if await self.maybe_launch_job(job):
            self.metrics.job_retry_launched(job_name)

    def _cluster_owner_moved(self, job: JobConfig) -> bool:
        """Whether another node is *positively* identified as ``job``'s owner.

        Used by ``schedule_retry_job`` to tell a genuine ownership move
        (another node runs the job on its own schedule, so a pending retry
        may be abandoned without double-running) from a transient fail-closed
        denial of ``_cluster_allows`` (lost quorum, a conflict, a
        still-converging view, a backend read error, no manager), where this
        node may well still be the rightful owner and abandoning would end
        the sequence for good.
        Decided from the seam's self-recognising ``is_available_*`` reads --
        never a display-name comparison -- so a lease holder in its
        self-demotion window (still observing itself as holder while
        ``is_leader()`` already reports False) is not mistaken for a move.
        """
        mgr = self.cluster_manager
        if mgr is None:
            return False  # election fails closed here; nobody else owns it
        try:
            if mgr.has_conflict():
                # the election is unsafe while nodes conflict: nobody is
                # positively the owner, so treat the denial as transient
                return False
            if not mgr.is_quorate():
                # no trustworthy view of leadership -> no positive owner
                return False
            if not mgr.view_settled():
                # a freshly rebuilt gossip manager holds the never-skip
                # available_* gates closed while peers re-attest its new
                # instance_id -- even on the rightful owner, and even while
                # QUORATE (quorum needs only a majority attesting us; the
                # hold waits for every current-build agreeing peer). A False
                # from those gates is then the hold, not an observed move;
                # abandoning here would end the sequence for good (fatal for
                # an @reboot keep-alive). Bounded (~2 poll intervals), so
                # defer and re-check like any transient denial.
                return False
            if mgr.distribution == "spread":
                return not mgr.is_available_job_owner(job.name)
            return not mgr.is_available_leader()
        except Exception:
            # mirrors _cluster_allows: a backend read error is a transient
            # fail-closed condition, never a confirmed ownership move.
            logger.exception(
                "cluster: error checking whether ownership of job %s moved; "
                "treating the denial as transient",
                job.name,
            )
            return False

    def _abandon_retry(self, job_name: str, retry_num: int) -> None:
        """End a pending retry sequence whose job's ownership moved off-node.

        Marks the state cancelled BEFORE dropping it: a RunningJob launched
        while the retry sat pending (a manual API start, a concurrencyPolicy
        Allow overlap) captured this same JobRetryState, and its own later
        failure would otherwise re-arm a retry on a state no longer in
        ``retry_state`` -- which ``cancel_job_retries`` could never find or
        cancel, so the orphan would relaunch the job even after a later
        successful run.
        """
        state = self.retry_state.get(job_name)
        if state is not None:
            state.cancelled = True
        self.retry_state.pop(job_name, None)
        # Wording note: the new owner picks up future *scheduled* firings; it
        # does NOT re-run this failed attempt, and an @reboot one-shot has no
        # future firing at all (its boot run is already recorded), so the
        # message must not promise the job "runs elsewhere".
        logger.warning(
            "Cron job %s retry (#%i) abandoned: the cluster moved ownership "
            "of it to another node; onPermanentFailure will not fire for "
            "this sequence, this attempt is not re-run elsewhere, and any "
            "future scheduled firings happen on the new owner (an @reboot "
            "one-shot has none)",
            job_name,
            retry_num,
        )
        # Record the abandonment in the run history, like a web-UI
        # cancellation: the sequence ended without a verdict on the job
        # itself, so the dashboard should show why the retries stopped.
        # There is no RunningJob at this point, so no report hook (and no
        # statsd metric, which is per-run) can fire; the record and the
        # WARNING above are the observable trace.
        output = JobOutputStream()
        output.close()
        self._record_run(
            job_name,
            JobRunInfo(
                outcome="cancelled",
                exit_code=None,
                started_at=None,
                finished_at=get_now(datetime.timezone.utc),
                fail_reason="retry abandoned: cluster ownership moved to "
                "another node",
                output=output,
            ),
        )

    async def handle_job_success(self, job: RunningJob) -> None:
        await self.cancel_job_retries(job.config.name)
        await job.report_success()

    async def cancel_job_retries(self, name: str) -> None:
        try:
            state = self.retry_state.pop(name)
        except KeyError:
            return
        state.cancelled = True
        if state.task is not None:
            if state.task.done():
                await state.task
            else:
                state.task.cancel()
