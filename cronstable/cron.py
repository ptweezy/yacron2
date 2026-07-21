import asyncio
import asyncio.subprocess
import datetime
import hashlib
import heapq
import hmac
import importlib.resources
import json
import logging
import logging.config
import os
import socket
import ssl
import zlib
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass
from functools import lru_cache, partial
from typing import (  # noqa
    TYPE_CHECKING,
    Any,
    Awaitable,
    Coroutine,
    Deque,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

if TYPE_CHECKING:  # the loopback job-state API is imported lazily at runtime
    from cronstable.jobapi import JobStateAPI

import aiohttp
from aiohttp import web

import cronstable.version
from cronstable import _json, platform, tlsutil
from cronstable.config import (
    ClusterConfig,
    ConfigError,
    CronstableConfig,
    DagConfig,
    JobConfig,
    JobDefaults,
    LoggingConfig,
    MCPConfig,
    StateConfig,
    WebConfig,
    cluster_config_warnings,
    parse_config_string,
    parse_config_with_sources,
    schedule_object_to_crontab,
)
from cronstable.cronexpr import CronTab
from cronstable.croninfo import (
    ScheduleEntry,
    describe_cron,
    duplicate_schedules,
    lint_schedule,
    next_fires,
    schedule_pressure,
    suggest_slot,
    why_no_run,
)
from cronstable.dagrun import DagScheduler
from cronstable.fingerprint import job_digest, job_set_id
from cronstable.ical import CalendarEntry, render_calendar
from cronstable.job import (
    JobOutputStream,
    JobRetryState,
    RunningJob,
    SlaBreachContext,
    report_sla_breach,
)
from cronstable.leadership import LeadershipBackend, make_backend
from cronstable.prometheus import (
    CONTENT_TYPE_OPENMETRICS,
    CONTENT_TYPE_TEXT,
    PrometheusMetrics,
    resolve_metrics_config,
)
from cronstable.redact import redact_lines
from cronstable.resources import (
    NodeResourceSampler,
    ResourceUsage,
    resolve_node_history_config,
)
from cronstable.state import Lease, StateBackend, make_state_backend

logger = logging.getLogger("cronstable")
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
# Hard cap on how many missed occurrences a single job replays on restart under
# onMissed: run-all, so a long outage (or a per-second job) cannot stampede
# or spin the loop enumerating occurrences. The newest bound-fitting window is
# preferred via startingDeadlineSeconds; this is the backstop when no deadline
# is set. Coalescing (run-once) is always exactly one launch regardless.
MAX_CATCHUP_OCCURRENCES = 100
# How many finished runs to retain per job for the web UI's history/stats view.
# In-memory only (like the rest of the run record), and bounded so a frequently
# scheduled job cannot grow memory without limit.
RUN_HISTORY_LIMIT = 50
# First page size the onlyIfLastSucceeded gate reads from the durable run
# ledger (see _depends_on_past_ok). The gate needs only the newest success/
# failure record, so it probes this many newest records and widens to the
# full RUN_HISTORY_LIMIT window only when the whole probe page is non-run
# outcomes (cancelled/skipped) -- reading a few records instead of 50 on the
# common path, while preserving the same skip window.
DEPENDS_GATE_PROBE = 8
# How many compact run summaries to embed per job in the /jobs payload — enough
# for the dashboard's inline sparkline without shipping the full detailed
# history on every poll. The full history is available from /jobs/{name}/runs.
JOBS_INLINE_HISTORY = 20
# Prefix under which a job's finished-run records live in the durable state
# store (cronstable.state), one stream per job. Scoped by
# JOB NAME (stable across
# config edits) rather than job-set id, so restart-durable history survives an
# ordinary reload instead of being orphaned every time the config changes.
RUN_STREAM_PREFIX = "runs/"
# Prefix for a job's archived captured output (opt-in archiveOutput), one
# stream per job, pruned to the same maxRunsPerJob bound as the run ledger.
LOG_STREAM_PREFIX = "logs/"
# Prefix for a job's catch-up checkpoint stream: an "open" intent is recorded
# before a backfill is scheduled and a "close" after it completes, so a
# restart mid-backfill (or mid-jitter) resumes from the intent's watermark
# instead of silently forfeiting the owed runs -- the run ledger's derived
# watermark alone cannot tell a backfilled slot from an ordinary run that
# advanced it past the still-missing slots.  At-least-once: a crash between
# the last launch and the close record replays; that is the documented trade.
CATCHUP_STREAM_PREFIX = "catchup/"
# How many checkpoint records to retain per job (each cycle writes two).
CATCHUP_STREAM_KEEP = 8
# Upper bound on any single awaited state-store READ issued from scheduling
# paths (the depends-on-past gate, the catch-up watermark, rehydration).  A
# hung mount (dead NFS server) must degrade the stateful features, never
# stall job scheduling: past the timeout the read is abandoned (its daemon
# worker thread is left to the OS) and the caller falls back.
STATE_OP_TIMEOUT = 10.0
# Backstop cap on the tracked fire-and-forget durable-write set. Each write is
# individually bounded by STATE_OP_TIMEOUT (see _track_state_write callers), so
# a wedged mount already drains the backlog at rate x timeout instead of
# forever; this cap is the second line of defence for a pathological write rate
# against a slow store, shedding new best-effort writes rather than letting the
# set (and its buffered records) grow without bound -> OOM. Sized well above
# any plausible healthy burst (a whole fleet firing in one slot drains in ms on
# a live backend), so it only trips when writes are genuinely not completing.
MAX_PENDING_STATE_WRITES = 8192
# How long to wait before re-evaluating catch-up when it could not resolve on
# a pass -- the state backend had not (re)started yet, or the cluster had not
# converged on an owner.  Keeps the retry off the per-second hot path.
CATCHUP_RECHECK_INTERVAL = 30.0
# Longest a backfill launch waits for a non-Forbid job to go idle between
# its serialized launches.  For Allow/Replace the wait is anti-stampede
# pacing, not correctness, so it must not starve forever when the job's
# scheduled instances always overlap; Forbid waits unbounded (launching
# would be swallowed).
CATCHUP_IDLE_WAIT_LIMIT = 30.0
# Floor (seconds) for the gate re-check interval of a deferred fail-closed
# retry: the cluster gate can stay closed for a while, and a job configured
# with a tiny/zero backoff delay must not hot-loop the scheduler (and spam the
# log) while it waits. See schedule_retry_job.
RETRY_GATE_RECHECK_FLOOR = 1.0
# Prefix for a job's durable retry-ladder stream: a "pending" record (with an
# ABSOLUTE notBefore deadline and the job's per-job config digest) is written
# when a retry is armed, and a "settled" record when the ladder resolves
# (launched / succeeded / superseded / exhausted / ...). Newest record wins:
# a boot that finds a "pending" on top re-arms the retry with only the
# remaining delay (see _rehydrate_retries). Job-name scoped like the run
# ledger; the digest inside the record is what invalidates on config change.
RETRY_STREAM_PREFIX = "retries/"
# How many retry-ladder records to retain per job (each ladder writes a
# handful; only the newest is ever read back).
RETRY_STREAM_KEEP = 8
# Prefix for a job's @reboot boot-marker stream (standalone dedupe): a marker
# records which HOST ran the job during which OS BOOT (boot_id / derived boot
# time) for which job definition (digest). A daemon restart within the same
# boot skips the re-run; a genuine reboot, a redefined job, or an unreadable
# marker runs it (at-least-once, today's behaviour). Host-scoped inside the
# records so several standalone daemons may share one store.
REBOOT_STREAM_PREFIX = "reboot/"
# Markers retained per job: bounds the stream while keeping enough history
# for a modest number of hosts sharing one store standalone.
REBOOT_STREAM_KEEP = 32
# Wall-clock slack when comparing DERIVED boot times (now - uptime): the
# derivation rides the current wall clock, so an NTP step shifts it. Two real
# boots are further apart than this in practice; where an exact boot_id
# exists (Linux) it is used instead and this never applies.
BOOT_TIME_TOLERANCE = 60.0
# Prefix for the per-HOST job manifest streams: each node periodically
# records the job names its loaded config defines to its OWN stream
# (``manifests/<host>``), mirroring COUNTER_STREAM_PREFIX. The union of
# RECENT manifests (every host's stream, same deploymentId) is what anchors
# cross-jobset garbage collection: a job stream is garbage only when nobody
# has claimed its name for state.gcGraceSeconds. Per-host (rather than one
# stream shared and count-pruned across the whole fleet) so the retained
# history a node can prove absence over never shrinks as the fleet grows --
# a single shared stream's count-based prune was reached by write VOLUME
# (nodes x writes/day), so past a fleet-size threshold the retained span fell
# under gcGraceSeconds and GC deferred forever, growing every removed job's
# streams without bound.
MANIFEST_STREAM_PREFIX = "manifests/"
# Manifest records retained per HOST (count-pruned; independent of fleet
# size). At 4 manifests/node/day, 512 records span ~128 days for any single
# host, comfortably outliving any realistic gcGraceSeconds regardless of how
# many other nodes share the store. (The GC pass additionally refuses to run
# until the retained history -- across every host's stream -- provably
# covers one full grace window.) A host that stops writing (scaled down,
# renamed) leaves its manifest stream at whatever size it last reached; that
# stream is then swept by the normal collect_garbage prefix/keep-set path
# once it ages past grace, exactly like an abandoned counters/<host> stream.
MANIFEST_STREAM_KEEP = 512
# Safety cap on distinct per-host manifest streams read in one GC pass (a
# pathological fleet with churning, never-reused host identities could in
# principle accumulate more members than is worth reading every pass); a
# real deployment is nowhere near this. Truncation is logged, never silent.
MANIFEST_HOSTS_CAP = 2000
# How often each node re-records its manifest (also written on every backend
# start), and how often the GC pass runs. Loop-clock gated, per process.
STATE_MANIFEST_INTERVAL = 21600.0
STATE_GC_INTERVAL = 86400.0
# Upper bound on one GC pass. Generous (a huge store sweeps many files on a
# worker thread), but finite: an unbounded await on a wedged mount would
# leave the single-flight _gc_task pending forever and silently disable
# automatic GC for the life of the process.
STATE_GC_TIMEOUT = 600.0
# Prefix for the per-HOST durable Prometheus counter snapshots (host-scoped:
# counters are per-process truth, and the host name is the stable identity a
# restart can reclaim, unlike the backend's per-process instance id).
COUNTER_STREAM_PREFIX = "counters/"
COUNTER_STREAM_KEEP = 4
# Minimum seconds between durable counter snapshots. Snapshots piggyback on
# the per-run persist task, so without a floor a per-second job would double
# every durable write for a low-value gain; the tail is flushed at shutdown.
COUNTER_SNAPSHOT_INTERVAL = 15.0
# Prefix for a job's in-flight run stream (newest-wins, like retries/): an
# "open" record lands when a job's FIRST live instance starts and a "closed"
# record when its LAST one finishes, so a crash leaves "open" on top and the
# next rehydration (same host) or slot takeover (another node) can make the
# interrupted run visible instead of it silently vanishing from the ledger.
# Written only when a state backend is configured.
INFLIGHT_STREAM_PREFIX = "inflight/"
INFLIGHT_STREAM_KEEP = 8
# Prefix for a job's concurrency-slot signalling stream (cancel requests for
# cluster-scoped Replace); the slot LEASE shares the same "slots/<name>"
# name in the lease namespace. See maybe_launch_job/_claim_cluster_slot.
SLOT_STREAM_PREFIX = "slots/"
SLOT_STREAM_KEEP = 8
# Lease-name prefix serializing cross-node retry claims (and the claiming
# side of the consume path) for one job; TTL bounds a crashed claimer.
RETRY_CLAIM_PREFIX = "retry-claim/"
RETRY_CLAIM_TTL = 30.0
# How stale (seconds past due) a foreign host's pending retry must be before
# the claim scan may take it over. This only covers a live owner whose fire
# is slightly late (slow loop, small clock skew); it CANNOT cover an owner
# deferring on a closed cluster gate, whose re-check cadence is its own
# ladder delay -- the consume-time newest-record re-check under the claim
# lease is what prevents a double-fire there, and is load-bearing.
RETRY_CLAIM_GRACE = 30.0
# Runtime pause/resume (POST /jobs/{name}/pause): how long a pause lasts
# when the caller gives neither a duration nor an explicit until, the hard
# ceiling on any pause window (30 days; a longer stop is a config edit,
# not a runtime toggle), and the accepted sizes of the free-text audit
# fields riding the pause record.
PAUSE_DEFAULT_SECONDS = 3600
PAUSE_MAX_SECONDS = 2592000
PAUSE_NOTE_MAX = 500
PAUSE_BY_MAX = 100
# Prefix for a job's durable pause stream (newest record wins, like
# retries/): a "paused" record carries an ABSOLUTE `until` deadline and is
# superseded by a "resumed" record. Expiry is reader-enforced (nothing in
# the store expires records; see _pause_active). The record's `host` is
# audit info ONLY: unlike retry records, a pause is honored by every node
# sharing the store.
PAUSE_STREAM_PREFIX = "paused/"
PAUSE_STREAM_KEEP = 8
# The per-job SLA check labels: the sla config keys minus their "Seconds"
# suffix. One vocabulary everywhere a check is named: the (job, check)
# breach latch, the cronstable_job_late/cronstable_job_sla_breaches metric
# label, the "sla" payload's "check" field and the onLate {{sla_check}}
# template variable.
SLA_CHECK_STALE = "maxTimeSinceSuccess"
SLA_CHECK_LATE = "lateAfter"
SLA_CHECK_RUNTIME = "maxRuntime"
# The complete, fixed set of check names any SLA surface can produce (the
# only second-half values ever keyed into _sla_state).  Because it is a
# bounded 3-tuple, the per-job latch bookkeeping walks THESE three names
# rather than scanning the whole (name, check) latch map for a matching
# name half -- so a widespread breach across many jobs stays O(jobs), not
# O(jobs x total-latches).
SLA_CHECKS = (SLA_CHECK_STALE, SLA_CHECK_LATE, SLA_CHECK_RUNTIME)
# How deep the boot rehydrate re-reads a job's run ledger when the warmed
# RUN_HISTORY_LIMIT window holds no success at all. Only jobs configuring
# maxTimeSinceSuccess pay for it, and only when they are failing more often
# than the warm window is wide (see _warm_last_success_beyond_history).
SLA_SUCCESS_SCAN_LIMIT = 1000
# How many finished pause windows are kept per job for the maxTimeSinceSuccess
# pause credit. Windows the staleness reference has already overtaken are
# dropped first, so reaching this cap needs that many pauses with no success
# in between; the oldest is then dropped, understating the credit rather than
# overstating it (see _sla_bank_pause).
SLA_PAUSE_SPANS_MAX = 64
# Aggregation windows served by GET /jobs/{name}/trends over the durable
# ledger (label, seconds). Bounded by state.maxRunsPerJob retention.
TREND_WINDOWS: Tuple[Tuple[str, float], ...] = (
    ("1h", 3600.0),
    ("24h", 86400.0),
    ("7d", 604800.0),
    ("30d", 2592000.0),
)
# Newest records the trends endpoint reads per request: with unbounded
# retention (maxRunsPerJob <= 0) an uncapped listing could hold a backend
# worker slot for the whole scan on every dashboard poll.
TREND_SCAN_LIMIT = 5000
# How long a built trends payload is served without re-reading the ledger.
# A trends drawer polls every few seconds and several clients can watch the
# same job; this collapses those to one TREND_SCAN_LIMIT-record read per
# job per window.  Kept short so the age-relative windows barely drift, and
# a locally finished run busts the cache outright (see _record_run), so the
# TTL only bounds ledger writes this node did not make (other cluster
# nodes' runs) -- exactly the (record_count, newest_finished_at) change a
# poll would otherwise re-read the whole ledger to notice.
JOB_TRENDS_CACHE_TTL = 5.0
# requests served without bearer-token auth even when authToken is configured.
# Only the UI page itself (which carries no data and no secrets) is public; the
# browser then authenticates every data request with the token the user enters.
WEB_PUBLIC_PATHS = frozenset({"/"})

# HTTP methods the cross-site Origin gate waves through: none of the web API's
# reads mutate anything, and the browser's same-origin policy already keeps a
# foreign page from READING their responses (no CORS headers are set unless
# the operator adds them). Everything else (the POST control endpoints) is a
# state change and gets the Origin check. OPTIONS passes so the /mcp CORS
# preflight handler keeps answering for its own allow-list.
WEB_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
# Paths that enforce their OWN Origin allow-list and are therefore exempt from
# the app-wide gate: /mcp validates Origin against mcp.allowedOrigins and
# answers CORS preflight itself (see cronstable.mcp.MCPHandler.handle_http);
# gating it here too would 403 a browser MCP client the operator explicitly
# allow-listed there.
WEB_ORIGIN_EXEMPT_PATHS = frozenset({"/mcp"})


def _origin_matches_host(origin: str, host: Optional[str]) -> bool:
    """Whether a browser ``Origin`` header names this request's own ``Host``.

    The same-origin test behind the web API's CSRF/DNS-rebinding gate
    (:meth:`Cron._make_origin_middleware`): a page served by this daemon
    posts with an Origin whose authority is exactly the URL the operator is
    browsing -- the ``Host`` header -- while a foreign page's Origin names
    the attacker's site.  ``Host`` cannot be chosen by the attacker's page
    (the browser sets both headers), so equality is a trustworthy
    same-origin signal.

    Compares authority only (hostname + port, default ports normalized from
    the Origin's scheme -- for a same-origin request both headers come from
    one URL, so their implicit ports agree).  The scheme is deliberately NOT
    compared: behind a TLS-terminating reverse proxy the browser's Origin
    says ``https`` while the daemon speaks plain http, and a strict scheme
    compare would 403 the operator's own dashboard.  ``Origin: null``
    (sandboxed iframes, some redirect chains) and anything unparsable never
    match -- fail closed.
    """
    if not host:
        return False
    try:
        parsed = urlparse(origin)
        if not parsed.scheme or not parsed.hostname:
            return False  # "null", garbage, or a scheme-less token
        default_port = 443 if parsed.scheme == "https" else 80
        origin_port = parsed.port if parsed.port is not None else default_port
        # the Host header is an authority, not a URL; give urlparse a
        # netloc-shaped string so bracketed IPv6 hosts parse correctly.
        hparsed = urlparse("//" + host)
        if not hparsed.hostname:
            return False
        host_port = hparsed.port if hparsed.port is not None else default_port
    except ValueError:
        # urlparse defers port validation to .port; a malformed port in
        # either header can never match anything.
        return False
    # .hostname lowercases both sides already
    return parsed.hostname == hparsed.hostname and origin_port == host_port


class ApiActionError(Exception):
    """A read/control action that failed with a client-facing reason.

    Carries an HTTP-style ``status`` so the shared payload/action helpers can
    raise one place and each surface translate it: the aiohttp handlers into
    the matching ``web.HTTP*`` response, and the MCP layer into an ``isError``
    tool result the model can read and correct.
    """

    def __init__(self, message: str, *, status: int = 400) -> None:
        super().__init__(message)
        self.message = message
        self.status = status


def _http_for_action_error(
    ex: "ApiActionError", headers: Optional[Any] = None
) -> web.HTTPException:
    """Map an :class:`ApiActionError` to the matching aiohttp response.

    ``headers`` (the operator's ``web.headers``) is applied where the pre-MCP
    handlers applied it -- the ``409`` conflict bodies of the job start/cancel
    routes -- and omitted elsewhere, preserving the documented behavior.
    """
    status_map = {
        400: web.HTTPBadRequest,
        403: web.HTTPForbidden,
        404: web.HTTPNotFound,
        409: web.HTTPConflict,
    }
    factory = status_map.get(ex.status, web.HTTPBadRequest)
    # HTTPNotFound rejects a text argument in some aiohttp versions only when
    # the body would conflict; passing the reason as text is supported by all
    # of these 4xx exceptions and gives the caller the actual cause.
    if headers is not None:
        return factory(text=ex.message, headers=headers)
    return factory(text=ex.message)


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


@dataclass(slots=True)
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
    # sampled CPU time + peak RSS for the run, when the job opted into
    # monitorResources; None otherwise (the common case). Defaulted so every
    # existing JobRunInfo construction site stays valid; the reaper fills it
    # from the finished RunningJob.
    resource_usage: Optional[ResourceUsage] = None
    # why a synthetic "skipped" row exists ("paused"); None for real runs.
    # Defaulted for the same construction-site reason as resource_usage.
    skip_reason: Optional[str] = None

    @property
    def duration(self) -> Optional[float]:
        if self.started_at is None:
            return None
        return (self.finished_at - self.started_at).total_seconds()

    def to_dict(self, *, include_series: bool = False) -> Dict[str, Any]:
        """JSON-serializable summary (everything except the output stream).

        ``include_series`` additionally embeds the run's downsampled CPU/RSS
        chart series: on for the durable ledger record (so charts survive
        restarts) and the dedicated resources endpoint, off for the polled
        payloads (/jobs and /jobs/{name}/runs stay summary-sized).

        ``ranAt`` mirrors ``finished_at`` on every row that represents an
        actual run, and is omitted entirely (never nulled: ``derive_max``
        folds over whatever value a present field holds) on a synthetic
        ``skipped`` one.  That gives the durable superseded-by-run guard a
        watermark (:meth:`Cron.durable_last_completed_at`) a pause cannot
        move, while ``finished_at`` stays unfiltered for the catch-up
        watermark, which intentionally advances over pause-skipped slots.
        """
        data: Dict[str, Any] = {
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
            "skip_reason": self.skip_reason,
            # omitted (null) for unmonitored runs so the record shape is
            # unchanged for the default config; a monitored run carries the
            # cpu/rss sub-object (see ResourceUsage.to_dict).
            "resources": (
                self.resource_usage.to_dict(include_series=include_series)
                if self.resource_usage is not None
                else None
            ),
        }
        if self.outcome != "skipped":
            data["ranAt"] = self.finished_at.isoformat()
        return data


@dataclass(slots=True)
class PauseInfo:
    """One job's runtime pause window (see :meth:`Cron.pause_job_by_name`).

    All datetimes are aware UTC.  A record whose ``until`` has passed is
    treated as absent at every read site (:meth:`Cron._pause_active`);
    auto-expiry is reader-enforced, never stored.
    """

    since: datetime.datetime
    until: datetime.datetime
    note: str
    by: str
    channel: str

    def to_dict(self) -> Dict[str, Any]:
        """JSON-safe form (the /jobs "paused" object and pause responses)."""
        return {
            "since": self.since.isoformat(),
            "until": self.until.isoformat(),
            "note": self.note,
            "by": self.by,
            "channel": self.channel,
        }


def _run_stats(runs: List[JobRunInfo]) -> Dict[str, Any]:
    """Aggregate stats over a job's retained run history, for the web UI."""
    total = len(runs)
    success = sum(1 for r in runs if r.outcome == "success")
    failure = sum(1 for r in runs if r.outcome == "failure")
    cancelled = sum(1 for r in runs if r.outcome == "cancelled")
    # crash-reconciled runs: the daemon crashed / lost the store mid-run, so
    # no completion was ever recorded. Bucketed on its own so it neither
    # vanishes into `total` alone nor is miscounted as a real failure, and
    # so the dashboard can call out interrupted runs distinctly.
    unknown = sum(1 for r in runs if r.outcome == "unknown")
    durations = [r.duration for r in runs if r.duration is not None]
    # resource-monitored runs only (monitorResources); an unmonitored history
    # leaves these all None/absent so the dashboard hides the section.
    monitored = [
        r.resource_usage for r in runs if r.resource_usage is not None
    ]
    cpu_totals = [u.cpu_total_seconds for u in monitored]
    rss_values = [u.max_rss_bytes for u in monitored]
    last_usage = runs[-1].resource_usage if runs else None
    return {
        "total": total,
        "success": success,
        "failure": failure,
        "cancelled": cancelled,
        "unknown": unknown,
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
        # CPU time (seconds) and peak resident memory (bytes) over the
        # monitored runs; None when no run in the window was monitored.
        "avg_cpu_seconds": (
            (sum(cpu_totals) / len(cpu_totals)) if cpu_totals else None
        ),
        "max_cpu_seconds": max(cpu_totals) if cpu_totals else None,
        "last_cpu_seconds": (
            last_usage.cpu_total_seconds if last_usage is not None else None
        ),
        "avg_rss_bytes": (
            (sum(rss_values) / len(rss_values)) if rss_values else None
        ),
        "max_rss_bytes": max(rss_values) if rss_values else None,
        "last_rss_bytes": (
            last_usage.max_rss_bytes if last_usage is not None else None
        ),
    }


def _parse_iso_utc(value: Any) -> Optional[datetime.datetime]:
    """Parse an ISO-8601 string to an AWARE datetime, or ``None``.

    Ledger records written by cronstable are always aware UTC, but the parsers
    must survive foreign/hand-written records: a naive timestamp is pinned to
    UTC rather than returned naive, because a naive datetime escaping into
    schedule arithmetic (``_compute_next_fire``) or a ``duration`` subtraction
    raises TypeError against the aware datetimes everything else uses -- and
    on the catch-up path that would crash the scheduler at every boot until
    the record is deleted.
    """
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed


def _in_pause_window(
    when: datetime.datetime,
    window: Tuple[Optional[datetime.datetime], datetime.datetime],
) -> bool:
    """Whether ``when`` falls inside a durable pause window.

    Half-open ``[since, until)``, the same window :meth:`Cron._pause_active`
    enforces live (it reports a pause whose ``until`` has arrived as absent),
    so a slot landing exactly on ``until`` is owed rather than excused.  A
    ``since`` of ``None`` (a record without the field) means the window has
    no known start and covers everything before ``until``.
    """
    since, until = window
    return when < until and (since is None or when >= since)


def _fold_manifest(
    rec: Dict[str, Any],
    names: Set[str],
    hosts: Set[str],
    art_scopes: Set[str],
    live_dags: Set[str],
) -> None:
    """Accumulate one recent manifest record into the GC keep sets.

    Shared by the daemon pass and `cronstable state gc` so both read a
    manifest identically; a missing/mis-typed key contributes nothing (an
    older node's record simply advertises less -- see
    :func:`_manifests_cover_scopes` for why that also gates artifact GC).
    """
    jobs = rec.get("jobs")
    if isinstance(jobs, list):
        names.update(str(job) for job in jobs)
    host = rec.get("host")
    if isinstance(host, str) and host:
        hosts.add(host)
    if isinstance(rec.get("scopes"), list):
        art_scopes.update(str(s) for s in rec["scopes"])
    if isinstance(rec.get("dags"), list):
        live_dags.update(str(d) for d in rec["dags"])


def _manifests_cover_scopes(recent: List[Dict[str, Any]]) -> bool:
    """Whether artifact streams / dag-run documents may be managed at all.

    Only once EVERY recent manifest advertises its scopes and dags: a
    pre-scopes node's manifest proves nothing about the shared artifact
    scopes its jobs may write or the dags it runs, so treating its silence
    as absence would collect a live peer's artifacts mid-rolling-upgrade.
    An empty ``recent`` also fails: with no manifest to anchor absence,
    nothing artifact-related may be collected.
    """
    return bool(recent) and all(
        isinstance(rec.get("scopes"), list)
        and isinstance(rec.get("dags"), list)
        for rec in recent
    )


def _job_run_info_from_dict(
    rec: Dict[str, Any], *, output: Optional[JobOutputStream] = None
) -> Optional["JobRunInfo"]:
    """Rebuild a :class:`JobRunInfo` from a durable ledger record.

    The inverse of :meth:`JobRunInfo.to_dict`, used to warm the in-memory
    history on restart.  The captured output stream is not persisted, so a
    rehydrated run gets an empty, already-closed :class:`JobOutputStream`: the
    dashboard's stats/sparkline never need it, and the log-replay endpoint
    returns an empty (cleanly-terminating) stream for it.  A record missing or
    with an unparseable ``finished_at`` is skipped (returns ``None``) rather
    than crashing the rehydration.

    ``output`` lets a bulk caller that never reads the stream (the trends
    aggregation) supply one shared closed placeholder instead of paying an
    allocation per record; leave it ``None`` for infos that enter
    ``run_history``, where log replay expects each run's own stream.
    """
    # _parse_iso_utc pins naive timestamps to UTC: a rehydrated JobRunInfo
    # mixing naive and aware datetimes would raise TypeError from the
    # `duration` property on every dashboard request.
    finished = _parse_iso_utc(rec.get("finished_at"))
    if finished is None:
        # a crash-reconciled record deliberately omits finished_at so the
        # catch-up watermark stays put (the interrupted slot is still owed
        # under onMissed run-once/run-all); its interruption instant
        # stands in for display ordering only.
        finished = _parse_iso_utc(rec.get("interruptedAt"))
    if finished is None:
        return None
    started = _parse_iso_utc(rec.get("started_at"))
    if output is None:
        output = JobOutputStream()
        output.closed = True
    outcome = rec.get("outcome")
    exit_code = rec.get("exit_code")
    fail_reason = rec.get("fail_reason")
    skip_reason = rec.get("skip_reason")
    return JobRunInfo(
        # an absent/corrupt outcome must NOT rehydrate as a fabricated
        # "success" (it would skew stats and could open the depends-on-past
        # gate); "unknown" is skipped by every outcome-sensitive consumer.
        outcome=outcome if isinstance(outcome, str) else "unknown",
        exit_code=exit_code if isinstance(exit_code, int) else None,
        started_at=started,
        finished_at=finished,
        fail_reason=fail_reason if isinstance(fail_reason, str) else None,
        skip_reason=skip_reason if isinstance(skip_reason, str) else None,
        output=output,
        # ResourceUsage.from_dict tolerates absent/foreign "resources" fields
        # (returns None), so a pre-monitoring or hand-edited record rehydrates
        # cleanly with no resource stats.
        resource_usage=ResourceUsage.from_dict(rec.get("resources")),
    )


@lru_cache(maxsize=1)
def load_index_html() -> str:
    """Return the bundled single-page web UI, cached after first load.

    Read from package data so it works identically for pip installs and the
    PyInstaller binary; falls back to a path relative to this module if the
    importlib.resources lookup is unavailable.
    """
    try:
        return (
            importlib.resources.files("cronstable.web")
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


def _json_response(
    payload: Any,
    *,
    status: int = 200,
    headers: Optional[Any] = None,
) -> web.Response:
    """A JSON ``web.Response`` serialized with the orjson-accelerated encoder.

    Drop-in for :func:`aiohttp.web.json_response` on the data endpoints: it
    serializes with :func:`cronstable._json.dumps_bytes` (orjson when present,
    several times faster than aiohttp's default ``json.dumps``, and compact
    separators so the payload ships fewer bytes) instead of the stdlib.  A web
    response is transient (never a durable, cross-fleet record), so a
    non-finite float or other non-portable value falls back to the stdlib and
    never 500s the endpoint -- exactly as :func:`cronstable.mcp._dumps` does.
    """
    try:
        body = _json.dumps_bytes(payload)
    except _json.UnsupportedValue:
        body = json.dumps(payload, default=str).encode("utf-8")
    return web.Response(
        body=body,
        status=status,
        headers=headers,
        content_type="application/json",
    )


#: Job count at or above which GET /jobs computes its ETag and serializes
#: its body on the default executor instead of inline.  Below it the
#: thread hop costs more than the encode; above it a large fleet's encode
#: must not stall the scheduling loop.
_JOBS_SERIALIZE_OFFLOAD_MIN = 200


def _etag_matches(header: Optional[str], etag: str) -> bool:
    """Whether an ``If-None-Match`` header carries ``etag``.

    Handles the comma-separated list form, the ``*`` wildcard, and a
    ``W/`` weak-validator prefix a cache may echo (``If-None-Match`` uses
    the weak comparison, so a strong/weak difference on the same opaque
    value still matches).
    """
    if not header:
        return False
    for token in header.split(","):
        token = token.strip()
        if token == "*":
            return True
        if token.startswith("W/"):
            token = token[2:]
        if token == etag:
            return True
    return False


def _jobs_conditional_response(
    payload: List[Dict[str, Any]],
    next_fire_iso: Dict[str, Optional[str]],
    if_none_match: Optional[str],
) -> Tuple[str, Optional[bytes]]:
    """The ETag for a ``/jobs`` payload and, unless the client already has
    it, the serialized body.

    The tag is a strong hash of the payload with each job's volatile
    relative ``scheduled_in`` swapped for its STABLE absolute next-fire
    instant.  So it changes exactly when the displayed data changes -- a
    fire advancing, a run starting/finishing, a pause, a reload, a live
    resource sample -- but NOT merely because the countdown ticked down
    between two polls (every client recomputes that locally from the
    poll it holds, so a 304 that keeps the old body stays correct).  Pure
    and free of scheduler state, so it can run on an executor for a large
    fleet without a cross-thread read of ``self``.
    """
    canonical = [
        {**job, "scheduled_in": next_fire_iso.get(job["name"])}
        for job in payload
    ]
    raw = json.dumps(canonical, separators=(",", ":"), default=str).encode(
        "utf-8"
    )
    etag = '"' + hashlib.blake2b(raw, digest_size=16).hexdigest() + '"'
    if _etag_matches(if_none_match, etag):
        return etag, None
    try:
        body = _json.dumps_bytes(payload)
    except _json.UnsupportedValue:
        body = json.dumps(payload, default=str).encode("utf-8")
    return etag, body


async def _sse_send_line(
    resp: web.StreamResponse, stream_name: str, line: str
) -> None:
    payload = _json.dumps_bytes(
        {"stream": stream_name, "line": line.rstrip("\n")}
    ).decode("utf-8")
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


async def _noop_state_write() -> None:
    """Placeholder body for a shed durable write (see _track_state_write).

    Returned as an already-scheduled, immediately-completing task so callers
    that chain on the tracked-write result behave identically whether or not
    the real write was tracked.
    """
    return None


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


def web_site_from_url(
    runner: web.AppRunner,
    url: str,
    ssl_context: Optional[ssl.SSLContext] = None,
) -> web.BaseSite:
    """One listener for ``url``, TLS-wrapped when the url says ``https``.

    ``ssl_context`` is built once per app (re)start and shared by every
    ``https://`` entry; it is applied per *site*, not per runner, so a listen
    list mixing ``http://`` and ``https://`` serves the same app plaintext on
    one port and over TLS on another. ``unix://`` listeners are always
    plaintext: they are already confined to the host's filesystem, where the
    socket's own permissions (``web.socketMode``) are the access control.
    """
    parsed = urlparse(url)
    if parsed.scheme in ("http", "https"):
        if parsed.hostname is None or parsed.port is None:
            # raise ValueError (not AssertionError) so a malformed url is
            # treated as a skippable bad-config entry, not an internal bug.
            # An explicit port is required for https too: aiohttp would
            # otherwise silently default a TLS site to 8443, which is not
            # what an operator who typed "https://0.0.0.0" meant.
            logger.warning(
                "Ignoring web listen url %s: %s url needs host and port",
                url,
                parsed.scheme,
            )
            raise ValueError(url)
        if parsed.scheme == "https" and ssl_context is None:
            # Config validation normally catches this (config._validate_web_tls
            # refuses an https listen with no web.tls), so reaching here means
            # the context failed to BUILD. Skip the listener; serving it in
            # cleartext on the port an operator asked to encrypt would be the
            # one failure mode worse than not serving it.
            logger.warning(
                "Ignoring web listen url %s: no usable web.tls material for "
                "an https listener",
                url,
            )
            raise ValueError(url)
        return web.TCPSite(
            runner,
            parsed.hostname,
            parsed.port,
            ssl_context=ssl_context if parsed.scheme == "https" else None,
        )
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
        # already recorded. See cronstable.prometheus.
        self.metrics = PrometheusMetrics()
        # whole-node CPU/memory sampler for the live node readout (GET /node
        # and, in a gossip cluster, the fleet view). One long-lived instance
        # so its "since last call" CPU% counters stay primed. Cheap and
        # dependency-safe: a no-op yielding None if psutil is unavailable.
        self._node_sampler = NodeResourceSampler()
        # list of cron jobs we /want/ to run
        self.cron_jobs = OrderedDict()  # type: Dict[str, JobConfig]
        # the orchestration DAGs (name -> DagConfig), maintained
        # alongside cron_jobs across reloads; empty keeps the classic no-DAG
        # behaviour.
        self.cron_dags: Dict[str, DagConfig] = OrderedDict()
        # Memoized job-set fingerprint (see job_set_id). The fingerprint is a
        # pure function of cron_jobs, but it is queried on hot, repeating paths
        # (every /metrics scrape, every peer poll, each gossip round, several
        # times per lease renew) while only ever changing on a reload. Computed
        # lazily, cached here, and invalidated (set None) at every point
        # cron_jobs is reassigned.
        self._job_set_id_cache = None  # type: Optional[str]
        # list of cron jobs already running
        # name -> list of RunningJob
        self.running_jobs = defaultdict(list)  # type: Dict[str, List[RunningJob]]
        # name -> the last scheduling slot (a UTC datetime) we launched the job
        # in, retained for status/introspection.  Pruned on reload; the
        # forward-only next-fire index below is what actually de-duplicates
        # launches, so this no longer gates firing. See _launch_plan.
        self._last_run_slot = {}  # type: Dict[str, datetime.datetime]
        # name -> most recent finished run, for the web UI (in-memory only)
        # name -> bounded history of recent finished runs, oldest first, for
        # the web UI's history/stats view (in-memory only, like last_run).
        # Both pruned on reload like _last_run_slot; initialized HERE, before
        # the update_config() below runs _apply_reload the first time.
        self.last_run = {}  # type: Dict[str, JobRunInfo]
        self.run_history = defaultdict(lambda: deque(maxlen=RUN_HISTORY_LIMIT))  # type: Dict[str, Deque[JobRunInfo]]
        # active runtime pauses by job name (POST /jobs/{name}/pause). The
        # fire path consults ONLY this map (never store I/O there); it is
        # rehydrated from the durable paused/ streams at boot and refreshed
        # on the housekeeping tick so peers sharing a store converge within
        # about a minute. Expired entries are ignored by every reader
        # (_pause_active) and swept by the housekeeping pass. Pruned by
        # _apply_reload, so it too must exist before update_config() below.
        self._paused: Dict[str, PauseInfo] = {}
        # monotonic count of local pause/resume writes per job. The
        # _pause_write_tail entry below is level-triggered and its own
        # done-callback deletes it, so a write that starts AND finishes
        # inside a refresh's store read leaves no trace there; this counter
        # is edge-triggered, which is what lets the refresh discard a
        # snapshot taken before a local change landed.
        self._pause_gen: Dict[str, int] = {}
        # pause records that could not be written because the store is
        # configured but down, newest per job, replayed when it comes back
        # (see _defer_pause_write). Both are pruned by _apply_reload like
        # _paused, so both must exist before update_config() below.
        self._pause_pending_writes: Dict[str, Dict[str, Any]] = {}
        # name -> (finished_at, outcome) of the newest success/failure, for
        # the onlyIfLastSucceeded gate (see _depends_on_past_ok). run_history
        # is a bounded ring that a long pause floods with synthetic "skipped"
        # rows, so the gate cannot rely on it alone. Pruned by _apply_reload
        # like the trackers below, so it must exist before update_config().
        self._last_real_outcome: Dict[str, Tuple[datetime.datetime, str]] = {}
        # name -> (monotonic deadline, payload): the short-lived trends cache
        # (see JOB_TRENDS_CACHE_TTL / job_trends_payload). Busted per job by
        # _record_run, so it never outlives a locally finished run.
        self._trends_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        # name -> finished_at of the newest row that represents an actual
        # run (anything but a synthetic "skipped"), for the retry ladder's
        # superseded-by-run guards. last_run alone cannot serve them: a
        # pause-held slot stamps a fresh finished_at on a row where nothing
        # ran, which would settle a pending ladder as if the run happened.
        # Pruned by _apply_reload like the trackers around it, so it must
        # exist before the update_config() below.
        self._last_completed_at: Dict[str, datetime.datetime] = {}
        # The SLA monitor's trackers (see _sla_periodic). All name-keyed
        # runtime state pruned by _apply_reload like _last_run_slot, so all
        # four must exist before the update_config() below.
        # name -> the finished_at of the job's newest known success: fed by
        # _record_run, warmed from the durable ledger at rehydrate. The
        # maxTimeSinceSuccess reference; a job with no entry is baselined on
        # _process_start so a stateless boot never pages instantly.
        self._sla_last_success = {}  # type: Dict[str, datetime.datetime]
        # name -> the newest scheduling slot recorded while the job was NOT
        # paused (pause-skipped slots are excused from lateAfter).
        self._sla_due = {}  # type: Dict[str, datetime.datetime]
        # name -> wall-clock instant of the newest actual launch (scheduled,
        # manual, catch-up or retry); any launch at/after the due slot
        # clears the lateAfter breach condition.
        self._sla_last_start = {}  # type: Dict[str, datetime.datetime]
        # (name, check) -> the instant the breach was first seen: the latch.
        # Present = breached (onLate already fired once); absent = ok.
        # In-memory only, so a restart re-fires a still-breached check once.
        self._sla_state = {}  # type: Dict[Tuple[str, str], datetime.datetime]
        # the maxTimeSinceSuccess fallback baseline (see _sla_last_success).
        self._process_start = get_now(datetime.timezone.utc)
        # name -> when the job first entered cron_jobs; seeded and pruned by
        # _apply_reload (which runs from update_config() below, so every
        # boot-present job is baselined on the process start beside it). The
        # maxTimeSinceSuccess reference for a job with no success on record:
        # a job a RELOAD added to a long-running daemon must age into the
        # breach from when it appeared, not from a process start it predates.
        self._sla_first_seen = {}  # type: Dict[str, datetime.datetime]
        # name -> the (since, ended_at) of finished pause windows, disjoint and
        # newest last. Deliberately held time is credited against
        # maxTimeSinceSuccess, so a resumed job gets a full threshold before it
        # can page instead of paging the instant the pause lifts.
        self._sla_pause_windows: Dict[
            str, List[Tuple[datetime.datetime, datetime.datetime]]
        ] = {}
        # name -> when a job's current DISABLED span began. Banked as a
        # staleness credit (through _sla_bank_pause, the same machinery a
        # pause uses) when the job is re-enabled, so a job that HAD succeeded
        # then sat disabled past maxTimeSinceSuccess does not page the whole
        # switched-off span the instant it is switched back on. Node-local,
        # like _sla_pause_windows.
        self._sla_disabled_since = {}  # type: Dict[str, datetime.datetime]
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
        # names already warned about a schedule with no future occurrence,
        # so the seeding pass says it loudly ONCE instead of every minute;
        # pruned on reload so a changed schedule re-warns if still dead.
        self._dead_schedules = set()  # type: Set[str]
        # wall-clock minute of the last housekeeping pass (config reload,
        # cluster/web (re)start, logging). Gates that work to once per minute
        # even while a second-level job wakes the loop far more often. run().
        self._last_housekeeping_minute: Optional[datetime.datetime] = None
        # Config-reload skip cache. strictyaml is a slow pure-Python parser,
        # so rereading and reparsing the whole config on every once-a-minute
        # housekeeping pass when nothing changed on disk is pure wasted CPU (in
        # a worker thread, but still real work + thread-pool churn). We
        # remember the set of files the last successful parse read, a cheap
        # stat fingerprint of them, and the config it produced; reload_config
        # skips the reparse whenever the fingerprint is unchanged. See
        # _config_signature / reload_config.
        self._config_sources: FrozenSet[str] = frozenset()
        self._config_sig: Optional[tuple] = None
        self._last_config: Optional[CronstableConfig] = None
        self.config_arg = config_arg
        if config_arg is not None:
            self.update_config()
        if config_yaml is not None:
            # config_yaml is for unit testing
            config = parse_config_string(config_yaml, "")
            self.cron_jobs = OrderedDict(
                (job.name, job) for job in config.jobs
            )
            self.cron_dags = OrderedDict((d.name, d) for d in config.dags)
            self._job_set_id_cache = None

        self._wait_for_running_jobs_task = None  # type: Optional[asyncio.Task]
        self._stop_event = asyncio.Event()
        self._jobs_running = asyncio.Event()
        self.retry_state = {}  # type: Dict[str, JobRetryState]
        self.web_runner = None  # type: Optional[web.AppRunner]
        self.web_config = None  # type: Optional[WebConfig]
        # On-disk fingerprint of the web.tls files as the RUNNING listener
        # loaded them. The SSLContext is built once per (re)start and never
        # reloaded, so an in-place rotation (same paths, new bytes, which is
        # how cert-manager / Vault / a Kubernetes secret refresh renews) is
        # otherwise invisible and the daemon serves the old certificate until
        # it expires. Only meaningful while web_runner is not None, which is
        # why every teardown clears it. See _web_tls_files_changed.
        self._web_tls_signature = None  # type: Optional[Dict[str, Any]]
        # the optional MCP server config and its handler. Both track the web
        # app's lifecycle: the handler is (re)built inside start_stop_web_app
        # so it always reflects the current config after a reload.
        self.mcp_config = None  # type: Optional[MCPConfig]
        self._mcp = None  # type: Optional[Any]
        # the leadership backend, when a cluster section is configured
        self.cluster_manager: Optional[LeadershipBackend] = None
        # optional gossip observability overlay: a SECOND, election-inert
        # gossip manager stood up alongside a lease leadership backend so a
        # kubernetes/etcd/filesystem cluster can still share fleet data
        # (per-node CPU/memory + job summaries). None when unused -- including
        # backend: gossip, where the election mesh (cluster_manager) already
        # carries fleet data and IS the fleet backend. See
        # start_stop_observability and _fleet_backend.
        self.observability_mesh: Optional[LeadershipBackend] = None
        # the durable state backend, when a `state` section is configured; None
        # keeps cronstable's classic stateless, in-memory behaviour. See
        # start_stop_state and cronstable.state.
        self.state_backend: Optional[StateBackend] = None
        # in-flight fire-and-forget durable run-record writes, tracked so they
        # are not GC'd mid-flight and can be flushed on shutdown. Durability is
        # never allowed to gate the loop, so _record_run schedules the write
        # here rather than awaiting it.
        self._pending_state_writes: Set[asyncio.Task] = set()
        # in-flight report+retry-arm sequences of finished jobs, spawned off
        # the reaper loop (one slow SMTP/webhook reporter must not stall every
        # other job's completion handling); tracked so shutdown can drain them
        # after the running-job drain, and chained per job name via
        # _completion_tail so overlapping instances of one job are handled in
        # finish order. See _queue_job_completion.
        self._completion_tasks: Set[asyncio.Task] = set()
        self._completion_tail: Dict[str, asyncio.Task] = {}
        # the monitor's onLate reports get their OWN per-job tail: they are
        # tracked in _completion_tasks (so shutdown drains them) and ordered
        # behind _completion_tail, but must never BECOME it: a slow reporter
        # sitting in the completion tail would delay the report and retry
        # arming of a real run finishing behind it. See _queue_sla_report.
        self._sla_report_tail: Dict[str, asyncio.Task] = {}
        # whether the in-memory history has been warmed from the durable ledger
        # yet; rehydration runs once, on the first successful backend start.
        self._state_rehydrated = False
        # how many finished runs to retain per job in the durable ledger; set
        # from state.maxRunsPerJob when the backend starts. <= 0 disables.
        self._state_max_runs = 0
        # whether missed-run catch-up has fully resolved; evaluation starts on
        # the first start-up pass but is NOT latched while it cannot actually
        # run yet (the state backend failed to start and is being retried, or
        # the cluster has no positive owner) -- latching there would forfeit
        # the owed backfill forever. See _catch_up.
        self._caught_up = False
        # job names whose catch-up decision is final (backfill scheduled,
        # nothing owed, or positively delegated to another node's owner), so
        # an unresolved job elsewhere does not re-process them next pass.
        self._catchup_done: Set[str] = set()
        # loop-clock gate for re-evaluating unresolved catch-up (see
        # CATCHUP_RECHECK_INTERVAL); 0.0 means "evaluate on the next pass".
        self._catchup_next_retry = 0.0
        # the instant of the FIRST catch-up evaluation: deferred retries
        # (backend down at boot) must count missed slots against this, not a
        # later "now" -- the live scheduler ran (statelessly) in between, so
        # a later window would replay runs that actually happened.
        self._catchup_reference: Optional[datetime.datetime] = None
        # the in-flight catch-up evaluation, when one is running.  The
        # evaluation awaits bounded store reads (up to STATE_OP_TIMEOUT
        # each), so it runs as a background task rather than inline on the
        # scheduler pass: a slow-but-alive mount must degrade catch-up, not
        # delay job launches.
        self._catchup_eval_task: Optional[asyncio.Task] = None
        # whether the loaded config HAS a state section, tracked separately
        # from state_backend so catch-up can tell "no durability configured"
        # (latch and warn) from "configured but not started yet" (retry).
        self._state_configured = False
        # effective state.onStoreUnavailable policy while a state section is
        # configured: "degrade" (default: gates fail open, writes drop with
        # a warning) or "fail-closed" (durable-truth gates prefer not
        # running). Reset to "degrade" when the section is removed.
        self._state_on_unavailable = "degrade"
        # effective state.gcGraceSeconds; <= 0 disables automatic GC.
        self._state_gc_grace = 0.0
        # host tag for the host-scoped durable streams (counter snapshots,
        # @reboot boot markers): stable across restarts, unlike the state
        # backend's per-process instance id, so a restarted daemon can
        # reclaim its own records.
        self._state_host = socket.gethostname() or "localhost"
        # loop-clock instant before which the next durable counter snapshot
        # is skipped (see COUNTER_SNAPSHOT_INTERVAL).
        self._counter_snapshot_next = 0.0
        # whether this PROCESS already seeded the Prometheus accumulators
        # from a durable snapshot. Never reset (unlike _state_rehydrated):
        # seeding ADDS into live counters, so a second seed -- e.g. after a
        # state.path change swapped stores -- would double-count.
        self._counters_seeded = False
        # loop-clock instants the next manifest write / GC pass are due.
        self._manifest_next = 0.0
        self._gc_next = 0.0
        # the in-flight GC pass, if any (single-flight; a slow store must
        # not stack passes).
        self._gc_task: Optional[asyncio.Task] = None
        # the newest in-flight retry-ladder write per job, so ladder records
        # can be ORDERED (a settle chained after its pending) -- two
        # unordered fire-and-forget appends could land newest-first
        # inverted and resurrect a consumed retry on the next boot.
        self._retry_write_tail: Dict[str, asyncio.Task] = {}
        # the newest in-flight pause-stream write per job: pause records are
        # newest-record-wins, so a pause and the resume racing it must land
        # in event order (the _retry_write_tail rationale). Also consulted
        # by the housekeeping refresh, so a store re-read cannot clobber a
        # local change whose write has not landed yet.
        self._pause_write_tail: Dict[str, asyncio.Task] = {}
        # the in-flight housekeeping refresh of the paused/ streams, if any
        # (single-flight; a slow store must not stack refresh passes).
        self._pause_refresh_task: Optional[asyncio.Task] = None
        # same ordering guard for the in-flight run stream: the open and its
        # paired close are separate fire-and-forget appends whose filename
        # sort key is the wall clock read on each write's own worker thread,
        # so for a near-instant run (e.g. a start_failed job) the close
        # could sort BEFORE the open and leave "open" newest for a finished
        # run -- which the next restart would reconcile as a spurious
        # interrupted run. Chaining each job's inflight writes keeps the
        # close after the open.
        self._inflight_write_tail: Dict[str, asyncio.Task] = {}
        # latched when a @reboot boot-marker store op times out during the
        # startup pass: the remaining @reboot jobs then apply the policy
        # without more I/O instead of serially stalling the first
        # scheduling pass ~20s per job on a hung mount.
        self._reboot_gate_sick = False
        # in-flight catch-up launch tasks (each may sleep its per-job jitter
        # before launching), tracked so they are not GC'd and can be cancelled
        # on shutdown.
        self._catchup_tasks: Set[asyncio.Task] = set()
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
        # @reboot jobs whose boot run a pause DEFERRED (never forfeited): the
        # boot marker is left unwritten, so the run is still owed after a
        # daemon restart inside the same OS boot, and _process_paused_reboots
        # fires it once the pause lifts.
        self._paused_reboot_jobs = set()  # type: Set[str]
        # A per-PROCESS token stamped into in-flight run records (with the
        # host and pid), so reconciliation can tell "a previous daemon on
        # this host wrote this" from "this very process wrote it" -- the
        # state backend's own instance id will not do, because a state-
        # section reload rebuilds the backend (new id) while runs from this
        # process are still live.
        self._proc_token = os.urandom(6).hex()
        # cluster-wide concurrency slots (concurrencyScope: cluster): the
        # TTL lease each slot-gated job holds while it runs here, the renew
        # task keeping it alive, a per-job asyncio.Lock serializing
        # claim/release (an unserialized release racing the next claim
        # could revoke the fresh claim's lease -- same-holder re-acquire
        # keeps the fence, so a stale release still matches), and the
        # single-flight Replace pursuit tasks waiting out a foreign holder.
        self._slot_leases: Dict[str, Lease] = {}
        self._slot_renewers: Dict[str, asyncio.Task] = {}
        self._slot_locks: Dict[str, asyncio.Lock] = {}
        self._slot_pursuits: Dict[str, asyncio.Task] = {}
        # count of live users of each job's slot: every successful claim
        # (one per launched instance) increments, every finished instance
        # of a cluster-scoped job decrements; the lease is released only at
        # zero. A plain running_jobs emptiness check would race the window
        # between a claim succeeding and its RunningJob being registered
        # (the subprocess spawn awaits in between).
        self._slot_refs: Dict[str, int] = {}
        # effective state.slotTtlSeconds while a state section is configured
        self._slot_ttl = 30.0
        # lock-fidelity latch for the slot gate: None until probed, then
        # either "" (locks behave) or the human reason they cannot be
        # trusted (treated per onStoreUnavailable at each claim). Reset
        # whenever the backend is rebuilt.
        self._slot_fidelity: Optional[str] = None
        # the in-flight cross-node retry claim scan, if any (single-flight,
        # spawned from the housekeeping pass; see _retry_claim_scan).
        self._retry_claim_task: Optional[asyncio.Task] = None
        # the loopback job-state API (cronstable.jobapi.JobStateAPI),
        # built when a `state` section with jobApi enabled starts and torn
        # down when the backend is (its per-run tokens and staged secrets go
        # with it). None keeps the classic behaviour: no endpoint, no injected
        # CRONSTABLE_STATE_* env, jobs unaware of the store.
        self._job_api: Optional["JobStateAPI"] = None
        # On-disk fingerprint of the job API's TLS files (cert/key) as the
        # RUNNING listener loaded them, or None for a plaintext endpoint. The
        # SSLContext is built once inside JobStateAPI.start() and never
        # reloaded, so an in-place rotation (same paths, new bytes) is
        # otherwise invisible and the endpoint keeps serving the old
        # certificate until it expires. The web-app analogue is
        # _web_tls_signature; only meaningful while _job_api is not None, which
        # is why _stop_job_api clears it. See _job_api_tls_files_changed.
        self._job_api_tls_signature = None  # type: Optional[Dict[str, Any]]
        # the durable DAG orchestrator (cronstable.dagrun.DagScheduler);
        # inert until a `dags:` section and a state backend are configured. It
        # holds a back-reference to this Cron and reuses its state/lease/launch
        # seams. Constructed here (cheaply) so every code path has it.
        self._dag = DagScheduler(self)

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
            config: Optional[CronstableConfig] = None
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
                    # the gossip observability overlay (lease clusters that opt
                    # into cluster.observability); after start_stop_cluster so
                    # the election backend exists first. No-op otherwise.
                    await self.start_stop_observability(config.cluster_config)
                    await self.start_stop_state(config.state_config)
                    # periodic durable-state chores (manifest, GC): cheap
                    # due-checks that spawn tracked background tasks.
                    self._state_periodic()
                except ConfigError as err:
                    logger.error(
                        "Error in configuration file(s), so not updating "
                        "any of the config.:\n%s",
                        str(err),
                    )
                except Exception:  # pragma: nocover
                    logger.exception("please report this as a bug (1)")
                self._pause_and_sla_periodic()
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
                        await self.start_stop_web_app(
                            config.web_config, config.mcp_config
                        )
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
            # settle=None: a graceful stop must NOT settle the durable
            # ladder records -- surviving the restart (re-arming from the
            # persisted "pending" on the next boot) is the entire point of
            # restart-durable retries.
            cancel_all = [
                self.cancel_job_retries(name, settle=None)
                for name in self.retry_state
            ]
            await asyncio.gather(*cancel_all)
        # Stop the launch-adjacent background work before the drain: a
        # Replace pursuit could otherwise LAUNCH a job mid-shutdown, and
        # the retry claim scan could arm a ladder nobody will run.
        self._cancel_coordination_tasks()
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
        # the observability overlay holds no leadership, so its teardown order
        # relative to the drain does not matter; stop it here alongside the
        # election manager so its gossip listener/poll loop is released too.
        if self.observability_mesh is not None:
            logger.info("Stopping cluster observability overlay")
            await self.observability_mesh.stop()
            self.observability_mesh = None
        await self._wait_for_running_jobs_task
        # the reaper spawned each finished run's report+retry-arm sequence as
        # its own task; drain those too (unbounded, exactly as the old inline
        # awaits were) so failure/success reports still go out on a graceful
        # stop.
        await self._drain_completions()
        # the drain released every slot (each finish cancels its renewer);
        # belt-and-braces for renewers whose release write raced teardown.
        for task in list(self._slot_renewers.values()):
            task.cancel()
        self._slot_renewers.clear()

        # cancel any pending catch-up backfills (they also self-abort on the
        # stop event, set above, but cancelling is prompt and tidy at exit).
        for task in list(self._catchup_tasks):
            task.cancel()

        if self.state_backend is not None:
            # one last counter snapshot (unthrottled), so restart-durable
            # counters lose at most the throttle window's worth of events;
            # it joins the pending writes and is flushed (bounded) below.
            self._track_state_write(self._persist_counter_snapshot())
            # flush the in-flight durable run-record writes so the last few
            # runs are not lost on a clean shutdown; bounded so a stuck store
            # cannot hang the exit (its writes are simply abandoned).
            if self._pending_state_writes:
                logger.info(
                    "Flushing %d pending state write(s)",
                    len(self._pending_state_writes),
                )
                await asyncio.wait(set(self._pending_state_writes), timeout=5)
            # release every held DAG advance lease (and stop its
            # renewers) while the backend is still up, so a peer can adopt the
            # runs at once rather than waiting out a whole lease TTL. The runs'
            # tasks drained above; their completions flushed here.
            await self._dag.shutdown()
            # stop the loopback job API while the backend is still alive, so it
            # can release every held job lock (rather than leaving the fleet's
            # locks pinned for a whole TTL after a clean shutdown).
            await self._stop_job_api()
            logger.info("Stopping state backend")
            await self.state_backend.stop()
            self.state_backend = None

        await self._node_sampler.stop_history()
        if self.web_runner is not None:
            logger.info("Stopping http server")
            await self.web_runner.cleanup()

    def _cancel_coordination_tasks(self) -> None:
        """Cancel the Replace pursuits, retry claim scan and pause refresh.

        All three are scoped to the current state backend; the pause refresh
        is cancelled rather than awaited because it mutates the pause gate
        map, which nothing left in the shutdown path reads.
        """
        for task in list(self._slot_pursuits.values()):
            task.cancel()
        self._slot_pursuits.clear()
        if self._retry_claim_task is not None:
            self._retry_claim_task.cancel()
            self._retry_claim_task = None
        if self._pause_refresh_task is not None:
            self._pause_refresh_task.cancel()
            self._pause_refresh_task = None

    def signal_shutdown(self) -> None:
        logger.debug("Signalling shutdown")
        self._stop_event.set()
        # Wake the job reaper if it is parked on the idle wait below, so it
        # re-evaluates the loop condition and exits promptly instead of after
        # its next poll. Harmless when a job is running (the reaper clears this
        # each busy iteration); the only other setter is a job launch.
        self._jobs_running.set()

    @staticmethod
    def _empty_config() -> CronstableConfig:
        """The config used when no config source is set (config_arg is None).

        Empty job set, no web/cluster/logging, so applying it is a no-op that
        leaves any test-injected cron_jobs (config_yaml) untouched. Kept as a
        factory rather than a constant because JobDefaults({}) is mutable.
        """
        return CronstableConfig(
            jobs=[],
            web_config=None,
            job_defaults=JobDefaults({}),
            logging_config=None,
        )

    def _config_signature(self, files: FrozenSet[str]) -> tuple:
        """A cheap stat fingerprint of the files a parse read.

        ``(abspath, st_mtime_ns, st_size)`` per file, sorted for determinism; a
        file that has vanished collapses to a sentinel so a deletion still
        registers as a change. When the config source is a DIRECTORY its own
        mtime is folded in as well, so a brand-new entry dropped into the dir
        (which touches none of the already-tracked files) is still noticed. All
        of this is a handful of ``os.stat`` calls -- microseconds -- versus a
        full strictyaml reparse, which is the whole point.
        """
        parts: List[tuple] = []
        for f in sorted(files):
            try:
                st = os.stat(f)
                parts.append((f, st.st_mtime_ns, st.st_size))
            except OSError:
                parts.append((f, None, None))
        if self.config_arg is not None and os.path.isdir(self.config_arg):
            try:
                parts.append(("\0dir", os.stat(self.config_arg).st_mtime_ns))
            except OSError:
                parts.append(("\0dir", None))
        return tuple(parts)

    def _record_config(
        self, config: CronstableConfig, sources: FrozenSet[str]
    ) -> None:
        """Cache a successful parse for the unchanged-config skip.

        Fingerprints ``sources`` immediately after the parse, so the next
        housekeeping pass compares against the on-disk state we actually
        parsed. (A file edited in the microseconds between the parse's read and
        this stat would be recorded as already-current and picked up only on a
        later change -- an acceptable, vanishingly narrow window for a
        once-a-minute reload.)
        """
        self._config_sources = sources
        self._config_sig = self._config_signature(sources)
        self._last_config = config

    def update_config(self) -> CronstableConfig:
        """Reload the config from disk and apply it, synchronously.

        Used at construction (where there is no running event loop to offload
        to) and by tests. The run loop instead calls :meth:`reload_config`,
        which does the same work but runs the disk read + reparse off the event
        loop; both paths share the pure parse
        (:func:`parse_config_with_sources`) and :meth:`_apply_reload`. Always
        parses (no unchanged-config skip): it runs once at construction to
        establish the baseline the skip later compares against.
        """
        if self.config_arg is None:
            return self._empty_config()
        try:
            config, sources = parse_config_with_sources(self.config_arg)
        except ConfigError:
            # feeds cronstable_config_last_reload_successful, the standard
            # "config broken on disk" alert signal.
            self.metrics.config_parse(False)
            raise
        result = self._apply_reload(config)
        self._record_config(config, sources)
        return result

    async def reload_config(self) -> CronstableConfig:
        """Like :meth:`update_config`, but runs the disk read + full reparse
        OFF the event loop, in a worker thread -- and skips it entirely when
        nothing the last parse read has changed on disk.

        The reparse is a synchronous file read plus a full strictyaml parse;
        run inline on the scheduling tick it froze the entire event loop -- web
        API, cluster gossip, job output pumping -- for its whole duration, once
        a minute. First we compare a cheap stat fingerprint
        (:meth:`_config_signature`) of the files the last successful parse read
        against the stored one; if they match, the config on disk is unchanged
        and we return the already-loaded config without touching strictyaml or
        the thread pool. The downstream cluster/web/logging (re)starts in
        :meth:`run` are idempotent, so handing them the same config object is a
        no-op. Only a real change offloads the parse to a worker thread;
        applying the result (which mutates shared scheduler state) stays on the
        loop thread via :meth:`_apply_reload`, so there is no cross-thread
        access to ``self``. The caller applies this BEFORE servicing due slots,
        so the cluster gate is always current for the tick.
        """
        if self.config_arg is None:
            return self._empty_config()
        if self._last_config is not None and (
            self._config_signature(self._config_sources) == self._config_sig
        ):
            logger.debug("config unchanged on disk; skipping reparse")
            return self._last_config
        loop = asyncio.get_running_loop()
        try:
            config, sources = await loop.run_in_executor(
                None, parse_config_with_sources, self.config_arg
            )
        except ConfigError:
            # feeds cronstable_config_last_reload_successful, the standard
            # "config broken on disk" alert signal. The parse ran in the worker
            # thread (parse_config_with_sources does not touch metrics), so
            # record the failure here, back on the loop thread.
            self.metrics.config_parse(False)
            raise
        result = self._apply_reload(config)
        self._record_config(config, sources)
        return result

    def _apply_reload(self, config: CronstableConfig) -> CronstableConfig:
        """Swap in a freshly parsed config's job set (event-loop thread only).

        Records the successful reload, installs the new jobs and prunes the
        per-job maps of jobs the reload removed. Kept separate from the parse
        itself so the parse can run in a worker thread (see :meth:`run`) while
        this mutation of shared scheduler state stays on the loop thread.
        """
        self.metrics.config_parse(True)
        old_jobs = self.cron_jobs
        self.cron_jobs = OrderedDict((job.name, job) for job in config.jobs)
        # swap in the reloaded DAG set (the DagScheduler reads this
        # live each pass, so a reload that adds/removes/edits a DAG is picked
        # up on the next service tick; in-flight runs of a removed DAG finish
        # and are GC'd).
        self.cron_dags = OrderedDict((d.name, d) for d in config.dags)
        # The job set changed: drop the memoized fingerprint so the next
        # job_set_id() recomputes it once. A failed parse raises before this
        # point, so a bad reload never stales the cache.
        self._job_set_id_cache = None
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
        # The trends cache is busted per job by _record_run, but a job the
        # reload REMOVED (or a classic-crontab name reminted when a line
        # shifts) never runs again under that name, so its entry would
        # orphan forever; prune it here with every other per-job map.
        self._trends_cache = {
            name: entry
            for name, entry in self._trends_cache.items()
            if name in keep
        }
        # Pause state survives a job-config edit (deliberately no digest
        # check, unlike retries: the operator paused the NAME, not one
        # definition of it); only a job the reload removed is pruned.
        self._paused = {
            name: info for name, info in self._paused.items() if name in keep
        }
        self._pause_gen = {
            name: gen for name, gen in self._pause_gen.items() if name in keep
        }
        self._pause_pending_writes = {
            name: rec
            for name, rec in self._pause_pending_writes.items()
            if name in keep
        }
        self._last_real_outcome = {
            name: outcome
            for name, outcome in self._last_real_outcome.items()
            if name in keep
        }
        self._last_completed_at = {
            name: at
            for name, at in self._last_completed_at.items()
            if name in keep
        }
        # The SLA trackers survive a job edit the same way (the thresholds
        # may have changed, but the job's history did not); only removed
        # jobs are pruned. The (name, check) latch prunes on its name half;
        # a check dropped from a surviving job's sla block is cleared by
        # the next _sla_periodic pass instead.
        self._sla_last_success = {
            name: at
            for name, at in self._sla_last_success.items()
            if name in keep
        }
        self._sla_due = {
            name: at for name, at in self._sla_due.items() if name in keep
        }
        self._sla_last_start = {
            name: at
            for name, at in self._sla_last_start.items()
            if name in keep
        }
        self._sla_state = {
            key: since
            for key, since in self._sla_state.items()
            if key[0] in keep
        }
        self._sla_pause_windows = {
            name: spans
            for name, spans in self._sla_pause_windows.items()
            if name in keep
        }
        self._sla_disabled_since = {
            name: at
            for name, at in self._sla_disabled_since.items()
            if name in keep
        }
        # first-seen is the one tracker this pass also SEEDS: a job the reload
        # just added gets its own baseline here, so it ages into
        # maxTimeSinceSuccess from now rather than from a process start that
        # may be days old.
        self._sla_first_seen = {
            name: at
            for name, at in self._sla_first_seen.items()
            if name in keep
        }
        seen_at = get_now(datetime.timezone.utc)
        for name in self.cron_jobs:
            self._sla_first_seen.setdefault(name, seen_at)
        # Prune the finished-run display data the same way. A removed job's
        # last_run/run_history is unreachable (jobs_payload and the /runs
        # endpoints iterate cron_jobs only), so keeping it is pure leaked
        # memory -- worst under classic crontabs, whose <file>:<line> job
        # names are reminted by every line added or removed above them.
        self.last_run = {
            name: info for name, info in self.last_run.items() if name in keep
        }
        for name in [n for n in self.run_history if n not in keep]:
            del self.run_history[name]
        # Bring the next-fire index in step with the new job set: drop removed
        # / now-unscheduled jobs, reseed jobs whose schedule changed, and keep
        # the existing next-fire for jobs whose schedule is unchanged (a reseed
        # would recompute a STRICTLY-future fire and could skip a fire
        # that coincides with this reload's minute boundary).
        self._refresh_schedule(get_now(datetime.timezone.utc), old_jobs)
        return config

    def job_set_id(self) -> str:
        """Order-independent fingerprint of the currently-loaded job set.

        Two cronstable instances return the same value iff they hold the same
        set
        of jobs (same effective config, any order); see cronstable.fingerprint.

        Memoized: the fingerprint is a pure function of the loaded job set, so
        it is computed once per reload and cached (invalidated wherever
        cron_jobs is reassigned), keeping the per-job deepcopy/JSON/SHA-256
        work off the scrape / gossip / lease-renew paths that query it each
        cycle.
        """
        cached = self._job_set_id_cache
        if cached is None:
            cached = job_set_id(self.cron_jobs.values())
            self._job_set_id_cache = cached
        return cached

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
            text=cronstable.version.version,
            headers=self.web_config.get("headers", None),
        )

    async def _web_job_set_id(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        job_set = self.job_set_id()
        headers = self.web_config.get("headers", None)
        if request.headers.get("Accept") == "application/json":
            return _json_response(
                {"job_set_id": job_set, "jobs": len(self.cron_jobs)},
                headers=headers,
            )
        return web.Response(text=job_set, headers=headers)

    def cluster_payload(self) -> Dict[str, Any]:
        """This node's cluster/leadership view.

        Behind ``GET /cluster`` and MCP ``cron_get_cluster``.
        ``enabled: false`` when no cluster section is configured.
        """
        if self.cluster_manager is None:
            return {"enabled": False, "peers": []}
        payload = dict(self.cluster_manager.view_dict())
        payload["enabled"] = True
        # lease backends (kubernetes/etcd/filesystem) have no fleet view of
        # their own, but the observability overlay mesh serves one when
        # installed (see _fleet_backend) -- tell the dashboard whether its
        # fleet view has data behind it. The gossip payload stays unchanged:
        # its UI path always shows the fleet view.
        if payload.get("backend") != "gossip":
            payload["fleet"] = self.observability_mesh is not None
        # this node's own live CPU/memory, sampled fresh: always shown in the
        # cluster panel (it is local and free), independent of whether peers
        # share theirs. Peer load rides view_dict's per-peer node_stats (only
        # populated when the cluster shares node stats -- observability).
        payload["node_stats"] = self.node_resource_snapshot()
        return payload

    async def _web_get_cluster(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        return _json_response(
            self.cluster_payload(),
            headers=self.web_config.get("headers", None),
        )

    async def _web_get_fleet(self, request: web.Request) -> web.Response:
        """The cluster-wide per-job run view (the dashboard's fleet view).

        Merged entirely from state this node already holds: its own scheduler
        snapshot plus the per-job summaries each peer piggybacked on the
        gossip exchanges this node has already made (see
        :meth:`cronstable.cluster.ClusterManager.fleet_view`) -- serving this
        endpoint triggers no peer traffic.  ``enabled: false`` when there is
        no cluster, or the backend has no node-to-node channel to have
        carried summaries (a lease backend without the observability
        overlay); the dashboard then hides its fleet view.
        """
        assert self.web_config is not None
        return _json_response(
            self.fleet_payload(), headers=self.web_config.get("headers", None)
        )

    def fleet_payload(self) -> Dict[str, Any]:
        """The cluster-wide per-job run view.

        Behind ``GET /fleet`` and MCP ``cron_get_fleet``.
        ``enabled: false`` when there is no cluster, or the backend has no
        node-to-node channel to have carried summaries.
        """
        # the overlay mesh when a lease cluster opted into observability, else
        # the leadership backend (gossip provides the view; lease backends
        # return None -> feature unavailable).
        mgr = self._fleet_backend()
        fleet = mgr.fleet_view() if mgr is not None else None
        if fleet is None:
            return {"enabled": False, "nodes": []}
        return fleet

    async def _web_get_node(self, request: web.Request) -> web.Response:
        """This node's live CPU/memory (the dashboard's node readout).

        Whole-host CPU% and memory plus this daemon's own footprint, sampled
        fresh per request from
        :class:`cronstable.resources.NodeResourceSampler`.
        ``resources`` is ``null`` when sampling is unavailable (psutil could
        not read the host); the dashboard then hides the node meter.
        """
        assert self.web_config is not None
        return _json_response(
            self.node_payload(), headers=self.web_config.get("headers", None)
        )

    def node_payload(self, history: bool = False) -> Dict[str, Any]:
        """This node's live CPU/memory (`GET /node`, MCP `cron_get_node`).

        ``resources`` is ``None`` when sampling is unavailable.  With
        ``history=True`` a ``history`` block (the retained ring the dashboard
        chart uses) rides along.
        """
        # the cluster node name when clustered, else the plain hostname the
        # durable-state layer already uses as this node's identity.
        mgr = self.cluster_manager
        node_name = (
            mgr.node_name
            if mgr is not None and getattr(mgr, "node_name", None)
            else self._state_host
        )
        payload: Dict[str, Any] = {
            "node_name": node_name,
            "resources": self._node_sampler.snapshot(),
        }
        if history:
            hist = self._node_sampler.history()
            payload["history"] = {
                "enabled": hist is not None,
                "interval": hist["interval"] if hist is not None else None,
                "points": hist["points"] if hist is not None else [],
            }
        return payload

    async def _web_node_history(self, request: web.Request) -> web.Response:
        """The node's retained CPU/memory history (the dashboard node chart).

        Oldest-first ``[t, cpu%, mem%]`` points from the background sampler
        (see ``web.nodeHistory``), fetched lazily when the chart is opened
        rather than riding the /node poll.  ``enabled`` is false -- with no
        points -- when the sampler is off (``nodeHistory: false``) or psutil
        is unavailable, so the dashboard hides the chart instead of showing
        an eternally-empty one.
        """
        assert self.web_config is not None
        mgr = self.cluster_manager
        node_name = (
            mgr.node_name
            if mgr is not None and getattr(mgr, "node_name", None)
            else self._state_host
        )
        history = self._node_sampler.history()
        return _json_response(
            {
                "node_name": node_name,
                "enabled": history is not None,
                "interval": (
                    history["interval"] if history is not None else None
                ),
                "points": history["points"] if history is not None else [],
            },
            headers=self.web_config.get("headers", None),
        )

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

    def status_payload(self) -> List[Dict[str, Any]]:
        """Per-job status rows (running / disabled / scheduled).

        The data behind ``GET /status`` and the MCP ``cron_get_status`` tool.
        """
        # the old cron library's untyped next() left this list's value type
        # as Any; the in-house engine types it Optional[float], so spell out
        # the mixed-shape rows explicitly.
        out: List[Dict[str, Any]] = []
        for name, job in self.cron_jobs.items():
            running = self.running_jobs.get(name, None)
            if running:
                row = {
                    "job": name,
                    "status": "running",
                    "pid": [
                        runjob.proc.pid
                        for runjob in running
                        if runjob.proc is not None
                    ],
                }  # type: Dict[str, Any]
                if self._schedule_never_fires(name, job):
                    # a running job with a dead schedule still never fires
                    # again; flag it here exactly as /jobs does, so the two
                    # surfaces agree (the text renderer keeps saying
                    # "running": only the JSON row gains the field).
                    row["never_fires"] = True
                out.append(row)
            elif not job.enabled:
                # disabled jobs never run on schedule; report that honestly
                # instead of an inapplicable "scheduled (in N seconds)".
                out.append({"job": name, "status": "disabled"})
            else:
                crontab = job.schedule  # type: Union[CronTab, str]
                scheduled_in = (
                    self._scheduled_in(name, job, False)
                    if isinstance(crontab, CronTab)
                    else str(crontab)
                )
                row = {
                    "job": name,
                    "status": "scheduled",
                    "scheduled_in": scheduled_in,
                }
                if isinstance(crontab, CronTab) and scheduled_in is None:
                    # "scheduled" with no instant means NEVER; say so
                    # instead of leaving a null for consumers to guess at
                    row["never_fires"] = True
                out.append(row)
        return out

    async def _web_get_status(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        out = self.status_payload()
        if request.headers.get("Accept") == "application/json":
            return _json_response(
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
                elif jobstat.get("never_fires"):
                    status = "never fires (schedule has no future occurrence)"
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

    @staticmethod
    def _zone_from_name(tz_name: Optional[str]) -> datetime.tzinfo:
        """A ``?tz=`` query value as a tzinfo (UTC when absent).

        Raises ValueError for an unknown name; the handlers turn that
        into a 400.
        """
        if not tz_name:
            return datetime.timezone.utc
        try:
            return ZoneInfo(tz_name)
        except (ZoneInfoNotFoundError, ValueError, KeyError, OSError):
            # ZoneInfo maps its key onto a filesystem path, so an over-long
            # (ENAMETOOLONG) or otherwise unopenable key surfaces as OSError
            # rather than ZoneInfoNotFoundError -- still just an unknown name.
            raise ValueError("unknown timezone: {}".format(tz_name)) from None

    @staticmethod
    def _timezone_error(tz_name: Optional[str]) -> Optional[str]:
        """The 400 message for a ``?tz=`` value that will not resolve, or
        ``None`` when it does.

        Built from the requested name, never from the caught exception
        (whose text can carry a server-side ZoneInfo filesystem path): a
        handler validates here so a schedule endpoint answers 400 without
        any exception string reaching the response body.  Mirrors
        :meth:`_zone_from_name`'s own resolve/except so the two agree on
        which names are unknown.
        """
        if not tz_name:
            return None
        try:
            ZoneInfo(tz_name)
        except (ZoneInfoNotFoundError, ValueError, KeyError, OSError):
            return "unknown timezone: {}".format(tz_name)
        return None

    @staticmethod
    def _period_error(period: str) -> Optional[str]:
        """The 400 message for an unsupported ``?period=`` value, or
        ``None`` when it is valid.

        Mirrors :func:`suggest_slot`'s own check so ``/schedule/suggest``
        can answer 400 from the request value without stringifying the
        builder's exception.
        """
        if period not in ("hourly", "daily"):
            return "period must be 'hourly' or 'daily', got {!r}".format(
                period
            )
        return None

    @staticmethod
    def _invalid_timestamp_message(text: str) -> str:
        """The 400 message for an unparseable ``?at=`` value.

        Built from the requested value (its :func:`repr` plus the accepted
        forms), so ``/schedule/why`` can answer 400 from the request string
        without stringifying the parse exception.  Shared with
        :meth:`_parse_probe_timestamp` so the raised and the handler-built
        text never diverge.
        """
        return (
            "invalid timestamp {!r}: pass ISO 8601, e.g. "
            "2026-07-14T09:00, 2026-07-14T09:00:00+02:00 or "
            "2026-07-14T09:00:00Z".format((text or "").strip())
        )

    @staticmethod
    def _parse_probe_timestamp(text: str) -> datetime.datetime:
        """An ``at=`` timestamp as a datetime (aware when it has an offset).

        Accepts ISO 8601 as :func:`datetime.datetime.fromisoformat` does,
        plus the ubiquitous trailing ``Z`` (which the py310 parser still
        rejects).  Raises ValueError, with the accepted forms in the
        message, for anything else; the handlers turn that into a 400.
        """
        raw = (text or "").strip()
        iso = raw
        if iso.endswith(("Z", "z")):
            iso = iso[:-1] + "+00:00"
        try:
            return datetime.datetime.fromisoformat(iso)
        except ValueError:
            raise ValueError(Cron._invalid_timestamp_message(raw)) from None

    def schedule_preview_payload(
        self,
        expr: str,
        tz_name: Optional[str] = None,
        count: int = 12,
        seed: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Parse, describe, preview and lint one schedule expression.

        The data behind ``GET /schedule/preview``: the single source of
        truth for sandboxes, computed by the same engine, describer and
        linter the scheduler itself runs, so a preview can never disagree
        with what the daemon will do (the web page's client-side preview is
        a convenience, not an authority).  Raises ValueError for an unknown
        timezone name; the handler turns that into a 400.  ``seed`` is the
        hash key for the ``H`` form (a job name, real or prospective);
        without it an ``H`` expression comes back invalid, with the
        engine's own error saying a hash key is needed.
        """
        zone = self._zone_from_name(tz_name)
        text = (expr or "").strip()
        payload = {
            "expression": text,
            "timezone": str(zone),
        }  # type: Dict[str, Any]
        if seed is not None:
            payload["seed"] = seed
        if text.lower() == "@reboot":
            payload.update(
                {
                    "valid": True,
                    "reboot": True,
                    "description": describe_cron(text),
                    "fires": [],
                    "never_fires": False,
                    "lint": [],
                }
            )
            return payload
        try:
            tab = CronTab(text, hash_key=seed)
        except (ValueError, KeyError) as err:
            payload.update({"valid": False, "error": str(err)})
            return payload
        fires = next_fires(text, count, tz=zone, hash_key=seed)
        payload.update(
            {
                "valid": True,
                "reboot": False,
                "normalized": str(tab),
                "description": describe_cron(text, hash_key=seed),
                "fires": [when.isoformat() for when in fires],
                "never_fires": not fires,
                "lint": [
                    finding._asdict()
                    for finding in lint_schedule(
                        text, timezone=zone, hash_key=seed
                    )
                ],
            }
        )
        if tab.resolved_source != str(tab):
            payload["resolved"] = tab.resolved_source
        return payload

    async def _web_schedule_preview(
        self, request: web.Request
    ) -> web.Response:
        """Decode an arbitrary expression for the dashboards' sandboxes."""
        assert self.web_config is not None
        headers = self.web_config.get("headers", None)
        expr = request.query.get("expr", "")
        if not expr.strip():
            return _json_response(
                {"error": "missing ?expr= query parameter"},
                status=400,
                headers=headers,
            )
        tz = request.query.get("tz") or None
        # Validate the one user input that would raise (the timezone), and
        # answer 400 from the requested name.  The builder's own parse errors
        # for the expression come back as payload data (valid=False), so no
        # exception text ever reaches the response body.
        tz_error = self._timezone_error(tz)
        if tz_error is not None:
            return _json_response(
                {"error": tz_error}, status=400, headers=headers
            )
        count = self._web_int_query(request, "count", default=12, lo=1, hi=60)
        payload = self.schedule_preview_payload(
            expr, tz, count, request.query.get("seed")
        )
        return _json_response(payload, headers=headers)

    def _job_or_dag_schedule(self, name: str) -> Optional[JobConfig]:
        """The named job, or a DAG's synthetic ``dag:<name>`` schedule job.

        DAG schedule jobs launch real work on real instants, so "why did
        this not run?" is as answerable for them as for any job; they are
        just not in ``cron_jobs``.
        """
        job = self.cron_jobs.get(name)
        if job is not None:
            return job
        for dag in self.cron_dags.values():
            sched = dag.schedule_job
            if sched is not None and sched.name == name:
                return sched
        return None

    def schedule_why_payload(
        self, name: str, at: str
    ) -> Optional[Dict[str, Any]]:
        """Why a job's schedule did (not) select a timestamp
        (``GET /schedule/why``, MCP ``cron_why_no_run``).

        Decomposes the engine's own match test field by field via
        :func:`cronstable.croninfo.why_no_run`, in the job's OWN resolved
        timezone: an aware ``at`` is converted into that zone, a naive one
        reads as wall time there.  The payload adds the job-level facts an
        answer needs (``enabled``, the ``@reboot`` case) and the nearest
        real fire on each side of the probe, from the same occurrence walk
        the scheduler runs.  Returns ``None`` for an unknown job; raises
        ValueError for an unparseable timestamp.
        """
        job = self._job_or_dag_schedule(name)
        if job is None:
            return None
        # a schedule match says nothing about whether the fire would LAUNCH:
        # an active pause skips it, so the answer must carry that fact.
        pause = self._pause_active(name)
        pause_note: Optional[Dict[str, str]] = None
        if pause is not None:
            message = "job is paused until {} (by {})".format(
                pause.until.isoformat(), pause.by
            )
            if pause.note:
                message += ": " + pause.note
            pause_note = {
                "code": "paused",
                "level": "note",
                "message": message,
            }
        zone = job.timezone or datetime.datetime.now().astimezone().tzinfo
        payload: Dict[str, Any] = {
            "job": name,
            "enabled": job.enabled,
            "timezone": (
                str(job.timezone) if job.timezone is not None else "local"
            ),
            "at": at,
        }
        if not isinstance(job.schedule, CronTab):
            # "@reboot": no timetable exists for any timestamp to match.
            payload.update(
                {
                    "expression": str(job.schedule),
                    "reboot": True,
                    "description": describe_cron(str(job.schedule)),
                    "matches": False,
                    "checks": [],
                    "failed": [],
                    "notes": [
                        {
                            "code": "reboot",
                            "level": "note",
                            "message": "@reboot runs once when the daemon "
                            "starts; it never fires on a timetable, so no "
                            "timestamp can match",
                        }
                    ],
                    "previous_fire": None,
                    "next_fire": None,
                }
            )
            if pause_note is not None:
                payload["notes"].append(pause_note)
            return payload
        tab = job.schedule
        probe = self._parse_probe_timestamp(at)
        if probe.tzinfo is not None:
            aware = probe.astimezone(zone)
            civil = aware.replace(tzinfo=None)
        else:
            civil = probe
            # fold=0, the engine's own rule for resolving a wall time.
            aware = probe.replace(tzinfo=zone)
        payload.update(
            {
                "expression": str(tab),
                "reboot": False,
                "description": describe_cron(str(tab), hash_key=job.name),
                "at_in_zone": aware.isoformat(),
            }
        )
        if tab.resolved_source != str(tab):
            payload["resolved"] = tab.resolved_source
        payload.update(why_no_run(tab, civil, timezone=zone))
        next_fire = next(iter(tab.occurrences(aware)), None)
        prev_delta = tab.prev(now=aware)
        previous_fire = None
        if prev_delta is not None:
            fire_utc = aware.astimezone(
                datetime.timezone.utc
            ) - datetime.timedelta(seconds=prev_delta)
            # fires are whole seconds; round away float/microsecond residue
            # from the elapsed-seconds reconstruction.
            if fire_utc.microsecond >= 500000:
                fire_utc += datetime.timedelta(seconds=1)
            previous_fire = (
                fire_utc.replace(microsecond=0).astimezone(zone).isoformat()
            )
        payload["previous_fire"] = previous_fire
        payload["next_fire"] = (
            next_fire.isoformat() if next_fire is not None else None
        )
        if pause_note is not None:
            payload.setdefault("notes", []).append(pause_note)
        return payload

    async def _web_schedule_why(self, request: web.Request) -> web.Response:
        """Explain one job's schedule against one timestamp."""
        assert self.web_config is not None
        headers = self.web_config.get("headers", None)
        name = request.query.get("job", "").strip()
        at = request.query.get("at", "").strip()
        if not name or not at:
            return _json_response(
                {"error": "missing ?job= or ?at= query parameter"},
                status=400,
                headers=headers,
            )
        try:
            payload = self.schedule_why_payload(name, at)
        except ValueError:
            # The only ValueError here is the unparseable ``at=`` timestamp;
            # answer 400 from the requested value, not the exception text
            # (kept inside the try so an unknown job still 404s below).
            return _json_response(
                {"error": self._invalid_timestamp_message(at)},
                status=400,
                headers=headers,
            )
        if payload is None:
            raise web.HTTPNotFound()
        return _json_response(payload, headers=headers)

    def _schedule_entries(self) -> List[ScheduleEntry]:
        """The analyzable fleet: every enabled, cron-scheduled job.

        DAG schedules ride along as their synthetic ``dag:<name>`` job
        (they launch real work on real instants, so they belong in the
        collision picture).  @reboot jobs and disabled jobs are excluded:
        neither fires on a schedule, so neither can collide.
        """
        entries = [
            ScheduleEntry(name, job.schedule, job.timezone)
            for name, job in self.cron_jobs.items()
            if job.enabled and isinstance(job.schedule, CronTab)
        ]
        for dag in self.cron_dags.values():
            sched = dag.schedule_job
            if (
                sched is not None
                and sched.enabled
                and isinstance(sched.schedule, CronTab)
            ):
                entries.append(
                    ScheduleEntry(sched.name, sched.schedule, sched.timezone)
                )
        return entries

    def schedule_pressure_payload(
        self,
        hours: int = 24,
        tz_name: Optional[str] = None,
        entries: Optional[List[ScheduleEntry]] = None,
    ) -> Dict[str, Any]:
        """The fleet collision heatmap (``GET /schedule/pressure``).

        Every enabled schedule's fires over the next ``hours``, bucketed
        into the hour-by-minute grid by :func:`schedule_pressure`, plus
        how many jobs were excluded as disabled or @reboot.  Raises
        ValueError for an unknown timezone name.  ``entries`` is an
        optional pre-built fleet snapshot (see
        :meth:`schedule_pressure_payload_async`); when None it is built
        here.
        """
        if entries is None:
            entries = self._schedule_entries()
        payload = schedule_pressure(
            entries,
            hours=hours,
            tz=self._zone_from_name(tz_name),
        )
        # a plain-list snapshot: this builder can run on an executor thread
        # (see the async wrapper), so do not iterate the live mapping from
        # off the loop.
        jobs = list(self.cron_jobs.values())
        payload["excluded"] = {
            "disabled": sum(1 for job in jobs if not job.enabled),
            "reboot": sum(
                1
                for job in jobs
                if job.enabled and not isinstance(job.schedule, CronTab)
            ),
        }
        return payload

    def schedule_duplicates_payload(
        self, entries: Optional[List[ScheduleEntry]] = None
    ) -> Dict[str, Any]:
        """Semantically identical schedules (``GET /schedule/duplicates``).

        Groups via the engine's own equality (``*/5`` == ``0-59/5``), so
        the answer can never disagree with how the scheduler itself
        compares schedules across reloads.  ``entries`` is an optional
        pre-built fleet snapshot (see
        :meth:`schedule_duplicates_payload_async`); when None it is
        built here.
        """
        if entries is None:
            entries = self._schedule_entries()
        return {
            "jobs": len(entries),
            "groups": duplicate_schedules(entries),
        }

    def schedule_suggest_payload(
        self,
        period: str = "hourly",
        tz_name: Optional[str] = None,
        entries: Optional[List[ScheduleEntry]] = None,
    ) -> Dict[str, Any]:
        """The least-loaded slot for a new job (``GET /schedule/suggest``).

        Raises ValueError for an unknown period or timezone name.
        ``entries`` is an optional pre-built fleet snapshot (see
        :meth:`schedule_suggest_payload_async`); when None it is built
        here.
        """
        if entries is None:
            entries = self._schedule_entries()
        return suggest_slot(
            entries,
            period=period,
            tz=self._zone_from_name(tz_name),
        )

    # The three payload builders above walk up to 168 hours of fire
    # instants for every schedule in the fleet: pure CPU that can take
    # seconds at fleet scale.  The wrappers below take the (cheap) entries
    # snapshot on the loop, then run the walk on the default executor over
    # that immutable snapshot, keeping the scheduler's event loop free to
    # dispatch jobs while a dashboard or MCP client asks for analysis.
    # Both the web handlers and the MCP tools go through these, so the
    # offload lives in exactly one place.  Exceptions (ValueError for an
    # unknown timezone or period) propagate to the await unchanged.

    async def schedule_pressure_payload_async(
        self, hours: int = 24, tz_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Executor-offloaded :meth:`schedule_pressure_payload`."""
        entries = self._schedule_entries()
        return await asyncio.get_running_loop().run_in_executor(
            None,
            partial(
                self.schedule_pressure_payload,
                hours=hours,
                tz_name=tz_name,
                entries=entries,
            ),
        )

    async def schedule_duplicates_payload_async(self) -> Dict[str, Any]:
        """Executor-offloaded :meth:`schedule_duplicates_payload`."""
        entries = self._schedule_entries()
        return await asyncio.get_running_loop().run_in_executor(
            None, partial(self.schedule_duplicates_payload, entries=entries)
        )

    async def schedule_suggest_payload_async(
        self, period: str = "hourly", tz_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Executor-offloaded :meth:`schedule_suggest_payload`."""
        entries = self._schedule_entries()
        return await asyncio.get_running_loop().run_in_executor(
            None,
            partial(
                self.schedule_suggest_payload,
                period=period,
                tz_name=tz_name,
                entries=entries,
            ),
        )

    async def _web_schedule_pressure(
        self, request: web.Request
    ) -> web.Response:
        assert self.web_config is not None
        headers = self.web_config.get("headers", None)
        hours = self._web_int_query(request, "hours", default=24, lo=1, hi=168)
        tz = request.query.get("tz") or None
        # Validate up front and answer 400 from the requested name, so the
        # builder's exception text never reaches the response body.
        tz_error = self._timezone_error(tz)
        if tz_error is not None:
            return _json_response(
                {"error": tz_error}, status=400, headers=headers
            )
        payload = await self.schedule_pressure_payload_async(hours, tz)
        return _json_response(payload, headers=headers)

    async def _web_schedule_duplicates(
        self, request: web.Request
    ) -> web.Response:
        assert self.web_config is not None
        return _json_response(
            await self.schedule_duplicates_payload_async(),
            headers=self.web_config.get("headers", None),
        )

    async def _web_schedule_suggest(
        self, request: web.Request
    ) -> web.Response:
        assert self.web_config is not None
        headers = self.web_config.get("headers", None)
        period = request.query.get("period") or "hourly"
        tz = request.query.get("tz") or None
        # Validate both user inputs and answer 400 from the requested values,
        # so no builder exception text reaches the response body.  Timezone
        # first: the builder resolves the zone (a call argument) before the
        # period check runs, so a bad zone wins when both are wrong.
        tz_error = self._timezone_error(tz)
        if tz_error is not None:
            return _json_response(
                {"error": tz_error}, status=400, headers=headers
            )
        period_error = self._period_error(period)
        if period_error is not None:
            return _json_response(
                {"error": period_error}, status=400, headers=headers
            )
        payload = await self.schedule_suggest_payload_async(period, tz)
        return _json_response(payload, headers=headers)

    def _avg_duration(self, name: str) -> Optional[float]:
        """Mean runtime in seconds over retained history, or ``None``.

        The dashboard's own definition (:func:`_run_stats`), so the .ics
        feed's event lengths can never disagree with the run drawer.
        """
        runs = list(self.run_history.get(name) or [])
        avg = _run_stats(runs)["avg_duration"]
        return float(avg) if avg is not None else None

    def _calendar_entries(
        self, name: Optional[str] = None
    ) -> Optional[List[CalendarEntry]]:
        """The calendar renderer's rows: the fleet, or one job when ``name``.

        ``None`` for an unknown job name; a known job with no timetable
        (``@reboot``, disabled) or a fleet of none is an empty list.  The
        single-job feed filters the same :meth:`_schedule_entries` snapshot
        the fleet feed uses, so the two can never disagree about
        eligibility.  Reads live scheduler state, so this runs on the
        event loop; the render then walks the immutable result on an
        executor (see :meth:`_web_calendar_response`).
        """
        if name is None:
            schedule_entries = sorted(
                self._schedule_entries(), key=lambda entry: entry.name
            )
        else:
            if self._job_or_dag_schedule(name) is None:
                return None
            schedule_entries = [
                entry
                for entry in self._schedule_entries()
                if entry.name == name
            ]
        return [
            CalendarEntry(
                entry.name,
                entry.tab,
                entry.timezone,
                self._avg_duration(entry.name),
            )
            for entry in schedule_entries
        ]

    def calendar_payload(
        self,
        name: Optional[str] = None,
        days: int = 14,
        per_job: int = 100,
        start: Optional[datetime.datetime] = None,
        now: Optional[datetime.datetime] = None,
        entries: Optional[List[CalendarEntry]] = None,
    ) -> Optional[str]:
        """The iCalendar feed text: the fleet, or one job when ``name``.

        Behind ``GET /calendar.ics`` and ``GET /jobs/{name}/calendar.ics``:
        one VEVENT per upcoming fire over ``[start, start+days)``, from the
        same occurrence walk the scheduler runs (see
        :mod:`cronstable.ical`).  ``None`` for an unknown job name; a known
        job with no timetable (``@reboot``) or a fleet of none renders as
        a valid, empty calendar.  ``start``/``now`` pin the window and
        DTSTAMP for tests.  ``entries`` is an optional pre-built
        :meth:`_calendar_entries` snapshot (see the async handler); when
        None it is built here.
        """
        if entries is None:
            entries = self._calendar_entries(name)
        if entries is None:
            return None
        calname = (
            "cronstable" if name is None else "cronstable: {}".format(name)
        )
        if start is None:
            start = get_now(datetime.timezone.utc)
        return render_calendar(
            entries,
            start=start,
            days=days,
            per_job_cap=per_job,
            calname=calname,
            now=now,
            prodid_version=cronstable.version.version,
        )

    async def _web_calendar_response(
        self, name: Optional[str], request: web.Request
    ) -> web.Response:
        assert self.web_config is not None
        days = self._web_int_query(request, "days", default=14, lo=1, hi=60)
        per_job = self._web_int_query(
            request, "per_job", default=100, lo=1, hi=1000
        )
        # the entries snapshot reads live state, so it is taken on the
        # loop; the walk (jobs x fires, pure CPU) then runs on the
        # executor over the immutable snapshot, like the pressure/suggest
        # builders
        entries = self._calendar_entries(name)
        if entries is None:
            raise web.HTTPNotFound()
        text = await asyncio.get_running_loop().run_in_executor(
            None,
            partial(
                self.calendar_payload, name, days, per_job, entries=entries
            ),
        )
        headers = dict(self.web_config.get("headers") or {})
        headers["Content-Disposition"] = 'inline; filename="cronstable.ics"'
        return web.Response(
            text=text,
            content_type="text/calendar",
            charset="utf-8",
            headers=headers,
        )

    async def _web_calendar(self, request: web.Request) -> web.Response:
        """The fleet-wide iCal feed (``GET /calendar.ics``)."""
        return await self._web_calendar_response(None, request)

    async def _web_job_calendar(self, request: web.Request) -> web.Response:
        """One job's iCal feed (``GET /jobs/{name}/calendar.ics``)."""
        return await self._web_calendar_response(
            request.match_info["name"], request
        )

    async def start_job_by_name(self, name: str) -> None:
        """Launch a job now (`POST /jobs/{name}/start`, MCP `cron_run_job`).

        Raises :class:`ApiActionError` for an unknown (404) or disabled (409)
        job; otherwise honours the job's concurrencyPolicy exactly as the
        scheduler would.  A PAUSED job may still be started manually: a pause
        skips scheduled fires only, and the operator asking by hand is the
        operator overriding their own pause (unlike `enabled: false`, which
        is config the API must not silently override).
        """
        try:
            job = self.cron_jobs[name]
        except KeyError as ex:
            raise ApiActionError(
                "job {!r} not found".format(name), status=404
            ) from ex
        if not job.enabled:
            # a disabled job behaves "as if it isn't there"; refuse to launch
            # it manually rather than silently overriding the config.
            raise ApiActionError(
                "job {!r} is disabled".format(name), status=409
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
        # Same for a boot run a pause deferred: the manual start IS it, so
        # record the boot marker (the deferral left it unwritten) or the
        # pause lifting would run the one-shot a second time.
        if name in self._paused_reboot_jobs:
            self._paused_reboot_jobs.discard(name)
            logger.info(
                "manual start of @reboot job %s counts as the boot run its "
                "pause deferred",
                name,
            )
            if self._state_configured:
                await self._reboot_boot_gate(job)
        await self.maybe_launch_job(job)

    async def cancel_job_by_name(self, name: str) -> int:
        """Cancel a job's running instances; return how many were signalled.

        Behind ``POST /jobs/{name}/cancel`` and MCP ``cron_cancel_job``.
        Raises :class:`ApiActionError` for an unknown (404) or not-running
        (409) job.
        """
        if name not in self.cron_jobs:
            raise ApiActionError("job {!r} not found".format(name), status=404)
        running = list(self.running_jobs.get(name) or [])
        if not running:
            # nothing to cancel: report a conflict rather than a silent no-op
            # so the caller can tell the user the job was not running.
            raise ApiActionError(
                "job {!r} is not running".format(name), status=409
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
        return len(running)

    async def pause_job_by_name(
        self,
        name: str,
        *,
        duration: Optional[int] = None,
        until: Optional[datetime.datetime] = None,
        note: str = "",
        by: str = "api",
        channel: str = "api",
    ) -> Dict[str, Any]:
        """Pause a job's scheduled fires (`POST /jobs/{name}/pause`).

        The single pause path shared by the web API, MCP, and tests.  While
        paused, due fires are skipped (each writes a synthetic "skipped"
        ledger row so the catch-up watermark keeps advancing), pending
        retries defer, and catch-up owes nothing for the window; manual
        start and cancel are unaffected, as are running instances.  Exactly
        one of ``duration`` (seconds) or ``until`` may be given; neither
        means :data:`PAUSE_DEFAULT_SECONDS`.  Idempotent: pausing a paused
        job overwrites the window.  Returns the JSON-safe pause record.
        Raises :class:`ApiActionError` for an unknown job (404) or an
        invalid window/oversized audit field (400).
        """
        if name not in self.cron_jobs:
            raise ApiActionError("job {!r} not found".format(name), status=404)
        now = get_now(datetime.timezone.utc)
        if duration is not None and until is not None:
            raise ApiActionError(
                "give durationSeconds or until, not both", status=400
            )
        if until is None:
            seconds = PAUSE_DEFAULT_SECONDS if duration is None else duration
            if not 1 <= seconds <= PAUSE_MAX_SECONDS:
                raise ApiActionError(
                    "durationSeconds must be between 1 and {}".format(
                        PAUSE_MAX_SECONDS
                    ),
                    status=400,
                )
            until = now + datetime.timedelta(seconds=seconds)
        else:
            if until.tzinfo is None:
                until = until.replace(tzinfo=datetime.timezone.utc)
            if until <= now:
                raise ApiActionError("until is in the past", status=400)
            if (until - now).total_seconds() > PAUSE_MAX_SECONDS:
                raise ApiActionError(
                    "until is more than {} seconds away".format(
                        PAUSE_MAX_SECONDS
                    ),
                    status=400,
                )
        if len(note) > PAUSE_NOTE_MAX:
            raise ApiActionError(
                "note is longer than {} characters".format(PAUSE_NOTE_MAX),
                status=400,
            )
        if len(by) > PAUSE_BY_MAX:
            raise ApiActionError(
                "by is longer than {} characters".format(PAUSE_BY_MAX),
                status=400,
            )
        info = PauseInfo(
            since=now, until=until, note=note, by=by, channel=channel
        )
        replaced = self._paused.get(name)
        if replaced is not None:
            # pausing a paused job overwrites the window: close the old one
            # out at `now` so the time it already held is still credited, and
            # so the stretch the two windows share is not credited twice.
            self._sla_bank_pause(name, replaced, now)
        self._paused[name] = info
        # the job is excused from here on: drop any breach it latched while it
        # was still being evaluated, so the late gauge, the /jobs sla block and
        # the OVERDUE chip clear on this response rather than a minute later.
        self._sla_clear_latches(name)
        self.metrics.job_pause_state(name, True)
        self._persist_pause(name, info)
        logger.info(
            "Job %s paused until %s by %s (%s)%s",
            name,
            until.isoformat(),
            by,
            channel,
            ": " + note if note else "",
        )
        return info.to_dict()

    async def resume_job_by_name(
        self, name: str, *, by: str = "api", channel: str = "api"
    ) -> None:
        """Resume a paused job (`POST /jobs/{name}/resume`).

        A no-op for a job that is not paused, EXCEPT that the durable
        "resumed" record is appended regardless when a store is configured:
        a peer's pause this node has not refreshed into memory yet must
        still be revoked, or the fleet would keep skipping a job the
        operator just resumed.  Raises :class:`ApiActionError` for an
        unknown job (404).
        """
        if name not in self.cron_jobs:
            raise ApiActionError("job {!r} not found".format(name), status=404)
        was = self._paused.pop(name, None)
        self.metrics.job_pause_state(name, False)
        self._persist_resume(name, by, channel)
        if was is not None:
            self._sla_bank_pause(name, was, get_now(datetime.timezone.utc))
            logger.info("Job %s resumed by %s (%s)", name, by, channel)

    def _pause_active(self, name: str) -> Optional[PauseInfo]:
        """The job's live pause window, or ``None``; expiry enforced HERE.

        The one pause read every consumer (fire gate, retry gate, catch-up,
        payloads) goes through: nothing in the store expires records, so a
        window whose ``until`` has passed reads as absent everywhere at
        once.  The stale entry itself is swept (and the auto-resume logged)
        by the housekeeping pass; only memory is consulted, never store
        I/O on a scheduling path.
        """
        info = self._paused.get(name)
        if info is None:
            return None
        if info.until <= get_now(datetime.timezone.utc):
            return None
        return info

    def _pause_and_sla_periodic(self) -> None:
        """Per-minute pause and SLA housekeeping, guarded on its own.

        Deliberately NOT inside run()'s reload try/except: both passes work
        off the job set already in memory and need nothing the reload
        produces, while a broken config file on disk raises out of
        :meth:`reload_config`.  Sharing that block would let an unparseable
        config silently stop the late-run monitor, going quiet about jobs
        that stopped running (the exact failure the SLA feature exists to
        report), and would strand paused jobs past their expiry in the gauge
        and the durable refresh.
        """
        try:
            # pause expiry sweep + cross-node pause propagation; the sweep is
            # stateless, the durable refresh spawns a tracked task only when a
            # backend is up.
            self._pause_periodic()
            # per-job SLA checks: purely in-memory, so they run with or
            # without a state backend (hence a sibling of _state_periodic,
            # not part of it).
            self._sla_periodic()
        except Exception:  # pragma: nocover
            logger.exception("please report this as a bug (5)")

    def _pause_periodic(self) -> None:
        """Sweep expired pauses; refresh pause state from a shared store.

        Called from the housekeeping pass (at most once per wall-clock
        minute).  The sweep is purely in-memory and needs no durable write:
        expiry is already enforced at every read (:meth:`_pause_active`),
        so dropping the entry here only reclaims memory, clears the gauge,
        and logs the auto-resume once.  The durable refresh then re-reads
        the ``paused/`` streams as a tracked background task so peers
        sharing a store pick up each other's pauses and resumes within
        about a minute (the fire path itself never reads the store).

        Records buffered by a failed append are retried first, so a store
        that only hiccuped drains the backlog instead of holding those jobs
        out of the refresh forever.  Retrying BEFORE the refresh is spawned
        matters: :meth:`_queue_pause_write` installs the write tail
        synchronously, so the refresh in the same pass already sees the
        write in flight and leaves the job's memory alone.
        """
        now = get_now(datetime.timezone.utc)
        for name, info in list(self._paused.items()):
            if info.until <= now:
                del self._paused[name]
                self._sla_bank_pause(name, info, info.until)
                self.metrics.job_pause_state(name, False)
                logger.info(
                    "Job %s: pause expired at %s; scheduled runs resume",
                    name,
                    info.until.isoformat(),
                )
        if self.state_backend is None:
            return
        self._replay_pending_pause_writes()
        if self._pause_refresh_task is None or self._pause_refresh_task.done():
            self._pause_refresh_task = self._track_state_write(
                self._refresh_pauses_from_store()
            )

    async def _refresh_pauses_from_store(self) -> None:
        """Converge the in-memory pause map on the durable ``paused/`` streams.

        Cross-node propagation (and the boot rehydrate): newest record per
        stream wins. A live ``paused`` record installs the window; a
        ``resumed`` (or expired, or absent) record clears it.  A job whose
        own write chain still has a pause record in flight is skipped, as is
        one whose write generation moved while its record was being read:
        this node's memory is by definition newer than the store for it.  On
        any store trouble the LAST KNOWN in-memory state is kept and a
        warning logged, under both onStoreUnavailable policies,
        deliberately: a pause is an operator convenience, not a correctness
        fence, so an unreadable store must neither resurrect nor drop pauses
        at random, and must never block firing.  Store trouble reading ONE
        job's stream keeps only that job's state and the sweep carries on;
        only a failure to enumerate the streams at all ends the pass.
        """
        backend = self.state_backend
        if backend is None:
            return
        try:
            streams = await asyncio.wait_for(
                backend.list_stream_names(PAUSE_STREAM_PREFIX),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - keep last known state
            logger.warning(
                "state: cannot refresh pause state (keeping the last known "
                "in-memory state): %s",
                ex,
            )
            return
        now = get_now(datetime.timezone.utc)
        for stream in streams:
            name = stream[len(PAUSE_STREAM_PREFIX) :]
            if name not in self.cron_jobs:
                continue  # a removed job's stream; GC's business
            tail = self._pause_write_tail.get(name)
            if tail is not None and not tail.done():
                continue  # our own newer write has not landed yet
            if name in self._pause_pending_writes:
                # a buffered record (store down, or an append that failed)
                # means memory is newer than the stream for this job and the
                # write is still owed: applying the record it supersedes
                # would silently revoke the operator's pause or resume.
                continue
            gen = self._pause_gen.get(name, 0)
            try:
                recs = await asyncio.wait_for(
                    backend.list_records(stream, limit=1, newest_first=True),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception as ex:  # noqa: BLE001 - keep last known state
                logger.warning(
                    "state: cannot refresh pause state for %s (keeping the "
                    "last known in-memory state): %s",
                    name,
                    ex,
                )
                # this job only: one unreadable stream must not starve every
                # job after it in the sweep (the order is the store's sorted
                # stream list, so it would be the same jobs every pass, and
                # this sweep is also the boot rehydrate).
                continue
            if name not in self.cron_jobs:
                # a reload removed the job while we were reading its stream.
                # The generation guard below does not cover this: _apply_reload
                # prunes _pause_gen along with _paused, so the sampled and the
                # current generation are both 0 and it passes. Installing here
                # would leave a permanent stale _paused entry and re-create the
                # metric series prune() just dropped (PrometheusMetrics._job
                # auto-creates), and no later sweep cleans either up: they
                # all skip the stream at the membership test above.
                continue
            if self._pause_gen.get(name, 0) != gen:
                # a local pause/resume landed while we were reading: memory
                # is newer than the snapshot in hand. Re-checking the write
                # tail is NOT enough, since a write that also completed
                # inside that window has already deleted its tail entry.
                continue
            info = self._pause_info_from_record(recs[0] if recs else None)
            if info is not None and info.until > now:
                known = self._paused.get(name)
                if known is not None and known.until != info.until:
                    # a different window replaces the known one: bank what
                    # the old one already held (see _sla_bank_pause).
                    self._sla_bank_pause(name, known, now)
                self._paused[name] = info
                self.metrics.job_pause_state(name, True)
                if known is None or known.until != info.until:
                    logger.info(
                        "Job %s: paused until %s via the shared state store",
                        name,
                        info.until.isoformat(),
                    )
            else:
                was = self._paused.pop(name, None)
                if was is not None:
                    self._sla_bank_pause(name, was, now)
                    self.metrics.job_pause_state(name, False)
                    logger.info(
                        "Job %s: resumed via the shared state store", name
                    )

    @staticmethod
    def _pause_info_from_record(
        rec: Optional[Dict[str, Any]],
    ) -> Optional[PauseInfo]:
        """Rebuild a :class:`PauseInfo` from a durable ``paused`` record.

        ``None`` for anything else on top of the stream (a ``resumed``
        record, a foreign/corrupt record, an empty stream): the caller
        treats those identically as "not paused".  Expiry is NOT judged
        here; the caller compares ``until`` against its own clock.
        """
        if not rec or rec.get("kind") != "paused":
            return None
        until = _parse_iso_utc(rec.get("until"))
        if until is None:
            return None
        since = _parse_iso_utc(rec.get("since"))
        note = rec.get("note")
        by = rec.get("by")
        channel = rec.get("channel")
        return PauseInfo(
            since=since if since is not None else until,
            until=until,
            note=note if isinstance(note, str) else "",
            by=by if isinstance(by, str) else "",
            channel=channel if isinstance(channel, str) else "",
        )

    def _sla_periodic(self) -> None:
        """Evaluate the per-job SLA checks; latch, meter and report breaches.

        Called from the housekeeping pass (at most once per wall-clock
        minute) and deliberately NOT from _state_periodic: the checks are
        purely in-memory, so they must run with no state backend
        configured. A disabled or paused job is excused, and so is a job
        this node does not own under election (:meth:`_cluster_allows`),
        so one breach pages once, not once per node. Excused means the
        job's latches are DROPPED, not merely left unevaluated: a job the
        operator deliberately silenced must stop asserting a live breach,
        and one still breaching when it becomes eligible again re-latches
        and pages once, the same trade-off a restart already makes. The
        (job, check) latch drives the transitions: ok to breached fires
        onLate ONCE, sets the late gauge, counts the breach and warns;
        breached to ok clears the gauge and logs, with no report.
        Reporters are queued through the
        completion-task idiom (:meth:`_queue_sla_report`), never awaited
        here on the scheduler loop. The latch is in-memory only, so after
        a restart a still-breached check re-fires once; that is the
        documented trade-off.
        """
        now = get_now(datetime.timezone.utc)
        for name, job in self.cron_jobs.items():
            if not job.has_sla:
                # a job with no sla check (never had one, or a reload just
                # blanked its block) must not leave its latches (and the late
                # gauge) stuck at breached. has_sla is precomputed on the
                # JobConfig, so a deployment not using the feature does no more
                # than this O(1) test per job each pass.
                self._sla_clear_latches(name)
                continue
            if not job.enabled or self._pause_active(name) is not None:
                # excused: a breach latched before the pause/disable would
                # otherwise pin cronstable_job_late, the /jobs sla block and
                # the OVERDUE chip at breached for the whole window.
                self._sla_clear_latches(name)
                if not job.enabled:
                    # a disabled job cannot run, so its staleness baseline
                    # rolls forward with the clock: re-enabling then gives it
                    # a full threshold to succeed in, the same credit a pause
                    # banks, instead of paging maxTimeSinceSuccess for the
                    # whole span it was deliberately switched off. This roll
                    # covers the never-succeeded fallback (the _sla_first_seen
                    # arm of _sla_stale_reference); a job that HAS succeeded
                    # takes the _sla_last_success arm, whose disabled span is
                    # banked as a credit at the re-enable transition below.
                    # Record the span start on the first disabled tick.
                    self._sla_first_seen[name] = now
                    self._sla_disabled_since.setdefault(name, now)
                continue
            # Reaching here, the job is enabled and not paused. If it just
            # left a disabled span, bank that span as a staleness credit,
            # exactly as a lifted pause: _sla_paused_seconds then subtracts it
            # from observed in _sla_observations against the _sla_last_success
            # reference, so a previously-succeeded job does not page
            # maxTimeSinceSuccess for the whole span it was switched off (the
            # _sla_first_seen roll-forward above only reaches the
            # never-succeeded arm). Banked once at the transition, mirroring
            # _sla_bank_pause on resume, and before the cluster gate below so
            # the credit is recorded on every node whether or not this one
            # owns the job now.
            disabled_since = self._sla_disabled_since.pop(name, None)
            if disabled_since is not None:
                self._sla_bank_pause(
                    name,
                    PauseInfo(
                        since=disabled_since,
                        until=now,
                        note="",
                        by="",
                        channel="",
                    ),
                    now,
                )
            if not self._cluster_allows(job):
                # not this node's job right now: same latch drop, and drop
                # the lateAfter reference too. A slot recorded while this
                # node owned the job would page the moment ownership came
                # back, for a slot the owner of the day ran on time.
                self._sla_clear_latches(name)
                self._sla_due.pop(name, None)
                continue
            observations = self._sla_observations(name, job, now)
            for check, (threshold, observed, breached) in observations.items():
                since = self._sla_state.get((name, check))
                # restated every pass (an idempotent dict write) so the
                # late series exists at 0, and the breach counter is
                # zero-filled, from the first evaluation: increase() then
                # has a baseline sample before the first breach.
                self.metrics.job_sla_late(name, check, breached)
                if breached and since is None:
                    self._sla_state[(name, check)] = now
                    self.metrics.job_sla_breach(name, check)
                    logger.warning(
                        "Job %s: SLA check %s breached: observed %.0fs "
                        "exceeds the threshold of %ss",
                        name,
                        check,
                        observed,
                        threshold,
                    )
                    self._queue_sla_report(job, check, threshold, observed)
                elif not breached and since is not None:
                    del self._sla_state[(name, check)]
                    logger.info(
                        "Job %s: SLA check %s recovered (observed %.0fs is "
                        "back within the threshold of %ss)",
                        name,
                        check,
                        observed,
                        threshold,
                    )
            # a reload can drop ONE check while keeping the sla block: clear
            # its stale latch the same way. Walks the fixed SLA_CHECKS set,
            # not the whole latch map, to stay O(checks) per job.
            for check in SLA_CHECKS:
                if check in observations:
                    continue
                if self._sla_state.pop((name, check), None) is not None:
                    self.metrics.job_sla_late(name, check, False)

    def _sla_clear_latches(self, name: str) -> None:
        """Drop every breach latch of ``name`` and clear its late gauge.

        Walks the fixed :data:`SLA_CHECKS` set rather than scanning the whole
        (name, check) latch map for this name's half, so clearing one job's
        latches stays O(checks) even when many other jobs are latched.
        """
        if not self._sla_state:
            return
        for check in SLA_CHECKS:
            if self._sla_state.pop((name, check), None) is not None:
                self.metrics.job_sla_late(name, check, False)

    def _sla_peer_owns_slot(self, name: str) -> None:
        """Excuse this node's lateAfter slot: a peer holds the run.

        A cluster-scoped slot denied by a LIVE foreign holder means the
        job is running somewhere in the fleet, so the slot this node
        recorded as due was never this node's to launch.  Dropping the
        reference lands lateAfter on its "nothing to be late for" branch
        instead of paging every node that lost the race.  The fail-closed
        denial (the store did not answer) deliberately keeps the
        reference: no peer is known to have run it, so a breach there is
        real.
        """
        self._sla_due.pop(name, None)

    def _sla_stale_reference(self, name: str) -> datetime.datetime:
        """The instant maxTimeSinceSuccess measures the job's staleness from.

        The job's newest known success, fed by :meth:`_record_run` and
        warmed from the durable ledger at rehydrate.  With none on record
        (a stateless daemon, or a job that has never succeeded) it falls
        back to when the job was first seen, so a fresh boot and a job a
        reload just added both age into the breach instead of paging
        instantly.  Process start is the last resort, for a name that
        somehow predates the first-seen map.
        """
        reference = self._sla_last_success.get(name)
        if reference is not None:
            return reference
        return self._sla_first_seen.get(name, self._process_start)

    def _sla_bank_pause(
        self, name: str, was: PauseInfo, ended_at: datetime.datetime
    ) -> None:
        """Bank a pause window that just ended, for the staleness credit.

        Called wherever a window leaves ``self._paused``: the expiry
        sweep, an explicit resume, a re-pause that overwrites the window,
        a peer's pause/resume arriving through the store refresh, and a
        disabled span crediting itself on re-enable.  The banked spans are
        kept sorted and disjoint by inserting the new window and coalescing
        every span it touches, so a shared stretch is never credited twice
        and a window that arrives OUT OF ``since`` order (a disabled span
        banked after a later pause that overlaps it) still merges whole
        rather than dropping the earlier stretch.  A window rehydrated from
        the store carries its original ``since``, so a pause spanning a
        restart is credited whole.  Windows the staleness reference has
        already passed can never contribute again and are dropped, and what
        is left is capped: dropping the OLDEST span understates the credit,
        which fails toward paging.
        """
        if ended_at > was.until:
            ended_at = was.until
        if ended_at <= was.since:
            return
        raw = sorted(
            self._sla_pause_windows.get(name, []) + [(was.since, ended_at)]
        )
        merged: List[Tuple[datetime.datetime, datetime.datetime]] = []
        for start, end in raw:
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
            else:
                merged.append((start, end))
        reference = self._sla_stale_reference(name)
        merged = [span for span in merged if span[1] > reference]
        del merged[:-SLA_PAUSE_SPANS_MAX]
        if merged:
            self._sla_pause_windows[name] = merged
        else:
            self._sla_pause_windows.pop(name, None)

    def _sla_paused_seconds(
        self, name: str, reference: datetime.datetime, now: datetime.datetime
    ) -> float:
        """Seconds of ``[reference, now]`` the job spent deliberately paused.

        Credited against maxTimeSinceSuccess so a held job gets a full
        threshold once the pause lifts, rather than paging unattended the
        instant it does.  The banked windows are disjoint
        (:meth:`_sla_bank_pause`) and clamped to the measured interval
        here, so no window counts twice and none counts before the
        reference the check is measuring from.
        """
        spans = self._sla_pause_windows.get(name)
        if not spans:
            return 0.0
        credit = 0.0
        for start, end in spans:
            overlap = (min(now, end) - max(reference, start)).total_seconds()
            if overlap > 0:
                credit += overlap
        return credit

    def _sla_observations(
        self, name: str, job: JobConfig, now: datetime.datetime
    ) -> Dict[str, Tuple[int, float, bool]]:
        """The job's configured SLA checks, freshly measured against ``now``.

        check label -> (threshold_seconds, observed_seconds, breached),
        one entry per non-null sla key, in the config block's order.
        Shared by the monitor (which latches on it) and the /jobs payload
        (which reports live observed values for latched checks), so both
        surfaces measure the same way.
        """
        out: Dict[str, Tuple[int, float, bool]] = {}
        threshold = job.sla.get("maxTimeSinceSuccessSeconds")
        if threshold is not None:
            reference = self._sla_stale_reference(name)
            # time the operator deliberately held the job is not staleness:
            # excluding it gives a resumed job a full threshold before it can
            # page, the same excusal lateAfter already gets in _launch_plan.
            observed = (now - reference).total_seconds() - (
                self._sla_paused_seconds(name, reference, now)
            )
            out[SLA_CHECK_STALE] = (threshold, observed, observed > threshold)
        threshold = job.sla.get("lateAfterSeconds")
        if threshold is not None:
            due = self._sla_due.get(name)
            if due is None:
                # no unexcused slot recorded yet this process: nothing to
                # be late FOR (also the restart baseline).
                out[SLA_CHECK_LATE] = (threshold, 0.0, False)
            else:
                started = self._sla_last_start.get(name)
                observed = (now - due).total_seconds()
                out[SLA_CHECK_LATE] = (
                    threshold,
                    observed,
                    observed > threshold
                    and (started is None or started < due)
                    # an instance is still running, so the slot the
                    # concurrency policy dropped is not an unserved slot:
                    # an overrun is maxRuntime's to report, once. Read with
                    # .get(): running_jobs is a defaultdict, and a bare
                    # subscript here would mint a phantom key.
                    and not self.running_jobs.get(name),
                )
        threshold = job.sla.get("maxRuntimeSeconds")
        if threshold is not None:
            observed = 0.0
            for runjob in self.running_jobs.get(name) or []:
                # started_at is the run's aware-UTC launch instant (the
                # same field the /jobs/{name}/resources payload reports);
                # never killed here, the check only observes.
                if runjob.started_at is None:
                    continue
                running_for = (now - runjob.started_at).total_seconds()
                if running_for > observed:
                    observed = running_for
            out[SLA_CHECK_RUNTIME] = (
                threshold,
                observed,
                observed > threshold,
            )
        return out

    def _queue_sla_report(
        self, job: JobConfig, check: str, threshold: int, observed: float
    ) -> None:
        """Dispatch one onLate report as a tracked, per-job-chained task.

        The :meth:`_queue_job_completion` idiom: reporters (SMTP, shell
        commands, webhooks) legitimately take tens of seconds and must
        never run inline on the scheduler loop. The report waits behind
        ``_completion_tail`` (so it is ordered after the same job's
        in-flight completion reports) but installs itself in its OWN
        ``_sla_report_tail`` instead of becoming the completion tail: a
        monitor-initiated report must never sit in FRONT of a real run's
        report+retry-arm sequence, which blocks on the completion tail.
        maxRuntime makes that the ordinary case rather than a corner: it
        breaches while the run is still executing, so the report is
        guaranteed to be in flight when that run finishes.
        ``_completion_tasks`` membership still means shutdown (and tests)
        drain it via :meth:`_drain_completions`.
        """
        name = job.name
        last_success = self._sla_last_success.get(name)
        ctx = SlaBreachContext(
            job,
            check=check,
            threshold_seconds=threshold,
            observed_seconds=observed,
            last_success_at=(
                last_success.isoformat() if last_success is not None else None
            ),
        )
        report_config = job.onLate["report"]
        earlier = [
            self._completion_tail.get(name),
            self._sla_report_tail.get(name),
        ]

        async def _sequenced() -> None:
            for prev in earlier:
                if prev is not None and not prev.done():
                    await asyncio.wait({prev})
            try:
                await report_sla_breach(ctx, report_config)
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "Unexpected error reporting the SLA breach of job %s; "
                    "please report this as a bug (7)",
                    name,
                )

        task = asyncio.create_task(_sequenced())
        self._completion_tasks.add(task)
        task.add_done_callback(self._completion_tasks.discard)
        self._sla_report_tail[name] = task

        def _clear(done: asyncio.Task) -> None:
            if self._sla_report_tail.get(name) is done:
                del self._sla_report_tail[name]

        task.add_done_callback(_clear)

    async def _web_start_job(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        try:
            await self.start_job_by_name(request.match_info["name"])
        except ApiActionError as ex:
            raise self._action_http_error(ex) from ex
        return web.Response(headers=self.web_config.get("headers", None))

    async def _web_cancel_job(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        try:
            await self.cancel_job_by_name(request.match_info["name"])
        except ApiActionError as ex:
            raise self._action_http_error(ex) from ex
        return web.Response(headers=self.web_config.get("headers", None))

    async def _web_pause_job(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        body = await self._web_json_body(request)
        duration = body.get("durationSeconds")
        # bool is an int subclass; `true` must not read as one second.
        if duration is not None and (
            not isinstance(duration, int) or isinstance(duration, bool)
        ):
            raise web.HTTPBadRequest(text="durationSeconds must be an integer")
        until_raw = body.get("until")
        until = None
        if until_raw is not None:
            if not isinstance(until_raw, str):
                raise web.HTTPBadRequest(
                    text="until must be an ISO-8601 timestamp string"
                )
            until = _parse_iso_utc(until_raw)
            if until is None:
                raise web.HTTPBadRequest(
                    text="until is not a valid ISO-8601 timestamp"
                )
        note = body.get("note")
        if note is not None and not isinstance(note, str):
            raise web.HTTPBadRequest(text="note must be a string")
        by = body.get("by")
        if by is not None and not isinstance(by, str):
            raise web.HTTPBadRequest(text="by must be a string")
        try:
            paused = await self.pause_job_by_name(
                request.match_info["name"],
                duration=duration,
                until=until,
                note=note or "",
                by=by or "api",
                channel="api",
            )
        except ApiActionError as ex:
            raise self._action_http_error(ex) from ex
        return _json_response(
            {"paused": paused}, headers=self.web_config.get("headers", None)
        )

    async def _web_resume_job(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        body = await self._web_json_body(request)
        by = body.get("by")
        if by is not None and not isinstance(by, str):
            raise web.HTTPBadRequest(text="by must be a string")
        try:
            await self.resume_job_by_name(
                request.match_info["name"], by=by or "api", channel="api"
            )
        except ApiActionError as ex:
            raise self._action_http_error(ex) from ex
        return _json_response(
            {"paused": None}, headers=self.web_config.get("headers", None)
        )

    def _action_http_error(self, ex: "ApiActionError") -> web.HTTPException:
        # historical parity: web.headers ride the 409 conflict bodies of the
        # start/cancel routes, but NOT their 404 (unknown job).
        assert self.web_config is not None
        headers = (
            self.web_config.get("headers", None) if ex.status == 409 else None
        )
        return _http_for_action_error(ex, headers)

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

    def _scheduled_in(
        self, name: str, job: JobConfig, running: bool
    ) -> Optional[float]:
        """Seconds until the job's next scheduled run.

        ``None`` when not applicable: disabled, currently running, a
        one-off ``@reboot`` schedule (a string, not a crontab), or a
        schedule with no future occurrence.  Steady state reads the
        loop's own next-fire index (the same source prometheus.py's
        next-run gauge reads) rather than re-walking the crontab: this
        runs per job on every /jobs poll, every /status call, and every
        gossiped fleet summary.  The engine search survives only as the
        fallback for the startup window before the loop's first pass
        seeds the index.
        """
        if not job.enabled or running:
            return None
        crontab = job.schedule  # type: Union[CronTab, str]
        if not isinstance(crontab, CronTab):
            return None
        when = self._next_fire.get(name)
        if when is not None:
            # clamped at zero: a job due this very instant reads as
            # marginally past until the pass advances it beyond its slot
            now = get_now(datetime.timezone.utc)
            return max(0.0, (when - now).total_seconds())
        if name in self._dead_schedules:
            # no future occurrence; the engine's answer would be None too,
            # found only after walking the remaining horizon
            return None
        seconds: Optional[float] = crontab.next(
            now=get_now(job.timezone), default_utc=job.utc
        )
        return seconds

    def _schedule_never_fires(self, name: str, job: JobConfig) -> bool:
        """True when an enabled cron job's schedule has no future instant.

        Holds for running jobs too (a running job with a dead schedule
        still never fires again).  Shared by the /jobs and /status
        payloads so the two surfaces cannot drift.  Steady state is two
        set probes: once seeded, an enabled CronTab job is either in the
        next-fire index (fires again) or in the dead-schedules latch
        (never does).  The full engine search survives only as the
        unseeded-startup fallback, which is the worst place for it: a
        dead schedule is precisely the one that walks the whole horizon,
        and it used to do so per running job per poll.
        """
        if not (job.enabled and isinstance(job.schedule, CronTab)):
            return False
        if name in self._next_fire:
            return False
        if name in self._dead_schedules:
            return True
        return (
            job.schedule.next(now=get_now(job.timezone), default_utc=job.utc)
            is None
        )

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
                "scheduled_in": self._scheduled_in(name, job, running),
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
        scheduled_in = self._scheduled_in(name, job, bool(running))
        # a dead schedule's None means NEVER, which the dashboards must be
        # able to tell apart from the running/disabled Nones above.  For a
        # job that is not running, _scheduled_in above already answered
        # this (enabled + cron + no next instant is precisely "never
        # fires"), so reuse its answer instead of asking twice per job on
        # every /jobs poll.  Only a running job (whose scheduled_in is
        # None by design) needs the direct probe: a running job with a
        # dead schedule still never fires again.
        if running:
            never_fires = self._schedule_never_fires(name, job)
        else:
            never_fires = (
                job.enabled
                and isinstance(job.schedule, CronTab)
                and scheduled_in is None
            )

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
            "never_fires": never_fires,
            # advisory lint from config load (see JobConfig), so the
            # dashboards can badge footguns without re-deriving them
            "schedule_findings": [
                finding._asdict() for finding in job.schedule_findings
            ],
            "last_run": last_run,
            "history": recent,
            # the active pause window, or null. Always present (unlike the
            # conditional blocks below) so the dashboards can bind to it
            # without probing: paused is first-class job state, not an
            # optional feature's extra.
            "paused": (
                pause.to_dict()
                if (pause := self._pause_active(name)) is not None
                else None
            ),
        }  # type: Dict[str, Any]
        if isinstance(
            job.schedule, CronTab
        ) and job.schedule.resolved_source != str(job.schedule):
            # the H hash form: also ship the plain-dialect spelling it
            # resolved to, so the dashboards display the H the user wrote
            # while their client-side engines (which know no H) compute
            # previews from this. Omitted for every other schedule.
            result["schedule_resolved"] = job.schedule.resolved_source
        # live CPU/memory of the currently-running instances (monitorResources
        # jobs only). Summed across instances so a job running N copies shows
        # its aggregate footprint; omitted entirely when nothing is monitored
        # or no sample has landed yet, so an unmonitored job's payload is
        # unchanged.
        live_snaps = [
            snap
            for runjob in running
            if (snap := runjob.live_resources()) is not None
        ]
        if live_snaps:
            result["running_resources"] = {
                "cpu_percent": sum(s["cpu_percent"] for s in live_snaps),
                "cpu_seconds": sum(s["cpu_seconds"] for s in live_snaps),
                "rss_bytes": sum(s["rss_bytes"] for s in live_snaps),
                "instances": len(live_snaps),
            }
        # durable-retry visibility: when a retry ladder is ARMED for this job,
        # surface attempt/backoff so the dashboard can render a live
        # "attempt N/M · next retry in Xs" chip. Gated on count > 0: the ladder
        # is created eagerly at launch with count 0 even for a run that will
        # succeed, so presence alone would flag every healthy retry-configured
        # job with a phantom "attempt 0" chip. Omitted otherwise (lean).
        retry_state = self.retry_state.get(name)
        if (
            retry_state is not None
            and not retry_state.cancelled
            and retry_state.count > 0
        ):
            retry_cfg = job.onFailure.get("retry", {}) if job.onFailure else {}
            max_retries = retry_cfg.get("maximumRetries")
            result["retry"] = {
                "attempt": retry_state.count,
                # -1 means unlimited; surface as null (no ceiling to render).
                "maxAttempts": None if max_retries == -1 else max_retries,
                "nextRetryAt": (
                    retry_state.next_retry_at.isoformat()
                    if retry_state.next_retry_at is not None
                    else None
                ),
                "delaySeconds": retry_state.scheduled_delay,
            }
        # per-job SLA introspection, only when the job configures a check:
        # the non-null thresholds, the latched verdict, and the live
        # breach detail (since = when the monitor latched it; observed is
        # re-measured at payload time so the dashboards show a moving
        # number, not the minute-old latch snapshot).
        thresholds = {k: v for k, v in job.sla.items() if v is not None}
        if thresholds:
            observations = self._sla_observations(
                name, job, get_now(datetime.timezone.utc)
            )
            breaches = []
            for check, (
                threshold,
                observed,
                _breached,
            ) in observations.items():
                since = self._sla_state.get((name, check))
                if since is None:
                    continue
                breaches.append(
                    {
                        "check": check,
                        "since": since.isoformat(),
                        "observed_seconds": observed,
                        "threshold_seconds": threshold,
                    }
                )
            result["sla"] = {
                "thresholds": thresholds,
                "state": "late" if breaches else "ok",
                "breaches": breaches,
            }
        # a deferred @reboot one-shot still awaiting its boot run (the cluster
        # had not elected an owner at boot, or a pause is holding it): lets
        # the dashboard distinguish "pending boot run" from "already ran".
        if (
            name in self._pending_reboot_jobs
            or name in self._paused_reboot_jobs
        ):
            result["rebootPending"] = True
        # cluster-wide concurrency slot (concurrencyScope: cluster): whether
        # THIS node holds the job's slot lease and how many live instances
        # reference it. Only emitted for cluster-scoped jobs.
        if job.concurrencyScope == "cluster":
            # _slot_leases/_slot_refs are keyed by plain JOB name (only the
            # on-disk lease/stream name carries the "slots/" prefix; see
            # _slot_name and _claim_cluster_slot).
            lease = self._slot_leases.get(name)
            result["concurrencyScope"] = "cluster"
            result["slot"] = {
                "held": lease is not None,
                "holder": lease.holder if lease is not None else None,
                "refs": self._slot_refs.get(name, 0),
            }
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

    def jobs_payload(self) -> List[Dict[str, Any]]:
        """Full per-job dicts for ``GET /jobs`` and MCP ``cron_list_jobs``."""
        return [
            self._job_to_dict(name, job)
            for name, job in self.cron_jobs.items()
        ]

    def job_detail_payload(self, name: str) -> Optional[Dict[str, Any]]:
        """One job's full dict, or ``None`` when there is no such job."""
        job = self.cron_jobs.get(name)
        if job is None:
            return None
        return self._job_to_dict(name, job)

    async def _web_list_jobs(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        # Build on the loop (it reads live scheduler state), then hash +
        # serialize off it for a large fleet.  A content-hash ETag lets a
        # conditional client (or a cache/proxy) 304 an unchanged poll,
        # skipping the body encode and transfer; the tag is keyed on the
        # ABSOLUTE next-fire, not the relative scheduled_in, so it stays
        # put while the countdown ticks and moves the instant a fire lands.
        payload = self.jobs_payload()
        next_fire_iso: Dict[str, Optional[str]] = {}
        for name in self.cron_jobs:
            when = self._next_fire.get(name)
            next_fire_iso[name] = (
                when.isoformat() if when is not None else None
            )
        inm = request.headers.get("If-None-Match")
        if len(payload) >= _JOBS_SERIALIZE_OFFLOAD_MIN:
            etag, body = await asyncio.get_running_loop().run_in_executor(
                None, _jobs_conditional_response, payload, next_fire_iso, inm
            )
        else:
            etag, body = _jobs_conditional_response(
                payload, next_fire_iso, inm
            )
        headers = self._web_jobs_headers(etag)
        if body is None:
            return web.Response(status=304, headers=headers)
        return web.Response(
            body=body,
            status=200,
            headers=headers,
            content_type="application/json",
        )

    def _web_jobs_headers(self, etag: str) -> Dict[str, str]:
        """The configured web response headers plus the ``/jobs`` ETag."""
        assert self.web_config is not None
        base = self.web_config.get("headers", None)
        headers: Dict[str, str] = dict(base) if base else {}
        headers["ETag"] = etag
        return headers

    # --- DAG introspection + control --------------------------------------

    def _web_headers(self) -> Any:
        assert self.web_config is not None
        return self.web_config.get("headers", None)

    async def dags_payload(self) -> List[Dict[str, Any]]:
        """Configured DAGs + tasks (`GET /dags`, MCP `cron_list_dags`)."""
        dags = await self._dag.list_dags()
        # graft the human-readable schedule string here (schedule_str lives in
        # this module; dagrun cannot import it without a cycle).
        for entry in dags:
            cfg = self.cron_dags.get(entry["name"])
            if cfg is not None and cfg.schedule_job is not None:
                entry["schedule"] = schedule_str(cfg.schedule_job)
        return dags

    async def _web_list_dags(self, request: web.Request) -> web.Response:
        return _json_response(
            await self.dags_payload(), headers=self._web_headers()
        )

    async def _web_dag_runs(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        limit = self._web_int_query(request, "limit", default=50, lo=1, hi=500)
        runs = await self._dag.list_runs(name, limit=limit)
        if runs is None:
            raise web.HTTPNotFound()
        return _json_response(
            {"dag": name, "runs": runs}, headers=self._web_headers()
        )

    async def _web_dag_run(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        run_key = request.match_info["run_key"]
        body = await self._dag.get_run(name, run_key)
        if body is None:
            raise web.HTTPNotFound()
        return _json_response(body, headers=self._web_headers())

    async def _web_dag_xcom(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        run_key = request.match_info["run_key"]
        result = await self._dag.xcom_for_run(name, run_key)
        if result is None:
            raise web.HTTPNotFound()
        return _json_response(result, headers=self._web_headers())

    # --- durable state inspector (metadata-only) --------------------------

    async def _web_state(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        return _json_response(
            await self.state_payload(),
            headers=self.web_config.get("headers", None),
        )

    async def state_payload(self) -> Dict[str, Any]:
        """Store health + topology for the state inspector, metadata only.

        Per-prefix stream/document counts, capped scope lists, and active
        leases -- never a record payload or a KV value.  Also carries THIS
        node's live retry ladder and held concurrency slots (straight from
        memory).  ``enabled: false`` when no state backend is configured.
        Behind ``GET /state`` and MCP ``cron_inspect_state``.
        """
        backend = self.state_backend
        if backend is None:
            return {"enabled": False}
        try:
            inv = await asyncio.wait_for(
                backend.inventory(), timeout=STATE_OP_TIMEOUT
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade to health only
            logger.warning("state: inventory failed (%s)", ex)
            inv = {
                "view": backend.view_dict(),
                "stats": backend.stats(),
                "enumerable": False,
                "records": {},
                "documents": {},
                "leases": [],
                "quarantine": 0,
            }
        inv["enabled"] = True
        # this node's freshest HA state, straight from memory (no store read).
        inv["node"] = {
            "host": self._state_host,
            "retries": [
                {
                    "job": name,
                    "attempt": st.count,
                    "nextRetryAt": (
                        st.next_retry_at.isoformat()
                        if st.next_retry_at is not None
                        else None
                    ),
                    "delaySeconds": st.scheduled_delay,
                }
                for name, st in self.retry_state.items()
                if not st.cancelled and st.count > 0
            ],
            "slots": [
                {
                    "slot": slot_name,
                    "holder": lease.holder,
                    "fence": lease.fence,
                    "expiresAt": lease.expires_at,
                    "refs": self._slot_refs.get(slot_name, 0),
                }
                for slot_name, lease in self._slot_leases.items()
            ],
        }
        return inv

    async def state_documents_payload(self, ns: str) -> Dict[str, Any]:
        """Documents of one KV/cursor/idempotency namespace, redacted.

        KV values are stripped to a ``valueSize``/``valueType`` summary
        (metadata-only stance); cursor watermarks and idempotency claim
        metadata are returned verbatim (no user secret there).  Behind
        ``GET /state/documents`` and MCP ``cron_inspect_state``.  Raises
        :class:`ApiActionError` when there is no store or ``ns`` is not an
        inspectable namespace.
        """
        backend = self.state_backend
        if backend is None:
            raise ApiActionError("state store is not configured", status=404)
        if not ns.startswith(("kv/", "cursor/", "idem/")):
            raise ApiActionError(
                "ns must be a kv/, cursor/ or idem/ namespace", status=400
            )
        try:
            docs = await asyncio.wait_for(
                backend.list_documents(ns), timeout=STATE_OP_TIMEOUT
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - degrade to empty
            docs = []
        # KV values are stripped to a size/type summary (a KV value is
        # arbitrary job-authored data that may be sensitive). Cursor
        # watermarks are returned verbatim ON PURPOSE -- they are small
        # progress markers (a timestamp / offset / id), the operator opted
        # into seeing them, and hiding them would gut the cursor panel.
        # Idempotency docs carry only key/claimedAt/expiresAt (no value).
        redact_values = ns.startswith("kv/")
        out = []
        for doc in docs:
            if redact_values and "value" in doc:
                value = doc.get("value")
                summary = {k: v for k, v in doc.items() if k != "value"}
                try:
                    summary["valueSize"] = len(
                        json.dumps(value).encode("utf-8")
                    )
                except (TypeError, ValueError):
                    summary["valueSize"] = None
                summary["valueType"] = type(value).__name__
                out.append(summary)
            else:
                out.append(doc)
        return {"namespace": ns, "documents": out}

    async def _web_state_documents(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        try:
            payload = await self.state_documents_payload(
                request.query.get("ns", "")
            )
        except ApiActionError as ex:
            raise _http_for_action_error(ex) from ex
        return _json_response(
            payload, headers=self.web_config.get("headers", None)
        )

    async def state_records_payload(
        self, stream: str, limit: int = 100
    ) -> Dict[str, Any]:
        """Newest records of one stream, metadata-only.

        Archived-output (``logs/``) streams are refused: they carry raw job
        output, which the metadata-only stance keeps off this surface.  Behind
        ``GET /state/records`` and MCP ``cron_inspect_state``.  Raises
        :class:`ApiActionError` for a missing store, empty stream, or a log
        stream.
        """
        backend = self.state_backend
        if backend is None:
            raise ApiActionError("state store is not configured", status=404)
        if not stream:
            raise ApiActionError("a stream is required", status=400)
        if stream.startswith("logs/") or stream == "logs":
            # archived job output: raw content, excluded from the metadata
            # inspector on purpose.
            raise ApiActionError(
                "log streams carry raw output and are not inspectable",
                status=403,
            )
        try:
            recs = await asyncio.wait_for(
                backend.list_records(stream, limit=limit, newest_first=True),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - degrade to empty
            recs = []
        return {"stream": stream, "records": recs}

    async def _web_state_records(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        limit = self._web_int_query(
            request, "limit", default=100, lo=1, hi=500
        )
        try:
            payload = await self.state_records_payload(
                request.query.get("stream", ""), limit=limit
            )
        except ApiActionError as ex:
            raise _http_for_action_error(ex) from ex
        return _json_response(
            payload, headers=self.web_config.get("headers", None)
        )

    async def _web_dag_trigger(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        run_key = await self._dag.trigger_run(name)
        if run_key is None:
            raise web.HTTPNotFound()
        return _json_response(
            {"dag": name, "runKey": run_key}, headers=self._web_headers()
        )

    async def _web_dag_backfill(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        payload = await self._web_json_body(request)
        start = payload.get("from")
        end = payload.get("to")
        if not isinstance(start, str) or not isinstance(end, str):
            raise web.HTTPBadRequest(
                text="backfill needs string `from` and `to` ISO dates"
            )
        result = await self._dag.backfill(name, start, end)
        if not result.get("ok"):
            raise web.HTTPBadRequest(text=str(result.get("reason")))
        return _json_response(result, headers=self._web_headers())

    async def _web_dag_decision(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        run_key = request.match_info["run_key"]
        taskkey = request.match_info["taskkey"]
        payload = await self._web_json_body(request)
        decision = payload.get("decision")
        if decision not in ("approve", "reject"):
            raise web.HTTPBadRequest(
                text="decision must be 'approve' or 'reject'"
            )
        by = str(payload.get("by") or "api")
        result = await self._dag.approve(
            name, run_key, taskkey, approved=(decision == "approve"), by=by
        )
        if not result.get("ok"):
            raise web.HTTPConflict(text=str(result.get("reason")))
        return _json_response(result, headers=self._web_headers())

    @staticmethod
    def _web_int_query(
        request: web.Request, name: str, *, default: int, lo: int, hi: int
    ) -> int:
        """A clamped integer query param; falls back to ``default`` on a
        missing or unparseable value (a bad query is never a 400 here)."""
        raw = request.query.get(name)
        if raw is None:
            return default
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return default
        return max(lo, min(hi, value))

    @staticmethod
    async def _web_json_body(request: web.Request) -> Dict[str, Any]:
        if not request.can_read_body:
            return {}
        try:
            body = await request.json()
        except Exception as ex:  # noqa: BLE001 - a malformed body is a 400
            raise web.HTTPBadRequest(
                text="request body is not valid JSON"
            ) from ex
        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="request body must be a JSON object")
        return body

    def job_runs_payload(self, name: str) -> Optional[Dict[str, Any]]:
        """Retained run history + stats for one job, or ``None`` if unknown.

        Behind ``GET /jobs/{name}/runs`` and MCP ``cron_list_runs``.
        """
        if name not in self.cron_jobs:
            return None
        runs = list(self.run_history.get(name) or [])
        return {
            "name": name,
            "runs": [r.to_dict() for r in runs],  # oldest first
            "stats": _run_stats(runs),
        }

    async def _web_job_runs(self, request: web.Request) -> web.Response:
        assert self.web_config is not None
        name = request.match_info["name"]
        payload = self.job_runs_payload(name)
        if payload is None:
            raise web.HTTPNotFound()
        return _json_response(
            payload, headers=self.web_config.get("headers", None)
        )

    async def _web_job_resources(self, request: web.Request) -> web.Response:
        """Chart-grade CPU/RSS series for one job (monitorResources jobs).

        The heavyweight sibling of the summary numbers that ride /jobs and
        /jobs/{name}/runs: fetched lazily when the dashboard opens a job's
        resource chart, never on the poll loop.  ``live`` carries the
        run-so-far series of each currently-running monitored instance;
        ``runs`` the recorded series of recent finished runs (oldest first,
        rehydrated from the durable ledger across restarts), capped by the
        ``runs`` query parameter.  Both are empty -- and ``monitored`` false
        -- for a job that never opted in, so the dashboard can tell "not
        monitored" from "no data yet".
        """
        assert self.web_config is not None
        name = request.match_info["name"]
        max_runs = self._web_int_query(
            request, "runs", default=20, lo=0, hi=RUN_HISTORY_LIMIT
        )
        payload = self.job_resources_payload(name, max_runs)
        if payload is None:
            raise web.HTTPNotFound()
        return _json_response(
            payload, headers=self.web_config.get("headers", None)
        )

    def job_resources_payload(
        self, name: str, max_runs: int
    ) -> Optional[Dict[str, Any]]:
        """CPU/RSS series for a job's live + recent runs, or ``None``.

        Behind ``GET /jobs/{name}/resources`` and MCP
        ``cron_get_job_resources``.  ``max_runs`` caps the recorded-run tail.
        """
        job = self.cron_jobs.get(name)
        if job is None:
            return None
        live = []
        for runjob in self.running_jobs.get(name) or []:
            series = runjob.live_resource_series()
            snap = runjob.live_resources()
            if series is None and snap is None:
                continue  # unmonitored / not yet sampled
            live.append(
                {
                    "started_at": (
                        runjob.started_at.isoformat()
                        if runjob.started_at is not None
                        else None
                    ),
                    "pid": (
                        runjob.proc.pid if runjob.proc is not None else None
                    ),
                    "current": snap,
                    "series": series or [],
                }
            )
        history = list(self.run_history.get(name) or [])
        monitored = [r for r in history if r.resource_usage is not None]
        runs = [
            r.to_dict(include_series=True)
            for r in monitored[len(monitored) - max_runs :]
        ]
        return {
            "name": name,
            "monitored": bool(job.monitorResources),
            "interval": job.monitorResourcesInterval,
            "live": live,
            "runs": runs,
        }

    async def _web_job_trends(self, request: web.Request) -> web.Response:
        """SLA trend aggregates over the durable run ledger.

        The long-horizon sibling of ``/jobs/{name}/runs``: the same stats
        shape (:func:`_run_stats`), computed per :data:`TREND_WINDOWS`
        window (plus ``all``) over the ledger, which survives restarts and
        -- on a shared mount -- merges every node's runs.  Bounded by the
        store's ``maxRunsPerJob`` retention.  Degrades to the in-memory
        history (``source: memory``) without a healthy backend, so the
        endpoint always answers.
        """
        assert self.web_config is not None
        name = request.match_info["name"]
        payload = await self.job_trends_payload(name)
        if payload is None:
            raise web.HTTPNotFound()
        return _json_response(
            payload, headers=self.web_config.get("headers", None)
        )

    async def job_trends_payload(self, name: str) -> Optional[Dict[str, Any]]:
        """SLA trend aggregates over the durable run ledger, or ``None``.

        Behind ``GET /jobs/{name}/trends`` and MCP ``cron_get_job_trends``
        (both routes share this method, so the executor offload below lives
        in exactly one place).  Only the backend read is awaited on the
        loop; the record parse and window aggregation (up to
        :data:`TREND_SCAN_LIMIT` records, pure CPU) run on the default
        executor over immutable snapshots, like the schedule
        pressure/suggest/calendar builders, so a dashboard poll cannot
        stall job dispatch.
        """
        if name not in self.cron_jobs:
            return None
        loop = asyncio.get_running_loop()
        cached = self._trends_cache.get(name)
        if cached is not None and loop.time() < cached[0]:
            # a recent poll already read and aggregated this job's ledger;
            # serve that within the TTL instead of re-scanning up to
            # TREND_SCAN_LIMIT records again (see JOB_TRENDS_CACHE_TTL).
            return cached[1]
        recs: Optional[List[Dict[str, Any]]] = None
        backend = self.state_backend
        if backend is not None:
            try:
                # newest-first with a cap: an unbounded-retention stream
                # must not hold a backend worker slot for a whole scan on
                # every dashboard poll.
                recs = await asyncio.wait_for(
                    backend.list_records(
                        self._run_stream(name),
                        limit=TREND_SCAN_LIMIT,
                        newest_first=True,
                    ),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception as ex:  # noqa: BLE001 - degrade, never 500
                logger.warning(
                    "state: cannot read the run ledger for trends on %s "
                    "(%s); serving the in-memory history",
                    name,
                    ex,
                )
        # The in-memory fallback reads live state, so its snapshot is taken
        # on the loop; the ledger snapshot above is already ours alone.  The
        # builder then touches nothing but its arguments.
        fallback = (
            list(self.run_history.get(name) or []) if recs is None else None
        )
        payload = await loop.run_in_executor(
            None, partial(self._job_trends_build, name, recs, fallback)
        )
        self._trends_cache[name] = (
            loop.time() + JOB_TRENDS_CACHE_TTL,
            payload,
        )
        return payload

    def _job_trends_build(
        self,
        name: str,
        recs: Optional[List[Dict[str, Any]]],
        fallback: Optional[List[JobRunInfo]],
    ) -> Dict[str, Any]:
        """Parse and aggregate for :meth:`job_trends_payload` (executor side).

        Pure CPU over snapshots captured on the loop: ``recs`` is the
        newest-first ledger listing when the backend read succeeded,
        otherwise ``fallback`` is the in-memory history copy.
        """
        if recs is not None:
            source = "durable"
            infos: List[JobRunInfo] = []
            recs.reverse()  # oldest first, matching _run_stats
            # one shared, already-closed output stream for every rehydrated
            # record: the trends aggregation never reads output, so the
            # per-record buffer allocation would be pure waste at
            # TREND_SCAN_LIMIT scale.
            empty = JobOutputStream()
            empty.closed = True
            for rec in recs:
                restored = _job_run_info_from_dict(rec, output=empty)
                if restored is not None:
                    infos.append(restored)
        else:
            source = "memory"
            infos = fallback or []
        now = get_now(datetime.timezone.utc)
        # One pass instead of one filter pass per window: each info's age is
        # computed once and the info appended to every window it fits, which
        # reproduces the per-window filters exactly (same members, same
        # relative order).  Bucketing rather than a bisect slice because the
        # listing's append order is not provably finished_at order: run
        # records persist fire-and-forget, a shared mount interleaves other
        # nodes' appends, and a crash-reconciled record carries a past
        # interruption instant but lands in the stream at boot.
        window_runs: Dict[str, List[JobRunInfo]] = {
            label: [] for label, _ in TREND_WINDOWS
        }
        for info in infos:
            age = (now - info.finished_at).total_seconds()
            for label, seconds in TREND_WINDOWS:
                if age <= seconds:
                    window_runs[label].append(info)
        windows = {
            label: _run_stats(runs) for label, runs in window_runs.items()
        }
        windows["all"] = _run_stats(infos)
        return {
            "name": name,
            "source": source,
            "generated_at": now.isoformat(),
            "windows": windows,
        }

    def _job_output(self, name: str) -> Optional[JobOutputStream]:
        # the live output of the most recent running instance, else the last
        # finished run's retained output, else nothing captured yet.
        running = self.running_jobs.get(name) or []
        if running:
            return running[-1].output
        last = self.last_run.get(name)
        return last.output if last is not None else None

    def _dag_task_output(
        self, dag_name: str, run_key: str, taskkey: str
    ) -> Optional[JobOutputStream]:
        """The live output stream of a DAG task instance, or ``None``.

        A DAG task runs as a :class:`RunningJob` under the template name
        ``<dag>.<task_id>`` (its instances share that key), so locate the one
        whose ``dag_ref`` matches this run + instance key.  Only a *currently
        running* instance has a reachable buffer -- a finished DAG task's
        output is not retained under the template name (its completion routes
        to the DAG driver, not the per-job last_run), so this returns ``None``
        once the task is done.
        """
        # the base task id: a mapped instance key is ``id#<index>``.
        task_id = taskkey.split("#", 1)[0]
        template_name = "{}.{}".format(dag_name, task_id)
        for running in self.running_jobs.get(template_name, []) or []:
            dref = getattr(running, "dag_ref", None)
            if (
                dref is not None
                and dref.run_key == run_key
                and dref.taskkey == taskkey
            ):
                return running.output
        return None

    def _sse_headers(self) -> Dict[str, str]:
        assert self.web_config is not None
        headers = {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            # tell reverse proxies (nginx) not to buffer the event stream
            "X-Accel-Buffering": "no",
        }
        custom = self.web_config.get("headers", None)
        if custom:
            headers.update(custom)
        return headers

    async def _pump_output(
        self, resp: web.StreamResponse, output: JobOutputStream
    ) -> None:
        """Replay the retained buffer then live-tail an output stream over SSE.

        Shared by the job- and DAG-task log endpoints.  The response must
        already be prepared.
        """
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

    @staticmethod
    def _tail_payload(
        output: Optional[JobOutputStream],
        tail: int,
        cursor: Optional[int],
    ) -> Dict[str, Any]:
        """Poll-friendly snapshot of a retained output buffer.

        The non-streaming counterpart of :meth:`_pump_output`, backing the MCP
        log-tail tools (a client polls with the returned ``cursor`` to fetch
        only newly appended lines).  Without a ``cursor`` returns the last
        ``tail`` lines; with one, the lines after that offset (capped at
        ``tail``).  ``cursor`` is a line offset into the bounded in-memory
        buffer, so after the buffer rotates it can only ever skip forward,
        never resurrect dropped lines.
        """
        lines = list(output.lines) if output is not None else []
        total = len(lines)
        if cursor is None:
            start = max(0, total - tail)
        else:
            start = min(max(cursor, 0), total)
        selected = lines[start : start + tail]
        return {
            "lines": [
                {"stream": stream, "line": line} for stream, line in selected
            ],
            "cursor": start + len(selected),
            # older retained lines exist above what we returned (only
            # meaningful on a cursor-less "give me the tail" call).
            "truncated": cursor is None and start > 0,
        }

    def job_logs_tail_payload(
        self, name: str, tail: int = 100, cursor: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """Last retained log lines of a job, or ``None`` if unknown.

        Behind MCP ``cron_tail_job_logs`` -- the poll/cursor projection of the
        SSE stream at ``GET /jobs/{name}/logs``.
        """
        if name not in self.cron_jobs:
            return None
        payload = self._tail_payload(self._job_output(name), tail, cursor)
        payload["name"] = name
        payload["running"] = bool(self.running_jobs.get(name))
        return payload

    def dag_task_logs_tail_payload(
        self,
        dag: str,
        run_key: str,
        taskkey: str,
        tail: int = 100,
        cursor: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """Last retained log lines of a running DAG task instance, or ``None``.

        Behind MCP ``cron_tail_dag_task_logs``.  Only a currently-running
        instance has a reachable buffer (a finished task's output is not
        retained under the template name), so ``lines`` is empty once it ends.
        """
        if dag not in self.cron_dags:
            return None
        output = self._dag_task_output(dag, run_key, taskkey)
        payload = self._tail_payload(output, tail, cursor)
        payload["dag"] = dag
        payload["run_key"] = run_key
        payload["taskkey"] = taskkey
        return payload

    async def _web_job_logs(self, request: web.Request) -> web.StreamResponse:
        assert self.web_config is not None
        name = request.match_info["name"]
        if name not in self.cron_jobs:
            raise web.HTTPNotFound()

        resp = web.StreamResponse(headers=self._sse_headers())
        await resp.prepare(request)

        output = self._job_output(name)
        if output is None:
            await resp.write(b'event: end\ndata: {"reason": "no-output"}\n\n')
            return resp
        await self._pump_output(resp, output)
        return resp

    async def _web_dag_task_logs(
        self, request: web.Request
    ) -> web.StreamResponse:
        """Live-tail a DAG task instance's stdout/stderr over SSE.

        Serves the LIVE output of a currently-running task instance (a
        finished instance's buffer is not retained); the dashboard shows
        "no live output" otherwise.
        """
        assert self.web_config is not None
        name = request.match_info["name"]
        run_key = request.match_info["run_key"]
        taskkey = request.match_info["taskkey"]
        if name not in self.cron_dags:
            raise web.HTTPNotFound()

        resp = web.StreamResponse(headers=self._sse_headers())
        await resp.prepare(request)

        output = self._dag_task_output(name, run_key, taskkey)
        if output is None:
            await resp.write(b'event: end\ndata: {"reason": "no-output"}\n\n')
            return resp
        await self._pump_output(resp, output)
        return resp

    def _web_restart_reason(
        self,
        web_config: Optional[WebConfig],
        mcp_config: Optional[MCPConfig],
    ) -> Optional[str]:
        """Why the running web app must be torn down, or None to keep it.

        The same reason-string triage :meth:`start_stop_cluster` uses, for the
        same reasons: three distinguishable causes, one of which (a
        certificate rotation) is gated on the new material actually loading.
        """
        if web_config is None or web_config != self.web_config:
            reason = "configuration changed"
        # an mcp-only change (e.g. readOnly flipped, a toolset added) must
        # also restart the app: the /mcp route set is fixed at build time,
        # so it only picks up config through a fresh routes list.
        elif mcp_config != self.mcp_config:
            reason = "mcp configuration changed"
        # an in-place certificate rotation leaves the config bytes identical
        # but the on-disk material new; without this the listener keeps
        # serving the old certificate until it expires.
        elif self._web_tls_files_changed():
            reason = "TLS certificate files changed"
        else:
            return None
        if web_config is not None and not tlsutil.listener_tls_loadable(
            web_config.get("tls")
        ):
            # Make-before-break is infeasible here for the same reason it is
            # for gossip: the new runner binds the same port / socket path the
            # old one still holds. So only proceed once the NEW material
            # loads. A half-written rotation (cert-manager, Vault and
            # Kubernetes secret refreshes are not atomic across the files), or
            # a config edit racing one, would otherwise tear down a working
            # listener and then fail to rebuild, leaving nothing serving until
            # the next reload.
            logger.warning(
                "web: new TLS material is not yet loadable (a "
                "partial/half-written rotation, or a config edit racing "
                "one?); keeping the running listener and retrying next reload"
            )
            return None
        return reason

    def _build_web_tls(
        self, web_tls: Optional[Dict[str, Any]]
    ) -> "tuple[Optional[ssl.SSLContext], Optional[Dict[str, Any]], bool]":
        """``(context, file signature, failed)`` for a listener about to start.

        On failure the caller must NOT fall back to a plaintext listener and
        must NOT latch ``web_config``: leaving it unchanged is exactly what
        makes the next reload retry, instead of concluding "nothing changed"
        and never trying again.
        """
        # Callers gate on tlsutil.listener_tls_configured, so cert and key
        # are present here; the Optional is only so the guarded call site
        # type-checks.
        assert web_tls is not None
        # Snapshot the files BEFORE loading them: a rotation landing in the
        # gap then compares unequal on the next reload, which is a spurious
        # restart rather than a missed one. (cluster.py latches AFTER its
        # load; this order is the safer one.)
        signature = tlsutil.tls_file_signature(
            web_tls, tlsutil.LISTENER_TLS_KEYS
        )
        try:
            context = tlsutil.build_listener_ssl_context(
                web_tls["cert"],
                web_tls["key"],
                client_ca=web_tls.get("clientCa"),
            )
        except (OSError, ssl.SSLError) as ex:
            logger.error(
                "web: TLS material is not loadable, so the web API is not "
                "starting (retrying on the next config reload): %s",
                ex,
            )
            return None, None, True
        return context, signature, False

    async def start_stop_web_app(
        self,
        web_config: Optional[WebConfig],
        mcp_config: Optional[MCPConfig] = None,
    ):
        if self.web_runner is not None:
            reason = self._web_restart_reason(web_config, mcp_config)
            if reason is not None:
                logger.info("web: %s, stopping http server", reason)
                await self.web_runner.cleanup()
                self.web_runner = None
                self._web_tls_signature = None

        # Build the listener's TLS context ONCE per (re)start, before anything
        # is bound, so a context failure never leaves a half-built runner.
        # Guarded on a start actually being about to happen: a healthy running
        # listener must not re-read and re-parse the PEMs every housekeeping
        # tick.
        start_wanted = bool(
            web_config is not None
            and web_config["listen"]
            and self.web_runner is None
        )
        web_tls = web_config.get("tls") if web_config is not None else None
        # listener_tls_configured, not a bare truthiness test on the dict: a
        # `tls:` block whose values are blank parses to a truthy dict of
        # Nones, and building a context from that would raise a TypeError
        # rather than the OSError the failure path expects. Such a block also
        # cannot coexist with an https:// listener (config validation refuses
        # that pair), so skipping it here serves the plaintext listeners the
        # config actually asks for.
        tls_context, tls_signature, tls_failed = (
            self._build_web_tls(web_tls)
            if start_wanted and tlsutil.listener_tls_configured(web_tls)
            else (None, None, False)
        )

        # `web_config is not None` is implied by start_wanted; it is repeated
        # so the narrowing survives for the whole start branch below.
        if start_wanted and not tls_failed and web_config is not None:
            ui_enabled = web_config.get("ui", True)
            metrics_config = resolve_metrics_config(web_config)
            middlewares = []
            # Cross-site request defense for the mutating endpoints, ALWAYS
            # installed -- with authToken unset this is the only thing between
            # a localhost-bound daemon and any web page the operator visits
            # (see _make_origin_middleware). Outermost, so a foreign page is
            # refused before auth or handlers run. An operator who explicitly
            # published the API cross-origin via a wildcard
            # Access-Control-Allow-Origin response header keeps that (loudly);
            # a specific ACAO origin is folded into the allow-list so an
            # existing deliberate cross-origin dashboard survives the upgrade.
            allowed_origins = set(web_config.get("allowedOrigins") or [])
            custom_headers = web_config.get("headers") or {}
            acao = next(
                (
                    value
                    for key, value in custom_headers.items()
                    if key.lower() == "access-control-allow-origin"
                ),
                None,
            )
            if acao == "*":
                logger.warning(
                    "web: headers set Access-Control-Allow-Origin: '*'; "
                    "honouring it and NOT enforcing the cross-site Origin "
                    "check on mutating endpoints"
                )
            else:
                if acao:
                    allowed_origins.add(acao)
                middlewares.append(
                    self._make_origin_middleware(frozenset(allowed_origins))
                )
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
                web.get("/node", self._web_get_node),
                web.get("/node/history", self._web_node_history),
                web.get("/status", self._web_get_status),
                web.get("/schedule/preview", self._web_schedule_preview),
                web.get("/schedule/pressure", self._web_schedule_pressure),
                web.get("/schedule/duplicates", self._web_schedule_duplicates),
                web.get("/schedule/suggest", self._web_schedule_suggest),
                web.get("/schedule/why", self._web_schedule_why),
                web.get("/calendar.ics", self._web_calendar),
                web.get("/jobs", self._web_list_jobs),
                web.get("/jobs/{name}/runs", self._web_job_runs),
                web.get("/jobs/{name}/calendar.ics", self._web_job_calendar),
                web.get("/jobs/{name}/resources", self._web_job_resources),
                web.get("/jobs/{name}/trends", self._web_job_trends),
                web.post("/jobs/{name}/start", self._web_start_job),
                web.post("/jobs/{name}/cancel", self._web_cancel_job),
                web.post("/jobs/{name}/pause", self._web_pause_job),
                web.post("/jobs/{name}/resume", self._web_resume_job),
                web.get("/jobs/{name}/logs", self._web_job_logs),
                # DAG introspection + control
                web.get("/dags", self._web_list_dags),
                web.get("/dags/{name}/runs", self._web_dag_runs),
                web.get("/dags/{name}/runs/{run_key}", self._web_dag_run),
                web.get(
                    "/dags/{name}/runs/{run_key}/xcom", self._web_dag_xcom
                ),
                web.get(
                    "/dags/{name}/runs/{run_key}/tasks/{taskkey}/logs",
                    self._web_dag_task_logs,
                ),
                web.post("/dags/{name}/trigger", self._web_dag_trigger),
                web.post("/dags/{name}/backfill", self._web_dag_backfill),
                web.post(
                    "/dags/{name}/runs/{run_key}/tasks/{taskkey}/decision",
                    self._web_dag_decision,
                ),
                # durable state inspector (metadata-only)
                web.get("/state", self._web_state),
                web.get("/state/documents", self._web_state_documents),
                web.get("/state/records", self._web_state_records),
            ]
            # The MCP server (POST /mcp) rides these same listeners and the
            # auth middleware above -- /mcp is NEVER added to `public`, so it
            # inherits the bearer-token gate. Built here (not in __init__) so a
            # reload rebuilds it against the current config. The fail-closed
            # no-token-on-a-routable-listener check ran at parse time
            # (config._validate_mcp_config); this only wires it up.
            self._mcp = None
            if mcp_config is not None and mcp_config.get("enabled"):
                from cronstable.mcp import MCPHandler

                self._mcp = MCPHandler(self, mcp_config)
                routes.append(web.post("/mcp", self._mcp.handle_http))
                routes.append(web.get("/mcp", self._mcp.handle_http_get))
                routes.append(web.options("/mcp", self._mcp.handle_options))
                logger.info("mcp: serving the MCP endpoint at POST /mcp")
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
                    site = web_site_from_url(
                        self.web_runner, addr, tls_context
                    )
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
            self.mcp_config = mcp_config
            self._web_tls_signature = tls_signature

        # Node history sampling follows the web API's lifecycle: the ring
        # only feeds the dashboard's node chart, so it runs whenever the web
        # app does (subject to web.nodeHistory) and stops with it. Both
        # branches are idempotent: start_history no-ops when the running
        # task already matches the config, stop_history when there is none.
        history_config = (
            resolve_node_history_config(web_config)
            if web_config is not None and self.web_runner is not None
            else None
        )
        if history_config is not None:
            self._node_sampler.start_history(
                interval=history_config["interval"],
                points=history_config["points"],
            )
        else:
            await self._node_sampler.stop_history()

    @staticmethod
    def _election_relevant(cluster_config: ClusterConfig) -> Dict[str, Any]:
        """The cluster config minus its observability-only keys.

        ``shareNodeStats`` and ``observabilityMesh`` are resolved onto the
        same ClusterConfig dict (see
        :func:`cronstable.config._attach_observability`) but are
        election-inert:
        they feed the overlay lifecycle (:meth:`start_stop_observability`) and
        the share-flag reconciliation in :meth:`start_stop_cluster`, never the
        election manager's behavior. Restarting the manager on a difference in
        them would, on a lease backend, drop the leadership lease and pause
        Leader jobs fleet-wide for an edit that changes nothing about
        election -- so the restart comparison strips them from both sides.
        """
        return {
            key: value
            for key, value in cluster_config.items()
            if key not in ("shareNodeStats", "observabilityMesh")
        }

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
            # observability-only edits (shareNodeStats / observabilityMesh)
            # are stripped from the comparison: they never require an election
            # restart (see _election_relevant); the overlay lifecycle and the
            # share-flag reconciliation below pick them up instead.
            if cluster_config is None or self._election_relevant(
                cluster_config
            ) != self._election_relevant(mgr.config):
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
            from cronstable.cluster import gossip_tls_loadable

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
        if cluster_config is not None and self.cluster_manager is not None:
            # The manager was KEPT across this reload (only observability
            # keys -- or nothing -- changed), but it latched the node-stats
            # share flag once, at whichever set_node_stats_provider call it
            # last saw. Re-reconcile it to the NEW config unconditionally, or
            # a shareNodeStats toggle would never reach a running gossip
            # election mesh: off would keep gossiping CPU/memory until some
            # unrelated restart, on would never start. Safe on a running
            # manager -- the call only reassigns the provider and flag, picked
            # up on the next /peer round -- and a no-op on the lease backends
            # (their seam default ignores it). Same share expression as the
            # build path below.
            self.cluster_manager.set_node_stats_provider(
                self.node_resource_snapshot,
                share=bool(cluster_config.get("shareNodeStats"))
                and cluster_config.get("observabilityMesh") is None,
            )
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
                # Always install the node-stats provider so a gossip cluster
                # shows THIS node's own load in its /cluster + /fleet self
                # readouts (local, free); `share` gates whether we ALSO gossip
                # it to peers -- on only when observability is enabled with
                # backend: gossip (no separate overlay mesh; the lease+overlay
                # case installs on the overlay in start_stop_observability). A
                # no-op on the lease backends (their seam default ignores it).
                manager.set_node_stats_provider(
                    self.node_resource_snapshot,
                    share=bool(cluster_config.get("shareNodeStats"))
                    and cluster_config.get("observabilityMesh") is None,
                )
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

    def node_resource_snapshot(self) -> Optional[Dict[str, Any]]:
        """This node's live CPU/memory for gossip and GET /node.

        The callable installed as the gossip node-stats provider; also used by
        the /node endpoint. Best-effort: returns None when psutil is
        unavailable.
        """
        return self._node_sampler.snapshot()

    def _fleet_backend(self) -> Optional[LeadershipBackend]:
        """The backend that answers the fleet view / carries fleet gossip.

        The observability overlay mesh when one is running (a lease cluster
        that opted into ``cluster.observability``), else the leadership backend
        itself -- which provides the fleet view directly under ``backend:
        gossip`` and returns ``None`` for the lease backends (no fleet).
        """
        return (
            self.observability_mesh
            if self.observability_mesh is not None
            else self.cluster_manager
        )

    async def start_stop_observability(
        self, cluster_config: Optional[ClusterConfig]
    ) -> None:
        """(Re)build the gossip observability overlay to match the config.

        The overlay is a SECOND, election-inert gossip manager that a lease
        cluster (kubernetes/etcd/filesystem) stands up purely to exchange fleet
        data -- per-node CPU/memory and job summaries -- since a lease backend
        has no node-to-node channel of its own.  It is built from the resolved
        ``observabilityMesh`` config (see
        :func:`cronstable.config._attach_observability`); ``None`` there means
        no overlay is wanted (the section is absent, or ``backend: gossip``
        already carries the data on the election mesh, handled in
        :meth:`start_stop_cluster`).

        Mirrors the rebuild logic of :meth:`start_stop_cluster` but simpler:
        the overlay never elects, so there is no leadership/quorum transition
        to log.  Like the election manager it is rebuilt on a config change or
        an in-place TLS cert rotation, and a start failure is logged and
        swallowed so a misconfigured overlay never stops jobs from running.
        """
        mesh_config = (
            cluster_config.get("observabilityMesh")
            if cluster_config is not None
            else None
        )
        mesh = self.observability_mesh
        if mesh is not None:
            if mesh_config is None or mesh_config != mesh.config:
                reason = "configuration changed"
            elif mesh.tls_files_changed():
                reason = "TLS certificate files changed"
            else:
                reason = None
            # make-before-break is infeasible for gossip (same listen port), so
            # a cert rotation only tears down once the NEW material loads --
            # otherwise keep the running overlay serving the valid old cert.
            if reason is not None and mesh_config is not None:
                from cronstable.cluster import gossip_tls_loadable

                if not gossip_tls_loadable(mesh_config):
                    logger.warning(
                        "cluster.observability: new TLS material is not yet "
                        "loadable (a partial rotation?); keeping the running "
                        "overlay and retrying next reload"
                    )
                    reason = None
            if reason is not None:
                logger.info("cluster.observability: %s, stopping", reason)
                await mesh.stop()
                self.observability_mesh = None
        if mesh_config is not None and self.observability_mesh is not None:
            # The overlay was KEPT across this reload -- and shareNodeStats
            # lives on the CLUSTER config, not on the resolved mesh config the
            # keep/rebuild comparison above sees, so a toggle always lands
            # here. The mesh latched the flag at its last
            # set_node_stats_provider call, so re-reconcile it to the new
            # config unconditionally or a toggle off keeps gossiping
            # CPU/memory until an unrelated restart and a toggle on never
            # starts. Safe on a running mesh: the call only reassigns the
            # provider and flag, picked up on the next /peer round. Same share
            # expression as the build path below.
            self.observability_mesh.set_node_stats_provider(
                self.node_resource_snapshot,
                share=bool(
                    cluster_config is not None
                    and cluster_config.get("shareNodeStats")
                ),
            )
        if mesh_config is not None and self.observability_mesh is None:
            try:
                mgr = make_backend(mesh_config, self.job_set_id)
                # fleet providers BEFORE start(): its first poll round may race
                # peers polling us back, and their first absorbed snapshot
                # should already carry our jobs + load, not an empty block.
                mgr.set_job_summaries_provider(self.fleet_job_summaries)
                # always install (so the overlay's own /fleet self readout
                # shows this node's load); share gates gossiping it to peers.
                mgr.set_node_stats_provider(
                    self.node_resource_snapshot,
                    share=bool(
                        cluster_config is not None
                        and cluster_config.get("shareNodeStats")
                    ),
                )
                await mgr.start()
            except (
                OSError,
                ssl.SSLError,
                ValueError,
                ConfigError,
                aiohttp.ClientError,
                asyncio.TimeoutError,
            ) as ex:
                # same swallow-and-keep-running contract as the election
                # backend: a bad overlay cert/listen/peer must not stop jobs.
                logger.error("cluster.observability: failed to start: %s", ex)
                return
            self.observability_mesh = mgr

    async def start_stop_state(
        self, state_config: Optional[StateConfig]
    ) -> None:
        """(Re)build the durable state backend to match the config.

        Mirrors :meth:`start_stop_cluster` but simpler: the store backend has
        no election or convergence to reason about, and is rebuilt only when
        the ``state`` section is added, removed, or changed (the backend
        tracks the job-set id itself via ``self.job_set_id``, so an ordinary
        reload that only edits jobs does not disturb it).  A start failure --
        an unwritable path, a bad mount -- is logged and swallowed, exactly
        like a cluster start failure, so durability being misconfigured never
        stops cronstable from running jobs in memory.

        The loopback job API in front of the backend *can* serve TLS (an
        off-host ``state.jobApi.listen`` over ``https://``); an in-place
        rotation of its certificate is picked up by restarting just that
        listener, without disturbing the backend or its leases (see
        :meth:`_maybe_restart_job_api_for_tls`).
        """
        self._state_configured = state_config is not None
        if state_config is not None:
            self._state_on_unavailable = str(
                state_config.get("onStoreUnavailable") or "degrade"
            )
            self._state_gc_grace = float(
                state_config.get("gcGraceSeconds") or 0
            )
            self._slot_ttl = float(state_config.get("slotTtlSeconds") or 30)
        else:
            self._state_on_unavailable = "degrade"
            self._state_gc_grace = 0.0
            self._slot_ttl = 30.0
        backend = self.state_backend
        if backend is not None and (
            state_config is None or state_config != backend.config
        ):
            logger.info("state: configuration changed, stopping")
            # the pause refresh reads the OLD store through a local backend
            # binding it captured before the swap: left to finish, it
            # re-installs that store's pauses into the gate map on top of
            # whatever the new store's rehydrate just resolved. Cancel it
            # BEFORE the first await of this teardown: both stop() and
            # _stop_job_api() yield (with a keep-alive connection open the
            # latter spans several loop turns), and a pass whose store read
            # resolves in that window runs its whole mutation loop against
            # the abandoned store before any later cancel is reached. The
            # new store's rehydrate does not undo it either: that pass
            # walks only the streams that exist in the NEW store, so a job
            # with no paused/ stream there stays gated until the dead
            # window's own `until` elapses.
            if self._pause_refresh_task is not None:
                self._pause_refresh_task.cancel()
                self._pause_refresh_task = None
            await backend.stop()
            self.state_backend = None
            # the loopback job-state API belongs to this backend generation
            # (its per-run tokens and staged secrets are meaningless against a
            # different store): stop it here, and a replacement is started
            # below if the new config still wants one.
            await self._stop_job_api()
            # a replacement backend (different path/namespace) serves a
            # different store: let it warm the dashboard history for jobs
            # that have none in memory yet, instead of serving the old
            # store's history forever.
            self._state_rehydrated = False
            # the concurrency slots live in the OLD store: drop the held
            # leases (they lapse there by TTL) and stop their renewers and
            # any Replace pursuits -- re-claiming in the new store is the
            # next launch's business. The lock-fidelity verdict is also
            # per-store.
            for task in list(self._slot_renewers.values()):
                task.cancel()
            self._slot_renewers.clear()
            self._slot_leases.clear()
            for task in list(self._slot_pursuits.values()):
                task.cancel()
            self._slot_pursuits.clear()
            self._slot_fidelity = None
            if self._retry_claim_task is not None:
                self._retry_claim_task.cancel()
                self._retry_claim_task = None
            # the DAG advance leases and next-fire index also belong
            # to the old store; drop them (renewers cancelled, leases lapse by
            # TTL) so the new store's active runs are re-adopted from scratch
            # by reconcile_on_boot (re-run because _state_rehydrated cleared).
            self._dag.forget()
        elif backend is not None and state_config is not None:
            # config byte-identical, so the teardown above did not fire: the
            # store backend stays, but the job API's TLS certificate may have
            # rotated in place under it. Cheap stat-compare once per pass.
            await self._maybe_restart_job_api_for_tls(state_config)
        if state_config is not None and self.state_backend is None:
            try:
                # Construct INSIDE the try: building the backend resolves and
                # creates the store directories and runs a write probe, any of
                # which can raise OSError on a bad/unwritable path or mount.
                # BOUNDED: on a hard-hung mount (dead NFS server) the probe's
                # syscalls block uninterruptibly on the worker thread, and an
                # unbounded await here would stall run() before it ever
                # schedules a job.  Timing out degrades to the in-memory path
                # and retries on the next housekeeping pass, exactly like the
                # OSError branch.
                backend = make_state_backend(state_config, self.job_set_id)
                await asyncio.wait_for(
                    backend.start(), timeout=STATE_OP_TIMEOUT
                )
            except (OSError, ConfigError, asyncio.TimeoutError) as ex:
                # an operational misconfiguration (unwritable path, bad mount)
                # to log and keep running through, not the run loop's generic
                # "report a bug" path.
                logger.error(
                    "state: failed to start: %s",
                    str(ex) or type(ex).__name__,
                )
                return
            self.state_backend = backend
            self._state_max_runs = state_config.get("maxRunsPerJob", 0)
            # a fresh backend generation re-anchors the periodic chores:
            # record this node's manifest immediately (the GC anchor), and
            # let the first GC pass run on the next housekeeping tick --
            # gcGraceSeconds is what protects young state, not a delay here.
            self._manifest_next = 0.0
            self._gc_next = 0.0
            # land any pause/resume taken while the store was down BEFORE
            # the rehydrate below re-reads the paused/ streams, so it cannot
            # revert those jobs to the records they supersede.
            self._replay_pending_pause_writes()
            # warm the in-memory history from the ledger the first time a
            # backend comes up, so a restart's dashboard/status is populated at
            # once instead of blank until each job next runs.
            await self._rehydrate_from_state()
            # expose this backend to job commands over a loopback
            # endpoint (opt-out via state.jobApi.enabled). A start failure is
            # logged and swallowed -- the scheduler's own durable features do
            # not depend on it.
            await self._start_job_api(state_config)

    async def _start_job_api(self, state_config: StateConfig) -> None:
        """Stand up the loopback job-state API for this backend, if enabled."""
        job_api_cfg = dict(state_config.get("jobApi") or {})
        if not job_api_cfg.get("enabled", True):
            return
        # lazy import (like state_admin and the lease backends): the module
        # never enters the graph unless a job API is actually configured.
        from cronstable.jobapi import JobStateAPI

        # Snapshot the TLS files BEFORE api.start() loads them, mirroring
        # _build_web_tls: a rotation landing in the gap then compares unequal
        # on the next pass, which is a spurious restart (the safe direction),
        # not a missed one. None for a plaintext endpoint (no cert/key), which
        # therefore never triggers a rotation restart.
        job_api_tls = job_api_cfg.get("tls")
        tls_signature: Optional[Dict[str, Any]] = None
        if tlsutil.listener_tls_configured(job_api_tls):
            # listener_tls_configured is truthy only when cert and key are
            # present, so the block is non-None here; the assert is only so
            # the call type-checks (it is not a TypeGuard).
            assert job_api_tls is not None
            tls_signature = tlsutil.tls_file_signature(
                job_api_tls, tlsutil.JOB_API_TLS_KEYS
            )
        api = JobStateAPI(
            lambda: self.state_backend,
            host=self._state_host,
            base_holder=self._slot_holder(),
            config=job_api_cfg,
        )
        try:
            await asyncio.wait_for(api.start(), timeout=STATE_OP_TIMEOUT)
        except (OSError, asyncio.TimeoutError) as ex:
            logger.error(
                "state: job API failed to start (jobs will run without the "
                "loopback state endpoint): %s",
                str(ex) or type(ex).__name__,
            )
            return
        self._job_api = api
        self._job_api_tls_signature = tls_signature

    async def _stop_job_api(self) -> None:
        api = self._job_api
        if api is None:
            return
        self._job_api = None
        self._job_api_tls_signature = None
        try:
            await asyncio.wait_for(api.stop(), timeout=STATE_OP_TIMEOUT)
        except (OSError, asyncio.TimeoutError) as ex:
            logger.warning("state: job API did not stop cleanly: %s", ex)

    def _job_api_tls_files_changed(
        self, tls: Optional[Dict[str, Any]]
    ) -> bool:
        """Whether the running job API's TLS files differ from what it loaded.

        The job-state analogue of :meth:`_web_tls_files_changed`, and the only
        thing that makes an in-place certificate rotation of the job API
        listener visible: the state config is byte-identical across such a
        rotation, so the ``state_config != backend.config`` gate never fires
        for it.

        Only the server material is watched, via
        :data:`cronstable.tlsutil.JOB_API_TLS_KEYS` (``cert``/``key``): the
        ``ca`` is handed to jobs as a path and read fresh by each one, so
        nothing the daemon holds goes stale when it rotates.

        Gated on ``_job_api_tls_signature`` (``None`` for a plaintext
        endpoint, which therefore never restarts on this) rather than on the
        config, so a loopback endpoint is untouched.
        """
        if self._job_api_tls_signature is None or not tls:
            return False
        return (
            tlsutil.tls_file_signature(tls, tlsutil.JOB_API_TLS_KEYS)
            != self._job_api_tls_signature
        )

    async def _maybe_restart_job_api_for_tls(
        self, state_config: StateConfig
    ) -> None:
        """Restart the job API listener if its TLS cert/key rotated in place.

        Called on an otherwise no-op reload (state config byte-identical). The
        web-app analogue is the TLS arm of :meth:`_web_restart_reason`; this
        is lighter because only the listener is rebuilt: the store backend,
        its leases and its renewers are untouched.

        Restarting is gated on the new material actually loading. A
        half-written rotation (cert-manager, Vault and Kubernetes secret
        refreshes are not atomic across the files), or a config edit racing
        one, would otherwise tear a working endpoint down and then fail to
        rebuild it, leaving jobs with no state endpoint until the next reload;
        keeping the old listener up and retrying is the safe direction.

        The loadability gate covers the material; the rebind itself is not
        pre-validated (make-before-break is infeasible, since the new listener
        wants the same port the old one holds). A rebind that fails leaves the
        endpoint down with the error :meth:`_start_job_api` logs, the same
        degraded state as a failed initial start, until the ``state`` config
        next changes.
        """
        tls = (state_config.get("jobApi") or {}).get("tls")
        if self._job_api is None or not self._job_api_tls_files_changed(tls):
            return
        if not tlsutil.listener_tls_loadable(tls):
            logger.warning(
                "state: new job API TLS material is not yet loadable (a "
                "partial/half-written rotation, or a config edit racing "
                "one?); keeping the running endpoint and retrying next reload"
            )
            return
        logger.info(
            "state: job API TLS certificate files changed, restarting the "
            "loopback state endpoint"
        )
        await self._stop_job_api()
        await self._start_job_api(state_config)

    def _track_state_write(
        self, coro: Coroutine[Any, Any, None]
    ) -> asyncio.Task:
        """Run a durable-state write as a tracked fire-and-forget task.

        The single scheduling idiom for every durable write: tracked in
        ``_pending_state_writes`` so it is not GC'd mid-flight and the
        shutdown flush can bound-wait it; never awaited on a scheduling
        path.  The coroutine itself is responsible for catching and logging
        its own failures (they are all best-effort).

        When the tracked set has grown past :data:`MAX_PENDING_STATE_WRITES`
        (a store so slow that writes are not draining), the new write is shed
        rather than tracked: its coroutine is closed, the drop is counted, and
        an already-resolved placeholder task is returned so callers that store
        or chain on the result (``_inflight_write_tail``, the pause-refresh and
        GC/retry tasks) keep working unchanged.  Shedding is safe because
        every state write is best-effort; the alternative is unbounded growth.
        """
        if len(self._pending_state_writes) >= MAX_PENDING_STATE_WRITES:
            coro.close()
            self.metrics.state_write_dropped("overflow")
            task = asyncio.create_task(_noop_state_write())
        else:
            task = asyncio.create_task(coro)
        self._pending_state_writes.add(task)
        task.add_done_callback(self._pending_state_writes.discard)
        return task

    def _state_periodic(self) -> None:
        """Kick off the periodic durable-state chores that are due.

        Called from the housekeeping pass: a pair of loop-clock due-checks
        (cheap) that spawn tracked background tasks (manifest write, GC
        pass).  No-op without a running backend.
        """
        if self.state_backend is None:
            return
        now = asyncio.get_running_loop().time()
        if now >= self._manifest_next:
            self._manifest_next = now + STATE_MANIFEST_INTERVAL
            self._track_state_write(self._persist_manifest())
        if (
            self._state_gc_grace > 0
            and now >= self._gc_next
            and (self._gc_task is None or self._gc_task.done())
        ):
            self._gc_next = now + STATE_GC_INTERVAL
            self._gc_task = self._track_state_write(
                self._collect_state_garbage()
            )
        if self._retry_resume_active() and (
            self._retry_claim_task is None or self._retry_claim_task.done()
        ):
            # cross-node retry resume: scan for claimable foreign ladders
            # about once a minute (the housekeeping cadence).
            self._retry_claim_task = self._track_state_write(
                self._retry_claim_scan()
            )

    def _manifest_stream(self) -> str:
        return MANIFEST_STREAM_PREFIX + self._state_host

    def _artifact_scope_names(self) -> Set[str]:
        """Every artifact scope this config can write beyond its job names.

        The shared scope plus each job's / dag task template's
        stateAllowedScopes: with jobs writing artifacts under their own name
        by default, this is exactly the set of scopes a keep-set cannot
        derive from the job names alone.  Advertised in the manifest and
        folded into the GC keep map so a scope stays alive while any node's
        config still names it.
        """
        from cronstable.jobstate import GLOBAL_SCOPE

        scopes: Set[str] = {GLOBAL_SCOPE}
        for job in self.cron_jobs.values():
            scopes.update(job.stateAllowedScopes)
        for dagcfg in self.cron_dags.values():
            for template in dagcfg.task_templates.values():
                scopes.update(template.stateAllowedScopes)
        return scopes

    async def _persist_manifest(self) -> None:
        """Record this node's loaded job set to its OWN manifest stream.

        The anchor for cross-jobset garbage collection: a job's durable
        streams are garbage only when NO recent manifest -- from any host's
        stream, running any job set, under this deploymentId -- references
        its name. Every node sharing the store contributes its own
        ``manifests/<host>`` stream (see :data:`MANIFEST_STREAM_PREFIX`), so a
        fleet whose members run different job sets never collects each
        other's state, and the retained history never shrinks as the fleet
        grows (each host's own count-based prune is independent of every
        other host's write volume).
        """
        backend = self.state_backend
        if backend is None:
            return
        record = {
            "jobSetId": self.job_set_id(),
            "host": self._state_host,
            "jobs": sorted(self.cron_jobs),
            # what this node's config can WRITE beyond its job names: the
            # shared artifact scopes its jobs/dag tasks may publish under and
            # the dags it runs.  Load-bearing for GC: a keep-set built while
            # any recent manifest lacks these keys cannot prove a peer's
            # artifact scopes or dags absent, so artifact streams (and
            # removed dags' runs) stay unmanaged until the whole fleet
            # advertises them (see _collect_state_garbage).
            "scopes": sorted(self._artifact_scope_names()),
            "dags": sorted(self.cron_dags),
            "at": get_now(datetime.timezone.utc).isoformat(),
        }
        stream = self._manifest_stream()
        try:
            await backend.append_record(
                stream, record, prune_keep=MANIFEST_STREAM_KEEP
            )
        except Exception as ex:  # noqa: BLE001 - best-effort; log, survive
            self.metrics.state_write_dropped("manifest")
            logger.warning("state: failed to record the job manifest: %s", ex)

    async def _live_pause_keep(
        self, backend: StateBackend, names: Set[str], now: datetime.datetime
    ) -> Set[str]:
        """The pause-stream keep-set: kept jobs that hold a LIVE pause.

        Starts from every kept job name and DROPS only those whose
        ``paused/<job>`` stream tops out at a non-live record -- a resume, an
        expired window, or a foreign/corrupt one -- so
        :meth:`StateBackend.collect_garbage` reclaims the dead stream. That
        collection stays grace-gated on the record's OWN age, which
        independently protects a pause a peer wrote moments ago (its fresh
        record is younger than grace), so this need not re-check the race.
        Fail-safe throughout: an unenumerable prefix, an unreadable stream,
        or any doubt keeps the name, so GC never eats a live pause. A job with
        no pause stream never appears in the listing and is simply kept. Reads
        only streams that exist, so once the dead ones are collected the cost
        falls away -- and a later pause re-creates a collected stream, leaving
        cross-node propagation (:meth:`_refresh_pauses_from_store`) intact.
        """
        keep = set(names)
        try:
            streams = await asyncio.wait_for(
                backend.list_stream_names(PAUSE_STREAM_PREFIX),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - keep everything on any doubt
            logger.warning(
                "state: not reclaiming dead pause streams this GC pass: "
                "cannot enumerate them (%s)",
                ex,
            )
            return keep
        for stream in streams:
            name = stream[len(PAUSE_STREAM_PREFIX) :]
            if name not in keep:
                continue  # a removed job's stream: already collected by name
            if name in self._paused:
                # paused right now on THIS node: keep unconditionally, without
                # a read. Covers the window where the pause write is still in
                # flight and the stream's newest durable record is the prior
                # resume -- reading it would drop the stream mid-pause (GC then
                # grace-keeps it only if that stale resume is young enough).
                continue
            try:
                recs = await asyncio.wait_for(
                    backend.list_records(stream, limit=1, newest_first=True),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception as ex:  # noqa: BLE001 - keep this one on doubt
                logger.warning(
                    "state: keeping the pause stream of %s this GC pass "
                    "(cannot read it): %s",
                    name,
                    ex,
                )
                continue
            info = self._pause_info_from_record(recs[0] if recs else None)
            if info is None or info.until <= now:
                # resumed / expired / foreign: no live pause to protect.
                keep.discard(name)
        return keep

    async def _collect_state_garbage(self) -> None:
        """One automatic garbage-collection pass (see state.gcGraceSeconds).

        Builds the keep-set from the union of recent manifests -- read across
        every host's own ``manifests/<host>`` stream, bounded per host -- plus
        this node's own loaded config, so GC still cannot eat live jobs even
        when a manifest stream is unreadable or empty, and hands the deletion
        to the backend.  The same pass manages the ``artifacts/`` streams and
        removed dags' run documents (see :meth:`_gc_dag_state`) and finishes
        by sweeping payload blobs no surviving artifact record references
        (see :meth:`_sweep_orphan_artifact_blobs`).  Every failure degrades
        to "collect nothing this pass".
        """
        backend = self.state_backend
        grace = self._state_gc_grace
        if backend is None or grace <= 0:
            return
        try:
            stream_names = await asyncio.wait_for(
                backend.list_stream_names(MANIFEST_STREAM_PREFIX),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade, never crash
            logger.warning(
                "state: skipping garbage collection: cannot enumerate the "
                "manifest streams (%s)",
                ex,
            )
            return
        # this node's own stream must always be included even if the
        # enumeration above raced its very first write.
        stream_names = sorted(set(stream_names) | {self._manifest_stream()})
        if len(stream_names) > MANIFEST_HOSTS_CAP:
            logger.warning(
                "state: %d manifest streams found, reading only the first "
                "%d this pass (a churning fleet with never-reused host "
                "identities?); the rest are considered this GC pass only "
                "once a run drops the count back under the cap",
                len(stream_names),
                MANIFEST_HOSTS_CAP,
            )
            stream_names = stream_names[:MANIFEST_HOSTS_CAP]
        manifests: List[Dict[str, Any]] = []
        try:
            for name in stream_names:
                manifests.extend(
                    await asyncio.wait_for(
                        backend.list_records(
                            name,
                            limit=MANIFEST_STREAM_KEEP,
                            newest_first=True,
                        ),
                        timeout=STATE_OP_TIMEOUT,
                    )
                )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade, never crash
            logger.warning(
                "state: skipping garbage collection: cannot read the "
                "manifest streams (%s)",
                ex,
            )
            return
        now = get_now(datetime.timezone.utc)
        # The anchor must be able to PROVE absence before anything is
        # deleted: unless the manifest history reaches back at least one
        # full grace window, a job could be missing from every manifest
        # simply because nobody had recorded manifests yet (a fresh store,
        # or the first pass after upgrading a pre-manifest store) -- defer
        # rather than collect with zero effective grace.
        oldest: Optional[datetime.datetime] = None
        for rec in manifests:
            at = _parse_iso_utc(rec.get("at"))
            if at is not None and (oldest is None or at < oldest):
                oldest = at
        if oldest is None or (now - oldest).total_seconds() < grace:
            logger.info(
                "state: garbage collection deferred: the manifest history "
                "does not yet span gcGraceSeconds, so absence cannot be "
                "proven"
            )
            return
        names = set(self.cron_jobs)
        hosts = {self._state_host}
        live_dags = set(self.cron_dags)
        art_scopes = self._artifact_scope_names()
        recent: List[Dict[str, Any]] = []
        for rec in manifests:
            at = _parse_iso_utc(rec.get("at"))
            if at is None or (now - at).total_seconds() > grace:
                continue
            recent.append(rec)
            _fold_manifest(rec, names, hosts, art_scopes, live_dags)
        # job names keep their default artifact scope too.
        art_scopes |= names
        scopes_covered = _manifests_cover_scopes(recent)
        # Restrict pause-stream retention to jobs holding a LIVE pause. A job
        # paused then resumed (or expired) long ago otherwise keeps a dead
        # paused/<job> stream forever, which _refresh_pauses_from_store
        # re-reads every minute just to conclude "not paused"; dropping it
        # from the keep-set lets this same pass collect it, at GC cadence, once
        # newest record ages past grace. Fail-safe (any doubt keeps the
        # stream), and a future pause re-creates a collected stream, so
        # cross-node pause propagation is unaffected.
        pause_keep = await self._live_pause_keep(backend, names, now)
        keep: Dict[str, Set[str]] = {
            RUN_STREAM_PREFIX: names,
            LOG_STREAM_PREFIX: names,
            CATCHUP_STREAM_PREFIX: names,
            RETRY_STREAM_PREFIX: names,
            REBOOT_STREAM_PREFIX: names,
            COUNTER_STREAM_PREFIX: hosts,
            INFLIGHT_STREAM_PREFIX: names,
            SLOT_STREAM_PREFIX: names,
            PAUSE_STREAM_PREFIX: pause_keep,
            # a host that stops writing (scaled down, renamed) leaves its own
            # manifests/<host> stream behind forever otherwise; sweeping it
            # once it is not among the currently-seen hosts and has aged past
            # grace mirrors exactly how an abandoned counters/<host> stream
            # is collected above.
            MANIFEST_STREAM_PREFIX: hosts,
        }
        if scopes_covered:
            # folds artifacts/<scope> into ``keep`` (so a removed scope's
            # stream ages out like any other) and collects removed dags' run
            # documents; skipped entirely -- everything kept -- while any
            # recent manifest predates scope advertising.
            await self._gc_dag_state(
                backend, keep, art_scopes, live_dags, grace
            )
        else:
            logger.info(
                "state: leaving artifact streams and dag-run documents "
                "unmanaged this GC pass: a recent manifest does not "
                "advertise its scopes/dags (a node predating them, or the "
                "first grace window after upgrading)"
            )
        from cronstable.dag import DAG_LEASE_PREFIX

        try:
            # bounded: a worker thread wedged in a dead-mount syscall must
            # not leave _gc_task pending forever -- the single-flight check
            # would then disable automatic GC for the life of the process.
            result = await asyncio.wait_for(
                backend.collect_garbage(
                    keep=keep,
                    grace=grace,
                    # only the per-run advance leases are reclaimable: every
                    # other lease's fence can outlive the grace window inside
                    # persisted slot cancel records (see the backend's GC
                    # docstring).
                    ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,),
                ),
                timeout=STATE_GC_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade, never crash
            logger.warning("state: garbage collection failed: %s", ex)
            return
        if result.get("streams_removed") or result.get("tmp_removed"):
            logger.info(
                "state: garbage collected %s stream(s) (%s record(s)), "
                "%s temp file(s), %s quarantined record(s)",
                result.get("streams_removed", 0),
                result.get("records_removed", 0),
                result.get("tmp_removed", 0),
                result.get("quarantine_removed", 0),
            )
        # only after a successful collect pass: the records deleted above
        # (and the XCom streams dagrun's retention pruned since the last
        # pass) are what release their blobs for the sweep.
        await self._sweep_orphan_artifact_blobs(backend, grace)

    async def _gc_dag_state(
        self,
        backend: StateBackend,
        keep: Dict[str, Set[str]],
        art_scopes: Set[str],
        live_dags: Set[str],
        grace: float,
    ) -> None:
        """Extend one GC pass over artifact streams and dag run documents.

        Enumerates the store's ``dagrun/<dag>`` namespaces, hands the dags
        that are in neither any live config nor any recent manifest to
        :meth:`DagScheduler.gc_removed_dags` (terminal runs older than the
        grace only), then adds ``artifacts/`` to the keep map keyed by the
        live scopes: job names, configured/manifested shared scopes, and the
        XCom scope of every run document still on disk.  Any doubt --
        namespaces or documents unreadable, a namespace whose name is
        unrecoverable -- leaves artifact streams unmanaged (all kept) this
        pass instead of collecting on a partial view.
        """
        from cronstable.dag import DAG_RUN_NS_PREFIX, xcom_scope
        from cronstable.jobstate import ARTIFACT_STREAM_PREFIX

        try:
            namespaces, complete = await asyncio.wait_for(
                backend.list_document_namespaces(DAG_RUN_NS_PREFIX),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade, never crash
            logger.warning(
                "state: leaving artifact streams unmanaged this GC pass: "
                "cannot enumerate the dag-run namespaces (%s)",
                ex,
            )
            return
        if not complete:
            logger.warning(
                "state: leaving artifact streams unmanaged this GC pass: a "
                "dag-run namespace exists whose name cannot be recovered, "
                "so its runs' XCom scopes cannot be protected"
            )
            return
        removed_dags = {
            ns[len(DAG_RUN_NS_PREFIX) :] for ns in namespaces
        } - live_dags
        if removed_dags:
            await self._dag.gc_removed_dags(backend, removed_dags, grace)
        try:
            for ns in namespaces:
                dag_name = ns[len(DAG_RUN_NS_PREFIX) :]
                docs = await asyncio.wait_for(
                    backend.list_documents(ns),
                    timeout=STATE_OP_TIMEOUT,
                )
                for body in docs:
                    run_id = body.get("runId")
                    if run_id:
                        art_scopes.add(xcom_scope(dag_name, str(run_id)))
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade, never crash
            logger.warning(
                "state: leaving artifact streams unmanaged this GC pass: "
                "cannot read the dag-run documents (%s)",
                ex,
            )
            return
        keep[ARTIFACT_STREAM_PREFIX] = art_scopes

    async def _sweep_orphan_artifact_blobs(
        self, backend: StateBackend, grace: float
    ) -> None:
        """Reclaim artifact/XCom payload blobs no surviving record names.

        The reference set spans every enumerable ``artifacts/`` stream
        (blobs dedupe across scopes), read strictly.  Deletion is biased to
        KEEP on every doubt: the sweep is skipped outright when any artifact
        stream is unenumerable (a legacy truncated directory without its
        name sidecar) or any record unreadable, and the backend's own age
        guard keeps blobs younger than the grace (a just-landed payload
        whose record has not been appended yet).
        """
        from cronstable.jobstate import (
            ARTIFACT_STREAM_PREFIX,
            referenced_blob_digests,
        )

        try:
            stream_names, complete = await asyncio.wait_for(
                backend.list_stream_names_audit(ARTIFACT_STREAM_PREFIX),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade, never crash
            logger.warning(
                "state: skipping the orphan-blob sweep: cannot enumerate "
                "the artifact streams (%s)",
                ex,
            )
            return
        if not complete:
            logger.warning(
                "state: skipping the orphan-blob sweep: an artifact stream "
                "exists whose records cannot be enumerated, so its blob "
                "references cannot be ruled out"
            )
            return
        scopes = [name[len(ARTIFACT_STREAM_PREFIX) :] for name in stream_names]
        try:
            referenced = await asyncio.wait_for(
                referenced_blob_digests(backend, scopes, strict=True),
                timeout=STATE_GC_TIMEOUT,
            )
            removed = await asyncio.wait_for(
                backend.sweep_orphan_blobs(referenced, grace),
                timeout=STATE_GC_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade, never crash
            logger.warning(
                "state: skipping the orphan-blob sweep: an artifact record "
                "could not be read, so its blob reference cannot be ruled "
                "out (%s)",
                ex,
            )
            return
        if removed:
            logger.info("state: swept %d orphaned artifact blob(s)", removed)

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
            # UnicodeDecodeError alongside OSError: a binary token file
            # raises it from read(), and only ConfigError gets the clean
            # "not starting the web API" handling (see run()); anything else
            # is logged as an internal bug. Mirrors config._resolve_secret.
            except (OSError, UnicodeDecodeError) as ex:
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
        token_bytes = token.encode("utf-8")

        @web.middleware
        async def auth_middleware(request, handler):
            if public_paths and request.path in public_paths:
                return await handler(request)
            header = request.headers.get("Authorization", "")
            scheme, _, presented = header.partition(" ")
            # RFC 7235: the auth scheme is case-insensitive (Bearer/bearer).
            # Compare only the token, in constant time, to avoid leaking it via
            # timing (the scheme is not secret).
            if scheme.lower() != "bearer":
                # Calendar clients subscribing to an .ics feed cannot attach
                # a bearer header, so for exactly the calendar-feed paths
                # the token may ride a `token` query parameter instead (the
                # secret-address model calendar services use).  Same token,
                # same constant-time compare; every other path keeps the
                # token out of URLs (and so out of logs and referrers).
                # Matched precisely so no future route gains URL-token auth
                # by accident of its name.
                if request.path.endswith("/calendar.ics"):
                    presented = request.query.get("token", "")
                else:
                    raise web.HTTPUnauthorized()
            if not presented:
                raise web.HTTPUnauthorized()
            try:
                # compare as bytes: compare_digest raises TypeError on any
                # non-ASCII str (turning a garbage token into a 500, not a
                # 401), and a header that cannot even encode (surrogates
                # from raw header bytes) can never match a real token.
                presented_bytes = presented.encode("utf-8")
            except UnicodeEncodeError:
                raise web.HTTPUnauthorized() from None
            if not hmac.compare_digest(presented_bytes, token_bytes):
                raise web.HTTPUnauthorized()
            return await handler(request)

        return auth_middleware

    @staticmethod
    def _make_origin_middleware(allowed_origins: "frozenset[str]"):
        """Refuse cross-site browser requests to the mutating endpoints.

        The CSRF/DNS-rebinding gate for the control API, mirroring the
        Origin allow-list /mcp already enforces (see
        :meth:`cronstable.mcp.MCPHandler.handle_http`): the POST control
        routes (``/jobs/{name}/start``, ``/jobs/{name}/cancel``,
        ``/jobs/{name}/pause``, ``/jobs/{name}/resume``,
        ``/dags/{name}/trigger``, ...) are CORS "simple requests" -- no
        preflight -- so without this any web page the operator happens to
        visit can fire them at a localhost-bound daemon.  ``web.authToken``
        also defeats that (a foreign page cannot attach the bearer header),
        but the token is opt-in, and the default posture of an enabled
        dashboard must not be "any website may start and cancel my jobs".

        The rule, per request:

        * safe methods (:data:`WEB_SAFE_METHODS`) pass -- nothing they hit
          mutates, and CORS preflight must keep reaching /mcp's handler;
        * paths enforcing their own list (:data:`WEB_ORIGIN_EXEMPT_PATHS`)
          pass -- /mcp 403s foreign Origins itself;
        * no ``Origin`` header passes -- curl/CLI/monitoring clients (every
          current browser sends Origin on cross-site POSTs, which are the
          attack this defends against);
        * an Origin naming this daemon's own authority
          (:func:`_origin_matches_host`) or on the operator's
          ``web.allowedOrigins`` list passes;
        * anything else is a 403.

        Honest residual: a DNS-rebinding page served on the daemon's OWN
        port (so Origin and Host genuinely agree after the rebind) passes
        the equality test -- Origin-vs-Host cannot distinguish it from the
        real dashboard. Cross-port and cross-host rebinds are refused, and
        ``web.authToken`` closes the residual completely (a rebound page
        holds no token).
        """

        @web.middleware
        async def origin_middleware(request, handler):
            if request.method in WEB_SAFE_METHODS:
                return await handler(request)
            if request.path in WEB_ORIGIN_EXEMPT_PATHS:
                return await handler(request)
            origin = request.headers.get("Origin")
            if origin is None:
                return await handler(request)
            if origin in allowed_origins or _origin_matches_host(
                origin, request.host
            ):
                return await handler(request)
            raise web.HTTPForbidden(
                text="Origin not allowed: cross-site requests to the "
                "cronstable control API are refused; add the origin to "
                "web.allowedOrigins if it is trusted"
            )

        return origin_middleware

    def _web_tls_files_changed(self) -> bool:
        """Whether the running listener's TLS files differ from what it loaded.

        The web-app analogue of
        :meth:`cronstable.cluster.ClusterManager.tls_files_changed`, and the
        only thing that makes an in-place certificate rotation visible: the
        config bytes are identical across such a rotation, so the ordinary
        ``web_config != self.web_config`` gate never fires for it.

        Gated on ``_web_tls_signature`` rather than on ``web_config``, because
        a teardown leaves ``web_config`` stale while clearing the signature.
        """
        if self.web_config is None or self._web_tls_signature is None:
            return False
        tls = self.web_config.get("tls")
        if not tls:
            return False
        return (
            tlsutil.tls_file_signature(tls, tlsutil.LISTENER_TLS_KEYS)
            != self._web_tls_signature
        )

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
        system-local zone when it has none) and ask the cron engine for the
        delay to the next match.  The frame is kept timezone-AWARE in both
        cases, so ``CronTab.next`` computes the delay as a real duration --
        correcting for any utcoffset (DST) change across the interval -- and
        adding it back to the UTC ``after`` yields the correct UTC fire
        instant.  A naive local frame would defeat that correction: the
        engine would return a civil wall-clock delta that, added to a UTC
        instant, lands an hour off across a spring-forward/fall-back (the
        same wall time the old per-tick ``crontab.test`` matched correctly).
        ``None`` when the schedule has no further occurrence (a fixed past
        year), so the job drops out of the index.
        """
        crontab = job.schedule
        assert isinstance(crontab, CronTab)
        if job.timezone is not None:
            frame = after.astimezone(job.timezone)  # type: datetime.datetime
        else:
            # no explicit timezone -> the system-local wall clock, but kept
            # AWARE (not .replace(tzinfo=None)) so the engine applies its
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
                    self._dead_schedules.discard(name)
                elif name not in self._dead_schedules:
                    # without this, a schedule with no future occurrence
                    # (Feb 30, a past year) would just never enter the fire
                    # index and vanish without a trace
                    self._dead_schedules.add(name)
                    logger.warning(
                        "job %r: schedule %r has no future occurrence and "
                        "will NEVER fire; fix the schedule or disable the "
                        "job (its status reports never_fires)",
                        name,
                        schedule_str(job),
                    )

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
        # keep the never-fires warning latch only for jobs whose schedule
        # survived the reload unchanged; anything removed or edited gets a
        # fresh chance to warn (or to seed) in _ensure_seeded below
        self._dead_schedules = {
            name
            for name in self._dead_schedules
            if name in self.cron_jobs
            and (old := old_jobs.get(name)) is not None
            and self._same_schedule(old, self.cron_jobs[name])
        }
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
        # wake sooner when a DAG sensor poke, task retry, or scheduled
        # run is due, so sub-minute poke/retry schedules are honoured instead
        # of waiting for the once-a-minute housekeeping boundary.
        dag_wake = self._dag.next_wake_delay()
        if dag_wake is not None:
            housekeeping = min(housekeeping, max(0.0, dag_wake))
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

    @staticmethod
    def _catchup_offset(name: str, jitter: int) -> float:
        """Deterministic per-job start offset in ``[0, jitter)`` seconds.

        Derived from the job name (crc32) so the spread is stable across boots
        and across the fleet, and needs no RNG.  ``0.0`` when jitter is off.
        """
        if jitter <= 0:
            return 0.0
        return (zlib.crc32(name.encode("utf-8")) % (jitter * 1000)) / 1000.0

    @staticmethod
    def _catchup_stream(name: str) -> str:
        """The durable checkpoint stream for a job's catch-up cycles."""
        return CATCHUP_STREAM_PREFIX + name

    async def _pending_catchup_watermark(self, name: str) -> Optional[str]:
        """The watermark of an unfinished backfill cycle, if one is open.

        Reads the newest checkpoint record: an ``open`` without a following
        ``close`` means a previous backfill (here or on a crashed node) never
        completed, and catch-up should resume from ITS watermark rather than
        the run ledger's -- ordinary runs finishing after that boot advanced
        the derived watermark past the still-unreplayed slots.
        """
        backend = self.state_backend
        if backend is None:
            return None
        recs = await asyncio.wait_for(
            backend.list_records(
                self._catchup_stream(name), limit=1, newest_first=True
            ),
            timeout=STATE_OP_TIMEOUT,
        )
        if recs and recs[0].get("kind") == "open":
            watermark = recs[0].get("watermark")
            if isinstance(watermark, str):
                return watermark
        return None

    async def _pause_excusal_window(
        self, name: str
    ) -> Optional[Tuple[Optional[datetime.datetime], datetime.datetime]]:
        """The newest durable pause window ``(since, until)``, for catch-up.

        Slots that fell inside a pause window while the daemon was DOWN are
        not owed (a live pause skips them via the ledger rows; being down
        must not owe more), so catch-up excuses the occurrences inside this
        window.  It is a window, NOT a floor: a backlog owed from before
        ``since`` predates the operator's decision to pause and is still
        owed once the pause lifts, exactly as the slots after ``until`` are.
        Read from the store, not ``self._paused``: an EXPIRED pause is
        absent from memory at every read site by design, yet its window
        still excuses the slots it covered.  ``since`` reads as ``None`` on
        a record that does not carry it, which excuses everything up to
        ``until``.  Degrades to no window on store trouble (backfilling is
        the pre-pause behaviour, and pause is an operator convenience, not a
        correctness fence).
        """
        backend = self.state_backend
        if backend is None:
            return None
        try:
            recs = await asyncio.wait_for(
                backend.list_records(
                    self._pause_stream(name), limit=1, newest_first=True
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade to no window
            logger.warning(
                "catch-up: cannot read the %s pause stream (%s); assuming "
                "no pause window",
                name,
                ex,
            )
            return None
        if recs and recs[0].get("kind") == "paused":
            until = _parse_iso_utc(recs[0].get("until"))
            if until is not None:
                return _parse_iso_utc(recs[0].get("since")), until
        return None

    async def _checkpoint_catchup(
        self, name: str, kind: str, watermark: Optional[str]
    ) -> None:
        """Append an ``open``/``close`` catch-up checkpoint (best-effort).

        A failure to checkpoint must never block the backfill itself: it only
        costs crash-resume fidelity, which is logged.  At-least-once by
        design in one more way: a checkpoint write abandoned by its timeout
        is still applied later by its daemon worker thread, so a stalled
        ``open`` can land on disk AFTER the cycle's ``close`` and sort newer
        (record order is the writer's clock at the actual write).  The next
        restart then resumes an already-completed cycle -- a bounded replay,
        never a loss.
        """
        backend = self.state_backend
        if backend is None:
            return
        record = {
            "kind": kind,
            "watermark": watermark or "",
            "at": get_now(datetime.timezone.utc).isoformat(),
        }
        stream = self._catchup_stream(name)
        try:
            await asyncio.wait_for(
                backend.append_record(
                    stream, record, prune_keep=CATCHUP_STREAM_KEEP
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except Exception as ex:  # noqa: BLE001 - checkpoint is best-effort
            self.metrics.state_write_dropped("checkpoint")
            logger.warning(
                "catch-up: could not checkpoint %r for %s (%s); a restart "
                "mid-backfill may not resume it",
                kind,
                name,
                ex,
            )

    async def _missed_occurrences(
        self, job: JobConfig, now: datetime.datetime
    ) -> Tuple[int, Optional[str]]:
        """How many catch-up launches ``job`` is owed, and from where.

        Reads the durable last-run watermark -- hoisted back to an open
        checkpoint's (older) watermark when a previous backfill never closed
        (see :meth:`_pending_catchup_watermark`) -- and steps the schedule
        forward from it (DST-safe, via :meth:`_compute_next_fire`), bounded by
        ``startingDeadlineSeconds`` and :data:`MAX_CATCHUP_OCCURRENCES`.
        Occurrences inside a durable pause window are stepped over instead of
        counted (see :meth:`_pause_excusal_window`); occurrences on either
        side of it stay owed.
        Returns ``(0, ...)`` when nothing was missed or the job never ran
        under this store (no reference point, so -- like anacron/systemd -- a
        first-ever run just schedules forward); ``(1, ...)`` for ``run-once``
        when at least one slot was missed (every missed slot coalesced into a
        single launch); or the bounded count of missed slots for ``run-all``.
        The second element is the reference watermark (ISO string) for the
        cycle's checkpoint.  Store errors and timeouts propagate: the callers
        treat them as "cannot evaluate yet", never as "nothing owed".
        """
        watermark = await asyncio.wait_for(
            self.durable_last_run_at(job.name), timeout=STATE_OP_TIMEOUT
        )
        after = _parse_iso_utc(watermark)
        pending = await self._pending_catchup_watermark(job.name)
        pending_dt = _parse_iso_utc(pending)
        if pending_dt is not None and (after is None or pending_dt < after):
            after, watermark = pending_dt, pending
        if after is None:
            return 0, None
        # Slots inside a (possibly already expired) pause window are never
        # owed, and ONLY those: the window is skipped over while walking,
        # never used as a floor on `after`, so slots that came due before
        # the operator paused stay owed (see _pause_excusal_window).
        window = await self._pause_excusal_window(job.name)
        if window is not None and after is not None:
            # Belt and braces beside the open-checkpoint pin in
            # _evaluate_catch_up: a pause whose whole lifetime slipped between
            # two startup catch-up passes (a sub-CATCHUP_RECHECK_INTERVAL
            # pause, or catch-up deferred for cluster-election / backend
            # startup while a short pause came and went) is never pinned, so
            # held-slot skip rows can have walked durable_last_run_at past the
            # pre-pause backlog. When a window exists, fall back to the
            # skip-blind durable_last_completed_at if it is OLDER, restoring
            # `after` to the last real run so the pre-pause slots stay owed.
            # The `real < after` guard keeps the (older) checkpoint hoist
            # above winning when both apply; the extra read fires only when a
            # window exists.
            real = _parse_iso_utc(
                await asyncio.wait_for(
                    self.durable_last_completed_at(job.name),
                    timeout=STATE_OP_TIMEOUT,
                )
            )
            if real is not None and real < after:
                after, watermark = real, real.isoformat()
        deadline = job.startingDeadlineSeconds
        if deadline:
            cutoff = now - datetime.timedelta(seconds=deadline)
            if cutoff > after:
                after = cutoff  # only the recent window (bounds run-all)
        count = 0
        nxt = self._compute_next_fire(job, after)
        while nxt is not None and nxt <= now:
            if window is not None and _in_pause_window(nxt, window):
                # Jump the cursor to the window's end rather than stepping
                # over every excused slot: a long pause on a per-minute job
                # is thousands of them. One microsecond back so a slot
                # landing exactly on `until` (outside the half-open window)
                # is not stepped past. There is only ever one window, so
                # drop it once crossed.
                nxt = self._compute_next_fire(
                    job, window[1] - datetime.timedelta(microseconds=1)
                )
                window = None
                continue
            count += 1
            if job.onMissed == "run-once":
                return 1, watermark  # all missed slots coalesce into one
            # run-all: count each missed occurrence, hard-capped.
            if count >= MAX_CATCHUP_OCCURRENCES:
                logger.warning(
                    "catch-up: %s missed at least %d runs; replaying %d and "
                    "dropping the rest (set startingDeadlineSeconds to bound "
                    "the window, or use onMissed: run-once)",
                    job.name,
                    MAX_CATCHUP_OCCURRENCES,
                    MAX_CATCHUP_OCCURRENCES,
                )
                break
            nxt = self._compute_next_fire(job, nxt)
        return count, watermark

    async def _catch_up(self, now: datetime.datetime) -> None:
        """Replay (or coalesce) runs missed while down, on start-up.

        Evaluation begins on the first start-up pass, after the state backend
        and the cluster gate are up.  For each enabled ``onMissed`` job it
        counts the missed occurrences and, when this node is the job's cluster
        owner, schedules the catch-up launches spread over
        ``catchupJitterSeconds`` so a fleet does not all fire at once.  A
        no-op without a ``state`` section (there is no durable watermark to
        compare against) -- catch-up is a stateful-only feature.

        Resolution is NOT latched while it cannot actually happen yet: with a
        configured-but-unstarted backend (start_stop_state retries it every
        housekeeping pass), a cluster that has no positive owner for a job
        yet (still electing at boot), or a live pause (which excuses the
        slots inside its own window, not a backlog owed from before it), the
        affected jobs stay pending and are re-evaluated every
        :data:`CATCHUP_RECHECK_INTERVAL`.  Latching there
        (the old behaviour) forfeited the owed backfill forever.  Per-job
        decisions that ARE final -- backfill scheduled, nothing owed, another
        node positively owns it -- are remembered in ``_catchup_done`` so they
        are not re-processed while a sibling job stays pending.  All store
        I/O here is guarded and bounded: a store error defers (never forfeits,
        never crashes the caller).

        Every evaluation (including a deferred retry) is anchored to the
        FIRST attempt's instant, not the current pass's: while the backend
        was down the live scheduler kept firing jobs statelessly (nothing
        recorded), so counting missed slots up to a later "now" would replay
        runs that actually ran.  The owed window is the pre-boot downtime,
        full stop.
        """
        if self._caught_up:
            return
        if asyncio.get_running_loop().time() < self._catchup_next_retry:
            return
        if self._catchup_reference is None:
            self._catchup_reference = now
        now = self._catchup_reference
        if not self._state_configured:
            wants = [
                j for j in self.cron_jobs.values() if j.onMissed != "skip"
            ]
            if wants:
                logger.warning(
                    "onMissed catch-up is set on %d job(s) but needs a "
                    "`state` backend for the last-run watermark; skipping",
                    len(wants),
                )
            inert = [j for j in self.cron_jobs.values() if j.archiveOutput]
            if inert:
                logger.warning(
                    "archiveOutput is set on %d job(s) but archives nothing "
                    "without a `state` backend",
                    len(inert),
                )
            gated = [
                j for j in self.cron_jobs.values() if j.onlyIfLastSucceeded
            ]
            if gated:
                logger.info(
                    "onlyIfLastSucceeded is set on %d job(s) with no `state` "
                    "backend: the gate works from in-memory history only and "
                    "resets on restart",
                    len(gated),
                )
            self._caught_up = True
            return
        unresolved = False
        if self.state_backend is None:
            # configured but not (yet) running -- a bad mount at boot that
            # start_stop_state keeps retrying: keep the whole evaluation
            # pending rather than forfeiting the backfill.
            unresolved = True
        else:
            try:
                unresolved = await self._evaluate_catch_up(now)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - defer, never surface: this
                # runs as a detached task, so an escaped exception would be
                # an unretrieved-task warning and a silently dead catch-up.
                logger.exception(
                    "catch-up: unexpected error evaluating; will retry"
                )
                unresolved = True
        if unresolved:
            self._catchup_next_retry = (
                asyncio.get_running_loop().time() + CATCHUP_RECHECK_INTERVAL
            )
        else:
            self._caught_up = True
            self._catchup_done.clear()

    async def _evaluate_catch_up(self, now: datetime.datetime) -> bool:
        """One catch-up evaluation pass; returns whether jobs stay pending."""
        unresolved = False
        for name, job in list(self.cron_jobs.items()):
            if name in self._catchup_done:
                continue
            if (
                job.onMissed == "skip"
                or not job.enabled
                or not isinstance(job.schedule, CronTab)
            ):
                self._catchup_done.add(name)
                continue
            if self._pause_active(name) is not None:
                # No backfill while paused, but a pause is transient and
                # excuses only its own window: a backlog owed from before it
                # began survives it. Defer (like a transient cluster denial),
                # never latch, or that backlog is forfeited forever. The
                # deferred retry still counts only the pre-boot downtime:
                # every pass is anchored to _catchup_reference.
                #
                # Pin the pre-pause watermark before deferring. While paused,
                # every held slot writes a synthetic "skipped" ledger row
                # whose finished_at advances durable_last_run_at (the watermark
                # _missed_occurrences reads) forward through the window, so by
                # the time the pause lifts the derived watermark has walked
                # past _catchup_reference and the deferred retry would count
                # nothing owed. An open checkpoint fixes the watermark at the
                # last real run: durable_last_completed_at is skip-blind, so it
                # is immune to the held-slot rows however many land, and the
                # checkpoint survives a manual resume that erases the pause
                # window or a restart taken mid-pause. _missed_occurrences
                # hoists `after` back to it. The is-None guard keeps the
                # recheck loop from re-pinning (and churning the checkpoint
                # stream) once the watermark is fixed.
                try:
                    if await self._pending_catchup_watermark(name) is None:
                        real = await asyncio.wait_for(
                            self.durable_last_completed_at(name),
                            timeout=STATE_OP_TIMEOUT,
                        )
                        if real is not None:
                            await self._checkpoint_catchup(name, "open", real)
                except asyncio.CancelledError:
                    raise
                except Exception as ex:  # noqa: BLE001 - defer, never latch
                    logger.warning(
                        "catch-up: cannot pin the pre-pause watermark for %s "
                        "(%s); will retry",
                        name,
                        ex,
                    )
                logger.debug(
                    "catch-up: %s is paused; deferring its evaluation until "
                    "the pause lifts",
                    name,
                )
                unresolved = True
                continue
            # Gate before the durable read: no store I/O for a job this
            # node may not run.
            if not self._cluster_allows(job):
                if self._cluster_owner_moved(job):
                    # positive confirmation another node owns it: its
                    # owner sees the same ledger and does the backfill.
                    logger.info(
                        "catch-up: %s is owned by another node; leaving "
                        "any backfill to its owner",
                        name,
                    )
                    self._catchup_done.add(name)
                else:
                    # transient denial (no owner elected yet, no quorum,
                    # conflict): nobody would backfill if we latched now.
                    unresolved = True
                continue
            try:
                count, watermark = await self._missed_occurrences(job, now)
            except asyncio.CancelledError:
                raise
            except Exception as ex:  # noqa: BLE001 - defer, never crash
                logger.warning(
                    "catch-up: cannot read the %s watermark (%s); will retry",
                    name,
                    ex,
                )
                unresolved = True
                continue
            if count <= 0:
                self._catchup_done.add(name)
                continue
            # Checkpoint the intent BEFORE scheduling: a crash/restart
            # mid-jitter or mid-backfill then resumes from `watermark`
            # instead of losing the owed slots to the advancing ledger.
            await self._checkpoint_catchup(name, "open", watermark)
            offset = self._catchup_offset(name, job.catchupJitterSeconds)
            task = asyncio.create_task(
                self._run_catch_up(job, count, offset, now)
            )
            self._catchup_tasks.add(task)
            task.add_done_callback(self._catchup_tasks.discard)
            self._catchup_done.add(name)
        return unresolved

    async def _run_catch_up(
        self,
        job: JobConfig,
        count: int,
        offset: float,
        now: datetime.datetime,
    ) -> None:
        """Launch a job's catch-up runs, after its jitter offset.

        Sleeps out the per-job jitter (interruptibly, so shutdown wakes it at
        once), then REVALIDATES everything the sleep may have invalidated --
        the job may have been removed/disabled/edited by a reload (the live
        definition is launched, never the boot-time capture), cluster
        ownership may have moved (the new owner resumes from the open
        checkpoint; launching here too would double-run the backfill), and
        the owed count may have changed (another node backfilled).  Then
        launches ``count`` times through the concurrency-gated path,
        SERIALIZED: each launch waits for the job's previous instance(s) to
        drain, so ``concurrencyPolicy: Forbid`` cannot swallow the rest of the
        owed runs and ``run-all`` cannot stampede N concurrent instances.
        Uses :meth:`maybe_launch_job` with ``with_retries=False``, not
        :meth:`launch_scheduled_job`: a backfill is best-effort and must not
        arm retries -- nor capture a LIVE retry ladder armed by a concurrent
        scheduled fire, whose budget its failures would burn.  Failure
        reporting still applies (the reaper reports every finished run), and
        each finished run is recorded to the ledger, advancing the watermark
        so a later restart does not re-backfill the same slots; the cycle's
        checkpoint is closed once the backfill completes.
        """
        try:
            if offset > 0:
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=offset
                    )
                except asyncio.TimeoutError:
                    pass  # normal: the jitter elapsed without a shutdown
            if self._stop_event.is_set():
                return
            current = self.cron_jobs.get(job.name)
            if (
                current is None
                or current.onMissed == "skip"
                or not current.enabled
                or not isinstance(current.schedule, CronTab)
            ):
                logger.info(
                    "catch-up: %s was removed or disabled during its jitter "
                    "window; dropping the backfill",
                    job.name,
                )
                return
            job = current  # the live definition, not the boot-time capture
            if not self._cluster_allows(job):
                logger.info(
                    "catch-up: ownership of %s moved during its jitter "
                    "window; leaving the backfill to the new owner",
                    job.name,
                )
                return
            # Recompute against the ORIGINAL pass instant, not a fresh clock
            # read: the open checkpoint anchors the window's start at the
            # boot-time watermark, so a fresh `now` would stretch the window
            # over slots the live scheduler already fired during the jitter
            # and replay them.  Slots that became due during the jitter are
            # the scheduler's, not the backfill's.
            try:
                count, watermark = await self._missed_occurrences(job, now)
            except asyncio.CancelledError:
                raise
            except Exception as ex:  # noqa: BLE001 - drop, resume on restart
                logger.warning(
                    "catch-up: cannot re-read the %s watermark after its "
                    "jitter (%s); dropping the backfill (the open checkpoint "
                    "resumes it on the next restart)",
                    job.name,
                    ex,
                )
                return
            if count <= 0:
                # someone else (another node, or ordinary runs) already
                # covered it: close the cycle so restarts stop resuming it.
                await self._checkpoint_catchup(job.name, "close", watermark)
                return
            logger.info(
                "catch-up: replaying %d missed run(s) for %s", count, job.name
            )
            # Only Forbid must wait for TOTAL idleness (launching would be
            # swallowed).  For Allow/Replace the wait is mere anti-stampede
            # pacing, so it is bounded: a job whose scheduled instances
            # always overlap (runtime > interval) would otherwise starve the
            # backfill forever and leave its checkpoint open.
            max_wait: Optional[float] = (
                None
                if job.concurrencyPolicy == "Forbid"
                else CATCHUP_IDLE_WAIT_LIMIT
            )
            for _ in range(count):
                if not await self._wait_job_idle(job.name, max_wait=max_wait):
                    return  # shutdown while draining
                # Revalidate EVERY iteration, not just after the jitter: a
                # serialized run-all backfill spans count x run-duration,
                # plenty of time for a reload to remove/disable the job or
                # for ownership to move -- launching on after either would
                # run a dead definition or double-run against the new owner
                # (which resumes from the still-open checkpoint).
                live = self.cron_jobs.get(job.name)
                if (
                    live is None
                    or live.onMissed == "skip"
                    or not live.enabled
                    or not isinstance(live.schedule, CronTab)
                ):
                    logger.info(
                        "catch-up: %s was removed or disabled mid-backfill; "
                        "dropping the remaining runs",
                        job.name,
                    )
                    return
                job = live
                if not self._cluster_allows(job):
                    logger.info(
                        "catch-up: ownership of %s moved mid-backfill; "
                        "leaving the remainder to the new owner",
                        job.name,
                    )
                    return
                if self._pause_active(job.name) is not None:
                    # dropping WITHOUT closing the checkpoint: slots owed
                    # from before the pause stay resumable after it expires
                    # (the pause window excuses only its own slots).
                    logger.info(
                        "catch-up: %s was paused mid-backfill; dropping "
                        "the remaining runs",
                        job.name,
                    )
                    return
                await self.maybe_launch_job(job, with_retries=False)
            # drain the final launch so its run record lands before the
            # checkpoint closes (a crash in between merely replays: the
            # checkpoint is at-least-once by design).
            if not await self._wait_job_idle(job.name, max_wait=max_wait):
                return
            await self._checkpoint_catchup(job.name, "close", watermark)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a backfill must never kill the loop
            logger.exception(
                "catch-up: unexpected error backfilling %s", job.name
            )

    async def _wait_job_idle(
        self, name: str, *, max_wait: Optional[float] = None
    ) -> bool:
        """Wait until no instance of ``name`` is running (backfill pacing).

        Returns ``False`` when shutdown was signalled while waiting; ``True``
        means "go ahead" -- either the job went idle or ``max_wait`` seconds
        elapsed first (used by the non-Forbid policies, where the wait is
        pacing rather than correctness and must not starve forever).  Polling
        (rather than plumbing a completion event out of the reaper) keeps the
        backfill decoupled from the reaper's bookkeeping; the half-second
        cadence is plenty for runs that just finished.
        """
        waited = 0.0
        while self.running_jobs.get(name):
            if max_wait is not None and waited >= max_wait:
                return not self._stop_event.is_set()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                waited += 0.5
                continue
            return False
        return not self._stop_event.is_set()

    async def _service_slots(self, startup: bool) -> None:
        """Service the jobs due on this pass.

        Reads the clock once (AFTER any slow housekeeping, so a fire the reload
        pushed past is still serviced instead of dropped) and hands that
        instant to :meth:`spawn_jobs`.  On the start-up pass it also seeds the
        next-fire index for every scheduled job, so their first fire is the
        boundary strictly after start-up (the skip-the-partial-period start),
        and then runs missed-run catch-up once (:meth:`_catch_up`).
        """
        now = get_now(datetime.timezone.utc)
        if startup:
            self._ensure_seeded(now)
        await self.spawn_jobs(startup, now)
        # Every pass, not just start-up: _catch_up latches itself once fully
        # resolved (one boolean check per pass thereafter) but must be able
        # to retry when the backend or the cluster was not ready at boot --
        # latching on the first pass forfeited the owed backfill forever.
        # Spawned, not awaited: the evaluation performs bounded store reads
        # (up to STATE_OP_TIMEOUT each), and a slow-but-alive mount must not
        # stall this pass -- job launches outrank catch-up bookkeeping.
        # Tracked in _catchup_tasks so shutdown cancels a straggler.
        if not self._caught_up and (
            self._catchup_eval_task is None or self._catchup_eval_task.done()
        ):
            task = asyncio.create_task(self._catch_up(now))
            self._catchup_eval_task = task
            self._catchup_tasks.add(task)
            task.add_done_callback(self._catchup_tasks.discard)
        # let the DAG scheduler create due runs and advance active
        # ones. Single-flight and self-gated (only spawns work when a run's
        # wake, a scheduled fire, or an adoption/GC interval is due), so a pass
        # with no DAG work due is a couple of cheap in-memory checks.
        self._dag.service()

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
        await self._process_paused_reboots()

    async def _spawn_reboot_jobs(self) -> None:
        """Launch ``@reboot`` jobs at start-up, in config order.

        A ``@reboot`` Leader/PreferLeader job under election is deferred until
        the cluster elects an owner (:meth:`_process_pending_reboots`) rather
        than run now, when ownership is unknown: running it now would either
        skip it forever (Leader sees no quorum) or run it on every node
        (PreferLeader sees only itself).  ``EveryNode`` @reboot is not
        deferred: it is meant to run on every node at boot.

        A PAUSED @reboot job is deferred too (:meth:`_process_paused_reboots`)
        instead of being handed to the launcher, whose pause gate would skip
        it after :meth:`_reboot_boot_gate` had already burnt this OS boot's
        marker: a pause defers the boot run, it does not forfeit it.
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
                if self._pause_active(job.name) is not None:
                    self._defer_paused_reboot(job.name)
                    continue
                to_launch.append(job)
        if to_launch and self._state_configured:
            # state-backed boot dedupe (standalone / EveryNode): a daemon
            # restart within one OS boot must not re-run boot one-shots.
            # Deferred Leader/PreferLeader jobs never reach here; their
            # dedupe is the cluster's reboot_ran path.
            gated = []
            for job in to_launch:
                if await self._reboot_boot_gate(job):
                    gated.append(job)
            to_launch = gated
        await self._launch_concurrently(to_launch)

    def _defer_paused_reboot(self, name: str) -> None:
        """Hold a paused @reboot job's boot run until the pause lifts."""
        if name in self._paused_reboot_jobs:
            return
        self._paused_reboot_jobs.add(name)
        pause = self._pause_active(name)
        logger.info(
            "Job %s (@reboot) is paused%s; its boot run is deferred, not "
            "lost: no boot marker is recorded, so it runs once the pause "
            "lifts (or after a restart, still once per OS boot)",
            name,
            (
                " until {}".format(pause.until.isoformat())
                if pause is not None
                else ""
            ),
        )

    async def _process_paused_reboots(self) -> None:
        """Run a paused @reboot job's deferred boot run once the pause lifts.

        Called every scheduling pass (:meth:`spawn_jobs`), so an expiry or an
        explicit resume fires the held one-shot without a restart.  The boot
        marker is written HERE, by the normal :meth:`_reboot_boot_gate`, so
        the deferred run is still deduplicated to once per OS boot: a daemon
        that restarts while the job is still paused finds no marker, defers
        again, and still owes the run.

        Retirement mirrors :meth:`_process_pending_reboots`: a name that is
        momentarily absent from ``cron_jobs`` stays owed (never-lose), while
        one reused for a job that is no longer an enabled ``@reboot`` is
        dropped without running.
        """
        if not self._paused_reboot_jobs:
            return
        to_launch = []  # type: List[JobConfig]
        for name in list(self._paused_reboot_jobs):
            job = self.cron_jobs.get(name)
            if job is None:
                continue  # transiently absent -> still owed, re-check later
            if (
                not isinstance(job.schedule, str)
                or job.schedule != "@reboot"
                or not job.enabled
            ):
                self._paused_reboot_jobs.discard(name)
                continue
            if self._pause_active(name) is not None:
                continue
            if not self._cluster_allows(job):
                continue  # ownership moved: keep it owed, as the gated path
            self._paused_reboot_jobs.discard(name)
            logger.info(
                "Job %s (@reboot): its pause lifted; running the boot run "
                "it deferred",
                name,
            )
            to_launch.append(job)
        if to_launch and self._state_configured:
            gated = []
            for job in to_launch:
                if await self._reboot_boot_gate(job):
                    gated.append(job)
            to_launch = gated
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
                # drop it from the index so it is not revisited, and latch
                # the dead-schedules set so the status surfaces keep
                # reporting never_fires from the latch instead of each
                # re-walking the schedule's whole remaining horizon per
                # poll (the latch is what _scheduled_in and
                # _schedule_never_fires consult).
                self._next_fire.pop(name, None)
                if name not in self._dead_schedules:
                    self._dead_schedules.add(name)
                    logger.warning(
                        "job %r: schedule %r has no further occurrence "
                        "and will NEVER fire again; fix the schedule or "
                        "disable the job (its status reports "
                        "never_fires)",
                        name,
                        schedule_str(job),
                    )
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
                    # the lateAfter reference, recorded INSIDE the ownership
                    # gate: only the node that would actually run the slot
                    # owes it. A follower recording slots it never launches
                    # has no matching _sla_last_start, so the next failover
                    # would page a false breach on the incoming owner the
                    # moment the gate opened. A slot skipped by a pause is
                    # excused, so it never becomes the check's due slot.
                    if self._pause_active(job.name) is None:
                        self._sla_due[job.name] = fires[r]
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

    def _same_boot(self, rec: Dict[str, Any]) -> bool:
        """Whether a boot-marker record was written during THIS OS boot.

        Prefers the exact per-boot UUID (Linux); falls back to comparing
        derived boot times within :data:`BOOT_TIME_TOLERANCE` (the
        derivation rides the wall clock, so NTP steps shift it slightly).
        ``False`` when neither side can be identified: an unprovable "same
        boot" must run the job (today's behaviour) rather than eat it.
        """
        boot_id = platform.os_boot_id()
        rec_id = rec.get("bootId")
        if boot_id is not None and isinstance(rec_id, str) and rec_id:
            return rec_id == boot_id
        boot_time = platform.os_boot_time()
        rec_time = rec.get("bootTime")
        if boot_time is not None and isinstance(rec_time, (int, float)):
            return abs(float(rec_time) - boot_time) <= BOOT_TIME_TOLERANCE
        return False

    async def _reboot_marker_covers(self, job: JobConfig) -> bool:
        """Whether the durable marker shows ``job``'s boot run already
        happened on THIS host during THIS OS boot, for THIS job definition.

        Raises whatever the store read raises (bounded); callers map that
        to their own policy.  A marker for a different definition (digest)
        answers ``False``: a redefined @reboot job runs again, mirroring
        the cluster reboot_ran path's job-set scoping.
        """
        backend = self.state_backend
        if backend is None:
            return False
        recs = await asyncio.wait_for(
            backend.list_records(
                self._reboot_stream(job.name),
                limit=REBOOT_STREAM_KEEP,
                newest_first=True,
            ),
            timeout=STATE_OP_TIMEOUT,
        )
        for rec in recs:
            if rec.get("host") != self._state_host:
                continue
            # newest marker from this host decides; older ones are moot.
            if rec.get("jobDigest") != job_digest(job):
                return False
            return self._same_boot(rec)
        return False

    async def _reboot_boot_gate(self, job: JobConfig) -> bool:
        """Record-then-run boot dedupe for a non-deferred @reboot job.

        ``True`` -> launch (with the marker recorded FIRST, so a crash
        between record and spawn errs toward not re-running -- the same
        at-most-once ordering as the cluster's mark_reboot_ran path).
        ``False`` -> skip: the marker proves this boot's run already
        happened, or the store is unavailable under ``onStoreUnavailable:
        fail-closed``.  Under the default ``degrade`` policy an unreadable
        or unwritable store runs the job (at-least-once, exactly the
        stateless behaviour).  A store op TIMEOUT latches
        ``_reboot_gate_sick`` so the startup pass's remaining @reboot jobs
        apply the policy without further I/O, instead of serially stalling
        the first scheduling pass on a hung mount.
        """
        backend = self.state_backend
        fail_closed = self._state_on_unavailable == "fail-closed"
        if backend is None or self._reboot_gate_sick:
            if fail_closed:
                logger.warning(
                    "Job %s (@reboot) skipped: the state store is "
                    "unavailable and onStoreUnavailable is fail-closed",
                    job.name,
                )
                return False
            if self._reboot_gate_sick:
                logger.warning(
                    "state: store unhealthy; running @reboot job %s "
                    "without boot-marker dedupe",
                    job.name,
                )
            return True
        try:
            covered = await self._reboot_marker_covers(job)
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - policy decides below
            if isinstance(ex, asyncio.TimeoutError):
                self._reboot_gate_sick = True
            if fail_closed:
                logger.warning(
                    "Job %s (@reboot) skipped: cannot read its boot marker "
                    "(%s) and onStoreUnavailable is fail-closed",
                    job.name,
                    ex,
                )
                return False
            logger.warning(
                "state: cannot read the @reboot marker for %s (%s); "
                "running it (may repeat a boot run)",
                job.name,
                ex,
            )
            covered = False
        if covered:
            logger.info(
                "Job %s (@reboot) already ran during this OS boot; "
                "skipping (state-backed dedupe)",
                job.name,
            )
            return False
        record = {
            "host": self._state_host,
            "bootId": platform.os_boot_id(),
            "bootTime": platform.os_boot_time(),
            "jobDigest": job_digest(job),
            "at": get_now(datetime.timezone.utc).isoformat(),
        }
        stream = self._reboot_stream(job.name)
        try:
            # prune_keep folds the stream bound into the append's own worker
            # call; the backend applies it only after the append LANDED and
            # swallows its own failures, so it can never re-decide the launch
            # the way a separate failing prune op once threatened to.
            await asyncio.wait_for(
                backend.append_record(
                    stream, record, prune_keep=REBOOT_STREAM_KEEP
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - policy decides below
            self.metrics.state_write_dropped("reboot-marker")
            if isinstance(ex, asyncio.TimeoutError):
                self._reboot_gate_sick = True
                if fail_closed:
                    # A timed-out append is NOT a failed append: the
                    # abandoned worker thread can still land the marker
                    # later, and skipping now would then lose the boot run
                    # for this whole boot (every later restart would read
                    # "already ran"). Re-check once: marker visible ->
                    # record-before-run held, so launch; still absent ->
                    # skip under the policy (the residual late-landing
                    # window is accepted and logged).
                    try:
                        if await self._reboot_marker_covers(job):
                            return True
                    except asyncio.CancelledError:
                        raise
                    except Exception:  # noqa: BLE001 - stays unknown
                        pass
            if fail_closed:
                logger.warning(
                    "Job %s (@reboot) skipped: cannot record its boot "
                    "marker (%s) and onStoreUnavailable is fail-closed "
                    "(if the write lands late, this boot's run is lost)",
                    job.name,
                    ex,
                )
                return False
            logger.warning(
                "state: cannot record the @reboot marker for %s (%s); "
                "running it anyway (may re-run after a daemon restart)",
                job.name,
                ex,
            )
            return True
        # the marker landed (the stream bound rode along inside the append's
        # worker call): the boot run is committed to happen.
        return True

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

        A PAUSED job keeps its pending entry untouched on every branch: a
        pause defers the boot run rather than forfeiting it, so neither the
        cluster's ``mark_reboot_ran`` token nor the entry itself is spent on
        a run the launcher's pause gate would merely skip.
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
                if self._pause_active(name) is not None:
                    # a pause DEFERS the boot run: keep it pending rather
                    # than retiring it unrun, and re-check next wakeup.
                    continue
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
                if self._pause_active(name) is not None:
                    continue  # deferred by the pause; still owed
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
            if self._pause_active(name) is not None:
                # a pause DEFERS the boot run: keep the entry pending so the
                # one-shot runs when the pause lifts. mark_reboot_ran must
                # not burn the cluster's once-per-boot token for a run the
                # launcher's pause gate would only skip.
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
        # The pause gate for scheduled fires. Manual start and catch-up go
        # through maybe_launch_job directly, and the cluster gate already
        # ran, so under election exactly the owning node writes the skip
        # row. The synthetic ledger row is what keeps the derived catch-up
        # watermark advancing across the pause: its finished_at is the skip
        # instant, so the skipped slots are never owed later. It carries no
        # ``ranAt`` and never enters _last_completed_at, so the retry
        # ladder's superseded-by-run guards cannot mistake a held slot for
        # the run that would resolve them.
        #
        # @reboot jobs are EXEMPT from this gate. One reaches this launcher
        # only from the three pause-aware reboot callers (_spawn_reboot_jobs,
        # _process_paused_reboots, _process_pending_reboots), never from a
        # scheduled fire, because _ensure_seeded seeds the next-fire index
        # for CronTab schedules only. Each caller already decided to run the
        # job AND already spent the once-per-boot token (the boot marker, or
        # the cluster's mark_reboot_ran) across an await before reaching
        # here. That token cannot be un-spent, so skipping on a pause that
        # lands in the record-then-run window would FORFEIT the boot run
        # instead of deferring it, the exact loss the reboot callers avoid by
        # deferring a job that is paused when THEY examine it. This exemption
        # closes the remaining millisecond window between spending the token
        # and reaching this gate. Safe because no scheduled fire can arrive
        # here for an @reboot job, so nothing that should be paused is run.
        pause = self._pause_active(job.name)
        is_reboot = isinstance(job.schedule, str) and job.schedule == "@reboot"
        if pause is not None and not is_reboot:
            logger.info(
                "Job %s skipped: paused until %s%s",
                job.name,
                pause.until.isoformat(),
                " ({})".format(pause.note) if pause.note else "",
            )
            output = JobOutputStream()
            output.closed = True
            self._record_run(
                job.name,
                JobRunInfo(
                    outcome="skipped",
                    exit_code=None,
                    started_at=None,
                    finished_at=get_now(datetime.timezone.utc),
                    fail_reason=None,
                    output=output,
                    skip_reason="paused",
                ),
            )
            return
        if not await self._depends_on_past_ok(job):
            logger.info(
                "Job %s skipped: onlyIfLastSucceeded and its last run did "
                "not succeed",
                job.name,
            )
            return
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

    async def maybe_launch_job(
        self, job: JobConfig, *, with_retries: bool = True
    ) -> bool:
        """Launch ``job`` unless concurrencyPolicy forbids it.

        Returns whether a new instance was actually launched (False only
        for the ``Forbid`` skip), so a caller accounting for launches --
        the retry metric -- does not count a swallowed one.

        ``with_retries=False`` (catch-up backfills) launches WITHOUT the
        job's retry state: a backfill must not attach itself to a live retry
        ladder armed by a concurrent scheduled fire -- its failures would
        cancel the legitimate pending retry and burn the shared budget toward
        a premature onPermanentFailure.
        """
        # .get(), not self.running_jobs[job.name]: a bare subscript on this
        # defaultdict would INSERT an empty-list entry for a not-yet-running
        # job. Such a jobless key makes `self.running_jobs` truthy while
        # holding nothing to reap, and the reaper's idle wait
        # (_wait_for_running_jobs) blocks on _jobs_running without a timeout --
        # so a phantom key left behind (e.g. if start() below raises before the
        # append) would spin the reaper hot at shutdown instead of letting it
        # exit. Reading with .get() never creates the key.
        if self.running_jobs.get(job.name):
            logger.warning(
                "Job %s: still running and concurrencyPolicy is %s",
                job.name,
                job.concurrencyPolicy,
            )
            if job.concurrencyPolicy == "Allow":
                pass
            elif job.concurrencyPolicy == "Forbid":
                # the slot is deliberately DROPPED, not late: an overrun is
                # maxRuntime's to report, once. RECORD the excuse (pop the due
                # slot) rather than only inferring it from running_jobs, so it
                # survives the run ending and the reaper emptying running_jobs
                # -- otherwise the stale due latches lateAfter in the window
                # between the run finishing and the next slot launching. This
                # mirrors the cluster-slot path, which excuses via
                # _sla_peer_owns_slot. Popping lands lateAfter on its "nothing
                # to be late for" branch, so no fake start is fabricated and
                # the next genuinely unserved slot still latches once
                # _launch_plan records it.
                self._sla_due.pop(job.name, None)
                return False
            elif job.concurrencyPolicy == "Replace":
                for running_job in self.running_jobs[job.name]:
                    # mark before cancelling so the reaper treats the forced
                    # termination as a replacement, not a job failure.
                    running_job.replaced = True
                    await running_job.cancel()
            else:
                raise AssertionError  # pragma: no cover
        if job.concurrencyScope == "cluster":
            # the cluster-wide half of Forbid/Replace: a TTL slot lease on
            # the shared state store excludes instances on OTHER nodes.
            # Bounded (each store op capped at STATE_OP_TIMEOUT); a foreign
            # Replace holder is pursued by a background task, never waited
            # out here on the scheduler path.
            if not await self._claim_cluster_slot(job):
                return False
        logger.info("Starting job %s", job.name)
        retry_state = self.retry_state.get(job.name) if with_retries else None
        # register this run with the loopback state API (minting its
        # id + token and staging its secrets) BEFORE the child launches, so the
        # child's first callback is already authorised. extra_env carries the
        # endpoint URL/token/run-context the job needs to reach it.
        run_token, extra_env = self._prepare_job_api_run(job, retry_state)
        running_job = RunningJob(
            job,
            retry_state,
            extra_env=extra_env,
            state_token=run_token,
            run_id=extra_env.get("CRONSTABLE_RUN_ID"),
        )
        try:
            await running_job.start()
        except BaseException:
            # start() handles the expected spawn failures itself (the
            # instance still registers, start_failed, and the reaper pairs
            # the finish); anything escaping here never registers, so the
            # slot claim above must be handed back or its refcount -- and
            # the lease's renew task -- would outlive the launch forever.
            if job.concurrencyScope == "cluster":
                await self._release_cluster_slot(job)
            # likewise the job-API run registration: a launch that never
            # registers is never reaped, so drop its token/secrets here or
            # they would linger until shutdown.
            if run_token is not None and self._job_api is not None:
                await self._job_api.finish_run(run_token)
            raise
        first_instance = not self.running_jobs.get(job.name)
        self.running_jobs[job.name].append(running_job)
        # every actual launch (scheduled, manual, catch-up, retry) clears
        # the lateAfter breach condition (see _sla_periodic).
        self._sla_last_start[job.name] = get_now(datetime.timezone.utc)
        if self.state_backend is not None and first_instance:
            # record the run as in-flight (0 -> 1 instances) so a crash
            # leaves an "open" record for reconciliation; closed again when
            # the LAST instance finishes (see _handle_finished_job). Ordered
            # via the per-job inflight tail so the close cannot sort ahead.
            self._queue_inflight_write(
                job.name, self._persist_inflight_open(job, running_job)
            )
        logger.info("Job %s spawned", job.name)
        self._jobs_running.set()
        return True

    # --- cluster-wide concurrency slots (concurrencyScope: cluster) -------

    def _slot_holder(self) -> str:
        """The slot lease holder string: host plus a per-process token.

        Process-unique so a restarted daemon (or a second daemon on this
        host) can never adopt the other's slot; the host prefix is display
        only and never compared for gating.
        """
        return "{}#{}".format(self._state_host, self._proc_token)

    def _prepare_job_api_run(
        self, job: JobConfig, retry_state: Optional[JobRetryState]
    ) -> Tuple[Optional[str], Dict[str, str]]:
        """Register this run with the loopback state API; return its env.

        Mints the run id + bearer token, resolves and stages the job's
        run-scoped ``secrets`` (fresh, in memory), registers the whole
        :class:`~cronstable.jobapi.RunContext`, and returns
        ``(token, injected_env)`` so the launcher can hand the env to the child
        and the reaper can revoke the token by it.  Returns ``(None, {})`` when
        no job API is running (no ``state`` section, or jobApi disabled), so
        the classic no-endpoint path is byte-identical.
        """
        api = self._job_api
        if api is None or api.base_url is None:
            return None, {}
        from cronstable.config import _resolve_secret
        from cronstable.jobapi import RunContext, run_environment

        secrets: Dict[str, str] = {}
        for spec in job.secrets:
            name = spec.get("name")
            try:
                value = _resolve_secret(
                    spec, "job {} secret {}".format(job.name, name)
                )
            except ConfigError as ex:
                # a secret that cannot be resolved right now (an unreadable
                # fromFile, an unset fromEnvVar) is skipped, not fatal: the
                # job sees a 404 for it and fails as it sees fit, rather than
                # the whole launch dying over one secret.
                logger.warning(
                    "job %s: could not stage secret %r: %s",
                    job.name,
                    name,
                    ex,
                )
                continue
            if name and value is not None:
                secrets[name] = value
        slot = self._last_run_slot.get(job.name)
        ctx = RunContext(
            token=os.urandom(32).hex(),
            run_id=os.urandom(16).hex(),
            job_name=job.name,
            attempt=retry_state.count if retry_state is not None else 0,
            scheduled_at=slot.isoformat() if slot is not None else None,
            host=self._state_host,
            default_scope=job.name,
            allowed_scopes=set(job.stateAllowedScopes),
            secrets=secrets,
        )
        api.register_run(ctx)
        return ctx.token, run_environment(ctx, api.base_url, api.cacert)

    @staticmethod
    def _slot_name(name: str) -> str:
        """Both the slot LEASE name and the cancel-record stream name."""
        return SLOT_STREAM_PREFIX + name

    def _slot_mutex(self, name: str) -> asyncio.Lock:
        return self._slot_locks.setdefault(name, asyncio.Lock())

    async def _slot_fidelity_reason(self) -> Optional[str]:
        """The reason the store's locks cannot fence, or ``None`` (they can).

        Probed once per backend generation (see
        :meth:`cronstable.state.FilesystemStateBackend.verify_locking`) and
        latched; a probe that cannot run right now latches nothing and is
        retried on the next claim.
        """
        backend = self.state_backend
        if backend is None:
            return None
        if self._slot_fidelity is None:
            try:
                reason = await asyncio.wait_for(
                    backend.verify_locking(), timeout=STATE_OP_TIMEOUT
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - inconclusive; retry later
                return None
            self._slot_fidelity = reason or ""
            if reason:
                logger.error(
                    "state: the store's file locks cannot be trusted for "
                    "cluster-wide concurrency (%s); concurrencyScope: "
                    "cluster claims degrade per onStoreUnavailable",
                    reason,
                )
        return self._slot_fidelity or None

    async def _acquire_slot_lease(
        self, backend: StateBackend, lease_name: str
    ) -> Optional[Lease]:
        """``acquire_lease`` for a cluster slot, mapping a timeout OR a raised
        store error to ``None`` so the caller's read-back-and-policy path
        decides.  Never raises (bar cancellation): ``_claim_cluster_slot`` runs
        under the slot service task that ``run()`` awaits OUTSIDE its
        try/except, so an escaped store error (flock ENOLCK/EIO, ``os.open``
        EMFILE) would terminate the whole scheduler loop.
        """
        try:
            return await asyncio.wait_for(
                backend.acquire_lease(
                    lease_name, self._slot_holder(), self._slot_ttl
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.TimeoutError:
            return None
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a raised store error is as
            # ambiguous as a timeout; fail closed via the read-back path
            # rather than letting it escape and crash the loop.
            return None

    async def _claim_cluster_slot(self, job: JobConfig) -> bool:
        """Claim the cluster-wide concurrency slot for one launch of ``job``.

        ``True`` means launch (holding the slot lease, or having degraded
        to node-local enforcement per ``onStoreUnavailable``); ``False``
        means this launch is skipped (Forbid: a foreign holder; Replace:
        a background pursuit will re-attempt once the holder yields;
        fail-closed: the store did not answer).

        The whole claim runs under a per-job asyncio lock, serializing it
        against the release on the finish path: without it, a release
        landing between a fresh claim and its instance registration could
        revoke the new claim's lease (same-holder re-acquire keeps the
        fence, so the stale release still matches it).

        Honesty contract: at-least-once, not exactly-once.  A holder that
        loses its lease to a store outage keeps running (never kill work
        on a store blip), so a Forbid peer that then wins the slot overlaps
        it; ``degrade`` explicitly trades the cluster gate for availability
        when the store cannot answer.
        """
        backend = self.state_backend
        if not self._state_configured:
            # unreachable via the parse-time cross-check; test configs that
            # bypass it just fall back to node-local enforcement.
            return True
        fail_closed = self._state_on_unavailable == "fail-closed"
        name = job.name

        def _unavailable(why: str) -> bool:
            if fail_closed:
                logger.warning(
                    "Job %s skipped: cannot claim its cluster concurrency "
                    "slot (%s) and onStoreUnavailable is fail-closed",
                    name,
                    why,
                )
                return False
            logger.warning(
                "Job %s: cannot claim its cluster concurrency slot (%s); "
                "enforcing concurrencyPolicy on this node only for this "
                "run (onStoreUnavailable: degrade)",
                name,
                why,
            )
            self._slot_refs[name] = self._slot_refs.get(name, 0) + 1
            return True

        if backend is None:
            return _unavailable("the state store is unavailable")
        fidelity = await self._slot_fidelity_reason()
        if fidelity is not None:
            return _unavailable(fidelity)
        async with self._slot_mutex(name):
            held = self._slot_leases.get(name)
            renewer = self._slot_renewers.get(name)
            if held is not None and renewer is not None and not renewer.done():
                # already holding (a Replace re-launch, or Allow-scoped
                # overlap after a reload): adopt the live lease.
                self._slot_refs[name] = self._slot_refs.get(name, 0) + 1
                return True
            lease_name = self._slot_name(name)
            got = await self._acquire_slot_lease(backend, lease_name)
            if got is None:
                # denied, sick, or timed out -- a bounded read tells a live
                # foreign holder apart from a store that cannot answer (the
                # lease API fails closed, so None alone proves nothing).
                observed: Optional[Lease] = None
                answered = False
                try:
                    observed = await asyncio.wait_for(
                        backend.read_lease(lease_name),
                        timeout=STATE_OP_TIMEOUT,
                    )
                    answered = observed is not None
                except asyncio.TimeoutError:
                    pass
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 - a raised store error here
                    # is as ambiguous as a timeout: leave answered=False so the
                    # "store did not answer" branch below returns _unavailable
                    # (fail closed) instead of letting it crash the loop.
                    pass
                if observed is not None:
                    if observed.holder == self._slot_holder():
                        # our own acquire landed after its timeout was
                        # abandoned (the documented UNKNOWN case): adopt it.
                        got = observed
                    elif (
                        get_now(datetime.timezone.utc).timestamp()
                        > observed.expires_at
                    ):
                        # expired but unreclaimed: treat as unanswered and
                        # let the policy decide (the next attempt acquires).
                        answered = False
                        observed = None
                    elif job.concurrencyPolicy == "Forbid":
                        logger.warning(
                            "Job %s skipped: its cluster concurrency slot "
                            "is held by %s (concurrencyPolicy: Forbid, "
                            "concurrencyScope: cluster)",
                            name,
                            observed.holder.rsplit("#", 1)[0],
                        )
                        self._sla_peer_owns_slot(name)
                        return False
                    else:  # Replace
                        self._spawn_slot_pursuit(job, observed)
                        self._sla_peer_owns_slot(name)
                        return False
                if got is None and not answered:
                    return _unavailable("the store did not answer")
            if got is None:  # pragma: no cover - defensive; handled above
                return _unavailable("the store did not answer")
            self._slot_leases[name] = got
            self._slot_refs[name] = self._slot_refs.get(name, 0) + 1
            if renewer is not None and not renewer.done():
                renewer.cancel()
            self._slot_renewers[name] = asyncio.create_task(
                self._slot_renewer(name)
            )
            # a fresh slot win is the one moment a foreign orphaned run is
            # provably unrenewed: reconcile its in-flight record (does not
            # prove the process died -- see _reconcile_open_record).
            await self._reconcile_takeover_inflight(job)
            return True

    def _spawn_slot_pursuit(self, job: JobConfig, observed: Lease) -> None:
        """Start (or keep) the background Replace pursuit for ``job``.

        The pursuit -- asking the foreign holder to yield, waiting it out,
        then re-attempting the launch -- takes up to ~2 slot TTLs, so it
        must never run inline on the scheduler pass (one held slot would
        stall every other due job); single-flight per job.
        """
        name = job.name
        existing = self._slot_pursuits.get(name)
        if existing is not None and not existing.done():
            return
        logger.info(
            "Job %s: cluster Replace: asking the current slot holder (%s) "
            "to yield; the launch is re-attempted when the slot frees",
            name,
            observed.holder.rsplit("#", 1)[0],
        )
        task = asyncio.create_task(self._pursue_replace_slot(job, observed))
        self._slot_pursuits[name] = task

        def _clear(done: asyncio.Task) -> None:
            if self._slot_pursuits.get(name) is done:
                del self._slot_pursuits[name]

        task.add_done_callback(_clear)

    async def _pursue_replace_slot(
        self, job: JobConfig, observed: Lease
    ) -> None:
        """Ask a foreign slot holder to yield, wait, then re-attempt.

        The cancel request is an immutable record aimed at the holder's
        exact FENCE, so a request left over from a previous incarnation is
        inert (a takeover always bumps the fence).  The holder's renew task
        observes it within one renew period and cancels its instances
        (marked replaced); when the slot frees -- release or TTL expiry --
        the launch goes back through every normal gate.  Bounded: a holder
        that never yields (still running, or its node is gone but the
        record write failed) forfeits this launch with a warning -- no-run
        over double-run.
        """
        backend = self.state_backend
        name = job.name
        if backend is None:
            return
        cancel = {
            "kind": "cancel",
            "fence": observed.fence,
            "by": self._state_host,
            "at": get_now(datetime.timezone.utc).isoformat(),
        }
        stream = self._slot_name(name)
        try:
            await asyncio.wait_for(
                backend.append_record(
                    stream, cancel, prune_keep=SLOT_STREAM_KEEP
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - give up, log, no launch
            logger.warning(
                "Job %s: could not record the cluster Replace cancel "
                "request: %s",
                name,
                ex,
            )
            return
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 2 * self._slot_ttl
        poll = max(1.0, self._slot_ttl / 6)
        while True:
            if self._stop_event.is_set():
                return
            await asyncio.sleep(poll)
            current: Optional[Lease] = observed
            try:
                current = await asyncio.wait_for(
                    backend.read_lease(self._slot_name(name)),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - keep waiting
                pass
            now = get_now(datetime.timezone.utc).timestamp()
            if (
                current is None
                or now > current.expires_at
                or current.holder == self._slot_holder()
            ):
                break
            if loop.time() >= deadline:
                logger.warning(
                    "Job %s: the foreign holder (%s) did not yield its "
                    "cluster concurrency slot within %.0fs; skipping this "
                    "launch (no-run over double-run)",
                    name,
                    current.holder.rsplit("#", 1)[0],
                    2 * self._slot_ttl,
                )
                return
        if await self.maybe_launch_job(job):
            logger.info(
                "Job %s: launched after the previous cluster slot holder "
                "yielded (concurrencyPolicy: Replace)",
                name,
            )

    async def _slot_renewer(self, name: str) -> None:
        """Keep a held slot lease alive while the job runs here.

        Renews at a third of the TTL, and doubles as the Replace listener:
        each cycle reads the newest cancel record and, when one targets our
        exact fence, cancels the local instances (marked replaced -- not a
        failure) so the requesting node can take the slot.  A renew that is
        positively refused because another node took the lease over logs
        and stops renewing but NEVER cancels the running work (a store blip
        must not kill a healthy job); an ambiguous refusal keeps retrying
        -- the store deliberately allows a same-fence renew slightly past
        expiry, so a single unreadable blip self-heals.
        """
        period = max(1.0, self._slot_ttl / 3)
        me = asyncio.current_task()
        while True:
            await asyncio.sleep(period)
            backend = self.state_backend
            lease = self._slot_leases.get(name)
            if backend is None or lease is None:
                return
            try:
                recs = await asyncio.wait_for(
                    backend.list_records(
                        self._slot_name(name), limit=1, newest_first=True
                    ),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - the listener is best-effort
                recs = []
            rec = recs[0] if recs else None
            if (
                rec is not None
                and rec.get("kind") == "cancel"
                and rec.get("fence") == lease.fence
                and self.running_jobs.get(name)
            ):
                logger.info(
                    "Job %s: node %s requested this instance be replaced "
                    "(concurrencyPolicy: Replace, concurrencyScope: "
                    "cluster); cancelling",
                    name,
                    rec.get("by"),
                )
                for running_job in list(self.running_jobs.get(name) or []):
                    running_job.replaced = True
                    await running_job.cancel()
                # the finish path releases the slot; keep renewing till then
            try:
                renewed = await asyncio.wait_for(
                    backend.renew_lease(lease, self._slot_ttl),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.TimeoutError:
                continue  # unknown; retry next period
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - a RAISED store error (flock
                # ENOLCK/EIO on NFS, a mount blip) is exactly as ambiguous as
                # a timeout: it must NOT kill the renewer task, because a dead
                # renewer stops renewing silently and the slot lease then
                # expires under a live holder -> a standby takes it over and
                # double-fires the very job the slot fences.  Retry next
                # period, like the timeout and the sibling list/read calls.
                logger.warning(
                    "Job %s: cluster concurrency slot renewal errored; "
                    "will retry next period",
                    name,
                )
                continue
            if self._slot_renewers.get(name) is not me:
                # We were retired mid-renew: _release_cluster_slot popped us
                # from _slot_renewers, popped the lease, and scheduled its
                # release (or a re-acquire replaced us).  A cancel racing this
                # exact point does NOT reliably raise: on Python <=3.11
                # asyncio.wait_for returns the resolved renew result instead of
                # propagating the CancelledError (`if fut.done(): return
                # fut.result()`), so the cancel only lands at the next sleep --
                # after this iteration's tail would otherwise run.  Stand down
                # now, without touching _slot_leases: re-populating the entry
                # the release just popped would make _release_slot_lease treat
                # it as a fresh claim and skip the release (leaking the slot a
                # whole TTL, its would-be holder spinning); the takeover branch
                # below could likewise pop a genuine re-claim's lease.  The
                # finish path owns the release from here.
                return
            if renewed is not None:
                self._slot_leases[name] = renewed
                continue
            observed: Optional[Lease] = None
            try:
                observed = await asyncio.wait_for(
                    backend.read_lease(self._slot_name(name)),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - ambiguous; retry
                continue
            if observed is not None and (
                observed.holder != lease.holder
                or observed.fence != lease.fence
            ):
                logger.warning(
                    "Job %s: its cluster concurrency slot was taken over "
                    "by %s while it is still running here (a store outage "
                    "outlasted the slot TTL?); the run continues -- the "
                    "overlap is the documented at-least-once trade",
                    name,
                    observed.holder.rsplit("#", 1)[0],
                )
                self._slot_leases.pop(name, None)
                return
            # our lease on disk (blip) or unreadable: keep trying.

    async def _release_cluster_slot(self, job: JobConfig) -> None:
        """Hand back the slot when a cluster-scoped job's last user is done.

        Refcounted (see ``_slot_refs``): every claim increments, every
        finished instance decrements, and the lease is released only at
        zero AND with no registered instances -- so a Replace overlap or a
        claim whose instance is still being spawned cannot lose its lease
        to a stale release.  The release itself is fire-and-forget; when
        no lease was recorded (a degraded launch, or an acquire whose
        timeout abandoned a worker that later landed the write) a phantom
        check read is made and any lease held by THIS process released, so
        a phantom cannot block other nodes for a whole TTL.
        """
        name = job.name
        async with self._slot_mutex(name):
            refs = self._slot_refs.get(name, 0) - 1
            if refs > 0:
                self._slot_refs[name] = refs
                return
            self._slot_refs.pop(name, None)
            if self.running_jobs.get(name):
                return
            renewer = self._slot_renewers.pop(name, None)
            if renewer is not None and not renewer.done():
                renewer.cancel()
            lease = self._slot_leases.pop(name, None)
            if lease is not None:
                self._track_state_write(self._release_slot_lease(name, lease))
            else:
                self._track_state_write(self._release_phantom_slot(name))

    async def _release_slot_lease(self, name: str, lease: Lease) -> None:
        backend = self.state_backend
        if backend is None:
            return
        # Serialized under the per-job slot mutex, like _release_phantom_slot:
        # this write is scheduled fire-and-forget by _release_cluster_slot, so
        # a fresh same-holder re-claim can land before it -- and a same-holder
        # re-acquire KEEPS the fence, so this stale release would still match
        # on disk and revoke the new claim's lease (its renewer spinning, a
        # peer's Forbid claim then double-running). A fresh claim installs
        # _slot_leases[name] under this same mutex, so once one is present
        # this release is stale by definition and stands down; holding the
        # mutex across the write keeps a claim from interleaving with it.
        async with self._slot_mutex(name):
            if self._slot_leases.get(name) is not None:
                return  # a fresh claim adopted the on-disk lease; keep it
            try:
                await asyncio.wait_for(
                    backend.release_lease(lease), timeout=STATE_OP_TIMEOUT
                )
            except asyncio.CancelledError:
                raise
            except Exception as ex:  # noqa: BLE001 - TTL is the fallback
                logger.warning(
                    "state: failed to release the concurrency slot for %s "
                    "(%s); it frees by TTL",
                    name,
                    ex,
                )

    async def _release_phantom_slot(self, name: str) -> None:
        backend = self.state_backend
        if backend is None:
            return
        # Serialized under the per-job slot mutex: the phantom read_lease +
        # release match ONLY on the per-process holder string (a degraded
        # launch left our token on disk but no local Lease), so without the
        # mutex a fresh claim that installs a real lease L (same holder,
        # since the token is process-wide) between this read and its
        # release would see L released out from under a live run -- the
        # slot freed while we believe we hold it, its renewer spinning,
        # and a peer's Forbid claim then double-running. A fresh claim
        # installs _slot_leases[name] under this same mutex, so once one is
        # present this cleanup is a no-op.
        async with self._slot_mutex(name):
            if self._slot_leases.get(name) is not None:
                return  # a live claim owns the slot now; not a phantom
            try:
                observed = await asyncio.wait_for(
                    backend.read_lease(self._slot_name(name)),
                    timeout=STATE_OP_TIMEOUT,
                )
                if (
                    observed is not None
                    and observed.holder == self._slot_holder()
                    and self._slot_leases.get(name) is None
                ):
                    await asyncio.wait_for(
                        backend.release_lease(observed),
                        timeout=STATE_OP_TIMEOUT,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass

    # --- in-flight run records and crash reconciliation -------------------

    @staticmethod
    def _inflight_stream(name: str) -> str:
        return INFLIGHT_STREAM_PREFIX + name

    def _queue_inflight_write(
        self, name: str, coro: Coroutine[Any, Any, None]
    ) -> asyncio.Task:
        """Run an inflight-stream write ordered behind the job's previous one.

        The open and its paired close run on separate worker threads whose
        filename sort key is each thread's own wall-clock read, so an
        unordered pair could land filename-inverted (close before open) for
        a near-instant run and leave "open" newest -- a spurious interrupted
        run on the next restart.  Chaining each job's inflight writes (the
        same idiom as :meth:`_queue_retry_write`) keeps the stream's order
        equal to the launch/finish order.
        """
        prev = self._inflight_write_tail.get(name)

        async def _ordered() -> None:
            if prev is not None and not prev.done():
                await asyncio.wait({prev})
            await coro

        task = self._track_state_write(_ordered())
        self._inflight_write_tail[name] = task

        def _clear(done: asyncio.Task) -> None:
            if self._inflight_write_tail.get(name) is done:
                del self._inflight_write_tail[name]

        task.add_done_callback(_clear)
        return task

    async def _persist_inflight_open(
        self, job: JobConfig, running_job: RunningJob
    ) -> None:
        """Record that ``job`` went 0 -> 1 live instances on this node."""
        backend = self.state_backend
        if backend is None:
            return
        proc = getattr(running_job, "proc", None)
        pid = proc.pid if proc is not None else None
        record = {
            "kind": "open",
            "host": self._state_host,
            "proc": self._proc_token,
            "pid": pid,
            "startedAt": get_now(datetime.timezone.utc).isoformat(),
            "jobDigest": job_digest(job),
        }
        stream = self._inflight_stream(job.name)
        try:
            # Bounded like every other store op: a wedged mount times the
            # write out (caught below) instead of hanging this tracked task
            # forever, which would pile up in _pending_state_writes.
            await asyncio.wait_for(
                backend.append_record(
                    stream, record, prune_keep=INFLIGHT_STREAM_KEEP
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except Exception as ex:  # noqa: BLE001 - fire-and-forget
            self.metrics.state_write_dropped("inflight")
            logger.warning(
                "state: failed to record the in-flight run of %s: %s",
                job.name,
                ex,
            )

    async def _persist_inflight_closed(
        self, name: str, reason: str = "finished"
    ) -> None:
        """Record that ``name`` went 1 -> 0 live instances on this node."""
        backend = self.state_backend
        if backend is None:
            return
        record = {
            "kind": "closed",
            "host": self._state_host,
            "proc": self._proc_token,
            "reason": reason,
            "at": get_now(datetime.timezone.utc).isoformat(),
        }
        try:
            await asyncio.wait_for(
                backend.append_record(
                    self._inflight_stream(name),
                    record,
                    prune_keep=INFLIGHT_STREAM_KEEP,
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except Exception as ex:  # noqa: BLE001 - fire-and-forget
            self.metrics.state_write_dropped("inflight")
            logger.warning(
                "state: failed to close the in-flight record of %s: %s",
                name,
                ex,
            )

    async def _reconcile_inflight(self) -> None:
        """Close runs the PREVIOUS daemon on this host left in flight.

        Runs once per rehydration (after the ledger warm, before the retry
        re-arm).  An ``open`` record from this host whose writing process
        is gone means the run died with (or after) that daemon: it is made
        visible as an ``unknown``-outcome ledger row instead of silently
        vanishing.  Three guards keep live runs safe: a record written by
        THIS process (a state-section reload rebuilt the backend under a
        live run) is skipped; live local instances outrank the ledger; and
        a recorded pid that still exists is left alone -- a daemon crash
        does not kill the job processes it spawned.
        """
        backend = self.state_backend
        if backend is None:
            return
        for name, job in list(self.cron_jobs.items()):
            if self.running_jobs.get(name):
                continue
            try:
                recs = await asyncio.wait_for(
                    backend.list_records(
                        self._inflight_stream(name),
                        limit=1,
                        newest_first=True,
                    ),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "state: in-flight reconciliation timed out reading %s; "
                    "skipping the rest (store unhealthy?)",
                    name,
                )
                break
            except asyncio.CancelledError:
                raise
            except Exception as ex:  # noqa: BLE001 - degrade, never crash
                logger.warning(
                    "state: cannot read the in-flight record of %s: %s",
                    name,
                    ex,
                )
                continue
            rec = recs[0] if recs else None
            if rec is None or rec.get("kind") != "open":
                continue
            if rec.get("host") != self._state_host:
                continue  # another node's business (see the slot takeover)
            if rec.get("proc") == self._proc_token:
                continue  # our own live run; the backend was just rebuilt
            pid = rec.get("pid")
            if (
                isinstance(pid, int)
                and not isinstance(pid, bool)
                and platform.pid_alive(pid)
            ):
                logger.warning(
                    "Job %s: the previous daemon's run (pid %d) still "
                    "appears to be running; leaving its in-flight record "
                    "open",
                    name,
                    pid,
                )
                continue
            self._reconcile_open_record(name, job, rec, "reconciled-crash")

    async def _reconcile_takeover_inflight(self, job: JobConfig) -> None:
        """On a fresh slot win, close a foreign holder's orphaned run.

        A just-acquired slot proves the previous holder made NO successful
        renewal for a full TTL -- not that its process died (it may still
        be running if it lost store access; that overlap is the documented
        at-least-once trade).  The fence supersession is what makes closing
        the record safe: any late write the old incarnation makes is
        detectable against the bumped fence.
        """
        backend = self.state_backend
        if backend is None:
            return
        try:
            recs = await asyncio.wait_for(
                backend.list_records(
                    self._inflight_stream(job.name),
                    limit=1,
                    newest_first=True,
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - reconciliation is best-effort
            return
        rec = recs[0] if recs else None
        if rec is None or rec.get("kind") != "open":
            return
        if rec.get("host") == self._state_host:
            # a same-host orphan (a previous daemon on this host): our own
            # live run is never reconciled, and -- exactly as the
            # rehydration path does -- a recorded pid that is still alive
            # means the job process outlived its daemon (a crash does not
            # kill spawned children), so the run is NOT interrupted. Only a
            # genuinely foreign host's record is judged purely by fence
            # supersession (its pid names another machine).
            if rec.get("proc") == self._proc_token:
                return  # our own live run
            pid = rec.get("pid")
            if (
                isinstance(pid, int)
                and not isinstance(pid, bool)
                and platform.pid_alive(pid)
            ):
                logger.warning(
                    "Job %s: a previous daemon's run (pid %d) on this host "
                    "still appears to be running; leaving its in-flight "
                    "record open on the slot takeover",
                    job.name,
                    pid,
                )
                return
        self._reconcile_open_record(job.name, job, rec, "reconciled-takeover")

    def _reconcile_open_record(
        self,
        name: str,
        job: Optional[JobConfig],
        rec: Dict[str, Any],
        reason: str,
    ) -> None:
        """Close an orphaned ``open`` record and make the run visible.

        Appends a ``closed`` record (so the orphan is not re-reconciled)
        and a synthetic ``unknown``-outcome ledger row.  ``unknown`` is a
        non-verdict everywhere (onlyIfLastSucceeded, success-rate,
        superseded-by-run all ignore it) and the row carries no
        ``started_at`` so it cannot skew duration statistics.

        The catch-up watermark is policy-aware: for the default
        ``onMissed: skip`` the row carries ``finished_at`` (the run's start
        instant, so the watermark advances over exactly the interrupted
        slot); under ``run-once``/``run-all`` it carries the instant as
        ``interruptedAt`` instead, leaving the durable watermark untouched
        so the interrupted occurrence is still owed to catch-up -- crash
        recovery must not silently downgrade those jobs to at-most-once.
        """
        started_iso = rec.get("startedAt")
        if not isinstance(started_iso, str):
            started_iso = get_now(datetime.timezone.utc).isoformat()
        fail_reason = (
            "run interrupted: no completion was recorded for the run "
            "started at {} on {} (daemon crash, or the node lost access "
            "to the state store mid-run)".format(started_iso, rec.get("host"))
        )
        data: Dict[str, Any] = {
            "outcome": "unknown",
            "exit_code": None,
            "started_at": None,
            "duration": None,
            "fail_reason": fail_reason,
        }
        if job is None or job.onMissed == "skip":
            data["finished_at"] = started_iso
            # the run-instant mirror the durable superseded-by-run guard
            # folds over (see JobRunInfo.to_dict): an interrupted run IS a
            # run, so a ladder armed before it started is resolved.
            data["ranAt"] = started_iso
        else:
            data["interruptedAt"] = started_iso
        self._queue_inflight_write(
            name, self._persist_inflight_closed(name, reason)
        )
        self._track_state_write(self._persist_reconciled_record(name, data))
        # make it visible on this node's dashboard immediately (bypassing
        # _record_run: no metric emission, no double-persist).
        finished = _parse_iso_utc(started_iso) or get_now(
            datetime.timezone.utc
        )
        output = JobOutputStream()
        output.close()
        info = JobRunInfo(
            outcome="unknown",
            exit_code=None,
            started_at=None,
            finished_at=finished,
            fail_reason=fail_reason,
            output=output,
        )
        self.run_history[name].append(info)
        self.last_run[name] = info
        # a takeover can reconcile a foreign record older than a run this
        # node already recorded, so advance the supersede watermark rather
        # than assigning it (the durable side is a derive_max, i.e. already
        # monotonic).
        previous = self._last_completed_at.get(name)
        if previous is None or finished > previous:
            self._last_completed_at[name] = finished
        logger.warning(
            "Job %s: reconciled an interrupted run (%s): %s",
            name,
            reason,
            fail_reason,
        )

    async def _persist_reconciled_record(
        self, name: str, data: Dict[str, Any]
    ) -> None:
        backend = self.state_backend
        if backend is None:
            return
        stream = self._run_stream(name)
        try:
            await backend.append_record(
                stream,
                data,
                prune_keep=(
                    self._state_max_runs if self._state_max_runs > 0 else None
                ),
            )
        except Exception as ex:  # noqa: BLE001 - fire-and-forget
            self.metrics.state_write_dropped("run-record")
            logger.warning(
                "state: failed to persist the reconciled run record for "
                "%s: %s",
                name,
                ex,
            )

    # continually watches for the running jobs, clean them up when they exit
    async def _wait_for_running_jobs(self) -> None:
        # job -> wait task
        wait_tasks = {}  # type: Dict[RunningJob, asyncio.Task]
        # the standing wait on _jobs_running that sits in the busy branch's
        # wait set below, so a launch or the shutdown signal (both set the
        # event) wakes the reaper immediately. Fully event-driven: with it in
        # the set there is nothing left for the old 1-second poll timeout to
        # notice, so a daemon with one long-running job no longer wakes ~86k
        # times a day just to re-count its running set.
        event_wait = None  # type: Optional[asyncio.Task]
        try:
            while self.running_jobs or not self._stop_event.is_set():
                try:
                    for jobs in self.running_jobs.values():
                        for job in jobs:
                            if job not in wait_tasks:
                                wait_tasks[job] = asyncio.create_task(
                                    job.wait()
                                )
                    if not wait_tasks:
                        # Nothing running: block until a job launches or
                        # shutdown is signalled rather than polling. This is
                        # the scheduler's most frequent idle wakeup, and the
                        # loop condition can only change on those two events,
                        # so a plain wait loses no liveness.
                        await self._jobs_running.wait()
                        continue
                    # Every job now running has its wait task registered
                    # above, with no await in between, so clearing here
                    # cannot swallow a launch notification for a job the
                    # wait set does not cover.
                    self._jobs_running.clear()
                    if event_wait is None or event_wait.done():
                        event_wait = asyncio.create_task(
                            self._jobs_running.wait()
                        )
                    done_tasks, _ = await asyncio.wait(
                        [event_wait, *wait_tasks.values()],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    done_jobs = set()
                    for job, task in list(wait_tasks.items()):
                        if task in done_tasks:
                            done_jobs.add(job)
                    try:
                        for job in done_jobs:
                            task = wait_tasks.pop(job)
                            try:
                                task.result()
                            except Exception:  # pragma: no cover
                                logger.exception(
                                    "Unexpected error while waiting on job "
                                    "%s; please report this as a bug (2)",
                                    job.config.name,
                                )
                            try:
                                await self._handle_finished_job(job)
                            except asyncio.CancelledError:
                                raise
                            except Exception:
                                # Per-job, so one job's failure to finish
                                # (e.g. the state backend 503ing out of
                                # _job_api.finish_run) does not skip the
                                # remaining jobs in this batch. No
                                # `pragma: no cover` unlike its siblings
                                # below: this arm has a test
                                # (test_reaper_finishes_whole_batch_when_
                                # one_job_raises), so let the coverage gate
                                # notice if it stops being reached.
                                logger.exception(
                                    "Unexpected error finishing job %s; "
                                    "please report this as a bug (6)",
                                    job.config.name,
                                )
                    finally:
                        # Record all DAG-task completions buffered while
                        # handling this batch in one RMW per run (a mapped
                        # fan-out finishing together no longer pays a full
                        # document write+fsync per task). No-op when no DAG
                        # tasks finished.
                        #
                        # In a finally: the buffer holds completions from jobs
                        # ALREADY handled in this batch, so letting an
                        # exception from a later job skip the flush would
                        # strand their dag_run entries as RUNNING until some
                        # unrelated job completes and reaches the next flush
                        # (indefinitely, if none does: nothing else drains
                        # this buffer; _retry_completions only sees
                        # _pending_completions, which flush_completions
                        # populates from its own except arm). Batching is what
                        # made this cross-job: pre-batching each completion
                        # wrote itself.
                        await self._dag.flush_completions()
                except asyncio.CancelledError:
                    raise
                except Exception:  # pragma: no cover
                    logger.exception("please report this as a bug (3)")
                    await asyncio.sleep(1)
        finally:
            if event_wait is not None and not event_wait.done():
                event_wait.cancel()

    def _record_run(self, name: str, info: JobRunInfo) -> None:
        # the latest finished run (for status/log replay) plus the bounded
        # history (for the dashboard's history/stats view); in-memory only.
        self.last_run[name] = info
        self.run_history[name].append(info)
        # this run changes the trends aggregate, so drop any cached payload
        # for the job rather than serve it stale out to the TTL.
        self._trends_cache.pop(name, None)
        # every recorded run also feeds the Prometheus counters/histogram,
        # so /metrics and the run-history API always agree on outcomes.
        self.metrics.job_run_recorded(
            name, info.outcome, info.duration, info.resource_usage
        )
        # the maxTimeSinceSuccess reference (see _sla_periodic): every
        # recorded success moves it, whatever path ran the job.
        if info.outcome == "success" and info.finished_at is not None:
            self._sla_last_success[name] = info.finished_at
        # the onlyIfLastSucceeded gate's eviction-proof memo (see
        # _depends_on_past_ok). run_history is a bounded ring, so a long
        # pause (one synthetic "skipped" row per held slot) can push the
        # last real outcome out of it and reopen a gate that a failure had
        # correctly closed. This keeps the newest real outcome regardless of
        # how many non-run rows follow it.
        if info.outcome in ("success", "failure") and (
            info.finished_at is not None
        ):
            self._last_real_outcome[name] = (info.finished_at, info.outcome)
        # the retry ladder's superseded-by-run watermark (see
        # _validate_pending_retry). Every outcome but "skipped" counts: a
        # cancelled or crash-reconciled run still resolved the attempt,
        # while a pause-held slot ran nothing and must not settle a ladder
        # that is only waiting for the resume.
        if info.outcome != "skipped" and info.finished_at is not None:
            self._last_completed_at[name] = info.finished_at
        # and, when a durable state backend is configured, persist the run to
        # the ledger so history/last-run survive a restart. Fire-and-forget: a
        # slow store must never stall run handling, so the write is a tracked
        # background task rather than an await here (this method is sync and on
        # the finished-job path). No-op on the stateless default.
        if self.state_backend is not None:
            self._track_state_write(self._persist_run_record(name, info))

    @staticmethod
    def _run_stream(name: str) -> str:
        """The durable ledger stream name for a job's finished runs."""
        return RUN_STREAM_PREFIX + name

    @staticmethod
    def _log_stream(name: str) -> str:
        """The durable stream name for a job's archived captured output."""
        return LOG_STREAM_PREFIX + name

    @staticmethod
    def _retry_stream(name: str) -> str:
        """The durable stream name for a job's retry-ladder records."""
        return RETRY_STREAM_PREFIX + name

    @staticmethod
    def _reboot_stream(name: str) -> str:
        """The durable stream name for a job's @reboot boot markers."""
        return REBOOT_STREAM_PREFIX + name

    @staticmethod
    def _pause_stream(name: str) -> str:
        """The durable stream name for a job's pause/resume records."""
        return PAUSE_STREAM_PREFIX + name

    def _counters_stream(self) -> str:
        """The durable stream name for this host's counter snapshots."""
        return COUNTER_STREAM_PREFIX + self._state_host

    async def _persist_run_record(self, name: str, info: JobRunInfo) -> None:
        """Append one finished run to the durable ledger, prune, and archive.

        Runs as a background task (see :meth:`_record_run`).  Errors are logged
        and swallowed: a durability failure must never break job handling, and
        an unhandled exception in a fire-and-forget task would otherwise show
        as a noisy "task exception was never retrieved".  Pruning right after
        the append bounds the stream where it just grew, avoiding a per-minute
        fleet-wide scan.  When the job opts into ``archiveOutput`` the run's
        captured output is archived too, in the same task.
        """
        backend = self.state_backend
        if backend is None:  # torn down between scheduling and running
            return
        stream = self._run_stream(name)
        try:
            # include_series: the ledger is what rehydrates the resource
            # charts after a restart. Bounded per record by the job's
            # monitorResources.history, per stream by the folded prune.
            await asyncio.wait_for(
                backend.append_record(
                    stream,
                    info.to_dict(include_series=True),
                    prune_keep=(
                        self._state_max_runs
                        if self._state_max_runs > 0
                        else None
                    ),
                ),
                timeout=STATE_OP_TIMEOUT,
            )
            job = self.cron_jobs.get(name)
            if job is not None and job.archiveOutput:
                await self._archive_output(job, info)
        except Exception as ex:  # noqa: BLE001 - fire-and-forget; log, survive
            self.metrics.state_write_dropped("run-record")
            logger.warning(
                "state: failed to persist run record for %s: %s", name, ex
            )
        # piggyback the (throttled) durable counter snapshot on the same
        # background task: one finished run is also the moment the counters
        # changed. Has its own error handling.
        await self._persist_counter_snapshot(throttled=True)

    async def _persist_counter_snapshot(
        self, *, throttled: bool = False
    ) -> None:
        """Append a durable snapshot of the Prometheus counter accumulators.

        Host-scoped stream (each node's counters are its own truth); pruned
        to a handful, newest wins on rehydration.  ``throttled`` skips the
        write when one landed within COUNTER_SNAPSHOT_INTERVAL, so a busy
        job cannot double every durable write; the shutdown path writes one
        final unthrottled snapshot.  Lossy by design: a crash forfeits at
        most the events since the last snapshot, which Prometheus reads as
        a small, ordinary counter reset.  Gated on ``_counters_seeded``: a
        run finishing in the window between the backend coming up and the
        seed attempt must not write a snapshot the seed would then read
        back -- ingesting this process's own events twice.
        """
        backend = self.state_backend
        if backend is None or not self._counters_seeded:
            return
        if throttled:
            now = asyncio.get_running_loop().time()
            if now < self._counter_snapshot_next:
                return
            self._counter_snapshot_next = now + COUNTER_SNAPSHOT_INTERVAL
        record = self.metrics.counters_snapshot()
        record["at"] = get_now(datetime.timezone.utc).isoformat()
        stream = self._counters_stream()
        try:
            await asyncio.wait_for(
                backend.append_record(
                    stream, record, prune_keep=COUNTER_STREAM_KEEP
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except Exception as ex:  # noqa: BLE001 - fire-and-forget; log, survive
            self.metrics.state_write_dropped("counters")
            logger.warning(
                "state: failed to persist the counter snapshot: %s", ex
            )

    async def _archive_output(self, job: JobConfig, info: JobRunInfo) -> None:
        """Write a finished run's captured output to the durable log store.

        Opt-in per job (``archiveOutput``).  What is archived is the run's
        live-tail ring buffer -- the newest
        :data:`cronstable.job.LIVE_LOG_LIMIT`
        lines (each already bounded by ``maxLineLength``); older lines were
        evicted from the ring before archiving and are accounted for in the
        record's ``dropped_lines`` rather than silently lost.  A job with
        ``saveLimit: 0`` (the operator's explicit "retain no output") archives
        nothing.  The lines are scrubbed of recognisable secrets
        (:func:`cronstable.redact.redact_lines`, which also tracks multi-line
        PEM private-key blocks) unless the job set
        ``redactArchivedSecrets: false``, then written as one immutable record
        linked to the run by its ``finished_at``.  Encryption-at-rest is the
        mount's job (an encrypted volume, EFS/S3 server-side encryption);
        this only redacts.  Pruned to the same per-job bound as the ledger.
        """
        backend = self.state_backend
        if backend is None:
            return
        if job.saveLimit == 0:
            return
        redact = job.redactArchivedSecrets
        raw = list(info.output.lines)
        if redact:
            # Executor-offloaded: the scrub is pure CPU over job-controlled
            # text (up to LIVE_LOG_LIMIT lines of maxLineLength each), and a
            # pathological line must degrade THIS archive write, not stall
            # job dispatch, the web server and cluster heartbeats.  The
            # patterns themselves are kept linear (see cronstable.redact),
            # so this is defence in depth against a future regex regression.
            texts = await asyncio.get_running_loop().run_in_executor(
                None, redact_lines, [line for _stream, line in raw]
            )
        else:
            texts = [line for _stream, line in raw]
        lines = [
            {"stream": stream_name, "line": text}
            for (stream_name, _), text in zip(raw, texts, strict=True)
        ]
        record = {
            "finished_at": info.finished_at.isoformat(),
            "outcome": info.outcome,
            "exit_code": info.exit_code,
            "redacted": redact,
            "dropped_lines": max(0, info.output.published - len(raw)),
            "lines": lines,
        }
        stream = self._log_stream(job.name)
        # Bounded; the caller (_persist_run_record) catches a timeout as a
        # dropped write rather than letting a wedged mount hang the task.
        await asyncio.wait_for(
            backend.append_record(
                stream,
                record,
                prune_keep=(
                    self._state_max_runs if self._state_max_runs > 0 else None
                ),
            ),
            timeout=STATE_OP_TIMEOUT,
        )

    async def _warm_last_success_beyond_history(
        self, name: str, history: Iterable[JobRunInfo]
    ) -> None:
        """Find the staleness reference outside the warmed history window.

        The rehydrate warms RUN_HISTORY_LIMIT records; a job failing more
        often than that since its last success has no success among them,
        and leaving the reference unset re-baselines maxTimeSinceSuccess
        on process start, so every restart buys a genuinely stale job
        another silent threshold.  Re-read the stream once, deeper (the
        ledger usually still holds the success: maxRunsPerJob defaults an
        order of magnitude above the warm window), and failing that fall
        back to the OLDEST record seen by either read.  Every record seen
        is a non-success, so the true last success is at or before it: a
        lower bound on staleness, which can only page later than the
        truth, never earlier.  Only jobs configuring the check reach here,
        so nothing else pays for the extra read.
        """
        backend = self.state_backend
        seen = [r.finished_at for r in history if r.finished_at is not None]
        if backend is not None:
            deeper = max(SLA_SUCCESS_SCAN_LIMIT, self._state_max_runs)
            try:
                recs = await asyncio.wait_for(
                    backend.list_records(
                        self._run_stream(name),
                        limit=deeper,
                        newest_first=True,
                    ),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception as ex:  # noqa: BLE001 - the floor below stands
                logger.warning(
                    "state: cannot widen the last-success scan for %s "
                    "(falling back to the oldest warmed record): %s",
                    name,
                    ex,
                )
                recs = []
            successes = []
            for restored in (_job_run_info_from_dict(r) for r in recs):
                if restored is None or restored.finished_at is None:
                    continue
                if restored.outcome == "success":
                    successes.append(restored.finished_at)
                seen.append(restored.finished_at)
            if successes:
                self._sla_last_success.setdefault(name, max(successes))
                return
        if seen:
            self._sla_last_success.setdefault(name, min(seen))

    async def _seed_stale_reference(
        self, name: str, history: Iterable[JobRunInfo]
    ) -> None:
        """Warm the maxTimeSinceSuccess staleness reference from ``history``.

        Split out of the rehydrate warm-up so the two early-continue guards
        (a job that already carries in-memory history, or one that finished a
        run while the read awaited) still seed the reference before skipping
        the rest of the warm-up. Without it, a job that FAILED during a state
        outage -- non-empty run_history by the time the store finally comes
        up -- would skip seeding, and _sla_stale_reference would re-baseline
        it on process start: exactly the silent threshold the deep re-read
        exists to prevent, one guard higher. By finished_at, not by position:
        record filenames order on WRITE time and run-record writes are
        unserialized, so the last-APPENDED success can be older than one
        appended before it. setdefault throughout, so a live success recorded
        by _record_run while an await yielded is never clobbered.
        """
        history = list(history)
        successes = [
            r.finished_at
            for r in history
            if r.outcome == "success" and r.finished_at is not None
        ]
        if successes:
            self._sla_last_success.setdefault(name, max(successes))
            return
        # a reload during one of the awaits above can drop the job, so .get()
        # rather than a subscript.
        warmed_job = self.cron_jobs.get(name)
        if warmed_job is not None and (
            warmed_job.sla.get("maxTimeSinceSuccessSeconds") is not None
        ):
            await self._warm_last_success_beyond_history(name, history)

    async def _rehydrate_from_state(self) -> None:
        """Warm the in-memory history from the durable ledger, once, on boot.

        After the backend first starts, load each job's newest records back
        into ``last_run`` and ``run_history`` so ``/status``, ``/jobs`` and the
        dashboard (latest status, sparkline, success-rate stats) are correct
        from the first scrape after a restart instead of blank until the job
        next runs.  Bypasses :meth:`_record_run` deliberately: rehydration must
        not re-emit Prometheus counters or re-persist what it just read.  A
        poison record is skipped by :func:`_job_run_info_from_dict` (and
        quarantined by the backend), never fatal to startup.
        """
        backend = self.state_backend
        if backend is None or self._state_rehydrated:
            return
        self._state_rehydrated = True
        warmed = 0
        for name in list(self.cron_jobs):
            # a job that already accumulated in-memory history this process
            # (unusual at boot) is left as the live source of truth -- but the
            # staleness reference is still seeded from that history, so a job
            # that only FAILED during a state outage does not re-baseline
            # maxTimeSinceSuccess on process start when the store returns.
            if self.run_history.get(name):
                await self._seed_stale_reference(name, self.run_history[name])
                continue
            try:
                recs = await asyncio.wait_for(
                    backend.list_records(
                        self._run_stream(name),
                        limit=RUN_HISTORY_LIMIT,
                        newest_first=True,
                    ),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.TimeoutError:
                # a store that cannot serve one read in STATE_OP_TIMEOUT is
                # unhealthy (hung mount): abandon the whole warm-up rather
                # than stalling boot for jobs x timeout. The dashboard fills
                # in as jobs run, exactly as with no rehydration.
                logger.warning(
                    "state: rehydration timed out reading %s; skipping the "
                    "rest of the warm-up (store unhealthy?)",
                    name,
                )
                break
            except OSError as ex:
                logger.warning(
                    "state: failed to rehydrate history for %s: %s", name, ex
                )
                continue
            if self.run_history.get(name):
                # a run finished while we awaited the read (the await above
                # yields): the live run is fresher than anything in the
                # ledger snapshot; appending the old records after it would
                # regress last_run and scramble the history's order. The
                # staleness reference is still seeded from that live history
                # (setdefault, so a fresher live success is never clobbered).
                await self._seed_stale_reference(name, self.run_history[name])
                continue
            recs.reverse()  # oldest-first, to match the append order
            for rec in recs:
                restored = _job_run_info_from_dict(rec)
                if restored is not None:
                    self.run_history[name].append(restored)
            history = self.run_history.get(name)
            if history:
                self.last_run[name] = history[-1]
                warmed += 1
                # warm the staleness reference too: with a durable ledger
                # the maxTimeSinceSuccess check must page from the REAL last
                # success after a restart, not re-baseline on process start
                # (that grace is only for stateless boots). Shared with the
                # two early-continue guards above, so a job with pre-existing
                # in-memory history is seeded the same way.
                await self._seed_stale_reference(name, history)
                # and the onlyIfLastSucceeded memo, for the same reason the
                # gate keeps one: a pause running across the restart would
                # otherwise leave the warmed ring full of "skipped" rows
                # with no real outcome behind them. By finished_at, not by
                # position (the same unserialized-write hazard as the success
                # scan above): seeding the last-APPENDED real outcome could
                # pick an older success over a newer failure and reopen the
                # very gate this memo exists to hold.
                reals = [
                    r
                    for r in history
                    if r.outcome in ("success", "failure")
                    and r.finished_at is not None
                ]
                if reals:
                    newest = max(reals, key=lambda r: r.finished_at)
                    self._last_real_outcome.setdefault(
                        name, (newest.finished_at, newest.outcome)
                    )
                # and the retry ladder's supersede watermark: last_run is
                # history[-1], which a pause running across the restart
                # makes a "skipped" row with a fresh finished_at. Reading
                # the ladder's guard off that row would settle every
                # pending retry the pause is merely holding.
                for restored in reversed(history):
                    if (
                        restored.outcome != "skipped"
                        and restored.finished_at is not None
                    ):
                        self._last_completed_at.setdefault(
                            name, restored.finished_at
                        )
                        break
        if warmed:
            logger.info(
                "state: rehydrated run history for %d job(s) from the ledger",
                warmed,
            )
        # BEFORE the retry re-arm: a reconciled interrupted run updates
        # last_run, and the superseded-by-run guard must see it.
        await self._reconcile_inflight()
        await self._rehydrate_counters()
        # pause state before the retry re-arm too: a re-armed ladder's gate
        # loop must defer on a durable pause from its very first check.
        await self._refresh_pauses_from_store()
        await self._rehydrate_retries()
        # adopt and reconcile this node's active DAG runs from durable
        # state (the DAG analogue of _reconcile_inflight): a run whose per-task
        # state shows a task interrupted by the crash is resumed from that
        # state, never from memory.
        await self._dag.reconcile_on_boot()

    async def _rehydrate_counters(self) -> None:
        """Seed the Prometheus accumulators from the newest durable snapshot.

        Attempted at most ONCE per process (never per backend generation):
        seeding ADDS into the live accumulators (pre-restart and
        post-restart events are disjoint), so seeding twice -- or, worse,
        seeding from a snapshot THIS process already wrote -- would
        double-count.  The latch is therefore set BEFORE the read: it also
        gates :meth:`_persist_counter_snapshot`, so no snapshot of this
        process's own counters can exist in the store until the one seed
        attempt has finished.  A store unreadable at that instant simply
        forfeits the seed (no retry on a later backend start), which the
        lossy-durable contract allows: counters resume from zero, an
        ordinary counter reset to Prometheus.
        """
        backend = self.state_backend
        if backend is None or self._counters_seeded:
            return
        self._counters_seeded = True
        try:
            recs = await asyncio.wait_for(
                backend.list_records(
                    self._counters_stream(), limit=1, newest_first=True
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade, never crash
            logger.warning(
                "state: cannot rehydrate the metric counters (the seed is "
                "forfeited for this process): %s",
                ex,
            )
            return
        if not recs:
            return
        seeded = self.metrics.seed_counters(recs[0], set(self.cron_jobs))
        if seeded:
            logger.info(
                "state: rehydrated Prometheus counters for %d job(s) from "
                "the durable snapshot",
                seeded,
            )

    async def _rehydrate_retries(self) -> None:
        """Re-arm pending durable retries after a restart.

        The restart-surviving half of the retry ladder: a ``pending`` record
        on top of a job's retry stream is a retry the previous process armed
        but never resolved.  ABSOLUTE-deadline re-arming: the record's
        ``notBefore`` is an instant, so the re-armed task sleeps only the
        remaining time -- zero when the deadline passed while the daemon was
        down.  Invalidation is by PER-JOB config digest
        (:func:`cronstable.fingerprint.job_digest`): stricter than whole-set
        job-set-id invalidation, which would drop every pending retry
        whenever ANY job changed, while a digest mismatch means THIS job's
        behaviour-affecting config changed and its old ladder must not run
        the new definition.  Every ambiguous case settles the ladder (no
        re-arm): with live asyncio ladders, cluster gates, and @reboot
        keep-alives in play, the wrong move here is a double-run, and
        no-run-on-ambiguity is the documented bias.  For an ``@reboot`` job
        the pending retry is re-armed only when the boot marker proves the
        boot run already happened THIS boot (the keep-alive-continuity
        case); when the job will fire fresh at this startup pass, the fresh
        boot run supersedes the stale ladder.  The re-armed task is the
        ordinary :meth:`schedule_retry_job`, so cluster-gate re-checks,
        job-vanished cleanup, and shutdown behaviour are identical to a
        never-restarted ladder.

        Two more guards keep shared and unlucky stores honest: a pending
        record written by ANOTHER host is that host's live business and is
        neither re-armed nor settled here; and a pending record older than
        the job's newest KNOWN run (the history warmed just above, plus
        anything recorded in-memory) is settled as superseded -- the ladder
        demonstrably resolved somehow (perhaps while the store was down and
        the settle write was dropped), and re-running it would be the exact
        double-run this method promises to avoid.
        """
        backend = self.state_backend
        if backend is None:
            return
        for name, job in list(self.cron_jobs.items()):
            if name in self.retry_state or self.running_jobs.get(name):
                # live activity always outranks the ledger
                continue
            try:
                recs = await asyncio.wait_for(
                    backend.list_records(
                        self._retry_stream(name), limit=1, newest_first=True
                    ),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "state: retry re-arm timed out reading %s; skipping "
                    "the rest (store unhealthy?)",
                    name,
                )
                break
            except asyncio.CancelledError:
                raise
            except Exception as ex:  # noqa: BLE001 - degrade, never crash
                logger.warning(
                    "state: cannot read pending retries for %s: %s", name, ex
                )
                continue
            if not recs or recs[0].get("kind") != "pending":
                continue
            rec = recs[0]
            rec_host = rec.get("host")
            if isinstance(rec_host, str) and rec_host != self._state_host:
                # another node's live ladder (shared store): not ours to
                # re-arm OR settle. Cross-node retry resume is a later
                # phase's leased, reconciled affair.
                continue
            if name not in self._last_completed_at:
                # The warmed ring (_rehydrate_from_state, above) is capped at
                # RUN_HISTORY_LIMIT, so a pause holding at least that many
                # slots fills every ring entry with "skipped" rows and floods
                # out the real run that DID resolve this ladder, leaving the
                # memo unset. The superseded-by-run guard would then read
                # None and re-arm a ladder a real run already settled, a
                # double-run this method exists to avoid (and a regression:
                # pre-memo the skip row's fresh finished_at settled it by
                # accident). The durable fold is flood-independent
                # (derive_max over ranAt), so seed the memo from it before the
                # guard reads it. One extra read, only when a pending record
                # actually exists, so steady state is unchanged; it mirrors
                # the deeper-read _warm_last_success_beyond_history sets for
                # the SLA memo.
                try:
                    durable_at = await asyncio.wait_for(
                        self.durable_last_completed_at(name),
                        timeout=STATE_OP_TIMEOUT,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 - unknown -> guard stays open
                    durable_at = None
                parsed = (
                    _parse_iso_utc(durable_at)
                    if isinstance(durable_at, str)
                    else None
                )
                if parsed is not None:
                    self._last_completed_at[name] = parsed
            validated = self._validate_pending_retry(name, job, rec)
            if validated is None:
                continue
            attempt, not_before = validated
            if isinstance(job.schedule, str) and job.schedule == "@reboot":
                try:
                    covered = await self._reboot_marker_covers(job)
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 - unknown -> not covered
                    covered = False
                if not covered:
                    self._persist_retry_settled(
                        name, "superseded-by-reboot", attempt
                    )
                    continue
            retry = job.onFailure["retry"]
            state = JobRetryState(
                retry["initialDelay"],
                retry["backoffMultiplier"],
                retry["maximumDelay"],
            )
            # replay the ladder to the persisted position: count == attempt,
            # delay == what the NEXT failure would sleep.
            for _ in range(attempt):
                state.next_delay()
            now = get_now(datetime.timezone.utc)
            remaining = max(0.0, (not_before - now).total_seconds())
            self.retry_state[name] = state
            state.task = asyncio.create_task(
                self.schedule_retry_job(name, remaining, attempt)
            )
            logger.info(
                "Job %s: re-armed pending retry #%d from the durable "
                "ledger (due in %.1f seconds)",
                name,
                attempt,
                remaining,
            )

    def _validate_pending_retry(
        self, name: str, job: JobConfig, rec: Dict[str, Any]
    ) -> Optional[Tuple[int, datetime.datetime]]:
        """Judge a pending-retry record against the LIVE job definition.

        Returns ``(attempt, notBefore)`` when the ladder may be re-armed;
        ``None`` after settling it with the reason it must not be:
        unparseable content, retries disabled or the job's config digest
        changed since arming, a newer run proving the ladder resolved,
        the job disabled, the budget exhausted, or the record staler than
        the job's own startingDeadlineSeconds window.
        """
        retry = job.onFailure["retry"]
        attempt = rec.get("attempt")
        not_before = _parse_iso_utc(rec.get("notBefore"))
        if (
            isinstance(attempt, bool)
            or not isinstance(attempt, int)
            or attempt < 1
            or not_before is None
        ):
            self._persist_retry_settled(name, "invalid-record")
            return None
        if not retry["maximumRetries"] or rec.get("jobDigest") != job_digest(
            job
        ):
            # retries disabled since arming, or any behaviour-affecting
            # field changed: the old ladder must not run the new definition
            # (nor lurk until a later config revert).
            self._persist_retry_settled(name, "config-changed", attempt)
            return None
        # a handoff carries the original arm time in ``armedAt`` (a pending has
        # only ``at``, which for a pending IS its arm time).
        armed_at = (
            _parse_iso_utc(rec.get("armedAt"))
            or _parse_iso_utc(rec.get("at"))
            or not_before
        )
        # the newest ACTUAL run, not last_run: a pause-held slot appends a
        # "skipped" row stamped now, and reading last_run here would settle
        # every ladder the pause is only holding until the resume. It must
        # still be the newest non-skipped instant rather than "ignore the
        # guard when last_run is skipped", or a real run buried under a
        # later pause would go unseen and the ladder would double-run.
        last_at = self._last_completed_at.get(name)
        if last_at is not None and last_at > armed_at:
            # a run finished AFTER this retry was armed: the ladder was
            # resolved some way (its settle may have been dropped while
            # the store was down). No-run beats double-run.
            self._persist_retry_settled(name, "superseded-by-run", attempt)
            return None
        if not job.enabled:
            self._persist_retry_settled(name, "disabled", attempt)
            return None
        maximum = retry["maximumRetries"]
        if maximum != -1 and attempt > maximum:
            self._persist_retry_settled(name, "exhausted", attempt)
            return None
        now = get_now(datetime.timezone.utc)
        deadline = job.startingDeadlineSeconds
        if deadline and (now - not_before).total_seconds() > deadline:
            # same bound catch-up honours: a retry stale beyond the job's
            # own catch-up window is not worth replaying.
            self._persist_retry_settled(name, "deadline-passed", attempt)
            return None
        return attempt, not_before

    async def durable_last_run_at(self, name: str) -> Optional[str]:
        """The last finished-run timestamp for a job, from the durable ledger.

        The restart-surviving "last fired" watermark, derived as the max
        ``finished_at`` over the immutable records (order-independent, so it is
        correct even when several nodes append to one job's stream on a shared
        mount).  ISO-8601 UTC, so a lexicographic max is a chronological max.
        ``None`` with no backend or no records.  Consumed by the missed-run
        catch-up; the ledger it reads is the durable run-record stream.
        """
        backend = self.state_backend
        if backend is None:
            return None
        result = await backend.derive_max(
            self._run_stream(name), "finished_at"
        )
        return result if isinstance(result, str) else None

    async def durable_last_completed_at(self, name: str) -> Optional[str]:
        """The last ACTUAL-run timestamp for a job, from the durable ledger.

        :meth:`durable_last_run_at`'s skip-blind twin, for the cross-node
        superseded-by-run guard.  ``finished_at`` is stamped on synthetic
        ``skipped`` rows too (deliberately: the catch-up watermark must
        advance over a pause rather than owe every held slot on resume), so
        folding it here would let a pause settle every ladder the pause is
        only holding.  This folds ``ranAt`` instead, which
        :meth:`JobRunInfo.to_dict` writes on run rows only.

        Records appended before ``ranAt`` existed carry only ``finished_at``,
        and dropping them would re-arm ladders an upgrade's own ledger
        already proves resolved, so the newest window of them is folded in
        by outcome.  No pause could have written a ``skipped`` row into that
        older shape, and a row carrying ``ranAt`` is already folded above.

        Read by the claim scan (only when a foreign stale ladder actually
        exists) AND by the local retry rehydrate
        (:meth:`_rehydrate_retries`), so both the cross-node and single-node
        superseded-by-run paths see the same resolved-ladder truth.  The
        ``ranAt`` fold above is one ``derive_max`` and the capped fold below
        one bounded read, so steady state pays two reads; the deeper re-read
        fires ONLY on the pre-``ranAt`` None path, off that steady state.
        """
        backend = self.state_backend
        if backend is None:
            return None
        stream = self._run_stream(name)
        derived = await backend.derive_max(stream, "ranAt")
        best = derived if isinstance(derived, str) else None

        def _fold_pre_ranat(
            records: List[Dict[str, Any]], acc: Optional[str]
        ) -> Optional[str]:
            # A row with no ``ranAt`` and outcome != skipped is a real run
            # from before ``ranAt`` existed (a pause-skip row never took that
            # older shape); fold its finished_at, a row carrying ``ranAt`` is
            # already in the derive_max above.
            for rec in records:
                if "ranAt" in rec or rec.get("outcome") == "skipped":
                    continue
                at = rec.get("finished_at")
                if isinstance(at, str) and (acc is None or at > acc):
                    acc = at
            return acc

        recs = await backend.list_records(
            stream, limit=RUN_HISTORY_LIMIT, newest_first=True
        )
        best = _fold_pre_ranat(recs, best)
        if best is None:
            # KNOWN BOUND, now closed for the common case: a ledger written
            # entirely before ``ranAt`` existed, then buried under at least
            # RUN_HISTORY_LIMIT pause-skip rows, leaves every real row outside
            # the capped window above and carrying no ``ranAt`` for derive_max
            # to catch, so it folds to None. Both the LOCAL retry rehydrate
            # (via _rehydrate_retries) and a peer's claim scan would then
            # re-arm a ladder the ledger already proves resolved. Re-read ONCE,
            # deeper, on this None path only, mirroring
            # _warm_last_success_beyond_history: any post-upgrade run row
            # carries ``ranAt`` and keeps this off the steady-state path. A
            # residual bound survives only past `deeper` pre-``ranAt`` rows,
            # which one post-upgrade real run heals.
            deeper = max(SLA_SUCCESS_SCAN_LIMIT, self._state_max_runs)
            deep = await backend.list_records(
                stream, limit=deeper, newest_first=True
            )
            best = _fold_pre_ranat(deep, best)
        return best

    async def _list_gate_records(
        self, backend: Any, name: str
    ) -> List[Dict[str, Any]]:
        """Newest durable run records for the onlyIfLastSucceeded gate.

        Read incrementally: the gate consumes only the single newest
        success/failure record, but non-run outcomes (cancelled/skipped) at the
        head must be skipped over.  Probe :data:`DEPENDS_GATE_PROBE` newest
        records first and widen to the full :data:`RUN_HISTORY_LIMIT` window
        ONLY when that page is entirely non-run outcomes -- so the common case
        (the newest record is a real outcome) materialises a few records
        instead of all 50, without shrinking the skip window a real answer
        may need.

        Raises through to the caller's fail-closed/degrade handling on a store
        error or timeout, exactly as the single read it replaces.
        """
        stream = self._run_stream(name)
        probe = min(DEPENDS_GATE_PROBE, RUN_HISTORY_LIMIT)
        recs: List[Dict[str, Any]] = await asyncio.wait_for(
            backend.list_records(stream, limit=probe, newest_first=True),
            timeout=STATE_OP_TIMEOUT,
        )
        # a real outcome in the probe page, or a page shorter than the probe
        # (the stream is exhausted), means widening would reveal nothing newer.
        if len(recs) < probe or any(
            r.get("outcome") in ("success", "failure") for r in recs
        ):
            return recs
        widened: List[Dict[str, Any]] = await asyncio.wait_for(
            backend.list_records(
                stream, limit=RUN_HISTORY_LIMIT, newest_first=True
            ),
            timeout=STATE_OP_TIMEOUT,
        )
        return widened

    async def _depends_on_past_ok(self, job: JobConfig) -> bool:
        """Whether ``job``'s depends-on-past gate permits a scheduled fire.

        ``True`` (allow) unless ``onlyIfLastSucceeded`` is set AND the job's
        most recent *run* outcome was a failure -- or its previous instance is
        STILL RUNNING (an unfinished run has not "succeeded", and letting the
        answer depend on whether it happens to finish first would make the
        gate a race).  The last real outcome is the NEWEST of two sources,
        by ``finished_at``:

        * the in-memory history (``run_history``), which the finished-run
          path updates synchronously -- the durable write behind it is
          fire-and-forget, so the ledger alone can be a beat stale and would
          re-run a job whose failure record is still in flight;
        * the durable ledger, which sees runs from OTHER nodes on a shared
          mount (guarded and bounded: a store error/timeout degrades to the
          in-memory view with a warning -- fail open, like "no backend" --
          rather than stalling or crashing the launch path, which runs
          outside run()'s try/except).

        Non-run outcomes (``cancelled``/``skipped``) are skipped in both, so
        a skipped tick does not itself clear the gate and only a genuine
        success re-opens it -- within each source's bounded window
        (:data:`RUN_HISTORY_LIMIT` newest entries), which a pathological pile
        of consecutive non-run records could in principle exhaust.  No prior
        run in either source -> allow (there is nothing to depend on, and a
        first-ever run must not be blocked).  Without a state backend the
        gate still works from the in-memory history -- it simply is not
        restart-surviving (history resets with the process).

        The still-running block is SKIPPED for ``concurrencyPolicy:
        Replace``: that policy's contract is that a new fire supersedes the
        running instance (its reaping happens in :meth:`maybe_launch_job`),
        so blocking here would let one hung run freeze the job forever; the
        gate then judges the last *finished* outcome, as it always did.
        Applies to scheduled and @reboot fires
        (:meth:`launch_scheduled_job`); retries, catch-up backfills, and
        manual API triggers deliberately bypass it.
        """
        if not job.onlyIfLastSucceeded:
            return True
        if job.concurrencyPolicy != "Replace" and self.running_jobs.get(
            job.name
        ):
            return False
        # The newest real outcome by finished_at, NOT by list position: run
        # records are written unserialized (two concurrencyPolicy: Allow
        # instances, or a peer node on a shared mount), so the last-APPENDED
        # real run can be OLDER than one appended before it. A positional
        # `reversed(...); break` walk would take that newer-by-position stale
        # record and clear the gate on a failure that is actually the newest.
        reals = [
            info
            for info in (self.run_history.get(job.name) or ())
            if info.outcome in ("success", "failure")
            and info.finished_at is not None
        ]
        newest = max(reals, key=lambda i: i.finished_at, default=None)
        latest: Optional[Tuple[datetime.datetime, str]] = (
            (newest.finished_at, newest.outcome)
            if newest is not None
            else None
        )
        # the ring above is bounded, so enough consecutive non-run rows (a
        # pause writes one per held slot) evict the last real outcome from
        # it entirely. The memo survives that eviction; take whichever is
        # newer so a genuine success recorded after it still wins.
        memo = self._last_real_outcome.get(job.name)
        if memo is not None and (latest is None or memo[0] > latest[0]):
            latest = memo
        backend = self.state_backend
        if (
            backend is None
            and self._state_configured
            and self._state_on_unavailable == "fail-closed"
        ):
            # the store holds the durable truth this gate exists for, it is
            # configured but down, and the operator asked for fail-closed:
            # prefer not running over deciding from a possibly-stale memory.
            logger.warning(
                "Job %s: onlyIfLastSucceeded blocked: the state store is "
                "configured but unavailable and onStoreUnavailable is "
                "fail-closed",
                job.name,
            )
            return False
        if backend is not None:
            try:
                recs = await self._list_gate_records(backend, job.name)
            except asyncio.CancelledError:
                raise
            except Exception as ex:  # noqa: BLE001 - policy decides below
                if self._state_on_unavailable == "fail-closed":
                    logger.warning(
                        "Job %s: onlyIfLastSucceeded blocked: cannot read "
                        "the run ledger (%s) and onStoreUnavailable is "
                        "fail-closed",
                        job.name,
                        ex,
                    )
                    return False
                logger.warning(
                    "state: cannot read the run ledger for the "
                    "onlyIfLastSucceeded gate on %s (%s); deciding from the "
                    "in-memory history",
                    job.name,
                    ex,
                )
                recs = []
            for rec in recs:
                outcome = rec.get("outcome")
                if outcome not in ("success", "failure"):
                    continue
                finished = _parse_iso_utc(rec.get("finished_at"))
                candidate = (
                    finished
                    or datetime.datetime.min.replace(
                        tzinfo=datetime.timezone.utc
                    ),
                    str(outcome),
                )
                # Fold over ALL real records (already bounded to
                # RUN_HISTORY_LIMIT) and keep the max by finished_at, NOT the
                # first-by-sequence then break: an out-of-order write (a peer,
                # or two Allow instances racing) can land a newer success
                # ahead of the true-newest failure by sequence, and a
                # first-real-then-break would clear the gate on that stale
                # success while never examining the newer failure behind it.
                if latest is None or candidate[0] > latest[0]:
                    latest = candidate
        if latest is None:
            return True
        return latest[1] == "success"

    async def _handle_finished_job(self, job: RunningJob) -> None:
        if getattr(job, "dag_ref", None) is not None:
            # a DAG task instance, not a scheduled job. Route its
            # completion to the DAG scheduler (which records the durable
            # per-task transition and advances the graph) and skip the whole
            # job record/retry/inflight/cluster-slot path -- a task's lifecycle
            # lives in its dag_run document, not the job streams.
            await self._handle_finished_dag_task(job)
            return
        jobs_list = self.running_jobs[job.config.name]
        jobs_list.remove(job)
        last_instance = not jobs_list
        if not jobs_list:
            del self.running_jobs[job.config.name]
        if last_instance and self.state_backend is not None:
            # the job went 1 -> 0 live instances here: close the in-flight
            # record. Fire-and-forget; runs before the replaced/cancelled
            # early-returns below on purpose -- a replaced instance ending
            # the job's last local instance must still close the record.
            # Ordered behind the open so a near-instant run's close cannot
            # sort ahead of it (see _inflight_write_tail).
            self._queue_inflight_write(
                job.config.name,
                self._persist_inflight_closed(job.config.name),
            )
        if job.config.concurrencyScope == "cluster":
            # every claimed launch pairs with exactly one finish here; the
            # slot lease is released when the refcount drains (see
            # _release_cluster_slot). Before the early-returns below for
            # the same reason as the in-flight close.
            await self._release_cluster_slot(job.config)

        if self._job_api is not None and job.state_token is not None:
            # revoke this run's loopback token and staged secrets and
            # release any mutex/semaphore it still holds. Before the early
            # returns below (a replaced or cancelled run must clean up too),
            # and paired one-to-one with the _prepare_job_api_run at launch.
            await self._job_api.finish_run(job.state_token)

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
                    resource_usage=getattr(job, "resource_usage", None),
                ),
            )
            await self.cancel_job_retries(job.config.name, settle="cancelled")
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
                resource_usage=getattr(job, "resource_usage", None),
            ),
        )
        self._queue_job_completion(job, failed=fail_reason is not None)

    def _queue_job_completion(self, job: RunningJob, *, failed: bool) -> None:
        """Run a finished job's report+retry-arm sequence as a tracked task.

        Reporters (SMTP, webhooks, shell commands) legitimately take seconds
        (bounded only in the tens of seconds) and used to run inline on
        the reaper, the daemon's single job-completion loop, so one slow
        reporter delayed every other job's completion handling, slot release
        and retry arming daemon-wide.  Spawned per finished run instead,
        chained behind the same job's previous sequence (the idiom of
        :meth:`_queue_inflight_write`): the retry-ladder handling for
        overlapping instances of one job keeps the reaper's old serial
        semantics, while distinct jobs no longer wait on each other.
        Tracked in ``_completion_tasks`` so shutdown drains the in-flight
        reports after the running-job drain (see :meth:`_drain_completions`).
        """
        name = job.config.name
        prev = self._completion_tail.get(name)

        async def _sequenced() -> None:
            if prev is not None and not prev.done():
                await asyncio.wait({prev})
            try:
                if failed:
                    await self.handle_job_failure(job)
                else:
                    await self.handle_job_success(job)
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "Unexpected error handling the completion of job %s; "
                    "please report this as a bug (5)",
                    name,
                )

        task = asyncio.create_task(_sequenced())
        self._completion_tasks.add(task)
        task.add_done_callback(self._completion_tasks.discard)
        self._completion_tail[name] = task

        def _clear(done: asyncio.Task) -> None:
            if self._completion_tail.get(name) is done:
                del self._completion_tail[name]

        task.add_done_callback(_clear)

    async def _drain_completions(self) -> None:
        """Await every in-flight report+retry-arm sequence.

        The shutdown path runs this after the running-job drain, unbounded
        exactly as the reaper's old inline awaits were: reports for runs that
        finished before the stop signal must still go out.  Also the seam
        tests use to observe completion side effects deterministically.
        """
        while self._completion_tasks:
            await asyncio.wait(set(self._completion_tasks))

    async def _handle_finished_dag_task(self, job: RunningJob) -> None:
        """Reap one finished DAG task instance (see ``_handle_finished_job``).

        Removes it from the running set, drops its loopback token (and
        any lock it still holds), then hands the outcome to the DAG scheduler,
        which records the durable per-task transition and advances the graph.
        Writes no ``runs/`` / ``retries/`` / ``inflight/`` records: a DAG
        task's whole lifecycle lives in its ``dag_run`` document.
        """
        jobs_list = self.running_jobs.get(job.config.name)
        if jobs_list is not None:
            try:
                jobs_list.remove(job)
            except ValueError:  # pragma: no cover - defensive
                pass
            if not jobs_list:
                del self.running_jobs[job.config.name]
        if self._job_api is not None and job.state_token is not None:
            await self._job_api.finish_run(job.state_token)
        try:
            await self._dag.on_task_finished(job)
        except Exception:  # noqa: BLE001 - never kill the reaper
            logger.exception("dag: failed to record a task completion")

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
                self._reap_retry_task(job.config.name, state.task)
            else:
                state.task.cancel()
        retry = job.config.onFailure["retry"]
        if (
            state.count >= retry["maximumRetries"]
            and retry["maximumRetries"] != -1
        ):
            await self.cancel_job_retries(job.config.name, settle="exhausted")
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
        # Persist the pending retry (fire-and-forget, ordered behind the
        # job's earlier ladder writes) with its ABSOLUTE deadline, so a
        # restart re-arms it with only the remaining delay (see
        # _rehydrate_retries). A write that never lands simply loses the
        # durability (the retry dies with the process, exactly the
        # pre-durable behaviour); later ladder writes are ordered after it
        # via the per-job write chain (_queue_retry_write).
        pending_job = self.cron_jobs.get(job_name)
        if pending_job is not None:
            now_arm = get_now(datetime.timezone.utc)
            not_before = now_arm + datetime.timedelta(seconds=delay)
            self._persist_retry_pending(pending_job, retry_num, not_before)
            # record the armed retry's absolute fire time so GET /jobs can
            # render a live next-retry countdown (see _job_to_dict), and the
            # arm instant so a later cross-node hand-off can anchor its
            # superseded-by-run guard on when the attempt was ARMED rather than
            # on the hand-off instant (see _abandon_retry).
            armed_state = self.retry_state.get(job_name)
            if armed_state is not None:
                armed_state.next_retry_at = not_before
                armed_state.scheduled_delay = delay
                armed_state.armed_at = now_arm
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
                self._persist_retry_settled(job_name, "job-removed", retry_num)
                return
            # A paused job DEFERS its pending retry exactly like a transient
            # gate denial below: the attempt waits and fires after the
            # resume, never consumed or cancelled by the pause (a pause is
            # "hold my fires", not a verdict on the ladder). One gate covers
            # boot-rehydrated ladders too, since they re-arm through here.
            pause = self._pause_active(job_name)
            if pause is not None:
                state = self.retry_state.get(job_name)
                if (
                    state is None
                    or state.cancelled
                    or self._stop_event.is_set()
                ):
                    return
                recheck = max(delay, RETRY_GATE_RECHECK_FLOOR)
                log = logger.info if deferrals == 0 else logger.debug
                deferrals += 1
                log(
                    "Cron job %s retry (#%i) deferred: the job is paused "
                    "until %s; re-checking in %.1f seconds",
                    job_name,
                    retry_num,
                    pause.until.isoformat(),
                    recheck,
                )
                await asyncio.sleep(recheck)
                continue
            # Re-check the leadership gate before relaunching: a retry can
            # outlive the leadership it started under (a partition / quorum
            # loss / reload moved ownership while we slept), and
            # maybe_launch_job does NOT gate. Relaunching unconditionally
            # would run a Leader-policy job here while the new owner also
            # runs it on its next tick -- the exact double-run the
            # abstraction promises to prevent.
            if self._cluster_allows(job):
                # Settle the durable pending record BEFORE launching (the
                # same record-before-run ordering as the @reboot marker):
                # a crash right after the launch must find the ladder
                # settled, not re-arm the attempt that already ran. Under
                # onStoreUnavailable: fail-closed an unsettleable record
                # defers the launch like a closed gate; under degrade it
                # launches anyway (at-least-once, bounded replay). When
                # cross-node retry resume is active the decision also
                # serializes on the per-job claim lease and re-checks that
                # the newest ladder record is still OUR OWN pending -- a
                # peer that claimed this ladder while we slept or deferred
                # ends it here ("abort") without settling, so the
                # claimer's record stays newest.
                decision = await self._retry_consume_decision(
                    job, retry_num, quiet=deferrals > 0
                )
                if decision == "launch":
                    break
                if decision == "abort":
                    state = self.retry_state.get(job_name)
                    if state is not None:
                        state.cancelled = True
                    self.retry_state.pop(job_name, None)
                    logger.warning(
                        "Cron job %s retry (#%i) dropped: another node "
                        "claimed this retry ladder (cross-node retry "
                        "resume); it fires there",
                        job_name,
                        retry_num,
                    )
                    return
            elif self._cluster_owner_moved(job):
                # ownership genuinely moved: end this node's retry sequence
                # (on a shared store the ladder is handed off for the new
                # owner to resume; otherwise the new owner picks up only
                # the job's future scheduled firings -- see _abandon_retry).
                self._abandon_retry(job, retry_num)
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

    def _persist_retry_pending(
        self,
        job: JobConfig,
        attempt: int,
        not_before: datetime.datetime,
    ) -> Optional[asyncio.Task]:
        """Fire-and-forget append of a pending-retry record for ``job``.

        Carries the ABSOLUTE deadline (``notBefore``) and the job's config
        digest, which is everything a restart needs to re-arm the ladder at
        the right position (the delay ladder itself is a pure function of
        the retry config and the attempt number).  Returns the write task so
        the caller can ORDER later ladder writes after it (never to gate on
        its success).
        """
        if self.state_backend is None:
            self._note_retry_write_dropped(job.name, "pending")
            return None
        record = {
            "kind": "pending",
            "attempt": attempt,
            "notBefore": not_before.isoformat(),
            "jobDigest": job_digest(job),
            # the arming node: on a shared store another node's boot must
            # neither re-arm this ladder (its owner is alive) nor settle it.
            "host": self._state_host,
            "at": get_now(datetime.timezone.utc).isoformat(),
        }
        return self._queue_retry_write(job.name, record)

    def _persist_retry_settled(
        self, name: str, reason: str, attempt: Optional[int] = None
    ) -> None:
        """Fire-and-forget append of a settled-ladder record for ``name``.

        Whatever ended the ladder (success, supersession, exhaustion,
        abandonment, an invalidation at re-arm time) writes one of these on
        top of the stream so the next boot finds nothing pending.
        """
        if self.state_backend is None:
            self._note_retry_write_dropped(name, reason)
            return
        record: Dict[str, Any] = {
            "kind": "settled",
            "reason": reason,
            "host": self._state_host,
            "at": get_now(datetime.timezone.utc).isoformat(),
        }
        if attempt is not None:
            record["attempt"] = attempt
        self._queue_retry_write(name, record)

    def _note_retry_write_dropped(self, name: str, what: str) -> None:
        """Make a retry-ladder write dropped for want of a backend VISIBLE.

        Only when a ``state`` section is configured (stateless installs
        write nothing by design): the store being down/rebuilding here can
        leave a stale ``pending`` on top of the stream, which a later boot
        would resurrect were it not for the superseded-by-run re-arm guard
        -- worth a counter and a line, never silence.
        """
        if not self._state_configured:
            return
        self.metrics.state_write_dropped("retry")
        logger.warning(
            "state: dropping retry-ladder record (%s) for %s: the state "
            "store is unavailable",
            what,
            name,
        )

    def _queue_retry_write(
        self, name: str, record: Dict[str, Any]
    ) -> asyncio.Task:
        """Queue a retry-stream write ORDERED after the job's previous one.

        Newest-record-wins makes ordering load-bearing: two unordered
        fire-and-forget appends (a pending and the settle racing it) run on
        separate worker threads and could land filename-inverted, leaving
        ``pending`` newest and resurrecting a consumed retry on the next
        boot.  Chaining each job's writes behind the previous one keeps the
        stream's order equal to the ladder's event order.
        """
        prev = self._retry_write_tail.get(name)

        async def _ordered() -> None:
            if prev is not None and not prev.done():
                # ordering only; the previous write handles its own errors
                # and _track_state_write tasks never raise.
                await asyncio.wait({prev})
            await self._append_retry_record(name, record)

        task = self._track_state_write(_ordered())
        self._retry_write_tail[name] = task

        def _clear(done: asyncio.Task) -> None:
            if self._retry_write_tail.get(name) is done:
                del self._retry_write_tail[name]

        task.add_done_callback(_clear)
        return task

    async def _append_retry_record(
        self, name: str, record: Dict[str, Any]
    ) -> None:
        backend = self.state_backend
        if backend is None:  # torn down between scheduling and running
            self._note_retry_write_dropped(name, str(record.get("kind")))
            return
        stream = self._retry_stream(name)
        try:
            await backend.append_record(
                stream, record, prune_keep=RETRY_STREAM_KEEP
            )
        except Exception as ex:  # noqa: BLE001 - fire-and-forget; log, survive
            self.metrics.state_write_dropped("retry")
            logger.warning(
                "state: failed to persist retry state for %s: %s", name, ex
            )

    def _persist_pause(self, name: str, info: "PauseInfo") -> None:
        """Fire-and-forget append of a durable ``paused`` record.

        ``host`` is audit info ONLY: unlike retry records, a pause is
        honored by every node sharing the store (see
        :data:`PAUSE_STREAM_PREFIX`).  Without a backend the pause still
        holds in this process's memory, exactly like the classic stateless
        behaviour of every other runtime nicety, and when a store IS
        configured the record is held for replay (see
        :meth:`_defer_pause_write`).
        """
        record = {
            "kind": "paused",
            "since": info.since.isoformat(),
            "until": info.until.isoformat(),
            "note": info.note,
            "by": info.by,
            "channel": info.channel,
            "at": get_now(datetime.timezone.utc).isoformat(),
            "host": self._state_host,
        }
        if self.state_backend is None:
            self._defer_pause_write(name, record)
            return
        self._queue_pause_write(name, record)

    def _persist_resume(self, name: str, by: str, channel: str) -> None:
        """Fire-and-forget append of a durable ``resumed`` record."""
        record = {
            "kind": "resumed",
            "by": by,
            "channel": channel,
            "at": get_now(datetime.timezone.utc).isoformat(),
            "host": self._state_host,
        }
        if self.state_backend is None:
            self._defer_pause_write(name, record)
            return
        self._queue_pause_write(name, record)

    def _defer_pause_write(self, name: str, record: Dict[str, Any]) -> None:
        """Hold a pause record for replay once the store comes back.

        A configured store that is down is transient: ``start_stop_state``
        retries it every housekeeping pass.  Dropping the record meanwhile
        would leave the stream's newest record contradicting memory, and the
        refresh that follows the store's return would then quietly revert
        the operator's pause (or resume): the exact opposite of the "keep
        the last known in-memory state" contract, which covers only failed
        READS.  An append that FAILS against a live store buffers the same
        way (the record is owed either way) and the housekeeping pass retries
        it.  Newest-per-job wins here as it does in the stream itself, so
        a pause/resume pair taken during the outage collapses to the final
        intent.  Stateless installs (no ``state`` section) buffer nothing
        and say nothing: memory-only is their contract, but a buffer left
        from before the ``state`` section was removed is DISCARDED here, or
        a pause held from the outage would be replayed as fresh intent when
        the section returns and re-pause a job resumed in between.  Bumping
        the write generation keeps a refresh already reading this job's
        stream from applying its now-stale snapshot.
        """
        if not self._state_configured:
            self._pause_pending_writes.pop(name, None)
            return
        self._pause_gen[name] = self._pause_gen.get(name, 0) + 1
        self._pause_pending_writes[name] = record
        self.metrics.state_write_dropped("pause")
        logger.warning(
            "state: cannot write the %s of job %s; it holds in memory only "
            "and will be written when the store accepts it",
            record.get("kind"),
            name,
        )

    def _replay_pending_pause_writes(self) -> None:
        """Queue the pause records buffered by an outage or a failed write.

        Called from :meth:`start_stop_state` with a fresh backend in hand and
        BEFORE the rehydrate's refresh pass, and again from every
        :meth:`_pause_periodic` pass while a backend is up (which is what
        retries an append that failed against a live store).  Both callers
        run it before the refresh, so each replayed write installs its
        :attr:`_pause_write_tail` entry first and that refresh leaves the
        job's memory alone instead of reverting it to the stale record still
        on top of the stream.
        """
        pending = list(self._pause_pending_writes.items())
        self._pause_pending_writes.clear()
        for name, record in pending:
            logger.info(
                "state: writing the %s of job %s held in memory only",
                record.get("kind"),
                name,
            )
            self._queue_pause_write(name, record)

    def _queue_pause_write(
        self, name: str, record: Dict[str, Any]
    ) -> asyncio.Task:
        """Queue a pause-stream write ORDERED after the job's previous one.

        The :meth:`_queue_retry_write` idiom: newest-record-wins makes
        ordering load-bearing (a resume racing its pause could land
        filename-inverted and leave ``paused`` newest forever).  The tail
        entry doubles as the "local write in flight" signal the
        housekeeping refresh consults, and the generation bump is the
        edge-triggered half of that signal (see :attr:`_pause_gen`).
        """
        self._pause_gen[name] = self._pause_gen.get(name, 0) + 1
        prev = self._pause_write_tail.get(name)

        async def _ordered() -> None:
            if prev is not None and not prev.done():
                # ordering only; the previous write handles its own errors
                # and _track_state_write tasks never raise.
                await asyncio.wait({prev})
            await self._append_pause_record(name, record)

        task = self._track_state_write(_ordered())
        self._pause_write_tail[name] = task

        def _clear(done: asyncio.Task) -> None:
            if self._pause_write_tail.get(name) is done:
                del self._pause_write_tail[name]

        task.add_done_callback(_clear)
        return task

    async def _append_pause_record(
        self, name: str, record: Dict[str, Any]
    ) -> None:
        backend = self.state_backend
        if backend is None:  # torn down between queueing and running
            self._defer_pause_write(name, record)
            return
        stream = self._pause_stream(name)
        try:
            await backend.append_record(
                stream, record, prune_keep=PAUSE_STREAM_KEEP
            )
        except Exception as ex:  # noqa: BLE001 - fire-and-forget; log, survive
            logger.warning(
                "state: failed to persist pause state for %s: %s", name, ex
            )
            # a dropped pause write is not just a lost audit row: the record
            # this one meant to supersede is still newest in the stream, so
            # the next refresh would quietly revert the operator's intent.
            # Buffer it (which also counts the drop and bumps the generation)
            # and let the housekeeping pass retry the write.
            self._defer_pause_write(name, record)

    async def _retry_consume_ok(
        self, job_name: str, retry_num: int, *, quiet: bool
    ) -> bool:
        """Settle the pending-retry record ahead of the launch; may defer.

        ``True`` -> proceed with the launch.  The bounded settle write is
        the record-before-run half of restart-durable retries: once it
        lands, a crash cannot re-arm the attempt that is about to run.
        When it cannot land: the default ``degrade`` policy launches anyway
        (at-least-once -- a crash in the narrow window before the record
        is retried by a later settle could replay this one attempt after a
        restart), while ``fail-closed`` reports False so the caller defers
        the launch and re-checks, exactly like a closed cluster gate.
        Stateless (no ``state`` section) is always ``True`` with no I/O.
        """
        backend = self.state_backend
        fail_closed = (
            self._state_configured
            and self._state_on_unavailable == "fail-closed"
        )
        if backend is None:
            if fail_closed:
                if not quiet:
                    logger.warning(
                        "Cron job %s retry (#%i) deferred: the state store "
                        "is unavailable and onStoreUnavailable is "
                        "fail-closed",
                        job_name,
                        retry_num,
                    )
                return False
            return True
        # Order behind any in-flight ladder write (the pending append for
        # this very attempt, with a tiny/zero delay): the settle below must
        # sort newest. Bounded; a wedged earlier write only costs ordering,
        # and the superseded-by-run re-arm guard is the backstop.
        prev = self._retry_write_tail.get(job_name)
        if prev is not None and not prev.done():
            try:
                await asyncio.wait_for(
                    asyncio.shield(prev), timeout=STATE_OP_TIMEOUT
                )
            except asyncio.TimeoutError:
                pass
        record = {
            "kind": "settled",
            "reason": "launched",
            "attempt": retry_num,
            "host": self._state_host,
            "at": get_now(datetime.timezone.utc).isoformat(),
        }
        stream = self._retry_stream(job_name)
        try:
            # the stream bound rides along inside the append's worker call;
            # the backend applies it only after the append LANDED and
            # swallows its own failures, so it cannot affect the settle
            # decision below.
            await asyncio.wait_for(
                backend.append_record(
                    stream, record, prune_keep=RETRY_STREAM_KEEP
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - policy decides below
            self.metrics.state_write_dropped("retry")
            if fail_closed:
                if not quiet:
                    logger.warning(
                        "Cron job %s retry (#%i) deferred: cannot settle "
                        "its durable record (%s) and onStoreUnavailable "
                        "is fail-closed",
                        job_name,
                        retry_num,
                        ex,
                    )
                return False
            logger.warning(
                "state: cannot settle the durable record for %s retry "
                "(#%i) (%s); launching anyway (a crash could replay this "
                "attempt after a restart)",
                job_name,
                retry_num,
                ex,
            )
            return True
        return True

    # --- cross-node retry resume -------------------------------------------

    def _retry_resume_active(self) -> bool:
        """Whether cross-node retry resume applies right now.

        Needs a SHARED store (other nodes can see the ladder records at
        all), leader election (ownership is what moves), and a live
        manager (the claim scan gates on ``_cluster_allows``).
        """
        backend = self.state_backend
        return (
            backend is not None
            and backend.supports_shared_locking()
            and self._elect_leader_configured
            and self.cluster_manager is not None
        )

    def _retry_cross_node_eligible(self, job: JobConfig) -> bool:
        """Whether ``job``'s retry ladder may move between nodes.

        ``EveryNode`` ladders are strictly per-node (every node runs its
        own copy; a foreign pending on the shared stream is another node's
        live ladder, exactly as in rehydration).  ``@reboot`` ladders are
        anchored to a HOST's boot (the re-arm validity is judged against
        this host's boot marker), so they never move either -- an
        abandoned @reboot keep-alive still ends cluster-wide, as
        documented.
        """
        return (
            self._retry_resume_active()
            and job.clusterPolicy != "EveryNode"
            and not (
                isinstance(job.schedule, str) and job.schedule == "@reboot"
            )
        )

    @staticmethod
    def _retry_claim_lease(name: str) -> str:
        return RETRY_CLAIM_PREFIX + name

    async def _acquire_retry_claim(
        self,
        backend: StateBackend,
        job: JobConfig,
        retry_num: int,
        *,
        quiet: bool,
    ) -> Optional[Lease]:
        """``acquire_lease`` for a retry claim, mapping a timeout OR a raised
        store error to ``None`` so the caller's read-back-and-policy path
        decides -- the same containment as :meth:`_acquire_slot_lease`.  An
        escape here kills the ``schedule_retry_job`` task (silently dropping
        the due retry) AND is re-raised by ``cancel_job_retries``' awaiter on
        the job's next fire, outside ``run()``'s try/except: the whole
        daemon crashes.
        """
        try:
            return await asyncio.wait_for(
                backend.acquire_lease(
                    self._retry_claim_lease(job.name),
                    self._slot_holder(),
                    RETRY_CLAIM_TTL,
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.TimeoutError:
            return None
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - flock ENOLCK/EIO/ESTALE on
            # a sick shared mount is as ambiguous as a timeout; the policy
            # fork (defer under fail-closed, unserialized proceed under
            # degrade) decides, never the exception.
            if not quiet:
                logger.warning(
                    "Cron job %s retry (#%i): the retry-claim store call "
                    "raised (%s); treating the claim as unanswered",
                    job.name,
                    retry_num,
                    ex,
                )
            return None

    async def _retry_consume_decision(
        self, job: JobConfig, retry_num: int, *, quiet: bool
    ) -> str:
        """Decide a due retry's fate: ``launch`` | ``defer`` | ``abort``.

        Without cross-node resume this is exactly the classic
        :meth:`_retry_consume_ok` (launch/defer).  With it, two additions
        close the claim/consume race:

        * the consume serializes on the SAME per-job claim lease the scan
          uses, so a claimer's re-read-then-append and our re-check-then-
          settle cannot interleave;
        * the newest ladder record must still be a record THIS host wrote
          -- a foreign newest record (a claimer's pending, or its
          settled/"launched" after it already fired) means the ladder
          positively moved, and the only safe move is to end it locally
          without settling (``abort``): our settle landing on top would
          bury the claimer's pending and could resurrect the attempt on
          the next boot.

        The staleness grace (:data:`RETRY_CLAIM_GRACE`) cannot protect a
        gate-deferred owner -- its re-check cadence is its own ladder
        delay, arbitrarily longer than any constant -- so this re-check is
        load-bearing for at-most-once, not defensive hardening.  Read/
        acquire failures follow ``onStoreUnavailable``: degrade proceeds
        (unserialized, at-least-once), fail-closed defers.
        """
        if not self._retry_cross_node_eligible(job):
            ok = await self._retry_consume_ok(job.name, retry_num, quiet=quiet)
            return "launch" if ok else "defer"
        backend = self.state_backend
        if backend is None:
            ok = await self._retry_consume_ok(job.name, retry_num, quiet=quiet)
            return "launch" if ok else "defer"
        fail_closed = self._state_on_unavailable == "fail-closed"
        lease = await self._acquire_retry_claim(
            backend, job, retry_num, quiet=quiet
        )
        if lease is None:
            observed: Optional[Lease] = None
            try:
                observed = await asyncio.wait_for(
                    backend.read_lease(self._retry_claim_lease(job.name)),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - as ambiguous as a timeout;
                # observed stays None so the policy fork below decides.
                pass
            if observed is not None and observed.holder != self._slot_holder():
                # a live claimer is working this very ladder: defer and
                # re-check; if it claimed, the next pass aborts.
                return "defer"
            if observed is None and fail_closed:
                if not quiet:
                    logger.warning(
                        "Cron job %s retry (#%i) deferred: cannot "
                        "serialize with cross-node claims (store "
                        "unavailable) and onStoreUnavailable is "
                        "fail-closed",
                        job.name,
                        retry_num,
                    )
                return "defer"
            if observed is not None:
                lease = observed  # our own late-landing acquire: adopt it
        try:
            try:
                recs = await asyncio.wait_for(
                    backend.list_records(
                        self._retry_stream(job.name),
                        limit=1,
                        newest_first=True,
                    ),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - policy fork below
                if fail_closed:
                    return "defer"
                recs = []
            rec = recs[0] if recs else None
            if (
                rec is not None
                and isinstance(rec.get("host"), str)
                and rec.get("host") != self._state_host
            ):
                return "abort"
            ok = await self._retry_consume_ok(job.name, retry_num, quiet=quiet)
            return "launch" if ok else "defer"
        finally:
            if lease is not None:
                try:
                    await asyncio.wait_for(
                        backend.release_lease(lease),
                        timeout=STATE_OP_TIMEOUT,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 - TTL is the fallback
                    pass

    async def _retry_claim_scan(self) -> None:
        """Scan for foreign retry ladders this node should resume.

        The cross-node half of restart-surviving retries: a pending record
        whose owner crashed (stale past its deadline plus
        :data:`RETRY_CLAIM_GRACE`) or a ``handoff`` record from an owner
        that positively relinquished is claimed -- under a per-job TTL
        lease, with a re-read inside it -- and re-armed locally exactly
        like rehydration re-arms this host's own pendings.  Spawned from
        the housekeeping pass about once a minute; every failure degrades
        to "not this pass".
        """
        if not self._retry_resume_active():
            return
        for name, job in list(self.cron_jobs.items()):
            try:
                await self._maybe_claim_retry(name, job)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - one job must not end the scan
                logger.exception(
                    "state: error scanning job %s for a claimable retry",
                    name,
                )

    async def _maybe_claim_retry(self, name: str, job: JobConfig) -> None:
        backend = self.state_backend
        if backend is None or not self._retry_cross_node_eligible(job):
            return
        if not job.enabled or not job.onFailure["retry"]["maximumRetries"]:
            return
        if self.running_jobs.get(name):
            return
        state = self.retry_state.get(name)
        if state is not None and (state.task is not None or state.count > 0):
            # a live local ladder outranks; a count-0, taskless leftover
            # (a slot-denied scheduled fire armed it and never launched)
            # does not block a claim.
            return
        if not self._cluster_allows(job):
            return
        try:
            recs = await asyncio.wait_for(
                backend.list_records(
                    self._retry_stream(name), limit=1, newest_first=True
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - not this pass
            return
        rec = recs[0] if recs else None
        if rec is None:
            return
        claimable = self._retry_record_claimable(name, job, rec)
        if claimable is None:
            return
        attempt, not_before = claimable
        lease: Optional[Lease] = None
        try:
            lease = await asyncio.wait_for(
                backend.acquire_lease(
                    self._retry_claim_lease(name),
                    self._slot_holder(),
                    RETRY_CLAIM_TTL,
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.TimeoutError:
            lease = None
        if lease is None:
            return  # a rival claimer or a sick store: next scan retries
        try:
            claimed = await self._claim_retry_under_lease(
                name, job, rec, attempt, not_before
            )
        finally:
            try:
                await asyncio.wait_for(
                    backend.release_lease(lease), timeout=STATE_OP_TIMEOUT
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - TTL is the fallback
                pass
        if not claimed:
            return
        # Re-apply the top-guard invariant: the awaits above (list/acquire/
        # claim/release, each up to STATE_OP_TIMEOUT) yield, and in that
        # window a scheduled fire of this job could have launched, failed,
        # and armed a LIVE local ladder (its retry_state.task). Overwriting
        # retry_state[name] here would strand that task as a second,
        # uncancelled same-node ladder -- and because both write host ==
        # self._state_host, the foreign-record abort in the consume path
        # never fires, so the job double-fires on ONE node. That live
        # ladder outranks (exactly as the top guard at the method start
        # would have declined the claim); drop the just-made claim. Its
        # durable pending is host-local and the live ladder settles it on
        # consume.
        existing = self.retry_state.get(name)
        if self.running_jobs.get(name) or (
            existing is not None
            and (existing.task is not None or existing.count > 0)
        ):
            logger.info(
                "Job %s: dropping a just-made retry claim; a local retry "
                "ladder was armed while claiming (it supersedes)",
                name,
            )
            return
        retry = job.onFailure["retry"]
        state = JobRetryState(
            retry["initialDelay"],
            retry["backoffMultiplier"],
            retry["maximumDelay"],
        )
        for _ in range(attempt):
            state.next_delay()
        now = get_now(datetime.timezone.utc)
        remaining = max(0.0, (not_before - now).total_seconds())
        self.retry_state[name] = state
        state.task = asyncio.create_task(
            self.schedule_retry_job(name, remaining, attempt)
        )
        logger.info(
            "Job %s: claimed pending retry #%d from host %s (cross-node "
            "retry resume); due in %.1f seconds",
            name,
            attempt,
            rec.get("host") or rec.get("fromHost"),
            remaining,
        )

    async def _claim_retry_under_lease(
        self,
        name: str,
        job: JobConfig,
        rec: Dict[str, Any],
        attempt: int,
        not_before: datetime.datetime,
    ) -> bool:
        """The claim's critical section: re-check, validate, append.

        Runs while holding the per-job claim lease.  ``True`` means the
        claim record landed and the caller may arm the local ladder.
        """
        backend = self.state_backend
        if backend is None:
            return False
        # re-read under the lease: the record must not have moved.
        try:
            recheck = await asyncio.wait_for(
                backend.list_records(
                    self._retry_stream(name), limit=1, newest_first=True
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - not this pass
            return False
        if not recheck or recheck[0] != rec:
            return False
        # superseded-by-run against the DURABLE ledger: the run that
        # resolved this ladder most likely happened on ANOTHER host,
        # which this node's in-memory history knows nothing about.  A handoff
        # carries the original arm time in ``armedAt`` (its ``at`` is the
        # hand-off instant, which would hide a run the prior owner already
        # completed); a pending's own ``at`` is its arm time.
        armed_at = rec.get("armedAt") or rec.get("at") or rec.get("notBefore")
        try:
            # the run-only watermark: a peer holding this job's slots under
            # a pause stamps skip rows the catch-up watermark counts and
            # this guard must not (see durable_last_completed_at).
            last_durable = await asyncio.wait_for(
                self.durable_last_completed_at(name),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - ambiguity settles: no claim
            return False
        if (
            isinstance(last_durable, str)
            and isinstance(armed_at, str)
            and last_durable > armed_at
        ):
            # a run finished after the ladder was armed: it resolved (its
            # settle may have been dropped). Settle it here so the scan
            # stops revisiting it; no-run beats double-run.
            self._persist_retry_settled(name, "superseded-by-run", attempt)
            return False
        claim = {
            "kind": "pending",
            "attempt": attempt,
            "notBefore": not_before.isoformat(),
            "jobDigest": job_digest(job),
            "host": self._state_host,
            "at": get_now(datetime.timezone.utc).isoformat(),
            "claimedFrom": rec.get("host") or rec.get("fromHost"),
        }
        write = self._queue_retry_write(name, claim)
        try:
            # the claim record must LAND before the lease is released, or
            # a rival's post-release re-read could still see the old
            # record and claim it too.
            await asyncio.wait_for(
                asyncio.shield(write), timeout=STATE_OP_TIMEOUT
            )
        except asyncio.TimeoutError:
            # We are abandoning this claim (the caller arms no ladder). The
            # write is shielded, so without this cancel it would still land
            # LATER as an own-host pending -- which our own future scans
            # skip (a host never claims its own pending) and rehydration
            # never re-arms, while a live original owner reading it aborts
            # its ladder: an unreclaimable orphan that silently drops the
            # retry. Cancel it so the foreign record stays newest and the
            # next scan (here or on a peer) can re-claim cleanly. (If the
            # append already completed at the instant of the timeout the
            # cancel is a harmless no-op; the vanishingly small residual is
            # the same at-least-once window every claim path accepts.)
            write.cancel()
            return False
        return True

    def _retry_record_claimable(
        self, name: str, job: JobConfig, rec: Dict[str, Any]
    ) -> Optional[Tuple[int, datetime.datetime]]:
        """Judge whether a ladder record is another node's claimable retry.

        Mirrors :meth:`_validate_pending_retry`'s checks (shape, digest,
        budget, deadline) with the cross-node rules on top: only a FOREIGN
        ``pending`` stale past :data:`RETRY_CLAIM_GRACE` (a crashed owner
        -- a live one fires within moments of its deadline) or a
        ``handoff`` (an owner that positively relinquished; no grace)
        qualifies.  Every validation failure here just declines the claim
        -- settling another host's record on local suspicion alone would
        race its live owner; the durable superseded-by-run check (which
        has store-wide evidence) happens under the claim lease instead.
        """
        kind = rec.get("kind")
        if kind not in ("pending", "handoff"):
            return None
        attempt = rec.get("attempt")
        not_before = _parse_iso_utc(rec.get("notBefore"))
        if (
            isinstance(attempt, bool)
            or not isinstance(attempt, int)
            or attempt < 1
            or not_before is None
        ):
            return None
        if rec.get("jobDigest") != job_digest(job):
            return None
        retry = job.onFailure["retry"]
        maximum = retry["maximumRetries"]
        if maximum != -1 and attempt > maximum:
            return None
        now = get_now(datetime.timezone.utc)
        deadline = job.startingDeadlineSeconds
        if deadline and (now - not_before).total_seconds() > deadline:
            return None
        # the newest ACTUAL run (see _validate_pending_retry): a pause-held
        # slot's "skipped" row is not evidence anything ran.
        last_at = self._last_completed_at.get(name)
        # a handoff carries the original arm time in ``armedAt``; its ``at`` is
        # the hand-off instant, which would hide a run the prior owner already
        # completed (a pending has no ``armedAt`` and its ``at`` is its arm).
        armed_at = (
            _parse_iso_utc(rec.get("armedAt"))
            or _parse_iso_utc(rec.get("at"))
            or not_before
        )
        if last_at is not None and last_at > armed_at:
            return None  # locally-known newer run; the ladder resolved
        if kind == "handoff":
            return attempt, max(not_before, now)
        host = rec.get("host")
        if not isinstance(host, str) or host == self._state_host:
            # our own pending is rehydration's business, never the scan's
            return None
        due_anchor = max(not_before, armed_at)
        if (now - due_anchor).total_seconds() <= RETRY_CLAIM_GRACE:
            return None
        return attempt, not_before

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

    def _abandon_retry(self, job: JobConfig, retry_num: int) -> None:
        """End a pending retry sequence whose job's ownership moved off-node.

        Marks the state cancelled BEFORE dropping it: a RunningJob launched
        while the retry sat pending (a manual API start, a concurrencyPolicy
        Allow overlap) captured this same JobRetryState, and its own later
        failure would otherwise re-arm a retry on a state no longer in
        ``retry_state`` -- which ``cancel_job_retries`` could never find or
        cancel, so the orphan would relaunch the job even after a later
        successful run.

        When cross-node retry resume is active (a shared store plus leader
        election) the ladder is HANDED OFF instead of settled dead: a
        ``handoff`` record carrying the attempt, the job digest and a
        now-due deadline lands on the stream, and the new owner's claim
        scan picks it up (no staleness grace -- the old owner has
        positively relinquished).  No ``cancelled`` run-history record is
        written on that path: the attempt is not ending, it is moving.
        """
        job_name = job.name
        state = self.retry_state.get(job_name)
        if state is not None:
            state.cancelled = True
        self.retry_state.pop(job_name, None)
        if self._retry_cross_node_eligible(job):
            now = get_now(datetime.timezone.utc)
            # Anchor the new owner's superseded-by-run guard on when this
            # attempt was originally ARMED, not on this hand-off instant. A
            # peer that took ownership while we were demoted-but-blind may
            # have claimed and RUN this attempt; that run finished BEFORE now,
            # so a now-stamped anchor ("at") would make the completed run look
            # older than the record and the new owner would re-run it -- a
            # double-fire across failover. notBefore stays now so the new owner
            # still runs a genuinely-unresolved ladder promptly.
            armed_at = state.armed_at if state is not None else None
            self._queue_retry_write(
                job_name,
                {
                    "kind": "handoff",
                    "attempt": retry_num,
                    "notBefore": now.isoformat(),
                    "jobDigest": job_digest(job),
                    "fromHost": self._state_host,
                    "at": now.isoformat(),
                    "armedAt": (
                        armed_at.isoformat()
                        if armed_at is not None
                        else now.isoformat()
                    ),
                },
            )
            logger.warning(
                "Cron job %s retry (#%i) handed off: the cluster moved "
                "ownership of it to another node; the new owner resumes "
                "the ladder from its durable record (cross-node retry "
                "resume)",
                job_name,
                retry_num,
            )
            return
        # settle the durable ladder: the new owner runs the job's future
        # firings, so re-arming this attempt on OUR next boot would be the
        # exact cross-node double-run the abandonment avoids.
        self._persist_retry_settled(job_name, "owner-moved", retry_num)
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
        await self.cancel_job_retries(job.config.name, settle="succeeded")
        await job.report_success()

    @staticmethod
    def _reap_retry_task(name: str, task: "asyncio.Task[None]") -> None:
        """Retrieve (never re-raise) a finished retry task's outcome.

        Both awaiters (here and in ``handle_job_failure``) run on launch/
        finish paths outside ``run()``'s try/except, so re-raising an
        exception stored in a dead retry task would crash the whole
        scheduler.  ``.exception()`` also marks the exception retrieved,
        silencing the event loop's "never retrieved" report.
        """
        if task.cancelled():
            return
        ex = task.exception()
        if ex is not None:
            logger.error(
                "Cron job %s: its retry task died with an unexpected "
                "error; that pending retry was lost",
                name,
                exc_info=ex,
            )

    async def cancel_job_retries(
        self, name: str, *, settle: Optional[str] = "superseded"
    ) -> None:
        try:
            state = self.retry_state.pop(name)
        except KeyError:
            return
        state.cancelled = True
        # Settle the durable ladder record (fire-and-forget) so a pending
        # retry is not re-armed on the next boot. Skipped when settle is
        # None -- the graceful-shutdown path, where surviving the restart is
        # the point -- and when no retry was ever scheduled this ladder
        # (count == 0: nothing durable was written, and settling here would
        # add one durable write to every successful run of a retry-armed
        # job).
        if settle is not None and state.count > 0:
            self._persist_retry_settled(name, settle, state.count)
        if state.task is not None:
            if state.task.done():
                self._reap_retry_task(name, state.task)
            else:
                state.task.cancel()
