"""Native Prometheus metrics for the yacron2 web API (``GET /metrics``).

The pull-side sibling of :mod:`yacron2.statsd`: instead of pushing per-run
UDP events, the daemon exposes cumulative counters and live gauges for a
Prometheus server to scrape. The exposition is hand-rolled -- both the
classic text format (0.0.4) and OpenMetrics 1.0, selected by the scraper's
Accept header -- so the feature adds no runtime dependency and stays
architecture-portable, like the hand-rolled HTTP the leadership backends
use.

Split of responsibilities:

* :class:`PrometheusMetrics` (one instance, owned by ``Cron`` so it
  survives web-app restarts and cluster-manager rebuilds) accumulates the
  monotonic state that cannot be derived at scrape time: run outcomes,
  duration histograms, retry/permanent-failure counts, config-reload
  results, and leadership/quorum transition counts.
* Everything else -- per-job enabled/running/next-run gauges, last-run
  summaries, and the whole cluster block -- is read live from the ``Cron``
  object at scrape time, from the same state that backs the JSON API
  (``cron_jobs``, ``running_jobs``, ``last_run``,
  ``cluster_manager.view_dict()``).

All reads and writes happen on the single process-wide event loop, so no
locking is needed anywhere here.
"""

import logging
import math
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from crontab import CronTab

import yacron2.version

if TYPE_CHECKING:  # pragma: no cover -- import cycle guard, types only
    from yacron2.cron import Cron

logger = logging.getLogger("prometheus")

CONTENT_TYPE_TEXT = "text/plain; version=0.0.4; charset=utf-8"
CONTENT_TYPE_OPENMETRICS = (
    "application/openmetrics-text; version=1.0.0; charset=utf-8"
)

# Upper bounds (seconds) of the default job-duration histogram: cron jobs
# range from sub-second probes to multi-hour batch runs, so the buckets are
# roughly logarithmic across that whole span. Overridable per deployment via
# web.metrics.durationBuckets.
DEFAULT_DURATION_BUCKETS = (
    0.1,
    0.5,
    1.0,
    5.0,
    15.0,
    60.0,
    300.0,
    900.0,
    3600.0,
)

# The run outcomes _record_run can produce (see cron.JobRunInfo). Emitted
# zero-filled for every job so alert expressions like
# increase(yacron2_job_runs_total{status="failure"}[1h]) see the series from
# the first scrape, not only after the first failure.
RUN_OUTCOMES = ("success", "failure", "cancelled")

# Mirror of the per-peer STATUS_* constants in yacron2.cluster (a documented
# API surface: the /cluster payload and its wiki table). Kept as literals so
# this leaf module never imports the cluster machinery.
PEER_STATUSES = (
    "unknown",
    "self",
    "agreed",
    "syncing",
    "drifted",
    "unreachable",
    "untrusted",
    "conflict",
)


def escape_label_value(value: str) -> str:
    """Escape a label value per the exposition formats (\\, ", newline)."""
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def escape_help(text: str, openmetrics: bool = False) -> str:
    """Escape HELP text: backslash and newline in both formats.

    OpenMetrics additionally requires a double quote to be escaped (its
    ABNF excludes a raw ``"`` from HELP text), while the classic text
    format treats ``\\"`` as two literal characters -- so the quote is
    escaped only on the OpenMetrics rendering.
    """
    escaped = text.replace("\\", "\\\\").replace("\n", "\\n")
    if openmetrics:
        escaped = escaped.replace('"', '\\"')
    return escaped


def format_value(value: Union[int, float]) -> str:
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    if math.isnan(value):
        return "NaN"
    # ints (and integral floats within exact-float range) render without a
    # decimal point; everything else uses repr, the shortest round-tripping
    # form, so timestamps keep full precision.
    if value == int(value) and abs(value) < 2**53:
        return str(int(value))
    return repr(float(value))


def _bucket_bound(bound: float) -> str:
    # histogram "le" values as canonical floats ("1.0", not "1"), the one
    # spelling valid in both the text format and OpenMetrics.
    return "+Inf" if math.isinf(bound) else repr(float(bound))


class MetricFamily:
    """One metric family: a ``# TYPE`` group and its samples.

    ``name`` is the base name without a type-mandated suffix: the renderer
    appends ``_total`` for counters and ``_info`` for info metrics, per
    format. Histogram samples must pass their explicit ``_bucket`` /
    ``_sum`` / ``_count`` suffix to :meth:`add`.
    """

    def __init__(self, name: str, mtype: str, help_text: str) -> None:
        self.name = name
        self.mtype = mtype  # "counter" | "gauge" | "histogram" | "info"
        self.help_text = help_text
        # (suffix, labels, value)
        self.samples: List[Tuple[str, Dict[str, str], float]] = []

    def add(
        self,
        labels: Dict[str, str],
        value: Union[int, float],
        suffix: str = "",
    ) -> None:
        self.samples.append((suffix, labels, float(value)))


def render_families(
    families: Iterable[MetricFamily], openmetrics: bool = False
) -> str:
    """Render metric families as exposition text.

    The two formats differ only in metadata spelling: OpenMetrics names a
    counter family without its ``_total`` suffix and knows a first-class
    ``info`` type, while the text format names the full sample name and
    downgrades info metrics to gauges. OpenMetrics additionally requires
    the ``# EOF`` terminator.
    """
    out: List[str] = []
    for family in families:
        if not family.samples:
            continue
        if family.mtype == "counter":
            sample_base = family.name + "_total"
        elif family.mtype == "info":
            sample_base = family.name + "_info"
        else:
            sample_base = family.name
        if openmetrics:
            type_name, mtype = family.name, family.mtype
        else:
            type_name = sample_base
            mtype = "gauge" if family.mtype == "info" else family.mtype
        out.append(
            "# HELP {} {}".format(
                type_name, escape_help(family.help_text, openmetrics)
            )
        )
        out.append("# TYPE {} {}".format(type_name, mtype))
        for suffix, labels, value in family.samples:
            name = sample_base + suffix
            if labels:
                label_str = ",".join(
                    '{}="{}"'.format(key, escape_label_value(str(val)))
                    for key, val in labels.items()
                )
                out.append(
                    "{}{{{}}} {}".format(name, label_str, format_value(value))
                )
            else:
                out.append("{} {}".format(name, format_value(value)))
    if openmetrics:
        out.append("# EOF")
    return "\n".join(out) + "\n"


def resolve_metrics_config(
    web_config: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Resolve the raw ``web.metrics`` option into effective settings.

    Returns ``None`` when the endpoint is disabled, else a dict with
    ``public`` (serve /metrics without the bearer token) and
    ``durationBuckets`` (histogram upper bounds). The option is enabled by
    default whenever the web API itself is on; ``metrics: false`` /
    ``metrics: true`` are shorthands for the map form.
    """
    raw = web_config.get("metrics")
    if raw is False:
        return None
    if raw is None or raw is True:
        raw = {}
    if not raw.get("enabled", True):
        return None
    buckets = raw.get("durationBuckets")
    return {
        "public": bool(raw.get("public", False)),
        "durationBuckets": (
            tuple(buckets) if buckets else DEFAULT_DURATION_BUCKETS
        ),
    }


class _JobMetrics:
    """Per-job monotonic accumulators (counters and last-event times)."""

    __slots__ = (
        "runs",
        "retries",
        "permanent_failures",
        "start_failures",
        "duration_sum",
        "duration_count",
        "bucket_counts",
        "last_success_time",
        "last_failure_time",
    )

    def __init__(self, n_buckets: int) -> None:
        self.runs: Dict[str, int] = {}
        self.retries = 0
        self.permanent_failures = 0
        self.start_failures = 0
        self.duration_sum = 0.0
        self.duration_count = 0
        # cumulative per configured bound (observations <= bound); the +Inf
        # bucket is duration_count itself.
        self.bucket_counts = [0] * n_buckets
        self.last_success_time: Optional[float] = None
        self.last_failure_time: Optional[float] = None


class PrometheusMetrics:
    """Metric accumulators plus the ``/metrics`` renderer.

    Owned by :class:`yacron2.cron.Cron` for the life of the process, so
    counters survive web-app restarts and cluster-manager rebuilds. Every
    mutator is a plain synchronous increment: instrumentation can never
    block or crash the scheduler.
    """

    def __init__(self) -> None:
        self._jobs: Dict[str, _JobMetrics] = {}
        self._buckets: Tuple[float, ...] = DEFAULT_DURATION_BUCKETS
        # The histogram "le" label strings are a pure function of the (fixed
        # between config changes) bucket bounds, so render them once here (and
        # in set_duration_buckets) rather than repr()-ing every bound for every
        # job on every scrape.
        self._bucket_bound_strs: Tuple[str, ...] = tuple(
            _bucket_bound(b) for b in self._buckets
        )
        self._start_time = time.time()
        self._last_reload_ok: Optional[bool] = None
        self._last_reload_success_time: Optional[float] = None
        self._leader_transitions = 0
        self._quorum_transitions = 0

    # -- configuration ----------------------------------------------------

    def set_duration_buckets(self, buckets: Sequence[float]) -> None:
        new = tuple(buckets)
        if new == self._buckets:
            return
        self._buckets = new
        self._bucket_bound_strs = tuple(_bucket_bound(b) for b in new)
        # Bucket bounds changed: past observations cannot be re-binned, so
        # every job's histogram restarts from zero -- an ordinary counter
        # reset to Prometheus. The run/outcome counters are unaffected.
        for job in self._jobs.values():
            job.bucket_counts = [0] * len(new)
            job.duration_sum = 0.0
            job.duration_count = 0

    def prune(self, job_names: Iterable[str]) -> None:
        """Drop accumulators for jobs no longer in the loaded config."""
        keep = set(job_names)
        for name in list(self._jobs):
            if name not in keep:
                del self._jobs[name]

    def _job(self, name: str) -> _JobMetrics:
        job = self._jobs.get(name)
        if job is None:
            job = _JobMetrics(len(self._buckets))
            self._jobs[name] = job
        return job

    # -- event hooks (called from yacron2.cron) ----------------------------

    def job_run_recorded(
        self, name: str, outcome: str, duration: Optional[float]
    ) -> None:
        job = self._job(name)
        job.runs[outcome] = job.runs.get(outcome, 0) + 1
        now = time.time()
        if outcome == "success":
            job.last_success_time = now
        elif outcome == "failure":
            job.last_failure_time = now
        if duration is not None:
            job.duration_sum += duration
            job.duration_count += 1
            for i, bound in enumerate(self._buckets):
                if duration <= bound:
                    job.bucket_counts[i] += 1

    def job_start_failed(self, name: str) -> None:
        self._job(name).start_failures += 1

    def job_retry_launched(self, name: str) -> None:
        self._job(name).retries += 1

    def job_permanent_failure(self, name: str) -> None:
        self._job(name).permanent_failures += 1

    def config_parse(self, ok: bool) -> None:
        self._last_reload_ok = ok
        if ok:
            self._last_reload_success_time = time.time()

    def cluster_leader_transition(self) -> None:
        self._leader_transitions += 1

    def cluster_quorum_transition(self) -> None:
        self._quorum_transitions += 1

    # -- rendering ---------------------------------------------------------

    def render(self, cron: "Cron", openmetrics: bool = False) -> str:
        return render_families(self._families(cron), openmetrics)

    def _families(self, cron: "Cron") -> List[MetricFamily]:
        families = self._daemon_families(cron)
        families.extend(self._job_families(cron))
        families.extend(self._cluster_families(cron))
        return families

    def _daemon_families(self, cron: "Cron") -> List[MetricFamily]:
        families = []
        info = MetricFamily("yacron2", "info", "yacron2 build information.")
        info.add({"version": yacron2.version.version}, 1)
        families.append(info)

        start = MetricFamily(
            "yacron2_start_time_seconds",
            "gauge",
            "Unix time this yacron2 process started.",
        )
        start.add({}, self._start_time)
        families.append(start)

        job_set = MetricFamily(
            "yacron2_job_set",
            "info",
            "Fingerprint of the currently loaded job set (see /job-set-id).",
        )
        job_set.add({"job_set_id": cron.job_set_id()}, 1)
        families.append(job_set)

        jobs = MetricFamily(
            "yacron2_jobs",
            "gauge",
            "Number of configured jobs by enablement state.",
        )
        enabled_count = sum(
            1 for job in cron.cron_jobs.values() if job.enabled
        )
        jobs.add({"state": "enabled"}, enabled_count)
        jobs.add({"state": "disabled"}, len(cron.cron_jobs) - enabled_count)
        families.append(jobs)

        if self._last_reload_ok is not None:
            reload_ok = MetricFamily(
                "yacron2_config_last_reload_successful",
                "gauge",
                "Whether the last configuration parse succeeded.",
            )
            reload_ok.add({}, 1 if self._last_reload_ok else 0)
            families.append(reload_ok)
        if self._last_reload_success_time is not None:
            reload_time = MetricFamily(
                "yacron2_config_last_reload_success_timestamp_seconds",
                "gauge",
                "Unix time of the last successful configuration parse.",
            )
            reload_time.add({}, self._last_reload_success_time)
            families.append(reload_time)
        return families

    def _job_families(self, cron: "Cron") -> List[MetricFamily]:
        # Local import: cron.py imports this module, so the cycle can only
        # be broken at call time (mirrors the deferred imports elsewhere).
        # get_now/datetime are only touched by the next-fire fallback below.
        import datetime

        from yacron2.cron import get_now, schedule_str

        # Ensure every configured job has an accumulator so its counters
        # are emitted zero-filled from the first scrape (prune keeps this
        # aligned with the loaded config on reload).
        for name in cron.cron_jobs:
            self._job(name)

        runs = MetricFamily(
            "yacron2_job_runs",
            "counter",
            "Finished job runs by outcome, as recorded in the run history.",
        )
        retries = MetricFamily(
            "yacron2_job_retries",
            "counter",
            "Job retry attempts actually launched (onFailure.retry).",
        )
        permanent = MetricFamily(
            "yacron2_job_permanent_failures",
            "counter",
            "Failed runs with no retry remaining "
            "(the onPermanentFailure condition).",
        )
        start_failures = MetricFamily(
            "yacron2_job_start_failures",
            "counter",
            "Runs whose command could not be launched at all "
            "(recorded as failures with exit code 127).",
        )
        duration = MetricFamily(
            "yacron2_job_duration_seconds",
            "histogram",
            "Duration of finished job runs.",
        )
        last_success = MetricFamily(
            "yacron2_job_last_success_timestamp_seconds",
            "gauge",
            "Unix time this job last finished successfully.",
        )
        last_failure = MetricFamily(
            "yacron2_job_last_failure_timestamp_seconds",
            "gauge",
            "Unix time this job last finished as a failure.",
        )
        for name in sorted(self._jobs):
            job = self._jobs[name]
            labels = {"job_name": name}
            for outcome in RUN_OUTCOMES:
                runs.add(
                    {"job_name": name, "status": outcome},
                    job.runs.get(outcome, 0),
                )
            retries.add(labels, job.retries)
            permanent.add(labels, job.permanent_failures)
            start_failures.add(labels, job.start_failures)
            # bucket_counts is stored cumulatively (every bound >= the
            # observation is incremented), so the counts render as-is. The "le"
            # label strings are precomputed (self._bucket_bound_strs).
            for le, count in zip(
                self._bucket_bound_strs, job.bucket_counts, strict=True
            ):
                duration.add(
                    {"job_name": name, "le": le},
                    count,
                    suffix="_bucket",
                )
            duration.add(
                {"job_name": name, "le": "+Inf"},
                job.duration_count,
                suffix="_bucket",
            )
            duration.add(labels, job.duration_sum, suffix="_sum")
            duration.add(labels, job.duration_count, suffix="_count")
            if job.last_success_time is not None:
                last_success.add(labels, job.last_success_time)
            if job.last_failure_time is not None:
                last_failure.add(labels, job.last_failure_time)

        info = MetricFamily(
            "yacron2_job",
            "info",
            "Static per-job configuration facts.",
        )
        enabled = MetricFamily(
            "yacron2_job_enabled",
            "gauge",
            "Whether the job is enabled in the loaded configuration.",
        )
        running = MetricFamily(
            "yacron2_job_running",
            "gauge",
            "Number of currently running instances of the job.",
        )
        next_run = MetricFamily(
            "yacron2_job_next_run_timestamp_seconds",
            "gauge",
            "Unix time of the job's next scheduled run "
            "(absent for disabled and @reboot jobs).",
        )
        last_run_time = MetricFamily(
            "yacron2_job_last_run_timestamp_seconds",
            "gauge",
            "Unix time the job's most recent run finished.",
        )
        last_run_duration = MetricFamily(
            "yacron2_job_last_run_duration_seconds",
            "gauge",
            "Duration of the job's most recent finished run.",
        )
        last_run_exit_code = MetricFamily(
            "yacron2_job_last_run_exit_code",
            "gauge",
            "Exit code of the job's most recent finished run.",
        )
        last_run_success = MetricFamily(
            "yacron2_job_last_run_success",
            "gauge",
            "Whether the job's most recent finished run succeeded "
            "(cancelled runs count as 0).",
        )
        for name, job_config in cron.cron_jobs.items():
            labels = {"job_name": name}
            info.add(
                {
                    "job_name": name,
                    "schedule": schedule_str(job_config),
                    "cluster_policy": job_config.clusterPolicy,
                },
                1,
            )
            enabled.add(labels, 1 if job_config.enabled else 0)
            running.add(labels, len(cron.running_jobs.get(name) or ()))
            # Reuse the scheduler's authoritative next-fire instant instead of
            # re-walking the crontab and building two aware datetimes per job
            # per scrape: cron._next_fire holds the aware-UTC next fire for
            # exactly the enabled CronTab jobs, maintained incrementally by the
            # loop (and computed the same way the fallback below does). This is
            # the steady-state path.
            when = cron._next_fire.get(name)
            if when is not None:
                next_run.add(labels, when.timestamp())
            elif job_config.enabled and isinstance(
                job_config.schedule, CronTab
            ):
                # Index not yet seeded: a scrape in the startup window before
                # the loop's first tick, or metrics rendered on a Cron whose
                # loop never ran. Compute the next fire directly so the gauge
                # is still emitted (absent for disabled/@reboot jobs).
                seconds = job_config.schedule.next(
                    now=get_now(job_config.timezone),
                    default_utc=job_config.utc,
                )
                if seconds is not None:
                    next_run.add(
                        labels,
                        get_now(datetime.timezone.utc).timestamp() + seconds,
                    )
            last = cron.last_run.get(name)
            if last is not None:
                last_run_time.add(labels, last.finished_at.timestamp())
                if last.duration is not None:
                    last_run_duration.add(labels, last.duration)
                if last.exit_code is not None:
                    last_run_exit_code.add(labels, last.exit_code)
                last_run_success.add(
                    labels, 1 if last.outcome == "success" else 0
                )
        return [
            runs,
            retries,
            permanent,
            start_failures,
            duration,
            last_success,
            last_failure,
            info,
            enabled,
            running,
            next_run,
            last_run_time,
            last_run_duration,
            last_run_exit_code,
            last_run_success,
        ]

    def _cluster_families(self, cron: "Cron") -> List[MetricFamily]:
        families = []
        manager = cron.cluster_manager
        enabled = MetricFamily(
            "yacron2_cluster_enabled",
            "gauge",
            "Whether a cluster leadership backend is currently running.",
        )
        enabled.add({}, 1 if manager is not None else 0)
        families.append(enabled)

        # The transition hooks only run when leader election is configured
        # (see _log_cluster_role), so in an observe-only cluster the
        # counters are omitted rather than exposed permanently frozen at 0
        # while the quorate gauge visibly changes.
        elect_leader = cron._elect_leader_configured
        if elect_leader or self._leader_transitions:
            leader_transitions = MetricFamily(
                "yacron2_cluster_leader_transitions",
                "counter",
                "Times this node acquired or lost scheduled-job leadership "
                "(observed at scheduler cadence).",
            )
            leader_transitions.add({}, self._leader_transitions)
            families.append(leader_transitions)
        if elect_leader or self._quorum_transitions:
            quorum_transitions = MetricFamily(
                "yacron2_cluster_quorum_transitions",
                "counter",
                "Times this node joined or left quorum "
                "(observed at scheduler cadence).",
            )
            quorum_transitions.add({}, self._quorum_transitions)
            families.append(quorum_transitions)

        if manager is None:
            return families
        try:
            view = manager.view_dict()
        except Exception:
            # A backend read should never raise; if one does, the scrape
            # must still return the job metrics rather than 500. Mirrors
            # the fail-safe reads in cron._cluster_allows.
            logger.exception(
                "error reading cluster state for /metrics; "
                "omitting cluster metrics from this scrape"
            )
            return families

        info = MetricFamily(
            "yacron2_cluster",
            "info",
            "Static cluster configuration facts for this node.",
        )
        info.add(
            {
                "backend": str(view.get("backend", "")),
                "node_name": str(view.get("node_name", "")),
                "distribution": str(view.get("distribution", "")),
            },
            1,
        )
        families.append(info)

        size = MetricFamily(
            "yacron2_cluster_size",
            "gauge",
            "Effective cluster size N this node coordinates against.",
        )
        size.add({}, view.get("cluster_size", 0))
        families.append(size)

        quorum = MetricFamily(
            "yacron2_cluster_quorum",
            "gauge",
            "Number of agreeing nodes required for leader election.",
        )
        quorum.add({}, view.get("quorum", 0))
        families.append(quorum)

        quorate = MetricFamily(
            "yacron2_cluster_quorate",
            "gauge",
            "Whether this node is currently part of a quorum.",
        )
        quorate.add({}, 1 if view.get("quorate") else 0)
        families.append(quorate)

        is_leader = MetricFamily(
            "yacron2_cluster_is_leader",
            "gauge",
            "Whether this node holds scheduled-job leadership "
            "(always 0 under distribution: spread; ownership is per job).",
        )
        is_leader.add({}, 1 if view.get("is_leader") else 0)
        families.append(is_leader)

        leader = view.get("leader")
        if leader:
            leader_info = MetricFamily(
                "yacron2_cluster_leader",
                "info",
                "The current cluster leader as observed by this node.",
            )
            leader_info.add({"leader": str(leader)}, 1)
            families.append(leader_info)

        conflict = MetricFamily(
            "yacron2_cluster_conflict",
            "gauge",
            "Detected coordination conflicts by kind; any 1 makes Leader "
            "jobs stand down cluster-wide.",
        )
        conflict.add(
            {"kind": "nodename"}, 1 if view.get("conflict_names") else 0
        )
        conflict.add({"kind": "size"}, 1 if view.get("size_conflict") else 0)
        conflict.add(
            {"kind": "policy"}, 1 if view.get("policy_conflict") else 0
        )
        families.append(conflict)

        peer_list = view.get("peers") or []
        if peer_list:
            peers = MetricFamily(
                "yacron2_cluster_peers",
                "gauge",
                "Configured gossip peers by observed status.",
            )
            counts = dict.fromkeys(PEER_STATUSES, 0)
            for peer in peer_list:
                status = str(peer.get("status", "unknown"))
                counts[status] = counts.get(status, 0) + 1
            for status, count in counts.items():
                peers.add({"status": status}, count)
            families.append(peers)
        return families
