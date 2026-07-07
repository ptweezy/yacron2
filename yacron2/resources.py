"""Per-run CPU and memory accounting for job subprocesses.

The wall-clock duration of a run is recorded by the scheduler
(:attr:`yacron2.cron.JobRunInfo.duration`, from ``started_at`` and
``finished_at``); this module adds the other half of the resource picture --
how much CPU time a run burned and how much resident memory it peaked at.

asyncio's subprocess reaping only surfaces the child's exit code, never its
``rusage``, so the numbers are gathered by *sampling* the job's process tree
with :mod:`psutil` while it runs.  A :class:`ResourceMonitor` is created with
the launched child's pid, polls the tree on a fixed interval, and hands back a
:class:`ResourceUsage` when the run ends.

Two properties of the design matter:

* **Best-effort, never fatal.**  Every psutil interaction is guarded: a
  process that exits mid-sample, a platform that denies the read, or psutil
  raising anything at all simply yields whatever was captured so far.  Resource
  accounting must never crash a job or the scheduler loop, and it never changes
  a job's success/failure verdict.

* **Sampled, so approximate for short runs.**  Peak RSS is a sampled
  high-water mark, and CPU time is accumulated per tree member (a departing
  member's last reading is banked before it is forgotten), so the only CPU
  that escapes accounting entirely is a child that spawns *and* exits within
  a single sampling gap.  The runs whose resource use actually matters -- the
  long, heavy ones -- are sampled many times and measured well.
"""

import asyncio
import logging
import math
import os
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, Iterator, List, Optional, Tuple

try:
    import psutil
except ImportError:  # pragma: no cover - psutil is a core dependency
    # Guarded so a source checkout missing the (unconditional) dependency
    # degrades to "monitoring unavailable" instead of failing to import
    # yacron2 at all; the feature is off by default anyway.
    psutil = None  # type: ignore[assignment]

logger = logging.getLogger("yacron2")

# How often (seconds) the monitor samples the process tree.  Peak RSS is a
# sampled high-water mark, so a shorter interval catches sharper spikes at the
# cost of more wakeups; total CPU is cumulative and re-read every sample, so it
# converges regardless of the interval as long as the run outlives one tick.
# Per-job override: monitorResources.interval (yacron2.config).
SAMPLE_INTERVAL = 1.0

# Default cap on the per-run CPU/RSS series retained for charts (points, not
# samples: a run longer than the cap is downsampled in place, see
# _SeriesRecorder).  Sized so a full series stays a few KB inside the durable
# run record.  Per-job override: monitorResources.history; 0 disables the
# series and keeps the summary numbers only.
MONITOR_HISTORY_DEFAULT = 240

# Hard cap applied when *parsing* a series out of a ledger record
# (ResourceUsage.from_dict): a foreign or hand-edited record must not be able
# to balloon the in-memory history, whatever it claims.
MAX_SERIES_POINTS = 4096

# Node history defaults: one sample every 5s, 720 points = the last hour.
# Overrides: web.nodeHistory.{interval,points} (yacron2.config).
NODE_HISTORY_INTERVAL = 5.0
NODE_HISTORY_POINTS = 720

# How long (seconds) a NodeResourceSampler.snapshot() result is memoised.
# psutil.cpu_percent(interval=None) measures "since the previous call" on one
# shared counter, and several consumers read snapshots within the same tick
# (the dashboard fires /cluster and /node together; every gossip peer payload
# reads one too) -- without the cache the later readers would measure the
# sub-100ms windows psutil documents as meaningless.
NODE_SNAPSHOT_TTL = 1.0

# psutil exceptions that mean "this particular process is gone / unreadable
# right now" -- expected during sampling as a tree shrinks, and always
# swallowed.  Kept as a tuple built defensively so importing this module never
# depends on psutil being present.
if psutil is not None:  # pragma: no branch - trivial availability guard
    _TRANSIENT_ERRORS: tuple = (
        psutil.NoSuchProcess,
        psutil.AccessDenied,
        psutil.ZombieProcess,
    )
else:  # pragma: no cover - only when the optional import failed
    _TRANSIENT_ERRORS = ()


class _SeriesRecorder:
    """A bounded, self-downsampling time series of ``[t, cpu%, rss]`` points.

    Samples arrive on a fixed cadence; the recorder groups them into buckets
    of ``stride`` samples (initially 1) and emits one point per full bucket:
    the bucket's *last* timestamp, its *average* CPU%, and its *peak* RSS --
    the aggregates that keep a downsampled chart honest (an averaged RSS would
    hide exactly the spikes people monitor memory for).  When the stored
    points hit the cap, adjacent pairs are merged with the same aggregation
    and the stride doubles, so a run of any length occupies at most
    ``maxpoints`` points with uniform bucket widths and its resolution decays
    gracefully (a 4-minute run keeps 1s buckets; a 3-day run ends up around
    20-minute buckets).

    Timestamps are wall-clock epoch seconds, so historical series from
    different runs (and different nodes on a shared ledger) line up on a
    common axis.  All mutation happens under the owning monitor's sample
    lock; :meth:`points` copies, so readers never see a half-merged list.
    """

    __slots__ = (
        "_max",
        "_points",
        "_stride",
        "_count",
        "_cpu_sum",
        "_rss_max",
        "_last_t",
    )

    def __init__(self, maxpoints: int) -> None:
        self._max = max(2, maxpoints)
        self._points: List[List[float]] = []
        self._stride = 1  # samples per emitted point
        # the accumulating (not yet emitted) bucket
        self._count = 0
        self._cpu_sum = 0.0
        self._rss_max = 0
        self._last_t = 0.0

    def add(self, t: float, cpu_percent: float, rss: int) -> None:
        self._count += 1
        self._cpu_sum += cpu_percent
        self._rss_max = max(self._rss_max, rss)
        self._last_t = t
        if self._count < self._stride:
            return
        self._points.append(
            [
                round(self._last_t, 2),
                round(self._cpu_sum / self._count, 2),
                self._rss_max,
            ]
        )
        self._count = 0
        self._cpu_sum = 0.0
        self._rss_max = 0
        if len(self._points) >= self._max:
            self._compact()

    def _compact(self) -> None:
        """Merge adjacent point pairs and double the stride."""
        merged: List[List[float]] = []
        pts = self._points
        for i in range(0, len(pts) - 1, 2):
            a, b = pts[i], pts[i + 1]
            merged.append(
                [b[0], round((a[1] + b[1]) / 2.0, 2), max(a[2], b[2])]
            )
        if len(pts) % 2:
            # an odd tail point (possible only right after a stride change)
            # stays as-is: it is the newest data, never worth dropping.
            merged.append(pts[-1])
        self._points = merged
        self._stride *= 2

    def points(self) -> List[List[float]]:
        """A copy of the emitted points, oldest first.

        The accumulating partial bucket is included as a provisional final
        point so a live chart tracks the newest reading instead of lagging up
        to a full (possibly minutes-wide) bucket behind.
        """
        out = [list(p) for p in self._points]
        if self._count:
            out.append(
                [
                    round(self._last_t, 2),
                    round(self._cpu_sum / self._count, 2),
                    self._rss_max,
                ]
            )
        return out


def _parse_series(raw: Any) -> Optional[List[List[float]]]:
    """Sanitise a ``series`` field from a ledger record, or ``None``.

    Applies the same distrust as :meth:`ResourceUsage.from_dict`: entries
    must be ``[t, cpu%, rss]`` triples of finite numbers (bools excluded --
    they are int subclasses), anything else is dropped point-wise, and the
    whole list is capped at :data:`MAX_SERIES_POINTS`.  Returns ``None``
    rather than an empty list so "no series" has a single spelling.
    """
    if not isinstance(raw, list):
        return None
    out: List[List[float]] = []
    for entry in raw[:MAX_SERIES_POINTS]:
        if not isinstance(entry, list) or len(entry) != 3:
            continue
        if any(isinstance(v, bool) for v in entry):
            continue
        try:
            t = float(entry[0])
            cpu = float(entry[1])
            rss = int(entry[2])
        except (TypeError, ValueError, OverflowError):
            continue
        if not (math.isfinite(t) and math.isfinite(cpu)):
            continue
        out.append([t, max(0.0, cpu), max(0, rss)])
    return out or None


@dataclass(slots=True)
class ResourceUsage:
    """A finished run's sampled CPU and memory usage.

    ``cpu_user_seconds``/``cpu_system_seconds`` are the total user- and
    system-mode CPU time the job's process tree consumed; ``max_rss_bytes`` is
    the highest resident-set size observed across all samples; ``samples`` is
    how many times the tree was successfully read (0 means nothing usable was
    captured, in which case the monitor returns ``None`` rather than a
    :class:`ResourceUsage`).
    """

    cpu_user_seconds: float
    cpu_system_seconds: float
    max_rss_bytes: int
    samples: int
    # downsampled [t, cpu%, rss] chart series for the run (bounded by the
    # job's monitorResources.history); None when series capture is off or
    # nothing was recorded.  Defaulted so pre-existing construction sites
    # (and summary-only ledger records) stay valid.
    series: Optional[List[List[float]]] = None

    @property
    def cpu_total_seconds(self) -> float:
        return self.cpu_user_seconds + self.cpu_system_seconds

    def to_dict(self, *, include_series: bool = False) -> Dict[str, Any]:
        """JSON-serialisable summary for the API / durable ledger.

        The chart series is opt-in (``include_series``): the durable ledger
        record and the dedicated resources endpoint carry it, while the
        polled dashboard payloads keep the summary-only shape so a monitored
        job does not multiply the size of every /jobs tick.
        """
        data: Dict[str, Any] = {
            "cpu_user_seconds": self.cpu_user_seconds,
            "cpu_system_seconds": self.cpu_system_seconds,
            # denormalised for convenience -- every consumer wants the total,
            # and recomputing it client-side is one more place to get it wrong.
            "cpu_total_seconds": self.cpu_total_seconds,
            "max_rss_bytes": self.max_rss_bytes,
            "samples": self.samples,
        }
        if include_series and self.series:
            data["series"] = [list(p) for p in self.series]
        return data

    @classmethod
    def from_dict(cls, data: Any) -> Optional["ResourceUsage"]:
        """Rebuild from :meth:`to_dict` output; ``None`` if malformed.

        Survives foreign / hand-edited ledger records the same way the run
        record parsers do: a missing or wrong-typed field yields ``None``
        (the run simply has no resource stats) rather than raising.
        """
        if not isinstance(data, dict):
            return None
        raw_user: Any = data.get("cpu_user_seconds")
        raw_system: Any = data.get("cpu_system_seconds")
        raw_rss: Any = data.get("max_rss_bytes")
        # bool is an int subclass, so float(True) would "succeed"; and stdlib
        # json.loads accepts NaN/Infinity from hand-edited ledger records,
        # which aiohttp's default json.dumps would happily re-emit and
        # browsers then reject.  Both count as malformed here.
        if any(isinstance(v, bool) for v in (raw_user, raw_system, raw_rss)):
            return None
        try:
            cpu_user = float(raw_user)
            cpu_system = float(raw_system)
            max_rss = int(raw_rss)
        except (TypeError, ValueError, OverflowError):
            # OverflowError: int(float("inf")).
            return None
        if not (math.isfinite(cpu_user) and math.isfinite(cpu_system)):
            return None
        samples = data.get("samples")
        if not isinstance(samples, int) or isinstance(samples, bool):
            samples = 0
        return cls(
            cpu_user,
            cpu_system,
            max_rss,
            samples,
            series=_parse_series(data.get("series")),
        )


class ResourceMonitor:
    """Samples a job subprocess tree for peak RSS and total CPU time.

    Constructed with the launched child's pid; :meth:`start` begins a
    background sampling task and :meth:`stop` ends it and returns the
    accumulated :class:`ResourceUsage` (or ``None`` when nothing could be
    sampled -- psutil absent, the pid already gone, or the run too short to
    catch a single reading).

    One instance monitors one run.  It is created and driven entirely on the
    scheduler's event loop, so it needs no locking.
    """

    def __init__(
        self,
        pid: int,
        *,
        job_name: str,
        interval: float = SAMPLE_INTERVAL,
        history: int = MONITOR_HISTORY_DEFAULT,
    ) -> None:
        self._pid = pid
        self._job_name = job_name
        self._interval = interval
        # bounded chart series of the run's samples (see _SeriesRecorder);
        # history <= 0 keeps the summary numbers only.
        self._recorder = _SeriesRecorder(history) if history > 0 else None
        self._task: Optional[asyncio.Task] = None
        self._proc: Any = None  # psutil.Process, once attached
        # Accumulated totals.  CPU time is tracked per tree member: _members
        # maps a (pid, create_time) key -- so a reused pid reads as a new
        # process -- to that member's last-seen (user, system) times, and a
        # member that leaves the tree has its last reading banked into the
        # _departed_* accumulators (see _sample), so sequential children all
        # count.
        self._cpu_user = 0.0
        self._cpu_system = 0.0
        self._max_rss = 0
        self._samples = 0
        self._members: Dict[tuple, tuple] = {}
        self._departed_user = 0.0
        self._departed_system = 0.0
        # Serialises _sample.  Normally only the single sampling task runs
        # it, but a cancelled to_thread call can leave its worker thread
        # finishing in the background while stop() takes the final reading.
        self._sample_lock = threading.Lock()
        # Live (instantaneous) readings for the "currently running" dashboard
        # view, updated every sample: the tree's current RSS (not the peak)
        # and its CPU% since the previous sample. _prev_cpu is (cpu_total,
        # monotonic instant) of the last sample, deriving the percentage.
        self._live_rss = 0
        self._live_cpu_percent = 0.0
        self._prev_cpu: Optional[tuple] = None

    @property
    def available(self) -> bool:
        """Whether monitoring could actually attach to the process."""
        return self._proc is not None

    def snapshot(self) -> Optional[Dict[str, Any]]:
        """Current live usage of the running tree, or ``None`` if unsampled.

        Read by the scheduler while the job is still running (see
        :meth:`yacron2.job.RunningJob.live_resources`) to drive the dashboard's
        live per-job CPU/memory readout.  ``cpu_seconds`` is cumulative,
        ``cpu_percent`` is the usage since the previous sample (can exceed 100
        across multiple cores), and ``rss_bytes`` is the tree's current
        resident memory.
        """
        if self._samples == 0:
            return None
        return {
            "cpu_seconds": self._cpu_user + self._cpu_system,
            "cpu_percent": self._live_cpu_percent,
            "rss_bytes": self._live_rss,
        }

    def series(self) -> Optional[List[List[float]]]:
        """The run-so-far ``[t, cpu%, rss]`` chart series, oldest first.

        ``None`` when series capture is off (history 0) or nothing has been
        sampled yet.  Taken under the sample lock -- a worker-thread sample
        may be mid-merge when the dashboard asks -- and already a copy, safe
        to serialise as-is.
        """
        if self._recorder is None:
            return None
        with self._sample_lock:
            pts = self._recorder.points()
        return pts or None

    def start(self) -> None:
        """Attach to the pid and begin sampling (no-op if unavailable).

        Safe to call unconditionally: if psutil is missing or the process has
        already exited, the monitor stays inert and :meth:`stop` returns
        ``None``.
        """
        if psutil is None:
            return
        try:
            self._proc = psutil.Process(self._pid)
        except Exception:  # noqa: BLE001 - NoSuchProcess/AccessDenied/anything
            # The child raced us and is already gone, or the platform denies
            # the read.  Leave the monitor inert.
            self._proc = None
            return
        # Take an immediate first reading so even a short run has a chance of a
        # sample, then poll on the interval.
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        try:
            while True:
                # Threaded because the sample walks the entire process table
                # (a full /proc scan on Linux), which must not block the
                # event loop.
                await asyncio.to_thread(self._sample)
                await asyncio.sleep(self._interval)
        except asyncio.CancelledError:
            # normal shutdown path (stop() cancels us); re-raise so the task
            # is marked cancelled rather than swallowing it.
            raise
        except Exception:  # noqa: BLE001 - a sampler bug must not crash the loop
            logger.warning(
                "Job %s: resource sampler stopped on an unexpected error",
                self._job_name,
                exc_info=True,
            )

    def _sample(self) -> None:
        """Read the process tree once, folding it into the running totals.

        Runs in a worker thread (see :meth:`_run`), guarded by the sample
        lock.  Everything written here is a plain int/float/dict read on the
        event loop under the GIL; :meth:`snapshot` may observe a mid-sample
        mix, which is harmless for a live readout.
        """
        with self._sample_lock:
            self._sample_locked()

    def _sample_locked(self) -> None:
        proc = self._proc
        if proc is None:
            return
        try:
            tree: List[Any] = [proc] + proc.children(recursive=True)
        except _TRANSIENT_ERRORS:
            # the root is gone; still try to read it alone below (it may be a
            # zombie whose cpu_times is readable on some platforms).
            tree = [proc]
        except Exception:  # noqa: BLE001 - never let sampling raise
            return
        live: Dict[tuple, tuple] = {}
        tree_pids = set()
        rss = 0
        for member in tree:
            tree_pids.add(member.pid)
            try:
                # oneshot() batches the per-process reads (one syscall set) so
                # cpu_times(), memory_info() and create_time() are cheap and
                # consistent.
                with member.oneshot():
                    times = member.cpu_times()
                    mem = member.memory_info()
                    key = (member.pid, member.create_time())
            except _TRANSIENT_ERRORS:
                continue  # this member exited mid-sample; skip it
            except Exception:  # noqa: BLE001 - never let sampling raise
                continue
            live[key] = (times.user, times.system)
            rss += mem.rss
        if not live:
            return
        self._samples += 1
        # Per-process CPU time is monotonic, but tree membership changes: a
        # member missing from this sample has exited, so bank its last-seen
        # reading in the departed accumulators before forgetting it.  The
        # (pid, create_time) key keeps a reused pid from being mistaken for
        # the process that departed.  A member that is still listed in the
        # tree but failed this round's read (a transient AccessDenied, say)
        # has NOT departed: carry its last reading forward instead, or its
        # next successful read would double-count on top of the banked value.
        live_pids = {k[0] for k in live}
        for key, (user, system) in self._members.items():
            if key in live:
                continue
            if key[0] in tree_pids and key[0] not in live_pids:
                live[key] = (user, system)
                continue
            self._departed_user += user
            self._departed_system += system
        self._members = live
        # Totals are departed + live, so sequential children (`sh -c 'a; b'`)
        # accumulate instead of plateauing at the largest instantaneous tree
        # sum.  The max() only guards against per-sample jitter in the
        # readings ever nudging a total backwards.
        cpu_user = self._departed_user + sum(u for u, _ in live.values())
        cpu_system = self._departed_system + sum(s for _, s in live.values())
        self._cpu_user = max(self._cpu_user, cpu_user)
        self._cpu_system = max(self._cpu_system, cpu_system)
        self._max_rss = max(self._max_rss, rss)
        # live readings: current tree RSS + CPU% since the previous sample.
        self._live_rss = rss
        cpu_total = self._cpu_user + self._cpu_system
        now = time.monotonic()
        if self._prev_cpu is not None:
            prev_total, prev_t = self._prev_cpu
            dt = now - prev_t
            if dt > 0:
                self._live_cpu_percent = max(
                    0.0, (cpu_total - prev_total) / dt * 100.0
                )
        self._prev_cpu = (cpu_total, now)
        # chart series: one [t, cpu%, rss] point per sample, downsampled in
        # place by the recorder.  The first sample's 0.0 CPU% is recorded
        # as-is (there is no previous sample to measure against).
        if self._recorder is not None:
            self._recorder.add(time.time(), self._live_cpu_percent, rss)

    async def stop(self) -> Optional[ResourceUsage]:
        """Stop sampling and return the accumulated usage (``None`` if none).

        Idempotent: a second call (or a call after a monitor that never
        attached) returns the same result without error.
        """
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        # One last opportunistic read: on a cancel/replace path the child may
        # still be alive here, and this catches its final CPU/RSS (any
        # still-live members are summed into the totals by _sample itself).
        # Threaded like the periodic samples, to keep the process-table walk
        # off the loop.
        await asyncio.to_thread(self._sample)
        if self._samples == 0:
            return None
        return ResourceUsage(
            cpu_user_seconds=self._cpu_user,
            cpu_system_seconds=self._cpu_system,
            max_rss_bytes=self._max_rss,
            samples=self._samples,
            series=self.series(),
        )


# Where the cgroup v2 unified hierarchy is mounted on every mainstream Linux
# distribution.  Overridable in the reader's constructor purely for tests.
CGROUP_ROOT = "/sys/fs/cgroup"


class _CgroupV2Reader:
    """Best-effort reader of this process's cgroup v2 slice.

    psutil's node-wide counters come from ``/proc``, which is *not* virtualised
    by cgroups: inside a container (or a systemd slice with limits) they
    describe the whole host, so a daemon confined to 512 MiB would happily
    report the host's 64 GiB as its memory.  This reader recovers the
    container's own slice by reading the unified-hierarchy files directly --
    the same sources ``docker stats`` and kubelet use:

    * ``memory.max`` -- the memory limit (``max`` means unlimited).
    * ``memory.current`` minus ``memory.stat``'s ``inactive_file`` -- usage
      with the easily-reclaimable page cache excluded, matching how the docker
      CLI and Kubernetes' working-set metric count "used" (raw
      ``memory.current`` includes file cache and reads misleadingly high).
    * ``cpu.max`` -- ``<quota> <period>`` in microseconds; quota/period is the
      number of CPUs the slice may burn (``max`` quota means unlimited).
    * ``cpu.stat``'s ``usage_usec`` -- cumulative CPU time, whose delta over
      wall-clock time yields utilisation.

    Limits are hierarchical, so both lookups walk from our own cgroup up to
    the mount root and take the *lowest* limit on the path (as the JVM's
    container-awareness does).  The reader stays inert -- every method returns
    ``None`` -- on cgroup v1 / hybrid hosts, on non-Linux platforms, and when
    no limit is set anywhere on the path, in which case the caller keeps its
    host-wide numbers.  ``cpuset`` pinning is deliberately not considered:
    ``--cpus``-style quotas are how container platforms hand out CPU.

    Like everything in this module it is best-effort and never fatal: any
    unreadable or malformed file simply yields ``None``.
    """

    def __init__(
        self,
        root: str = CGROUP_ROOT,
        proc_cgroup: str = "/proc/self/cgroup",
    ) -> None:
        self._root = os.path.normpath(root)
        self._dir: Optional[str] = None
        try:
            self._dir = self._resolve_own_dir(self._root, proc_cgroup)
        except Exception:  # noqa: BLE001 - never fatal
            self._dir = None

    @staticmethod
    def _resolve_own_dir(root: str, proc_cgroup: str) -> Optional[str]:
        """Locate this process's cgroup directory, or ``None``.

        Only the unified (v2) hierarchy is supported, marked by the
        ``cgroup.controllers`` file at the mount root.  The process's own
        position comes from the ``0::<path>`` line of ``/proc/self/cgroup``;
        with cgroup namespaces (the container default) that path is ``/`` and
        the mount root *is* our slice.  Without namespaces the host-side path
        may not exist under our mount, in which case the reader stays inert.
        """
        if not os.path.isfile(os.path.join(root, "cgroup.controllers")):
            return None
        rel: Optional[str] = None
        with open(proc_cgroup, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                # the v2 entry is "0::<path>"; v1 lines carry controller
                # names in the middle field and never match this prefix.
                if line.startswith("0::"):
                    rel = line[3:].strip().lstrip("/")
                    break
        if rel is None:
            return None
        if ".." in rel.split("/"):
            return None  # never escape the mount root
        path = os.path.normpath(os.path.join(root, rel)) if rel else root
        if not os.path.isdir(path):
            return None
        return path

    @property
    def available(self) -> bool:
        """Whether a v2 hierarchy was found and our cgroup dir resolved."""
        return self._dir is not None

    def _ancestry(self) -> Iterator[str]:
        """Our cgroup dir, then each ancestor up to the mount root."""
        d = self._dir
        if d is None:  # callers check available first; belt and braces
            return
        while True:
            yield d
            if d == self._root:
                return
            parent = os.path.dirname(d)
            if parent == d:  # filesystem root; never walked above the mount
                return
            d = parent

    @staticmethod
    def _read_first_line(path: str) -> Optional[str]:
        try:
            with open(path, encoding="ascii", errors="replace") as fh:
                return fh.readline().strip()
        except OSError:
            return None

    @staticmethod
    def _read_stat_field(path: str, field: str) -> Optional[int]:
        """A ``<field> <value>`` line from a flat keyed file, or ``None``."""
        try:
            with open(path, encoding="ascii", errors="replace") as fh:
                for line in fh:
                    parts = line.split()
                    if len(parts) == 2 and parts[0] == field:
                        return int(parts[1])
        except (OSError, ValueError):
            return None
        return None

    def memory_limit(self) -> Optional[int]:
        """Lowest ``memory.max`` on the path to the root, or ``None``."""
        if self._dir is None:
            return None
        limit: Optional[int] = None
        for d in self._ancestry():
            raw = self._read_first_line(os.path.join(d, "memory.max"))
            if raw is None or raw == "max":
                continue
            try:
                value = int(raw)
            except ValueError:
                continue
            if value > 0 and (limit is None or value < limit):
                limit = value
        return limit

    def memory_used(self) -> Optional[int]:
        """``memory.current`` less reclaimable file cache, or ``None``."""
        if self._dir is None:
            return None
        raw = self._read_first_line(os.path.join(self._dir, "memory.current"))
        if raw is None:
            return None
        try:
            current = int(raw)
        except ValueError:
            return None
        # inactive_file is page cache the kernel can drop without swapping,
        # so it does not count against "how close to the limit am I" (docker
        # stats and the k8s working-set subtract it too).  A missing/broken
        # memory.stat just leaves the raw figure.
        inactive = self._read_stat_field(
            os.path.join(self._dir, "memory.stat"), "inactive_file"
        )
        if inactive is not None:
            current -= inactive
        return max(0, current)

    def cpu_limit(self) -> Optional[float]:
        """Lowest ``cpu.max`` quota on the path, in CPUs, or ``None``."""
        if self._dir is None:
            return None
        limit: Optional[float] = None
        for d in self._ancestry():
            raw = self._read_first_line(os.path.join(d, "cpu.max"))
            if raw is None:
                continue
            parts = raw.split()
            if not parts or parts[0] == "max":
                continue
            try:
                quota = int(parts[0])
                # the period defaults to 100ms when the file (abnormally)
                # carries only the quota.
                period = int(parts[1]) if len(parts) > 1 else 100_000
            except ValueError:
                continue
            if quota <= 0 or period <= 0:
                continue
            value = quota / period
            if limit is None or value < limit:
                limit = value
        return limit

    def cpu_usage_seconds(self) -> Optional[float]:
        """Cumulative CPU seconds consumed by our slice, or ``None``."""
        if self._dir is None:
            return None
        usage = self._read_stat_field(
            os.path.join(self._dir, "cpu.stat"), "usage_usec"
        )
        if usage is None or usage < 0:
            return None
        return usage / 1_000_000.0


class NodeResourceSampler:
    """Whole-node (and own-process) CPU/memory for the local host.

    One long-lived instance per daemon (owned by the scheduler); its
    :meth:`snapshot` is read whenever the dashboard asks for node stats, and --
    in a gossip cluster -- advertised to peers so the fleet view shows every
    node's load.  ``psutil.cpu_percent(interval=None)`` reports usage *since
    the previous call*, so the first snapshot after construction is a priming
    ``0.0`` and every later one covers the interval since the last fresh read
    (snapshots are memoised for :data:`NODE_SNAPSHOT_TTL`, so near-simultaneous
    readers share one measurement window instead of shrinking it for each
    other).

    Container-aware: when this process runs under a cgroup v2 limit (a
    container, or a systemd slice with ``MemoryMax``/``CPUQuota``), the
    CPU/memory fields describe *our slice* -- limit as the total, usage and
    percentage measured against it -- instead of the host-wide numbers psutil
    reports from ``/proc`` (see :class:`_CgroupV2Reader`).  Each resource
    falls back to host-wide independently when it has no cgroup limit, and
    the snapshot's shape never changes either way.

    Best-effort like everything else here: any psutil error yields ``None``
    rather than raising, so a node that cannot read its own stats simply shows
    none.
    """

    def __init__(self) -> None:
        self._proc: Any = None
        # snapshot() memoisation (see NODE_SNAPSHOT_TTL).
        self._cache: Optional[Dict[str, Any]] = None
        self._cache_time = 0.0
        if psutil is not None:
            try:
                self._proc = psutil.Process()
                # prime the "since last call" counters so the first real
                # snapshot is meaningful rather than always-zero.
                psutil.cpu_percent(interval=None)
                self._proc.cpu_percent(interval=None)
            except Exception:  # noqa: BLE001 - never fatal
                self._proc = None
        # cgroup-slice overlay (inert off Linux / without v2 / unlimited).
        # The CPU reading is a delta between snapshots, so prime it here the
        # same way the psutil counters are primed above.
        self._cgroup = _CgroupV2Reader()
        self._cgroup_prev_cpu: Optional[Tuple[float, float]] = None
        if self._cgroup.available:
            usage = self._cgroup.cpu_usage_seconds()
            if usage is not None:
                self._cgroup_prev_cpu = (usage, time.monotonic())
        # background node history (see start_history): a bounded ring of
        # [t, cpu%, mem%] points feeding the dashboard's node chart.
        self._history: Optional[Deque[List[float]]] = None
        self._history_interval = NODE_HISTORY_INTERVAL
        self._history_task: Optional[asyncio.Task] = None

    def snapshot(self) -> Optional[Dict[str, Any]]:
        """Current node CPU%/memory (+ this daemon's own), or ``None``."""
        if psutil is None:
            return None
        # Memoised (see NODE_SNAPSHOT_TTL) so back-to-back readers share one
        # measurement window instead of each resetting the since-last-call
        # CPU counters.  Callers get a copy, so mutating a returned snapshot
        # cannot poison the cache.
        now = time.monotonic()
        if (
            self._cache is not None
            and now - self._cache_time < NODE_SNAPSHOT_TTL
        ):
            return dict(self._cache)
        try:
            vm = psutil.virtual_memory()
            data: Dict[str, Any] = {
                # system-wide CPU utilisation since the previous snapshot,
                # 0..100 (already averaged across cores by psutil).
                "cpu_percent": psutil.cpu_percent(interval=None),
                "cpu_count": psutil.cpu_count() or 0,
                "mem_percent": vm.percent,
                "mem_used_bytes": vm.total - vm.available,
                "mem_total_bytes": vm.total,
            }
        except Exception:  # noqa: BLE001 - never fatal
            logger.warning("node resource sampling failed", exc_info=True)
            return None
        # inside a cgroup limit, replace the host-wide numbers with our
        # slice's (same keys, so every consumer -- dashboard, gossip peers,
        # the /node endpoint -- is agnostic to the source).
        if self._cgroup.available:
            self._overlay_cgroup(data)
        # the daemon's own footprint, best-effort on top (may be denied on
        # some platforms even when the system-wide read succeeded).
        if self._proc is not None:
            try:
                data["proc_rss_bytes"] = self._proc.memory_info().rss
                data["proc_cpu_percent"] = self._proc.cpu_percent(
                    interval=None
                )
            except Exception:  # noqa: BLE001 - never fatal
                pass
        self._cache = data
        self._cache_time = now
        return dict(data)

    def _overlay_cgroup(self, data: Dict[str, Any]) -> None:
        """Swap host-wide fields for our cgroup slice's, where limited.

        Memory and CPU are overlaid independently -- a container run with
        ``-m 512m`` but no ``--cpus`` keeps the host-wide CPU reading, which
        is what it can genuinely use.  ``cpu_percent`` is measured against
        the quota (0..100 of *our allowance*, mirroring ``mem_percent``
        against the limit) and ``cpu_count`` becomes the quota rounded up,
        as the JVM's ``availableProcessors`` does.  Any hiccup leaves the
        already-populated host-wide values in place.
        """
        try:
            limit = self._cgroup.memory_limit()
            if limit:
                used = self._cgroup.memory_used()
                if used is not None:
                    data["mem_total_bytes"] = limit
                    data["mem_used_bytes"] = used
                    data["mem_percent"] = round(
                        min(100.0, used * 100.0 / limit), 1
                    )
            quota = self._cgroup.cpu_limit()
            if quota:
                usage = self._cgroup.cpu_usage_seconds()
                if usage is not None:
                    now = time.monotonic()
                    prev = self._cgroup_prev_cpu
                    self._cgroup_prev_cpu = (usage, now)
                    if prev is not None:
                        delta, dt = usage - prev[0], now - prev[1]
                        # micro-bursts can nudge usage past the quota within
                        # a window; clamp like mem_percent above.
                        if dt > 0 and delta >= 0:
                            data["cpu_percent"] = round(
                                min(100.0, delta * 100.0 / dt / quota), 1
                            )
                data["cpu_count"] = max(1, math.ceil(quota))
        except Exception:  # noqa: BLE001 - never fatal
            pass

    def start_history(self, *, interval: float, points: int) -> None:
        """Begin (or reconfigure) background node-history sampling.

        Idempotent per configuration: called on every web-app (re)start, it
        leaves a running task alone when nothing changed, and rebuilds the
        ring -- carrying over the retained points -- when the window size
        changes, so a config reload does not blank the node chart.  No-op
        when psutil is unavailable (snapshots would never land anyway).
        """
        if psutil is None:
            return
        if (
            self._history_task is not None
            and not self._history_task.done()
            and self._history is not None
            and self._history.maxlen == points
            and self._history_interval == interval
        ):
            return
        retained = list(self._history) if self._history is not None else []
        self._history = deque(retained[-points:], maxlen=points)
        self._history_interval = interval
        if self._history_task is not None:
            self._history_task.cancel()
        self._history_task = asyncio.create_task(self._history_run())

    async def stop_history(self) -> None:
        """Cancel the history task (retaining the ring for a later restart)."""
        task, self._history_task = self._history_task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _history_run(self) -> None:
        try:
            while True:
                snap = self.snapshot()
                history = self._history
                if snap is not None and history is not None:
                    history.append(
                        [
                            round(time.time(), 2),
                            round(float(snap.get("cpu_percent") or 0.0), 2),
                            round(float(snap.get("mem_percent") or 0.0), 2),
                        ]
                    )
                await asyncio.sleep(self._history_interval)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - history must never crash the loop
            logger.warning(
                "node history sampler stopped on an unexpected error",
                exc_info=True,
            )

    def history(self) -> Optional[Dict[str, Any]]:
        """The retained node history, or ``None`` when never started.

        ``points`` is oldest-first ``[t, cpu%, mem%]``; ``interval`` is the
        sampling cadence so a chart can mark gaps (a stretch with no points
        wider than the cadence means the daemon was down, not idle).
        """
        if self._history is None:
            return None
        return {
            "interval": self._history_interval,
            "points": [list(p) for p in self._history],
        }


def resolve_node_history_config(
    web_config: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Resolve the raw ``web.nodeHistory`` option into effective settings.

    Returns ``None`` when disabled, else ``{"interval", "points"}``.  Enabled
    by default whenever the web API is on; ``nodeHistory: false`` /
    ``nodeHistory: true`` are shorthands for the map form (mirroring how
    ``web.metrics`` resolves in yacron2.prometheus).
    """
    raw = web_config.get("nodeHistory")
    if raw is False:
        return None
    if raw is None or raw is True:
        raw = {}
    if not raw.get("enabled", True):
        return None
    return {
        "interval": float(raw.get("interval", NODE_HISTORY_INTERVAL)),
        "points": int(raw.get("points", NODE_HISTORY_POINTS)),
    }
