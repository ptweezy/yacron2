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
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

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
SAMPLE_INTERVAL = 1.0

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

    @property
    def cpu_total_seconds(self) -> float:
        return self.cpu_user_seconds + self.cpu_system_seconds

    def to_dict(self) -> Dict[str, Any]:
        """JSON-serialisable summary for the API / durable ledger."""
        return {
            "cpu_user_seconds": self.cpu_user_seconds,
            "cpu_system_seconds": self.cpu_system_seconds,
            # denormalised for convenience -- every consumer wants the total,
            # and recomputing it client-side is one more place to get it wrong.
            "cpu_total_seconds": self.cpu_total_seconds,
            "max_rss_bytes": self.max_rss_bytes,
            "samples": self.samples,
        }

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
        if any(
            isinstance(v, bool) for v in (raw_user, raw_system, raw_rss)
        ):
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
        return cls(cpu_user, cpu_system, max_rss, samples)


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
    ) -> None:
        self._pid = pid
        self._job_name = job_name
        self._interval = interval
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
        cpu_system = self._departed_system + sum(
            s for _, s in live.values()
        )
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
        )


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
