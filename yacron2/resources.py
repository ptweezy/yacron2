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
  high-water mark and total CPU is read from the live tree, so a run that
  finishes between two samples (or whose short-lived grandchildren come and go
  between samples) is accounted only approximately.  The runs whose resource
  use actually matters -- the long, heavy ones -- are sampled many times and
  measured well.
"""

import asyncio
import logging
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
        try:
            cpu_user = float(data["cpu_user_seconds"])
            cpu_system = float(data["cpu_system_seconds"])
            max_rss = int(data["max_rss_bytes"])
        except (KeyError, TypeError, ValueError):
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
        # Running high-water marks.  CPU totals are kept as a running max
        # (see _sample) so a child's CPU is not forgotten when it leaves the
        # tree between samples.
        self._cpu_user = 0.0
        self._cpu_system = 0.0
        self._max_rss = 0
        self._samples = 0
        # Live (instantaneous) readings for the "currently running" dashboard
        # view, updated every sample: the tree's current RSS (not the peak) and
        # its CPU% since the previous sample. _prev_cpu is (cpu_total, monotonic
        # instant) of the last sample, from which the percentage is derived.
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
                self._sample()
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
        """Read the process tree once, folding it into the high-water marks."""
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
        cpu_user = 0.0
        cpu_system = 0.0
        rss = 0
        got = False
        for member in tree:
            try:
                # oneshot() batches the per-process reads (one syscall set) so
                # cpu_times() and memory_info() are cheap and consistent.
                with member.oneshot():
                    times = member.cpu_times()
                    mem = member.memory_info()
            except _TRANSIENT_ERRORS:
                continue  # this member exited mid-sample; skip it
            except Exception:  # noqa: BLE001 - never let sampling raise
                continue
            cpu_user += times.user
            cpu_system += times.system
            rss += mem.rss
            got = True
        if not got:
            return
        self._samples += 1
        # Per-process CPU time is monotonic, but the TREE's membership shrinks
        # as children exit, so a later tree-sum can be smaller than an earlier
        # one.  Keeping the running max preserves the CPU a since-departed
        # child already contributed; the root's own time keeps climbing on top.
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
        # still be alive here, and this catches its final CPU/RSS.
        self._sample()
        if self._samples == 0:
            return None
        return ResourceUsage(
            cpu_user_seconds=self._cpu_user,
            cpu_system_seconds=self._cpu_system,
            max_rss_bytes=self._max_rss,
            samples=self._samples,
        )
