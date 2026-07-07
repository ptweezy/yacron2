"""Tests for per-run CPU/memory accounting (yacron2.resources)."""

import asyncio
import contextlib
import sys
from types import SimpleNamespace

import pytest

import yacron2.resources as resources
from yacron2.resources import (
    NODE_SNAPSHOT_TTL,
    NodeResourceSampler,
    ResourceMonitor,
    ResourceUsage,
)

# a short busy-loop that also holds a chunk of memory, so both CPU time and
# RSS are non-trivially observable while the monitor samples it. Runs the test
# interpreter so it is portable (no /bin/sh), like tests/_commands.py.
_BUSY = (
    "import time; "
    "buf = bytearray(20 * 1024 * 1024); "  # ~20 MiB resident
    "buf[::4096] = b'x' * len(buf[::4096]); "  # touch pages so they fault in
    "end = time.time() + {dur}; "
    "x = 0\n"
    "while time.time() < end: x += 1"
)


async def _spawn_busy(dur):
    return await asyncio.create_subprocess_exec(
        sys.executable, "-c", _BUSY.format(dur=dur)
    )


# ---- ResourceUsage (de)serialization -------------------------------------


def test_resource_usage_to_dict_round_trip():
    usage = ResourceUsage(
        cpu_user_seconds=1.5,
        cpu_system_seconds=0.5,
        max_rss_bytes=1234567,
        samples=4,
    )
    data = usage.to_dict()
    assert data["cpu_total_seconds"] == 2.0
    restored = ResourceUsage.from_dict(data)
    assert restored == usage


@pytest.mark.parametrize(
    "bad",
    [
        None,
        "not a dict",
        {},  # missing required keys
        {"cpu_user_seconds": "x", "cpu_system_seconds": 0, "max_rss_bytes": 0},
        {"cpu_user_seconds": 1.0, "cpu_system_seconds": 1.0},  # no rss
        # stdlib json.loads parses NaN/Infinity from hand-edited ledgers;
        # neither may survive into API payloads (browsers reject them).
        {
            "cpu_user_seconds": float("nan"),
            "cpu_system_seconds": 0.0,
            "max_rss_bytes": 0,
        },
        {
            "cpu_user_seconds": 0.0,
            "cpu_system_seconds": float("inf"),
            "max_rss_bytes": 0,
        },
        {
            "cpu_user_seconds": 0.0,
            "cpu_system_seconds": 0.0,
            "max_rss_bytes": float("inf"),
        },
        # bool is an int subclass, but True is not a CPU time.
        {
            "cpu_user_seconds": True,
            "cpu_system_seconds": 0.0,
            "max_rss_bytes": 0,
        },
        {
            "cpu_user_seconds": 0.0,
            "cpu_system_seconds": 0.0,
            "max_rss_bytes": False,
        },
    ],
)
def test_resource_usage_from_dict_tolerates_garbage(bad):
    assert ResourceUsage.from_dict(bad) is None


def test_resource_usage_from_dict_defaults_samples():
    usage = ResourceUsage.from_dict(
        {
            "cpu_user_seconds": 1.0,
            "cpu_system_seconds": 2.0,
            "max_rss_bytes": 42,
            # samples absent / wrong type -> 0
            "samples": True,
        }
    )
    assert usage is not None
    assert usage.samples == 0
    assert usage.cpu_total_seconds == 3.0


# ---- ResourceMonitor lifecycle -------------------------------------------


@pytest.mark.asyncio
async def test_monitor_samples_a_real_process():
    proc = await _spawn_busy(0.6)
    monitor = ResourceMonitor(proc.pid, job_name="busy", interval=0.05)
    monitor.start()
    assert monitor.available
    await proc.wait()
    usage = await monitor.stop()
    assert usage is not None
    assert usage.samples >= 1
    # it burned CPU and held memory; both must be strictly positive.
    assert usage.cpu_total_seconds > 0.0
    assert usage.max_rss_bytes > 0


@pytest.mark.asyncio
async def test_monitor_live_snapshot():
    proc = await _spawn_busy(0.6)
    monitor = ResourceMonitor(proc.pid, job_name="busy", interval=0.05)
    # no sample yet -> no live snapshot
    assert monitor.snapshot() is None
    monitor.start()
    await asyncio.sleep(0.2)  # let a couple of samples land while it runs
    snap = monitor.snapshot()
    assert snap is not None
    assert set(snap) == {"cpu_seconds", "cpu_percent", "rss_bytes"}
    assert snap["rss_bytes"] > 0
    assert snap["cpu_percent"] >= 0.0
    await proc.wait()
    await monitor.stop()


class _FakeProcess:
    """Minimal psutil.Process stand-in for driving _sample() directly."""

    def __init__(self, pid, create_time, user=0.0, system=0.0, rss=1024):
        self.pid = pid
        self._create_time = create_time
        self.user = user
        self.system = system
        self.rss = rss
        self.child_list = []

    def oneshot(self):
        return contextlib.nullcontext()

    def cpu_times(self):
        return SimpleNamespace(user=self.user, system=self.system)

    def memory_info(self):
        return SimpleNamespace(rss=self.rss)

    def create_time(self):
        return self._create_time

    def children(self, recursive=False):
        return list(self.child_list)


@pytest.mark.asyncio
async def test_monitor_accumulates_sequential_children():
    # regression: an `sh -c 'a; b'` style run, where one child exits before
    # the next starts, must accumulate every child's CPU time rather than
    # plateauing at the largest instantaneous tree sum.
    monitor = ResourceMonitor(123, job_name="seq", interval=0.05)
    root = _FakeProcess(pid=123, create_time=100.0, user=0.1)
    child_a = _FakeProcess(pid=200, create_time=101.0, user=10.0)
    # child B reuses child A's pid (fresh create_time): the accounting must
    # treat it as a new process, not a rewind of child A's counters.
    child_b = _FakeProcess(pid=200, create_time=102.0, user=0.5)
    monitor._proc = root

    root.child_list = [child_a]
    monitor._sample()

    # child A exits; child B starts with fresh near-zero counters.
    root.user = 0.2
    root.child_list = [child_b]
    monitor._sample()

    child_b.user = 9.0
    monitor._sample()

    usage = await monitor.stop()
    assert usage is not None
    assert usage.samples == 4  # three explicit + stop()'s final read
    # 10.0 (child A) + 9.0 (child B) + 0.2 (root), not the ~10.1 a running
    # max of tree sums would report.
    assert usage.cpu_user_seconds == pytest.approx(19.2)
    # the live cumulative readout reflects the accumulated total too.
    snap = monitor.snapshot()
    assert snap is not None
    assert snap["cpu_seconds"] == pytest.approx(19.2)


@pytest.mark.asyncio
async def test_monitor_transient_read_failure_does_not_double_count():
    # regression: a member that stays in the tree but fails one read (a
    # transient AccessDenied) has not departed -- its last reading must be
    # carried forward, not banked, or the next successful read would count
    # its CPU twice (banked total + full cumulative reading).
    import psutil

    monitor = ResourceMonitor(123, job_name="flaky", interval=0.05)
    root = _FakeProcess(pid=123, create_time=100.0, user=0.1)
    child = _FakeProcess(pid=200, create_time=101.0, user=5.0)
    monitor._proc = root
    root.child_list = [child]
    monitor._sample()

    # one transient failure while the child is still listed in the tree.
    real_cpu_times = child.cpu_times
    child.cpu_times = lambda: (_ for _ in ()).throw(psutil.AccessDenied(200))
    monitor._sample()

    # the read recovers with the cumulative counter a bit higher.
    child.cpu_times = real_cpu_times
    child.user = 6.0
    monitor._sample()

    usage = await monitor.stop()
    assert usage is not None
    # 0.1 (root) + 6.0 (child), not 11.1 from banking the 5.0 reading and
    # then re-counting the child's full cumulative time on top.
    assert usage.cpu_user_seconds == pytest.approx(6.1)


def test_node_sampler_snapshot():
    sampler = NodeResourceSampler()
    snap = sampler.snapshot()
    # psutil is a core dependency, so a snapshot is expected here
    assert snap is not None
    for key in ("cpu_percent", "mem_percent", "mem_used_bytes",
                "mem_total_bytes"):
        assert key in snap
    assert 0 <= snap["mem_percent"] <= 100
    assert snap["mem_total_bytes"] > 0


def test_node_sampler_without_psutil(monkeypatch):
    monkeypatch.setattr("yacron2.resources.psutil", None)
    assert NodeResourceSampler().snapshot() is None


def test_node_sampler_snapshot_is_memoised(monkeypatch):
    # snapshot() is cached for NODE_SNAPSHOT_TTL so near-simultaneous readers
    # (dashboard endpoints, gossip payloads) share one measurement window
    # instead of resetting psutil's since-last-call CPU counter on each
    # other. Clock is monkeypatched -- no real sleeps, no timing windows.
    sampler = NodeResourceSampler()
    clock = {"now": 1000.0}
    monkeypatch.setattr(resources.time, "monotonic", lambda: clock["now"])
    calls = []
    real_cpu_percent = resources.psutil.cpu_percent

    def counting_cpu_percent(interval=None):
        calls.append(interval)
        return real_cpu_percent(interval)

    monkeypatch.setattr(resources.psutil, "cpu_percent",
                        counting_cpu_percent)

    first = sampler.snapshot()
    second = sampler.snapshot()
    assert first is not None
    assert first == second
    assert first is not second  # callers get copies, not the cache itself
    assert len(calls) == 1  # psutil sampled once for both reads

    # mutating a returned snapshot must not poison the cache.
    second["cpu_percent"] = -12345.0
    assert sampler.snapshot()["cpu_percent"] != -12345.0

    # once the TTL lapses, the node is actually sampled again.
    clock["now"] += NODE_SNAPSHOT_TTL + 0.001
    assert sampler.snapshot() is not None
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_monitor_stop_is_idempotent():
    proc = await _spawn_busy(0.2)
    monitor = ResourceMonitor(proc.pid, job_name="busy", interval=0.05)
    monitor.start()
    await proc.wait()
    first = await monitor.stop()
    # a second stop must not raise and returns the same accumulated value.
    second = await monitor.stop()
    assert first == second


@pytest.mark.asyncio
async def test_monitor_bogus_pid_is_inert():
    # a pid that is (almost certainly) not a live process: the monitor must
    # stay inert and yield no usage rather than raising.
    monitor = ResourceMonitor(2**31 - 1, job_name="ghost", interval=0.05)
    monitor.start()
    assert not monitor.available
    assert await monitor.stop() is None


@pytest.mark.asyncio
async def test_monitor_without_psutil_is_noop(monkeypatch):
    # simulate a checkout without the optional import resolving.
    monkeypatch.setattr("yacron2.resources.psutil", None)
    monitor = ResourceMonitor(1, job_name="x", interval=0.05)
    monitor.start()
    assert not monitor.available
    assert await monitor.stop() is None
