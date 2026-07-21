"""Tests for per-run CPU/memory accounting (cronstable.resources)."""

import asyncio
import contextlib
import logging
import sys
from collections import deque
from types import SimpleNamespace

import psutil
import pytest

import cronstable.resources as resources
from cronstable.resources import (
    MAX_SERIES_POINTS,
    NODE_SNAPSHOT_TTL,
    NodeResourceSampler,
    ResourceMonitor,
    ResourceUsage,
    _SeriesRecorder,
    resolve_node_history_config,
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
    monkeypatch.setattr("cronstable.resources.psutil", None)
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
    monkeypatch.setattr("cronstable.resources.psutil", None)
    monitor = ResourceMonitor(1, job_name="x", interval=0.05)
    monitor.start()
    assert not monitor.available
    assert await monitor.stop() is None


# ---- cgroup v2 container awareness ----------------------------------------
#
# All hermetic: the reader takes its mount root and /proc/self/cgroup paths as
# constructor arguments, so these build a fake cgroup v2 tree under tmp_path
# and run identically on every platform (including Windows).

MIB = 1024 * 1024


def _fake_cgroup(tmp_path, rel="box", proc_line=None):
    """A fake unified-hierarchy mount; returns (root, own-dir, proc-file)."""
    root = tmp_path / "cgroup"
    root.mkdir(exist_ok=True)
    (root / "cgroup.controllers").write_text("cpu memory\n")
    d = root
    for part in rel.split("/"):
        if part:
            d = d / part
            d.mkdir(exist_ok=True)
    proc = tmp_path / "proc_self_cgroup"
    proc.write_text(proc_line if proc_line is not None else f"0::/{rel}\n")
    return root, d, proc


def _reader(root, proc):
    return resources._CgroupV2Reader(str(root), str(proc))


def test_cgroup_reader_needs_v2_marker(tmp_path):
    # no cgroup.controllers at the root -> v1/hybrid host -> inert.
    root, d, proc = _fake_cgroup(tmp_path)
    (root / "cgroup.controllers").unlink()
    reader = _reader(root, proc)
    assert not reader.available
    assert reader.memory_limit() is None
    assert reader.memory_used() is None
    assert reader.cpu_limit() is None
    assert reader.cpu_usage_seconds() is None


def test_cgroup_reader_inert_when_own_dir_missing(tmp_path):
    # a host-side path that is not mounted here (container without cgroupns).
    root, d, proc = _fake_cgroup(
        tmp_path, proc_line="0::/system.slice/docker-beef.scope\n"
    )
    assert not _reader(root, proc).available


def test_cgroup_reader_inert_without_v2_entry(tmp_path):
    # a pure v1 /proc/self/cgroup has controller names, no "0::" line.
    root, d, proc = _fake_cgroup(
        tmp_path, proc_line="12:memory:/foo\n3:cpu,cpuacct:/foo\n"
    )
    assert not _reader(root, proc).available


def test_cgroup_reader_rejects_escaping_path(tmp_path):
    root, d, proc = _fake_cgroup(tmp_path, proc_line="0::/../../etc\n")
    assert not _reader(root, proc).available


def test_cgroup_reader_namespaced_root(tmp_path):
    # with cgroup namespaces (the container default) the entry is "0::/" and
    # the mount root IS our slice; limits written there must be found.
    root, d, proc = _fake_cgroup(tmp_path, rel="", proc_line="0::/\n")
    (root / "memory.max").write_text(f"{512 * MIB}\n")
    (root / "memory.current").write_text(f"{200 * MIB}\n")
    reader = _reader(root, proc)
    assert reader.available
    assert reader.memory_limit() == 512 * MIB
    assert reader.memory_used() == 200 * MIB  # no memory.stat -> raw figure


def test_cgroup_memory_limit_is_lowest_on_path(tmp_path):
    # limits are hierarchical: an unlimited leaf under a limited parent is
    # still limited, and the lowest limit on the path wins.
    root, d, proc = _fake_cgroup(tmp_path, rel="parent/leaf")
    (d / "memory.max").write_text("max\n")
    (root / "parent" / "memory.max").write_text(f"{512 * MIB}\n")
    (root / "memory.max").write_text(f"{1024 * MIB}\n")
    assert _reader(root, proc).memory_limit() == 512 * MIB


def test_cgroup_memory_unlimited_and_malformed(tmp_path):
    root, d, proc = _fake_cgroup(tmp_path)
    (d / "memory.max").write_text("max\n")
    reader = _reader(root, proc)
    assert reader.memory_limit() is None
    (d / "memory.max").write_text("banana\n")
    assert reader.memory_limit() is None
    (d / "memory.current").write_text("banana\n")
    assert reader.memory_used() is None


def test_cgroup_memory_used_subtracts_inactive_file(tmp_path):
    # "used" excludes reclaimable page cache, matching docker stats and the
    # k8s working-set metric; a used figure can never go negative.
    root, d, proc = _fake_cgroup(tmp_path)
    (d / "memory.current").write_text(f"{200 * MIB}\n")
    (d / "memory.stat").write_text(
        f"anon {100 * MIB}\nfile {90 * MIB}\ninactive_file {50 * MIB}\n"
    )
    reader = _reader(root, proc)
    assert reader.memory_used() == 150 * MIB
    (d / "memory.stat").write_text(f"inactive_file {900 * MIB}\n")
    assert reader.memory_used() == 0


def test_cgroup_cpu_limit_parsing(tmp_path):
    root, d, proc = _fake_cgroup(tmp_path)
    (d / "cpu.max").write_text("150000 100000\n")
    reader = _reader(root, proc)
    assert reader.cpu_limit() == pytest.approx(1.5)
    (d / "cpu.max").write_text("max 100000\n")
    assert reader.cpu_limit() is None
    (d / "cpu.max").write_text("0 0\n")  # malformed: never a zero quota
    assert reader.cpu_limit() is None


def test_cgroup_cpu_limit_is_lowest_on_path(tmp_path):
    root, d, proc = _fake_cgroup(tmp_path, rel="parent/leaf")
    (d / "cpu.max").write_text("max 100000\n")
    (root / "parent" / "cpu.max").write_text("200000 100000\n")
    assert _reader(root, proc).cpu_limit() == pytest.approx(2.0)


def test_cgroup_cpu_usage_seconds(tmp_path):
    root, d, proc = _fake_cgroup(tmp_path)
    (d / "cpu.stat").write_text(
        "usage_usec 30000000\nuser_usec 20000000\nsystem_usec 10000000\n"
    )
    assert _reader(root, proc).cpu_usage_seconds() == pytest.approx(30.0)
    (d / "cpu.stat").unlink()
    assert _reader(root, proc).cpu_usage_seconds() is None


def test_node_sampler_cgroup_overlay(tmp_path, monkeypatch):
    # inside a limited slice the snapshot reports the slice: the limit as the
    # total, docker-stats-style used bytes, and CPU% of the quota -- same keys
    # as the host-wide readout, so consumers never see a shape change.
    root, d, proc = _fake_cgroup(tmp_path)
    (d / "memory.max").write_text(f"{512 * MIB}\n")
    (d / "memory.current").write_text(f"{200 * MIB}\n")
    (d / "memory.stat").write_text(f"inactive_file {72 * MIB}\n")
    (d / "cpu.max").write_text("200000 100000\n")  # 2 CPUs
    (d / "cpu.stat").write_text("usage_usec 35000000\n")

    sampler = NodeResourceSampler()
    sampler._cgroup = _reader(root, proc)
    clock = {"now": 1000.0}
    monkeypatch.setattr(resources.time, "monotonic", lambda: clock["now"])
    # previous reading: 30 cpu-seconds at t=990 -> 5s over a 10s window on a
    # 2-CPU quota = 25% of the allowance.
    sampler._cgroup_prev_cpu = (30.0, 990.0)

    snap = sampler.snapshot()
    assert snap is not None
    assert snap["mem_total_bytes"] == 512 * MIB
    assert snap["mem_used_bytes"] == 128 * MIB
    assert snap["mem_percent"] == pytest.approx(25.0)
    assert snap["cpu_count"] == 2
    assert snap["cpu_percent"] == pytest.approx(25.0)
    # the delta base advanced to this reading for the next window.
    assert sampler._cgroup_prev_cpu == (35.0, 1000.0)


def test_node_sampler_cgroup_overlay_is_per_resource(tmp_path, monkeypatch):
    # -m without --cpus: memory reports the slice, CPU stays host-wide (the
    # slice really can use every host core).
    root, d, proc = _fake_cgroup(tmp_path)
    (d / "memory.max").write_text(f"{512 * MIB}\n")
    (d / "memory.current").write_text(f"{100 * MIB}\n")
    (d / "cpu.max").write_text("max 100000\n")

    sampler = NodeResourceSampler()
    sampler._cgroup = _reader(root, proc)
    snap = sampler.snapshot()
    assert snap is not None
    assert snap["mem_total_bytes"] == 512 * MIB
    assert snap["cpu_count"] == resources.psutil.cpu_count()


def test_node_sampler_unlimited_cgroup_keeps_host_numbers(tmp_path):
    # a v2 host with no limit anywhere on the path is the common bare-metal
    # case: the snapshot must be the plain host-wide psutil readout.
    root, d, proc = _fake_cgroup(tmp_path)
    (d / "memory.max").write_text("max\n")
    (d / "cpu.max").write_text("max 100000\n")

    sampler = NodeResourceSampler()
    sampler._cgroup = _reader(root, proc)
    snap = sampler.snapshot()
    assert snap is not None
    assert snap["mem_total_bytes"] == resources.psutil.virtual_memory().total


# ---- _SeriesRecorder (per-run chart series) --------------------------------


def test_series_recorder_stride_one_keeps_every_sample():
    rec = _SeriesRecorder(8)
    for i in range(4):
        rec.add(100.0 + i, 10.0 * i, 1000 * (i + 1))
    assert rec.points() == [
        [100.0, 0.0, 1000],
        [101.0, 10.0, 2000],
        [102.0, 20.0, 3000],
        [103.0, 30.0, 4000],
    ]


def test_series_recorder_bucket_aggregates_avg_cpu_max_rss():
    # hitting the cap merges adjacent pairs: last t, mean CPU%, peak RSS --
    # the peak must never be averaged away (spikes are the point of the chart)
    rec = _SeriesRecorder(4)
    rec.add(0.0, 0.0, 100)
    rec.add(1.0, 10.0, 50)
    rec.add(2.0, 20.0, 300)
    rec.add(3.0, 30.0, 200)
    assert rec.points() == [[1.0, 5.0, 100], [3.0, 25.0, 300]]


def test_series_recorder_stays_bounded_and_ordered():
    rec = _SeriesRecorder(16)
    for i in range(10_000):
        rec.add(float(i), float(i % 7), i)
    pts = rec.points()
    assert len(pts) <= 16
    ts = [p[0] for p in pts]
    assert ts == sorted(ts)
    # the global RSS peak (the last, largest sample) survives downsampling
    assert max(p[2] for p in pts) == 9_999


def test_series_recorder_partial_bucket_is_provisional():
    # once the stride exceeds 1 an accumulating bucket may hold data for many
    # seconds; points() must surface it so a live chart tracks the newest
    # reading instead of lagging a full bucket behind.
    rec = _SeriesRecorder(4)
    for i in range(10):
        rec.add(float(i), 0.0, i)
    rec.add(99.0, 50.0, 123456)
    pts = rec.points()
    assert pts[-1][0] == 99.0
    assert pts[-1][2] == 123456


# ---- ResourceUsage.series (de)serialization --------------------------------


def test_resource_usage_series_is_opt_in():
    usage = ResourceUsage(1.0, 0.5, 2048, 3, series=[[1.0, 2.0, 300]])
    # polled payloads stay summary-sized: no series unless asked for
    assert "series" not in usage.to_dict()
    data = usage.to_dict(include_series=True)
    assert data["series"] == [[1.0, 2.0, 300]]
    restored = ResourceUsage.from_dict(data)
    assert restored is not None
    assert restored.series == [[1.0, 2.0, 300]]
    # a summary-only record rehydrates with no series (not an error)
    summary = ResourceUsage.from_dict(usage.to_dict())
    assert summary is not None
    assert summary.series is None


def test_resource_usage_series_drops_malformed_points():
    base = {
        "cpu_user_seconds": 1.0,
        "cpu_system_seconds": 0.0,
        "max_rss_bytes": 5,
        "samples": 1,
    }
    usage = ResourceUsage.from_dict(
        dict(
            base,
            series=[
                [1.0, 2.0, 3],  # good
                "junk",  # not a triple
                [1.0, 2.0],  # wrong arity
                [float("nan"), 1.0, 2],  # non-finite time
                [1.0, True, 2],  # bool is not a CPU%
                [2.0, -5.0, -7],  # negatives clamp to zero
            ],
        )
    )
    assert usage is not None
    assert usage.series == [[1.0, 2.0, 3], [2.0, 0.0, 0]]
    # nothing valid (or a non-list) collapses to "no series"
    for bad in (["x"], "nope", 42, {}):
        parsed = ResourceUsage.from_dict(dict(base, series=bad))
        assert parsed is not None
        assert parsed.series is None


def test_resource_usage_series_parse_is_capped():
    base = {
        "cpu_user_seconds": 1.0,
        "cpu_system_seconds": 0.0,
        "max_rss_bytes": 5,
        "samples": 1,
    }
    series = [[float(i), 1.0, 1] for i in range(MAX_SERIES_POINTS + 100)]
    usage = ResourceUsage.from_dict(dict(base, series=series))
    assert usage is not None
    assert usage.series is not None
    assert len(usage.series) == MAX_SERIES_POINTS


# ---- ResourceMonitor series capture ----------------------------------------


@pytest.mark.asyncio
async def test_monitor_records_chart_series():
    proc = await _spawn_busy(0.6)
    monitor = ResourceMonitor(proc.pid, job_name="busy", interval=0.05)
    monitor.start()
    await asyncio.sleep(0.25)
    live = monitor.series()
    assert live
    assert all(len(p) == 3 for p in live)
    await proc.wait()
    usage = await monitor.stop()
    assert usage is not None
    assert usage.series
    ts = [p[0] for p in usage.series]
    assert ts == sorted(ts)
    # per-point RSS never exceeds the run's recorded peak
    assert max(p[2] for p in usage.series) <= usage.max_rss_bytes


@pytest.mark.asyncio
async def test_monitor_history_zero_disables_series():
    proc = await _spawn_busy(0.4)
    monitor = ResourceMonitor(
        proc.pid, job_name="busy", interval=0.05, history=0
    )
    monitor.start()
    await asyncio.sleep(0.15)
    assert monitor.series() is None
    await proc.wait()
    usage = await monitor.stop()
    assert usage is not None
    assert usage.series is None


# ---- NodeResourceSampler history ring --------------------------------------


def test_node_sampler_history_none_before_start():
    assert NodeResourceSampler().history() is None


@pytest.mark.asyncio
async def test_node_sampler_history_records_and_bounds():
    sampler = NodeResourceSampler()
    sampler.start_history(interval=0.05, points=10)
    try:
        await asyncio.sleep(0.4)
        hist = sampler.history()
        assert hist is not None
        assert hist["interval"] == 0.05
        pts = hist["points"]
        assert pts
        assert len(pts) <= 10
        assert all(len(p) == 3 for p in pts)
        ts = [p[0] for p in pts]
        assert ts == sorted(ts)
    finally:
        await sampler.stop_history()


@pytest.mark.asyncio
async def test_node_sampler_history_reconfigure_keeps_points():
    sampler = NodeResourceSampler()
    sampler.start_history(interval=0.05, points=10)
    try:
        for _ in range(100):  # wait for a couple of samples, without flaking
            await asyncio.sleep(0.05)
            hist = sampler.history()
            if hist is not None and len(hist["points"]) >= 2:
                break
        before = sampler.history()["points"]
        assert before
        # shrinking the window keeps the newest retained points
        sampler.start_history(interval=0.05, points=5)
        hist = sampler.history()
        assert hist is not None
        assert hist["points"] == before[-5:]
    finally:
        await sampler.stop_history()


@pytest.mark.asyncio
async def test_node_sampler_history_without_psutil(monkeypatch):
    monkeypatch.setattr(resources, "psutil", None)
    sampler = NodeResourceSampler()
    sampler.start_history(interval=0.05, points=5)  # must stay inert
    assert sampler.history() is None
    await sampler.stop_history()


def test_resolve_node_history_config():
    # enabled by default whenever the web API is on
    assert resolve_node_history_config({}) == {
        "interval": resources.NODE_HISTORY_INTERVAL,
        "points": resources.NODE_HISTORY_POINTS,
    }
    assert resolve_node_history_config({"nodeHistory": True}) is not None
    assert resolve_node_history_config({"nodeHistory": False}) is None
    assert (
        resolve_node_history_config({"nodeHistory": {"enabled": False}})
        is None
    )
    cfg = resolve_node_history_config(
        {"nodeHistory": {"interval": 2, "points": 100}}
    )
    assert cfg == {"interval": 2.0, "points": 100}


# ---- portable resource-accounting internals --------------------------------
#
# The following group drives the hermetic internals directly: the series
# recorder's odd-tail compaction, the ledger series parser's numeric-coercion
# guard, the shared ppid index and sampling ticker, the process-tree folding
# from a snapshot index, and the NodeResourceSampler snapshot/overlay/history
# branches. Everything is hermetic: fake psutil.Process stand-ins and fake
# cgroup trees under tmp_path, no real network, no wall-clock races.


# ---- _SeriesRecorder._compact odd tail -------------------------------------


def test_series_recorder_compact_keeps_odd_tail():
    # an odd-sized point buffer at compaction time (reachable when the cap is
    # odd) merges the leading pair and carries the newest tail point through
    # unmerged, rather than dropping it.
    rec = _SeriesRecorder(3)
    rec.add(0.0, 0.0, 10)
    rec.add(1.0, 10.0, 20)
    rec.add(2.0, 20.0, 30)  # third point trips _compact() with 3 points
    # pair (0,1) -> [1.0, mean(0,10)=5.0, max(10,20)=20]; tail [2,20,30] kept.
    assert rec.points() == [[1.0, 5.0, 20], [2.0, 20.0, 30]]


# ---- _parse_series numeric-coercion guard ----------------------------------


def test_parse_series_drops_unconvertible_points():
    # a point whose rss is not int-coercible, or whose time is not
    # float-coercible, is dropped point-wise (the try/except continue), while
    # a well-formed neighbour survives.
    assert resources._parse_series([[1.0, 2.0, "notanint"]]) is None
    assert resources._parse_series([["notafloat", 1.0, 2]]) is None
    parsed = resources._parse_series([[1.0, 2.0, "x"], [3.0, 4.0, 5]])
    assert parsed == [[3.0, 4.0, 5]]


# ---- _ppid_index -----------------------------------------------------------


def test_ppid_index_none_without_psutil(monkeypatch):
    monkeypatch.setattr(resources, "psutil", None)
    assert resources._ppid_index() is None


def test_ppid_index_builds_from_ppid_map(monkeypatch):
    # one snapshot of pid->ppid folds into ppid->[child pids].
    monkeypatch.setattr(resources.psutil, "_ppid_map",
                        lambda: {2: 1, 3: 1, 1: 0})
    index = resources._ppid_index()
    assert index is not None
    assert set(index[1]) == {2, 3}
    assert index[0] == [1]


def test_ppid_index_none_when_map_raises(monkeypatch):
    def boom():
        raise RuntimeError("snapshot failed")

    monkeypatch.setattr(resources.psutil, "_ppid_map", boom)
    assert resources._ppid_index() is None


# ---- _SharedSampleTicker ---------------------------------------------------


def test_ticker_unregister_wakes_remaining(monkeypatch):
    # unregistering one of several monitors keeps the task and just nudges the
    # wake event so the ticker recomputes its next-due instant.
    ticker = resources._SharedSampleTicker()
    m1, m2 = object(), object()
    ticker._due = {m1: 0.0, m2: 0.0}
    ticker._wake.clear()
    ticker.unregister(m1)
    assert m1 not in ticker._due
    assert m2 in ticker._due
    assert ticker._wake.is_set()


class _TickMon:
    """Minimal monitor stand-in for driving _SharedSampleTicker._run."""

    def __init__(self, interval, on_sample):
        self._interval = interval
        self._on_sample = on_sample

    def _sample(self, index):
        self._on_sample(self, index)


async def test_ticker_run_returns_when_last_monitor_leaves():
    # a monitor that unregisters itself during its own sample drains _due; the
    # run loop then returns cleanly instead of spinning on an empty schedule.
    ticker = resources._SharedSampleTicker()
    seen = []

    def on_sample(mon, index):
        seen.append(index)
        ticker._due.pop(mon, None)

    mon = _TickMon(0.01, on_sample)
    ticker._due = {mon: 0.0}
    await asyncio.wait_for(ticker._run(), timeout=5)
    assert len(seen) == 1  # sampled exactly once, then the loop returned
    assert ticker._due == {}


async def test_ticker_run_swallows_sampler_bug(caplog):
    # a bug in a monitor's _sample must not crash the ticker: it is logged and
    # the run loop exits rather than propagating.
    ticker = resources._SharedSampleTicker()

    def on_sample(mon, index):
        raise ValueError("sampler bug")

    mon = _TickMon(0.01, on_sample)
    ticker._due = {mon: 0.0}
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await asyncio.wait_for(ticker._run(), timeout=5)
    assert any("sampling ticker stopped" in r.message
               for r in caplog.records)


# ---- ResourceMonitor._tree_from_index / _sample_locked ---------------------


class _FakeProc:
    """psutil.Process stand-in for driving the sampler directly."""

    def __init__(self, pid, create_time, user=0.0, system=0.0, rss=1024):
        self.pid = pid
        self._create_time = create_time
        self.user = user
        self.system = system
        self.rss = rss
        self.children_list = []
        self.children_error = None
        self.cpu_times_error = None
        self.memory_info_error = None
        self.create_time_error = None

    def oneshot(self):
        return contextlib.nullcontext()

    def cpu_times(self):
        if self.cpu_times_error is not None:
            raise self.cpu_times_error
        return SimpleNamespace(user=self.user, system=self.system)

    def memory_info(self):
        if self.memory_info_error is not None:
            raise self.memory_info_error
        return SimpleNamespace(rss=self.rss)

    def create_time(self):
        if self.create_time_error is not None:
            raise self.create_time_error
        return self._create_time

    def children(self, recursive=False):
        if self.children_error is not None:
            raise self.children_error
        return list(self.children_list)


def _patch_process_table(monkeypatch, procs):
    """Point resources.psutil.Process at a fixed pid->_FakeProc table."""

    def fake_process(pid):
        if pid not in procs:
            raise psutil.NoSuchProcess(pid)
        return procs[pid]

    monkeypatch.setattr(resources.psutil, "Process", fake_process)


def test_tree_from_index_folds_tree_and_skips_impostors(monkeypatch):
    # the tree is derived from the shared ppid snapshot: real descendants are
    # included, a pid created BEFORE the root (recycled) is rejected, and a
    # pid that vanished since the snapshot is skipped.
    root = _FakeProc(pid=100, create_time=1000.0, user=0.1, system=0.05,
                     rss=1000)
    child = _FakeProc(pid=200, create_time=1001.0, user=2.0, system=1.0,
                      rss=2000)
    grandchild = _FakeProc(pid=300, create_time=1002.0, user=3.0,
                           system=0.5, rss=4000)
    stale = _FakeProc(pid=400, create_time=999.0)  # older than root
    procs = {200: child, 300: grandchild, 400: stale}  # 500 is absent
    _patch_process_table(monkeypatch, procs)

    monitor = ResourceMonitor(100, job_name="tree", interval=1.0, history=0)
    monitor._proc = root
    # 200 is listed a second time under itself: the already-seen guard must
    # skip the duplicate rather than looping.
    index = {100: [200, 400, 500], 200: [300, 200]}
    monitor._sample_locked(index)

    assert monitor._samples == 1
    assert monitor._cpu_user == pytest.approx(0.1 + 2.0 + 3.0)
    assert monitor._cpu_system == pytest.approx(0.05 + 1.0 + 0.5)
    assert monitor._max_rss == 1000 + 2000 + 4000  # stale/absent excluded


def test_tree_from_index_root_gone_returns_root_only():
    # if the root's create_time read fails, the tree collapses to the root
    # alone (read below on its own).
    monitor = ResourceMonitor(100, job_name="lone", interval=1.0, history=0)
    root = _FakeProc(pid=100, create_time=1000.0)
    root.create_time_error = psutil.NoSuchProcess(100)
    tree = monitor._tree_from_index(root, {100: [200]})
    assert tree == [root]


def test_sample_locked_skips_unreadable_members(monkeypatch):
    # a member that raises a transient error (AccessDenied) and one that
    # raises an unexpected error during its read are both skipped; the root
    # still contributes its reading.
    root = _FakeProc(pid=100, create_time=1000.0, user=0.5, rss=1000)
    denied = _FakeProc(pid=200, create_time=1001.0, user=9.0, rss=9000)
    denied.cpu_times_error = psutil.AccessDenied(200)
    broken = _FakeProc(pid=300, create_time=1002.0, user=7.0, rss=7000)
    broken.memory_info_error = RuntimeError("weird")
    _patch_process_table(monkeypatch, {200: denied, 300: broken})

    monitor = ResourceMonitor(100, job_name="skip", interval=1.0, history=0)
    monitor._proc = root
    monitor._sample_locked({100: [200, 300]})

    assert monitor._samples == 1
    assert monitor._cpu_user == pytest.approx(0.5)  # only the root counted
    assert monitor._max_rss == 1000


def test_sample_locked_children_walk_root_transient():
    # without a shared index the tree is walked via children(); a transient
    # failure there falls back to reading the root alone.
    monitor = ResourceMonitor(100, job_name="walk", interval=1.0, history=0)
    root = _FakeProc(pid=100, create_time=1000.0, user=1.5, rss=2048)
    root.children_error = psutil.NoSuchProcess(100)
    monitor._proc = root
    monitor._sample_locked()
    assert monitor._samples == 1
    assert monitor._cpu_user == pytest.approx(1.5)
    assert monitor._max_rss == 2048


def test_sample_locked_children_walk_hard_error_is_inert():
    # a non-transient error from children() aborts the sample entirely (no
    # reading banked) rather than raising out of the sampler.
    monitor = ResourceMonitor(100, job_name="hard", interval=1.0, history=0)
    root = _FakeProc(pid=100, create_time=1000.0, user=1.5)
    root.children_error = RuntimeError("table read blew up")
    monitor._proc = root
    monitor._sample_locked()
    assert monitor._samples == 0


# ---- NodeResourceSampler init / snapshot / overlay / history ---------------


def _make_cgroup(tmp_path, *, cpu_usec=None):
    """A minimal available cgroup v2 reader over a fake tree under tmp_path."""
    root = tmp_path / "cgroup"
    root.mkdir()
    (root / "cgroup.controllers").write_text("cpu memory\n")
    d = root / "box"
    d.mkdir()
    if cpu_usec is not None:
        (d / "cpu.stat").write_text(f"usage_usec {cpu_usec}\n")
    proc = tmp_path / "proc_self_cgroup"
    proc.write_text("0::/box\n")
    return resources._CgroupV2Reader(str(root), str(proc)), d


def test_node_sampler_init_handles_psutil_process_failure(monkeypatch):
    # if the daemon's own psutil.Process cannot be built, the sampler still
    # constructs, just without its own-footprint proc handle.
    def boom(*a, **k):
        raise RuntimeError("no self process")

    monkeypatch.setattr(resources.psutil, "Process", boom)
    sampler = NodeResourceSampler()
    assert sampler._proc is None


def test_node_sampler_init_primes_cgroup_cpu_baseline(tmp_path, monkeypatch):
    # inside a limited slice the constructor primes the CPU delta baseline
    # from the current cgroup usage so the first snapshot has a window.
    reader, _d = _make_cgroup(tmp_path, cpu_usec=5_000_000)  # 5.0 CPU-seconds
    assert reader.available
    monkeypatch.setattr(resources, "_CgroupV2Reader", lambda *a, **k: reader)
    sampler = NodeResourceSampler()
    assert sampler._cgroup_prev_cpu is not None
    assert sampler._cgroup_prev_cpu[0] == pytest.approx(5.0)


def test_node_sampler_snapshot_includes_own_footprint():
    # a live snapshot on this host carries the daemon's own rss/cpu on top of
    # the system-wide numbers.
    sampler = NodeResourceSampler()
    snap = sampler.snapshot()
    assert snap is not None
    assert "proc_rss_bytes" in snap
    assert "proc_cpu_percent" in snap
    assert snap["proc_rss_bytes"] > 0


def test_node_sampler_snapshot_overlays_available_cgroup(tmp_path):
    # when a cgroup slice is available the snapshot swaps host-wide memory for
    # the slice's limit/usage (same keys), exercising the overlay call path.
    reader, d = _make_cgroup(tmp_path)
    mib = 1024 * 1024
    (d / "memory.max").write_text(f"{512 * mib}\n")
    (d / "memory.current").write_text(f"{128 * mib}\n")
    sampler = NodeResourceSampler()
    sampler._cgroup = reader
    snap = sampler.snapshot()
    assert snap is not None
    assert snap["mem_total_bytes"] == 512 * mib
    assert snap["mem_used_bytes"] == 128 * mib
    assert snap["mem_percent"] == pytest.approx(25.0)


def test_node_sampler_snapshot_none_when_system_read_fails(monkeypatch,
                                                           caplog):
    def boom():
        raise RuntimeError("proc read denied")

    monkeypatch.setattr(resources.psutil, "virtual_memory", boom)
    sampler = NodeResourceSampler()
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        assert sampler.snapshot() is None
    assert any("node resource sampling failed" in r.message
               for r in caplog.records)


def test_node_sampler_snapshot_survives_own_footprint_failure():
    # the own-footprint read is best-effort: if it is denied the system-wide
    # snapshot is still returned, just without the proc_* keys.
    sampler = NodeResourceSampler()

    class _BadSelf:
        def memory_info(self):
            raise RuntimeError("denied")

        def cpu_percent(self, interval=None):
            raise RuntimeError("denied")

    sampler._proc = _BadSelf()
    snap = sampler.snapshot()
    assert snap is not None
    assert "mem_total_bytes" in snap
    assert "proc_rss_bytes" not in snap


def test_node_sampler_overlay_cgroup_swallows_errors():
    # a cgroup read that raises leaves the already-populated host-wide values
    # untouched rather than propagating.
    sampler = NodeResourceSampler()

    class _BadCgroup:
        available = True

        def memory_limit(self):
            raise RuntimeError("cgroup read blew up")

    sampler._cgroup = _BadCgroup()
    data = {
        "cpu_percent": 3.0,
        "cpu_count": 4,
        "mem_percent": 2.0,
        "mem_used_bytes": 1,
        "mem_total_bytes": 999,
    }
    sampler._overlay_cgroup(data)
    assert data["mem_total_bytes"] == 999  # unchanged by the failed overlay


async def test_node_sampler_history_run_swallows_snapshot_error(monkeypatch,
                                                                caplog):
    sampler = NodeResourceSampler()
    sampler._history = deque(maxlen=5)

    def boom():
        raise ValueError("snapshot blew up")

    monkeypatch.setattr(sampler, "snapshot", boom)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await asyncio.wait_for(sampler._history_run(), timeout=5)
    assert any("node history sampler stopped" in r.message
               for r in caplog.records)


# ---- additional cgroup-reader edge branches --------------------------------


def test_cgroup_reader_init_swallows_resolve_error(tmp_path):
    # the v2 marker is present but /proc/self/cgroup is unreadable (here it
    # simply does not exist): the resolve raises, __init__ swallows it, and the
    # reader stays inert rather than propagating.
    root = tmp_path / "cgroup"
    root.mkdir()
    (root / "cgroup.controllers").write_text("cpu memory\n")
    missing_proc = tmp_path / "does_not_exist"
    reader = resources._CgroupV2Reader(str(root), str(missing_proc))
    assert not reader.available
    assert reader._dir is None


def test_cgroup_ancestry_empty_when_inert(tmp_path):
    # _ancestry short-circuits when no dir resolved (callers check available
    # first, but the generator guards itself all the same).
    root, d, proc = _fake_cgroup(tmp_path)
    (root / "cgroup.controllers").unlink()
    reader = _reader(root, proc)
    assert reader._dir is None
    assert list(reader._ancestry()) == []


def test_cgroup_ancestry_stops_at_filesystem_root():
    # a dir whose parent is itself (the filesystem root) terminates the walk
    # even when it never equals the configured mount root.
    reader = resources._CgroupV2Reader("/no-such-root", "/no-such-proc")
    fs_root = resources.os.path.abspath(resources.os.sep)
    reader._dir = fs_root
    reader._root = "/a-mount-root-never-reached"
    assert list(reader._ancestry()) == [fs_root]


def test_cgroup_read_stat_field_absent_field_is_none(tmp_path):
    # memory.current present but memory.stat carries no inactive_file line: the
    # field lookup falls off the end and the raw figure is used unchanged.
    root, d, proc = _fake_cgroup(tmp_path)
    (d / "memory.current").write_text(f"{200 * MIB}\n")
    (d / "memory.stat").write_text(f"anon {100 * MIB}\nfile {90 * MIB}\n")
    assert _reader(root, proc).memory_used() == 200 * MIB


def test_cgroup_memory_used_none_when_current_missing(tmp_path):
    # no memory.current file at all -> the first-line read is None -> used None.
    root, d, proc = _fake_cgroup(tmp_path)
    assert _reader(root, proc).memory_used() is None


def test_cgroup_cpu_limit_malformed_quota_is_skipped(tmp_path):
    # a non-integer quota field raises ValueError inside the parse and that
    # ancestor is skipped rather than crashing the reader.
    root, d, proc = _fake_cgroup(tmp_path)
    (d / "cpu.max").write_text("banana 100000\n")
    assert _reader(root, proc).cpu_limit() is None


def test_cgroup_cpu_limit_keeps_lowest_when_ancestor_is_higher(tmp_path):
    # the leaf carries the lower quota; a higher ancestor quota must not
    # replace it (the "not lower" side of the min comparison).
    root, d, proc = _fake_cgroup(tmp_path, rel="parent/leaf")
    (d / "cpu.max").write_text("100000 100000\n")  # 1.0 CPU
    (root / "parent" / "cpu.max").write_text("200000 100000\n")  # 2.0 CPU
    assert _reader(root, proc).cpu_limit() == pytest.approx(1.0)


# ---- _SharedSampleTicker register / run scheduling branches ----------------


async def test_ticker_register_reuses_running_task():
    # registering a second monitor while the ticker task is already running
    # must not spawn a second task (the "task exists and is live" skip).
    ticker = resources._SharedSampleTicker()
    m1 = _TickMon(1.0, lambda mon, index: None)
    m2 = _TickMon(1.0, lambda mon, index: None)
    ticker.register(m1)
    first = ticker._task
    assert first is not None
    ticker.register(m2)  # task still pending -> reused, not recreated
    assert ticker._task is first
    # drain and cancel without ever running the loop body.
    ticker.unregister(m1)
    ticker.unregister(m2)
    if first is not None:
        first.cancel()
        try:
            await first
        except asyncio.CancelledError:
            pass


async def test_ticker_run_exits_when_started_empty():
    # entering _run with an already-empty schedule takes the while-loop exit
    # immediately and returns.
    ticker = resources._SharedSampleTicker()
    ticker._due = {}
    await asyncio.wait_for(ticker._run(), timeout=5)


async def test_ticker_run_waits_then_drains_on_wake():
    # a monitor due far in the future leaves nothing due this pass (the empty
    # "due" branch), so the loop waits on the wake event; draining the schedule
    # and waking it then exits via the while-loop condition.
    ticker = resources._SharedSampleTicker()
    loop = asyncio.get_running_loop()
    mon = _TickMon(1.0, lambda m, index: None)
    ticker._due = {mon: loop.time() + 3600.0}
    task = asyncio.create_task(ticker._run())
    await asyncio.sleep(0)  # let it reach the wait
    ticker._due.clear()
    ticker._wake.set()
    await asyncio.wait_for(task, timeout=5)


async def test_ticker_run_reschedules_without_waiting():
    # a zero-interval monitor is instantly due again, so the delay is not
    # positive and the loop re-runs without waiting; the callback drains after
    # the second sample to end it.
    ticker = resources._SharedSampleTicker()
    calls = []

    def on_sample(mon, index):
        calls.append(1)
        if len(calls) >= 2:
            ticker._due.pop(mon, None)

    mon = _TickMon(0.0, on_sample)
    ticker._due = {mon: 0.0}
    await asyncio.wait_for(ticker._run(), timeout=5)
    assert len(calls) == 2


async def test_loop_ticker_reuses_existing_ticker():
    # a second call on the same loop returns the already-registered ticker
    # rather than building a new one.
    first = resources._loop_ticker()
    second = resources._loop_ticker()
    assert first is second


# ---- ResourceMonitor._sample zero-dt live CPU branch -----------------------


def test_sample_locked_zero_dt_keeps_previous_cpu_percent(monkeypatch):
    # two samples at the same monotonic instant give dt == 0, so the live CPU%
    # is left untouched rather than dividing by zero.
    monkeypatch.setattr(resources.time, "monotonic", lambda: 500.0)
    monitor = ResourceMonitor(100, job_name="dt0", interval=1.0, history=0)
    root = _FakeProc(pid=100, create_time=1000.0, user=1.0, system=0.5,
                     rss=2048)
    monitor._proc = root
    monitor._sample_locked()  # primes _prev_cpu at t=500.0
    assert monitor._prev_cpu is not None
    monitor._sample_locked()  # same instant -> dt == 0, no update
    assert monitor._samples == 2
    assert monitor._live_cpu_percent == 0.0


# ---- NodeResourceSampler init cgroup-priming branches ----------------------


def test_node_sampler_init_no_prime_without_cgroup(monkeypatch):
    # off Linux / without a v2 slice the reader is inert, so the constructor
    # never primes a CPU baseline.
    inert = resources._CgroupV2Reader("/no-such-root", "/no-such-proc")
    assert not inert.available
    monkeypatch.setattr(resources, "_CgroupV2Reader", lambda *a, **k: inert)
    sampler = NodeResourceSampler()
    assert sampler._cgroup_prev_cpu is None


def test_node_sampler_init_no_prime_when_usage_unreadable(tmp_path,
                                                          monkeypatch):
    # an available slice whose cpu.stat is missing yields no usage, so the
    # baseline stays unprimed even though the reader is available.
    reader, _d = _make_cgroup(tmp_path)  # no cpu.stat written
    assert reader.available
    assert reader.cpu_usage_seconds() is None
    monkeypatch.setattr(resources, "_CgroupV2Reader", lambda *a, **k: reader)
    sampler = NodeResourceSampler()
    assert sampler._cgroup_prev_cpu is None


# ---- NodeResourceSampler.snapshot host-only branches -----------------------


def test_node_sampler_snapshot_skips_overlay_without_cgroup(monkeypatch):
    # an inert cgroup reader means snapshot never calls the overlay: the plain
    # host-wide psutil readout is returned unchanged.
    inert = resources._CgroupV2Reader("/no-such-root", "/no-such-proc")
    sampler = NodeResourceSampler()
    sampler._cgroup = inert
    snap = sampler.snapshot()
    assert snap is not None
    assert "mem_total_bytes" in snap


def test_node_sampler_snapshot_without_own_proc(monkeypatch):
    # with no own-process handle the snapshot skips the proc_* footprint keys
    # yet still returns the system-wide numbers.
    inert = resources._CgroupV2Reader("/no-such-root", "/no-such-proc")
    sampler = NodeResourceSampler()
    sampler._cgroup = inert
    sampler._proc = None
    snap = sampler.snapshot()
    assert snap is not None
    assert "proc_rss_bytes" not in snap
    assert "mem_total_bytes" in snap


# ---- NodeResourceSampler._overlay_cgroup partial branches ------------------


def test_overlay_cgroup_memory_limit_but_no_used(tmp_path):
    # a memory limit with an unreadable memory.current keeps the host-wide
    # memory values (the "used is None" skip), and with no cpu limit the CPU
    # side is untouched too.
    reader, d = _make_cgroup(tmp_path)
    (d / "memory.max").write_text(f"{512 * MIB}\n")  # no memory.current
    sampler = NodeResourceSampler()
    sampler._cgroup = reader
    data = {
        "cpu_percent": 3.0, "cpu_count": 4,
        "mem_percent": 2.0, "mem_used_bytes": 1, "mem_total_bytes": 999,
    }
    sampler._overlay_cgroup(data)
    assert data["mem_total_bytes"] == 999  # unchanged: no usable used figure


def test_overlay_cgroup_cpu_quota_but_no_usage(tmp_path):
    # a CPU quota with no cpu.stat still fixes cpu_count to the quota but leaves
    # cpu_percent host-wide (the "usage is None" skip).
    reader, d = _make_cgroup(tmp_path)  # no cpu.stat
    (d / "cpu.max").write_text("200000 100000\n")  # 2 CPUs
    sampler = NodeResourceSampler()
    sampler._cgroup = reader
    data = {
        "cpu_percent": 42.0, "cpu_count": 8,
        "mem_percent": 2.0, "mem_used_bytes": 1, "mem_total_bytes": 999,
    }
    sampler._overlay_cgroup(data)
    assert data["cpu_count"] == 2
    assert data["cpu_percent"] == 42.0  # unchanged: no usage to measure


def test_overlay_cgroup_cpu_first_reading_has_no_baseline(tmp_path):
    # the first CPU overlay with no prior baseline records one and fixes
    # cpu_count, but cannot yet compute a percentage (the "prev is None" skip).
    reader, d = _make_cgroup(tmp_path, cpu_usec=10_000_000)  # 10.0 CPU-seconds
    (d / "cpu.max").write_text("200000 100000\n")  # 2 CPUs
    sampler = NodeResourceSampler()
    sampler._cgroup = reader
    sampler._cgroup_prev_cpu = None
    data = {
        "cpu_percent": 55.0, "cpu_count": 8,
        "mem_percent": 2.0, "mem_used_bytes": 1, "mem_total_bytes": 999,
    }
    sampler._overlay_cgroup(data)
    assert data["cpu_count"] == 2
    assert data["cpu_percent"] == 55.0  # unchanged on the first reading
    assert sampler._cgroup_prev_cpu is not None


def test_overlay_cgroup_cpu_zero_window_keeps_host_percent(tmp_path,
                                                           monkeypatch):
    # a baseline taken at the same instant gives a zero window, so no CPU%
    # is derived (the "dt > 0 and delta >= 0" skip) though cpu_count still set.
    monkeypatch.setattr(resources.time, "monotonic", lambda: 700.0)
    reader, d = _make_cgroup(tmp_path, cpu_usec=10_000_000)  # 10.0 CPU-seconds
    (d / "cpu.max").write_text("200000 100000\n")  # 2 CPUs
    sampler = NodeResourceSampler()
    sampler._cgroup = reader
    sampler._cgroup_prev_cpu = (10.0, 700.0)  # same usage, same instant
    data = {
        "cpu_percent": 33.0, "cpu_count": 8,
        "mem_percent": 2.0, "mem_used_bytes": 1, "mem_total_bytes": 999,
    }
    sampler._overlay_cgroup(data)
    assert data["cpu_count"] == 2
    assert data["cpu_percent"] == 33.0  # unchanged: zero-length window


# ---- NodeResourceSampler._history_run none-snapshot branch -----------------


async def test_node_sampler_history_run_skips_none_snapshot(monkeypatch):
    # a snapshot that comes back None is not appended to the ring; the loop
    # simply sleeps and tries again.
    sampler = NodeResourceSampler()
    sampler._history = deque(maxlen=5)
    sampler._history_interval = 0.01
    monkeypatch.setattr(sampler, "snapshot", lambda: None)
    task = asyncio.create_task(sampler._history_run())
    await asyncio.sleep(0.03)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert list(sampler._history) == []  # nothing recorded from a None snap
