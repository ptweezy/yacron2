"""Tests for per-run CPU/memory accounting (yacron2.resources)."""

import asyncio
import sys

import pytest

from yacron2.resources import ResourceMonitor, ResourceUsage

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
