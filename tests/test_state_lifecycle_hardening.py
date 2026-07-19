"""Lifecycle hardening of the durable state layer.

Adversarial-review follow-ups not covered elsewhere: the platform file
lock must BLOCK (never raise) under contention, the backend's
daemon-thread ``_call`` helper must keep a hung store abandonable, and
the shutdown flush in :meth:`cronstable.cron.Cron.run` must persist
in-flight run records without ever letting a wedged store hang exit.

Timing discipline (Windows CI has coarse ~15.6ms timers and slow
spawns): every wait here is an event or a generous bound, and nothing
asserts a duration or a tight window -- only ordering and completion.
"""

import asyncio
import contextlib
import datetime
import os
import threading
import time

import pytest

from cronstable.config import parse_config_string
from cronstable.cron import Cron, JobRunInfo
from cronstable.job import JobOutputStream
from cronstable.platform import exclusive_file_lock
from cronstable.state import FilesystemStateBackend

_UTC = datetime.timezone.utc

_ONE_JOB = (
    "jobs:\n  - name: j\n    command: 'true'\n    schedule: '* * * * *'\n"
)


def _info(second=0, outcome="success"):
    dt = datetime.datetime(2026, 7, 1, 0, 0, second, tzinfo=_UTC)
    return JobRunInfo(
        outcome=outcome,
        exit_code=0,
        started_at=dt,
        finished_at=dt,
        fail_reason=None,
        output=JobOutputStream(),
    )


def _state_cfg(yaml):
    return parse_config_string(yaml, "").state_config


def _backend(path):
    cfg = {
        "path": str(path),
        "topology": "single-node",
        "deploymentId": None,
    }
    return FilesystemStateBackend(
        cfg,  # type: ignore[arg-type]
        lambda: "jobset-test",
    )


async def _wait_until(pred, tries=1000, interval=0.01):
    # poll a predicate instead of sleeping a fixed time (the house
    # pattern from test_cron.py): fast when it is fast, a ~10s bound
    # when CI is slow, a clean failure instead of a hang when never.
    for _ in range(tries):
        if pred():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met within {} tries".format(tries))


# --- exclusive_file_lock: block-then-succeed under contention -------------


def test_exclusive_lock_blocks_then_succeeds_under_contention(tmp_path):
    # The semantic guarantee both platforms must give: a second locker
    # BLOCKS while the first holds, then SUCCEEDS once it releases --
    # it never raises.  (msvcrt's LK_LOCK raised OSError after ~10
    # one-second retries; the Windows path is now a non-blocking retry
    # loop, and this is its contract test.  On POSIX flock blocks
    # natively.)  Ordering is asserted with events only, never times.
    lock_file = tmp_path / "contended.lock"
    lock_file.write_bytes(b"\0")  # msvcrt needs a byte present to lock

    a_holding = threading.Event()  # A is inside the locked section
    a_release = threading.Event()  # main tells A to let go
    b_attempting = threading.Event()  # B is about to block on the lock
    b_acquired = threading.Event()  # B made it inside
    b_saw_release_order = []  # was A told to release before B got in?
    errors = []

    def hold_a():
        fd = os.open(str(lock_file), os.O_RDWR)
        try:
            with exclusive_file_lock(fd):
                a_holding.set()
                a_release.wait(timeout=30)
        except OSError as ex:  # pragma: no cover - the failure under test
            errors.append(ex)
        finally:
            os.close(fd)

    def contend_b():
        fd = os.open(str(lock_file), os.O_RDWR)
        try:
            b_attempting.set()
            with exclusive_file_lock(fd):
                # a_release is set by the main thread strictly before A
                # can exit its locked section, so if mutual exclusion
                # holds B can only ever observe it already set.
                b_saw_release_order.append(a_release.is_set())
                b_acquired.set()
        except OSError as ex:  # pragma: no cover - the failure under test
            errors.append(ex)
        finally:
            os.close(fd)

    thread_a = threading.Thread(target=hold_a, daemon=True)
    thread_a.start()
    assert a_holding.wait(timeout=10)
    thread_b = threading.Thread(target=contend_b, daemon=True)
    thread_b.start()
    assert b_attempting.wait(timeout=10)
    # hold the lock a while with B contending.  B staying out is a
    # mutual-exclusion check, not a timing one: b_acquired can only be
    # set by actually acquiring, impossible while A holds the lock.
    time.sleep(0.5)
    assert not b_acquired.is_set()
    assert errors == []  # above all, B must not have RAISED while blocked
    a_release.set()
    assert b_acquired.wait(timeout=10)  # B succeeds once A releases
    thread_a.join(timeout=10)
    thread_b.join(timeout=10)
    assert not thread_a.is_alive()
    assert not thread_b.is_alive()
    assert errors == []
    assert b_saw_release_order == [True]


# --- FilesystemStateBackend._call: the daemon-thread seam -----------------


async def test_call_runs_sync_half_on_daemon_thread(tmp_path):
    # _call must run the blocking half on a DAEMON thread named
    # "cronstable-state", never the default executor: non-daemonic workers
    # are joined at interpreter exit, so one wedged in a dead NFS hard
    # mount would hang process shutdown forever.
    backend = _backend(tmp_path)
    await backend.start()
    seen = {}
    real_append = backend._append_sync

    def spy(stream, data, prune_keep=None):
        thread = threading.current_thread()
        seen["daemon"] = thread.daemon
        seen["name"] = thread.name
        return real_append(stream, data, prune_keep)

    backend._append_sync = spy  # type: ignore[method-assign]
    rec_id = await backend.append_record("s", {"i": 1})
    assert rec_id
    assert seen["daemon"] is True
    assert seen["name"].startswith("cronstable-state")


class _StoreBoom(Exception):
    pass


async def test_call_propagates_sync_half_exception(tmp_path):
    # an exception raised on the worker thread must surface, as itself,
    # to the awaiter -- not vanish and not wedge the await.
    backend = _backend(tmp_path)
    await backend.start()

    def boom(stream, data, prune_keep=None):
        raise _StoreBoom("sync half exploded")

    backend._append_sync = boom  # type: ignore[method-assign]
    with pytest.raises(_StoreBoom, match="sync half exploded"):
        await backend.append_record("s", {"i": 1})


async def test_call_survives_abandoned_await(tmp_path):
    # An awaiter that times out (asyncio.wait_for) abandons _call's
    # future; when the daemon thread later finishes, _resolve must see
    # the cancelled future and drop the result silently -- no
    # InvalidStateError thrown into the loop, no unhandled-exception
    # noise.  This is the "hung store, caller moved on" path shutdown
    # relies on.
    backend = _backend(tmp_path)
    await backend.start()
    entered = threading.Event()
    release = threading.Event()

    def blocked(stream, data, prune_keep=None):
        entered.set()
        release.wait(timeout=30)  # released below; bound is a safety net
        return "late-result"

    backend._append_sync = blocked  # type: ignore[method-assign]

    loop = asyncio.get_running_loop()
    captured = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda lp, ctx: captured.append(ctx))
    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                backend.append_record("s", {"i": 1}), timeout=0.1
            )
        await _wait_until(entered.is_set)  # the worker really is in-flight
        release.set()  # let the daemon thread finish into the dead future
        await asyncio.sleep(0.3)  # a generous beat for _resolve to run
    finally:
        loop.set_exception_handler(previous)
    assert captured == []


async def test_inventory_runs_via_call_daemon_thread(tmp_path):
    # inventory() used to submit its full listdir walk to the DEFAULT
    # executor (loop.run_in_executor(None, ...)), bypassing _call's
    # abandonable daemon threads, lane cap and throttle: a dashboard
    # polling GET /state against a hung mount wedged the non-daemon
    # default workers one by one, after which config reload -- and the
    # interpreter-exit join of those workers -- hung behind them.  The
    # walk must ride the same "cronstable-state" daemon lane as every other
    # op, and be accounted in the per-op stats like one.
    backend = _backend(tmp_path)
    await backend.start()
    seen = {}
    real_inventory = backend._inventory_sync

    def spy():
        thread = threading.current_thread()
        seen["daemon"] = thread.daemon
        seen["name"] = thread.name
        return real_inventory()

    backend._inventory_sync = spy  # type: ignore[method-assign]
    inv = await backend.inventory()
    assert inv["enumerable"] is True
    assert seen["daemon"] is True
    assert seen["name"].startswith("cronstable-state")
    assert "inventory" in backend.stats()["ops"]


# --- Cron.run(): the shutdown flush ----------------------------------------


_FLUSH_CFG = """\
jobs:
  - name: j
    command: echo hi
    schedule: "0 0 29 2 *"
state:
  path: {path}
"""


async def test_shutdown_flushes_pending_run_record(tmp_path):
    # The exact data loss the flush exists to prevent: a run record
    # scheduled fire-and-forget moments before shutdown must still be
    # durable after run() returns.  Drives the REAL run() loop (the
    # test_cron.py pattern): start -> backend up -> record ->
    # signal_shutdown -> clean exit -> read the store back cold.
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text(_FLUSH_CFG.format(path=state_dir))

    cron = Cron(str(cfg))
    task = asyncio.create_task(cron.run())
    try:
        await _wait_until(lambda: cron.state_backend is not None)
        # schedule the persist and shut down in the SAME loop step: the
        # write task has not run even once yet, so without the shutdown
        # flush it would be abandoned mid-air.
        cron._record_run("j", _info(second=1))
        # at least the run-record write is pending and un-run (the backend
        # start may also have queued chore writes, e.g. the manifest)
        assert len(cron._pending_state_writes) >= 1
    finally:
        cron.signal_shutdown()
        await asyncio.wait_for(task, timeout=30)

    assert cron.state_backend is None  # run() tore the backend down
    reader = _backend(state_dir)
    recs = await reader.list_records("runs/j")
    assert any(
        r["finished_at"] == "2026-07-01T00:00:01+00:00"
        and r["outcome"] == "success"
        for r in recs
    )


async def test_shutdown_completes_despite_hung_state_write(
    tmp_path, monkeypatch
):
    # A wedged store (the classic dead-NFS-server hard mount) must not
    # hang exit: the flush is BOUNDED, the hung write is abandoned, and
    # run() still returns.  Never asserts how fast -- only that it
    # completes at all, within a x10-generous outer bound.
    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    assert cron.state_backend is not None

    hang = asyncio.get_running_loop().create_future()

    async def hung_append(stream, data, *, prune_keep=None):
        await hang  # never resolves

    monkeypatch.setattr(cron.state_backend, "append_record", hung_append)

    # Shrink run()'s hardcoded flush bound (asyncio.wait(..., timeout=5))
    # so the test proves "bounded" without idling out the real 5s; every
    # other asyncio.wait call in flight passes through untouched.
    real_wait = asyncio.wait

    async def fast_wait(fs, timeout=None, **kwargs):
        if timeout == 5:
            timeout = 0.2
        return await real_wait(fs, timeout=timeout, **kwargs)

    monkeypatch.setattr(asyncio, "wait", fast_wait)

    cron._record_run("j", _info())
    assert len(cron._pending_state_writes) == 1
    pending = next(iter(cron._pending_state_writes))

    # Straight to the shutdown sequence (the pattern of test_cron.py's
    # test_shutdown_stops_cluster_manager_before_job_drain): the loop
    # body never runs, so housekeeping cannot replace the rigged
    # backend, and run() goes directly to the flush block.
    cron.signal_shutdown()
    await asyncio.wait_for(cron.run(), timeout=30)

    assert cron.state_backend is None  # shutdown ran to completion
    assert not pending.done()  # the hung write was abandoned, not awaited
    pending.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await pending
