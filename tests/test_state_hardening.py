"""Regression tests for the adversarial-review hardening of yacron2.state.

Each test pins a CONFIRMED bug fixed in the hardening pass; if a fix
regresses, the matching test fails.  Covered invariants:

* record ids stay unique when several worker threads append at once (the
  ``_seq`` counter is now behind a lock);
* lease fences are monotonic for the life of the store, across release and
  expiry (release marks the lease expired in place, never deletes it);
* an unreadable lease file fails CLOSED on the locked paths and stays
  best-effort on the unlocked observer;
* poison record CONTENT is quarantined while transient I/O errors are not;
* a failed atomic write never leaks its temp file;
* two backend instances over one mount (the shared-mount deployment) keep a
  coherent, non-colliding ledger;
* ``~`` in ``state.path`` means the home directory, not a literal ``~``
  directory under the daemon's CWD.

No tight wall-clock timing anywhere: lease expiry is driven by
monkeypatching ``yacron2.state._now`` (the one time source), so nothing here
depends on the coarse Windows clock.
"""

import asyncio
import json
import os
import threading

import pytest

from yacron2 import state
from yacron2.state import FilesystemStateBackend


def _backend(tmp_path, **over):
    # mirrors tests/test_state.py: a plain dict shaped like StateConfig, so
    # the backend is constructed exactly the way the config layer feeds it.
    cfg = {
        "path": str(tmp_path),
        "topology": "single-node",
        "deploymentId": None,
    }
    cfg.update(over)
    return FilesystemStateBackend(
        cfg,  # type: ignore[arg-type]
        lambda: "jobset-hardening",
    )


# --- 1: record-id uniqueness under thread concurrency ----------------------


async def test_append_sync_ids_unique_under_thread_contention(
    tmp_path, monkeypatch
):
    # The sync halves run on daemon worker threads, several of which can be
    # in flight at once (two jobs finishing together each schedule an
    # append).  Before the fix `self._seq += 1` was an unlocked read-modify-
    # write; two threads could interleave it, and a duplicated seq plus the
    # coarse Windows clock meant a duplicated record id -- one record
    # silently clobbering another via the atomic rename.
    backend = _backend(tmp_path)
    await backend.start()
    # Freeze the clock: the record id is "<epoch>-<instance>-<seq>", so with
    # a frozen epoch (and a single instance) uniqueness rests ENTIRELY on
    # the locked counter, making a seq race a deterministic id collision
    # instead of a needs-the-right-millisecond one.
    monkeypatch.setattr(state, "_now", lambda: 1000.0)
    n_threads, per_thread = 8, 50
    buckets = [[] for _ in range(n_threads)]
    errors = []

    def worker(bucket):
        try:
            for i in range(per_thread):
                bucket.append(backend._append_sync("s", {"i": i}))
        except BaseException as ex:  # noqa: BLE001 - relayed to the test
            errors.append(ex)

    threads = [
        threading.Thread(target=worker, args=(buckets[t],))
        for t in range(n_threads)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    ids = [rid for bucket in buckets for rid in bucket]
    assert len(ids) == n_threads * per_thread
    # every id unique: a duplicate means one record overwrote another.
    assert len(set(ids)) == n_threads * per_thread
    got = await backend.list_records("s")
    assert len(got) == n_threads * per_thread


# --- 2: lease fence monotonicity ------------------------------------------


async def test_fence_monotonic_across_release(tmp_path):
    # Release used to unlink the lease file -- the fence counter's only home
    # -- so the next acquire restarted at fence=1, re-issuing a fence value
    # already handed out and defeating stale-writer detection.  Release now
    # marks the lease expired IN PLACE, so the counter survives.
    backend = _backend(tmp_path)
    await backend.start()
    a = await backend.acquire_lease("L", "A", ttl=30)
    assert a is not None and a.fence == 1
    await backend.release_lease(a)
    b = await backend.acquire_lease("L", "B", ttl=30)
    assert b is not None and b.holder == "B"
    assert b.fence > a.fence  # regression: reset to 1 after release
    assert b.fence == 2


async def test_fence_bumps_on_expiry_and_stale_renew_denied(
    tmp_path, monkeypatch
):
    backend = _backend(tmp_path)
    await backend.start()
    # drive expiry through the module's one time source instead of real
    # sleeps: deterministic on the coarse Windows clock.
    clock = {"t": 1000.0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])
    a = await backend.acquire_lease("L", "A", ttl=5)  # expires at 1005
    assert a is not None and a.fence == 1
    clock["t"] = 1010.0  # past expiry
    b = await backend.acquire_lease("L", "B", ttl=5)
    assert b is not None and b.holder == "B"
    assert b.fence == a.fence + 1  # takeover must bump the fence
    # A's renew with its stale lease is fenced off, not honoured.
    assert await backend.renew_lease(a, ttl=5) is None


# --- 3: unreadable lease fails closed --------------------------------------


async def test_corrupt_lease_fails_closed_on_acquire(tmp_path):
    # An unreadable lease is not a FREE lease.  Before the fix a corrupt (or
    # transiently unreadable) lease file read as "no lease", letting an
    # acquirer steal a possibly-valid lease from its live holder with a
    # reset fence=1.  The locked paths must deny instead.
    backend = _backend(tmp_path)
    await backend.start()
    _lock_path, lease_path = backend._lease_paths("L")
    os.makedirs(os.path.dirname(lease_path), exist_ok=True)
    with open(lease_path, "w") as fobj:
        fobj.write("{definitely not json")
    assert await backend.acquire_lease("L", "H", ttl=30) is None
    # the unlocked observer stays best-effort: None, never an exception.
    assert await backend.read_lease("L") is None


# --- 4: release keeps the file, observers see it as free -------------------


async def test_release_marks_expired_in_place_keeps_file(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    lease = await backend.acquire_lease("L", "A", ttl=30)
    assert lease is not None
    await backend.release_lease(lease)
    # observers must see "nobody holds it" after a release...
    assert await backend.read_lease("L") is None
    # ...but the file must still exist: it is what preserves the fence
    # counter across release/re-acquire cycles (see the monotonicity test).
    _lock_path, lease_path = backend._lease_paths("L")
    assert os.path.exists(lease_path)
    with open(lease_path, "rb") as fobj:
        on_disk = json.loads(fobj.read())
    assert on_disk["expiresAt"] == 0.0


# --- 5: _read_record error taxonomy ----------------------------------------


async def test_invalid_json_record_is_quarantined(tmp_path):
    # Bad CONTENT (truncated JSON from a crash mid-write on a store without
    # atomic rename) is the record's fault: quarantine it so one poison
    # object can never brick every later read of the stream.
    backend = _backend(tmp_path)
    await backend.start()
    await backend.append_record("s", {"good": True})
    stream_dir = backend._stream_dir("s")
    with open(os.path.join(stream_dir, "00000-bad.json"), "w") as fobj:
        fobj.write("{not json")
    assert await backend.list_records("s") == [{"good": True}]
    assert "00000-bad.json" not in os.listdir(stream_dir)
    quarantine = os.path.join(backend.base, "quarantine")
    assert any(n.startswith("00000-bad.json") for n in os.listdir(quarantine))


async def test_deeply_nested_json_record_is_quarantined(tmp_path):
    # A hostile >1000-deep nesting makes json.loads raise RecursionError,
    # which is NOT an OSError or ValueError: before the fix it escaped the
    # except clauses and crashed whichever caller was reading the stream.
    # Content poison must be caught and quarantined like any bad record.
    backend = _backend(tmp_path)
    await backend.start()
    await backend.append_record("s", {"ok": 1})
    stream_dir = backend._stream_dir("s")
    bomb = "[" * 1300 + "]" * 1300
    with open(os.path.join(stream_dir, "00000-deep.json"), "w") as fobj:
        fobj.write(bomb)
    got = await backend.list_records("s")  # must not raise
    assert got == [{"ok": 1}]
    assert "00000-deep.json" not in os.listdir(stream_dir)
    quarantine = os.path.join(backend.base, "quarantine")
    assert any(n.startswith("00000-deep.json") for n in os.listdir(quarantine))


async def test_transient_read_error_skips_but_never_quarantines(
    tmp_path, monkeypatch
):
    # A transient I/O error (an NFS blip, an AV scanner's momentary hold) is
    # the ENVIRONMENT's fault, not the record's.  Quarantining on it would
    # eject perfectly valid history -- and regress the derived watermark --
    # on every store hiccup.  The read must skip the record for this pass
    # and leave the file in place.
    backend = _backend(tmp_path)
    await backend.start()
    await backend.append_record("s", {"i": 0})
    await backend.append_record("s", {"i": 1})
    stream_dir = backend._stream_dir("s")
    names = sorted(n for n in os.listdir(stream_dir) if n.endswith(".json"))
    victim = os.path.normpath(os.path.join(stream_dir, names[0]))
    real_open = open
    failing = {"on": True}

    def flaky_open(path, *args, **kwargs):
        if failing["on"] and os.path.normpath(str(path)) == victim:
            raise PermissionError(13, "transient hold", str(path))
        return real_open(path, *args, **kwargs)

    # shadow the module's open, the same seam tests/test_state.py patches.
    monkeypatch.setattr(state, "open", flaky_open, raising=False)
    got = await backend.list_records("s")
    assert [r["i"] for r in got] == [1]  # unreadable one skipped
    # still in the stream (NOT quarantined), and quarantine stays empty.
    assert names[0] in os.listdir(stream_dir)
    assert os.listdir(os.path.join(backend.base, "quarantine")) == []
    # once the blip clears the record is readable again, proving the file
    # was left fully intact.
    failing["on"] = False
    assert [r["i"] for r in await backend.list_records("s")] == [0, 1]


# --- 6: atomic-write temp-file hygiene --------------------------------------


async def test_failed_replace_leaves_no_tmp_files(tmp_path, monkeypatch):
    # A failed rename must not strand its temp file: on a long-lived store
    # leaked w-*.tmp files accumulate forever (and on a shared mount every
    # node's leaks pile into the same directory).
    backend = _backend(tmp_path)
    await backend.start()

    def boom(*args):
        raise OSError("simulated rename failure")

    monkeypatch.setattr(
        state.FilesystemStateBackend, "_replace", staticmethod(boom)
    )
    with pytest.raises(OSError, match="simulated rename failure"):
        await backend.append_record("s", {"i": 0})
    tmp_dir = os.path.join(backend.base, "tmp")
    assert [n for n in os.listdir(tmp_dir) if n.endswith(".tmp")] == []
    # and the half-written record never became visible either.
    monkeypatch.undo()
    assert await backend.list_records("s") == []


# --- 7: two instances over one mount (shared-mount simulation) --------------


async def test_two_instances_share_one_ledger(tmp_path):
    # The shared-mount deployment: two processes (here: two backend
    # instances) over the SAME store and namespace.  Ids must never collide
    # (each instance mixes its own random `_instance` token into every
    # filename), reads must see the union, and the derived cursor must be
    # the true max regardless of which node wrote it.
    b1 = _backend(tmp_path)
    b2 = _backend(tmp_path)
    await b1.start()
    await b2.start()
    assert b1._instance != b2._instance
    stream = "runs/shared-job"
    ids = []
    for i in range(5):  # interleaved appends, as two live nodes produce
        ids.append(
            await b1.append_record(stream, {"finished_at": 2 * i, "n": 1})
        )
        ids.append(
            await b2.append_record(stream, {"finished_at": 2 * i + 1, "n": 2})
        )
    assert len(set(ids)) == 10  # no cross-instance id collision
    seen1 = await b1.list_records(stream)
    seen2 = await b2.list_records(stream)
    assert len(seen1) == len(seen2) == 10
    union = {(r["n"], r["finished_at"]) for r in seen1}
    assert union == {(r["n"], r["finished_at"]) for r in seen2}
    assert {r["n"] for r in seen1} == {1, 2}  # both writers visible
    # the max (9) was written by node 2 but must be derived identically
    # from either node -- order-independent, never last-writer-wins.
    assert await b1.derive_max(stream, "finished_at") == 9
    assert await b2.derive_max(stream, "finished_at") == 9
    # one node prunes while the other lists: neither may raise.  The racing
    # list may observe any prefix of the deletes; only convergence matters.
    removed, racing = await asyncio.gather(
        b1.prune_records(stream, keep=3),
        b2.list_records(stream),
    )
    assert 3 <= len(racing) <= 10
    # a Windows lister can transiently hold a record open, making prune's
    # unlink of that one file fail (silently skipped by design); a second
    # pass after the reader finished sweeps any such straggler.
    swept = await b1.prune_records(stream, keep=3)
    assert removed + swept == 7
    assert len(await b1.list_records(stream)) == 3
    assert len(await b2.list_records(stream)) == 3


# --- 8: ~ expansion ----------------------------------------------------------


def test_tilde_path_expands_to_home():
    # `path: ~/state` must mean the home directory: before the fix the raw
    # "~" went through abspath, creating a literal "~" directory under
    # whatever CWD the daemon started in.  Construction only -- no start(),
    # so nothing is created under the real home.
    backend = _backend("~/some-yacron2-test-path")
    home = os.path.expanduser("~")
    expected = os.path.abspath(os.path.join(home, "some-yacron2-test-path"))
    assert backend.root == expected
    assert backend.root.startswith(home)


# --- 9: renew cannot resurrect a released lease -----------------------------


async def test_renew_after_release_is_denied(tmp_path):
    # Release marks the lease expired in place with the SAME holder+fence
    # (that is what keeps the fence monotonic), so a renew that checks only
    # holder+fence would still match and silently un-release it -- an
    # in-flight renew loop racing shutdown would resurrect the lease and
    # block other nodes for a full TTL after a clean release.
    backend = _backend(tmp_path)
    await backend.start()
    lease = await backend.acquire_lease("leader", "node-a", 30.0)
    assert lease is not None
    await backend.release_lease(lease)
    assert await backend.read_lease("leader") is None
    assert await backend.renew_lease(lease, 30.0) is None
    # ...and the lease is genuinely still free for the next holder, with a
    # bumped fence.
    lease2 = await backend.acquire_lease("leader", "node-b", 30.0)
    assert lease2 is not None
    assert lease2.fence > lease.fence


# --- 10: a failed lease write denies instead of raising ----------------------


async def test_lease_write_failure_denies_instead_of_raising(
    tmp_path, monkeypatch
):
    # On Windows a reader/AV holding the .lease file open past the replace
    # retries surfaces PermissionError from the write; the lease API must
    # translate that into a clean denial (fail closed), never an exception
    # escaping acquire/renew to its caller.
    backend = _backend(tmp_path)
    await backend.start()
    lease = await backend.acquire_lease("leader", "node-a", 30.0)
    assert lease is not None

    def boom(path, obj):
        raise PermissionError(5, "sharing violation", path)

    monkeypatch.setattr(backend, "_write_lease_file", boom)
    assert await backend.renew_lease(lease, 30.0) is None
    monkeypatch.setattr(
        state, "_now", lambda: lease.expires_at + 1.0
    )  # expire it
    assert await backend.acquire_lease("leader", "node-b", 30.0) is None
