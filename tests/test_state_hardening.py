"""Regression tests for the adversarial-review hardening of cronstable.state.

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
  directory under the daemon's CWD;
* length-truncated stream tokens round-trip through ``list_stream_names``
  via the logical-name sidecar, and a legacy truncated dir without one is
  skipped/kept -- never returned garbled or collected;
* garbage collection reclaims ONLY the ephemeral per-run lease classes
  (``dagadvance/``) once provably dead past the whole grace window --
  never slot/retry-claim/election leases, whose fences persist in durable
  slot cancel records -- and sweeps orphaned ``.lock`` side-files (a
  deleted document's, a bare lease lock's) only once idle past the grace,
  while ``delete_document`` itself never unlinks the ``.lock`` (an eager
  unlink split the document mutex across nodes on NFS);
* stream/document-namespace enumeration reports hidden (unnameable)
  entries, the orphan-blob sweep's age guard keeps young payloads, and a
  dedupe re-put re-arms that guard -- the KEEP biases the blob sweep
  builds on;
* the document delete path rides out Windows sharing violations like the
  replace-write path does.

No tight wall-clock timing anywhere: lease expiry is driven by
monkeypatching ``cronstable.state._now`` (the one time source), so nothing here
depends on the coarse Windows clock.
"""

import asyncio
import json
import os
import threading
import time

import pytest

from cronstable import state
from cronstable.dag import DAG_LEASE_PREFIX
from cronstable.state import FilesystemStateBackend


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
    backend = _backend("~/some-cronstable-test-path")
    home = os.path.expanduser("~")
    expected = os.path.abspath(os.path.join(home, "some-cronstable-test-path"))
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


# --- 11: truncated stream tokens round-trip through list_stream_names -------


async def test_list_stream_names_roundtrips_truncated_tokens(tmp_path):
    # _fs_safe truncates a >_FS_SAFE_MAX token to head + "%." + digest, and
    # unquote()ing that token back produced a GARBLED name that re-encoded
    # to a DIFFERENT token: a host with a long (or multibyte -- 9 encoded
    # chars per CJK char) hostname had its manifest stream invisible to
    # every GC keep-set builder, and its jobs' durable state was collected
    # as garbage.  The name sidecar written on append must make every
    # returned name round-trip exactly to its on-disk token.
    backend = _backend(tmp_path)
    await backend.start()
    long_ascii = "manifests/" + "H" * 140  # uppercase: 3 encoded chars each
    multibyte = "manifests/" + "主机" * 20  # a CJK hostname
    plain = "manifests/plain-host"
    for stream in (long_ascii, multibyte, plain):
        await backend.append_record(stream, {"x": 1})
    names = await backend.list_stream_names("manifests/")
    assert set(names) == {long_ascii, multibyte, plain}
    # the property under test: _fs_safe over every returned name reproduces
    # exactly the set of on-disk stream tokens (nothing garbled, nothing
    # skipped), so keep-sets built from these names protect the real dirs.
    records_root = os.path.join(backend.base, "records")
    prefix_token = state._fs_safe_fragment("manifests/")
    on_disk = {
        t for t in os.listdir(records_root) if t.startswith(prefix_token)
    }
    assert {state._fs_safe(n) for n in names} == on_disk
    # and each name feeds straight back into a record read, the exact call
    # the GC keep-set builders make.
    for name in names:
        assert await backend.list_records(name) == [{"x": 1}]


async def test_gc_keeps_legacy_truncated_stream_and_skips_its_name(
    tmp_path, monkeypatch
):
    # A store written BEFORE the sidecar existed: a truncated dir's logical
    # name is unrecoverable, so enumeration must SKIP it (never return a
    # garbled, non-round-tripping name) and GC must treat it as
    # unclassifiable (keep) -- until an append lands the sidecar and makes
    # it classifiable again.
    backend = _backend(tmp_path)
    await backend.start()
    clock = {"t": 1000.0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])
    long_job = "runs/" + "J" * 200
    await backend.append_record(long_job, {"x": 1})
    stream_dir = backend._stream_dir(long_job)
    # simulate the legacy dir by removing the sidecar the append wrote.
    os.unlink(os.path.join(stream_dir, state._STREAM_NAME_SIDECAR))
    assert await backend.list_stream_names("runs/") == []
    # aged far past the grace and unreferenced: still KEPT, because its
    # absence from the keep map proves nothing (the builder never saw it).
    clock["t"] = 1000.0 + 8 * 86400.0
    result = await backend.collect_garbage(
        keep={"runs/": set()}, grace=7 * 86400.0
    )
    assert result["streams_removed"] == 0
    assert await backend.list_records(long_job) == [{"x": 1}]
    # the next append self-heals the sidecar: enumerable again...
    await backend.append_record(long_job, {"x": 2})
    assert await backend.list_stream_names("runs/") == [long_job]
    # ...and therefore classifiable: aged and unreferenced now really goes.
    clock["t"] = 1000.0 + 16 * 86400.0
    result = await backend.collect_garbage(
        keep={"runs/": set()}, grace=7 * 86400.0
    )
    assert result["streams_removed"] == 1
    assert not os.path.isdir(stream_dir)


async def test_list_stream_names_audit_reports_hidden_streams(tmp_path):
    # The orphan-blob sweep builds its referenced-digest set from the stream
    # listing, so it must be able to tell "these are ALL the artifact
    # streams" from "a legacy truncated dir is hiding one": a hidden
    # stream's records still reference blobs, and a sweep that could not
    # see them would delete live payloads.
    backend = _backend(tmp_path)
    await backend.start()
    long_scope = "artifacts/" + "S" * 200
    await backend.append_record("artifacts/plain", {"sha256": "a" * 64})
    await backend.append_record(long_scope, {"sha256": "b" * 64})
    names, complete = await backend.list_stream_names_audit("artifacts/")
    assert complete is True
    assert set(names) == {"artifacts/plain", long_scope}
    # a legacy dir (pre-sidecar store): the stream becomes unnameable and
    # the audit must say so instead of silently shrinking the listing.
    stream_dir = backend._stream_dir(long_scope)
    os.unlink(os.path.join(stream_dir, state._STREAM_NAME_SIDECAR))
    names, complete = await backend.list_stream_names_audit("artifacts/")
    assert complete is False
    assert names == ["artifacts/plain"]
    # the plain (non-audit) listing keeps its established best-effort shape.
    assert await backend.list_stream_names("artifacts/") == [
        "artifacts/plain"
    ]
    # a prefix with nothing hidden under it stays complete.
    names, complete = await backend.list_stream_names_audit("manifests/")
    assert (names, complete) == ([], True)


async def test_list_document_namespaces_skips_truncated_namespace(tmp_path):
    # The GC discovers dag-run namespaces (dagrun/<dag>) through this
    # listing; a length-truncated namespace has no name sidecar to recover
    # its logical name from, so it must be reported as an INCOMPLETE
    # listing -- never returned garbled (a garbled name would be treated as
    # a removed dag and its runs' XCom scopes left unprotected).
    backend = _backend(tmp_path)
    await backend.start()

    def put(body):
        return lambda _cur: (body, None)

    await backend.mutate_document("dagrun/a", "r1", put({"runId": "1"}))
    await backend.mutate_document("dagrun/b", "r1", put({"runId": "2"}))
    await backend.mutate_document("kv/x", "k", put({"value": 1}))
    names, complete = await backend.list_document_namespaces("dagrun/")
    assert (names, complete) == (["dagrun/a", "dagrun/b"], True)
    await backend.mutate_document(
        "dagrun/" + "D" * 200, "r1", put({"runId": "3"})
    )
    names, complete = await backend.list_document_namespaces("dagrun/")
    assert names == ["dagrun/a", "dagrun/b"]
    assert complete is False
    # unrelated prefixes are unaffected by the truncated dagrun namespace.
    assert await backend.list_document_namespaces("kv/") == (["kv/x"], True)


async def test_sweep_orphan_blobs_keeps_young_unreferenced_blobs(tmp_path):
    # The put-blob-then-append-record window: a payload that has just
    # landed has no record yet, so an unreferenced-but-young blob must
    # survive the sweep (the grace is the age guard) and only a blob both
    # unreferenced AND older than the grace may go.
    backend = _backend(tmp_path)
    await backend.start()
    digest = await backend.put_blob(b"just-landed")
    assert await backend.sweep_orphan_blobs(set(), 3600.0) == 0
    assert await backend.get_blob(digest) is not None
    old = time.time() - 7200.0
    os.utime(backend._blob_path(digest), (old, old))
    # dry run counts it but must not delete...
    assert (
        await backend.sweep_orphan_blobs(set(), 3600.0, dry_run=True) == 1
    )
    assert await backend.get_blob(digest) is not None
    # ...the real pass reclaims it.
    assert await backend.sweep_orphan_blobs(set(), 3600.0) == 1
    assert await backend.get_blob(digest) is None


async def test_put_blob_dedupe_rearms_the_sweep_age_guard(tmp_path):
    # a re-put of identical content dedupes to the EXISTING blob file; if
    # that file kept its old mtime, a sweep racing the re-putter's
    # record append would read the blob as an aged orphan (its previous
    # references may be mid-deletion) and delete a payload about to be
    # referenced again.  The dedupe hit must refresh the mtime, re-arming
    # the age guard.
    backend = _backend(tmp_path)
    await backend.start()
    digest = await backend.put_blob(b"republished-content")
    old = time.time() - 7200.0
    os.utime(backend._blob_path(digest), (old, old))
    assert await backend.put_blob(b"republished-content") == digest
    assert await backend.sweep_orphan_blobs(set(), 3600.0) == 0
    assert await backend.get_blob(digest) == b"republished-content"


def test_blob_path_rejects_a_non_sha256_digest(tmp_path):
    # A digest is a content-addressed lowercase sha256 hex string.  A crafted
    # value (e.g. a "sha256" field from a malicious restore archive) must be
    # rejected before it builds a path, so it can never traverse out of the
    # blob directory via ".." or a path separator.
    backend = _backend(tmp_path)
    for bad in ("../../etc/passwd", "0" * 63, "0" * 65, "g" * 64, "AB" * 32):
        with pytest.raises(ValueError, match="invalid blob digest"):
            backend._blob_path(bad)
    # a well-formed digest still yields a normal sharded path.
    good = "a" * 64
    assert backend._blob_path(good).endswith(good + ".blob")


# --- 12: GC reclaims ONLY ephemeral leases, only once dead past grace -------


async def test_gc_reclaims_ephemeral_leases_dead_past_grace_only(
    tmp_path, monkeypatch
):
    # dagrun takes one uniquely-named advance lease per DAG run, and nothing
    # ever deleted the files: ~210k permanent files/year for a 5-minute DAG.
    # GC must reclaim a lease (and its .lock sibling) matching an EPHEMERAL
    # prefix once dead past the whole grace window -- and never sooner.
    backend = _backend(tmp_path)
    await backend.start()
    t0 = time.time()
    clock = {"t": t0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])
    lease = await backend.acquire_lease("dagadvance/d/r1", "A", ttl=10.0)
    assert lease is not None
    lock_path, lease_path = backend._lease_paths("dagadvance/d/r1")
    grace = 3600.0
    # expired, but within the grace window: never touched (the fence home).
    clock["t"] = t0 + 60.0
    result = await backend.collect_garbage(
        keep={}, grace=grace, ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,)
    )
    assert result["leases_removed"] == 0
    assert os.path.exists(lease_path)
    # dead past the whole window: dry run counts it but deletes nothing...
    clock["t"] = t0 + grace + 60.0
    dry = await backend.collect_garbage(
        keep={},
        grace=grace,
        ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,),
        dry_run=True,
    )
    assert dry["leases_removed"] == 1
    assert os.path.exists(lease_path) and os.path.exists(lock_path)
    # ...and the real pass reclaims BOTH files.
    result = await backend.collect_garbage(
        keep={}, grace=grace, ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,)
    )
    assert result["leases_removed"] == 1
    assert not os.path.exists(lease_path)
    assert not os.path.exists(lock_path)
    # without the prefix (a caller that names no ephemeral classes) the
    # same dead-past-grace lease would never have been touched.
    revived = await backend.acquire_lease("dagadvance/d/r2", "A", ttl=10.0)
    assert revived is not None
    clock["t"] = t0 + 2 * (grace + 60.0)
    result = await backend.collect_garbage(keep={}, grace=grace)
    assert result["leases_removed"] == 0
    assert os.path.exists(backend._lease_paths("dagadvance/d/r2")[1])


async def test_gc_never_reclaims_non_ephemeral_leases(tmp_path, monkeypatch):
    # REGRESSION of the previous fix round: reclaiming ANY dead lease reset
    # slot fences that persisted Replace-cancel records still reference
    # ({kind: cancel, fence: N} in slots/<job> stays newest until the next
    # cancel), so a reborn fence could re-collide and a healthy future run
    # be silently cancelled.  A slot lease dead past ANY grace window must
    # survive GC and the next acquire must CONTINUE its fence line.
    backend = _backend(tmp_path)
    await backend.start()
    t0 = time.time()
    clock = {"t": t0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])
    first = await backend.acquire_lease("slots/j", "A", ttl=10.0)
    assert first is not None and first.fence == 1
    # the durable cancel record a Replace takeover leaves behind: it names
    # fence 1 and nothing will ever prune it until another cancel lands.
    await backend.append_record("slots/j", {"kind": "cancel", "fence": 1})
    await backend.release_lease(first)
    grace = 3600.0
    clock["t"] = t0 + grace + 60.0  # dead past the whole grace window
    result = await backend.collect_garbage(
        keep={"slots/": {"j"}},
        grace=grace,
        ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,),
    )
    assert result["leases_removed"] == 0
    lock_path, lease_path = backend._lease_paths("slots/j")
    assert os.path.exists(lease_path)  # the fence counter's only home
    assert os.path.exists(lock_path)
    # the fence line CONTINUES: the reborn holder is at fence 2, so the
    # stale fence-1 cancel record can never match it again.
    nxt = await backend.acquire_lease("slots/j", "B", ttl=10.0)
    assert nxt is not None and nxt.fence == first.fence + 1
    records = await backend.list_records("slots/j")
    assert records == [{"kind": "cancel", "fence": 1}]
    assert all(rec["fence"] != nxt.fence for rec in records)


async def test_gc_dates_released_lease_by_mtime_not_expiry(
    tmp_path, monkeypatch
):
    # release marks expiresAt 0.0 IN PLACE -- ancient by expiry alone.  An
    # ephemeral lease released moments ago must survive the full grace
    # window (a stale fence from just before the release could still be
    # live), so the sweep dates a release by the file's mtime and the
    # fence counter survives.
    backend = _backend(tmp_path)
    await backend.start()
    t0 = time.time()
    clock = {"t": t0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])
    lease = await backend.acquire_lease("dagadvance/d/r1", "A", ttl=30.0)
    assert lease is not None
    await backend.release_lease(lease)
    clock["t"] = t0 + 120.0  # well within the grace window
    result = await backend.collect_garbage(
        keep={}, grace=3600.0, ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,)
    )
    assert result["leases_removed"] == 0
    _lock_path, lease_path = backend._lease_paths("dagadvance/d/r1")
    assert os.path.exists(lease_path)  # the fence counter's only home
    nxt = await backend.acquire_lease("dagadvance/d/r1", "B", ttl=30.0)
    assert nxt is not None and nxt.fence == lease.fence + 1


async def test_lease_gc_fence_reset_cannot_enable_stale_ops(
    tmp_path, monkeypatch
):
    # The safety argument for deleting an ephemeral lease file at all, as a
    # test: a lease GC reclaims has every fence it ever issued expired >=
    # grace ago, so the post-reclaim fence reset to 1 must be unobservable
    # -- a stale Lease from before the reclaim (same holder, higher fence)
    # can neither renew nor release the reborn lease.
    backend = _backend(tmp_path)
    await backend.start()
    t0 = time.time()
    clock = {"t": t0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])
    name = "dagadvance/d/rX"
    first = await backend.acquire_lease(name, "A", ttl=10.0)
    assert first is not None and first.fence == 1
    clock["t"] = t0 + 60.0
    stale = await backend.acquire_lease(name, "A", ttl=10.0)  # takeover
    assert stale is not None and stale.fence == 2
    grace = 3600.0
    clock["t"] = t0 + 60.0 + grace + 60.0  # both fences dead past grace
    result = await backend.collect_garbage(
        keep={}, grace=grace, ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,)
    )
    assert result["leases_removed"] == 1
    lock_path, lease_path = backend._lease_paths(name)
    assert not os.path.exists(lease_path)
    assert not os.path.exists(lock_path)
    reborn = await backend.acquire_lease(name, "A", ttl=30.0)
    assert reborn is not None and reborn.fence == 1
    # fence EQUALITY (never >=) is what keeps the reset safe: the stale
    # renew and the stale release must both miss the reborn lease.
    assert await backend.renew_lease(stale, ttl=30.0) is None
    await backend.release_lease(stale)
    current = await backend.read_lease(name)
    assert current is not None
    assert current.holder == "A" and current.fence == 1  # untouched


# --- 13: delete_document leaves the .lock for the GC orphan sweep -----------


async def test_delete_document_leaves_lock_sibling_alone(tmp_path):
    # REGRESSION of the previous fix round: delete_document unlinked the
    # .lock side-file eagerly, which splits the document mutex across nodes
    # on a shared NFS/EFS store (a waiter that wins the flock on the ghost
    # inode can pass the post-acquire stat re-verify through a stale
    # dentry/attribute cache while another node locks a fresh file).  The
    # hot path must never unlink a doc .lock; reclamation belongs to the
    # GC orphan sweep, gated on idle-past-grace.
    backend = _backend(tmp_path)
    await backend.start()

    def put(value):
        def transform(_current):
            return {"v": value}, None

        return transform

    await backend.mutate_document("dagrun/d", "r1", put(1))
    lock_path, doc_path = backend._doc_paths("dagrun/d", "r1")
    assert os.path.exists(doc_path) and os.path.exists(lock_path)
    assert await backend.delete_document("dagrun/d", "r1") is True
    assert not os.path.exists(doc_path)
    assert os.path.exists(lock_path)  # left for the GC sweep, never eager
    # the key stays fully usable: a fresh mutation recreates the doc under
    # the SAME lock file and reads back.
    body, _ = await backend.mutate_document("dagrun/d", "r1", put(2))
    assert body == {"v": 2}
    assert await backend.read_document("dagrun/d", "r1") == {"v": 2}
    assert os.path.exists(lock_path)


# --- 13b: the GC orphan-lock sweep ------------------------------------------


async def test_gc_sweeps_orphaned_doc_lock_only_when_doc_absent_and_idle(
    tmp_path,
):
    # a doc .lock is swept only when BOTH hold: the document is absent AND
    # the lock sat idle (mtime) past the whole grace window.  A present
    # document keeps its lock whatever the mtime says.
    backend = _backend(tmp_path)
    await backend.start()
    grace = 3600.0
    old = time.time() - grace - 120.0

    await backend.mutate_document("kv/a", "k", lambda _c: ({"v": 1}, None))
    kept_lock, kept_doc = backend._doc_paths("kv/a", "k")
    os.utime(kept_lock, (old, old))  # ancient, but the doc is PRESENT

    await backend.mutate_document("kv/b", "k", lambda _c: ({"v": 1}, None))
    await backend.delete_document("kv/b", "k")
    young_lock, _young_doc = backend._doc_paths("kv/b", "k")
    # doc absent but the lock was just touched: still within the grace.

    await backend.mutate_document("kv/c", "k", lambda _c: ({"v": 1}, None))
    await backend.delete_document("kv/c", "k")
    dead_lock, dead_doc = backend._doc_paths("kv/c", "k")
    os.utime(dead_lock, (old, old))  # doc absent AND idle past grace

    dry = await backend.collect_garbage(keep={}, grace=grace, dry_run=True)
    assert dry["locks_removed"] == 1
    assert os.path.exists(dead_lock)  # dry run deletes nothing
    result = await backend.collect_garbage(keep={}, grace=grace)
    assert result["locks_removed"] == 1
    assert os.path.exists(kept_lock)  # doc present: never swept
    assert os.path.exists(young_lock)  # idle clock not yet past grace
    assert not os.path.exists(dead_lock)
    assert not os.path.exists(dead_doc)


async def test_mutate_touch_refreshes_the_doc_lock_idle_clock(tmp_path):
    # a flock never updates mtime, so every mutate must utime the lock:
    # a doc-absent .lock that was CONTENDED recently (an idempotency key
    # between claims) must read as active and survive the sweep.
    backend = _backend(tmp_path)
    await backend.start()
    grace = 3600.0
    old = time.time() - grace - 120.0
    await backend.mutate_document("kv/t", "k", lambda _c: ({"v": 1}, None))
    await backend.delete_document("kv/t", "k")
    lock_path, _doc_path = backend._doc_paths("kv/t", "k")
    os.utime(lock_path, (old, old))
    # a doc-keeping probe of the absent key: acquires (and touches) the
    # lock without recreating the document.
    body, _ = await backend.mutate_document(
        "kv/t", "k", lambda _c: (state.DOC_KEEP, None)
    )
    assert body is None
    assert os.stat(lock_path).st_mtime > old  # the touch really landed
    result = await backend.collect_garbage(keep={}, grace=grace)
    assert result["locks_removed"] == 0
    assert os.path.exists(lock_path)


async def test_gc_sweeps_bare_lease_lock_only_once_idle_past_grace(tmp_path):
    # REGRESSION of the previous fix round: _gc_leases_sync keys off .lease
    # names, so a .lock orphaned by a lost post-release unlink (Windows AV
    # scanner handle) was never revisited -- it leaked forever.  The orphan
    # sweep must reclaim a BARE .lock (no .lease sibling) once idle past
    # the grace, and keep both a fresh bare lock and a lock whose .lease
    # still exists.
    backend = _backend(tmp_path)
    await backend.start()
    grace = 3600.0
    old = time.time() - grace - 120.0
    lease_root = os.path.join(backend.base, state.LEASES_DIR)

    # a live lease: .lock has its .lease sibling, whatever the mtime.
    lease = await backend.acquire_lease("slots/j", "A", ttl=30.0)
    assert lease is not None
    live_lock, live_lease = backend._lease_paths("slots/j")
    os.utime(live_lock, (old, old))

    # the orphan: a bare .lock aged past the grace window.
    dead_lock = os.path.join(lease_root, "dagadvance%2Fd%2Fgone.lock")
    with open(dead_lock, "wb") as fobj:
        fobj.write(b"\0")
    os.utime(dead_lock, (old, old))

    # a fresh bare .lock (an acquire between open and lease write).
    young_lock = os.path.join(lease_root, "dagadvance%2Fd%2Fnew.lock")
    with open(young_lock, "wb") as fobj:
        fobj.write(b"\0")

    dry = await backend.collect_garbage(keep={}, grace=grace, dry_run=True)
    assert dry["locks_removed"] == 1
    assert os.path.exists(dead_lock)
    result = await backend.collect_garbage(keep={}, grace=grace)
    assert result["locks_removed"] == 1
    assert not os.path.exists(dead_lock)
    assert os.path.exists(live_lock) and os.path.exists(live_lease)
    assert os.path.exists(young_lock)


# --- 14: document delete rides out Windows sharing violations ---------------


async def test_document_delete_retries_sharing_violation(
    tmp_path, monkeypatch
):
    # On Windows a concurrent reader/AV scan holding a .doc open makes
    # os.unlink raise PermissionError; the write path always retried this
    # (see _replace) but the delete path surfaced it as a spurious error
    # from a healthy store.  The unlink must retry the transient hold away.
    backend = _backend(tmp_path)
    await backend.start()
    await backend.mutate_document("ns", "k", lambda _c: ({"v": 1}, None))
    _lock_path, doc_path = backend._doc_paths("ns", "k")
    monkeypatch.setattr(state, "IS_WINDOWS", True)  # force the retry path
    calls = {"n": 0}
    real_unlink = os.unlink

    def flaky_unlink(path, *args, **kwargs):
        if (
            os.path.normpath(str(path)) == os.path.normpath(doc_path)
            and calls["n"] < 2
        ):
            calls["n"] += 1
            raise PermissionError(5, "sharing violation", str(path))
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(state.os, "unlink", flaky_unlink)
    assert await backend.delete_document("ns", "k") is True
    assert calls["n"] == 2  # the transient hold really was retried away
    assert not os.path.exists(doc_path)
    assert await backend.read_document("ns", "k") is None
