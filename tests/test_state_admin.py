"""Backend maintenance mechanics + the `cronstable state` admin CLI.

Backend half (FilesystemStateBackend): the self-observability stats
(op counts/errors/latency, lock waits), the maxOpsPerSecond token bucket,
the store version stamp, collect_garbage's deletion rules, and
migrate_schema's converter walk.  CLI half (cronstable.state_admin via
cronstable.__main__): backup/restore/migrate/gc/check/migrate-schema driven
through main_loop with the test_main.py argv/exit pattern.
"""

import asyncio
import datetime
import io
import json
import os
import stat
import sys
import tarfile
import time

import pytest

import cronstable.__main__
import cronstable.state as state_mod
from cronstable import state_admin
from cronstable.platform import IS_WINDOWS
from cronstable.state import _TokenBucket
from tests.test_state import _backend

_UTC = datetime.timezone.utc


class ExitError(RuntimeError):
    pass


def _exit(num):
    raise ExitError(num)


def _run(coro):
    return asyncio.run(coro)


# --- self-observability stats ---------------------------------------------


async def test_op_stats_count_errors_and_seconds(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await backend.append_record("runs/j", {"n": 1})
    await backend.list_records("runs/j")

    def _boom():
        raise OSError("nope")

    with pytest.raises(OSError):
        await backend._call("append", _boom)
    stats = backend.stats()
    assert stats["ops"]["start"]["count"] == 1
    assert stats["ops"]["append"]["count"] == 2
    assert stats["ops"]["append"]["errors"] == 1
    assert stats["ops"]["append"]["seconds"] >= 0.0
    assert stats["ops"]["list"] == {
        "count": 1,
        "errors": 0,
        "seconds": stats["ops"]["list"]["seconds"],
    }


async def test_lock_wait_stats_accumulate(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    assert backend.stats()["lock"]["acquisitions"] == 0
    lease = await backend.acquire_lease("leader", "n1", ttl=30.0)
    assert lease is not None
    stats = backend.stats()
    assert stats["lock"]["acquisitions"] == 1
    assert stats["lock"]["wait_seconds"] >= 0.0


# --- maxOpsPerSecond rate control -----------------------------------------


async def test_token_bucket_burst_then_wait():
    bucket = _TokenBucket(50.0)
    # a take from the initial burst is free
    assert await bucket.throttle() == 0.0
    # an empty bucket with no refill credit must wait (tiny: 1/50 s)
    bucket._tokens = 0.0
    bucket._last = None
    waited = await bucket.throttle()
    assert waited > 0.0


def test_gc_grace_config_floor(tmp_path):
    from cronstable.config import ConfigError, parse_config_string

    with pytest.raises(ConfigError, match="gcGraceSeconds"):
        parse_config_string(
            "state:\n  path: {}\n  gcGraceSeconds: 3600\n".format(tmp_path),
            "",
        )
    # 0/negative (disabled) and >= a day are both fine
    for value in ("0", "-1", "86400"):
        cfg = parse_config_string(
            "state:\n  path: {}\n  gcGraceSeconds: {}\n".format(
                tmp_path, value
            ),
            "",
        ).state_config
        assert cfg is not None


def test_rate_limit_wiring(tmp_path):
    assert _backend(tmp_path)._rate_limit is None
    assert _backend(tmp_path, maxOpsPerSecond=0)._rate_limit is None
    limited = _backend(tmp_path, maxOpsPerSecond=25)
    assert limited._rate_limit is not None
    assert limited._rate_limit.rate == 25.0


async def test_throttle_stats_counted(tmp_path):
    backend = _backend(tmp_path, maxOpsPerSecond=200)
    await backend.start()
    # force the empty-bucket state (no refill credit): the next op waits
    backend._rate_limit._tokens = 0.0
    backend._rate_limit._last = None
    await backend.list_records("runs/j")
    stats = backend.stats()
    assert stats["throttle"]["count"] == 1
    assert stats["throttle"]["wait_seconds"] > 0.0


# --- store version stamp ----------------------------------------------------


async def test_meta_stamp_written_once(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    recs = await backend.list_records("meta")
    assert recs == [{"storeVersion": "v1"}]
    # a restart (same store) does not stack stamps
    backend2 = _backend(tmp_path)
    await backend2.start()
    assert await backend2.list_records("meta") == [{"storeVersion": "v1"}]


async def test_meta_stamp_newer_scheme_warns(tmp_path, caplog):
    backend = _backend(tmp_path)
    await backend.start()
    stream_dir = backend._stream_dir("meta")
    for name in os.listdir(stream_dir):
        with open(os.path.join(stream_dir, name), "wb") as fobj:
            fobj.write(
                json.dumps(
                    {"schemaVersion": "v9", "data": {"storeVersion": "v9"}}
                ).encode()
            )
    backend2 = _backend(tmp_path)
    with caplog.at_level("WARNING", logger="cronstable.state"):
        await backend2.start()
    assert any(
        "record scheme 'v9'" in r.getMessage() for r in caplog.records
    )
    # the newer stamp is respected, not overwritten
    names = os.listdir(stream_dir)
    assert len(names) == 1


# --- collect_garbage deletion rules ----------------------------------------


async def _old_append(backend, monkeypatch, stream, data, age=7200.0):
    old = state_mod._now() - age
    monkeypatch.setattr(state_mod, "_now", lambda: old)
    try:
        await backend.append_record(stream, data)
    finally:
        monkeypatch.undo()


async def test_collect_garbage_rules(tmp_path, monkeypatch):
    backend = _backend(tmp_path)
    await backend.start()
    await _old_append(backend, monkeypatch, "runs/orphan", {"x": 1})
    await _old_append(backend, monkeypatch, "runs/kept", {"x": 1})
    await _old_append(backend, monkeypatch, "logs/orphan", {"x": 1})
    await backend.append_record("runs/fresh-orphan", {"x": 1})
    # an unmanaged stream (no known prefix) must never be deleted
    await _old_append(backend, monkeypatch, "custom", {"x": 1})
    keep = {"runs/": {"kept"}, "logs/": {"kept"}}

    dry = await backend.collect_garbage(keep=keep, grace=3600.0, dry_run=True)
    assert dry["dry_run"] is True
    assert dry["streams_removed"] == 2
    assert sorted(dry["removed"]) == ["logs%2Forphan", "runs%2Forphan"]
    # dry run deleted nothing
    assert len(await backend.list_records("runs/orphan")) == 1

    lease = await backend.acquire_lease("leader", "n1", ttl=30.0)
    assert lease is not None
    result = await backend.collect_garbage(keep=keep, grace=3600.0)
    assert result["streams_removed"] == 2
    assert result["records_removed"] == 2
    assert await backend.list_records("runs/orphan") == []
    assert await backend.list_records("logs/orphan") == []
    # kept: referenced, too fresh, unmanaged, protected
    assert len(await backend.list_records("runs/kept")) == 1
    assert len(await backend.list_records("runs/fresh-orphan")) == 1
    assert len(await backend.list_records("custom")) == 1
    assert len(await backend.list_records("meta")) == 1
    # lease files are NEVER touched: a lease file is its fence's only home
    lease_after = await backend.read_lease("leader")
    assert lease_after is not None
    assert lease_after.fence == lease.fence


async def test_collect_garbage_sweeps_tmp_and_quarantine(
    tmp_path, monkeypatch
):
    backend = _backend(tmp_path)
    await backend.start()
    old = time.time() - 8 * 86400.0
    tmp_file = os.path.join(backend.base, "tmp", "w-dead-000000000001.tmp")
    with open(tmp_file, "wb") as fobj:
        fobj.write(b"debris")
    os.utime(tmp_file, (old, old))
    fresh_tmp = os.path.join(backend.base, "tmp", "w-live-000000000002.tmp")
    with open(fresh_tmp, "wb") as fobj:
        fobj.write(b"in-flight")
    quarantined = os.path.join(backend.base, "quarantine", "bad.rec.bad")
    with open(quarantined, "wb") as fobj:
        fobj.write(b"poison")
    os.utime(quarantined, (old, old))

    result = await backend.collect_garbage(keep={}, grace=7 * 86400.0)
    assert result["tmp_removed"] == 1
    assert result["quarantine_removed"] == 1
    assert not os.path.exists(tmp_file)
    assert os.path.exists(fresh_tmp)
    assert not os.path.exists(quarantined)


# --- migrate_schema ---------------------------------------------------------


def _write_raw_record(backend, stream, name, payload):
    stream_dir = backend._stream_dir(stream)
    os.makedirs(stream_dir, exist_ok=True)
    with open(os.path.join(stream_dir, name), "wb") as fobj:
        fobj.write(json.dumps(payload).encode())


async def test_migrate_schema_converts_known_versions(tmp_path, monkeypatch):
    backend = _backend(tmp_path)
    await backend.start()
    await backend.append_record("runs/j", {"outcome": "success"})
    _write_raw_record(
        backend,
        "runs/j",
        "00000000000000000001-old-000000000001.json",
        {"schemaVersion": "v0", "data": {"result": "ok"}},
    )
    _write_raw_record(
        backend,
        "runs/j",
        "00000000000000000002-old-000000000002.json",
        {"schemaVersion": "vX", "data": {"???": 1}},
    )
    monkeypatch.setitem(
        state_mod.RECORD_MIGRATIONS,
        "v0",
        lambda data: {"outcome": data.get("result")},
    )

    dry = await backend.migrate_schema(dry_run=True)
    assert dry["converted"] == 1
    assert dry["unknown"] == 1
    # meta stamp + the fresh v1 record stay current
    assert dry["current"] == 2

    result = await backend.migrate_schema()
    assert result["converted"] == 1
    recs = await backend.list_records("runs/j")
    # the vX record is quarantined by this read; the converted one now parses
    assert {"outcome": "ok"} in recs
    assert {"outcome": "success"} in recs
    after = await backend.migrate_schema(dry_run=True)
    assert after["converted"] == 0


async def test_migrate_schema_counts_converter_failures(
    tmp_path, monkeypatch
):
    backend = _backend(tmp_path)
    await backend.start()

    def _bad(_data):
        raise ValueError("converter bug")

    monkeypatch.setitem(state_mod.RECORD_MIGRATIONS, "v0", _bad)
    monkeypatch.setitem(
        state_mod.RECORD_MIGRATIONS, "v0.5", lambda data: None
    )
    _write_raw_record(
        backend,
        "runs/j",
        "00000000000000000001-old-000000000001.json",
        {"schemaVersion": "v0", "data": {}},
    )
    _write_raw_record(
        backend,
        "runs/j",
        "00000000000000000002-old-000000000002.json",
        {"schemaVersion": "v0.5", "data": {}},
    )
    result = await backend.migrate_schema()
    assert result["failed"] == 1
    assert result["unknown"] == 1


# --- the `cronstable state` CLI
# ------------------------------------------------


_JOB_BLOCK = """jobs:
  - name: j
    command: ls
    schedule: "0 0 * * *"
"""


def _write_config(tmp_path, store, name="cfg.yaml", state=True):
    parts = []
    if state:
        parts.append("state:\n  path: {}\n".format(store))
    parts.append(_JOB_BLOCK)
    config = tmp_path / name
    config.write_text("".join(parts))
    return str(config)


def _seed_store(store):
    async def go():
        backend = _backend(store)
        await backend.start()
        await backend.append_record("runs/j", {"outcome": "success"})
        await backend.append_record("runs/j", {"outcome": "failure"})

    _run(go())


def _read_store(store, stream):
    async def go():
        backend = _backend(store)
        return await backend.list_records(stream)

    return _run(go())


def _cli(monkeypatch, argv):
    loop = asyncio.new_event_loop()
    try:
        monkeypatch.setattr(sys, "argv", ["cronstable"] + argv)
        monkeypatch.setattr(sys, "exit", _exit)
        with pytest.raises(ExitError) as excinfo:
            cronstable.__main__.main_loop(loop)
        return excinfo.value.args[0]
    finally:
        loop.close()


def test_cli_check(tmp_path, monkeypatch, capsys):
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)
    _seed_store(store)
    assert _cli(monkeypatch, ["state", "check", "-c", config]) == 0
    out = capsys.readouterr().out
    assert "is writable" in out
    assert "runs: 2 record(s)" in out


def test_cli_backup_restore_roundtrip(tmp_path, monkeypatch, capsys):
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)
    _seed_store(store)
    archive = str(tmp_path / "backup.tar.gz")
    assert (
        _cli(
            monkeypatch,
            ["state", "backup", "-c", config, "-o", archive],
        )
        == 0
    )
    assert os.path.exists(archive)

    store2 = tmp_path / "restored"
    config2 = _write_config(tmp_path, store2, name="cfg2.yaml")
    assert (
        _cli(monkeypatch, ["state", "restore", "-c", config2, archive]) == 0
    )
    recs = _read_store(store2, "runs/j")
    assert {"outcome": "success"} in recs
    assert {"outcome": "failure"} in recs

    # a second restore refuses to clobber without --force ...
    assert (
        _cli(monkeypatch, ["state", "restore", "-c", config2, archive]) == 1
    )
    assert "refusing" in capsys.readouterr().out
    # ... and proceeds with it
    assert (
        _cli(
            monkeypatch,
            ["state", "restore", "-c", config2, "--force", archive],
        )
        == 0
    )


def test_cli_backup_restore_carries_docs_and_blobs(tmp_path, monkeypatch):
    # docs/ (idempotency keys, KV, cursors, distributed-lock docs) and blobs/
    # (artifact payloads) are committed durable state: backup+restore must
    # carry them, or a restore silently loses idempotency/KV/cursor/lock state
    # (a once-only guarded job re-runs) and orphans every artifact (410 gone).
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)

    async def _seed():
        backend = _backend(store)
        await backend.start()
        await backend.mutate_document(
            "kv/j", "k", lambda cur: ({"value": "v"}, None)
        )
        return await backend.put_blob(b"artifact-payload")

    digest = _run(_seed())

    archive = str(tmp_path / "backup.tar.gz")
    assert _cli(
        monkeypatch, ["state", "backup", "-c", config, "-o", archive]
    ) == 0

    store2 = tmp_path / "restored"
    config2 = _write_config(tmp_path, store2, name="cfg2.yaml")
    assert _cli(
        monkeypatch, ["state", "restore", "-c", config2, archive]
    ) == 0

    async def _check():
        backend = _backend(store2)
        await backend.start()
        doc = await backend.read_document("kv/j", "k")
        blob = await backend.get_blob(digest)
        return doc, blob

    doc, blob = _run(_check())
    assert doc is not None  # the job-state document survived the round-trip
    assert blob == b"artifact-payload"  # the artifact blob survived too


@pytest.mark.skipif(
    IS_WINDOWS, reason="POSIX file modes are not representable on Windows"
)
def test_cli_backup_archive_created_0600(tmp_path, monkeypatch):
    # the archive flattens records/docs/blobs -- captured job output, KV
    # values, artifact payloads, where secrets live -- into one file: it
    # must get the store's own 0o600, not the default 0o644, including
    # when the output path already exists with wider permissions.
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)
    _seed_store(store)
    archive = tmp_path / "backup.tar.gz"
    archive.write_bytes(b"stale")
    os.chmod(archive, 0o644)
    assert (
        _cli(
            monkeypatch,
            ["state", "backup", "-c", config, "-o", str(archive)],
        )
        == 0
    )
    assert stat.S_IMODE(os.stat(archive).st_mode) == 0o600


def test_cli_restore_force_does_not_regress_lease_fences(
    tmp_path, monkeypatch, capsys
):
    # a backup's lease files are older by definition; restoring them over
    # the store's current ones would regress the fence counters (a lease
    # file is its fence's only home), re-issuing already-handed-out fence
    # values -- the double-execution hazard in a fleet.  Restore must keep
    # the newer current lease (fence-max merge) and say so.
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)

    async def _seed():
        backend = _backend(store)
        await backend.start()
        await backend.append_record("runs/j", {"outcome": "success"})
        return await backend.acquire_lease("slot", "n1", ttl=30.0)

    lease = _run(_seed())
    assert lease is not None and lease.fence == 1

    archive = str(tmp_path / "backup.tar.gz")
    assert (
        _cli(monkeypatch, ["state", "backup", "-c", config, "-o", archive])
        == 0
    )

    # bump the store's fence past the archived one: taking over a released
    # lease increments it (release marks the lease expired in place).
    async def _bump():
        backend = _backend(store)
        await backend.start()
        current = await backend.read_lease("slot")
        await backend.release_lease(current)
        return await backend.acquire_lease("slot", "n2", ttl=30.0)

    bumped = _run(_bump())
    assert bumped is not None and bumped.fence == 2

    capsys.readouterr()
    assert (
        _cli(
            monkeypatch,
            ["state", "restore", "-c", config, "--force", archive],
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "kept 1 current lease file" in out

    async def _after():
        backend = _backend(store)
        await backend.start()
        return await backend.read_lease("slot")

    after = _run(_after())
    assert after is not None
    assert after.fence == 2  # NOT regressed to the archived fence 1
    assert after.holder == "n2"
    # the non-lease payload still merged in.
    recs = _read_store(store, "runs/j")
    assert {"outcome": "success"} in recs

    # into an EMPTY store (disaster recovery) the archived lease IS carried,
    # fence and all, so fence issuance continues from the archived value.
    store2 = tmp_path / "restored"
    config2 = _write_config(tmp_path, store2, name="cfg2.yaml")
    assert (
        _cli(monkeypatch, ["state", "restore", "-c", config2, archive]) == 0
    )

    async def _fresh():
        backend = _backend(store2)
        await backend.start()
        return await backend.read_lease("slot")

    fresh = _run(_fresh())
    assert fresh is not None and fresh.fence == 1


def test_cli_migrate(tmp_path, monkeypatch, capsys):
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)
    _seed_store(store)
    dest = tmp_path / "mount"
    assert (
        _cli(
            monkeypatch,
            ["state", "migrate", "-c", config, "--dest", str(dest)],
        )
        == 0
    )
    assert "migrated" in capsys.readouterr().out
    recs = _read_store(dest, "runs/j")
    assert len(recs) == 2
    # refusing a same-store "migration"
    assert (
        _cli(
            monkeypatch,
            ["state", "migrate", "-c", config, "--dest", str(store)],
        )
        == 1
    )


def test_cli_gc_defers_on_young_manifest_history(
    tmp_path, monkeypatch, capsys
):
    # a store with no (or young) manifest history cannot prove absence:
    # the pass must defer rather than collect with zero effective grace.
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)
    _seed_store(store)
    assert (
        _cli(monkeypatch, ["state", "gc", "--dry-run", "-c", config]) == 0
    )
    out = capsys.readouterr().out
    assert "gc deferred" in out


def test_cli_gc_dry_run(tmp_path, monkeypatch, capsys):
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)
    _seed_store(store)

    # seed a manifest OLDER than the grace window so the history-depth
    # guard is satisfied (grace is the default 7 days). Manifests are
    # per-host streams under "manifests/<host>" (see MANIFEST_STREAM_PREFIX).
    async def seed_manifest():
        backend = _backend(store)
        old = datetime.datetime.now(_UTC) - datetime.timedelta(days=8)
        await backend.append_record(
            "manifests/h",
            {
                "jobSetId": "v1:x",
                "host": "h",
                "jobs": [],
                "at": old.isoformat(),
            },
        )

    _run(seed_manifest())
    assert _cli(monkeypatch, ["state", "gc", "--dry-run", "-c", config]) == 0
    out = capsys.readouterr().out
    assert "gc would remove" in out


def test_cli_gc_reclaims_only_ephemeral_leases(tmp_path, monkeypatch):
    # `cronstable state gc` must pass the ephemeral-lease prefix through like
    # the daemon's pass: a dead-past-grace dagadvance/ per-run lease is
    # reclaimed while a slots/ lease of the same age survives (its fence
    # can live on in durable Replace-cancel records, so no grace window
    # ever makes a slot fence reset safe).
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)

    async def seed():
        backend = _backend(store)
        await backend.start()
        old = state_mod._now() - 8 * 86400.0  # dead past the 7-day grace
        monkeypatch.setattr(state_mod, "_now", lambda: old)
        try:
            assert await backend.acquire_lease(
                "dagadvance/d/r1", "A", ttl=10.0
            )
            assert await backend.acquire_lease("slots/j", "A", ttl=10.0)
        finally:
            monkeypatch.undo()
        dag_paths = backend._lease_paths("dagadvance/d/r1")
        slot_paths = backend._lease_paths("slots/j")
        for path in (dag_paths[1], slot_paths[1]):
            os.utime(path, (old, old))
        return dag_paths, slot_paths

    (dag_lock, dag_lease), (slot_lock, slot_lease) = _run(seed())
    _seed_gc_manifests(store)
    assert _cli(monkeypatch, ["state", "gc", "-c", config]) == 0
    assert not os.path.exists(dag_lease)
    assert not os.path.exists(dag_lock)
    assert os.path.exists(slot_lease)  # never touched, whatever its age
    assert os.path.exists(slot_lock)


def _seed_gc_manifests(backend_coro_store):
    """Manifests letting a default-grace (7 day) CLI gc prove absence."""

    async def go():
        backend = _backend(backend_coro_store)
        now = datetime.datetime.now(_UTC)
        await backend.append_record(
            "manifests/old-host",
            {
                "jobSetId": "v1:old",
                "host": "old-host",
                "jobs": [],
                "at": (now - datetime.timedelta(days=8)).isoformat(),
            },
        )
        await backend.append_record(
            "manifests/other-host",
            {
                "jobSetId": "v1:other",
                "host": "other-host",
                "jobs": [],
                "scopes": [],
                "dags": [],
                "at": now.isoformat(),
            },
        )

    _run(go())


def test_cli_gc_reclaims_artifacts_and_blobs(tmp_path, monkeypatch, capsys):
    # `cronstable state gc` must reclaim what the daemon pass reclaims: a
    # removed scope's artifact stream ages out and the orphaned payload
    # blob is swept -- with --dry-run reporting both without deleting.
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)

    async def seed():
        from cronstable import jobstate

        backend = _backend(store)
        await backend.start()
        old = state_mod._now() - 8 * 86400.0
        monkeypatch.setattr(state_mod, "_now", lambda: old)
        try:
            gone = await jobstate.artifact_put(
                backend, "gone", "a", b"gone-payload"
            )
            kept = await jobstate.artifact_put(
                backend, "j", "k", b"job-payload"
            )
        finally:
            monkeypatch.undo()
        for rec in (gone, kept):
            os.utime(backend._blob_path(rec["sha256"]), (old, old))
        return gone, kept

    gone, kept = _run(seed())
    _seed_gc_manifests(store)

    # dry run: the stream and its blob are reported, nothing is deleted.
    assert _cli(monkeypatch, ["state", "gc", "--dry-run", "-c", config]) == 0
    out = capsys.readouterr().out
    assert "gc would remove" in out
    assert "would remove 1 orphaned artifact blob(s)" in out
    assert len(_read_store(store, "artifacts/gone")) == 1

    assert _cli(monkeypatch, ["state", "gc", "-c", config]) == 0
    out = capsys.readouterr().out
    assert "removed 1 orphaned artifact blob(s)" in out
    assert _read_store(store, "artifacts/gone") == []
    # the config job's artifact scope survives, record and blob alike.
    assert len(_read_store(store, "artifacts/j")) == 1

    async def blobs():
        backend = _backend(store)
        return (
            await backend.get_blob(gone["sha256"]),
            await backend.get_blob(kept["sha256"]),
        )

    gone_blob, kept_blob = _run(blobs())
    assert gone_blob is None
    assert kept_blob == b"job-payload"


def test_cli_gc_sweep_skipped_on_hidden_artifact_stream(
    tmp_path, monkeypatch, capsys
):
    # the KEEP fail-safe end to end: a legacy truncated artifact stream
    # (no name sidecar) makes the enumeration incomplete, so the CLI must
    # skip the blob sweep, say why, and leave the hidden stream's payload
    # untouched.
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)

    async def seed():
        from cronstable import jobstate

        backend = _backend(store)
        await backend.start()
        old = state_mod._now() - 8 * 86400.0
        monkeypatch.setattr(state_mod, "_now", lambda: old)
        try:
            rec = await jobstate.artifact_put(
                backend, "S" * 200, "a", b"hidden-payload"
            )
        finally:
            monkeypatch.undo()
        os.utime(backend._blob_path(rec["sha256"]), (old, old))
        stream_dir = backend._stream_dir("artifacts/" + "S" * 200)
        os.unlink(os.path.join(stream_dir, state_mod._STREAM_NAME_SIDECAR))
        return rec

    rec = _run(seed())
    _seed_gc_manifests(store)
    assert _cli(monkeypatch, ["state", "gc", "-c", config]) == 0
    out = capsys.readouterr().out
    assert "orphan-blob sweep skipped" in out

    async def check():
        backend = _backend(store)
        return await backend.get_blob(rec["sha256"])

    assert _run(check()) == b"hidden-payload"


def test_cli_migrate_schema(tmp_path, monkeypatch, capsys):
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)
    _seed_store(store)
    assert (
        _cli(
            monkeypatch,
            ["state", "migrate-schema", "--dry-run", "-c", config],
        )
        == 0
    )
    assert "migrate-schema would convert 0" in capsys.readouterr().out


def test_cli_requires_state_section(tmp_path, monkeypatch, capsys):
    config = _write_config(tmp_path, tmp_path / "store", state=False)
    assert _cli(monkeypatch, ["state", "check", "-c", config]) == 1
    assert "no `state:` section" in capsys.readouterr().out


def test_cli_state_without_action(tmp_path, monkeypatch, capsys):
    assert _cli(monkeypatch, ["state"]) == 2
    assert "no action" in capsys.readouterr().out


def test_cli_root_config_position_also_works(tmp_path, monkeypatch, capsys):
    # `cronstable -c X state check` (root -c before the subcommand)
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)
    _seed_store(store)
    assert _cli(monkeypatch, ["-c", config, "state", "check"]) == 0
    assert "is writable" in capsys.readouterr().out

# ===========================================================================
# Failure surfaces: absent stores, blocked destinations, archive
# hygiene, unparseable leases, and the gc/keep-set config guards.
# ===========================================================================

# a dag section so the keep-set walk sees task templates (and their scopes)
_DAG_BLOCK = (
    "dags:\n"
    "  - name: etl\n"
    "    tasks:\n"
    "      - id: a\n"
    "        command: 'true'\n"
)


def _config_text(tmp_path, store, extra="", name="cfg.yaml"):
    config = tmp_path / name
    config.write_text("state:\n  path: {}\n".format(store) + extra)
    return str(config)


# ---------------------------------------------------------------------------
# absent / blocked stores
# ---------------------------------------------------------------------------


def test_backup_without_store_reports_nothing_to_do(
    tmp_path, monkeypatch, capsys
):
    config = _write_config(tmp_path, tmp_path / "never-created")
    archive = str(tmp_path / "b.tar.gz")
    code = _cli(monkeypatch, ["state", "backup", "-c", config, "-o", archive])
    assert code == 1
    assert "nothing to back up" in capsys.readouterr().out


def test_backup_into_unwritable_output_is_clean_error(
    tmp_path, monkeypatch, capsys
):
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)
    _seed_store(store)
    archive = str(tmp_path / "no-such-dir" / "b.tar.gz")
    code = _cli(monkeypatch, ["state", "backup", "-c", config, "-o", archive])
    assert code == 1
    assert "cronstable state error" in capsys.readouterr().out


def test_migrate_without_store_reports_nothing_to_do(
    tmp_path, monkeypatch, capsys
):
    config = _write_config(tmp_path, tmp_path / "never-created")
    code = _cli(
        monkeypatch,
        ["state", "migrate", "-c", config, "--dest", str(tmp_path / "d")],
    )
    assert code == 1
    assert "nothing to migrate" in capsys.readouterr().out


def test_migrate_refuses_populated_dest_without_force(
    tmp_path, monkeypatch, capsys
):
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)
    _seed_store(store)
    dest = tmp_path / "dest"
    _seed_store(dest)
    code = _cli(
        monkeypatch, ["state", "migrate", "-c", config, "--dest", str(dest)]
    )
    assert code == 1
    assert "--force" in capsys.readouterr().out


def test_migrate_with_dest_deployment_id(tmp_path, monkeypatch, capsys):
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)
    _seed_store(store)
    dest = tmp_path / "dest"
    code = _cli(
        monkeypatch,
        [
            "state",
            "migrate",
            "-c",
            config,
            "--dest",
            str(dest),
            "--dest-deployment-id",
            "prod-2",
        ],
    )
    assert code == 0


def test_migrate_blocked_destination_path(tmp_path, monkeypatch, capsys):
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)
    _seed_store(store)
    dest = tmp_path / "dest"
    dest.mkdir()
    # the deployment subtree cannot be created: a file sits in its place
    (dest / "default").write_text("not a directory")
    code = _cli(
        monkeypatch, ["state", "migrate", "-c", config, "--dest", str(dest)]
    )
    assert code == 1
    out = capsys.readouterr().out
    assert "failed to copy" in out or "cronstable state error" in out


def test_restore_blocked_destination_path(tmp_path, monkeypatch, capsys):
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)
    _seed_store(store)
    archive = str(tmp_path / "b.tar.gz")
    assert (
        _cli(monkeypatch, ["state", "backup", "-c", config, "-o", archive])
        == 0
    )
    dest = tmp_path / "dest"
    (dest / "default").mkdir(parents=True)
    (dest / "default" / "records").write_text("not a directory")
    config2 = _write_config(tmp_path, dest, name="cfg2.yaml")
    code = _cli(
        monkeypatch, ["state", "restore", "-c", config2, "--force", archive]
    )
    assert code == 1
    out = capsys.readouterr().out
    assert "failed to restore" in out or "cronstable state error" in out


# ---------------------------------------------------------------------------
# gc guards + dag keep-sets
# ---------------------------------------------------------------------------


def test_gc_requires_grace_window(tmp_path, monkeypatch, capsys):
    store = tmp_path / "store"
    # gcGraceSeconds: 0 disables gc entirely; a manual gc must refuse
    config = _config_text(tmp_path, store, "  gcGraceSeconds: 0\n")
    _seed_store(store)
    code = _cli(monkeypatch, ["state", "gc", "-c", config])
    assert code == 1
    assert "gcGraceSeconds" in capsys.readouterr().out


def test_gc_dry_run_with_dag_config(tmp_path, monkeypatch, capsys):
    store = tmp_path / "store"
    config = _config_text(
        tmp_path, store, "  gcGraceSeconds: 86400\n" + _DAG_BLOCK
    )
    _seed_store(store)
    code = _cli(monkeypatch, ["state", "gc", "--dry-run", "-c", config])
    assert code == 0


# ---------------------------------------------------------------------------
# check: degraded stores
# ---------------------------------------------------------------------------


def test_check_empty_store_layout(tmp_path, monkeypatch, capsys):
    store = tmp_path / "store"
    store.mkdir()  # exists but has no records/quarantine subtrees yet
    config = _write_config(tmp_path, store)
    code = _cli(monkeypatch, ["state", "check", "-c", config])
    assert code == 0
    assert "is writable" in capsys.readouterr().out


def test_check_skips_stray_files_in_records(tmp_path, monkeypatch, capsys):
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)
    _seed_store(store)
    (store / "default" / "records" / "stray.txt").write_text(
        "not a stream dir"
    )
    code = _cli(monkeypatch, ["state", "check", "-c", config])
    assert code == 0
    assert "runs: 2 record(s)" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# archive hygiene helpers (direct)
# ---------------------------------------------------------------------------


def _tar_with(members):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, kind in members:
            info = tarfile.TarInfo(name)
            if kind == "dir":
                info.type = tarfile.DIRTYPE
                tar.addfile(info)
            else:
                payload = b"{}"
                info.size = len(payload)
                tar.addfile(info, io.BytesIO(payload))
    buf.seek(0)
    return tarfile.open(fileobj=buf, mode="r")


def test_safe_members_filters_escapes(tmp_path):
    tar = _tar_with(
        [
            ("records/runs/ok.json", "file"),
            ("records", "dir"),  # a directory member is never yielded
            ("../escape.json", "file"),  # traversal out of dest
        ]
    )
    names = [m.name for m in state_admin._safe_members(tar, str(tmp_path))]
    assert names == ["records/runs/ok.json"]


@pytest.mark.skipif(
    not IS_WINDOWS, reason="mixed-drive commonpath only raises on Windows"
)
def test_safe_members_skips_foreign_drive_members(tmp_path):
    tar = _tar_with([("Z:\\evil.json", "file")])
    assert list(state_admin._safe_members(tar, str(tmp_path))) == []


def test_lease_fence_parsing():
    assert state_admin._lease_fence(b'{"fence": 7}') == 7
    assert state_admin._lease_fence(b"not json") is None
    assert state_admin._lease_fence(b'{"no": "fence"}') is None


# ---------------------------------------------------------------------------
# restore: unreadable current lease still restores (fence unprovable)
# ---------------------------------------------------------------------------


def test_restore_over_unreadable_lease(tmp_path, monkeypatch, capsys):
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)

    async def seed():
        backend = _backend(store)
        await backend.start()
        await backend.append_record("runs/j", {"outcome": "success"})
        lease = await backend.acquire_lease("leader", "n1", ttl=3600.0)
        assert lease is not None

    _run(seed())
    archive = str(tmp_path / "b.tar.gz")
    assert (
        _cli(monkeypatch, ["state", "backup", "-c", config, "-o", archive])
        == 0
    )
    # find the lease member the backup carried and block its restore target
    with tarfile.open(archive) as tar:
        lease_members = [
            m.name
            for m in tar.getmembers()
            if m.isfile() and m.name.startswith("leases/")
        ]
    assert lease_members
    dest = tmp_path / "dest"
    config2 = _write_config(tmp_path, dest, name="cfg2.yaml")
    target = dest / "default" / lease_members[0]
    target.mkdir(parents=True)  # a directory where the lease file would go
    code = _cli(
        monkeypatch, ["state", "restore", "-c", config2, "--force", archive]
    )
    # the current lease exists but its fence cannot be read: unprovable, so
    # the restore keeps the store's copy rather than risk a fence regression
    assert code == 0
    assert "kept 1 current lease file(s)" in capsys.readouterr().out
