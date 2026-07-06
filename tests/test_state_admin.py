"""Backend maintenance mechanics + the `yacron2 state` admin CLI.

Backend half (FilesystemStateBackend): the self-observability stats
(op counts/errors/latency, lock waits), the maxOpsPerSecond token bucket,
the store version stamp, collect_garbage's deletion rules, and
migrate_schema's converter walk.  CLI half (yacron2.state_admin via
yacron2.__main__): backup/restore/migrate/gc/check/migrate-schema driven
through main_loop with the test_main.py argv/exit pattern.
"""

import asyncio
import datetime
import json
import os
import sys
import time
from pathlib import Path

import pytest

import yacron2.__main__
import yacron2.state as state_mod
from yacron2.state import _TokenBucket
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
    from yacron2.config import ConfigError, parse_config_string

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
    with caplog.at_level("WARNING", logger="yacron2.state"):
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


# --- the `yacron2 state` CLI ------------------------------------------------


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
        monkeypatch.setattr(sys, "argv", ["yacron2"] + argv)
        monkeypatch.setattr(sys, "exit", _exit)
        with pytest.raises(ExitError) as excinfo:
            yacron2.__main__.main_loop(loop)
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
            {"jobSetId": "v1:x", "host": "h", "jobs": [], "at": old.isoformat()},
        )

    _run(seed_manifest())
    assert (
        _cli(monkeypatch, ["state", "gc", "--dry-run", "-c", config]) == 0
    )
    out = capsys.readouterr().out
    assert "gc would remove" in out


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
    # `yacron2 -c X state check` (root -c before the subcommand)
    store = tmp_path / "store"
    config = _write_config(tmp_path, store)
    _seed_store(store)
    assert _cli(monkeypatch, ["-c", config, "state", "check"]) == 0
    assert "is writable" in capsys.readouterr().out
