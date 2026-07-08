"""The optional durable state backend: config, records, cursor, lease, probe.

Exercises :mod:`cronstable.state` and its config/lifecycle wiring end to end
against a real temp directory (the "local filesystem == Amazon S3 Files, one
backend" path), with the topology probe and clock stubbed where a test needs to
drive them deterministically.
"""

import asyncio
import datetime
import json
import logging
import os
import threading

import pytest

from cronstable import state
from cronstable.config import ConfigError, parse_config_string
from cronstable.cron import Cron, JobRunInfo, _job_run_info_from_dict
from cronstable.job import JobOutputStream
from cronstable.state import (
    FilesystemStateBackend,
    Lease,
    _fs_safe,
    _unescape_mount,
    detect_topology,
    make_state_backend,
)

_UTC = datetime.timezone.utc


def _info(second=0, outcome="success", exit_code=0, fail_reason=None):
    dt = datetime.datetime(2026, 7, 1, 0, 0, second, tzinfo=_UTC)
    return JobRunInfo(
        outcome=outcome,
        exit_code=exit_code,
        started_at=dt,
        finished_at=dt,
        fail_reason=fail_reason,
        output=JobOutputStream(),
    )


_ONE_JOB = (
    "jobs:\n  - name: j\n    command: 'true'\n    schedule: '* * * * *'\n"
)


async def _drain_state_writes(cron):
    await asyncio.gather(*list(cron._pending_state_writes))

# --- config parsing -------------------------------------------------------


def _state_cfg(yaml):
    return parse_config_string(yaml, "").state_config


def test_no_state_section_is_none():
    # stateless by default: no `state:` -> no config, nothing constructed.
    assert _state_cfg("") is None


def test_state_defaults_filled():
    cfg = _state_cfg("state:\n  path: /var/lib/cronstable\n")
    assert cfg is not None
    assert cfg["path"] == "/var/lib/cronstable"
    assert cfg["topology"] == "auto"
    assert cfg["deploymentId"] is None


def test_state_all_fields():
    cfg = _state_cfg(
        "state:\n"
        "  path: /mnt/s3files/cronstable\n"
        "  topology: shared\n"
        "  deploymentId: my-app\n"
    )
    assert cfg["topology"] == "shared"
    assert cfg["deploymentId"] == "my-app"


def test_state_path_required_by_schema():
    # `path` is a required key in the schema.
    with pytest.raises(ConfigError):
        _state_cfg("state:\n  topology: shared\n")


def test_state_path_rejects_blank():
    with pytest.raises(ConfigError, match="state.path is required"):
        _state_cfg("state:\n  path: '   '\n")


def test_state_topology_enum_validated():
    with pytest.raises(ConfigError):
        _state_cfg("state:\n  path: /x\n  topology: bogus\n")


def test_multiple_state_sections_via_include_rejected(tmp_path):
    child = tmp_path / "child.yaml"
    child.write_text("state:\n  path: /b\n")
    parent = tmp_path / "parent.yaml"
    parent.write_text("state:\n  path: /a\ninclude:\n  - child.yaml\n")
    from cronstable.config import parse_config_file

    with pytest.raises(ConfigError, match="multiple state configs"):
        parse_config_file(str(parent))


def test_state_section_from_config_dir(tmp_path):
    # a config directory: the `state` section is picked up from whichever file
    # carries it (the multi-file merge path in _parse_config_dir).
    (tmp_path / "10-jobs.yaml").write_text(
        "jobs:\n  - name: j\n    command: 'true'\n    schedule: '* * * * *'\n"
    )
    (tmp_path / "20-state.yaml").write_text("state:\n  path: /srv/state\n")
    from cronstable.config import parse_config

    cfg = parse_config(str(tmp_path))
    assert cfg.state_config is not None
    assert cfg.state_config["path"] == "/srv/state"


def test_multiple_state_sections_in_dir_rejected(tmp_path):
    (tmp_path / "a.yaml").write_text("state:\n  path: /a\n")
    (tmp_path / "b.yaml").write_text("state:\n  path: /b\n")
    from cronstable.config import parse_config

    with pytest.raises(ConfigError, match="Multiple 'state' configurations"):
        parse_config(str(tmp_path))


# --- backend construction / factory --------------------------------------


def _backend(tmp_path, **over):
    cfg = {
        "path": str(tmp_path),
        "topology": "single-node",
        "deploymentId": None,
    }
    cfg.update(over)
    return FilesystemStateBackend(cfg, lambda: "jobset-abc")  # type: ignore[arg-type]


def test_factory_builds_filesystem(tmp_path):
    cfg = _state_cfg("state:\n  path: " + str(tmp_path) + "\n")
    backend = make_state_backend(cfg, lambda: "js")
    assert isinstance(backend, FilesystemStateBackend)


def test_factory_unknown_backend_raises(tmp_path):
    cfg = {"path": str(tmp_path), "backend": "nope"}
    with pytest.raises(ConfigError, match="unknown state.backend"):
        make_state_backend(cfg, lambda: "js")  # type: ignore[arg-type]


async def test_start_creates_layout(tmp_path):
    backend = _backend(tmp_path, deploymentId="app1")
    await backend.start()
    base = os.path.join(str(tmp_path), "app1")
    for sub in ("records", "leases", "quarantine", "tmp"):
        assert os.path.isdir(os.path.join(base, sub))
    # the write-probe file is cleaned up after start.
    assert not any(
        n.endswith(".probe")
        for n in os.listdir(os.path.join(base, "tmp"))
    )
    await backend.stop()


async def test_default_namespace(tmp_path):
    backend = _backend(tmp_path)  # deploymentId None
    await backend.start()
    assert backend.namespace == "default"
    assert os.path.isdir(os.path.join(str(tmp_path), "default", "records"))


async def test_start_failure_on_unwritable_path(tmp_path):
    # point the store at an existing *file*: makedirs under it raises OSError.
    afile = tmp_path / "not-a-dir"
    afile.write_text("x")
    backend = _backend(tmp_path, path=str(afile))
    with pytest.raises(OSError):
        await backend.start()


# --- records + derived cursor --------------------------------------------


async def test_append_and_list_roundtrip(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    rid = await backend.append_record("runs", {"outcome": "success", "n": 1})
    assert isinstance(rid, str) and rid
    await backend.append_record("runs", {"outcome": "failure", "n": 2})
    got = await backend.list_records("runs")
    assert [r["n"] for r in got] == [1, 2]
    assert got[0]["outcome"] == "success"


async def test_list_missing_stream_is_empty(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    assert await backend.list_records("never-written") == []


async def test_list_newest_first_and_limit(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    for i in range(5):
        await backend.append_record("s", {"i": i})
    newest = await backend.list_records("s", newest_first=True, limit=2)
    assert [r["i"] for r in newest] == [4, 3]
    oldest = await backend.list_records("s", limit=3)
    assert [r["i"] for r in oldest] == [0, 1, 2]


async def test_records_are_immutable_and_versioned(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await backend.append_record("s", {"k": "v"})
    stream_dir = backend._stream_dir("s")
    files = [n for n in os.listdir(stream_dir) if n.endswith(".json")]
    assert len(files) == 1
    on_disk = json.loads((open(os.path.join(stream_dir, files[0])).read()))
    assert on_disk["schemaVersion"] == state.SCHEME_VERSION
    assert on_disk["data"] == {"k": "v"}


async def test_derive_max_is_order_independent(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    for ts in (5, 3, 8, 1, 4):
        await backend.append_record("s", {"ts": ts})
    assert await backend.derive_max("s", "ts") == 8


async def test_derive_max_empty_and_missing_field(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    assert await backend.derive_max("s", "ts") is None
    await backend.append_record("s", {"other": 1})
    assert await backend.derive_max("s", "ts") is None


async def test_derive_max_ignores_incomparable(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await backend.append_record("s", {"v": "abc"})
    await backend.append_record("s", {"v": 5})  # int vs str: incomparable
    # does not raise; keeps the first-seen value.
    assert await backend.derive_max("s", "v") == "abc"


async def test_derive_max_fails_closed_on_read_error(tmp_path):
    # H1: an ENVIRONMENTAL read error (NFS blip, AV hold, cross-user EACCES)
    # on one record must fail the whole derive (raise), never silently return
    # the max over the SURVIVORS -- a value below the true max, which regresses
    # the "last fired" watermark and makes catch-up replay an already-run slot.
    backend = _backend(tmp_path)
    await backend.start()
    for ts in (1, 2, 3):
        await backend.append_record("s", {"ts": ts})
    stream_dir = backend._stream_dir("s")
    names = sorted(n for n in os.listdir(stream_dir) if n.endswith(".json"))
    # make the NEWEST record raise OSError on open: swap the file for a
    # directory of the same name (open() on a dir raises OSError everywhere).
    victim = os.path.join(stream_dir, names[-1])
    os.remove(victim)
    os.mkdir(victim)
    with pytest.raises(OSError):
        await backend.derive_max("s", "ts")
    # list_records stays BEST-EFFORT (non-strict): it still swallows the
    # unreadable record and returns the survivors, so the fix is localized to
    # the watermark path.
    survived = await backend.list_records("s")
    assert {r["ts"] for r in survived} == {1, 2}


async def test_derive_max_still_skips_poison_record(tmp_path):
    # the strict path fails closed only for ENVIRONMENTAL errors; a genuinely
    # corrupt (content-bad) record is unrecoverable and is still quarantined
    # and skipped, so one poison object can never wedge the watermark forever.
    backend = _backend(tmp_path)
    await backend.start()
    await backend.append_record("s", {"ts": 5})
    stream_dir = backend._stream_dir("s")
    with open(os.path.join(stream_dir, "00000-bad.json"), "w") as fobj:
        fobj.write("{not json")
    assert await backend.derive_max("s", "ts") == 5


# --- worker lanes: lease isolation + wedge observability -----------------


async def _wait_until(predicate, timeout=3.0):
    """Poll ``predicate`` (generous window; never tightens on slow CI)."""
    for _ in range(int(timeout / 0.01)):
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return predicate()


async def test_lease_lane_isolated_from_saturated_bulk_pool(tmp_path):
    # H2/M27: lease/coordination ops run in a DEDICATED worker lane, so a bulk
    # pool fully saturated by slow or wedged record writes cannot starve a
    # lease renew below its TTL -- which would expire a live holder's lease and
    # hand its fenced work to a standby (split-brain / double-fire).  Saturate
    # every bulk slot with a wedged op and prove a lease op still gets through
    # while a further bulk op does not.
    backend = _backend(tmp_path)
    await backend.start()
    gate = threading.Event()

    def _wedge():
        gate.wait(timeout=30.0)  # hold the worker (and its bulk slot) hostage

    bulk = [
        asyncio.create_task(backend._call("bulk-wedge", _wedge))
        for _ in range(state.BULK_CALL_SLOTS)
    ]
    try:
        # every wedged op acquires its bulk slot: the bulk lane is now full.
        assert await _wait_until(
            lambda: backend.stats()["workers"]["bulk_inflight"]
            == state.BULK_CALL_SLOTS
        )
        assert backend.stats()["workers"]["lease_inflight"] == 0
        # a LEASE op still completes promptly on its own lane...
        got = await asyncio.wait_for(
            backend._call("lease-probe", lambda: "ok"), timeout=2.0
        )
        assert got == "ok"
        # ...while a further BULK op is blocked (the bulk lane is exhausted).
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                backend._call("bulk-extra", lambda: "nope"), timeout=0.3
            )
    finally:
        gate.set()
        await asyncio.gather(*bulk)
    await backend.stop()


async def test_stats_reports_worker_lane_occupancy(tmp_path):
    # M27: a wedged store must be VISIBLE.  The op counters only tick when an
    # op FINISHES, so a hung mount reads as idle; the worker gauge shows the
    # live occupancy, so a lane pinned at capacity is the "wedged" signal.
    backend = _backend(tmp_path)
    await backend.start()
    w = backend.stats()["workers"]
    assert w["bulk_capacity"] == state.BULK_CALL_SLOTS
    assert w["lease_capacity"] == state.LEASE_CALL_SLOTS
    assert w["bulk_inflight"] == 0 and w["lease_inflight"] == 0
    assert w["bulk_peak"] >= 1  # start() itself ran a bulk op

    gate = threading.Event()
    task = asyncio.create_task(
        backend._call("bulk-wedge", lambda: gate.wait(30.0))
    )
    try:
        # the wedged op is observable as one in-flight bulk worker.
        assert await _wait_until(
            lambda: backend.stats()["workers"]["bulk_inflight"] == 1
        )
    finally:
        gate.set()
        await task
    # and it returns to zero once the op drains.
    assert await _wait_until(
        lambda: backend.stats()["workers"]["bulk_inflight"] == 0
    )
    await backend.stop()


# --- directory-entry durability -------------------------------------------


async def test_append_to_fresh_stream_fsyncs_new_directory_chain(
    tmp_path, monkeypatch
):
    # the very first append into a stream that never existed before must
    # makedirs it durably: without flushing each newly-created level's
    # PARENT, a power loss right after could drop the whole new subtree
    # (parent and all), silently taking the just-fsynced record with it.
    flushed = []
    monkeypatch.setattr(
        state, "fsync_directory", lambda p: flushed.append(p)
    )
    backend = _backend(tmp_path)
    await backend.start()
    flushed.clear()
    await backend.append_record("brand-new-stream", {"x": 1})
    stream_dir = backend._stream_dir("brand-new-stream")
    # the stream dir's own PARENT (records/) must have been flushed, so the
    # newly-created stream directory's entry is durable.
    assert os.path.dirname(stream_dir) in flushed
    # and a second append into the now-existing stream does not re-walk/
    # re-flush the directory chain (it already exists).
    flushed.clear()
    await backend.append_record("brand-new-stream", {"x": 2})
    assert os.path.dirname(stream_dir) not in flushed


async def test_document_delete_fsyncs_namespace_directory(
    tmp_path, monkeypatch
):
    # without this, an unlinked idempotency/KV document can RESURRECT after
    # a power loss (the unlink itself was never made durable), silently
    # undoing the delete and letting guarded once-only work run again.
    backend = _backend(tmp_path)
    await backend.start()
    await backend.mutate_document("ns", "k", lambda c: ({"v": 1}, None))
    flushed = []
    monkeypatch.setattr(
        state, "fsync_directory", lambda p: flushed.append(p)
    )
    await backend.mutate_document(
        "ns", "k", lambda c: (state.DOC_DELETE, None)
    )
    _lock_path, doc_path = backend._doc_paths("ns", "k")
    assert os.path.dirname(doc_path) in flushed
    assert await backend.read_document("ns", "k") is None


# --- corrupt-record quarantine -------------------------------------------


async def test_corrupt_json_is_quarantined(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await backend.append_record("s", {"good": True})
    stream_dir = backend._stream_dir("s")
    # a truncated/garbage record (as a crash mid-write on a non-atomic store
    # could leave) lands under the stream's final name.
    with open(os.path.join(stream_dir, "00000-bad.json"), "w") as fobj:
        fobj.write("{not json")
    got = await backend.list_records("s")
    assert got == [{"good": True}]  # bad one skipped
    # and moved out of the stream into quarantine.
    assert "00000-bad.json" not in os.listdir(stream_dir)
    quarantine = os.path.join(backend.base, "quarantine")
    assert any(n.startswith("00000-bad.json") for n in os.listdir(quarantine))


async def test_unrecognised_schema_version_is_left_in_place(tmp_path):
    # A well-formed record with a schemaVersion this build doesn't recognise
    # is most likely written by a newer peer mid rolling-upgrade: it must be
    # skipped, never destructively quarantined (deleting it would erase that
    # peer's record fleet-wide the moment an old node reads the shared store).
    backend = _backend(tmp_path)
    await backend.start()
    stream_dir = backend._stream_dir("s")
    os.makedirs(stream_dir, exist_ok=True)
    with open(os.path.join(stream_dir, "00001-new.json"), "w") as fobj:
        json.dump({"schemaVersion": "v99", "data": {"x": 1}}, fobj)
    assert await backend.list_records("s") == []
    assert "00001-new.json" in os.listdir(stream_dir)
    quarantine = os.path.join(backend.base, "quarantine")
    assert not os.path.isdir(quarantine) or os.listdir(quarantine) == []


async def test_unrecognised_schema_version_fails_closed_when_strict(tmp_path):
    from cronstable.state import _DocumentUnreadable

    backend = _backend(tmp_path)
    await backend.start()
    stream_dir = backend._stream_dir("s")
    os.makedirs(stream_dir, exist_ok=True)
    with open(os.path.join(stream_dir, "00001-new.json"), "w") as fobj:
        json.dump({"schemaVersion": "v99", "data": {"finished_at": "x"}}, fobj)
    with pytest.raises(_DocumentUnreadable):
        await backend.derive_max("s", "finished_at")


async def test_malformed_record_shape_is_still_quarantined(tmp_path):
    # Distinct from an unrecognised-but-well-formed schemaVersion: a record
    # whose "data" isn't even a dict is genuinely unreadable content, not a
    # forward-compat gap, so destructive quarantine is still correct here.
    backend = _backend(tmp_path)
    await backend.start()
    stream_dir = backend._stream_dir("s")
    os.makedirs(stream_dir, exist_ok=True)
    with open(os.path.join(stream_dir, "00001-bad.json"), "w") as fobj:
        json.dump(
            {"schemaVersion": state.SCHEME_VERSION, "data": "not-a-dict"}, fobj
        )
    assert await backend.list_records("s") == []
    assert "00001-bad.json" not in os.listdir(stream_dir)
    quarantine = os.path.join(backend.base, "quarantine")
    assert any(n.startswith("00001-bad.json") for n in os.listdir(quarantine))


# --- lease primitive ------------------------------------------------------


async def test_lease_acquire_read_release(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    lease = await backend.acquire_lease("job-x", "node-a", ttl=30)
    assert lease is not None
    assert lease.holder == "node-a"
    assert lease.fence == 1
    observed = await backend.read_lease("job-x")
    assert observed is not None and observed.holder == "node-a"
    await backend.release_lease(lease)
    assert await backend.read_lease("job-x") is None


async def test_lease_denies_second_holder_while_valid(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    a = await backend.acquire_lease("L", "A", ttl=30)
    assert a is not None
    # B cannot take a validly-held lease.
    assert await backend.acquire_lease("L", "B", ttl=30) is None


async def test_lease_same_holder_renews_keeps_fence(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    a1 = await backend.acquire_lease("L", "A", ttl=30)
    a2 = await backend.acquire_lease("L", "A", ttl=30)
    assert a1 is not None and a2 is not None
    assert a2.fence == a1.fence == 1


async def test_lease_takeover_after_expiry_bumps_fence(tmp_path, monkeypatch):
    backend = _backend(tmp_path)
    await backend.start()
    clock = {"t": 1000.0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])

    a = await backend.acquire_lease("L", "A", ttl=10)  # expires at 1010
    assert a is not None and a.fence == 1
    clock["t"] = 1020.0  # past expiry
    b = await backend.acquire_lease("L", "B", ttl=10)
    assert b is not None
    assert b.holder == "B"
    assert b.fence == 2  # takeover bumps the fence

    # A's stale renew is fenced off (someone took over).
    assert await backend.renew_lease(a, ttl=10) is None
    # B can still renew.
    b2 = await backend.renew_lease(b, ttl=10)
    assert b2 is not None and b2.fence == 2


async def test_lease_release_only_by_current_holder(tmp_path, monkeypatch):
    backend = _backend(tmp_path)
    await backend.start()
    clock = {"t": 1000.0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])
    a = await backend.acquire_lease("L", "A", ttl=10)
    assert a is not None
    clock["t"] = 1020.0
    b = await backend.acquire_lease("L", "B", ttl=10)
    assert b is not None
    # A trying to release does not remove B's lease.
    await backend.release_lease(a)
    still = await backend.read_lease("L")
    assert still is not None and still.holder == "B"


async def test_renew_missing_lease_returns_none(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    ghost = Lease(name="L", holder="A", fence=1, expires_at=0.0)
    assert await backend.renew_lease(ghost, ttl=10) is None


async def test_release_missing_lease_is_noop(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    ghost = Lease(name="L", holder="A", fence=1, expires_at=0.0)
    await backend.release_lease(ghost)  # must not raise


async def test_read_corrupt_lease_is_none(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    lock, lease_path = backend._lease_paths("L")
    os.makedirs(os.path.dirname(lease_path), exist_ok=True)
    with open(lease_path, "w") as fobj:
        fobj.write("garbage")
    assert await backend.read_lease("L") is None


# --- topology probe -------------------------------------------------------


async def test_topology_explicit_shared(tmp_path, monkeypatch):
    # explicit shared overrides even when the probe disagrees (and warns).
    monkeypatch.setattr(state, "IS_WINDOWS", False)
    monkeypatch.setattr(state, "_mount_fstype", lambda p: "ext4")
    backend = _backend(tmp_path, topology="shared")
    await backend.start()
    assert backend.topology == "shared"
    assert backend.supports_shared_locking() is True


async def test_topology_explicit_single_node(tmp_path):
    backend = _backend(tmp_path, topology="single-node")
    await backend.start()
    assert backend.topology == "single-node"
    assert backend.supports_shared_locking() is False


async def test_topology_auto_detects_shared(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "IS_WINDOWS", False)
    monkeypatch.setattr(state, "_mount_fstype", lambda p: "nfs4")
    backend = _backend(tmp_path, topology="auto")
    await backend.start()
    assert backend.topology == "shared"


async def test_topology_auto_detects_single_node(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "IS_WINDOWS", False)
    monkeypatch.setattr(state, "_mount_fstype", lambda p: "xfs")
    backend = _backend(tmp_path, topology="auto")
    await backend.start()
    assert backend.topology == "single-node"


async def test_topology_auto_unknown_falls_back(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "IS_WINDOWS", False)
    monkeypatch.setattr(state, "_mount_fstype", lambda p: None)
    backend = _backend(tmp_path, topology="auto")
    await backend.start()
    assert backend.topology == "single-node"


def test_detect_topology_uses_fstype(monkeypatch):
    monkeypatch.setattr(state, "IS_WINDOWS", False)
    monkeypatch.setattr(state, "_mount_fstype", lambda p: "nfs")
    assert detect_topology("/x") == "shared"
    monkeypatch.setattr(state, "_mount_fstype", lambda p: "btrfs")
    assert detect_topology("/x") == "single-node"
    monkeypatch.setattr(state, "_mount_fstype", lambda p: None)
    assert detect_topology("/x") is None


def test_detect_topology_windows_returns_none(monkeypatch):
    # on Windows there is no cross-host lock story and no /proc to probe.
    monkeypatch.setattr(state, "IS_WINDOWS", True)
    assert detect_topology("/x") is None


def test_mount_fstype_longest_prefix(tmp_path, monkeypatch):
    mounts = (
        "rootfs / rootfs rw 0 0\n"
        "srv /srv ext4 rw 0 0\n"
        "efs /srv/data\\040dir nfs4 rw 0 0\n"
    )
    real = os.path.realpath

    def fake_open(path, *a, **k):
        assert path == "/proc/mounts"
        import io

        return io.StringIO(mounts)

    monkeypatch.setattr(state, "open", fake_open, raising=False)
    monkeypatch.setattr(os.path, "realpath", lambda p: p)
    # deepest matching mountpoint wins; the escaped space is decoded.
    assert state._mount_fstype("/srv/data dir/x") == "nfs4"
    assert state._mount_fstype("/srv/other") == "ext4"
    assert state._mount_fstype("/elsewhere") == "rootfs"
    monkeypatch.setattr(os.path, "realpath", real)


def test_mount_fstype_no_proc(monkeypatch):
    def boom(path, *a, **k):
        raise OSError("no /proc")

    monkeypatch.setattr(state, "open", boom, raising=False)
    assert state._mount_fstype("/x") is None


# --- small helpers --------------------------------------------------------


def test_fs_safe_is_injective_and_portable():
    assert _fs_safe("plain-name_1") == "plain-name_1"
    # path separators / spaces / unicode are percent-encoded, not dropped.
    assert "/" not in _fs_safe("a/b c")
    assert _fs_safe("a/b") != _fs_safe("a-b")  # no collision
    assert _fs_safe("") == "_"


def test_fs_safe_case_insensitive_injectivity():
    # NTFS/APFS resolve names case-insensitively: two jobs differing only by
    # case must not share one on-disk stream (merged ledgers -> wrong
    # watermark/gate decisions), so uppercase is encoded, not passed through.
    assert _fs_safe("Backup").lower() != _fs_safe("backup").lower()
    assert _fs_safe("JOB").lower() != _fs_safe("job").lower()


def test_fs_safe_neutralizes_traversal_and_windows_hazards():
    # "." is encoded, so no name can yield a "." / ".." path component (a
    # deploymentId of ".." would otherwise escape the state root) or the
    # trailing-dot aliases Windows strips.
    assert _fs_safe("..") != ".."
    assert "." not in _fs_safe("..")
    assert not _fs_safe("job.").endswith(".")
    # reserved Windows device names are re-encoded to something openable.
    for name in ("con", "nul", "com1", "lpt9"):
        assert _fs_safe(name) not in {name, name.upper()}
    # ...without breaking injectivity against a literal "%63on"-style name.
    assert _fs_safe("con") != _fs_safe("%63on")


def test_fs_safe_caps_component_length():
    # percent-encoding expands 3x per non-safe byte (9x per CJK char):
    # without a cap a long job name exceeds NAME_MAX=255 and every append
    # for its stream fails with ENAMETOOLONG forever. Long names truncate to
    # a digest-uniqued token that still tells inputs apart.
    long_a = "測試" * 60
    long_b = "測試" * 60 + "x"
    token_a, token_b = _fs_safe(long_a), _fs_safe(long_b)
    assert len(token_a.encode()) <= 150
    assert len(token_b.encode()) <= 150
    assert token_a != token_b


def test_unescape_mount():
    assert _unescape_mount("/plain/path") == "/plain/path"
    assert _unescape_mount("/a\\040b") == "/a b"  # \040 == space
    assert _unescape_mount("/a\\011b") == "/a\tb"  # \011 == tab


def test_view_dict(tmp_path):
    backend = _backend(tmp_path, deploymentId="dep")
    backend._topology = "shared"
    view = backend.view_dict()
    assert view["backend"] == "filesystem"
    assert view["namespace"] == "dep"
    assert view["topology"] == "shared"
    assert view["shared_locking"] is True
    assert view["job_set_id"] == "jobset-abc"


# --- Cron lifecycle wiring ------------------------------------------------


async def test_cron_start_stop_state(tmp_path):
    cron = Cron(None)
    assert cron.state_backend is None
    cfg = _state_cfg("state:\n  path: " + str(tmp_path) + "\n")
    await cron.start_stop_state(cfg)
    assert cron.state_backend is not None
    # unchanged config -> the same backend instance is kept (no churn).
    same = cron.state_backend
    await cron.start_stop_state(cfg)
    assert cron.state_backend is same
    # removing the section tears it down.
    await cron.start_stop_state(None)
    assert cron.state_backend is None


async def test_cron_state_rebuilds_on_change(tmp_path):
    cron = Cron(None)
    a = tmp_path / "a"
    b = tmp_path / "b"
    a.mkdir()
    b.mkdir()
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(a) + "\n"))
    first = cron.state_backend
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(b) + "\n"))
    assert cron.state_backend is not None
    assert cron.state_backend is not first


async def test_cron_state_start_failure_is_swallowed(tmp_path, caplog):
    cron = Cron(None)
    afile = tmp_path / "afile"
    afile.write_text("x")
    cfg = _state_cfg("state:\n  path: " + str(afile) + "\n")
    await cron.start_stop_state(cfg)
    # a bad path is logged and swallowed; jobs keep running in memory.
    assert cron.state_backend is None
    assert any(
        "state: failed to start" in r.getMessage() for r in caplog.records
    )


# --- retention / pruning ----------------------------------------


async def test_prune_keeps_newest(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    for i in range(5):
        await backend.append_record("s", {"i": i})
    removed = await backend.prune_records("s", keep=2)
    assert removed == 3
    assert [r["i"] for r in await backend.list_records("s")] == [3, 4]


async def test_prune_keep_zero_deletes_all(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    for i in range(3):
        await backend.append_record("s", {"i": i})
    assert await backend.prune_records("s", keep=0) == 3
    assert await backend.list_records("s") == []


async def test_prune_fewer_than_keep_is_noop(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await backend.append_record("s", {"i": 0})
    assert await backend.prune_records("s", keep=10) == 0
    assert len(await backend.list_records("s")) == 1


async def test_prune_missing_stream(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    assert await backend.prune_records("nope", keep=5) == 0


# --- config retention knob --------------------------------------


def test_state_max_runs_default():
    assert _state_cfg("state:\n  path: /x\n")["maxRunsPerJob"] == 1000


def test_state_max_runs_custom():
    cfg = _state_cfg("state:\n  path: /x\n  maxRunsPerJob: 5\n")
    assert cfg["maxRunsPerJob"] == 5


# --- JobRunInfo reconstruction ----------------------------------


def test_job_run_info_from_dict_roundtrip():
    dt = datetime.datetime(2026, 7, 3, 12, 0, 0, tzinfo=_UTC)
    orig = JobRunInfo(
        outcome="failure",
        exit_code=2,
        started_at=dt,
        finished_at=dt,
        fail_reason="boom",
        output=JobOutputStream(),
    )
    restored = _job_run_info_from_dict(orig.to_dict())
    assert restored is not None
    assert restored.outcome == "failure"
    assert restored.exit_code == 2
    assert restored.fail_reason == "boom"
    assert restored.finished_at == dt
    assert restored.started_at == dt
    # output is not persisted: a rehydrated run gets an empty, closed stream.
    assert restored.output.closed is True


def test_job_run_info_from_dict_no_started_at():
    dt = datetime.datetime(2026, 7, 3, 12, 0, 0, tzinfo=_UTC)
    rec = {
        "outcome": "success",
        "exit_code": None,
        "started_at": None,
        "finished_at": dt.isoformat(),
        "fail_reason": None,
    }
    restored = _job_run_info_from_dict(rec)
    assert restored is not None
    assert restored.started_at is None
    assert restored.duration is None


def test_job_run_info_from_dict_bad_record_returns_none():
    assert _job_run_info_from_dict({}) is None
    assert _job_run_info_from_dict({"finished_at": "not-a-date"}) is None
    assert _job_run_info_from_dict({"finished_at": 123}) is None


# --- Cron durable run ledger ------------------------------------


async def test_record_run_persists_to_ledger(tmp_path):
    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    cron._record_run("j", _info(1, outcome="success"))
    await _drain_state_writes(cron)
    recs = await cron.state_backend.list_records(cron._run_stream("j"))
    assert len(recs) == 1
    assert recs[0]["outcome"] == "success"


async def test_record_run_noop_without_backend():
    cron = Cron(None, config_yaml=_ONE_JOB)
    cron._record_run("j", _info(0))
    # no backend -> no durable write scheduled, classic in-memory path only.
    assert not cron._pending_state_writes
    assert len(cron.run_history["j"]) == 1


async def test_prune_on_append_bounds_ledger(tmp_path):
    cron = Cron(None, config_yaml=_ONE_JOB)
    cfg = _state_cfg(
        "state:\n  path: " + str(tmp_path) + "\n  maxRunsPerJob: 3\n"
    )
    await cron.start_stop_state(cfg)
    for i in range(6):
        cron._record_run("j", _info(i))
        await _drain_state_writes(cron)  # sequential -> deterministic bound
    recs = await cron.state_backend.list_records(cron._run_stream("j"))
    assert len(recs) == 3


async def test_persist_error_is_swallowed(tmp_path, caplog):
    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))

    async def boom(*a, **k):
        raise OSError("disk full")

    cron.state_backend.append_record = boom  # type: ignore[method-assign]
    cron._record_run("j", _info(0))
    await _drain_state_writes(cron)
    assert any(
        "failed to persist run record" in r.getMessage()
        for r in caplog.records
    )


# --- warm-dashboard rehydration + watermark ---------------------


async def _prepopulate_ledger(tmp_path, finished_isos):
    backend = _backend(tmp_path)
    await backend.start()
    for iso in finished_isos:
        await backend.append_record(
            "runs/j",
            {
                "outcome": "success",
                "exit_code": 0,
                "started_at": None,
                "finished_at": iso,
                "duration": None,
                "fail_reason": None,
            },
        )


async def test_cron_rehydrates_history_on_restart(tmp_path):
    await _prepopulate_ledger(
        tmp_path,
        [
            "2026-07-01T00:00:00+00:00",
            "2026-07-02T00:00:00+00:00",
            "2026-07-03T00:00:00+00:00",
        ],
    )
    # a fresh process: same store, same job -> history warmed from the ledger.
    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    assert len(cron.run_history["j"]) == 3
    assert cron.last_run["j"].outcome == "success"
    assert (
        cron.last_run["j"].finished_at.isoformat()
        == "2026-07-03T00:00:00+00:00"
    )


async def test_rehydration_runs_once(tmp_path):
    await _prepopulate_ledger(tmp_path, ["2026-07-01T00:00:00+00:00"])
    cron = Cron(None, config_yaml=_ONE_JOB)
    cfg = _state_cfg("state:\n  path: " + str(tmp_path))
    await cron.start_stop_state(cfg)
    assert cron._state_rehydrated is True
    before = len(cron.run_history["j"])
    # a second housekeeping pass with the unchanged config keeps the same
    # backend and must NOT rehydrate again (which would duplicate history).
    await cron.start_stop_state(cfg)
    assert len(cron.run_history["j"]) == before


async def test_durable_last_run_at_watermark(tmp_path):
    await _prepopulate_ledger(
        tmp_path,
        ["2026-07-01T00:00:00+00:00", "2026-07-03T00:00:00+00:00"],
    )
    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    assert await cron.durable_last_run_at("j") == "2026-07-03T00:00:00+00:00"


async def test_durable_last_run_at_no_backend():
    cron = Cron(None, config_yaml=_ONE_JOB)
    assert await cron.durable_last_run_at("j") is None


# --- missed-run catch-up ----------------------------------------

_NOW = datetime.datetime(2026, 7, 1, 10, 10, 30, tzinfo=_UTC)


def _catchup_yaml(
    onmissed="run-all", deadline=None, jitter=0, sched="* * * * *"
):
    lines = [
        "jobs:",
        "  - name: j",
        "    command: 'true'",
        "    schedule: '" + sched + "'",
        "    onMissed: " + onmissed,
        "    catchupJitterSeconds: " + str(jitter),
    ]
    if deadline is not None:
        lines.append("    startingDeadlineSeconds: " + str(deadline))
    return "\n".join(lines) + "\n"


async def _cron_with_watermark(tmp_path, watermark_iso, **jobkw):
    cron = Cron(None, config_yaml=_catchup_yaml(**jobkw))
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    if watermark_iso is not None:
        await cron.state_backend.append_record(
            cron._run_stream("j"),
            {
                "outcome": "success",
                "exit_code": 0,
                "started_at": None,
                "finished_at": watermark_iso,
                "duration": None,
                "fail_reason": None,
            },
        )
    return cron


def _count_launcher():
    calls = []

    async def fake(job, *, with_retries=True):
        calls.append(job.name)
        return True

    return calls, fake


# --- config surface ---


def test_onmissed_defaults():
    cfg = parse_config_string(_ONE_JOB, "")
    j = cfg.jobs[0]
    assert j.onMissed == "skip"
    assert j.startingDeadlineSeconds is None
    assert j.catchupJitterSeconds == 0


def test_onmissed_custom_fields():
    cfg = parse_config_string(
        _catchup_yaml(onmissed="run-all", deadline=60, jitter=5), ""
    )
    j = cfg.jobs[0]
    assert j.onMissed == "run-all"
    assert j.startingDeadlineSeconds == 60
    assert j.catchupJitterSeconds == 5


def test_onmissed_invalid_rejected():
    with pytest.raises(ConfigError):
        parse_config_string(_catchup_yaml(onmissed="bogus"), "")


def test_starting_deadline_must_be_positive():
    with pytest.raises(ConfigError, match="startingDeadlineSeconds"):
        parse_config_string(_catchup_yaml(deadline=0), "")


def test_catchup_jitter_must_be_nonnegative():
    with pytest.raises(ConfigError, match="catchupJitterSeconds"):
        parse_config_string(_catchup_yaml(jitter=-1), "")


# --- _catchup_offset (pure) ---


def test_catchup_offset_zero_when_disabled():
    assert Cron._catchup_offset("j", 0) == 0.0


def test_catchup_offset_deterministic_and_in_range():
    off = Cron._catchup_offset("job-name", 10)
    assert 0.0 <= off < 10.0
    assert Cron._catchup_offset("job-name", 10) == off  # stable across calls


# --- _missed_occurrences ---


async def test_missed_zero_when_never_ran(tmp_path):
    cron = await _cron_with_watermark(tmp_path, None, onmissed="run-all")
    count, watermark = await cron._missed_occurrences(
        cron.cron_jobs["j"], _NOW
    )
    assert (count, watermark) == (0, None)


async def test_missed_zero_when_current(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:10:00+00:00", onmissed="run-all"
    )
    count, watermark = await cron._missed_occurrences(
        cron.cron_jobs["j"], _NOW
    )
    assert count == 0
    assert watermark == "2026-07-01T10:10:00+00:00"


async def test_missed_run_once_coalesces_to_one(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    count, watermark = await cron._missed_occurrences(
        cron.cron_jobs["j"], _NOW
    )
    assert count == 1
    assert watermark == "2026-07-01T10:00:00+00:00"


async def test_missed_run_all_counts_each(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    # per-minute job, 10:01..10:10 missed by 10:10:30 -> 10 occurrences.
    count, _ = await cron._missed_occurrences(cron.cron_jobs["j"], _NOW)
    assert count == 10


async def test_missed_deadline_bounds_window(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all", deadline=180
    )
    # only the last 180s (10:08, 10:09, 10:10) count.
    count, _ = await cron._missed_occurrences(cron.cron_jobs["j"], _NOW)
    assert count == 3


async def test_missed_hard_capped(tmp_path, caplog):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    far = datetime.datetime(2026, 7, 1, 12, 30, 0, tzinfo=_UTC)  # 150 min
    count, _ = await cron._missed_occurrences(cron.cron_jobs["j"], far)
    assert count == 100
    assert any("dropping the rest" in r.getMessage() for r in caplog.records)


async def test_missed_naive_watermark_is_pinned_to_utc(tmp_path):
    # a foreign/hand-written record with a NAIVE finished_at must not raise
    # TypeError out of the schedule arithmetic (which would crash the
    # scheduler at every boot): it is pinned to UTC instead.
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00", onmissed="run-once"
    )
    count, _ = await cron._missed_occurrences(cron.cron_jobs["j"], _NOW)
    assert count == 1


# --- _catch_up orchestration ---


async def test_catch_up_run_once_launches_once(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    await cron._catch_up(_NOW)
    await asyncio.gather(*list(cron._catchup_tasks))
    assert calls == ["j"]
    assert cron._caught_up is True


async def test_catch_up_run_all_launches_each(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    now = datetime.datetime(2026, 7, 1, 10, 3, 30, tzinfo=_UTC)  # 3 missed
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    await cron._catch_up(now)
    await asyncio.gather(*list(cron._catchup_tasks))
    assert calls == ["j", "j", "j"]


async def test_catch_up_runs_only_once(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    await cron._catch_up(_NOW)
    await asyncio.gather(*list(cron._catchup_tasks))
    await cron._catch_up(_NOW)  # second call is a no-op
    assert calls == ["j"]


async def test_catch_up_skip_schedules_nothing(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="skip"
    )
    await cron._catch_up(_NOW)
    assert cron._catchup_tasks == set()


async def test_catch_up_without_backend_warns(tmp_path, caplog):
    cron = Cron(None, config_yaml=_catchup_yaml(onmissed="run-all"))
    await cron._catch_up(_NOW)  # no state backend configured
    assert cron._catchup_tasks == set()
    assert any(
        "needs a" in r.getMessage() and "state" in r.getMessage()
        for r in caplog.records
    )


async def test_catch_up_respects_cluster_gate(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    cron._cluster_allows = lambda job: False  # type: ignore[method-assign]
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    await cron._catch_up(_NOW)
    await asyncio.gather(*list(cron._catchup_tasks))
    assert calls == []  # non-owner leaves the backfill to the owner


async def test_run_catch_up_bails_when_stopping(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    cron._stop_event.set()
    await cron._run_catch_up(cron.cron_jobs["j"], 5, 0.0, _NOW)
    assert calls == []


async def test_run_catch_up_waits_out_jitter(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    # a small non-zero offset exercises the interruptible jitter sleep before
    # the launch (the cross-job stagger path).
    await cron._run_catch_up(cron.cron_jobs["j"], 1, 0.02, _NOW)
    assert calls == ["j"]


async def test_run_catch_up_revalidates_after_jitter(tmp_path):
    # ownership moving (or the job being disabled/removed) during the jitter
    # sleep must abort the backfill: launching anyway would double-run it
    # alongside the new owner's own catch-up.
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    cron._cluster_allows = lambda job: False  # type: ignore[method-assign]
    await cron._run_catch_up(cron.cron_jobs["j"], 1, 0.0, _NOW)
    assert calls == []
    del cron.cron_jobs["j"]  # removed by a reload during the jitter
    cron._cluster_allows = lambda job: True  # type: ignore[method-assign]
    await cron._run_catch_up(
        parse_config_string(_catchup_yaml(onmissed="run-once"), "").jobs[0],
        1,
        0.0,
        _NOW,
    )
    assert calls == []


# --- depends_on_past (onlyIfLastSucceeded) -----------------------

_DEP_JOB = (
    "jobs:\n  - name: j\n    command: 'true'\n    schedule: '* * * * *'\n"
    "    onlyIfLastSucceeded: true\n"
)


async def _dep_cron(tmp_path):
    cron = Cron(None, config_yaml=_DEP_JOB)
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    return cron


async def _put_outcome(cron, outcome, ts):
    await cron.state_backend.append_record(
        cron._run_stream("j"), {"outcome": outcome, "finished_at": ts}
    )


def test_phase3_config_defaults():
    j = parse_config_string(_ONE_JOB, "").jobs[0]
    assert j.onlyIfLastSucceeded is False
    assert j.archiveOutput is False
    assert j.redactArchivedSecrets is True


def test_phase3_config_custom():
    j = parse_config_string(
        _archive_yaml(archive=True, redact=False), ""
    ).jobs[0]
    assert j.archiveOutput is True
    assert j.redactArchivedSecrets is False
    assert parse_config_string(_DEP_JOB, "").jobs[0].onlyIfLastSucceeded


async def test_depends_on_past_allows_without_flag(tmp_path):
    cron = Cron(None, config_yaml=_ONE_JOB)  # flag defaults off
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is True


async def test_depends_on_past_allows_without_backend():
    cron = Cron(None, config_yaml=_DEP_JOB)  # flag on, no state backend
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is True


async def test_depends_on_past_allows_first_run(tmp_path):
    cron = await _dep_cron(tmp_path)  # empty ledger
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is True


async def test_depends_on_past_blocks_after_failure(tmp_path):
    cron = await _dep_cron(tmp_path)
    await _put_outcome(cron, "failure", "2026-07-01T10:00:00+00:00")
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is False


async def test_depends_on_past_allows_after_success(tmp_path):
    cron = await _dep_cron(tmp_path)
    await _put_outcome(cron, "failure", "2026-07-01T10:00:00+00:00")
    await _put_outcome(cron, "success", "2026-07-01T10:05:00+00:00")
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is True


async def test_depends_on_past_ignores_cancelled(tmp_path):
    # a cancelled run after a failure does not itself re-open the gate.
    cron = await _dep_cron(tmp_path)
    await _put_outcome(cron, "failure", "2026-07-01T10:00:00+00:00")
    await _put_outcome(cron, "cancelled", "2026-07-01T10:05:00+00:00")
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is False


async def test_launch_scheduled_job_skips_on_depends_on_past(tmp_path, caplog):
    caplog.set_level(logging.INFO, logger="cronstable")
    cron = await _dep_cron(tmp_path)
    await _put_outcome(cron, "failure", "2026-07-01T10:00:00+00:00")
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    await cron.launch_scheduled_job(cron.cron_jobs["j"])
    assert calls == []
    assert any(
        "skipped: onlyIfLastSucceeded" in r.getMessage()
        for r in caplog.records
    )


# --- output/log archival with redaction -------------------------


def _archive_yaml(archive=True, redact=True):
    return (
        "jobs:\n  - name: j\n    command: 'true'\n    schedule: '* * * * *'\n"
        "    archiveOutput: " + ("true" if archive else "false") + "\n"
        "    redactArchivedSecrets: " + ("true" if redact else "false") + "\n"
    )


def _info_with_output(pairs):
    out = JobOutputStream()
    for stream_name, line in pairs:
        out.publish(stream_name, line)
    dt = datetime.datetime(2026, 7, 1, 10, 0, 0, tzinfo=_UTC)
    return JobRunInfo(
        outcome="success",
        exit_code=0,
        started_at=dt,
        finished_at=dt,
        fail_reason=None,
        output=out,
    )


async def test_archive_output_writes_redacted(tmp_path):
    cron = Cron(None, config_yaml=_archive_yaml(archive=True, redact=True))
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    cron._record_run(
        "j",
        _info_with_output(
            [("stdout", "password=hunter2"), ("stderr", "normal line")]
        ),
    )
    await _drain_state_writes(cron)
    logs = await cron.state_backend.list_records(cron._log_stream("j"))
    assert len(logs) == 1
    assert logs[0]["redacted"] is True
    lines = logs[0]["lines"]
    assert lines[0]["line"] == "password=***REDACTED***"
    assert lines[1]["line"] == "normal line"


async def test_archive_output_without_redaction(tmp_path):
    cron = Cron(None, config_yaml=_archive_yaml(archive=True, redact=False))
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    cron._record_run("j", _info_with_output([("stdout", "password=hunter2")]))
    await _drain_state_writes(cron)
    logs = await cron.state_backend.list_records(cron._log_stream("j"))
    assert logs[0]["redacted"] is False
    assert logs[0]["lines"][0]["line"] == "password=hunter2"


async def test_no_archive_without_flag(tmp_path):
    cron = Cron(None, config_yaml=_ONE_JOB)  # archiveOutput defaults off
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    cron._record_run("j", _info_with_output([("stdout", "x")]))
    await _drain_state_writes(cron)
    assert await cron.state_backend.list_records(cron._log_stream("j")) == []


async def test_archive_output_pruned_to_max(tmp_path):
    cron = Cron(None, config_yaml=_archive_yaml(archive=True))
    cfg = _state_cfg(
        "state:\n  path: " + str(tmp_path) + "\n  maxRunsPerJob: 2\n"
    )
    await cron.start_stop_state(cfg)
    for i in range(4):
        cron._record_run("j", _info_with_output([("stdout", "run %d" % i)]))
        await _drain_state_writes(cron)
    logs = await cron.state_backend.list_records(cron._log_stream("j"))
    assert len(logs) == 2
