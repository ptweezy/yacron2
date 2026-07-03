"""The optional durable state backend: config, records, cursor, lease, probe.

Exercises :mod:`yacron2.state` and its config/lifecycle wiring end to end
against a real temp directory (the "local filesystem == Amazon S3 Files, one
backend" path), with the topology probe and clock stubbed where a test needs to
drive them deterministically.
"""

import asyncio
import datetime
import json
import os

import pytest

from yacron2 import state
from yacron2.config import ConfigError, parse_config_string
from yacron2.cron import Cron, JobRunInfo, _job_run_info_from_dict
from yacron2.job import JobOutputStream
from yacron2.state import (
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
    cfg = _state_cfg("state:\n  path: /var/lib/yacron2\n")
    assert cfg is not None
    assert cfg["path"] == "/var/lib/yacron2"
    assert cfg["topology"] == "auto"
    assert cfg["deploymentId"] is None


def test_state_all_fields():
    cfg = _state_cfg(
        "state:\n"
        "  path: /mnt/s3files/yacron2\n"
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
    from yacron2.config import parse_config_file

    with pytest.raises(ConfigError, match="multiple state configs"):
        parse_config_file(str(parent))


def test_state_section_from_config_dir(tmp_path):
    # a config directory: the `state` section is picked up from whichever file
    # carries it (the multi-file merge path in _parse_config_dir).
    (tmp_path / "10-jobs.yaml").write_text(
        "jobs:\n  - name: j\n    command: 'true'\n    schedule: '* * * * *'\n"
    )
    (tmp_path / "20-state.yaml").write_text("state:\n  path: /srv/state\n")
    from yacron2.config import parse_config

    cfg = parse_config(str(tmp_path))
    assert cfg.state_config is not None
    assert cfg.state_config["path"] == "/srv/state"


def test_multiple_state_sections_in_dir_rejected(tmp_path):
    (tmp_path / "a.yaml").write_text("state:\n  path: /a\n")
    (tmp_path / "b.yaml").write_text("state:\n  path: /b\n")
    from yacron2.config import parse_config

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


async def test_unknown_schema_version_is_quarantined(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    stream_dir = backend._stream_dir("s")
    os.makedirs(stream_dir, exist_ok=True)
    with open(os.path.join(stream_dir, "00001-old.json"), "w") as fobj:
        json.dump({"schemaVersion": "v0", "data": {"x": 1}}, fobj)
    assert await backend.list_records("s") == []
    assert "00001-old.json" not in os.listdir(stream_dir)


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
    assert _fs_safe("plain-name.1") == "plain-name.1"
    # path separators / spaces / unicode are percent-encoded, not dropped.
    assert "/" not in _fs_safe("a/b c")
    assert _fs_safe("a/b") != _fs_safe("a-b")  # no collision
    assert _fs_safe("") == "_"


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


# --- Phase 1: retention / pruning ----------------------------------------


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


# --- Phase 1: config retention knob --------------------------------------


def test_state_max_runs_default():
    assert _state_cfg("state:\n  path: /x\n")["maxRunsPerJob"] == 1000


def test_state_max_runs_custom():
    cfg = _state_cfg("state:\n  path: /x\n  maxRunsPerJob: 5\n")
    assert cfg["maxRunsPerJob"] == 5


# --- Phase 1: JobRunInfo reconstruction ----------------------------------


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


# --- Phase 1: Cron durable run ledger ------------------------------------


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


# --- Phase 1: warm-dashboard rehydration + watermark ---------------------


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
