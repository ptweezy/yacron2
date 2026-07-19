"""State as a first-class job primitive.

Two layers are exercised here:

* the backend primitives it all rests on -- the mutable *document*
  store (one file per key, atomic-rename rewrite under an advisory flock) and
  the content-addressed *blob* store -- driven straight against a real temp
  directory through :func:`tests.test_state._backend`;
* the pure logic layer in :mod:`cronstable.jobstate` (KV, monotonic cursor,
  create-if-absent idempotency, named artifacts) over that backend.

Style matches the other state test files: no frozen clock, bare ``async def``
tests (``asyncio_mode = auto``), module seams monkeypatched rather than time
asserted.  Server/CLI wiring lives in test_state_job_api.py.
"""

import os

import pytest

from cronstable import jobstate, state
from cronstable.config import ConfigError, parse_config, parse_config_string
from cronstable.jobstate import JobStateError
from cronstable.state import DOC_DELETE, DOC_KEEP
from tests.test_state import _backend


def _cfg(yaml):
    return parse_config_string(yaml, "")


def _break_record_reads(monkeypatch, backend, stream):
    """Make every record file of ``stream`` transiently unreadable."""
    stream_dir = backend._stream_dir(stream)
    victims = {
        os.path.normpath(os.path.join(stream_dir, n))
        for n in os.listdir(stream_dir)
        if n.endswith(".json")
    }
    assert victims
    real_open = open

    def flaky_open(path, *args, **kwargs):
        if os.path.normpath(str(path)) in victims:
            raise PermissionError(13, "transient hold", str(path))
        return real_open(path, *args, **kwargs)

    # shadow the module's open, the seam tests/test_state.py already patches.
    monkeypatch.setattr(state, "open", flaky_open, raising=False)


# --------------------------------------------------------------------------
# Config: state.jobApi + per-job secrets
# --------------------------------------------------------------------------


def test_jobapi_defaults_filled():
    cfg = _cfg("state:\n  path: /x\n").state_config
    api = cfg["jobApi"]
    assert api["enabled"] is True
    assert api["listen"] is None
    assert api["maxValueBytes"] == 1024 * 1024
    assert api["lockTtlSeconds"] == 30


def test_jobapi_partial_override_keeps_other_defaults():
    cfg = _cfg(
        "state:\n  path: /x\n  jobApi:\n    enabled: false\n"
    ).state_config
    api = cfg["jobApi"]
    assert api["enabled"] is False
    # the untouched keys survive the partial block.
    assert api["maxValueBytes"] == 1024 * 1024
    assert api["lockTtlSeconds"] == 30


def test_jobapi_lock_ttl_floor():
    with pytest.raises(ConfigError, match="lockTtlSeconds must be >= 5"):
        _cfg("state:\n  path: /x\n  jobApi:\n    lockTtlSeconds: 2\n")


def test_jobapi_listen_rejects_unix():
    with pytest.raises(ConfigError, match="must be an http:// URL"):
        _cfg(
            "state:\n  path: /x\n  jobApi:\n"
            "    listen: unix:///run/y.sock\n"
        )


def test_jobapi_listen_loopback_bind_allowed_by_default():
    for host in ("127.0.0.1:9000", "'[::1]:9000'", "localhost:9000"):
        cfg = _cfg(
            "state:\n  path: /x\n  jobApi:\n    listen: {}\n".format(host)
        ).state_config
        assert cfg["jobApi"]["listen"] == host.strip("'")


def test_jobapi_listen_non_loopback_bind_refused():
    with pytest.raises(ConfigError, match="is not loopback"):
        _cfg("state:\n  path: /x\n  jobApi:\n    listen: 0.0.0.0:9000\n")


def test_jobapi_listen_non_loopback_bind_allowed_with_explicit_opt_in():
    cfg = _cfg(
        "state:\n  path: /x\n  jobApi:\n"
        "    listen: 0.0.0.0:9000\n"
        "    allowNonLoopbackBind: true\n"
    ).state_config
    assert cfg["jobApi"]["listen"] == "0.0.0.0:9000"


def test_secrets_parsed():
    cfg = _cfg(
        "state:\n  path: /x\n"
        "jobs:\n  - name: j\n    command: 'true'\n"
        "    schedule: '* * * * *'\n"
        "    secrets:\n"
        "      - name: TOKEN\n        value: hunter2\n"
    )
    (job,) = cfg.jobs
    assert job.secrets == [{"name": "TOKEN", "value": "hunter2"}]


def test_state_allowed_scopes_default_empty():
    cfg = _cfg(
        "state:\n  path: /x\n"
        "jobs:\n  - name: j\n    command: 'true'\n"
        "    schedule: '* * * * *'\n"
    )
    (job,) = cfg.jobs
    assert job.stateAllowedScopes == []


def test_state_allowed_scopes_parsed():
    cfg = _cfg(
        "state:\n  path: /x\n"
        "jobs:\n  - name: j\n    command: 'true'\n"
        "    schedule: '* * * * *'\n"
        "    stateAllowedScopes:\n      - shared-team\n"
    )
    (job,) = cfg.jobs
    assert job.stateAllowedScopes == ["shared-team"]


def test_secret_missing_source_rejected():
    with pytest.raises(ConfigError, match="needs a value, fromFile"):
        _cfg(
            "state:\n  path: /x\n"
            "jobs:\n  - name: j\n    command: 'true'\n"
            "    schedule: '* * * * *'\n"
            "    secrets:\n      - name: TOKEN\n"
        )


def test_secret_duplicate_name_last_wins():
    # like environment variables, a same-named secret merges to last-wins
    # rather than staging two secrets under one name.
    cfg = _cfg(
        "state:\n  path: /x\n"
        "jobs:\n  - name: j\n    command: 'true'\n"
        "    schedule: '* * * * *'\n"
        "    secrets:\n"
        "      - name: T\n        value: a\n"
        "      - name: T\n        value: b\n"
    )
    (job,) = cfg.jobs
    assert job.secrets == [{"name": "T", "value": "b"}]


def test_secrets_require_state_section(tmp_path):
    path = tmp_path / "c.yaml"
    path.write_text(
        "jobs:\n  - name: j\n    command: 'true'\n"
        "    schedule: '* * * * *'\n"
        "    secrets:\n      - name: T\n        value: a\n"
    )
    with pytest.raises(ConfigError, match="requires a `state` section"):
        parse_config(str(path))


def test_secrets_require_jobapi_enabled(tmp_path):
    path = tmp_path / "c.yaml"
    path.write_text(
        "state:\n  path: " + str(tmp_path) + "\n"
        "  jobApi:\n    enabled: false\n"
        "jobs:\n  - name: j\n    command: 'true'\n"
        "    schedule: '* * * * *'\n"
        "    secrets:\n      - name: T\n        value: a\n"
    )
    with pytest.raises(ConfigError, match="jobApi.enabled is false"):
        parse_config(str(path))


def test_secrets_merge_from_defaults():
    cfg = _cfg(
        "defaults:\n"
        "  secrets:\n      - name: SHARED\n        value: base\n"
        "state:\n  path: /x\n"
        "jobs:\n  - name: j\n    command: 'true'\n"
        "    schedule: '* * * * *'\n"
        "    secrets:\n      - name: OWN\n        value: mine\n"
    )
    (job,) = cfg.jobs
    names = sorted(s["name"] for s in job.secrets)
    assert names == ["OWN", "SHARED"]


# --------------------------------------------------------------------------
# Backend: mutable document store
# --------------------------------------------------------------------------


async def test_document_absent_reads_none(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    assert await backend.read_document("ns", "missing") is None
    assert await backend.list_documents("ns") == []
    await backend.stop()


async def test_document_write_read_roundtrip(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()

    def _put(current):
        return {"key": "k", "value": {"a": 1}}, "created"

    stored, result = await backend.mutate_document("ns", "k", _put)
    assert result == "created"
    assert stored == {"key": "k", "value": {"a": 1}}
    assert await backend.read_document("ns", "k") == {
        "key": "k",
        "value": {"a": 1},
    }
    await backend.stop()


async def test_document_keep_leaves_value(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await backend.mutate_document("ns", "k", lambda c: ({"value": 1}, None))
    stored, res = await backend.mutate_document(
        "ns", "k", lambda c: (DOC_KEEP, "unchanged")
    )
    assert res == "unchanged"
    assert stored == {"value": 1}
    assert await backend.read_document("ns", "k") == {"value": 1}
    await backend.stop()


async def test_document_delete(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await backend.mutate_document("ns", "k", lambda c: ({"value": 1}, None))
    assert await backend.delete_document("ns", "k") is True
    assert await backend.read_document("ns", "k") is None
    # deleting an absent document reports it did not exist.
    assert await backend.delete_document("ns", "k") is False
    await backend.stop()


async def test_document_delete_via_sentinel(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await backend.mutate_document("ns", "k", lambda c: ({"value": 1}, None))
    stored, res = await backend.mutate_document(
        "ns", "k", lambda c: (DOC_DELETE, "gone")
    )
    assert stored is None
    assert res == "gone"
    assert await backend.read_document("ns", "k") is None
    await backend.stop()


async def test_document_transform_sees_current(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await backend.mutate_document(
        "ns", "counter", lambda c: ({"value": 0}, None)
    )

    def _incr(current):
        n = current["value"] if current else 0
        return {"value": n + 1}, n + 1

    for expected in (1, 2, 3):
        _stored, res = await backend.mutate_document("ns", "counter", _incr)
        assert res == expected
    assert (await backend.read_document("ns", "counter"))["value"] == 3
    await backend.stop()


async def test_document_list_returns_bodies(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    for key in ("a", "b", "c"):
        await backend.mutate_document(
            "ns", key, lambda c, k=key: ({"key": k, "value": k}, None)
        )
    bodies = await backend.list_documents("ns")
    assert sorted(b["key"] for b in bodies) == ["a", "b", "c"]
    await backend.stop()


async def test_document_namespaces_isolated(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await backend.mutate_document("ns1", "k", lambda c: ({"value": 1}, None))
    await backend.mutate_document("ns2", "k", lambda c: ({"value": 2}, None))
    assert (await backend.read_document("ns1", "k"))["value"] == 1
    assert (await backend.read_document("ns2", "k"))["value"] == 2
    assert len(await backend.list_documents("ns1")) == 1
    await backend.stop()


async def test_document_survives_restart(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await backend.mutate_document(
        "ns", "k", lambda c: ({"value": "durable"}, None)
    )
    await backend.stop()

    backend2 = _backend(tmp_path)
    await backend2.start()
    assert (await backend2.read_document("ns", "k"))["value"] == "durable"
    await backend2.stop()


async def test_document_weird_key_is_filename_safe(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    # slashes, spaces, unicode, path traversal attempts: all injective-safe.
    for key in ("a/b/c", "../escape", "with space", "uniçode", "CON"):
        await backend.mutate_document(
            "ns", key, lambda c, k=key: ({"key": k, "value": 1}, None)
        )
        assert await backend.read_document("ns", key) is not None
    # nothing escaped the docs namespace directory.
    ns_dir = os.path.join(backend.base, "docs")
    assert os.path.isdir(ns_dir)
    await backend.stop()


async def test_document_corrupt_reads_none_but_mutate_fails(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await backend.mutate_document("ns", "k", lambda c: ({"value": 1}, None))
    _lock, doc_path = backend._doc_paths("ns", "k")
    with open(doc_path, "wb") as fobj:
        fobj.write(b"{ this is not valid json")
    # best-effort read swallows the corruption...
    assert await backend.read_document("ns", "k") is None
    # ...but a read-modify-write fails closed rather than clobbering it.
    with pytest.raises(state._DocumentUnreadable):
        await backend.mutate_document(
            "ns", "k", lambda c: ({"value": 2}, None)
        )
    await backend.stop()


# --------------------------------------------------------------------------
# Backend: content-addressed blob store
# --------------------------------------------------------------------------


async def test_blob_put_get_roundtrip(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    digest = await backend.put_blob(b"hello world")
    assert len(digest) == 64  # sha-256 hex
    assert await backend.get_blob(digest) == b"hello world"
    await backend.stop()


async def test_blob_is_content_addressed_and_deduped(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    d1 = await backend.put_blob(b"same")
    d2 = await backend.put_blob(b"same")
    assert d1 == d2
    # only one file on disk for identical content.
    shard = os.path.join(backend.base, "blobs", d1[:2])
    assert os.listdir(shard) == [d1 + ".blob"]
    await backend.stop()


async def test_blob_missing_returns_none(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    assert await backend.get_blob("0" * 64) is None
    await backend.stop()


async def test_blob_survives_restart(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    digest = await backend.put_blob(b"persisted bytes")
    await backend.stop()

    backend2 = _backend(tmp_path)
    await backend2.start()
    assert await backend2.get_blob(digest) == b"persisted bytes"
    await backend2.stop()


# --------------------------------------------------------------------------
# Logic layer: durable key/value
# --------------------------------------------------------------------------


async def test_kv_set_get_delete(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    assert await jobstate.kv_get(backend, "job-a", "greeting") is None
    await jobstate.kv_set(backend, "job-a", "greeting", "hello")
    body = await jobstate.kv_get(backend, "job-a", "greeting")
    assert body["value"] == "hello"
    assert await jobstate.kv_delete(backend, "job-a", "greeting") is True
    assert await jobstate.kv_get(backend, "job-a", "greeting") is None
    await backend.stop()


async def test_kv_null_value_distinct_from_absent(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await jobstate.kv_set(backend, "job-a", "k", None)
    body = await jobstate.kv_get(backend, "job-a", "k")
    assert body is not None and body["value"] is None
    await backend.stop()


async def test_kv_scopes_isolated(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await jobstate.kv_set(backend, "job-a", "k", "a")
    await jobstate.kv_set(backend, "job-b", "k", "b")
    assert (await jobstate.kv_get(backend, "job-a", "k"))["value"] == "a"
    assert (await jobstate.kv_get(backend, "job-b", "k"))["value"] == "b"
    keys_a = [b["key"] for b in await jobstate.kv_list(backend, "job-a")]
    assert keys_a == ["k"]
    await backend.stop()


async def test_kv_size_limit(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    with pytest.raises(JobStateError) as ei:
        await jobstate.kv_set(backend, "job-a", "k", "x" * 100, max_bytes=10)
    assert ei.value.status == 413
    await backend.stop()


async def test_kv_rejects_non_portable_values(tmp_path):
    # H9/M26: a NaN/Infinity float or a >64-bit int is written differently (or
    # unreadably) with vs. without orjson, so on a mixed fleet it corrupts the
    # store.  It must be refused at the boundary with a clean 400, on every
    # node, BEFORE any write -- not accepted here and unreadable elsewhere.
    backend = _backend(tmp_path)
    await backend.start()
    for bad in (float("inf"), float("nan"), 2**64, {"x": float("-inf")}):
        with pytest.raises(JobStateError) as ei:
            await jobstate.kv_set(backend, "job-a", "k", bad)
        assert ei.value.status == 400
    # the rejected keys were never written -- no unreadable document remains.
    assert await jobstate.kv_list(backend, "job-a") == []
    # a portable value still round-trips.
    await jobstate.kv_set(backend, "job-a", "k", {"n": 2**63 - 1})
    got = await jobstate.kv_get(backend, "job-a", "k")
    assert got["value"] == {"n": 2**63 - 1}
    await backend.stop()


async def test_cursor_rejects_non_portable_value(tmp_path):
    # the exact reported vector: `cursor advance wm 1e400` (-> inf) on a
    # non-orjson node would persist `{"value":Infinity}` that orjson nodes
    # cannot read, wedging the cursor.  Refuse it up front.
    backend = _backend(tmp_path)
    await backend.start()
    with pytest.raises(JobStateError) as ei:
        await jobstate.cursor_advance(backend, "etl", "wm", float("1e400"))
    assert ei.value.status == 400
    # the cursor was never created, so it still reads as unset (not wedged).
    assert await jobstate.cursor_get(backend, "etl", "wm") is None
    await backend.stop()


async def test_kv_list_sorted(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    for key in ("charlie", "alpha", "bravo"):
        await jobstate.kv_set(backend, "s", key, key)
    keys = [b["key"] for b in await jobstate.kv_list(backend, "s")]
    assert keys == ["alpha", "bravo", "charlie"]
    await backend.stop()


# --------------------------------------------------------------------------
# Logic layer: incremental cursor / watermark
# --------------------------------------------------------------------------


async def test_cursor_monotonic_advance(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    r1 = await jobstate.cursor_advance(backend, "etl", "wm", 100)
    assert r1 == {"value": 100, "advanced": True}
    r2 = await jobstate.cursor_advance(backend, "etl", "wm", 200)
    assert r2 == {"value": 200, "advanced": True}
    # a lower value never regresses the watermark.
    r3 = await jobstate.cursor_advance(backend, "etl", "wm", 150)
    assert r3 == {"value": 200, "advanced": False}
    assert (await jobstate.cursor_get(backend, "etl", "wm"))["value"] == 200
    await backend.stop()


async def test_cursor_iso_string_watermark(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await jobstate.cursor_advance(backend, "etl", "ts", "2026-07-01T00:00:00")
    r = await jobstate.cursor_advance(
        backend, "etl", "ts", "2026-06-01T00:00:00"
    )
    # ISO-8601 sorts lexicographically == chronologically, so the older
    # timestamp does not advance the cursor.
    assert r["advanced"] is False
    assert (
        await jobstate.cursor_get(backend, "etl", "ts")
    )["value"] == "2026-07-01T00:00:00"
    await backend.stop()


async def test_cursor_force_rewind(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await jobstate.cursor_advance(backend, "etl", "wm", 500)
    r = await jobstate.cursor_advance(backend, "etl", "wm", 10, force=True)
    assert r == {"value": 10, "advanced": True}
    await backend.stop()


async def test_cursor_type_clash_rejected(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await jobstate.cursor_advance(backend, "etl", "wm", 100)
    with pytest.raises(JobStateError) as ei:
        await jobstate.cursor_advance(backend, "etl", "wm", "not-a-number")
    assert ei.value.status == 409
    await backend.stop()


# --------------------------------------------------------------------------
# Logic layer: idempotency keys
# --------------------------------------------------------------------------


async def test_idempotency_first_claim_fresh(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    assert (await jobstate.idempotency_claim(backend, "s", "order-1"))["fresh"]
    # a second claim of the same key is not fresh.
    assert not (
        await jobstate.idempotency_claim(backend, "s", "order-1")
    )["fresh"]
    await backend.stop()


async def test_idempotency_ttl_reclaim(tmp_path, monkeypatch):
    backend = _backend(tmp_path)
    await backend.start()
    clock = {"t": 1000.0}
    monkeypatch.setattr(jobstate, "_now", lambda: clock["t"])
    assert (
        await jobstate.idempotency_claim(backend, "s", "k", ttl=30)
    )["fresh"]
    # still within the window: not fresh.
    clock["t"] = 1020.0
    assert not (
        await jobstate.idempotency_claim(backend, "s", "k", ttl=30)
    )["fresh"]
    # past the window: re-winnable.
    clock["t"] = 1040.0
    assert (
        await jobstate.idempotency_claim(backend, "s", "k", ttl=30)
    )["fresh"]
    await backend.stop()


async def test_idempotency_release(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await jobstate.idempotency_claim(backend, "s", "k")
    assert await jobstate.idempotency_release(backend, "s", "k") is True
    assert (await jobstate.idempotency_claim(backend, "s", "k"))["fresh"]
    await backend.stop()


# --------------------------------------------------------------------------
# Logic layer: named artifact store
# --------------------------------------------------------------------------


async def test_artifact_put_get(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    rec = await jobstate.artifact_put(backend, "s", "report.csv", b"a,b,c\n")
    assert rec["size"] == 6
    got = await jobstate.artifact_get(backend, "s", "report.csv")
    assert got is not None
    record, data = got
    assert data == b"a,b,c\n"
    assert record["sha256"] == rec["sha256"]
    await backend.stop()


async def test_artifact_newest_version_wins(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await jobstate.artifact_put(backend, "s", "x", b"v1")
    await jobstate.artifact_put(backend, "s", "x", b"v2")
    _rec, data = await jobstate.artifact_get(backend, "s", "x")
    assert data == b"v2"
    await backend.stop()


async def test_artifact_list_newest_per_name(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    await jobstate.artifact_put(backend, "s", "a", b"1")
    await jobstate.artifact_put(backend, "s", "b", b"2")
    await jobstate.artifact_put(backend, "s", "a", b"3")
    listing = await jobstate.artifact_list(backend, "s")
    assert [r["name"] for r in listing] == ["a", "b"]
    await backend.stop()


async def test_artifact_put_prunes_superseded_same_name_records(tmp_path):
    # Republishing one name must not grow the stream without bound: the name-
    # keyed prune drops superseded records, keeping only the newest per name,
    # while reads still return the latest value.
    backend = _backend(tmp_path)
    await backend.start()
    try:
        for i in range(30):
            await jobstate.artifact_put(
                backend, "s", "rolling", str(i).encode()
            )
        _rec, data = await jobstate.artifact_get(backend, "s", "rolling")
        assert data == b"29"  # newest value survives
        raw = await backend.list_records(
            jobstate.ARTIFACT_STREAM_PREFIX + "s"
        )
        # amortised prune bounds the stream near the distinct-name count (1),
        # far below the 30 publishes (slack of at most the prune cadence).
        assert len(raw) <= state._PRUNE_EVERY_APPENDS
        listing = await jobstate.artifact_list(backend, "s")
        assert [r["name"] for r in listing] == ["rolling"]
    finally:
        await backend.stop()


async def test_artifact_prune_keeps_every_live_name(tmp_path):
    # The safety property a blind newest-N prune would violate: with more
    # distinct names than the prune cadence, every name's latest must survive.
    # (A newest-N record prune would evict live names once their count exceeds
    # N, then the orphan-blob sweep would delete still-referenced blobs.)
    backend = _backend(tmp_path)
    await backend.start()
    try:
        names = ["n{}".format(i) for i in range(20)]  # > _PRUNE_EVERY_APPENDS
        for nm in names:
            await jobstate.artifact_put(backend, "s", nm, b"v1")
        for nm in names:  # republish each, triggering prunes along the way
            await jobstate.artifact_put(
                backend, "s", nm, (nm + "-v2").encode()
            )
        listing = await jobstate.artifact_list(backend, "s")
        assert sorted(r["name"] for r in listing) == sorted(names)
        for nm in names:
            got = await jobstate.artifact_get(backend, "s", nm)
            assert got is not None and got[1] == (nm + "-v2").encode()
    finally:
        await backend.stop()


async def test_artifact_missing_returns_none(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    assert await jobstate.artifact_get(backend, "s", "nope") is None
    await backend.stop()


async def test_artifact_get_strict_propagates_a_transient_read_error(
    tmp_path, monkeypatch
):
    # Non-strict, an unreadable record is SKIPPED -- so a published artifact
    # reads back as never published. That silent lie is fatal wherever absence
    # is acted on as PERMANENT: the DAG mapped expansion records the empty
    # fan-out once and never recomputes it, so one NFS blip silently skips the
    # whole task's work. strict=True must surface the error instead, exactly
    # as referenced_blob_digests(strict=True) already does for the blob sweep.
    backend = _backend(tmp_path)
    await backend.start()
    await jobstate.artifact_put(backend, "s", "items", b'["a"]')
    _break_record_reads(
        monkeypatch, backend, jobstate.ARTIFACT_STREAM_PREFIX + "s"
    )

    # best-effort: indistinguishable from "never published" (the bug)
    assert await jobstate.artifact_get_record(backend, "s", "items") is None
    assert await jobstate.artifact_get(backend, "s", "items") is None
    # strict: the environment's failure is the caller's to see and retry
    with pytest.raises(OSError):
        await jobstate.artifact_get_record(backend, "s", "items", strict=True)
    with pytest.raises(OSError):
        await jobstate.artifact_get(backend, "s", "items", strict=True)

    # and once the blip clears the record reads back intact, proving strict
    # only ever reported the read, never damaged the stream.
    monkeypatch.undo()
    rec = await jobstate.artifact_get_record(backend, "s", "items", strict=True)
    assert rec["name"] == "items"
    got = await jobstate.artifact_get(backend, "s", "items", strict=True)
    assert got is not None and got[1] == b'["a"]'
    await backend.stop()


async def test_artifact_size_limit(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    with pytest.raises(JobStateError) as ei:
        await jobstate.artifact_put(backend, "s", "big", b"x" * 100,
                                    max_bytes=10)
    assert ei.value.status == 413
    await backend.stop()


async def test_artifact_orphan_blob_swept_after_reference_gone(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    digest = (await jobstate.artifact_put(backend, "s", "x", b"payload"))[
        "sha256"
    ]
    # referenced by a surviving record: not swept even at grace 0.
    assert await backend.sweep_orphan_blobs({digest}, 0.0) == 0
    assert await backend.get_blob(digest) is not None
    # unreferenced (its scope was collected) and old enough: swept.
    assert await backend.sweep_orphan_blobs(set(), 0.0) == 1
    assert await backend.get_blob(digest) is None
    await backend.stop()


async def test_referenced_blob_digests(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    d1 = (await jobstate.artifact_put(backend, "s1", "a", b"one"))["sha256"]
    d2 = (await jobstate.artifact_put(backend, "s2", "b", b"two"))["sha256"]
    refs = await jobstate.referenced_blob_digests(backend, ["s1", "s2"])
    assert refs == {d1, d2}
    await backend.stop()


# --------------------------------------------------------------------------
# Logic layer: scope guard and artifact edge branches
# --------------------------------------------------------------------------


async def test_require_scope_rejects_blank(tmp_path):
    # a blank (or whitespace-only) scope strips to empty and is refused before
    # any store work -- _require_scope's guard branch.
    backend = _backend(tmp_path)
    await backend.start()
    try:
        with pytest.raises(JobStateError, match="non-empty scope"):
            await jobstate.kv_get(backend, "   ", "k")
    finally:
        await backend.stop()


async def test_artifact_put_stores_optional_meta(tmp_path):
    # the optional meta mapping rides along on the record and reads back.
    backend = _backend(tmp_path)
    await backend.start()
    try:
        rec = await jobstate.artifact_put(
            backend, "s", "r.csv", b"a,b\n", meta={"kind": "csv"}
        )
        assert rec["meta"] == {"kind": "csv"}
        got = await jobstate.artifact_get(backend, "s", "r.csv")
        assert got is not None
        record, data = got
        assert record["meta"] == {"kind": "csv"}
        assert data == b"a,b\n"
    finally:
        await backend.stop()


async def test_artifact_get_record_absent_returns_none(tmp_path):
    # a name never published reads back as None on both the record and the
    # (record, payload) accessor -- the exhausted-stream branch.
    backend = _backend(tmp_path)
    await backend.start()
    try:
        assert await jobstate.artifact_get_record(backend, "s", "nope") is None
        assert await jobstate.artifact_get(backend, "s", "nope") is None
    finally:
        await backend.stop()


async def test_artifact_get_raises_410_when_blob_swept(tmp_path):
    # the record survives but its content-addressed blob was garbage-collected:
    # artifact_get must fail closed with a 410, not return empty bytes.
    backend = _backend(tmp_path)
    await backend.start()
    try:
        await jobstate.artifact_put(backend, "s", "x", b"payload")
        # sweep with an empty referenced set: nothing is referenced, so the
        # blob is reclaimed while its record remains in the stream.
        assert await backend.sweep_orphan_blobs(set(), 0.0) == 1
        with pytest.raises(JobStateError) as ei:
            await jobstate.artifact_get(backend, "s", "x")
        assert ei.value.status == 410
    finally:
        await backend.stop()
