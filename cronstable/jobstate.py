"""The job-facing state primitives, as pure functions over a backend.

This module is the *logic* half of "state as a first-class job primitive": it
turns the small, general surface of a :class:`cronstable.state.StateBackend`
(mutable documents, content-addressed blobs, append-only records, TTL leases)
into the six primitives a job command actually reaches for:

* **durable key/value** -- per-job (or shared) restart-surviving settings;
* **incremental cursor / watermark** -- a monotonic marker an ETL job advances
  and never sees regress, even when several nodes advance it at once;
* **idempotency keys** -- a fleet-wide create-if-absent claim so a retried or
  duplicated run can tell "already did this" from "first time";
* **named artifact store** -- small blobs published under a name and read back
  by later runs or peer nodes (the cross-task hand-off DAGs build on).

The mutex/semaphore and run-scoped secrets are *not* here: those need the live
daemon (a lease held and renewed on the run's behalf, secrets staged in
memory) and live in :mod:`cronstable.jobapi`.

Everything here is a plain ``async`` function taking the backend and a
``scope`` string, with no aiohttp and no CLI: the loopback server
(:mod:`cronstable.jobapi`) and the offline CLI (:mod:`cronstable.jobcli`) are
two thin front-ends over the identical logic, and the unit tests drive it
directly.  A ``scope`` is the isolation boundary -- by default a job's own
name, so ``kv set`` in job A cannot read job B's keys; callers pass a shared
scope (conventionally ``"global"``) to opt into cross-job sharing.

Errors a caller (a job) can provoke -- an oversized value, a cursor type
clash -- are raised as :class:`JobStateError` carrying the HTTP-ish status the
loopback server should answer with; everything else (a dead store) propagates
as the backend's own exception.
"""

import time
from typing import Any, Dict, List, Optional, Tuple

from cronstable import _json
from cronstable.state import DOC_KEEP, StateBackend

# Document-namespace prefixes (under the backend's ``docs/`` tree) and the
# artifact records-stream prefix (under ``records/``).  Exported so the
# scheduler's garbage collector can keep artifact streams of live scopes (KV /
# cursor / idempotency documents live under ``docs/`` and are never swept:
# they are durable state by definition).
KV_NS_PREFIX = "kv/"
CURSOR_NS_PREFIX = "cursor/"
IDEM_NS_PREFIX = "idem/"
ARTIFACT_STREAM_PREFIX = "artifacts/"

# The shared scope name jobs use to opt into cross-job state; only a naming
# convention (any scope string works), surfaced here so the server and CLI
# agree on the default.
GLOBAL_SCOPE = "global"


class JobStateError(Exception):
    """A caller-provoked failure, carrying the status the API should return.

    ``status`` mirrors HTTP so :mod:`cronstable.jobapi` can answer with it
    directly (400 bad request, 409 conflict, 413 too large, 404 not found);
    the CLI just prints the message and exits non-zero.  It is deliberately
    distinct from the backend's own exceptions (a dead store), which are
    *not* the job's fault and surface as 503 upstream.
    """

    def __init__(self, message: str, *, status: int = 400) -> None:
        super().__init__(message)
        self.status = status


def _now() -> float:
    """Wall-clock epoch seconds for record/document timestamps.

    A separate seam from :func:`cronstable.state._now` (the store's lease
    clock)
    so tests can drive idempotency-key expiry here without touching lease
    timing; both are plain ``time.time`` in production.
    """
    return time.time()


def _require_scope(scope: str) -> str:
    """Validate ``scope`` WITHOUT normalizing it.

    A scope is the isolation boundary, so distinct scope strings must never
    collapse onto one namespace.  This used to ``strip()`` the scope, which
    quietly merged ``'report'``, ``'report '`` and ``'report\\xa0'`` into ONE
    namespace AFTER jobapi authorized the raw string and BEFORE the store's
    injective encoder named the on-disk path -- letting a job whose name
    differs only by surrounding whitespace read and overwrite another job's
    private state, and desynchronizing the on-disk scope token from the GC
    keep-sets (built from the un-stripped config name), which then deleted a
    live job's artifacts.  A non-normalized scope is therefore REJECTED, not
    repaired: the authorized string, the boundary name and the keep-set entry
    stay one and the same string, or the call fails with a clear 400.
    """
    if not scope or not scope.strip():
        raise JobStateError("a non-empty scope is required")
    if scope != scope.strip():
        raise JobStateError(
            "scope {!r} carries leading or trailing whitespace; scopes are "
            "isolation boundaries and are matched exactly (rename the job, "
            "or pass the exact scope string)".format(scope)
        )
    return scope


#: How many dict levels the store adds above a client value before writing it:
#: the record wrapper ``{"schemaVersion": .., "data": <body>}`` plus the body
#: ``{"key": .., "value": <value>, ..}`` that carries it.  :func:`_check_size`
#: must judge portability against THAT shape -- gating the bare value instead
#: let a value within a serializer's nesting bound pass the pre-flight and then
#: fail inside ``_mutate_document_sync``, after the directory fsync and with
#: the document flock held, as a 500 rather than the documented 400.
_STORE_WRAPPER_DEPTH = 2


def _as_stored(value: Any) -> Any:
    """``value`` nested as deeply as the store will actually persist it."""
    wrapped = value
    for _ in range(_STORE_WRAPPER_DEPTH):
        wrapped = {"data": wrapped}
    return wrapped


def _check_size(kind: str, value: Any, max_bytes: int) -> None:
    """Reject a client value that is unportable, then that is over-size.

    Portability is checked FIRST and ALWAYS (even with no size limit): a
    non-finite float or an out-of-64-bit-range int is written differently -- or
    unreadably -- by a node with orjson than one without, so on a mixed fleet
    it silently corrupts the value or permanently wedges every reading node.
    Rejecting it here, with the SAME serializer that will persist it, gives the
    caller a clean 400 before any store work and on every node identically --
    instead of a 500, a silent ``null``, or an unreadable document downstream.
    The check runs against the value in its :data:`_STORE_WRAPPER_DEPTH`
    wrapper, so a nesting-bound verdict here matches the one the write will
    reach.  The size is then measured against the persisted (compact) bytes of
    the value itself, not a looser stdlib estimate, so the limit means what it
    says.
    """
    try:
        _json.ensure_portable(_as_stored(value))
        encoded = _json.dumps_bytes(value)
    except _json.UnsupportedValue as ex:
        raise JobStateError(
            "{} is not portable across the fleet: {}".format(kind, ex)
        ) from ex
    if max_bytes and max_bytes > 0 and len(encoded) > max_bytes:
        raise JobStateError(
            "{} of {} bytes exceeds the configured limit of {} bytes".format(
                kind, len(encoded), max_bytes
            ),
            status=413,
        )


# --------------------------------------------------------------------------
# Durable key/value
# --------------------------------------------------------------------------


async def kv_get(
    backend: StateBackend, scope: str, key: str
) -> Optional[Dict[str, Any]]:
    """The stored body of ``key`` (``{key, value, updatedAt}``), or ``None``.

    ``None`` means the key is absent; a present key whose value is ``null`` is
    a body with ``value: None`` -- the two are distinguishable, which a plain
    "return the value" signature could not do.
    """
    return await backend.read_document(
        KV_NS_PREFIX + _require_scope(scope), key
    )


async def kv_set(
    backend: StateBackend,
    scope: str,
    key: str,
    value: Any,
    *,
    max_bytes: int = 0,
) -> Dict[str, Any]:
    """Set ``key`` to ``value`` (last write wins under the per-key lock)."""
    _check_size("value", value, max_bytes)
    body = {"key": key, "value": value, "updatedAt": _now()}

    def _put(_current: Optional[Dict[str, Any]]) -> Tuple[Any, None]:
        return body, None

    await backend.mutate_document(
        KV_NS_PREFIX + _require_scope(scope), key, _put
    )
    return body


async def kv_delete(backend: StateBackend, scope: str, key: str) -> bool:
    """Delete ``key``; return whether it existed."""
    return await backend.delete_document(
        KV_NS_PREFIX + _require_scope(scope), key
    )


async def kv_list(backend: StateBackend, scope: str) -> List[Dict[str, Any]]:
    """Every key/value body in ``scope`` (order-independent)."""
    bodies = await backend.list_documents(KV_NS_PREFIX + _require_scope(scope))
    return sorted(bodies, key=lambda b: str(b.get("key", "")))


# --------------------------------------------------------------------------
# Incremental cursor / watermark
# --------------------------------------------------------------------------


async def cursor_get(
    backend: StateBackend, scope: str, name: str
) -> Optional[Dict[str, Any]]:
    """The cursor body (``{name, value, updatedAt}``), or ``None`` if unset."""
    return await backend.read_document(
        CURSOR_NS_PREFIX + _require_scope(scope), name
    )


async def cursor_advance(
    backend: StateBackend,
    scope: str,
    name: str,
    value: Any,
    *,
    force: bool = False,
    max_bytes: int = 0,
) -> Dict[str, Any]:
    """Advance cursor ``name`` toward ``value`` and return its new state.

    By default the advance is **monotonic**: the stored value only ever moves
    to ``max(current, value)``, so an out-of-order or replayed batch cannot
    walk an ETL watermark backwards, and two nodes racing to advance the same
    cursor converge on the larger value regardless of who wins the lock.  Pass
    ``force`` to set the value unconditionally (a deliberate rewind).  The
    whole compare-and-set runs under the document's advisory lock, so the
    monotonic guarantee holds fleet-wide on a shared mount.

    Returns the new cursor state as ``{"value": ..., "advanced": bool}``,
    where ``advanced`` is False when a monotonic advance was a no-op (the
    given value was not greater than the current cursor).
    """
    _check_size("cursor value", value, max_bytes)
    now = _now()

    def _advance(
        current: Optional[Dict[str, Any]],
    ) -> Tuple[Any, Dict[str, Any]]:
        cur = current.get("value") if current else None
        if force or cur is None:
            new = value
        else:
            try:
                greater = value > cur
            except TypeError as ex:
                raise JobStateError(
                    "cursor {!r} holds a {} but was advanced with a {}; "
                    "advance it with a comparable value or pass "
                    "force".format(
                        name, type(cur).__name__, type(value).__name__
                    ),
                    status=409,
                ) from ex
            new = value if greater else cur
        advanced = current is None or new != cur
        if not advanced:
            # no change: leave the document (and its updatedAt) untouched so a
            # busy no-op advance is not a write.
            return DOC_KEEP, {"value": cur, "advanced": False}
        body = {"name": name, "value": new, "updatedAt": now}
        return body, {"value": new, "advanced": True}

    _stored, result = await backend.mutate_document(
        CURSOR_NS_PREFIX + _require_scope(scope), name, _advance
    )
    return result


# --------------------------------------------------------------------------
# Idempotency keys
# --------------------------------------------------------------------------


async def idempotency_claim(
    backend: StateBackend,
    scope: str,
    key: str,
    *,
    ttl: float = 0.0,
) -> Dict[str, Any]:
    """Claim ``key`` once, fleet-wide; return ``{"fresh": bool}``.

    The first caller to claim a key gets ``fresh: True`` and should do the
    work; every later caller gets ``fresh: False`` and should skip it -- the
    classic "run this side effect at most once" guard for a retried or
    duplicated job.  The claim is a create-if-absent under the document lock,
    so exactly one of any number of racing callers wins.  A positive ``ttl``
    makes the claim expire after that many seconds, so it may then be re-won
    (a bounded dedupe window); ``ttl == 0`` is a permanent claim.

    Honest bound: like every cronstable coordination primitive this is
    at-least-once, not exactly-once -- a caller that wins the claim and then
    crashes before finishing has "claimed but not done" work, which is why the
    claim is a guard around an idempotent side effect, not a transaction.
    """
    now = _now()

    def _claim(
        current: Optional[Dict[str, Any]],
    ) -> Tuple[Any, Dict[str, Any]]:
        if current is not None:
            expires = current.get("expiresAt")
            if expires is None or expires > now:
                return DOC_KEEP, {
                    "fresh": False,
                    "claimedAt": current.get("claimedAt"),
                }
            # else: the previous claim's TTL lapsed -- fall through and re-win.
        body: Dict[str, Any] = {"key": key, "claimedAt": now}
        if ttl and ttl > 0:
            body["expiresAt"] = now + ttl
        return body, {"fresh": True, "claimedAt": now}

    _stored, result = await backend.mutate_document(
        IDEM_NS_PREFIX + _require_scope(scope), key, _claim
    )
    return result


async def idempotency_release(
    backend: StateBackend, scope: str, key: str
) -> bool:
    """Drop an idempotency claim so ``key`` can be claimed fresh again."""
    return await backend.delete_document(
        IDEM_NS_PREFIX + _require_scope(scope), key
    )


# --------------------------------------------------------------------------
# Named artifact store
# --------------------------------------------------------------------------


async def artifact_put(
    backend: StateBackend,
    scope: str,
    name: str,
    data: bytes,
    *,
    max_bytes: int = 0,
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Publish ``data`` under ``name``; return the artifact record.

    The payload is written to the content-addressed blob store (identical
    payloads dedupe to one blob) and an immutable record ``{name, sha256,
    size, at, meta}`` is appended to the scope's ``artifacts/`` stream, so a
    later run or a peer node reads the newest version back by name.  Only the
    newest record per name is ever read back, so the append carries
    ``prune_latest_by="name"``: superseded older records of the same name are
    amortised away (and their now-orphan blobs reclaimed by the next sweep),
    bounding the stream to the number of distinct names rather than the number
    of publishes.  The scope's whole artifact stream is still reclaimed
    together when the job is garbage collected.
    """
    if max_bytes and max_bytes > 0 and len(data) > max_bytes:
        raise JobStateError(
            "artifact of {} bytes exceeds the configured limit of {} "
            "bytes".format(len(data), max_bytes),
            status=413,
        )
    scope = _require_scope(scope)
    digest = await backend.put_blob(data)
    record: Dict[str, Any] = {
        "name": name,
        "sha256": digest,
        "size": len(data),
        "at": _now(),
    }
    if meta:
        record["meta"] = meta
    await backend.append_record(
        ARTIFACT_STREAM_PREFIX + scope, record, prune_latest_by="name"
    )
    return record


async def artifact_get_record(
    backend: StateBackend, scope: str, name: str, *, strict: bool = False
) -> Optional[Dict[str, Any]]:
    """The newest artifact record published under ``name``, or ``None``.

    The scan is best-effort by default: a record that cannot be read *right
    now* -- a transient I/O error on a shared mount (ESTALE/EIO), or one
    written by a NEWER node's schema -- is skipped, so a blip can make a
    published name read back as never published, or resolve it to a STALE
    older version of itself.  ``strict=True`` makes such a record PROPAGATE
    instead, exactly as :func:`referenced_blob_digests` does: pass it wherever
    "absent" is acted on as PERMANENT and irreversible -- the DAG
    mapped-expansion read, where it becomes a vacuously-successful empty
    fan-out that silently skips the whole task's work -- so the caller can
    tell "nothing published" from "could not read" and retry the latter.

    Strictness spans every record from the newest down to the returned
    match, not just the records carrying ``name``: until a record has been
    read there is no telling which name it holds, and the unreadable one
    could be the newest publish of exactly this one.  Records OLDER than
    the newest readable match need no such guarantee -- they could only
    hold superseded versions, which cannot change the answer.  (A record
    with *bad content* is skipped even under ``strict``: it is
    unrecoverable, and failing closed on it forever would wedge the caller
    permanently.)
    """
    scope = _require_scope(scope)
    stream = ARTIFACT_STREAM_PREFIX + scope
    # Newest-first early-stopping scan: a stream accumulates one immutable
    # record per publish (newest last, so newest_first reads the current
    # version first), and the wanted name is usually the newest record. The
    # predicate + max_matches=1 makes the backend stop parsing at the first
    # record carrying ``name`` -- one parse in the common case -- instead of
    # materialising a whole page and iterating it here. A miss still scans the
    # stream (no record matched), exactly as the full read did; strictness
    # still spans every record from the newest down to the match, since each
    # is read before the predicate sees it.
    matches = await backend.list_records(
        stream,
        newest_first=True,
        strict=strict,
        predicate=lambda record: record.get("name") == name,
        max_matches=1,
    )
    return matches[0] if matches else None


async def artifact_get(
    backend: StateBackend,
    scope: str,
    name: str,
    *,
    strict: bool = False,
    max_bytes: Optional[int] = None,
) -> Optional[Tuple[Dict[str, Any], bytes]]:
    """The newest ``(record, payload)`` published under ``name``, or ``None``.

    ``None`` if the name was never published.  A record whose blob has since
    been swept raises :class:`JobStateError` (410 gone) rather than silently
    returning empty bytes.  ``strict`` is passed through to
    :func:`artifact_get_record`: see there for when an unreadable record must
    raise rather than read back as absent.

    ``max_bytes`` caps the payload the caller is willing to load: when the
    record's stored ``size`` exceeds it, a :class:`JobStateError` (413) is
    raised BEFORE the blob is fetched, so an oversized artifact never enters
    memory.  This is the consumer-side guard for a publisher that opted out of
    the publish-time size limit (``maxArtifactBytes: 0``); the DAG mapped
    fan-out read uses it so one upstream cannot OOM the daemon.
    """
    record = await artifact_get_record(backend, scope, name, strict=strict)
    if record is None:
        return None
    if max_bytes is not None:
        size = record.get("size")
        if isinstance(size, int) and size > max_bytes:
            raise JobStateError(
                "artifact {!r} is {} bytes, over the {}-byte budget".format(
                    name, size, max_bytes
                ),
                status=413,
            )
    digest = record.get("sha256")
    data = await backend.get_blob(str(digest)) if digest else None
    if data is None:
        raise JobStateError(
            "artifact {!r} record survives but its payload blob is gone "
            "(garbage collected)".format(name),
            status=410,
        )
    return record, data


async def artifact_list(
    backend: StateBackend, scope: str
) -> List[Dict[str, Any]]:
    """The newest record for each distinct artifact name in ``scope``."""
    scope = _require_scope(scope)
    records = await backend.list_records(
        ARTIFACT_STREAM_PREFIX + scope, newest_first=True
    )
    seen: Dict[str, Dict[str, Any]] = {}
    for record in records:
        name = record.get("name")
        if isinstance(name, str) and name not in seen:
            seen[name] = record
    return [seen[name] for name in sorted(seen)]


async def referenced_blob_digests(
    backend: StateBackend, scopes: List[str], *, strict: bool = False
) -> "set[str]":
    """Every blob digest the surviving artifact records of ``scopes`` name.

    Fed to :meth:`StateBackend.sweep_orphan_blobs` by the garbage collector so
    a blob is kept while any live artifact record still points at it (blobs
    dedupe across scopes, so the reference set must span every surviving
    scope, not one).  The collectors pass ``strict=True``: a record that
    cannot be read right now (an NFS blip, a newer node's schema) then
    PROPAGATES instead of being skipped, because a silently-missed record
    would present its still-live blob to the sweep as an orphan.  The caller
    treats the exception as "reference set unknown" and skips the sweep.
    """
    digests: set[str] = set()
    for scope in scopes:
        records = await backend.list_records(
            ARTIFACT_STREAM_PREFIX + scope, strict=strict
        )
        for record in records:
            digest = record.get("sha256")
            if isinstance(digest, str):
                digests.add(digest)
    return digests
