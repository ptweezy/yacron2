"""Optional durable state backend: one filesystem seam for local disk and
Amazon S3 Files.

cronstable is stateless by default -- run history, retry counters, the
next-fire
index and the leadership view all live in memory and reset on restart, and that
zero-disk story is a feature.  This module adds the *opt-in* other half: when a
``state`` config section is present, a :class:`StateBackend` gives cronstable a
durable, restart-surviving place to keep records and a lock it can coordinate
on.  Absent the section the backend is never constructed and the in-memory path
is byte-identical to before.

The one design decision that makes this elegant is that a **local filesystem**
and an **Amazon S3 Files** mount are the *same kind of backend*: a POSIX
filesystem with atomic file rename and advisory ``flock``.  So there is one
implementation, :class:`FilesystemStateBackend`, and the *mount*, not the code
-- decides its reach:

* point ``state.path`` at a local directory and you get single-node restart
  durability;
* point it at an Amazon S3 Files / EFS mount and the identical code gets S3
  durability *plus* fleet-wide coordination, because the advisory NFSv4
  lock and atomic rename an EFS-backed mount provides are honoured across
  every host that mounts it.

Two invariants keep that correct on every backing store, including an S3 Files
mount whose object side has no native rename:

* **one immutable object per record.**  Records are never rewritten in place;
  each is written once to a unique filename (via a temp file + atomic rename)
  and thereafter only read or deleted.  The "last fired" cursor is therefore
  *derived* (the max over the immutable records), never a mutable file, so
  nothing depends on rewriting an existing object.
* **every record is schema-versioned.**  A record this build cannot understand
  (an unknown ``schemaVersion``, truncated JSON from a crash mid-write on a
  store without atomic rename) is quarantined on read, never guessed at, so one
  poison object can never brick startup.

The coordination primitive is a TTL *lease* guarded by an advisory ``flock``
over a dedicated lock file (never the data file, which is swapped out by the
atomic rename), with a monotonic ``fence`` for takeover detection.  The whole
locked read-modify-write runs in a worker thread so a blocking lock never
freezes the event loop.

This module is imported only when ``state`` is configured (see
:func:`cronstable.cron.Cron.start_stop_state`), and it uses nothing outside the
standard library, so it costs the common, stateless install nothing.
"""

import abc
import asyncio
import contextlib
import hashlib
import logging
import math
import os
import threading
import time
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    cast,
)

from cronstable import _json
from cronstable.config import ConfigError, StateConfig
from cronstable.platform import (
    IS_WINDOWS,
    exclusive_file_lock,
    fsync_directory,
)

_T = TypeVar("_T")

logger = logging.getLogger("cronstable.state")

#: Per-record on-disk schema version.  Every record is written wrapped as
#: ``{"schemaVersion": SCHEME_VERSION, "data": {...}}``; a record whose version
#: this build does not recognise is quarantined on read rather than guessed at.
#: Bump this when the wrapper (not a caller's ``data``) changes shape, so old
#: and new records are told apart instead of silently mis-read.
SCHEME_VERSION = "v1"

# Registry of record-scheme converters for `cronstable state migrate-schema`:
# maps an OLD wrapper schemaVersion to a callable converting that version's
# ``data`` dict to the CURRENT version's shape (return ``None`` to declare
# the record unconvertible, leaving it to be quarantined on read).  Empty
# while v1 is the only scheme ever shipped; when a v2 arrives it registers
# its v1 converter here and `state migrate-schema` rewrites stores in place.
RECORD_MIGRATIONS: Dict[
    str, Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
] = {}

# Streams never garbage collected regardless of manifests: the store's
# version stamp and the manifest anchor stream itself.
PROTECTED_STREAMS = frozenset({"meta", "manifests"})

# Age (seconds) past which an orphaned write-temp file is swept by garbage
# collection.  No legitimate in-flight write lives anywhere near this long
# (each op writes a small file and either renames or unlinks it within
# seconds); anything older is debris from a crash mid-write.
TMP_MAX_AGE = 86400.0

# Subdirectories under a namespace root.  Records live under RECORDS_DIR in a
# per-stream directory; leases under LEASES_DIR; corrupt records are moved into
# QUARANTINE_DIR; TMP_DIR holds the write-temp files atomically renamed into
# place.  DOCS_DIR holds the mutable job-facing documents (KV / cursor /
# idempotency), one file per key rewritten via atomic rename under an advisory
# flock -- the same lease-file discipline generalised to arbitrary values, so
# it is equally safe on an S3 Files mount (file rename is atomic there even
# though object rename is not).  BLOBS_DIR holds the content-addressed artifact
# payloads, each an immutable file named by its SHA-256.  Directories are only
# ever *created*, never renamed (a directory rename is the one costly operation
# on an S3 Files mount), so this layout is safe there.
RECORDS_DIR = "records"
LEASES_DIR = "leases"
QUARANTINE_DIR = "quarantine"
TMP_DIR = "tmp"
DOCS_DIR = "docs"
BLOBS_DIR = "blobs"

# Worker-thread concurrency caps (see :meth:`FilesystemStateBackend._call`).
# BULK bounds the high-volume record/document ops so a wedged mount plus a busy
# scheduler cannot pile up an unbounded number of stuck daemon threads.  LEASE
# is a SEPARATE, dedicated lane for the coordination ops (lease acquire /
# renew / release / read): a burst of bulk record writes -- or bulk threads
# wedged on a hung mount -- must never hold every slot and starve a lease renew
# below its TTL, which would expire a live holder's lease and hand its fenced
# work to a standby (split-brain / double-fire).  Leases are few and each op is
# tiny, so a small isolated lane both prevents the starvation and still bounds
# how many lease threads a fully-hung mount can strand.
BULK_CALL_SLOTS = 16
LEASE_CALL_SLOTS = 8

# Sentinels a :meth:`StateBackend.mutate_document` transform returns *in place
# of* a new document body: leave the document exactly as it was (KEEP), or
# delete it (DELETE).  Anything else the transform returns is the new document
# body to persist.  Distinct ``object()`` identities so no real JSON value a
# caller might store can be mistaken for one.
DOC_KEEP: Any = object()
DOC_DELETE: Any = object()

# Network/shared filesystem types (as they appear in /proc/mounts) that a lock
# is honoured across hosts on, so the backend may offer fleet-wide
# coordination.  An Amazon S3 Files / EFS mount presents as nfs4.  Anything not
# listed (ext4/xfs/btrfs/apfs/overlay/tmpfs/...) is treated as single-node.
_SHARED_FSTYPES = frozenset(
    {
        "nfs",
        "nfs4",
        "nfs3",
        "efs",
        "cifs",
        "smb3",
        "smbfs",
        "lustre",
        "glusterfs",
        "ceph",
        "cephfs",
        "fuse.sshfs",
    }
)

# Characters kept as-is in a filename; everything else in a stream/namespace/
# lease name is percent-encoded (see _fs_safe), which is injective, so two
# distinct job names can never collide on one on-disk path.  Deliberately
# lowercase-only (uppercase is encoded) so the mapping stays injective on
# case-INsensitive filesystems (NTFS, APFS), and dot-free (``.`` is encoded)
# so no name can produce ``.``/``..`` path components or the trailing-dot
# aliases Windows strips.
_FS_SAFE = frozenset("abcdefghijklmnopqrstuvwxyz0123456789_-")

# Windows device names, reserved in every directory (case-insensitively).  A
# lowercase job name could otherwise pass through _fs_safe verbatim and make
# every open/mkdir under it fail (or hit the console device) on Windows.
_WINDOWS_RESERVED = frozenset(
    {"con", "prn", "aux", "nul"}
    | {"com{}".format(i) for i in range(1, 10)}
    | {"lpt{}".format(i) for i in range(1, 10)}
)

# Longest _fs_safe token emitted, comfortably under NAME_MAX (255 bytes) once
# the surrounding prefixes/suffixes ("runs%2F", ".json", ".lease") are added.
# Longer encodings are truncated and made unique again with a digest; without
# this a long (or non-ASCII, at 3 encoded chars per UTF-8 byte) job name makes
# every append/list for its stream fail with ENAMETOOLONG forever.
_FS_SAFE_MAX = 130

# Marker joining a length-truncated token's kept head to its digest (see
# _fs_safe).  The natural encoding ("%" + two uppercase hex digits) can never
# emit it, so its presence in an on-disk token positively identifies a
# truncated token -- one whose logical name is NOT recoverable from the
# token alone.
_FS_TRUNCATION_MARKER = "%."

# Filename of the sidecar written inside a length-truncated stream's
# directory, holding the exact logical stream name (raw UTF-8) -- the only
# way such a stream's name can round-trip back out of list_stream_names
# (a garbled name re-encodes to a DIFFERENT token, making the stream
# invisible to the GC keep-set builders and its state collectable as
# garbage).  Deliberately not ``.json`` so record listing/pruning/migration
# never mistake it for a record.
_STREAM_NAME_SIDECAR = "stream-name.txt"


def _now() -> float:
    """Wall-clock epoch seconds; the one time source, so tests can patch it.

    Lease expiry and filename ordering are judged against this.  Across a
    *shared* mount the comparison spans hosts, so the HA use of leases assumes
    bounded clock skew (NTP) -- documented, and irrelevant to single-node use.
    """
    return time.time()


def _fs_safe(name: str) -> str:
    """Return ``name`` as an injective, filename-safe token.

    Any byte outside :data:`_FS_SAFE` is percent-encoded from its UTF-8
    encoding, so arbitrary job names (which may contain ``/``, spaces, unicode)
    map to distinct, portable filenames without collisions.  Injectivity holds
    even case-insensitively: the safe set has no uppercase, and the uppercase
    hex the escapes emit is fixed-case, so two tokens that differ only by case
    cannot both be produced.  Three escape hatches keep the token usable as a
    path component everywhere:

    * a token that IS a reserved Windows device name (``con``, ``nul``, ...)
      gets its first character force-encoded -- unambiguous, since the natural
      encoding never escapes a safe character;
    * a token longer than :data:`_FS_SAFE_MAX` (ENAMETOOLONG territory) is
      truncated and re-uniqued with a SHA-256 digest of the original name,
      joined by ``%.`` -- a marker the natural encoding (``%`` + 2 uppercase
      hex) can never emit;
    * an empty name maps to ``_``.
    """
    out: List[str] = []
    for byte in name.encode("utf-8"):
        char = chr(byte)
        if char in _FS_SAFE:
            out.append(char)
        else:
            out.append("%{:02X}".format(byte))
    token = "".join(out) or "_"
    if token in _WINDOWS_RESERVED:
        token = "%{:02X}".format(ord(token[0])) + token[1:]
    if len(token) > _FS_SAFE_MAX:
        digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:32]
        token = token[: _FS_SAFE_MAX - 34] + _FS_TRUNCATION_MARKER + digest
    return token


def _fs_safe_fragment(fragment: str) -> str:
    """Per-byte escape of a stream-name PREFIX, for on-disk prefix matching.

    Applies :func:`_fs_safe`'s byte encoding without its whole-token
    adjustments.  Valid for *prefix* matching because those adjustments only
    ever rewrite a token's FIRST character (reserved device names, which are
    whole-token matches a multi-part prefix can never be) or its over-length
    TAIL (the digest truncation keeps the head intact) -- a managed prefix
    like ``runs/`` therefore always survives verbatim at the front of the
    stream's encoded directory name.
    """
    out: List[str] = []
    for byte in fragment.encode("utf-8"):
        char = chr(byte)
        if char in _FS_SAFE:
            out.append(char)
        else:
            out.append("%{:02X}".format(byte))
    return "".join(out)


def _record_epoch(name: str) -> float:
    """The write-epoch a record filename sorts by, or ``+inf`` (unknown).

    Unknown/foreign filenames map to ``+inf`` so an age-based sweep treats
    them as brand new and keeps their stream -- never delete what cannot be
    classified.  Guarded against ``float()``'s non-numeric spellings: a file
    named ``nan-...`` would otherwise parse to NaN, every comparison against
    it would be False, and the keep-unclassifiable contract would invert.
    """
    try:
        epoch = float(name.split("-", 1)[0])
    except ValueError:
        return float("inf")
    if math.isnan(epoch) or math.isinf(epoch):
        return float("inf")
    return epoch


def _unescape_mount(field: str) -> str:
    """Decode the octal escapes /proc/mounts uses for spaces/tabs/etc.

    A space in a mountpoint is written ``\\040``: a backslash then three
    octal digits.
    """
    if "\\" not in field:
        return field
    out: List[str] = []
    i = 0
    size = len(field)
    while i < size:
        octal = field[i + 1 : i + 4]
        if field[i] == "\\" and len(octal) == 3 and octal.isdigit():
            try:
                out.append(chr(int(octal, 8)))
                i += 4
                continue
            except ValueError:  # pragma: no cover - malformed escape
                pass
        out.append(field[i])
        i += 1
    return "".join(out)


def _mount_entry(path: str) -> Optional[Tuple[str, str]]:
    """The ``(fstype, options)`` of the mount ``path`` lives on, or ``None``.

    Parses ``/proc/mounts`` and picks the longest mountpoint that is a prefix
    of the resolved path.  Linux-only (no portable ``statfs`` f_type in the
    stdlib); returns ``None`` where ``/proc`` is absent (macOS/Windows),
    which the caller treats as "cannot tell -> single-node".  The options
    column feeds the lock-fidelity check: an NFS mount carrying ``nolock``
    (or ``local_lock=flock``/``all``) honours flock only host-locally, which
    the fstype alone cannot reveal.
    """
    try:
        with open("/proc/mounts", encoding="utf-8") as fobj:
            lines = fobj.read().splitlines()
    except OSError:
        return None
    real = os.path.realpath(path)
    best_mount = ""
    best: Optional[Tuple[str, str]] = None
    for line in lines:
        parts = line.split(" ")
        if len(parts) < 4:
            continue
        mountpoint = _unescape_mount(parts[1])
        fstype = parts[2]
        options = parts[3]
        prefix = mountpoint.rstrip("/") + "/"
        if real == mountpoint or real.startswith(prefix) or mountpoint == "/":
            # longest matching mountpoint wins (>= so "/" is a fallback only)
            if len(mountpoint) >= len(best_mount):
                best_mount = mountpoint
                best = (fstype, options)
    return best


def _mount_fstype(path: str) -> Optional[str]:
    """The filesystem type of the mount ``path`` lives on, or ``None``."""
    entry = _mount_entry(path)
    return entry[0] if entry is not None else None


def _local_lock_reason(path: str) -> Optional[str]:
    """A human reason the mount's locks are host-local, or ``None`` (fine).

    Inspects the mount options of NFS-family mounts: ``nolock`` and
    ``local_lock=flock``/``local_lock=all`` make the kernel satisfy flock
    requests locally without ever consulting the server, so two hosts each
    "hold" the same exclusive lock -- exactly the silent double-run a
    coordination consumer must refuse.  Linux-only (like the topology
    probe); an undecidable mount returns ``None``, leaving the functional
    probe and the operator's ``topology`` assertion as the remaining
    guards.
    """
    entry = _mount_entry(path)
    if entry is None:
        return None
    fstype, options = entry
    if not fstype.startswith("nfs"):
        return None
    opts = options.split(",")
    if "nolock" in opts:
        return "the NFS mount is mounted with 'nolock'"
    for opt in opts:
        if opt.startswith("local_lock=") and opt.split("=", 1)[1] in (
            "flock",
            "all",
        ):
            return "the NFS mount is mounted with '{}'".format(opt)
    return None


def detect_topology(path: str) -> Optional[str]:
    """Probe: ``"shared"`` | ``"single-node"`` | ``None`` (cannot tell).

    ``None`` means the probe could not decide (no ``/proc``, or Windows), and
    the caller then falls back to ``single-node`` under ``topology: auto`` and
    lets an operator override with an explicit ``topology: shared``.
    """
    if IS_WINDOWS:
        # Windows has no cross-host lock story here and no /proc to probe; an
        # operator wanting shared semantics must assert it explicitly.
        return None
    fstype = _mount_fstype(path)
    if fstype is None:
        return None
    return "shared" if fstype in _SHARED_FSTYPES else "single-node"


class _LeaseUnreadable(Exception):
    """A lease file exists (or may exist) but cannot be trusted right now.

    Raised (internally) when the lease file is unreadable for any reason other
    than plain absence: a transient I/O error on a shared mount (ESTALE/EIO),
    a permissions problem, or corrupt content.  The lease operations treat it
    as *fail closed*: an acquire/renew is denied rather than treating the
    unreadable state as "no lease" and stealing a possibly-valid, unexpired
    lease from its live holder (with a reset fence).
    """


class _DocumentUnreadable(Exception):
    """A document file exists (or may exist) but cannot be trusted right now.

    The document analogue of :class:`_LeaseUnreadable`: raised (internally)
    from the strict read inside :meth:`FilesystemStateBackend.mutate_document`
    when the document file is unreadable for any reason other than plain
    absence -- a transient I/O error on a shared mount, or corrupt content.
    A read-modify-write cannot proceed safely without a trustworthy current
    value (it would silently clobber a live document, or advance a monotonic
    cursor backwards), so the mutation *fails* rather than guessing.  It
    surfaces to the job-facing caller as an error, never as a wrong value.
    """


@dataclass
class Lease:
    """A held (or observed) TTL lease.

    ``fence`` increases every time the lease is *taken over* (fixed only
    across a same-holder renew of a still-valid lease), so a stale holder can
    be detected and its late writes fenced off.  It is monotonic for the life
    of the store: release marks the lease expired *in place* (never deletes
    the file), so the counter survives release/re-acquire cycles instead of
    resetting to 1 and re-issuing fence values already handed out.
    ``expires_at`` is wall-clock epoch seconds; the lease is free to take over
    once ``_now() > expires_at``.
    """

    name: str
    holder: str
    fence: int
    expires_at: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "holder": self.holder,
            "fence": self.fence,
            "expiresAt": self.expires_at,
        }


class _TokenBucket:
    """Async token bucket bounding store operations per second.

    The request-rate/cost control for stores that bill per request (the
    future native-S3 backend; harmless on a filesystem).  Refilled from the
    event loop's monotonic clock; burst capacity is one second's worth of
    tokens (at least 1), so a quiet store still serves a small flurry
    immediately.  Single-loop use only (no lock): every await point is
    between full read-modify-write passes.
    """

    def __init__(self, rate: float) -> None:
        self.rate = rate
        self.burst = max(1.0, rate)
        self._tokens = self.burst
        self._last: Optional[float] = None

    async def throttle(self) -> float:
        """Take one token, sleeping until one is available.

        Returns the seconds slept (0.0 when a token was free), so the caller
        can account throttling separately from store latency.
        """
        loop = asyncio.get_running_loop()
        waited = 0.0
        while True:
            now = loop.time()
            if self._last is not None:
                self._tokens = min(
                    self.burst, self._tokens + (now - self._last) * self.rate
                )
            self._last = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return waited
            need = (1.0 - self._tokens) / self.rate
            waited += need
            await asyncio.sleep(need)


class StateBackend(abc.ABC):
    """The seam every durable-state and coordination call goes through.

    Kept deliberately small: an append-only record store (with a derived-max
    read for cursors), a TTL lease, a topology read, and a lifecycle.  A future
    native-S3 (SigV4/conditional-write) backend would use the same surface
    without a shared mount; for now :class:`FilesystemStateBackend` is the only
    implementation, serving both local disk and Amazon S3 Files.
    """

    #: the resolved state config this backend was built from
    config: StateConfig
    #: backend name surfaced in :meth:`view_dict`
    backend_name: str = "state"

    # --- lifecycle -------------------------------------------------------

    @abc.abstractmethod
    async def start(self) -> None:
        """Create the store layout, probe topology, verify writability."""

    @abc.abstractmethod
    async def stop(self) -> None:
        """Release any resources (best-effort; the store itself persists)."""

    # --- durable immutable records ---------------------------------------

    @abc.abstractmethod
    async def append_record(self, stream: str, data: Dict[str, Any]) -> str:
        """Append one immutable record to ``stream``; return its record id."""

    @abc.abstractmethod
    async def list_records(
        self,
        stream: str,
        *,
        limit: Optional[int] = None,
        newest_first: bool = False,
        strict: bool = False,
    ) -> List[Dict[str, Any]]:
        """Read back a stream's records (corrupt ones quarantined).

        ``strict=True`` makes an environmentally-unreadable record (an NFS
        blip) or one written by a NEWER schema PROPAGATE as an exception
        instead of being silently skipped -- required by any caller for whom
        a missed record is worse than a failed read (the orphan-blob sweep,
        which must not mistake "a reference I could not read" for "no
        reference").  The default stays best-effort.
        """

    @abc.abstractmethod
    async def list_stream_names(self, prefix: str) -> List[str]:
        """Logical stream names currently on disk starting with ``prefix``.

        For a *family* of per-host/per-scope streams sharing a prefix (e.g.
        ``"manifests/"`` -- one stream per host, see
        :data:`cronstable.cron.MANIFEST_STREAM_PREFIX`) a caller that must read
        every member's own records (not just check keep-set membership, which
        :meth:`collect_garbage`'s ``keep`` mapping already covers) needs to
        first discover which members currently exist.  Best-effort: an
        unreadable store returns ``[]`` rather than raising.
        """

    async def list_stream_names_audit(
        self, prefix: str
    ) -> "Tuple[List[str], bool]":
        """``(names, complete)``: the listing plus whether it is exhaustive.

        ``complete`` is ``False`` when a stream matching ``prefix`` exists
        but could not be NAMED (a legacy length-truncated directory without
        its logical-name sidecar, which :meth:`list_stream_names` silently
        skips).  A caller that will DELETE based on the listing -- the
        orphan-blob sweep builds its referenced-digest set from it -- must
        distinguish "no other streams" from "streams I cannot see" and keep
        on any doubt.  The base backend cannot enumerate at all, so it
        reports an incomplete empty listing.
        """
        return [], False

    @abc.abstractmethod
    async def derive_max(self, stream: str, field: str) -> Optional[Any]:
        """The max value of ``field`` over a stream's records (the cursor).

        Order-independent, so on a shared mount where several nodes append to
        the same stream the result is the deterministic max, never a
        last-writer-wins race.  ``None`` if the stream is empty / the field
        absent.
        """

    @abc.abstractmethod
    async def prune_records(self, stream: str, *, keep: int) -> int:
        """Delete all but the newest ``keep`` records; return the # removed.

        Keeps a stream bounded the way the in-memory ``maxlen`` deque did.
        ``keep <= 0`` deletes the whole stream.  Single-node safe (per-key
        deletes); a cluster where several nodes prune the same stream may race
        on individual deletes, which is harmless (a missing file is ignored) --
        the leader-gated variant is a later phase.
        """

    # --- mutable documents (job-facing KV / cursor / idempotency) --------

    @abc.abstractmethod
    async def read_document(
        self, namespace: str, key: str
    ) -> Optional[Dict[str, Any]]:
        """The current body of document ``key`` in ``namespace``, or ``None``.

        An unlocked, best-effort read: a document that is absent, unreadable
        right now, or corrupt all read back as ``None`` (the strict read used
        for the read-modify-write lives inside :meth:`mutate_document`).
        """

    @abc.abstractmethod
    async def mutate_document(
        self,
        namespace: str,
        key: str,
        transform: "Callable[[Optional[Dict[str, Any]]], Tuple[Any, _T]]",
    ) -> "Tuple[Optional[Dict[str, Any]], _T]":
        """Atomically read-modify-write document ``key``.

        Runs ``transform(current_body)`` under an advisory ``flock`` over the
        document's dedicated lock file, so on a shared mount the whole RMW is
        serialised fleet-wide -- the property a monotonic cursor and a
        create-if-absent idempotency claim both depend on.  ``transform``
        returns ``(new_body, result)``: ``new_body`` is the JSON body to
        persist, or :data:`DOC_KEEP` to leave the document untouched, or
        :data:`DOC_DELETE` to remove it.  Returns ``(stored_body, result)``
        where ``stored_body`` is the body now on disk (``None`` after a
        delete).  ``transform`` must be a pure, side-effect-free callable: it
        runs on a worker thread and may be retried on a torn read.
        """

    @abc.abstractmethod
    async def delete_document(self, namespace: str, key: str) -> bool:
        """Delete document ``key``; return whether it existed."""

    @abc.abstractmethod
    async def list_documents(self, namespace: str) -> List[Dict[str, Any]]:
        """Every readable document body in ``namespace``, order-independent."""

    async def list_document_namespaces(
        self, prefix: str
    ) -> "Tuple[List[str], bool]":
        """``(namespaces, complete)``: namespaces starting with ``prefix``.

        The garbage collector uses this to discover the per-dag run-document
        namespaces (``dagrun/<dag>``) so it can keep every live run's XCom
        stream and collect the runs of dags removed from config.  ``complete``
        is ``False`` when a matching namespace exists on disk but its logical
        name is unrecoverable (a length-truncated directory -- document
        namespaces have no name sidecar), so a deleting caller keeps instead.
        The base backend cannot enumerate at all, so it reports an incomplete
        empty listing.
        """
        return [], False

    # --- content-addressed blobs (job-facing artifact payloads) ----------

    @abc.abstractmethod
    async def put_blob(self, data: bytes) -> str:
        """Store ``data`` (deduplicated by content); return its SHA-256 hex."""

    @abc.abstractmethod
    async def get_blob(self, digest: str) -> Optional[bytes]:
        """Read the blob with SHA-256 ``digest``, or ``None`` if absent."""

    # --- advisory-lock TTL lease -----------------------------------------

    @abc.abstractmethod
    async def acquire_lease(
        self, name: str, holder: str, ttl: float
    ) -> Optional[Lease]:
        """Take (or renew) lease ``name`` for ``ttl``s, else ``None``.

        A caller that bounds this with a timeout must treat a timeout as
        UNKNOWN, not as denied: the abandoned worker may still complete the
        acquisition on disk, leaving the lease held (by this holder) until
        its TTL lapses.
        """

    @abc.abstractmethod
    async def renew_lease(self, lease: Lease, ttl: float) -> Optional[Lease]:
        """Extend a still-held lease; ``None`` if it was taken over."""

    @abc.abstractmethod
    async def release_lease(self, lease: Lease) -> None:
        """Release a lease we hold (a no-op if we no longer hold it)."""

    @abc.abstractmethod
    async def read_lease(self, name: str) -> Optional[Lease]:
        """Observe a lease without taking it (best-effort, unlocked read)."""

    # --- maintenance -------------------------------------------------------

    async def collect_garbage(
        self,
        *,
        keep: Dict[str, "Set[str]"],
        grace: float,
        ephemeral_lease_prefixes: "Tuple[str, ...]" = (),
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Remove streams no recent manifest references (see the filesystem
        backend for semantics).  ``ephemeral_lease_prefixes`` names the
        per-run lease classes whose dead files may be reclaimed; every
        other lease is never touched.  The base backend has nothing to
        collect."""
        return {}

    async def migrate_schema(self, *, dry_run: bool = False) -> Dict[str, Any]:
        """Rewrite records of older known schemes to the current one (see
        :data:`RECORD_MIGRATIONS`); the base backend has nothing to walk."""
        return {}

    async def sweep_orphan_blobs(
        self,
        referenced: "Set[str]",
        grace: float,
        *,
        dry_run: bool = False,
    ) -> int:
        """Delete artifact blobs no surviving record references (see the
        filesystem backend); the base backend stores no blobs."""
        return 0

    # --- introspection ---------------------------------------------------

    @property
    @abc.abstractmethod
    def topology(self) -> str:
        """``"shared"`` | ``"single-node"`` | ``"unknown"`` (before start)."""

    def supports_shared_locking(self) -> bool:
        """Whether a lease here excludes across hosts (HA-capable)."""
        return self.topology == "shared"

    async def verify_locking(self) -> Optional[str]:
        """Why the store's locks must not be trusted for coordination, or
        ``None`` (they behave, or the backend has no way to tell).  See the
        filesystem backend for the real probe."""
        return None

    def stats(self) -> Dict[str, Any]:
        """Self-observability counters (op counts/errors/latency, lock
        contention, throttling, worker-lane occupancy); ``{}`` for a backend
        with none."""
        return {}

    def view_dict(self) -> Dict[str, Any]:
        """The state view for a future ``GET /state`` / the dashboard."""
        return {"backend": self.backend_name, "topology": self.topology}

    async def inventory(self) -> Dict[str, Any]:
        """A metadata-only topology snapshot for the dashboard's state
        inspector: health (:meth:`view_dict` + :meth:`stats`) plus, on
        backends that can enumerate their store, per-prefix stream/document
        counts, scope lists, and active leases.  NEVER returns record payloads
        or document values -- the inspector is a metadata surface only.  The
        base backend cannot enumerate, so it reports ``enumerable: false`` and
        the health block alone."""
        return {
            "view": self.view_dict(),
            "stats": self.stats(),
            "enumerable": False,
            "records": {},
            "documents": {},
            "leases": [],
            "quarantine": 0,
        }


class FilesystemStateBackend(StateBackend):
    """A durable state backend over any POSIX filesystem.

    Serves both a local directory (single-node durability) and an Amazon S3
    Files / EFS mount (durability + fleet-wide coordination) with identical
    code; see the module docstring for why that works and what it assumes.
    """

    backend_name = "filesystem"

    def __init__(
        self, config: StateConfig, get_job_set_id: Callable[[], str]
    ) -> None:
        self.config = config
        self.get_job_set_id = get_job_set_id
        # expanduser first: `path: ~/state` must mean the home directory, not
        # a literal "~" directory under whatever CWD the daemon started in.
        self.root = os.path.abspath(os.path.expanduser(config["path"]))
        # a stable prefix so several deployments can share one store without
        # colliding; job-set scoping (like the lease backends' @reboot set) is
        # layered on top by callers via the stream name.
        self.namespace = config.get("deploymentId") or "default"
        self._configured_topology: str = config.get("topology", "auto")
        self._topology = "unknown"
        # a per-process id mixed into every written filename, so records and
        # temp files from different nodes/processes onto one shared mount never
        # collide on a name.  os.urandom is fine (uniqueness, not secrecy).
        self._instance = os.urandom(6).hex()
        # The sync halves below run on (daemon) worker threads, several of
        # which may be in flight at once -- two jobs finishing together each
        # schedule an append.  An unlocked `self._seq += 1` is a read-modify-
        # write two threads can interleave, and a duplicated seq (plus the
        # coarse Windows clock) means a duplicated record id: one record
        # silently clobbering another via the atomic rename.
        self._seq = 0
        self._seq_lock = threading.Lock()
        # Bounds concurrent worker threads (see _call).  Daemon threads make
        # a hung store abandonable, but without a cap a wedged mount plus a
        # busy scheduler would pile up one stuck thread per finished run;
        # excess calls queue on the semaphore (cheap pending tasks) instead.
        # Created lazily so construction needs no running event loop.  The
        # LEASE lane is deliberately SEPARATE (see BULK_CALL_SLOTS /
        # LEASE_CALL_SLOTS) so bulk record traffic can never starve a lease
        # renew below its TTL.
        self._call_slots: Optional[asyncio.Semaphore] = None
        self._lease_slots: Optional[asyncio.Semaphore] = None
        # Optional request-rate control (state.maxOpsPerSecond): every op
        # takes a token before its worker thread is spawned, so a billing-
        # sensitive mount sees a bounded request rate. 0/absent -> off.
        rate = float(config.get("maxOpsPerSecond") or 0)
        self._rate_limit = _TokenBucket(rate) if rate > 0 else None
        # Self-observability accumulators (see stats()).  Updated from the
        # worker threads, hence the plain lock; read (snapshotted) from the
        # event loop at scrape time.
        self._stats_lock = threading.Lock()
        # op -> [count, errors, seconds-of-store-time]
        self._op_stats: Dict[str, List[float]] = {}
        self._lock_acquisitions = 0
        self._lock_wait_seconds = 0.0
        self._throttled_ops = 0
        self._throttle_wait_seconds = 0.0
        # Live worker-thread gauges per lane, plus high-water marks.  A slot is
        # held for a thread's whole lifetime, so a hung mount pins its lane's
        # gauge at capacity -- exactly the "the store is wedged" signal that
        # the completed-op counters above (which only tick when an op FINISHES)
        # cannot show.  Touched only from the event-loop thread, but guarded by
        # _stats_lock so stats() reads a consistent snapshot.
        self._inflight_bulk = 0
        self._inflight_lease = 0
        self._inflight_peak_bulk = 0
        self._inflight_peak_lease = 0

    # --- paths -----------------------------------------------------------

    @property
    def base(self) -> str:
        """The namespaced root all this backend's files live under."""
        return os.path.join(self.root, _fs_safe(self.namespace))

    def _stream_dir(self, stream: str) -> str:
        return os.path.join(self.base, RECORDS_DIR, _fs_safe(stream))

    def _lease_paths(self, name: str) -> Tuple[str, str]:
        safe = _fs_safe(name)
        leases = os.path.join(self.base, LEASES_DIR)
        return (
            os.path.join(leases, safe + ".lock"),
            os.path.join(leases, safe + ".lease"),
        )

    def _doc_dir(self, namespace: str) -> str:
        return os.path.join(self.base, DOCS_DIR, _fs_safe(namespace))

    def _doc_paths(self, namespace: str, key: str) -> Tuple[str, str]:
        """The ``(lock file, doc file)`` for one document.

        Like a lease, the flock rides a stable side-file (``.lock``) while the
        value file (``.doc``) is swapped out by the atomic rename -- locking
        the value file directly would lock an inode about to be replaced.
        """
        ns_dir = self._doc_dir(namespace)
        safe = _fs_safe(key)
        return (
            os.path.join(ns_dir, safe + ".lock"),
            os.path.join(ns_dir, safe + ".doc"),
        )

    def _blob_path(self, digest: str) -> str:
        """The on-disk path of a content-addressed blob.

        Sharded by the first two hex characters so one namespace's blob
        directory never grows to a single flat directory of millions of
        entries (which some filesystems handle poorly).
        """
        return os.path.join(self.base, BLOBS_DIR, digest[:2], digest + ".blob")

    def _next_seq(self) -> int:
        with self._seq_lock:
            self._seq += 1
            return self._seq

    def _tmp_path(self) -> str:
        return os.path.join(
            self.base,
            TMP_DIR,
            "w-{}-{:012d}.tmp".format(self._instance, self._next_seq()),
        )

    # --- worker threads ----------------------------------------------------

    async def _call(self, op: str, fn: Callable[..., _T], *args: Any) -> _T:
        """Run blocking ``fn(*args)`` on a *daemon* thread and await it.

        Not ``asyncio.to_thread``: the default executor's threads are
        non-daemonic and joined at interpreter exit, so one worker wedged in
        an uninterruptible NFS syscall (the classic dead-server hard mount)
        would hang process shutdown forever -- exactly what the bounded
        shutdown flush promises cannot happen.  A daemon thread per call keeps
        a hung store abandonable: callers can time out (``asyncio.wait_for``)
        and exit; the OS reclaims the stuck thread.  State ops are low-rate
        (a handful per finished run), so thread-per-call is cheap.

        ``op`` labels the call in the self-observability stats: count, error
        count, and seconds of store time (measured around ``fn`` itself on
        the worker thread, so queueing and throttling are excluded) are
        accumulated per label and surfaced via :meth:`stats`.
        """
        loop = asyncio.get_running_loop()
        is_lease = op.startswith("lease-")
        if self._rate_limit is not None and not is_lease:
            # take the rate token BEFORE a worker slot, so a throttled op
            # queues as a cheap pending coroutine, not a held thread slot.
            # Lease operations BYPASS the bucket: they are tiny, and a
            # coordination renew queued behind a burst of bulk record writes
            # could overshoot its TTL -- expiring a live holder's lease and
            # double-running the very job the lease exists to fence.  The
            # billing cost this exempts is a few small requests per renew
            # period, not the bulk traffic the bucket is for.
            waited = await self._rate_limit.throttle()
            if waited > 0.0:
                with self._stats_lock:
                    self._throttled_ops += 1
                    self._throttle_wait_seconds += waited
        # Pick the worker lane.  Lease/coordination ops get their OWN pool so a
        # burst of bulk record writes -- or bulk threads wedged on a hung mount
        # -- can never hold every slot and delay a lease renew past its TTL.
        # Same split-brain hazard the rate-limiter bypass above guards against,
        # extended to the worker-slot pool it left exposed: the bypass kept a
        # renew off the throttle queue, but it still had to win one of the
        # shared slots, which a bulk burst/wedge can exhaust.
        if is_lease:
            if self._lease_slots is None:
                self._lease_slots = asyncio.Semaphore(LEASE_CALL_SLOTS)
            slots = self._lease_slots
        else:
            if self._call_slots is None:
                self._call_slots = asyncio.Semaphore(BULK_CALL_SLOTS)
            slots = self._call_slots
        # The slot is held for the THREAD's lifetime, released from its
        # completion callback -- not scoped to this await, which a wait_for
        # timeout can cancel while the thread is still stuck in a syscall.
        # Scoping it here would un-bound the thread count in exactly the
        # hung-store case the cap exists for.
        await slots.acquire()
        self._enter_inflight(is_lease)
        future: asyncio.Future = loop.create_future()

        def _resolve(result: Any, exc: Optional[BaseException]) -> None:
            slots.release()
            self._exit_inflight(is_lease)
            if future.cancelled():
                return  # the awaiter timed out / went away: nobody to tell
            if exc is not None:
                future.set_exception(exc)
            else:
                future.set_result(result)

        def _runner() -> None:
            result: Any = None
            exc: Optional[BaseException] = None
            began = time.perf_counter()
            try:
                result = fn(*args)
            except BaseException as ex:  # noqa: BLE001 - relayed to awaiter
                exc = ex
            # Stats update on the worker thread (never lost to an abandoned
            # await): an op stuck in a syscall simply reports when it
            # finally returns, which is exactly the latency worth seeing.
            elapsed = time.perf_counter() - began
            with self._stats_lock:
                entry = self._op_stats.setdefault(op, [0, 0, 0.0])
                entry[0] += 1
                if exc is not None:
                    entry[1] += 1
                entry[2] += elapsed
            try:
                loop.call_soon_threadsafe(_resolve, result, exc)
            except RuntimeError:
                # the loop already closed (late finish during teardown):
                # nothing is waiting; drop the result (the slot stays taken,
                # which is moot -- the loop is gone).
                pass

        try:
            threading.Thread(
                target=_runner, daemon=True, name="cronstable-state"
            ).start()
        except BaseException:
            slots.release()  # the thread never ran; nobody else will free it
            self._exit_inflight(is_lease)
            raise
        return cast(_T, await future)

    def _enter_inflight(self, is_lease: bool) -> None:
        """Count a just-acquired worker slot (and track the lane's peak)."""
        with self._stats_lock:
            if is_lease:
                self._inflight_lease += 1
                self._inflight_peak_lease = max(
                    self._inflight_peak_lease, self._inflight_lease
                )
            else:
                self._inflight_bulk += 1
                self._inflight_peak_bulk = max(
                    self._inflight_peak_bulk, self._inflight_bulk
                )

    def _exit_inflight(self, is_lease: bool) -> None:
        """Release a worker slot from the live gauge (peak is left intact)."""
        with self._stats_lock:
            if is_lease:
                self._inflight_lease -= 1
            else:
                self._inflight_bulk -= 1

    # --- lifecycle -------------------------------------------------------

    @property
    def topology(self) -> str:
        return self._topology

    async def start(self) -> None:
        await self._call("start", self._start_sync)
        logger.info(
            "state: filesystem backend ready at %s "
            "(namespace=%s, topology=%s, shared_locking=%s)",
            self.base,
            self.namespace,
            self._topology,
            self.supports_shared_locking(),
        )

    def _start_sync(self) -> None:
        # 0o700 (further narrowed by the umask): records and archived output
        # can carry job output -- secrets, even post-redaction -- so nothing
        # here should be world-readable on a multi-user host.  Only applied to
        # directories this process creates; an operator who pre-created the
        # tree with wider modes has made that choice deliberately.
        for sub in (
            RECORDS_DIR,
            LEASES_DIR,
            QUARANTINE_DIR,
            TMP_DIR,
            DOCS_DIR,
            BLOBS_DIR,
        ):
            self._makedirs_durable(os.path.join(self.base, sub))
        self._topology = self._resolve_topology()
        # Fail start() loudly if the store is not actually writable (a bad
        # mount, wrong permissions) rather than silently swallowing every later
        # write: write, fsync and remove a tiny probe file.  start_stop_state
        # catches the OSError, logs it, and keeps running the in-memory path.
        probe = os.path.join(
            self.base, TMP_DIR, "startup-{}.probe".format(self._instance)
        )
        try:
            with open(probe, "wb") as fobj:
                fobj.write(b"ok")
                fobj.flush()
                os.fsync(fobj.fileno())
        finally:
            # remove the probe even when the write/fsync raised (a probe
            # that failed midway would otherwise leak into TMP_DIR on every
            # failed start retry).
            with contextlib.suppress(OSError):
                os.unlink(probe)
        self._stamp_meta_sync()

    def _stamp_meta_sync(self) -> None:
        """Stamp a fresh store with the record-scheme version (once).

        The per-record ``schemaVersion`` already isolates unreadable records
        (quarantine on read); this stream-level stamp is the *upfront* signal:
        a store last written by a build with a NEWER scheme logs one pointed
        warning at start instead of quietly quarantining history record by
        record.  Read raw (not via ``_read_record``): a newer-versioned stamp
        is exactly the record whose version mismatch is meaningful, and the
        normal reader would quarantine it.  Best-effort throughout -- the
        stamp is advisory, never load-bearing.
        """
        stream_dir = self._stream_dir("meta")
        try:
            names = sorted(
                n for n in os.listdir(stream_dir) if n.endswith(".json")
            )
        except OSError:
            names = []
        for name in reversed(names):
            try:
                with open(os.path.join(stream_dir, name), "rb") as fobj:
                    obj = _json.loads(fobj.read())
            except Exception:  # noqa: BLE001 - unreadable stamp: keep looking
                continue
            if isinstance(obj, dict) and "schemaVersion" in obj:
                version = obj.get("schemaVersion")
                if version != SCHEME_VERSION:
                    logger.warning(
                        "state: the store at %s was last stamped by a build "
                        "writing record scheme %r (this build writes %r); "
                        "records this build cannot read are quarantined -- "
                        "consider `cronstable state migrate-schema`",
                        self.base,
                        version,
                        SCHEME_VERSION,
                    )
                return
        with contextlib.suppress(OSError):
            self._append_sync("meta", {"storeVersion": SCHEME_VERSION})

    def _resolve_topology(self) -> str:
        configured = self._configured_topology
        detected = detect_topology(self.root)
        if configured in ("shared", "single-node"):
            if detected is not None and detected != configured:
                logger.warning(
                    "state: topology configured as %r but the mount at %s "
                    "looks %r; trusting the configured value (make sure the "
                    "mount really does%s support cross-host locking)",
                    configured,
                    self.root,
                    detected,
                    "" if configured == "shared" else " not",
                )
            return configured
        # auto
        if detected is None:
            logger.info(
                "state: could not determine whether %s is a shared mount; "
                "assuming single-node (set state.topology: shared to enable "
                "fleet-wide coordination on a network mount)",
                self.root,
            )
            return "single-node"
        return detected

    async def stop(self) -> None:
        # Nothing to tear down: there are no background tasks and no long-lived
        # open handles (each op opens, acts, closes).  The filesystem is the
        # state.  Present for symmetry with the ABC and future connection-held
        # backends.
        return None

    # --- record store ----------------------------------------------------

    @staticmethod
    def _replace(src: str, dest: str) -> None:
        """``os.replace`` that rides out Windows sharing violations.

        On Windows, replacing a file another handle has open (the deliberately
        unlocked ``read_lease``, an antivirus/backup scan) raises
        ``PermissionError`` because CPython opens files without
        FILE_SHARE_DELETE.  Such holds are transient by nature, so retry
        briefly before giving up; on POSIX this is a single plain replace.
        """
        if not IS_WINDOWS:
            os.replace(src, dest)
            return
        for attempt in range(5):
            try:
                os.replace(src, dest)
                return
            except PermissionError:
                if attempt == 4:
                    raise
                time.sleep(0.02 * (attempt + 1))

    @staticmethod
    def _unlink(path: str) -> None:
        """``os.unlink`` that rides out Windows sharing violations.

        The delete-side twin of :meth:`_replace`: unlinking a file another
        handle transiently has open (a concurrent read/list on another
        worker thread, an antivirus/backup scan) raises ``PermissionError``
        on Windows because CPython opens files without FILE_SHARE_DELETE.
        Such holds clear in milliseconds, so retry briefly instead of
        surfacing a spurious error from a healthy store; on POSIX this is a
        single plain unlink.
        """
        if not IS_WINDOWS:
            os.unlink(path)
            return
        for attempt in range(5):
            try:
                os.unlink(path)
                return
            except PermissionError:
                if attempt == 4:
                    raise
                time.sleep(0.02 * (attempt + 1))

    def _atomic_write(self, dest: str, payload: bytes) -> None:
        """Write ``payload`` to ``dest`` via a temp file + atomic rename.

        The rename is atomic on a local filesystem, on Windows (os.replace),
        and -- crucially -- on an Amazon S3 Files mount, where *file* rename is
        atomic even though the underlying object store has no native rename.  A
        reader therefore never observes a half-written ``dest``.

        Data files are created 0o600 (narrowed further by the umask): records
        and archived output can carry job output, which is exactly where
        secrets live.  After the rename the parent directory is flushed (see
        :func:`cronstable.platform.fsync_directory`), because without it the
        rename itself is not crash-durable -- a power loss could silently
        drop an acknowledged record, regress the derived watermark, and
        double-run jobs on the next boot.
        """
        tmp = self._tmp_path()
        try:
            fdesc = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fdesc, "wb") as fobj:
                fobj.write(payload)
                fobj.flush()
                os.fsync(fobj.fileno())
            self._replace(tmp, dest)
        except BaseException:
            # never leave the temp file behind on a failed write/rename
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            raise
        fsync_directory(os.path.dirname(dest))

    def _makedirs_durable(self, path: str) -> None:
        """``os.makedirs(path, exist_ok=True)``, but crash-durably.

        A freshly created stream/namespace/blob-shard directory can have
        every file written into it individually fsynced, yet the directory
        ENTRY that makes the subtree reachable from its parent was never
        itself made durable -- a power loss right after can drop the whole
        newly-created subtree (parent and all), taking every acknowledged
        record inside it with it.  Walks up from ``path`` to the first
        already-existing ancestor *before* creating anything, so exactly the
        newly-created levels are known; after ``makedirs``, flushes each
        newly-created directory's PARENT (the parent is where the "this
        subdirectory exists" entry actually lives) -- which is exactly the
        pre-existing ancestor plus every newly-created level except the
        leaf itself (the leaf's own directory entry is covered by whichever
        write follows into it, e.g. :meth:`_atomic_write`).
        """
        if os.path.isdir(path):
            return
        created = []
        cur = path
        while cur and not os.path.isdir(cur):
            created.append(cur)
            parent = os.path.dirname(cur)
            if parent == cur:
                break
            cur = parent
        os.makedirs(path, mode=0o700, exist_ok=True)
        for level in created:
            fsync_directory(os.path.dirname(level))

    async def append_record(self, stream: str, data: Dict[str, Any]) -> str:
        return await self._call("append", self._append_sync, stream, data)

    def _append_sync(self, stream: str, data: Dict[str, Any]) -> str:
        stream_dir = self._stream_dir(stream)
        self._makedirs_durable(stream_dir)
        token = os.path.basename(stream_dir)
        if _FS_TRUNCATION_MARKER in token:
            # a truncated token cannot round-trip through enumeration on its
            # own; land (or lazily repair) the logical-name sidecar so
            # list_stream_names can return the exact name.
            self._ensure_stream_name_sidecar(stream_dir, token, stream)
        # Filename sort key is the write-time epoch (zero-padded so it sorts
        # lexicographically == chronologically), then instance+seq for
        # uniqueness.  The record's own logical timestamp lives in ``data`` and
        # is what derive_max reads; the filename only orders listing.
        rec_id = "{:020.6f}-{}-{:012d}".format(
            _now(), self._instance, self._next_seq()
        )
        payload = _json.dumps_bytes(
            {"schemaVersion": SCHEME_VERSION, "data": data}, sort_keys=True
        )
        self._atomic_write(os.path.join(stream_dir, rec_id + ".json"), payload)
        return rec_id

    def _quarantine(self, path: str, name: str, reason: str) -> None:
        dest = os.path.join(
            self.base,
            QUARANTINE_DIR,
            "{}.{}.bad".format(name, self._instance),
        )
        try:
            self._replace(path, dest)
            logger.warning(
                "state: quarantined corrupt record %s (%s)", name, reason
            )
            # stamp the QUARANTINE time: rename preserves the original write
            # mtime, and the GC quarantine sweep ages by mtime -- without
            # this, an old-written poison record could be swept in the same
            # pass it was quarantined, losing the forensics window.
            with contextlib.suppress(OSError):
                os.utime(dest, None)
        except OSError:
            # already moved/removed by another pass or node, or unwritable:
            # never let cleanup of a poison record raise into a read.
            pass

    def _read_record(
        self, stream_dir: str, name: str, *, strict: bool = False
    ) -> Optional[Dict[str, Any]]:
        path = os.path.join(stream_dir, name)
        try:
            with open(path, "rb") as fobj:
                obj = _json.loads(fobj.read())
        except FileNotFoundError:
            # raced away (pruned/quarantined) between listdir and open: skip.
            return None
        except OSError as ex:
            # An I/O error is the ENVIRONMENT failing (an NFS blip, an AV
            # scanner's transient hold), not the record: skip it for this
            # read but leave it in place.  Quarantining here would eject
            # perfectly valid history -- and regress the derived watermark --
            # on every store hiccup.
            hint = ""
            if isinstance(ex, PermissionError):
                # data files are deliberately 0o600 (they carry job output):
                # a persistent EACCES here usually means two nodes run as
                # DIFFERENT users against one shared store, which silently
                # hides half the history -- worth a pointed hint.
                hint = (
                    " (records are created 0o600: every node sharing this "
                    "store must run as the same user)"
                )
            logger.warning(
                "state: cannot read record %s (%s); leaving it in place%s",
                name,
                ex,
                hint,
            )
            if strict:
                # A derived-watermark/cursor read MUST fail closed on an
                # environmental error: silently dropping this record would
                # let derive_max return the max over the surviving subset --
                # a value strictly BELOW the true max -- and the catch-up
                # caller would replay an occurrence that already ran.
                # Propagate so the caller treats the watermark as UNKNOWN
                # (defer/retry), never as a lower value.  (A content-bad
                # record is still skipped even here: it is unrecoverable, and
                # failing closed on it forever would wedge the watermark.)
                raise
            return None
        except Exception:  # noqa: BLE001 - any content-driven parse failure
            # The CONTENT is bad: invalid/truncated JSON (ValueError), or a
            # hostile shape like >1000-deep nesting (RecursionError).  This
            # must catch everything content-dependent -- a poison record that
            # raised out of here would escape quarantine and crash whichever
            # caller is reading the stream ("never fatal" is the invariant).
            self._quarantine(path, name, "unreadable-or-invalid-json")
            return None
        if not isinstance(obj, dict) or not isinstance(obj.get("data"), dict):
            # the SHAPE is wrong regardless of schemaVersion: not a genuine
            # record this or any version of this code ever wrote.
            # Unrecoverable, so quarantine.
            self._quarantine(path, name, "unknown-schema")
            return None
        if obj.get("schemaVersion") != SCHEME_VERSION:
            # Well-formed, just a schema version this build does not
            # recognise -- almost always a NEWER version written by a peer
            # ahead in a rolling upgrade, not corruption. Quarantining (i.e.
            # deleting) it here would let an old node erase a new node's
            # records fleet-wide the moment it starts reading a shared store
            # mid-upgrade, losing whatever it encoded (a retry ladder, a
            # dedupe marker, ...). Leave it in place, exactly like the
            # environmental-error branch above: this build simply cannot
            # interpret it (yet).
            logger.warning(
                "state: record %s has unrecognised schemaVersion %r; "
                "leaving it in place (likely written by a newer version)",
                name,
                obj.get("schemaVersion"),
            )
            if strict:
                # Mirrors the environmental-error branch: a derived
                # watermark/cursor read must fail closed rather than silently
                # compute the max over the subset it understood, which could
                # be a value strictly below the true max.
                raise _DocumentUnreadable(
                    "record {} has unrecognised schemaVersion {!r}".format(
                        name, obj.get("schemaVersion")
                    )
                )
            return None
        data: Dict[str, Any] = obj["data"]
        return data

    async def list_records(
        self,
        stream: str,
        *,
        limit: Optional[int] = None,
        newest_first: bool = False,
        strict: bool = False,
    ) -> List[Dict[str, Any]]:
        return await self._call(
            "list", self._list_sync, stream, limit, newest_first, strict
        )

    async def list_stream_names(self, prefix: str) -> List[str]:
        return await self._call(
            "list-stream-names", self._list_stream_names_sync, prefix
        )

    async def list_stream_names_audit(
        self, prefix: str
    ) -> Tuple[List[str], bool]:
        return await self._call(
            "list-stream-names", self._list_stream_names_audit_sync, prefix
        )

    def _read_stream_name_sidecar(
        self, stream_dir: str, token: str
    ) -> Optional[str]:
        """The logical stream name recorded inside a truncated stream dir.

        ``None`` when the sidecar is absent, unreadable, or fails the
        round-trip check: ``_fs_safe(name)`` must reproduce ``token``
        exactly, or the name is corrupt/foreign and handing it to a keep-set
        builder would protect the WRONG token while this one gets collected.
        """
        path = os.path.join(stream_dir, _STREAM_NAME_SIDECAR)
        try:
            with open(path, "rb") as fobj:
                name = fobj.read().decode("utf-8")
        except (OSError, UnicodeDecodeError):
            return None
        if not name or _fs_safe(name) != token:
            return None
        return name

    def _ensure_stream_name_sidecar(
        self, stream_dir: str, token: str, stream: str
    ) -> None:
        """Durably record a truncated stream's exact logical name.

        A length-truncated token cannot be decoded back to its logical name
        (the digest replaced the tail), so without the sidecar
        :meth:`list_stream_names` would hand every consumer a garbled name
        that re-encodes to a different token -- and the GC keep-set built
        from it would miss this stream entirely.  Best-effort: a failed
        sidecar write must never fail the append it rides on (the stream is
        then merely skipped by enumeration until a later append lands it).
        """
        if self._read_stream_name_sidecar(stream_dir, token) == stream:
            return
        with contextlib.suppress(OSError):
            self._atomic_write(
                os.path.join(stream_dir, _STREAM_NAME_SIDECAR),
                stream.encode("utf-8"),
            )

    def _list_stream_names_sync(self, prefix: str) -> List[str]:
        return self._list_stream_names_audit_sync(prefix)[0]

    def _list_stream_names_audit_sync(
        self, prefix: str
    ) -> Tuple[List[str], bool]:
        from urllib.parse import unquote

        records_root = os.path.join(self.base, RECORDS_DIR)
        token_prefix = _fs_safe_fragment(prefix)
        try:
            tokens = os.listdir(records_root)
        except FileNotFoundError:
            # no store written yet: exhaustively empty, not unreadable.
            return [], True
        except OSError:
            return [], False
        names: List[str] = []
        complete = True
        for token in tokens:
            if not token.startswith(token_prefix):
                continue
            stream_dir = os.path.join(records_root, token)
            if not os.path.isdir(stream_dir):
                continue
            if _FS_TRUNCATION_MARKER in token:
                # a length-truncated token is not decodable: only its name
                # sidecar knows the logical name.  A stream without a
                # verifiable sidecar is SKIPPED, never returned garbled --
                # a garbled name re-encodes to a different token, so a GC
                # keep-set built from it would miss the real stream and its
                # host's state would be collected as garbage.  The skip is
                # reported through ``complete`` so the orphan-blob sweep can
                # tell this listing hides a stream (whose records may still
                # reference blobs) and keep instead.
                name = self._read_stream_name_sidecar(stream_dir, token)
                if name is not None:
                    names.append(name)
                else:
                    complete = False
                continue
            names.append(unquote(token, errors="replace"))
        return sorted(names), complete

    def _list_sync(
        self,
        stream: str,
        limit: Optional[int],
        newest_first: bool,
        strict: bool = False,
    ) -> List[Dict[str, Any]]:
        stream_dir = self._stream_dir(stream)
        try:
            names = sorted(
                n for n in os.listdir(stream_dir) if n.endswith(".json")
            )
        except FileNotFoundError:
            return []
        if newest_first:
            names.reverse()
        out: List[Dict[str, Any]] = []
        for name in names:
            if limit is not None and len(out) >= limit:
                break
            data = self._read_record(stream_dir, name, strict=strict)
            if data is not None:
                out.append(data)
        return out

    async def derive_max(self, stream: str, field: str) -> Optional[Any]:
        return await self._call(
            "derive-max", self._derive_max_sync, stream, field
        )

    def _derive_max_sync(self, stream: str, field: str) -> Optional[Any]:
        best: Optional[Any] = None
        # strict=True: an environmental read error must PROPAGATE (fail the
        # whole derive), never silently shrink the max -- see _read_record.
        for data in self._list_sync(stream, None, False, strict=True):
            if field not in data:
                continue
            value = data[field]
            if best is None:
                best = value
                continue
            try:
                if value > best:
                    best = value
            except TypeError:
                # incomparable types in one stream (a caller bug); keep the
                # first-seen rather than raising out of a cursor read.
                continue
        return best

    async def prune_records(self, stream: str, *, keep: int) -> int:
        return await self._call("prune", self._prune_sync, stream, keep)

    def _prune_sync(self, stream: str, keep: int) -> int:
        stream_dir = self._stream_dir(stream)
        try:
            names = sorted(
                n for n in os.listdir(stream_dir) if n.endswith(".json")
            )
        except FileNotFoundError:
            return 0
        # names sort chronologically (write-epoch filename prefix); keep the
        # newest ``keep`` (the tail), delete the rest.  keep <= 0 -> all.
        to_delete = names if keep <= 0 else names[:-keep]
        deleted = 0
        for name in to_delete:
            try:
                os.unlink(os.path.join(stream_dir, name))
                deleted += 1
            except OSError:
                # already gone (raced with another prune/node): ignore.
                pass
        return deleted

    # --- lease -----------------------------------------------------------

    @contextlib.contextmanager
    def _locked(
        self, lock_path: str, *, touch: bool = False
    ) -> Iterator[None]:
        """Hold the advisory exclusive lock on ``lock_path`` for the block.

        The lock file is separate from the ``.lease`` data file on purpose: the
        data file is replaced by an atomic rename, which would swap the inode
        out from under a lock taken on it; locking a stable side-file avoids
        that entirely.

        Lock files are also RECLAIMED by garbage collection (a dead
        ephemeral lease's, an orphaned idle lock's -- see
        :meth:`_gc_orphan_locks_sync`; never on any hot path), so a
        waiter can win the flock on an inode that was unlinked while it
        waited -- a ghost nobody arriving later will ever contend on.  After
        acquiring, re-verify the path still names the locked inode and
        re-open if not; without this, one mutator serialises on the ghost
        while another serialises on the reclaimer's replacement file, and
        the mutual exclusion silently splits across two inodes.  Sound on a
        local filesystem, where ``os.stat`` cannot be stale.

        ``touch`` (the document lane only) refreshes the lock file's mtime
        after acquiring: a flock never updates mtime, and the orphan-lock
        sweep uses mtime as the activity signal that keeps a live
        document's lock out of its reach.
        """
        self._makedirs_durable(os.path.dirname(lock_path))
        began = time.perf_counter()
        while True:
            fdesc = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                # msvcrt.locking needs a byte present to lock; guarantee one.
                if os.fstat(fdesc).st_size == 0:
                    os.write(fdesc, b"\0")
                with exclusive_file_lock(fdesc):
                    try:
                        same = os.path.samestat(
                            os.fstat(fdesc), os.stat(lock_path)
                        )
                    except OSError:
                        same = False
                    if not same:
                        continue  # ghost inode: re-open and contend afresh
                    if touch:
                        # best-effort: a missed touch only ages the lock
                        # towards the sweep, which still requires the
                        # document ABSENT and runs under this same flock.
                        with contextlib.suppress(OSError):
                            if os.utime in os.supports_fd:
                                os.utime(fdesc)
                            else:
                                os.utime(lock_path)
                    # time-to-acquire is the contention signal: near zero on
                    # an idle store, and the cross-host wait on a fought-over
                    # lease.
                    waited = time.perf_counter() - began
                    with self._stats_lock:
                        self._lock_acquisitions += 1
                        self._lock_wait_seconds += waited
                    yield
                    return
            finally:
                os.close(fdesc)

    def _read_lease_file(
        self, lease_path: str, *, strict: bool = False
    ) -> Optional[Lease]:
        """Read a lease file; ``None`` means *positively absent*.

        With ``strict`` (the locked read-modify-write paths), anything short
        of plain absence -- a transient I/O error, corrupt content -- raises
        :class:`_LeaseUnreadable` instead of returning ``None``.  Conflating
        "unreadable right now" with "no lease" would let one NFS blip steal a
        valid, unexpired lease from its live holder and re-issue a stale
        fence.  The unlocked observer (:meth:`read_lease`) stays best-effort.
        """
        try:
            with open(lease_path, "rb") as fobj:
                obj = _json.loads(fobj.read())
        except FileNotFoundError:
            return None
        except Exception as ex:  # noqa: BLE001 - classified below
            if strict:
                raise _LeaseUnreadable(str(ex)) from ex
            return None
        try:
            if not isinstance(obj, dict):
                raise TypeError("lease file is not a JSON object")
            return Lease(
                name=str(obj["name"]),
                holder=str(obj["holder"]),
                fence=int(obj["fence"]),
                expires_at=float(obj["expiresAt"]),
            )
        except (KeyError, TypeError, ValueError) as ex:
            # corrupt content: _write_lease_file is atomic so this should not
            # happen; failing closed (deny) beats guessing at a fence.
            if strict:
                raise _LeaseUnreadable(str(ex)) from ex
            return None

    def _write_lease_file(self, lease_path: str, lease: Lease) -> None:
        payload = _json.dumps_bytes(lease.to_dict(), sort_keys=True)
        self._atomic_write(lease_path, payload)

    async def acquire_lease(
        self, name: str, holder: str, ttl: float
    ) -> Optional[Lease]:
        return await self._call(
            "lease-acquire", self._acquire_sync, name, holder, ttl
        )

    def _acquire_sync(
        self, name: str, holder: str, ttl: float
    ) -> Optional[Lease]:
        lock_path, lease_path = self._lease_paths(name)
        with self._locked(lock_path):
            try:
                current = self._read_lease_file(lease_path, strict=True)
            except _LeaseUnreadable as ex:
                # fail CLOSED: an unreadable lease is not a free lease.
                logger.warning(
                    "state: lease %s unreadable (%s); denying acquire",
                    name,
                    ex,
                )
                return None
            now = _now()
            if (
                current is not None
                and current.holder != holder
                and current.expires_at > now
            ):
                # validly held by someone else: deny.
                return None
            if current is None:
                fence = 1
            elif current.holder == holder and current.expires_at > now:
                # a renew of our own still-valid lease keeps the fence.
                fence = current.fence
            else:
                # taking over an EXPIRED (or released) lease -- even our own:
                # bump the fence so any late writes issued under the previous
                # incarnation can be fenced off.  Monotonic because release
                # marks the lease expired in place instead of deleting it.
                fence = current.fence + 1
            lease = Lease(
                name=name,
                holder=holder,
                fence=fence,
                expires_at=now + ttl,
            )
            try:
                self._write_lease_file(lease_path, lease)
            except OSError as ex:
                # a write that cannot land (Windows sharing violation past
                # the retries, a read-only blip) means we did NOT acquire:
                # deny, never raise out of the lease API.
                logger.warning(
                    "state: lease %s write failed (%s); denying acquire",
                    name,
                    ex,
                )
                return None
            return lease

    async def renew_lease(self, lease: Lease, ttl: float) -> Optional[Lease]:
        return await self._call("lease-renew", self._renew_sync, lease, ttl)

    def _renew_sync(self, lease: Lease, ttl: float) -> Optional[Lease]:
        lock_path, lease_path = self._lease_paths(lease.name)
        with self._locked(lock_path):
            try:
                current = self._read_lease_file(lease_path, strict=True)
            except _LeaseUnreadable as ex:
                # fail closed: without a trustworthy read we cannot prove we
                # still hold it.  Losing a renew is safe; renewing a lease
                # someone else took over is not.
                logger.warning(
                    "state: lease %s unreadable (%s); denying renew",
                    lease.name,
                    ex,
                )
                return None
            # Renew only if we still hold it: same holder AND same fence (a
            # takeover would have bumped the fence).  Allowed even a hair past
            # expiry, as long as nobody else took over in the meantime -- but
            # NOT past a release: a released lease is marked expired in place
            # with the same holder+fence, and a renew landing after our own
            # release (an in-flight renew loop racing shutdown) must not
            # silently resurrect it.
            if (
                current is None
                or current.holder != lease.holder
                or current.fence != lease.fence
                or current.expires_at <= 0.0
            ):
                return None
            renewed = Lease(
                name=lease.name,
                holder=lease.holder,
                fence=lease.fence,
                expires_at=_now() + ttl,
            )
            try:
                self._write_lease_file(lease_path, renewed)
            except OSError as ex:
                # cannot persist the extension: the holder must treat the
                # renew as failed (fail closed), not crash on it.
                logger.warning(
                    "state: lease %s write failed (%s); denying renew",
                    lease.name,
                    ex,
                )
                return None
            return renewed

    async def release_lease(self, lease: Lease) -> None:
        await self._call("lease-release", self._release_sync, lease)

    def _release_sync(self, lease: Lease) -> None:
        lock_path, lease_path = self._lease_paths(lease.name)
        with self._locked(lock_path):
            try:
                current = self._read_lease_file(lease_path, strict=True)
            except _LeaseUnreadable:
                # cannot prove ownership: leave it to expire by TTL.
                return
            if (
                current is not None
                and current.holder == lease.holder
                and current.fence == lease.fence
            ):
                # Mark expired IN PLACE rather than unlinking: the lease file
                # is the fence counter's only home, and deleting it would
                # reset the next acquire to fence=1 -- re-issuing fence values
                # already handed out and defeating stale-writer detection.
                with contextlib.suppress(OSError):
                    self._write_lease_file(
                        lease_path,
                        Lease(
                            name=lease.name,
                            holder=lease.holder,
                            fence=lease.fence,
                            expires_at=0.0,
                        ),
                    )

    async def read_lease(self, name: str) -> Optional[Lease]:
        _lock_path, lease_path = self._lease_paths(name)
        lease = await self._call(
            "lease-read", self._read_lease_file, lease_path
        )
        if lease is not None and lease.expires_at <= 0.0:
            # a released lease: observers see "nobody holds it".
            return None
        return lease

    # --- mutable documents -----------------------------------------------

    def _read_doc_file(
        self, doc_path: str, *, strict: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Read a document body; ``None`` means *positively absent*.

        Mirrors :meth:`_read_lease_file`.  With ``strict`` (the locked RMW
        inside :meth:`mutate_document`), anything short of plain absence -- a
        transient I/O error, corrupt content, an unknown schema version --
        raises :class:`_DocumentUnreadable` so the mutation fails closed
        rather than clobbering a live value or reading a torn one.  Without
        ``strict`` (the best-effort :meth:`read_document` / list) it returns
        ``None`` for every one of those, so a single hiccup never crashes a
        read.
        """
        try:
            with open(doc_path, "rb") as fobj:
                obj = _json.loads(fobj.read())
        except FileNotFoundError:
            return None
        except Exception as ex:  # noqa: BLE001 - classified below
            if strict:
                raise _DocumentUnreadable(str(ex)) from ex
            return None
        if (
            not isinstance(obj, dict)
            or obj.get("schemaVersion") != SCHEME_VERSION
            or not isinstance(obj.get("data"), dict)
        ):
            if strict:
                raise _DocumentUnreadable("unknown-schema-or-not-a-document")
            return None
        return cast(Dict[str, Any], obj["data"])

    async def read_document(
        self, namespace: str, key: str
    ) -> Optional[Dict[str, Any]]:
        return await self._call(
            "doc-read", self._read_document_sync, namespace, key
        )

    def _read_document_sync(
        self, namespace: str, key: str
    ) -> Optional[Dict[str, Any]]:
        _lock_path, doc_path = self._doc_paths(namespace, key)
        return self._read_doc_file(doc_path)

    async def mutate_document(
        self,
        namespace: str,
        key: str,
        transform: Callable[[Optional[Dict[str, Any]]], Tuple[Any, _T]],
    ) -> Tuple[Optional[Dict[str, Any]], _T]:
        return await self._call(
            "doc-mutate", self._mutate_document_sync, namespace, key, transform
        )

    def _mutate_document_sync(
        self,
        namespace: str,
        key: str,
        transform: Callable[[Optional[Dict[str, Any]]], Tuple[Any, _T]],
    ) -> Tuple[Optional[Dict[str, Any]], _T]:
        lock_path, doc_path = self._doc_paths(namespace, key)
        # the lock file's directory is the namespace dir, created here so the
        # very first write to a fresh namespace has somewhere to land.
        self._makedirs_durable(os.path.dirname(lock_path))
        # ``touch``: every mutate refreshes the lock file's mtime, the idle
        # clock the GC orphan-lock sweep judges a doc ``.lock`` by.
        with self._locked(lock_path, touch=True):
            current = self._read_doc_file(doc_path, strict=True)
            new_body, result = transform(current)
            if new_body is DOC_KEEP:
                return current, result
            if new_body is DOC_DELETE:
                with contextlib.suppress(FileNotFoundError):
                    self._unlink(doc_path)
                    # without this, a released idempotency key or a
                    # deleted KV entry can RESURRECT after a power loss
                    # (the unlink never became durable), silently
                    # un-doing the delete and letting guarded once-only
                    # work run again.
                    fsync_directory(os.path.dirname(doc_path))
                # the ``.lock`` side-file is deliberately NOT unlinked
                # here: on a shared NFS/EFS store a waiter's post-acquire
                # ``os.stat`` re-verify can be answered from a stale
                # dentry/attribute cache, pass samestat against the ghost
                # inode, and split the document mutex across nodes.
                # Orphaned doc locks are reclaimed by the GC sweep
                # (:meth:`_gc_orphan_locks_sync`) once idle past the
                # grace window instead.
                return None, result
            if not isinstance(new_body, dict):
                raise TypeError(
                    "mutate_document transform must return a dict body, "
                    "DOC_KEEP or DOC_DELETE"
                )
            payload = _json.dumps_bytes(
                {"schemaVersion": SCHEME_VERSION, "data": new_body},
                sort_keys=True,
            )
            self._atomic_write(doc_path, payload)
            return new_body, result

    async def delete_document(self, namespace: str, key: str) -> bool:
        def _delete(current: Optional[Dict[str, Any]]) -> Tuple[Any, bool]:
            return DOC_DELETE, current is not None

        _stored, existed = await self.mutate_document(namespace, key, _delete)
        return existed

    async def list_documents(self, namespace: str) -> List[Dict[str, Any]]:
        return await self._call(
            "doc-list", self._list_documents_sync, namespace
        )

    async def list_document_namespaces(
        self, prefix: str
    ) -> Tuple[List[str], bool]:
        return await self._call(
            "doc-list", self._list_document_namespaces_sync, prefix
        )

    def _list_document_namespaces_sync(
        self, prefix: str
    ) -> Tuple[List[str], bool]:
        from urllib.parse import unquote

        docs_root = os.path.join(self.base, DOCS_DIR)
        token_prefix = _fs_safe_fragment(prefix)
        try:
            tokens = os.listdir(docs_root)
        except FileNotFoundError:
            # no document ever written: exhaustively empty, not unreadable.
            return [], True
        except OSError:
            return [], False
        names: List[str] = []
        complete = True
        for token in tokens:
            if not token.startswith(token_prefix):
                continue
            if not os.path.isdir(os.path.join(docs_root, token)):
                continue
            if _FS_TRUNCATION_MARKER in token:
                # a truncated namespace token is not decodable and (unlike a
                # record stream) has no logical-name sidecar to recover it
                # from: report the listing incomplete rather than hand a
                # garbled name to the GC, which would then collect the XCom
                # streams this namespace's run documents still anchor.
                complete = False
                continue
            names.append(unquote(token, errors="replace"))
        return sorted(names), complete

    def _list_documents_sync(self, namespace: str) -> List[Dict[str, Any]]:
        ns_dir = self._doc_dir(namespace)
        try:
            names = sorted(n for n in os.listdir(ns_dir) if n.endswith(".doc"))
        except FileNotFoundError:
            return []
        out: List[Dict[str, Any]] = []
        for name in names:
            data = self._read_doc_file(os.path.join(ns_dir, name))
            if data is not None:
                out.append(data)
        return out

    # --- content-addressed blobs -----------------------------------------

    async def put_blob(self, data: bytes) -> str:
        return await self._call("blob-put", self._put_blob_sync, data)

    def _put_blob_sync(self, data: bytes) -> str:
        digest = hashlib.sha256(data).hexdigest()
        path = self._blob_path(digest)
        # content-addressed: an existing blob with this digest already holds
        # exactly this payload, so skip the rewrite (and its fsync cost) --
        # but refresh its mtime, which is the orphan-blob sweep's age guard:
        # this payload was just (re)published and its new record has not
        # landed yet, so a concurrent sweep whose surviving references are
        # all mid-deletion must read it as too-young, not as an aged orphan.
        if os.path.exists(path):
            with contextlib.suppress(OSError):
                os.utime(path)
            return digest
        self._makedirs_durable(os.path.dirname(path))
        # _atomic_write renames over any existing file; a concurrent writer of
        # the same content is therefore harmless (identical bytes either way).
        self._atomic_write(path, data)
        return digest

    async def get_blob(self, digest: str) -> Optional[bytes]:
        return await self._call("blob-get", self._get_blob_sync, digest)

    def _get_blob_sync(self, digest: str) -> Optional[bytes]:
        try:
            with open(self._blob_path(digest), "rb") as fobj:
                return fobj.read()
        except FileNotFoundError:
            return None
        except OSError as ex:
            # a transient read error is the environment, not a missing blob:
            # surface it (the awaiter can retry) rather than reporting absence.
            logger.warning("state: cannot read blob %s (%s)", digest, ex)
            raise

    # --- lock-fidelity probe -------------------------------------------------

    async def verify_locking(self) -> Optional[str]:
        """Probe whether the store's advisory locks actually exclude.

        Returns ``None`` when the locks behave, else a human-readable reason
        they must not be trusted for coordination.  Two checks:

        * a **functional** probe: lock a scratch file through one file
          descriptor, then attempt a non-blocking exclusive lock through a
          second descriptor of the same file.  On every real lock
          implementation the second attempt fails with contention (POSIX
          ``flock`` is per-open-file-description, Windows byte-range locks
          are per-handle); a mount whose locks are silent no-ops (some FUSE
          filesystems) grants it -- positive proof the TTL lease's mutual
          exclusion is fiction;
        * a **mount-option** sniff (Linux): an NFS mount carrying ``nolock``
          or ``local_lock=flock``/``all`` satisfies flock host-locally, so
          the functional probe passes on every node while no lock ever
          reaches the server -- the silent cross-host double-run.

        Honest limits: both checks run on one host, so a mount whose locks
        are real locally but not propagated across hosts (the ``local_lock``
        case on a platform without ``/proc/mounts`` -- Windows, macOS) is
        undetectable here; that residual rests on the operator's
        ``topology`` assertion and is documented.  A probe that cannot run
        (I/O error) is inconclusive and reports ``None`` rather than
        refusing a healthy store on a blip.
        """
        return await self._call("lock-probe", self._verify_locking_sync)

    def _verify_locking_sync(self) -> Optional[str]:
        reason = _local_lock_reason(self.root)
        if reason is not None:
            return (
                "{}, so file locks are host-local and cannot fence "
                "other nodes".format(reason)
            )
        probe = os.path.join(
            self.base, TMP_DIR, "lock-probe-{}".format(self._instance)
        )
        fd1 = fd2 = -1
        try:
            fd1 = os.open(probe, os.O_RDWR | os.O_CREAT, 0o600)
            # msvcrt.locking needs a byte present to lock; guarantee one.
            if os.fstat(fd1).st_size == 0:
                os.write(fd1, b"\0")
            fd2 = os.open(probe, os.O_RDWR)
            with exclusive_file_lock(fd1, blocking=False):
                try:
                    with exclusive_file_lock(fd2, blocking=False):
                        return (
                            "the mount at {} grants two exclusive locks on "
                            "one file (its locks are no-ops)".format(self.root)
                        )
                except OSError:
                    # contention: the second descriptor was refused while
                    # the first held the lock -- locks genuinely exclude.
                    return None
        except OSError as ex:
            logger.debug("state: lock-fidelity probe inconclusive: %s", ex)
            return None
        finally:
            for fdesc in (fd1, fd2):
                if fdesc != -1:
                    with contextlib.suppress(OSError):
                        os.close(fdesc)
            with contextlib.suppress(OSError):
                os.unlink(probe)

    # --- maintenance -------------------------------------------------------

    async def collect_garbage(
        self,
        *,
        keep: Dict[str, Set[str]],
        grace: float,
        ephemeral_lease_prefixes: Tuple[str, ...] = (),
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Remove durable state nothing references anymore.

        ``keep`` maps a managed stream *prefix* (``"runs/"``, ``"logs/"``,
        ...) to the set of suffixes (job names, hosts) that must survive --
        the caller derives it from the recent manifests plus its own loaded
        config, which is what anchors cross-jobset GC to the deployment
        rather than to any single node's job set.  A stream is deleted only
        when it POSITIVELY matches a managed prefix, its suffix is not kept,
        AND its newest record is older than ``grace`` seconds (belt and
        braces for a store whose manifests are missing).  Anything that
        cannot be classified is kept -- including a length-truncated stream
        directory without a verifiable logical-name sidecar, whose name the
        keep-set builder could never have seen; :data:`PROTECTED_STREAMS`
        are never touched.

        Lease files: only the EPHEMERAL per-run classes named by
        ``ephemeral_lease_prefixes`` (the callers pass dagrun's
        ``dagadvance/`` prefix) are ever reclaimed, and then only when
        PROVABLY dead for the whole grace window -- both the recorded
        expiry and the last write (release marks expiry ``0.0`` in place,
        so the file mtime, not the expiry, dates a release) older than
        ``grace``.  Every other lease file is never deleted, whatever its
        age: a lease file is its fence counter's only home, and fence
        values are PERSISTED beyond it (a Replace-cancel record in a
        ``slots/<job>`` stream carries the fence it cancelled and stays
        newest until the next cancel), so a fence reset after ANY grace
        window can re-collide with such a record and silently cancel a
        healthy future run (see :meth:`_gc_leases_sync`).

        Orphaned ``.lock`` side-files -- a deleted document's, a reclaimed
        or half-reclaimed lease's -- are swept once idle past the grace
        window (:meth:`_gc_orphan_locks_sync`).  Also sweeps crash debris:
        write-temp files older than :data:`TMP_MAX_AGE` and quarantined
        records older than ``grace``.
        """
        return await self._call(
            "gc",
            self._gc_sync,
            keep,
            grace,
            ephemeral_lease_prefixes,
            dry_run,
        )

    def _gc_sync(
        self,
        keep: Dict[str, Set[str]],
        grace: float,
        ephemeral_lease_prefixes: Tuple[str, ...],
        dry_run: bool,
    ) -> Dict[str, Any]:
        now = _now()
        cutoff = now - max(0.0, grace)
        keep_tokens = {_fs_safe(stream) for stream in PROTECTED_STREAMS}
        prefix_tokens: List[str] = []
        for prefix, suffixes in keep.items():
            prefix_tokens.append(_fs_safe_fragment(prefix))
            for suffix in suffixes:
                keep_tokens.add(_fs_safe(prefix + suffix))
        removed_streams: List[str] = []
        removed_records = 0
        kept_streams = 0
        records_root = os.path.join(self.base, RECORDS_DIR)
        try:
            entries = sorted(os.listdir(records_root))
        except OSError:
            entries = []
        for token in entries:
            stream_dir = os.path.join(records_root, token)
            if not os.path.isdir(stream_dir):
                continue
            if token in keep_tokens or not any(
                token.startswith(p) for p in prefix_tokens
            ):
                # referenced, protected, or unrecognised: never delete what
                # is still wanted or cannot be classified.
                kept_streams += 1
                continue
            if (
                _FS_TRUNCATION_MARKER in token
                and self._read_stream_name_sidecar(stream_dir, token) is None
            ):
                # a length-truncated token with no verifiable name sidecar
                # (a legacy dir written before sidecars existed) was
                # invisible to the keep-set builder -- list_stream_names
                # skips it -- so its absence from ``keep`` proves nothing:
                # unclassifiable, keep.  The next append to the stream
                # lands the sidecar and makes it classifiable again.
                kept_streams += 1
                continue
            try:
                names = os.listdir(stream_dir)
            except OSError:
                kept_streams += 1
                continue
            records = [n for n in names if n.endswith(".json")]
            newest = max(
                (_record_epoch(n) for n in records), default=float("-inf")
            )
            if not records:
                # an empty managed dir: usually deletable debris, but a
                # writer may have JUST created it (its first record's temp
                # file is still being renamed in), so age the DIRECTORY
                # itself against the grace instead of deleting on sight.
                try:
                    newest = os.stat(stream_dir).st_mtime
                except OSError:
                    newest = float("inf")
            if newest > cutoff:
                kept_streams += 1
                continue
            removed_streams.append(token)
            removed_records += len(records)
            if dry_run:
                continue
            for name in names:
                with contextlib.suppress(OSError):
                    os.unlink(os.path.join(stream_dir, name))
            # a straggler unlink (Windows sharing hold) leaves the dir
            # non-empty; the rmdir then fails and the next pass converges.
            with contextlib.suppress(OSError):
                os.rmdir(stream_dir)
        leases_removed = self._gc_leases_sync(
            cutoff, dry_run, ephemeral_lease_prefixes
        )
        locks_removed = self._gc_orphan_locks_sync(cutoff, dry_run)
        tmp_removed = self._sweep_dir_sync(
            os.path.join(self.base, TMP_DIR), now - TMP_MAX_AGE, dry_run
        )
        quarantine_removed = self._sweep_dir_sync(
            os.path.join(self.base, QUARANTINE_DIR), cutoff, dry_run
        )
        return {
            "dry_run": dry_run,
            "streams_removed": len(removed_streams),
            "removed": removed_streams,
            "records_removed": removed_records,
            "streams_kept": kept_streams,
            "leases_removed": leases_removed,
            "locks_removed": locks_removed,
            "tmp_removed": tmp_removed,
            "quarantine_removed": quarantine_removed,
        }

    def _lease_dead_past_grace(self, lease_path: str, cutoff: float) -> bool:
        """Whether a lease was provably dead for the whole grace window.

        True only when BOTH the recorded expiry and the file's mtime (the
        last acquire/renew/release write -- release marks expiry ``0.0`` in
        place, so the expiry alone cannot date it) predate ``cutoff``.
        Every fence ever issued for the name then expired at least the
        grace window ago (each takeover happens strictly after its
        predecessor's expiry, and the release write postdates the fence it
        retires), so no live actor can still hold a stale ``Lease``.
        That alone does NOT make deletion safe: fence values can be
        persisted in durable records that outlive any grace window, which
        is why only the ephemeral lease classes are ever eligible (see
        :meth:`_gc_leases_sync`).  Anything unreadable is NOT reclaimable:
        never delete what cannot be classified.
        """
        try:
            mtime = os.stat(lease_path).st_mtime
            lease = self._read_lease_file(lease_path, strict=True)
        except (OSError, _LeaseUnreadable):
            return False
        if lease is None:
            return False
        return lease.expires_at < cutoff and mtime < cutoff

    def _gc_leases_sync(
        self,
        cutoff: float,
        dry_run: bool,
        ephemeral_prefixes: Tuple[str, ...],
    ) -> int:
        """Reclaim EPHEMERAL lease files dead past the grace window.

        Only a lease whose logical name matches one of
        ``ephemeral_prefixes`` is eligible; every other lease file is
        never deleted, whatever its age.  A lease file is its fence
        counter's only home, and fences are PERSISTED beyond it: a
        ``slots/<job>`` stream keeps ``{kind: cancel, fence: N}`` records
        (written by Replace takeovers, pruned only by the next cancel)
        that outlive any grace window, so resetting a slot or retry-claim
        fence lets a reborn fence re-collide with a stale cancel record
        and silently cancel a healthy future run.  Those bounded per-job
        names are harmless to keep forever anyway; only dagrun's per-run
        ``dagadvance/<dag>/<run_key>`` leases grow without bound (one
        uniquely-named file per DAG run).  Reclaiming those is safe: the
        name recurs only if the same run key is re-created after its run
        document was already GC'd, and no fence for it is persisted
        outside the run document's own lifetime -- the grace argument
        (:meth:`_lease_dead_past_grace`) covers every in-memory holder.

        The check-and-delete runs under the per-lease flock so it cannot
        race a concurrent re-acquire (which holds the same flock); the
        ``.lock`` sibling goes LAST -- an acquirer recreating it after the
        ``.lease`` vanished simply takes a fresh fence-1 lease.
        """
        if not ephemeral_prefixes:
            return 0
        # prefix matching happens on the encoded filename: _fs_safe only
        # rewrites a token's first character (whole-token reserved names)
        # or its over-length tail, so an encoded prefix survives verbatim
        # at the front -- same argument as the stream keep-set matching.
        prefix_tokens = tuple(
            _fs_safe_fragment(p) for p in ephemeral_prefixes if p
        )
        if not prefix_tokens:
            return 0
        removed = 0
        lease_root = os.path.join(self.base, LEASES_DIR)
        try:
            names = os.listdir(lease_root)
        except OSError:
            return 0
        for name in names:
            if not name.endswith(".lease"):
                continue
            token = name[: -len(".lease")]
            if not any(token.startswith(p) for p in prefix_tokens):
                continue  # non-ephemeral: never touched
            lease_path = os.path.join(lease_root, name)
            lock_path = os.path.join(lease_root, token + ".lock")
            # cheap unlocked pre-check: skip anything plausibly live before
            # paying for its flock.
            if not self._lease_dead_past_grace(lease_path, cutoff):
                continue
            if dry_run:
                removed += 1
                continue
            with self._locked(lock_path):
                # re-judge under the lock: a concurrent re-acquire may have
                # just revived (rewritten) it.
                if not self._lease_dead_past_grace(lease_path, cutoff):
                    continue
                with contextlib.suppress(OSError):
                    os.unlink(lease_path)
                    removed += 1
                if not IS_WINDOWS:
                    # drop the lock side-file while STILL HOLDING it:
                    # _locked re-verifies inode identity after acquiring,
                    # so waiters on this unlinked inode re-open instead of
                    # splitting the mutex with the recreated file.
                    with contextlib.suppress(OSError):
                        os.unlink(lock_path)
            if IS_WINDOWS:
                # post-release: our own handle is closed now; a concurrent
                # acquirer's open handle (no FILE_SHARE_DELETE) makes this
                # fail harmlessly; a lost race orphans a BARE .lock, which
                # the orphan-lock sweep (not this .lease-keyed loop)
                # converges on a later pass.
                with contextlib.suppress(OSError):
                    self._unlink(lock_path)
        if removed and not dry_run:
            # make the reclamation itself crash-durable, once per pass.
            fsync_directory(lease_root)
        return removed

    def _gc_orphan_locks_sync(self, cutoff: float, dry_run: bool) -> int:
        """Sweep ``.lock`` side-files whose owner is gone and idle past grace.

        Two orphan classes, both otherwise permanent:

        * a document ``.lock`` whose ``.doc`` is ABSENT -- ``DOC_DELETE``
          never unlinks the lock file (an eager unlink split the document
          mutex across nodes on NFS/EFS: a waiter's post-acquire stat
          re-verify can be served by a stale dentry/attribute cache and
          pass samestat against the ghost inode while another node locks a
          fresh file).  mutate_document touches the lock's mtime on every
          acquire, so idle-past-grace means no mutator ran for a whole
          grace window;
        * a BARE lease ``.lock`` with no ``.lease`` sibling -- the Windows
          post-release unlink in :meth:`_gc_leases_sync` can lose to a
          scanner's transient handle, and the ``.lease``-keyed loop never
          revisits the name.  No ``.lease`` means no durable fence, so no
          prefix restriction is needed here.

        Each candidate is re-judged and deleted under its own flock, with
        :meth:`_locked`'s ghost re-verify protecting any waiter.  Accepted
        residual risk, on shared mounts only: deleting a lock idle for >=
        the grace window can in principle race a waiter that opened it in
        the deletion instant and stat-verifies through a stale NFS cache;
        the idle-past-grace gate plus the daily GC cadence bounds this to
        a vanishing window, unlike the constant hot-path window the eager
        DOC_DELETE-time unlink had.
        """
        removed = 0
        docs_root = os.path.join(self.base, DOCS_DIR)
        try:
            ns_tokens = os.listdir(docs_root)
        except OSError:
            ns_tokens = []
        for ns_token in ns_tokens:
            ns_dir = os.path.join(docs_root, ns_token)
            if not os.path.isdir(ns_dir):
                continue
            try:
                names = os.listdir(ns_dir)
            except OSError:
                continue
            for name in names:
                if not name.endswith(".lock"):
                    continue
                lock_path = os.path.join(ns_dir, name)
                doc_path = os.path.join(ns_dir, name[: -len(".lock")]) + ".doc"
                if self._reclaim_idle_lock_sync(
                    lock_path, doc_path, cutoff, dry_run
                ):
                    removed += 1
        lease_root = os.path.join(self.base, LEASES_DIR)
        try:
            names = os.listdir(lease_root)
        except OSError:
            names = []
        for name in names:
            if not name.endswith(".lock"):
                continue
            lock_path = os.path.join(lease_root, name)
            lease_path = (
                os.path.join(lease_root, name[: -len(".lock")]) + ".lease"
            )
            if self._reclaim_idle_lock_sync(
                lock_path, lease_path, cutoff, dry_run
            ):
                removed += 1
        # no fsync: a lock unlink that never becomes durable merely
        # resurfaces the orphan for the next pass.
        return removed

    def _reclaim_idle_lock_sync(
        self, lock_path: str, sibling_path: str, cutoff: float, dry_run: bool
    ) -> bool:
        """Check-and-delete one orphaned ``.lock`` (see the sweep above).

        Reclaims only when the lock's mtime predates ``cutoff`` AND its
        owning data file (``sibling_path``) is absent, judged both before
        paying for the flock and again while holding it.
        """
        try:
            if os.stat(lock_path).st_mtime >= cutoff:
                return False
        except OSError:
            # gone or unreadable: nothing to reclaim / cannot classify.
            return False
        if os.path.exists(sibling_path):
            return False
        if dry_run:
            return True
        with self._locked(lock_path):
            # re-judge under the flock: a concurrent acquire may have just
            # touched the lock or re-created the sibling (and _locked's
            # O_CREAT may have re-created a fresh file if another sweeper
            # won the race -- its new mtime fails the age gate).
            try:
                if os.stat(lock_path).st_mtime >= cutoff:
                    return False
            except OSError:
                return False
            if os.path.exists(sibling_path):
                return False
            if not IS_WINDOWS:
                # unlink while STILL HOLDING the flock: waiters re-verify
                # inode identity and re-open instead of splitting the
                # mutex with a recreated file.
                with contextlib.suppress(OSError):
                    os.unlink(lock_path)
                return True
        # Windows: our own open handle forbids the unlink under the lock;
        # post-release, a concurrent acquirer's open handle (no
        # FILE_SHARE_DELETE) makes this fail harmlessly and a later pass
        # converges.
        with contextlib.suppress(OSError):
            self._unlink(lock_path)
        return True

    @staticmethod
    def _sweep_dir_sync(path: str, cutoff: float, dry_run: bool) -> int:
        """Unlink files under ``path`` last modified before ``cutoff``."""
        removed = 0
        try:
            names = os.listdir(path)
        except OSError:
            return 0
        for name in names:
            full = os.path.join(path, name)
            try:
                if not os.path.isfile(full):
                    continue
                if os.stat(full).st_mtime >= cutoff:
                    continue
                if not dry_run:
                    os.unlink(full)
                removed += 1
            except OSError:
                continue
        return removed

    async def migrate_schema(self, *, dry_run: bool = False) -> Dict[str, Any]:
        """Rewrite records of OLDER known schemes to the current one.

        Walks every record wrapper and, for a ``schemaVersion`` with a
        registered converter (:data:`RECORD_MIGRATIONS`), rewrites the file
        in place via the same temp-file + atomic rename as any write, so a
        concurrent reader never sees a torn record.  This is the one
        sanctioned exception to "records are never rewritten": an explicit,
        operator-run admin action (`cronstable state migrate-schema`) whose
        rewrite is a pure re-encoding of the same logical record.  Records
        with no converter are left alone (counted; the normal readers
        quarantine what they cannot parse), as are unreadable files.
        """
        return await self._call("migrate", self._migrate_sync, dry_run)

    def _migrate_sync(self, dry_run: bool) -> Dict[str, Any]:
        current = converted = unknown = unreadable = failed = 0
        records_root = os.path.join(self.base, RECORDS_DIR)
        try:
            streams = sorted(os.listdir(records_root))
        except OSError:
            streams = []
        for token in streams:
            stream_dir = os.path.join(records_root, token)
            if not os.path.isdir(stream_dir):
                continue
            try:
                names = sorted(
                    n for n in os.listdir(stream_dir) if n.endswith(".json")
                )
            except OSError:
                continue
            for name in names:
                path = os.path.join(stream_dir, name)
                try:
                    with open(path, "rb") as fobj:
                        obj = _json.loads(fobj.read())
                except Exception:  # noqa: BLE001 - quarantined on next read
                    unreadable += 1
                    continue
                version = (
                    obj.get("schemaVersion") if isinstance(obj, dict) else None
                )
                if version == SCHEME_VERSION:
                    current += 1
                    continue
                convert = RECORD_MIGRATIONS.get(str(version))
                data = obj.get("data") if isinstance(obj, dict) else None
                if convert is None or not isinstance(data, dict):
                    unknown += 1
                    continue
                try:
                    new_data = convert(data)
                except Exception:  # noqa: BLE001 - a converter bug, counted
                    failed += 1
                    continue
                if new_data is None:
                    unknown += 1
                    continue
                converted += 1
                if dry_run:
                    continue
                payload = _json.dumps_bytes(
                    {"schemaVersion": SCHEME_VERSION, "data": new_data},
                    sort_keys=True,
                )
                try:
                    self._atomic_write(path, payload)
                except OSError:
                    converted -= 1
                    failed += 1
        return {
            "dry_run": dry_run,
            "current": current,
            "converted": converted,
            "unknown": unknown,
            "unreadable": unreadable,
            "failed": failed,
        }

    async def sweep_orphan_blobs(
        self,
        referenced: Set[str],
        grace: float,
        *,
        dry_run: bool = False,
    ) -> int:
        """Delete artifact blobs no surviving record references.

        Content-addressed blobs outlive the artifact records that point at
        them only as debris: when a scope's ``artifacts/`` stream is garbage
        collected, its blobs become unreferenced.  ``referenced`` is the set
        of SHA-256 digests every surviving artifact record still names (the
        caller derives it from the store's live records); a blob is removed
        only when its digest is absent from that set AND it is older than
        ``grace`` seconds -- the age guard keeps a blob a writer has *just*
        landed but not yet recorded (the put-blob-then-append-record window)
        from being swept out from under the pending record.
        """
        return await self._call(
            "blob-sweep",
            self._sweep_orphan_blobs_sync,
            referenced,
            grace,
            dry_run,
        )

    def _sweep_orphan_blobs_sync(
        self, referenced: Set[str], grace: float, dry_run: bool
    ) -> int:
        cutoff = _now() - max(0.0, grace)
        removed = 0
        blobs_root = os.path.join(self.base, BLOBS_DIR)
        try:
            shards = os.listdir(blobs_root)
        except OSError:
            return 0
        for shard in shards:
            shard_dir = os.path.join(blobs_root, shard)
            try:
                names = os.listdir(shard_dir)
            except OSError:
                continue
            for name in names:
                if not name.endswith(".blob"):
                    continue
                digest = name[: -len(".blob")]
                if digest in referenced:
                    continue
                full = os.path.join(shard_dir, name)
                try:
                    if os.stat(full).st_mtime >= cutoff:
                        # too young: an in-flight put not yet recorded
                        continue
                    if not dry_run:
                        os.unlink(full)
                    removed += 1
                except OSError:
                    continue
            # a now-empty shard directory is harmless debris; drop it best
            # effort so the blob tree does not accumulate empty shards.
            if not dry_run:
                with contextlib.suppress(OSError):
                    os.rmdir(shard_dir)
        return removed

    # --- introspection ---------------------------------------------------

    def stats(self) -> Dict[str, Any]:
        with self._stats_lock:
            ops = {
                op: {
                    "count": int(entry[0]),
                    "errors": int(entry[1]),
                    "seconds": entry[2],
                }
                for op, entry in self._op_stats.items()
            }
            return {
                "ops": ops,
                "lock": {
                    "acquisitions": self._lock_acquisitions,
                    "wait_seconds": self._lock_wait_seconds,
                },
                "throttle": {
                    "count": self._throttled_ops,
                    "wait_seconds": self._throttle_wait_seconds,
                },
                # Live worker-lane occupancy.  ``*_inflight`` at its
                # ``*_capacity`` (especially sustained) is the "store wedged"
                # signal the op counters cannot show -- they only advance when
                # an op FINISHES, so a fully-hung mount otherwise reads idle.
                # The lease lane is separate, so a saturated bulk lane does
                # not imply lease renewals are blocked.
                "workers": {
                    "bulk_inflight": self._inflight_bulk,
                    "bulk_peak": self._inflight_peak_bulk,
                    "bulk_capacity": BULK_CALL_SLOTS,
                    "lease_inflight": self._inflight_lease,
                    "lease_peak": self._inflight_peak_lease,
                    "lease_capacity": LEASE_CALL_SLOTS,
                },
            }

    def view_dict(self) -> Dict[str, Any]:
        return {
            "backend": self.backend_name,
            "path": self.base,
            "namespace": self.namespace,
            "topology": self._topology,
            "shared_locking": self.supports_shared_locking(),
            "job_set_id": self.get_job_set_id(),
        }

    async def inventory(self) -> Dict[str, Any]:
        """Metadata-only topology snapshot (see the base docstring).

        Walks the on-disk tree off the event loop and returns per-prefix
        stream/document counts, capped scope lists, and active leases -- never
        a record payload or a document value.  Routed through :meth:`_call`
        like every other op -- never the default executor, whose non-daemon
        threads a dashboard polling this against a hung mount would wedge
        one by one until config reload (and interpreter exit) hang behind
        them; ``_call``'s abandonable daemon threads, lane cap and throttle
        exist for exactly that store.
        """
        base_dict = await self._call("inventory", self._inventory_sync)
        base_dict["view"] = self.view_dict()
        base_dict["stats"] = self.stats()
        base_dict["enumerable"] = True
        return base_dict

    def _inventory_sync(self) -> Dict[str, Any]:
        from urllib.parse import unquote

        cap = 200

        def decode(token: str) -> str:
            return unquote(token, errors="replace")

        def walk(root: str, suffix: str) -> Dict[str, Any]:
            # group per first path segment: {prefix: {count, streams, scopes}}
            groups: Dict[str, Dict[str, Any]] = {}
            try:
                tokens = sorted(os.listdir(root))
            except OSError:
                return groups
            for tok in tokens:
                node = os.path.join(root, tok)
                if not os.path.isdir(node):
                    continue
                try:
                    count = sum(
                        1 for n in os.listdir(node) if n.endswith(suffix)
                    )
                except OSError:
                    continue
                logical = decode(tok)
                prefix, _sep, scope = logical.partition("/")
                bucket = groups.setdefault(
                    prefix, {"count": 0, "streams": 0, "scopes": []}
                )
                bucket["count"] += count
                bucket["streams"] += 1
                if len(bucket["scopes"]) < cap:
                    bucket["scopes"].append({"scope": scope, "count": count})
            return groups

        records = walk(os.path.join(self.base, RECORDS_DIR), ".json")
        documents = walk(os.path.join(self.base, DOCS_DIR), ".doc")

        leases: List[Dict[str, Any]] = []
        lease_root = os.path.join(self.base, LEASES_DIR)
        now = _now()
        try:
            lease_files = sorted(os.listdir(lease_root))
        except OSError:
            lease_files = []
        for fname in lease_files:
            if not fname.endswith(".lease") or len(leases) >= cap:
                continue
            try:
                lease = self._read_lease_file(os.path.join(lease_root, fname))
            except Exception:  # noqa: BLE001 - best-effort observe
                lease = None
            if lease is None or lease.expires_at <= 0.0:
                continue  # released/absent lease: nobody holds it
            leases.append(
                {
                    "name": decode(fname[: -len(".lease")]),
                    "holder": lease.holder,
                    "fence": lease.fence,
                    "expiresAt": lease.expires_at,
                    "expired": lease.expires_at <= now,
                }
            )

        try:
            quarantine = len(
                os.listdir(os.path.join(self.base, QUARANTINE_DIR))
            )
        except OSError:
            quarantine = 0

        return {
            "records": records,
            "documents": documents,
            "leases": leases,
            "quarantine": quarantine,
        }


def make_state_backend(
    state_config: StateConfig,
    get_job_set_id: Callable[[], str],
) -> StateBackend:
    """Build the state backend for a ``state`` config section.

    Mirrors :func:`cronstable.leadership.make_backend`.  Today there is one
    backend -- ``filesystem`` -- because a local disk and an Amazon S3 Files
    mount are the same POSIX backend, distinguished only by the mount.  The
    factory (and the ``backend`` key it reads, defaulting to ``filesystem``)
    keeps the seam ready for a future native-S3 (SigV4/conditional-write)
    backend, which would be imported *lazily* here (like the lease backends)
    so it never enters the import graph unless selected.
    """
    backend = state_config.get("backend", "filesystem")
    if backend == "filesystem":
        return FilesystemStateBackend(state_config, get_job_set_id)
    raise ConfigError(  # pragma: no cover - no other backend yet / gated
        "unknown state.backend {!r}".format(backend)
    )
