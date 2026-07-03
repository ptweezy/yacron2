"""Optional durable state backend: one filesystem seam for local disk and
Amazon S3 Files.

yacron2 is stateless by default -- run history, retry counters, the next-fire
index and the leadership view all live in memory and reset on restart, and that
zero-disk story is a feature.  This module adds the *opt-in* other half: when a
``state`` config section is present, a :class:`StateBackend` gives yacron2 a
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
:func:`yacron2.cron.Cron.start_stop_state`), and it uses nothing outside the
standard library, so it costs the common, stateless install nothing.
"""

import abc
import asyncio
import contextlib
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from yacron2.config import ConfigError, StateConfig
from yacron2.platform import IS_WINDOWS, exclusive_file_lock

logger = logging.getLogger("yacron2.state")

#: Per-record on-disk schema version.  Every record is written wrapped as
#: ``{"schemaVersion": SCHEME_VERSION, "data": {...}}``; a record whose version
#: this build does not recognise is quarantined on read rather than guessed at.
#: Bump this when the wrapper (not a caller's ``data``) changes shape, so old
#: and new records are told apart instead of silently mis-read.
SCHEME_VERSION = "v1"

# Subdirectories under a namespace root.  Records live under RECORDS_DIR in a
# per-stream directory; leases under LEASES_DIR; corrupt records are moved into
# QUARANTINE_DIR; TMP_DIR holds the write-temp files atomically renamed into
# place.  Directories are only ever *created*, never renamed (a directory
# rename is the one costly operation on an S3 Files mount), so this layout is
# safe there.
RECORDS_DIR = "records"
LEASES_DIR = "leases"
QUARANTINE_DIR = "quarantine"
TMP_DIR = "tmp"

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
# distinct job names can never collide on one on-disk path.
_FS_SAFE = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-"
)


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
    map to distinct, portable filenames without collisions.
    """
    out: List[str] = []
    for byte in name.encode("utf-8"):
        char = chr(byte)
        if char in _FS_SAFE:
            out.append(char)
        else:
            out.append("%{:02X}".format(byte))
    return "".join(out) or "_"


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


def _mount_fstype(path: str) -> Optional[str]:
    """The filesystem type of the mount ``path`` lives on, or ``None``.

    Parses ``/proc/mounts`` and picks the longest mountpoint that is a prefix
    of the resolved path.  Linux-only (no portable ``statfs`` f_type in the
    stdlib); returns ``None`` where ``/proc`` is absent (macOS/Windows),
    which the caller treats as "cannot tell -> single-node".
    """
    try:
        with open("/proc/mounts", encoding="utf-8") as fobj:
            lines = fobj.read().splitlines()
    except OSError:
        return None
    real = os.path.realpath(path)
    best_mount = ""
    best_type: Optional[str] = None
    for line in lines:
        parts = line.split(" ")
        if len(parts) < 3:
            continue
        mountpoint = _unescape_mount(parts[1])
        fstype = parts[2]
        prefix = mountpoint.rstrip("/") + "/"
        if real == mountpoint or real.startswith(prefix) or mountpoint == "/":
            # longest matching mountpoint wins (>= so "/" is a fallback only)
            if len(mountpoint) >= len(best_mount):
                best_mount = mountpoint
                best_type = fstype
    return best_type


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


@dataclass
class Lease:
    """A held (or observed) TTL lease.

    ``fence`` increases every time the lease is *taken over* from an expired
    holder (fixed across a same-holder renew), so a stale holder can be
    detected and its late writes fenced off; ``expires_at`` is wall-clock epoch
    seconds; the lease is free to take over once ``_now() > expires_at``.
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
    async def append_record(
        self, stream: str, data: Dict[str, Any]
    ) -> str:
        """Append one immutable record to ``stream``; return its record id."""

    @abc.abstractmethod
    async def list_records(
        self,
        stream: str,
        *,
        limit: Optional[int] = None,
        newest_first: bool = False,
    ) -> List[Dict[str, Any]]:
        """Read back a stream's records (corrupt ones quarantined)."""

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

    # --- advisory-lock TTL lease -----------------------------------------

    @abc.abstractmethod
    async def acquire_lease(
        self, name: str, holder: str, ttl: float
    ) -> Optional[Lease]:
        """Take (or renew) lease ``name`` for ``ttl``s, else ``None``."""

    @abc.abstractmethod
    async def renew_lease(
        self, lease: Lease, ttl: float
    ) -> Optional[Lease]:
        """Extend a still-held lease; ``None`` if it was taken over."""

    @abc.abstractmethod
    async def release_lease(self, lease: Lease) -> None:
        """Release a lease we hold (a no-op if we no longer hold it)."""

    @abc.abstractmethod
    async def read_lease(self, name: str) -> Optional[Lease]:
        """Observe a lease without taking it (best-effort, unlocked read)."""

    # --- introspection ---------------------------------------------------

    @property
    @abc.abstractmethod
    def topology(self) -> str:
        """``"shared"`` | ``"single-node"`` | ``"unknown"`` (before start)."""

    def supports_shared_locking(self) -> bool:
        """Whether a lease here excludes across hosts (HA-capable)."""
        return self.topology == "shared"

    def view_dict(self) -> Dict[str, Any]:
        """The state view for a future ``GET /state`` / the dashboard."""
        return {"backend": self.backend_name, "topology": self.topology}


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
        self.root = os.path.abspath(config["path"])
        # a stable prefix so several deployments can share one store without
        # colliding; job-set scoping (like the lease backends' @reboot set) is
        # layered on top by callers via the stream name, in later phases.
        self.namespace = config.get("deploymentId") or "default"
        self._configured_topology: str = config.get("topology", "auto")
        self._topology = "unknown"
        # a per-process id mixed into every written filename, so records and
        # temp files from different nodes/processes onto one shared mount never
        # collide on a name.  os.urandom is fine (uniqueness, not secrecy).
        self._instance = os.urandom(6).hex()
        self._seq = 0

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

    def _tmp_path(self) -> str:
        self._seq += 1
        return os.path.join(
            self.base,
            TMP_DIR,
            "w-{}-{:012d}.tmp".format(self._instance, self._seq),
        )

    # --- lifecycle -------------------------------------------------------

    @property
    def topology(self) -> str:
        return self._topology

    async def start(self) -> None:
        await asyncio.to_thread(self._start_sync)
        logger.info(
            "state: filesystem backend ready at %s "
            "(namespace=%s, topology=%s, shared_locking=%s)",
            self.base,
            self.namespace,
            self._topology,
            self.supports_shared_locking(),
        )

    def _start_sync(self) -> None:
        for sub in (RECORDS_DIR, LEASES_DIR, QUARANTINE_DIR, TMP_DIR):
            os.makedirs(os.path.join(self.base, sub), exist_ok=True)
        self._topology = self._resolve_topology()
        # Fail start() loudly if the store is not actually writable (a bad
        # mount, wrong permissions) rather than silently swallowing every later
        # write: write, fsync and remove a tiny probe file.  start_stop_state
        # catches the OSError, logs it, and keeps running the in-memory path.
        probe = os.path.join(
            self.base, TMP_DIR, "startup-{}.probe".format(self._instance)
        )
        with open(probe, "wb") as fobj:
            fobj.write(b"ok")
            fobj.flush()
            os.fsync(fobj.fileno())
        os.unlink(probe)

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

    def _atomic_write(self, dest: str, payload: bytes) -> None:
        """Write ``payload`` to ``dest`` via a temp file + atomic rename.

        The rename is atomic on a local filesystem, on Windows (os.replace),
        and -- crucially -- on an Amazon S3 Files mount, where *file* rename is
        atomic even though the underlying object store has no native rename.  A
        reader therefore never observes a half-written ``dest``.
        """
        tmp = self._tmp_path()
        with open(tmp, "wb") as fobj:
            fobj.write(payload)
            fobj.flush()
            os.fsync(fobj.fileno())
        os.replace(tmp, dest)

    async def append_record(
        self, stream: str, data: Dict[str, Any]
    ) -> str:
        return await asyncio.to_thread(self._append_sync, stream, data)

    def _append_sync(self, stream: str, data: Dict[str, Any]) -> str:
        stream_dir = self._stream_dir(stream)
        os.makedirs(stream_dir, exist_ok=True)
        # Filename sort key is the write-time epoch (zero-padded so it sorts
        # lexicographically == chronologically), then instance+seq for
        # uniqueness.  The record's own logical timestamp lives in ``data`` and
        # is what derive_max reads; the filename only orders listing.
        self._seq += 1
        rec_id = "{:020.6f}-{}-{:012d}".format(
            _now(), self._instance, self._seq
        )
        payload = json.dumps(
            {"schemaVersion": SCHEME_VERSION, "data": data},
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        self._atomic_write(
            os.path.join(stream_dir, rec_id + ".json"), payload
        )
        return rec_id

    def _quarantine(self, path: str, name: str, reason: str) -> None:
        dest = os.path.join(
            self.base,
            QUARANTINE_DIR,
            "{}.{}.bad".format(name, self._instance),
        )
        try:
            os.replace(path, dest)
            logger.warning(
                "state: quarantined corrupt record %s (%s)", name, reason
            )
        except OSError:
            # already moved/removed by another pass or node, or unwritable:
            # never let cleanup of a poison record raise into a read.
            pass

    def _read_record(
        self, stream_dir: str, name: str
    ) -> Optional[Dict[str, Any]]:
        path = os.path.join(stream_dir, name)
        try:
            with open(path, "rb") as fobj:
                obj = json.loads(fobj.read())
        except FileNotFoundError:
            # raced away (pruned/quarantined) between listdir and open: skip.
            return None
        except (OSError, ValueError):
            self._quarantine(path, name, "unreadable-or-invalid-json")
            return None
        if (
            not isinstance(obj, dict)
            or obj.get("schemaVersion") != SCHEME_VERSION
            or not isinstance(obj.get("data"), dict)
        ):
            self._quarantine(path, name, "unknown-schema")
            return None
        data: Dict[str, Any] = obj["data"]
        return data

    async def list_records(
        self,
        stream: str,
        *,
        limit: Optional[int] = None,
        newest_first: bool = False,
    ) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(
            self._list_sync, stream, limit, newest_first
        )

    def _list_sync(
        self, stream: str, limit: Optional[int], newest_first: bool
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
            data = self._read_record(stream_dir, name)
            if data is not None:
                out.append(data)
        return out

    async def derive_max(self, stream: str, field: str) -> Optional[Any]:
        return await asyncio.to_thread(self._derive_max_sync, stream, field)

    def _derive_max_sync(self, stream: str, field: str) -> Optional[Any]:
        best: Optional[Any] = None
        for data in self._list_sync(stream, None, False):
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
        return await asyncio.to_thread(self._prune_sync, stream, keep)

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
    def _locked(self, lock_path: str) -> Iterator[None]:
        """Hold the advisory exclusive lock on ``lock_path`` for the block.

        The lock file is separate from the ``.lease`` data file on purpose: the
        data file is replaced by an atomic rename, which would swap the inode
        out from under a lock taken on it; locking a stable side-file avoids
        that entirely.
        """
        os.makedirs(os.path.dirname(lock_path), exist_ok=True)
        fdesc = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            # msvcrt.locking needs a byte present to lock; guarantee one.
            if os.fstat(fdesc).st_size == 0:
                os.write(fdesc, b"\0")
            with exclusive_file_lock(fdesc):
                yield
        finally:
            os.close(fdesc)

    def _read_lease_file(self, lease_path: str) -> Optional[Lease]:
        try:
            with open(lease_path, "rb") as fobj:
                obj = json.loads(fobj.read())
        except FileNotFoundError:
            return None
        except (OSError, ValueError):
            return None
        if not isinstance(obj, dict):
            return None
        try:
            return Lease(
                name=str(obj["name"]),
                holder=str(obj["holder"]),
                fence=int(obj["fence"]),
                expires_at=float(obj["expiresAt"]),
            )
        except (KeyError, TypeError, ValueError):
            return None

    def _write_lease_file(self, lease_path: str, lease: Lease) -> None:
        payload = json.dumps(
            lease.to_dict(), separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        self._atomic_write(lease_path, payload)

    async def acquire_lease(
        self, name: str, holder: str, ttl: float
    ) -> Optional[Lease]:
        return await asyncio.to_thread(
            self._acquire_sync, name, holder, ttl
        )

    def _acquire_sync(
        self, name: str, holder: str, ttl: float
    ) -> Optional[Lease]:
        lock_path, lease_path = self._lease_paths(name)
        with self._locked(lock_path):
            current = self._read_lease_file(lease_path)
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
            elif current.holder == holder:
                # a renew by the same holder keeps the fence.
                fence = current.fence
            else:
                # taking over an expired lease: bump the fence so the old
                # holder's late writes can be fenced off.
                fence = current.fence + 1
            lease = Lease(
                name=name,
                holder=holder,
                fence=fence,
                expires_at=now + ttl,
            )
            self._write_lease_file(lease_path, lease)
            return lease

    async def renew_lease(
        self, lease: Lease, ttl: float
    ) -> Optional[Lease]:
        return await asyncio.to_thread(self._renew_sync, lease, ttl)

    def _renew_sync(self, lease: Lease, ttl: float) -> Optional[Lease]:
        lock_path, lease_path = self._lease_paths(lease.name)
        with self._locked(lock_path):
            current = self._read_lease_file(lease_path)
            # Renew only if we still hold it: same holder AND same fence (a
            # takeover would have bumped the fence).  Allowed even a hair past
            # expiry, as long as nobody else took over in the meantime.
            if (
                current is None
                or current.holder != lease.holder
                or current.fence != lease.fence
            ):
                return None
            renewed = Lease(
                name=lease.name,
                holder=lease.holder,
                fence=lease.fence,
                expires_at=_now() + ttl,
            )
            self._write_lease_file(lease_path, renewed)
            return renewed

    async def release_lease(self, lease: Lease) -> None:
        await asyncio.to_thread(self._release_sync, lease)

    def _release_sync(self, lease: Lease) -> None:
        lock_path, lease_path = self._lease_paths(lease.name)
        with self._locked(lock_path):
            current = self._read_lease_file(lease_path)
            if (
                current is not None
                and current.holder == lease.holder
                and current.fence == lease.fence
            ):
                with contextlib.suppress(OSError):
                    os.unlink(lease_path)

    async def read_lease(self, name: str) -> Optional[Lease]:
        _lock_path, lease_path = self._lease_paths(name)
        return await asyncio.to_thread(self._read_lease_file, lease_path)

    # --- introspection ---------------------------------------------------

    def view_dict(self) -> Dict[str, Any]:
        return {
            "backend": self.backend_name,
            "path": self.base,
            "namespace": self.namespace,
            "topology": self._topology,
            "shared_locking": self.supports_shared_locking(),
            "job_set_id": self.get_job_set_id(),
        }


def make_state_backend(
    state_config: StateConfig,
    get_job_set_id: Callable[[], str],
) -> StateBackend:
    """Build the state backend for a ``state`` config section.

    Mirrors :func:`yacron2.leadership.make_backend`.  Today there is one
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
