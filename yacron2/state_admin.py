"""Offline administration of the durable state store (`yacron2 state ...`).

The operational other half of :mod:`yacron2.state`: backup/restore, store
migration (local disk <-> an Amazon S3 Files / EFS mount -- the same POSIX
layout either way, so a migration is a faithful file copy), manual garbage
collection, a health/inventory check, and record-scheme migration.  All of
it works offline, straight from the ``state`` config section, with no
running daemon required; against a RUNNING daemon every command stays safe
(records are immutable, copies/reads never lock), though a backup taken
mid-write is a point-in-time-ish snapshot rather than an exact one.

Imported lazily by ``yacron2.__main__`` only when a ``state`` subcommand is
used, so the daemon's import graph (and the stateless install) pays nothing
for it.
"""

import asyncio
import io
import os
import shutil
import socket
import tarfile
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

from yacron2.config import ConfigError, parse_config
from yacron2.state import (
    LEASES_DIR,
    RECORDS_DIR,
    FilesystemStateBackend,
    make_state_backend,
)

# what a backup/migration carries: the immutable records and the lease files
# (a lease file is its fence counter's only home, so dropping it would
# re-issue fence values). Deliberately NOT carried: tmp/ (transient debris)
# and quarantine/ (poison records; forensics stay with the source store).
_CARRIED_DIRS = (RECORDS_DIR, LEASES_DIR)


def _load_state_backend(config_arg: str) -> FilesystemStateBackend:
    """Build the (unstarted) filesystem backend the config points at.

    Goes through the full config parse so the CLI resolves path/deploymentId
    (and the one-state-section guarantee) exactly as the daemon would.
    """
    config = parse_config(config_arg)
    if config.state_config is None:
        raise ConfigError(
            "the configuration has no `state:` section; "
            "`yacron2 state` administers the durable store it defines"
        )
    backend = make_state_backend(
        config.state_config, lambda: "yacron2-state-cli"
    )
    if not isinstance(backend, FilesystemStateBackend):
        raise ConfigError(
            "`yacron2 state` supports the filesystem backend only"
        )
    return backend


def _job_names(config_arg: str) -> Set[str]:
    return {job.name for job in parse_config(config_arg).jobs}


def _walk_carried(base: str) -> Iterator[Tuple[str, str]]:
    """Yield (absolute path, base-relative arcname) for every carried file."""
    for sub in _CARRIED_DIRS:
        root = os.path.join(base, sub)
        for dirpath, _dirnames, filenames in os.walk(root):
            for filename in sorted(filenames):
                full = os.path.join(dirpath, filename)
                yield full, os.path.relpath(full, base).replace(os.sep, "/")


def cmd_backup(config_arg: str, output: str) -> int:
    """Write a gzipped tar of the store's namespace to ``output``."""
    backend = _load_state_backend(config_arg)
    base = backend.base
    if not os.path.isdir(base):
        print("state: nothing to back up: {} does not exist".format(base))
        return 1
    count = 0
    with tarfile.open(output, "w:gz") as tar:
        for full, arcname in _walk_carried(base):
            # Read the file fully BEFORE writing its tar header: tar.add
            # streams header-then-data, so a read failing midway (a prune,
            # a Windows sharing violation) would truncate the member and
            # silently corrupt the whole archive. Records are small; a
            # file that cannot be read is skipped, by design for a backup
            # taken against a live daemon.
            try:
                with open(full, "rb") as fobj:
                    payload = fobj.read()
                info = tar.gettarinfo(full, arcname=arcname)
            except OSError:
                continue
            info.size = len(payload)
            tar.addfile(info, io.BytesIO(payload))
            count += 1
    print(
        "state: backed up {} file(s) from {} to {}".format(count, base, output)
    )
    return 0


def _safe_members(
    tar: tarfile.TarFile, dest: str
) -> Iterator[tarfile.TarInfo]:
    """Yield only members that extract to a plain file strictly inside
    ``dest`` (no absolute paths, no ``..`` escapes, no links/devices)."""
    real_dest = os.path.realpath(dest)
    for member in tar.getmembers():
        if not member.isfile():
            continue
        try:
            target = os.path.realpath(os.path.join(dest, member.name))
            inside = os.path.commonpath([real_dest, target]) == real_dest
        except ValueError:
            # commonpath raises on mixed drives/UNC roots (Windows): such a
            # member cannot be inside dest -- skip it, never abort the
            # restore mid-extraction.
            continue
        if not inside:
            continue
        yield member


def cmd_restore(config_arg: str, archive: str, force: bool) -> int:
    """Extract a backup archive into the configured store namespace."""
    backend = _load_state_backend(config_arg)
    base = backend.base
    populated = any(
        os.path.isdir(os.path.join(base, sub))
        and os.listdir(os.path.join(base, sub))
        for sub in _CARRIED_DIRS
    )
    if populated and not force:
        print(
            "state: refusing to restore into the non-empty store at {} "
            "(pass --force to merge into it)".format(base)
        )
        return 1
    os.makedirs(base, mode=0o700, exist_ok=True)
    count = 0
    with tarfile.open(archive, "r:gz") as tar:
        for member in _safe_members(tar, base):
            # extract with modest permissions; the daemon re-creates its
            # directory modes, and record files carry job output (0o600).
            member.mode = 0o600
            tar.extract(member, path=base)
            count += 1
    print(
        "state: restored {} file(s) from {} into {}".format(
            count, archive, base
        )
    )
    return 0


def cmd_migrate(
    config_arg: str,
    dest_path: str,
    dest_deployment: Optional[str],
    force: bool,
) -> int:
    """Copy the store to another path/mount (FS <-> S3 Files migration).

    A local directory and an Amazon S3 Files / EFS mount share one on-disk
    layout, so migration in either direction is a faithful file copy into
    the destination namespace.  Each file lands via a temp sibling + atomic
    rename, so a reader of the DESTINATION never observes a torn record --
    important when cutting over to a shared mount that other nodes already
    watch.  A POPULATED destination is refused without ``--force``: blindly
    overwriting its lease files would regress their fence counters (a
    lease file is its fence's only home) under any daemon already using
    that store.
    """
    backend = _load_state_backend(config_arg)
    src_base = backend.base
    if not os.path.isdir(src_base):
        print("state: nothing to migrate: {} does not exist".format(src_base))
        return 1
    dest_cfg = dict(backend.config)
    dest_cfg["path"] = dest_path
    if dest_deployment is not None:
        dest_cfg["deploymentId"] = dest_deployment
    dest_backend = FilesystemStateBackend(
        dest_cfg,  # type: ignore[arg-type]
        lambda: "yacron2-state-cli",
    )
    dest_base = dest_backend.base
    real_src = os.path.realpath(src_base)
    real_dest = os.path.realpath(dest_base)
    if real_dest == real_src or real_dest.startswith(real_src + os.sep):
        # identical stores, or a destination nested INSIDE the source --
        # the copy walk would start finding its own output.
        print("state: destination must be a store outside the source")
        return 1
    populated = any(
        os.path.isdir(os.path.join(dest_base, sub))
        and os.listdir(os.path.join(dest_base, sub))
        for sub in _CARRIED_DIRS
    )
    if populated and not force:
        print(
            "state: refusing to migrate into the non-empty store at {} "
            "(pass --force to overwrite; NOT safe while a daemon uses "
            "it)".format(dest_base)
        )
        return 1
    count = 0
    for full, arcname in _walk_carried(src_base):
        target = os.path.join(dest_base, arcname.replace("/", os.sep))
        os.makedirs(os.path.dirname(target), mode=0o700, exist_ok=True)
        tmp = target + ".migrating"
        try:
            shutil.copyfile(full, tmp)
            # the backend's replace, not a bare os.replace: it rides out
            # transient Windows sharing violations (AV scans, readers).
            FilesystemStateBackend._replace(tmp, target)
        except OSError as ex:
            print("state: failed to copy {}: {}".format(arcname, ex))
            try:
                os.unlink(tmp)
            except OSError:
                pass
            return 1
        count += 1
    print(
        "state: migrated {} file(s) from {} to {}".format(
            count, src_base, dest_base
        )
    )
    print(
        "state: point state.path at the new location (and update "
        "deploymentId if you changed it) to cut over"
    )
    return 0


async def _gc_async(
    backend: FilesystemStateBackend, keep_names: Set[str], dry_run: bool
) -> Dict[str, Any]:
    import datetime

    from yacron2.cron import (
        CATCHUP_STREAM_PREFIX,
        COUNTER_STREAM_PREFIX,
        INFLIGHT_STREAM_PREFIX,
        LOG_STREAM_PREFIX,
        MANIFEST_HOSTS_CAP,
        MANIFEST_STREAM_KEEP,
        MANIFEST_STREAM_PREFIX,
        REBOOT_STREAM_PREFIX,
        RETRY_STREAM_PREFIX,
        RUN_STREAM_PREFIX,
        SLOT_STREAM_PREFIX,
        _parse_iso_utc,
        get_now,
    )

    await backend.start()
    grace = float(backend.config.get("gcGraceSeconds") or 0)
    if grace <= 0:
        raise ConfigError(
            "state.gcGraceSeconds is disabled (<= 0); nothing to collect"
        )
    # read every host's own manifests/<host> stream (see cron.py's automatic
    # pass): a single shared, count-pruned stream would have its retained
    # history shrink as the fleet grows, eventually falling under grace and
    # deferring GC forever.
    stream_names = sorted(set(await backend.list_stream_names(
        MANIFEST_STREAM_PREFIX
    )))[:MANIFEST_HOSTS_CAP]
    manifests: List[Dict[str, Any]] = []
    for name in stream_names:
        manifests.extend(
            await backend.list_records(
                name, limit=MANIFEST_STREAM_KEEP, newest_first=True
            )
        )
    now = get_now(datetime.timezone.utc)
    # same young-history deferral as the daemon's automatic pass: unless
    # the manifest history spans one full grace window, absence cannot be
    # proven and nothing may be deleted.
    oldest = None
    for rec in manifests:
        at = _parse_iso_utc(rec.get("at"))
        if at is not None and (oldest is None or at < oldest):
            oldest = at
    if oldest is None or (now - oldest).total_seconds() < grace:
        return {"deferred": True}
    names = set(keep_names)
    # keep this machine's own counter snapshots even if no daemon has
    # manifested from here recently.
    hosts: Set[str] = {socket.gethostname() or "localhost"}
    for rec in manifests:
        at = _parse_iso_utc(rec.get("at"))
        if at is None or (now - at).total_seconds() > grace:
            continue
        jobs = rec.get("jobs")
        if isinstance(jobs, list):
            names.update(str(job) for job in jobs)
        host = rec.get("host")
        if isinstance(host, str) and host:
            hosts.add(host)
    keep: Dict[str, Set[str]] = {
        RUN_STREAM_PREFIX: names,
        LOG_STREAM_PREFIX: names,
        CATCHUP_STREAM_PREFIX: names,
        RETRY_STREAM_PREFIX: names,
        REBOOT_STREAM_PREFIX: names,
        COUNTER_STREAM_PREFIX: hosts,
        # without these two, a manual `yacron2 state gc` diverges from the
        # daemon's automatic pass and never reclaims in-flight/slot-lease
        # bookkeeping for a removed job.
        INFLIGHT_STREAM_PREFIX: names,
        SLOT_STREAM_PREFIX: names,
        MANIFEST_STREAM_PREFIX: hosts,
    }
    return await backend.collect_garbage(
        keep=keep, grace=grace, dry_run=dry_run
    )


def cmd_gc(config_arg: str, dry_run: bool) -> int:
    """Run one manual garbage-collection pass (respects gcGraceSeconds)."""
    backend = _load_state_backend(config_arg)
    names = _job_names(config_arg)
    result = asyncio.run(_gc_async(backend, names, dry_run))
    if result.get("deferred"):
        print(
            "state: gc deferred: the manifest history does not yet span "
            "gcGraceSeconds, so the store cannot prove what is orphaned"
        )
        return 0
    verb = "would remove" if dry_run else "removed"
    print(
        "state: gc {} {} stream(s) ({} record(s)), {} temp file(s), "
        "{} quarantined record(s); kept {} stream(s)".format(
            verb,
            result.get("streams_removed", 0),
            result.get("records_removed", 0),
            result.get("tmp_removed", 0),
            result.get("quarantine_removed", 0),
            result.get("streams_kept", 0),
        )
    )
    for token in result.get("removed") or []:
        print("  - {}".format(token))
    return 0


async def _check_async(backend: FilesystemStateBackend) -> Dict[str, Any]:
    await backend.start()
    return backend.view_dict()


def cmd_check(config_arg: str) -> int:
    """Verify the store starts (writable probe) and print an inventory."""
    backend = _load_state_backend(config_arg)
    view = asyncio.run(_check_async(backend))
    print("state: store at {} is writable".format(view.get("path")))
    for key in ("backend", "namespace", "topology", "shared_locking"):
        print("  {}: {}".format(key, view.get(key)))
    base = backend.base
    records_root = os.path.join(base, RECORDS_DIR)
    streams: List[str] = []
    records = 0
    try:
        streams = sorted(os.listdir(records_root))
    except OSError:
        pass
    per_prefix: Dict[str, int] = {}
    for token in streams:
        stream_dir = os.path.join(records_root, token)
        if not os.path.isdir(stream_dir):
            continue
        try:
            count = len(
                [n for n in os.listdir(stream_dir) if n.endswith(".json")]
            )
        except OSError:
            continue
        records += count
        prefix = token.split("%2F", 1)[0] if "%2F" in token else token
        per_prefix[prefix] = per_prefix.get(prefix, 0) + count
    print("  streams: {} ({} record(s))".format(len(streams), records))
    for prefix in sorted(per_prefix):
        print("    {}: {} record(s)".format(prefix, per_prefix[prefix]))
    try:
        quarantined = len(os.listdir(os.path.join(base, "quarantine")))
    except OSError:
        quarantined = 0
    print("  quarantined: {} record(s)".format(quarantined))
    return 0


async def _migrate_schema_async(
    backend: FilesystemStateBackend, dry_run: bool
) -> Dict[str, Any]:
    await backend.start()
    return await backend.migrate_schema(dry_run=dry_run)


def cmd_migrate_schema(config_arg: str, dry_run: bool) -> int:
    """Rewrite records of older known schemes to the current one."""
    backend = _load_state_backend(config_arg)
    result = asyncio.run(_migrate_schema_async(backend, dry_run))
    verb = "would convert" if dry_run else "converted"
    print(
        "state: migrate-schema {} {} record(s); {} current, {} unknown, "
        "{} unreadable, {} failed".format(
            verb,
            result.get("converted", 0),
            result.get("current", 0),
            result.get("unknown", 0),
            result.get("unreadable", 0),
            result.get("failed", 0),
        )
    )
    return 0


def dispatch(args: Any) -> int:
    """Route a parsed `yacron2 state <action>` invocation; return exit code."""
    action = getattr(args, "state_command", None)
    try:
        if action == "backup":
            return cmd_backup(args.config, args.output)
        if action == "restore":
            return cmd_restore(args.config, args.archive, args.force)
        if action == "migrate":
            return cmd_migrate(
                args.config, args.dest, args.dest_deployment_id, args.force
            )
        if action == "gc":
            return cmd_gc(args.config, args.dry_run)
        if action == "check":
            return cmd_check(args.config)
        if action == "migrate-schema":
            return cmd_migrate_schema(args.config, args.dry_run)
    except ConfigError as ex:
        print("yacron2 state error: {}".format(ex))
        return 1
    except OSError as ex:
        print("yacron2 state error: {}".format(ex))
        return 1
    print("yacron2 state: no action given (see `yacron2 state --help`)")
    return 2
