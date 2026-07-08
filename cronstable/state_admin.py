"""Offline administration of the durable state store (`cronstable state ...`).

The operational other half of :mod:`cronstable.state`: backup/restore, store
migration (local disk <-> an Amazon S3 Files / EFS mount -- the same POSIX
layout either way, so a migration is a faithful file copy), manual garbage
collection, a health/inventory check, and record-scheme migration.  All of
it works offline, straight from the ``state`` config section, with no
running daemon required.  Against a RUNNING daemon the commands that only
read the store (or write through the backend's own locked paths) stay safe
-- records are immutable, copies/reads never lock -- though a backup taken
mid-write is a point-in-time-ish snapshot rather than an exact one.  The
exceptions are ``restore --force`` and ``migrate --force``: both write
straight into a store's namespace and are NOT safe while a daemon uses it.

Imported lazily by ``cronstable.__main__`` only when a ``state`` subcommand is
used, so the daemon's import graph (and the stateless install) pays nothing
for it.
"""

import asyncio
import io
import json
import os
import shutil
import socket
import tarfile
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

from cronstable.config import ConfigError, parse_config
from cronstable.state import (
    BLOBS_DIR,
    DOCS_DIR,
    LEASES_DIR,
    RECORDS_DIR,
    FilesystemStateBackend,
    make_state_backend,
)

# what a backup/migration carries: the FULL durable state -- the immutable
# records, the mutable job-state documents (idempotency keys, KV, cursors and
# distributed-lock docs all live under docs/), the content-addressed artifact
# payloads (blobs/), and the lease files (a lease file is its fence counter's
# only home, so dropping it would re-issue fence values). Omitting docs/ or
# blobs/ would silently lose idempotency/KV/cursor/lock state on restore (a
# once-only guarded job would re-run) and orphan every artifact record (its
# blob gone -> 410). Deliberately NOT carried: tmp/ (transient debris) and
# quarantine/ (poison records; forensics stay with the source store).
_CARRIED_DIRS = (RECORDS_DIR, DOCS_DIR, BLOBS_DIR, LEASES_DIR)


def _load_state_backend(config_arg: str) -> FilesystemStateBackend:
    """Build the (unstarted) filesystem backend the config points at.

    Goes through the full config parse so the CLI resolves path/deploymentId
    (and the one-state-section guarantee) exactly as the daemon would.
    """
    config = parse_config(config_arg)
    if config.state_config is None:
        raise ConfigError(
            "the configuration has no `state:` section; "
            "`cronstable state` administers the durable store it defines"
        )
    backend = make_state_backend(
        config.state_config, lambda: "cronstable-state-cli"
    )
    if not isinstance(backend, FilesystemStateBackend):
        raise ConfigError(
            "`cronstable state` supports the filesystem backend only"
        )
    return backend


def _config_keep_sets(config_arg: str) -> Tuple[Set[str], Set[str], Set[str]]:
    """(job names, extra artifact scopes, dag names) the config keeps alive.

    The same three seed sets the daemon derives from its loaded config for a
    GC pass (see Cron._collect_state_garbage / _artifact_scope_names), so a
    manual ``cronstable state gc`` protects exactly what a daemon running this
    config would.
    """
    config = parse_config(config_arg)
    names = {job.name for job in config.jobs}
    scopes: Set[str] = set()
    for job in config.jobs:
        scopes.update(job.stateAllowedScopes)
    dag_names: Set[str] = set()
    for dagcfg in config.dags:
        dag_names.add(dagcfg.name)
        for template in dagcfg.task_templates.values():
            scopes.update(template.stateAllowedScopes)
    return names, scopes, dag_names


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
    # The archive gets the store's own 0o600, not the default 0o644: it
    # flattens records/docs/blobs -- captured job output, KV values,
    # artifact payloads, exactly where secrets live -- into one file, so a
    # world-readable archive would leak everything the store's 0o700/0o600
    # tree keeps private.  os.open's mode only applies to a NEW file, so a
    # pre-existing output is chmod'ed too, before any content is written.
    # (On Windows the POSIX mode is mostly a no-op; POSIX is where the
    # leak is.)
    out_fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(out_fd, "wb") as out_fobj:
        os.chmod(output, 0o600)
        with tarfile.open(fileobj=out_fobj, mode="w:gz") as tar:
            for full, arcname in _walk_carried(base):
                # Read the file fully BEFORE writing its tar header: tar.add
                # streams header-then-data, so a read failing midway (a
                # prune, a Windows sharing violation) would truncate the
                # member and silently corrupt the whole archive. Records are
                # small; a file that cannot be read is skipped, by design
                # for a backup taken against a live daemon.
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


def _lease_fence(payload: bytes) -> Optional[int]:
    """The fence counter in a lease file's JSON; ``None`` if unparseable."""
    try:
        return int(json.loads(payload)["fence"])
    except Exception:  # noqa: BLE001 - unparseable means unprovable
        return None


def cmd_restore(config_arg: str, archive: str, force: bool) -> int:
    """Extract a backup archive into the configured store namespace.

    Every member lands via a temp sibling + atomic replace (the same
    pattern as :func:`cmd_migrate`), never a stream into the final path: a
    concurrent reader of a half-written ``.json`` would see a torn record,
    which the store QUARANTINES -- silently losing the restored record.
    When merging into a populated store, a lease file only replaces the
    current one if its archived fence is not older (fence-max merge): a
    lease file is its fence counter's only home, and regressing it would
    hand out already-issued fence values (double execution in a fleet).
    """
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
            "(pass --force to merge into it; NOT safe while a daemon "
            "uses it)".format(base)
        )
        return 1
    os.makedirs(base, mode=0o700, exist_ok=True)
    count = 0
    skipped_leases = 0
    with tarfile.open(archive, "r:gz") as tar:
        for member in _safe_members(tar, base):
            fobj = tar.extractfile(member)
            if fobj is None:
                continue
            payload = fobj.read()
            target = os.path.join(base, member.name.replace("/", os.sep))
            if populated and member.name.startswith(LEASES_DIR + "/"):
                if not member.name.endswith(".lease"):
                    # a .lock side-file carries no data, and a live daemon
                    # may hold an OS lock on that very inode: replacing it
                    # would split the lock across two inodes.
                    continue
                current: Optional[int] = None
                current_exists = os.path.exists(target)
                if current_exists:
                    try:
                        with open(target, "rb") as cur:
                            current = _lease_fence(cur.read())
                    except OSError:
                        current = None
                archived = _lease_fence(payload)
                if current_exists and (
                    archived is None or current is None or archived < current
                ):
                    # cannot prove the archived fence is >= the store's:
                    # keep the current lease file.
                    skipped_leases += 1
                    continue
            os.makedirs(os.path.dirname(target), mode=0o700, exist_ok=True)
            tmp = target + ".restoring"
            try:
                # created 0o600 (record files carry job output) and swapped
                # in via the backend's replace, which rides out transient
                # Windows sharing violations (AV scans, readers).
                fdesc = os.open(
                    tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600
                )
                with os.fdopen(fdesc, "wb") as tmp_fobj:
                    tmp_fobj.write(payload)
                FilesystemStateBackend._replace(tmp, target)
            except OSError as ex:
                print(
                    "state: failed to restore {}: {}".format(member.name, ex)
                )
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                return 1
            count += 1
    print(
        "state: restored {} file(s) from {} into {}".format(
            count, archive, base
        )
    )
    if skipped_leases:
        print(
            "state: kept {} current lease file(s) whose archived fence "
            "was older (restoring those would regress fence "
            "counters)".format(skipped_leases)
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
        lambda: "cronstable-state-cli",
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
    backend: FilesystemStateBackend,
    keep_names: Set[str],
    keep_scopes: Set[str],
    keep_dags: Set[str],
    dry_run: bool,
) -> Dict[str, Any]:
    import datetime

    from cronstable.cron import (
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
        _fold_manifest,
        _manifests_cover_scopes,
        _parse_iso_utc,
        get_now,
    )
    from cronstable.dag import DAG_LEASE_PREFIX, DAG_RUN_NS_PREFIX, xcom_scope
    from cronstable.jobstate import ARTIFACT_STREAM_PREFIX, GLOBAL_SCOPE

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
    stream_names = sorted(
        set(await backend.list_stream_names(MANIFEST_STREAM_PREFIX))
    )[:MANIFEST_HOSTS_CAP]
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
    art_scopes = set(keep_scopes) | {GLOBAL_SCOPE}
    live_dags = set(keep_dags)
    recent: List[Dict[str, Any]] = []
    for rec in manifests:
        at = _parse_iso_utc(rec.get("at"))
        if at is None or (now - at).total_seconds() > grace:
            continue
        recent.append(rec)
        _fold_manifest(rec, names, hosts, art_scopes, live_dags)
    # job names keep their default artifact scope too.
    art_scopes |= names
    keep: Dict[str, Set[str]] = {
        RUN_STREAM_PREFIX: names,
        LOG_STREAM_PREFIX: names,
        CATCHUP_STREAM_PREFIX: names,
        RETRY_STREAM_PREFIX: names,
        REBOOT_STREAM_PREFIX: names,
        COUNTER_STREAM_PREFIX: hosts,
        # without these two, a manual `cronstable state gc` diverges from the
        # daemon's automatic pass and never reclaims in-flight/slot-lease
        # bookkeeping for a removed job.
        INFLIGHT_STREAM_PREFIX: names,
        SLOT_STREAM_PREFIX: names,
        MANIFEST_STREAM_PREFIX: hosts,
    }
    # artifact streams are managed only when (a) every recent manifest
    # advertises its scopes/dags (an older node's silence proves nothing --
    # mirrors the daemon's pass) and (b) every dagrun/<dag> namespace could
    # be enumerated by name, so every live run's XCom scope is protectable.
    # Run DOCUMENTS of removed dags are left to the daemon's DagScheduler,
    # which alone knows what it owns; once it deletes them, this pass
    # collects their aged streams too.
    if _manifests_cover_scopes(recent):
        namespaces, complete = await backend.list_document_namespaces(
            DAG_RUN_NS_PREFIX
        )
        if complete:
            for ns in namespaces:
                dag_name = ns[len(DAG_RUN_NS_PREFIX) :]
                for body in await backend.list_documents(ns):
                    run_id = body.get("runId")
                    if run_id:
                        art_scopes.add(xcom_scope(dag_name, str(run_id)))
            keep[ARTIFACT_STREAM_PREFIX] = art_scopes
    result = await backend.collect_garbage(
        keep=keep,
        grace=grace,
        # mirror the daemon's pass: only dagrun's per-run advance leases
        # are ephemeral; every other lease's fence can be persisted in
        # slot cancel records and must survive any grace window.
        ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,),
        dry_run=dry_run,
    )
    removed, skip_reason = await _sweep_blobs_async(
        backend,
        grace,
        dry_run,
        # dry run: collect_garbage only REPORTED these stream tokens, so
        # their records still exist; exclude them from the reference walk
        # so the count matches what a real pass would free.
        set(result.get("removed") or []) if dry_run else set(),
    )
    result["blobs_removed"] = removed
    if skip_reason:
        result["blob_sweep_skipped"] = skip_reason
    return result


async def _sweep_blobs_async(
    backend: FilesystemStateBackend,
    grace: float,
    dry_run: bool,
    pruned_tokens: Set[str],
) -> Tuple[int, Optional[str]]:
    """One orphan-blob sweep; ``(count, why-skipped-or-None)``.

    Biased to KEEP on every doubt, exactly like the daemon's pass
    (Cron._sweep_orphan_artifact_blobs): skipped outright when any artifact
    stream is unenumerable or any record unreadable, and the backend's age
    guard keeps blobs younger than the grace.
    """
    from cronstable.jobstate import (
        ARTIFACT_STREAM_PREFIX,
        referenced_blob_digests,
    )
    from cronstable.state import _fs_safe

    stream_names, complete = await backend.list_stream_names_audit(
        ARTIFACT_STREAM_PREFIX
    )
    if not complete:
        return 0, (
            "an artifact stream exists whose records cannot be enumerated, "
            "so its blob references cannot be ruled out"
        )
    scopes = [
        name[len(ARTIFACT_STREAM_PREFIX) :]
        for name in stream_names
        if _fs_safe(name) not in pruned_tokens
    ]
    try:
        referenced = await referenced_blob_digests(
            backend, scopes, strict=True
        )
    except Exception as ex:  # noqa: BLE001 - a missed reference must KEEP
        return 0, (
            "an artifact record could not be read, so its blob reference "
            "cannot be ruled out ({})".format(ex)
        )
    return (
        await backend.sweep_orphan_blobs(referenced, grace, dry_run=dry_run),
        None,
    )


def cmd_gc(config_arg: str, dry_run: bool) -> int:
    """Run one manual garbage-collection pass (respects gcGraceSeconds)."""
    backend = _load_state_backend(config_arg)
    names, scopes, dag_names = _config_keep_sets(config_arg)
    result = asyncio.run(_gc_async(backend, names, scopes, dag_names, dry_run))
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
    if result.get("blob_sweep_skipped"):
        print(
            "state: orphan-blob sweep skipped: {}".format(
                result["blob_sweep_skipped"]
            )
        )
    else:
        print(
            "state: gc {} {} orphaned artifact blob(s)".format(
                verb, result.get("blobs_removed", 0)
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
    """Route a parsed `cronstable state <action>` call; return exit code."""
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
        print("cronstable state error: {}".format(ex))
        return 1
    except OSError as ex:
        print("cronstable state error: {}".format(ex))
        return 1
    print("cronstable state: no action given (see `cronstable state --help`)")
    return 2
