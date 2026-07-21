"""OS-specific behavior, isolated so the rest of cronstable stays portable.

cronstable began life POSIX-only.  Everything that genuinely differs between
Unix
and Windows lives here behind a small, uniform surface, so the scheduler, the
job runner, the config loader and the entry point read the same on every
platform and only this module needs a per-OS branch:

* :data:`DEFAULT_SHELL` -- how a string command is handed to a shell;
* :data:`DEFAULT_CONFIG_PATH` -- where ``-c`` looks by default;
* :func:`supports_unix_sockets` -- whether ``unix://`` web listeners work;
* :func:`encode_argv` -- the argv form the platform's subprocess layer wants;
* :func:`new_process_group_kwargs` / :func:`kill_process_group` -- spawning a
  job so its descendants are reachable as one unit, and taking that unit down;
* :func:`install_shutdown_handlers` -- wiring Ctrl-C / termination to a
  graceful-shutdown callback on whichever event loop the platform provides.

Per-job ``user``/``group`` switching stays in :mod:`cronstable.config` (it
needs
the ``grp``/``pwd`` databases), but is likewise gated on :data:`IS_WINDOWS`.
"""

import asyncio
import contextlib
import errno
import logging
import os
import signal
import sys
import time
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

# Platform-specific file-locking primitive, imported behind a ``sys.platform``
# guard so each OS pulls in only the module it has (``fcntl`` is Unix-only,
# ``msvcrt`` Windows-only) and mypy -- pinned to ``platform = linux`` -- checks
# just the POSIX branch, exactly as the signal handling below is arranged.
if sys.platform == "win32":  # pragma: no cover - exercised on Windows only
    import msvcrt
else:
    import fcntl

logger = logging.getLogger("cronstable")

#: True on Windows, where the absent POSIX facilities (signals on the event
#: loop, unix sockets, ``grp``/``pwd``, ``setuid``) are routed around.
IS_WINDOWS = sys.platform == "win32"


# --- Default shell --------------------------------------------------------
# On POSIX a string command runs as ``/bin/sh -c "<command>"``.  Windows has no
# /bin/sh; an empty default tells the job runner (and the shell reporter) to
# hand the command to the native command processor (``%ComSpec%`` -- i.e.
# cmd.exe) via :func:`asyncio.create_subprocess_shell`, the closest equivalent.
# Either platform's default can still be overridden per job with ``shell:``.
DEFAULT_SHELL = "" if IS_WINDOWS else "/bin/sh"


# --- Default config location (the ``-c`` default) -------------------------
def _default_config_path() -> str:
    if IS_WINDOWS:  # pragma: no cover - Windows-only path
        # Per-user config under roaming AppData, e.g.
        # ``C:\Users\<you>\AppData\Roaming\cronstable``.  Falls back to the
        # user profile if APPDATA is somehow unset (rare; e.g. a bare service
        # account with no roaming profile).
        base = os.environ.get("APPDATA") or os.path.expanduser("~")
        return os.path.join(base, "cronstable")
    return "/etc/cronstable.d"


#: The directory (or file) ``-c`` defaults to when not given on the command
#: line.  Platform-appropriate so the daemon has a sensible home on each OS.
DEFAULT_CONFIG_PATH = _default_config_path()


# --- Unix-domain socket support ------------------------------------------
def supports_unix_sockets() -> bool:
    """Whether ``unix://`` web listeners can be bound on this platform.

    asyncio's Windows Proactor loop has no ``create_unix_server``, so aiohttp's
    ``UnixSite`` cannot bind there (AF_UNIX exists on recent Windows, but
    asyncio does not drive it).  Such listeners are skipped with a warning.
    """
    return not IS_WINDOWS


# --- Subprocess argv ------------------------------------------------------
def encode_argv(argv: List[str]) -> List[Union[str, bytes]]:
    """Return ``argv`` in the form this platform's subprocess layer expects.

    On POSIX the arguments are encoded to UTF-8 bytes so the child's argv is
    independent of the (possibly non-UTF-8) locale.  On Windows processes are
    created with the wide ``CreateProcessW`` API, which works from ``str`` and
    rejects ``bytes`` (``subprocess`` would fail building the command line), so
    the strings are passed through unchanged.
    """
    if IS_WINDOWS:  # pragma: no cover - Windows-only path
        return list(argv)
    return [arg.encode() for arg in argv]


# --- Subprocess process groups -------------------------------------------
#: How long :func:`kill_process_group` waits for a Windows ``taskkill`` to
#: report back before giving up on it.  Generous: the caller's fallback is to
#: kill the direct child only, so a slow-but-working taskkill is worth waiting
#: for -- but it must never park the job runner indefinitely.
TASKKILL_TIMEOUT = 10.0


def new_process_group_kwargs() -> Dict[str, Any]:
    """Subprocess kwargs that isolate a job in its own process group.

    A job command routinely leaves descendants behind (``sh -c 'helper &
    main'``), and each one inherits the write-end of the job's stdout/stderr
    pipe.  Signalling only the direct child on an ``executionTimeout`` --
    which is all ``Popen.terminate`` can do -- kills the shell but not the
    helper, so the pipe never reaches EOF, the run never finishes draining,
    and the job's slot is held forever (see
    :meth:`cronstable.job.RunningJob._read_job_streams`).

    On POSIX ``start_new_session`` puts the child in a brand-new session, so
    it and every descendant share one process-group id -- the child's own pid
    -- which :func:`kill_process_group` can then signal as a unit.  Windows
    has no equivalent at spawn time; descendants are reached through the
    process tree instead, so no creation flag is needed there.
    """
    if IS_WINDOWS:  # pragma: no cover - Windows-only path
        return {}
    return {"start_new_session": True}


async def kill_process_group(pid: int, *, force: bool) -> bool:
    """Signal the whole process group / tree rooted at ``pid``.

    ``force`` selects an unconditional kill (POSIX ``SIGKILL``) over a
    graceful request to exit (``SIGTERM``).  Returns whether the group was
    signalled: ``False`` means the caller should fall back to signalling the
    direct child on its own (:meth:`asyncio.subprocess.Process.terminate`),
    which is all this module could do before.

    ``pid`` must be the child spawned with :func:`new_process_group_kwargs`,
    whose pid is by construction its own pgid.  Signalling the *group* rather
    than the pid is what reaches an orphaned descendant, and it keeps working
    after the leader itself has exited: a process group lives as long as any
    member does, and the kernel will not recycle a pid that is still in use as
    a pgid, so there is no risk of hitting an unrelated group.

    Windows has no process group to signal, and no graceful equivalent at all
    (``TerminateProcess`` is unconditional), so a non-forced call reports
    ``False`` there and leaves the caller's direct-child terminate as the
    graceful step.  A forced call shells out to ``taskkill /T``, which walks
    the live parent/child tree.  Honest bound: a descendant already orphaned
    when taskkill runs (its parent exited first) is no longer in that tree and
    survives -- which is why the stream drain is separately bounded rather
    than trusting this to always succeed.
    """
    if IS_WINDOWS:  # pragma: no cover - Windows-only path
        if not force:
            return False
        return await _taskkill_tree(pid)
    sig = signal.SIGKILL if force else signal.SIGTERM
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        return False  # the whole group is already gone: nothing to signal
    except OSError as ex:
        logger.warning(
            "could not signal the process group of pid %s (%s); "
            "falling back to signalling that process alone",
            pid,
            ex,
        )
        return False
    return True


async def _taskkill_tree(pid: int) -> bool:  # pragma: no cover - Windows-only
    """Kill ``pid`` and its process tree via ``taskkill /F /T``."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "taskkill",
            "/F",
            "/T",
            "/PID",
            str(pid),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
    except OSError as ex:
        logger.warning("could not run taskkill for pid %s (%s)", pid, ex)
        return False
    try:
        retcode = await asyncio.wait_for(proc.wait(), TASKKILL_TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning(
            "taskkill for pid %s did not finish; abandoning it", pid
        )
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        return False
    # 128 is taskkill's "process not found": the tree is already gone, so
    # nothing was signalled and the caller's fallback is a no-op either way.
    return retcode == 0


# --- Graceful shutdown signalling ----------------------------------------
def install_shutdown_handlers(
    loop: asyncio.AbstractEventLoop, callback: Callable[[], None]
) -> Callable[[], None]:
    """Arrange for ``callback`` to run on a shutdown request (Ctrl-C / TERM).

    Returns a zero-argument cleanup function that removes whatever was
    installed; call it once the loop has finished.

    On POSIX this uses the event loop's native signal handling for SIGINT and
    SIGTERM.  On Windows, where ``loop.add_signal_handler`` raises
    ``NotImplementedError`` on the Proactor loop, it falls back to
    ``signal.signal`` for SIGINT (Ctrl-C) and SIGBREAK (Ctrl-Break / console
    close), marshalling the callback back onto the loop thread with
    ``call_soon_threadsafe`` and ticking a short timer so the interpreter runs
    the pending handler promptly even while the loop is blocked in IOCP.
    """
    if not IS_WINDOWS:
        sigs = (signal.SIGINT, signal.SIGTERM)
        for sig in sigs:
            loop.add_signal_handler(sig, callback)

        def remove_loop_handlers() -> None:
            for sig in sigs:
                loop.remove_signal_handler(sig)

        return remove_loop_handlers

    # Windows path lives in its own helper so it is measured only where it can
    # run (like :func:`_taskkill_tree`); this delegation never executes on
    # POSIX, where the branch above has already returned.
    return _install_windows_shutdown_handlers(  # pragma: no cover - Windows
        loop, callback
    )


def _install_windows_shutdown_handlers(  # pragma: no cover - Windows-only
    loop: asyncio.AbstractEventLoop, callback: Callable[[], None]
) -> Callable[[], None]:
    """Windows fallback for :func:`install_shutdown_handlers`.

    Installs plain C-level handlers and hops onto the loop thread, because
    ``loop.add_signal_handler`` raises ``NotImplementedError`` on the Proactor
    loop.  (getattr, not signal.SIGBREAK, so this module also type-checks on
    POSIX, where SIGBREAK does not exist.)
    """
    win_sigs = [signal.SIGINT]
    sigbreak = getattr(signal, "SIGBREAK", None)
    if sigbreak is not None:
        win_sigs.append(sigbreak)  # Ctrl-Break / console close (Windows-only)
    previous = {}

    def handler(signum, frame):  # runs in the main thread
        loop.call_soon_threadsafe(callback)

    for sig in win_sigs:
        previous[sig] = signal.signal(sig, handler)

    # A Python signal handler only runs when the main thread returns to the
    # interpreter; while the Proactor loop is blocked in GetQueuedCompletion
    # Status that can be delayed indefinitely.  A lightweight repeating timer
    # keeps the loop ticking so Ctrl-C is observed within the interval.
    heartbeat = None  # type: Union[asyncio.TimerHandle, None]

    def _tick() -> None:
        nonlocal heartbeat
        heartbeat = loop.call_later(0.25, _tick)

    heartbeat = loop.call_later(0.25, _tick)

    def restore_signal_handlers() -> None:
        if heartbeat is not None:
            heartbeat.cancel()
        for sig, prev in previous.items():
            signal.signal(sig, prev)

    return restore_signal_handlers


# --- OS boot identity ------------------------------------------------------
def os_boot_id() -> Optional[str]:
    """A stable, unique identifier of the current OS boot, or ``None``.

    Linux publishes a fresh UUID per boot; where the file is unavailable
    (Windows, macOS, BSD) callers fall back to :func:`os_boot_time`.  Used by
    the state-backed standalone ``@reboot`` dedupe: a daemon restart within
    one OS boot must not re-run a boot one-shot, while a genuine reboot must.
    """
    path = "/proc/sys/kernel/random/boot_id"
    try:
        with open(path, encoding="ascii") as fobj:
            value = fobj.read().strip()
    except (OSError, ValueError):
        return None
    return value or None


def os_boot_time() -> Optional[float]:
    """Wall-clock epoch seconds the OS booted at, or ``None`` (cannot tell).

    Derived as ``now - uptime``: on Windows from ``GetTickCount64`` (a 64-bit
    millisecond tick count that keeps running across sleep/hibernate and is
    unaffected by wall-clock steps), on POSIX from ``/proc/uptime``.  The
    derivation rides the *current* wall clock, so an NTP step shifts the
    result by the step size -- which is why consumers compare boot times with
    a tolerance rather than exactly.  ``None`` where neither source exists
    (macOS/BSD): the caller then treats every daemon start as a fresh boot,
    which is the pre-dedupe behaviour.
    """
    if IS_WINDOWS:  # pragma: no cover - Windows-only path
        try:
            import ctypes

            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            ticks = kernel32.GetTickCount64
            ticks.restype = ctypes.c_uint64
            uptime = float(ticks()) / 1000.0
        except Exception:  # noqa: BLE001 - any ctypes failure -> cannot tell
            return None
        return time.time() - uptime
    try:
        with open("/proc/uptime", encoding="ascii") as fobj:
            uptime = float(fobj.read().split()[0])
    except (OSError, ValueError, IndexError):
        return None
    return time.time() - uptime


# --- Process liveness -------------------------------------------------------
def pid_alive(pid: int) -> Optional[bool]:
    """Whether a process with ``pid`` currently exists, or ``None``.

    Used by in-flight run reconciliation (:mod:`cronstable.cron`) as a
    same-host safety check before declaring a previous daemon's run dead: a
    daemon crash does NOT kill the job processes it spawned, so an ``open``
    in-flight record whose recorded pid is still running must be left alone.
    PID reuse can make this report ``True`` for an unrelated process; that
    errs toward *not* reconciling, the safe direction.  ``None`` means the
    platform could not answer (treated by callers the same as dead, since
    the per-process token in the record already proved a different daemon
    wrote it).
    """
    if pid <= 0:
        return None
    if sys.platform == "win32":  # pragma: no cover - Windows-only path
        try:
            import ctypes

            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            handle = kernel32.OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION, False, pid
            )
            if handle:
                # the pid may name a zombie whose handles are still open;
                # check the exit code: STILL_ACTIVE (259) means running.
                STILL_ACTIVE = 259
                code = ctypes.c_ulong()
                alive = None  # type: Optional[bool]
                if kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                    alive = code.value == STILL_ACTIVE
                kernel32.CloseHandle(handle)
                return alive
            return False
        except Exception:  # noqa: BLE001 - any ctypes failure -> cannot tell
            return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # exists, owned by someone else (should not happen for our own
        # spawned jobs, but existence is what was asked).
        return True
    except OSError:
        return None
    return True


# --- Advisory exclusive file locking -------------------------------------
@contextlib.contextmanager
def exclusive_file_lock(
    fileno: int, *, blocking: bool = True
) -> Iterator[None]:
    """Hold an advisory, exclusive lock on ``fileno`` for the block.

    Used by :class:`cronstable.state.FilesystemStateBackend` to serialise the
    read-modify-write of a lease file.  The reach of the lock is a property of
    the *mount*, not this code, which is what lets one backend serve both
    deployment shapes:

    * on a **local** filesystem the lock excludes other processes on the same
      host -- exactly right for single-node durability;
    * on a **shared** NFSv4 mount (an Amazon S3 Files / EFS mount) the same
      lock is honoured *across hosts*, so it excludes the fleet -- exactly
      right for HA.

    On POSIX this is ``fcntl.flock`` (whole-file, advisory: it does not block
    I/O by non-cooperating processes, which is fine because cronstable owns
    both
    sides).  On Windows it is ``msvcrt.locking`` over the first byte; Windows
    has no cross-host story, so it only ever serialises same-host processes
    (single-node), which is all the Windows target needs.  Blocking (the
    default): a stuck holder would wait here, so callers run the whole locked
    section in a worker thread (``asyncio.to_thread``) to keep it off the
    event loop, and the section itself only rewrites a tiny file, so
    contention is brief.

    With ``blocking=False`` a contended lock raises ``OSError`` immediately
    instead of waiting (``EWOULDBLOCK``/``EAGAIN`` on POSIX, ``EACCES`` on
    Windows).  Used by the lock-fidelity probe
    (:meth:`cronstable.state.FilesystemStateBackend.verify_locking`), whose
    whole
    point is observing that a second lock attempt on an already-locked file
    *fails*: a mount whose locks are silent no-ops would grant it.
    """
    if sys.platform == "win32":  # pragma: no cover - Windows-only path
        # msvcrt.locking locks ``nbytes`` from the current file position; lock
        # the first byte (the caller guarantees the lock file has one).
        # msvcrt has no true blocking mode: LK_LOCK retries internally about
        # once a second for ~10 attempts and then raises OSError -- which
        # would surface as a spurious lease failure whenever another process
        # held the lock a little long.  Emulate flock's indefinite block with
        # a non-blocking attempt loop instead; callers already run this on a
        # worker thread, so the sleep never touches the event loop.
        os.lseek(fileno, 0, os.SEEK_SET)
        while True:
            try:
                msvcrt.locking(fileno, msvcrt.LK_NBLCK, 1)
                break
            except OSError as ex:
                # retry only CONTENTION (EACCES from LK_NBLCK, EDEADLOCK
                # from the CRT); any other error -- a closed/invalid fd,
                # say -- must surface, not become an infinite spin.
                if ex.errno not in (errno.EACCES, errno.EDEADLOCK):
                    raise
                if not blocking:
                    raise
                time.sleep(0.05)
        try:
            yield
        finally:
            os.lseek(fileno, 0, os.SEEK_SET)
            msvcrt.locking(fileno, msvcrt.LK_UNLCK, 1)
    else:
        flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
        fcntl.flock(fileno, flags)
        try:
            yield
        finally:
            fcntl.flock(fileno, fcntl.LOCK_UN)


def fsync_directory(path: str) -> None:
    """Best-effort flush of a directory's own durability to disk.

    A file's own fsync only guarantees ITS bytes are durable; the directory
    ENTRY that makes the file (or a freshly created subdirectory) reachable
    from its parent is separate metadata, and needs its own flush -- without
    it a power loss can drop a perfectly-fsynced file because the directory
    forgot it was ever created.  Used by
    :class:`cronstable.state.FilesystemStateBackend` after an atomic rename, a
    document delete, and when a stream/namespace/blob-shard directory is
    freshly created.

    POSIX opens the directory like any other file handle and fsyncs it. The
    ``os`` module has no equivalent for Windows, so this reaches for the
    underlying Win32 calls via ctypes: ``CreateFileW`` with
    ``FILE_FLAG_BACKUP_SEMANTICS`` to obtain a directory handle at all
    (``GENERIC_WRITE`` access -- a directory handle opened read-only is
    accepted but ``FlushFileBuffers`` on it fails with ACCESS_DENIED), then
    ``FlushFileBuffers`` on it.  Best-effort either way: any failure (a
    filesystem that does not support it, a permissions quirk, a path that
    vanished) is swallowed, because the data this guards is still correct
    without it, just not crash-durable for this one write.
    """
    if IS_WINDOWS:  # pragma: no cover - Windows-only path
        try:
            import ctypes
            from ctypes import wintypes

            GENERIC_WRITE = 0x40000000
            FILE_SHARE_READ = 0x00000001
            FILE_SHARE_WRITE = 0x00000002
            FILE_SHARE_DELETE = 0x00000004
            OPEN_EXISTING = 3
            FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
            INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value

            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            kernel32.CreateFileW.restype = wintypes.HANDLE
            kernel32.CreateFileW.argtypes = [
                wintypes.LPCWSTR,
                wintypes.DWORD,
                wintypes.DWORD,
                wintypes.LPVOID,
                wintypes.DWORD,
                wintypes.DWORD,
                wintypes.HANDLE,
            ]
            kernel32.FlushFileBuffers.restype = wintypes.BOOL
            kernel32.FlushFileBuffers.argtypes = [wintypes.HANDLE]
            kernel32.CloseHandle.restype = wintypes.BOOL
            kernel32.CloseHandle.argtypes = [wintypes.HANDLE]

            handle = kernel32.CreateFileW(
                path,
                GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                None,
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS,
                None,
            )
            if handle in (0, INVALID_HANDLE_VALUE):
                return
            try:
                kernel32.FlushFileBuffers(handle)
            finally:
                kernel32.CloseHandle(handle)
        except Exception:  # noqa: BLE001 - best-effort; never raise
            return
        return
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)
