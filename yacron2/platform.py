"""OS-specific behavior, isolated so the rest of yacron2 stays portable.

yacron2 began life POSIX-only.  Everything that genuinely differs between Unix
and Windows lives here behind a small, uniform surface, so the scheduler, the
job runner, the config loader and the entry point read the same on every
platform and only this module needs a per-OS branch:

* :data:`DEFAULT_SHELL` -- how a string command is handed to a shell;
* :data:`DEFAULT_CONFIG_PATH` -- where ``-c`` looks by default;
* :func:`supports_unix_sockets` -- whether ``unix://`` web listeners work;
* :func:`encode_argv` -- the argv form the platform's subprocess layer wants;
* :func:`install_shutdown_handlers` -- wiring Ctrl-C / termination to a
  graceful-shutdown callback on whichever event loop the platform provides.

Per-job ``user``/``group`` switching stays in :mod:`yacron2.config` (it needs
the ``grp``/``pwd`` databases), but is likewise gated on :data:`IS_WINDOWS`.
"""

import asyncio
import logging
import os
import signal
import sys
from typing import Callable, List, Union

logger = logging.getLogger("yacron2")

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
    if IS_WINDOWS:
        # Per-user config under roaming AppData, e.g.
        # ``C:\Users\<you>\AppData\Roaming\yacron2``.  Falls back to the user
        # profile if APPDATA is somehow unset (rare; e.g. a bare service
        # account with no roaming profile).
        base = os.environ.get("APPDATA") or os.path.expanduser("~")
        return os.path.join(base, "yacron2")
    return "/etc/yacron2.d"


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
    if IS_WINDOWS:
        return list(argv)
    return [arg.encode() for arg in argv]


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

    # Windows: install plain C-level handlers and hop onto the loop thread.
    # (getattr, not signal.SIGBREAK, so this module also type-checks on POSIX,
    # where SIGBREAK does not exist.)
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
