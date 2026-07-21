import asyncio
import os

import pytest
from aiohttp import web

import cronstable.config
import cronstable.cron
from cronstable import platform


def test_encode_argv_matches_platform():
    argv = ["echo", "héllo"]
    encoded = platform.encode_argv(argv)
    if platform.IS_WINDOWS:
        # CreateProcessW takes str; bytes would break list2cmdline.
        assert encoded == argv
        assert all(isinstance(a, str) for a in encoded)
    else:
        # locale-independent UTF-8 argv on POSIX.
        assert encoded == [a.encode() for a in argv]
        assert all(isinstance(a, bytes) for a in encoded)


def test_default_shell_matches_platform():
    if platform.IS_WINDOWS:
        # empty -> route through create_subprocess_shell (cmd.exe /c)
        assert platform.DEFAULT_SHELL == ""
    else:
        assert platform.DEFAULT_SHELL == "/bin/sh"


def test_default_config_path_matches_platform():
    if platform.IS_WINDOWS:
        assert platform.DEFAULT_CONFIG_PATH.endswith("cronstable")
        assert platform.DEFAULT_CONFIG_PATH != "cronstable"  # has a parent dir
    else:
        assert platform.DEFAULT_CONFIG_PATH == "/etc/cronstable.d"


def test_supports_unix_sockets_matches_platform():
    assert platform.supports_unix_sockets() == (not platform.IS_WINDOWS)


def test_new_process_group_kwargs_matches_platform():
    kwargs = platform.new_process_group_kwargs()
    if platform.IS_WINDOWS:
        # no session to create at spawn time; the tree is walked at kill time.
        assert kwargs == {}
    else:
        assert kwargs == {"start_new_session": True}


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="killpg / process groups are POSIX-only"
)
@pytest.mark.asyncio
async def test_kill_process_group_signals_the_group_then_reports_it_gone():
    import sys

    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import time; time.sleep(30)",
        **platform.new_process_group_kwargs(),
    )
    assert await platform.kill_process_group(proc.pid, force=True)
    await asyncio.wait_for(proc.wait(), 10)
    # the group is empty now: nothing was signalled, so the caller is told to
    # fall back rather than being left thinking the kill landed.
    assert not await platform.kill_process_group(proc.pid, force=True)


def test_config_uses_platform_default_shell():
    conf = cronstable.config.parse_config_string(
        """
jobs:
  - name: t
    command: echo hi
    schedule: "* * * * *"
""",
        "",
    )
    assert conf.jobs[0].shell == platform.DEFAULT_SHELL


@pytest.mark.skipif(
    not platform.IS_WINDOWS, reason="user/group rejection is Windows-specific"
)
def test_user_group_rejected_on_windows():
    with pytest.raises(cronstable.config.ConfigError) as exc:
        cronstable.config.parse_config_string(
            """
jobs:
  - name: t
    command: echo hi
    schedule: "* * * * *"
    user: someuser
""",
            "",
        )
    assert "Windows" in str(exc.value)


def test_web_site_from_url_unix_socket():
    url = "unix:///tmp/cronstable.sock"
    if platform.IS_WINDOWS:
        # asyncio can't serve a unix socket on Windows: skipped as a bad entry
        # (raises before the runner is ever touched).
        with pytest.raises(ValueError):
            cronstable.cron.web_site_from_url(None, url)
    else:
        # POSIX: a unix listener is accepted. UnixSite.__init__ dereferences
        # runner.server, so pass a minimal stand-in instead of None.
        class _FakeRunner:
            server = object()

        site = cronstable.cron.web_site_from_url(_FakeRunner(), url)
        assert isinstance(site, web.UnixSite)


def test_install_shutdown_handlers_roundtrip():
    # Exercises install + the returned cleanup on both platforms (loop signal
    # handlers on POSIX; signal.signal + heartbeat on Windows) without firing a
    # real signal.  Must run on the main thread (signal.signal requires it).
    loop = asyncio.new_event_loop()
    try:
        called = []
        cleanup = platform.install_shutdown_handlers(
            loop, lambda: called.append(1)
        )
        assert callable(cleanup)
        cleanup()
    finally:
        loop.close()


def test_nonblocking_lock_raises_on_contention(tmp_path):
    # blocking=False must surface contention as an immediate OSError (the
    # lock-fidelity probe DEPENDS on the second attempt failing; a store
    # where it succeeds has no-op locks). Two descriptors of one file
    # contend on both platforms: POSIX flock is per-open-file-description,
    # Windows byte-range locks are per-handle.
    path = tmp_path / "lockfile"
    path.write_bytes(b"\0")
    fd1 = os.open(str(path), os.O_RDWR)
    fd2 = os.open(str(path), os.O_RDWR)
    try:
        with platform.exclusive_file_lock(fd1, blocking=False):
            with pytest.raises(OSError):
                with platform.exclusive_file_lock(fd2, blocking=False):
                    pass
        # released: the second descriptor may now take it
        with platform.exclusive_file_lock(fd2, blocking=False):
            pass
    finally:
        os.close(fd1)
        os.close(fd2)


def test_pid_alive_own_and_bogus_pid():
    # our own process exists; a hugely out-of-range pid does not. None is
    # reserved for "cannot tell" (treated as dead by reconciliation, which
    # the per-process token already vouches for).
    assert platform.pid_alive(os.getpid()) is True
    assert platform.pid_alive(2**22 + 12345) in (False, None)
    assert platform.pid_alive(0) is None


def test_fsync_directory_on_existing_and_nested_dir(tmp_path):
    # must not raise for a plain existing dir, nor for a directory nested
    # several levels deep and freshly created in this same test (the case
    # that matters: a stream/namespace dir a state write just makedirs'd).
    platform.fsync_directory(str(tmp_path))
    nested = tmp_path / "a" / "b" / "c"
    nested.mkdir(parents=True)
    platform.fsync_directory(str(nested))


def test_fsync_directory_swallows_missing_path(tmp_path):
    # best-effort: a vanished/never-existed path must not raise.
    platform.fsync_directory(str(tmp_path / "does" / "not" / "exist"))


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="killpg is POSIX-only"
)
@pytest.mark.asyncio
async def test_kill_process_group_falls_back_when_killpg_errors(monkeypatch):
    # A killpg that fails with a generic OSError (not ProcessLookupError) is
    # logged and reported as "not signalled" so the caller falls back to
    # terminating the direct child alone, rather than assuming the kill landed.
    def boom(pid, sig):
        raise OSError("no permission to signal that group")

    monkeypatch.setattr(platform.os, "killpg", boom)
    assert not await platform.kill_process_group(4242, force=True)


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="reads /proc, POSIX-only"
)
def test_os_boot_id_reads_the_kernel_value_or_none():
    # On Linux this file publishes a fresh UUID per boot; the reader returns a
    # non-empty string (or None where the file is unavailable, e.g. a container
    # that does not expose it). Either way it must never raise.
    value = platform.os_boot_id()
    assert value is None or (isinstance(value, str) and value)


def test_os_boot_id_returns_none_when_unreadable(monkeypatch):
    # A missing/unreadable boot_id file yields None (callers fall back to
    # os_boot_time), not an exception.
    import builtins

    real_open = builtins.open

    def refuse(path, *a, **k):
        if "boot_id" in str(path):
            raise OSError("no such file")
        return real_open(path, *a, **k)

    monkeypatch.setattr(builtins, "open", refuse)
    assert platform.os_boot_id() is None


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="reads /proc/uptime, POSIX-only"
)
def test_os_boot_time_returns_none_when_uptime_unreadable(monkeypatch):
    # /proc/uptime unreadable (or garbage) -> cannot tell -> None, so callers
    # treat the daemon start as a fresh boot instead of crashing.
    import builtins

    real_open = builtins.open

    def refuse(path, *a, **k):
        if "uptime" in str(path):
            raise OSError("no such file")
        return real_open(path, *a, **k)

    monkeypatch.setattr(builtins, "open", refuse)
    assert platform.os_boot_time() is None


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="uses os.kill(pid, 0), POSIX-only"
)
def test_pid_alive_reports_permission_denied_as_alive(monkeypatch):
    # A pid we cannot signal but that exists (EPERM) counts as alive: existence
    # is what reconciliation asks, not ownership.
    def eperm(pid, sig):
        raise PermissionError("operation not permitted")

    monkeypatch.setattr(platform.os, "kill", eperm)
    assert platform.pid_alive(4242) is True


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="uses os.kill(pid, 0), POSIX-only"
)
def test_pid_alive_reports_none_on_other_oserror(monkeypatch):
    # Any other OSError means the platform could not answer -> None ("cannot
    # tell"), which reconciliation treats as dead.
    def oserr(pid, sig):
        raise OSError("something unexpected")

    monkeypatch.setattr(platform.os, "kill", oserr)
    assert platform.pid_alive(4242) is None


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="POSIX fsync path"
)
def test_fsync_directory_swallows_fsync_failure(monkeypatch, tmp_path):
    # A directory that opens but whose fsync fails (some filesystems reject it)
    # is swallowed: the data it guards is correct without it, just not
    # crash-durable for this one write.
    def refuse(fd):
        raise OSError("fsync not supported here")

    monkeypatch.setattr(platform.os, "fsync", refuse)
    platform.fsync_directory(str(tmp_path))
