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
