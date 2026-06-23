import asyncio

import pytest
from aiohttp import web

import yacron2.config
import yacron2.cron
from yacron2 import platform


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
        assert platform.DEFAULT_CONFIG_PATH.endswith("yacron2")
        assert platform.DEFAULT_CONFIG_PATH != "yacron2"  # has a parent dir
    else:
        assert platform.DEFAULT_CONFIG_PATH == "/etc/yacron2.d"


def test_supports_unix_sockets_matches_platform():
    assert platform.supports_unix_sockets() == (not platform.IS_WINDOWS)


def test_config_uses_platform_default_shell():
    conf = yacron2.config.parse_config_string(
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
    with pytest.raises(yacron2.config.ConfigError) as exc:
        yacron2.config.parse_config_string(
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
    url = "unix:///tmp/yacron2.sock"
    if platform.IS_WINDOWS:
        # asyncio can't serve a unix socket on Windows: skipped as a bad entry
        # (raises before the runner is ever touched).
        with pytest.raises(ValueError):
            yacron2.cron.web_site_from_url(None, url)
    else:
        # POSIX: a unix listener is accepted. UnixSite.__init__ dereferences
        # runner.server, so pass a minimal stand-in instead of None.
        class _FakeRunner:
            server = object()

        site = yacron2.cron.web_site_from_url(_FakeRunner(), url)
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
