import asyncio
import logging
import sys
from pathlib import Path

import pytest

import cronstable.__main__
import cronstable.__main__ as main
import cronstable.version
from cronstable.config import parse_config
from cronstable.cron import ConfigError
from cronstable.fingerprint import SCHEME_VERSION


class FakeCron:
    def __init__(self, config_arg):
        parse_config(config_arg)

    async def run(self):
        return

    def signal_shutdown(self):
        pass


class ExitError(RuntimeError):
    pass


def exit(num):
    raise ExitError(num)


def test_good_config(monkeypatch):
    loop = asyncio.new_event_loop()
    # main_loop imports Cron lazily (from cronstable.cron, inside the function)
    # so a job-facing CLI call never drags in the daemon graph; patch it at its
    # source module, not on cronstable.__main__.
    monkeypatch.setattr("cronstable.cron.Cron", FakeCron)
    config_file = str(Path(__file__).parent / "testconfig.yaml")
    monkeypatch.setattr(sys, "argv", ["cronstable", "-c", config_file])
    cronstable.__main__.main_loop(loop)


def test_broken_config(monkeypatch):
    loop = asyncio.new_event_loop()
    monkeypatch.setattr("cronstable.cron.Cron", FakeCron)
    config_file = str(Path(__file__).parent / "testbrokenconfig.yaml")
    monkeypatch.setattr(sys, "argv", ["cronstable", "-c", config_file])
    monkeypatch.setattr(sys, "exit", exit)
    with pytest.raises(ExitError):
        cronstable.__main__.main_loop(loop)


def test_missing_config(monkeypatch):
    loop = asyncio.new_event_loop()
    monkeypatch.setattr("cronstable.cron.Cron", FakeCron)
    config_file = str(Path(__file__).parent / "doesnotexist.yaml")
    monkeypatch.setattr(sys, "argv", ["cronstable", "-c", config_file])
    monkeypatch.setattr(sys, "exit", exit)
    with pytest.raises(ExitError):
        cronstable.__main__.main_loop(loop)


def test_job_set_id_flag(monkeypatch, capsys):
    # uses the real Cron so the printed id reflects the parsed config
    loop = asyncio.new_event_loop()
    config_file = str(Path(__file__).parent / "testconfig.yaml")
    monkeypatch.setattr(
        sys, "argv", ["cronstable", "-c", config_file, "--job-set-id"]
    )
    monkeypatch.setattr(sys, "exit", exit)
    with pytest.raises(ExitError):
        cronstable.__main__.main_loop(loop)
    out = capsys.readouterr().out.strip()
    assert out.startswith(SCHEME_VERSION + ":")
    assert len(out.split(":", 1)[1]) == 64


# --- main_loop arg handling and dispatch routing ---
# Exercises the CLI entry's portable branches: --version, the `--`
# trailing-command split (both the `lock run` success path and the error
# path), the state / cursor / mcp / tui routing branches, the missing-default-
# config exit, and the Cron-backed --job-set-id / --validate-config /
# run-and-shutdown wiring. Each daemon-graph import is patched at its source
# module (as the tests above do with cronstable.cron.Cron) so a job-facing
# branch never drags in the real daemon.


def _loop():
    return asyncio.new_event_loop()


def test_version_prints_and_exits(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["cronstable", "--version"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == cronstable.version.version


def test_trailing_dashdash_without_lock_run_errors(monkeypatch, capsys):
    # `--` before anything other than a `lock run` command is rejected by the
    # hand-rolled split (argparse would already have exited otherwise).
    monkeypatch.setattr(sys, "argv", ["cronstable", "--", "echo", "hi"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 2  # argparse's parser.error() exit status
    assert "only valid before a `lock run`" in capsys.readouterr().err


def test_state_get_routes_to_jobcli(monkeypatch):
    seen = {}

    def fake_dispatch(args):
        seen["command"] = args.command
        seen["state_command"] = args.state_command
        return 7

    monkeypatch.setattr("cronstable.jobcli.dispatch", fake_dispatch)
    monkeypatch.setattr(sys, "argv", ["cronstable", "state", "get", "mykey"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 7
    assert seen == {"command": "state", "state_command": "get"}


def test_state_check_routes_to_state_admin(monkeypatch):
    seen = {}

    def fake_dispatch(args):
        seen["command"] = args.command
        seen["state_command"] = args.state_command
        return 3

    monkeypatch.setattr("cronstable.state_admin.dispatch", fake_dispatch)
    monkeypatch.setattr(sys, "argv", ["cronstable", "state", "check"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 3
    assert seen == {"command": "state", "state_command": "check"}


def test_cursor_routes_to_jobcli(monkeypatch):
    seen = {}

    def fake_dispatch(args):
        seen["command"] = args.command
        return 5

    monkeypatch.setattr("cronstable.jobcli.dispatch", fake_dispatch)
    monkeypatch.setattr(sys, "argv", ["cronstable", "cursor", "get", "wm"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 5
    assert seen["command"] == "cursor"


def test_lock_run_trailing_command_captured(monkeypatch):
    # The `lock run NAME -- CMD...` success path: the tokens after `--` are
    # split off and stored on args.run_command before dispatch.
    seen = {}

    def fake_dispatch(args):
        seen["command"] = args.command
        seen["lock_command"] = args.lock_command
        seen["run_command"] = args.run_command
        return 0

    monkeypatch.setattr("cronstable.jobcli.dispatch", fake_dispatch)
    monkeypatch.setattr(
        sys,
        "argv",
        ["cronstable", "lock", "run", "mylock", "--", "echo", "hi"],
    )
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 0
    assert seen["command"] == "lock"
    assert seen["lock_command"] == "run"
    assert seen["run_command"] == ["echo", "hi"]


def test_mcp_routes_to_mcpcli(monkeypatch):
    seen = {}

    def fake_dispatch(args):
        seen["command"] = args.command
        return 0

    monkeypatch.setattr("cronstable.mcpcli.dispatch", fake_dispatch)
    monkeypatch.setattr(sys, "argv", ["cronstable", "mcp"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 0
    assert seen["command"] == "mcp"


def test_tui_routes_to_tui(monkeypatch):
    seen = {}

    def fake_dispatch(args):
        seen["command"] = args.command
        return 0

    monkeypatch.setattr("cronstable.tui.dispatch", fake_dispatch)
    monkeypatch.setattr(sys, "argv", ["cronstable", "tui"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 0
    assert seen["command"] == "tui"


def test_missing_default_config_exits_1(monkeypatch, capsys):
    # No -c given (args.config stays at CONFIG_DEFAULT) and the default path
    # does not exist -> print an error, dump help, exit 1.
    monkeypatch.setattr("cronstable.__main__.os.path.exists", lambda p: False)
    monkeypatch.setattr(sys, "argv", ["cronstable"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 1
    assert "configuration file not found" in capsys.readouterr().err


def test_config_error_exits_1(monkeypatch):
    # A ConfigError from constructing Cron is caught and turned into exit 1.
    class BadCron:
        def __init__(self, config):
            raise ConfigError("boom")

    monkeypatch.setattr("cronstable.cron.Cron", BadCron)
    monkeypatch.setattr(sys, "argv", ["cronstable", "-c", "config.yaml"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 1


def test_job_set_id_prints_and_exits(monkeypatch, capsys):
    class FakeCron:
        def __init__(self, config):
            pass

        def job_set_id(self):
            return "deadbeef"

    monkeypatch.setattr("cronstable.cron.Cron", FakeCron)
    monkeypatch.setattr(
        sys, "argv", ["cronstable", "-c", "config.yaml", "--job-set-id"]
    )
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == "deadbeef"


def test_validate_config_exits_0(monkeypatch, caplog):
    class FakeCron:
        def __init__(self, config):
            pass

    monkeypatch.setattr("cronstable.cron.Cron", FakeCron)
    monkeypatch.setattr(
        sys, "argv", ["cronstable", "-c", "config.yaml", "-v"]
    )
    with caplog.at_level(logging.INFO, logger="cronstable"):
        with pytest.raises(SystemExit) as exc:
            main.main_loop(_loop())
    assert exc.value.code == 0
    assert "Configuration is valid." in caplog.text


def test_run_and_shutdown_wiring(monkeypatch):
    # The daemon path: install shutdown handlers, drive cron.run() to
    # completion on the loop, then tear the handlers down in the finally.
    ran = {"value": False}

    class RunCron:
        def __init__(self, config):
            pass

        async def run(self):
            ran["value"] = True

        def signal_shutdown(self):
            pass

    monkeypatch.setattr("cronstable.cron.Cron", RunCron)
    monkeypatch.setattr(sys, "argv", ["cronstable", "-c", "config.yaml"])
    loop = asyncio.new_event_loop()
    try:
        # returns normally (no sys.exit) once cron.run() finishes
        main.main_loop(loop)
    finally:
        loop.close()
    assert ran["value"] is True
