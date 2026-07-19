"""The `cronstable mcp` / `cronstable tui` subparsers are registered from
lightweight stubs in cronstable.__main__ so building the CLI never imports the
heavy tui / mcpcli modules (a cost every job-spawned thin client would pay).

These tests pin the stubs flag-for-flag to the real add_mcp_command /
add_tui_command, so any drift in the originals fails here instead of silently
diverging the two code paths.
"""

import argparse

import pytest

import cronstable.__main__ as main


def _register(func):
    parser = argparse.ArgumentParser(prog="cronstable")
    sub = parser.add_subparsers(dest="command")
    func(sub)
    return sub.choices


def _action_summary(parser):
    """A comparable, order-independent view of a subparser's options.

    Keyed by the sorted option strings; the auto-added -h/--help is dropped
    since both parsers get it for free.
    """
    summary = {}
    for action in parser._actions:
        opts = tuple(sorted(action.option_strings))
        if opts == ("--help", "-h"):
            continue
        summary[opts] = {
            "dest": action.dest,
            "default": action.default,
            "choices": (
                list(action.choices) if action.choices is not None else None
            ),
            "nargs": action.nargs,
            "const": action.const,
            "type": getattr(action.type, "__name__", action.type),
            "metavar": action.metavar,
            "help": action.help,
            "cls": type(action).__name__,
        }
    return summary


def test_mcp_stub_matches_real_registration():
    from cronstable import mcpcli

    real = _register(mcpcli.add_mcp_command)["mcp"]
    stub = _register(main._add_mcp_stub)["mcp"]
    assert _action_summary(stub) == _action_summary(real)


def test_tui_stub_matches_real_registration():
    from cronstable import tui

    real = _register(tui.add_tui_command)["tui"]
    stub = _register(main._add_tui_stub)["tui"]
    assert _action_summary(stub) == _action_summary(real)


@pytest.mark.parametrize(
    "const_name, module_name, attr",
    [
        ("_MCP_DEFAULT_URL", "cronstable.mcpcli", "DEFAULT_URL"),
        ("_MCP_ENV_TOKEN", "cronstable.mcpcli", "ENV_TOKEN"),
        (
            "_MCP_DEFAULT_PROTOCOL_VERSION",
            "cronstable.mcpcli",
            "DEFAULT_PROTOCOL_VERSION",
        ),
        ("_MCP_DEFAULT_TIMEOUT", "cronstable.mcpcli", "DEFAULT_TIMEOUT"),
        ("_TUI_DEFAULT_URL", "cronstable.tui", "DEFAULT_URL"),
        ("_TUI_ENV_TOKEN", "cronstable.tui", "ENV_TOKEN"),
        ("_TUI_THEME_HUES", "cronstable.tui", "THEME_HUES"),
    ],
)
def test_stub_constants_match_source(const_name, module_name, attr):
    import importlib

    module = importlib.import_module(module_name)
    assert getattr(main, const_name) == getattr(module, attr)


def test_building_cli_does_not_import_tui(monkeypatch):
    """The whole point: constructing the parser must not pull in cronstable.tui
    (its module body + unicodedata table cost ~50ms). mcpcli is likewise heavy
    enough to keep off the parser-build path every thin client walks.
    """
    import sys

    # Evict any copy imported by an earlier test; if building the parser
    # re-imports either module it reappears in sys.modules and the check fails.
    for name in ("cronstable.tui", "cronstable.mcpcli"):
        monkeypatch.delitem(sys.modules, name, raising=False)

    parser = argparse.ArgumentParser(prog="cronstable")
    main._add_state_subcommands(parser)

    assert "cronstable.tui" not in sys.modules
    assert "cronstable.mcpcli" not in sys.modules
