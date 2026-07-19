import argparse
import asyncio
import logging
import os
import sys
from typing import Any

import cronstable.version
from cronstable import platform

# Where -c looks when not given: /etc/cronstable.d on POSIX,
# %APPDATA%\cronstable on
# Windows (see cronstable.platform).
CONFIG_DEFAULT = platform.DEFAULT_CONFIG_PATH


def _add_state_subcommands(parser: argparse.ArgumentParser) -> None:
    """Wire the `cronstable state <action>` administration subcommands.

    Bare `cronstable` (no subcommand) stays the daemon.  Each action accepts
    its own -c/--config (same dest and default as the daemon flag) so both
    `cronstable -c X state gc` and `cronstable state gc -c X` work.
    """
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    state = sub.add_parser(
        "state",
        help="administer the durable state store (backup/restore/migrate/"
        "gc/check/migrate-schema)",
    )
    actions = state.add_subparsers(dest="state_command", metavar="ACTION")

    def _with_config(sub_parser):
        # SUPPRESS, not CONFIG_DEFAULT: a subparser default would otherwise
        # OVERWRITE a root-level `cronstable -c X state ...` value (argparse
        # applies subparser defaults after the root parse). The root parser
        # already supplies the default.
        sub_parser.add_argument(
            "-c",
            "--config",
            default=argparse.SUPPRESS,
            metavar="FILE-OR-DIR",
            help="configuration with the `state:` section to administer",
        )
        return sub_parser

    backup = _with_config(
        actions.add_parser(
            "backup", help="write a .tar.gz backup of the store"
        )
    )
    backup.add_argument("-o", "--output", required=True, metavar="FILE.tar.gz")
    restore = _with_config(
        actions.add_parser("restore", help="restore a backup into the store")
    )
    restore.add_argument("archive", metavar="FILE.tar.gz")
    restore.add_argument(
        "--force",
        default=False,
        action="store_true",
        help="merge into a non-empty store (NOT safe while a daemon uses it)",
    )
    migrate = _with_config(
        actions.add_parser(
            "migrate",
            help="copy the store to another path or mount "
            "(local disk <-> S3 Files / EFS)",
        )
    )
    migrate.add_argument("--dest", required=True, metavar="PATH")
    migrate.add_argument(
        "--dest-deployment-id",
        default=None,
        metavar="ID",
        help="namespace at the destination (default: keep the current one)",
    )
    migrate.add_argument(
        "--force",
        default=False,
        action="store_true",
        help="overwrite a non-empty destination store",
    )
    gc = _with_config(
        actions.add_parser(
            "gc", help="garbage-collect state of unreferenced jobs"
        )
    )
    gc.add_argument("--dry-run", default=False, action="store_true")
    _with_config(
        actions.add_parser(
            "check", help="verify the store is usable and print an inventory"
        )
    )
    migrate_schema = _with_config(
        actions.add_parser(
            "migrate-schema",
            help="rewrite records of older known record schemes",
        )
    )
    migrate_schema.add_argument(
        "--dry-run", default=False, action="store_true"
    )

    # The job-facing state commands. The KV actions (get/set/delete/
    # keys) hang off the SAME `state` subparser as the admin actions above and
    # coexist with them (the action name routes); the other verbs (cursor/
    # lock/artifact/idempotent/secret) are their own top-level commands. Both
    # are thin clients of the daemon's loopback endpoint. Imported here (not at
    # module load) so the import cost is paid only when the CLI is built.
    from cronstable import jobcli

    jobcli.add_state_job_actions(actions)
    jobcli.add_job_commands(sub)

    # `cronstable mcp` and `cronstable tui` register as bare stubs below, NOT
    # by importing cronstable.mcpcli / cronstable.tui. Importing tui alone runs
    # its ~7000-line module body and pulls unicodedata's C table plus dozens of
    # other modules (~50ms), and every job-spawned thin client (`state get`,
    # `lock`, `xcom pull`) builds this parser first, so the eager import
    # taxed a hot path for two commands almost never the one invoked. The real
    # modules are imported only inside their dispatch branches (see main_loop).
    # A parity test (tests/test_cli_stubs.py) keeps the stub flags in lockstep
    # with the real add_mcp_command / add_tui_command definitions.
    _add_mcp_stub(sub)
    _add_tui_stub(sub)


# Mirrors of cronstable.mcpcli / cronstable.tui module constants used only to
# register their subparsers. Kept here so building the CLI never imports those
# modules; the parity test asserts these match the originals.
_MCP_DEFAULT_URL = "http://127.0.0.1:8080"
_MCP_ENV_TOKEN = "CRONSTABLE_WEB_TOKEN"
_MCP_DEFAULT_PROTOCOL_VERSION = "2025-11-25"
_MCP_DEFAULT_TIMEOUT = 30.0
_TUI_DEFAULT_URL = "http://127.0.0.1:8080"
_TUI_ENV_TOKEN = "CRONSTABLE_WEB_TOKEN"
_TUI_THEME_HUES = ["carolina", "amber", "green", "modern", "standard"]


def _add_mcp_stub(sub: Any) -> None:
    """Register `cronstable mcp` without importing cronstable.mcpcli.

    Must stay flag-for-flag identical to mcpcli.add_mcp_command; the parity
    test enforces it.
    """
    parser = sub.add_parser(
        "mcp",
        help="run the MCP stdio bridge to a running daemon's /mcp endpoint "
        "(for desktop MCP clients)",
    )
    parser.add_argument(
        "--url",
        default=_MCP_DEFAULT_URL,
        metavar="URL",
        help="daemon web base URL serving /mcp (default: %(default)s)",
    )
    parser.add_argument(
        "--token",
        default=None,
        metavar="TOKEN",
        help="web.authToken bearer value (prefer --token-env to keep it out "
        "of the process table)",
    )
    parser.add_argument(
        "--token-env",
        default=None,
        metavar="VAR",
        help="env var holding the bearer token (default: {} if set)".format(
            _MCP_ENV_TOKEN
        ),
    )
    parser.add_argument(
        "--protocol-version",
        default=None,
        metavar="REV",
        help="pin the MCP-Protocol-Version sent before initialize "
        "(default: {})".format(_MCP_DEFAULT_PROTOCOL_VERSION),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=_MCP_DEFAULT_TIMEOUT,
        metavar="SECONDS",
        help="per-request deadline (default: %(default)s)",
    )
    parser.add_argument(
        "--check",
        dest="mcp_check",
        default=False,
        action="store_true",
        help="handshake the endpoint (initialize + tools/list) and exit, "
        "instead of proxying stdin",
    )


def _add_tui_stub(sub: Any) -> None:
    """Register `cronstable tui` without importing cronstable.tui.

    Must stay flag-for-flag identical to tui.add_tui_command; the parity test
    enforces it.
    """
    parser = sub.add_parser(
        "tui",
        help=(
            "open the terminal dashboard (the web dashboard's TUI "
            "sibling) against a running daemon's web listener"
        ),
        description=(
            "A keyboard-driven terminal rendition of the cronstable web "
            "dashboard, speaking the same HTTP control API. The web "
            "page's shortcuts apply: j/k move, Enter opens a job, r "
            "runs it, x cancels, / filters, Ctrl-K opens the command "
            "palette, ? lists every key."
        ),
    )
    parser.add_argument(
        "--url",
        default=_TUI_DEFAULT_URL,
        help="daemon web listener (default: %(default)s)",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="bearer token for web.authToken-protected daemons",
    )
    parser.add_argument(
        "--token-env",
        default=_TUI_ENV_TOKEN,
        metavar="VAR",
        help=(
            "environment variable to read the token from when --token "
            "is not given (default: %(default)s)"
        ),
    )
    parser.add_argument(
        "--theme",
        default=None,
        choices=list(_TUI_THEME_HUES)
        + [h + "-light" for h in _TUI_THEME_HUES],
        help="start on a specific theme (persisted for next time)",
    )
    parser.add_argument(
        "--tv",
        action="store_true",
        help="start straight on the wallboard (the page's #tv)",
    )
    parser.add_argument(
        "--job",
        default=None,
        metavar="NAME",
        help="open a job's drawer at startup (the page's #job/NAME)",
    )
    parser.add_argument(
        "--boot",
        action="store_true",
        help="force the boot self-test even if one ran recently",
    )
    parser.add_argument(
        "--no-boot",
        action="store_true",
        help="skip the boot self-test",
    )
    parser.add_argument(
        "--ascii",
        action="store_true",
        help="plain-ASCII status glyphs (limited fonts/terminals)",
    )
    parser.add_argument(
        "--poll",
        type=float,
        default=None,
        metavar="SECONDS",
        help="refresh interval; 0 pauses (default: remembered, else 3)",
    )


def main_loop(loop):
    parser = argparse.ArgumentParser(prog="cronstable")
    parser.add_argument(
        "-c",
        "--config",
        default=CONFIG_DEFAULT,
        metavar="FILE-OR-DIR",
        help="configuration file, or directory containing configuration files",
    )
    parser.add_argument("-l", "--log-level", default="INFO")
    parser.add_argument(
        "-v", "--validate-config", default=False, action="store_true"
    )
    parser.add_argument(
        "--job-set-id",
        default=False,
        action="store_true",
        help="print the job-set id (an order-independent hash of every job's "
        "effective configuration) and exit; identical across instances "
        "running the same set of jobs",
    )
    parser.add_argument("--version", default=False, action="store_true")
    _add_state_subcommands(parser)
    # `lock run NAME [flags] -- CMD...` carries an arbitrary trailing command.
    # argparse cannot capture it portably: nargs=REMAINDER swallows our own
    # flags into the command list, while nargs="*" only picks up the tokens
    # after "--" on Python >= 3.13 (older argparse reports them as
    # "unrecognized arguments"). So split the command off at the first "--"
    # ourselves -- identical on every supported Python -- and hand argparse
    # only the head, where our flags and NAME parse cleanly everywhere.
    argv = sys.argv[1:]
    trailing_command = None
    if "--" in argv:
        cut = argv.index("--")
        argv, trailing_command = argv[:cut], argv[cut + 1 :]
    args = parser.parse_args(argv)
    if trailing_command is not None:
        if (
            getattr(args, "command", None) == "lock"
            and getattr(args, "lock_command", None) == "run"
        ):
            args.run_command = trailing_command
        else:
            parser.error("`--` is only valid before a `lock run` command")

    logging.basicConfig(level=getattr(logging, args.log_level))
    # logging.getLogger("asyncio").setLevel(logging.WARNING)
    logger = logging.getLogger("cronstable")

    if args.version:
        print(cronstable.version.version)
        sys.exit(0)

    command = getattr(args, "command", None)
    if command == "state":
        # `state get/set/delete/keys` are job-facing (they reach the running
        # daemon's loopback endpoint); everything else under `state` is offline
        # store administration. Route by action name so the two coexist.
        from cronstable import jobcli

        if getattr(args, "state_command", None) in jobcli.STATE_JOB_ACTIONS:
            sys.exit(jobcli.dispatch(args))
        # lazy import: the admin module (tarfile etc.) costs the daemon and
        # the stateless install nothing.
        from cronstable import state_admin

        sys.exit(state_admin.dispatch(args))

    if command in (
        "cursor",
        "lock",
        "artifact",
        "idempotent",
        "secret",
        "xcom",
    ):
        from cronstable import jobcli

        sys.exit(jobcli.dispatch(args))

    if command == "mcp":
        # the MCP stdio bridge: a thin stdlib client of the running daemon,
        # like the job-facing subcommands, so it never imports Cron/aiohttp.
        from cronstable import mcpcli

        sys.exit(mcpcli.dispatch(args))

    if command == "tui":
        # the terminal dashboard: a client of the running daemon's web
        # listener, dispatched before the Cron import below so its startup
        # pays for aiohttp only (inside cronstable.tui), never the daemon
        # graph.
        from cronstable import tui

        sys.exit(tui.dispatch(args))

    if args.config == CONFIG_DEFAULT and not os.path.exists(args.config):
        print(
            "cronstable error: configuration file not found, please provide "
            "one with the --config option",
            file=sys.stderr,
        )
        parser.print_help(sys.stderr)
        sys.exit(1)

    # Imported here, not at module top: this pulls in aiohttp, strictyaml,
    # sentry_sdk and the rest of the daemon graph (~300ms of import). The
    # branches that exit before this point -- --version and the state / xcom
    # / lock / cursor / artifact / idempotent / secret subcommands (thin
    # urllib clients of the running daemon, routinely spawned from inside
    # jobs) -- never touch Cron, so a job-facing CLI call no longer pays that
    # cost. Everything from here down (the daemon, --job-set-id,
    # --validate-config) needs it.
    from cronstable.cron import ConfigError, Cron

    try:
        cron = Cron(args.config)
    except ConfigError as err:
        logger.error("Configuration error: %s", str(err))
        sys.exit(1)

    if args.job_set_id:
        print(cron.job_set_id())
        sys.exit(0)

    if args.validate_config:
        logger.info("Configuration is valid.")
        sys.exit(0)

    # Wire Ctrl-C / termination to a graceful shutdown.  The mechanism differs
    # per platform (loop signal handlers on POSIX, signal.signal on Windows),
    # so it lives behind platform.install_shutdown_handlers.
    remove_shutdown_handlers = platform.install_shutdown_handlers(
        loop, cron.signal_shutdown
    )
    try:
        loop.run_until_complete(cron.run())
    finally:
        remove_shutdown_handlers()


def _new_event_loop():  # pragma: no cover
    """The event loop to run on: uvloop's faster libuv loop when available,
    otherwise stock asyncio.

    uvloop is a drop-in, libuv-based replacement for asyncio's selector loop
    that runs cronstable's I/O paths -- cluster gossip/lease HTTP, the web
    dashboard, the Prometheus scrape -- markedly faster. It is strictly
    optional (install the ``speedups`` extra to pull it in): it has no Windows
    build (where cronstable also needs the Proactor loop for subprocess
    support)
    and ships no wheels for some of the leaner architectures we target, so a
    missing or unimportable uvloop silently falls back to stock asyncio with
    identical behavior. Selecting the loop directly (rather than via
    ``asyncio.set_event_loop_policy``) sidesteps the event-loop-policy API that
    Python 3.14 deprecates.

    ``asyncio.new_event_loop()`` yields the right stock loop per platform: a
    subprocess-capable Proactor loop on Windows (the default since 3.8) and a
    selector loop on POSIX.
    """
    if sys.platform != "win32":
        try:
            import uvloop
        except ImportError:
            pass
        else:
            return uvloop.new_event_loop()
    return asyncio.new_event_loop()


def main():  # pragma: no cover
    _loop = _new_event_loop()
    try:
        main_loop(_loop)
    finally:
        _loop.close()


if __name__ == "__main__":  # pragma: no cover
    main()
