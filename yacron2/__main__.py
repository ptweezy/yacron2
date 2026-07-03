import argparse
import asyncio
import logging
import os
import sys

import yacron2.version
from yacron2 import platform
from yacron2.cron import ConfigError, Cron

# Where -c looks when not given: /etc/yacron2.d on POSIX, %APPDATA%\yacron2 on
# Windows (see yacron2.platform).
CONFIG_DEFAULT = platform.DEFAULT_CONFIG_PATH


def main_loop(loop):
    parser = argparse.ArgumentParser(prog="yacron2")
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
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level))
    # logging.getLogger("asyncio").setLevel(logging.WARNING)
    logger = logging.getLogger("yacron2")

    if args.version:
        print(yacron2.version.version)
        sys.exit(0)

    if args.config == CONFIG_DEFAULT and not os.path.exists(args.config):
        print(
            "yacron2 error: configuration file not found, please provide one "
            "with the --config option",
            file=sys.stderr,
        )
        parser.print_help(sys.stderr)
        sys.exit(1)

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
    that runs yacron2's I/O paths -- cluster gossip/lease HTTP, the web
    dashboard, the Prometheus scrape -- markedly faster. It is strictly
    optional (install the ``speedups`` extra to pull it in): it has no Windows
    build (where yacron2 also needs the Proactor loop for subprocess support)
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
