import argparse
import asyncio
import logging
import os
import signal
import sys

import yacron2.version
from yacron2.cron import ConfigError, Cron

CONFIG_DEFAULT = "/etc/yacron2.d"


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

    if args.validate_config:
        logger.info("Configuration is valid.")
        sys.exit(0)

    loop.add_signal_handler(signal.SIGINT, cron.signal_shutdown)
    loop.add_signal_handler(signal.SIGTERM, cron.signal_shutdown)
    try:
        loop.run_until_complete(cron.run())
    finally:
        loop.remove_signal_handler(signal.SIGINT)
        loop.remove_signal_handler(signal.SIGTERM)


def main():  # pragma: no cover
    # yacron2 is POSIX-only (config.py imports grp/pwd at module load), so
    # there is no Windows event-loop branch to maintain here.
    _loop = asyncio.new_event_loop()
    try:
        main_loop(_loop)
    finally:
        _loop.close()


if __name__ == "__main__":  # pragma: no cover
    main()
