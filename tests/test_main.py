import asyncio
import sys
from pathlib import Path

import pytest

import yacron2.__main__
from yacron2.config import parse_config


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
    monkeypatch.setattr(yacron2.__main__, "Cron", FakeCron)
    config_file = str(Path(__file__).parent / "testconfig.yaml")
    monkeypatch.setattr(sys, "argv", ["yacron2", "-c", config_file])
    yacron2.__main__.main_loop(loop)


def test_broken_config(monkeypatch):
    loop = asyncio.new_event_loop()
    monkeypatch.setattr(yacron2.__main__, "Cron", FakeCron)
    config_file = str(Path(__file__).parent / "testbrokenconfig.yaml")
    monkeypatch.setattr(sys, "argv", ["yacron2", "-c", config_file])
    monkeypatch.setattr(sys, "exit", exit)
    with pytest.raises(ExitError):
        yacron2.__main__.main_loop(loop)


def test_missing_config(monkeypatch):
    loop = asyncio.new_event_loop()
    monkeypatch.setattr(yacron2.__main__, "Cron", FakeCron)
    config_file = str(Path(__file__).parent / "doesnotexist.yaml")
    monkeypatch.setattr(sys, "argv", ["yacron2", "-c", config_file])
    monkeypatch.setattr(sys, "exit", exit)
    with pytest.raises(ExitError):
        yacron2.__main__.main_loop(loop)
