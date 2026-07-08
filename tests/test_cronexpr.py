"""The in-house cron engine matches the library it replaced, vector by vector.

``tests/data/cron_golden.json`` records what the legacy ``crontab``
(parse-crontab) package did for every expression in the corpus: whether it
parsed, ``next()`` from fixed naive and timezone-aware instants, ``test()``
over fixed datetimes, and semantic-equality verdicts.  These tests replay
every vector against :mod:`cronstable.cronexpr` -- a full compatibility proof
that needs no copy of the old package (the vectors are program OUTPUT, not
code).  Regenerate/extend the vectors with ``tests/gen_cron_golden.py``.

If the old package happens to be importable (a dev machine, never CI), a live
differential test ALSO cross-checks every vector directly, so corpus edits
that predate a regeneration still get caught.
"""

import datetime
import json
import os
from zoneinfo import ZoneInfo

import pytest

from cronstable.cronexpr import CronTab

# next() deltas are compared to a nanosecond: the legacy library computed
# its float seconds along a different arithmetic path, so a microsecond
# `now` can disagree in the last bits (1.999999 vs 1.9999989999999999).
# Real divergences are at least one whole microsecond.
_TOL = 1e-9


def _close(got, expected):
    if got is None or expected is None:
        return got is None and expected is None
    return abs(got - expected) < _TOL


GOLDEN = os.path.join(os.path.dirname(__file__), "data", "cron_golden.json")

with open(GOLDEN, encoding="utf-8") as _f:
    _G = json.load(_f)

_EXPRS = sorted(_G["exprs"])


def _aware(spec):
    dt = datetime.datetime.fromisoformat(spec["dt"])
    return dt.replace(tzinfo=ZoneInfo(spec["tz"]), fold=spec["fold"])


@pytest.mark.parametrize("expr", _EXPRS)
def test_golden(expr):
    entry = _G["exprs"][expr]
    if not entry["ok"]:
        with pytest.raises(ValueError):
            CronTab(expr)
        return
    ct = CronTab(expr)
    for now_s, expected in zip(
        _G["naive_next_nows"], entry["next"], strict=True
    ):
        got = ct.next(
            now=datetime.datetime.fromisoformat(now_s), default_utc=True
        )
        assert _close(got, expected), "next({}) for {!r}: {} != {}".format(
            now_s, expr, got, expected
        )
    for spec, expected in zip(
        _G["aware_next_nows"], entry["aware_next"], strict=True
    ):
        got = ct.next(now=_aware(spec), default_utc=False)
        assert _close(got, expected), "next({}) for {!r}: {} != {}".format(
            spec, expr, got, expected
        )
    for dt_s, expected in zip(_G["test_dts"], entry["test"], strict=True):
        got = ct.test(datetime.datetime.fromisoformat(dt_s))
        assert got is expected, "test({}) for {!r}".format(dt_s, expr)


def test_golden_equality():
    for pair in _G["equality"]:
        got = CronTab(pair["a"]) == CronTab(pair["b"])
        assert got is pair["equal"], "{a!r} == {b!r}".format(**pair)


def test_next_returns_float_and_none():
    ct = CronTab("* * * * *")
    value = ct.next(
        now=datetime.datetime(2026, 1, 7, 12, 0, 30), default_utc=True
    )
    assert isinstance(value, float) and value == 30.0
    gone = CronTab("0 0 1 1 * 2020")
    assert (
        gone.next(now=datetime.datetime(2026, 1, 7), default_utc=True) is None
    )


def test_unhashable_like_the_old_class():
    with pytest.raises(TypeError):
        hash(CronTab("* * * * *"))


def test_eq_against_other_types():
    assert (CronTab("* * * * *") == "* * * * *") is False
    assert (CronTab("* * * * *") != "* * * * *") is True


def test_str_is_stable_and_normalized():
    # The dag scheduler embeds str(schedule) in its reload signature; two
    # parses of the same expression must stringify identically.
    assert str(CronTab("*/5  *   * * *")) == "*/5 * * * *"
    assert str(CronTab("*/5 * * * *")) == str(CronTab("*/5 * * * *"))
    assert str(CronTab("@daily")) == "@daily"
    assert repr(CronTab("@daily")) == "CronTab('@daily')"


def test_aware_test_reads_civil_fields():
    ny = ZoneInfo("America/New_York")
    ct = CronTab("30 2 * * *")
    assert ct.test(datetime.datetime(2026, 1, 7, 2, 30, tzinfo=ny))
    assert not ct.test(datetime.datetime(2026, 1, 7, 7, 30, tzinfo=ny))


def test_now_omitted_uses_wall_clock():
    # Not covered by the (deterministic) goldens: with no `now`, the engine
    # reads the current wall clock -- UTC when default_utc says so.
    value = CronTab("* * * * *").next(default_utc=True)
    assert value is not None and 0 < value <= 60
    value = CronTab("* * * * *").next(default_utc=False)
    assert value is not None and 0 < value <= 60


def test_live_differential_against_old_library():
    """Cross-check every golden vector against the real old library.

    Runs only where the legacy package is installed (a dev machine after
    ``pip install "crontab>=1,<2"``); CI relies on the committed vectors.
    """
    old = pytest.importorskip("crontab")
    for expr in _EXPRS:
        entry = _G["exprs"][expr]
        try:
            old_ct = old.CronTab(expr)
        except ValueError:
            assert not entry["ok"], expr
            continue
        assert entry["ok"], expr
        new_ct = CronTab(expr)
        for now_s in _G["naive_next_nows"]:
            now = datetime.datetime.fromisoformat(now_s)
            assert _close(
                new_ct.next(now=now, default_utc=True),
                old_ct.next(now=now, default_utc=True),
            ), (expr, now_s)
        for spec in _G["aware_next_nows"]:
            now = _aware(spec)
            assert _close(
                new_ct.next(now=now, default_utc=False),
                old_ct.next(now=now, default_utc=False),
            ), (expr, spec)
        for dt_s in _G["test_dts"]:
            dt = datetime.datetime.fromisoformat(dt_s)
            assert new_ct.test(dt) is bool(old_ct.test(dt)), (expr, dt_s)
