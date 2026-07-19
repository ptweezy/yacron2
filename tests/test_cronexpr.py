"""The in-house cron engine matches the library it replaced, vector by vector.

``tests/data/cron_golden.json`` records what the legacy ``crontab``
(parse-crontab) package did for every expression in the corpus: whether it
parsed, ``next()`` from fixed naive and timezone-aware instants, ``test()``
over fixed datetimes, and semantic-equality verdicts.  These tests replay
every vector against :mod:`cronstable.cronexpr` -- a full compatibility proof
that needs no copy of the old package (the vectors are program OUTPUT, not
code).  Two deliberate exceptions: the ``aware_next`` vectors pin the
in-house engine's real-instant DST policy, because at a DST edge the legacy
library can answer a NEGATIVE delay; and entries flagged ``"extension"``
hold the ``L-n``, ``nW``/``LW`` and ``d#n`` day forms the legacy library
rejected, their vectors recorded from the in-house engine, so the replay
pins the extended dialect the same way.  Regenerate/extend the vectors
with ``tests/gen_cron_golden.py``.

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

UTC = datetime.timezone.utc

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
            # The old library must reject exactly the entries recorded as
            # errors PLUS the flagged dialect extensions (whose vectors
            # came from the in-house engine; test_golden covers them).
            assert not entry["ok"] or entry.get("extension"), expr
            continue
        assert entry["ok"] and not entry.get("extension"), expr
        new_ct = CronTab(expr)
        for now_s in _G["naive_next_nows"]:
            now = datetime.datetime.fromisoformat(now_s)
            assert _close(
                new_ct.next(now=now, default_utc=True),
                old_ct.next(now=now, default_utc=True),
            ), (expr, now_s)
        for spec, pinned in zip(
            _G["aware_next_nows"], entry["aware_next"], strict=True
        ):
            now = _aware(spec)
            old_value = old_ct.next(now=now, default_utc=False)
            if not _close(old_value, pinned):
                # Intentional divergence: at a DST edge the legacy library
                # answers with civil arithmetic (even a negative delay);
                # the aware vectors pin the in-house real-instant policy,
                # and test_golden already holds the engine to them.
                continue
            assert _close(
                new_ct.next(now=now, default_utc=False), old_value
            ), (expr, spec)
        for dt_s in _G["test_dts"]:
            dt = datetime.datetime.fromisoformat(dt_s)
            assert new_ct.test(dt) is bool(old_ct.test(dt)), (expr, dt_s)


# ---------------------------------------------------------------------------
# prev(): the backward mirror of next()
# ---------------------------------------------------------------------------


def test_prev_basic_and_strictly_before():
    ct = CronTab("30 9 * * *")
    ago = ct.prev(
        now=datetime.datetime(2026, 7, 18, 12, 0), default_utc=True
    )
    assert ago == 2.5 * 3600
    # an instant that matches "now" yields the PREVIOUS occurrence, the
    # mirror of next()'s strictly-future rule
    ago = ct.prev(now=datetime.datetime(2026, 7, 18, 9, 30), default_utc=True)
    assert ago == 86400.0


def test_prev_microseconds_round_toward_the_match():
    ct = CronTab("* * * * *")
    # 12:00:00.5 is strictly after 12:00:00, so :00 is the previous match
    ago = ct.prev(
        now=datetime.datetime(2026, 7, 18, 12, 0, 0, 500000),
        default_utc=True,
    )
    assert ago == 0.5


def test_prev_none_when_nothing_earlier():
    assert (
        CronTab("0 0 1 1 * 2030").prev(
            now=datetime.datetime(2026, 1, 1), default_utc=True
        )
        is None
    )
    # nothing strictly before the 1970 floor
    assert (
        CronTab("* * * * *").prev(
            now=datetime.datetime(1970, 1, 1, 0, 0, 0), default_utc=True
        )
        is None
    )


def test_prev_past_year_column_is_reachable():
    ago = CronTab("0 0 1 1 * 2020").prev(
        now=datetime.datetime(2020, 1, 2), default_utc=True
    )
    assert ago == 86400.0


def test_prev_aware_returns_true_elapsed_across_fall_back():
    ny = ZoneInfo("America/New_York")
    # 2026-11-01: 01:30 EDT fired at 05:30Z; from 03:00 EST (08:00Z) the
    # civil gap is 1.5h but the true elapsed time is 2.5h
    ago = CronTab("30 1 * * *").prev(
        now=datetime.datetime(2026, 11, 1, 3, 0, tzinfo=ny)
    )
    assert ago == 2.5 * 3600


def test_prev_next_round_trip():
    ct = CronTab("*/15 2-4 * * mon-fri")
    now = datetime.datetime(2026, 7, 15, 13, 37, 21)
    delay = ct.next(now=now, default_utc=True)
    fire = now + datetime.timedelta(seconds=delay)
    assert ct.test(fire)
    # one second past the fire, prev() finds it again
    ago = ct.prev(
        now=fire + datetime.timedelta(seconds=1), default_utc=True
    )
    assert ago == 1.0


# ---------------------------------------------------------------------------
# occurrences(): iteration that matches the scheduler's own stepping
# ---------------------------------------------------------------------------


def _accumulate(ct, start, count):
    """The pre-occurrences() algorithm: step an aware now through next()."""
    fires = []
    current = start
    for _ in range(count):
        delay = ct.next(current)
        if delay is None:
            break
        current = (
            current.astimezone(datetime.timezone.utc)
            + datetime.timedelta(seconds=delay)
        ).astimezone(current.tzinfo)
        fires.append(current)
    return fires


@pytest.mark.parametrize(
    "expr",
    ["*/5 * * * *", "30 2 * * *", "0 0 l * *", "15 14 1 * *", "30 1 * * 0"],
)
def test_occurrences_equals_stepping_next_across_dst(expr):
    ny = ZoneInfo("America/New_York")
    ct = CronTab(expr)
    start = datetime.datetime(2026, 3, 1, 12, 0, tzinfo=ny)
    got = []
    for when in ct.occurrences(start):
        got.append(when)
        if len(got) >= 120:
            break
    assert got == _accumulate(ct, start, 120)


def test_occurrences_spring_forward_fires_once_at_shifted_label():
    ny = ZoneInfo("America/New_York")
    ct = CronTab("30 2 * * *")
    start = datetime.datetime(2026, 3, 7, 12, 0, tzinfo=ny)
    it = ct.occurrences(start)
    first, second = next(it), next(it)
    # 02:30 does not exist on 2026-03-08; the fire lands at 07:30Z, whose
    # wall label is 03:30 EDT, and the day yields exactly one fire
    assert first.astimezone(datetime.timezone.utc) == datetime.datetime(
        2026, 3, 8, 7, 30, tzinfo=datetime.timezone.utc
    )
    assert (first.hour, first.minute) == (3, 30)
    assert second.astimezone(datetime.timezone.utc) == datetime.datetime(
        2026, 3, 9, 6, 30, tzinfo=datetime.timezone.utc
    )


def test_occurrences_fall_back_first_occurrence_only():
    ny = ZoneInfo("America/New_York")
    ct = CronTab("30 1 * * *")
    start = datetime.datetime(2026, 10, 31, 12, 0, tzinfo=ny)
    it = ct.occurrences(start)
    first, second = next(it), next(it)
    utc = datetime.timezone.utc
    # 01:30 EDT (05:30Z), and NOT again at 01:30 EST (06:30Z); the next
    # fire is the following day, 25 real hours later
    assert first.astimezone(utc) == datetime.datetime(
        2026, 11, 1, 5, 30, tzinfo=utc
    )
    assert second.astimezone(utc) == datetime.datetime(
        2026, 11, 2, 6, 30, tzinfo=utc
    )


def test_occurrences_naive_and_exhaustion():
    fires = list(
        CronTab("0 0 1 1 * 2027").occurrences(datetime.datetime(2026, 6, 1))
    )
    assert fires == [datetime.datetime(2027, 1, 1)]
    assert (
        list(CronTab("0 0 30 2 *").occurrences(datetime.datetime(2026, 6, 1)))
        == []
    )


def test_occurrences_matches_test_and_is_strictly_future():
    ct = CronTab("*/20 6,18 * * *")
    start = datetime.datetime(2026, 7, 18, 6, 0)
    fires = []
    for when in ct.occurrences(start):
        fires.append(when)
        if len(fires) >= 12:
            break
    assert all(ct.test(when) for when in fires)
    assert fires[0] > start
    assert fires == sorted(fires)


# ---------------------------------------------------------------------------
# '?' (Quartz) day fields and dialect hints
# ---------------------------------------------------------------------------


def test_question_mark_reads_as_star_in_day_fields():
    assert CronTab("0 12 ? * ?") == CronTab("0 12 * * *")
    assert CronTab("0 12 ? * mon") == CronTab("0 12 * * mon")
    # a 7-field Quartz layout (identical column order) parses verbatim
    assert CronTab("0 0 12 ? * mon *") == CronTab("0 0 12 * * mon *")


def test_question_mark_rejected_outside_day_fields():
    with pytest.raises(ValueError, match="day-of-month"):
        CronTab("? 0 * * *")
    with pytest.raises(ValueError, match="day-of-month"):
        CronTab("0 ? * * *")
    # not accepted inside a list either
    with pytest.raises(ValueError):
        CronTab("0 0 1,? * *")


def test_quartz_six_field_layout_gets_a_conversion_hint():
    with pytest.raises(ValueError, match="Quartz"):
        CronTab("0 */5 * * * ?")
    with pytest.raises(ValueError, match="append a trailing"):
        CronTab("0 15 10 ? * *")


def test_wrong_field_hash_and_w_raise_with_wrong_field_hints():
    # '#' and 'W' are dialect now, but each is valid in exactly one field;
    # anywhere else the error names the right one
    with pytest.raises(ValueError, match="day-of-week field"):
        CronTab("0 0 1#2 * *")
    with pytest.raises(ValueError, match="day-of-week field"):
        CronTab("1#2 0 * * *")
    with pytest.raises(ValueError, match="day-of-month field"):
        CronTab("0 0 * * 15w")
    with pytest.raises(ValueError, match="day-of-month field"):
        CronTab("0 0 * * lw")
    with pytest.raises(ValueError, match="day-of-month field"):
        CronTab("0 0 * 15w *")


def test_quartz_trailing_l_gets_a_spelling_hint():
    with pytest.raises(ValueError, match=r"L<n>"):
        CronTab("0 0 * * 5L")
    # 'jul' is a month name, not a trailing-L weekday; no hint noise
    with pytest.raises(ValueError) as excinfo:
        CronTab("0 0 * * jul")
    assert "last-weekday" not in str(excinfo.value)


def test_weekday_names_do_not_trip_the_w_hint():
    ct = CronTab("0 0 * * wed")
    assert ct.days_of_week == frozenset({3})
    with pytest.raises(ValueError) as excinfo:
        CronTab("0 0 * * wde")  # a typo, not Quartz
    assert "Quartz" not in str(excinfo.value)


# ---------------------------------------------------------------------------
# the extension day forms: L-<n>, <n>W / LW, <d>#<n>
# ---------------------------------------------------------------------------
# Fixed 2026 anchors: Jan 1 is a Thursday, so Jan has Fridays 2/9/16/23/30
# and its 31st is a Saturday; Feb 1 is a Sunday; May 31 and Aug 1 fall on
# Sunday and Saturday.  The golden extension vectors sweep the grids; these
# spell out the semantics on hand-checked dates.


def _matches(expr, *ymd):
    return CronTab(expr).test(datetime.datetime(*ymd))


def test_nth_weekday_matches_only_that_occurrence():
    assert _matches("0 0 * * 5#3", 2026, 1, 16)  # third Friday
    assert not _matches("0 0 * * 5#3", 2026, 1, 9)
    assert not _matches("0 0 * * 5#3", 2026, 1, 23)
    assert _matches("0 0 * * 1#2", 2026, 1, 12)  # second Monday
    assert _matches("0 0 * * fri#3", 2026, 1, 16)  # names resolve too


def test_nth_weekday_fifth_occurrence_and_fold():
    # five Sundays in Mar 2026 (1/8/15/22/29); Jan has only four
    assert _matches("0 0 * * 0#5", 2026, 3, 29)
    assert not _matches("0 0 * * 0#5", 2026, 1, 25)
    assert CronTab("0 0 * * 7#1") == CronTab("0 0 * * 0#1")
    assert CronTab("0 0 * * fri#3") == CronTab("0 0 * * 5#3")


def test_nearest_weekday_stays_put_on_weekdays():
    assert _matches("0 0 15W * *", 2026, 1, 15)  # a Thursday
    assert not _matches("0 0 15W * *", 2026, 1, 14)


def test_nearest_weekday_saturday_resolves_to_friday():
    assert _matches("0 0 17W * *", 2026, 1, 16)  # Sat 17th -> Fri 16th
    assert not _matches("0 0 17W * *", 2026, 1, 17)


def test_nearest_weekday_sunday_resolves_to_monday():
    assert _matches("0 0 1W 2 *", 2026, 2, 2)  # Sun the 1st -> Mon the 2nd
    assert not _matches("0 0 1W 2 *", 2026, 2, 1)


def test_nearest_weekday_flips_inward_at_month_edges():
    # Sat Aug 1: backward would leave the month, so Monday the 3rd
    assert _matches("0 0 1W 8 *", 2026, 8, 3)
    assert not _matches("0 0 1W 8 *", 2026, 8, 1)
    # Sun May 31 is the final day: forward would leave, so Friday the 29th
    assert _matches("0 0 31W 5 *", 2026, 5, 29)
    assert not _matches("0 0 31W 5 *", 2026, 5, 31)


def test_nearest_weekday_day_beyond_month_never_fires():
    assert (
        CronTab("0 0 31W 4 *").next(
            now=datetime.datetime(2026, 1, 1), default_utc=True
        )
        is None
    )


def test_last_weekday_of_month():
    assert _matches("0 0 LW * *", 2026, 1, 30)  # Jan 31 is a Saturday
    assert not _matches("0 0 LW * *", 2026, 1, 31)
    assert _matches("0 0 LW 8 *", 2026, 8, 31)  # a Monday, stays put
    assert _matches("0 0 LW 5 *", 2026, 5, 29)  # May 31 is a Sunday


def test_last_day_offsets():
    assert _matches("0 0 L-3 * *", 2026, 1, 28)
    assert _matches("0 0 L-3 2 *", 2026, 2, 25)
    assert _matches("0 0 L-30 * *", 2026, 1, 1)
    # 28 - 30 < 1: no such day in February
    assert not any(
        CronTab("0 0 L-30 2 *").test(datetime.datetime(2026, 2, d))
        for d in range(1, 29)
    )


def test_extension_forms_are_ordinary_list_items():
    assert _matches("0 0 1,15W,L * *", 2026, 1, 1)
    assert _matches("0 0 1,15W,L * *", 2026, 1, 15)
    assert _matches("0 0 1,15W,L * *", 2026, 1, 31)
    assert _matches("0 0 * * mon#1,L5", 2026, 1, 5)
    assert _matches("0 0 * * mon#1,L5", 2026, 1, 30)


def test_extension_forms_respect_the_day_and_rule():
    # 15W AND Friday: first hit from Jan 2026 is Fri May 15
    fires = CronTab("0 0 15W * fri").occurrences(
        datetime.datetime(2026, 1, 1)
    )
    assert next(fires) == datetime.datetime(2026, 5, 15)


def test_extension_prev_and_occurrences_agree():
    tab = CronTab("0 0 * * 5#3")
    ago = tab.prev(now=datetime.datetime(2026, 2, 1), default_utc=True)
    assert ago == (
        datetime.datetime(2026, 2, 1) - datetime.datetime(2026, 1, 16)
    ).total_seconds()
    first = next(iter(tab.occurrences(datetime.datetime(2026, 1, 1))))
    assert first == datetime.datetime(2026, 1, 16)


def test_extension_properties_expose_parsed_forms():
    ct = CronTab("0 0 1,15W,L,l-3,LW * mon#1,L5,tue")
    assert ct.days_of_month == frozenset({1})
    assert ct.last_day_offsets == frozenset({0, 3})
    assert ct.last_day_of_month is True
    assert ct.nearest_weekday_days == frozenset({15})
    assert ct.last_weekday_of_month is True
    assert ct.nth_days_of_week == frozenset({(1, 1)})
    assert ct.last_days_of_week == frozenset({5})
    assert ct.days_of_week == frozenset({2})
    # legacy spellings unchanged
    assert CronTab("0 0 l * *").last_day_offsets == frozenset({0})
    assert CronTab("0 0 1 * *").last_day_of_month is False


def test_extension_errors():
    for bad in (
        "0 0 * * 5#0",
        "0 0 * * 5#6",
        "0 0 * * 1-2#3",
        "0 0 * * #3",
        "0 0 * * 5#",
        "0 0 * * 5#3#4",
        "0 0 * * 5#3/2",
        "0 0 * * mon#fri",
        "0 0 L-0 * *",
        "0 0 L-31 * *",
        "0 0 0W * *",
        "0 0 32W * *",
        "0 0 W * *",
        "0 0 15W/2 * *",
        "0 0 1-15W * *",
        "0 0 LW-3 * *",
        "0 0 l-x * *",
    ):
        with pytest.raises(ValueError):
            CronTab(bad)


# ---------------------------------------------------------------------------
# introspection properties
# ---------------------------------------------------------------------------


def test_field_set_properties():
    ct = CronTab("*/15 9-17 1,15,l * mon-fri")
    assert ct.seconds == frozenset({0})
    assert ct.minutes == frozenset({0, 15, 30, 45})
    assert ct.hours == frozenset(range(9, 18))
    assert ct.days_of_month == frozenset({1, 15})
    assert ct.last_day_of_month is True
    assert ct.months == frozenset(range(1, 13))
    assert ct.days_of_week == frozenset({1, 2, 3, 4, 5})
    assert ct.last_days_of_week == frozenset()
    assert ct.years is None


def test_field_set_properties_seconds_year_and_l_dow():
    ct = CronTab("*/10 0 0 * * l5 2027")
    assert ct.seconds == frozenset({0, 10, 20, 30, 40, 50})
    assert ct.last_days_of_week == frozenset({5})
    assert ct.years == frozenset({2027})
    # 7 (Sunday) folds to 0 in the exposed set, like matching itself
    assert CronTab("0 0 * * 7").days_of_week == frozenset({0})


# ---------------------------------------------------------------------------
# the H (hashed slot) form
# ---------------------------------------------------------------------------


def test_hash_slot_is_stable_and_semantic():
    a = CronTab("H * * * *", hash_key="backup-db")
    b = CronTab("H * * * *", hash_key="backup-db")
    assert len(a.minutes) == 1
    assert a.minutes == b.minutes
    (minute,) = a.minutes
    assert 0 <= minute <= 59
    # semantic equality against the plain spelling of the same instants,
    # which is what the scheduler's reload path compares
    assert a == CronTab("{} * * * *".format(minute))


def test_hash_algorithm_is_pinned():
    # The slot is part of the product contract: a job's hashed minute must
    # never move across releases, restarts or hosts (that predictability is
    # the whole point vs random jitter).  These literals freeze the
    # sha256 over "<field label> newline <key>"; if this test breaks, the
    # H slots of every deployed fleet just moved.
    tab = CronTab("H H H H H", hash_key="report-gen")
    assert tab.resolved_source == "43 9 21 8 6"
    assert CronTab("H * * * * * *", hash_key="tick").seconds == {9}
    assert CronTab("H/15 * * * *", hash_key="x").resolved_source == (
        "1,16,31,46 * * * *"
    )


def test_hash_step_and_range_forms():
    stepped = CronTab("H/15 * * * *", hash_key="x")
    (phase,) = {m % 15 for m in stepped.minutes}
    assert stepped.minutes == {phase, phase + 15, phase + 30, phase + 45}
    ranged = CronTab("H(0-29) * * * *", hash_key="x")
    (minute,) = ranged.minutes
    assert 0 <= minute <= 29
    both = CronTab("H(10-49)/10 * * * *", hash_key="x")
    assert len(both.minutes) == 4
    assert all(10 <= m <= 49 for m in both.minutes)


def test_hash_fields_are_salted_per_field():
    # the same key hashed in different fields draws from per-field-salted
    # values; pin one witness pair rather than asserting on distributions
    tab = CronTab("H H * * *", hash_key="report-gen")
    assert (tab.resolved_source.split()[:2]) == ["43", "9"]
    # 43 % 24 == 19 != 9: the hour is NOT the minute reduced mod 24
    assert next(iter(tab.hours)) != next(iter(tab.minutes)) % 24


def test_hash_bare_dom_stays_within_short_months():
    for key in ("a", "b", "c", "backup", "report-gen", "x7"):
        (day,) = CronTab("0 0 H * *", hash_key=key).days_of_month
        assert 1 <= day <= 28


def test_hash_resolved_source_reparses_without_a_key():
    tab = CronTab("H H(2-6) * * H", hash_key="etl")
    again = CronTab(tab.resolved_source)
    assert tab == again
    # non-H expressions resolve to themselves, byte-identical
    plain = CronTab("*/5 10-12 * * MON", hash_key="x")
    assert plain.resolved_source == str(plain) == "*/5 10-12 * * MON"
    assert CronTab("@daily", hash_key="x").resolved_source == "@daily"


def test_hash_key_does_not_change_plain_expressions():
    assert CronTab("*/5 * * * *", hash_key="a") == CronTab("*/5 * * * *")


def test_hash_next_and_occurrences_work():
    tab = CronTab("H * * * *", hash_key="backup-db")
    (minute,) = tab.minutes
    now = datetime.datetime(2026, 7, 18, 0, 0, 0)
    delay = tab.next(now=now)
    assert delay == minute * 60 or delay == (minute + 60) * 60
    first = next(iter(tab.occurrences(now)))
    assert first.minute == minute


def test_hash_errors():
    with pytest.raises(ValueError, match="hash key"):
        CronTab("H * * * *")
    with pytest.raises(ValueError, match="year field"):
        CronTab("* * * * * H", hash_key="x")
    with pytest.raises(ValueError, match="forms"):
        CronTab("H-5 * * * *", hash_key="x")
    with pytest.raises(ValueError, match="exceeds"):
        CronTab("H(0-3)/6 * * * *", hash_key="x")
    with pytest.raises(ValueError, match="start 9 > end 2"):
        CronTab("H(9-2) * * * *", hash_key="x")
    with pytest.raises(ValueError, match="out of range"):
        CronTab("H(0-99) * * * *", hash_key="x")
    # a '#' in a failed H item still carries the Quartz hint
    with pytest.raises(ValueError, match="Quartz"):
        CronTab("H#3 * * * *", hash_key="x")


# ---------------------------------------------------------------------------
# the backward walk and iteration internals: prev() / _prev_civil /
# _last_time / occurrences, plus two parse-error edges
# ---------------------------------------------------------------------------


# _prev_civil: year and month rollback branches


def test_prev_rolls_back_through_year_column():
    # base year 2026 is not in the year set {2020}; the walk drops to the
    # previous listed year, then finds Jan 1 2020 as the fire.
    ct = CronTab("0 0 1 1 * 2020")
    ago = ct.prev(now=datetime.datetime(2026, 1, 1), default_utc=True)
    assert ago == (
        datetime.datetime(2026, 1, 1) - datetime.datetime(2020, 1, 1)
    ).total_seconds()


def test_prev_month_rollback_crosses_year_boundary():
    # months set is {6}; from March the current-year scan finds no month
    # at or before June-that-is-<=-March, so it steps to the prior year.
    ct = CronTab("0 0 1 6 *")
    got = ct._prev_civil(datetime.datetime(2026, 3, 1))
    assert got == datetime.datetime(2025, 6, 1)


def test_prev_month_end_clamps_the_rollback_sentinel():
    # rolling back into February with the day sentinel at 31 must clamp to
    # the real month end (28) before scanning days.
    ct = CronTab("0 0 * 2 *")  # every day of February
    got = ct._prev_civil(datetime.datetime(2026, 3, 15))
    assert got == datetime.datetime(2026, 2, 28)


def test_prev_scans_back_across_months_for_a_dom():
    # day 15 is absent from the days below March 9, so the walk decrements
    # the month and finds Feb 15.
    ct = CronTab("0 0 15 * *")
    got = ct._prev_civil(datetime.datetime(2026, 3, 10))
    assert got == datetime.datetime(2026, 2, 15)


def test_prev_single_month_wraps_month_below_one():
    # only January fires; from Jan 10 the same-month scan fails, month
    # decrements below 1 and wraps to December of the prior year.
    ct = CronTab("0 0 15 1 *")
    got = ct._prev_civil(datetime.datetime(2026, 1, 10))
    assert got == datetime.datetime(2025, 1, 15)


# _prev_in


def test_prev_in_returns_none_below_the_floor():
    ct = CronTab("* * * * *")
    assert CronTab._prev_in((6,), 3) is None
    assert CronTab._prev_in((2, 6, 9), 7) == 6
    # nothing strictly before the 1970 floor: the walk exhausts its years
    # and returns None.
    assert ct._prev_civil(datetime.datetime(1970, 1, 1, 0, 0, 0)) is None


# _last_time: earlier-minute selection within a matching hour


def test_prev_picks_earlier_minute_in_the_same_hour():
    # at 09:45 the most recent same-day fire of 0/30 past 9 is 09:30, which
    # forces _last_time down the minute < target branch.
    ct = CronTab("0,30 9 * * *")
    got = ct._prev_civil(datetime.datetime(2026, 7, 18, 9, 45))
    assert got == datetime.datetime(2026, 7, 18, 9, 30)


def test_last_time_earlier_hour_and_second_branches():
    ct = CronTab("0,30 8,12 * * * *")
    # target hour 10 has no matching hour; the latest hour below it (8) is
    # returned at its own last minute/second.
    assert ct._last_time(datetime.time(10, 5, 0)) == datetime.time(8, 30, 0)


# prev(): now omitted, and the aware DST-gap anchor walk


def test_prev_naive_none_when_year_column_has_nothing_earlier():
    # base year 2025 is not in {2030} and no listed year precedes it, so the
    # naive walk returns None straight from the year column.
    ct = CronTab("0 0 1 1 * 2030")
    assert ct.prev(now=datetime.datetime(2026, 1, 1), default_utc=True) is None


def test_prev_aware_none_when_nothing_earlier():
    ny = ZoneInfo("America/New_York")
    ct = CronTab("0 0 1 1 * 2030")
    now = datetime.datetime(2026, 1, 1, tzinfo=ny)
    assert ct.prev(now=now) is None


def test_prev_now_omitted_uses_wall_clock():
    ct = CronTab("* * * * *")
    ago_utc = ct.prev(default_utc=True)
    ago_local = ct.prev(default_utc=False)
    assert ago_utc is not None and 0 < ago_utc <= 60
    assert ago_local is not None and 0 < ago_local <= 60


def test_prev_aware_walks_past_a_spring_forward_gap_anchor():
    ny = ZoneInfo("America/New_York")
    ct = CronTab("30 2 * * *")
    # On 2026-03-08 the 02:30 label is skipped; the fire lands at 03:30 EDT
    # (07:30Z).  From 04:00 EDT (08:00Z) the most recent real fire is that
    # shifted-label instant, 30 minutes earlier.  The backward anchor first
    # lands in the gap and must be re-walked to an anchor the zone keeps.
    now = datetime.datetime(2026, 3, 8, 4, 0, tzinfo=ny)
    ago = ct.prev(now=now)
    assert ago == 30 * 60


def test_prev_aware_true_elapsed_matches_forward_iteration():
    ny = ZoneInfo("America/New_York")
    ct = CronTab("*/30 * * * *")
    now = datetime.datetime(2026, 7, 18, 15, 47, tzinfo=ny)
    ago = ct.prev(now=now)
    # 15:30 EDT is 17 minutes back; result is the true elapsed seconds.
    assert ago == 17 * 60


# occurrences(): start omitted


def test_occurrences_start_omitted_utc_and_local():
    ct = CronTab("* * * * *")
    before_utc = datetime.datetime.now(UTC).replace(tzinfo=None)
    first_utc = next(iter(ct.occurrences(default_utc=True)))
    assert isinstance(first_utc, datetime.datetime) and first_utc.tzinfo is None
    assert 0 <= (first_utc - before_utc).total_seconds() <= 120

    before_local = datetime.datetime.now()
    first_local = next(iter(ct.occurrences(default_utc=False)))
    assert 0 <= (first_local - before_local).total_seconds() <= 120


# parse-error edges


def test_non_integer_step_is_rejected():
    with pytest.raises(ValueError, match="step"):
        CronTab("*/x * * * *")


def test_hash_step_must_be_positive():
    with pytest.raises(ValueError, match="positive"):
        CronTab("H/0 * * * *", hash_key="x")
