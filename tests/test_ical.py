"""The iCalendar renderer: RFC 5545 mechanics and window semantics.

Pure-module tests for :mod:`cronstable.ical` (escaping, folding, event
windows, UID stability); the HTTP endpoints riding on it are covered in
``test_ui_endpoints.py``.
"""

import datetime

import pytest

from cronstable.cronexpr import CronTab
from cronstable.ical import (
    CalendarEntry,
    _block_seconds,
    _duration_text,
    _escape,
    _fold,
    _runtime_phrase,
    render_calendar,
)

_UTC = datetime.timezone.utc
_START = datetime.datetime(2026, 7, 1, tzinfo=_UTC)
_NOW = datetime.datetime(2026, 7, 18, 12, 0, tzinfo=_UTC)


def _render(entries, days=35, **kwargs):
    kwargs.setdefault("start", _START)
    kwargs.setdefault("now", _NOW)
    return render_calendar(entries, days=days, **kwargs)


def test_escape_specials_and_newlines():
    assert _escape("a,b;c\nd\\e") == "a\\,b\\;c\\nd\\\\e"
    assert _escape("x\r\ny\rz") == "x\\ny\\nz"


def test_fold_keeps_lines_within_75_octets():
    line = "DESCRIPTION:" + "x" * 300
    folded = _fold(line)
    for physical in folded.split("\r\n"):
        assert len(physical.encode("utf-8")) <= 75
    # unfolding (strip the leading space of continuations) restores the line
    assert folded.replace("\r\n ", "") == line


def test_fold_never_splits_a_multibyte_character():
    line = "SUMMARY:" + "é" * 60  # 2 octets each
    folded = _fold(line)
    for physical in folded.split("\r\n"):
        payload = physical.encode("utf-8")
        assert len(payload) <= 75
        payload.decode("utf-8")  # would raise if a char was split
    assert folded.replace("\r\n ", "") == line


def test_duration_text_and_block_rounding():
    assert _duration_text(300) == "PT5M"
    assert _duration_text(3600) == "PT1H"
    assert _duration_text(5400) == "PT1H30M"
    assert _block_seconds(None) == 300  # no history: the 5-minute floor
    assert _block_seconds(30.0) == 300
    assert _block_seconds(520.0) == 540  # rounded up to a whole minute
    assert _block_seconds(10 ** 9) == 24 * 3600  # capped


def test_render_requires_an_aware_start():
    with pytest.raises(ValueError, match="aware"):
        render_calendar([], start=datetime.datetime(2026, 7, 1), days=7)


def test_empty_fleet_is_a_valid_empty_calendar():
    text = _render([])
    assert text.startswith("BEGIN:VCALENDAR\r\n")
    assert text.endswith("END:VCALENDAR\r\n")
    assert "BEGIN:VEVENT" not in text
    assert "METHOD:PUBLISH" in text
    # CRLF terminators only, per RFC 5545
    assert "\n" not in text.replace("\r\n", "")


def test_events_fill_the_window_strictly_after_start():
    entry = CalendarEntry("daily", CronTab("0 0 * * *"), _UTC)
    text = _render([entry], days=3, start=_START)
    # midnight July 1 is not strictly after the start, and the window end
    # (July 4 00:00, start + 3 days) is exclusive: exactly the 2nd and 3rd
    assert "DTSTART:20260701T000000Z" not in text
    assert "DTSTART:20260702T000000Z" in text
    assert "DTSTART:20260703T000000Z" in text
    assert "DTSTART:20260704T000000Z" not in text
    assert text.count("BEGIN:VEVENT") == 2


def test_extension_day_forms_place_events_on_engine_dates():
    # July 2026: the 31st is a Friday (LW in place), the 3rd Friday is
    # the 17th; both pinned by test_cronexpr's extension vectors
    entries = [
        CalendarEntry("monthly-close", CronTab("30 1 LW * *"), _UTC),
        CalendarEntry("third-friday", CronTab("0 2 * * 5#3"), _UTC),
    ]
    text = _render(entries, days=35)
    assert "DTSTART:20260731T013000Z" in text
    assert "DTSTART:20260717T020000Z" in text
    assert text.count("BEGIN:VEVENT") == 2


def test_job_timezone_resolves_to_real_utc_instants():
    from zoneinfo import ZoneInfo

    ny = ZoneInfo("America/New_York")
    entry = CalendarEntry("east-coast", CronTab("0 9 * * *"), ny)
    text = _render([entry], days=2)
    # 09:00 in New York during EDT is 13:00Z
    assert "DTSTART:20260701T130000Z" in text


def test_uid_stability_and_shape():
    entry = CalendarEntry("daily", CronTab("0 0 * * *"), _UTC)
    first = _render([entry], days=2)
    second = _render([entry], days=2)
    assert first == second  # same window + pinned DTSTAMP: identical text
    for line in first.split("\r\n"):
        if line.startswith("UID:"):
            assert line.endswith("@cronstable")
            assert "20260702T000000Z" in line


def test_per_job_cap_truncates_loudly():
    entry = CalendarEntry("minutely", CronTab("* * * * *"), _UTC)
    text = _render([entry], days=1, per_job_cap=5)
    assert text.count("BEGIN:VEVENT") == 5
    assert "X-CRONSTABLE-TRUNCATED;CAP=5:minutely" in text


def test_description_names_schedule_but_never_commands():
    entry = CalendarEntry(
        "monthly-close", CronTab("30 1 LW * *"), _UTC, avg_duration=520.0
    )
    text = _render([entry], days=35)
    unfolded = text.replace("\r\n ", "")
    assert "Schedule: 30 1 LW * *" in unfolded
    assert "on the last weekday of the month" in unfolded
    assert "Typical runtime: 9m" in unfolded
    assert "DURATION:PT9M" in unfolded
    assert "TRANSP:TRANSPARENT" in unfolded


def test_summary_escapes_awkward_job_names():
    entry = CalendarEntry("sync; a,b", CronTab("0 0 * * *"), _UTC)
    text = _render([entry], days=2)
    assert "SUMMARY:sync\\; a\\,b" in text


# ---------------------------------------------------------------------------
# _runtime_phrase: the seconds, minutes, and hours arms.
# ---------------------------------------------------------------------------


def test_runtime_phrase_seconds_arm():
    # sub-minute durations render in seconds, rounded to the nearest whole
    assert _runtime_phrase(30) == "30s"
    assert _runtime_phrase(59.4) == "59s"
    assert _runtime_phrase(0.4) == "0s"


def test_runtime_phrase_minutes_and_hours_arms():
    # the minute arm (kept as a control) and the hour arm (>= 60 minutes)
    assert _runtime_phrase(300) == "5m"
    assert _runtime_phrase(5400) == "1.5h"  # 90 min -> 1.5h
    assert _runtime_phrase(7200) == "2.0h"
