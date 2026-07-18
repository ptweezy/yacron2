"""The shared schedule-intelligence module: previews and the linter.

``describe_cron``/``next_fires`` moved here from the TUI (their behaviour
is pinned by ``test_tui.py`` through the re-exports, which this file also
asserts); the advisory linter is new and covered rule by rule.  ``now`` is
always pinned so the DST findings land on known transition dates.
"""

import datetime
from zoneinfo import ZoneInfo

from cronstable.croninfo import (
    Finding,
    describe_cron,
    lint_schedule,
    next_fires,
)

_UTC = datetime.timezone.utc
_NY = ZoneInfo("America/New_York")
#: a fixed reference instant: July 2026, between the US transitions
_NOW = datetime.datetime(2026, 7, 18, 12, 0, tzinfo=_UTC)


def _codes(expr, tz=None):
    return [f.code for f in lint_schedule(expr, timezone=tz, now=_NOW)]


def _by_code(expr, code, tz=None):
    for finding in lint_schedule(expr, timezone=tz, now=_NOW):
        if finding.code == code:
            return finding
    raise AssertionError("no {} finding for {!r}".format(code, expr))


# ---------------------------------------------------------------------------
# module plumbing
# ---------------------------------------------------------------------------


def test_tui_still_reexports_the_moved_names():
    from cronstable import tui

    assert tui.describe_cron is describe_cron
    assert tui.next_fires is next_fires


def test_findings_are_json_shaped():
    finding = _by_code("0 0 30 2 *", "never-fires")
    assert finding._asdict() == {
        "code": "never-fires",
        "level": "warning",
        "message": finding.message,
    }
    assert isinstance(finding, Finding)


# ---------------------------------------------------------------------------
# never-fires
# ---------------------------------------------------------------------------


def test_never_fires_impossible_date():
    finding = _by_code("0 0 30 2 *", "never-fires")
    assert finding.level == "warning"
    assert "never fire" in finding.message


def test_never_fires_past_year_names_the_year():
    finding = _by_code("0 0 1 1 * 2020", "never-fires")
    assert "2020" in finding.message


def test_never_fires_suppresses_month_refinements():
    # "it never fires at all" beats "it skips February"
    assert _codes("0 0 30 2 *") == ["never-fires"]


def test_live_schedules_do_not_warn():
    assert _codes("*/15 * * * *") == []
    assert _codes("@daily") == []
    assert _codes("@reboot", tz=_NY) == []
    assert _codes("not a schedule") == []


# ---------------------------------------------------------------------------
# both day fields restricted (AND semantics)
# ---------------------------------------------------------------------------


def test_both_day_fields_restricted_warns():
    finding = _by_code("0 0 13 * 5", "day-fields-both-restricted")
    assert finding.level == "warning"
    assert "Vixie" in finding.message


def test_one_day_field_alone_is_fine():
    assert "day-fields-both-restricted" not in _codes("0 0 13 * *")
    assert "day-fields-both-restricted" not in _codes("0 0 * * 5")
    # L forms count as restrictions too
    assert "day-fields-both-restricted" in _codes("0 0 l * 5")
    assert "day-fields-both-restricted" in _codes("0 0 13 * l5")


# ---------------------------------------------------------------------------
# uneven steps
# ---------------------------------------------------------------------------


def test_uneven_minute_step_names_the_wrap_gap():
    finding = _by_code("*/7 * * * *", "uneven-step")
    assert finding.level == "warning"
    assert "4 minutes" in finding.message


def test_uneven_hour_month_dow_and_second_steps():
    assert "uneven-step" in _codes("0 */5 * * *")
    assert "uneven-step" in _codes("0 0 1 */5 *")
    assert "uneven-step" in _codes("0 0 * * */2")
    assert "uneven-step" in _codes("*/7 * * * * * *")
    # the singular gap reads grammatically
    finding = _by_code("0 0 * * */2", "uneven-step")
    assert "only 1 day" in finding.message
    assert "1 days" not in finding.message


def test_dividing_steps_are_clean():
    assert _codes("*/15 * * * *") == []
    assert _codes("0 */6 * * *") == []
    assert _codes("0 0 * * * 2030/5") == []  # year steps never flagged


def test_day_of_month_step_is_a_note():
    finding = _by_code("0 0 */2 * *", "uneven-step")
    assert finding.level == "note"
    assert "month lengths differ" in finding.message


def test_explicit_range_steps_read_as_deliberate():
    assert _codes("10-40/7 * * * *") == []


# ---------------------------------------------------------------------------
# month lengths
# ---------------------------------------------------------------------------


def test_day_31_in_a_30_day_month_warns():
    finding = _by_code("0 0 31 1,4 *", "skipped-months")
    assert finding.level == "warning"
    assert "April" in finding.message and "January" not in finding.message


def test_day_31_every_month_lists_all_short_months():
    message = _by_code("0 0 31 * *", "skipped-months").message
    for name in ("February", "April", "June", "September", "November"):
        assert name in message


def test_a_reachable_smaller_day_keeps_months_alive():
    # the 1st fires in April; only unreachable-everywhere months warn
    assert "skipped-months" not in _codes("0 0 1,31 * *")


def test_leap_day_only_note():
    finding = _by_code("0 0 29 2 *", "leap-day-only")
    assert finding.level == "note"
    assert "leap years" in finding.message
    # day 29 in ALL months: February still only matches in leap years
    assert "leap-day-only" in _codes("0 0 29 * *")


def test_last_day_covers_every_month():
    assert _codes("0 0 l 2 *") == []


# ---------------------------------------------------------------------------
# DST notes (job timezone required)
# ---------------------------------------------------------------------------


def test_dst_gap_note_names_the_transition_date():
    finding = _by_code("30 2 * * *", "dst-skipped-time", tz=_NY)
    assert finding.level == "note"
    assert "2027-03-14" in finding.message
    assert "02:30" in finding.message
    assert "America/New_York" in finding.message


def test_dst_fold_note_names_the_transition_date():
    finding = _by_code("30 1 * * *", "dst-repeated-time", tz=_NY)
    assert "2026-11-01" in finding.message
    assert "first occurrence" in finding.message


def test_dst_notes_only_with_restricted_hours_and_a_real_zone():
    # every-hour schedules fire through transitions; nothing to call out
    assert _codes("30 * * * *", tz=_NY) == []
    # no zone, or a fixed-offset zone: the scan cannot / need not run
    assert _codes("30 2 * * *") == []
    assert _codes("30 2 * * *", tz=_UTC) == []


def test_dst_notes_respect_the_day_fields():
    # 02:30 only on the 1st of the month: the 2027-03-14 gap never hits it,
    # and the 2026-11-01 fold is not a gap, so no skipped-time note appears
    codes = _codes("30 2 1 * *", tz=_NY)
    assert "dst-skipped-time" not in codes


# ---------------------------------------------------------------------------
# next_fires on the occurrences iterator
# ---------------------------------------------------------------------------


def test_next_fires_series_and_degenerate_inputs():
    start = datetime.datetime(2026, 7, 18, 11, 50, tzinfo=_UTC)
    fires = next_fires("*/15 * * * *", 3, start=start)
    assert [f.minute for f in fires] == [0, 15, 30]
    assert all(f.tzinfo is _UTC for f in fires)
    assert next_fires("@reboot", 3) == []
    assert next_fires("garbage", 3) == []
    assert next_fires("0 0 30 2 *", 3) == []


def test_next_fires_dst_gap_keeps_the_wall_clock_label():
    start = datetime.datetime(2026, 3, 7, 12, 0, tzinfo=_NY)
    fires = next_fires("30 2 * * *", 2, tz=_NY, start=start)
    assert fires[0].astimezone(_UTC) == datetime.datetime(
        2026, 3, 8, 7, 30, tzinfo=_UTC
    )
    # the label is the clock that actually exists at that instant
    assert (fires[0].hour, fires[0].minute) == (3, 30)


def test_describe_cron_question_mark_matches_star():
    assert describe_cron("0 12 ? * ?") == describe_cron("0 12 * * *")


# ---------------------------------------------------------------------------
# H threading through the describers and linter
# ---------------------------------------------------------------------------


def test_describe_cron_resolves_h_with_a_key():
    from cronstable.cronexpr import CronTab

    resolved = CronTab("H H * * *", hash_key="report-gen").resolved_source
    assert describe_cron("H H * * *", hash_key="report-gen") == (
        describe_cron(resolved) + " (H slots hashed from the job name)"
    )
    # without a key the tolerant fallback stays
    assert describe_cron("H * * * *").startswith("Custom schedule")


def test_lint_hashed_slot_note_and_uneven_h_step():
    findings = lint_schedule("H * * * *", hash_key="spread")
    codes = [f.code for f in findings]
    assert "hashed-slot" in codes
    note = next(f for f in findings if f.code == "hashed-slot")
    assert note.level == "note"
    assert "renaming the job re-hashes" in note.message
    # H/7 spans the whole minute field like */7, so the uneven-step
    # warning applies to it too
    uneven = lint_schedule("H/7 * * * *", hash_key="spread")
    assert "uneven-step" in [f.code for f in uneven]
    # a plain schedule with a key gains no hashed-slot note
    assert "hashed-slot" not in [
        f.code for f in lint_schedule("*/5 * * * *", hash_key="spread")
    ]


def test_next_fires_uses_the_hash_key():
    fires = next_fires("H * * * *", 2, tz=_UTC, hash_key="backup-db")
    assert len(fires) == 2
    assert fires[0].minute == fires[1].minute
    assert next_fires("H * * * *", 2, tz=_UTC) == []


# ---------------------------------------------------------------------------
# fleet analyzers: pressure, duplicates, suggest
# ---------------------------------------------------------------------------
from cronstable.cronexpr import CronTab  # noqa: E402
from cronstable.croninfo import (  # noqa: E402
    ScheduleEntry,
    duplicate_schedules,
    schedule_pressure,
    suggest_slot,
)

#: aligned to a midnight so every civil label is predictable
_P_START = datetime.datetime(2026, 7, 20, 0, 0, tzinfo=_UTC)


def _entry(name, expr, tz=_UTC, key=None):
    return ScheduleEntry(name, CronTab(expr, hash_key=key), tz)


def test_schedule_pressure_counts_the_herd():
    entries = [
        _entry("herd-%02d" % i, "0 * * * *") for i in range(37)
    ] + [_entry("mid", "30 3 * * *")]
    payload = schedule_pressure(entries, start=_P_START)
    # occurrences are strictly after start, and the window end is
    # exclusive, so an hourly job fires 23 times in an aligned 24h window
    assert payload["jobs"] == 38
    assert payload["by_minute_jobs"][0] == 37
    assert payload["by_minute_fires"][0] == 37 * 23
    assert payload["grid"][3][30] == 1
    assert payload["busiest_minute"]["minute"] == 0
    assert payload["busiest_minute"]["jobs"] == 37
    # 58 empty minutes: only :00 and :30 see fires
    assert len(payload["empty_minutes"]) == 58
    assert 23 not in [payload["busiest_minute"]["minute"]]
    top = payload["top_cells"][0]
    assert top["minute"] == 0 and top["fires"] == 37
    assert len(top["jobs"]) == 10  # capped names


def test_schedule_pressure_weighs_subminute_and_frames_timezones():
    entries = [
        _entry("ticker", "*/15 30 3 * * * *"),  # 4 fires inside 03:30
        _entry("ny", "0 0 * * *", tz=ZoneInfo("America/New_York")),
    ]
    payload = schedule_pressure(entries, start=_P_START)
    assert payload["grid"][3][30] == 4
    # NY midnight in July is 04:00 UTC
    assert payload["grid"][4][0] == 1


def test_duplicate_schedules_semantic_and_tz_aware():
    entries = [
        _entry("a", "*/5 * * * *"),
        _entry("b", "0-59/5 * * * *"),
        _entry("c", "@hourly"),
        _entry("d", "0 * * * *"),
        _entry("e", "0 * * * *", tz=ZoneInfo("America/New_York")),
        _entry("f", "17 4 * * *"),
        _entry("hashed", "H * * * *", key="hashed"),
    ]
    groups = duplicate_schedules(entries)
    assert len(groups) == 2
    exprs = {g["expression"]: g for g in groups}
    assert exprs["*/5 * * * *"]["jobs"] == ["a", "b"]
    assert exprs["@hourly"]["jobs"] == ["c", "d"]
    # e shares d's instants only textually: its zone differs, so it is
    # NOT a duplicate; f and the hashed job are singletons
    for g in groups:
        assert "e" not in g["jobs"]
    assert groups[0]["count"] == 2
    assert "description" in groups[0] and groups[0]["description"]


def test_suggest_slot_prefers_quiet_minutes_far_from_the_herd():
    herd = [_entry("h%d" % i, "0 * * * *") for i in range(10)]
    got = suggest_slot(herd, "hourly", start=_P_START)
    assert got["minute"] != 0
    assert got["fires_in_window"] == 0
    assert got["expression"].endswith(" * * * *")
    # ties break circularly FARTHEST from the busiest minute (:00 herd)
    assert got["minute"] == 30
    assert got["hash_hint"] == "H * * * *"
    assert len(got["alternatives"]) == 2


def test_suggest_slot_daily_and_empty_fleet():
    empty = suggest_slot([], "hourly", start=_P_START)
    assert empty["expression"] == "30 * * * *"
    daily = suggest_slot(
        [_entry("h%d" % i, "0 0 * * *") for i in range(3)],
        "daily",
        start=_P_START,
    )
    assert "hour" in daily
    assert daily["fires_in_window"] == 0
    assert daily["expression"].split()[2:] == ["*", "*", "*"]
    assert daily["hash_hint"] == "H H * * *"
    import pytest

    with pytest.raises(ValueError, match="period"):
        suggest_slot([], "weekly")


# ---------------------------------------------------------------------------
# why_no_run: the per-instant, per-field no-run explainer
# ---------------------------------------------------------------------------
from cronstable.croninfo import why_no_run  # noqa: E402


def _why(expr, when, tz=None, key=None):
    return why_no_run(CronTab(expr, hash_key=key), when, timezone=tz)


def test_why_agrees_with_the_engine_everywhere():
    # the explainer is a DECOMPOSITION of CronTab.test, so on any instant
    # its verdict must equal the engine's own answer, bit for bit
    schedules = [
        "* * * * *",
        "0 9 * * mon,fri",
        "0 0 13 * 5",
        "*/7 * * * *",
        "0 0 L * *",
        "0 12 * * L5",
        "0 0 1 1 * 2030",
        "15 0 9 * * mon-fri *",
        "0 0 29 2 *",
        "5,10 8-10 1-15 mar,jun 1-5",
    ]
    base = datetime.datetime(2026, 1, 1)
    probes = [
        base + datetime.timedelta(hours=7 * i, minutes=13 * i % 60)
        for i in range(300)
    ] + [
        datetime.datetime(2028, 2, 29),
        datetime.datetime(2026, 1, 31, 23, 59, 59),
        datetime.datetime(2030, 1, 1),
    ]
    for expr in schedules:
        tab = CronTab(expr)
        for probe in probes:
            got = why_no_run(tab, probe)
            assert got["matches"] == tab.test(probe), (expr, probe)
            assert got["matches"] == (not got["failed"])


def test_why_names_the_failing_field():
    # Tuesday 2026-07-14 against a Monday/Friday schedule: every other
    # field matched, and the verdict says who did not and what it wanted
    got = _why("0 9 * * mon,fri", datetime.datetime(2026, 7, 14, 9, 0))
    assert got["matches"] is False
    assert got["failed"] == ["day-of-week"]
    dow = got["checks"][5]
    assert dow["field"] == "day-of-week"
    assert dow["value"] == 2
    assert dow["label"] == "Tuesday"
    assert dow["allowed"] == "Monday and Friday"
    assert dow["matched"] is False


def test_why_checks_are_ordered_and_json_shaped():
    got = _why("0 9 * * mon,fri", datetime.datetime(2026, 7, 17, 9, 0))
    assert got["matches"] is True
    assert [c["field"] for c in got["checks"]] == [
        "second",
        "minute",
        "hour",
        "day-of-month",
        "month",
        "day-of-week",
        "year",
    ]
    for check in got["checks"]:
        assert set(check) == {"field", "value", "label", "allowed", "matched"}
    # unrestricted fields read "any"; month labels are month names
    assert got["checks"][3]["allowed"] == "any"
    assert got["checks"][4]["label"] == "July"


def test_why_seconds_field_of_a_five_field_schedule():
    # a 5-field schedule fires at second 0 only; probing :30s says so
    got = _why("0 9 * * mon,fri", datetime.datetime(2026, 7, 17, 9, 0, 30))
    assert got["failed"] == ["second"]
    assert got["checks"][0]["allowed"] == "0"


def test_why_year_column():
    got = _why("0 0 1 1 * 2030,2031,2032", datetime.datetime(2026, 1, 1))
    assert got["failed"] == ["year"]
    assert got["checks"][6]["allowed"] == "2030-2032"


def test_why_and_rule_note_fires_when_exactly_one_day_field_matched():
    # 2026-07-13 is a Monday the 13th: dom matched, dow did not, and in
    # Vixie cron (either-field) this WOULD have run; the note says so
    got = _why("0 0 13 * 5", datetime.datetime(2026, 7, 13, 0, 0))
    assert got["failed"] == ["day-of-week"]
    codes = [n["code"] for n in got["notes"]]
    assert codes == ["day-fields-and-rule"]
    assert "Vixie" in got["notes"][0]["message"]
    # both matched (a real Friday the 13th): no note, it fires
    got = _why("0 0 13 * 5", datetime.datetime(2026, 3, 13, 0, 0))
    assert got["matches"] is True
    assert got["notes"] == []
    # neither matched: no note, Vixie would not have fired either
    got = _why("0 0 13 * 5", datetime.datetime(2026, 7, 14, 0, 0))
    assert [n["code"] for n in got["notes"]] == []
    # only one day field restricted: the AND rule cannot be the story
    got = _why("0 9 * * mon,fri", datetime.datetime(2026, 7, 14, 9, 0))
    assert got["notes"] == []
    # another field failed too: Vixie would NOT have fired at this
    # instant either, so the note must not overclaim
    got = _why("0 0 13 * 5", datetime.datetime(2026, 7, 13, 0, 30))
    assert got["failed"] == ["minute", "day-of-week"]
    assert got["notes"] == []


def test_why_last_day_forms():
    # bare L in day-of-month
    got = _why("0 0 L * *", datetime.datetime(2026, 7, 30, 0, 0))
    assert got["failed"] == ["day-of-month"]
    assert got["checks"][3]["allowed"] == "the month's last day (L)"
    assert _why("0 0 L * *", datetime.datetime(2026, 7, 31))["matches"]
    # L5 in day-of-week: only the month's LAST Friday
    got = _why("0 12 * * L5", datetime.datetime(2026, 7, 24, 12, 0))
    assert got["failed"] == ["day-of-week"]
    assert got["checks"][5]["allowed"] == "the month's last Friday"
    assert _why("0 12 * * L5", datetime.datetime(2026, 7, 31, 12, 0))[
        "matches"
    ]
    # mixed spellings list every accepted form
    got = _why("0 9 1,15,L * *", datetime.datetime(2026, 7, 2, 9, 0))
    assert got["checks"][3]["allowed"] == "1, 15 and the month's last day (L)"
    got = _why("0 9 * * mon,L5", datetime.datetime(2026, 7, 14, 9, 0))
    assert got["checks"][5]["allowed"] == "Monday and the month's last Friday"


def test_why_allowed_prose_compacts_runs():
    base = datetime.datetime(2026, 1, 1)
    assert _why("1,2,3,7 * * * *", base)["checks"][1]["allowed"] == "1-3 and 7"
    assert (
        _why("0 9 * * mon-fri", base)["checks"][5]["allowed"]
        == "Monday-Friday"
    )


def test_why_hashed_schedule_reports_the_resolved_slot():
    got = _why(
        "H * * * *", datetime.datetime(2026, 7, 18, 3, 0), key="hashed"
    )
    minute = CronTab("H * * * *", hash_key="hashed").resolved_source.split()[0]
    assert got["failed"] == ["minute"]
    assert got["checks"][1]["allowed"] == minute


def test_why_dst_gap_note_on_a_matching_skipped_wall_time():
    # 2026-03-08 02:30 does not exist in America/New_York
    got = _why("30 2 * * *", datetime.datetime(2026, 3, 8, 2, 30), tz=_NY)
    assert got["matches"] is True
    assert [n["code"] for n in got["notes"]] == ["dst-skipped-time"]
    assert "03:30:00" in got["notes"][0]["message"]


def test_why_dst_fold_note_on_a_matching_repeated_wall_time():
    # 2026-11-01 01:30 occurs twice in America/New_York
    got = _why("30 1 * * *", datetime.datetime(2026, 11, 1, 1, 30), tz=_NY)
    assert got["matches"] is True
    assert [n["code"] for n in got["notes"]] == ["dst-repeated-time"]
    assert "first occurrence" in got["notes"][0]["message"]


def test_why_dst_notes_skip_fixed_offset_zones_and_misses():
    # UTC never transitions, so a matching instant carries no DST note
    got = _why("30 2 * * *", datetime.datetime(2026, 3, 8, 2, 30), tz=_UTC)
    assert got["matches"] is True and got["notes"] == []
    # a NON-matching instant in the gap explains the fields, not the zone
    got = _why("0 5 * * *", datetime.datetime(2026, 3, 8, 2, 30), tz=_NY)
    assert got["matches"] is False
    assert got["notes"] == []
