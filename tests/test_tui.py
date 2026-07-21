"""Tests for the terminal dashboard (cronstable.tui).

Two layers, mirroring the module's own split:

* pure-logic tests for the ports of the web dashboard's client-side
  brain (health/verdict/fuzzy/describeCron/formatting) and for the
  terminal plumbing (key decoding, ANSI measurement, themes, prefs);
  these must agree with the web page, so several fixtures are lifted
  verbatim from ``cronstable/web/index.html``;
* end-to-end app tests that boot the real :class:`TuiApp` headless
  against a fake daemon served by aiohttp on a loopback port, drive it
  with a scripted key queue, and assert on the painted frames.

Everything here is tty-free on purpose: the same suite runs on POSIX
CI and on a Windows checkout.
"""

import asyncio
import datetime
import json
import time
from typing import Any, Dict, List, Optional

from aiohttp import web

from cronstable import tui
from cronstable.tui import (
    Api,
    HeadlessTerm,
    KeyDecoder,
    ScriptedKeys,
    Theme,
    TuiApp,
    compute_view,
    correlate,
    cut_to_width,
    describe_cron,
    fmt_ago,
    fmt_bytes,
    fmt_countdown,
    fmt_duration,
    fmt_in,
    fmt_til,
    fuzzy,
    health,
    load_prefs,
    next_fires,
    oneline,
    pad_to,
    rewrite_sgr,
    sanitize_log_line,
    save_prefs,
    scrub_non_sgr,
    sla_overdue,
    spark_cells,
    strip_ansi,
    text_width,
    truncate,
    verdict_info,
)


# ===================================================================
#  helpers
# ===================================================================
def _job(
    name: str,
    *,
    enabled: bool = True,
    running: bool = False,
    outcome: Optional[str] = None,
    exit_code: Optional[int] = 0,
    fail_reason: Optional[str] = None,
    finished_ago: float = 60.0,
    duration: float = 1.0,
    schedule: str = "* * * * *",
    command: str = "echo hi",
    scheduled_in: Optional[float] = 30.0,
    history: Optional[List[Dict[str, Any]]] = None,
    paused: Any = None,
    late: bool = False,
) -> Dict[str, Any]:
    last_run = None
    if outcome is not None:
        finished = datetime.datetime.now(
            datetime.timezone.utc
        ) - datetime.timedelta(seconds=finished_ago)
        last_run = {
            "outcome": outcome,
            "exit_code": exit_code,
            "started_at": (
                finished - datetime.timedelta(seconds=duration)
            ).isoformat(),
            "finished_at": finished.isoformat(),
            "duration": duration,
            "fail_reason": fail_reason,
            "resources": None,
        }
    # paused=True synthesizes a fresh one-hour record; a dict is used
    # verbatim (the daemon's shape: since/until/note/by/channel)
    if paused is True:
        now = datetime.datetime.now(datetime.timezone.utc)
        paused = {
            "since": now.isoformat(),
            "until": (now + datetime.timedelta(hours=1)).isoformat(),
            "note": "",
            "by": "tests",
            "channel": "api",
        }
    job = {
        "name": name,
        "enabled": enabled,
        "schedule": schedule,
        "command": command,
        "captureStdout": True,
        "captureStderr": True,
        "utc": True,
        "timezone": None,
        "running": running,
        "pids": [1] if running else [],
        "scheduled_in": scheduled_in,
        "last_run": last_run,
        "history": history if history is not None else [],
        "paused": paused or None,
    }
    if late:
        job["sla"] = {
            "thresholds": {"lateAfterSeconds": 60},
            "state": "late",
            "breaches": [
                {
                    "check": "lateAfter",
                    "since": datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat(),
                    "observed_seconds": 120.0,
                    "threshold_seconds": 60,
                }
            ],
        }
    return job


# ===================================================================
#  the client-brain ports
# ===================================================================
def test_health_matches_the_web_classifier():
    assert health(_job("a", enabled=False))[0] == "disabled"
    assert health(_job("a", running=True))[0] == "run"
    assert health(_job("a", outcome="failure"))[0] == "fail"
    assert health(_job("a", outcome="cancelled"))[0] == "cancelled"
    assert health(_job("a", outcome="unknown"))[0] == "unknown"
    assert health(_job("a", outcome="success"))[0] == "ok"
    assert health(_job("a"))[0] == "pending"
    # disabled wins over a recorded run; running wins over last_run
    assert health(_job("a", enabled=False, outcome="failure"))[0] == (
        "disabled"
    )
    assert health(_job("a", running=True, outcome="failure"))[0] == "run"
    # paused sits after disabled and before the run/outcome checks, but
    # never masks a live run (web branch order)
    assert health(_job("a", paused=True))[0] == "paused"
    assert health(_job("a", paused=True, outcome="failure"))[0] == "paused"
    assert health(_job("a", paused=True, running=True))[0] == "run"
    assert health(_job("a", paused=True, enabled=False))[0] == "disabled"
    # a pause-skipped slot never ran, so it must not read as a success once
    # the pause lifts and the row becomes the job's last_run
    assert health(_job("a", outcome="skipped")) == ("pending", "Skipped")


def test_fmt_til_is_the_pause_expiry_clock():
    assert fmt_til("2026-12-31T23:45:00+00:00") == "til 23:45"
    assert fmt_til("2026-12-31T22:45:00-01:00") == "til 23:45"  # UTC frame
    assert fmt_til(None) == "til ?"
    assert fmt_til("not a stamp") == "til ?"


def test_sla_overdue_reads_the_payload_flag():
    assert not sla_overdue(_job("a", outcome="success"))
    assert sla_overdue(_job("a", outcome="success", late=True))
    assert not sla_overdue({"sla": {"state": "ok"}})
    assert not sla_overdue({"sla": "late"})  # foreign shape stays False


def test_fuzzy_is_the_web_scorer():
    # substring: 100 - index
    assert fuzzy("run", "run all failing") == 100
    assert fuzzy("all", "run all failing") == 96
    # scattered subsequence scores exactly 1
    assert fuzzy("rlf", "run all failing") == 1
    # no match scores 0; empty query scores 1
    assert fuzzy("zzz", "run all failing") == 0
    assert fuzzy("", "anything") == 1
    # case-insensitive
    assert fuzzy("RUN", "Run all") == 100


def test_compute_view_filters_and_sorts():
    jobs = [
        _job("charlie", command="echo delta"),
        _job("alpha", outcome="failure", duration=9.0),
        _job("bravo", running=True, scheduled_in=None),
    ]
    # text filter matches name OR command, lowercased
    assert [
        j["name"] for j in compute_view(jobs, "DELT", "all", "name", 1)
    ] == ["charlie"]
    # status segments; "off" = disabled
    assert [j["name"] for j in compute_view(jobs, "", "fail", "name", 1)] == [
        "alpha"
    ]
    # status sort: run < fail < ... ; ties break on name
    by_status = compute_view(jobs, "", "all", "status", 1)
    assert [j["name"] for j in by_status] == ["bravo", "alpha", "charlie"]
    # duration sort puts the longest run first
    by_dur = compute_view(jobs, "", "all", "duration", 1)
    assert by_dur[0]["name"] == "alpha"
    # direction flip reverses
    assert compute_view(jobs, "", "all", "name", -1)[0]["name"] == ("charlie")


def test_compute_view_name_sort_matches_web_locale_collation():
    # The web sorts with a.name.localeCompare(b.name): CLDR root collation,
    # case-insensitive at the primary level, lowercase before uppercase on
    # a pure case tie.  The TUI's raw str compare was code-point order --
    # every uppercase-initial name first -- so the DEFAULT first screen of
    # the two frontends disagreed for any mixed-case fleet.  These fleets
    # (and expected orders) are cross-checked against localeCompare under
    # node for the default locale and en-US/en-GB/de-DE/fr-FR/ja-JP.
    jobs = [_job(n) for n in ("backup", "Backup")]
    got = [j["name"] for j in compute_view(jobs, "", "all", "name", 1)]
    assert got == ["backup", "Backup"]  # lowercase first on a case tie

    jobs = [_job(n) for n in ("Backup", "apple", "Deploy", "zeta")]
    got = [j["name"] for j in compute_view(jobs, "", "all", "name", 1)]
    assert got == ["apple", "Backup", "Deploy", "zeta"]
    # descending is the same order reversed wholesale (the web's r * dir)
    got_desc = [j["name"] for j in compute_view(jobs, "", "all", "name", -1)]
    assert got_desc == ["zeta", "Deploy", "Backup", "apple"]

    # the bare name is also the trailing tie-break of EVERY other sort key,
    # so a fleet tying on the primary key (all "ok" here) must collate the
    # same way in the status column
    jobs = [_job(n, outcome="success") for n in ("Zeta", "alpha", "Beta")]
    got = [j["name"] for j in compute_view(jobs, "", "all", "status", 1)]
    assert got == ["alpha", "Beta", "Zeta"]


def test_verdict_single_failure_is_a_warn():
    jobs = [
        _job("ok-1", outcome="success"),
        _job("bad-1", outcome="failure", exit_code=69, fail_reason="boom"),
    ]
    verdict, incident = verdict_info(jobs, None)
    assert verdict is not None
    assert verdict["sev"] == "warn"
    assert "JOB FAILING — bad-1" in verdict["head"]
    assert "exit 69" in verdict["sub"]
    assert "boom" in verdict["sub"]
    assert incident == ["bad-1"]


def test_verdict_correlates_a_shared_signature():
    jobs = [
        _job("a", outcome="failure", exit_code=69, finished_ago=10),
        _job("b", outcome="failure", exit_code=69, finished_ago=20),
        _job("c", outcome="failure", exit_code=1, finished_ago=15),
    ]
    verdict, incident = verdict_info(jobs, None)
    assert verdict is not None
    assert verdict["sev"] == "crit"
    assert "FLEET EVENT — 3 jobs failing" in verdict["head"]
    assert "×2 share exit=69" in verdict["sub"]
    assert "likely one cause" in verdict["sub"]
    # the blast radius is the correlated pair, not all three
    assert sorted(incident) == ["a", "b"]


def test_verdict_uncorrelated_failures():
    jobs = [
        _job("a", outcome="failure", exit_code=2),
        _job("b", outcome="failure", exit_code=3),
    ]
    verdict, incident = verdict_info(jobs, None)
    assert verdict is not None
    assert "no shared failure signature" in verdict["sub"]
    assert sorted(incident) == ["a", "b"]


def test_verdict_cluster_alert_outranks_everything():
    jobs = [_job("a", outcome="failure")]
    alert = {
        "bad": True,
        "reason": "no quorum — Leader jobs paused",
        "node": "n1",
    }
    verdict, _ = verdict_info(jobs, alert)
    assert verdict is not None
    assert verdict["sev"] == "crit"
    assert verdict["glyph"] == "☢"
    assert "CLUSTER ALERT" in verdict["head"]
    assert "this node: n1" in verdict["sub"]


def test_verdict_healthy_is_none():
    verdict, incident = verdict_info([_job("a", outcome="success")], None)
    assert verdict is None
    assert incident == []


def test_correlate_ignores_singletons():
    jobs = [
        _job("a", outcome="failure", exit_code=1),
        _job("b", outcome="failure", exit_code=2),
    ]
    assert correlate(jobs) is None


# ===================================================================
#  schedule intelligence
# ===================================================================
def test_describe_cron_common_shapes():
    assert describe_cron("@reboot") == (
        "Once, when cronstable starts (@reboot)"
    )
    assert describe_cron("@daily") == "Every day at midnight"
    assert describe_cron("* * * * *") == "Every minute, every day"
    assert describe_cron("*/5 * * * *") == "Every 5 minutes, every day"
    assert describe_cron("0 3 * * *") == "At 03:00, every day"
    assert describe_cron("30 * * * *") == "Every hour at :30, every day"
    assert describe_cron("0 0 * * 0") == "At 00:00, on Sunday"
    assert describe_cron("0 12 1 * *") == ("At 12:00, on the 1st of the month")
    # dom + dow must BOTH match: the engine's deliberate AND rule
    # ("0 0 13 * 5" is Friday the 13th), unlike standard cron's OR
    text = describe_cron("0 0 1 * 1")
    assert "on the 1st, and only on Monday" in text
    # out-of-range fields degrade to prose instead of raising
    assert describe_cron("* * * 13 *") == "Custom schedule: * * * 13 *"
    assert describe_cron("* * * * 8") == "Custom schedule: * * * * 8"
    # a step that does not divide 60 is enumerated, not phrased
    assert "Every 7 minutes" not in describe_cron("*/7 * * * *")
    # seconds column (7-field): true cadence only when top is free
    assert describe_cron("*/10 * * * * * *") == "Every 10 seconds"
    assert describe_cron("bogus") == "Custom schedule: bogus"


def test_next_fires_agrees_with_the_engine():
    start = datetime.datetime(
        2026, 7, 17, 11, 59, 30, tzinfo=datetime.timezone.utc
    )
    fires = next_fires("0 12 * * *", 2, start=start)
    assert [f.strftime("%H:%M") for f in fires] == ["12:00", "12:00"]
    assert fires[0].date().isoformat() == "2026-07-17"
    assert fires[1].date().isoformat() == "2026-07-18"
    assert next_fires("@reboot", 3) == []
    assert next_fires("not a schedule", 3) == []


def test_next_fires_steps_in_absolute_time_across_dst():
    from zoneinfo import ZoneInfo

    tz = ZoneInfo("America/New_York")
    # spring forward (2026-03-08): exactly one 06:00 fire per day, no
    # phantom 05:00 fire on the transition day
    start = datetime.datetime(2026, 3, 7, 12, 0, tzinfo=tz)
    fires = next_fires("0 6 * * *", 3, tz=tz, start=start)
    assert [f.strftime("%m-%d %H:%M") for f in fires] == [
        "03-08 06:00",
        "03-09 06:00",
        "03-10 06:00",
    ]
    # fall back (2026-11-01): the first fire after the transition is
    # still at 06:00 wall time, not shifted an hour
    start = datetime.datetime(2026, 10, 31, 12, 0, tzinfo=tz)
    fires = next_fires("0 6 * * *", 2, tz=tz, start=start)
    assert [f.strftime("%m-%d %H:%M") for f in fires] == [
        "11-01 06:00",
        "11-02 06:00",
    ]


# ===================================================================
#  formatting
# ===================================================================
def test_format_helpers():
    assert fmt_in(None) == "—"
    assert fmt_in(0) == "now"
    assert fmt_in(42) == "in 42s"
    assert fmt_in(90) == "in 1m"
    assert fmt_in(7200) == "in 2h"
    assert fmt_duration(0.85) == "850ms"
    assert fmt_duration(4.2) == "4.2s"
    assert fmt_duration(190) == "3m10s"
    assert fmt_duration(7440) == "2h04m"
    assert fmt_countdown(65) == "01:05"
    assert fmt_countdown(7199) == "2h00m"  # rounds minutes first
    assert fmt_bytes(512) == "512B"
    assert fmt_bytes(2048) == "2.0KiB"
    now = time.time()
    iso = datetime.datetime.fromtimestamp(
        now - 90, tz=datetime.timezone.utc
    ).isoformat()
    assert fmt_ago(iso, now) == "1m ago"
    assert fmt_ago(None) == "—"
    assert fmt_ago("garbage") == "—"


# ===================================================================
#  terminal plumbing
# ===================================================================
def test_key_decoder_basics():
    dec = KeyDecoder()
    assert dec.feed(b"j") == ["j"]
    assert dec.feed(b"\x1b[A") == ["up"]
    assert dec.feed(b"\x1b[B\x1b[D") == ["down", "left"]
    assert dec.feed(b"\x1bOA") == ["up"]
    assert dec.feed(b"\x1b[5~") == ["pgup"]
    assert dec.feed(b"\x1b[Z") == ["shift+tab"]
    assert dec.feed(b"\r") == ["enter"]
    assert dec.feed(b"\t") == ["tab"]
    assert dec.feed(b"\x7f") == ["backspace"]
    assert dec.feed(b"\x0b") == ["ctrl+k"]
    assert dec.feed(b"\x10") == ["ctrl+p"]
    assert dec.feed(b"\x03") == ["ctrl+c"]


def test_key_decoder_modified_and_split_sequences():
    dec = KeyDecoder()
    # a modifier-carrying arrow collapses to the plain key
    assert dec.feed(b"\x1b[1;5A") == ["up"]
    # sequences split across reads reassemble
    assert dec.feed(b"\x1b[") == []
    assert dec.feed(b"6~") == ["pgdn"]
    # a lone Esc resolves via the quiet-gap flush
    assert dec.feed(b"\x1b") == []
    assert dec.flush_escape() == ["esc"]
    # utf-8 text split across reads survives too
    assert dec.feed("é".encode("utf-8")[:1]) == []
    assert dec.feed("é".encode("utf-8")[1:]) == ["é"]


def test_ansi_measurement_and_cutting():
    theme = Theme("carolina", light=False)
    styled = theme.fg("ok") + "hello" + "\x1b[0m" + " world"
    assert strip_ansi(styled) == "hello world"
    assert text_width(styled) == 11
    assert truncate("hello world", 8) == "hello w…"
    assert pad_to("hi", 5) == "hi   "
    cut = cut_to_width(styled, 7)
    assert strip_ansi(cut).rstrip() == "hello w"
    # wide characters count as two cells
    assert text_width("日本") == 4
    assert strip_ansi(cut_to_width("日本語", 5)).rstrip() == "日本"


def test_rewrite_sgr_reinks_log_colors():
    theme = Theme("carolina", light=False)
    out = rewrite_sgr("\x1b[31mred\x1b[0m plain", theme)
    assert "red" in strip_ansi(out)
    assert theme.fg("fail") in out  # 31 -> the theme's fail ink
    assert "\x1b[31m" not in out  # raw palette gone
    # OSC and other non-SGR escapes are stripped entirely
    assert strip_ansi(rewrite_sgr("\x1b]0;title\x07text", theme)) == "text"
    # 256-color foregrounds collapse to the bright ink, not garbage
    out = rewrite_sgr("\x1b[38;5;196mX", theme)
    assert strip_ansi(out) == "X"


def test_oneline_flattens_multiline_commands():
    # grand-tour-style multi-line shell commands must not break rows
    assert oneline("set -eu\ntoken=$(get)\ncurl x") == (
        "set -eu token=$(get) curl x"
    )
    assert oneline("  spaced\t\tout  ") == "spaced out"


def test_sanitize_log_line_defuses_control_chars():
    # cmd.exe CRLF tail: the stray \r must not reach a painted row
    assert sanitize_log_line("heartbeat ok\r") == "heartbeat ok"
    # mid-line \r gets log-viewer overwrite semantics (progress bars)
    assert sanitize_log_line("10%\r55%\r100%") == "100%"
    assert sanitize_log_line("busy\r") == "busy"
    assert sanitize_log_line("\ttabbed") == "    tabbed"
    assert sanitize_log_line("a\x00b\x07c") == "abc"
    # ESC survives so rewrite_sgr can re-ink colours
    assert "\x1b[31m" in sanitize_log_line("\x1b[31mred")


def test_hostile_escapes_never_reach_a_frame():
    """Every non-SGR escape family must die in every paint path: a job
    name or log line is attacker-influenced (under clustering it comes
    from other machines over gossip), and a survivor could reset the
    terminal, rewrite the clipboard (OSC 52), or retitle the window."""
    theme = Theme("carolina", light=False)
    hostile = [
        "a\x1bcb",  # RIS hard reset
        "a\x1b(0b",  # DEC line-drawing charset
        "a\x1bP1;2|payload\x1b\\b",  # DCS
        "a\x1b_apc payload\x1b\\b",  # APC
        "a\x1b]52;c;cHduZWQ=\x07b",  # OSC 52 clipboard write
        "a\x1b]0;title\x07b",  # OSC 0 window title
        "a\x1b[<0;3;4Myb",  # CSI private-param mouse report
        "a\x1b7\x1b8b",  # cursor save/restore
        "a\x1b",  # bare trailing ESC
    ]
    for text in hostile:
        for out in (
            rewrite_sgr(sanitize_log_line(text), theme),
            scrub_non_sgr(text),
            pad_to(text, 24),
            truncate(text, 24),
            cut_to_width(text, 24),
            oneline(text),
        ):
            for pos in range(len(out)):
                if out[pos] == "\x1b":
                    # any surviving ESC must open an SGR token
                    assert out[pos : pos + 2] == "\x1b[", repr((text, out))
                    end = out.find("m", pos)
                    body = out[pos + 2 : end]
                    assert end > 0 and all(
                        c.isdigit() or c in ";:" for c in body
                    ), repr((text, out))
        # visible payload text still renders
        assert "a" in strip_ansi(scrub_non_sgr(text))
    # the painter's own SGR styling survives the same paths
    styled = "\x1b[31mred\x1b[0m plain"
    assert "\x1b[31m" in cut_to_width(styled, 30)
    assert "\x1b[31m" in scrub_non_sgr(styled)
    assert "\x1b[31m" in pad_to(styled, 30)


def test_spark_cells_scale_and_color():
    history = [
        {"outcome": "success", "duration": 1.0},
        {"outcome": "failure", "duration": 4.0},
        {"outcome": "success", "duration": 2.0},
    ]
    cells = spark_cells(history, 10)
    assert len(cells) == 3
    assert cells[1][1] == "fail"
    assert cells[0][1] == "ok"
    # the longest run gets the tallest bar
    bars = "▁▂▃▄▅▆▇█"
    assert bars.index(cells[1][0]) > bars.index(cells[0][0])


def test_theme_lookup_and_cvd():
    dark = Theme("carolina", light=False)
    light = Theme("carolina", light=True)
    assert dark.colors["bg"] != light.colors["bg"]
    deutan = Theme("carolina", light=False, cvd="deutan")
    assert deutan.colors["ok"] != dark.colors["ok"]
    # unknown hue falls back rather than raising
    assert Theme("nope", light=False).hue == "carolina"


def test_prefs_roundtrip(tmp_path):
    path = str(tmp_path / "tui.json")
    prefs = load_prefs(path)  # missing file -> defaults
    assert prefs["theme"] == "carolina"
    prefs["theme"] = "amber"
    prefs["poll_ms"] = 5000
    save_prefs(prefs, path)
    again = load_prefs(path)
    assert again["theme"] == "amber"
    assert again["poll_ms"] == 5000
    # corrupt file -> defaults, no raise
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("{nope")
    assert load_prefs(path)["theme"] == "carolina"
    # a bad stored theme falls back
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"theme": "plaid"}, fh)
    assert load_prefs(path)["theme"] == "carolina"


def test_help_overlay_carries_the_web_table():
    """Keyboard parity is the feature: the help overlay must list the
    web dashboard's shortcut table verbatim, parsed out of the real
    ``fillHelp()`` in ``cronstable/web/index.html``, so a web-side edit
    fails here instead of silently drifting the two frontends apart."""
    import pathlib
    import re

    web_html = (
        pathlib.Path(tui.__file__).parent / "web" / "index.html"
    ).read_text(encoding="utf-8")
    block = re.search(
        r"function fillHelp\(\)\s*\{\s*const rows = \[(.*?)\];",
        web_html,
        re.S,
    )
    assert block, "fillHelp() rows array not found in web/index.html"
    web_rows = re.findall(
        r'\[\s*"((?:[^"\\]|\\.)*)"\s*,\s*"((?:[^"\\]|\\.)*)"\s*\]',
        block.group(1),
    )
    # a parse regression must fail loudly, never pass as [] == []
    assert len(web_rows) >= 10, "fillHelp parse found %d rows" % len(web_rows)
    assert tui.HELP_ROWS == web_rows


def test_help_rows_pin_the_pause_row_after_cancel():
    """The pause row's strings are pinned byte-for-byte for web parity,
    directly after the cancel row (both surfaces insert at that index)."""
    idx = tui.HELP_ROWS.index(("p", "Pause or resume selected job"))
    assert tui.HELP_ROWS[idx - 1] == ("x", "Cancel selected (running) job")


# ===================================================================
#  the app, headless against a fake daemon
# ===================================================================
class FakeDaemon:
    """A loopback daemon: the endpoints the TUI touches, scriptable.

    Every payload attribute defaults to the feature-off shape, so tests
    opt panels in by assigning the richer fixtures (see test_tui_tour
    for the full-fleet ones, whose shapes were verified against a live
    grand-tour daemon).
    """

    def __init__(self) -> None:
        self.jobs: List[Dict[str, Any]] = []
        self.token: Optional[str] = None
        self.posts: List[str] = []
        self.post_bodies: List[Any] = []
        self.log_lines: Dict[str, List[Dict[str, str]]] = {}
        self.fail_logs_for: set = set()  # names whose SSE 500s
        self.cluster: Dict[str, Any] = {"enabled": False, "peers": []}
        self.fleet: Dict[str, Any] = {"enabled": False, "nodes": []}
        self.dags_list: List[Dict[str, Any]] = []
        self.dag_runs: Dict[str, List[Dict[str, Any]]] = {}
        self.dag_docs: Dict[str, Dict[str, Any]] = {}  # runKey -> doc
        self.dag_xcom: Dict[str, Dict[str, Any]] = {}  # runKey -> body
        self.state: Dict[str, Any] = {"enabled": False}
        self.state_documents: Dict[str, List[Any]] = {}
        self.state_records: Dict[str, List[Any]] = {}
        self.node: Dict[str, Any] = {
            "node_name": "test-node",
            "resources": None,
        }
        self.node_history: Dict[str, Any] = {
            "node_name": "test-node",
            "enabled": False,
            "interval": None,
            "points": [],
        }
        self.job_resources: Dict[str, Dict[str, Any]] = {}
        self.runner: Optional[web.AppRunner] = None
        self.url = ""

    async def start(self) -> None:
        app = web.Application(middlewares=[self._auth])
        app.router.add_get("/version", self._version)
        app.router.add_get("/job-set-id", self._job_set_id)
        app.router.add_get("/jobs", self._jobs)
        app.router.add_get("/cluster", self._cluster)
        app.router.add_get("/fleet", self._fleet)
        app.router.add_get("/node", self._node)
        app.router.add_get("/node/history", self._node_history)
        app.router.add_get("/dags", self._dags)
        app.router.add_get("/dags/{name}/runs", self._dag_runs)
        app.router.add_get("/dags/{name}/runs/{rk}", self._dag_doc)
        app.router.add_get("/dags/{name}/runs/{rk}/xcom", self._dag_xcom)
        app.router.add_get(
            "/dags/{name}/runs/{rk}/tasks/{task}/logs", self._task_logs
        )
        app.router.add_post("/dags/{name}/trigger", self._dag_trigger)
        app.router.add_post("/dags/{name}/backfill", self._dag_backfill)
        app.router.add_post(
            "/dags/{name}/runs/{rk}/tasks/{task}/decision",
            self._dag_decision,
        )
        app.router.add_get("/state", self._state)
        app.router.add_get("/state/documents", self._state_documents)
        app.router.add_get("/state/records", self._state_records)
        app.router.add_get("/jobs/{name}/runs", self._runs)
        app.router.add_get("/jobs/{name}/resources", self._resources)
        app.router.add_get("/jobs/{name}/logs", self._logs)
        app.router.add_post("/jobs/{name}/{verb}", self._verb)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]  # noqa: SLF001
        self.url = "http://127.0.0.1:%d" % port

    async def stop(self) -> None:
        if self.runner is not None:
            await self.runner.cleanup()

    @web.middleware
    async def _auth(self, request, handler):
        if self.token is not None:
            got = request.headers.get("Authorization", "")
            if got != "Bearer %s" % self.token:
                return web.Response(status=401)
        return await handler(request)

    async def _version(self, request):
        return web.Response(text="9.9-test")

    async def _job_set_id(self, request):
        return web.Response(text="cafebabe12")

    async def _jobs(self, request):
        return web.json_response(self.jobs)

    async def _cluster(self, request):
        return web.json_response(self.cluster)

    async def _fleet(self, request):
        return web.json_response(self.fleet)

    async def _node(self, request):
        return web.json_response(self.node)

    async def _node_history(self, request):
        return web.json_response(self.node_history)

    async def _dags(self, request):
        return web.json_response(self.dags_list)

    async def _dag_runs(self, request):
        name = request.match_info["name"]
        if not any(d.get("name") == name for d in self.dags_list):
            return web.Response(status=404)
        return web.json_response(
            {"dag": name, "runs": self.dag_runs.get(name, [])}
        )

    async def _dag_doc(self, request):
        doc = self.dag_docs.get(request.match_info["rk"])
        if doc is None:
            return web.Response(status=404)
        return web.json_response(doc)

    async def _dag_xcom(self, request):
        return web.json_response(
            self.dag_xcom.get(request.match_info["rk"], {})
        )

    async def _task_logs(self, request):
        task = request.match_info["task"]
        resp = web.StreamResponse(
            headers={"Content-Type": "text/event-stream"}
        )
        await resp.prepare(request)
        frame = "event: line\ndata: %s\n\n" % json.dumps(
            {"stream": "stdout", "line": "task %s says hi" % task}
        )
        await resp.write(frame.encode("utf-8"))
        await resp.write(b"event: end\ndata: {}\n\n")
        return resp

    async def _dag_trigger(self, request):
        name = request.match_info["name"]
        self.posts.append("dag/%s/trigger" % name)
        return web.json_response({"dag": name, "runKey": "manual-new"})

    async def _dag_backfill(self, request):
        name = request.match_info["name"]
        self.posts.append("dag/%s/backfill" % name)
        self.post_bodies.append(await request.json())
        return web.json_response({"dag": name, "queued": 2})

    async def _dag_decision(self, request):
        self.posts.append(
            "dag/%s/%s/%s/decision"
            % (
                request.match_info["name"],
                request.match_info["rk"],
                request.match_info["task"],
            )
        )
        self.post_bodies.append(await request.json())
        return web.json_response({"ok": True})

    async def _state(self, request):
        return web.json_response(self.state)

    async def _state_documents(self, request):
        ns = request.query.get("ns", "")
        return web.json_response(
            {
                "namespace": ns,
                "documents": self.state_documents.get(ns, []),
            }
        )

    async def _state_records(self, request):
        stream = request.query.get("stream", "")
        return web.json_response(
            {"stream": stream, "records": self.state_records.get(stream, [])}
        )

    async def _resources(self, request):
        name = request.match_info["name"]
        payload = self.job_resources.get(name)
        if payload is None:
            return web.json_response(
                {
                    "name": name,
                    "monitored": False,
                    "interval": None,
                    "live": [],
                    "runs": [],
                }
            )
        return web.json_response(payload)

    async def _runs(self, request):
        name = request.match_info["name"]
        job = next((j for j in self.jobs if j["name"] == name), None)
        if job is None:
            return web.Response(status=404)
        runs = [job["last_run"]] if job.get("last_run") else []
        stats = {
            "total": len(runs),
            "success": sum(1 for r in runs if r["outcome"] == "success"),
            "failure": sum(1 for r in runs if r["outcome"] == "failure"),
            "cancelled": 0,
            "unknown": 0,
            "success_rate": 1.0 if runs else None,
            "avg_duration": 1.0,
            "min_duration": 1.0,
            "max_duration": 1.0,
            "last_duration": 1.0,
        }
        return web.json_response({"name": name, "runs": runs, "stats": stats})

    async def _logs(self, request):
        name = request.match_info["name"]
        if name in self.fail_logs_for:
            return web.Response(status=500)
        resp = web.StreamResponse(
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
            }
        )
        await resp.prepare(request)
        for line in self.log_lines.get(name, []):
            frame = "event: line\ndata: %s\n\n" % json.dumps(line)
            await resp.write(frame.encode("utf-8"))
        await resp.write(b"event: end\ndata: {}\n\n")
        return resp

    async def _verb(self, request):
        name = request.match_info["name"]
        verb = request.match_info["verb"]
        self.posts.append("%s/%s" % (name, verb))
        # mirror the daemon's semantics: 404 unknown job, 409 for a
        # disabled start or a cancel with nothing running
        job = next((j for j in self.jobs if j["name"] == name), None)
        if job is None:
            return web.Response(status=404)
        if verb == "start" and not job.get("enabled"):
            return web.Response(status=409)
        if verb == "cancel" and not job.get("running"):
            return web.Response(status=409)
        if verb == "pause":
            # the daemon's pause is an idempotent overwrite: re-pausing
            # a paused job answers 200 with the fresh record, never 409
            now = datetime.datetime.now(datetime.timezone.utc)
            job["paused"] = {
                "since": now.isoformat(),
                "until": (now + datetime.timedelta(hours=1)).isoformat(),
                "note": "",
                "by": "tui",
                "channel": "api",
            }
            return web.json_response({"paused": job["paused"]})
        if verb == "resume":
            # resuming an unpaused job is a 200 no-op, not a 409
            job["paused"] = None
            return web.json_response({"paused": None})
        return web.Response(status=200)


async def _wait_for(predicate, timeout=5.0, interval=0.01):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met within %ss" % timeout)


class Harness:
    def __init__(self) -> None:
        self.daemon = FakeDaemon()
        self.term = HeadlessTerm(110, 32)
        self.keys = ScriptedKeys()
        self.app: Optional[TuiApp] = None
        self._task: Optional[asyncio.Task] = None

    async def start(self, tmp_path, token=None, **app_kwargs):
        await self.daemon.start()
        prefs = dict(tui.PREF_DEFAULTS)
        prefs["poll_ms"] = 200
        prefs["boot"] = False
        self.app = TuiApp(
            Api(self.daemon.url, token),
            self.term,
            self.keys,
            prefs,
            boot=False,
            prefs_file=str(tmp_path / "prefs.json"),
            **app_kwargs,
        )
        self._task = asyncio.get_running_loop().create_task(self.app.run())
        return self.app

    async def settle(self):
        """Wait for the next painted frame after pending work."""
        await asyncio.sleep(0.08)
        await _wait_for(lambda: bool(self.term.frames))

    async def stop(self):
        if self.app is not None and self._task is not None:
            self.app.quit = True
            self.keys.send("q")  # unblock the input loop
            try:
                await asyncio.wait_for(self._task, 5)
            except asyncio.TimeoutError:
                self._task.cancel()
        await self.daemon.stop()


async def test_app_boots_and_paints_the_board(tmp_path):
    h = Harness()
    h.daemon.jobs = [
        _job("heartbeat", outcome="success"),
        _job(
            "north-beacon",
            outcome="failure",
            exit_code=69,
            fail_reason="exited with code 69",
        ),
    ]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 2)
        await h.settle()
        screen = h.term.screen()
        assert "heartbeat" in screen
        assert "north-beacon" in screen
        assert "9.9-test" in screen  # version chip
        assert "2 jobs" in screen
        assert "JOB FAILING — north-beacon" in screen  # verdict bar
        assert "live" in screen  # connection dot
    finally:
        await h.stop()


async def test_selection_run_and_cancel_keys(tmp_path):
    h = Harness()
    h.daemon.jobs = [
        _job("alpha", outcome="success"),
        _job("bravo", outcome="success"),
        _job("runner", running=True, scheduled_in=None),
    ]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 3)
        # j moves; r runs the selected (eligible) job
        h.keys.send("j")
        await _wait_for(lambda: app.sel == 1)
        assert app.selected_job()["name"] == "bravo"
        h.keys.send("r")
        await _wait_for(lambda: "bravo/start" in h.daemon.posts)
        # x cancels only a running job
        h.keys.send("j")  # -> runner
        await _wait_for(lambda: app.sel == 2)
        h.keys.send("x")
        await _wait_for(lambda: "runner/cancel" in h.daemon.posts)
        # r on a running job is a no-op (web guard: enabled && !running)
        h.keys.send("r")
        await asyncio.sleep(0.1)
        assert "runner/start" not in h.daemon.posts
        # selection wraps at the bottom, like the web table
        h.keys.send("j")
        await _wait_for(lambda: app.sel == 0)
    finally:
        await h.stop()


async def test_selection_pause_and_resume_key(tmp_path):
    h = Harness()
    h.daemon.jobs = [
        _job("alpha", outcome="success"),
        _job("bravo", outcome="success", paused=True),
    ]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 2)
        assert app.selected_job()["name"] == "alpha"
        # p pauses the selected (unpaused) job...
        h.keys.send("p")
        await _wait_for(lambda: "alpha/pause" in h.daemon.posts)
        # ...and once the forced re-poll delivers the record, the same
        # key resumes it
        await _wait_for(
            lambda: bool((app.by_name.get("alpha") or {}).get("paused"))
        )
        h.keys.send("p")
        await _wait_for(lambda: "alpha/resume" in h.daemon.posts)
        # a job that arrives already paused resumes on the first press
        h.keys.send("j")
        await _wait_for(lambda: app.sel == 1)
        assert app.selected_job()["name"] == "bravo"
        h.keys.send("p")
        await _wait_for(lambda: "bravo/resume" in h.daemon.posts)
    finally:
        await h.stop()


async def test_pause_key_is_inert_against_an_older_daemon(tmp_path):
    """A /jobs payload without the "paused" key (an older daemon) makes
    p a silent no-op instead of POSTing an endpoint that is not there."""
    h = Harness()
    legacy = _job("legacy", outcome="success")
    legacy.pop("paused")
    h.daemon.jobs = [legacy]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 1)
        h.keys.send("p")
        await asyncio.sleep(0.1)
        assert not any(p.startswith("legacy/") for p in h.daemon.posts)
    finally:
        await h.stop()


async def test_drawer_logs_and_esc_priority(tmp_path):
    h = Harness()
    h.daemon.jobs = [_job("tailme", outcome="success")]
    h.daemon.log_lines["tailme"] = [
        {"stream": "stdout", "line": "hello from the job"},
        {"stream": "stderr", "line": "\x1b[31ma red warning\x1b[0m"},
    ]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 1)
        h.keys.send("enter")
        await _wait_for(lambda: app.is_open("drawer"))
        await _wait_for(
            lambda: app.log_tail is not None and len(app.log_tail.lines) >= 2
        )
        await h.settle()
        screen = h.term.screen()
        assert "hello from the job" in screen
        assert "a red warning" in screen
        assert "end of run output" in screen
        # tab cycles to the history tab
        h.keys.send("tab")
        await _wait_for(lambda: app.drawer_tab == "history")
        # esc closes the drawer (nothing else is open)
        h.keys.send("esc")
        await _wait_for(lambda: not app.is_open("drawer"))
        assert app.log_tail is None  # stream torn down
    finally:
        await h.stop()


async def test_filter_focus_and_typing(tmp_path):
    h = Harness()
    h.daemon.jobs = [
        _job("north-beacon"),
        _job("south-beacon"),
        _job("pulse-check"),
    ]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 3)
        h.keys.send("/", "b", "e", "a", "c")
        await _wait_for(lambda: app.filter_text == "beac")
        assert [j["name"] for j in app.view] == [
            "north-beacon",
            "south-beacon",
        ]
        # while the field is focused, list keys type instead of acting
        h.keys.send("j")
        await _wait_for(lambda: app.filter_text == "beacj")
        h.keys.send("backspace")
        await _wait_for(lambda: app.filter_text == "beac")
        # enter commits + blurs; j moves the selection again
        h.keys.send("enter")
        await _wait_for(lambda: app.focus is None)
        h.keys.send("j")
        await _wait_for(lambda: app.sel == 1)
    finally:
        await h.stop()


async def test_palette_runs_a_job_action(tmp_path):
    h = Harness()
    h.daemon.jobs = [_job("deploy", outcome="success")]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 1)
        h.keys.send("ctrl+k")
        await _wait_for(lambda: app.is_open("palette"))
        for ch in "run: dep":
            h.keys.send(ch)
        await _wait_for(
            lambda: (
                app.palette_matches()
                and app.palette_matches()[0][1] == "Run: deploy"
            )
        )
        h.keys.send("enter")
        await _wait_for(lambda: "deploy/start" in h.daemon.posts)
        assert not app.is_open("palette")
    finally:
        await h.stop()


async def test_wallboard_and_stale_banner(tmp_path):
    h = Harness()
    h.daemon.jobs = [
        _job("tile-a", outcome="success"),
        _job("tile-b", outcome="failure"),
    ]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 2)
        h.keys.send("w")
        await _wait_for(lambda: app.wallboard)
        await h.settle()
        screen = h.term.screen()
        assert "tile-a" in screen
        assert "tile-b" in screen
        assert "esc/w exit" in screen
        assert "NO SIGNAL" not in screen
        # age the data past the stale floor: the banner must appear.
        # Pause polling FIRST and let any in-flight cycle drain, so a
        # late poll cannot re-stamp fetched_mono under the assertion.
        app.prefs["poll_ms"] = 0
        await asyncio.sleep(0.35)
        app.fetched_mono -= 120
        app.mark()
        await h.settle()
        assert "NO SIGNAL" in h.term.screen()
        # w toggles back off even while the wallboard is up
        h.keys.send("w")
        await _wait_for(lambda: not app.wallboard)
    finally:
        await h.stop()


async def test_incident_timeline_overlay(tmp_path):
    h = Harness()
    h.daemon.jobs = [
        _job("bad-a", outcome="failure", exit_code=69, finished_ago=30),
        _job("ok-b", outcome="success", finished_ago=300),
    ]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 2)
        h.keys.send("i")
        await _wait_for(lambda: app.is_open("timeline"))
        await h.settle()
        screen = h.term.screen()
        assert "incident timeline" in screen
        assert "bad-a" in screen
        assert "ok-b" in screen  # every job's most recent finish
        # f narrows to failures only
        h.keys.send("f")
        await _wait_for(lambda: app.timeline_fail_only)
        assert [e[0] for e in app.timeline_entries()] == ["bad-a"]
        h.keys.send("esc")
        await _wait_for(lambda: not app.is_open("timeline"))
    finally:
        await h.stop()


async def test_token_modal_on_401(tmp_path):
    h = Harness()
    h.daemon.jobs = [_job("secret-job")]
    h.daemon.token = "s3cr3t"
    try:
        app = await h.start(tmp_path)  # no token -> 401s
        await _wait_for(lambda: app.is_open("token"))
        await h.settle()
        assert "access token" in h.term.screen()
        for ch in "s3cr3t":
            h.keys.send(ch)
        h.keys.send("enter")
        await _wait_for(lambda: not app.is_open("token"))
        await _wait_for(lambda: len(app.jobs) == 1, timeout=8)
        assert app.connected
    finally:
        await h.stop()


async def test_theme_cycling_persists(tmp_path):
    h = Harness()
    h.daemon.jobs = [_job("a")]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 1)
        assert app.theme.hue == "carolina"
        h.keys.send("t")
        await _wait_for(lambda: app.theme.hue == "amber")
        h.keys.send("T")
        await _wait_for(lambda: app.theme.light)
        saved = load_prefs(str(tmp_path / "prefs.json"))
        assert saved["theme"] == "amber"
        assert saved["light"] is True
    finally:
        await h.stop()


async def test_help_overlay_lists_every_key(tmp_path):
    h = Harness()
    h.daemon.jobs = [_job("a")]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 1)
        h.keys.send("?")
        await _wait_for(lambda: app.is_open("help"))
        await h.settle()
        screen = h.term.screen()
        assert "Command palette" in screen
        assert "Wallboard (TV) mode" in screen
        assert "terminal extras" in screen
        h.keys.send("?")
        await _wait_for(lambda: not app.is_open("help"))
    finally:
        await h.stop()


async def test_quit_key_ends_the_app(tmp_path):
    h = Harness()
    h.daemon.jobs = [_job("a")]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 1)
        h.keys.send("q")
        await asyncio.wait_for(h._task, 5)  # noqa: SLF001
        assert app.quit
    finally:
        await h.stop()


async def test_manual_refresh_works_while_paused(tmp_path):
    """--poll 0 is a first-class mode: g (and every post-action
    refresh) must still fetch exactly once per press."""
    h = Harness()
    h.daemon.jobs = [_job("alpha")]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 1)
        app.prefs["poll_ms"] = 0
        await asyncio.sleep(0.05)  # let the poll loop park itself
        h.daemon.jobs = [_job("alpha"), _job("bravo")]
        h.keys.send("g")
        await _wait_for(lambda: len(app.jobs) == 2)
    finally:
        await h.stop()


async def test_wallboard_keeps_the_palette_closed(tmp_path):
    """Ctrl-K on the TV board must stay inert: the wallboard composes
    no overlays, so an invisible palette would swallow keys and could
    fire unseen job actions on Enter."""
    h = Harness()
    h.daemon.jobs = [_job("alpha", outcome="success")]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 1)
        h.keys.send("w")
        await _wait_for(lambda: app.wallboard)
        h.keys.send("ctrl+k")
        await asyncio.sleep(0.1)
        assert not app.is_open("palette")
        h.keys.send("w")  # and w still leaves the board
        await _wait_for(lambda: not app.wallboard)
    finally:
        await h.stop()


async def test_log_search_cycles_and_esc_blurs_first(tmp_path):
    h = Harness()
    h.daemon.jobs = [_job("tailme", outcome="success")]
    h.daemon.log_lines["tailme"] = [
        {"stream": "stdout", "line": "alpha error"},
        {"stream": "stdout", "line": "quiet middle"},
        {"stream": "stdout", "line": "beta error"},
    ]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 1)
        h.keys.send("enter")
        await _wait_for(
            lambda: app.log_tail is not None and len(app.log_tail.lines) >= 3
        )
        for ch in "/error":
            h.keys.send(ch)
        await _wait_for(lambda: app.inputs["logsearch"] == "error")
        # Enter releases the input and lands on the FIRST match
        h.keys.send("enter")
        await _wait_for(lambda: app.focus is None)
        assert app.log_matches == [0, 2]
        assert app.log_match_idx == 0
        # n / N cycle with wrap-around instead of pinning
        h.keys.send("n")
        await _wait_for(lambda: app.log_match_idx == 1)
        h.keys.send("n")
        await _wait_for(lambda: app.log_match_idx == 0)
        h.keys.send("N")
        await _wait_for(lambda: app.log_match_idx == 1)
        # Esc while typing blurs the search; the drawer survives
        h.keys.send("/")
        await _wait_for(lambda: app.focus == "logsearch")
        h.keys.send("esc")
        await _wait_for(lambda: app.focus is None)
        assert app.is_open("drawer")
        h.keys.send("esc")
        await _wait_for(lambda: not app.is_open("drawer"))
    finally:
        await h.stop()


async def test_log_tail_does_not_duplicate_a_finished_run(
    tmp_path, monkeypatch
):
    """The daemon replays a finished run's whole buffer on every SSE
    re-attach; the tail must show that run once, not once per 5s."""
    monkeypatch.setattr(tui, "TAIL_RETRY_MS", 20)
    h = Harness()
    h.daemon.jobs = [_job("tailme", outcome="success")]
    h.daemon.log_lines["tailme"] = [
        {"stream": "stdout", "line": "one"},
        {"stream": "stdout", "line": "two"},
    ]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 1)
        h.keys.send("enter")
        await _wait_for(
            lambda: app.log_tail is not None and len(app.log_tail.lines) >= 3
        )
        # sit through several re-attach cycles of the finished run
        await asyncio.sleep(0.5)
        texts = [line for _, line, _ in app.log_tail.lines]
        assert texts == ["one", "two", "end of run output"]
    finally:
        await h.stop()


async def test_race_skip_lets_the_skip_key_win():
    loop = asyncio.get_running_loop()

    async def slow():
        await asyncio.sleep(5)
        return "late"

    skip = loop.create_task(asyncio.sleep(0.01))
    assert await tui._race_skip(slow(), skip, "skipped") == "skipped"
    # a finished probe wins while the skip key is still pending
    pending = loop.create_task(asyncio.sleep(5))

    async def fast():
        return "value"

    assert await tui._race_skip(fast(), pending, "skipped") == "value"
    pending.cancel()


def test_dispatch_refuses_without_a_tty(monkeypatch):
    class NotATty:
        def isatty(self):
            return False

    monkeypatch.setattr("sys.stdin", NotATty())
    monkeypatch.setattr("sys.stdout", NotATty())

    class Args:
        url = tui.DEFAULT_URL
        token = None
        token_env = tui.ENV_TOKEN

    assert tui.dispatch(Args()) == 2


async def test_schedule_pressure_overlay(tmp_path):
    h = Harness()
    h.daemon.jobs = [
        _job("herd-a", schedule="0 * * * *", outcome="success"),
        _job("herd-b", schedule="0 * * * *", outcome="success"),
        _job("spread", schedule="H * * * *", outcome="success"),
    ]
    # the daemon resolves H before serving /jobs; emulate that field
    h.daemon.jobs[2]["schedule_resolved"] = "58 * * * *"
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 3)
        app._toggle("press")
        await _wait_for(lambda: app.pressure is not None)
        await h.settle()
        screen = h.term.screen()
        assert "schedule pressure" in screen
        assert "duplicate schedules" in screen
        assert "0 * * * *" in screen          # the herd's group
        assert "suggest" in screen
        assert "busiest :00" in screen
        # the H job counted via its resolved schedule: 2 herd + 1 spread
        assert "3 schedules" in screen
        # esc closes the overlay again
        h.keys.send("esc")
        await h.settle()
        assert not app.is_open("press")
    finally:
        await h.stop()


async def test_week_calendar_overlay(tmp_path):
    # the TUI sibling of the web week calendar: business-day fires make
    # the agenda, a minutely job summarizes into the background hum
    h = Harness()
    h.daemon.jobs = [
        _job("monthly-close", schedule="30 1 LW * *", outcome="success"),
        _job("daily-report", schedule="0 6 * * *", outcome="success"),
        _job("heartbeat", schedule="* * * * *", outcome="success"),
    ]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 3)
        app._toggle("week")
        await _wait_for(lambda: app.week is not None)
        await h.settle()
        screen = h.term.screen()
        assert "week calendar (UTC)" in screen
        assert "upcoming fires" in screen
        assert "daily-report" in screen
        # the minutely job is hum, not agenda: named once, in the strip
        assert "background hum" in screen
        assert "heartbeat" in screen
        data = app.week
        agenda_names = {name for _when, name in data["items"]}
        assert "daily-report" in agenda_names
        assert "heartbeat" not in agenda_names
        assert any(name == "heartbeat" for name, _n, _c in data["frequent"])
        # the LW job's fire (if this 7-day window holds one) matches the
        # engine's own answer exactly
        from cronstable.cronexpr import CronTab
        import datetime as _dt

        start = data["start"]
        probe = start - _dt.timedelta(seconds=1)
        expected = [
            when
            for when in __import__("itertools").islice(
                CronTab("30 1 LW * *").occurrences(probe), 3
            )
            if when < start + _dt.timedelta(days=7)
        ]
        got = [when for when, name in data["items"] if name == "monthly-close"]
        assert got == expected
        h.keys.send("esc")
        await h.settle()
        assert not app.is_open("week")
    finally:
        await h.stop()


# ===================================================================
#  paint-path performance plumbing: the ANSI memo + scan gating
# ===================================================================
def _bare_app(tmp_path) -> TuiApp:
    """An app instance with no daemon and no run loop, for driving the
    pure render helpers directly."""
    prefs = dict(tui.PREF_DEFAULTS)
    return TuiApp(
        Api("http://127.0.0.1:1", None),
        HeadlessTerm(110, 32),
        ScriptedKeys(),
        prefs,
        boot=False,
        prefs_file=str(tmp_path / "prefs.json"),
    )


def _stub_tail(app: TuiApp, lines) -> "tui.LogTail":
    tail = tui.LogTail(app.api, "/jobs/x/logs", "x", app.mark)
    tail.lines = list(lines)
    return tail


async def test_ansi_memo_repaint_is_identical_and_regex_free(
    tmp_path, monkeypatch
):
    """A repaint of an unchanged buffer must render the same rows from
    the memo without re-running rewrite_sgr on any line."""
    app = _bare_app(tmp_path)
    paint = tui.Painter(app.theme)
    lines = [("stdout", "plain %03d" % i, 100.0 + i) for i in range(30)]
    lines.append(("stderr", "\x1b[31mred alert\x1b[0m", 200.0))
    app.log_tail = _stub_tail(app, lines)
    calls: List[str] = []
    real = tui.rewrite_sgr
    monkeypatch.setattr(
        tui,
        "rewrite_sgr",
        lambda line, theme: calls.append(line) or real(line, theme),
    )
    first = app._drawer_logs(paint, 100, 20)
    warm = len(calls)
    assert warm > 0
    assert "red alert" in strip_ansi("\n".join(first))
    second = app._drawer_logs(paint, 100, 20)
    assert second == first
    assert len(calls) == warm  # every visible line was a cache hit
    # the memo hands back the same entry object on a hit
    assert app._ansi_line("plain 005") is app._ansi_line("plain 005")
    # a theme change invalidates the memo: entries carry theme ink
    app.prefs["light"] = not app.prefs["light"]
    app._retheme()
    assert app._ansi_cache == {}
    app._drawer_logs(tui.Painter(app.theme), 100, 20)
    assert len(calls) > warm


async def test_ansi_memo_is_bounded(tmp_path):
    app = _bare_app(tmp_path)
    app.ANSI_CACHE_MAX = 8
    for i in range(50):
        app._ansi_line("line %d" % i)
        assert len(app._ansi_cache) <= 8
    # still serves correct transforms after a clear-and-refill
    assert app._ansi_line("\x1b[31mx\x1b[0m")[1] == "x"


async def test_drawer_log_slice_matches_the_full_walk(tmp_path):
    """The unwrapped drawer clamps its scroll window from counts and
    transforms only the visible slice.  With every line narrower than
    the pane, the wrap path (which still walks and renders the whole
    buffer) must paint the identical rows at every scroll position."""
    app = _bare_app(tmp_path)
    paint = tui.Painter(app.theme)
    lines = [
        ("stderr" if i % 7 == 0 else "stdout", "row %03d" % i, 50.0 + i)
        for i in range(40)
    ]
    lines.insert(20, ("meta", "end of run output", 70.5))
    tail = _stub_tail(app, lines)
    tail.error = "stream lost"
    app.log_tail = tail
    app.inputs["logsearch"] = "row 03"
    for scroll in (0, 3, 17, 999):
        app.wrap = False
        app.log_scroll = scroll
        sliced = app._drawer_logs(paint, 90, 12)
        app.wrap = True
        app.log_scroll = scroll
        walked = app._drawer_logs(paint, 90, 12)
        assert sliced == walked


async def test_log_search_recompute_skips_unchanged_inputs(tmp_path):
    app = _bare_app(tmp_path)
    app.log_tail = _stub_tail(
        app,
        [
            ("stdout", "alpha error", 1.0),
            ("stdout", "quiet", 2.0),
            ("stdout", "beta error", 3.0),
        ],
    )
    app.inputs["logsearch"] = "error"
    app._log_search_recompute(reset=True)
    assert app.log_matches == [0, 2]
    # unchanged needle + buffer: the per-paint call must not rescan
    app.log_matches = [999]
    app._log_search_recompute()
    assert app.log_matches == [999]
    # reset (the needle-edit path) always forces the rescan
    app._log_search_recompute(reset=True)
    assert app.log_matches == [0, 2]
    # an append at a constant line count (the MAX_LINES trim case)
    # still re-arms the scan
    tail = app.log_tail
    tail.lines.append(("stdout", "gamma error", 4.0))
    del tail.lines[0]
    app._log_search_recompute()
    assert app.log_matches == [1, 2]
    # n/N navigation state survives further appends: the cursor stays
    # on the same match while the list grows
    app.log_match_idx = 1  # on "gamma error" (buffer index 2)
    tail.lines.append(("stderr", "delta error", 5.0))
    app._log_search_recompute()
    assert app.log_matches == [1, 2, 3]
    assert app.log_match_idx == 1


async def test_render_tail_window_matches_the_full_merge(tmp_path):
    """The console merges only the last (visible + scroll) entries of
    each tail; the painted window must equal a naive sort of all lines
    from all tails, including timestamp ties across tails."""
    app = _bare_app(tmp_path)
    specs = (
        ("aa", 0.0, 3.0, 40),
        ("bb", 1.0, 3.0, 25),
        ("cc", 0.0, 3.0, 40),  # ties with aa on every stamp
        ("dd", 2.0, 100.0, 2),  # short tail, one ancient entry
    )
    for name, base, step, count in specs:
        tail = tui.LogTail(app.api, "/jobs/%s/logs" % name, name, app.mark)
        tail.lines = [
            ("stdout", "%s line %03d" % (name, i), base + i * step)
            for i in range(count)
        ]
        app.tails.append(tail)
    paint = tui.Painter(app.theme)

    def naive(scroll: int, lines: int) -> List[str]:
        merged = []
        for idx, tail in enumerate(app.tails):
            for _stream, line, when in tail.lines:
                merged.append((when, idx, line))
        merged.sort(key=lambda item: item[0])
        available = max(3, lines - 10)
        scroll = min(scroll, max(0, len(merged) - available))
        end = len(merged) - scroll
        return [line for _, _, line in merged[max(0, end - available) : end]]

    for scroll in (0, 5, 37, 10_000):
        app.panel_scroll = scroll
        body = [strip_ansi(row) for row in app.render_tail(paint, 110, 24)]
        expect = naive(scroll, 24)
        joined = "\n".join(body)
        # every expected line appears, in order, and nothing else does
        pos = -1
        for line in expect:
            pos = joined.index(line, pos + 1)
        assert sum(" line " in row for row in body) == len(expect)


async def test_dag_logs_tab_transforms_only_the_visible_slice(tmp_path):
    app = _bare_app(tmp_path)
    lines = [("stdout", "row %02d" % i, float(i)) for i in range(20)]
    lines.append(("meta", "done", 20.0))
    tail = tui.LogTail(app.api, "/dag/t/logs", "task", app.mark)
    tail.lines = lines
    app.dag_task_tail = tail
    paint = tui.Painter(app.theme)
    app.panel_scroll = 0
    rows = [strip_ansi(r) for r in app._dag_logs_tab(paint, 80, 6)]
    assert rows == [
        " task: task",
        " ▏row 16",
        " ▏row 17",
        " ▏row 18",
        " ▏row 19",
        "  ── end of log ──",
    ]
    app.panel_scroll = 3
    rows = [strip_ansi(r) for r in app._dag_logs_tab(paint, 80, 6)]
    assert rows[1:] == [" ▏row %d" % n for n in (13, 14, 15, 16, 17)]
    # the error suffix rides at the very end of the scrollback
    tail.error = "boom"
    app.panel_scroll = 0
    rows = [strip_ansi(r) for r in app._dag_logs_tab(paint, 80, 6)]
    assert rows[-1] == "  ⚠ boom"
    assert rows[-2] == "  ── end of log ──"
    # over-scroll clamps to the oldest rows
    app.panel_scroll = 999
    rows = [strip_ansi(r) for r in app._dag_logs_tab(paint, 80, 6)]
    assert rows[1:] == [" ▏row %02d" % n for n in range(5)]


# ===================================================================
#  pause / SLA surfaces: table layout, drawer text, web parity
# ===================================================================
def test_pause_and_overdue_widths_never_cost_the_command_column(tmp_path):
    """The OVERDUE badge and the "til HH:MM" pause cell are fleet-wide
    column widths driven by a single job's transient state. They are
    bonuses paid out of leftover slack: at a narrow terminal one paused
    job must not shed the whole command column for the other 39."""
    app = _bare_app(tmp_path)
    app.jobs = [_job("job-%02d" % i, outcome="success") for i in range(40)]
    plain = app._columns(80)
    assert "cmd" in [c for c, _ in plain]
    app.jobs[3]["paused"] = _job("x", paused=True)["paused"]
    app.jobs[4].update(_job("y", late=True))
    mixed = app._columns(80)
    assert [c for c, _ in mixed] == [c for c, _ in plain]
    assert dict(mixed)["cmd"] >= 20
    # with room to spare the bonuses are actually paid out
    wide = dict(app._columns(160))
    assert wide["status"] == 19 and wide["next"] == 12
    assert dict(_bare_app(tmp_path)._columns(160))["status"] == 11


def test_drawer_pause_note_cannot_smuggle_control_chars(tmp_path):
    """``note``/``by`` are operator free text that the daemon stores and
    serves verbatim; a bare CR in the drawer line would yank the cursor
    back to column 1 and let the note overwrite cronstable's own row."""
    job = _job(
        "nightly",
        paused={
            "since": "2026-07-20T00:00:00+00:00",
            "until": "2026-07-20T01:00:00+00:00",
            "note": "ok\rPAUSED BY SECURITY\b\b",
            "by": "alice\nbob\x1b[31m",
        },
    )
    app = _bare_app(tmp_path)
    app.jobs = [job]
    app.by_name = {"nightly": job}
    app.drawer_job = "nightly"
    blob = "".join(app.render_drawer_panel(tui.Painter(app.theme), 60, 24))
    for ch in ("\r", "\n", "\b"):
        assert ch not in blob, repr(blob)
    plain = strip_ansi(blob)
    assert "ok PAUSED BY SECURITY" in plain
    assert "by alice bob" in plain


def _web_page():
    """The shipped dashboard's source. ``docs/demo/index.html`` needs no
    separate pass: test_web_demo_mirror pins it to this file byte for
    byte outside its fake-backend block."""
    import pathlib

    page = pathlib.Path(tui.__file__).parent / "web" / "index.html"
    return str(page), page.read_text(encoding="utf-8")


def test_web_outcome_mapping_has_exactly_one_home():
    """A run outcome maps to its state class in ``outcomeCls`` and nowhere
    else. Hand-rolled copies drift: two of them missed the ``skipped`` arm
    and painted a pause-held fire as a green OK."""
    import re

    path, html = _web_page()
    mapper = re.search(r"const outcomeCls = \(o\) =>(.*)", html)
    assert mapper, path
    arms = set(re.findall(r'o === "(\w+)"', mapper.group(1)))
    assert arms == {"failure", "cancelled", "unknown", "skipped"}, path
    # any OTHER expression enumerating outcomes is a divergent copy
    for num, line in enumerate(html.split("\n"), 1):
        if "outcomeCls" in line:
            continue
        hits = len(re.findall(r'outcome === "\w+"', line))
        where = "%s:%d" % (path, num)
        assert hits < 2, "%s outcome ladder outside outcomeCls" % where


def test_web_styles_every_class_outcome_cls_can_return():
    """Each consumer of ``outcomeCls`` needs a colour rule per class, or
    a state renders in the default ink and reads as an ordinary one."""
    path, html = _web_page()
    for cls in ("ok", "fail", "cancelled", "unknown", "skipped"):
        assert "#fleetPanel .cell.%s {" % cls in html, (path, cls)
        assert ".tlrow.%s .tlg {" % cls in html, (path, cls)


def test_tui_outcome_mapping_has_exactly_one_home():
    """The terminal dashboard's twin of the web guard above. Five panels
    each carried their own copy of the outcome ladder and every one of
    them had dropped the ``skipped`` arm, so a slot a pause held back
    painted in the green a real success earns. One mapper, no copies."""
    import pathlib
    import re

    path = pathlib.Path(tui.__file__)
    source = path.read_text(encoding="utf-8")
    assert set(tui.OUTCOME_KEY) == {
        "failure",
        "cancelled",
        "unknown",
        "skipped",
    }
    # every key the mapper can return needs a colour, or a state paints
    # in the default ink and reads as an ordinary one
    for key in set(tui.OUTCOME_KEY.values()) | {"ok"}:
        assert key in tui.OUTCOME_COLOR, key
        assert key in tui.GLYPH, key
        assert key in tui.GLYPH_ASCII, key
    # ``health`` maps an outcome onto a ROW status (its own labels, and
    # the web's health() likewise); every other enumeration of outcomes
    # is a divergent copy of outcome_key
    lines = source.split("\n")
    start = next(
        i for i, ln in enumerate(lines) if ln.startswith("def health")
    )
    end = next(
        i for i, ln in enumerate(lines[start + 1 :], start + 1) if ln == ""
    )
    for num, line in enumerate(lines, 1):
        if start < num <= end + 1:
            continue
        where = "%s:%d" % (path, num)
        assert '.get(outcome, "ok")' not in line, (
            "%s outcome ladder outside outcome_key" % where
        )
        hits = len(re.findall(r'outcome == "\w+"', line))
        assert hits < 2, "%s outcome ladder outside outcome_key" % where


def test_tui_paints_a_pause_held_slot_as_skipped_not_ok():
    """#28 residual: the web fix routed both of its outcome ladders
    through ``outcomeCls``, but the TUI's five copies were untouched, so
    a paused job's held slots still painted success-green in the
    terminal. Every panel that paints a run is checked here."""
    assert tui.outcome_key("skipped") == "skipped"
    assert tui.outcome_color("skipped") != tui.outcome_color("success")

    # 1. sparkline (job rows and wallboard tiles)
    cells = spark_cells(
        [
            {"outcome": "success", "duration": 1.0},
            {"outcome": "skipped", "duration": 1.0},
        ]
    )
    assert cells[0][1] == "ok"
    assert cells[1][1] != "ok"


async def test_tui_panels_paint_a_pause_held_slot_as_skipped(tmp_path):
    """The rendering half of the check above: timeline, fleet matrix,
    heatmap and the drawer's run history must not emit the theme's ok
    ink for a ``skipped`` row."""
    app = _bare_app(tmp_path)
    paint = tui.Painter(app.theme)
    ok_ink = app.theme.fg("ok")
    when = "2020-01-01T10:00:00+00:00"

    def ok_spans(rows: List[str], needle: str) -> List[str]:
        return [r for r in rows if needle in strip_ansi(r) and ok_ink in r]

    # 2. incident timeline
    app.jobs = [
        {
            "name": "held",
            "enabled": True,
            "last_run": {"outcome": "skipped", "finished_at": when},
        },
        {
            "name": "real",
            "enabled": True,
            "last_run": {"outcome": "success", "finished_at": when},
        },
    ]
    rows = app.render_timeline(paint, 110, 24)
    assert ok_spans(rows, "real"), "a real success still paints ok"
    assert not ok_spans(rows, "held")

    # 3. fleet matrix: the cell reads "skip", not a truncated "skippe"
    app.fleet = {
        "enabled": True,
        "nodes": [
            {
                "name": "n1",
                "jobs": {
                    "held": {
                        "last": {"outcome": "skipped", "finished_at": when}
                    },
                    "real": {
                        "last": {"outcome": "success", "finished_at": when}
                    },
                },
            }
        ],
    }
    rows = app.render_fleet(paint, 110, 24)
    assert ok_spans(rows, "real")
    assert not ok_spans(rows, "held")
    assert "skip " in strip_ansi("".join(rows))
    assert "skippe" not in strip_ansi("".join(rows))

    # 4. activity heatmap: an hour of nothing but pause holds is not green,
    # but one real success in that same hour still outranks the holds
    now = time.time()
    stamp = (
        datetime.datetime.fromtimestamp(now - 60, tz=datetime.timezone.utc)
    ).isoformat()
    app.heat_data = {
        "held": [{"outcome": "skipped", "finished_at": stamp}],
        "mixed": [
            {"outcome": "skipped", "finished_at": stamp},
            {"outcome": "success", "finished_at": stamp},
        ],
    }
    rows = app.render_heat(paint, 110, 24)
    assert not ok_spans(rows, "held")
    assert ok_spans(rows, "mixed"), "a real success outranks the holds"

    # 5. drawer run history / duration bars
    app.drawer_runs = {
        "stats": {"total": 2},
        "runs": [
            {"outcome": "skipped", "started_at": when, "duration": 1.0},
            {"outcome": "success", "started_at": when, "duration": 1.0},
        ],
    }
    rows = app._drawer_history(paint, 80, 24)
    painted = [r for r in rows if ok_ink in r]
    assert len(painted) == 1, "only the real success paints ok"


def test_web_drawer_shows_the_sla_state_the_wiki_promises():
    """``wiki/Web-Dashboard.md`` sends operators to the drawer to read an
    overdue job's breached checks, so the drawer has to render them."""
    import pathlib
    import re

    path, html = _web_page()
    body = re.search(r"function renderDrawerMeta\(\)(.*?)\n  \}\n", html, re.S)
    assert body, path
    drawer = body.group(1)
    assert 'job.sla.state === "late"' in drawer, path
    assert "OVERDUE" in drawer, path
    assert ".breaches" in drawer, path
    assert ".drawer-head .meta .overdue" in html, path
    wiki = (
        pathlib.Path(tui.__file__).parent.parent / "wiki" / "Web-Dashboard.md"
    ).read_text(encoding="utf-8")
    assert "**OVERDUE** badge on its row, drawer, and wallboard tile" in wiki
