"""Tests for the terminal dashboard (cronstable.tui).

Two layers, mirroring the module's own split:

* pure-logic tests for the ports of the web dashboard's client-side
  brain (health/verdict/fuzzy/describeCron/formatting) and for the
  terminal plumbing (key decoding, ANSI measurement, themes, prefs) --
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
    fuzzy,
    health,
    load_prefs,
    next_fires,
    pad_to,
    rewrite_sgr,
    save_prefs,
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
    return {
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
    }


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
    assert [j["name"] for j in compute_view(jobs, "DELT", "all",
                                            "name", 1)] == ["charlie"]
    # status segments; "off" = disabled
    assert [j["name"] for j in compute_view(jobs, "", "fail",
                                            "name", 1)] == ["alpha"]
    # status sort: run < fail < ... ; ties break on name
    by_status = compute_view(jobs, "", "all", "status", 1)
    assert [j["name"] for j in by_status] == ["bravo", "alpha", "charlie"]
    # duration sort puts the longest run first
    by_dur = compute_view(jobs, "", "all", "duration", 1)
    assert by_dur[0]["name"] == "alpha"
    # direction flip reverses
    assert compute_view(jobs, "", "all", "name", -1)[0]["name"] == (
        "charlie"
    )


def test_verdict_single_failure_is_a_warn():
    jobs = [
        _job("ok-1", outcome="success"),
        _job("bad-1", outcome="failure", exit_code=69,
             fail_reason="boom"),
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
    alert = {"bad": True, "reason": "no quorum — Leader jobs paused",
             "node": "n1"}
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
    assert describe_cron("0 12 1 * *") == (
        "At 12:00, on the 1st of the month"
    )
    # dom + dow combine with OR, like standard cron
    text = describe_cron("0 0 1 * 1")
    assert "on Monday or on the 1st" in text
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
    assert theme.fg("fail") in out           # 31 -> the theme's fail ink
    assert "\x1b[31m" not in out             # raw palette gone
    # OSC and other non-SGR escapes are stripped entirely
    assert strip_ansi(rewrite_sgr("\x1b]0;title\x07text", theme)) == "text"
    # 256-color foregrounds collapse to the bright ink, not garbage
    out = rewrite_sgr("\x1b[38;5;196mX", theme)
    assert strip_ansi(out) == "X"


def test_sanitize_log_line_defuses_control_chars():
    from cronstable.tui import sanitize_log_line

    # cmd.exe CRLF tail: the stray \r must not reach a painted row
    assert sanitize_log_line("heartbeat ok\r") == "heartbeat ok"
    # mid-line \r gets log-viewer overwrite semantics (progress bars)
    assert sanitize_log_line("10%\r55%\r100%") == "100%"
    assert sanitize_log_line("busy\r") == "busy"
    assert sanitize_log_line("\ttabbed") == "    tabbed"
    assert sanitize_log_line("a\x00b\x07c") == "abc"
    # ESC survives so rewrite_sgr can re-ink colours
    assert "\x1b[31m" in sanitize_log_line("\x1b[31mred")


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
    web dashboard's shortcut table verbatim (from fillHelp)."""
    web_rows = [
        ("⌘K / Ctrl+K", "Command palette"),
        ("/", "Focus filter"),
        ("j / ↓", "Next job"),
        ("k / ↑", "Previous job"),
        ("Enter", "Open selected job"),
        ("r", "Run selected job"),
        ("x", "Cancel selected (running) job"),
        ("c", "Copy selected command"),
        ("g", "Refresh now"),
        ("t", "Cycle theme"),
        ("T", "Light / dark theme"),
        ("i", "Incident timeline"),
        ("w", "Wallboard (TV) mode"),
        ("a", "Acknowledge failure alarm"),
        ("?", "This help"),
        ("Esc", "Close panel / drawer"),
    ]
    assert tui.HELP_ROWS == web_rows


# ===================================================================
#  the app, headless against a fake daemon
# ===================================================================
class FakeDaemon:
    """A loopback daemon: the endpoints the TUI touches, scriptable."""

    def __init__(self) -> None:
        self.jobs: List[Dict[str, Any]] = []
        self.token: Optional[str] = None
        self.posts: List[str] = []
        self.log_lines: Dict[str, List[Dict[str, str]]] = {}
        self.runner: Optional[web.AppRunner] = None
        self.url = ""

    async def start(self) -> None:
        app = web.Application(middlewares=[self._auth])
        app.router.add_get("/version", self._version)
        app.router.add_get("/job-set-id", self._job_set_id)
        app.router.add_get("/jobs", self._jobs)
        app.router.add_get("/cluster", self._cluster)
        app.router.add_get("/node", self._node)
        app.router.add_get("/dags", self._dags)
        app.router.add_get("/jobs/{name}/runs", self._runs)
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
        return web.json_response({"enabled": False, "peers": []})

    async def _node(self, request):
        return web.json_response(
            {"node_name": "test-node", "resources": None}
        )

    async def _dags(self, request):
        return web.json_response([])

    async def _runs(self, request):
        name = request.match_info["name"]
        job = next((j for j in self.jobs if j["name"] == name), None)
        if job is None:
            return web.Response(status=404)
        runs = [job["last_run"]] if job.get("last_run") else []
        stats = {
            "total": len(runs),
            "success": sum(
                1 for r in runs if r["outcome"] == "success"
            ),
            "failure": sum(
                1 for r in runs if r["outcome"] == "failure"
            ),
            "cancelled": 0,
            "unknown": 0,
            "success_rate": 1.0 if runs else None,
            "avg_duration": 1.0,
            "min_duration": 1.0,
            "max_duration": 1.0,
            "last_duration": 1.0,
        }
        return web.json_response(
            {"name": name, "runs": runs, "stats": stats}
        )

    async def _logs(self, request):
        name = request.match_info["name"]
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
        self._task = asyncio.get_running_loop().create_task(
            self.app.run()
        )
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
        _job("north-beacon", outcome="failure", exit_code=69,
             fail_reason="exited with code 69"),
    ]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 2)
        await h.settle()
        screen = h.term.screen()
        assert "heartbeat" in screen
        assert "north-beacon" in screen
        assert "9.9-test" in screen           # version chip
        assert "2 jobs" in screen
        assert "JOB FAILING — north-beacon" in screen  # verdict bar
        assert "live" in screen               # connection dot
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
            lambda: app.log_tail is not None
            and len(app.log_tail.lines) == 2
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
        _job("north-beacon"), _job("south-beacon"), _job("pulse-check"),
    ]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 3)
        h.keys.send("/", "b", "e", "a", "c")
        await _wait_for(lambda: app.filter_text == "beac")
        assert [j["name"] for j in app.view] == [
            "north-beacon", "south-beacon",
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
            lambda: app.palette_matches()
            and app.palette_matches()[0][1] == "Run: deploy"
        )
        h.keys.send("enter")
        await _wait_for(lambda: "deploy/start" in h.daemon.posts)
        assert not app.is_open("palette")
    finally:
        await h.stop()


async def test_wallboard_and_stale_banner(tmp_path):
    h = Harness()
    h.daemon.jobs = [_job("tile-a", outcome="success"),
                     _job("tile-b", outcome="failure")]
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
        assert "ok-b" in screen      # every job's most recent finish
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
