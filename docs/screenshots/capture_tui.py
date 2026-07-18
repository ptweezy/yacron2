"""Capture cronstable TUI screenshots off the running grand-tour fleet.

Usage: python capture_tui.py [shot ...]
With no args, captures every shot. Shots land in ./shots/ as tui-*.png.

The TUI is driven for real: a headless :class:`cronstable.tui.TuiApp`
(HeadlessTerm + ScriptedKeys standing in for the tty) runs against
meridian-a of the grand tour, the same fleet the web dashboard shots
use, staged the same way (the deliberate red + CPU burner for the hero,
DAG runs including parking release-train on its approval gate, and the
correlated db-health incident saved for last).  Captured ANSI frames
are then rasterized to PNG through Playwright's Chromium at
deviceScaleFactor 2, in a terminal-window card styled after Windows
Terminal, with Cascadia Mono.

Like the web capture, the fleet should have 10-15 minutes of uptime
first so sparklines and history have filled in.
"""

import asyncio
import html
import json
import re
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cronstable.tui import (  # noqa: E402
    PREF_DEFAULTS,
    Api,
    HeadlessTerm,
    ScriptedKeys,
    TuiApp,
    health,
    strip_ansi,
)

BASE = "http://localhost:8080"
OUT = Path(__file__).parent / "shots"
OUT.mkdir(exist_ok=True)
ONLY = set(sys.argv[1:])

#: clean release-style version for the header chip (matches the web set)
VERSION = "1.2.14"

COLS, LINES = 150, 38
FRAMES: dict = {}
results: dict = {}


def wants(name: str) -> bool:
    return not ONLY or name in ONLY


def api(method: str, path: str, body=None):
    req = urllib.request.Request(BASE + path, method=method)
    data = None
    if body is not None:
        req.add_header("Content-Type", "application/json")
        data = json.dumps(body).encode()
    try:
        with urllib.request.urlopen(req, data=data, timeout=10) as r:
            raw = r.read().decode()
            try:
                return json.loads(raw)
            except Exception:
                return raw
    except Exception as e:
        print(f"    api {method} {path}: {e}")
        return None


# ===================================================================
#  driving the app
# ===================================================================
async def wait_for(pred, timeout=30.0, what=""):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if pred():
                return True
        except Exception:
            pass
        await asyncio.sleep(0.05)
    print(f"    wait timed out: {what}")
    return False


async def snap(app, term, name):
    """One clean frame: no toasts, freshly painted after our changes."""
    app.toasts = []
    app.version = VERSION
    app.mark()
    count = len(term.frames)
    await wait_for(lambda: len(term.frames) > count, 10, "paint " + name)
    FRAMES[name] = list(term.frames[-1])
    results[name] = "ok"
    print(f"  [shot] {name}")


def reset(app):
    """Back to a clean board between stages (Esc-all, filters off)."""
    while app.open_overlays:
        app.close(app.open_overlays[-1])
    app.wallboard = False
    app.zen_on = False
    app.filter_text = ""
    app.inputs["filter"] = ""
    app.inputs["logsearch"] = ""
    app.focus = None
    app.timestamps = False
    app.recompute_view()
    app.mark()


async def capture_boot(prefs_file):
    if not wants("tui-boot"):
        return
    prefs = dict(PREF_DEFAULTS)
    prefs["poll_ms"] = 1000
    keys = ScriptedKeys()
    term = HeadlessTerm(COLS, LINES)
    app = TuiApp(
        Api(BASE, None),
        term,
        keys,
        prefs,
        boot=True,
        prefs_file=prefs_file,
    )
    task = asyncio.get_running_loop().create_task(app.run())
    await wait_for(lambda: app.booting, 15, "boot starts")
    await wait_for(lambda: not app.booting, 45, "boot finishes")
    # the last frame still showing the POST is the finished self-test;
    # its verdict line varies with fleet health (ALL CHECKS PASSED /
    # N JOBS FAILING / DEGRADED), so accept any of them
    for frame in reversed(term.frames):
        text = "\n".join(strip_ansi(r) for r in frame)
        if "POWER-ON SELF-TEST" in text and any(
            marker in text for marker in ("CHECKS", "FAILING", "DEGRADED")
        ):
            FRAMES["tui-boot"] = list(frame)
            results["tui-boot"] = "ok"
            print("  [shot] tui-boot")
            break
    else:
        results["tui-boot"] = "FAIL no boot frame"
    app.quit = True
    keys.send("q")
    try:
        await asyncio.wait_for(task, 10)
    except asyncio.TimeoutError:
        task.cancel()


async def capture_all():  # noqa: C901 - one linear staging walk
    prefs_file = str(OUT / "tui-capture-prefs.json")
    await capture_boot(prefs_file)

    prefs = dict(PREF_DEFAULTS)
    prefs["poll_ms"] = 1000
    prefs["boot"] = False
    keys = ScriptedKeys()
    term = HeadlessTerm(COLS, LINES)
    app = TuiApp(
        Api(BASE, None),
        term,
        keys,
        prefs,
        boot=False,
        prefs_file=prefs_file,
    )
    task = asyncio.get_running_loop().create_task(app.run())
    await wait_for(lambda: len(app.jobs) >= 40, 60, "fleet jobs load")
    app.version = VERSION

    # ---- hero: one deliberate red + a guaranteed cpu-burner ----------
    api("POST", "/jobs/alert-selftest/start")
    api("POST", "/jobs/risk-model-recompute/start")
    await asyncio.sleep(7)  # let a poll land, like the web capture
    app.sort_key = "status"
    app.recompute_view()
    if wants("tui-overview"):
        reset(app)
        app.sort_key = "status"
        app.recompute_view()
        await snap(app, term, "tui-overview")

    # ---- theme variants on the same board ----------------------------
    for hue, light, fname in [
        ("amber", False, "tui-theme-amber"),
        ("green", False, "tui-theme-green"),
        ("modern", False, "tui-theme-modern"),
        ("carolina", True, "tui-theme-carolina-light"),
    ]:
        if not wants(fname):
            continue
        app.prefs["theme"], app.prefs["light"] = hue, light
        app._retheme()
        await snap(app, term, fname)
    app.prefs["theme"], app.prefs["light"] = "carolina", False
    app._retheme()

    # ---- job drawer: live logs on the 5s heartbeat probe -------------
    if wants("tui-logs"):
        reset(app)
        app.open_drawer("pulse-liveness", "logs")
        app.timestamps = True
        await wait_for(
            lambda: app.log_tail is not None and len(app.log_tail.lines) > 3,
            30,
            "probe log lines",
        )
        await asyncio.sleep(10)  # accumulate a few probe runs
        app.inputs["logsearch"] = "UP"
        app._log_search_recompute()
        await snap(app, term, "tui-logs")
        reset(app)

    # ---- history + per-run cpu/peak-mem (monitorResources) -----------
    if wants("tui-history"):
        app.open_drawer("risk-model-recompute", "history")
        await wait_for(lambda: app.drawer_runs is not None, 15, "history")
        await snap(app, term, "tui-history")
        reset(app)

    # ---- schedule tab on a timezone job ------------------------------
    if wants("tui-schedule"):
        app.open_drawer("finance-eod-close", "schedule")
        await asyncio.sleep(0.8)
        await snap(app, term, "tui-schedule")
        reset(app)

    # ---- command palette ---------------------------------------------
    if wants("tui-palette"):
        await app.handle_key("ctrl+k")
        for ch in "run":
            await app.handle_key(ch)
        await snap(app, term, "tui-palette")
        reset(app)

    # ---- shortcut overlay --------------------------------------------
    if wants("tui-shortcuts"):
        await app.handle_key("?")
        await snap(app, term, "tui-shortcuts")
        reset(app)

    # ---- settings ----------------------------------------------------
    if wants("tui-settings"):
        app.open("settings")
        await snap(app, term, "tui-settings")
        reset(app)

    # ---- DAG index panel ---------------------------------------------
    if wants("tui-dags"):
        app._toggle_dags()
        await wait_for(lambda: app.dags, 15, "dags load")
        await snap(app, term, "tui-dags")
        reset(app)

    # ---- DAG run: trigger the diamond, catch the graph mid-flight ----
    if wants("tui-dag-graph"):
        r = api("POST", "/dags/data-quality-gate/trigger")
        print(f"    data-quality-gate trigger -> {r}")
        await asyncio.sleep(3.5)
        app.open_dag("data-quality-gate")
        await wait_for(lambda: app.dag_runs, 15, "dag runs")
        await app.handle_key("enter")  # newest run -> tasks tab
        await wait_for(lambda: app.dag_run is not None, 15, "run doc")
        app.dag_tab = "graph"
        await snap(app, term, "tui-dag-graph")
        reset(app)

    # ---- DAG approval gate: release-train waits on a human -----------
    if wants("tui-dag-approval"):
        r = api("POST", "/dags/release-train/trigger")
        print(f"    release-train trigger -> {r}")
        run_key = r.get("runKey") if isinstance(r, dict) else None
        for _ in range(45):
            await asyncio.sleep(2)
            if not run_key:
                break
            doc = api("GET", f"/dags/release-train/runs/{run_key}")
            tasks = (doc or {}).get("tasks") or {}
            vals = tasks.values() if isinstance(tasks, dict) else tasks
            if any(
                isinstance(t, dict) and t.get("awaitingApproval") for t in vals
            ):
                break
        app.open_dag("release-train")
        await wait_for(lambda: app.dag_runs, 15, "release runs")
        await app.handle_key("enter")
        await wait_for(lambda: app.dag_run is not None, 15, "release doc")
        app.dag_tab = "tasks"
        await snap(app, term, "tui-dag-approval")
        reset(app)

    # ---- cluster panel (9 peers) -------------------------------------
    if wants("tui-cluster"):
        app._toggle("cluster")
        await wait_for(
            lambda: (app.cluster or {}).get("enabled"), 15, "cluster"
        )
        await snap(app, term, "tui-cluster")
        reset(app)

    # ---- fleet matrix (jobs x nodes) ---------------------------------
    if wants("tui-fleet"):
        app._toggle("fleet")
        await wait_for(
            lambda: len((app.fleet or {}).get("nodes") or []) >= 9,
            30,
            "fleet nodes",
        )
        await snap(app, term, "tui-fleet")
        reset(app)

    # ---- durable-state inspector -------------------------------------
    if wants("tui-state"):
        app._toggle("state")
        await wait_for(
            lambda: (app.state_data or {}).get("enabled"), 20, "state"
        )
        await snap(app, term, "tui-state")
        reset(app)

    # ---- multi-tail console ------------------------------------------
    if wants("tui-multitail"):
        app.open_tail(["pulse-liveness", "pulse-latency"])
        failing = [j["name"] for j in app.jobs if health(j)[0] == "fail"]
        for name in failing[:2]:
            app.add_tail(name)
        await asyncio.sleep(20)  # accumulate merged lines
        app.timestamps = True
        await snap(app, term, "tui-multitail")
        reset(app)

    # ---- heatmap ------------------------------------------------------
    if wants("tui-heatmap"):
        app._toggle("heat")
        await wait_for(lambda: app.heat_data, 60, "heatmap batch")
        await snap(app, term, "tui-heatmap")
        reset(app)

    # ---- LAST: the correlated db-health incident ---------------------
    if (
        wants("tui-incident")
        or wants("tui-incident-timeline")
        or wants("tui-wallboard")
    ):
        # the staged db-health jobs only FAIL while the UTC minute is
        # 15-19 (a simulated outage window; see platform.yaml). Wait
        # for the window, nudge all four to run right away, then wait
        # for the ×4 CORRELATED verdict -- not merely a crit one, which
        # the other staged failures already keep lit.
        now = time.time()
        minute = int((now // 60) % 60)
        if not (15 <= minute <= 19):
            wait_s = ((15 - minute) % 60) * 60 - (now % 60) + 5
            print(f"    waiting {wait_s:.0f}s for the :15-:19 window")
            await asyncio.sleep(wait_s)
        for j in (
            "db-health-orders",
            "db-health-inventory",
            "db-health-payments",
            "db-health-warehouse",
        ):
            api("POST", f"/jobs/{j}/start")
        await wait_for(
            lambda: (
                app.verdict is not None
                and "share exit=" in app.verdict.get("sub", "")
            ),
            40,
            "correlated verdict",
        )
        await asyncio.sleep(2)
        if wants("tui-incident"):
            reset(app)
            app.sort_key = "status"
            app.recompute_view()
            await snap(app, term, "tui-incident")
        if wants("tui-incident-timeline"):
            await app.handle_key("i")
            await snap(app, term, "tui-incident-timeline")
            reset(app)
        if wants("tui-wallboard"):
            app.set_wallboard(True)
            await asyncio.sleep(1.5)
            await snap(app, term, "tui-wallboard")
            app.set_wallboard(False)

    app.quit = True
    keys.send("q")
    try:
        await asyncio.wait_for(task, 10)
    except asyncio.TimeoutError:
        task.cancel()


# ===================================================================
#  rasterizing ANSI frames -> PNG
# ===================================================================
SGR = re.compile(r"\x1b\[([0-9;]*)m")
ANSI_ANY = re.compile(
    r"\x1b(?:\[[0-9;:?]*[ -/]*[@-~]|\][^\x07\x1b]*(?:\x07|\x1b\\)?)"
)
DEF_FG = "#9ed3f5"

#: per-theme page/window chrome behind the frame (theme -> bg of frame)
_THEME_BG = {
    "tui-theme-amber": "#160d02",
    "tui-theme-green": "#03130a",
    "tui-theme-modern": "#101418",
    "tui-theme-carolina-light": "#eef4f9",
}
_THEME_FG = {
    "tui-theme-amber": "#f5c169",
    "tui-theme-green": "#7ee2a1",
    "tui-theme-modern": "#d7dde3",
    "tui-theme-carolina-light": "#173751",
}


def row_to_html(row, def_fg):
    out = []
    fg, bg, bold, dim, rev = def_fg, None, False, False, False
    pos = 0

    def emit(text):
        if not text:
            return
        f, b = (fg, bg)
        if rev:
            f, b = (b or "#000"), fg
        style = [f"color:{f}"]
        if b:
            style.append(f"background:{b}")
        if bold:
            style.append("font-weight:700")
        if dim:
            style.append("opacity:.62")
        out.append(
            f'<span style="{";".join(style)}">{html.escape(text)}</span>'
        )

    for match in ANSI_ANY.finditer(row):
        emit(row[pos : match.start()])
        pos = match.end()
        sgr = SGR.fullmatch(match.group(0))
        if not sgr:
            continue
        parts = (sgr.group(1) or "0").split(";")
        i = 0
        while i < len(parts):
            code = int(parts[i] or "0")
            if code == 0:
                fg, bg, bold, dim, rev = def_fg, None, False, False, False
            elif code == 1:
                bold = True
            elif code == 2:
                dim = True
            elif code == 7:
                rev = True
            elif code == 22:
                bold = dim = False
            elif code == 27:
                rev = False
            elif code == 38 and i + 1 < len(parts) and parts[i + 1] == "2":
                fg = "#%02x%02x%02x" % (
                    int(parts[i + 2]),
                    int(parts[i + 3]),
                    int(parts[i + 4]),
                )
                i += 4
            elif code == 48 and i + 1 < len(parts) and parts[i + 1] == "2":
                bg = "#%02x%02x%02x" % (
                    int(parts[i + 2]),
                    int(parts[i + 3]),
                    int(parts[i + 4]),
                )
                i += 4
            i += 1
    emit(row[pos:])
    return "".join(out)


def frame_html(name, rows):
    term_bg = _THEME_BG.get(name, "#06131d")
    def_fg = _THEME_FG.get(name, DEF_FG)
    body = "\n".join(row_to_html(r.rstrip(), def_fg) or "&nbsp;" for r in rows)
    bar_bg = "#0a1a28" if name not in _THEME_BG else term_bg
    return f"""<!doctype html><html><head><meta charset="utf-8"><style>
html,body {{ margin:0; padding:24px; background:#101418; }}
.term {{
  display:inline-block; background:{term_bg};
  border:1px solid rgba(128,148,170,.28); border-radius:10px;
  overflow:hidden; box-shadow:0 16px 44px rgba(0,0,0,.5);
}}
.bar {{
  display:flex; align-items:center; gap:8px; padding:9px 14px;
  background:{bar_bg}; border-bottom:1px solid rgba(128,148,170,.22);
}}
.dot {{ width:11px; height:11px; border-radius:50%;
  background:rgba(128,148,170,.35); }}
.title {{ margin-left:8px; font:500 12px/1 "Cascadia Mono", Consolas,
  monospace; color:rgba(128,148,170,.85); }}
pre {{
  margin:0; padding:10px 14px;
  font:13px/1.32 "Cascadia Mono", Consolas, monospace;
  font-variant-numeric: tabular-nums;
}}
</style></head><body>
<div class="term"><div class="bar"><span class="dot"></span>
<span class="dot"></span><span class="dot"></span>
<span class="title">cronstable tui</span></div>
<pre>{body}</pre></div>
</body></html>"""


def render_pngs():
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_context(device_scale_factor=2).new_page()
        for name, rows in FRAMES.items():
            page.set_content(frame_html(name, rows))
            page.wait_for_timeout(120)
            card = page.locator(".term")
            card.screenshot(path=str(OUT / f"{name}.png"))
            print(f"  [png] {name}.png")
        browser.close()


def main():
    asyncio.run(capture_all())
    if FRAMES:
        render_pngs()
    print("\n== capture summary ==")
    for k, v in results.items():
        print(f"  {k}: {v}")
    fails = [k for k, v in results.items() if v != "ok"]
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
