"""Capture cronstable dashboard screenshots off the running grand-tour fleet.

Usage: python capture_dashboard.py [shot ...]
With no args, captures every shot. Shots are saved to ./shots/ at 2880x1800
(1440x900 viewport, deviceScaleFactor=2), matching the existing docs/img set.

Order matters: clean-board shots come first; deliberately-staged failures
(incident correlation) come last so they don't pollute earlier frames.
"""
import json
import sys
import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8080"
OUT = Path(__file__).parent / "shots"
OUT.mkdir(exist_ok=True)
ONLY = set(sys.argv[1:])

# clean release-style version for the header (the local build carries a long
# setuptools-scm dev string; a release install shows a clean one like this)
VERSION = "1.2.14"

results = {}


def api(method, path, body=None):
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


def wants(name):
    return not ONLY or name in ONLY


def shot(page, name):
    page.screenshot(path=str(OUT / f"{name}.png"))
    results[name] = "ok"
    print(f"  [shot] {name}")


def close_overlays(page):
    for _ in range(3):
        page.keyboard.press("Escape")
        page.wait_for_timeout(150)
    page.evaluate("window.scrollTo(0, 0)")
    page.wait_for_timeout(200)


def set_sort(page, order="status"):
    try:
        page.select_option("#sortSel", order)
        page.wait_for_timeout(300)
    except Exception as e:
        print(f"    set_sort: {e}")


def wait_ready(page, min_rows=40, timeout=60000):
    page.wait_for_function(
        f"document.querySelectorAll('#rows tr').length >= {min_rows}",
        timeout=timeout,
    )
    # under distribution: spread the Owner column arrives with cluster data and
    # flips the fluid (wide) layout; wait for it so no frame catches the narrow
    # centered layout mid-transition (harmless timeout on non-cluster daemons)
    try:
        page.wait_for_function(
            "document.querySelector('main').classList.contains('wide')",
            timeout=15000,
        )
    except Exception:
        pass
    page.wait_for_timeout(500)
    set_sort(page)


def fresh(page, theme=None, extra_prefs=None):
    """Reload the page with a given theme/pref set via localStorage."""
    prefs = {"boot": "false", "zen": "false"}
    # always pin the theme: a prior themed reload leaves its choice in
    # localStorage, so "no theme" must mean the carolina default, not "keep"
    prefs["theme"] = json.dumps(theme or "carolina")
    if extra_prefs:
        prefs.update(extra_prefs)
    js = ";".join(
        f"localStorage.setItem('cronstable.{k}', {json.dumps(v)})"
        for k, v in prefs.items()
    )
    page.evaluate(js)
    page.reload()
    wait_ready(page)


def open_job(page, name, tab=None):
    close_overlays(page)
    row = page.locator("#rows tr", has_text=name).first
    row.scroll_into_view_if_needed()
    row.click()
    page.wait_for_selector("#drawer.open", timeout=5000)
    if tab:
        page.click(f'#dTabs button[data-tab="{tab}"]')
    page.wait_for_timeout(1200)


def scroll_card(page, sel):
    page.wait_for_selector(sel, state="visible", timeout=15000)
    page.evaluate(
        """(sel) => {
            const el = document.querySelector(sel);
            const y = el.getBoundingClientRect().top + window.scrollY - 66;
            window.scrollTo(0, Math.max(0, y));
        }""",
        sel,
    )
    page.wait_for_timeout(400)


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(
            # 16:10 like the original 1440x900 set, but wide enough that the
            # spread-mode board (Owner column + resource chips) and the 9-node
            # fleet matrix render without clipping under the fluid layout
            viewport={"width": 1680, "height": 1050},
            device_scale_factor=2,
            # page CSP has no unsafe-eval; needed for evaluate()
            bypass_csp=True,
            # headless default suppresses the boot POST
            reduced_motion="no-preference",
        )
        ctx.route(
            "**/version",
            lambda route: route.fulfill(
                status=200,
                content_type="text/plain; charset=utf-8",
                body=VERSION,
            ),
        )
        ctx.add_init_script(
            "try{if(!localStorage.getItem('cronstable.bootWant'))"
            "localStorage.setItem('cronstable.boot','false');"
            "localStorage.setItem('cronstable.zen','false');}catch(e){}"
            # the mark is a live pendulum sim (rAF-driven, so reduced-motion
            # CSS can't freeze it); park it balanced at exact upright the
            # moment it mounts so no shot catches it mid-sway. The hook wraps
            # mountGlyph as window.CronstableLogo is assigned — page code runs
            # unmodified, and a reload re-parks automatically. sync() is
            # stubbed so nothing (kickMark, live-state flips) restarts it.
            "(()=>{let CL;Object.defineProperty(window,'CronstableLogo',{"
            "configurable:true,get:()=>CL,set:(v)=>{const orig=v.mountGlyph;"
            "v.mountGlyph=function(slot,opts){const L=orig.call(v,slot,opts);"
            "L.sync=()=>{};if(L._raf)cancelAnimationFrame(L._raf);L._raf=0;"
            "L.sim.opts.gusts=false;L.sim.setConnected(true);"
            "L.sim.s=[0,0,0,0,0,0];L.sim.mode='balance';L.sim.a=0;"
            "L._render();window.__pendLogo=L;return L;};CL=v;}});})();"
        )
        page = ctx.new_page()
        page.goto(BASE)
        wait_ready(page)

        # ---- boot self-test (needs boot pref ON; capture mid-POST) ----
        if wants("dashboard-boot"):
            try:
                page.evaluate(
                    "localStorage.setItem('cronstable.bootWant','1');"
                    "localStorage.setItem('cronstable.boot','true');"
                    # 12h replay gate
                    "localStorage.removeItem('cronstable.bootShownAt')"
                )
                page.reload()
                page.wait_for_selector(
                    "#bootScreen", state="visible", timeout=8000
                )
                # shoot inside the READY hold (650 ms at full opacity, every
                # POST line printed) — a fixed sleep races the fade-out and
                # catches a washed-out overlay instead
                page.wait_for_selector("#bootLog .boot-ready", timeout=8000)
                # pin the READY cursor on (its 1s blink is 50/50 at shot time)
                page.add_style_tag(
                    content=".boot-cur{animation:none!important;"
                    "opacity:1!important}"
                )
                page.wait_for_timeout(150)
                shot(page, "dashboard-boot")
            except Exception as e:
                results["dashboard-boot"] = f"FAIL {e}"
                print(f"  boot shot failed: {e}")
            page.evaluate(
                "localStorage.removeItem('cronstable.bootWant');"
                "localStorage.setItem('cronstable.boot','false')"
            )
            page.reload()
            wait_ready(page)

        # ---- stage the hero: one deliberate red + a guaranteed cpu-burner
        # ----
        api("POST", "/jobs/alert-selftest/start")  # fails instantly, by design
        api(
            "POST", "/jobs/risk-model-recompute/start"
        )  # 30s CPU burn -> live cpu%
        page.wait_for_timeout(7000)  # let a poll land

        if wants("dashboard-overview"):
            try:
                close_overlays(page)
                set_sort(page)
                shot(page, "dashboard-overview")
            except Exception as e:
                results["dashboard-overview"] = f"FAIL {e}"

        # ---- theme variants on the same board ----
        for theme, fname in [
            ("amber", "dashboard-theme-amber"),
            ("green", "dashboard-theme-green"),
            ("modern", "dashboard-theme-modern"),
            ("carolina-light", "dashboard-theme-carolina-light"),
        ]:
            if not wants(fname):
                continue
            try:
                fresh(page, theme=theme)
                page.wait_for_timeout(1500)
                close_overlays(page)
                set_sort(page)
                shot(page, fname)
            except Exception as e:
                results[fname] = f"FAIL {e}"
        if not ONLY or any(
            wants(f"dashboard-theme-{t}")
            for t in ("amber", "green", "modern", "carolina-light")
        ):
            fresh(page)  # back to the default carolina

        # ---- job drawer: live logs on the 5s heartbeat probe ----
        if wants("dashboard-logs"):
            try:
                open_job(page, "pulse-liveness", tab="logs")
                page.check("#optTs")
                page.wait_for_timeout(14000)  # accumulate a few probe runs
                page.fill("#logSearch", "UP")
                page.wait_for_timeout(800)
                shot(page, "dashboard-logs")
                close_overlays(page)
            except Exception as e:
                results["dashboard-logs"] = f"FAIL {e}"
                close_overlays(page)

        # ---- history + per-run cpu/peak-mem (monitorResources) ----
        if wants("dashboard-history"):
            try:
                open_job(page, "risk-model-recompute", tab="history")
                page.wait_for_timeout(1500)
                shot(page, "dashboard-history")
                close_overlays(page)
            except Exception as e:
                results["dashboard-history"] = f"FAIL {e}"
                close_overlays(page)

        # ---- schedule tab on a timezone job ----
        if wants("dashboard-schedule"):
            try:
                open_job(page, "finance-eod-close", tab="schedule")
                page.wait_for_timeout(800)
                shot(page, "dashboard-schedule")
                close_overlays(page)
            except Exception as e:
                results["dashboard-schedule"] = f"FAIL {e}"
                close_overlays(page)

        # ---- command palette ----
        if wants("dashboard-palette"):
            try:
                close_overlays(page)
                page.keyboard.press("Control+k")
                page.wait_for_selector(
                    "#paletteWrap.open, #paletteWrap.show", timeout=4000
                )
                page.fill("#paletteInput", "run")
                page.wait_for_timeout(600)
                shot(page, "dashboard-palette")
                close_overlays(page)
            except Exception as e:
                results["dashboard-palette"] = f"FAIL {e}"
                close_overlays(page)

        # ---- shortcut overlay ----
        if wants("dashboard-shortcuts"):
            try:
                close_overlays(page)
                page.keyboard.type("?")
                page.wait_for_selector(
                    "#helpWrap.open, #helpWrap.show", timeout=4000
                )
                page.wait_for_timeout(400)
                shot(page, "dashboard-shortcuts")
                close_overlays(page)
            except Exception as e:
                results["dashboard-shortcuts"] = f"FAIL {e}"
                close_overlays(page)

        # ---- settings ----
        if wants("dashboard-settings"):
            try:
                close_overlays(page)
                page.click("#settingsBtn")
                page.wait_for_selector(
                    "#settingsWrap.open, #settingsWrap.show", timeout=4000
                )
                page.wait_for_timeout(400)
                shot(page, "dashboard-settings")
                close_overlays(page)
            except Exception as e:
                results["dashboard-settings"] = f"FAIL {e}"
                close_overlays(page)

        # ---- DAG index card ----
        if wants("dashboard-dags"):
            try:
                close_overlays(page)
                scroll_card(page, "#dagCard")
                shot(page, "dashboard-dags")
            except Exception as e:
                results["dashboard-dags"] = f"FAIL {e}"

        # ---- DAG run: trigger the diamond, catch the graph mid-flight ----
        if wants("dashboard-dag-graph"):
            try:
                close_overlays(page)
                r = api("POST", "/dags/data-quality-gate/trigger")
                print(f"    data-quality-gate trigger -> {r}")
                page.wait_for_timeout(3500)
                scroll_card(page, "#dagCard")
                page.click('[data-dagopen="data-quality-gate"]')
                page.wait_for_selector("#dagDrawer.open", timeout=5000)
                page.wait_for_timeout(1000)
                # open the newest run, then its graph
                try:
                    page.locator("#dgRuns tr[data-runkey]").first.click(
                        timeout=3000
                    )
                    page.wait_for_timeout(500)
                except Exception:
                    pass
                page.click('#dagTabs button[data-dtab="graph"]')
                page.wait_for_timeout(1200)
                shot(page, "dashboard-dag-graph")
                close_overlays(page)
            except Exception as e:
                results["dashboard-dag-graph"] = f"FAIL {e}"
                close_overlays(page)

        # ---- DAG approval gate: release-train waits on a human ----
        if wants("dashboard-dag-approval"):
            try:
                close_overlays(page)
                r = api("POST", "/dags/release-train/trigger")
                print(f"    release-train trigger -> {r}")
                run_key = r.get("runKey") if isinstance(r, dict) else None
                # poll the run document until the gate parks awaiting a
                # decision
                for _ in range(45):
                    page.wait_for_timeout(2000)
                    if not run_key:
                        break
                    doc = api("GET", f"/dags/release-train/runs/{run_key}")
                    tasks = (doc or {}).get("tasks") or {}
                    vals = tasks.values() if isinstance(tasks, dict) else tasks
                    if any(
                        isinstance(t, dict) and t.get("awaitingApproval")
                        for t in vals
                    ):
                        break
                scroll_card(page, "#dagCard")
                page.click('[data-dagopen="release-train"]')
                page.wait_for_selector("#dagDrawer.open", timeout=5000)
                page.wait_for_timeout(1500)
                try:
                    page.locator("#dgRuns tr[data-runkey]").first.click(
                        timeout=3000
                    )
                    page.wait_for_timeout(1000)
                except Exception:
                    pass
                page.click('#dagTabs button[data-dtab="tasks"]')
                page.wait_for_selector("[data-approve]", timeout=20000)
                page.wait_for_timeout(500)
                shot(page, "dashboard-dag-approval")
                close_overlays(page)
            except Exception as e:
                results["dashboard-dag-approval"] = f"FAIL {e}"
                close_overlays(page)

        # ---- cluster panel (9 peers + per-node load) ----
        if wants("dashboard-cluster"):
            try:
                close_overlays(page)
                scroll_card(page, "#clusterCard")
                shot(page, "dashboard-cluster")
            except Exception as e:
                results["dashboard-cluster"] = f"FAIL {e}"

        # ---- fleet view (jobs x nodes matrix) ----
        if wants("dashboard-fleet"):
            try:
                close_overlays(page)
                scroll_card(page, "#clusterCard")
                page.click("#fleetBtn")
                page.wait_for_selector(
                    "#fleetPanel", state="visible", timeout=8000
                )
                page.wait_for_timeout(2500)
                scroll_card(page, "#fleetPanel")
                shot(page, "dashboard-fleet")
                page.click("#fleetBtn")  # toggle back off
            except Exception as e:
                results["dashboard-fleet"] = f"FAIL {e}"

        # ---- durable-state inspector ----
        if wants("dashboard-state"):
            try:
                fresh(page, extra_prefs={"stateInsp": "true"})
                scroll_card(page, "#stateCard")
                page.wait_for_timeout(2500)
                shot(page, "dashboard-state")
                fresh(page)  # back to defaults
            except Exception as e:
                results["dashboard-state"] = f"FAIL {e}"
                fresh(page)

        # ---- multi-tail console ----
        if wants("dashboard-multitail"):
            try:
                close_overlays(page)
                page.click("#tailBtn")
                page.wait_for_selector(
                    "#tailWrap.open, #tailWrap.show", timeout=4000
                )
                # the console caps at 4 streams: seed the two second-level
                # probes first (steady line flow), let +failing fill the rest
                for j in ("pulse-liveness", "pulse-latency"):
                    page.fill("#tailAddInput", j)
                    page.keyboard.press("Enter")
                    page.wait_for_timeout(400)
                page.click("#tailAddFailing")
                page.wait_for_timeout(20000)  # accumulate merged lines
                shot(page, "dashboard-multitail")
                close_overlays(page)
            except Exception as e:
                results["dashboard-multitail"] = f"FAIL {e}"
                close_overlays(page)

        # ---- heatmap (may be sparse on a young fleet) ----
        if wants("dashboard-heatmap"):
            try:
                close_overlays(page)
                page.click("#heatBtn")
                page.wait_for_selector(
                    "#heatCard", state="visible", timeout=8000
                )
                page.wait_for_timeout(4000)
                scroll_card(page, "#heatCard")
                shot(page, "dashboard-heatmap")
                page.click("#heatBtn")
            except Exception as e:
                results["dashboard-heatmap"] = f"FAIL {e}"

        # ---- week calendar (business-day chips + the background-hum strip) ----
        if wants("dashboard-week"):
            try:
                close_overlays(page)
                page.click("#weekBtn")
                page.wait_for_selector(
                    "#weekCard", state="visible", timeout=8000
                )
                page.wait_for_selector("#weekBody .wk-ev", timeout=8000)
                scroll_card(page, "#weekCard")
                shot(page, "dashboard-week")
                page.click("#weekBtn")
            except Exception as e:
                results["dashboard-week"] = f"FAIL {e}"

        # ---- LAST: stage a correlated multi-job failure (incident tools) ----
        if (
            wants("dashboard-incident")
            or wants("dashboard-incident-timeline")
            or wants("dashboard-wallboard")
        ):
            try:
                for j in (
                    "db-health-orders",
                    "db-health-inventory",
                    "db-health-payments",
                    "db-health-warehouse",
                ):
                    api("POST", f"/jobs/{j}/start")
                page.wait_for_timeout(9000)
                close_overlays(page)
                set_sort(page)
                if wants("dashboard-incident"):
                    page.wait_for_selector(
                        "#verdictBar", state="visible", timeout=10000
                    )
                    shot(page, "dashboard-incident")
                if wants("dashboard-incident-timeline"):
                    page.keyboard.type("i")
                    page.wait_for_selector(
                        "#timelineWrap.open, #timelineWrap.show", timeout=4000
                    )
                    page.wait_for_timeout(600)
                    shot(page, "dashboard-incident-timeline")
                    close_overlays(page)
            except Exception as e:
                results["dashboard-incident"] = f"FAIL {e}"
                close_overlays(page)

        # ---- wallboard, worst-first with the incident set lit up ----
        if wants("dashboard-wallboard"):
            try:
                close_overlays(page)
                # the toolbar button is deterministic; the "w" hotkey is
                # swallowed if a closing overlay still holds focus
                page.click("#tvBtn")
                page.wait_for_selector(
                    "#wallboard", state="visible", timeout=4000
                )
                page.wait_for_timeout(1500)
                shot(page, "dashboard-wallboard")
                page.keyboard.press("Escape")
                page.wait_for_timeout(400)
            except Exception as e:
                results["dashboard-wallboard"] = f"FAIL {e}"
                close_overlays(page)

        browser.close()

    print("\n== capture summary ==")
    for k, v in results.items():
        print(f"  {k}: {v}")
    fails = [k for k, v in results.items() if v != "ok"]
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
