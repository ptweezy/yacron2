"""Capture the animated-showcase frames off the running grand-tour fleet.

This is the still-frame source for the README's animated hero reel
(`docs/img/dashboard-reel.webp`) and the animated theme row
(`docs/img/dashboard-themes.webp`). It stages each marquee screen exactly
once, then re-shoots that *identical* frame under a rotation of themes by
calling the page's own `setTheme()` live (no reload, so the board, scroll
position and any open overlay stay pixel-stable while only the palette
changes). `build_reel.py` then stitches these stills into the loops.

Usage: python capture_showcase.py [scene ...]
With no args, captures every scene. Raw frames land in ./reel/ as
`<scene>@<theme>.png` alongside a `manifest.json` the builder reads.

Order matters, exactly as in capture_dashboard.py: the clean-board scenes
come first and the deliberately-staged correlated failure (the four
`db-health-*` reds that light up the incident tools) is saved for last so it
does not bleed into earlier frames.
"""
import json
import sys
import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8080"
OUT = Path(__file__).parent / "reel"
OUT.mkdir(exist_ok=True)
ONLY = set(sys.argv[1:])

# clean release-style version for the header (the local build carries a long
# setuptools-scm dev string; a release install shows a clean one like this)
VERSION = "1.2.14"

# the full theme matrix (5 hues x dark/paper); the overview is shot under all
# ten to drive the theme-row loop, other scenes take a tasteful subset
ALL_THEMES = [
    "carolina", "amber", "green", "modern", "standard",
    "carolina-light", "amber-light", "green-light",
    "modern-light", "standard-light",
]

# the hero reel stays in one theme throughout: the light carolina (paper)
# look. Marquee scenes and the a11y beat are shot here in both fonts so the
# reel can use either without another capture pass.
HERO_THEME = "carolina-light"

manifest = {}   # scene -> [theme, ...] actually captured
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


def set_theme_live(page, theme):
    """Flip the palette on the *current* frame, no reload. Drives the app's own
    settings <select id=setTheme> so the real applyTheme() runs -- prefs persist
    and the CRT fx/scanline classes toggle correctly (the flat modern/standard
    themes drop them). Falls back to setting the attribute by hand."""
    ok = page.evaluate(
        """(t) => {
          const s = document.querySelector('#setTheme');
          if (s) { s.value = t;
                   s.dispatchEvent(new Event('change', {bubbles: true}));
                   return document.documentElement.getAttribute('data-theme') === t; }
          return false;
        }""",
        theme,
    )
    if not ok:
        flat = theme in ("modern", "standard", "modern-light", "standard-light")
        page.evaluate(
            """([t, flat]) => {
              document.documentElement.setAttribute('data-theme', t);
              document.body.classList.toggle('fx', !flat);
              document.body.classList.toggle('flicker', !flat);
              document.body.classList.toggle('scan', !flat);
            }""",
            [theme, flat],
        )
    page.wait_for_timeout(450)


def set_select(page, sel_id, value):
    """Drive any settings <select> (font/scale/cvd) the same way as the theme
    picker -- dispatch a real change so the app's applyA11y() runs."""
    page.evaluate(
        """([id, v]) => {
          const s = document.querySelector('#' + id);
          if (s) { s.value = v;
                   s.dispatchEvent(new Event('change', {bubbles: true})); }
        }""",
        [sel_id, str(value)],
    )
    page.wait_for_timeout(450)


def shot(page, name):
    page.screenshot(path=str(OUT / f"{name}.png"))
    manifest.setdefault(name.split("@")[0], []).append(
        name.split("@")[1] if "@" in name else "carolina"
    )
    results[name.split("@")[0]] = "ok"
    print(f"  [shot] {name}")


def shoot_themes(page, scene, themes, clip=None):
    """Take one screenshot of the staged scene per theme."""
    got = []
    for theme in themes:
        try:
            set_theme_live(page, theme)
            page.screenshot(path=str(OUT / f"{scene}@{theme}.png"), clip=clip)
            got.append(theme)
            print(f"  [shot] {scene}@{theme}")
        except Exception as e:
            print(f"    {scene}@{theme}: {e}")
    if got:
        manifest[scene] = got
        results[scene] = "ok"
    else:
        results[scene] = "FAIL no frames"
    # leave the board on the default theme for the next scene's staging
    set_theme_live(page, "carolina")


def shoot_fonts(page, scene, fonts=("mono", "sans"), clip=None):
    """Shoot the staged scene under the interface-font options, all in the
    default carolina theme. The mono shot keeps the plain `<scene>@carolina`
    name; the sans shot gets a `<scene>-sans@carolina` name, so the reel can
    pick either. Logs and cron strings stay monospace by design; the chrome,
    job names and labels switch to the proportional sans."""
    got = []
    for f in fonts:
        try:
            set_select(page, "setFont", f)
            page.wait_for_timeout(400)
            name = scene if f == "mono" else f"{scene}-sans"
            page.screenshot(path=str(OUT / f"{name}@carolina.png"), clip=clip)
            manifest.setdefault(name, []).append("carolina")
            got.append(f)
            print(f"  [shot] {name}@carolina ({f})")
        except Exception as e:
            print(f"    {scene} font {f}: {e}")
    set_select(page, "setFont", "mono")   # reset for the next scene
    results[scene] = "ok" if got else "FAIL no frames"


def shoot_combo(page, scene, combos, clip=None):
    """Shoot the staged scene under a list of (theme, font) pairs, driving both
    the theme picker and the font select live so the frame stays pixel-stable.
    File names encode both axes: `<scene>@<theme>` (mono) or
    `<scene>-sans@<theme>` (sans). Resets to carolina/mono afterwards."""
    got = []
    for theme, font in combos:
        try:
            set_theme_live(page, theme)
            set_select(page, "setFont", font)
            page.wait_for_timeout(400)
            stem = scene if font == "mono" else f"{scene}-sans"
            page.screenshot(path=str(OUT / f"{stem}@{theme}.png"), clip=clip)
            manifest.setdefault(stem, []).append(theme)
            got.append((theme, font))
            print(f"  [shot] {stem}@{theme} ({font})")
        except Exception as e:
            print(f"    {scene} {theme}/{font}: {e}")
    set_select(page, "setFont", "mono")
    set_theme_live(page, "carolina")
    results[scene] = "ok" if got else "FAIL no frames"


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
    try:
        page.wait_for_function(
            "document.querySelector('main').classList.contains('wide')",
            timeout=15000,
        )
    except Exception:
        pass
    page.wait_for_timeout(500)
    set_sort(page)


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
            # 16:9 and >=1680 wide so the spread-mode board (Owner column +
            # resource chips) and the 9-node fleet matrix never clip; the
            # builder supersamples this down to the reel's 1280-wide frame
            viewport={"width": 1680, "height": 945},
            device_scale_factor=2,
            bypass_csp=True,
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
            # park the pendulum mark at exact upright at mount so every frame
            # is pixel-identical across themes/fonts (see capture_dashboard.py)
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

        # ---- boot self-test (carolina only; capture mid-POST) ----
        if wants("boot"):
            try:
                page.evaluate(
                    "localStorage.setItem('cronstable.bootWant','1');"
                    "localStorage.setItem('cronstable.boot','true');"
                    "localStorage.removeItem('cronstable.bootShownAt')"
                )
                page.reload()
                page.wait_for_selector(
                    "#bootScreen", state="visible", timeout=8000
                )
                # shoot inside the READY hold (650 ms at full opacity, every
                # POST line printed) — a fixed sleep races the fade-out
                page.wait_for_selector("#bootLog .boot-ready", timeout=8000)
                # pin the READY cursor on (its 1s blink is 50/50 at shot time)
                page.add_style_tag(
                    content=".boot-cur{animation:none!important;"
                    "opacity:1!important}"
                )
                page.wait_for_timeout(150)
                page.screenshot(path=str(OUT / "boot@carolina.png"))
                manifest["boot"] = ["carolina"]
                results["boot"] = "ok"
                print("  [shot] boot@carolina")
            except Exception as e:
                results["boot"] = f"FAIL {e}"
                print(f"  boot shot failed: {e}")
            page.evaluate(
                "localStorage.removeItem('cronstable.bootWant');"
                "localStorage.setItem('cronstable.boot','false')"
            )
            page.reload()
            wait_ready(page)

        # ---- stage the hero board: one deliberate red + a live cpu-burner ----
        api("POST", "/jobs/alert-selftest/start")       # fails instantly
        api("POST", "/jobs/risk-model-recompute/start")  # 30s CPU burn
        page.wait_for_timeout(7000)

        # ---- overview: the marquee frame, shot under ALL ten themes ----
        if wants("overview"):
            close_overlays(page)
            set_sort(page)
            shoot_themes(page, "overview", ALL_THEMES)   # mono, every theme
            # ...and the same board in the readable sans font under every
            # theme too, so the theme row can show BOTH axes (theme x font)
            shoot_combo(page, "overview", [(t, "sans") for t in ALL_THEMES])

        # ---- command palette ----
        if wants("palette"):
            try:
                close_overlays(page)
                page.keyboard.press("Control+k")
                page.wait_for_selector(
                    "#paletteWrap.open, #paletteWrap.show", timeout=4000
                )
                page.fill("#paletteInput", "run")
                page.wait_for_timeout(600)
                shoot_combo(page, "palette",
                            [(HERO_THEME, "mono"), (HERO_THEME, "sans")])
                close_overlays(page)
            except Exception as e:
                results["palette"] = f"FAIL {e}"
                close_overlays(page)

        # ---- job drawer: live logs on the 5s heartbeat probe ----
        if wants("logs"):
            try:
                open_job(page, "pulse-liveness", tab="logs")
                page.check("#optTs")
                page.wait_for_timeout(14000)
                page.fill("#logSearch", "UP")
                page.wait_for_timeout(800)
                shoot_combo(page, "logs",
                            [(HERO_THEME, "mono"), (HERO_THEME, "sans")])
                close_overlays(page)
            except Exception as e:
                results["logs"] = f"FAIL {e}"
                close_overlays(page)

        # ---- history + per-run cpu/peak-mem ----
        if wants("history"):
            try:
                open_job(page, "risk-model-recompute", tab="history")
                page.wait_for_timeout(1500)
                shoot_combo(page, "history",
                            [(HERO_THEME, "mono"), (HERO_THEME, "sans")])
                close_overlays(page)
            except Exception as e:
                results["history"] = f"FAIL {e}"
                close_overlays(page)

        # ---- schedule tab on a timezone job ----
        if wants("schedule"):
            try:
                open_job(page, "finance-eod-close", tab="schedule")
                page.wait_for_timeout(800)
                shoot_combo(page, "schedule",
                            [(HERO_THEME, "mono"), (HERO_THEME, "sans")])
                close_overlays(page)
            except Exception as e:
                results["schedule"] = f"FAIL {e}"
                close_overlays(page)

        # ---- DAG run: trigger the diamond, catch the graph mid-flight ----
        if wants("dag-graph"):
            try:
                close_overlays(page)
                r = api("POST", "/dags/data-quality-gate/trigger")
                print(f"    data-quality-gate trigger -> {r}")
                page.wait_for_timeout(3500)
                scroll_card(page, "#dagCard")
                page.click('[data-dagopen="data-quality-gate"]')
                page.wait_for_selector("#dagDrawer.open", timeout=5000)
                page.wait_for_timeout(1000)
                try:
                    page.locator("#dgRuns tr[data-runkey]").first.click(
                        timeout=3000
                    )
                    page.wait_for_timeout(500)
                except Exception:
                    pass
                page.click('#dagTabs button[data-dtab="graph"]')
                page.wait_for_timeout(1200)
                shoot_combo(page, "dag-graph",
                            [(HERO_THEME, "mono"), (HERO_THEME, "sans")])
                close_overlays(page)
            except Exception as e:
                results["dag-graph"] = f"FAIL {e}"
                close_overlays(page)

        # ---- cluster panel (9 peers + per-node load) ----
        if wants("cluster"):
            try:
                close_overlays(page)
                scroll_card(page, "#clusterCard")
                shoot_combo(page, "cluster",
                            [(HERO_THEME, "mono"), (HERO_THEME, "sans")])
            except Exception as e:
                results["cluster"] = f"FAIL {e}"

        # ---- fleet view (jobs x nodes matrix) ----
        if wants("fleet"):
            try:
                close_overlays(page)
                scroll_card(page, "#clusterCard")
                page.click("#fleetBtn")
                page.wait_for_selector(
                    "#fleetPanel", state="visible", timeout=8000
                )
                page.wait_for_timeout(2500)
                scroll_card(page, "#fleetPanel")
                shoot_combo(page, "fleet",
                            [(HERO_THEME, "mono"), (HERO_THEME, "sans")])
                page.click("#fleetBtn")
            except Exception as e:
                results["fleet"] = f"FAIL {e}"

        # ---- accessibility beat, in the hero's carolina-light theme: the same
        # board made colour-blind safe (deuteranopia) and zoomed (125%), each
        # in both fonts so the reel can use either. ----
        if wants("a11y"):
            try:
                close_overlays(page)
                set_sort(page)
                set_theme_live(page, HERO_THEME)
                # 1) colour-vision-deficiency palette (deuteranopia)
                set_select(page, "setCvd", "deutan")
                for font in ("mono", "sans"):
                    set_select(page, "setFont", font)
                    page.wait_for_timeout(500)
                    stem = "a11y-cvd" + ("-sans" if font == "sans" else "")
                    shot(page, f"{stem}@{HERO_THEME}")
                set_select(page, "setCvd", "none")
                # 2) larger UI scale
                set_select(page, "setScale", "125")
                for font in ("mono", "sans"):
                    set_select(page, "setFont", font)
                    page.wait_for_timeout(500)
                    stem = "a11y-scale" + ("-sans" if font == "sans" else "")
                    shot(page, f"{stem}@{HERO_THEME}")
                # reset every a11y pref + theme for the scenes that follow
                set_select(page, "setScale", "100")
                set_select(page, "setFont", "mono")
                set_select(page, "setCvd", "none")
                set_theme_live(page, "carolina")
                page.wait_for_timeout(300)
            except Exception as e:
                results["a11y"] = f"FAIL {e}"
                set_select(page, "setScale", "100")
                set_select(page, "setFont", "mono")
                set_select(page, "setCvd", "none")
                set_theme_live(page, "carolina")

        # ---- settings panel (carolina-light), scrolled to the a11y controls ----
        if wants("settings-a11y"):
            try:
                close_overlays(page)
                set_theme_live(page, HERO_THEME)
                page.click("#settingsBtn")
                page.wait_for_selector(
                    "#settingsWrap.open, #settingsWrap.show", timeout=4000
                )
                page.wait_for_timeout(300)
                # bring the Interface font / UI scale / colour-vision selects
                # into view inside the settings panel
                try:
                    page.eval_on_selector(
                        "#setFont",
                        "el => el.scrollIntoView({block: 'center'})",
                    )
                except Exception:
                    pass
                for font in ("mono", "sans"):
                    set_select(page, "setFont", font)
                    page.wait_for_timeout(400)
                    stem = "settings-a11y" + ("-sans" if font == "sans" else "")
                    shot(page, f"{stem}@{HERO_THEME}")
                set_select(page, "setFont", "mono")
                close_overlays(page)
                set_theme_live(page, "carolina")
            except Exception as e:
                results["settings-a11y"] = f"FAIL {e}"
                close_overlays(page)
                set_theme_live(page, "carolina")

        # ---- LAST: stage the correlated multi-job failure (incident tools) ----
        need_incident = (
            wants("wallboard") or wants("incident-timeline")
        )
        if need_incident:
            for j in (
                "db-health-orders", "db-health-inventory",
                "db-health-payments", "db-health-warehouse",
            ):
                api("POST", f"/jobs/{j}/start")
            page.wait_for_timeout(9000)
            close_overlays(page)
            set_sort(page)

        # ---- incident timeline overlay ----
        if wants("incident-timeline"):
            try:
                page.keyboard.type("i")
                page.wait_for_selector(
                    "#timelineWrap.open, #timelineWrap.show", timeout=4000
                )
                page.wait_for_timeout(600)
                shoot_combo(page, "incident-timeline",
                            [(HERO_THEME, "mono"), (HERO_THEME, "sans")])
                close_overlays(page)
            except Exception as e:
                results["incident-timeline"] = f"FAIL {e}"
                close_overlays(page)

        # ---- wallboard, worst-first with the incident set lit up ----
        if wants("wallboard"):
            try:
                close_overlays(page)
                # the toolbar button is deterministic; the "w" hotkey is
                # swallowed if a closing overlay still holds focus
                page.click("#tvBtn")
                page.wait_for_selector(
                    "#wallboard", state="visible", timeout=4000
                )
                page.wait_for_timeout(1500)
                shoot_combo(page, "wallboard",
                            [(HERO_THEME, "mono"), (HERO_THEME, "sans")])
                page.keyboard.press("Escape")
                page.wait_for_timeout(400)
            except Exception as e:
                results["wallboard"] = f"FAIL {e}"
                close_overlays(page)

        browser.close()

    # merge into any existing manifest so single-scene reruns don't wipe others
    mpath = OUT / "manifest.json"
    existing = {}
    if mpath.exists():
        try:
            existing = json.loads(mpath.read_text())
        except Exception:
            existing = {}
    existing.update(manifest)
    mpath.write_text(json.dumps(existing, indent=2))

    print("\n== showcase capture summary ==")
    for k, v in results.items():
        print(f"  {k}: {v}")
    fails = [k for k, v in results.items() if v != "ok"]
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
