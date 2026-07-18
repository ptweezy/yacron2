# Regenerating the dashboard screenshots

The images in `docs/img/dashboard-*.png` are captured off a **real running
fleet** (no mocked data) at a 1680x1050 viewport with `deviceScaleFactor: 2`
(3360x2100 PNGs). The width matters: under `distribution: spread` the board
grows an Owner column and flips the page into its fluid (wide) layout, and
1680 is enough for that plus the 9-node fleet matrix to render without
clipping. To refresh them after a UI change:

1. **Boot the grand tour** (builds the image from the working tree, so your
   local `cronstable/web/index.html` is what gets photographed), then give it
   10-15 minutes of uptime so sparklines and history fill in:

   ```shell
   docker compose -f example/grand-tour/docker-compose.yml up --build -d
   ```

2. **Run the capture script** (needs `playwright` + its Chromium in the
   environment; shots land in a `shots/` directory next to the script):

   ```shell
   python docs/screenshots/capture_dashboard.py                    # everything
   python docs/screenshots/capture_dashboard.py dashboard-overview # one shot
   ```

   The script stages the board deliberately: it starts a CPU-burner and a
   deliberate failure for the hero shot, triggers DAG runs (including parking
   `release-train` on its approval gate), and saves the staged incident
   (the four `db-health-*` failures) for last so earlier frames stay clean.

3. **The log-tail closeup** uses a separate one-job daemon whose job actually
   produces a colorful multi-line stream (the grand-tour jobs are terse
   one-liners). Run it locally, then capture:

   ```shell
   cronstable -c docs/screenshots/logs-demo.yaml &
   python docs/screenshots/capture_logs_closeup.py
   ```

4. **The pendulum-logo loops** (`logo-balance` + `logo-balance-light`, each a
   24-bit `.webp` primary with a `.gif` twin) need no daemon at all: the
   script serves the working tree itself, unhooks the page's own animation
   loop, and steps the mark's cart/double-pendulum simulation frame-by-frame
   at an exact 50 fps, so the recording is deterministic, true-speed physics.
   The choreography — theme-hop glitches that knock the mark, one full
   signal-loss collapse with a verified catch on reconnect, and a calm
   settle so the loop seam is invisible — is seed-searched headlessly
   through the page's own sim before anything is captured. Needs Pillow
   alongside playwright:

   ```shell
   python docs/screenshots/capture_logo_gif.py
   ```

5. **The GitHub social-preview card** (`social-preview.png`, 1280x640) is
   rendered from `social-card.html`, a static page styled after the carolina
   theme with `docs/img/dashboard-overview.png` inset as the product shot, so
   regenerate that overview first if the UI changed. Also needs Pillow:

   ```shell
   python docs/screenshots/capture_social_card.py
   ```

   GitHub has no API for the social preview: after regenerating, upload
   `docs/img/social-preview.png` by hand under **Settings -> General ->
   Social preview** (1 MB limit). That image is what link unfurls on Slack,
   Discord, Teams, and X/Twitter show for the repo URL.

6. **The terminal-dashboard set** (`docs/img/tui-*.png`) comes off the same
   running grand-tour fleet, staged the same way: the real
   `cronstable.tui.TuiApp` is driven headless against meridian-a (a scripted
   key queue and an in-memory terminal stand in for the tty), and the
   captured ANSI frames are rasterized to PNG through Playwright's Chromium
   at deviceScaleFactor 2, in a terminal-window card set in Cascadia Mono:

   ```shell
   python docs/screenshots/capture_tui.py                 # everything
   python docs/screenshots/capture_tui.py tui-overview    # one shot
   ```

   The incident shots (`tui-incident*`, `tui-wallboard`) wait for the
   fleet's simulated outage window (the `db-health-*` jobs only fail while
   the UTC minute is 15-19), so a full run can idle up to ~55 minutes
   before shooting them — run those three when the window is close, or let
   it wait.

7. Review the PNGs, then copy the keepers over `docs/img/` (re-saving with
   Pillow's `optimize=True` shaves a few percent).

## Regenerating the animated hero reel + theme row

The README's two animated loops — `docs/img/dashboard-reel.webp` (the hero
tour) and `docs/img/dashboard-themes.webp` (the ten-theme sweep), each with a
`.gif` twin — are built in two steps off the **same running grand-tour fleet**
as the stills above (so boot it first, per step 1, and let it warm):

```shell
# 1. capture the source stills: each marquee screen is shot once, then
#    re-shot under a rotation of themes AND the accessibility prefs (sans
#    interface font, colour-vision palette, larger UI scale) by driving the
#    dashboard's own settings <select>s live — so the frames are pixel-stable
#    and only the palette / font / scale changes. Frames land in ./reel/.
python docs/screenshots/capture_showcase.py                 # every scene
python docs/screenshots/capture_showcase.py overview a11y   # just some

# 2. stitch the stills into the loops (needs Pillow; no daemon):
python docs/screenshots/build_reel.py                       # both
python docs/screenshots/build_reel.py reel                  # just the hero
```

`build_reel.py` keeps the files small by treating each held screen as one
long-duration frame and **cutting hard** between screens (a cut costs zero
frames). The hero reel stays in **one style throughout** — the light carolina
theme, terminal monospace — and gets its variety from the different screens it
tours. The theme + font showcase is the theme row, which cuts through the
overview under all ten themes, each in both the monospace and the readable
sans interface font. Both loops are cut-only (no soft dissolve frames), so
every frame is a pristine still and they run at full 1600px / q94. Tune the
`SEGMENTS` timeline and per-asset width/quality at the top of the script and
re-run — it writes straight to `docs/img/`.

Notes:

* The scripts intercept `GET /version` and substitute the next release number
  so the header doesn't show a long `setuptools-scm` dev string.
* Prefer capturing at a "quiet minute" of the grand tour's deterministic
  failure calendar (see `example/grand-tour/README.md`) unless you *want* the
  incident chrome in frame.
* Screenshot prefs are seeded through `localStorage` (`cronstable.boot`,
  `cronstable.zen`, `cronstable.theme`, ...); the context needs `bypass_csp: true`
  (the page CSP has no `unsafe-eval`) and `reduced_motion: "no-preference"`
  (headless Chromium otherwise suppresses the boot POST screen and CRT
  animation).
* The header mark is a live pendulum simulation, so the still-capture scripts
  park it balanced at exact upright the moment it mounts (an init-script hook
  around `CronstableLogo.mountGlyph` — see `capture_dashboard.py`); every
  frame is then pixel-identical across themes, fonts, and reloads.
