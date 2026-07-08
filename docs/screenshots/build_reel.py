"""Stitch the showcase stills into the README's animated loops.

Reads the frames `capture_showcase.py` dropped in ./reel/ and assembles two
seamless loops:

* `dashboard-reel.webp`  -- the hero tour, in ONE consistent style throughout
  (the light carolina / paper theme, terminal monospace): the overview and the
  marquee screens (palette, live logs, a DAG graph, the cluster + fleet matrix,
  the wallboard and the incident timeline), plus an accessibility beat
  (colour-vision-safe palette, larger UI scale, the settings panel). No theme
  or font cycling here -- the variety is the screens.
* `dashboard-themes.webp` -- the theme + font showcase: the identical overview
  frame under all ten themes, each shown in both the terminal monospace and
  the readable proportional-sans interface font.

Each also gets a `.gif` twin as a fallback for clients that don't render
animated WebP. WebP is the primary (24-bit colour, no phosphor-glow banding);
the GIF is 256-colour and scaled down.

How it stays small: animated WebP stores each frame in full (no interframe
delta), so the file size is driven by the *frame count*. We keep that low with
a variable-duration timeline -- a held screen is ONE frame with a long
duration, not many repeats -- and by cutting hard between screens (a cut costs
zero frames, and no soft blended frames means every frame is a pristine still).
Both loops are pure cuts, so they can run at full 1600px / q94 and still land
in a few MB. Tune SEGMENTS / holds below and re-run; it needs no daemon, just
Pillow.

Usage: python build_reel.py [reel|themes]   (default: both)
"""
import json
import sys
from pathlib import Path

from PIL import Image

HERE = Path(__file__).parent
SRC = HERE / "reel"
IMG = HERE.parent / "img"          # docs/img
MANIFEST = json.loads((SRC / "manifest.json").read_text()) if (
    SRC / "manifest.json"
).exists() else {}

# The reel cuts hard (few frames), so we can afford a large, high-quality
# frame: 1600px supersampled from the 3360px stills, near-max libwebp quality.
# Both loops are cut-only (no soft dissolve frames), so every single frame is a
# pristine still and we can run them at full resolution / near-max quality
# without the file exploding.
REEL_W = 1600          # hero reel width (16:9), supersampled from 3360
REEL_Q = 94            # libwebp quality for the reel (crisp dense text)
THEMES_W = 1600        # theme-row width (matches the reel; crisp dense text)
THEMES_Q = 94          # theme-row quality
WEBP_METHOD = 6        # libwebp effort (0-6): best ratio, fine at low frame counts
GIF_W = 900            # GIF fallback width (256-colour anyway)

SCENE_FADE_MS = 0      # hero reel cuts hard between screens (0 == pure cuts)
SCENE_FADE_N = 0       # frames spent on scene dissolves (0 == none)
THEME_FADE_MS = 0      # theme row also cuts hard, so every frame stays sharp
THEME_FADE_N = 0

# ---- the hero tour: (scene, theme, hold_seconds) in play order ----
# consecutive entries with the SAME scene (the overview theme sweep, and the
# in-place theme flips on logs/dag/fleet/wallboard) hard-cut -- that is the
# "same frame, different theme" beat. Scene *changes* get a short dissolve.
# The final overview@carolina matches the first overview so the loop closes.
# The hero reel stays in ONE style throughout -- the light carolina (paper)
# theme, terminal monospace -- and gets its variety from the different screens
# it tours. The theme + font showcase lives in the theme row below, not here.
HERO_THEME = "carolina-light"
SEGMENTS = [
    ("overview", HERO_THEME, 2.2),          # hero: the live board
    ("palette", HERO_THEME, 1.3),           # command palette
    ("logs", HERO_THEME, 1.6),              # live log tail
    ("dag-graph", HERO_THEME, 1.6),         # a DAG's task graph
    ("cluster", HERO_THEME, 1.5),           # 9-peer cluster panel
    ("fleet", HERO_THEME, 1.6),             # jobs x nodes fleet matrix
    ("wallboard", HERO_THEME, 1.5),         # TV wallboard
    ("incident-timeline", HERO_THEME, 1.5),
    # accessibility beat (same theme): colour-blind-safe palette, larger UI
    # scale, and the settings that drive the readability / a11y options
    ("a11y-cvd", HERO_THEME, 1.3),
    ("a11y-scale", HERO_THEME, 1.4),
    ("settings-a11y", HERO_THEME, 1.6),
    # close on the hero still so the loop seam is invisible (frame N == frame 0)
    ("overview", HERO_THEME, 1.2),
]

# ---- the theme row: overview under every theme, in BOTH the monospace and
# the readable sans interface font, dissolving between each -- so it showcases
# the theme axis and the font axis together. ----
THEME_ORDER = [
    "carolina", "carolina-light", "amber", "amber-light",
    "green", "green-light", "modern", "modern-light",
    "standard", "standard-light",
]
THEMES_HOLD = 0.85


def load(scene, theme, width):
    """Load a still, downscale to `width` (supersampled from ~3360), pad to a
    stable 16:9 canvas so every scene shares one frame size."""
    path = SRC / f"{scene}@{theme}.png"
    if not path.exists():
        return None
    im = Image.open(path).convert("RGB")
    h = round(width * im.height / im.width)
    im = im.resize((width, h), Image.LANCZOS)
    canvas_h = round(width * 9 / 16)
    canvas_h += canvas_h & 1
    bg = Image.new("RGB", (width, canvas_h), (0, 0, 0))
    bg.paste(im, (0, max(0, (canvas_h - h) // 2)))
    return bg


def build_timeline(segments, width, fade_ms, fade_n, always_fade):
    """Return [(PIL.Image, duration_ms)]. Held screens are single long frames;
    a transition is a hard cut (same scene, unless always_fade) or a short
    cross-dissolve. The last->first transition closes the loop."""
    stills, missing = [], []
    for scene, theme, hold in segments:
        im = load(scene, theme, width)
        if im is None:
            missing.append(f"{scene}@{theme}")
            continue
        stills.append((scene, im, hold))
    if missing:
        print(f"  ! missing frames skipped: {', '.join(missing)}")
    if not stills:
        return []

    frames = []
    n = len(stills)
    step_ms = round(fade_ms / fade_n) if fade_n else 0
    for i, (scene, im, hold) in enumerate(stills):
        frames.append((im, round(hold * 1000)))
        # fade_n == 0 means a pure-cut montage (no dissolves at all): the hero
        # reel cuts hard, which keeps the "same board, new palette" theme flips
        # snappy and avoids muddy double-exposures when two very different
        # screens would otherwise cross-dissolve.
        if not fade_n:
            continue
        nscene, nxt, _ = stills[(i + 1) % n]
        if always_fade or nscene != scene:          # else: hard cut
            for s in range(1, fade_n + 1):
                frames.append((Image.blend(im, nxt, s / (fade_n + 1)), step_ms))
    return frames


def save_webp(frames, out, quality):
    ims = [f for f, _ in frames]
    ims[0].save(
        out, format="WEBP", save_all=True, append_images=ims[1:],
        duration=[d for _, d in frames], loop=0, quality=quality,
        method=WEBP_METHOD,
    )
    print(f"  [webp] {out.name}: {len(ims)}f, q{quality}, "
          f"{out.stat().st_size // 1024} KB")


def save_gif(frames, out):
    durations, ims = [], []
    for f, d in frames:
        if f.width != GIF_W:
            f = f.resize((GIF_W, round(GIF_W * f.height / f.width)),
                         Image.LANCZOS)
        ims.append(f.quantize(colors=256, method=Image.Quantize.FASTOCTREE,
                              dither=Image.Dither.FLOYDSTEINBERG))
        durations.append(d)
    ims[0].save(
        out, format="GIF", save_all=True, append_images=ims[1:],
        duration=durations, loop=0, optimize=True, disposal=1,
    )
    print(f"  [gif]  {out.name}: {len(ims)}f, {out.stat().st_size // 1024} KB")


def build(which):
    IMG.mkdir(parents=True, exist_ok=True)
    if which in ("reel", "both"):
        print("building hero reel...")
        frames = build_timeline(SEGMENTS, REEL_W, SCENE_FADE_MS, SCENE_FADE_N,
                                always_fade=False)
        if frames:
            save_webp(frames, IMG / "dashboard-reel.webp", REEL_Q)
            save_gif(frames, IMG / "dashboard-reel.gif")
    if which in ("themes", "both"):
        print("building theme row...")
        # each theme shown in monospace then the readable sans, so the loop
        # demonstrates both the theme and the interface-font options
        segs = []
        for t in THEME_ORDER:
            segs.append(("overview", t, THEMES_HOLD))       # monospace
            segs.append(("overview-sans", t, THEMES_HOLD))  # readable sans
        frames = build_timeline(segs, THEMES_W, THEME_FADE_MS, THEME_FADE_N,
                                always_fade=True)
        if frames:
            save_webp(frames, IMG / "dashboard-themes.webp", THEMES_Q)
            save_gif(frames, IMG / "dashboard-themes.gif")


if __name__ == "__main__":
    build(sys.argv[1] if len(sys.argv) > 1 else "both")
