"""Capture the header brand (spinning logo + wordmark) as seamless-loop GIFs.

Unlike the dashboard PNGs this needs NO running daemon: the page is served
straight off the working tree, and the mark's rotation is driven angle-by-angle
through an injected `!important` style (which outranks the app's own rAF
inline transform). Every frame lands on an exact fraction of a revolution, so
the last frame wraps perfectly onto the first and the loop never stutters.

The loop spans several whole revolutions (LOOP_REVS) so a single glitch can be
buried mid-loop: for ~10 frames the brand's ink *switches* clean to the next
theme (no dim — it holds the new colour from there on) while the whole wordmark
eases sideways and the glyph splits into a chromatic misregistration — offset
silhouettes in saturated red / cyan / green / violet plus a digital slice tear
— which pulses once and settles. A GIF repeats identically every cycle, so
stretching the loop is the only way to make the glitch read as *occasional*
rather than every
second. Pixel dimensions are unchanged — only the loop gets longer. This touches
the exported GIFs only; the live dashboard mark is left exactly as it was.

The replay speed is derived from the page's MARK_CRUISE constant, so a retune
there keeps the GIF honest on the next regen.

Each variant is written twice: a 24-bit `<name>.webp` (the primary — no
256-palette banding on the glow or the glitch's saturated ghosts) and a
256-colour `<name>.gif` twin for clients that don't render animated WebP, the
same webp-primary / gif-fallback convention as build_reel.py.

Needs playwright (+ its Chromium) and Pillow. Files land in shots/.
"""
import http.server
import math
import random
import re
import threading
from functools import partial
from io import BytesIO
from pathlib import Path

from PIL import Image, ImageChops, ImageOps
from playwright.sync_api import sync_playwright

WEB = Path(__file__).resolve().parents[2] / "cronstable" / "web"
OUT = Path(__file__).parent / "shots"
PORT = 8123

# 52 frames/rev lands the true 345deg/s cruise exactly at the ~20ms GIF decoder
# floor, so DON'T raise it for smoothness (it would only slow the spin). Extra
# frames come from more revolutions instead.
FRAMES_PER_REV = 52   # angular resolution of one revolution (true-speed at ~20ms)
LOOP_REVS = 43        # whole revolutions per loop; more revs => each stop holds longer
FRAMES = FRAMES_PER_REV * LOOP_REVS
PAD = 12              # css px around the brand box (glow + rotation overflow)
SCALE = 2             # device pixels per css px, matches the PNG set

# WebP is the primary (24-bit colour -> no 256-palette banding on the glow or
# the glitch's saturated ghosts); the GIF is a 256-colour twin for clients that
# don't render animated WebP. Mirrors docs/screenshots/build_reel.py.
WEBP_LOSSLESS = True  # True = pixel-perfect (larger); False = high-q lossy
WEBP_QUALITY = 92     # lossy mode: fidelity. lossless mode: compression effort (->100)
WEBP_METHOD = 6       # libwebp effort (0-6): best ratio

# ---- glitch (tuned live in docs/screenshots/logo-glitch-tuner) --------------
# The ink SWITCHES clean to the target theme and holds (no dim); on top of it the
# glyph is torn into a chromatic misregistration — offset colour
# silhouettes that jitter per frame — plus a digital slice tear.
GLITCH = True
GLITCH_MS = 200       # length of each glitch/switch (clamped to whole frames, ~10f)
GLITCH_SPLIT = 4.5    # px of chromatic offset for the colour ghosts at peak
GLITCH_TEAR = 3       # horizontal slice shears at the peak (0 = none)
GLITCH_MOVE = 2.0     # css px the wordmark itself eases sideways & back (peak, *SCALE)
MIN_HOLD_MS = 2500    # a colour holds at least this long before it can hop again
SEED = 20260708       # fixes the per-frame glitch jitter (reproducible)
HOP_SEED = 7          # fixes the random hop spacing; reroll for a different rhythm

# the saturated silhouette inks the glyph splits into during a glitch. Screened
# on over a dark theme (they glow) and multiplied under a paper theme (they read
# as CMYK misregistration), so each variant gets its own tuned set.
GHOSTS_DARK = [(255, 46, 104), (54, 214, 255), (60, 255, 150), (188, 108, 255)]
GHOSTS_PAPER = [(210, 24, 66), (0, 132, 194), (22, 150, 74), (124, 48, 196)]

# The colour JOURNEY each loop takes. It starts on the base theme and the last
# hop returns to it, so the base spans the loop seam and the wrap is seamless. In
# between, every listed theme is switched to by a glitch and then HELD until the
# next hop — the colour stays, it doesn't flick back. Add stops for a longer
# tour (e.g. ["amber", "green"]).  (output name, base theme, stops to visit)
# NB: "standard" is intentionally left out — for the logo it's a near-white,
# no-glow ink indistinguishable from "modern", so two adjacent white stops would
# look like a glitch that changes nothing. One neutral beat (modern) is plenty.
# (basename — each variant writes both <name>.webp and <name>.gif)
VARIANTS = [
    ("logo-spin", "carolina", ["amber", "green", "modern"]),
    ("logo-spin-light", "carolina-light",
     ["amber-light", "green-light", "modern-light"]),
]

# brand-box ink tokens, lifted verbatim from index.html. Setting these inline on
# <html> outranks the `html[data-theme=...]` rules, so the mark + wordmark + tag
# + glow all flick to the target palette without disturbing the background.
INK_KEYS = ("--fg", "--fg-dim", "--fg-faint", "--accent", "--glow")
THEME_INK = {
    "amber":          ("#ffb000", "#c98a2c", "#a37a38", "#ffd98a", "rgba(255,176,0,.55)"),
    "green":          ("#38ff7a", "#1fbf57", "#22a85c", "#b6ffce", "rgba(56,255,122,.5)"),
    "modern":         ("#d6dee8", "#9aa4b2", "#8b95a3", "#79c0ff", "rgba(121,192,255,.0)"),
    "standard":       ("#e6e9ed", "#a6aeb9", "#8f98a3", "#4c8dff", "rgba(76,141,255,0)"),
    "amber-light":    ("#3f2d00", "#6d500b", "#755c20", "#855700", "rgba(138,90,0,.28)"),
    "green-light":    ("#0b3a1e", "#1a6238", "#3a7050", "#0d7034", "rgba(18,105,55,.25)"),
    "modern-light":   ("#1f2328", "#57606a", "#656e79", "#0969da", "rgba(9,105,218,0)"),
    "standard-light": ("#14181d", "#4b5563", "#5d6675", "#0b5ed7", "rgba(11,94,215,0)"),
}

# the page's resting spin rate, deg/s
CRUISE = float(
    re.search(
        r"MARK_CRUISE\s*=\s*([\d.]+)",
        (WEB / "index.html").read_text(encoding="utf-8"),
    ).group(1)
)
# ms per frame for a true-speed replay; decoders clamp delays below ~20ms. Timed
# per *revolution* so a longer loop plays at the same speed, just for longer.
FRAME_MS = max(20, round(360 / CRUISE * 1000 / FRAMES_PER_REV / 10) * 10)


def journey(base, stops):
    """Resolve the colour journey into held segments + glitch hops.

    Returns (seq, starts): `seq` is the held colour per segment, `starts[i]` is
    the frame the glitch that switches INTO seq[i+1] begins on. seq[0] == the
    base and the last segment is the base again, so the base spans the seam.

    Hops are placed at RANDOM (but floored) intervals so the glitches don't fall
    on a predictable beat — each colour holds at least MIN_HOLD_MS, with the rest
    of the loop shared out by random weights. Deterministic via SEED.
    """
    seq = [base] + list(stops) + [base] if GLITCH and stops else [base]
    hops = len(seq) - 1
    if hops <= 0:
        return seq, []
    length = max(1, round(GLITCH_MS / FRAME_MS))
    segs = hops + 1
    floor = length + round(MIN_HOLD_MS / FRAME_MS)     # min frames per segment
    free = FRAMES - floor * segs
    rng = random.Random(HOP_SEED)
    if free <= 0:                                      # loop too short: even split
        seg_lens = [FRAMES // segs] * segs
    else:
        w = [rng.uniform(1.0, 3.2) for _ in range(segs)]
        wt = sum(w)
        seg_lens = [floor + int(free * x / wt) for x in w]
    seg_lens[-1] += FRAMES - sum(seg_lens)             # absorb rounding drift
    starts, acc = [], 0
    for L in seg_lens[:-1]:
        acc += L
        starts.append(acc)
    starts[-1] = min(starts[-1], FRAMES - length - 2)  # last hop resolves pre-seam
    return seq, starts


def glitch_env(i, span):
    """0..1 aberration strength across a glitch of `span` frames: one smooth
    rise-and-fall, ~0 at the ends so the base wordmark returns home cleanly. The
    colour *switch* itself is separate (held from the hop onward); this only
    shapes the chromatic-split / tear / base-slide intensity."""
    return math.sin(math.pi * (i + 0.5) / span)


def _silhouette(gray, color, paper):
    """A single-colour copy of the ink, neutral against its ground: bright on
    black (screen), white-backed on paper (multiply)."""
    if paper:
        return ImageOps.colorize(ImageOps.invert(gray), black=(255, 255, 255), white=color)
    return ImageOps.colorize(gray, black=(0, 0, 0), white=color)


def spiderverse(img, env, rng, paper):
    """Split the (already colour-switched) glyph into jittered, saturated
    silhouettes for a chromatic misregistration, then slice-tear. The whole
    wordmark also eases sideways (GLITCH_MOVE) so the foundation itself moves."""
    if env <= 0:
        return img
    img = ImageChops.offset(img, int(round(GLITCH_MOVE * SCALE * env)), 0)
    gray = img.convert("L")
    ghosts = GHOSTS_PAPER if paper else GHOSTS_DARK
    reach = GLITCH_SPLIT * SCALE * (0.55 + 0.75 * env)
    order = list(range(len(ghosts)))
    rng.shuffle(order)
    out = img
    for gi in order[:3]:                         # three colour ghosts per frame
        dx = int(round(rng.uniform(-reach, reach)))
        dy = int(round(rng.uniform(-reach, reach) * 0.55))
        layer = ImageChops.offset(_silhouette(gray, ghosts[gi], paper), dx, dy)
        if paper:
            out = ImageChops.darker(out, layer)   # colours print under the paper
        else:
            out = ImageChops.lighter(out, layer)  # colours glow over the black
    if GLITCH_TEAR:
        w, h = out.size
        torn = out.copy()
        max_shift = int(round((GLITCH_SPLIT + 3) * SCALE * env)) + 1
        for _ in range(GLITCH_TEAR):
            y = rng.randint(0, h - 1)
            band_h = rng.randint(max(2, h // 36), max(3, h // 10))
            band = out.crop((0, y, w, min(h, y + band_h)))
            torn.paste(ImageChops.offset(band, rng.randint(-max_shift, max_shift), 0), (0, y))
        out = torn
    return out


def capture(browser, theme, stops, base):
    ctx = browser.new_context(
        viewport={"width": 900, "height": 200},
        device_scale_factor=SCALE,
        bypass_csp=True,
        reduced_motion="no-preference",  # keep the CRT glow classes on
    )
    ctx.add_init_script(
        "try{localStorage.setItem('cronstable.boot','false');"
        "localStorage.setItem('cronstable.zen','false');"
        f"localStorage.setItem('cronstable.theme','\"{theme}\"');}}catch(e){{}}"
    )
    page = ctx.new_page()
    page.goto(f"http://127.0.0.1:{PORT}/index.html")
    page.wait_for_selector("#mark")
    page.evaluate("document.fonts.ready")  # settle glyph/wordmark metrics
    page.evaluate(
        "const s=document.createElement('style');s.id='gifDrive';"
        "s.textContent='#mark{transform:rotate(0deg) !important}';"
        "document.head.appendChild(s)"
    )
    box = page.evaluate("""() => {
      const rs = ['#mark', '#brandName', '.brand .tag']
        .map((s) => document.querySelector(s).getBoundingClientRect());
      const x = Math.min(...rs.map((r) => r.left));
      const y = Math.min(...rs.map((r) => r.top));
      return { x, y,
               width: Math.max(...rs.map((r) => r.right)) - x,
               height: Math.max(...rs.map((r) => r.bottom)) - y };
    }""")
    clip = {
        "x": max(0, box["x"] - PAD),
        "y": max(0, box["y"] - PAD),
        "width": box["width"] + 2 * PAD,
        "height": box["height"] + 2 * PAD,
    }

    paper = theme.endswith("-light")   # the ground the colour ghosts blend on
    seq, starts = journey(theme, stops)
    length = max(1, round(GLITCH_MS / FRAME_MS))
    inks = {
        c: (None if c == theme else dict(zip(INK_KEYS, THEME_INK[c])))
        for c in set(seq)
    }
    frames, glitch_reps = [], []
    for k in range(FRAMES):
        held = seq[sum(1 for s in starts if k >= s)]  # colour switches at each hop
        page.evaluate(
            """([deg, ink]) => new Promise((res) => {
              document.getElementById('gifDrive').textContent =
                `#mark{transform:rotate(${deg}deg) !important}`;
              const el = document.documentElement, keys =
                ['--fg','--fg-dim','--fg-faint','--accent','--glow'];
              if (ink) keys.forEach((k) => el.style.setProperty(k, ink[k]));
              else keys.forEach((k) => el.style.removeProperty(k));
              requestAnimationFrame(() => requestAnimationFrame(res));
            })""",
            [k * 360 / FRAMES_PER_REV, inks[held]],
        )
        img = Image.open(BytesIO(page.screenshot(clip=clip))).convert("RGB")
        for s in starts:
            if s <= k < s + length:
                img = spiderverse(img, glitch_env(k - s, length), random.Random(SEED + k), paper)
                if k == s + length // 2:
                    glitch_reps.append(k)
                break
        frames.append(img)
    ctx.close()

    # one shared palette (no per-frame requantize -> no palette flicker), no
    # dither (a shifting dither pattern would shimmer between frames). The
    # palette source stacks a clean frame from every held colour plus the middle
    # of each glitch, so the switched theme inks AND the saturated colour ghosts
    # (which differ frame to frame) all get real palette slots.
    w, h = frames[0].size
    seg_bounds = [0, *starts, FRAMES]
    clean_reps = [(seg_bounds[i] + seg_bounds[i + 1]) // 2 for i in range(len(seg_bounds) - 1)]
    picks = sorted(set(clean_reps + glitch_reps)) or [0]
    src = Image.new("RGB", (w, h * len(picks)))
    for i, fk in enumerate(picks):
        src.paste(frames[fk], (0, i * h))
    colors = 256 if starts else 128
    pal = src.quantize(colors=colors, method=Image.Quantize.MEDIANCUT)
    mapped = [f.quantize(palette=pal, dither=Image.Dither.NONE) for f in frames]

    OUT.mkdir(exist_ok=True)
    # WebP primary: full 24-bit RGB frames, no palette (the quality win). In
    # lossless mode `quality` is the compression *effort* (100 = smallest, still
    # pixel-perfect); in lossy mode it's fidelity.
    webp = OUT / f"{base}.webp"
    frames[0].save(
        webp, format="WEBP", save_all=True, append_images=frames[1:],
        duration=FRAME_MS, loop=0, method=WEBP_METHOD,
        lossless=WEBP_LOSSLESS,
        quality=100 if WEBP_LOSSLESS else WEBP_QUALITY,
    )
    # GIF twin: the 256-colour fallback
    gif = OUT / f"{base}.gif"
    mapped[0].save(
        gif, format="GIF", save_all=True, append_images=mapped[1:],
        duration=FRAME_MS, loop=0, optimize=True,
    )
    tour = " -> ".join(seq) if starts else theme
    bounds = [0, *starts, FRAMES]
    holds = "/".join(f"{(b - a) * FRAME_MS / 1000:.1f}" for a, b in zip(bounds, bounds[1:]))
    print(f"[img] {base}: {FRAMES}f @ {FRAME_MS}ms ({FRAMES * FRAME_MS / 1000:.1f}s), "
          f"{len(starts)} hops [{tour}], holds {holds}s "
          f"-> webp {webp.stat().st_size // 1024} KB, gif {gif.stat().st_size // 1024} KB")


class Quiet(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *a):
        pass


def main():
    handler = partial(Quiet, directory=str(WEB))
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", PORT), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    with sync_playwright() as p:
        browser = p.chromium.launch()
        for base, theme, stops in VARIANTS:
            capture(browser, theme, stops, base)
        browser.close()
    srv.shutdown()


if __name__ == "__main__":
    main()
