"""Capture the header brand (spinning logo + wordmark) as seamless-loop GIFs.

Unlike the dashboard PNGs this needs NO running daemon: the page is served
straight off the working tree, and the mark's rotation is driven angle-by-angle
through an injected `!important` style (which outranks the app's own rAF
inline transform). Every frame lands on an exact fraction of a revolution, so
the last frame wraps perfectly onto the first and the loop never stutters.

The replay speed is derived from the page's MARK_CRUISE constant, so a retune
there keeps the GIF honest on the next regen.

Needs playwright (+ its Chromium) and Pillow. GIFs land in shots/.
"""
import http.server
import re
import threading
from functools import partial
from io import BytesIO
from pathlib import Path

from PIL import Image
from playwright.sync_api import sync_playwright

WEB = Path(__file__).resolve().parents[2] / "cronstable" / "web"
OUT = Path(__file__).parent / "shots"
PORT = 8123

FRAMES = 52   # one full revolution per loop
PAD = 12      # css px around the brand box (glow + rotation overflow)
SCALE = 2     # device pixels per css px, matches the PNG set

VARIANTS = [
    ("logo-spin.gif", "carolina"),
    ("logo-spin-light.gif", "carolina-light"),
]

# the page's resting spin rate, deg/s
CRUISE = float(
    re.search(
        r"MARK_CRUISE\s*=\s*([\d.]+)",
        (WEB / "index.html").read_text(encoding="utf-8"),
    ).group(1)
)
# ms per frame for a true-speed replay; decoders clamp delays below ~20ms
FRAME_MS = max(20, round(360 / CRUISE * 1000 / FRAMES / 10) * 10)


def capture(browser, theme, fname):
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
    frames = []
    for k in range(FRAMES):
        page.evaluate(
            "(d) => new Promise((res) => {"
            "  document.getElementById('gifDrive').textContent ="
            "    `#mark{transform:rotate(${d}deg) !important}`;"
            "  requestAnimationFrame(() => requestAnimationFrame(res));"
            "})",
            k * 360 / FRAMES,
        )
        frames.append(
            Image.open(BytesIO(page.screenshot(clip=clip))).convert("RGB")
        )
    ctx.close()
    # one shared palette (no per-frame requantize -> no palette flicker), no
    # dither (a shifting dither pattern would shimmer between frames)
    base = frames[0].quantize(colors=128, method=Image.Quantize.MEDIANCUT)
    pal = [base] + [
        f.quantize(palette=base, dither=Image.Dither.NONE) for f in frames[1:]
    ]
    OUT.mkdir(exist_ok=True)
    pal[0].save(
        OUT / fname,
        save_all=True,
        append_images=pal[1:],
        duration=FRAME_MS,
        loop=0,
        optimize=True,
    )
    kb = (OUT / fname).stat().st_size // 1024
    print(f"[gif] {fname}: {FRAMES}f @ {FRAME_MS}ms ({theme}), {kb} KB")


class Quiet(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *a):
        pass


def main():
    handler = partial(Quiet, directory=str(WEB))
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", PORT), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    with sync_playwright() as p:
        browser = p.chromium.launch()
        for fname, theme in VARIANTS:
            capture(browser, theme, fname)
        browser.close()
    srv.shutdown()


if __name__ == "__main__":
    main()
