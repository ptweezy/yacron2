"""Render social-card.html to the 1280x640 GitHub social-preview PNG.

Needs no daemon: the card is a static page styled after the dashboard's
carolina theme, with docs/img/dashboard-overview.png inset for the product
shot (regenerate that first if the UI changed). The PNG lands in shots/;
GitHub wants it uploaded by hand under Settings -> General -> Social preview
(there is no API for it), and the upload limit is 1 MB.

Needs playwright (+ its Chromium) and Pillow.
"""
import http.server
import threading
from functools import partial
from io import BytesIO
from pathlib import Path

from PIL import Image
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[2]
WEB = ROOT / "cronstable" / "web"
OUT = Path(__file__).parent / "shots"
PORT = 8125


def engine_js():
    """Lift the logo-engine <script> block out of the dashboard page, so the
    card always wears the real mark (never a drifting copy)."""
    html = (WEB / "index.html").read_text(encoding="utf-8")
    i = html.index("logo engine — a real self-balancing")
    start = html.rindex("<script>", 0, i) + len("<script>")
    return html[start:html.index("</script>", i)]


class Quiet(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *a):
        pass


def main():
    handler = partial(Quiet, directory=str(ROOT))
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", PORT), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 640})
        page.goto(f"http://127.0.0.1:{PORT}/docs/screenshots/social-card.html")
        page.evaluate("document.fonts.ready")
        page.wait_for_function(
            "const i = document.querySelector('.shot');"
            "i.complete && i.naturalWidth > 0"
        )
        # mount the pendulum as the wordmark's l, parked balanced-upright
        # (reducedMotion -> the engine's honest still pose for a live daemon)
        page.add_script_tag(content=engine_js())
        page.evaluate(
            "CronstableLogo.mountGlyph(document.getElementById('mark'),"
            " { connected: () => true, reducedMotion: () => true })"
        )
        png = page.screenshot()
        browser.close()
    srv.shutdown()
    OUT.mkdir(exist_ok=True)
    img = Image.open(BytesIO(png))
    img.save(OUT / "social-preview.png", optimize=True)
    kb = (OUT / "social-preview.png").stat().st_size // 1024
    print(f"[shot] social-preview.png {img.size[0]}x{img.size[1]}, {kb} KB"
          + (" (over GitHub's 1 MB upload limit!)" if kb > 1024 else ""))


if __name__ == "__main__":
    main()
