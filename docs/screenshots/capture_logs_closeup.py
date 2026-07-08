"""Capture the single-job live-log-tail closeup from the logs-demo daemon."""

import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE = "http://127.0.0.1:8899"
OUT = Path(__file__).parent / "shots"


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(
            viewport={
                "width": 1680,
                "height": 1050,
            },  # match the main capture set
            device_scale_factor=2,
            bypass_csp=True,
            reduced_motion="no-preference",
        )
        ctx.route(
            "**/version",
            lambda route: route.fulfill(
                status=200,
                content_type="text/plain; charset=utf-8",
                body="1.2.8",
            ),
        )
        ctx.add_init_script(
            "try{localStorage.setItem('cronstable.boot','false');"
            "localStorage.setItem('cronstable.zen','false');}catch(e){}"
            # pin the idle-cruising logo upright (see capture_dashboard.py)
            "document.addEventListener('DOMContentLoaded',()=>{"
            "const s=document.createElement('style');"
            "s.textContent='#mark{transform:none !important}';"
            "document.head.appendChild(s)});"
        )
        page = ctx.new_page()
        page.goto(BASE)
        page.wait_for_function(
            "document.querySelectorAll('#rows tr').length >= 5", timeout=30000
        )
        # start the chatty run, then open its drawer and let lines stream in
        urllib.request.urlopen(
            urllib.request.Request(
                BASE + "/jobs/orders-ingest/start", method="POST"
            ),
            timeout=5,
        )
        page.wait_for_timeout(2500)
        row = page.locator("#rows tr", has_text="orders-ingest").first
        row.click()
        page.wait_for_selector("#drawer.open", timeout=5000)
        page.check("#optTs")
        page.wait_for_timeout(9000)  # ~15 colored lines by now, still running
        page.fill("#logSearch", "rows")
        page.wait_for_timeout(700)
        page.screenshot(path=str(OUT / "dashboard-logs.png"))
        print("[shot] dashboard-logs (local demo)")
        browser.close()


if __name__ == "__main__":
    main()
