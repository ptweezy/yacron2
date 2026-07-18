"""The dashboard's client-side cron engine matches the daemon's, live.

The web page ships a second implementation of the schedule dialect (see
``cronstable/web/index.html``: ``parseCron``/``nextRuns``/``describeCron``),
deliberately, so previews cost no round-trips. Two engines can drift, so
this differential replays the whole golden corpus through BOTH: the client
functions are extracted from the page and driven in a real Chromium via
Playwright, and every mutually-valid expression must agree on the next
eight fire instants and on the plain-English description, byte for byte.

Runs only where Playwright and its Chromium build are installed (a dev
machine after ``pip install playwright && playwright install chromium``,
never CI), exactly like the legacy-library differential in
``test_cronexpr.py``: CI relies on the committed vectors and unit suites.

One asymmetry is deliberate and asserted AS an asymmetry: the client
parser is tolerant of out-of-range values and steps (it degrades while
the user is typing; the daemon is authoritative), so an expression the
daemon rejects MAY still parse client-side. The reverse is a bug: the
client must never reject an expression the daemon accepts (a job the
daemon runs would lose its previews), except ``@reboot`` (a scheduler
concept the client flags as its own type) and ``H`` forms (resolved
server-side; the client parses ``schedule_resolved`` instead).
"""

import datetime
import itertools
import json
import os

import pytest

from cronstable.cronexpr import CronTab
from cronstable.croninfo import describe_cron

playwright_api = pytest.importorskip("playwright.sync_api")

GOLDEN = os.path.join(os.path.dirname(__file__), "data", "cron_golden.json")
INDEX = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "cronstable",
    "web",
    "index.html",
)

#: fixed naive instant, read as UTC on both sides
_FROM = datetime.datetime(2026, 1, 7, 12, 0, 30)
_FROM_MS = int(
    _FROM.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000
)
_COUNT = 8


def _engine_script() -> str:
    """The client engine, sliced out of the page by its section markers."""
    with open(INDEX, encoding="utf-8") as f:
        html = f.read()
    start = html.index("  //  cron schedule intelligence  (client-side)")
    end = html.index("  function listJoin(arr) {")
    end += html[end : end + 400].index("\n  }") + 4
    return (
        'const pad2 = (n) => String(n).padStart(2, "0");\n'
        + html[start:end]
        + "\nwindow.__engine = { parseCron, nextRuns, describeCron };"
    )


def _python_side(exprs):
    out = {}
    for expr in exprs:
        try:
            tab = CronTab(expr)
        except (ValueError, KeyError):
            out[expr] = {"valid": False}
            continue
        fires = [
            int(
                dt.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000
            )
            for dt in itertools.islice(tab.occurrences(_FROM), _COUNT)
        ]
        out[expr] = {
            "valid": True,
            "fires": fires,
            "describe": describe_cron(expr),
        }
    return out


def test_client_engine_matches_daemon_engine_over_the_golden_corpus():
    with open(GOLDEN, encoding="utf-8") as f:
        exprs = sorted(json.load(f)["exprs"])
    expected = _python_side(exprs)

    with playwright_api.sync_playwright() as p:
        try:
            browser = p.chromium.launch()
        except Exception as exc:  # no chromium provisioned
            pytest.skip("playwright chromium unavailable: {}".format(exc))
        page = browser.new_page()
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.set_content("<html><body></body></html>")
        page.add_script_tag(content=_engine_script())
        got = page.evaluate(
            """(args) => {
                const out = {};
                for (const expr of args.exprs) {
                    const p = window.__engine.parseCron(expr);
                    if (p.type !== "fields") {
                        out[expr] = { valid: false, type: p.type };
                        continue;
                    }
                    const runs = window.__engine.nextRuns(
                        p, args.count, "UTC", args.fromMs);
                    out[expr] = {
                        valid: true,
                        fires: runs.map((x) => x.t),
                        describe: window.__engine.describeCron(expr),
                    };
                }
                return out;
            }""",
            {"exprs": exprs, "fromMs": _FROM_MS, "count": _COUNT},
        )
        browser.close()

    assert not errors, errors
    for expr in exprs:
        py, js = expected[expr], got[expr]
        if not py["valid"]:
            # the client MAY be more tolerant; never assert it rejects
            continue
        if not js["valid"]:
            # @reboot and unkeyed H forms never reach here (the daemon
            # rejects both without a hash key, so py["valid"] is False);
            # any hit is a real preview-losing gap
            pytest.fail(
                "client rejected a daemon-valid expression: "
                "{!r}".format(expr)
            )
        assert py["fires"] == js["fires"], expr
        assert py["describe"] == js["describe"], expr
