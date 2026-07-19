"""``docs/demo/index.html`` stays a faithful mirror of the shipped dashboard.

The demo page is the shipped dashboard plus a fake-backend layer, maintained
by hand: no build step generates it, so every dashboard edit has to be ported
across twice.  That discipline is invisible when it lapses, and a stale demo
silently misrepresents the product on the docs site, so this pins the mirror
structurally instead.

Only the deltas the mirror exists for are allowed: the ``<title>``, the
injected ``cronstable-demo-backend`` script block together with the
demo-only note that follows it about the inlined logo engine, and the blank
line at the injection point that the block consumes.  Anything else is drift.
Pure text comparison, so unlike ``test_web_engine_parity`` this runs
everywhere, including CI.
"""

import difflib
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEB = os.path.join(ROOT, "cronstable", "web", "index.html")
DEMO = os.path.join(ROOT, "docs", "demo", "index.html")

# The injected block, matched by its script id (kept stable for exactly this
# reason, cf. the banner comments test_web_engine_parity slices on), plus the
# demo-only note that trails it explaining why the logo engine is inlined.
# Both are part of the same insertion, so they are stripped together.
_DEMO_BLOCK = re.compile(
    r'[ \t]*<script id="cronstable-demo-backend">.*?</script>\n'
    r"(?:<!-- logo engine: copied verbatim.*?-->\n)?",
    re.DOTALL,
)
_TITLE = re.compile(r"<title>.*?</title>", re.DOTALL)


def _read(path):
    # newline="" so a stray CRLF shows up as drift rather than being
    # normalised away; the whole repo is LF.
    with open(path, encoding="utf-8", newline="") as fh:
        return fh.read()


def test_demo_is_the_dashboard_plus_only_its_fake_backend():
    web = _read(WEB)
    demo = _read(DEMO)

    stripped, count = _DEMO_BLOCK.subn("", demo)
    assert count == 1, (
        "expected exactly one <script id='cronstable-demo-backend'> block in "
        "%s, found %d. If the block was renamed, update this test; it is the "
        "anchor the mirror check relies on." % (DEMO, count)
    )
    # normalise the one intentional content delta
    stripped = _TITLE.sub(_TITLE.search(web).group(0), stripped, count=1)

    web_lines = web.splitlines(keepends=True)
    demo_lines = stripped.splitlines(keepends=True)
    if web_lines == demo_lines:
        return

    # The injection point absorbs a single blank line; tolerate that one
    # difference and nothing else.
    diff = [
        line
        for line in difflib.unified_diff(
            web_lines, demo_lines, "web", "demo", n=0
        )
        if line[:1] in "+-" and line[:3] not in ("+++", "---")
    ]
    assert diff == ["-\n"], (
        "cronstable/web/index.html and docs/demo/index.html have drifted "
        "apart. Port the change to both copies. Unexpected differences:\n"
        + "".join(diff[:40])
    )


def test_demo_mirror_has_no_crlf():
    # A Windows editor or a Python open(..., "w") without newline="" rewrites
    # the whole file CRLF, which shows up as a several-thousand-line diff and
    # trips the repo's LF-only CI check.
    for path in (WEB, DEMO):
        assert "\r\n" not in _read(path), "%s picked up CRLF endings" % path
