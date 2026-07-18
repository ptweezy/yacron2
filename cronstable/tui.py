"""A terminal (TUI) rendition of the cronstable web dashboard.

``cronstable tui`` opens a keyboard-driven control room in the terminal,
talking to a running daemon over the same HTTP control API the web
dashboard uses (``GET /jobs``, the SSE log streams, ``POST .../start``,
and friends; see the HTTP-API wiki page).  It works against a local or
remote daemon and needs nothing on the daemon side beyond the existing
``web:`` listener.

Design notes, in the spirit of the rest of the codebase:

* **No new dependencies.**  The terminal layer (raw mode, ANSI painting,
  key decoding, themes) is hand-rolled on the stdlib, exactly as the MCP
  server hand-rolls JSON-RPC; the HTTP/SSE client rides the core
  ``aiohttp`` dependency the daemon already carries.  ``aiohttp`` is
  imported lazily so registering the subcommand keeps ``cronstable
  --help`` (and the other thin CLIs) fast.
* **Keyboard parity with the web dashboard.**  The web page's shortcut
  map is mirrored key for key (``j``/``k``, ``Enter``, ``r``/``x``,
  ``g``/``t``/``T``/``i``/``w``/``a``, ``/``, ``?``, ``Ctrl-K``
  palette, ``Esc`` close-priority), including its guard semantics: list
  keys are suppressed while an overlay is open or a text field is
  focused, and modifier chords fall through.  Terminal-only additions
  (drawer tab switching, ``q`` to quit, sort/filter cycling) are grouped
  separately in the ``?`` overlay so the shared muscle memory stays
  honest.
* **Same client semantics as the page.**  The status classifier, sort
  order, failure-verdict correlation, palette fuzzy scoring, and the
  plain-English schedule text are line-for-line ports of the web UI's
  client-side logic, so both frontends always agree on what they say.
* **Windows and POSIX alike.**  Key input uses ``termios`` +
  ``loop.add_reader`` on POSIX and an ``msvcrt`` reader thread on
  Windows (the Proactor loop cannot watch stdin); ANSI output is enabled
  on Windows via ``SetConsoleMode``.  All of it follows the
  ``sys.platform`` guard idiom from :mod:`cronstable.platform`.

The module is deliberately one file, like the web dashboard it mirrors
(one self-contained ``index.html``): sections below are ordered
utilities -> terminal engine -> API client -> views -> app -> CLI.
"""

import asyncio
import base64
import codecs
import contextlib
import datetime
import functools
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import textwrap
import time
import unicodedata
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    cast,
)

from cronstable.cronexpr import CronTab
from cronstable.croninfo import (  # noqa: F401  (re-exported for tests/back-compat)
    Finding,
    ScheduleEntry,
    _local_tzinfo,
    describe_cron,
    duplicate_schedules,
    lint_schedule,
    next_fires,
    pad2,
    schedule_pressure,
    suggest_slot,
)
from cronstable.platform import IS_WINDOWS

if sys.platform == "win32":  # pragma: no cover - exercised on Windows only
    import ctypes
    import msvcrt
    import threading
else:
    import fcntl  # noqa: F401  (re-exported guard parity; unused directly)
    import signal
    import termios
    import tty

logger = logging.getLogger("tui")

#: Client-side conventions shared with the web dashboard (same values).
DEFAULT_URL = "http://127.0.0.1:8080"
ENV_TOKEN = "CRONSTABLE_WEB_TOKEN"

#: Poll cadence choices (ms), mirroring the web settings sheet; 0 = paused.
POLL_CHOICES = [1000, 2000, 3000, 5000, 10000, 0]
DEFAULT_POLL_MS = 3000

#: Failures whose finishes fall inside this window may share a cause
#: (verdict correlation); the same constant as the web UI.
CORR_WINDOW_MS = 60000

#: Wallboard "NO SIGNAL" floor: data older than max(this, 2 polls) is stale.
WB_STALE_AFTER_MS = 15000

#: Multi-tail: max concurrent SSE streams and the re-attach throttle,
#: mirroring TAIL_MAX / TAIL_RETRY_MS in the web page.
TAIL_MAX = 4
TAIL_RETRY_MS = 5000

#: The boot self-test replays after this long, like the web page's.
BOOT_EVERY_S = 12 * 3600

#: Status glyphs, identical to the web dashboard's GLYPH map, with a
#: plain-ASCII fallback for terminals/fonts that lack them (--ascii).
GLYPH = {
    "ok": "●",
    "fail": "✕",
    "run": "▶",
    "pending": "◔",
    "disabled": "◌",
    "cancelled": "⊘",
    "unknown": "◍",
}
GLYPH_ASCII = {
    "ok": "o",
    "fail": "x",
    "run": ">",
    "pending": ".",
    "disabled": "-",
    "cancelled": "/",
    "unknown": "?",
}

#: Sort rank of each health key in the jobs table (web STATUS_ORDER).
STATUS_ORDER = {
    "run": 0,
    "fail": 1,
    "pending": 2,
    "unknown": 3,
    "cancelled": 4,
    "ok": 5,
    "disabled": 6,
}

#: Wallboard tile order: worst first (web WB_ORDER).
WB_ORDER = {
    "fail": 0,
    "run": 1,
    "pending": 2,
    "unknown": 3,
    "cancelled": 4,
    "ok": 5,
    "disabled": 6,
}

#: Status-filter segments, in toolbar order (web segment row).
STATUS_SEGMENTS = ["all", "ok", "fail", "run", "off"]

#: Sort keys, in the web dropdown's order.
SORT_KEYS = ["name", "status", "last", "next", "duration"]


# ===================================================================
#  small formatting helpers (ports of the web page's fmt* family)
#  (pad2 moved to cronstable.croninfo with the schedule describers)
# ===================================================================
def fmt_in(sec: Optional[float]) -> str:
    """``in 42s`` / ``in 3m`` / ``now``: the next-fire column."""
    if sec is None:
        return "—"
    if sec <= 0:
        return "now"
    if sec < 60:
        return "in %ds" % int(sec)
    if sec < 3600:
        return "in %dm" % (sec // 60)
    if sec < 172800:
        return "in %dh" % (sec // 3600)
    return "in %dd" % (sec // 86400)


def fmt_ago(iso: Optional[str], now: Optional[float] = None) -> str:
    """``42s ago`` / ``3m ago`` from an ISO timestamp."""
    t = parse_iso(iso)
    if t is None:
        return "—"
    delta = (now if now is not None else time.time()) - t
    if delta < 0:
        delta = 0
    if delta < 60:
        return "%ds ago" % int(delta)
    if delta < 3600:
        return "%dm ago" % (delta // 60)
    if delta < 172800:
        return "%dh ago" % (delta // 3600)
    return "%dd ago" % (delta // 86400)


def fmt_ago_any(value: Any, now: Optional[float] = None) -> str:
    """:func:`fmt_ago` over either an ISO string or epoch seconds.

    DAG run documents stamp epoch floats (``createdAt``/``updatedAt``)
    where job payloads use ISO strings; the age column takes both.
    """
    if isinstance(value, bool) or value is None:
        return "—"
    if isinstance(value, (int, float)):
        stamp = datetime.datetime.fromtimestamp(
            float(value), tz=datetime.timezone.utc
        ).isoformat()
        return fmt_ago(stamp, now)
    return fmt_ago(value, now)


def ago_short(iso: Optional[str], now: Optional[float] = None) -> str:
    """Compact age for dense cells: ``42s`` / ``3m`` / ``7h`` / ``2d``."""
    t = parse_iso(iso)
    if t is None:
        return "?"
    delta = max(0.0, (now if now is not None else time.time()) - t)
    if delta < 60:
        return "%ds" % int(delta)
    if delta < 3600:
        return "%dm" % (delta // 60)
    if delta < 172800:
        return "%dh" % (delta // 3600)
    return "%dd" % (delta // 86400)


def fmt_duration(sec: Optional[float]) -> str:
    """``850ms`` / ``4.2s`` / ``3m10s`` / ``2h04m`` for run durations."""
    if sec is None:
        return "—"
    if sec < 1:
        return "%dms" % round(sec * 1000)
    if sec < 60:
        return ("%.1fs" % sec) if sec < 10 else ("%ds" % round(sec))
    if sec < 3600:
        return "%dm%02ds" % (sec // 60, round(sec % 60))
    return "%dh%02dm" % (sec // 3600, (sec % 3600) // 60)


def fmt_countdown(sec: float) -> str:
    """Radar countdown: ``mm:ss`` under an hour, ``XhYYm`` above."""
    s = max(0, round(sec))
    if s >= 3600:
        m0 = round(s / 60)
        return "%dh%sm" % (m0 // 60, pad2(m0 % 60))
    return "%s:%s" % (pad2(s // 60), pad2(s % 60))


def fmt_percent(p: Optional[float]) -> str:
    if p is None:
        return "—"
    return ("%.1f%%" % p) if p < 10 else ("%d%%" % round(p))


def fmt_bytes(n: Optional[float]) -> str:
    if n is None:
        return "—"
    value = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            if unit == "B":
                return "%d%s" % (value, unit)
            return "%.1f%s" % (value, unit)
        value /= 1024
    return "—"  # pragma: no cover - unreachable


def parse_iso(value: Any) -> Optional[float]:
    """ISO-8601 -> POSIX seconds; naive stamps are pinned to UTC.

    Mirrors the daemon's own tolerant parser: the payloads cronstable emits
    are always aware UTC, but the TUI must not crash on a foreign record.
    """
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed.timestamp()


def utc_clock(now: Optional[float] = None) -> str:
    t = datetime.datetime.fromtimestamp(
        now if now is not None else time.time(), tz=datetime.timezone.utc
    )
    return t.strftime("%H:%M:%S UTC")


# ===================================================================
#  status / health / verdict  (ports of the web page's client logic)
# ===================================================================
def health(job: Dict[str, Any]) -> Tuple[str, str]:
    """``(key, label)`` for a /jobs entry: the web ``health()`` port."""
    if not job.get("enabled"):
        return ("disabled", "Disabled")
    if job.get("running"):
        return ("run", "Running")
    last = job.get("last_run")
    if last:
        outcome = last.get("outcome")
        if outcome == "failure":
            return ("fail", "Failed")
        if outcome == "cancelled":
            return ("cancelled", "Cancelled")
        if outcome == "unknown":
            return ("unknown", "Unknown")
        return ("ok", "OK")
    return ("pending", "Pending")


def segment_of(key: str) -> str:
    """Map a health key onto the toolbar's status segments."""
    if key == "disabled":
        return "off"
    if key in ("ok", "fail", "run"):
        return key
    return ""  # pending/unknown/cancelled match only "all"


def correlate(
    failing: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Group failing jobs by (exit_code, fail_reason); dominant group wins.

    Port of the web ``correlate()``: the returned dict carries the group
    size ``n``, the finish-time ``span`` (ms), the shared ``exit`` /
    ``reason``, and the member ``jobs``.
    """
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for job in failing:
        last = job.get("last_run") or {}
        exit_code = last.get("exit_code")
        key = "%s|%s" % (
            "?" if exit_code is None else exit_code,
            last.get("fail_reason") or "",
        )
        groups.setdefault(key, []).append(job)
    best: Optional[Dict[str, Any]] = None
    for group in groups.values():
        if len(group) < 2:
            continue
        times = [
            t
            for t in (
                parse_iso((j.get("last_run") or {}).get("finished_at"))
                for j in group
            )
            if t is not None
        ]
        span = (max(times) - min(times)) * 1000 if times else 0.0
        if best is None or len(group) > best["n"]:
            last = group[0].get("last_run") or {}
            best = {
                "n": len(group),
                "span": span,
                "exit": last.get("exit_code"),
                "reason": last.get("fail_reason"),
                "jobs": group,
            }
    return best


def cluster_alert(data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Distill /cluster into one alert signal (web ``setClusterAlert``)."""
    if not data or not data.get("enabled"):
        return None
    bad = False
    reason = ""
    if data.get("elect_leader"):
        if data.get("conflict"):
            if data.get("conflict_names"):
                reason = "duplicate nodeName"
            elif data.get("size_conflict"):
                reason = "cluster size mismatch"
            elif data.get("policy_conflict"):
                reason = "coordination policy mismatch"
            else:
                reason = "conflict"
            reason += " — Leader jobs paused"
            bad = True
        elif not data.get("quorate"):
            backend = data.get("backend")
            reason = (
                "lease store unreachable — Leader jobs failing closed"
                if backend and backend != "gossip"
                else "no quorum — Leader jobs paused"
            )
            bad = True
    return {"bad": bad, "reason": reason, "node": data.get("node_name")}


def verdict_info(
    jobs: List[Dict[str, Any]],
    alert: Optional[Dict[str, Any]],
) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """One operator headline, or ``None`` when healthy.

    Port of the web ``verdictInfo()``: returns ``(verdict, incident_set)``
    where verdict is ``{sev, glyph, head, sub, ago?}`` and incident_set is
    the blast-radius job-name list the timeline/mitigate consoles use.
    """
    failing = [j for j in jobs if health(j)[0] == "fail"]
    corr = correlate(failing) if len(failing) > 1 else None
    incident = [
        j["name"]
        for j in (corr["jobs"] if corr and corr["n"] >= 2 else failing)
    ]
    if alert and alert.get("bad"):
        head = "CLUSTER ALERT — " + str(alert.get("reason", "")).upper()
        sub = (
            ("this node: %s · " % alert["node"]) if alert.get("node") else ""
        ) + (
            "%d job%s also failing"
            % (len(failing), "s" if len(failing) > 1 else "")
            if failing
            else "leadership / quorum degraded"
        )
        return (
            {"sev": "crit", "glyph": "☢", "head": head, "sub": sub},
            incident,
        )
    if not failing:
        return (None, [])
    if len(failing) == 1:
        job = failing[0]
        last = job.get("last_run") or {}
        exit_code = last.get("exit_code")
        sub = "exit %s" % ("?" if exit_code is None else exit_code)
        if last.get("fail_reason"):
            sub += " · %s" % last["fail_reason"]
        return (
            {
                "sev": "warn",
                "glyph": "▲",
                "head": "JOB FAILING — %s" % job["name"],
                "sub": sub,
                "ago": last.get("finished_at"),
            },
            incident,
        )
    head = "FLEET EVENT — %d jobs failing" % len(failing)
    if corr:
        exit_code = "?" if corr["exit"] is None else corr["exit"]
        sub = "×%d share exit=%s" % (corr["n"], exit_code)
        if corr["reason"]:
            sub += " (%s)" % corr["reason"]
        if corr["span"] <= CORR_WINDOW_MS:
            sub += " within %ds" % max(1, round(corr["span"] / 1000))
        sub += " — likely one cause"
    else:
        sub = "no shared failure signature — likely independent"
    return ({"sev": "crit", "glyph": "▲", "head": head, "sub": sub}, incident)


def fuzzy(query: str, label: str) -> int:
    """The palette's fuzzy score, an exact port of the web ``fuzzy()``.

    Substring match scores ``100 - index`` (earlier is better), a scattered
    subsequence scores 1, no match scores 0, an empty query scores 1.
    """
    q = query.lower()
    s = label.lower()
    if not q:
        return 1
    idx = s.find(q)
    if idx >= 0:
        return 100 - idx
    qi = 0
    for ch in s:
        if qi < len(q) and ch == q[qi]:
            qi += 1
    return 1 if qi == len(q) else 0


def compute_view(
    jobs: List[Dict[str, Any]],
    filter_text: str,
    status_filter: str,
    sort_key: str,
    sort_dir: int,
) -> List[Dict[str, Any]]:
    """Filter + sort the job list (web ``computeView`` port).

    The text filter substring-matches name OR command, lowercased; the
    status segments match the health key (``off`` = disabled); ties always
    break on name so the order is stable under equal keys.
    """
    needle = filter_text.strip().lower()
    view = []
    for job in jobs:
        if needle and (
            needle not in job.get("name", "").lower()
            and needle not in job.get("command", "").lower()
        ):
            continue
        key = health(job)[0]
        if status_filter != "all" and segment_of(key) != status_filter:
            continue
        view.append(job)

    def sort_value(job: Dict[str, Any]) -> Tuple[Any, str]:
        name = job.get("name", "")
        if sort_key == "status":
            return (STATUS_ORDER.get(health(job)[0], 9), name)
        if sort_key == "last":
            last = job.get("last_run") or {}
            t = parse_iso(last.get("finished_at"))
            # newest first under ascending sort, like the web table
            return (-(t or 0.0), name)
        if sort_key == "next":
            sched = job.get("scheduled_in")
            return (sched if sched is not None else float("inf"), name)
        if sort_key == "duration":
            last = job.get("last_run") or {}
            dur = last.get("duration")
            return (-(dur if dur is not None else -1.0), name)
        return (name, name)

    view.sort(key=sort_value, reverse=sort_dir < 0)
    return view


# ===================================================================
#  cron schedule intelligence: describe_cron / next_fires / the linter
#  moved to cronstable.croninfo (imported and re-exported above), so
#  the daemon's /schedule/preview endpoint and the TUI share one
#  implementation instead of drifting apart.
# ===================================================================


# ===================================================================
#  themes: the web dashboard's ten looks, re-inked in ANSI
# ===================================================================
#: hue -> (dark aka phosphor, light aka paper) palettes.  Each palette is
#: a flat name->#rrggbb map; the painter turns them into SGR sequences.
#: Same five hues and the same t / T cycling as the web page.
THEME_HUES = ["carolina", "amber", "green", "modern", "standard"]

_P = {
    # carolina: the default Carolina-blue CRT phosphor
    "carolina": {
        "bg": "#06131d",
        "fg": "#9ed3f5",
        "bright": "#d3ecfd",
        "dim": "#3f6d8c",
        "accent": "#4b9cd3",
        "border": "#1d4056",
        "sel": "#12324a",
        "ok": "#37d495",
        "fail": "#ff5d5d",
        "run": "#4b9cd3",
        "pending": "#c9a94a",
        "warn": "#ffb64a",
        "off": "#3f5b6e",
    },
    "carolina-light": {
        "bg": "#eef4f9",
        "fg": "#173751",
        "bright": "#0a2437",
        "dim": "#6d8ba1",
        "accent": "#20618f",
        "border": "#b9cedd",
        "sel": "#cfe2f0",
        "ok": "#0d7a4f",
        "fail": "#c22929",
        "run": "#20618f",
        "pending": "#8a6d1c",
        "warn": "#a05e00",
        "off": "#8aa0b1",
    },
    # amber phosphor CRT
    "amber": {
        "bg": "#160d02",
        "fg": "#f5c169",
        "bright": "#ffe3ad",
        "dim": "#8a6a34",
        "accent": "#ffb000",
        "border": "#4d3510",
        "sel": "#3a2a0c",
        "ok": "#8fd44a",
        "fail": "#ff5d43",
        "run": "#ffb000",
        "pending": "#c9a94a",
        "warn": "#ffcf6a",
        "off": "#6e5a35",
    },
    "amber-light": {
        "bg": "#faf3e4",
        "fg": "#4a3510",
        "bright": "#2d1f05",
        "dim": "#9a8557",
        "accent": "#9a6b00",
        "border": "#e0d0a8",
        "sel": "#f0e2bd",
        "ok": "#3d7a0d",
        "fail": "#c23415",
        "run": "#9a6b00",
        "pending": "#8a6d1c",
        "warn": "#a05e00",
        "off": "#a8956a",
    },
    # green phosphor CRT
    "green": {
        "bg": "#03130a",
        "fg": "#7ee2a1",
        "bright": "#c8f7d8",
        "dim": "#37744e",
        "accent": "#33ff66",
        "border": "#124d28",
        "sel": "#0c3a1e",
        "ok": "#33ff66",
        "fail": "#ff6e5d",
        "run": "#57d9ff",
        "pending": "#c9c94a",
        "warn": "#ffd24a",
        "off": "#3f6e51",
    },
    "green-light": {
        "bg": "#eef8f0",
        "fg": "#123a22",
        "bright": "#07230f",
        "dim": "#5f8f70",
        "accent": "#0e7a33",
        "border": "#b6dcc2",
        "sel": "#cdeccf",
        "ok": "#0e7a33",
        "fail": "#c22f18",
        "run": "#106a8a",
        "pending": "#7a7a10",
        "warn": "#a05e00",
        "off": "#7fa38c",
    },
    # flat modern (no CRT physics on the web; plain here too)
    "modern": {
        "bg": "#101418",
        "fg": "#d7dde3",
        "bright": "#ffffff",
        "dim": "#788591",
        "accent": "#5aa7e8",
        "border": "#2a343d",
        "sel": "#22303c",
        "ok": "#4cc38a",
        "fail": "#f2555a",
        "run": "#5aa7e8",
        "pending": "#d6a648",
        "warn": "#f0a13c",
        "off": "#5c6770",
    },
    "modern-light": {
        "bg": "#f7f9fb",
        "fg": "#22303c",
        "bright": "#101418",
        "dim": "#7d8b98",
        "accent": "#1d6cb0",
        "border": "#d4dde4",
        "sel": "#dbe7f1",
        "ok": "#177a4c",
        "fail": "#c62f34",
        "run": "#1d6cb0",
        "pending": "#8a6d1c",
        "warn": "#a05e00",
        "off": "#93a1ad",
    },
    # plain white-and-saturated-color "standard"
    "standard": {
        "bg": "#000000",
        "fg": "#c0c0c0",
        "bright": "#ffffff",
        "dim": "#707070",
        "accent": "#3b78ff",
        "border": "#3a3a3a",
        "sel": "#264f78",
        "ok": "#16c60c",
        "fail": "#e74856",
        "run": "#3b78ff",
        "pending": "#c19c00",
        "warn": "#f9f1a5",
        "off": "#767676",
    },
    "standard-light": {
        "bg": "#ffffff",
        "fg": "#1f1f1f",
        "bright": "#000000",
        "dim": "#767676",
        "accent": "#0037da",
        "border": "#d0d0d0",
        "sel": "#cde5ff",
        "ok": "#107c10",
        "fail": "#c42b1c",
        "run": "#0037da",
        "pending": "#805b00",
        "warn": "#9d5d00",
        "off": "#909090",
    },
}

#: Colour-vision remaps (web "color vision" setting): status colours are
#: re-inked so ok/fail/pending stay apart for red-green (deutan) and
#: blue-yellow (tritan) colour blindness; glyph shapes differ regardless.
_CVD = {
    "none": {},
    "deutan": {"ok": "#4aa8ff", "fail": "#ffb000", "pending": "#c8c8c8"},
    "tritan": {"ok": "#4cc38a", "fail": "#ff5d8a", "pending": "#c8c8c8"},
}
CVD_MODES = ["none", "deutan", "tritan"]


class Theme:
    """One resolved theme: named colours -> ready-made SGR fragments."""

    def __init__(self, hue: str, light: bool, cvd: str = "none") -> None:
        self.hue = hue if hue in THEME_HUES else THEME_HUES[0]
        self.light = light
        self.cvd = cvd if cvd in _CVD else "none"
        name = self.hue + ("-light" if light else "")
        palette = dict(_P.get(name, _P["carolina"]))
        palette.update(_CVD[self.cvd])
        self.colors = palette

    @property
    def name(self) -> str:
        return self.hue + ("-light" if self.light else "")

    def fg(self, key: str) -> str:
        return _sgr_fg(self.colors.get(key, self.colors["fg"]))

    def bg(self, key: str) -> str:
        return _sgr_bg(self.colors.get(key, self.colors["bg"]))


def _hex_rgb(spec: str) -> Tuple[int, int, int]:
    spec = spec.lstrip("#")
    return (int(spec[0:2], 16), int(spec[2:4], 16), int(spec[4:6], 16))


def _sgr_fg(spec: str) -> str:
    r, g, b = _hex_rgb(spec)
    return "\x1b[38;2;%d;%d;%dm" % (r, g, b)


def _sgr_bg(spec: str) -> str:
    r, g, b = _hex_rgb(spec)
    return "\x1b[48;2;%d;%d;%dm" % (r, g, b)


RESET = "\x1b[0m"
BOLD = "\x1b[1m"
DIM_SGR = "\x1b[2m"
REVERSE = "\x1b[7m"


# ===================================================================
#  text measurement + ANSI handling
# ===================================================================
_ANSI_RE = re.compile(
    r"\x1b(?:"
    r"\[[0-?]*[ -/]*[@-~]"  # CSI, incl. private params < = > (mouse, DEC)
    r"|\][^\x07\x1b]*(?:\x07|\x1b\\)?"  # OSC (title, clipboard, ...)
    r"|[PX^_][^\x1b]*(?:\x1b\\)?"  # DCS / SOS / PM / APC strings
    r"|[ -/]*[0-~]"  # ESC+intermediates+final: RIS, charset, ESC 7/8/M...
    r"|)"  # a bare or trailing ESC
)


def strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


def scrub_non_sgr(text: str) -> str:
    """Drop every escape sequence except SGR styling.

    API-derived strings (job and node names arrive from the daemon, and
    under clustering from *other machines* over gossip) are painted to
    the live terminal, so anything that could move the cursor, retitle
    the window, or write the clipboard (OSC 52) must never survive into
    a frame; the painter's own SGR colours do.
    """
    if "\x1b" not in text:
        return text
    return _ANSI_RE.sub(
        lambda m: m.group(0) if _SGR_TOKEN_RE.fullmatch(m.group(0)) else "",
        text,
    )


def char_width(ch: str) -> int:
    """Display cells for one character (0 for combining, 2 for wide)."""
    if ch == "\t":
        return 1  # tabs are pre-expanded by callers; safety net
    cat = unicodedata.category(ch)
    if cat in ("Mn", "Me", "Cf") and ch != "­":
        return 0
    if unicodedata.east_asian_width(ch) in ("F", "W"):
        return 2
    return 1


def text_width(text: str) -> int:
    return sum(char_width(ch) for ch in strip_ansi(text))


def truncate(text: str, width: int, ellipsis: str = "…") -> str:
    """Cut plain text to ``width`` display cells (ellipsis included)."""
    if width <= 0:
        return ""
    text = scrub_non_sgr(text)
    if text_width(text) <= width:
        return text
    ell_w = text_width(ellipsis)
    out: List[str] = []
    used = 0
    for ch in text:
        w = char_width(ch)
        if used + w > width - ell_w:
            break
        out.append(ch)
        used += w
    return "".join(out) + ellipsis


def pad_to(text: str, width: int) -> str:
    """Pad (or truncate) plain text to exactly ``width`` cells."""
    text = scrub_non_sgr(text)
    w = text_width(text)
    if w > width:
        text = truncate(text, width)
        w = text_width(text)
    return text + " " * (width - w)


def oneline(text: Any) -> str:
    """Collapse whitespace runs (incl. newlines) to single spaces.

    Multi-line job commands are common (``set -eu\\n...``); a literal
    newline inside a table cell would break the painted row, so cells
    show the command flattened; copying still yields the original.
    Escapes and C0 controls are dropped outright: these strings come
    from the API and carry no legitimate styling of their own.
    """
    return " ".join(_CTRL_RE.sub("", strip_ansi(str(text))).split())


#: C0 controls that must never reach a painted frame (ESC survives for
#: :func:`rewrite_sgr`; \\r and \\t are handled semantically below).
_CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1a\x1c-\x1f\x7f]")


def sanitize_log_line(line: str) -> str:
    """Make raw job output safe to paint into a terminal row.

    A stray ``\\r`` would yank the cursor to column 1 mid-row (cmd.exe
    jobs emit CRLF; progress bars emit many), so carriage returns get
    log-viewer overwrite semantics: keep the last non-empty segment.
    Tabs expand, and the remaining C0 controls are dropped, except
    ``ESC``, which :func:`rewrite_sgr` re-inks or strips.
    """
    if "\r" in line:
        segments = line.split("\r")
        kept = ""
        for segment in reversed(segments):
            if segment:
                kept = segment
                break
        line = kept
    return _CTRL_RE.sub("", line.replace("\t", "    "))


#: The 8 basic + 8 bright ANSI palette positions, re-inked per theme so a
#: job's coloured log output stays legible on every background (the web
#: page does the same with its log-ANSI palette).
_LOG_BASE = [
    "dim",
    "fail",
    "ok",
    "pending",
    "run",
    "accent",
    "run",
    "fg",
    "dim",
    "fail",
    "ok",
    "warn",
    "run",
    "accent",
    "bright",
    "bright",
]
_SGR_TOKEN_RE = re.compile(r"\x1b\[([0-9;]*)m")


def rewrite_sgr(line: str, theme: Theme) -> str:
    """Translate a log line's SGR colours into theme colours.

    Bold/dim/reset survive; the 16-colour and 256/truecolour foregrounds
    are mapped onto the theme's log palette (background requests are
    dropped: the TUI owns the background).  All non-SGR escapes are
    stripped.
    """

    def replace(match: "re.Match[str]") -> str:
        out: List[str] = []
        params = match.group(1) or "0"
        parts = params.split(";")
        i = 0
        while i < len(parts):
            p = parts[i] or "0"
            try:
                code = int(p)
            except ValueError:
                i += 1
                continue
            if code == 0:
                out.append(RESET + theme.fg("fg"))
            elif code in (1, 2, 3, 4, 7, 22, 23, 24, 27):
                out.append("\x1b[%dm" % code)
            elif 30 <= code <= 37:
                out.append(theme.fg(_LOG_BASE[code - 30]))
            elif 90 <= code <= 97:
                out.append(theme.fg(_LOG_BASE[code - 90 + 8]))
            elif code == 39:
                out.append(theme.fg("fg"))
            elif code in (38, 48) and i + 1 < len(parts):
                # eat 38;5;n / 38;2;r;g;b (and the bg variants) whole
                skip = 2 if parts[i + 1] == "5" else 4
                if code == 38:
                    out.append(theme.fg("bright"))
                i += skip
            i += 1
        return "".join(out)

    def dispatch(outer: "re.Match[str]") -> str:
        sgr = _SGR_TOKEN_RE.fullmatch(outer.group(0))
        return replace(sgr) if sgr is not None else ""

    return _ANSI_RE.sub(dispatch, line)


def sparkline(history: List[Dict[str, Any]], width: int = 10) -> str:
    """Recent-run sparkline: bar height = relative duration, one bar per
    run, oldest first (the terminal cousin of the web SVG sparkline).
    Returns a plain string; the caller colours per-bar via
    :func:`spark_cells`.
    """
    return "".join(ch for ch, _ in spark_cells(history, width))


_SPARK_BARS = "▁▂▃▄▅▆▇█"

#: week calendar bounds: the hum threshold matches the web panel (a job
#: firing more often than ~8x/day is background hum, summarized instead of
#: charted); the enumeration cap bounds the hum count this panel displays
#: ("x200+"), which the web strip does not show
WEEK_PER_JOB_CAP = 200
WEEK_FREQ_MAX = 56


def spark_cells(
    history: List[Dict[str, Any]], width: int = 10
) -> List[Tuple[str, str]]:
    """``(bar-char, health-colour-key)`` cells for the recent-run tail."""
    tail = history[-width:] if history else []
    durations = [
        float(r["duration"]) for r in tail if r.get("duration") is not None
    ]
    top = max(durations) if durations else 0.0
    cells: List[Tuple[str, str]] = []
    for run in tail:
        dur = run.get("duration") or 0
        idx = (
            min(
                len(_SPARK_BARS) - 1,
                int((dur / top) * (len(_SPARK_BARS) - 1) + 0.5),
            )
            if top
            else 0
        )
        outcome = run.get("outcome")
        color = (
            "fail"
            if outcome == "failure"
            else "cancelled"
            if outcome == "cancelled"
            else "unknown"
            if outcome == "unknown"
            else "ok"
        )
        color = {"cancelled": "off", "unknown": "pending"}.get(color, color)
        cells.append((_SPARK_BARS[idx], color))
    return cells


# ===================================================================
#  preferences: the localStorage analogue (a small JSON file)
# ===================================================================
#: Defaults mirror the web page's prefs where they translate to a tty.
PREF_DEFAULTS: Dict[str, Any] = {
    "theme": "carolina",  # hue
    "light": False,  # phosphor (dark) vs paper (light)
    "cvd": "none",  # colour-vision remap
    "poll_ms": DEFAULT_POLL_MS,
    "wrap": False,  # log line wrap
    "timestamps": False,  # per-line log timestamps
    "compact": False,  # compact density
    "sound": False,  # terminal bell on failure cues
    "boot": True,  # BIOS-style boot self-test
    "boot_last": 0.0,  # last boot POST, epoch seconds
    "zen": True,  # wallboard screensaver
    "zen_idle_s": 90,
    "ascii": False,  # ASCII status glyphs
}


def prefs_path() -> str:
    """``%APPDATA%\\cronstable\\tui.json`` / ``$XDG_CONFIG_HOME`` analogue."""
    if IS_WINDOWS:  # pragma: no cover - exercised on Windows only
        base = os.environ.get("APPDATA") or os.path.expanduser("~")
        return os.path.join(base, "cronstable", "tui.json")
    base = os.environ.get("XDG_CONFIG_HOME") or os.path.join(
        os.path.expanduser("~"), ".config"
    )
    return os.path.join(base, "cronstable", "tui.json")


def load_prefs(path: Optional[str] = None) -> Dict[str, Any]:
    """Read prefs; unknown keys are dropped, bad values fall back."""
    prefs = dict(PREF_DEFAULTS)
    target = path or prefs_path()
    try:
        with open(target, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except (OSError, ValueError):
        return prefs
    if isinstance(raw, dict):
        for key, default in PREF_DEFAULTS.items():
            value = raw.get(key, default)
            if isinstance(value, type(default)) or (
                isinstance(default, float) and isinstance(value, (int, float))
            ):
                prefs[key] = value
    if prefs["theme"] not in THEME_HUES:
        prefs["theme"] = PREF_DEFAULTS["theme"]
    if prefs["cvd"] not in CVD_MODES:
        prefs["cvd"] = "none"
    return prefs


def save_prefs(prefs: Dict[str, Any], path: Optional[str] = None) -> None:
    """Best-effort persist; the TUI never fails over a prefs write."""
    target = path or prefs_path()
    try:
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "w", encoding="utf-8") as fh:
            json.dump(
                {k: prefs[k] for k in PREF_DEFAULTS if k in prefs},
                fh,
                indent=2,
                sort_keys=True,
            )
    except OSError as exc:  # pragma: no cover - depends on host FS
        logger.debug("could not save TUI prefs: %s", exc)


# ===================================================================
#  terminal engine: raw mode, ANSI painting, key decoding
# ===================================================================
ALT_SCREEN_ON = "\x1b[?1049h"
ALT_SCREEN_OFF = "\x1b[?1049l"
CURSOR_HIDE = "\x1b[?25l"
CURSOR_SHOW = "\x1b[?25h"
CLEAR = "\x1b[2J"
SYNC_ON = "\x1b[?2026h"  # "synchronized output"; ignored where unknown
SYNC_OFF = "\x1b[?2026l"

#: CSI/SS3 escape tails -> key names (POSIX byte-stream decoding).
_CSI_KEYS = {
    "A": "up",
    "B": "down",
    "C": "right",
    "D": "left",
    "H": "home",
    "F": "end",
    "Z": "shift+tab",
    "1~": "home",
    "4~": "end",
    "3~": "delete",
    "5~": "pgup",
    "6~": "pgdn",
    "2~": "insert",
}
_SS3_KEYS = {
    "A": "up",
    "B": "down",
    "C": "right",
    "D": "left",
    "H": "home",
    "F": "end",
}

#: msvcrt scan codes (after a '\x00'/'\xe0' prefix) -> key names.
_WIN_KEYS = {
    "H": "up",
    "P": "down",
    "K": "left",
    "M": "right",
    "I": "pgup",
    "Q": "pgdn",
    "G": "home",
    "O": "end",
    "S": "delete",
    "R": "insert",
    "\x0f": "shift+tab",
    # ctrl-arrow variants collapse onto the plain arrows
    "\x8d": "up",
    "\x91": "down",
    "s": "left",
    "t": "right",
}


def _decode_control(ch: str) -> str:
    """Map a control character onto its key name."""
    if ch in ("\r", "\n"):
        return "enter"
    if ch == "\t":
        return "tab"
    if ch in ("\x7f", "\x08"):
        return "backspace"
    if ch == "\x1b":
        return "esc"
    if "\x01" <= ch <= "\x1a":
        return "ctrl+" + chr(ord(ch) + 96)
    return ch


class KeyDecoder:
    """Incremental bytes -> key-name decoder for the POSIX byte stream.

    Escape sequences may arrive split across reads, and a bare ``Esc``
    press is only distinguishable from the head of a sequence by time;
    ``flush_escape()`` is called by the reader after a short quiet gap to
    resolve a pending lone escape.
    """

    def __init__(self) -> None:
        self._utf8 = codecs.getincrementaldecoder("utf-8")("replace")
        self._pending = ""  # a partially-received escape sequence

    def feed(self, data: bytes) -> List[str]:
        keys: List[str] = []
        for ch in self._utf8.decode(data):
            if self._pending:
                self._pending += ch
                done, name = self._try_escape(self._pending)
                if done:
                    self._pending = ""
                    if name:
                        keys.append(name)
                continue
            if ch == "\x1b":
                self._pending = ch
                continue
            if ch < " " or ch == "\x7f":
                keys.append(_decode_control(ch))
            else:
                keys.append(ch)
        return keys

    def flush_escape(self) -> List[str]:
        """Resolve a lone ``Esc`` (or abandon a malformed sequence)."""
        if not self._pending:
            return []
        pending, self._pending = self._pending, ""
        return ["esc"] if pending == "\x1b" else []

    @staticmethod
    def _try_escape(seq: str) -> Tuple[bool, Optional[str]]:
        """``(complete, key-or-None)`` for a buffered escape sequence."""
        if len(seq) == 1:
            return (False, None)
        second = seq[1]
        if second == "[":
            body = seq[2:]
            if body and body[-1] >= "@":  # final byte reached
                if body in _CSI_KEYS:
                    return (True, _CSI_KEYS[body])
                # modified arrows/navigation ("1;5A") -> the plain key
                if body[-1] in "ABCDHF":
                    return (True, _CSI_KEYS.get(body[-1]))
                stripped = body.rstrip("~").split(";")[0]
                if body[-1] == "~" and stripped + "~" in _CSI_KEYS:
                    return (True, _CSI_KEYS[stripped + "~"])
                return (True, None)  # unrecognised CSI: swallow
            return (len(body) > 16, None)  # runaway guard
        if second == "O":
            if len(seq) >= 3:
                return (True, _SS3_KEYS.get(seq[2]))
            return (False, None)
        if second == "]":  # OSC: swallow to terminator
            if seq.endswith("\x07") or seq.endswith("\x1b\\"):
                return (True, None)
            return (len(seq) > 256, None)
        # Alt+char and anything else: swallow the pair
        return (True, None)


class PosixKeyReader:
    """stdin -> key-name queue on POSIX, via ``loop.add_reader``."""

    def __init__(self, loop: asyncio.AbstractEventLoop, fd: int) -> None:
        self._loop = loop
        self._fd = fd
        self._decoder = KeyDecoder()
        self._queue: "asyncio.Queue[str]" = asyncio.Queue()
        self._flusher: Optional[asyncio.TimerHandle] = None
        loop.add_reader(fd, self._on_readable)

    def _on_readable(self) -> None:
        try:
            data = os.read(self._fd, 1024)
        except OSError:
            return
        for key in self._decoder.feed(data):
            self._queue.put_nowait(key)
        if self._flusher is not None:
            self._flusher.cancel()
        # a lone Esc resolves after a quiet gap (sequence bytes arrive
        # effectively instantly, so 30ms is generous and imperceptible)
        self._flusher = self._loop.call_later(0.03, self._flush)

    def _flush(self) -> None:
        for key in self._decoder.flush_escape():
            self._queue.put_nowait(key)

    async def get(self) -> str:
        return await self._queue.get()

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._loop.remove_reader(self._fd)
        if self._flusher is not None:
            self._flusher.cancel()


if sys.platform == "win32":  # pragma: no cover - exercised on Windows only

    class WindowsKeyReader:
        """msvcrt reader thread -> key-name queue (Proactor-safe).

        The Proactor loop cannot watch stdin, so a daemon thread blocks
        in ``getwch()`` and marshals decoded keys onto the loop.
        """

        def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
            self._loop = loop
            self._queue: "asyncio.Queue[str]" = asyncio.Queue()
            self._stop = False
            self._thread = threading.Thread(
                target=self._pump, name="tui-keys", daemon=True
            )
            self._thread.start()

        def _pump(self) -> None:
            while not self._stop:
                try:
                    ch = msvcrt.getwch()
                except Exception:
                    return
                if ch in ("\x00", "\xe0"):
                    code = msvcrt.getwch()
                    name = _WIN_KEYS.get(code)
                    if name is None:
                        continue
                elif ch == "\x1b":
                    name = "esc"
                elif ch < " " or ch == "\x7f":
                    name = _decode_control(ch)
                else:
                    name = ch
                try:
                    self._loop.call_soon_threadsafe(
                        self._queue.put_nowait, name
                    )
                except RuntimeError:  # loop already closed mid-exit
                    return

        async def get(self) -> str:
            return await self._queue.get()

        def close(self) -> None:
            self._stop = True


def _enable_vt_windows() -> None:  # pragma: no cover - Windows only
    """Turn on ANSI/VT processing for the console (idempotent)."""
    if sys.platform == "win32":
        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        for std in (-11, -12):  # stdout, stderr
            handle = kernel32.GetStdHandle(std)
            mode = ctypes.c_uint32()
            if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
                # 0x0004 = ENABLE_VIRTUAL_TERMINAL_PROCESSING
                kernel32.SetConsoleMode(handle, mode.value | 0x0004)


class Term:
    """The live terminal: raw mode, alternate screen, diffed painting.

    All escape output is written in one buffered chunk per frame, and
    only rows that changed since the previous frame are repainted, so a
    1-second tick over an idle board costs a handful of bytes.
    """

    def __init__(self, stream: Any = None) -> None:
        self._out = stream if stream is not None else sys.stdout
        self._saved: Any = None
        self._last_rows: List[str] = []
        self._last_size = (0, 0)

    # ---- lifecycle ---------------------------------------------------
    def enter(self) -> None:
        if sys.platform == "win32":  # pragma: no cover - Windows only
            _enable_vt_windows()
        else:
            fd = sys.stdin.fileno()
            self._saved = termios.tcgetattr(fd)
            tty.setraw(fd, termios.TCSADRAIN)
        self._write(ALT_SCREEN_ON + CURSOR_HIDE + CLEAR)
        self.flush()

    def exit(self) -> None:
        self._write(RESET + ALT_SCREEN_OFF + CURSOR_SHOW)
        self.flush()
        if sys.platform != "win32" and self._saved is not None:
            termios.tcsetattr(
                sys.stdin.fileno(), termios.TCSADRAIN, self._saved
            )

    # ---- painting ----------------------------------------------------
    def size(self) -> Tuple[int, int]:
        """``(cols, rows)`` right now."""
        try:
            sz = shutil.get_terminal_size()
            return (sz.columns, sz.lines)
        except OSError:  # pragma: no cover - no tty
            return (80, 24)

    def paint(self, rows: List[str], bg: str) -> None:
        """Present a frame: ``rows`` are ready-made ANSI row strings."""
        cols, lines = self.size()
        full = self._last_size != (cols, lines)
        self._last_size = (cols, lines)
        out: List[str] = [SYNC_ON]
        if full:
            out.append(bg + CLEAR)
        for idx in range(lines):
            row = rows[idx] if idx < len(rows) else ""
            if (
                not full
                and idx < len(self._last_rows)
                and self._last_rows[idx] == row
            ):
                continue
            out.append("\x1b[%d;1H" % (idx + 1))
            out.append(bg + "\x1b[K" + row + RESET)
        out.append(SYNC_OFF)
        self._last_rows = list(rows[:lines])
        self._write("".join(out))
        self.flush()

    def invalidate(self) -> None:
        """Force the next paint to redraw every row."""
        self._last_size = (0, 0)
        self._last_rows = []

    # ---- little extras -----------------------------------------------
    def bell(self) -> None:
        self._write("\x07")
        self.flush()

    def osc52_copy(self, text: str) -> None:
        """Ask the terminal to place ``text`` on the system clipboard."""
        payload = base64.b64encode(text.encode("utf-8")).decode("ascii")
        self._write("\x1b]52;c;%s\x07" % payload)
        self.flush()

    def _write(self, data: str) -> None:
        self._out.write(data)

    def flush(self) -> None:
        with contextlib.suppress(Exception):
            self._out.flush()


class HeadlessTerm(Term):
    """A fixed-size, in-memory terminal for the test-suite.

    Frames are recorded rather than diff-painted, and ``screen()`` gives
    the visible text with all escapes stripped, so tests assert on what a
    user would actually see.
    """

    def __init__(self, cols: int = 100, lines: int = 30) -> None:
        super().__init__(stream=None)
        self._cols = cols
        self._lines = lines
        self.frames: List[List[str]] = []
        self.bells = 0
        self.copied: List[str] = []

    def enter(self) -> None:  # no tty to configure
        pass

    def exit(self) -> None:
        pass

    def size(self) -> Tuple[int, int]:
        return (self._cols, self._lines)

    def paint(self, rows: List[str], bg: str) -> None:
        self.frames.append(list(rows))

    def bell(self) -> None:
        self.bells += 1

    def osc52_copy(self, text: str) -> None:
        self.copied.append(text)

    def screen(self) -> str:
        if not self.frames:
            return ""
        return "\n".join(strip_ansi(row) for row in self.frames[-1])


class ScriptedKeys:
    """A scriptable key source for the test-suite."""

    def __init__(self) -> None:
        self.queue: "asyncio.Queue[str]" = asyncio.Queue()

    def send(self, *keys: str) -> None:
        for key in keys:
            self.queue.put_nowait(key)

    async def get(self) -> str:
        return await self.queue.get()

    def close(self) -> None:
        pass


def copy_to_clipboard(term: Term, text: str) -> bool:
    """Best-effort clipboard: OSC 52 plus the platform's copy tool."""
    term.osc52_copy(text)
    try:
        if IS_WINDOWS:  # pragma: no cover - Windows only
            proc = subprocess.run(
                ["clip.exe"], input=text.encode("utf-16-le"), timeout=3
            )
            return proc.returncode == 0
        if sys.platform == "darwin":  # pragma: no cover - macOS only
            proc = subprocess.run(
                ["pbcopy"], input=text.encode("utf-8"), timeout=3
            )
            return proc.returncode == 0
        for tool in (["wl-copy"], ["xclip", "-selection", "clipboard"]):
            if shutil.which(tool[0]):
                proc = subprocess.run(
                    tool, input=text.encode("utf-8"), timeout=3
                )
                return proc.returncode == 0
    except (OSError, subprocess.SubprocessError):  # pragma: no cover
        pass
    return True  # OSC 52 alone very likely worked; stay quiet either way


# ===================================================================
#  HTTP + SSE client (the thin layer the web page's apiFetch plays)
# ===================================================================
class Unauthorized(Exception):
    """401 from the daemon: the app opens the token modal, like the page."""


class ApiError(Exception):
    """A non-2xx response worth telling the operator about."""

    def __init__(self, status: int, message: str = "") -> None:
        super().__init__(message or ("HTTP %d" % status))
        self.status = status


class Api:
    """Bearer-authenticated JSON/SSE client over the core aiohttp dep.

    ``aiohttp`` is imported on first use (not at module import), so the
    CLI's subcommand registration stays light; see the module
    docstring.  A missing/wrong token surfaces as :class:`Unauthorized`
    exactly where the web page would pop its token modal.
    """

    def __init__(self, url: str, token: Optional[str]) -> None:
        self.url = url.rstrip("/")
        self.token = token
        self._session: Any = None

    async def _ensure(self) -> Any:
        if self._session is None:
            import aiohttp

            # No total timeout: SSE streams are held open indefinitely.
            # Individual JSON calls pass their own per-request timeout.
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=None)
            )
        return self._session

    def _headers(self, accept: str = "application/json") -> Dict[str, str]:
        headers = {"Accept": accept}
        if self.token:
            headers["Authorization"] = "Bearer %s" % self.token
        return headers

    async def get_json(self, path: str, timeout_s: float = 10.0) -> Any:
        import aiohttp

        session = await self._ensure()
        async with session.get(
            self.url + path,
            headers=self._headers(),
            # a bounded connect keeps an unreachable host (VPN down,
            # firewall DROP) from eating the whole request budget
            timeout=aiohttp.ClientTimeout(
                total=timeout_s, connect=min(3.0, timeout_s)
            ),
        ) as resp:
            if resp.status == 401:
                raise Unauthorized()
            if resp.status >= 400:
                raise ApiError(resp.status)
            return await resp.json(content_type=None)

    async def get_text(self, path: str, timeout_s: float = 10.0) -> str:
        import aiohttp

        session = await self._ensure()
        # text/plain, deliberately: /job-set-id content-negotiates and
        # would answer a JSON Accept with its JSON shape
        async with session.get(
            self.url + path,
            headers=self._headers(accept="text/plain"),
            timeout=aiohttp.ClientTimeout(
                total=timeout_s, connect=min(3.0, timeout_s)
            ),
        ) as resp:
            if resp.status == 401:
                raise Unauthorized()
            if resp.status >= 400:
                raise ApiError(resp.status)
            text: str = await resp.text()
            return text

    async def post(
        self,
        path: str,
        body: Optional[Dict[str, Any]] = None,
        timeout_s: float = 15.0,
    ) -> Tuple[int, Any]:
        """POST; returns ``(status, parsed-body-or-text)``, raising only
        on auth (the callers toast per-status, mirroring the page)."""
        import aiohttp

        session = await self._ensure()
        kwargs: Dict[str, Any] = {
            "headers": self._headers(),
            "timeout": aiohttp.ClientTimeout(total=timeout_s),
        }
        if body is not None:
            kwargs["json"] = body
        async with session.post(self.url + path, **kwargs) as resp:
            if resp.status == 401:
                raise Unauthorized()
            try:
                payload = await resp.json(content_type=None)
            except ValueError:
                payload = await resp.text()
            return (resp.status, payload)

    async def stream(self, path: str) -> Any:
        """Async iterator over an SSE endpoint: yields ``(event, data)``.

        Parses the ``event:``/``data:`` frames the daemon emits
        (``event: line`` with a JSON body, ``event: end``, and ``: ping``
        keep-alives, which are skipped).  The caller cancels the iterator
        (closing the response) when its panel closes.
        """
        import aiohttp

        session = await self._ensure()
        # no total timeout (the stream is held open indefinitely), but a
        # bounded connect, and a read timeout comfortably above the
        # daemon's 15s SSE keep-alive ping so a silently dead connection
        # surfaces as an error instead of freezing the tail forever
        async with session.get(
            self.url + path,
            headers=self._headers(),
            timeout=aiohttp.ClientTimeout(
                total=None, connect=5.0, sock_read=60.0
            ),
        ) as resp:
            if resp.status == 401:
                raise Unauthorized()
            if resp.status >= 400:
                raise ApiError(resp.status)
            event = "message"
            data_lines: List[str] = []
            async for raw in resp.content:
                line = raw.decode("utf-8", "replace").rstrip("\r\n")
                if line == "":
                    if data_lines:
                        text = "\n".join(data_lines)
                        try:
                            payload = json.loads(text)
                        except ValueError:
                            payload = {"line": text}
                        yield (event, payload)
                    event = "message"
                    data_lines = []
                    continue
                if line.startswith(":"):
                    continue  # keep-alive comment
                if line.startswith("event:"):
                    event = line[6:].strip()
                elif line.startswith("data:"):
                    data_lines.append(line[5:].lstrip())

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None


class LogTail:
    """One job/task's live log: an SSE stream feeding a capped buffer.

    Mirrors the page's tail semantics: the daemon replays the retained
    buffer then follows; the stream ends when the run does.  With
    ``follow`` the tail re-attaches after ``TAIL_RETRY_MS`` (the page's
    reconnect throttle) so the pane picks up the next run.
    """

    MAX_LINES = 5000

    def __init__(
        self,
        api: Api,
        path: str,
        label: str,
        on_change: Callable[[], None],
    ) -> None:
        self.api = api
        self.path = path
        self.label = label
        self.lines: List[Tuple[str, str, float]] = []  # (stream, line, t)
        self.ended: Optional[str] = None  # end reason ("" = plain end)
        self.error: Optional[str] = None
        self.follow = True
        self._on_change = on_change
        self._task: Optional["asyncio.Task[None]"] = None

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.get_running_loop().create_task(self._run())

    def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None

    def _last_block(self) -> List[Tuple[str, str]]:
        """The ``(stream, line)`` pairs of the newest run on screen:
        the entries between the last two end markers."""
        block: List[Tuple[str, str]] = []
        for stream, line, _ in reversed(self.lines):
            if stream == "meta":
                if not block:
                    continue  # skip the trailing end marker itself
                break
            block.append((stream, line))
        block.reverse()
        return block

    async def _run(self) -> None:
        clear_next = False  # the previous attempt died mid-run
        dedupe_next = False  # the previous attempt saw its run end
        retry = TAIL_RETRY_MS / 1000
        while True:
            self.ended = None
            self.error = None
            fresh_attempt = clear_next
            clear_next = False
            # After a clean end the daemon replays the *same finished
            # run's* retained buffer on every re-attach (the stream
            # carries no run identity), so replayed lines are held
            # aside until they diverge from the block already on
            # screen: an identical replay that just ends again is the
            # old run repeated and is dropped whole, while divergence
            # is the next run's output and flushes through, so runs
            # stack up behind their end markers, like the page.
            expect = self._last_block() if dedupe_next else []
            staged: Optional[List[Tuple[str, str, float]]] = (
                [] if dedupe_next else None
            )
            dedupe_next = False
            try:
                async for event, payload in self.api.stream(self.path):
                    if event == "line":
                        if fresh_attempt:
                            # a mid-run reconnect replays the same run's
                            # retained buffer: start clean rather than
                            # duplicate it.
                            self.lines = []
                            fresh_attempt = False
                        entry = (
                            str(payload.get("stream", "")),
                            sanitize_log_line(str(payload.get("line", ""))),
                            time.time(),
                        )
                        if staged is not None:
                            if (
                                len(staged) < len(expect)
                                and expect[len(staged)] == entry[:2]
                            ):
                                staged.append(entry)
                                continue
                            self.lines.extend(staged)  # diverged: new run
                            staged = None
                        self.lines.append(entry)
                        if len(self.lines) > self.MAX_LINES:
                            del self.lines[: -self.MAX_LINES]
                        self._on_change()
                    elif event == "end":
                        self.ended = str(payload.get("reason") or "")
                        duplicate = staged is not None and len(staged) == len(
                            expect
                        )
                        if staged is not None and not duplicate:
                            self.lines.extend(staged)
                        staged = None
                        dedupe_next = True
                        # each duplicate replay backs the re-attach off
                        # (the daemon re-serves the whole retained
                        # buffer per attach); new output resets it
                        retry = (
                            min(retry * 2, 30.0)
                            if duplicate
                            else TAIL_RETRY_MS / 1000
                        )
                        if not duplicate:
                            if payload.get("reason") != "no-output":
                                self.lines.append(
                                    (
                                        "meta",
                                        "end of run output",
                                        time.time(),
                                    )
                                )
                            if len(self.lines) > self.MAX_LINES:
                                del self.lines[: -self.MAX_LINES]
                            self._on_change()
            except asyncio.CancelledError:
                raise
            except Unauthorized:
                self.error = "unauthorized"
                self._on_change()
                return
            except Exception as exc:  # noqa: BLE001 - surfaced in-pane
                self.error = str(exc) or exc.__class__.__name__
                clear_next = True  # replay would duplicate this run
                dedupe_next = False
                retry = TAIL_RETRY_MS / 1000
                self._on_change()
            if not self.follow:
                return
            await asyncio.sleep(retry)


# ===================================================================
#  frame composition helpers
# ===================================================================
def cut_to_width(row: str, width: int) -> str:
    """Cut an ANSI row string to ``width`` visible cells.

    SGR sequences are copied through (they occupy no cells), so styling
    survives the cut; every other escape is dropped here as the last
    line of defense before a row reaches the terminal.  A trailing
    reset keeps later splices clean.  This is what lets the job drawer
    sit beside the dimmed table like the web page's right-hand aside.
    """
    if width <= 0:
        return RESET
    out: List[str] = []
    used = 0
    idx = 0
    while idx < len(row) and used < width:
        match = _ANSI_RE.match(row, idx)
        if match:
            if _SGR_TOKEN_RE.fullmatch(match.group(0)):
                out.append(match.group(0))
            idx = max(match.end(), idx + 1)
            continue
        ch = row[idx]
        w = char_width(ch)
        if used + w > width:
            break
        out.append(ch)
        used += w
        idx += 1
    out.append(" " * (width - used))
    out.append(RESET)
    return "".join(out)


def overlay_center(
    base: List[str],
    panel: List[str],
    cols: int,
    lines: int,
    fill: str,
) -> List[str]:
    """Replace a centered band of ``base`` rows with ``panel`` rows.

    Modal surfaces (palette, help, settings, ...) take whole rows (the
    margins are painted with the dimmed ``fill`` style), which keeps the
    compositor trivial and every frame byte-identical for the differ.
    """
    rows = list(base)
    top = max(0, (lines - len(panel)) // 2)
    for offset, prow in enumerate(panel):
        idx = top + offset
        if 0 <= idx < lines:
            pw = text_width(prow)
            left = max(0, (cols - pw) // 2)
            rows[idx] = (
                fill
                + " " * left
                + RESET
                + prow
                + RESET
                + fill
                + " " * max(0, cols - left - pw)
                + RESET
            )
    return rows


class Painter:
    """Row-string assembly against one theme."""

    def __init__(self, theme: Theme) -> None:
        self.theme = theme
        self.bg = theme.bg("bg")

    def style(
        self,
        text: str,
        fg: str = "fg",
        bg: Optional[str] = None,
        bold: bool = False,
        dim: bool = False,
        reverse: bool = False,
    ) -> str:
        parts = [self.theme.fg(fg)]
        if bg is not None:
            parts.append(self.theme.bg(bg))
        if bold:
            parts.append(BOLD)
        if dim:
            parts.append(DIM_SGR)
        if reverse:
            parts.append(REVERSE)
        parts.append(text)
        parts.append(RESET)
        if bg is None:
            parts.append(self.bg)
        return "".join(parts)

    def row(self, *spans: str) -> str:
        return self.bg + "".join(spans)

    def hline(self, width: int, char: str = "─") -> str:
        return self.style(char * width, "border")

    def glyph(self, key: str, ascii_mode: bool) -> str:
        table = GLYPH_ASCII if ascii_mode else GLYPH
        return table.get(key, "?")


def finding_rows(
    findings: List[Finding],
    paint: "Painter",
    width: int,
    limit: int,
) -> List[str]:
    """Painted rows for schedule-lint findings, shared by the cron
    sandbox and the job drawer so the two panels cannot drift in how a
    finding level looks."""
    rows: List[str] = []
    for finding in findings[:limit]:
        if finding.code == "never-fires":
            marker, color = "✕", "fail"
        elif finding.level == "warning":
            marker, color = "⚠", "warn"
        else:
            marker, color = "·", "dim"
        segments = textwrap.wrap(finding.message, max(20, width - 7)) or [""]
        for i, segment in enumerate(segments):
            prefix = " %s " % marker if i == 0 else "   "
            rows.append(paint.style(prefix + segment, color))
    return rows


def panel_frame(
    paint: Painter,
    title: str,
    body: List[str],
    width: int,
    footer: str = "",
) -> List[str]:
    """A bordered modal panel: title bar, body rows, optional hint row.

    ``body`` rows are pre-styled ANSI strings no wider than ``width - 4``
    visible cells; they are padded onto the panel background.
    """
    inner = width - 2
    top = paint.style(
        "┌" + ("╴" + title + "╶").center(inner, "─") + "┐", "accent"
    )
    rows = [top]
    for line in body:
        line_cut = cut_to_width(line, inner - 2)
        rows.append(
            paint.style("│ ", "accent")
            + paint.theme.bg("bg")
            + line_cut
            + paint.theme.bg("bg")
            + paint.style(" │", "accent")
        )
    if footer:
        rows.append(paint.style("├" + "─" * inner + "┤", "accent"))
        rows.append(
            paint.style("│ ", "accent")
            + paint.style(pad_to(footer, inner - 2), "dim")
            + paint.style(" │", "accent")
        )
    rows.append(paint.style("└" + "─" * inner + "┘", "accent"))
    return rows


def scroll_window(total: int, visible: int, cursor: int, offset: int) -> int:
    """Keep ``cursor`` inside the ``visible`` window; returns new offset."""
    if total <= visible:
        return 0
    offset = min(max(0, offset), total - visible)
    if cursor < offset:
        offset = cursor
    elif cursor >= offset + visible:
        offset = cursor - visible + 1
    return offset


# ===================================================================
#  the application
# ===================================================================
#: Overlay identifiers, in the web page's Esc close-priority order:
#: the FIRST open surface in this list is the one Esc closes.
ESC_PRIORITY = [
    "token",
    "settings",
    "help",
    "mitigate",
    "sandbox",
    "timeline",
    "tail",
    "dag",
    "drawer",
    # the card-panels (web cards adapted to overlay screens) close
    # after the drawers: a drawer opened FROM a panel (the DAGs index
    # -> a DAG's drawer) stacks on top of it, so Esc must peel the
    # drawer first, exactly like the web page's close order does for
    # its own surfaces
    "dags",
    "state",
    "cluster",
    "fleet",
    "heat",
    "press",
    "week",
    "radar",
    "node",
]

#: Text inputs and the overlay each belongs to (focus routing).
INPUT_HOMES = {
    "filter": None,
    "palette": "palette",
    "logsearch": "drawer",
    "tailadd": "tail",
    "sandbox": "sandbox",
    "token": "token",
    "backfill": "dag",
}


class App:
    """State + tasks + key dispatch for the TUI (the page's script tag).

    The terminal, key source, and clock hooks are injectable so the test
    suite drives the whole app headless: a scripted key queue in, painted
    frames out, with a fake daemon on a loopback port behind ``api``.
    """

    def __init__(
        self,
        api: Api,
        term: Term,
        keys: Any,
        prefs: Dict[str, Any],
        start_wallboard: bool = False,
        start_job: Optional[str] = None,
        boot: Optional[bool] = None,
        prefs_file: Optional[str] = None,
    ) -> None:
        self.api = api
        self.term = term
        self.keys = keys
        self.prefs = prefs
        self.prefs_file = prefs_file
        self.theme = Theme(
            prefs["theme"], bool(prefs["light"]), str(prefs["cvd"])
        )

        # ---- data mirrors of the daemon ----
        self.jobs: List[Dict[str, Any]] = []
        self.by_name: Dict[str, Dict[str, Any]] = {}
        self.fetched_mono = 0.0  # monotonic stamp of the last good /jobs
        self.version = ""
        self.job_set_id = ""
        self.cluster: Optional[Dict[str, Any]] = None
        self.fleet: Optional[Dict[str, Any]] = None
        self.dags: List[Dict[str, Any]] = []
        self.state_data: Optional[Dict[str, Any]] = None
        self.node: Optional[Dict[str, Any]] = None
        self.connected = False
        self.conn_error = ""

        # ---- list view ----
        self.view: List[Dict[str, Any]] = []
        self.sel = 0
        self.table_offset = 0
        self.filter_text = ""
        self.status_filter = "all"
        self.sort_key = "name"
        self.sort_dir = 1

        # ---- surfaces ----
        self.open_overlays: List[str] = []  # stack, last = topmost
        self.wallboard = bool(start_wallboard)
        self.booting = False
        self.focus: Optional[str] = None
        self.inputs: Dict[str, str] = dict.fromkeys(INPUT_HOMES, "")
        self.quit = False
        self._start_job = start_job
        self._boot_override = boot

        # ---- verdict / alarm (fleetSound port) ----
        self.verdict: Optional[Dict[str, Any]] = None
        self.incident_set: List[str] = []
        self.prev_fin: Dict[str, str] = {}
        self.just_failed: Set[str] = set()
        self.any_failing = False
        self.alarm_ack = False

        # ---- drawer ----
        self.drawer_job: Optional[str] = None
        self.drawer_tab = "logs"
        self.drawer_runs: Optional[Dict[str, Any]] = None
        self.drawer_res: Optional[Dict[str, Any]] = None
        self.log_tail: Optional[LogTail] = None
        self.log_scroll = 0  # rows up from the tail; 0 = following
        self.log_matches: List[int] = []
        self.log_match_idx = 0
        self.wrap = bool(prefs["wrap"])
        self.timestamps = bool(prefs["timestamps"])

        # ---- DAG drawer ----
        self.dag_name: Optional[str] = None
        self.dag_tab = "runs"
        self.dag_runs: List[Dict[str, Any]] = []
        self.dag_run: Optional[Dict[str, Any]] = None
        self.dag_run_key: Optional[str] = None
        self.dag_xcom: Optional[Dict[str, Any]] = None
        self.dag_sel = 0
        self.dag_task_tail: Optional[LogTail] = None

        # ---- multi-tail ----
        self.tails: List[LogTail] = []
        self.tail_sel = 0

        # ---- palette ----
        self.palette_sel = 0

        # ---- timeline / mitigate ----
        self.timeline_fail_only = False
        self.timeline_sel = 0
        self.mitigate_names: List[str] = []
        self.mitigate_label = ""
        self.mitigate_log: List[str] = []
        self.mitigate_running = False
        self.mitigate_abort = False

        # ---- cards ----
        self.heat_data: Dict[str, List[Dict[str, Any]]] = {}
        self.heat_loaded = 0.0
        # schedule pressure: computed LOCALLY from the /jobs snapshot via
        # croninfo (the same analyzers the daemon serves), so the panel
        # works against any daemon version; recomputed when stale.
        self.pressure: Optional[Dict[str, Any]] = None
        self.press_dups: List[Dict[str, Any]] = []
        self.press_suggest: Dict[str, Dict[str, Any]] = {}
        self.press_computed = 0.0
        self._press_busy = False
        # week calendar: 7-day fire outlook computed locally, like pressure
        self.week: Optional[Dict[str, Any]] = None
        self.week_computed = 0.0
        self._week_busy = False
        self.state_tab = "view"
        self.state_detail: Optional[Dict[str, Any]] = None
        self.state_sel = 0
        self.settings_sel = 0
        self.panel_scroll = 0
        self.dags_sel = 0
        self.fleet_fail_only = False
        self.node_history: Optional[Dict[str, Any]] = None

        # ---- wallboard / zen / boot ----
        self.zen_on = False
        self.last_key_mono = time.monotonic()
        self.boot_rows: List[str] = []
        self.wb_exit_hint_at = 0.0

        # ---- plumbing ----
        self.toasts: List[Tuple[str, str, float]] = []
        self.dirty = True
        self._dirty_event = asyncio.Event()
        self._poll_wakeup = asyncio.Event()
        self._tasks: List["asyncio.Task[None]"] = []
        self._paint_gate = 0.0

    # ---------------------------------------------------------------
    #  little state helpers
    # ---------------------------------------------------------------
    def mark(self) -> None:
        self.dirty = True
        self._dirty_event.set()

    def toast(self, kind: str, message: str) -> None:
        # toasts embed job names and server error strings; flatten and
        # strip escapes so API-derived text cannot reach the tty raw
        self.toasts.append((kind, oneline(message), time.monotonic() + 3.0))
        self.mark()

    def top_overlay(self) -> Optional[str]:
        return self.open_overlays[-1] if self.open_overlays else None

    def is_open(self, name: str) -> bool:
        return name in self.open_overlays

    def open(self, name: str) -> None:
        if name not in self.open_overlays:
            self.open_overlays.append(name)
        self.panel_scroll = 0
        self.mark()

    def close(self, name: str) -> None:
        if name in self.open_overlays:
            self.open_overlays.remove(name)
        if self.focus and INPUT_HOMES.get(self.focus) == name:
            self.focus = None
        if name == "drawer":
            self._close_drawer_streams()
        if name == "dag":
            self._close_dag_streams()
        if name == "tail":
            for tail in self.tails:
                tail.stop()
            self.tails = []
        self.mark()

    def selected_job(self) -> Optional[Dict[str, Any]]:
        if not self.view:
            return None
        return self.view[min(self.sel, len(self.view) - 1)]

    def next_run_seconds(self, job: Dict[str, Any]) -> Optional[float]:
        """scheduled_in, drift-corrected since the poll (web port)."""
        sched = job.get("scheduled_in")
        if sched is None:
            return None
        return float(sched) - (time.monotonic() - self.fetched_mono)

    def recompute_view(self) -> None:
        keep = None
        current = self.selected_job()
        if current is not None:
            keep = current.get("name")
        self.view = compute_view(
            self.jobs,
            self.filter_text,
            self.status_filter,
            self.sort_key,
            self.sort_dir,
        )
        if keep is not None:
            for idx, job in enumerate(self.view):
                if job.get("name") == keep:
                    self.sel = idx
                    break
        self.sel = max(0, min(self.sel, max(0, len(self.view) - 1)))

    def stale(self) -> bool:
        """Wallboard NO-SIGNAL rule: data older than max(15s, 2 polls)."""
        if self.fetched_mono == 0.0:
            return True
        poll_ms = int(self.prefs["poll_ms"]) or DEFAULT_POLL_MS
        horizon = max(WB_STALE_AFTER_MS, poll_ms * 2) / 1000
        return (time.monotonic() - self.fetched_mono) > horizon

    # ---------------------------------------------------------------
    #  lifecycle
    # ---------------------------------------------------------------
    async def run(self) -> None:
        self.term.enter()
        try:
            if sys.platform != "win32":
                with contextlib.suppress(
                    NotImplementedError, RuntimeError
                ):  # pragma: no cover - needs a real loop signal API
                    asyncio.get_running_loop().add_signal_handler(
                        signal.SIGWINCH, self._on_resize
                    )
            do_boot = (
                self._boot_override
                if self._boot_override is not None
                else bool(self.prefs["boot"])
                and time.time() - float(self.prefs["boot_last"]) > BOOT_EVERY_S
            )
            if do_boot:
                await self._boot_sequence()
            # first load runs OFF the critical path: the input and
            # paint loops are live first, so against an unreachable
            # daemon the header says "disconnected" and q/Ctrl-C work
            # instead of freezing a blank, un-quittable screen while
            # the startup probes time out.
            startup = asyncio.get_running_loop().create_task(self._startup())
            self._tasks = [
                asyncio.get_running_loop().create_task(coro)
                for coro in (
                    self._poll_loop(),
                    self._tick_loop(),
                    self._input_loop(),
                    self._paint_loop(),
                )
            ]
            done, pending = await asyncio.wait(
                self._tasks, return_when=asyncio.FIRST_COMPLETED
            )
            startup.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await startup
            for task in pending:
                task.cancel()
            for task in done:  # surface a crashed task's traceback
                exc = task.exception()
                if exc is not None and not isinstance(
                    exc, asyncio.CancelledError
                ):
                    raise exc
        finally:
            self._close_drawer_streams()
            self._close_dag_streams()
            for tail in self.tails:
                tail.stop()
            with contextlib.suppress(Exception):
                self.keys.close()
            await self.api.close()
            self.term.exit()

    def _on_resize(self) -> None:  # pragma: no cover - tty signal only
        self.term.invalidate()
        self.mark()

    async def _startup(self) -> None:
        """First load plus the ``--job`` deep link, as a background task
        so the UI is responsive from the first frame."""
        await self._first_load()
        if self._start_job:
            self.open_drawer(self._start_job, "logs")

    async def _first_load(self) -> None:
        with contextlib.suppress(Exception):
            self.version = (await self.api.get_text("/version")).strip()
        with contextlib.suppress(Exception):
            self.job_set_id = (await self.api.get_text("/job-set-id")).strip()
        await self._poll_once()
        with contextlib.suppress(Exception):
            await self._load_dags()

    # ---------------------------------------------------------------
    #  background loops
    # ---------------------------------------------------------------
    async def _poll_loop(self) -> None:
        while not self.quit:
            poll_ms = int(self.prefs["poll_ms"])
            if poll_ms <= 0:  # paused: poll only on a manual refresh
                await self._poll_wakeup.wait()
                self._poll_wakeup.clear()
                if self.quit:
                    return
                await self._poll_once()
                continue
            try:
                await asyncio.wait_for(
                    self._poll_wakeup.wait(), poll_ms / 1000
                )
            except asyncio.TimeoutError:
                pass
            self._poll_wakeup.clear()
            if self.quit:
                return
            await self._poll_once()

    def refresh_now(self) -> None:
        self._poll_wakeup.set()

    async def _poll_once(self) -> None:
        try:
            jobs = await self.api.get_json("/jobs")
        except Unauthorized:
            self.connected = False
            self.conn_error = "unauthorized"
            if not self.is_open("token"):
                self.open("token")
                self.focus = "token"
            self.mark()
            return
        except Exception as exc:  # noqa: BLE001 - shown in the header
            self.connected = False
            self.conn_error = str(exc) or exc.__class__.__name__
            self.mark()
            return
        first = self.fetched_mono == 0.0
        self.connected = True
        self.conn_error = ""
        self.jobs = jobs if isinstance(jobs, list) else []
        self.by_name = {j.get("name", ""): j for j in self.jobs}
        self.fetched_mono = time.monotonic()
        self._fleet_sound(first)
        self.recompute_view()
        # verdict rides the cluster alert; /cluster is refreshed in the
        # same fan-out the web page does on every successful poll
        await self._fanout()
        alert = cluster_alert(self.cluster)
        self.verdict, self.incident_set = verdict_info(self.jobs, alert)
        self.mark()

    async def _fanout(self) -> None:
        with contextlib.suppress(Exception):
            self.cluster = await self.api.get_json("/cluster")
        with contextlib.suppress(Exception):
            self.node = await self.api.get_json("/node")
        if self.is_open("fleet"):
            with contextlib.suppress(Exception):
                self.fleet = await self.api.get_json("/fleet")
        if self.is_open("state") and self.state_tab == "view":
            with contextlib.suppress(Exception):
                self.state_data = await self.api.get_json("/state")
        if self.is_open("heat") and (time.monotonic() - self.heat_loaded > 60):
            await self._load_heat()
        if self.is_open("press") and (
            time.monotonic() - self.press_computed > 60
        ):
            await self._recompute_pressure_bg()
        # same cadence as pressure, so the panel tracks reloads and rolls
        # its 7-day window past midnight without a manual refresh
        if self.is_open("week") and (
            time.monotonic() - self.week_computed > 60
        ):
            await self._recompute_week_bg()

    def _fleet_sound(self, first: bool) -> None:
        """Poll-diff for failure cues + the standing alarm (web port)."""
        just_failed: Set[str] = set()
        next_fin: Dict[str, str] = {}
        for job in self.jobs:
            name = job.get("name", "")
            last = job.get("last_run") or {}
            fin = last.get("finished_at")
            prev = self.prev_fin.get(name)
            if not first and fin and prev is not None and prev != fin:
                if last.get("outcome") == "failure":
                    just_failed.add(name)
                    if self.prefs["sound"]:
                        self.term.bell()
            if fin:
                next_fin[name] = fin
            elif prev is not None:
                next_fin[name] = prev
        self.prev_fin = next_fin
        self.just_failed = just_failed
        now_failing = any(
            (j.get("last_run") or {}).get("outcome") == "failure"
            for j in self.jobs
        )
        if just_failed:
            self.alarm_ack = False
        if not now_failing:
            self.alarm_ack = False
        self.any_failing = now_failing

    async def _tick_loop(self) -> None:
        while not self.quit:
            await asyncio.sleep(1)
            now = time.monotonic()
            if self.toasts and any(t[2] <= now for t in self.toasts):
                self.toasts = [t for t in self.toasts if t[2] > now]
            # zen: engage on an idle, healthy wallboard (web governor)
            if self.wallboard and self.prefs["zen"]:
                idle = now - self.last_key_mono
                healthy = not self.any_failing and not any(
                    j.get("running") for j in self.jobs
                )
                want = (
                    idle > float(self.prefs["zen_idle_s"])
                    and healthy
                    and not self.stale()
                )
                if want != self.zen_on:
                    self.zen_on = want
            elif self.zen_on:
                self.zen_on = False
            self.mark()  # clocks, countdowns, ages

    async def _input_loop(self) -> None:
        while not self.quit:
            key = await self.keys.get()
            self.last_key_mono = time.monotonic()
            if self.zen_on:  # any key wakes zen without acting
                self.zen_on = False
                self.mark()
                continue
            await self.handle_key(key)
            self.mark()

    async def _paint_loop(self) -> None:
        while not self.quit:
            await self._dirty_event.wait()
            self._dirty_event.clear()
            # coalesce bursts (SSE floods, key repeats) to ~30 fps
            delay = self._paint_gate - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            self._paint_gate = time.monotonic() + 0.033
            self.dirty = False
            self.paint()
        # one final frame so "quitting" states are not left half-drawn

    # ---------------------------------------------------------------
    #  data loads for panels
    # ---------------------------------------------------------------
    async def _load_dags(self) -> None:
        with contextlib.suppress(Exception):
            data = await self.api.get_json("/dags")
            self.dags = data if isinstance(data, list) else []

    async def _load_heat(self) -> None:
        """Batch /jobs/{name}/runs for the punchcard (capped, cached)."""
        self.heat_loaded = time.monotonic()
        for job in self.jobs[:40]:  # same spirit as the web page's cap
            name = job.get("name", "")
            with contextlib.suppress(Exception):
                data = await self.api.get_json("/jobs/%s/runs" % _quote(name))
                self.heat_data[name] = data.get("runs", [])
        self.mark()

    def _pressure_entries(self) -> List[ScheduleEntry]:
        """Analyzable rows from the /jobs and /dags snapshots.

        Mirrors the daemon's own entry builder: enabled cron-scheduled
        jobs plus each scheduled DAG's synthetic ``dag:<name>`` job (the
        /dags rows graft its schedule string).  An H job's
        ``schedule_resolved`` is parsed instead of its source, so no hash
        key is needed here.  Two remote-TUI approximations the payloads
        force: a job on the daemon's local clock (``utc: false``, no
        explicit timezone) resolves in THIS terminal's zone, which only
        matches the daemon when the two hosts share a zone, and a DAG
        schedule is assumed to be in the config default frame (UTC),
        because /dags carries neither flag.
        """
        from zoneinfo import ZoneInfo

        entries: List[ScheduleEntry] = []
        for job in self.jobs:
            if not job.get("enabled"):
                continue
            text = str(
                job.get("schedule_resolved") or job.get("schedule") or ""
            ).strip()
            if not text or text.lower() == "@reboot":
                continue
            try:
                tab = CronTab(text)
            except (ValueError, KeyError):
                continue
            tz: Optional[datetime.tzinfo]
            tz_name = job.get("timezone")
            if tz_name:
                try:
                    tz = ZoneInfo(str(tz_name))
                except Exception:  # noqa: BLE001 - unknown zone: fall back
                    tz = datetime.timezone.utc
            else:
                tz = datetime.timezone.utc if job.get("utc", True) else None
            entries.append(ScheduleEntry(str(job.get("name", "")), tab, tz))
        seen = {entry.name for entry in entries}
        for dag in self.dags:
            name = "dag:%s" % dag.get("name", "")
            text = str(dag.get("schedule") or "").strip()
            if not text or text.lower() == "@reboot" or name in seen:
                continue
            try:
                tab = CronTab(text)
            except (ValueError, KeyError):
                continue  # e.g. an H form; /dags has no resolved spelling
            entries.append(ScheduleEntry(name, tab, datetime.timezone.utc))
        return entries

    async def _recompute_pressure_bg(self) -> None:
        """The pressure walk on a worker thread.

        The compute is pure CPU over a snapshot of the /jobs and /dags
        rows, so running it off the loop keeps keys and repaints live on
        a large fleet; the guard collapses overlapping requests."""
        if self._press_busy:
            return
        self._press_busy = True
        try:
            await asyncio.to_thread(self._recompute_pressure)
        finally:
            self._press_busy = False

    def _recompute_pressure(self) -> None:
        """Refresh the schedule-pressure panel's data, locally.

        Same analyzers the daemon serves on /schedule/pressure (croninfo),
        run over the /jobs snapshot, so the panel needs no new endpoint
        and works against an older daemon.  Failures keep the last data.
        """
        try:
            entries = self._pressure_entries()
            start = datetime.datetime.now(datetime.timezone.utc)
            pressure = schedule_pressure(entries, start=start)
            # both suggestions score the pressure walk's own grid, so the
            # panel pays for ONE fire enumeration instead of three
            self.press_suggest = {
                "hourly": suggest_slot(
                    entries, "hourly", start=start, grid=pressure["grid"]
                ),
                "daily": suggest_slot(
                    entries, "daily", start=start, grid=pressure["grid"]
                ),
            }
            self.pressure = pressure
            self.press_dups = duplicate_schedules(entries)
            self.press_computed = time.monotonic()
        except Exception:  # noqa: BLE001 - an analyzer bug must not kill
            logger.exception("schedule pressure recompute failed")
        self.mark()

    async def _recompute_week_bg(self) -> None:
        """The week-calendar walk on a worker thread, like pressure's."""
        if self._week_busy:
            return
        self._week_busy = True
        try:
            await asyncio.to_thread(self._recompute_week)
        finally:
            self._week_busy = False

    def _recompute_week(self) -> None:
        """Refresh the week calendar's data, locally.

        The TUI sibling of the web dashboard's week calendar: every fire
        over the next 7 UTC days, enumerated per job in its own zone by
        the engine itself (the same rows as the pressure panel).  Jobs
        firing more than WEEK_FREQ_MAX times in the window are background
        hum, summarized by name and count instead of flooding the agenda,
        exactly like the web panel's strip.
        """
        try:
            entries = self._pressure_entries()
            now = datetime.datetime.now(datetime.timezone.utc)
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end = start + datetime.timedelta(days=7)
            local_tz = _local_tzinfo()
            grid = [[0] * 24 for _ in range(7)]
            items: List[Tuple[datetime.datetime, str]] = []
            frequent: List[Tuple[str, int, bool]] = []
            # from one second before midnight, so a fire exactly at 00:00
            # lands in the window (occurrences() is strictly-after), the
            # same rule as the web panel
            probe = start - datetime.timedelta(seconds=1)
            for entry in entries:
                zone = entry.timezone or local_tz
                fires: List[datetime.datetime] = []
                capped = False
                for when in entry.tab.occurrences(probe.astimezone(zone)):
                    utc = when.astimezone(datetime.timezone.utc)
                    if utc >= end:
                        break
                    if len(fires) >= WEEK_PER_JOB_CAP:
                        capped = True
                        break
                    fires.append(utc)
                if len(fires) > WEEK_FREQ_MAX:
                    frequent.append((entry.name, len(fires), capped))
                    continue
                for utc in fires:
                    items.append((utc, entry.name))
                    grid[(utc - start).days][utc.hour] += 1
            items.sort(key=lambda item: (item[0], item[1]))
            frequent.sort()
            self.week = {
                "start": start,
                "grid": grid,
                "items": items,
                "frequent": frequent,
                "schedules": len(entries),
            }
            self.week_computed = time.monotonic()
        except Exception:  # noqa: BLE001 - an analyzer bug must not kill
            logger.exception("week calendar recompute failed")
        self.mark()

    async def _load_drawer_runs(self) -> None:
        if not self.drawer_job:
            return
        with contextlib.suppress(Exception):
            self.drawer_runs = await self.api.get_json(
                "/jobs/%s/runs" % _quote(self.drawer_job)
            )
            self.mark()

    async def _load_drawer_resources(self) -> None:
        if not self.drawer_job:
            return
        with contextlib.suppress(Exception):
            self.drawer_res = await self.api.get_json(
                "/jobs/%s/resources" % _quote(self.drawer_job)
            )
            self.mark()

    async def _load_dag_runs(self) -> None:
        if not self.dag_name:
            return
        with contextlib.suppress(Exception):
            data = await self.api.get_json(
                "/dags/%s/runs?limit=50" % _quote(self.dag_name)
            )
            self.dag_runs = data.get("runs", [])
            self.mark()

    async def _load_dag_run(self) -> None:
        if not self.dag_name or not self.dag_run_key:
            return
        with contextlib.suppress(Exception):
            self.dag_run = await self.api.get_json(
                "/dags/%s/runs/%s"
                % (_quote(self.dag_name), _quote(self.dag_run_key))
            )
            self.mark()

    async def _load_dag_xcom(self) -> None:
        if not self.dag_name or not self.dag_run_key:
            return
        with contextlib.suppress(Exception):
            self.dag_xcom = await self.api.get_json(
                "/dags/%s/runs/%s/xcom"
                % (_quote(self.dag_name), _quote(self.dag_run_key))
            )
            self.mark()

    async def _load_state_detail(self) -> None:
        if self.state_tab == "documents":
            namespaces = self._state_namespaces()
            if not namespaces:
                return
            ns = namespaces[min(self.state_sel, len(namespaces) - 1)]
            with contextlib.suppress(Exception):
                self.state_detail = await self.api.get_json(
                    "/state/documents?ns=%s" % _quote(ns)
                )
                self.mark()
        elif self.state_tab == "records":
            streams = self._state_streams()
            if not streams:
                return
            stream = streams[min(self.state_sel, len(streams) - 1)]
            with contextlib.suppress(Exception):
                self.state_detail = await self.api.get_json(
                    "/state/records?stream=%s&limit=100" % _quote(stream)
                )
                self.mark()

    def _state_namespaces(self) -> List[str]:
        data = self.state_data or {}
        docs = data.get("documents") or {}
        if isinstance(docs, dict):
            return sorted(str(k) for k in docs.keys())
        return []

    def _state_streams(self) -> List[str]:
        data = self.state_data or {}
        records = data.get("records") or {}
        if isinstance(records, dict):
            return sorted(
                str(k)
                for k in records.keys()
                if not str(k).startswith("logs/")
            )
        return []

    @staticmethod
    def _dag_state_color(state: str) -> str:
        """Theme colour key for a DAG run/task state string."""
        low = state.lower()
        if low in ("success", "succeeded", "done"):
            return "ok"
        if low in ("failed", "failure", "error"):
            return "fail"
        if low in ("running", "launched"):
            return "run"
        if low in ("awaiting", "waiting", "queued", "pending", "scheduled"):
            return "pending"
        return "dim"

    # ---- implemented by the mixin layers below (one concrete class,
    #      :class:`TuiApp`; the stubs keep each layer type-checkable) ----
    def open_drawer(self, name: str, tab: str = "logs") -> None:
        raise NotImplementedError

    def _close_drawer_streams(self) -> None:
        raise NotImplementedError

    def _close_dag_streams(self) -> None:
        raise NotImplementedError

    async def _boot_sequence(self) -> None:
        raise NotImplementedError

    async def handle_key(self, key: str) -> None:
        raise NotImplementedError

    def paint(self) -> None:
        raise NotImplementedError

    def settings_rows(
        self,
    ) -> List[Tuple[str, str, Callable[[], None]]]:
        raise NotImplementedError

    def timeline_entries(
        self,
    ) -> List[Tuple[str, Optional[str], str, Any, str, Any]]:
        raise NotImplementedError

    def _compose_drawer(
        self,
        paint: "Painter",
        base: List[str],
        cols: int,
        lines: int,
        which: str,
    ) -> List[str]:
        raise NotImplementedError

    def render_overlay(
        self, paint: "Painter", top: str, cols: int, lines: int
    ) -> List[str]:
        raise NotImplementedError


def _quote(text: str) -> str:
    from urllib.parse import quote

    return quote(text, safe="")


# ===================================================================
#  the application: actions
# ===================================================================
class AppActions(App):
    """Operator actions (run/cancel/theme/palette/...), kept apart from
    the state plumbing above purely for readability; ``TuiApp`` at the
    bottom of the file is the concrete class the CLI instantiates."""

    # ---- job actions (instant + toast, exactly like the page) -------
    async def run_job(self, name: str) -> None:
        try:
            status, _ = await self.api.post("/jobs/%s/start" % _quote(name))
        except Unauthorized:
            self.open("token")
            self.focus = "token"
            return
        except Exception as exc:  # noqa: BLE001 - toast + carry on
            self.toast("fail", "start %s: %s" % (name, exc))
            return
        if status == 200:
            self.toast("ok", "▶ started %s" % name)
            self.refresh_now()
        elif status == 409:
            self.toast("warn", "%s is disabled" % name)
        elif status == 404:
            self.toast("fail", "no such job: %s" % name)
        else:
            self.toast("fail", "start %s: HTTP %d" % (name, status))

    async def cancel_job(self, name: str) -> None:
        try:
            status, _ = await self.api.post("/jobs/%s/cancel" % _quote(name))
        except Unauthorized:
            self.open("token")
            self.focus = "token"
            return
        except Exception as exc:  # noqa: BLE001 - toast + carry on
            self.toast("fail", "cancel %s: %s" % (name, exc))
            return
        if status == 200:
            self.toast("ok", "■ cancelled %s" % name)
            self.refresh_now()
        elif status == 409:
            self.toast("warn", "%s is not running" % name)
        else:
            self.toast("fail", "cancel %s: HTTP %d" % (name, status))

    async def run_all_failing(self) -> None:
        failing = [j["name"] for j in self.jobs if health(j)[0] == "fail"]
        if not failing:
            self.toast("info", "nothing failing")
            return
        for name in failing:
            await self.run_job(name)

    def copy_command(self, job: Dict[str, Any]) -> None:
        copy_to_clipboard(self.term, job.get("command", ""))
        self.toast("ok", "❏ copied command")

    # ---- theme + prefs ----------------------------------------------
    def _retheme(self) -> None:
        self.theme = Theme(
            str(self.prefs["theme"]),
            bool(self.prefs["light"]),
            str(self.prefs["cvd"]),
        )
        self.term.invalidate()
        self.mark()

    def save_prefs(self) -> None:
        save_prefs(self.prefs, self.prefs_file)

    def cycle_theme(self) -> None:
        hues = THEME_HUES
        idx = hues.index(str(self.prefs["theme"]))
        self.prefs["theme"] = hues[(idx + 1) % len(hues)]
        self.save_prefs()
        self._retheme()
        self.toast("info", "◐ theme: %s" % self.theme.name)

    def toggle_light_dark(self) -> None:
        self.prefs["light"] = not bool(self.prefs["light"])
        self.save_prefs()
        self._retheme()
        self.toast(
            "info", "◑ %s" % ("paper" if self.prefs["light"] else "phosphor")
        )

    def cycle_cvd(self) -> None:
        idx = CVD_MODES.index(str(self.prefs["cvd"]))
        self.prefs["cvd"] = CVD_MODES[(idx + 1) % len(CVD_MODES)]
        self.save_prefs()
        self._retheme()
        self.toast("info", "◓ color vision: %s" % self.prefs["cvd"])

    def cycle_poll(self) -> None:
        try:
            idx = POLL_CHOICES.index(int(self.prefs["poll_ms"]))
        except ValueError:
            idx = -1
        self.prefs["poll_ms"] = POLL_CHOICES[(idx + 1) % len(POLL_CHOICES)]
        self.save_prefs()
        self.refresh_now()
        label = (
            "paused"
            if not self.prefs["poll_ms"]
            else "%gs" % (self.prefs["poll_ms"] / 1000)
        )
        self.toast("info", "↻ refresh: %s" % label)

    # ---- surfaces ----------------------------------------------------
    def open_drawer(self, name: str, tab: str = "logs") -> None:
        self._close_drawer_streams()
        self.drawer_job = name
        self.drawer_tab = tab
        self.drawer_runs = None
        self.drawer_res = None
        self.log_scroll = 0
        self.log_matches = []
        self.inputs["logsearch"] = ""
        self.open("drawer")
        self._spawn(self._load_drawer_runs())
        if tab == "resources":
            self._spawn(self._load_drawer_resources())
        self.log_tail = LogTail(
            self.api,
            "/jobs/%s/logs" % _quote(name),
            name,
            self.mark,
        )
        self.log_tail.start()

    def _close_drawer_streams(self) -> None:
        if self.log_tail is not None:
            self.log_tail.stop()
            self.log_tail = None
        self.drawer_job = None

    def open_dag(self, name: str) -> None:
        self._close_dag_streams()
        self.dag_name = name
        self.dag_tab = "runs"
        self.dag_runs = []
        self.dag_run = None
        self.dag_run_key = None
        self.dag_xcom = None
        self.dag_sel = 0
        self.open("dag")
        self._spawn(self._load_dag_runs())

    def _close_dag_streams(self) -> None:
        if self.dag_task_tail is not None:
            self.dag_task_tail.stop()
            self.dag_task_tail = None
        self.dag_name = None

    def open_tail(self, names: List[str]) -> None:
        self.open("tail")
        for name in names:
            self.add_tail(name)

    def add_tail(self, name: str) -> None:
        if len(self.tails) >= TAIL_MAX:
            self.toast("warn", "multi-tail is full (%d)" % TAIL_MAX)
            return
        if any(t.label == name for t in self.tails):
            return
        if name not in self.by_name:
            self.toast("fail", "no such job: %s" % name)
            return
        tail = LogTail(
            self.api, "/jobs/%s/logs" % _quote(name), name, self.mark
        )
        tail.start()
        self.tails.append(tail)
        self.mark()

    def tail_preset(self, kind: str) -> None:
        wanted = [j["name"] for j in self.jobs if (health(j)[0] == kind)][
            :TAIL_MAX
        ]
        if not wanted:
            self.toast("info", "no %s jobs right now" % kind)
            return
        self.open_tail(wanted)

    def open_mitigate(self, names: List[str], label: str) -> None:
        self.mitigate_names = names
        self.mitigate_label = label
        self.mitigate_log = []
        self.mitigate_running = False
        self.mitigate_abort = False
        self.open("mitigate")

    async def mitigate_bulk(self, kind: str) -> None:
        """Staggered bulk start/cancel over the mitigate set (web port)."""
        if self.mitigate_running:
            return
        targets = []
        for name in self.mitigate_names:
            job = self.by_name.get(name)
            if job is None:
                continue
            if kind == "cancel":
                if job.get("running"):
                    targets.append(name)
            elif job.get("enabled") and not job.get("running"):
                targets.append(name)
        verb = "cancel" if kind == "cancel" else "start"
        if not targets:
            self.mitigate_log.append("nothing to %s (no eligible jobs)" % verb)
            self.mark()
            return
        self.mitigate_running = True
        self.mitigate_abort = False
        self.mitigate_log.append(
            "— %sing %d job%s —"
            % (verb, len(targets), "s" if len(targets) > 1 else "")
        )
        done = failed = 0
        for name in targets:
            if self.mitigate_abort:
                self.mitigate_log.append(
                    "aborted (%d/%d sent)" % (done, len(targets))
                )
                break
            try:
                status, _ = await self.api.post(
                    "/jobs/%s/%s" % (_quote(name), verb)
                )
                if status == 200:
                    done += 1
                    self.mitigate_log.append("  ✓ %s %s" % (verb, name))
                else:
                    failed += 1
                    self.mitigate_log.append(
                        "  ✕ %s (HTTP %d)" % (name, status)
                    )
            except Unauthorized:
                self.mitigate_log.append("  ! unauthorized — set a token")
                break
            except Exception:  # noqa: BLE001 - keep the sweep going
                failed += 1
                self.mitigate_log.append("  ✕ %s (error)" % name)
            self.mark()
            await asyncio.sleep(0.3)  # the page's 300ms stagger
        self.mitigate_log.append(
            "done: %d ok%s"
            % (done, (", %d failed" % failed) if failed else "")
        )
        self.mitigate_running = False
        self.mark()
        self.refresh_now()

    def mitigate_writeup(self) -> str:
        """The Markdown incident summary the web console copies."""
        lines = [
            "## cronstable incident — %s" % self.mitigate_label,
            "",
            "As of %s:" % utc_clock(),
            "",
        ]
        for name in self.mitigate_names:
            job = self.by_name.get(name) or {}
            last = job.get("last_run") or {}
            exit_code = last.get("exit_code")
            lines.append(
                "- `%s`: exit=%s%s, finished %s"
                % (
                    name,
                    "?" if exit_code is None else exit_code,
                    (
                        ", reason: %s" % last["fail_reason"]
                        if last.get("fail_reason")
                        else ""
                    ),
                    fmt_ago(last.get("finished_at")),
                )
            )
        if self.verdict:
            lines += [
                "",
                "Verdict: %s — %s"
                % (self.verdict["head"], self.verdict["sub"]),
            ]
        return "\n".join(lines) + "\n"

    def ack_alarm(self) -> None:
        if self.any_failing and not self.alarm_ack:
            self.alarm_ack = True
            self.toast("info", "◔ alarm acknowledged")

    def set_wallboard(self, value: bool) -> None:
        self.wallboard = value
        self.zen_on = False
        self.term.invalidate()
        self.mark()

    async def dag_trigger(self, name: str) -> None:
        try:
            status, payload = await self.api.post(
                "/dags/%s/trigger" % _quote(name)
            )
        except Unauthorized:
            self.open("token")
            self.focus = "token"
            return
        except Exception as exc:  # noqa: BLE001
            self.toast("fail", "trigger %s: %s" % (name, exc))
            return
        if status == 200 and isinstance(payload, dict):
            self.toast("ok", "▶ %s run %s" % (name, payload.get("runKey", "")))
            self._spawn(self._load_dag_runs())
        else:
            self.toast("fail", "trigger %s: HTTP %d" % (name, status))

    async def dag_decision(self, task_key: str, decision: str) -> None:
        if not self.dag_name or not self.dag_run_key:
            return
        try:
            status, _ = await self.api.post(
                "/dags/%s/runs/%s/tasks/%s/decision"
                % (
                    _quote(self.dag_name),
                    _quote(self.dag_run_key),
                    _quote(task_key),
                ),
                body={"decision": decision, "by": "tui"},
            )
        except Unauthorized:
            self.open("token")
            self.focus = "token"
            return
        except Exception as exc:  # noqa: BLE001
            self.toast("fail", "%s: %s" % (decision, exc))
            return
        if status == 200:
            self.toast("ok", "%s %s" % (decision, task_key))
            self._spawn(self._load_dag_run())
        else:
            self.toast("fail", "%s: HTTP %d" % (decision, status))

    async def dag_backfill(self, spec: str) -> None:
        """``from..to`` ISO dates from the backfill input row."""
        if not self.dag_name:
            return
        parts = [p.strip() for p in re.split(r"\.\.| ", spec) if p.strip()]
        if len(parts) != 2:
            self.toast("warn", "backfill wants: FROM..TO (ISO dates)")
            return
        try:
            status, payload = await self.api.post(
                "/dags/%s/backfill" % _quote(self.dag_name),
                body={"from": parts[0], "to": parts[1]},
            )
        except Unauthorized:
            self.open("token")
            self.focus = "token"
            return
        except Exception as exc:  # noqa: BLE001
            self.toast("fail", "backfill: %s" % exc)
            return
        if status == 200:
            self.toast("ok", "▶ backfill queued")
            self._spawn(self._load_dag_runs())
        else:
            detail = ""
            if isinstance(payload, dict) and payload.get("error"):
                detail = " — %s" % payload["error"]
            self.toast("fail", "backfill: HTTP %d%s" % (status, detail))

    def save_log(self) -> None:
        """The Logs tab's download button: write the buffer to a file."""
        tail = self.log_tail
        if tail is None or not self.drawer_job:
            return
        stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        path = os.path.join(
            os.path.expanduser("~"),
            "cronstable-%s-%s.log" % (self.drawer_job, stamp),
        )
        try:
            with open(path, "w", encoding="utf-8") as fh:
                for stream, line, _ in tail.lines:
                    fh.write("[%s] %s\n" % (stream, strip_ansi(line)))
        except OSError as exc:
            self.toast("fail", "save failed: %s" % exc)
            return
        self.toast("ok", "saved %s" % path)

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> None:
        task: "asyncio.Task[None]" = asyncio.get_running_loop().create_task(
            coro
        )
        task.add_done_callback(lambda t: t.exception())


# ===================================================================
#  the application: command palette
# ===================================================================
class AppPalette(AppActions):
    def palette_commands(self) -> List[Tuple[str, str, Callable[[], Any]]]:
        """(icon, label, action) rows: the page's three command pools.

        Web-only rows (CRT effects, desktop notifications, UI scale, run
        ledger, columns) have no terminal analogue and are omitted; the
        panel toggles reach the TUI's overlay screens instead of inline
        cards, which is the same action under the same name.
        """
        out: List[Tuple[str, str, Callable[[], Any]]] = [
            ("↻", "Refresh now", self.refresh_now),
            (
                "▶",
                "Run all failing jobs",
                lambda: self._spawn(self.run_all_failing()),
            ),
            ("◐", "Cycle theme", self.cycle_theme),
            ("◑", "Toggle light / dark", self.toggle_light_dark),
            ("◓", "Cycle color vision mode", self.cycle_cvd),
            ("⊟", "Toggle compact density", self._toggle_compact),
            ("♪", "Toggle audible cues", self._toggle_sound),
            ("⌁", "Toggle next-fire radar", lambda: self._toggle("radar")),
            ("▦", "Toggle activity heatmap", lambda: self._toggle("heat")),
            ("▥", "Toggle schedule pressure", lambda: self._toggle("press")),
            ("◫", "Toggle week calendar", lambda: self._toggle("week")),
            ("⊞", "Toggle fleet view", lambda: self._toggle("fleet")),
            ("◉", "Toggle cluster panel", lambda: self._toggle("cluster")),
            ("▤", "Toggle node resources", lambda: self._toggle("node")),
            ("≋", "Multi-tail console", lambda: self.open_tail([])),
            (
                "≋",
                "Multi-tail: failing jobs",
                lambda: self.tail_preset("fail"),
            ),
            ("≋", "Multi-tail: running jobs", lambda: self.tail_preset("run")),
            (
                "▣",
                "Wallboard / TV mode",
                lambda: self.set_wallboard(not self.wallboard),
            ),
            ("▤", "Incident timeline", lambda: self.open("timeline")),
            ("▸", "Mitigate failing jobs", self._mitigate_failing),
            ("◴", "Cron sandbox", lambda: self.open("sandbox")),
            ("▮", "Toggle boot self-test", self._toggle_boot),
            ("❏", "Copy version", lambda: self._copy_chip(self.version)),
            ("❏", "Copy job set id", lambda: self._copy_chip(self.job_set_id)),
            ("⚙", "Open settings", lambda: self.open("settings")),
            ("?", "Keyboard shortcuts", lambda: self.open("help")),
            ("⚿", "Set access token", self._open_token),
            ("⧉", "Toggle DAGs panel", self._toggle_dags),
            ("⛁", "Toggle state inspector", lambda: self._toggle("state")),
            ("⌕", "Focus filter", self._focus_filter),
        ]
        ascii_mode = bool(self.prefs["ascii"])
        for dag in self.dags:
            name = str(dag.get("name", ""))
            out.append(
                ("⧉", "DAG: %s" % name, functools.partial(self.open_dag, name))
            )
            out.append(
                (
                    "▶",
                    "Trigger DAG: %s" % name,
                    functools.partial(self._act_trigger_dag, name),
                )
            )
        for job in self.jobs:
            name = str(job.get("name", ""))
            key = health(job)[0]
            glyph = (GLYPH_ASCII if ascii_mode else GLYPH).get(key, "?")
            out.append(
                (
                    glyph,
                    "Logs: %s" % name,
                    functools.partial(self.open_drawer, name, "logs"),
                )
            )
            if job.get("enabled") and not job.get("running"):
                out.append(
                    (
                        "▶",
                        "Run: %s" % name,
                        functools.partial(self._act_run_job, name),
                    )
                )
            if job.get("running"):
                out.append(
                    (
                        "■",
                        "Cancel: %s" % name,
                        functools.partial(self._act_cancel_job, name),
                    )
                )
            out.append(
                (
                    "❏",
                    "Copy command: %s" % name,
                    functools.partial(self._copy_job_command, name),
                )
            )
            out.append(
                (
                    "◴",
                    "Schedule: %s" % name,
                    functools.partial(self.open_drawer, name, "schedule"),
                )
            )
            out.append(
                (
                    "≋",
                    "Tail: %s" % name,
                    functools.partial(self._act_tail_one, name),
                )
            )
        return out

    def _act_trigger_dag(self, name: str) -> None:
        self._spawn(self.dag_trigger(name))

    def _act_run_job(self, name: str) -> None:
        self._spawn(self.run_job(name))

    def _act_cancel_job(self, name: str) -> None:
        self._spawn(self.cancel_job(name))

    def _act_tail_one(self, name: str) -> None:
        self.open_tail([name])

    def palette_matches(
        self,
    ) -> List[Tuple[str, str, Callable[[], Any]]]:
        query = self.inputs["palette"].strip()
        scored = [
            (fuzzy(query, label), (icon, label, action))
            for icon, label, action in self.palette_commands()
        ]
        matches = [
            item
            for score, item in sorted(scored, key=lambda pair: -pair[0])
            if score > 0
        ]
        return matches[:60]

    # small palette helpers -------------------------------------------
    def _toggle(self, name: str) -> None:
        if self.is_open(name):
            self.close(name)
            return
        self.open(name)
        if name == "fleet":
            self._spawn(self._refresh_json("fleet", "/fleet"))
        elif name == "state":
            self._spawn(self._refresh_json("state_data", "/state"))
        elif name == "heat":
            self._spawn(self._load_heat())
        elif name == "press":
            self._spawn(self._recompute_pressure_bg())
        elif name == "week":
            self._spawn(self._recompute_week_bg())
        elif name == "node":
            self._spawn(self._refresh_json("node_history", "/node/history"))

    async def _refresh_json(self, attr: str, path: str) -> None:
        with contextlib.suppress(Exception):
            setattr(self, attr, await self.api.get_json(path))
            self.mark()

    def _toggle_dags(self) -> None:
        if self.is_open("dags"):
            self.close("dags")
            return
        self.open("dags")
        self._spawn(self._load_dags())

    def _toggle_compact(self) -> None:
        self.prefs["compact"] = not bool(self.prefs["compact"])
        self.save_prefs()
        self.toast(
            "info",
            "⊟ compact: %s" % ("on" if self.prefs["compact"] else "off"),
        )

    def _toggle_sound(self) -> None:
        self.prefs["sound"] = not bool(self.prefs["sound"])
        self.save_prefs()
        self.toast(
            "info", "♪ cues: %s" % ("on" if self.prefs["sound"] else "off")
        )

    def _toggle_boot(self) -> None:
        self.prefs["boot"] = not bool(self.prefs["boot"])
        self.save_prefs()
        self.toast(
            "info",
            "▮ boot self-test: %s" % ("on" if self.prefs["boot"] else "off"),
        )

    def _mitigate_failing(self) -> None:
        failing = [j["name"] for j in self.jobs if health(j)[0] == "fail"]
        self.open_mitigate(failing, "failing jobs")

    def _copy_chip(self, value: str) -> None:
        if value:
            copy_to_clipboard(self.term, value)
            self.toast("ok", "❏ copied")

    def _copy_job_command(self, name: str) -> None:
        job = self.by_name.get(name)
        if job:
            self.copy_command(job)

    def _open_token(self) -> None:
        self.open("token")
        self.focus = "token"

    def _focus_filter(self) -> None:
        self.focus = "filter"


# ===================================================================
#  the application: key dispatch (the web keymap, ported)
# ===================================================================
class AppKeys(AppPalette):
    async def handle_key(self, key: str) -> None:
        """One key press.  Structure and guards mirror the web page's
        single keydown handler: palette submode first, then the palette
        chord (global, even over the wallboard), Esc close-priority,
        focused-field editing, ``w``/``a`` (list or wallboard, never in
        overlays), overlay-local keys, and finally the list keys."""
        if key == "ctrl+c":
            self.quit = True
            return
        if self.booting:
            return  # the boot sequence consumes its own skip key
        if self.is_open("palette"):
            await self._palette_key(key)
            return
        if key in ("ctrl+k", "ctrl+p"):
            if self.wallboard:
                # the TV board composes no overlays: an invisible
                # palette would swallow keys and could fire unseen
                # actions on Enter, so it stays inert like every
                # other surface here (leave with w or Esc first)
                return
            self.open("palette")
            self.inputs["palette"] = ""
            self.palette_sel = 0
            self.focus = "palette"
            return
        if key == "esc":
            self._close_topmost()
            return
        if self.focus is not None:
            await self._input_key(self.focus, key)
            return
        if key in ("w", "a") and not self.open_overlays:
            if key == "w":
                self.set_wallboard(not self.wallboard)
            else:
                self.ack_alarm()
            return
        if self.wallboard:
            return  # everything else is inert on the TV board
        top = self.top_overlay()
        if top is not None:
            await self._overlay_key(top, key)
            return
        await self._list_key(key)

    # ---------------------------------------------------------------
    def _close_topmost(self) -> None:
        if self.focus == "logsearch":
            # blur the in-drawer search box first; Esc again closes
            # the drawer itself (the other inputs live in dedicated
            # overlays, which close() blurs via INPUT_HOMES)
            self.focus = None
            self.mark()
            return
        for name in ESC_PRIORITY:
            if self.is_open(name):
                self.close(name)
                return
        if self.focus is not None:
            self.focus = None
            self.mark()
            return
        if self.wallboard:
            self.set_wallboard(False)

    async def _palette_key(self, key: str) -> None:
        matches = self.palette_matches()
        if key == "esc":
            self.close("palette")
            self.focus = None
            return
        if key == "down":
            self.palette_sel = min(
                self.palette_sel + 1, max(0, len(matches) - 1)
            )
            return
        if key == "up":
            self.palette_sel = max(0, self.palette_sel - 1)
            return
        if key == "enter":
            if matches:
                idx = min(self.palette_sel, len(matches) - 1)
                action = matches[idx][2]
                self.close("palette")
                self.focus = None
                result = action()
                if asyncio.iscoroutine(result):  # pragma: no cover
                    await result
            return
        self._edit_input("palette", key)
        self.palette_sel = 0

    async def _input_key(self, name: str, key: str) -> None:
        if key == "enter":
            await self._input_commit(name)
            return
        if key == "tab" and name == "filter":
            self.focus = None
            return
        self._edit_input(name, key)

    async def _input_commit(self, name: str) -> None:
        value = self.inputs[name].strip()
        if name == "filter":
            self.focus = None
        elif name == "token":
            self.api.token = value or None
            self.close("token")
            self.focus = None
            self.toast("ok", "⚿ token %s" % ("set" if value else "cleared"))
            self.refresh_now()
        elif name == "logsearch":
            # release the input so n/N/f/t/w/d act on the drawer again,
            # and land on the current (first) match, not the second
            self.focus = None
            self._log_search_jump(0)
        elif name == "tailadd":
            self.inputs["tailadd"] = ""
            self.focus = None
            if value:
                match = self._match_job(value)
                if match:
                    self.add_tail(match)
                else:
                    self.toast("fail", "no job matches %r" % value)
        elif name == "sandbox":
            self.focus = None  # results render live; Enter just settles
        elif name == "backfill":
            spec = value
            self.inputs["backfill"] = ""
            self.focus = None
            if spec:
                await self.dag_backfill(spec)

    def _match_job(self, query: str) -> Optional[str]:
        if query in self.by_name:
            return query
        scored = sorted(
            ((fuzzy(query, name), name) for name in self.by_name),
            key=lambda pair: -pair[0],
        )
        if scored and scored[0][0] > 0:
            return scored[0][1]
        return None

    def _edit_input(self, name: str, key: str) -> None:
        buf = self.inputs[name]
        if key == "backspace":
            buf = buf[:-1]
        elif key == "ctrl+u":
            buf = ""
        elif len(key) == 1 and key >= " ":
            buf += key
        else:
            return
        self.inputs[name] = buf
        if name == "filter":
            self.filter_text = buf
            self.recompute_view()
        if name == "logsearch":
            self._log_search_recompute(reset=True)
        self.mark()

    # ---------------------------------------------------------------
    async def _list_key(self, key: str) -> None:
        if self._list_move_key(key):
            return
        if key == "/":
            self.focus = "filter"
        elif key == "?":
            self.open("help")
        elif key == "g":
            self.refresh_now()
            self.toast("info", "↻ refreshing")
        elif key == "t":
            self.cycle_theme()
        elif key == "T":
            self.toggle_light_dark()
        elif key == "i":
            if self.is_open("timeline"):
                self.close("timeline")
            else:
                self.open("timeline")
        elif key == "enter":
            job = self.selected_job()
            if job:
                self.open_drawer(job["name"], "logs")
        elif key == "r":
            job = self.selected_job()
            if job and job.get("enabled") and not job.get("running"):
                await self.run_job(job["name"])
        elif key == "x":
            job = self.selected_job()
            if job and job.get("running"):
                await self.cancel_job(job["name"])
        elif key == "c":
            job = self.selected_job()
            if job:
                self.copy_command(job)
        else:
            self._list_extra_key(key)

    def _list_move_key(self, key: str) -> bool:
        """Selection movement; ``j``/``k`` wrap like the web table."""
        view = self.view
        if key in ("j", "down"):
            if view:
                self.sel = (self.sel + 1) % len(view)
        elif key in ("k", "up"):
            if view:
                self.sel = (self.sel - 1) % len(view)
        elif key == "pgdn":
            self.sel = min(len(view) - 1, self.sel + 10) if view else 0
        elif key == "pgup":
            self.sel = max(0, self.sel - 10)
        elif key == "home":
            self.sel = 0
        elif key == "end":
            self.sel = max(0, len(view) - 1)
        else:
            return False
        return True

    def _list_extra_key(self, key: str) -> None:
        """Terminal-only extras (grouped separately in the ? overlay)."""
        if key == "q":
            self.quit = True
        elif key == "s":
            idx = SORT_KEYS.index(self.sort_key)
            self.sort_key = SORT_KEYS[(idx + 1) % len(SORT_KEYS)]
            self.recompute_view()
            self.toast("info", "sort: %s" % self.sort_key)
        elif key == "S":
            self.sort_dir = -self.sort_dir
            self.recompute_view()
            self.toast(
                "info",
                "sort: %s%s"
                % (self.sort_key, " ↓" if self.sort_dir < 0 else " ↑"),
            )
        elif key == "f":
            idx = STATUS_SEGMENTS.index(self.status_filter)
            self.status_filter = STATUS_SEGMENTS[
                (idx + 1) % len(STATUS_SEGMENTS)
            ]
            self.recompute_view()
        elif key == "m":
            self.open_tail([])

    # ---------------------------------------------------------------
    async def _overlay_key(self, top: str, key: str) -> None:
        handler = getattr(self, "_key_" + top, None)
        if handler is not None:
            await handler(key)

    async def _key_help(self, key: str) -> None:
        if key == "?":
            self.close("help")
        elif key in ("j", "down"):
            self.panel_scroll += 1
        elif key in ("k", "up"):
            self.panel_scroll = max(0, self.panel_scroll - 1)

    async def _key_settings(self, key: str) -> None:
        rows = self.settings_rows()
        if key in ("j", "down"):
            self.settings_sel = min(len(rows) - 1, self.settings_sel + 1)
        elif key in ("k", "up"):
            self.settings_sel = max(0, self.settings_sel - 1)
        elif key in ("enter", " ", "right", "left"):
            idx = min(self.settings_sel, len(rows) - 1)
            rows[idx][2]()  # cycle/toggle the row's value

    async def _key_token(self, key: str) -> None:
        # all typing routes through the focused-input path; nothing else
        self.focus = "token"
        await self._input_key("token", key)

    async def _key_timeline(self, key: str) -> None:
        entries = self.timeline_entries()
        if key in ("j", "down"):
            self.timeline_sel = min(
                max(0, len(entries) - 1), self.timeline_sel + 1
            )
        elif key in ("k", "up"):
            self.timeline_sel = max(0, self.timeline_sel - 1)
        elif key == "f":
            self.timeline_fail_only = not self.timeline_fail_only
            self.timeline_sel = 0
        elif key == "m":
            names = self.incident_set or [
                j["name"] for j in self.jobs if health(j)[0] == "fail"
            ]
            self.close("timeline")
            self.open_mitigate(names, "incident set")
        elif key == "enter":
            if entries:
                idx = min(self.timeline_sel, len(entries) - 1)
                self.close("timeline")
                self.open_drawer(entries[idx][0], "logs")

    async def _key_mitigate(self, key: str) -> None:
        if key == "s" and not self.mitigate_running:
            self._spawn(self.mitigate_bulk("start"))
        elif key == "x" and not self.mitigate_running:
            self._spawn(self.mitigate_bulk("cancel"))
        elif key == "a" and self.mitigate_running:
            self.mitigate_abort = True
        elif key == "y":
            writeup = self.mitigate_writeup()
            copy_to_clipboard(self.term, writeup)
            stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            path = os.path.join(
                os.path.expanduser("~"),
                "cronstable-incident-%s.md" % stamp,
            )
            try:
                with open(path, "w", encoding="utf-8") as fh:
                    fh.write(writeup)
                self.toast("ok", "writeup: %s (and clipboard)" % path)
            except OSError:
                self.toast("ok", "writeup copied to clipboard")

    async def _key_sandbox(self, key: str) -> None:
        self.focus = "sandbox"
        await self._input_key("sandbox", key)

    async def _key_dags(self, key: str) -> None:
        if key in ("j", "down"):
            self.dags_sel = min(max(0, len(self.dags) - 1), self.dags_sel + 1)
        elif key in ("k", "up"):
            self.dags_sel = max(0, self.dags_sel - 1)
        elif key == "enter":
            if self.dags:
                idx = min(self.dags_sel, len(self.dags) - 1)
                self.open_dag(str(self.dags[idx].get("name", "")))
        elif key == "t":
            if self.dags:
                idx = min(self.dags_sel, len(self.dags) - 1)
                await self.dag_trigger(str(self.dags[idx].get("name", "")))
        elif key == "r":
            self._spawn(self._load_dags())

    async def _key_state(self, key: str) -> None:
        tabs = ["view", "documents", "records"]
        if key in ("left", "right", "tab", "shift+tab"):
            step = -1 if key in ("left", "shift+tab") else 1
            idx = (tabs.index(self.state_tab) + step) % len(tabs)
            self.state_tab = tabs[idx]
            self.state_sel = 0
            self.state_detail = None
        elif key in ("j", "down"):
            self.state_sel += 1
        elif key in ("k", "up"):
            self.state_sel = max(0, self.state_sel - 1)
        elif key == "enter":
            await self._load_state_detail()
        elif key == "r":
            self._spawn(self._refresh_json("state_data", "/state"))

    async def _key_cluster(self, key: str) -> None:
        await self._panel_scroll_key(key)

    async def _key_fleet(self, key: str) -> None:
        if key == "f":
            self.fleet_fail_only = not self.fleet_fail_only
        elif key == "r":
            self._spawn(self._refresh_json("fleet", "/fleet"))
        else:
            await self._panel_scroll_key(key)

    async def _key_heat(self, key: str) -> None:
        if key == "r":
            self.heat_loaded = 0.0
            self._spawn(self._load_heat())
        else:
            await self._panel_scroll_key(key)

    async def _key_press(self, key: str) -> None:
        if key == "r":
            await self._recompute_pressure_bg()
        else:
            await self._panel_scroll_key(key)

    async def _key_week(self, key: str) -> None:
        if key == "r":
            await self._recompute_week_bg()
        else:
            await self._panel_scroll_key(key)

    async def _key_radar(self, key: str) -> None:
        await self._panel_scroll_key(key)

    async def _key_node(self, key: str) -> None:
        await self._panel_scroll_key(key)

    async def _panel_scroll_key(self, key: str) -> None:
        if key in ("j", "down"):
            self.panel_scroll += 1
        elif key in ("k", "up"):
            self.panel_scroll = max(0, self.panel_scroll - 1)
        elif key == "pgdn":
            self.panel_scroll += 10
        elif key == "pgup":
            self.panel_scroll = max(0, self.panel_scroll - 10)
        elif key == "home":
            self.panel_scroll = 0

    async def _key_tail(self, key: str) -> None:
        if key == "a":
            self.focus = "tailadd"
        elif key == "x":
            if self.tails:
                idx = min(self.tail_sel, len(self.tails) - 1)
                self.tails.pop(idx).stop()
                self.tail_sel = max(0, self.tail_sel - 1)
        elif key in ("j", "down"):
            self.tail_sel = min(max(0, len(self.tails) - 1), self.tail_sel + 1)
        elif key in ("k", "up"):
            self.tail_sel = max(0, self.tail_sel - 1)
        elif key == "t":
            self.timestamps = not self.timestamps
        elif key == "w":
            self.wrap = not self.wrap
        elif key == "pgup":
            self.panel_scroll += 10
        elif key == "pgdn":
            self.panel_scroll = max(0, self.panel_scroll - 10)
        elif key == "end":
            self.panel_scroll = 0

    # ---- the job drawer ---------------------------------------------
    DRAWER_TABS = ["logs", "history", "resources", "schedule"]

    async def _key_drawer(self, key: str) -> None:
        if key in ("left", "right", "tab", "shift+tab"):
            step = -1 if key in ("left", "shift+tab") else 1
            tabs = self.DRAWER_TABS
            idx = (tabs.index(self.drawer_tab) + step) % len(tabs)
            self.drawer_tab = tabs[idx]
            self.panel_scroll = 0
            if self.drawer_tab == "history" and self.drawer_runs is None:
                self._spawn(self._load_drawer_runs())
            if self.drawer_tab == "resources" and self.drawer_res is None:
                self._spawn(self._load_drawer_resources())
            return
        if self.drawer_tab == "logs":
            await self._key_drawer_logs(key)
            return
        if key in ("j", "down"):
            self.panel_scroll += 1
        elif key in ("k", "up"):
            self.panel_scroll = max(0, self.panel_scroll - 1)
        elif key == "pgdn":
            self.panel_scroll += 10
        elif key == "pgup":
            self.panel_scroll = max(0, self.panel_scroll - 10)
        elif key == "r" and self.drawer_job:
            await self.run_job(self.drawer_job)
        elif key == "x" and self.drawer_job:
            await self.cancel_job(self.drawer_job)

    async def _key_drawer_logs(self, key: str) -> None:
        if key == "/":
            self.focus = "logsearch"
        elif key in ("j", "down"):
            self.log_scroll = max(0, self.log_scroll - 1)
        elif key in ("k", "up"):
            self.log_scroll += 1
        elif key == "pgup":
            self.log_scroll += 10
        elif key == "pgdn":
            self.log_scroll = max(0, self.log_scroll - 10)
        elif key == "end":
            self.log_scroll = 0  # back to following the tail
        elif key == "home":
            self.log_scroll = 10**9  # clamped to the top at render
        elif key == "n":
            self._log_search_jump(1)
        elif key == "N":
            self._log_search_jump(-1)
        elif key == "f" and self.log_tail is not None:
            self.log_tail.follow = not self.log_tail.follow
            self.toast(
                "info",
                "follow: %s" % ("on" if self.log_tail.follow else "off"),
            )
        elif key == "t":
            self.timestamps = not self.timestamps
            self.prefs["timestamps"] = self.timestamps
            self.save_prefs()
        elif key == "w":
            self.wrap = not self.wrap
            self.prefs["wrap"] = self.wrap
            self.save_prefs()
        elif key == "d":
            self.save_log()
        elif key == "r" and self.drawer_job:
            await self.run_job(self.drawer_job)
        elif key == "x" and self.drawer_job:
            await self.cancel_job(self.drawer_job)

    def _log_search_recompute(self, reset: bool = False) -> None:
        """Rebuild the match list; keep the cursor on its match unless
        ``reset`` (the needle changed): resetting on every repaint or
        jump would pin n/N to the same match forever."""
        needle = self.inputs["logsearch"].strip().lower()
        tail = self.log_tail
        prev: Optional[int] = None
        if not reset and self.log_matches:
            prev = self.log_matches[
                min(self.log_match_idx, len(self.log_matches) - 1)
            ]
        self.log_matches = []
        self.log_match_idx = 0
        if not needle or tail is None:
            return
        for idx, (_, line, _) in enumerate(tail.lines):
            if needle in strip_ansi(line).lower():
                self.log_matches.append(idx)
        if prev is not None and self.log_matches:
            for pos, line_idx in enumerate(self.log_matches):
                if line_idx >= prev:
                    self.log_match_idx = pos
                    break
            else:
                self.log_match_idx = len(self.log_matches) - 1

    def _log_search_jump(self, step: int) -> None:
        self._log_search_recompute()
        tail = self.log_tail
        if not self.log_matches or tail is None:
            return
        self.log_match_idx = (self.log_match_idx + step) % len(
            self.log_matches
        )
        target = self.log_matches[self.log_match_idx]
        # scroll so the match sits ~centre; render clamps the rest
        self.log_scroll = max(0, len(tail.lines) - target - 1)
        self.mark()

    # ---- the DAG drawer ---------------------------------------------
    DAG_TABS = ["runs", "graph", "tasks", "xcom", "logs"]

    async def _key_dag(self, key: str) -> None:
        if key in ("left", "right"):
            step = -1 if key == "left" else 1
            tabs = self.DAG_TABS
            idx = (tabs.index(self.dag_tab) + step) % len(tabs)
            self.dag_tab = tabs[idx]
            self.dag_sel = 0
            self.panel_scroll = 0
            if self.dag_tab == "xcom" and self.dag_xcom is None:
                self._spawn(self._load_dag_xcom())
            return
        if key == "t" and self.dag_name:
            await self.dag_trigger(self.dag_name)
            return
        if key == "b":
            self.focus = "backfill"
            return
        if self.dag_tab == "runs":
            if key in ("j", "down"):
                self.dag_sel = min(
                    max(0, len(self.dag_runs) - 1), self.dag_sel + 1
                )
            elif key in ("k", "up"):
                self.dag_sel = max(0, self.dag_sel - 1)
            elif key == "enter" and self.dag_runs:
                idx = min(self.dag_sel, len(self.dag_runs) - 1)
                run = self.dag_runs[idx]
                self.dag_run_key = str(
                    run.get("runKey") or run.get("run_key") or ""
                )
                self.dag_tab = "tasks"
                self.dag_sel = 0
                self._spawn(self._load_dag_run())
            return
        if self.dag_tab == "tasks":
            tasks = self.dag_run_tasks()
            if key in ("j", "down"):
                self.dag_sel = min(max(0, len(tasks) - 1), self.dag_sel + 1)
            elif key in ("k", "up"):
                self.dag_sel = max(0, self.dag_sel - 1)
            elif key in ("a", "R") and tasks:
                idx = min(self.dag_sel, len(tasks) - 1)
                task_key = str(tasks[idx].get("key", ""))
                decision = "approve" if key == "a" else "reject"
                await self.dag_decision(task_key, decision)
            elif key == "enter" and tasks:
                idx = min(self.dag_sel, len(tasks) - 1)
                self._open_dag_task_logs(str(tasks[idx].get("key", "")))
            return
        await self._panel_scroll_key(key)

    def dag_run_tasks(self) -> List[Dict[str, Any]]:
        run = self.dag_run or {}
        tasks = run.get("tasks")
        if isinstance(tasks, list):
            return tasks
        if isinstance(tasks, dict):
            return [
                dict(v, key=k) if isinstance(v, dict) else {"key": k}
                for k, v in tasks.items()
            ]
        return []

    def _open_dag_task_logs(self, task_key: str) -> None:
        if not self.dag_name or not self.dag_run_key:
            return
        if self.dag_task_tail is not None:
            self.dag_task_tail.stop()
        self.dag_task_tail = LogTail(
            self.api,
            "/dags/%s/runs/%s/tasks/%s/logs"
            % (
                _quote(self.dag_name),
                _quote(self.dag_run_key),
                _quote(task_key),
            ),
            task_key,
            self.mark,
        )
        self.dag_task_tail.follow = False  # a finished task's log ends
        self.dag_task_tail.start()
        self.dag_tab = "logs"
        self.panel_scroll = 0


# ===================================================================
#  the application: rendering, part 1 (base screen + wallboard)
# ===================================================================
class AppRender(AppKeys):
    def paint(self) -> None:
        cols, lines = self.term.size()
        paint = Painter(self.theme)
        if self.booting:
            rows = [paint.row(r) for r in self.boot_rows]
        elif self.wallboard:
            rows = (
                self.render_zen(paint, cols, lines)
                if self.zen_on
                else self.render_wallboard(paint, cols, lines)
            )
        else:
            rows = self.render_base(paint, cols, lines)
            top = self.top_overlay()
            if top in ("drawer", "dag"):
                rows = self._compose_drawer(paint, rows, cols, lines, top)
            elif top is not None:
                panel = self.render_overlay(paint, top, cols, lines)
                fill = self.theme.bg("bg") + DIM_SGR
                rows = overlay_center(rows, panel, cols, lines, fill)
            rows = self._compose_toasts(paint, rows, cols, lines)
        self.term.paint(rows, self.theme.bg("bg"))

    # ---------------------------------------------------------------
    def render_base(self, paint: Painter, cols: int, lines: int) -> List[str]:
        rows = [self.render_header(paint, cols)]
        rows.append(self.render_toolbar(paint, cols))
        if self.verdict is not None:
            rows.append(self.render_verdict_bar(paint, cols))
        body_top = len(rows) + 1  # + column headers
        body_rows = max(1, lines - body_top - 1)
        rows.extend(self.render_table(paint, cols, body_rows))
        while len(rows) < lines - 1:
            rows.append(paint.row())
        rows = rows[: lines - 1]
        rows.append(self.render_footer(paint, cols))
        return rows

    def render_header(self, paint: Painter, cols: int) -> str:
        left = [
            paint.style(" cronstable ", "bright", bold=True),
            paint.style("⌁ tui ", "accent"),
        ]
        if self.version:
            left.append(paint.style("v%s " % self.version, "dim"))
        if self.job_set_id:
            left.append(paint.style("· %s " % self.job_set_id[:10], "dim"))
        node = self.node or {}
        stats = node.get("resources") or None
        if stats:
            left.append(
                paint.style(
                    "· %s cpu %s mem "
                    % (
                        fmt_percent(stats.get("cpu_percent")),
                        fmt_percent(stats.get("mem_percent")),
                    ),
                    "dim",
                )
            )
        if self.connected:
            live = paint.style("● live ", "ok")
        else:
            live = paint.style(
                "◌ %s " % (self.conn_error or "offline"), "fail", bold=True
            )
        clock = paint.style(utc_clock() + " ", "bright")
        left_text = "".join(left)
        right_text = live + clock
        gap = cols - text_width(left_text) - text_width(right_text)
        return paint.row(left_text, paint.style(" " * max(1, gap)), right_text)

    def render_toolbar(self, paint: Painter, cols: int) -> str:
        filter_active = self.focus == "filter"
        filt = self.inputs["filter"]
        box = "/" + (filt if filt else ("…" if not filter_active else ""))
        if filter_active:
            box += "▌"
        spans = [
            paint.style(
                " %s " % pad_to(box, 18),
                "bright" if filter_active else "dim",
                bg="sel" if filter_active else None,
            )
        ]
        for seg in STATUS_SEGMENTS:
            active = seg == self.status_filter
            spans.append(
                paint.style(
                    " %s " % seg,
                    "bright" if active else "dim",
                    bg="sel" if active else None,
                    bold=active,
                )
            )
        spans.append(
            paint.style(
                "  sort:%s%s"
                % (self.sort_key, "↓" if self.sort_dir < 0 else "↑"),
                "dim",
            )
        )
        poll_ms = int(self.prefs["poll_ms"])
        spans.append(
            paint.style(
                "  poll:%s"
                % ("off" if not poll_ms else "%gs" % (poll_ms / 1000)),
                "dim",
            )
        )
        counts: Dict[str, int] = {}
        for job in self.jobs:
            counts[health(job)[0]] = counts.get(health(job)[0], 0) + 1
        spans.append(paint.style("   %d jobs " % len(self.jobs), "fg"))
        for key, color in (("run", "run"), ("fail", "fail"), ("ok", "ok")):
            if counts.get(key):
                spans.append(
                    paint.style(
                        "%s %d "
                        % (
                            paint.glyph(key, bool(self.prefs["ascii"])),
                            counts[key],
                        ),
                        color,
                    )
                )
        return cut_to_width(paint.row(*spans), cols)

    def render_verdict_bar(self, paint: Painter, cols: int) -> str:
        v = self.verdict or {}
        color = "fail" if v.get("sev") == "crit" else "warn"
        text = " %s %s — %s" % (
            v.get("glyph", "▲"),
            v.get("head", ""),
            v.get("sub", ""),
        )
        if v.get("ago"):
            text += " · %s" % fmt_ago(v["ago"])
        text += "   (i timeline)"
        return cut_to_width(
            paint.style(pad_to(text, cols), "bright", bg=color, bold=True),
            cols,
        )

    # ---- the jobs table ---------------------------------------------
    def _columns(self, cols: int) -> List[Tuple[str, int]]:
        """(column, width) picks that fit ``cols``, widest board first."""
        compact = bool(self.prefs["compact"])
        spread = any("clusterOwner" in j for j in self.jobs)
        monitored = any(
            j.get("running_resources") is not None for j in self.jobs
        )
        layout: List[Tuple[str, int]] = [("status", 11), ("name", 24)]
        if not compact:
            layout.append(("schedule", 14))
        layout += [("last", 10), ("next", 8), ("dur", 8)]
        if not compact:
            layout.append(("spark", 11))
        if monitored:
            layout.append(("res", 15))
        if spread:
            layout.append(("owner", 12))
        layout.append(("cmd", 20))
        drop_order = ["spark", "res", "schedule", "dur", "cmd", "owner"]
        total = sum(w for _, w in layout) + len(layout)
        for name in drop_order:
            if total <= cols:
                break
            for idx, (col, width) in enumerate(layout):
                if col == name:
                    layout.pop(idx)
                    total -= width + 1
                    break
        # the name and command columns flex into whatever is left
        slack = cols - (sum(w for _, w in layout) + len(layout))
        if slack > 0:
            names = [
                i for i, (c, _) in enumerate(layout) if c in ("cmd", "name")
            ]
            for idx in names:
                col, width = layout[idx]
                extra = slack // len(names)
                layout[idx] = (col, width + extra)
        return layout

    def render_table(
        self, paint: Painter, cols: int, body_rows: int
    ) -> List[str]:
        layout = self._columns(cols)
        titles = {
            "status": "status",
            "name": "name",
            "schedule": "schedule",
            "last": "last",
            "next": "next",
            "dur": "dur",
            "spark": "runs",
            "res": "cpu/mem",
            "owner": "owner",
            "cmd": "command",
        }
        header = " ".join(pad_to(titles[c], w) for c, w in layout)
        rows = [paint.style(pad_to(" " + header, cols), "dim", bold=True)]
        view = self.view
        self.table_offset = scroll_window(
            len(view), body_rows, self.sel, self.table_offset
        )
        visible = view[self.table_offset : self.table_offset + body_rows]
        for offset, job in enumerate(visible):
            idx = self.table_offset + offset
            rows.append(
                self._job_row(paint, job, layout, cols, idx == self.sel)
            )
        if not view:
            hint = (
                "no jobs match the filter"
                if self.jobs
                else (
                    "waiting for the daemon…"
                    if not self.connected
                    else "no jobs configured"
                )
            )
            rows.append(paint.style("   " + hint, "dim"))
        return rows

    def _job_row(
        self,
        paint: Painter,
        job: Dict[str, Any],
        layout: List[Tuple[str, int]],
        cols: int,
        selected: bool,
    ) -> str:
        key, label = health(job)
        ascii_mode = bool(self.prefs["ascii"])
        color = {
            "ok": "ok",
            "fail": "fail",
            "run": "run",
            "pending": "pending",
            "disabled": "off",
            "cancelled": "off",
            "unknown": "pending",
        }[key]
        last = job.get("last_run") or {}
        cells: List[str] = []
        bg = "sel" if selected else None
        for col, width in layout:
            if col == "status":
                text = "%s %s" % (paint.glyph(key, ascii_mode), label)
                cells.append(
                    paint.style(
                        pad_to(text, width),
                        color,
                        bg=bg,
                        bold=key in ("fail", "run"),
                    )
                )
            elif col == "name":
                flash = job.get("name") in self.just_failed
                cells.append(
                    paint.style(
                        pad_to(str(job.get("name", "")), width),
                        "bright" if selected or flash else "fg",
                        bg=bg,
                        bold=flash,
                    )
                )
            elif col == "schedule":
                cells.append(
                    paint.style(
                        pad_to(oneline(job.get("schedule", "")), width),
                        "dim",
                        bg=bg,
                    )
                )
            elif col == "last":
                retry = job.get("retry")
                if retry:
                    text = "↻ try %s" % retry.get("attempt")
                    cells.append(
                        paint.style(pad_to(text, width), "warn", bg=bg)
                    )
                else:
                    cells.append(
                        paint.style(
                            pad_to(
                                fmt_ago(last.get("finished_at"))
                                if last
                                else "—",
                                width,
                            ),
                            "fg",
                            bg=bg,
                        )
                    )
            elif col == "next":
                if job.get("running"):
                    text = "· · ·"
                elif not job.get("enabled"):
                    text = "—"
                else:
                    text = fmt_in(self.next_run_seconds(job))
                cells.append(paint.style(pad_to(text, width), "fg", bg=bg))
            elif col == "dur":
                cells.append(
                    paint.style(
                        pad_to(
                            fmt_duration(last.get("duration"))
                            if last
                            else "—",
                            width,
                        ),
                        "dim",
                        bg=bg,
                    )
                )
            elif col == "spark":
                spark = "".join(
                    paint.style(ch, ck, bg=bg)
                    for ch, ck in spark_cells(
                        job.get("history") or [], width - 1
                    )
                )
                pad_w = width - min(width - 1, len(job.get("history") or []))
                cells.append(
                    spark + paint.style(" " * max(0, pad_w), "fg", bg=bg)
                )
            elif col == "res":
                res = job.get("running_resources")
                if res:
                    text = "%s %s" % (
                        fmt_percent(res.get("cpu_percent")),
                        fmt_bytes(res.get("rss_bytes")),
                    )
                    cells.append(
                        paint.style(pad_to(text, width), "run", bg=bg)
                    )
                else:
                    cells.append(paint.style(pad_to("", width), "dim", bg=bg))
            elif col == "owner":
                owner = job.get("clusterOwner")
                text = (
                    str(owner)
                    if owner
                    else ("∅" if "clusterOwner" in job else "")
                )
                cells.append(paint.style(pad_to(text, width), "dim", bg=bg))
            elif col == "cmd":
                cells.append(
                    paint.style(
                        pad_to(oneline(job.get("command", "")), width),
                        "dim",
                        bg=bg,
                    )
                )
        marker = paint.style(
            "▎" if selected else " ", "accent", bg=bg, bold=True
        )
        row = marker + (paint.style(" ", "fg", bg=bg).join(cells))
        return cut_to_width(paint.row(row), cols)

    def render_footer(self, paint: Painter, cols: int) -> str:
        hints = (
            "j/k move · enter open · r run · x cancel · c copy · "
            "/ filter · g refresh · t theme · i incident · w wallboard · "
            "ctrl+k palette · ? help · q quit"
        )
        return cut_to_width(
            paint.style(" " + pad_to(hints, cols - 1), "dim"), cols
        )

    # ---- toasts ------------------------------------------------------
    def _compose_toasts(
        self, paint: Painter, rows: List[str], cols: int, lines: int
    ) -> List[str]:
        if not self.toasts:
            return rows
        colors = {"ok": "ok", "fail": "fail", "warn": "warn", "info": "accent"}
        for offset, (kind, message, _) in enumerate(self.toasts[-4:]):
            row_idx = lines - 2 - offset
            if row_idx < 0:
                break
            body = " %s " % truncate(message, max(10, cols // 2))
            toast = paint.style(
                body, "bright", bg=colors.get(kind, "accent"), bold=True
            )
            width = text_width(body)
            base = cut_to_width(rows[row_idx], cols - width - 1)
            rows[row_idx] = base + toast
        return rows

    # ---- wallboard + zen --------------------------------------------
    def render_wallboard(
        self, paint: Painter, cols: int, lines: int
    ) -> List[str]:
        ascii_mode = bool(self.prefs["ascii"])
        jobs = sorted(
            self.jobs,
            key=lambda j: (WB_ORDER.get(health(j)[0], 9), j.get("name", "")),
        )
        stale = self.stale()
        rows: List[str] = []
        if self.verdict is not None:
            rows.append(self.render_verdict_bar(paint, cols))
        if stale:
            banner = "  ◌ NO SIGNAL — data is stale  "
            rows.append(
                paint.style(
                    pad_to(banner.center(cols), cols),
                    "bright",
                    bg="fail",
                    bold=True,
                )
            )
        tile_w = max(20, min(30, cols // max(1, min(len(jobs), 4))))
        per_row = max(1, cols // (tile_w + 1))
        tile_rows = 3
        grid_top = len(rows) + 1
        avail = lines - grid_top - 1
        max_tiles = max(per_row, per_row * max(1, avail // (tile_rows + 1)))
        shown = jobs[:max_tiles]
        for chunk_start in range(0, len(shown), per_row):
            chunk = shown[chunk_start : chunk_start + per_row]
            lines3: List[List[str]] = [[], [], []]
            for job in chunk:
                key, _label = health(job)
                color = {
                    "ok": "ok",
                    "fail": "fail",
                    "run": "run",
                    "pending": "pending",
                    "disabled": "off",
                    "cancelled": "off",
                    "unknown": "pending",
                }[key]
                last = job.get("last_run") or {}
                name = truncate(str(job.get("name", "")), tile_w - 4)
                head = "%s %s" % (paint.glyph(key, ascii_mode), name)
                if key == "run":
                    line2 = "▶ running"
                elif key in ("fail", "cancelled", "unknown"):
                    line2 = fmt_ago(last.get("finished_at"))
                    if key == "fail" and last.get("exit_code") is not None:
                        line2 += " · exit %s" % last["exit_code"]
                elif not job.get("enabled"):
                    line2 = "—"
                else:
                    line2 = (
                        "stale"
                        if stale
                        else fmt_in(self.next_run_seconds(job))
                    )
                dim_all = stale
                lines3[0].append(
                    paint.style(
                        pad_to(" " + head, tile_w),
                        "bright" if not dim_all else "dim",
                        bg=color if key == "fail" and not dim_all else None,
                        bold=key == "fail",
                    )
                )
                lines3[1].append(
                    paint.style(
                        pad_to("  " + line2, tile_w),
                        color if not dim_all else "dim",
                    )
                )
                spark_row = "  " + "".join(
                    paint.style(ch, "dim" if dim_all else ck)
                    for ch, ck in spark_cells(
                        job.get("history") or [], tile_w - 4
                    )
                )
                pad_cells = (
                    tile_w - 2 - min(tile_w - 4, len(job.get("history") or []))
                )
                lines3[2].append(spark_row + " " * max(0, pad_cells))
            for triple in lines3:
                rows.append(cut_to_width(paint.row(" ".join(triple)), cols))
            rows.append(paint.row())
        while len(rows) < lines - 1:
            rows.append(paint.row())
        rows = rows[: lines - 1]
        counts: Dict[str, int] = {}
        for job in self.jobs:
            counts[health(job)[0]] = counts.get(health(job)[0], 0) + 1
        foot = " %d jobs · %d fail · %d run · %d ok" % (
            len(self.jobs),
            counts.get("fail", 0),
            counts.get("run", 0),
            counts.get("ok", 0),
        )
        if self.any_failing and not self.alarm_ack:
            foot += " · ◔ ALARM (a to ack)"
        right = "esc/w exit · %s " % utc_clock()
        gap = cols - text_width(foot) - text_width(right)
        rows.append(
            cut_to_width(
                paint.row(
                    paint.style(foot, "fg"),
                    paint.style(" " * max(1, gap)),
                    paint.style(right, "dim"),
                ),
                cols,
            )
        )
        return rows

    def render_zen(self, paint: Painter, cols: int, lines: int) -> List[str]:
        """The calm all-clear field: one breathing dot per job, pulsing
        on its real next fire, a terminal read of the web screensaver."""
        rows = [paint.row() for _ in range(lines)]
        now = time.monotonic()
        for job in self.jobs:
            name = str(job.get("name", ""))
            digest = hashlib.md5(name.encode("utf-8")).digest()
            row_idx = 1 + digest[0] % max(1, lines - 3)
            col_idx = 1 + digest[1] % max(1, cols - 3)
            nxt = self.next_run_seconds(job)
            phase = (now % 4) / 4
            if nxt is not None and nxt < 30:
                dot, color = "●", "ok"
            elif phase < 0.5:
                dot, color = "∙", "dim"
            else:
                dot, color = "·", "dim"
            row = rows[row_idx]
            pad_needed = col_idx - text_width(row)
            if pad_needed >= 0:
                rows[row_idx] = (
                    row + " " * pad_needed + paint.style(dot, color)
                )
        label = "all clear · %s" % utc_clock()
        rows[lines - 2] = paint.style(pad_to(label.center(cols), cols), "dim")
        return rows


# ===================================================================
#  the application: rendering, part 2 (overlays + drawers)
# ===================================================================
#: The shared shortcut table (web help overlay), plus terminal extras.
HELP_ROWS = [
    ("⌘K / Ctrl+K", "Command palette"),
    ("/", "Focus filter"),
    ("j / ↓", "Next job"),
    ("k / ↑", "Previous job"),
    ("Enter", "Open selected job"),
    ("r", "Run selected job"),
    ("x", "Cancel selected (running) job"),
    ("c", "Copy selected command"),
    ("g", "Refresh now"),
    ("t", "Cycle theme"),
    ("T", "Light / dark theme"),
    ("i", "Incident timeline"),
    ("w", "Wallboard (TV) mode"),
    ("a", "Acknowledge failure alarm"),
    ("?", "This help"),
    ("Esc", "Close panel / drawer"),
]
HELP_EXTRA_ROWS = [
    ("q", "Quit"),
    ("s / S", "Cycle sort key / direction"),
    ("f", "Cycle status filter"),
    ("m", "Multi-tail console"),
    ("←/→ or Tab", "Switch drawer tab"),
    ("PgUp / PgDn", "Scroll"),
    ("in logs: f/t/w", "Follow · timestamps · wrap"),
    ("in logs: / n N", "Search · next · previous"),
    ("in logs: d", "Save log to a file"),
]


class AppOverlays(AppRender):
    def render_overlay(
        self, paint: Painter, top: str, cols: int, lines: int
    ) -> List[str]:
        renderer = getattr(self, "render_" + top, None)
        if renderer is None:
            return []
        return cast(List[str], renderer(paint, cols, lines))

    # ---- help --------------------------------------------------------
    def render_help(self, paint: Painter, cols: int, lines: int) -> List[str]:
        width = min(64, cols - 4)
        body = [paint.style("the web dashboard's keys", "dim")]
        for keycap, action in HELP_ROWS:
            body.append(
                paint.style(pad_to(keycap, 16), "accent", bold=True)
                + paint.style(action, "fg")
            )
        body.append(paint.style("", "fg"))
        body.append(paint.style("terminal extras", "dim"))
        for keycap, action in HELP_EXTRA_ROWS:
            body.append(
                paint.style(pad_to(keycap, 16), "pending", bold=True)
                + paint.style(action, "fg")
            )
        visible = max(4, lines - 6)
        self.panel_scroll = max(
            0, min(self.panel_scroll, max(0, len(body) - visible))
        )
        body = body[self.panel_scroll : self.panel_scroll + visible]
        return panel_frame(
            paint, "keyboard shortcuts", body, width, "esc close · j/k scroll"
        )

    # ---- palette -----------------------------------------------------
    def render_palette(
        self, paint: Painter, cols: int, lines: int
    ) -> List[str]:
        width = min(72, cols - 4)
        matches = self.palette_matches()
        self.palette_sel = min(self.palette_sel, max(0, len(matches) - 1))
        query = self.inputs["palette"]
        body = [
            paint.style("⌕ ", "accent", bold=True)
            + paint.style(query + "▌", "bright")
        ]
        visible = max(4, min(lines - 8, 14))
        start = scroll_window(len(matches), visible, self.palette_sel, 0)
        for idx, (icon, label, _action) in enumerate(
            matches[start : start + visible]
        ):
            selected = start + idx == self.palette_sel
            body.append(
                paint.style(
                    " %s %s" % (icon, pad_to(label, width - 8)),
                    "bright" if selected else "fg",
                    bg="sel" if selected else None,
                    bold=selected,
                )
            )
        if not matches:
            body.append(paint.style("  no matching command", "dim"))
        return panel_frame(
            paint,
            "command palette",
            body,
            width,
            "↑/↓ move · enter run · esc close",
        )

    # ---- settings ----------------------------------------------------
    def settings_rows(
        self,
    ) -> List[Tuple[str, str, Callable[[], None]]]:
        prefs = self.prefs

        def poll_label() -> str:
            ms = int(prefs["poll_ms"])
            return "paused" if not ms else "%gs" % (ms / 1000)

        def zen_idle_cycle() -> None:
            choices = [30, 60, 90, 120, 300]
            try:
                idx = choices.index(int(prefs["zen_idle_s"]))
            except ValueError:
                idx = -1
            prefs["zen_idle_s"] = choices[(idx + 1) % len(choices)]
            self.save_prefs()

        def flip(key: str) -> Callable[[], None]:
            def action() -> None:
                prefs[key] = not bool(prefs[key])
                self.save_prefs()
                if key in ("wrap", "timestamps"):
                    setattr(self, key, bool(prefs[key]))
                self.mark()

            return action

        onoff = lambda k: "on" if prefs[k] else "off"  # noqa: E731
        return [
            ("Theme", "%s" % self.theme.name, self.cycle_theme),
            (
                "Light / dark",
                "paper" if prefs["light"] else "phosphor",
                self.toggle_light_dark,
            ),
            ("Color vision", str(prefs["cvd"]), self.cycle_cvd),
            ("Refresh interval", poll_label(), self.cycle_poll),
            ("Wrap log lines", onoff("wrap"), flip("wrap")),
            ("Log timestamps", onoff("timestamps"), flip("timestamps")),
            ("Compact density", onoff("compact"), flip("compact")),
            ("Audible cues (bell)", onoff("sound"), flip("sound")),
            ("Zen screensaver", onoff("zen"), flip("zen")),
            ("Zen idle", "%ds" % int(prefs["zen_idle_s"]), zen_idle_cycle),
            ("Boot self-test", onoff("boot"), flip("boot")),
            ("ASCII glyphs", onoff("ascii"), flip("ascii")),
        ]

    def render_settings(
        self, paint: Painter, cols: int, lines: int
    ) -> List[str]:
        width = min(56, cols - 4)
        rows = self.settings_rows()
        self.settings_sel = min(self.settings_sel, len(rows) - 1)
        body = []
        for idx, (label, value, _action) in enumerate(rows):
            selected = idx == self.settings_sel
            body.append(
                paint.style(
                    pad_to(" " + label, width - 18),
                    "bright" if selected else "fg",
                    bg="sel" if selected else None,
                    bold=selected,
                )
                + paint.style(
                    pad_to(value, 12),
                    "accent",
                    bg="sel" if selected else None,
                )
            )
        body.append(paint.style("", "fg"))
        body.append(
            paint.style(
                " prefs file: %s" % (self.prefs_file or prefs_path()), "dim"
            )
        )
        return panel_frame(
            paint,
            "settings",
            body,
            width,
            "j/k move · enter change · esc close",
        )

    # ---- token modal -------------------------------------------------
    def render_token(self, paint: Painter, cols: int, lines: int) -> List[str]:
        width = min(56, cols - 4)
        masked = "•" * len(self.inputs["token"])
        body = [
            paint.style("the daemon wants a bearer token", "fg"),
            paint.style(
                "(web.authToken; stored only for this session)", "dim"
            ),
            paint.style("", "fg"),
            paint.style("⚿ ", "accent") + paint.style(masked + "▌", "bright"),
        ]
        return panel_frame(
            paint, "access token", body, width, "enter save · esc cancel"
        )

    # ---- incident timeline ------------------------------------------
    def timeline_entries(
        self,
    ) -> List[Tuple[str, Optional[str], str, Any, str, Any]]:
        """(name, finished_at, outcome, exit, reason, duration), newest
        first: every job's most recent finish, like the web overlay."""
        out = []
        for job in self.jobs:
            last = job.get("last_run")
            if not last:
                continue
            outcome = str(last.get("outcome", ""))
            if self.timeline_fail_only and outcome != "failure":
                continue
            out.append(
                (
                    str(job.get("name", "")),
                    last.get("finished_at"),
                    outcome,
                    last.get("exit_code"),
                    str(last.get("fail_reason") or ""),
                    last.get("duration"),
                )
            )
        out.sort(key=lambda e: parse_iso(e[1]) or 0.0, reverse=True)
        return out

    def render_timeline(
        self, paint: Painter, cols: int, lines: int
    ) -> List[str]:
        width = min(90, cols - 4)
        entries = self.timeline_entries()
        ascii_mode = bool(self.prefs["ascii"])
        body = []
        blast = set(self.incident_set)
        visible = max(4, lines - 8)
        self.timeline_sel = min(self.timeline_sel, max(0, len(entries) - 1))
        start = scroll_window(len(entries), visible, self.timeline_sel, 0)
        for idx, entry in enumerate(entries[start : start + visible]):
            name, fin, outcome, exit_code, reason, duration = entry
            selected = start + idx == self.timeline_sel
            key = {
                "failure": "fail",
                "cancelled": "cancelled",
                "unknown": "unknown",
            }.get(outcome, "ok")
            color = {
                "fail": "fail",
                "cancelled": "off",
                "unknown": "pending",
                "ok": "ok",
            }[key]
            line = "%s %s %s" % (
                pad_to(fmt_ago(fin), 9),
                paint.glyph(key, ascii_mode),
                pad_to(name, 24),
            )
            detail = ""
            if outcome == "failure":
                detail = "exit %s" % ("?" if exit_code is None else exit_code)
                if reason:
                    detail += " · %s" % reason
            if duration is not None:
                detail += (" · " if detail else "") + fmt_duration(duration)
            if name in blast:
                detail += "  ◉ blast radius"
            body.append(
                paint.style(
                    line, color, bg="sel" if selected else None, bold=selected
                )
                + paint.style(
                    truncate(detail, max(0, width - 42)),
                    "dim",
                    bg="sel" if selected else None,
                )
            )
        if not entries:
            body.append(
                paint.style(
                    "  nothing has finished yet"
                    if not self.timeline_fail_only
                    else "  nothing failing — clear the filter (f)",
                    "dim",
                )
            )
        title = "incident timeline%s" % (
            " — failing only" if self.timeline_fail_only else ""
        )
        return panel_frame(
            paint,
            title,
            body,
            width,
            "j/k move · enter open · f fail-only · m mitigate · esc close",
        )

    # ---- mitigate console -------------------------------------------
    def render_mitigate(
        self, paint: Painter, cols: int, lines: int
    ) -> List[str]:
        width = min(76, cols - 4)
        body = [
            paint.style(
                " %d job%s in the set — %s"
                % (
                    len(self.mitigate_names),
                    "s" if len(self.mitigate_names) != 1 else "",
                    self.mitigate_label,
                ),
                "bright",
                bold=True,
            ),
        ]
        preview = ", ".join(self.mitigate_names[:6])
        if len(self.mitigate_names) > 6:
            preview += ", +%d more" % (len(self.mitigate_names) - 6)
        body.append(paint.style(" " + preview, "dim"))
        body.append(paint.style("", "fg"))
        log_rows = max(3, min(lines - 12, 12))
        for line in self.mitigate_log[-log_rows:]:
            color = (
                "ok"
                if "✓" in line
                else "fail"
                if "✕" in line or "!" in line
                else "fg"
            )
            body.append(paint.style(" " + line, color))
        if self.mitigate_running:
            body.append(paint.style(" … running (a to abort)", "warn"))
        return panel_frame(
            paint,
            "mitigate console",
            body,
            width,
            "s start all · x cancel all · a abort · y writeup · esc close",
        )

    # ---- cron sandbox ------------------------------------------------
    def render_sandbox(
        self, paint: Painter, cols: int, lines: int
    ) -> List[str]:
        width = min(64, cols - 4)
        expr = self.inputs["sandbox"]
        body = [
            paint.style("◴ ", "accent") + paint.style(expr + "▌", "bright"),
            paint.style("", "fg"),
        ]
        if expr.strip():
            text = describe_cron(expr.strip())
            valid = not text.startswith("Custom schedule:")
            hashed = False
            try:
                CronTab(expr.strip())
                parses = True
            except (ValueError, KeyError):
                parses = expr.strip().lower() == "@reboot"
                if not parses:
                    # a valid H schedule parses for any NAMED job; the
                    # empty hash key is the engine's own "validate the
                    # fields only" convention (crontab sniffing uses it),
                    # so this tells real H schedules from broken ones
                    try:
                        CronTab(expr.strip(), hash_key="")
                        hashed = True
                    except (ValueError, KeyError):
                        pass
            body.append(
                paint.style(
                    " " + text,
                    "ok" if parses else ("dim" if hashed else "fail"),
                )
            )
            if parses and valid:
                fires = next_fires(expr.strip(), 6)
                if fires:
                    body.append(paint.style("", "fg"))
                    body.append(paint.style(" next fires (UTC):", "dim"))
                    for when in fires:
                        body.append(
                            paint.style(
                                "   %s" % when.strftime("%Y-%m-%d %H:%M:%S"),
                                "fg",
                            )
                        )
            elif hashed:
                body.append(paint.style("", "fg"))
                body.append(
                    paint.style(
                        " an H slot is a stable hash of the job name,", "dim"
                    )
                )
                body.append(
                    paint.style(
                        " so it resolves per job, not in this sandbox", "dim"
                    )
                )
            elif not parses:
                body.append(
                    paint.style(
                        "  the daemon's engine rejects this expression", "dim"
                    )
                )
            if parses:
                # advisory lint (never-fires, AND day semantics, uneven
                # steps, skipped months), same rules the daemon logs at
                # config load; the UTC frame matches the preview above.
                findings = lint_schedule(
                    expr.strip(), timezone=datetime.timezone.utc
                )
                if findings:
                    body.append(paint.style("", "fg"))
                body.extend(finding_rows(findings, paint, width, 4))
        else:
            body.append(
                paint.style(
                    " type a cron expression — e.g. */5 9-17 * * mon-fri",
                    "dim",
                )
            )
        return panel_frame(paint, "cron sandbox", body, width, "esc close")

    # ---- DAGs index --------------------------------------------------
    def render_dags(self, paint: Painter, cols: int, lines: int) -> List[str]:
        width = min(76, cols - 4)
        body = []
        self.dags_sel = min(self.dags_sel, max(0, len(self.dags) - 1))
        for idx, dag in enumerate(self.dags):
            selected = idx == self.dags_sel
            name = str(dag.get("name", ""))
            tasks = dag.get("tasks")
            count = (
                len(tasks)
                if isinstance(tasks, (list, dict))
                else dag.get("taskCount", "?")
            )
            sched = str(dag.get("schedule", "") or "manual")
            latest = dag.get("latestRun") or {}
            lstate = str(latest.get("state", ""))
            body.append(
                paint.style(
                    pad_to(" ⧉ %s" % name, 28),
                    "bright" if selected else "fg",
                    bg="sel" if selected else None,
                    bold=selected,
                )
                + paint.style(
                    pad_to("%s tasks" % count, 12),
                    "dim",
                    bg="sel" if selected else None,
                )
                + paint.style(
                    pad_to(lstate, 11),
                    self._dag_state_color(lstate) if lstate else "dim",
                    bg="sel" if selected else None,
                )
                + paint.style(
                    truncate(sched, max(0, width - 56)),
                    "dim",
                    bg="sel" if selected else None,
                )
            )
        if not self.dags:
            body.append(paint.style("  no DAGs configured", "dim"))
        return panel_frame(
            paint,
            "orchestration DAGs",
            body,
            width,
            "enter open · t trigger · r reload · esc close",
        )

    # ---- durable-state inspector ------------------------------------
    def render_state(self, paint: Painter, cols: int, lines: int) -> List[str]:
        width = min(80, cols - 4)
        data = self.state_data or {}
        body = []
        tabs = ["view", "documents", "records"]
        tab_spans = []
        for tab in tabs:
            active = tab == self.state_tab
            tab_spans.append(
                paint.style(
                    " %s " % tab,
                    "bright" if active else "dim",
                    bg="sel" if active else None,
                    bold=active,
                )
            )
        body.append("".join(tab_spans))
        body.append(paint.style("", "fg"))
        if not data.get("enabled"):
            body.append(
                paint.style(
                    " durable state is not configured (no state: block)", "dim"
                )
            )
            return panel_frame(
                paint, "state inspector", body, width, "esc close"
            )
        if self.state_tab == "view":
            body.extend(self._render_state_view(paint, data, width))
        else:
            body.extend(self._render_state_listing(paint, width, lines))
        return panel_frame(
            paint,
            "state inspector",
            body,
            width,
            "←/→ tabs · j/k move · enter inspect · r refresh · esc close",
        )

    def _render_state_view(
        self, paint: Painter, data: Dict[str, Any], width: int
    ) -> List[str]:
        body = []
        for key in ("view", "stats"):
            section = data.get(key)
            if isinstance(section, dict):
                for name, value in list(section.items())[:12]:
                    body.append(
                        paint.style(pad_to(" %s" % name, 30), "dim")
                        + paint.style(
                            truncate(
                                json.dumps(value, default=str), width - 36
                            ),
                            "fg",
                        )
                    )
        node = data.get("node")
        if isinstance(node, dict):
            retries = node.get("retries") or []
            slots = node.get("slots") or []
            body.append(
                paint.style(
                    " armed retries: %d · held slots: %d"
                    % (len(retries), len(slots)),
                    "fg",
                )
            )
        docs = data.get("documents")
        if isinstance(docs, dict):
            body.append(
                paint.style(
                    " document namespaces: %d (documents tab)" % len(docs),
                    "dim",
                )
            )
        records = data.get("records")
        if isinstance(records, dict):
            body.append(
                paint.style(
                    " record streams: %d (records tab)" % len(records), "dim"
                )
            )
        return body

    def _render_state_listing(
        self, paint: Painter, width: int, lines: int
    ) -> List[str]:
        body = []
        names = (
            self._state_namespaces()
            if self.state_tab == "documents"
            else self._state_streams()
        )
        self.state_sel = min(self.state_sel, max(0, len(names) - 1))
        visible = max(3, min(8, lines - 16))
        start = scroll_window(len(names), visible, self.state_sel, 0)
        for idx, name in enumerate(names[start : start + visible]):
            selected = start + idx == self.state_sel
            body.append(
                paint.style(
                    pad_to(" %s" % name, width - 6),
                    "bright" if selected else "fg",
                    bg="sel" if selected else None,
                    bold=selected,
                )
            )
        if not names:
            body.append(paint.style("  nothing here yet", "dim"))
        detail = self.state_detail or {}
        items = detail.get("documents") or detail.get("records") or []
        if items:
            body.append(paint.style("", "fg"))
            for item in items[: max(3, lines - 18)]:
                body.append(
                    paint.style(
                        " "
                        + truncate(json.dumps(item, default=str), width - 8),
                        "dim",
                    )
                )
        return body

    # ---- cluster panel ----------------------------------------------
    def render_cluster(
        self, paint: Painter, cols: int, lines: int
    ) -> List[str]:
        width = min(84, cols - 4)
        data = self.cluster or {}
        body = []
        if not data.get("enabled"):
            body.append(
                paint.style(" no cluster configured (single node)", "dim")
            )
            return panel_frame(paint, "cluster", body, width, "esc close")
        node_stats = data.get("node_stats") or {}
        role = (
            "leader"
            if data.get("is_leader")
            else "follower (leader: %s)" % data["leader"]
            if data.get("quorate") and data.get("leader")
            else "follower"
            if data.get("quorate")
            else "no quorum"
        )
        line = " %s · %s · %s" % (
            data.get("node_name", "?"),
            data.get("backend", "gossip"),
            role,
        )
        if node_stats:
            line += " · this node %s cpu / %s mem" % (
                fmt_percent(node_stats.get("cpu_percent")),
                fmt_percent(node_stats.get("mem_percent")),
            )
        body.append(paint.style(line, "bright", bold=True))
        alert = cluster_alert(data)
        if alert and alert.get("bad"):
            body.append(
                paint.style(" ☢ %s" % alert["reason"], "fail", bold=True)
            )
        body.append(paint.style("", "fg"))
        peers = data.get("peers") or []
        if peers:
            for peer in peers[: max(3, lines - 14)]:
                if not isinstance(peer, dict):
                    continue
                name = str(
                    peer.get("node_name")
                    or peer.get("name")
                    or peer.get("host")
                    or "?"
                )
                status = str(peer.get("status", "?"))
                agreed = peer.get("agree")
                if agreed is None:
                    agreed = peer.get("agreed")
                mark = "✓" if agreed else "✕" if agreed is not None else "·"
                color = (
                    "ok" if agreed else "fail" if agreed is not None else "dim"
                )
                extra = ""
                stats = peer.get("node_stats") or {}
                if stats:
                    extra = " · %s cpu %s mem" % (
                        fmt_percent(stats.get("cpu_percent")),
                        fmt_percent(stats.get("mem_percent")),
                    )
                owns = peer.get("owns")
                if owns is not None:
                    extra += " · owns %s" % owns
                body.append(
                    paint.style(
                        " %s %s %s%s"
                        % (mark, pad_to(name, 22), pad_to(status, 12), extra),
                        color,
                    )
                )
        lease = data.get("lease")
        if isinstance(lease, dict) and lease:
            body.append(paint.style("", "fg"))
            labels = [
                ("holder", "held by"),
                ("expiry", "expires"),
                ("fence", "fence"),
                ("electionName", "election"),
                ("identity", "our identity"),
                ("path", "store path"),
            ]
            for key, label in labels:
                if lease.get(key) is None:
                    continue
                value = str(lease[key])
                if key == "expiry":
                    secs = (parse_iso(value) or 0) - time.time()
                    value = "%s · %s" % (
                        fmt_in(secs) if secs > 0 else "expired",
                        value.replace("T", " ")[:19],
                    )
                ok_mark = (
                    key == "holder"
                    and lease.get("holder") is not None
                    and lease.get("holder") == lease.get("identity")
                )
                body.append(
                    paint.style(pad_to(" %s" % label, 16), "dim")
                    + paint.style(
                        truncate(value, width - 22), "ok" if ok_mark else "fg"
                    )
                )
        return panel_frame(
            paint, "cluster", body, width, "j/k scroll · esc close"
        )

    # ---- fleet matrix ------------------------------------------------
    def render_fleet(self, paint: Painter, cols: int, lines: int) -> List[str]:
        # no fixed cap: a 9-node matrix needs the room, and the per-node
        # cell width below already shrinks to fit what the terminal gives
        width = cols - 4
        data = self.fleet or {}
        body = []
        if not data.get("enabled"):
            body.append(
                paint.style(
                    " fleet view needs a cluster with a node-to-node channel",
                    "dim",
                )
            )
            return panel_frame(paint, "fleet", body, width, "esc close")
        nodes = [n for n in data.get("nodes", []) if isinstance(n, dict)]
        names: Set[str] = set()
        for node in nodes:
            names.update((node.get("jobs") or {}).keys())
        rows = sorted(names)

        def failing(job_name: str) -> bool:
            for node in nodes:
                cell = (node.get("jobs") or {}).get(job_name)
                if (
                    cell
                    and not cell.get("running")
                    and (cell.get("last") or {}).get("outcome") == "failure"
                ):
                    return True
            return False

        fail_count = sum(1 for r in rows if failing(r))
        if self.fleet_fail_only:
            rows = [r for r in rows if failing(r)]
        running_cells = sum(
            1
            for node in nodes
            for cell in (node.get("jobs") or {}).values()
            if cell.get("running")
        )
        summary = " %d nodes · %d jobs · %d running" % (
            len(nodes),
            len(names),
            running_cells,
        )
        if fail_count:
            summary += " · %d failing" % fail_count
        if self.fleet_fail_only:
            summary += " · FAILING ONLY (f)"
        body.append(paint.style(summary, "bright", bold=True))
        cell_w = max(10, min(16, (width - 26) // max(1, len(nodes))))
        header = pad_to(" job", 24)
        for node in nodes:
            label = str(node.get("node_name") or node.get("host") or "?")
            if node.get("self"):
                label += "*"
            header += pad_to(truncate(label, cell_w - 1), cell_w)
        body.append(paint.style(header, "dim", bold=True))
        ascii_mode = bool(self.prefs["ascii"])
        visible = max(3, lines - 12)
        self.panel_scroll = max(
            0, min(self.panel_scroll, max(0, len(rows) - visible))
        )
        for job_name in rows[self.panel_scroll : self.panel_scroll + visible]:
            spans = [
                paint.style(pad_to(" " + truncate(job_name, 22), 24), "fg")
            ]
            for node in nodes:
                cell = (node.get("jobs") or {}).get(job_name)
                if node.get("jobs") is None:
                    text, color = "·", "dim"
                elif cell is None:
                    text, color = "—", "dim"
                elif cell.get("running"):
                    text, color = (
                        paint.glyph("run", ascii_mode) + " run",
                        "run",
                    )
                elif cell.get("last"):
                    outcome = str((cell["last"] or {}).get("outcome", ""))
                    key = {
                        "failure": "fail",
                        "cancelled": "cancelled",
                        "unknown": "unknown",
                    }.get(outcome, "ok")
                    label = {
                        "success": "ok",
                        "failure": "fail",
                        "cancelled": "cancel",
                        "unknown": "lost",
                    }.get(outcome, outcome[:6])
                    text = "%s %s %s" % (
                        paint.glyph(key, ascii_mode),
                        label,
                        ago_short((cell["last"] or {}).get("finished_at")),
                    )
                    color = {
                        "fail": "fail",
                        "cancelled": "off",
                        "unknown": "pending",
                        "ok": "ok",
                    }[key]
                elif cell.get("enabled") is False:
                    text, color = (
                        paint.glyph("disabled", ascii_mode) + " off",
                        "off",
                    )
                else:
                    text, color = paint.glyph("pending", ascii_mode), "dim"
                spans.append(
                    paint.style(
                        pad_to(truncate(text, cell_w - 1), cell_w), color
                    )
                )
            body.append(cut_to_width("".join(spans), width - 4))
        if not rows:
            body.append(
                paint.style(
                    "  nothing failing — clear the filter (f)"
                    if self.fleet_fail_only and names
                    else "  no jobs advertised yet",
                    "dim",
                )
            )
        return panel_frame(
            paint,
            "fleet — every node's runs",
            body,
            width,
            "f failing-only · j/k scroll · r refresh · esc close",
        )

    # ---- activity heatmap -------------------------------------------
    def render_heat(self, paint: Painter, cols: int, lines: int) -> List[str]:
        width = min(96, cols - 4)
        buckets = 24
        body = [
            paint.style(
                " last %d hours, one cell per hour — worst outcome, shaded "
                "by volume" % buckets,
                "dim",
            )
        ]
        now = time.time()
        shades = " ░▒▓█"
        names = sorted(self.heat_data.keys())
        visible = max(3, lines - 10)
        self.panel_scroll = max(
            0, min(self.panel_scroll, max(0, len(names) - visible))
        )
        for name in names[self.panel_scroll : self.panel_scroll + visible]:
            runs = self.heat_data.get(name, [])
            cells: List[Tuple[int, str]] = [(0, "ok") for _ in range(buckets)]
            for run in runs:
                t = parse_iso(run.get("finished_at"))
                if t is None:
                    continue
                age_h = (now - t) / 3600
                if age_h >= buckets:
                    continue
                idx = buckets - 1 - int(age_h)
                count, worst = cells[idx]
                outcome = str(run.get("outcome", ""))
                rank = {"ok": 0, "unknown": 1, "cancelled": 2, "fail": 3}
                key = {
                    "failure": "fail",
                    "cancelled": "cancelled",
                    "unknown": "unknown",
                }.get(outcome, "ok")
                if rank.get(key, 0) >= rank.get(worst, 0):
                    worst = key
                cells[idx] = (count + 1, worst)
            spans = [paint.style(pad_to(" " + truncate(name, 22), 24), "fg")]
            for count, worst in cells:
                shade = shades[min(len(shades) - 1, count)]
                color = {
                    "ok": "ok",
                    "fail": "fail",
                    "cancelled": "off",
                    "unknown": "pending",
                }[worst]
                spans.append(paint.style(shade, color))
            body.append("".join(spans))
        if not names:
            body.append(paint.style("  gathering run history…", "dim"))
        return panel_frame(
            paint,
            "activity heatmap",
            body,
            width,
            "j/k scroll · r refresh · esc close",
        )

    # ---- schedule pressure ------------------------------------------
    def render_press(self, paint: Painter, cols: int, lines: int) -> List[str]:
        """Forward-looking fleet collision view (web pressure card port):
        the next 24h of fires by minute of hour, the hour by minute grid,
        duplicate schedule groups, and the least-loaded slot."""
        width = min(96, cols - 4)
        data = self.pressure
        if not data:
            return panel_frame(
                paint,
                "schedule pressure",
                [paint.style("  computing the fire forecast…", "dim")],
                width,
                "r refresh · esc close",
            )
        body: List[str] = []
        busiest = data["busiest_minute"]
        body.append(
            paint.style(
                " next %dh: %d fires from %d schedules · busiest :%02d "
                "(%d jobs) · %d/60 minutes empty"
                % (
                    data["hours"],
                    data["total_fires"],
                    data["jobs"],
                    busiest["minute"],
                    busiest["jobs"],
                    len(data["empty_minutes"]),
                ),
                "dim",
            )
        )
        fires = data["by_minute_fires"]
        peak = max(fires) if any(fires) else 1
        spans = [paint.style(" by min ", "dim")]
        for value in fires:
            if not value:
                spans.append(paint.style(" ", "fg"))
                continue
            idx = max(
                1,
                min(
                    len(_SPARK_BARS) - 1,
                    int(value / peak * (len(_SPARK_BARS) - 1) + 0.5),
                ),
            )
            hot = value >= max(2, peak * 0.7)
            spans.append(
                paint.style(_SPARK_BARS[idx], "fail" if hot else "run")
            )
        body.append("".join(spans))
        axis = [" "] * 60
        for minute in range(0, 60, 10):
            for offset, char in enumerate(":%02d" % minute):
                axis[minute + offset] = char
        body.append(paint.style(" " * 8 + "".join(axis), "dim"))
        sug = self.press_suggest
        if sug:
            body.append(
                paint.style(" suggest ", "dim")
                + paint.style(sug["hourly"]["expression"], "accent", bold=True)
                + paint.style(" (hourly) · ", "dim")
                + paint.style(sug["daily"]["expression"], "accent", bold=True)
                + paint.style(" (daily) · or ", "dim")
                + paint.style("H * * * *", "accent", bold=True)
                + paint.style(" per-job hashed slots", "dim")
            )
        if self.press_dups:
            body.append(paint.style("", "fg"))
            body.append(
                paint.style(
                    " duplicate schedules (%d group%s, firing on identical "
                    "instants)"
                    % (
                        len(self.press_dups),
                        "" if len(self.press_dups) == 1 else "s",
                    ),
                    "dim",
                )
            )
            for group in self.press_dups[:4]:
                names = ", ".join(group["jobs"][:5])
                if len(group["jobs"]) > 5:
                    names += ", +%d more" % (len(group["jobs"]) - 5)
                body.append(
                    paint.style(
                        pad_to(" %s" % truncate(group["expression"], 18), 20),
                        "pending",
                        bold=True,
                    )
                    + paint.style("×%-3d " % group["count"], "bright")
                    + paint.style(truncate(names, width - 28), "fg")
                )
        body.append(paint.style("", "fg"))
        body.append(
            paint.style(
                " hour × minute fire grid (%s)" % data["timezone"], "dim"
            )
        )
        grid = data["grid"]
        grid_max = max((c for row in grid for c in row), default=0) or 1
        shades = " ░▒▓█"
        for hour in range(24):
            spans = [paint.style(" %02d " % hour, "dim")]
            for count in grid[hour]:
                if not count:
                    spans.append(paint.style("·", "off"))
                    continue
                shade = shades[max(1, min(4, 1 + int(3 * count / grid_max)))]
                hot = count >= max(5, grid_max * 0.7)
                spans.append(paint.style(shade, "fail" if hot else "run"))
            body.append("".join(spans))
        visible = max(6, lines - 6)
        self.panel_scroll = max(
            0, min(self.panel_scroll, max(0, len(body) - visible))
        )
        body = body[self.panel_scroll : self.panel_scroll + visible]
        return panel_frame(
            paint,
            "schedule pressure",
            body,
            width,
            "j/k scroll · r refresh · esc close",
        )

    # ---- week calendar ----------------------------------------------
    def render_week(self, paint: Painter, cols: int, lines: int) -> List[str]:
        """The web week calendar, terminal-shaped: a 7-day by 24-hour
        shaded fire grid, a chronological agenda of the calendar-worthy
        fires, and the background-hum summary of jobs too frequent to
        chart.  All labels UTC, the TUI's frame everywhere else."""
        width = min(96, cols - 4)
        data = self.week
        if not data:
            return panel_frame(
                paint,
                "week calendar (UTC)",
                [paint.style("  enumerating the week…", "dim")],
                width,
                "r refresh · esc close",
            )
        body: List[str] = []
        items = data["items"]
        frequent = data["frequent"]
        start = data["start"]
        body.append(
            paint.style(
                " next 7 days: %d fires from %d schedules"
                % (len(items), data["schedules"])
                + (
                    " · %d frequent jobs summarized below" % len(frequent)
                    if frequent
                    else ""
                ),
                "dim",
            )
        )
        # day x hour grid, shaded like the pressure grid
        grid = data["grid"]
        grid_max = max((c for row in grid for c in row), default=0) or 1
        shades = " ░▒▓█"
        axis = [paint.style(" " * 13, "dim")]
        for hour in range(0, 24, 3):
            axis.append(paint.style("%02d " % hour, "dim"))
        body.append("".join(axis))
        today = datetime.datetime.now(datetime.timezone.utc).date()
        for day in range(7):
            date = (start + datetime.timedelta(days=day)).date()
            label = "today" if date == today else date.strftime("%a")
            spans = [
                paint.style(
                    " %-5s %s " % (label, date.strftime("%m-%d")),
                    "accent" if date == today else "dim",
                )
            ]
            for hour in range(24):
                count = grid[day][hour]
                if not count:
                    spans.append(paint.style("·", "off"))
                    continue
                shade = shades[max(1, min(4, 1 + int(3 * count / grid_max)))]
                spans.append(paint.style(shade, "run"))
            body.append("".join(spans))
        if items:
            body.append("")
            body.append(paint.style(" upcoming fires (UTC)", "dim"))
            now = datetime.datetime.now(datetime.timezone.utc)
            for when, name in items:
                past = when < now
                body.append(
                    paint.style(
                        "  %s  " % when.strftime("%a %m-%d %H:%M"),
                        "dim" if past else "accent",
                    )
                    + paint.style(
                        truncate(name, width - 24),
                        "off" if past else "fg",
                    )
                )
        elif not frequent:
            body.append("")
            body.append(
                paint.style("  no scheduled fires in the next 7 days", "dim")
            )
        if frequent:
            body.append("")
            body.append(
                paint.style(" background hum (too frequent to chart):", "dim")
            )
            for name, count, capped in frequent:
                body.append(
                    paint.style("  %s" % truncate(name, width - 16), "fg")
                    + paint.style(
                        "  x%d%s" % (count, "+" if capped else ""), "dim"
                    )
                )
        visible = max(6, lines - 6)
        self.panel_scroll = max(
            0, min(self.panel_scroll, max(0, len(body) - visible))
        )
        body = body[self.panel_scroll : self.panel_scroll + visible]
        return panel_frame(
            paint,
            "week calendar (UTC)",
            body,
            width,
            "j/k scroll · r refresh · esc close",
        )

    # ---- next-fire radar --------------------------------------------
    def render_radar(self, paint: Painter, cols: int, lines: int) -> List[str]:
        width = min(56, cols - 4)
        items = []
        for job in self.jobs:
            if not job.get("enabled") or job.get("running"):
                continue
            nxt = self.next_run_seconds(job)
            if nxt is None or nxt < 0:
                continue
            items.append((nxt, str(job.get("name", ""))))
        items.sort()
        body = [paint.style(" %d upcoming" % len(items), "dim")]
        for nxt, name in items[:10]:
            body.append(
                paint.style(
                    pad_to(" " + fmt_countdown(nxt), 10), "accent", bold=True
                )
                + paint.style(truncate(name, width - 16), "fg")
            )
        if not items:
            body.append(paint.style("  no jobs scheduled to fire soon", "dim"))
        # a 10-minute track with a mark per fire, like the web timeline
        track = [" "] * (width - 8)
        for nxt, _name in items:
            if nxt <= 600:
                pos = int((nxt / 600) * (len(track) - 1))
                track[pos] = "◆"
        body.append(paint.style("", "fg"))
        body.append(paint.style(" now" + "".join(track)[3:], "dim"))
        return panel_frame(paint, "next-fire radar", body, width, "esc close")

    # ---- node resources ---------------------------------------------
    def render_node(self, paint: Painter, cols: int, lines: int) -> List[str]:
        width = min(64, cols - 4)
        node = self.node or {}
        body = [
            paint.style(
                " node: %s" % node.get("node_name", "?"), "bright", bold=True
            )
        ]
        resources = node.get("resources")
        if isinstance(resources, dict):
            for key, value in resources.items():
                if isinstance(value, (int, float)):
                    if "percent" in key:
                        text = fmt_percent(float(value))
                    elif "bytes" in key or key.startswith("rss"):
                        text = fmt_bytes(value)
                    else:
                        text = "%s" % value
                else:
                    text = str(value)
                body.append(
                    paint.style(pad_to(" %s" % key, 26), "dim")
                    + paint.style(text, "fg")
                )
        else:
            body.append(paint.style(" resource sampling unavailable", "dim"))
        history = self.node_history or {}
        points = history.get("points") or []
        if points:
            tail = points[-(width - 10) :]
            cpu_bar = "".join(
                _SPARK_BARS[
                    min(
                        len(_SPARK_BARS) - 1,
                        int((p[1] / 100) * (len(_SPARK_BARS) - 1)),
                    )
                ]
                for p in tail
                if isinstance(p, (list, tuple)) and len(p) >= 3
            )
            body.append(paint.style("", "fg"))
            body.append(
                paint.style(" cpu  ", "dim") + paint.style(cpu_bar, "run")
            )
            mem_bar = "".join(
                _SPARK_BARS[
                    min(
                        len(_SPARK_BARS) - 1,
                        int((p[2] / 100) * (len(_SPARK_BARS) - 1)),
                    )
                ]
                for p in tail
                if isinstance(p, (list, tuple)) and len(p) >= 3
            )
            body.append(
                paint.style(" mem  ", "dim") + paint.style(mem_bar, "accent")
            )
        return panel_frame(paint, "node resources", body, width, "esc close")


# ===================================================================
#  the application: rendering, part 3 (the drawers + multi-tail)
# ===================================================================
class AppDrawers(AppOverlays):
    def _compose_drawer(
        self,
        paint: Painter,
        base: List[str],
        cols: int,
        lines: int,
        which: str,
    ) -> List[str]:
        """Splice a right-hand drawer over the dimmed table, like the
        web page's aside."""
        drawer_w = max(46, min(90, (cols * 3) // 5))
        left_w = cols - drawer_w
        panel = (
            self.render_drawer_panel(paint, drawer_w, lines)
            if which == "drawer"
            else self.render_dag_panel(paint, drawer_w, lines)
        )
        rows = []
        border = paint.style("│", "accent")
        for idx in range(lines):
            left = cut_to_width(
                base[idx] if idx < len(base) else paint.row(), left_w - 1
            )
            row = panel[idx] if idx < len(panel) else ""
            rows.append(
                left + border + cut_to_width(paint.row(row), drawer_w - 1)
            )
        return self._compose_toasts(paint, rows, cols, lines)

    def _tabs_row(self, paint: Painter, tabs: List[str], active: str) -> str:
        spans = []
        for tab in tabs:
            is_active = tab == active
            spans.append(
                paint.style(
                    " %s " % tab,
                    "bright" if is_active else "dim",
                    bg="sel" if is_active else None,
                    bold=is_active,
                )
            )
        return " " + "".join(spans) + paint.style("  ←/→ switch", "dim")

    # ---- the job drawer ---------------------------------------------
    def render_drawer_panel(
        self, paint: Painter, width: int, lines: int
    ) -> List[str]:
        job = self.by_name.get(self.drawer_job or "") or {}
        key, label = health(job) if job else ("unknown", "?")
        color = {
            "ok": "ok",
            "fail": "fail",
            "run": "run",
            "pending": "pending",
            "disabled": "off",
            "cancelled": "off",
            "unknown": "pending",
        }[key]
        ascii_mode = bool(self.prefs["ascii"])
        rows = [
            " "
            + paint.style(
                "%s %s"
                % (paint.glyph(key, ascii_mode), self.drawer_job or "?"),
                color,
                bold=True,
            )
            + paint.style("  %s" % label, "dim")
            + paint.style("   r run · x cancel · esc close", "dim"),
            self._tabs_row(paint, self.DRAWER_TABS, self.drawer_tab),
            paint.hline(width - 2),
        ]
        body_lines = lines - len(rows) - 1
        if self.drawer_tab == "logs":
            rows.extend(self._drawer_logs(paint, width, body_lines))
        elif self.drawer_tab == "history":
            rows.extend(self._drawer_history(paint, width, body_lines))
        elif self.drawer_tab == "resources":
            rows.extend(self._drawer_resources(paint, width, body_lines))
        else:
            rows.extend(self._drawer_schedule(paint, width, body_lines))
        return rows

    def _drawer_logs(
        self, paint: Painter, width: int, body_lines: int
    ) -> List[str]:
        tail = self.log_tail
        rows: List[str] = []
        search = self.inputs["logsearch"]
        status_bits = []
        if tail is not None:
            status_bits.append("follow %s" % ("on" if tail.follow else "off"))
        if search:
            self._log_search_recompute()
            status_bits.append(
                "%d match%s"
                % (
                    len(self.log_matches),
                    "es" if len(self.log_matches) != 1 else "",
                )
            )
        head = " ⌕ " + (
            search + "▌"
            if self.focus == "logsearch"
            else (search or "/ to search")
        )
        rows.append(
            paint.style(head, "bright" if self.focus == "logsearch" else "dim")
            + paint.style("   " + " · ".join(status_bits), "dim")
        )
        available = body_lines - len(rows)
        if tail is None:
            rows.append(paint.style("  no stream", "dim"))
            return rows
        display: List[str] = []
        needle = search.strip().lower()
        for stream, line, when in tail.lines:
            if stream == "meta":  # inline end-of-run separator
                display.append(paint.style("  ── %s ──" % line, "dim"))
                continue
            text = rewrite_sgr(line, self.theme)
            plain = strip_ansi(line)
            prefix = ""
            if self.timestamps:
                stamp = datetime.datetime.fromtimestamp(when)
                prefix = paint.style(stamp.strftime("%H:%M:%S "), "dim")
            marker = paint.style(
                "▏", "fail" if stream == "stderr" else "border"
            )
            content_width = width - 4 - (9 if self.timestamps else 0)
            if needle and needle in plain.lower():
                text = paint.style(plain, "bright", bg="sel")
            if self.wrap and text_width(plain) > content_width:
                start = 0
                while start < len(plain):
                    chunk = plain[start : start + content_width]
                    display.append(
                        " " + marker + prefix + paint.style(chunk, "fg")
                    )
                    start += content_width
                continue
            display.append(" " + marker + prefix + text)
        if tail.error:
            display.append(paint.style("  ⚠ %s" % tail.error, "fail"))
        elif tail.ended == "no-output":
            display.append(
                paint.style("  ── end of run output (no-output) ──", "dim")
            )
        max_scroll = max(0, len(display) - available)
        self.log_scroll = min(self.log_scroll, max_scroll)
        end = len(display) - self.log_scroll
        rows.extend(display[max(0, end - available) : end])
        if not tail.lines and tail.ended is None and not tail.error:
            rows.append(paint.style("  waiting for output…", "dim"))
        return rows

    def _drawer_history(
        self, paint: Painter, width: int, body_lines: int
    ) -> List[str]:
        rows: List[str] = []
        data = self.drawer_runs
        if data is None:
            return [paint.style("  loading run history…", "dim")]
        stats = data.get("stats") or {}
        rate = stats.get("success_rate")
        rows.append(
            " "
            + paint.style(
                "%d runs" % (stats.get("total") or 0), "bright", bold=True
            )
            + paint.style(
                "  %s ok · %s fail · %s cancelled · %s unknown"
                % (
                    stats.get("success", 0),
                    stats.get("failure", 0),
                    stats.get("cancelled", 0),
                    stats.get("unknown", 0),
                ),
                "dim",
            )
        )
        rows.append(
            " "
            + paint.style(
                "success %s"
                % ("—" if rate is None else "%d%%" % round(rate * 100)),
                "ok" if (rate or 0) >= 0.9 else "warn",
            )
            + paint.style(
                "  avg %s · min %s · max %s"
                % (
                    fmt_duration(stats.get("avg_duration")),
                    fmt_duration(stats.get("min_duration")),
                    fmt_duration(stats.get("max_duration")),
                ),
                "dim",
            )
        )
        if stats.get("avg_cpu_seconds") is not None:
            rows.append(
                paint.style(
                    "  cpu avg %.1fs · peak rss %s"
                    % (
                        stats.get("avg_cpu_seconds") or 0,
                        fmt_bytes(stats.get("max_rss_bytes")),
                    ),
                    "dim",
                )
            )
        rows.append(paint.hline(width - 2))
        runs = list(reversed(data.get("runs") or []))
        durations = [
            r.get("duration") for r in runs if r.get("duration") is not None
        ]
        top = max(durations) if durations else 0
        ascii_mode = bool(self.prefs["ascii"])
        available = body_lines - len(rows)
        self.panel_scroll = max(
            0, min(self.panel_scroll, max(0, len(runs) - available))
        )
        for run in runs[self.panel_scroll : self.panel_scroll + available]:
            outcome = str(run.get("outcome", ""))
            key = {
                "failure": "fail",
                "cancelled": "cancelled",
                "unknown": "unknown",
            }.get(outcome, "ok")
            color = {
                "fail": "fail",
                "cancelled": "off",
                "unknown": "pending",
                "ok": "ok",
            }[key]
            dur = run.get("duration")
            bar_w = 12
            bar = ""
            if dur is not None and top:
                filled = max(1, int((dur / top) * bar_w))
                bar = "▪" * filled
            started = run.get("started_at")
            stamp = ""
            t = parse_iso(started)
            if t is not None:
                stamp = datetime.datetime.fromtimestamp(t).strftime(
                    "%m-%d %H:%M:%S"
                )
            detail = ""
            if outcome == "failure":
                exit_code = run.get("exit_code")
                detail = " exit %s" % ("?" if exit_code is None else exit_code)
                if run.get("fail_reason"):
                    detail += " · %s" % run["fail_reason"]
            resources = run.get("resources") or {}
            if resources.get("cpu_total_seconds") is not None:
                detail += " · %.1fs cpu" % resources["cpu_total_seconds"]
            rows.append(
                " "
                + paint.style(paint.glyph(key, ascii_mode), color)
                + paint.style(" %s " % stamp, "fg")
                + paint.style(pad_to(fmt_duration(dur), 8), "dim")
                + paint.style(pad_to(bar, bar_w + 1), color)
                + paint.style(truncate(detail, max(0, width - 44)), "dim")
            )
        if not runs:
            rows.append(paint.style("  no runs retained yet", "dim"))
        return rows

    def _drawer_resources(
        self, paint: Painter, width: int, body_lines: int
    ) -> List[str]:
        data = self.drawer_res
        if data is None:
            return [paint.style("  loading resource data…", "dim")]
        if not data.get("monitored"):
            return [
                paint.style("  this job has no resource monitoring", "dim"),
                paint.style(
                    "  (set monitorResources: true on the job)", "dim"
                ),
            ]
        rows: List[str] = []
        live = data.get("live") or []
        if live:
            snap = live[-1] if isinstance(live, list) else {}
            rows.append(
                " "
                + paint.style(
                    "live: %s cpu · %s rss"
                    % (
                        fmt_percent(snap.get("cpu_percent")),
                        fmt_bytes(snap.get("rss_bytes")),
                    ),
                    "run",
                    bold=True,
                )
            )
        runs = list(reversed(data.get("runs") or []))
        available = body_lines - len(rows) - 1
        rows.append(
            paint.style(
                pad_to(" started", 18) + pad_to("cpu", 10) + "peak rss",
                "dim",
                bold=True,
            )
        )
        for run in runs[:available]:
            started = parse_iso(run.get("started_at"))
            stamp = (
                datetime.datetime.fromtimestamp(started).strftime(
                    "%m-%d %H:%M"
                )
                if started
                else "?"
            )
            usage = run.get("resources") or run
            rows.append(
                paint.style(pad_to(" " + stamp, 18), "fg")
                + paint.style(
                    pad_to(
                        "%.1fs" % (usage.get("cpu_total_seconds") or 0), 10
                    ),
                    "dim",
                )
                + paint.style(fmt_bytes(usage.get("max_rss_bytes")), "dim")
            )
        return rows

    def _drawer_schedule(
        self, paint: Painter, width: int, body_lines: int
    ) -> List[str]:
        job = self.by_name.get(self.drawer_job or "") or {}
        schedule = str(job.get("schedule", ""))
        resolved = str(job.get("schedule_resolved") or "").strip()
        name = str(job.get("name", "")) or None
        # an H schedule: analyze the daemon's resolved spelling when the
        # payload carries it; hashing locally with the job's name is the
        # identical fallback (same salt) against an older daemon
        text = resolved or schedule
        rows = [
            " " + paint.style(schedule, "accent", bold=True),
            " " + paint.style(describe_cron(text, hash_key=name), "bright"),
        ]
        if resolved and resolved != schedule:
            rows.append(" " + paint.style("resolves to %s" % resolved, "dim"))
        rows.append(paint.style("", "fg"))
        tz_name = job.get("timezone")
        frame = "UTC" if job.get("utc", True) or not tz_name else str(tz_name)
        rows.append(paint.style(" reference frame: %s" % frame, "dim"))
        tz: datetime.tzinfo = datetime.timezone.utc
        if tz_name:
            try:
                from zoneinfo import ZoneInfo

                tz = ZoneInfo(str(tz_name))
            except Exception:  # noqa: BLE001 - fall back to UTC
                pass
        fires = next_fires(text, 8, tz, hash_key=name)
        if fires:
            rows.append(paint.style("", "fg"))
            rows.append(paint.style(" next runs:", "dim"))
            for when in fires:
                rows.append(
                    " "
                    + paint.style(when.strftime("%Y-%m-%d %H:%M:%S %Z"), "fg")
                )
        elif schedule.strip().lower() == "@reboot":
            rows.append(paint.style(" runs once, at daemon start", "dim"))
        # the daemon computed the findings at config load in the job's own
        # frame; render those when the payload ships them (an empty list
        # means "linted clean"), and lint locally with the same rules only
        # against an older daemon whose /jobs lacks the key.
        shipped = job.get("schedule_findings")
        if isinstance(shipped, list):
            findings = [
                Finding(
                    str(f.get("code", "")),
                    str(f.get("level", "note")),
                    str(f.get("message", "")),
                )
                for f in shipped
                if isinstance(f, dict)
            ]
        else:
            findings = lint_schedule(text, timezone=tz, hash_key=name)
        if findings:
            rows.append(paint.style("", "fg"))
        rows.extend(finding_rows(findings, paint, width, 3))
        sched_in = self.next_run_seconds(job)
        if sched_in is not None:
            rows.append(paint.style("", "fg"))
            rows.append(
                " "
                + paint.style(
                    "daemon says: next fire %s" % fmt_in(sched_in), "ok"
                )
            )
        return rows

    # ---- the DAG drawer ---------------------------------------------
    def render_dag_panel(
        self, paint: Painter, width: int, lines: int
    ) -> List[str]:
        dag = next(
            (d for d in self.dags if str(d.get("name", "")) == self.dag_name),
            {},
        )
        rows = [
            " "
            + paint.style("⧉ %s" % (self.dag_name or "?"), "accent", bold=True)
            + paint.style("  t trigger · b backfill · esc close", "dim"),
            self._tabs_row(paint, self.DAG_TABS, self.dag_tab),
            paint.hline(width - 2),
        ]
        if self.focus == "backfill":
            rows.insert(
                2,
                " "
                + paint.style("backfill FROM..TO: ", "dim")
                + paint.style(self.inputs["backfill"] + "▌", "bright"),
            )
        body_lines = lines - len(rows) - 1
        if self.dag_tab == "runs":
            rows.extend(self._dag_runs_tab(paint, width, body_lines))
        elif self.dag_tab == "graph":
            rows.extend(self._dag_graph_tab(paint, dag, width, body_lines))
        elif self.dag_tab == "tasks":
            rows.extend(self._dag_tasks_tab(paint, width, body_lines))
        elif self.dag_tab == "xcom":
            rows.extend(self._dag_xcom_tab(paint, width, body_lines))
        else:
            rows.extend(self._dag_logs_tab(paint, width, body_lines))
        return rows

    def _dag_runs_tab(
        self, paint: Painter, width: int, body_lines: int
    ) -> List[str]:
        rows: List[str] = []
        self.dag_sel = min(self.dag_sel, max(0, len(self.dag_runs) - 1))
        start = scroll_window(len(self.dag_runs), body_lines, self.dag_sel, 0)
        for idx, run in enumerate(self.dag_runs[start : start + body_lines]):
            selected = start + idx == self.dag_sel
            run_key = str(run.get("runKey") or run.get("run_key") or "?")
            state = str(run.get("state", "?"))
            when = (
                run.get("started_at")
                or run.get("createdAt")
                or run.get("created_at")
            )
            rows.append(
                paint.style(
                    pad_to(" " + truncate(run_key, 28), 30),
                    "bright" if selected else "fg",
                    bg="sel" if selected else None,
                    bold=selected,
                )
                + paint.style(
                    pad_to(state, 12),
                    self._dag_state_color(state),
                    bg="sel" if selected else None,
                )
                + paint.style(
                    fmt_ago_any(when),
                    "dim",
                    bg="sel" if selected else None,
                )
            )
        if not self.dag_runs:
            rows.append(paint.style("  no runs yet — t to trigger one", "dim"))
        return rows

    def _dag_graph_tab(
        self,
        paint: Painter,
        dag: Dict[str, Any],
        width: int,
        body_lines: int,
    ) -> List[str]:
        """The task graph as topological layers with edge lists."""
        tasks = dag.get("tasks")
        if isinstance(tasks, dict):
            task_list = [
                dict(v, key=k) if isinstance(v, dict) else {"key": k}
                for k, v in tasks.items()
            ]
        elif isinstance(tasks, list):
            task_list = [t for t in tasks if isinstance(t, dict)]
        else:
            task_list = []
        if not task_list:
            return [paint.style("  no task metadata", "dim")]
        # config entries name a task "id" (see dags_payload); run docs
        # key their tasks dict by the same string
        by_key = {
            str(t.get("id") or t.get("key") or t.get("name", "")): t
            for t in task_list
        }
        depth_cache: Dict[str, int] = {}

        def depth(key: str, seen: Tuple[str, ...] = ()) -> int:
            if key in depth_cache:
                return depth_cache[key]
            if key in seen:  # cycle guard; the daemon validates anyway
                return 0
            task = by_key.get(key) or {}
            deps = task.get("dependsOn") or task.get("depends_on") or []
            if isinstance(deps, str):
                deps = [deps]
            level = (
                1 + max(depth(str(d), seen + (key,)) for d in deps)
                if deps
                else 0
            )
            depth_cache[key] = level
            return level

        layers: Dict[int, List[str]] = {}
        for key in by_key:
            layers.setdefault(depth(key), []).append(key)
        run_states: Dict[str, str] = {}
        for task in self.dag_run_tasks():
            run_states[str(task.get("key", ""))] = str(task.get("state", ""))
        rows: List[str] = []
        ascii_mode = bool(self.prefs["ascii"])
        arrow = "->" if ascii_mode else "─▶"
        for level in sorted(layers):
            names = sorted(layers[level])
            spans = [paint.style(" %d " % level, "dim")]
            for name in names:
                state = run_states.get(name, "")
                color = self._dag_state_color(state) if state else "fg"
                spans.append(
                    paint.style(
                        "[%s%s]" % (name, (" " + state) if state else ""),
                        color,
                        bold=bool(state),
                    )
                )
                spans.append(paint.style(" ", "fg"))
            rows.append(cut_to_width("".join(spans), width - 4))
            for name in names:
                task = by_key[name] or {}
                deps = task.get("dependsOn") or task.get("depends_on") or []
                if isinstance(deps, str):
                    deps = [deps]
                for dep in deps:
                    rows.append(
                        paint.style(
                            "     %s %s %s" % (dep, arrow, name), "dim"
                        )
                    )
        return rows[:body_lines]

    def _dag_tasks_tab(
        self, paint: Painter, width: int, body_lines: int
    ) -> List[str]:
        tasks = self.dag_run_tasks()
        rows: List[str] = []
        if not self.dag_run_key:
            rows.append(
                paint.style("  open a run first (runs tab, enter)", "dim")
            )
            return rows
        rows.append(paint.style(" run %s" % self.dag_run_key, "dim"))
        self.dag_sel = min(self.dag_sel, max(0, len(tasks) - 1))
        start = scroll_window(len(tasks), body_lines - 1, self.dag_sel, 0)
        for idx, task in enumerate(tasks[start : start + body_lines - 1]):
            selected = start + idx == self.dag_sel
            key = str(task.get("key", "?"))
            state = str(task.get("state", "?"))
            # a parked approval gate reports state "running" with an
            # awaitingApproval flag riding along (see the dagrun docs)
            awaiting = bool(task.get("awaitingApproval")) or (
                state.lower() == "awaiting"
            )
            if awaiting:
                state = "awaiting"
            attempts = task.get("attempts") or task.get("attempt")
            extra = ""
            if attempts:
                extra += " · try %s" % attempts
            if awaiting:
                extra += "  ► a approve · R reject"
            rows.append(
                paint.style(
                    pad_to(" " + truncate(key, 26), 28),
                    "bright" if selected else "fg",
                    bg="sel" if selected else None,
                    bold=selected,
                )
                + paint.style(
                    pad_to(state, 11),
                    self._dag_state_color(state),
                    bg="sel" if selected else None,
                )
                + paint.style(
                    truncate(extra, max(0, width - 44)),
                    "dim",
                    bg="sel" if selected else None,
                )
            )
        if not tasks:
            rows.append(paint.style("  loading run detail…", "dim"))
        return rows

    def _dag_xcom_tab(
        self, paint: Painter, width: int, body_lines: int
    ) -> List[str]:
        data = self.dag_xcom
        if not self.dag_run_key:
            return [paint.style("  open a run first (runs tab, enter)", "dim")]
        if data is None:
            return [paint.style("  loading xcom…", "dim")]
        entries = data.get("xcom") if isinstance(data, dict) else None
        if entries is None and isinstance(data, dict):
            entries = {
                k: v
                for k, v in data.items()
                if k not in ("dag", "runKey", "run_key")
            }
        rows: List[str] = []
        if isinstance(entries, dict) and entries:
            for key, value in list(entries.items())[:body_lines]:
                rows.append(
                    paint.style(pad_to(" %s" % key, 24), "accent")
                    + paint.style(
                        truncate(
                            json.dumps(value, default=str), max(0, width - 30)
                        ),
                        "fg",
                    )
                )
        else:
            rows.append(paint.style("  no xcom values", "dim"))
        return rows

    def _dag_logs_tab(
        self, paint: Painter, width: int, body_lines: int
    ) -> List[str]:
        tail = self.dag_task_tail
        if tail is None:
            return [
                paint.style(
                    "  pick a task (tasks tab, enter) to read its log", "dim"
                )
            ]
        rows = [paint.style(" task: %s" % tail.label, "dim")]
        display: List[str] = []
        for stream, line, _when in tail.lines:
            if stream == "meta":
                display.append(paint.style("  ── end of log ──", "dim"))
                continue
            marker = paint.style(
                "▏", "fail" if stream == "stderr" else "border"
            )
            display.append(" " + marker + rewrite_sgr(line, self.theme))
        if tail.error:
            display.append(paint.style("  ⚠ %s" % tail.error, "fail"))
        elif tail.ended == "no-output":
            display.append(paint.style("  ── no output ──", "dim"))
        available = body_lines - 1
        max_scroll = max(0, len(display) - available)
        self.panel_scroll = min(self.panel_scroll, max_scroll)
        end = len(display) - self.panel_scroll
        rows.extend(display[max(0, end - available) : end])
        return rows

    # ---- multi-tail console -----------------------------------------
    def render_tail(self, paint: Painter, cols: int, lines: int) -> List[str]:
        width = min(cols - 4, 110)
        identity = ["run", "ok", "pending", "warn"]
        head_spans = []
        for idx, tail in enumerate(self.tails):
            selected = idx == self.tail_sel
            head_spans.append(
                paint.style(
                    " %s " % tail.label,
                    identity[idx % len(identity)],
                    bg="sel" if selected else None,
                    bold=selected,
                )
            )
        if not self.tails:
            head_spans.append(paint.style(" empty — a to add a job ", "dim"))
        body = ["".join(head_spans)]
        if self.focus == "tailadd":
            body.append(
                paint.style(" add: ", "dim")
                + paint.style(self.inputs["tailadd"] + "▌", "bright")
            )
        merged: List[Tuple[float, int, str, str, str]] = []
        for idx, tail in enumerate(self.tails):
            for stream, line, when in tail.lines:
                if stream == "meta":
                    line = "── %s ──" % line
                merged.append((when, idx, tail.label, stream, line))
        merged.sort(key=lambda item: item[0])
        available = max(3, lines - 10)
        max_scroll = max(0, len(merged) - available)
        self.panel_scroll = min(self.panel_scroll, max_scroll)
        end = len(merged) - self.panel_scroll
        for when, idx, label, stream, line in merged[
            max(0, end - available) : end
        ]:
            color = identity[idx % len(identity)]
            prefix = paint.style(pad_to(label, 14) + "▏", color)
            stamp = ""
            if self.timestamps:
                stamp = paint.style(
                    datetime.datetime.fromtimestamp(when).strftime(
                        "%H:%M:%S "
                    ),
                    "dim",
                )
            if stream == "meta":
                body.append(" " + prefix + stamp + paint.style(line, "dim"))
            else:
                body.append(
                    " " + prefix + stamp + rewrite_sgr(line, self.theme)
                )
        if self.tails and not merged:
            body.append(paint.style("  waiting for output…", "dim"))
        return panel_frame(
            paint,
            "multi-tail (%d/%d)" % (len(self.tails), TAIL_MAX),
            body,
            width,
            "a add · x remove · j/k pick · t timestamps · w wrap · "
            "pgup/pgdn scroll · esc close",
        )


# ===================================================================
#  the application: boot self-test + concrete class
# ===================================================================
class TuiApp(AppDrawers):
    """The concrete application class the CLI (and the tests) drive."""

    async def _boot_sequence(self) -> None:
        """The BIOS-style power-on self-test, with real probes behind
        each line.  Any key skips; runs at most every 12 hours (like the
        web page's boot POST) unless forced with --boot."""
        self.booting = True
        paint = Painter(self.theme)
        self.boot_rows = []
        skip = asyncio.get_running_loop().create_task(self.keys.get())

        async def type_line(text: str, style: str = "fg") -> bool:
            """Type one line; True when the user skipped."""
            for idx in range(0, len(text) + 1, 3):
                self.boot_rows = self.boot_rows[:-1] + [
                    paint.style(text[:idx] + "▌", style)
                ]
                self.paint()
                if skip.done():
                    return True
                await asyncio.sleep(0.012)
            self.boot_rows[-1] = paint.style(text, style)
            self.paint()
            return skip.done()

        def push() -> None:
            self.boot_rows.append("")

        try:
            push()
            if await type_line(
                " CRONSTABLE TUI — POWER-ON SELF-TEST", "accent"
            ):
                return
            push()
            push()
            start = time.monotonic()
            ok = True
            version = ""
            try:
                version = (
                    await _race_skip(self.api.get_text("/version"), skip, "")
                ).strip()
            except Unauthorized:
                version = "locked (token needed)"
            except Exception:  # noqa: BLE001 - reported on the line
                ok = False
            latency = (time.monotonic() - start) * 1000
            if await type_line(
                " link ........ %s"
                % (
                    "OK %s (%dms)" % (self.api.url, latency)
                    if ok
                    else "FAIL %s unreachable" % self.api.url
                ),
                "ok" if ok else "fail",
            ):
                return
            push()
            if await type_line(
                " firmware .... %s" % (version or "unknown"),
                "fg",
            ):
                return
            jobs: List[Dict[str, Any]] = []
            with contextlib.suppress(Exception):
                data = await _race_skip(self.api.get_json("/jobs"), skip, None)
                if isinstance(data, list):
                    jobs = data
            job_set = ""
            with contextlib.suppress(Exception):
                job_set = (
                    await _race_skip(
                        self.api.get_text("/job-set-id"), skip, ""
                    )
                ).strip()[:12]
            push()
            if await type_line(
                " job set ..... %d job%s%s"
                % (
                    len(jobs),
                    "s" if len(jobs) != 1 else "",
                    (" · %s" % job_set) if job_set else "",
                ),
                "fg",
            ):
                return
            soonest: Optional[float] = None
            for job in jobs:
                sched = job.get("scheduled_in")
                if sched is not None and (soonest is None or sched < soonest):
                    soonest = float(sched)
            push()
            if await type_line(
                " schedules ... %s"
                % (
                    "next fire %s" % fmt_in(soonest)
                    if soonest is not None
                    else "nothing scheduled"
                ),
                "fg",
            ):
                return
            cluster_line = "standalone"
            with contextlib.suppress(Exception):
                cluster = await _race_skip(
                    self.api.get_json("/cluster"), skip, {}
                )
                if cluster.get("enabled"):
                    cluster_line = "%s · %s" % (
                        cluster.get("backend", "gossip"),
                        "quorate" if cluster.get("quorate") else "NO QUORUM",
                    )
            push()
            if await type_line(" cluster ..... %s" % cluster_line, "fg"):
                return
            failing = sum(1 for j in jobs if health(j)[0] == "fail")
            push()
            push()
            verdictline = (
                " ALL CHECKS PASSED"
                if ok and not failing
                else " %d JOB%s FAILING — the board wants you"
                % (failing, "S" if failing != 1 else "")
                if ok
                else " DEGRADED — daemon unreachable"
            )
            if await type_line(
                verdictline, "ok" if ok and not failing else "warn"
            ):
                return
        finally:
            if not skip.done():
                skip.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await skip
            self.booting = False
            self.prefs["boot_last"] = time.time()
            self.save_prefs()
            self.term.invalidate()
            self.mark()
        await asyncio.sleep(0.35)


# ===================================================================
#  CLI plumbing (registered from cronstable.__main__, mcpcli-style)
# ===================================================================
async def _race_skip(
    coro: Coroutine[Any, Any, Any],
    skip: "asyncio.Task[Any]",
    default: Any,
) -> Any:
    """Await a boot-probe API call, but let the skip key win the race:
    a hung daemon must not hold the keyboard hostage during boot."""
    task = asyncio.get_running_loop().create_task(coro)
    await asyncio.wait({task, skip}, return_when=asyncio.FIRST_COMPLETED)
    if not task.done():
        task.cancel()
        with contextlib.suppress(BaseException):
            await task
        return default
    return task.result()


def add_tui_command(sub: Any) -> None:
    """Attach the ``tui`` subcommand to the root parser's subparsers."""
    parser = sub.add_parser(
        "tui",
        help=(
            "open the terminal dashboard (the web dashboard's TUI "
            "sibling) against a running daemon's web listener"
        ),
        description=(
            "A keyboard-driven terminal rendition of the cronstable web "
            "dashboard, speaking the same HTTP control API. The web "
            "page's shortcuts apply: j/k move, Enter opens a job, r "
            "runs it, x cancels, / filters, Ctrl-K opens the command "
            "palette, ? lists every key."
        ),
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help="daemon web listener (default: %(default)s)",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="bearer token for web.authToken-protected daemons",
    )
    parser.add_argument(
        "--token-env",
        default=ENV_TOKEN,
        metavar="VAR",
        help=(
            "environment variable to read the token from when --token "
            "is not given (default: %(default)s)"
        ),
    )
    parser.add_argument(
        "--theme",
        default=None,
        choices=list(THEME_HUES) + [h + "-light" for h in THEME_HUES],
        help="start on a specific theme (persisted for next time)",
    )
    parser.add_argument(
        "--tv",
        action="store_true",
        help="start straight on the wallboard (the page's #tv)",
    )
    parser.add_argument(
        "--job",
        default=None,
        metavar="NAME",
        help="open a job's drawer at startup (the page's #job/NAME)",
    )
    parser.add_argument(
        "--boot",
        action="store_true",
        help="force the boot self-test even if one ran recently",
    )
    parser.add_argument(
        "--no-boot",
        action="store_true",
        help="skip the boot self-test",
    )
    parser.add_argument(
        "--ascii",
        action="store_true",
        help="plain-ASCII status glyphs (limited fonts/terminals)",
    )
    parser.add_argument(
        "--poll",
        type=float,
        default=None,
        metavar="SECONDS",
        help="refresh interval; 0 pauses (default: remembered, else 3)",
    )


def _resolve_token(args: Any) -> Optional[str]:
    if getattr(args, "token", None):
        return str(args.token)
    env_name = getattr(args, "token_env", ENV_TOKEN) or ENV_TOKEN
    value = os.environ.get(env_name, "")
    return value or None


def dispatch(args: Any) -> int:
    """Run the TUI; returns a process exit code."""
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        print(
            "cronstable tui needs an interactive terminal "
            "(stdin/stdout are not a tty)",
            file=sys.stderr,
        )
        return 2
    prefs = load_prefs()
    if getattr(args, "theme", None):
        theme = str(args.theme)
        prefs["light"] = theme.endswith("-light")
        prefs["theme"] = theme.replace("-light", "")
    if getattr(args, "ascii", False):
        prefs["ascii"] = True
    if getattr(args, "poll", None) is not None:
        prefs["poll_ms"] = max(0, int(float(args.poll) * 1000))
    boot: Optional[bool] = None
    if getattr(args, "no_boot", False):
        boot = False
    elif getattr(args, "boot", False):
        boot = True

    async def _amain() -> int:
        loop = asyncio.get_running_loop()
        keys: Any
        if sys.platform == "win32":  # pragma: no cover - Windows only
            keys = WindowsKeyReader(loop)
        else:
            keys = PosixKeyReader(loop, sys.stdin.fileno())
        app = TuiApp(
            Api(str(args.url), _resolve_token(args)),
            Term(),
            keys,
            prefs,
            start_wallboard=bool(getattr(args, "tv", False)),
            start_job=getattr(args, "job", None),
            boot=boot,
        )
        try:
            await app.run()
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        return 0

    try:
        return asyncio.run(_amain())
    except KeyboardInterrupt:  # pragma: no cover - direct SIGINT race
        return 0
