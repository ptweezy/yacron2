#!/usr/bin/env python3
"""Fail CI if an installed dependency carries a license cronstable cannot ship.

cronstable's core is MIT: a permissive, non-copyleft license (see LICENSING.md).
The shipped artifacts (the PyInstaller binaries and the Docker images) bundle the
whole dependency tree, so a copyleft or non-open dependency would impose
obligations the project does not accept. This guard keeps the permissive baseline
from regressing by accident.

It reads license metadata straight from the INSTALLED distributions via
importlib.metadata, so it needs no third-party scanner and sees exactly what a
build would bundle. Each distribution is classified by the strongest obligation
found:

  FAIL  strong copyleft (GPL, AGPL) or non-open source-available (SSPL, BUSL,
        the Commons Clause)                                      -> exit 1
  warn  weak, file-scoped copyleft (LGPL, MPL, EPL, CDDL): allowed, because
        depending on such a library does not relicense our code, but surfaced
        so it stays a conscious choice (use --strict to fail on these too)
  ok    permissive (MIT, BSD, Apache, ISC, PSF, Zlib, Unlicense, ...)
  ?     no recognizable license signal (surfaced for manual review)

Classification matches short tokens (GPL, MPL, ...) only against the STRUCTURED
signals: the PEP 639 SPDX ``License-Expression`` and the ``License :: ...`` trove
classifiers. It never token-matches a full-text license *body*, because a body
contains accidental substrings (the MIT warranty line "EXPRESS OR IMPLIED"
contains "MPL"). A free-text body is used only as a fallback, matched against
long, unambiguous license-name phrases, for the rare package that ships no
classifier or SPDX expression.

Usage:
    python scripts/check_licenses.py            # scan the current environment
    python scripts/check_licenses.py --strict   # also fail on weak copyleft
"""

from __future__ import annotations

import argparse
import sys
from importlib import metadata

# Distributions whose metadata is missing or misleading but whose license has
# been verified by hand. Maps the (lowercased) distribution name to a short
# reason. Keep this small and always cite why; it is the only manual override.
ACKNOWLEDGED: dict[str, str] = {
    # "example": "PSF-equivalent; the wheel omits the trove classifier",
}

# The project itself is not a third-party dependency; skip it.
SELF = {"cronstable"}

OK, WEAK, DENY, UNKNOWN = "ok", "warn", "FAIL", "?"

# --- Short tokens: matched ONLY against structured signals (short + curated) ---
# Licenses we refuse outright. The bare "GPL" token is reached in classify only
# after AGPL and LGPL are ruled out, so it never mis-hits them.
DENY_TOKENS = (
    "SSPL",
    "SERVER SIDE PUBLIC",
    "BUSL",
    "BUSINESS SOURCE",
    "COMMONS CLAUSE",
    "GPL",
)
# Weak / file-scoped copyleft: allowed by default, reported, --strict fails.
WEAK_TOKENS = (
    "LGPL",
    "MPL",
    "MOZILLA PUBLIC",
    "EPL",
    "ECLIPSE PUBLIC",
    "CDDL",
    "EUPL",
    "OSL",
    "OPEN SOFTWARE LICENSE",
    "CECILL",
)
# Permissive signals. Not required to pass (anything not FAIL/warn passes), but
# used to tell a known-permissive license from an unrecognized one.
PERMISSIVE_TOKENS = (
    "MIT",
    "BSD",
    "APACHE",
    "ISC",
    "ZLIB",
    "0BSD",
    "PYTHON SOFTWARE FOUNDATION",
    "PSF",
    "PYTHON-2.0",
    "UNLICENSE",
    "PUBLIC DOMAIN",
    "WTFPL",
    "BOOST",
    "HPND",
)

# --- Long phrases: matched against a full-text body (fallback only) ----------
# Distinct enough not to collide with unrelated prose. AFFERO and LESSER are
# checked first in _classify_body so the plain-GPL phrase cannot swallow them.
DENY_PHRASES = (
    "GNU GENERAL PUBLIC LICENSE",
    "SERVER SIDE PUBLIC LICENSE",
    "BUSINESS SOURCE LICENSE",
    "COMMONS CLAUSE",
)
WEAK_PHRASES = (
    "MOZILLA PUBLIC LICENSE",
    "ECLIPSE PUBLIC LICENSE",
    "COMMON DEVELOPMENT AND DISTRIBUTION LICENSE",
    "EUROPEAN UNION PUBLIC LICENSE",
    "OPEN SOFTWARE LICENSE",
    "CECILL",
)
PERMISSIVE_PHRASES = (
    "PERMISSION IS HEREBY GRANTED",                  # MIT / ISC
    "REDISTRIBUTION AND USE IN SOURCE AND BINARY",   # BSD
    "APACHE LICENSE",
    "BOOST SOFTWARE LICENSE",
    "THIS IS FREE AND UNENCUMBERED SOFTWARE",        # Unlicense
)


def gather(dist: metadata.Distribution) -> tuple[str, str, str]:
    """Return (structured signals upper, body upper, short label)."""
    md = dist.metadata
    struct: list[str] = []
    labels: list[str] = []
    expr = md.get("License-Expression")
    if expr:
        struct.append(expr)
        labels.append(expr)
    for classifier in md.get_all("Classifier") or []:
        if classifier.startswith("License ::"):
            struct.append(classifier)
            labels.append(classifier.split("::")[-1].strip())
    body = md.get("License") or ""
    # A short License field is a name/identifier (safe to treat as structured);
    # a long, multi-line one is a full license body (fallback + display only).
    short = body.strip() if (0 < len(body.strip()) <= 40 and "\n" not in body) else ""
    if short:
        struct.append(short)
        labels.append(short)
    label = "; ".join(dict.fromkeys(x for x in labels if x))
    if not label:
        label = (body.strip().splitlines()[0][:40] if body.strip() else "") or "UNKNOWN"
    return " ".join(struct).upper(), body.upper(), label


def _classify_tokens(s: str) -> str:
    if not s.strip():
        return UNKNOWN
    # AGPL and LGPL first, so the bare "GPL" token below cannot mis-hit them.
    if "AGPL" in s or "AFFERO" in s:
        return DENY
    if "LGPL" in s:
        return WEAK
    if any(tok in s for tok in DENY_TOKENS):
        return DENY
    if any(tok in s for tok in WEAK_TOKENS):
        return WEAK
    if any(tok in s for tok in PERMISSIVE_TOKENS):
        return OK
    return UNKNOWN


def _classify_body(b: str) -> str:
    if not b.strip():
        return UNKNOWN
    if "AFFERO GENERAL PUBLIC LICENSE" in b:
        return DENY
    if "LESSER GENERAL PUBLIC LICENSE" in b:  # LGPL, before the plain-GPL phrase
        return WEAK
    if any(p in b for p in DENY_PHRASES):
        return DENY
    if any(p in b for p in WEAK_PHRASES):
        return WEAK
    if any(p in b for p in PERMISSIVE_PHRASES):
        return OK
    return UNKNOWN


def classify(struct: str, body: str) -> str:
    verdict = _classify_tokens(struct)
    if verdict != UNKNOWN:
        return verdict
    return _classify_body(body)  # fallback for classifier-less packages


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fail on copyleft / non-open dependency licenses."
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="also fail on weak, file-scoped copyleft (LGPL, MPL, ...).",
    )
    args = parser.parse_args(argv)

    rows: list[tuple[str, str, str, str]] = []
    denied: list[tuple[str, str]] = []
    for dist in metadata.distributions():
        name = (dist.metadata.get("Name") or "").strip()
        if not name or name.lower() in SELF:
            continue
        try:
            struct, body, label = gather(dist)
            verdict = classify(struct, body)
        except Exception as exc:  # never let odd metadata crash the gate
            verdict, label = UNKNOWN, "unreadable metadata: %s" % exc
        if name.lower() in ACKNOWLEDGED and verdict != OK:
            verdict = OK
        rows.append((verdict, name, dist.version or "?", label))
        if verdict == DENY or (args.strict and verdict == WEAK):
            denied.append((name, label))

    order = {DENY: 0, WEAK: 1, UNKNOWN: 2, OK: 3}
    rows.sort(key=lambda r: (order[r[0]], r[1].lower()))
    _print_report(rows)

    if denied:
        print()
        print("FAIL: dependency license policy violated (see LICENSING.md).")
        for name, label in denied:
            print("  - %s: %s" % (name, label))
        print()
        print(
            "cronstable stays MIT-permissive and bundles its dependencies, so a\n"
            "copyleft or source-available dependency is not acceptable. Drop it,\n"
            "find a permissive alternative, or (if this is a false positive) add\n"
            "it to ACKNOWLEDGED in this script with a reason."
        )
        return 1

    print()
    print("OK: no disallowed dependency licenses found (%d checked)." % len(rows))
    return 0


def _print_report(rows: list[tuple[str, str, str, str]]) -> None:
    if not rows:
        print("No third-party distributions found in this environment.")
        return
    wname = max(len(r[1]) for r in rows)
    wver = max(len(r[2]) for r in rows)
    print("%-4s  %-*s  %-*s  %s" % ("", wname, "PACKAGE", wver, "VERSION", "LICENSE"))
    for verdict, name, version, label in rows:
        print("%-4s  %-*s  %-*s  %s" % (verdict, wname, name, wver, version, label))


if __name__ == "__main__":
    sys.exit(main())
