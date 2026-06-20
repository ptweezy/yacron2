#!/usr/bin/env python3
"""Generate a HISTORY.md entry for a release commit.

Invoked by the ``commit-msg`` git hook. It only does anything when the commit
message has a release-marker line of its own -- ``[release]`` (minor) or
``[release:major|minor|patch]`` -- matching the release workflow's trigger. A
mention of ``[release]`` inside prose never counts. It then:

  1. works out the next version (latest ``X.Y.Z`` tag + the marker's bump,
     default minor),
  2. drafts a Markdown entry summarising the commits since that tag,
     using the ``claude`` CLI if one is on PATH, otherwise falling back to a
     deterministic list of commit subjects,
  3. inserts it at the top of HISTORY.md and stages the file so it lands in
     this same commit.

It is deliberately best-effort: any failure prints a warning and exits 0 so a
release commit is never blocked by changelog tooling.

Usage (from the hook): ``python gen_changelog_entry.py <commit-msg-file>``
"""

from __future__ import annotations

import datetime
import os
import re
import shutil
import subprocess
import sys

HISTORY = "HISTORY.md"
VERSION_HEADER_RE = re.compile(r"^## \d+\.\d+\.\d+ \(")
# A release marker must be on its OWN line: [release] (minor) or
# [release:major|minor|patch]. Anchored so prose mentions never match.
MARKER_RE = re.compile(
    r"^\s*\[release(?::(major|minor|patch))?\]\s*$", re.IGNORECASE
)
TOKEN_RE = re.compile(r"\[release(?::(?:major|minor|patch))?\]", re.IGNORECASE)
# Diff size handed to the LLM is capped so the call stays fast and cheap.
MAX_DIFF_CHARS = 8000


def git(*args: str) -> str:
    """Run a git command at the repo root and return stripped stdout."""
    return subprocess.run(
        ["git", *args],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def latest_release_tag() -> str:
    tags = [
        t
        for t in git("tag", "-l").splitlines()
        if re.fullmatch(r"\d+\.\d+\.\d+", t)
    ]
    # Sort numerically by (major, minor, patch) so 1.10.0 > 1.9.0.
    tags.sort(key=lambda t: tuple(int(p) for p in t.split(".")))
    return tags[-1] if tags else "0.0.0"


def release_bump(message: str) -> str | None:
    """Return the bump level if the message has a release-marker line, else None."""
    for line in message.splitlines():
        m = MARKER_RE.match(line)
        if m:
            return (m.group(1) or "minor").lower()
    return None


def next_version(latest: str, bump: str) -> str:
    major, minor, patch = (int(p) for p in latest.split("."))
    if bump == "major":
        major, minor, patch = major + 1, 0, 0
    elif bump == "minor":
        minor, patch = minor + 1, 0
    else:
        patch += 1
    return f"{major}.{minor}.{patch}"


def collect_changes(latest: str, message: str) -> tuple[list[str], str]:
    """Return (commit subjects since the tag, capped staged diff)."""
    subjects: list[str] = []
    if latest != "0.0.0":
        log = git(
            "log", f"{latest}..HEAD", "--no-merges", "--pretty=format:%s"
        )
        subjects = [s for s in log.splitlines() if s.strip()]
    # The in-progress commit isn't in the log yet; add its subject line.
    current_subject = next(
        (
            ln.strip()
            for ln in message.splitlines()
            if ln.strip() and not ln.startswith("#")
        ),
        "",
    )
    if current_subject:
        subjects.insert(0, current_subject)
    # Strip the control tokens so they never leak into the changelog text.
    subjects = [strip_tokens(s) for s in subjects if strip_tokens(s)]

    try:
        diff = git("diff", "--cached")
    except subprocess.CalledProcessError:
        diff = ""
    return dedupe(subjects), diff[:MAX_DIFF_CHARS]


def strip_tokens(text: str) -> str:
    return TOKEN_RE.sub("", text).strip()


def dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for it in items:
        if it not in seen:
            seen.add(it)
            out.append(it)
    return out


def llm_body(version: str, subjects: list[str], diff: str) -> str | None:
    """Draft the entry body with the claude CLI, or None if unavailable."""
    claude = shutil.which("claude")
    if not claude:
        return None
    prompt = (
        "You are writing a changelog entry for the Python project yacron2.\n"
        f"The new version is {version}.\n\n"
        "Write ONLY the body of a Markdown changelog entry:\n"
        "- a bulleted list using '- ' bullets, present tense, user-facing;\n"
        "- optionally grouped under '### ' headers like '### Bug fixes' or "
        "'### Features' (only if there is more than one group);\n"
        "- wrap code/identifiers in single backticks;\n"
        "- wrap lines at about 75 columns;\n"
        "- do NOT include the version number, the date, a top-level title, or "
        "code fences. Output raw Markdown only.\n\n"
        "Commits since the last release:\n"
        + "\n".join(f"- {s}" for s in subjects)
        + "\n\nStaged diff (truncated):\n"
        + diff
    )
    try:
        out = subprocess.run(
            [claude, "-p"],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        print(f"changelog: claude call failed ({exc}); using fallback.",
              file=sys.stderr)
        return None
    if out.returncode != 0:
        print(f"changelog: claude exited {out.returncode}; using fallback.",
              file=sys.stderr)
        return None
    body = out.stdout.strip()
    # Defensive: drop accidental code fences.
    body = re.sub(r"^```[a-zA-Z]*\n|\n```$", "", body).strip()
    return body or None


def fallback_body(subjects: list[str]) -> str:
    if not subjects:
        return "- Maintenance release."
    return "\n".join(f"- {s}" for s in subjects)


def insert_entry(version: str, body: str) -> bool:
    """Insert the entry into HISTORY.md. Return False if nothing changed."""
    with open(HISTORY, encoding="utf-8") as fh:
        text = fh.read()

    if re.search(rf"^## {re.escape(version)} \(", text, flags=re.MULTILINE):
        # Already present (e.g. a commit --amend or re-run); leave it alone.
        return False

    today = datetime.date.today().isoformat()
    entry = f"## {version} ({today})\n\n{body}\n"

    lines = text.splitlines(keepends=True)
    for i, line in enumerate(lines):
        if VERSION_HEADER_RE.match(line):
            lines.insert(i, entry + "\n\n")
            break
    else:
        # No existing release section; append at the end.
        lines.append("\n\n" + entry)

    with open(HISTORY, "w", encoding="utf-8") as fh:
        fh.write("".join(lines))
    return True


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        return 0
    try:
        with open(argv[1], encoding="utf-8") as fh:
            message = fh.read()
    except OSError:
        return 0

    bump = release_bump(message)
    if bump is None:
        return 0  # not a release commit; nothing to do

    os.chdir(git("rev-parse", "--show-toplevel"))
    if not os.path.exists(HISTORY):
        print("changelog: HISTORY.md not found; skipping.", file=sys.stderr)
        return 0

    latest = latest_release_tag()
    version = next_version(latest, bump)
    subjects, diff = collect_changes(latest, message)

    body = llm_body(version, subjects, diff) or fallback_body(subjects)

    if insert_entry(version, body):
        git("add", HISTORY)
        print(f"changelog: added HISTORY.md entry for {version}.",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv))
    except Exception as exc:  # never block a commit on changelog tooling
        print(f"changelog: unexpected error ({exc}); committing without entry.",
              file=sys.stderr)
        sys.exit(0)
