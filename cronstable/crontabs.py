r"""Classic (Vixie-style) crontab support.

cronstable's native configuration is YAML, but plenty of perfectly good
schedules already exist as plain crontabs.  This module accepts that
format: it recognises crontab files (:func:`is_crontab_path`,
:func:`looks_like_crontab`) and lowers each entry into the same plain job
dictionaries the YAML front end produces (:func:`parse_crontab`), so
everything downstream -- defaults merging, validation, scheduling,
concurrency, reporting, the web API and clustering -- treats a legacy
crontab job exactly like a YAML one.

The accepted syntax follows ``man 5 crontab`` (user crontabs):

* comment (``# ...``) and blank lines;
* ``NAME = value`` environment assignments, applying to the entries below
  them (position matters, as in cron); a value may be single- or
  double-quoted to preserve blanks;
* ``minute hour day-of-month month day-of-week command`` entries, with
  names, ranges, lists and steps in the time fields;
* the ``@reboot`` / ``@yearly`` / ``@annually`` / ``@monthly`` /
  ``@weekly`` / ``@daily`` / ``@midnight`` / ``@hourly`` nicknames;
* ``\%`` in the command as an escaped literal percent sign.

Everything *around* the schedule deliberately gets cronstable's standard
defaults rather than an emulation of cron's environment: schedules run in
UTC unless ``CRON_TZ`` says otherwise, failure is detected from stderr
output and exit status instead of mailed via ``MAILTO``, and the
``%``-as-stdin feature is refused rather than half-imitated.  See the
"Classic crontabs" documentation for the full list of deviations.
"""

import os
import re
from typing import Any, Dict, List
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from cronstable.cronexpr import CronTab

#: Filename markers the config loader treats as "this is a classic
#: crontab" without looking at the content: the two conventional
#: extensions, or a file literally named ``crontab`` (as ``/etc/crontab``
#: and ``crontab -l`` exports usually are).  Matched case-insensitively
#: for the benefit of case-preserving filesystems (Windows, macOS).
CRONTAB_EXTENSIONS = frozenset({".crontab", ".cron"})
CRONTAB_BASENAME = "crontab"

#: The ``man 5 crontab`` schedule nicknames.  All are accepted;
#: ``@midnight`` is rewritten to its synonym ``@daily`` (the cron
#: expression engine understands every nickname except that one) and
#: ``@reboot`` passes through untouched -- the scheduler, not the
#: cron-expression parser, gives it meaning (JobConfig._parse_schedule).
_NICKNAMES = frozenset(
    {
        "@reboot",
        "@yearly",
        "@annually",
        "@monthly",
        "@weekly",
        "@daily",
        "@midnight",
        "@hourly",
    }
)

# A ``NAME = value`` environment line: a POSIX-style variable name with
# optional blanks around ``=`` (both allowed by man 5 crontab).  Exotic
# names a real cron might tolerate (quoted names, names with dots) fall
# through to the job-line parser and get a clear per-line error instead.
_ENV_LINE = re.compile(
    r"^(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?P<value>.*)$"
)

# Characters refused inside a live (non-blank, non-comment) crontab line:
# the C0 controls except TAB (a legitimate field separator; LF cannot appear
# -- lines are split on it), DEL, the C1 controls, and the U+2028/U+2029
# line separators.  A NUL builds a job the OS can never spawn (exec refuses
# an argument carrying one) and the resulting ValueError would otherwise
# escape the scheduler; the others are exactly the ``str.splitlines()``
# family that renders as ONE line in an editor, ``cat``, ``git diff`` and a
# code review -- silently truncating a command or smuggling a job into a
# comment if honoured as separators, and confusing the shell if passed
# through.  Refusing with a file:line error follows the module's documented
# bias: refuse rather than half-imitate.
# Built from explicit code points rather than literal ``x-y`` ranges: a
# range spanning CR (0x0d) reads to static analysers (CodeQL
# py/overly-large-range) as a possible typo, and enumerating the points
# keeps the TAB/LF carve-out visible at the spot the class is defined.
_REFUSED_CONTROLS = (
    # C0 controls, minus TAB (0x09) and LF (0x0a)
    [c for c in range(0x00, 0x20) if c not in (0x09, 0x0A)]
    # DEL and the C1 controls
    + list(range(0x7F, 0xA0))
    # the Unicode line/paragraph separators
    + [0x2028, 0x2029]
)
_CONTROL_CHARS = re.compile(
    "[" + "".join(chr(c) for c in _REFUSED_CONTROLS) + "]"
)


def _physical_lines(data: str) -> List[str]:
    """``data`` split into physical lines, exactly as cron and editors do.

    A crontab is LF-delimited (``man 5 crontab``; cron splits on LF only),
    so this splits on ``"\\n"`` alone and strips one trailing ``"\\r"`` (a
    CRLF file read in binary mode).  ``str.splitlines()`` is deliberately
    NOT used: it additionally breaks on VT, FF, FS/GS/RS, NEL and
    U+2028/U+2029, which turned text sitting inside a ``#`` comment into a
    live job, silently truncated commands, and made every reported line
    number drift from the file's physical lines.
    """
    return [
        line[:-1] if line.endswith("\r") else line for line in data.split("\n")
    ]


class CrontabError(ValueError):
    """A classic crontab could not be parsed (message carries file:line).

    Deliberately not ``config.ConfigError`` -- importing that here would
    be circular.  The config module catches this and re-raises it as a
    ConfigError, so callers outside the parsing pipeline never see it.
    """


def is_crontab_path(path: str) -> bool:
    """Whether ``path``'s name alone marks it as a classic crontab."""
    name = os.path.basename(path).lower()
    return (
        name == CRONTAB_BASENAME
        or os.path.splitext(name)[1] in CRONTAB_EXTENSIONS
    )


def looks_like_crontab(data: str) -> bool:
    """Best-effort content sniff for files whose name decides nothing.

    Judges only the FIRST meaningful (non-blank, non-comment) line, and
    only accepts shapes no valid cronstable YAML document can open with: a
    ``NAME=value`` assignment (a YAML config opens with ``key:``), a
    leading ``@`` (a reserved YAML indicator, so no plain YAML scalar can
    start with it), or five valid cron fields followed by a command.
    Anything inconclusive is NOT a crontab, so an extensionless YAML file
    keeps its exact pre-crontab-support behavior and error messages.
    Lines are the same physical (LF-delimited) lines :func:`parse_crontab`
    reads, so the sniff and the parser can never judge different "first"
    lines of one file.
    """
    for line in _physical_lines(data):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if _ENV_LINE.match(line) or line.startswith("@"):
            return True
        fields = line.split(None, 5)
        if len(fields) < 6:
            return False
        try:
            # hash_key="": this is a content sniff, not a real job, and an
            # H field must still read as "valid cron fields" here.
            CronTab(" ".join(fields[:5]), hash_key="")
        except ValueError:
            return False
        return True
    return False


def parse_crontab(data: str, path: str) -> List[Dict[str, Any]]:
    """Lower classic crontab text into YAML-equivalent job dictionaries.

    Each returned dict is shaped exactly like one ``jobs:`` entry out of
    the YAML front end (name/command/schedule strings, plus environment /
    shell / timezone when the crontab sets them), ready for the standard
    defaults merge -- so a crontab job internally gets cronstable's standard
    configuration, not an emulation of cron's.

    Job names are ``<file name>:<line number>`` (``legacy.crontab:7``):
    unique within a file, stable across reloads while the file is
    unchanged, and traceable straight back to the source line.  Editing
    the file can renumber them, exactly as renaming a YAML job would.
    Lines are physical LF-delimited lines (:func:`_physical_lines`), so
    the numbers match the editor's and a ``#`` line is a comment for its
    whole length.

    :raise CrontabError: on the first unparsable line (or the first live
        line carrying a control character), with a ``path:line`` prefix.
    """
    label = os.path.basename(path) or CRONTAB_BASENAME
    environment: Dict[str, str] = {}
    jobs: List[Dict[str, Any]] = []
    for lineno, raw in enumerate(_physical_lines(data), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        where = "{}:{}".format(path or label, lineno)
        control = _CONTROL_CHARS.search(line)
        if control is not None:
            raise CrontabError(
                "{}: control character {!r} in a crontab line: it cannot "
                "be part of a schedule, command or environment value (a "
                "NUL is unspawnable, and the others read as line breaks "
                "in some tools but not others). Remove it, or move this "
                "job to a YAML config.".format(where, control.group())
            )
        assignment = _ENV_LINE.match(line)
        if assignment is not None:
            value = _unquote(assignment.group("value"))
            if assignment.group("name") == "CRON_TZ":
                # Checked at the assignment so the error carries the
                # file:line of the typo, not of some job below it.
                _check_timezone(value, where)
            environment[assignment.group("name")] = value
            continue
        jobs.append(_job_from_line(line, where, label, lineno, environment))
    return jobs


def _job_from_line(
    line: str,
    where: str,
    label: str,
    lineno: int,
    environment: Dict[str, str],
) -> Dict[str, Any]:
    if line.startswith("@"):
        parts = line.split(None, 1)
        nickname = parts[0]
        if nickname not in _NICKNAMES:
            raise CrontabError(
                "{}: unknown schedule nickname {!r}; supported: {}".format(
                    where, nickname, ", ".join(sorted(_NICKNAMES))
                )
            )
        schedule = "@daily" if nickname == "@midnight" else nickname
        command = parts[1] if len(parts) == 2 else ""
    else:
        fields = line.split(None, 5)
        if len(fields) < 6:
            raise CrontabError(
                "{}: not a valid crontab line: expected 'minute hour "
                "day-of-month month day-of-week command', an @nickname "
                "entry, or a NAME=value assignment; got {!r}".format(
                    where, line
                )
            )
        schedule = " ".join(fields[:5])
        command = fields[5]
    if not command:
        raise CrontabError(
            "{}: schedule {!r} has no command".format(where, schedule)
        )
    name = "{}:{}".format(label, lineno)
    if schedule != "@reboot":
        # Validate here so a bad field is reported with its file and line;
        # JobConfig parses the same string again later, but anonymously.
        # The job name seeds the H hash form, exactly as JobConfig will;
        # note these names embed the LINE number, so inserting lines above
        # an H entry re-hashes its slot along with renaming it.
        try:
            CronTab(schedule, hash_key=name)
        except ValueError as ex:
            raise CrontabError(
                "{}: invalid schedule {!r}: {}".format(where, schedule, ex)
            ) from ex
    job: Dict[str, Any] = {
        "name": name,
        "schedule": schedule,
        "command": _unescape_percent(command, where),
    }
    if environment:
        # Snapshot: assignments apply to the entries below them, so a
        # later reassignment must not leak back into this job.
        job["environment"] = [
            {"key": key, "value": value} for key, value in environment.items()
        ]
        # cron runs the command through the SHELL in scope; map it onto
        # the job's shell setting (it stays exported too, as in cron).
        if environment.get("SHELL"):
            job["shell"] = environment["SHELL"]
        # cronie's CRON_TZ: evaluate the schedule in this zone.  Without
        # it a crontab job keeps cronstable's standard default (UTC) -- NOT
        # cron's localtime; a deliberate, documented deviation.
        if environment.get("CRON_TZ"):
            job["timezone"] = environment["CRON_TZ"]
    return job


def _unquote(value: str) -> str:
    # man 5 crontab: one pair of matching outer quotes may wrap the value
    # (they exist to preserve leading/trailing blanks); strip exactly that.
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
        return value[1:-1]
    return value


def _check_timezone(value: str, where: str) -> None:
    # Mirrors JobConfig._resolve_timezone's acceptance rule.
    try:
        ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError) as ex:
        raise CrontabError(
            "{}: CRON_TZ: unknown timezone {!r}".format(where, value)
        ) from ex


def _unescape_percent(command: str, where: str) -> str:
    r"""Resolve the crontab ``%`` rules for a command.

    In ``man 5 crontab`` an unescaped ``%`` ends the command: it and the
    text after it become the command's standard input, with further ``%``
    signs acting as newlines.  cronstable does not feed stdin to jobs, and
    the two silent alternatives are both worse than refusing -- running
    the command without the input it expects, or leaving the input text
    on the command line for the shell to execute.  So an unescaped ``%``
    is a hard error with advice, while the escaped form ``\%`` --
    overwhelmingly the common case, e.g. ``date +\%F`` -- becomes a
    literal ``%`` exactly as cron would make it.
    """
    out: List[str] = []
    index = 0
    while index < len(command):
        char = command[index]
        if (
            char == "\\"
            and index + 1 < len(command)
            and command[index + 1] == "%"
        ):
            out.append("%")
            index += 2
            continue
        if char == "%":
            raise CrontabError(
                "{}: unescaped '%' in command: cron would treat the rest "
                "of the line as the command's standard input, which "
                "cronstable does not emulate. Escape it as \\% for a literal "
                "percent (e.g. date +\\%F), or move this job to a YAML "
                "config to use stdin redirection instead.".format(where)
            )
        out.append(char)
        index += 1
    return "".join(out)
