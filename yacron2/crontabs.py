r"""Classic (Vixie-style) crontab support.

yacron2's native configuration is YAML, but plenty of perfectly good
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

Everything *around* the schedule deliberately gets yacron2's standard
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

from yacron2.cronexpr import CronTab

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
    only accepts shapes no valid yacron2 YAML document can open with: a
    ``NAME=value`` assignment (a YAML config opens with ``key:``), a
    leading ``@`` (a reserved YAML indicator, so no plain YAML scalar can
    start with it), or five valid cron fields followed by a command.
    Anything inconclusive is NOT a crontab, so an extensionless YAML file
    keeps its exact pre-crontab-support behavior and error messages.
    """
    for line in data.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if _ENV_LINE.match(line) or line.startswith("@"):
            return True
        fields = line.split(None, 5)
        if len(fields) < 6:
            return False
        try:
            CronTab(" ".join(fields[:5]))
        except ValueError:
            return False
        return True
    return False


def parse_crontab(data: str, path: str) -> List[Dict[str, Any]]:
    """Lower classic crontab text into YAML-equivalent job dictionaries.

    Each returned dict is shaped exactly like one ``jobs:`` entry out of
    the YAML front end (name/command/schedule strings, plus environment /
    shell / timezone when the crontab sets them), ready for the standard
    defaults merge -- so a crontab job internally gets yacron2's standard
    configuration, not an emulation of cron's.

    Job names are ``<file name>:<line number>`` (``legacy.crontab:7``):
    unique within a file, stable across reloads while the file is
    unchanged, and traceable straight back to the source line.  Editing
    the file can renumber them, exactly as renaming a YAML job would.

    :raise CrontabError: on the first unparsable line, with a
        ``path:line`` prefix.
    """
    label = os.path.basename(path) or CRONTAB_BASENAME
    environment: Dict[str, str] = {}
    jobs: List[Dict[str, Any]] = []
    for lineno, raw in enumerate(data.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        where = "{}:{}".format(path or label, lineno)
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
    if schedule != "@reboot":
        # Validate here so a bad field is reported with its file and line;
        # JobConfig parses the same string again later, but anonymously.
        try:
            CronTab(schedule)
        except ValueError as ex:
            raise CrontabError(
                "{}: invalid schedule {!r}: {}".format(where, schedule, ex)
            ) from ex
    job: Dict[str, Any] = {
        "name": "{}:{}".format(label, lineno),
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
        # it a crontab job keeps yacron2's standard default (UTC) -- NOT
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
    signs acting as newlines.  yacron2 does not feed stdin to jobs, and
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
                "yacron2 does not emulate. Escape it as \\% for a literal "
                "percent (e.g. date +\\%F), or move this job to a YAML "
                "config to use stdin redirection instead.".format(where)
            )
        out.append(char)
        index += 1
    return "".join(out)
