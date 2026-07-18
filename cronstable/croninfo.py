"""Human-facing schedule intelligence, shared by every surface.

Plain-English descriptions (:func:`describe_cron`), fire previews
(:func:`next_fires`) and the advisory schedule linter
(:func:`lint_schedule`) in one importable module, so the TUI, the daemon's
``GET /schedule/preview`` endpoint and any future MCP tool all agree with
the engine that actually schedules (:mod:`cronstable.cronexpr`) instead of
re-implementing the arithmetic.  The TUI re-exports the names it always
had, so ``cronstable.tui.describe_cron`` keeps working.

The describers are deliberately tolerant: text the engine rejects degrades
to a "Custom schedule" phrase rather than raising, because the TUI renders
them while the user is still typing.  The linter is the opposite: it
assumes the expression already parses and reports advisory
:class:`Finding` rows for legal schedules that probably do not mean what
they say (level ``"warning"``) or behave in a way worth knowing about
(level ``"note"``).  Config loading logs the findings per job and the
status payloads carry them to the dashboards.

:func:`why_no_run` is the linter's sibling for one instant instead of
the whole schedule: it decomposes the engine's own :meth:`CronTab.test`
into a per-field verdict ("minute matched; day-of-week Tuesday is not in
Monday and Friday"), so "why didn't it run at 09:00?" gets answered from
ground truth.  The daemon serves it per job as ``GET /schedule/why`` and
the MCP server as the ``cron_why_no_run`` tool.

The fleet-level analyzers live here too, one :class:`ScheduleEntry` row
per scheduled job: :func:`schedule_pressure` (every fire over the next
24 hours, bucketed into an hour by minute collision grid),
:func:`duplicate_schedules` (groups of jobs whose schedules fire on the
identical instants, via the engine's semantic equality) and
:func:`suggest_slot` (the least-loaded minute or hour:minute for a new
job).  The daemon serves them as ``GET /schedule/pressure``,
``/schedule/duplicates`` and ``/schedule/suggest``; the TUI computes the
same payloads locally from its ``/jobs`` snapshot.
"""

import calendar
import datetime
import itertools
import re
from collections import Counter
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from cronstable.cronexpr import CronTab

__all__ = [
    "Finding",
    "ScheduleEntry",
    "describe_cron",
    "duplicate_schedules",
    "lint_schedule",
    "next_fires",
    "pad2",
    "schedule_pressure",
    "suggest_slot",
    "why_no_run",
]


def pad2(n: int) -> str:
    return "%02d" % n


# ===================================================================
#  plain-English descriptions (ports of the web page's describeCron)
# ===================================================================
_MONTHS = [
    "",
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]
_DOWN = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
]
_MACROS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}
_MACRO_TEXT = {
    "@yearly": "Once a year, at midnight on 1 January",
    "@annually": "Once a year, at midnight on 1 January",
    "@monthly": "At midnight on the 1st of every month",
    "@weekly": "At midnight every Sunday",
    "@daily": "Every day at midnight",
    "@midnight": "Every day at midnight",
    "@hourly": "Every hour, on the hour",
}


def _ordinal(n: int) -> str:
    suffix = ["th", "st", "nd", "rd"]
    v = n % 100
    if 20 <= v or v < 10:
        return "%d%s" % (n, suffix[v % 10] if v % 10 < 4 else "th")
    return "%dth" % n


def _list_join(items: Sequence[str]) -> str:
    parts = list(items)
    if len(parts) <= 1:
        return "".join(parts)
    if len(parts) == 2:
        return "%s and %s" % (parts[0], parts[1])
    return "%s and %s" % (", ".join(parts[:-1]), parts[-1])


def _field_values(
    spec: str, lo: int, hi: int, names: Optional[Dict[str, int]] = None
) -> Optional[List[int]]:
    """Enumerate a cron field, or ``None`` for an unrestricted ``*``/``?``.

    A tolerant re-implementation of the web page's ``parseField`` (kept
    here rather than reaching into :class:`CronTab` internals so malformed
    input degrades to prose instead of raising).
    """
    spec = spec.strip().lower()
    if spec in ("*", "?"):
        return None
    out: Set[int] = set()
    for part in spec.split(","):
        body, step = part, 1
        if "/" in part:
            body, step_text = part.split("/", 1)
            if not step_text.isdigit() or int(step_text) < 1:
                raise ValueError("bad step: %s" % part)
            step = int(step_text)

        def resolve(token: str) -> Optional[int]:
            token = token.strip().lower()
            if names and token in names:
                return names[token]
            return int(token) if token.isdigit() else None

        if body == "*":
            start, end = lo, hi
        elif "-" in body:
            a, b = body.split("-", 1)
            start_v, end_v = resolve(a), resolve(b)
            if start_v is None or end_v is None:
                raise ValueError("bad field: %s" % part)
            if hi == 6 and end_v == 0:
                # a day-of-week range ending in 0 reads its end as
                # Sunday-as-7, unconditionally, exactly like the engine
                # (sat-sun works; 0-0 is every day, a preserved quirk),
                # so the prose cannot claim "Sunday" for a daily schedule
                end_v = 7
            start, end = start_v, end_v
        else:
            v = resolve(body)
            if v is None:
                raise ValueError("bad field: %s" % part)
            start, end = v, (hi if "/" in part else v)
        values: List[int]
        if start <= end:
            values = list(range(start, end + 1, step))
        else:  # wrap-around range, e.g. fri-mon
            values = list(range(start, hi + 1, step)) + list(
                range(lo, end + 1, step)
            )
        for v in values:
            v = 0 if (hi == 6 and v == 7) else v
            if v < lo or v > hi:
                # out-of-range values (month 13, dow 8, minute 60) would
                # index past the name tables below; degrade to prose.
                raise ValueError("out of range: %s" % part)
            out.add(v)
    return sorted(out)


_MON_NAMES = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}
_DOW_NAMES = {
    "sun": 0,
    "mon": 1,
    "tue": 2,
    "wed": 3,
    "thu": 4,
    "fri": 5,
    "sat": 6,
}


def _finish_split(
    spec: str, plain: List[str], phrases: List[str]
) -> Tuple[str, List[str]]:
    """Shared epilogue of the two special-form splitters below.

    Rejects splits :func:`describe_cron` cannot phrase honestly, and
    dedupes repeated phrases (``L0-7`` folds both ends to Sunday; the
    engine's sets dedupe, so the prose must too).
    """
    if phrases:
        if any(p in ("*", "?") for p in plain):
            # "*,L" (or a star hiding among values, "1,*,L") is legal but
            # the star already covers every day; the phrases would
            # overstate the restriction, so punt to Custom
            raise ValueError("special forms beside a star: %s" % spec)
    elif not plain:
        raise ValueError("no usable items: %s" % spec)
    deduped: List[str] = []
    for phrase in phrases:
        if phrase not in deduped:
            deduped.append(phrase)
    return ",".join(plain), deduped


def _split_special_dom(spec: str) -> Tuple[str, List[str]]:
    """Partition a day-of-month field into plain items and L/W phrases.

    Returns the comma-joined plain items (possibly empty when only
    special forms remain) and one prose phrase per ``L``/``L-n``/``nW``/
    ``LW`` item, shaped to read after "on ".  Malformed or pointless
    forms raise, so :func:`describe_cron` degrades to its Custom line,
    matching the engine's own rejections.
    """
    plain: List[str] = []
    phrases: List[str] = []
    for item in spec.strip().lower().split(","):
        if item == "l":
            phrases.append("the last day of the month")
        elif item == "lw":
            phrases.append("the last weekday of the month")
        elif item.startswith("l-"):
            offset_text = item[2:]
            if not offset_text.isdigit() or not 1 <= int(offset_text) <= 30:
                raise ValueError("bad L- offset: %s" % item)
            offset = int(offset_text)
            # "N days before the last day", never an ordinal: "3rd-to-last"
            # invites an off-by-one reading (is the last day itself the
            # 1st-to-last?), and this phrase cannot be miscounted
            phrases.append(
                "the day before the last day of the month"
                if offset == 1
                else "%d days before the last day of the month" % offset
            )
        elif len(item) > 1 and item.endswith("w") and item[:-1].isdigit():
            day = int(item[:-1])
            if not 1 <= day <= 31:
                raise ValueError("bad W day: %s" % item)
            phrases.append("the weekday nearest the %s" % _ordinal(day))
        else:
            plain.append(item)
    return _finish_split(spec, plain, phrases)


def _split_special_dow(spec: str) -> Tuple[str, List[str]]:
    """Partition a day-of-week field into plain items and L/# phrases.

    The mirror of :func:`_split_special_dom` for ``L<n>`` (and its
    range form) and ``<d>#<n>`` items.
    """
    plain: List[str] = []
    phrases: List[str] = []
    for item in spec.strip().lower().split(","):
        if item.startswith("l") and len(item) > 1:
            a, dash, b = item[1:].partition("-")
            if not a.isdigit() or (dash and not b.isdigit()):
                raise ValueError("bad L day-of-week: %s" % item)
            lo, hi = int(a), int(b) if dash else int(a)
            if not (0 <= lo <= 7 and 0 <= hi <= 7):
                raise ValueError("out of range: %s" % item)
            phrases.extend(
                "the last %s of the month" % _DOWN[d % 7]
                for d in range(lo, hi + 1)
            )
        elif "#" in item:
            day_text, _, nth_text = item.partition("#")
            if day_text in _DOW_NAMES:
                day = _DOW_NAMES[day_text]
            elif day_text.isdigit() and 0 <= int(day_text) <= 7:
                day = int(day_text) % 7
            else:
                raise ValueError("bad '#' weekday: %s" % item)
            if not nth_text.isdigit() or not 1 <= int(nth_text) <= 5:
                raise ValueError("bad '#' ordinal: %s" % item)
            phrases.append(
                "the %s %s of the month"
                % (_ordinal(int(nth_text)), _DOWN[day])
            )
        else:
            plain.append(item)
    return _finish_split(spec, plain, phrases)


#: cheap gate for "does this expression use the H hash form?": an ``h``
#: opening an item (start of a field or after a comma) followed by one of
#: the delimiters an H item can continue with.  Weekday and month names
#: never OPEN with an h, so this cannot fire on ``thu``.
_HASH_HINT = re.compile(r"(?i)(?:^|[\s,])h(?:[\s,/(]|$)")


def describe_cron(expr: str, hash_key: Optional[str] = None) -> str:
    """Plain-English schedule text, a port of the web ``describeCron``.

    Handles the 5-field core plus the 6-/7-field (year / second) forms the
    daemon accepts; anything it cannot phrase degrades to ``Custom
    schedule: <expr>`` rather than raising.  With a ``hash_key`` (the job
    name), Jenkins-style ``H`` items are resolved through the engine and
    the prose describes the hashed slot, marked as such.
    """
    low = (expr or "").strip().lower()
    if low == "@reboot":
        return "Once, when cronstable starts (@reboot)"
    if low in _MACRO_TEXT:
        return _MACRO_TEXT[low]
    if hash_key is not None and _HASH_HINT.search(expr or ""):
        try:
            tab = CronTab(expr, hash_key=hash_key)
        except (ValueError, KeyError):
            return "Custom schedule: %s" % expr
        if tab.resolved_source != str(tab):
            return "%s (H slots hashed from the job name)" % describe_cron(
                tab.resolved_source
            )
    fields = _MACROS.get(low, expr).split()
    try:
        sec_spec, year_spec = "0", "*"
        if len(fields) == 5:
            core = fields
        elif len(fields) == 6:
            core, year_spec = fields[:5], fields[5]
        elif len(fields) == 7:
            sec_spec, core, year_spec = fields[0], fields[1:6], fields[6]
        else:
            return "Custom schedule: %s" % expr
        mi, hr, dom, mon, dow = core
        dom_plain, dom_phrases = _split_special_dom(dom)
        dow_plain, dow_phrases = _split_special_dow(dow)
        minutes = _field_values(mi, 0, 59)
        hours = _field_values(hr, 0, 23)
        # an empty plain remainder means the field held ONLY special
        # forms: restricted, but with no plain values to enumerate
        doms = _field_values(dom_plain, 1, 31) if dom_plain else []
        months = _field_values(mon, 1, 12, _MON_NAMES)
        dows = _field_values(dow_plain, 0, 6, _DOW_NAMES) if dow_plain else []
        seconds = _field_values(sec_spec, 0, 59)
        years = (
            _field_values(year_spec, 1970, 2099) if year_spec != "*" else None
        )
    except (ValueError, KeyError):
        return "Custom schedule: %s" % expr

    time_part = _describe_time(mi, hr, minutes, hours)

    day_clauses = []
    if dows or dow_phrases:
        day_clauses.append(
            "on " + _list_join([_DOWN[d] for d in dows or []] + dow_phrases)
        )
    if doms or dom_phrases:
        if dom_phrases:
            day_clauses.append(
                "on "
                + _list_join(
                    ["the " + _ordinal(d) for d in doms or []] + dom_phrases
                )
            )
        else:
            day_clauses.append(
                "on the "
                + _list_join([_ordinal(d) for d in doms or []])
                + (" of the month" if not (dows or dow_phrases) else "")
            )
    clauses = []
    if len(day_clauses) == 2:
        # dom and dow must BOTH match when both are restricted: the
        # daemon's engine (cronexpr._day_matches) deliberately keeps
        # parse-crontab's AND rule ("0 0 13 * 5" is Friday the 13th),
        # unlike std cron's OR, so the prose must say so too.
        clauses.append("%s, and only %s" % (day_clauses[1], day_clauses[0]))
    elif day_clauses:
        clauses.append(day_clauses[0])
    if months is not None:
        clauses.append("in " + _list_join([_MONTHS[m] for m in months]))
    elif not day_clauses:
        clauses.append("every day")
    if years is not None:
        clauses.append("in " + _list_join([str(y) for y in years]))
    base = ", ".join([time_part] + clauses)

    if seconds != [0] and len(fields) == 7:
        top_free = (
            minutes is None
            and hours is None
            and doms is None
            and months is None
            and dows is None
            and years is None
        )
        return _describe_seconds(sec_spec, seconds, base, top_free)
    return base


def _describe_time(
    mi: str,
    hr: str,
    minutes: Optional[List[int]],
    hours: Optional[List[int]],
) -> str:
    """The leading time-of-day phrase of :func:`describe_cron`."""
    step_m = re.match(r"^\*/(\d+)$", mi)
    step_h = re.match(r"^\*/(\d+)$", hr)
    # "*/n" only reads as a true fixed interval when n divides the span;
    # otherwise the pre-boundary gap is shorter, so enumerate instead.
    step_m_ok = step_m is not None and 60 % int(step_m.group(1)) == 0
    step_h_ok = step_h is not None and 24 % int(step_h.group(1)) == 0
    if minutes is None and hours is None:
        return "Every minute"
    if step_m_ok and hours is None:
        assert step_m is not None
        return "Every %s minutes" % step_m.group(1)
    if minutes is None and step_h_ok:
        assert step_h is not None
        return "Every minute, every %s hours" % step_h.group(1)
    if minutes is not None and hours is None and mi.isdigit():
        return "Every hour at :%s" % pad2(int(mi))
    if step_h_ok and mi.isdigit():
        assert step_h is not None
        return "At :%s every %s hours" % (pad2(int(mi)), step_h.group(1))
    if mi.isdigit() and hr.isdigit():
        return "At %s:%s" % (pad2(int(hr)), pad2(int(mi)))
    mp = (
        "every minute"
        if minutes is None
        else "minute%s %s"
        % (
            "s" if len(minutes) > 1 else "",
            ", ".join(pad2(x) for x in minutes),
        )
    )
    hp = (
        "every hour"
        if hours is None
        else "hour%s %s"
        % ("s" if len(hours) > 1 else "", ", ".join(pad2(x) for x in hours))
    )
    return "At %s past %s" % (mp, hp)


def _describe_seconds(
    sec_spec: str,
    seconds: Optional[List[int]],
    base: str,
    top_free: bool,
) -> str:
    """The seconds clause of :func:`describe_cron` (7-field forms).

    A standalone cadence phrase ("Every N seconds") is only true when
    nothing above the seconds column is restricted; otherwise the seconds
    merely sub-select within the matched minutes, so they append as a
    qualifying clause instead of overstating the frequency.
    """
    step_s = re.match(r"^\*/(\d+)$", sec_spec)
    step_s_ok = step_s is not None and 60 % int(step_s.group(1)) == 0
    if top_free:
        if seconds is None:
            return "Every second"
        if step_s_ok:
            assert step_s is not None
            return "Every %s seconds" % step_s.group(1)
        return "At second%s %s" % (
            "s" if len(seconds) > 1 else "",
            ", ".join(pad2(x) for x in seconds),
        )
    if seconds is None:
        return base + ", every second"
    return base + ", at second%s %s" % (
        "s" if len(seconds) > 1 else "",
        ", ".join(pad2(x) for x in seconds),
    )


# ===================================================================
#  fire previews
# ===================================================================
def next_fires(
    schedule: str,
    count: int,
    tz: Optional[datetime.tzinfo] = None,
    start: Optional[datetime.datetime] = None,
    hash_key: Optional[str] = None,
) -> List[datetime.datetime]:
    """The next ``count`` fire times of a schedule, straight from the
    daemon's own engine (:meth:`CronTab.occurrences`), so the preview
    always agrees with what the scheduler will actually do.  Returns
    ``[]`` for @reboot, for an expression the engine rejects, and for a
    schedule with no remaining occurrence.  ``tz`` picks the frame when
    ``start`` is omitted (UTC by default); with an aware start the
    returned datetimes are aware in that frame.  ``hash_key`` (the job
    name) resolves ``H`` items; without it an ``H`` schedule previews as
    ``[]``, like any other text the engine rejects.
    """
    text = (schedule or "").strip()
    if text.lower() == "@reboot":
        return []
    try:
        tab = CronTab(text, hash_key=hash_key)
    except (ValueError, KeyError):
        return []
    zone = tz or datetime.timezone.utc
    current = start if start is not None else datetime.datetime.now(zone)
    return list(itertools.islice(tab.occurrences(current), count))


# ===================================================================
#  the advisory schedule linter
# ===================================================================
class Finding(NamedTuple):
    """One advisory lint result for a schedule."""

    #: stable machine identifier, kebab-case (dashboards key styling on it)
    code: str
    #: ``"warning"`` (probable mistake) or ``"note"`` (behaviour worth
    #: knowing about)
    level: str
    #: one line of plain text, self-contained enough for a log line
    message: str


LEVEL_WARNING = "warning"
LEVEL_NOTE = "note"

_FULL_DOM = frozenset(range(1, 32))
_FULL_DOW = frozenset(range(7))
#: spans for the uneven-step rule: values wrap modulo the span, so a star
#: step that does not divide it leaves one short interval at the wrap.
#: Day-of-month is handled separately (month lengths vary) and the year
#: column does not wrap at all.
_STEP_SPANS = {
    "second": (60, "seconds"),
    "minute": (60, "minutes"),
    "hour": (24, "hours"),
    "month": (12, "months"),
    "day-of-week": (7, "days"),
}
#: the longest length each month can have (February counts its leap 29th)
_MONTH_MAX = {
    1: 31,
    2: 29,
    3: 31,
    4: 30,
    5: 31,
    6: 30,
    7: 31,
    8: 31,
    9: 30,
    10: 31,
    11: 30,
    12: 31,
}


def lint_schedule(
    expression: str,
    timezone: Optional[datetime.tzinfo] = None,
    now: Optional[datetime.datetime] = None,
    hash_key: Optional[str] = None,
) -> List[Finding]:
    """Advisory findings for a schedule the engine accepts.

    Returns ``[]`` for ``@reboot`` and for text that does not parse:
    rejecting bad syntax is the parser's job, the linter only flags legal
    schedules that probably do not mean what the author thinks.
    ``timezone`` is the job's resolved zone and enables the DST checks
    (skipped when ``None``, since the daemon's local zone rules are not
    knowable here, and for fixed-offset zones, which never transition).
    ``now`` fixes the reference instant for determinism in tests; it
    defaults to the current time in ``timezone`` (or UTC).  ``hash_key``
    (the job name) resolves ``H`` items; a schedule that uses them gains
    a note naming the concrete slots they hashed to.
    """
    text = (expression or "").strip()
    if text.lower() == "@reboot":
        return []
    try:
        tab = CronTab(text, hash_key=hash_key)
    except (ValueError, KeyError):
        return []
    if now is None:
        now = datetime.datetime.now(timezone or datetime.timezone.utc)
    findings: List[Finding] = []
    if tab.resolved_source != str(tab):
        findings.append(
            Finding(
                "hashed-slot",
                LEVEL_NOTE,
                "'H' resolves to '{}' for this job: the slot is a stable "
                "hash of the job name, so it survives restarts and "
                "reloads, but renaming the job re-hashes it".format(
                    tab.resolved_source
                ),
            )
        )
    dead = tab.next(now=now, default_utc=True) is None
    if dead:
        findings.append(
            Finding("never-fires", LEVEL_WARNING, _never_fires_message(tab))
        )
    findings.extend(_lint_day_fields(tab))
    findings.extend(_lint_steps(text))
    if not dead:
        # pointless refinements of "it never fires at all"
        findings.extend(_lint_month_lengths(tab))
        if timezone is not None:
            findings.extend(_lint_dst(tab, timezone, now))
    return findings


def _never_fires_message(tab: CronTab) -> str:
    years = tab.years
    if years is not None:
        # Blame the year column only when it is actually the culprit: a
        # probe from before the year floor tells an exhausted column ("0 0
        # 1 1 * 2020" did fire once) apart from a date that never exists
        # in ANY listed year ("0 0 30 2 * 2099" is dead because February
        # 30 is, not because 2099 is).
        rewound = tab.next(
            now=datetime.datetime(1969, 12, 31), default_utc=True
        )
        if rewound is not None:
            return (
                "no future occurrence: the year column ends at {}, so this "
                "schedule will never fire again".format(max(years))
            )
    return (
        "no future occurrence: the day, month and weekday fields never all "
        "line up on a real date, so this schedule will never fire"
    )


def _lint_day_fields(tab: CronTab) -> List[Finding]:
    """Both day fields restricted: the AND-semantics footgun.

    This dialect requires a day to satisfy BOTH fields (deliberately, see
    cronexpr), while classic Vixie cron fires when EITHER matches, so a
    schedule imported from a system crontab fires less often than it did
    there.  Say so whenever the combination appears.
    """
    # a field whose plain values already cover the whole range matches
    # every day whatever else (an L or W form) rides along, so only the
    # subset test decides restriction; a field of only special forms
    # leaves the plain set empty, which the same test correctly reads
    # as restricted.
    dom_restricted = not (_FULL_DOM <= tab.days_of_month)
    dow_restricted = not (_FULL_DOW <= tab.days_of_week)
    if dom_restricted and dow_restricted:
        return [
            Finding(
                "day-fields-both-restricted",
                LEVEL_WARNING,
                "day-of-month and day-of-week are both restricted, and a "
                "day must satisfy BOTH here ('0 0 13 * 5' is Friday the "
                "13th); classic Vixie cron fires when either field "
                "matches, so a schedule imported from a system crontab "
                "fires less often than it did there",
            )
        ]
    return []


def _lint_steps(expression: str) -> List[Finding]:
    """Star steps that do not divide their field's span run unevenly.

    ``*/7`` in the minute field fires at :56 and then :00 four minutes
    later, because the values restart at the wrap.  Only ``*/n`` and the
    hashed ``H/n`` (which spans the same full field) are flagged: an
    explicit range with a step reads as deliberate.  Day-of-month gets
    its own note (steps restart at day 1 each month and month lengths
    differ), and the year column never wraps.
    """
    low = expression.strip().lower()
    fields = _MACROS.get(low, low).split()
    labels: Sequence[str]
    if len(fields) == 7:
        labels = (
            "second",
            "minute",
            "hour",
            "day-of-month",
            "month",
            "day-of-week",
        )
    else:
        # 5 fields, or 6 where the extra trailing column is the year;
        # zip() drops it either way
        labels = ("minute", "hour", "day-of-month", "month", "day-of-week")
    findings: List[Finding] = []
    # 6-field forms have one more field (the year) than labels; the year
    # column never wraps, so non-strict zip dropping it is the point
    for label, field in zip(labels, fields, strict=False):
        for item in field.split(","):
            head, slash, step_text = item.partition("/")
            if not slash or head not in ("*", "h") or not step_text.isdigit():
                continue
            step = int(step_text)
            if step <= 1:
                continue
            if label == "day-of-month":
                findings.append(
                    Finding(
                        "uneven-step",
                        LEVEL_NOTE,
                        "'{}' in the day-of-month field restarts at day 1 "
                        "every month, and month lengths differ, so the "
                        "interval between runs varies at month "
                        "boundaries".format(item),
                    )
                )
                continue
            span, unit = _STEP_SPANS[label]
            if span % step:
                gap = span - ((span - 1) // step) * step
                findings.append(
                    Finding(
                        "uneven-step",
                        LEVEL_WARNING,
                        "'{}' in the {} field: {} does not divide the "
                        "field's span of {}, so one interval at the wrap "
                        "is only {} {}".format(
                            item,
                            label,
                            step,
                            span,
                            gap,
                            unit if gap != 1 else unit[:-1],
                        ),
                    )
                )
    return findings


def _lint_month_lengths(tab: CronTab) -> List[Finding]:
    """Selected days that no selected month is long enough to reach.

    Plain days and ``nW`` targets miss any month shorter than the day
    they name; an ``L-n`` offset misses any month whose final day it
    counts back past (``L-30`` reaches day 1 only in 31-day months).
    A bare ``L`` and ``LW`` land in every month, so they exempt the
    whole check.
    """
    if tab.last_day_of_month or tab.last_weekday_of_month:
        return []
    dom = tab.days_of_month
    day_like = dom | tab.nearest_weekday_days
    offsets = tab.last_day_offsets
    if (not day_like and not offsets) or _FULL_DOM <= dom:
        return []
    findings: List[Finding] = []
    dmin = min(day_like) if day_like else None
    omin = min(offsets) if offsets else None

    def reachable(month: int) -> bool:
        longest = _MONTH_MAX[month]
        if dmin is not None and dmin <= longest:
            return True
        return omin is not None and omin <= longest - 1

    skipped = [m for m in sorted(tab.months) if not reachable(m)]
    if skipped:
        reasons = []
        if dmin is not None:
            reasons.append(
                "the smallest selected day of month is {}".format(dmin)
            )
        if omin is not None:
            reasons.append(
                "the smallest 'L-' offset counts back {} days".format(omin)
            )
        findings.append(
            Finding(
                "skipped-months",
                LEVEL_WARNING,
                "{}, which never lands in {}; {} skipped entirely".format(
                    " and ".join(reasons),
                    _list_join([_MONTHS[m] for m in skipped]),
                    "that month is"
                    if len(skipped) == 1
                    else "those months are",
                ),
            )
        )
    if 2 in tab.months and 2 not in skipped:
        # fires in a leap February but never a common one: day 29, and
        # L-28 (which needs a 29th to count back from)
        fires_common = (dmin is not None and dmin <= 28) or (
            omin is not None and omin <= 27
        )
        if not fires_common:
            findings.append(
                Finding(
                    "leap-day-only",
                    LEVEL_NOTE,
                    "in February only the leap 29th makes the selected "
                    "days land, so February runs occur only in leap "
                    "years",
                )
            )
    return findings


def _lint_dst(
    tab: CronTab,
    timezone: datetime.tzinfo,
    now: datetime.datetime,
) -> List[Finding]:
    """DST transition notes for schedules with restricted hours.

    Scans the coming year for utcoffset changes in the zone; for each
    transition, reports the first scheduled wall time that falls in the
    skipped (nonexistent) or repeated (ambiguous) window.  Schedules with
    unrestricted hours are skipped: they fire right through a transition
    and have no single anomalous wall time worth calling out.
    """
    if len(tab.hours) >= 24:
        return []
    if isinstance(timezone, datetime.timezone):
        return []  # fixed-offset zones (UTC included) never transition
    if now.tzinfo is not None:
        day0 = now.astimezone(timezone).date()
    else:
        day0 = now.date()
    findings: List[Finding] = []
    prev_offset = _offset_at(timezone, day0)
    for i in range(1, 367):
        day = day0 + datetime.timedelta(days=i)
        offset = _offset_at(timezone, day)
        if offset != prev_offset:
            # the offset changed somewhere in the 24h before `day` 00:00;
            # scan both civil dates the window can touch
            finding = _dst_finding(
                tab, timezone, day - datetime.timedelta(days=1)
            )
            if finding is not None:
                findings.append(finding)
                if len(findings) >= 2:
                    break
        prev_offset = offset
    return findings


def _offset_at(
    timezone: datetime.tzinfo, day: datetime.date
) -> Optional[datetime.timedelta]:
    return (
        datetime.datetime.combine(day, datetime.time(0))
        .replace(tzinfo=timezone)
        .utcoffset()
    )


def _dst_finding(
    tab: CronTab, timezone: datetime.tzinfo, first_day: datetime.date
) -> Optional[Finding]:
    """The first scheduled wall time a transition around ``first_day``
    skips or repeats, as a Finding, or ``None`` when the schedule misses
    the anomalous window (or the day fields exclude the date)."""
    second = min(tab.seconds)
    zone_name = str(timezone)
    for offset in (0, 1):
        day = first_day + datetime.timedelta(days=offset)
        # cheap pre-pass: which hours are anomalous at all on this date
        # (probed on the half hour too, for zones with :30 transitions)
        affected: Set[int] = set()
        for hour in range(24):
            for minute in (0, 30):
                civil = datetime.datetime.combine(
                    day, datetime.time(hour, minute)
                )
                if _classify(timezone, civil) is not None:
                    affected.add(hour)
        for hour in sorted(affected & tab.hours):
            for minute in sorted(tab.minutes):
                civil = datetime.datetime.combine(
                    day, datetime.time(hour, minute, second)
                )
                if not tab.test(civil):
                    continue  # day fields or year exclude this date
                kind = _classify(timezone, civil)
                if kind == "gap":
                    return Finding(
                        "dst-skipped-time",
                        LEVEL_NOTE,
                        "on {} the wall time {:02d}:{:02d} does not exist "
                        "in {} (clocks jump forward); that run fires at "
                        "the shifted wall time instead of being "
                        "skipped".format(
                            day.isoformat(), hour, minute, zone_name
                        ),
                    )
                if kind == "fold":
                    return Finding(
                        "dst-repeated-time",
                        LEVEL_NOTE,
                        "on {} the wall time {:02d}:{:02d} occurs twice in "
                        "{} (clocks fall back); the run fires on the "
                        "first occurrence only".format(
                            day.isoformat(), hour, minute, zone_name
                        ),
                    )
    return None


def _classify(
    timezone: datetime.tzinfo, civil: datetime.datetime
) -> Optional[str]:
    """``"gap"`` (nonexistent), ``"fold"`` (ambiguous) or ``None``."""
    off0 = civil.replace(tzinfo=timezone, fold=0).utcoffset()
    off1 = civil.replace(tzinfo=timezone, fold=1).utcoffset()
    if off0 == off1:
        return None
    aware = civil.replace(tzinfo=timezone)
    roundtrip = aware.astimezone(datetime.timezone.utc).astimezone(timezone)
    if roundtrip.replace(tzinfo=None, fold=0) == civil:
        return "fold"
    return "gap"


# ===================================================================
#  the no-run explainer
# ===================================================================
def _value_runs(
    values: Iterable[int], names: Optional[Sequence[str]] = None
) -> List[str]:
    """Sorted values with consecutive runs collapsed to ``a-b`` ranges.

    ``[1, 2, 3, 7]`` becomes ``["1-3", "7"]``; with ``names`` (the
    weekday or month table), ``[1, 2, 3, 4, 5]`` becomes
    ``["Monday-Friday"]``.  Runs of two stay listed ("1", "2"), a range
    of two would obscure more than it saves.
    """

    def label(value: int) -> str:
        return names[value] if names is not None else str(value)

    ordered = sorted(values)
    runs: List[str] = []
    i = 0
    while i < len(ordered):
        j = i
        while j + 1 < len(ordered) and ordered[j + 1] == ordered[j] + 1:
            j += 1
        if j - i >= 2:
            runs.append("{}-{}".format(label(ordered[i]), label(ordered[j])))
        else:
            runs.extend(label(value) for value in ordered[i : j + 1])
        i = j + 1
    return runs


def _compact_values(
    values: Iterable[int], names: Optional[Sequence[str]] = None
) -> str:
    """:func:`_value_runs` as prose: "0, 15, 30 and 45", "1-3 and 7"."""
    return _list_join(_value_runs(values, names))


def _allowed_dom(tab: CronTab) -> str:
    """The day-of-month constraint as prose (explicit days plus L/W)."""
    if _FULL_DOM <= tab.days_of_month:
        return "any"
    parts = _value_runs(tab.days_of_month)
    parts.extend(
        "the weekday nearest day {0} ({0}W)".format(target)
        for target in sorted(tab.nearest_weekday_days)
    )
    for offset in sorted(tab.last_day_offsets):
        if offset == 0:
            parts.append("the month's last day (L)")
        else:
            parts.append(
                "{0} day{1} before the month's last (L-{0})".format(
                    offset, "" if offset == 1 else "s"
                )
            )
    if tab.last_weekday_of_month:
        parts.append("the month's last weekday (LW)")
    return _list_join(parts)


def _allowed_dow(tab: CronTab) -> str:
    """The day-of-week constraint as prose (plain days plus L<n>/#)."""
    if _FULL_DOW <= tab.days_of_week:
        return "any"
    parts = _value_runs(tab.days_of_week, _DOWN)
    parts.extend(
        "the month's last {}".format(_DOWN[dow])
        for dow in sorted(tab.last_days_of_week)
    )
    parts.extend(
        "the month's {} {}".format(_ordinal(nth), _DOWN[dow])
        for dow, nth in sorted(tab.nth_days_of_week)
    )
    return _list_join(parts)


def why_no_run(
    tab: CronTab,
    when: datetime.datetime,
    timezone: Optional[datetime.tzinfo] = None,
) -> Dict[str, Any]:
    """Field-by-field verdict on whether ``tab`` selects the instant
    ``when``, decomposing exactly what :meth:`CronTab.test` computes.

    ``when`` is a CIVIL wall-clock instant in the schedule's own frame
    (naive; callers convert an aware timestamp into the job's zone
    first; microseconds are ignored, as the engine ignores them).  The
    result is JSON-ready: ``matches`` agrees with ``tab.test(when)`` by
    construction, ``checks`` holds one row per cron field with the
    probed ``value``, its human ``label``, the field's accepted values
    as prose (``allowed``, "any" for an unrestricted field) and whether
    it ``matched``; ``failed`` lists the failing field names in field
    order.

    ``notes`` (Finding-shaped dicts) spells out the two semantics that
    make a no-run or an odd run genuinely surprising: the dialect's
    day-field AND rule, reported when it is the SOLE blocker (both day
    fields restricted, exactly one matched, every other field matched:
    classic Vixie cron fires when EITHER day field matches, so an
    imported schedule WOULD have run there), and, for a matching wall
    time probed with a real ``timezone``, a DST transition that skips
    (the run fires at the shifted label) or repeats it (the run fires on
    the first occurrence only).
    """
    month_end = calendar.monthrange(when.year, when.month)[1]
    dow = (datetime.date(when.year, when.month, when.day).weekday() + 1) % 7
    # the engine's own per-side predicates, so this decomposition can
    # never disagree with what the scheduler computes
    dom_ok = tab._dom_matches(when.year, when.month, when.day, month_end)
    dow_ok = tab._dow_matches(dow, when.day, month_end)
    checks = [
        {
            "field": "second",
            "value": when.second,
            "label": str(when.second),
            "allowed": (
                "any"
                if len(tab.seconds) >= 60
                else _compact_values(tab.seconds)
            ),
            "matched": when.second in tab.seconds,
        },
        {
            "field": "minute",
            "value": when.minute,
            "label": str(when.minute),
            "allowed": (
                "any"
                if len(tab.minutes) >= 60
                else _compact_values(tab.minutes)
            ),
            "matched": when.minute in tab.minutes,
        },
        {
            "field": "hour",
            "value": when.hour,
            "label": str(when.hour),
            "allowed": (
                "any" if len(tab.hours) >= 24 else _compact_values(tab.hours)
            ),
            "matched": when.hour in tab.hours,
        },
        {
            "field": "day-of-month",
            "value": when.day,
            "label": str(when.day),
            "allowed": _allowed_dom(tab),
            "matched": dom_ok,
        },
        {
            "field": "month",
            "value": when.month,
            "label": _MONTHS[when.month],
            "allowed": (
                "any"
                if len(tab.months) >= 12
                else _compact_values(tab.months, _MONTHS)
            ),
            "matched": when.month in tab.months,
        },
        {
            "field": "day-of-week",
            "value": dow,
            "label": _DOWN[dow],
            "allowed": _allowed_dow(tab),
            "matched": dow_ok,
        },
        {
            "field": "year",
            "value": when.year,
            "label": str(when.year),
            "allowed": (
                "any" if tab.years is None else _compact_values(tab.years)
            ),
            "matched": tab.years is None or when.year in tab.years,
        },
    ]
    matches = all(check["matched"] for check in checks)
    notes: List[Finding] = []
    dom_restricted = not (_FULL_DOM <= tab.days_of_month)
    dow_restricted = not (_FULL_DOW <= tab.days_of_week)
    # the "Vixie would have fired" claim is only true when the day-field
    # AND rule is the SOLE blocker, so every non-day field must match too.
    others_ok = all(
        check["matched"]
        for check in checks
        if check["field"] not in ("day-of-month", "day-of-week")
    )
    if dom_restricted and dow_restricted and dom_ok != dow_ok and others_ok:
        ok_field, bad_field = (
            ("day-of-month", "day-of-week")
            if dom_ok
            else ("day-of-week", "day-of-month")
        )
        notes.append(
            Finding(
                "day-fields-and-rule",
                LEVEL_NOTE,
                "day-of-month and day-of-week are both restricted and a "
                "day must satisfy BOTH here: {} matched but {} did not, "
                "so classic Vixie cron (which fires when either field "
                "matches) WOULD have run this schedule at this "
                "instant".format(ok_field, bad_field),
            )
        )
    if (
        matches
        and timezone is not None
        and not isinstance(timezone, datetime.timezone)
    ):
        civil = when.replace(microsecond=0)
        kind = _classify(timezone, civil)
        if kind == "gap":
            shifted = (
                civil.replace(tzinfo=timezone)
                .astimezone(datetime.timezone.utc)
                .astimezone(timezone)
            )
            notes.append(
                Finding(
                    "dst-skipped-time",
                    LEVEL_NOTE,
                    "the wall time {} did not exist on {} in {} (clocks "
                    "jump forward); the scheduler fired once at the "
                    "shifted wall time {} instead".format(
                        civil.time().isoformat(),
                        civil.date().isoformat(),
                        timezone,
                        shifted.time().isoformat(),
                    ),
                )
            )
        elif kind == "fold":
            notes.append(
                Finding(
                    "dst-repeated-time",
                    LEVEL_NOTE,
                    "the wall time {} occurred twice on {} in {} (clocks "
                    "fall back); the schedule fired on the first "
                    "occurrence only".format(
                        civil.time().isoformat(),
                        civil.date().isoformat(),
                        timezone,
                    ),
                )
            )
    return {
        "matches": matches,
        "checks": checks,
        "failed": [c["field"] for c in checks if not c["matched"]],
        "notes": [note._asdict() for note in notes],
    }


# ===================================================================
#  fleet-level schedule analysis
# ===================================================================
class ScheduleEntry(NamedTuple):
    """One scheduled job, as the fleet analyzers see it.

    ``timezone`` is the job's RESOLVED zone (``JobConfig.timezone``):
    ``None`` means the daemon's local wall clock, exactly as it does
    there.  Rows are built by the daemon from its live job set and by the
    TUI from its ``/jobs`` snapshot; @reboot jobs and disabled jobs never
    become entries, they cannot collide with anything.
    """

    name: str
    tab: CronTab
    timezone: Optional[datetime.tzinfo] = None


#: job names carried per grid cell and per duplicate group before the
#: payload switches to a count: keeps the pressure payload bounded when
#: hundreds of jobs share one cell.
_NAME_CAP = 10


def _minute_tab(tab: CronTab) -> Tuple[CronTab, int]:
    """A minute-granular twin of ``tab``, plus its fires per minute.

    A 7-field schedule can fire many times inside one minute; walking
    every second-level occurrence of a ``*/5``-seconds job across 24
    hours would be 17280 engine steps for information the field sets
    already hold.  Re-parse the resolved source (always plain dialect,
    so no hash key is needed) with the seconds column pinned to 0 and
    weight each matched minute by how many seconds it fires on.
    """
    fields = tab.resolved_source.split()
    if len(fields) == 7:
        return (
            CronTab(" ".join(["0"] + fields[1:])),
            len(tab.seconds),
        )
    return tab, 1


def _local_tzinfo() -> Optional[datetime.tzinfo]:
    """The daemon's own zone, for entries scheduled on the local clock."""
    return datetime.datetime.now().astimezone().tzinfo


def _fire_cells(
    entries: Sequence[ScheduleEntry],
    start: datetime.datetime,
    hours: int,
    tz: datetime.tzinfo,
) -> Tuple[
    List[List[int]],
    Dict[Tuple[int, int], List[str]],
    List[Set[str]],
]:
    """Walk every entry's fires over ``[start, start+hours)``.

    Returns the 24x60 grid of fire counts keyed by the DISPLAY zone's
    civil (hour, minute) label, the per-cell job names (capped at
    ``_NAME_CAP``), and the set of jobs firing at each minute-of-hour.
    Enumerates real instants through the engine's own
    :meth:`CronTab.occurrences`, so DST behaviour matches the scheduler:
    on a fall-back day both real fires of a repeated wall time land in
    (and truthfully double-count at) the same cell.
    """
    end = start + datetime.timedelta(hours=hours)
    local_tz = _local_tzinfo()
    grid = [[0] * 60 for _ in range(24)]
    cell_jobs: Dict[Tuple[int, int], List[str]] = {}
    minute_jobs: List[Set[str]] = [set() for _ in range(60)]
    cap = hours * 60 + 2  # backstop; a minute-granular walk cannot exceed it
    for entry in entries:
        zone = entry.timezone or local_tz
        try:
            mtab, weight = _minute_tab(entry.tab)
        except (ValueError, KeyError):  # pragma: no cover - defensive
            continue
        walked = 0
        for when in mtab.occurrences(start.astimezone(zone)):
            if when >= end or walked >= cap:
                break
            walked += 1
            label = when.astimezone(tz)
            grid[label.hour][label.minute] += weight
            minute_jobs[label.minute].add(entry.name)
            names = cell_jobs.setdefault((label.hour, label.minute), [])
            if len(names) < _NAME_CAP and entry.name not in names:
                names.append(entry.name)
    return grid, cell_jobs, minute_jobs


def schedule_pressure(
    entries: Sequence[ScheduleEntry],
    start: Optional[datetime.datetime] = None,
    hours: int = 24,
    tz: Optional[datetime.tzinfo] = None,
) -> Dict[str, Any]:
    """The fleet's collision heatmap: every fire over the next 24 hours.

    Enumerates each entry's fire instants over ``[start, start+hours)``
    with the scheduler's own engine and buckets them by the civil
    (hour, minute) label in ``tz`` (UTC by default), answering "37 jobs
    fire at :00 and minute 23 is empty" with data instead of vibes.

    The payload is JSON-ready: ``grid`` is 24 rows (hour of day) of 60
    fire counts (minute), ``by_minute_jobs``/``by_minute_fires`` collapse
    it to the 60-bin histogram the dashboards draw, ``top_cells`` names
    the worst offenders (job names capped at ``_NAME_CAP`` per cell) and
    ``empty_minutes`` lists the minutes nothing fires on.  Fire COUNTS
    weigh sub-minute schedules by their fires per matched minute; the
    per-minute JOB counts stay distinct job names.
    """
    zone = tz or datetime.timezone.utc
    if start is None:
        start = datetime.datetime.now(datetime.timezone.utc)
    hours = max(1, min(int(hours), 168))
    grid, cell_jobs, minute_jobs = _fire_cells(entries, start, hours, zone)
    by_minute_fires = [
        sum(grid[hour][minute] for hour in range(24)) for minute in range(60)
    ]
    by_minute_jobs = [len(minute_jobs[minute]) for minute in range(60)]
    by_hour = [sum(row) for row in grid]
    busiest = max(
        range(60), key=lambda m: (by_minute_jobs[m], by_minute_fires[m])
    )
    empty = [m for m in range(60) if by_minute_fires[m] == 0]
    ranked = sorted(
        cell_jobs,
        key=lambda cell: (-grid[cell[0]][cell[1]], cell),
    )
    top_cells = [
        {
            "hour": hour,
            "minute": minute,
            "fires": grid[hour][minute],
            "jobs": cell_jobs[(hour, minute)],
        }
        for hour, minute in ranked[:6]
    ]
    return {
        "start": start.astimezone(zone).isoformat(),
        "hours": hours,
        "timezone": str(zone),
        "jobs": len(entries),
        "total_fires": sum(by_hour),
        "grid": grid,
        "by_minute_fires": by_minute_fires,
        "by_minute_jobs": by_minute_jobs,
        "by_hour": by_hour,
        "busiest_minute": {
            "minute": busiest,
            "jobs": by_minute_jobs[busiest],
            "fires": by_minute_fires[busiest],
        },
        "empty_minutes": empty,
        "top_cells": top_cells,
    }


def _tz_label(timezone: Optional[datetime.tzinfo]) -> str:
    return str(timezone) if timezone is not None else "local"


def _semantic_key(tab: CronTab) -> Tuple[Any, ...]:
    """A hashable stand-in for the engine's semantic ``==``.

    Two CronTabs are equal exactly when every parsed field set matches
    (see :meth:`CronTab.__eq__`); this tuple lists the same sets, so
    grouping by it groups by "fires on the identical instants".
    """
    return (
        tab.seconds,
        tab.minutes,
        tab.hours,
        tab.days_of_month,
        tab.last_day_offsets,
        tab.nearest_weekday_days,
        tab.last_weekday_of_month,
        tab.months,
        tab.days_of_week,
        tab.last_days_of_week,
        tab.nth_days_of_week,
        tab.years,
    )


def duplicate_schedules(
    entries: Sequence[ScheduleEntry],
) -> List[Dict[str, Any]]:
    """Groups of jobs whose schedules fire on the identical instants.

    Grouping is SEMANTIC, by the engine's own equality, so ``*/5``,
    ``0-59/5`` and an ``H`` slot that happens to resolve to the same set
    all land in one group; and it includes the resolved timezone, so two
    ``0 0 * * *`` jobs in different zones (which never fire together) do
    NOT count as duplicates.  Each group reports the most common source
    spelling, a description of the shared schedule, and the member job
    names; groups of one are omitted.  Sorted largest first.
    """
    groups: Dict[Tuple[Any, ...], List[ScheduleEntry]] = {}
    for entry in entries:
        key = _semantic_key(entry.tab) + (_tz_label(entry.timezone),)
        groups.setdefault(key, []).append(entry)
    out: List[Dict[str, Any]] = []
    for members in groups.values():
        if len(members) < 2:
            continue
        sources = Counter(str(entry.tab) for entry in members)
        resolved = Counter(entry.tab.resolved_source for entry in members)
        out.append(
            {
                "expression": sources.most_common(1)[0][0],
                "description": describe_cron(resolved.most_common(1)[0][0]),
                "timezone": _tz_label(members[0].timezone),
                "count": len(members),
                "jobs": sorted(entry.name for entry in members),
            }
        )
    out.sort(key=lambda group: (-group["count"], group["expression"]))
    return out


def _circular_distance(a: int, b: int, span: int) -> int:
    return min((a - b) % span, (b - a) % span)


def suggest_slot(
    entries: Sequence[ScheduleEntry],
    period: str = "hourly",
    start: Optional[datetime.datetime] = None,
    tz: Optional[datetime.tzinfo] = None,
    grid: Optional[List[List[int]]] = None,
) -> Dict[str, Any]:
    """The least-loaded slot for a new job, from the fleet's real fires.

    ``period="hourly"`` picks a minute (a ``<m> * * * *`` schedule),
    ``period="daily"`` a minute and hour (``<m> <h> * * *``), scored on
    the same 24-hour fire walk as :func:`schedule_pressure`.  Ties break
    toward the slot circularly FARTHEST from the busiest one (on an idle
    fleet that lands mid-hour, away from the :00 stampede the outside
    world defaults to), then toward the earliest slot, so the answer is
    deterministic.  ``alternatives`` lists the two runners-up, and
    ``hash_hint`` names the ``H`` spelling that would keep spreading
    future jobs without anyone picking slots by hand.

    ``grid`` skips the walk: pass the 24x60 ``grid`` of a
    :func:`schedule_pressure` payload computed for the SAME entries, zone
    and a 24-hour window, so a caller wanting the heatmap plus both
    suggestions pays for one enumeration instead of three.
    """
    if period not in ("hourly", "daily"):
        raise ValueError(
            "period must be 'hourly' or 'daily', got {!r}".format(period)
        )
    zone = tz or datetime.timezone.utc
    if start is None:
        start = datetime.datetime.now(datetime.timezone.utc)
    if grid is None:
        grid, _cells, _minute_jobs = _fire_cells(entries, start, 24, zone)
    if period == "hourly":
        loads = [
            sum(grid[hour][minute] for hour in range(24))
            for minute in range(60)
        ]
        busiest = max(range(60), key=lambda m: (loads[m], -m))
        order = sorted(
            range(60),
            key=lambda m: (
                loads[m],
                -_circular_distance(m, busiest, 60),
                m,
            ),
        )
        slots = [
            {
                "minute": minute,
                "expression": "{} * * * *".format(minute),
                "fires_in_window": loads[minute],
            }
            for minute in order[:3]
        ]
        busiest_out: Dict[str, Any] = {
            "minute": busiest,
            "fires_in_window": loads[busiest],
        }
        hash_hint = "H * * * *"
    else:
        loads_by_cell = {
            (hour, minute): grid[hour][minute]
            for hour in range(24)
            for minute in range(60)
        }
        busiest_cell = max(
            loads_by_cell,
            key=lambda cell: (loads_by_cell[cell], -cell[0], -cell[1]),
        )
        busiest_of_day = busiest_cell[0] * 60 + busiest_cell[1]
        order_cells = sorted(
            loads_by_cell,
            key=lambda cell: (
                loads_by_cell[cell],
                -_circular_distance(
                    cell[0] * 60 + cell[1], busiest_of_day, 1440
                ),
                cell,
            ),
        )
        slots = [
            {
                "hour": hour,
                "minute": minute,
                "expression": "{} {} * * *".format(minute, hour),
                "fires_in_window": loads_by_cell[(hour, minute)],
            }
            for hour, minute in order_cells[:3]
        ]
        busiest_out = {
            "hour": busiest_cell[0],
            "minute": busiest_cell[1],
            "fires_in_window": loads_by_cell[busiest_cell],
        }
        hash_hint = "H H * * *"
    suggestion = slots[0]
    return {
        "period": period,
        "timezone": str(zone),
        "based_on": {
            "jobs": len(entries),
            "start": start.astimezone(zone).isoformat(),
            "hours": 24,
        },
        **suggestion,
        "busiest": busiest_out,
        "alternatives": slots[1:],
        "hash_hint": hash_hint,
    }
