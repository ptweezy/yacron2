"""iCalendar (RFC 5545) rendering of upcoming schedule fires.

:func:`render_calendar` turns a list of :class:`CalendarEntry` rows into
the text of an ``.ics`` feed: one ``VEVENT`` per upcoming fire instant,
enumerated by the daemon's own engine (:meth:`CronTab.occurrences`) in each
job's resolved timezone, so the calendar shows exactly what the scheduler
will do.  The daemon serves it as ``GET /calendar.ics`` (the whole fleet)
and ``GET /jobs/{name}/calendar.ics`` (one job); the "Calendar Export"
wiki page is the user documentation.

Rendering choices, all deliberate:

- ``DTSTART`` is emitted in UTC (the ``...Z`` form), never as a floating
  or zoned local time: the fire instants are real instants, clients
  localize them, and no ``VTIMEZONE`` blocks need shipping.
- ``UID`` is stable across regenerations (a hash of the job name plus the
  fire instant), so a subscribed client updates events in place instead of
  duplicating them each refresh.
- Events carry ``TRANSP:TRANSPARENT``: a maintenance job on the on-call
  engineer's calendar must not mark them busy.
- The event block length is the job's typical runtime rounded up to a
  minute when run history knows it, and never under 5 minutes: a
  zero-length event renders as an invisible sliver in week views, which
  defeats the purpose.  ``DESCRIPTION`` states the real average.
- No command lines, environment or output ever appear in the feed, only
  the job name, its schedule and the schedule's plain-English description:
  calendar feeds tend to end up on phones and third-party services, well
  outside the daemon's redaction reach.
"""

import datetime
import hashlib
import math
from typing import List, NamedTuple, Optional, Sequence

from cronstable.cronexpr import CronTab
from cronstable.croninfo import _local_tzinfo, describe_cron

__all__ = ["CalendarEntry", "render_calendar"]

_CRLF = "\r\n"

#: shortest event block rendered, seconds; see the module docstring
_MIN_BLOCK = 5 * 60

#: longest event block rendered, seconds: a runaway average (a backfill
#: that once took a day) must not paint whole days solid
_MAX_BLOCK = 24 * 3600


class CalendarEntry(NamedTuple):
    """One scheduled job, as the calendar renderer sees it.

    ``timezone`` is the job's RESOLVED zone; ``None`` means the daemon's
    local wall clock, exactly as in :class:`croninfo.ScheduleEntry`.
    ``avg_duration`` is the mean runtime in seconds over retained run
    history, or ``None`` when no history exists.
    """

    name: str
    tab: CronTab
    timezone: Optional[datetime.tzinfo] = None
    avg_duration: Optional[float] = None


def _escape(text: str) -> str:
    """RFC 5545 TEXT escaping (backslash first, then the specials)."""
    return (
        text.replace("\\", "\\\\")
        .replace(";", "\\;")
        .replace(",", "\\,")
        .replace("\r\n", "\\n")
        .replace("\n", "\\n")
        .replace("\r", "\\n")
    )


def _fold(line: str) -> str:
    """RFC 5545 line folding at 75 octets, never splitting a UTF-8 char.

    Continuation lines open with one space, so their content budget is 74
    octets; the fold point backs up over UTF-8 continuation bytes rather
    than cutting inside a multi-byte character.
    """
    data = line.encode("utf-8")
    if len(data) <= 75:
        return line
    chunks: List[str] = []
    limit = 75
    while data:
        cut = min(limit, len(data))
        while 0 < cut < len(data) and (data[cut] & 0xC0) == 0x80:
            cut -= 1
        chunks.append(data[:cut].decode("utf-8"))
        data = data[cut:]
        limit = 74
    return (_CRLF + " ").join(chunks)


def _stamp(dt: datetime.datetime) -> str:
    """An aware instant as the RFC 5545 UTC form (``YYYYMMDDTHHMMSSZ``)."""
    return dt.astimezone(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _block_seconds(avg_duration: Optional[float]) -> int:
    """The rendered event length for a job's typical runtime."""
    if avg_duration is None or avg_duration <= 0:
        return _MIN_BLOCK
    whole_minutes = 60 * math.ceil(avg_duration / 60)
    return max(_MIN_BLOCK, min(_MAX_BLOCK, whole_minutes))


def _duration_text(seconds: int) -> str:
    """Whole-minute seconds as an ISO 8601 duration (``PT5M``, ``PT1H30M``)."""
    hours, rest = divmod(seconds, 3600)
    minutes = rest // 60
    if hours and minutes:
        return "PT{}H{}M".format(hours, minutes)
    if hours:
        return "PT{}H".format(hours)
    return "PT{}M".format(minutes)


def _runtime_phrase(avg_duration: float) -> str:
    if avg_duration < 60:
        return "{}s".format(int(round(avg_duration)))
    minutes = avg_duration / 60
    if minutes < 60:
        return "{}m".format(int(round(minutes)))
    return "{:.1f}h".format(minutes / 60)


def render_calendar(
    entries: Sequence[CalendarEntry],
    start: datetime.datetime,
    days: int,
    per_job_cap: int = 100,
    calname: str = "cronstable",
    now: Optional[datetime.datetime] = None,
    prodid_version: str = "",
) -> str:
    """The complete ``.ics`` text for ``entries`` over ``[start, start+days)``.

    ``start`` must be aware; every fire STRICTLY after it and before the
    window's end becomes one event, at most ``per_job_cap`` per entry (a
    per-minute job would otherwise flood the feed; the cap is announced
    per event-starved entry via ``X-CRONSTABLE-TRUNCATED``).  ``now``
    fixes ``DTSTAMP`` for determinism in tests and defaults to the wall
    clock.  Entries render in the order given; pass them name-sorted for
    a stable feed.
    """
    if start.tzinfo is None:
        raise ValueError("render_calendar needs an aware start")
    if now is None:
        now = datetime.datetime.now(datetime.timezone.utc)
    dtstamp = _stamp(now)
    end_utc = (start + datetime.timedelta(days=days)).astimezone(
        datetime.timezone.utc
    )
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//cronstable//{}//EN".format(prodid_version or "unversioned"),
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "X-WR-CALNAME:" + _escape(calname),
        # subscription clients honour one of these two refresh hints
        "REFRESH-INTERVAL;VALUE=DURATION:PT1H",
        "X-PUBLISHED-TTL:PT1H",
    ]
    local_tz = _local_tzinfo()
    for entry in entries:
        zone = entry.timezone or local_tz
        block = _block_seconds(entry.avg_duration)
        duration = _duration_text(block)
        description = "Schedule: {}\n{}\nTimezone: {}".format(
            str(entry.tab),
            describe_cron(str(entry.tab), hash_key=entry.name),
            str(entry.timezone) if entry.timezone is not None else "local",
        )
        if entry.avg_duration is not None and entry.avg_duration > 0:
            description += "\nTypical runtime: {}".format(
                _runtime_phrase(entry.avg_duration)
            )
        uid_ns = hashlib.sha256(entry.name.encode("utf-8")).hexdigest()[:12]
        count = 0
        truncated = False
        for fire in entry.tab.occurrences(start.astimezone(zone)):
            fire_utc = fire.astimezone(datetime.timezone.utc)
            if fire_utc >= end_utc:
                break
            if count >= per_job_cap:
                truncated = True
                break
            count += 1
            stamp = fire_utc.strftime("%Y%m%dT%H%M%SZ")
            lines.extend(
                [
                    "BEGIN:VEVENT",
                    "UID:{}-{}@cronstable".format(uid_ns, stamp),
                    "DTSTAMP:" + dtstamp,
                    "DTSTART:" + stamp,
                    "DURATION:" + duration,
                    "SUMMARY:" + _escape(entry.name),
                    "DESCRIPTION:" + _escape(description),
                    "STATUS:CONFIRMED",
                    "TRANSP:TRANSPARENT",
                    "END:VEVENT",
                ]
            )
        if truncated:
            # the cap rides as a property parameter, not in the value: a
            # raw ';' inside a TEXT value is illegal per RFC 5545, and the
            # job name (the value) must stay unambiguous
            lines.append(
                "X-CRONSTABLE-TRUNCATED;CAP={}:{}".format(
                    per_job_cap, _escape(entry.name)
                )
            )
    lines.append("END:VCALENDAR")
    return _CRLF.join(_fold(line) for line in lines) + _CRLF
