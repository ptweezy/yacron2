# Why Didn't It Run?

"The report job didn't run this morning" is the classic scheduling
mystery, and the answer is usually buried in one cron field. cronstable
answers it from ground truth: `GET /schedule/why` (and the
`cron_why_no_run` [MCP tool](MCP)) takes **one job and one timestamp**
and decomposes the scheduler's own match test **field by field**, so the
verdict can never disagree with what the daemon actually computed.

```shell
$ http get "http://127.0.0.1:8080/schedule/why?job=weekday-report&at=2026-07-14T09:00"
{
    "job": "weekday-report",
    "enabled": true,
    "timezone": "UTC",
    "at": "2026-07-14T09:00",
    "at_in_zone": "2026-07-14T09:00:00+00:00",
    "expression": "0 9 * * mon,fri",
    "reboot": false,
    "description": "At 09:00, on Monday and Friday",
    "matches": false,
    "checks": [
        {"field": "second",       "value": 0,    "label": "0",       "allowed": "0",                 "matched": true},
        {"field": "minute",       "value": 0,    "label": "0",       "allowed": "0",                 "matched": true},
        {"field": "hour",         "value": 9,    "label": "9",       "allowed": "9",                 "matched": true},
        {"field": "day-of-month", "value": 14,   "label": "14",      "allowed": "any",               "matched": true},
        {"field": "month",        "value": 7,    "label": "July",    "allowed": "any",               "matched": true},
        {"field": "day-of-week",  "value": 2,    "label": "Tuesday", "allowed": "Monday and Friday", "matched": false},
        {"field": "year",         "value": 2026, "label": "2026",    "allowed": "any",               "matched": true}
    ],
    "failed": ["day-of-week"],
    "notes": [],
    "previous_fire": "2026-07-13T09:00:00+00:00",
    "next_fire": "2026-07-17T09:00:00+00:00"
}
```

Every other field matched; Tuesday is not in {Monday, Friday}. The
`previous_fire` / `next_fire` pair brackets the probe with the nearest
**real** fire instants (from the same occurrence walk the scheduler
runs, in the job's zone), so "when DID it run around then?" is answered
in the same breath.

## Reading the answer

- **The probe runs in the job's own timezone.** A timestamp with a UTC
  offset (`2026-07-14T11:00:00+02:00`, trailing `Z` accepted) is
  converted into the job's resolved zone first; a naive timestamp reads
  as wall time in that zone directly. `at_in_zone` shows the instant the
  fields were checked against.
- **One `checks` row per cron field**, in field order (second, minute,
  hour, day-of-month, month, day-of-week, year). `value` is the probe's
  value in cron terms (Sunday is `0`), `label` is its human name,
  `allowed` renders the field's accepted values as prose: an
  unrestricted field reads `any`, runs collapse (`1-3 and 7`,
  `Monday-Friday`), and the `L` forms are spelled out (`the month's
  last day (L)`, `the month's last Friday`).
- **`matches` is exactly the engine's verdict.** The decomposition is of
  `CronTab.test` itself, one term per row, so `matches` always equals
  what the scheduler would compute for that civil instant.
- **[`H` schedules](Hashed-Schedules) check against their resolved
  slots.** The payload carries `resolved` (the expression with every `H`
  replaced by its hashed values) and `allowed` names the concrete slot,
  so "minute 0 is not in 16" tells you where the hash actually landed.

## When the answer is genuinely surprising

Two scheduling semantics produce misses (or odd runs) that look like
bugs. The explainer flags both in `notes`:

- **`day-fields-and-rule`.** In this dialect a day must satisfy **both**
  day fields when both are restricted (`0 0 13 * 5` is Friday the 13th),
  while classic Vixie cron fires when **either** matches. When exactly
  one of the two matched, the note says so and states plainly that Vixie
  cron would have run the schedule at that instant: the number-one
  surprise when importing a system crontab. The
  [schedule linter](Schedule-Linting) warns about the combination at
  config load; this note pins it to the concrete instant you asked
  about.
- **`dst-skipped-time` / `dst-repeated-time`.** For a matching wall time
  that a DST transition in the job's zone skips, the note names the
  shifted wall time the run actually fired at; for a repeated wall time,
  it says the run fired on the first occurrence only. See
  [Schedules and Timezones](Schedules-and-Timezones) for the underlying
  time model.

## When the schedule is not the culprit

- **`matches: true`.** The timetable selected the instant, so the
  schedule is innocent: check the job's run history
  (`GET /jobs/<name>/runs`, or `cron_list_runs` over MCP) for what
  execution did. Daemon downtime, `concurrencyPolicy`, or cluster
  leadership are the usual suspects.
- **Disabled jobs** still explain their timetable and report
  `enabled: false`; a matching instant on a disabled job is its own
  answer.
- **`@reboot` jobs** answer `reboot: true` with no checks: they run once
  at daemon startup and never fire on a timetable.
- **DAG schedules** resolve under their synthetic `dag:<name>` job name,
  exactly as they appear in the
  [schedule pressure](Schedule-Pressure) fleet views.

## For AI agents

`cron_why_no_run` serves the same payload over [MCP](MCP), with a
one-line verdict an agent can act on ("NO: second, minute, hour,
day-of-month, month, year matched; day-of-week Tuesday is not in Monday
and Friday"). It pairs with `cron_validate_schedule` and
`cron_explain_schedule`, which run any prospective expression through
the daemon's engine (parse errors, description, upcoming fires,
[lint findings](Schedule-Linting)) before it becomes a job, so the
agent's authoring loop and its debugging loop use the same source of
truth the scheduler does.

## See also

- [HTTP Control API](HTTP-API): the `GET /schedule/why` reference, and
  `GET /schedule/preview` for arbitrary expressions.
- [MCP Server](MCP): wiring an agent to the tools.
- [Schedule Linting](Schedule-Linting): whole-schedule advisory findings
  at config load.
- [Schedules and Timezones](Schedules-and-Timezones): the dialect and
  the time model the explanations are grounded in.
