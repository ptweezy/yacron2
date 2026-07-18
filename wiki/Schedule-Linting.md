# Schedule Linting

cronstable lints every cron schedule it loads. The linter only looks at schedules the engine already *accepts* (rejecting bad syntax is the parser's job) and flags legal expressions that probably do not mean what the author intended, or that behave in a way worth knowing about. A dead schedule (one that can never fire again) is the loudest case: it used to vanish silently, and now announces itself everywhere.

The rules live in `cronstable/croninfo.py` (`lint_schedule`), the same module that computes plain-English descriptions and fire previews, so every surface (config load, the HTTP API, the [terminal dashboard](Terminal-Dashboard)) reports identical findings.

## Findings

Each finding has a stable `code`, a `level`, and a one-line `message`. `warning` means "probably a mistake"; `note` means "deliberate schedules do this too, but know what it does".

| Code | Level | What it means |
|------|-------|---------------|
| `never-fires` | warning | The schedule has no future occurrence: a fixed year in the past, or a date that never exists (`0 0 30 2 *`). The job stays loaded but will never run. |
| `day-fields-both-restricted` | warning | Day-of-month and day-of-week are both restricted. cronstable requires a day to satisfy **both** (`0 0 13 * 5` is Friday the 13th), while classic Vixie cron fires when *either* matches, so a schedule imported from a system crontab fires less often here than it did there. See [Schedules and Timezones](Schedules-and-Timezones). |
| `uneven-step` | warning | A `*/n` step where `n` does not divide the field's span. `*/7` in the minute field fires at :56 and then :00 four minutes later, because star steps restart at the wrap. |
| `uneven-step` (day-of-month) | note | Any `*/n` day-of-month step: values restart at day 1 every month and month lengths differ, so `*/2` is not "every 48 hours". |
| `skipped-months` | warning | The smallest selected day of month never occurs in one of the selected months (`0 0 31 1,4 *` never fires in April), so that month is skipped entirely. |
| `leap-day-only` | note | February runs can only match day 29, which exists only in leap years. |
| `dst-skipped-time` | note | The scheduled wall time falls in a spring-forward gap of the job's timezone (with the actual date, e.g. `2027-03-14` for `America/New_York`). The run is *not* lost: it fires at the shifted wall time. |
| `dst-repeated-time` | note | The scheduled wall time occurs twice on a fall-back date; the run fires on the first occurrence only. |
| `hashed-slot` | note | The schedule uses the [`H` hash form](Hashed-Schedules); the note names the exact expression it resolved to for this job, and reminds that renaming the job re-hashes the slot. |

The DST rules need a resolvable zone, so they run only for jobs with an explicit `timezone:` (a fixed-offset frame like UTC never transitions, and the daemon cannot see the DST rules behind a bare local clock). They are also skipped for schedules with unrestricted hours, which fire straight through a transition with nothing to call out.

## Where findings appear

- **Config load.** Every finding is logged when the job parses (`warning` findings at WARNING, `note` findings at INFO), so the load or reload that introduces a footgun says so immediately:

  ```
  WARNING:cronstable.config:job 'parked': schedule '0 0 1 1 * 2020': [never-fires] no future occurrence: the year column ends at 2020, so this schedule will never fire again
  ```

- **The HTTP API.** `GET /jobs` carries each job's findings verbatim (`schedule_findings`, a list of `{code, level, message}`) plus a computed `never_fires` boolean; `GET /status` marks dead schedules with `never_fires: true` (and says `never fires` in the plain-text form). `GET /schedule/preview` lints arbitrary expressions before they become jobs. See [HTTP API](HTTP-API).
- **The terminal dashboard.** The cron sandbox (`x`) lints as you type, and a job's schedule drawer shows findings in the job's own timezone, so DST notes carry real dates. See [Terminal Dashboard](Terminal-Dashboard).

## Dead schedules are loud, not fatal

A `never-fires` schedule stays a warning rather than a config error, deliberately: a fixed past year (`schedule: "0 0 1 1 * 2020"`) is also the working idiom for parking a job without deleting its config, and failing the whole config load over it would turn an upgrade into an outage. What changed is the silence. Besides the load-time warning, the scheduler logs once per (re)load when it drops a dead schedule from its fire index:

```
WARNING:cronstable:job 'parked': schedule '0 0 1 1 * 2020' has no future occurrence and will NEVER fire; fix the schedule or disable the job (its status reports never_fires)
```

and every status surface reports `never_fires` until the schedule changes. If a job should not run, prefer `enabled: false`, which says what it means.

## Linting expressions before they become jobs

`GET /schedule/preview?expr=<expression>&tz=<zone>&count=<n>` parses, describes, previews and lints any expression with the daemon's own engine, the single source of truth behind the sandboxes. See [HTTP API](HTTP-API) for the full response shape. AI agents get the same payload through the `cron_validate_schedule` and `cron_explain_schedule` [MCP tools](MCP).

## Explaining one instant instead of a whole schedule

The linter judges a schedule in the abstract; `GET /schedule/why` judges it against one concrete timestamp, decomposing the match test field by field ("day-of-week Tuesday is not in Monday and Friday") with notes for the AND day rule and DST effects at that instant. See [Why Didn't It Run?](Why-No-Run).
