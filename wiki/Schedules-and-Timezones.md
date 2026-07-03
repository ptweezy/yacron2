# Schedules and Timezones

Every job's `schedule` determines *when* it runs; `utc` and `timezone` determine *in which clock* the schedule is evaluated. This page documents the three accepted `schedule` forms, how the daemon wakes and tests them, and how the effective timezone is resolved.

## The `schedule` option

`schedule` is **required** on every job. The strictyaml schema accepts two YAML types for it (`yacron2/config.py`):

```
"schedule": Str()
| Map({
      Opt("second"): Str(),
      Opt("minute"): Str(),
      Opt("hour"): Str(),
      Opt("dayOfMonth"): Str(),
      Opt("month"): Str(),
      Opt("year"): Str(),
      Opt("dayOfWeek"): Str(),
  })
```

so `schedule` is either a string or an object. `JobConfig._parse_schedule` turns that raw value into one of:

- a `crontab.CronTab` instance (a parsed crontab expression), or
- the literal string `"@reboot"`.

Any other value raises `ConfigError("invalid schedule: ...")`.

The crontab dialect is [parse-crontab](https://github.com/josiahcarlson/parse-crontab) (`josiahcarlson/parse-crontab`), pinned as `crontab>=1,<2`. Field syntax (ranges `1-5`, steps `*/5`, lists `1,15,30`, names like `mon`/`jan`) follows that library, not the system `cron(5)` man page.

Schedules do not have to live in YAML at all: yacron2 also loads whole
classic crontab files (`*.crontab`, `*.cron`, or a file named `crontab`),
whose entries use this same field dialect plus the `@` nicknames and default
to UTC like every other yacron2 schedule. See [Classic Crontabs](Classic-Crontabs).

## Form 1: crontab string (5, 6 or 7 fields)

A standard five-field crontab expression: `minute hour day-of-month month day-of-week`.

```yaml
jobs:
  - name: every-five-minutes
    command: echo hello
    schedule: "*/5 * * * *"
```

The string is passed verbatim to `CronTab(...)`. A malformed expression is caught and re-raised as `ConfigError("invalid schedule '...': ...")` at config-load time (naming the offending expression), so a bad field fails the reload cleanly rather than as an anonymous traceback. Quote the value in YAML: a bare `*/5 * * * *` is not valid YAML scalar syntax in all positions.

parse-crontab reads **extra columns from the ends**, so the field count selects the dialect:

| Fields | Layout | Meaning |
|--------|--------|---------|
| 5 | `minute hour dayOfMonth month dayOfWeek` | the classic form; implicit second `0`, any year |
| 6 | `... dayOfWeek year` | adds a trailing **year** column (still second `0`) |
| 7 | `second minute hour dayOfMonth month dayOfWeek year` | adds a leading **second** column too |

So a seven-field string schedules at **second granularity** (see [Second-level schedules](#second-level-schedules) below), while a six-field string pins a `year`. A six-field string is *not* seconds — the extra column is the year.

## Second-level schedules

yacron2 can run jobs at second granularity. Give the schedule a `second` (via the seven-field string above, or the `second:` object key in [Form 3](#form-3-schedule-object)); both jobs below run every 15 seconds:

```yaml
jobs:
  - name: every-15s-string
    command: echo tick
    schedule: "*/15 * * * * * *"   # 7 fields; leading field is the second
  - name: every-15s-object
    command: echo tick
    schedule:
      second: "*/15"
```

The `second` field takes the same syntax as any other (`*`, `*/5`, `0,30`, `10-20`); `second: "*"` fires every second. While **any enabled job** specifies seconds, the scheduler switches from its once-a-minute cadence to once-a-second (see [How the scheduler ticks](#how-the-scheduler-ticks)); when none do, the original minute cadence and its zero overhead are retained. Minute-granular jobs are unaffected either way: they still fire exactly once in their scheduled minute. Second-level scheduling is a YAML-only feature — [classic crontab files](Classic-Crontabs) stay five-field and minute-granular.

## Form 2: `@reboot`

The exact string `"@reboot"` is stored as-is (not parsed into a `CronTab`). A `@reboot` job runs **only once, at daemon startup**, and never on a recurring schedule.

```yaml
jobs:
  - name: warm-cache
    command: /usr/local/bin/warm-cache
    schedule: "@reboot"
```

Behavior comes from `Cron.job_should_run` (`yacron2/cron.py`): on the first scheduler pass the `startup` flag is `True` and a `@reboot` job returns `True`; on every subsequent pass `startup` is `False`, so `@reboot` jobs return `False`. Conversely, `CronTab`-scheduled jobs return `False` during the startup pass and are only evaluated on later passes. There is no recurring "@reboot". To keep a long-running process alive, `README.md` recommends a `@reboot` schedule combined with `onFailure.retry.maximumRetries: -1` (retry forever), so yacron2 relaunches the process whenever it exits/fails.

`"@reboot"` is the only `@`-keyword recognized by yacron2 itself. Other shorthands (`@daily`, `@hourly`, etc.) are *not* intercepted by `_parse_schedule`; whether they work depends entirely on whether `parse-crontab` accepts them.

## Form 3: schedule object

An object lets you name fields individually. Each omitted key defaults to `"*"`.

```yaml
jobs:
  - name: noon-on-weekdays
    command: echo hello
    schedule:
      minute: "0"
      hour: "12"
      dayOfWeek: "mon-fri"
```

`schedule_object_to_crontab` (in `yacron2/config.py`) builds a crontab string from exactly these keys and `_parse_schedule` feeds it to `CronTab`:

| Object key   | Crontab field | Default if omitted |
|--------------|---------------|--------------------|
| `second`     | second        | *(omitted)*        |
| `minute`     | minute        | `*`                |
| `hour`       | hour          | `*`                |
| `dayOfMonth` | day-of-month  | `*`                |
| `month`      | month         | `*`                |
| `dayOfWeek`  | day-of-week   | `*`                |
| `year`       | year          | *(omitted)*        |

Only the columns you actually use are emitted, matching parse-crontab's end-column rule from [Form 1](#form-1-crontab-string-5-6-or-7-fields):

- neither `second` nor `year` → a five-field line (`f"{minute} {hour} {day} {month} {dow}"`), exactly as before;
- `year` only → a six-field line with the trailing year column;
- `second` present → a full seven-field line (`year` defaults to `*` if unset).

So `{minute: "*/5"}` is byte-for-byte the five-field string `"*/5 * * * *"` (and the two spellings share a [job-set fingerprint](Configuration-Reference)), while `{second: "*/15"}` is the seven-field `"*/15 * * * * * *"`.

All values are typed `Str()` in the schema, so write `minute: "0"`, not `minute: 0`. Although strictyaml will coerce an unquoted scalar to a string here, quoting is the documented convention and avoids surprises with values like `"7"`.

### The `year` key

`year` restricts the schedule to specific years (parse-crontab's optional trailing column). For example, this runs only during 2017:

```yaml
schedule:
  minute: "*/5"
  dayOfMonth: "19"
  month: "7"
  year: "2017"
```

> **Upgrade note (breaking for object-form `year`).** Earlier releases accepted `year` in the schema but silently dropped it when building the crontab string, so it had no effect — a job with an object-form `year` ran every year. It is **now honored**. If you have such a job, upgrading changes its behavior: `year: "2017"` now pins the schedule to 2017 (a past year means the job stops firing). Honoring `year` also changes that job's [job-set fingerprint](Configuration-Reference), so during a rolling upgrade of a cluster the old and new binaries compute different `job_set_id`s for the identical config and will not treat each other as agreed peers until every node is upgraded (the same transient, self-healing drift as any config rollout; leader election stays at-most-once throughout). Jobs that do **not** use object-form `year` are unaffected: their fingerprint is byte-for-byte identical to before. To keep the old "runs every year" behavior, simply remove the `year` key.

## Timezone resolution

The clock used to evaluate a schedule is resolved by `JobConfig._resolve_timezone`, driven by two job options:

| Option     | Type | Default | Description |
|------------|------|---------|-------------|
| `utc`      | Bool | `true`  | When no `timezone` is set: `true` evaluates the schedule in UTC; `false` uses the host's naive local time. |
| `timezone` | Str  | *(unset; `None`)* | IANA timezone name (e.g. `America/Los_Angeles`). When set, it overrides `utc`. |

Resolution order (`timezone` wins):

1. If `timezone` is set, the job uses `ZoneInfo(timezone)`. An unknown name raises `ConfigError("unknown timezone: ...")`. The `utc` value is ignored in this case.
2. Else if `utc` is `true` (the default), the job uses `datetime.timezone.utc`.
3. Else (`utc: false`, no `timezone`) the resolved tzinfo is `None`, i.e. naive **host local time**.

The resolved value is a `datetime.tzinfo` (or `None`) stored on the job and passed to `get_now(job.timezone)` when the schedule is tested. Because `utc` is `true` by default, **schedules are interpreted in UTC unless you opt out.**

Timezone names are resolved via the standard-library `zoneinfo`, with the `tzdata` package providing the database. yacron2 depends on `tzdata>=2024.1` so resolution works on minimal/distroless images that lack a system zoneinfo database. (Prior to the migration documented in `HISTORY.md`, yacron2 used `pytz`; invalid timezones now raise `ConfigError` rather than being silently accepted.)

Local time:

```yaml
jobs:
  - name: nightly-local
    command: /usr/local/bin/nightly
    schedule: "27 19 * * *"   # 19:27 host local time
    utc: false
```

Explicit timezone:

```yaml
jobs:
  - name: nightly-la
    command: /usr/local/bin/nightly
    schedule: "27 19 * * *"   # 19:27 America/Los_Angeles
    timezone: America/Los_Angeles
```

`utc` and `timezone` are ordinary job options and can be set per job or in a [`defaults` block](Includes-and-Defaults). See the [Configuration Reference](Configuration-Reference) for where they sit among all options.

## How the scheduler ticks

The scheduler does not run a per-job timer. It wakes on a cadence, tests every job, and launches those that are due (`yacron2/cron.py`). The cadence **adapts to the finest resolution any enabled job needs**:

- `Cron._needs_subminute()` is `True` when any enabled job's schedule pins a `second` (its `has_seconds` flag). While that holds, `next_sleep_interval(subminute=True)` snaps to the next whole-second boundary; otherwise it snaps to the next minute boundary (`now.replace(second=0) + WAKEUP_INTERVAL`, `WAKEUP_INTERVAL = 1 minute`). Alignment is computed in **UTC** each iteration, so a slightly late wake still catches up to the boundary.
- Each pass (`_service_slots` → `spawn_jobs`) reads the clock **once** and passes that one instant to every job, so the "is it due" test and the per-slot de-dup key can never straddle a slot boundary and double-launch a single-slot job. For a `CronTab` job it evaluates `crontab.test(schedule_slot(job, now))`, where `schedule_slot` truncates that instant (in the job's timezone) to the job's resolution: the whole **second** for a second-level job, or the top of the **minute** otherwise.
- Because a second-level job makes the whole loop tick every second, minute-level jobs would be tested up to 60 times per minute. `spawn_jobs` therefore **de-duplicates per scheduling slot**: it records the last slot each job launched in (`_last_run_slot`) and skips a job whose current slot already fired. So a minute-level job still fires exactly once in its minute, and a second-level job exactly once per matching second — even if two ticks land in the same second.
- **Catch-up for overrun seconds.** In sub-minute mode, if one pass runs long — many simultaneous launches, or the once-a-minute config reload — and the clock advances past one or more whole seconds before the next pass, `_service_slots` services each skipped second too (evaluating every job against that second's slot), so a second-level job due in the gap still fires instead of being dropped. The catch-up is bounded by `CATCHUP_LIMIT` (10 s): a larger gap is treated as a stall/suspend/clock-jump and skipped past with a warning, rather than replayed as a burst of backdated launches. Minute-level jobs need no catch-up — their minute-truncated slot already absorbs any sub-minute overrun.
- **No spurious run for the period in progress at startup.** When yacron2 starts (or restarts) partway through a minute, it seeds `_last_run_slot` with the in-progress slot for every scheduled job, so a minute-level job whose minute is already under way does not fire immediately on the first tick — it first fires at the next matching boundary, exactly as in minute-only mode. (Without this, merely having a second-level job present would make every minute-level job fire ~1 s after a mid-minute restart.)

Implications:

- **Second-level schedules fire on time.** With a `second` field the daemon wakes every second and tests at second resolution, so `*/15 * * * * * *` really does fire at seconds 0/15/30/45.
- **No cost when unused.** If no enabled job specifies seconds, the loop keeps its once-a-minute cadence and per-minute config reload exactly as before. (When it *is* ticking per second, configuration reload / cluster / web housekeeping is still gated to run at most once per minute — only the job-firing test runs every second, and any second that gating pushes past is caught up as described above.)
- A job whose schedule matches a given slot fires at most once for that slot. If multiple instances would overlap, [`concurrencyPolicy`](Concurrency-and-Timeouts) governs the outcome.
- A job that is [disabled](Configuration-Reference) (`enabled: false`) returns `False` from `job_should_run` regardless of schedule and never fires, including at `@reboot`; a disabled second-level job also does not force the per-second cadence.

## Inspecting the next run

The [HTTP control API](HTTP-API) `/status` endpoint reports, per job, either `running`, `disabled`, or `scheduled` with a `scheduled_in` value. For `CronTab` jobs that value is `crontab.next(now=now, default_utc=job.utc)` evaluated in the job's timezone; for `@reboot` jobs it is the literal string `@reboot`. This is the recommended way to verify that a schedule resolves to the instant you expect.

See also: [Commands and Environment](Commands-and-Environment), [Concurrency and Timeouts](Concurrency-and-Timeouts), [Troubleshooting and FAQ](Troubleshooting).
