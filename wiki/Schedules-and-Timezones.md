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

The scheduler does not run a per-job timer, and it does not scan every job on a fixed tick either. It keeps a **next-fire index** and **sleeps until the soonest job is due** (`yacron2/cron.py`):

- **The next-fire index.** Every enabled `CronTab` job carries the instant it next fires — an aware **UTC** datetime — in `Cron._next_fire`, mirrored into the `_fire_heap` min-heap. Each instant is computed by `crontab.next()` in the job's *own* frame (its `timezone`, or the system-local zone when it has none) and stored back in UTC, so a job's DST offset is handled where it applies and the heap still orders everything on one absolute timeline. `@reboot` and disabled jobs are not in the index.
- **Sleep until the soonest fire.** Each iteration sleeps until the earliest instant in the heap, capped at the next whole UTC minute so housekeeping (below) still runs about once a minute. On wake, only the jobs whose instant has arrived are serviced (`_due_names` pops them); nothing else is touched. An idle wake over a large fleet is an O(1) heap peek, and a wake with a due cohort does crontab work only for that cohort — cost scales with **jobs due**, not jobs configured.
- **Structural, forward-only de-duplication.** A fired slot cannot fire twice because advancing the index moves the job's next fire strictly *past* the slot it just fired (`_advance` → `_set_next_fire`). There is no per-tick "did this already fire?" check: `_last_run_slot` is retained only for status/introspection and no longer gates launching. So a minute-level job fires exactly once in its minute and a second-level job exactly once per matching second, however often the loop happens to wake.
- **Immune to clock steps.** The sleep length is derived from the wall clock but realized against the event loop's **monotonic** clock (`asyncio.wait_for`), and firing compares the wall clock against the fixed, forward-only instants in the heap. So a wall-clock/NTP step is absorbed on the next wake: a step **backward** simply defers the pending fire (it is not re-fired), and a step **forward** does not unleash a catch-up storm (see below).
- **Bounded catch-up.** If a job's due instant is no more than `CATCHUP_LIMIT` (10 s) behind — a slow pass, e.g. many simultaneous launches or the once-a-minute config reload — `_advance` replays each missed occurrence in the window, so a frequently scheduled job overrun by a slow pass is not silently dropped. A larger gap is treated as a stall/suspend/forward-clock-jump: the job resumes at the current slot (firing once only if *now* itself matches) and resyncs, in O(1), with a warning — never enumerating the missed window. This is cron's no-catch-up-after-an-outage rule, and it applies uniformly to minute- and second-level jobs.
- **No spurious run for the period in progress at startup.** At startup the index is seeded **strictly-future** (the first boundary *after* the start instant) for every scheduled job, so a job whose minute or second is already under way does not fire immediately — it first fires at the next matching boundary. `@reboot` jobs are unaffected and still fire once at startup.
- **Reload keeps the index in step.** On a config reload, a job whose schedule is unchanged **keeps** its existing next-fire (so a reload landing on that job's own boundary minute cannot recompute a strictly-future fire and skip it); a job whose schedule changed, or a newly added one, is reseeded strictly-future; a removed or now-disabled job is dropped.

Implications:

- **Second-level schedules fire on time.** With a `second` field the loop wakes exactly at each second boundary the schedule pins, so `*/15 * * * * * *` really does fire at seconds 0/15/30/45.
- **No cost when unused.** With no second-level job the effective cadence is still about once a minute. Housekeeping — config reload, cluster/web upkeep, logging — is gated to run at most once per wall-clock minute (`Cron._needs_subminute()` is `True` only while some enabled job pins a `second`), so a second-level job that wakes the loop many times a minute does not reread and reparse the config on every wake.
- A job whose schedule matches a given slot fires at most once for that slot. If multiple instances would overlap, [`concurrencyPolicy`](Concurrency-and-Timeouts) governs the outcome.
- A job that is [disabled](Configuration-Reference) (`enabled: false`) returns `False` from `job_should_run` regardless of schedule and never fires, including at `@reboot`; a disabled second-level job is also absent from the next-fire index, so it adds no scheduling cost.

## Inspecting the next run

The [HTTP control API](HTTP-API) `/status` endpoint reports, per job, either `running`, `disabled`, or `scheduled` with a `scheduled_in` value. For `CronTab` jobs that value is `crontab.next(now=now, default_utc=job.utc)` evaluated in the job's timezone; for `@reboot` jobs it is the literal string `@reboot`. This is the recommended way to verify that a schedule resolves to the instant you expect.

See also: [Commands and Environment](Commands-and-Environment), [Concurrency and Timeouts](Concurrency-and-Timeouts), [Troubleshooting and FAQ](Troubleshooting).
