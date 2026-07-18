# Schedules and Timezones

Every job's `schedule` determines *when* it runs; `utc` and `timezone` determine *in which clock* the schedule is evaluated. This page documents the three accepted `schedule` forms, how the daemon wakes and tests them, and how the effective timezone is resolved.

## The `schedule` option

`schedule` is **required** on every job. The strictyaml schema accepts two YAML types for it (`cronstable/config.py`):

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

- a `cronstable.cronexpr.CronTab` instance (a parsed crontab expression), or
- the literal string `"@reboot"`.

Any other value raises `ConfigError("invalid schedule: ...")`.

The crontab dialect is implemented by cronstable's own built-in engine (`cronstable/cronexpr.py`, no third-party dependency). Field syntax is the dialect cronstable has always accepted — ranges `1-5`, steps `*/5` (and `5/15`: start plus step, running to the field's end), lists `1,15,30`, case-insensitive names like `mon`/`jan` — not the system `cron(5)` man page. The notable rules:

- **Day-of-week** accepts `0`–`7`, with both `0` and `7` meaning Sunday; a range ending in `0` wraps its end to Sunday-as-7, so `sat-sun` and `6-0` mean Saturday+Sunday.
- **`L`** alone in day-of-month is the month's last day (`0 0 L * *`); `L<n>` in day-of-week is the month's *last* such weekday (`L5` = last Friday). No other field takes an `L`.
- **Business-day forms**: in day-of-month, `L-<n>` counts back from the month's final day (`L-3` = three days before it), `<n>W` is the weekday nearest day *n* within the same month, and `LW` is the month's last weekday; in day-of-week, `<d>#<n>` is the month's *n*-th such weekday (`5#3` = third Friday, `mon#1` = first Monday). Exact edge rules, examples, and Quartz porting notes: [Business-Day Schedules](Business-Day-Schedules).
- **`?`** standing alone in day-of-month or day-of-week reads as `*` (the Quartz spelling of "unrestricted"), so a seven-field Quartz expression (whose column layout matches cronstable's) parses verbatim. `?` anywhere else is an error.
- **Day-of-month AND day-of-week**: when both are restricted, a day must satisfy *both* (`0 0 13 * 5` fires only on Friday the 13th). Vixie cron fires when *either* matches; cronstable deliberately keeps the AND rule its schedules have always had. The [schedule linter](Schedule-Linting) warns whenever a schedule uses the combination.
- **`year`** accepts 1970–2099, and `next()` never searches past 2099: a schedule with no remaining occurrence (`year: "2020"`, or an impossible date like Feb 30) never fires. It is legal (a fixed past year is a working idiom for parking a job) but it is no longer silent: config load logs a `never-fires` warning, the scheduler warns once at seed time, and `/status` and `/jobs` report `never_fires` (see [Schedule Linting](Schedule-Linting)).

Earlier releases delegated this dialect to the third-party parse-crontab library; the built-in engine is behavior-compatible with it, vector-by-vector — see `tests/gen_cron_golden.py` and `tests/data/cron_golden.json` for the recorded compatibility corpus.

Expressions in **other dialects** fail with a hint instead of a bare field error: `#` or `W` used outside its one valid field names the right one (both are dialect now, in day-of-week and day-of-month respectively), Quartz's trailing-L (`5L`) points at this dialect's `L5` spelling, and a six-field seconds-first Quartz layout (`0 */5 * * * ?`) explains that cronstable's sixth field is a year and how to convert (append a trailing `*`).

Schedules do not have to live in YAML at all: cronstable also loads whole
classic crontab files (`*.crontab`, `*.cron`, or a file named `crontab`),
whose entries use this same field dialect plus the `@` nicknames and default
to UTC like every other cronstable schedule. See [Classic Crontabs](Classic-Crontabs).

## Form 1: crontab string (5, 6 or 7 fields)

A standard five-field crontab expression: `minute hour day-of-month month day-of-week`.

```yaml
jobs:
  - name: every-five-minutes
    command: echo hello
    schedule: "*/5 * * * *"
```

The string is passed verbatim to `CronTab(...)`. A malformed expression is caught and re-raised as `ConfigError("invalid schedule '...': ...")` at config-load time (naming the offending expression), so a bad field fails the reload cleanly rather than as an anonymous traceback. Quote the value in YAML: a bare `*/5 * * * *` is not valid YAML scalar syntax in all positions.

The engine reads **extra columns from the ends**, so the field count selects the dialect:

| Fields | Layout | Meaning |
|--------|--------|---------|
| 5 | `minute hour dayOfMonth month dayOfWeek` | the classic form; implicit second `0`, any year |
| 6 | `... dayOfWeek year` | adds a trailing **year** column (still second `0`) |
| 7 | `second minute hour dayOfMonth month dayOfWeek year` | adds a leading **second** column too |

So a seven-field string schedules at **second granularity** (see [Second-level schedules](#second-level-schedules) below), while a six-field string pins a `year`. A six-field string is *not* seconds — the extra column is the year.

Any field except the year also takes the Jenkins-style **`H` hash form** (`H`, `H(a-b)`, `H/n`, `H(a-b)/n`), which resolves to a slot hashed from the job's name: stable across restarts, reloads, and replicas, so a fleet of `H * * * *` jobs spreads across the hour without stampeding at `:00` while each job keeps one predictable fire time. See [Hashed Schedules](Hashed-Schedules) for the forms, the guarantees, and the rename caveat.

## Second-level schedules

cronstable can run jobs at second granularity. Give the schedule a `second` (via the seven-field string above, or the `second:` object key in [Form 3](#form-3-schedule-object)); both jobs below run every 15 seconds:

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

Behavior comes from `Cron.job_should_run` (`cronstable/cron.py`): on the first scheduler pass the `startup` flag is `True` and a `@reboot` job returns `True`; on every subsequent pass `startup` is `False`, so `@reboot` jobs return `False`. Conversely, `CronTab`-scheduled jobs return `False` during the startup pass and are only evaluated on later passes. There is no recurring "@reboot". To keep a long-running process alive, `README.md` recommends a `@reboot` schedule combined with `onFailure.retry.maximumRetries: -1` (retry forever), so cronstable relaunches the process whenever it exits/fails.

`"@reboot"` is the only `@`-keyword recognized by cronstable itself. Other shorthands are *not* intercepted by `_parse_schedule`; the cron engine accepts `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily` and `@hourly` (their classic five-field expansions), while `@midnight` is accepted only in [classic crontab files](Classic-Crontabs), whose loader rewrites it to `@daily`.

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

`schedule_object_to_crontab` (in `cronstable/config.py`) builds a crontab string from exactly these keys and `_parse_schedule` feeds it to `CronTab`:

| Object key   | Crontab field | Default if omitted |
|--------------|---------------|--------------------|
| `second`     | second        | *(omitted)*        |
| `minute`     | minute        | `*`                |
| `hour`       | hour          | `*`                |
| `dayOfMonth` | day-of-month  | `*`                |
| `month`      | month         | `*`                |
| `dayOfWeek`  | day-of-week   | `*`                |
| `year`       | year          | *(omitted)*        |

Only the columns you actually use are emitted, matching the engine's end-column rule from [Form 1](#form-1-crontab-string-5-6-or-7-fields):

- neither `second` nor `year` → a five-field line (`f"{minute} {hour} {day} {month} {dow}"`), exactly as before;
- `year` only → a six-field line with the trailing year column;
- `second` present → a full seven-field line (`year` defaults to `*` if unset).

So `{minute: "*/5"}` is byte-for-byte the five-field string `"*/5 * * * *"` (and the two spellings share a [job-set fingerprint](Configuration-Reference)), while `{second: "*/15"}` is the seven-field `"*/15 * * * * * *"`.

All values are typed `Str()` in the schema, so write `minute: "0"`, not `minute: 0`. Although strictyaml will coerce an unquoted scalar to a string here, quoting is the documented convention and avoids surprises with values like `"7"`.

### The `year` key

`year` restricts the schedule to specific years (the optional trailing column, 1970–2099). For example, this runs only during 2017:

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

Timezone names are resolved via the standard-library `zoneinfo`, with the `tzdata` package providing the database. cronstable depends on `tzdata>=2024.1` so resolution works on minimal/distroless images that lack a system zoneinfo database. (Prior to the migration documented in `HISTORY.md`, cronstable used `pytz`; invalid timezones now raise `ConfigError` rather than being silently accepted.)

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

The scheduler does not run a per-job timer, and it does not scan every job on a fixed tick either. It keeps a **next-fire index** and **sleeps until the soonest job is due** (`cronstable/cron.py`):

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
