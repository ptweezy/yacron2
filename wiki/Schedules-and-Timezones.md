# Schedules and Timezones

Every job's `schedule` determines *when* it runs; `utc` and `timezone` determine *in which clock* the schedule is evaluated. This page documents the three accepted `schedule` forms, how the daemon wakes and tests them, and how the effective timezone is resolved.

## The `schedule` option

`schedule` is **required** on every job. The strictyaml schema accepts two YAML types for it (`yacron2/config.py`):

```
"schedule": Str()
| Map({
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

## Form 1: crontab string (5 fields)

A standard five-field crontab expression: `minute hour day-of-month month day-of-week`.

```yaml
jobs:
  - name: every-five-minutes
    command: echo hello
    schedule: "*/5 * * * *"
```

The string is passed verbatim to `CronTab(...)`; yacron2 does not pre-validate or rewrite it, so a malformed expression surfaces as whatever `parse-crontab` raises at config-load time. Quote the value in YAML: a bare `*/5 * * * *` is not valid YAML scalar syntax in all positions.

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

`_parse_schedule` builds a five-field crontab string from exactly these keys and feeds it to `CronTab`:

| Object key   | Crontab field | Default if omitted |
|--------------|---------------|--------------------|
| `minute`     | field 1       | `*`                |
| `hour`       | field 2       | `*`                |
| `dayOfMonth` | field 3       | `*`                |
| `month`      | field 4       | `*`                |
| `dayOfWeek`  | field 5       | `*`                |

The assembled string is `f"{minute} {hour} {day} {month} {dow}"`. All values are typed `Str()` in the schema, so write `minute: "0"`, not `minute: 0`. Although strictyaml will coerce an unquoted scalar to a string here, quoting is the documented convention and avoids surprises with values like `"7"`.

### Caveat: the `year` key is silently dropped

The schema declares `Opt("year"): Str()`, and `README.md` shows a schedule object that includes `year: 2017`. **`_parse_schedule` never reads `year`.** It assembles only a five-field string from `minute`/`hour`/`dayOfMonth`/`month`/`dayOfWeek`; `schedule_unparsed.get("year", ...)` is never called and `year` is not appended to the crontab string.

Consequences:

- A `year` key is accepted by the schema (no validation error) but has **no effect** on scheduling.
- The README example claiming a job "only on the specific date 2017-07-19" does **not** restrict by year: it runs every July 19th, every year, that matches the other fields.

This is a discrepancy between the schema/README and the implementation. Do not rely on `year`. If you need a one-shot run, use `@reboot` plus an external guard, or remove the job after it fires.

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

The scheduler does not run a per-job timer. It wakes on a fixed cadence and tests every job (`yacron2/cron.py`):

- `WAKEUP_INTERVAL = datetime.timedelta(minutes=1)`.
- `next_sleep_interval()` computes the time until the next minute boundary using **UTC**: `now.replace(second=0) + WAKEUP_INTERVAL`. The daemon therefore wakes aligned to the top of each wall-clock minute (in UTC), not at a fixed N-second period from startup.
- On each wake, `spawn_jobs` iterates all jobs and calls `job_should_run`. For a `CronTab` job it evaluates `crontab.test(get_now(job.timezone).replace(second=0))`: the current time **truncated to the minute** in the job's resolved timezone.

Implications:

- **Sub-minute schedules are meaningless.** The seconds component is zeroed before testing, and the daemon only wakes once per minute, so the finest effective resolution is one minute. A crontab expression cannot schedule anything more frequent than every minute via yacron2, regardless of what `parse-crontab` itself supports.
- A job whose schedule matches a given minute fires at most once for that minute (one test per wake). If multiple instances would overlap, [`concurrencyPolicy`](Concurrency-and-Timeouts) governs the outcome.
- The reload/sleep loop catches up to the boundary even if a wake is slightly late; alignment is recomputed each iteration from the current UTC time.
- A job that is [disabled](Configuration-Reference) (`enabled: false`) returns `False` from `job_should_run` regardless of schedule and never fires, including at `@reboot`.

## Inspecting the next run

The [HTTP control API](HTTP-API) `/status` endpoint reports, per job, either `running`, `disabled`, or `scheduled` with a `scheduled_in` value. For `CronTab` jobs that value is `crontab.next(now=now, default_utc=job.utc)` evaluated in the job's timezone; for `@reboot` jobs it is the literal string `@reboot`. This is the recommended way to verify that a schedule resolves to the instant you expect.

See also: [Commands and Environment](Commands-and-Environment), [Concurrency and Timeouts](Concurrency-and-Timeouts), [Troubleshooting and FAQ](Troubleshooting).
