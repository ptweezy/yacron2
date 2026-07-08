# Classic Crontabs

cronstable's native configuration is YAML, but it also reads classic
(Vixie-style) crontabs, the `m h dom mon dow command` format described by
`man 5 crontab`. You can point `-c` at an existing crontab, drop one into a
config directory next to your YAML files, or pull one in with `include:`, and
every entry runs as a first-class cronstable job.

The contract is deliberately one-directional: **the crontab syntax is
supported, but the configuration around it is not emulated.** Each entry is
lowered into an ordinary job definition and then built exactly like a YAML
job, so it carries cronstable's standard defaults (UTC schedules, stderr and
exit-status failure detection, `concurrencyPolicy: Allow`, no retries, and so
on), not a re-creation of cron's environment, mailer, or quirks. The
[deviations](#deviations-from-cron) section below lists every place that
matters and what to do about each. All behavior on this page is implemented
in `cronstable/crontabs.py` and `cronstable/config.py`.

## How a crontab is recognised

The file *name* decides whenever it can:

| Name | Treated as |
| --- | --- |
| `*.crontab`, `*.cron` (case-insensitive) | classic crontab |
| a file named exactly `crontab` (case-insensitive), e.g. a `crontab -l > crontab` export | classic crontab |
| `*.yml`, `*.yaml` | YAML, always; never content-sniffed |
| anything else, passed explicitly with `-c` or pulled in with `include:` | content-sniffed (below) |
| anything else, inside a config directory | skipped, as before |

Name recognition also fires on `/etc/crontab`, but note that *system*
crontabs (`/etc/crontab`, `/etc/cron.d`) carry a sixth user column that
cronstable does not parse; only the five-field *user*-crontab format runs as-is
(see [deviations](#deviations-from-cron)).

In a config directory, crontab-named files load right alongside
`*.yml`/`*.yaml` files, in the same name-sorted order, and the usual skip
rule still applies: entries whose name starts with `_` or `.` are ignored
(see [Includes, Defaults, and Multi-File Config](Includes-and-Defaults)).

The content sniff exists so that `cronstable -c /var/spool/cron/crontabs/root`
just works even though the file has no telling name. It looks at the first
meaningful (non-blank, non-comment) line only, and only accepts shapes no
valid cronstable YAML document can open with: a `NAME=value` assignment, a line
starting with `@`, or five valid cron fields followed by a command. Anything
inconclusive is parsed as YAML, so extensionless YAML configs keep their
exact pre-existing behavior. When in doubt, name the file `*.crontab` and
the question never arises.

A YAML config can also pull a crontab in directly:

```yaml
include:
  - legacy.crontab
```

`cronstable -v -c legacy.crontab` validates a crontab the same way it validates
YAML; parse errors are reported with the offending `file:line`. A runnable
example mixing a crontab with a YAML file (and the web dashboard) ships in
the repository as `example/crontab`.

## Accepted syntax

The user-crontab format from `man 5 crontab`:

```crontab
# comments and blank lines are ignored
SHELL=/bin/bash
PATH = /usr/local/bin:/usr/bin:/bin
MAILTO="ops@example.com"

# m h dom mon dow command
*/15 * * * *  /usr/local/bin/backup --incremental
30 4 * * mon-fri  /usr/local/bin/report --daily
0 0 1 jan *  /usr/local/bin/happy-new-year

CRON_TZ=Europe/Berlin
0 6 * * *  echo "6am in Berlin, not UTC"

@daily  /usr/local/bin/rotate-logs
@reboot  echo "cronstable started"
0 0 * * *  pg_dump mydb > /backup/mydb-$(date +\%F).sql
```

Specifically:

- **Entries:** five time fields, then the rest of the line is the command.
  Ranges (`1-5`), steps (`*/5`), lists (`1,15,30`), and month/weekday names
  (`jan`, `mon-fri`) are supported; day-of-week accepts both `0` and `7` as
  Sunday. The field dialect is the same built-in cron engine that parses
  YAML `schedule` strings, so both formats accept identical expressions
  (see [Schedules and Timezones](Schedules-and-Timezones)).
- **Nicknames:** `@reboot`, `@yearly`, `@annually`, `@monthly`, `@weekly`,
  `@daily`, `@midnight`, `@hourly`. `@midnight` is rewritten to its synonym
  `@daily` at load time; `@reboot` behaves exactly like a YAML `@reboot`
  schedule (runs once at startup, and understands leadership under
  [clustering](Clustering-and-Leader-Election)).
- **Environment assignments:** `NAME = value` lines apply to the entries
  *below* them, exactly as in cron; a later reassignment affects later
  entries only. Values may be single- or double-quoted to preserve leading
  or trailing blanks. All assignments are exported to the job's
  environment, on top of the environment cronstable itself runs with.
- **Escaped percent signs:** `\%` in a command becomes a literal `%`, so the
  ubiquitous `date +\%F` idiom works unchanged. An *unescaped* `%` is a
  load-time error; see [deviations](#deviations-from-cron).

Two assignments are interpreted as well as exported:

| Variable | Effect |
| --- | --- |
| `SHELL` | Sets the job's `shell` option, so the command runs as `$SHELL -c "command"`, as in cron. Without it, cronstable's standard default applies (`/bin/sh` on POSIX, the native command processor on Windows). |
| `CRON_TZ` | Sets the job's `timezone` option: schedules below it are evaluated in that IANA zone (cronie's `CRON_TZ` semantics). An unknown zone is a load-time error at the assignment's line. |

## What each entry becomes

Every entry is lowered to a plain job definition, merged over the same
built-in defaults as a YAML job (`DEFAULT_CONFIG`), and validated by the
same `JobConfig` code path. From that point on, nothing downstream can tell
the two formats apart: crontab jobs appear in the
[web dashboard and HTTP API](HTTP-API), participate in the
[job-set fingerprint](Configuration-Reference) and
[clustering](Clustering-and-Leader-Election), and report failures like any
other job.

The defaults that matter most for a migrated crontab. Each row names a
behavior (with the per-job YAML option behind it), what the entry does now
that cronstable runs it, and what the same line did under classic cron:

| Behavior | Under cronstable | Under classic cron |
| --- | --- | --- |
| time basis (`utc` / `timezone`) | **UTC** (set `CRON_TZ` to change) | local time |
| failure detection (`failsWhen`) | non-zero exit **or any stderr output** is a failure | exit status ignored; output mailed |
| output (`captureStderr` / `captureStdout`) | stderr is read by cronstable (for failure detection, reports, and the dashboard log tail) and re-emitted into its log with a `[<job> stderr]` prefix; stdout is not read: it flows straight through to cronstable's own stdout, visible there but not to reports or the dashboard | both mailed to `MAILTO` |
| concurrency (`concurrencyPolicy`) | `Allow` (overlapping runs permitted) | overlapping runs permitted |
| retries (`onFailure.retry`) | none | none |
| user (`user`) | the user cronstable runs as | the crontab's owner |

There is no way to override these from inside a crontab (the format has no
vocabulary for it); that is by design. A crontab gets you *running* with
sensible, predictable standards, and the moment an entry needs reporting,
retries, timeouts, or any other per-job option, move that entry to YAML,
where every option in the [Configuration Reference](Configuration-Reference)
is available. Note that a `defaults:` section in a sibling or including YAML
file does **not** apply to crontab entries, for the same reason per-file
defaults never cross files (see
[Includes, Defaults, and Multi-File Config](Includes-and-Defaults)).

### Job names

Entries are named `<file name>:<line number>`, for example
`legacy.crontab:9`. The name is unique within a file, stable across reloads
while the file is unchanged, shows up in logs, the dashboard, and the HTTP
API like any other job name, and points you straight at the source line.
Inserting or removing lines renumbers the entries below the edit, which
cronstable treats the same way as renaming a YAML job (the old name's run
history ends and the new name starts fresh).

## Deviations from cron

Each of these is a deliberate choice in favor of cronstable's standard
behavior, made loudly rather than silently:

- **Schedules default to UTC, not local time.** This is cronstable's standard
  and by far the least surprising choice in containers. Put `CRON_TZ=<zone>`
  above the entries that need a specific zone.
- **`MAILTO` does not send mail.** It is exported to the job's environment
  but not interpreted; a crontab has nowhere to declare an SMTP server, and
  cronstable's failure handling is richer than mail-on-output. Configure
  [Reporting](Reporting) in YAML if you want failure mail. Failures are
  always visible in logs, the dashboard, and the HTTP API regardless:
  cronstable reads each entry's stderr for exactly that purpose, while stdout
  is left unread and flows straight to cronstable's own stdout (see the table
  above).
- **An unescaped `%` is a load-time error, not stdin.** In cron, `%` ends
  the command and everything after it is fed to the command as standard
  input. cronstable does not feed stdin to jobs, and the silent alternatives
  are both worse: running the command without input it expects, or leaving
  the input text on the command line for the shell to execute. The escaped
  form `\%` (the common case, e.g. `date +\%F`) works exactly as in cron.
  For genuine stdin data, use a YAML job with a heredoc or file redirect.
- **The system-crontab user column is not parsed.** `/etc/crontab` and
  `/etc/cron.d` files carry a sixth field naming the user to run as. A
  parser cannot reliably tell that column from the first word of a command,
  so cronstable reads the five-field user-crontab format only; a user column
  would land at the start of the command (and typically fail with
  `root: command not found` at run time). Remove the column, or move the
  entry to YAML and use the `user:` option
  ([Commands and Environment](Commands-and-Environment)).
- **Cron's implicit environment is not injected.** cron gives jobs a
  near-empty environment with `LOGNAME`, `HOME`, and `SHELL=/bin/sh`
  defaults. cronstable jobs inherit cronstable's own environment plus the
  crontab's assignments, the same rule as YAML jobs. A crontab that relied
  on cron's minimal `PATH` behaves the same once it sets `PATH=` itself, as
  most already do.

## Migrating to YAML

When an entry outgrows the crontab format, its YAML equivalent is mechanical.
This entry:

```crontab
CRON_TZ=Europe/Berlin
SHELL=/bin/bash
30 4 * * mon-fri  /usr/local/bin/report --daily
```

is exactly:

```yaml
jobs:
  - name: report
    command: /usr/local/bin/report --daily
    shell: /bin/bash
    schedule: "30 4 * * mon-fri"
    timezone: Europe/Berlin
```

plus whatever per-job options prompted the move. Both forms can coexist in
one config directory for as long as the migration takes.
