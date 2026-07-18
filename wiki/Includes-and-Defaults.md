# Includes, Defaults, and Multi-File Config

This page documents how cronstable loads configuration: the `-c` argument as a
single file or a directory, the `defaults` section and its merge precedence, the
`include` directive, and the special list-merge rules that govern how job
settings inherit from defaults. All behavior here is implemented in
`cronstable/config.py`.

## Config loading entry points

cronstable resolves the `-c`/`--config` argument (default `/etc/cronstable.d` on
POSIX, `%APPDATA%\cronstable` on Windows, falling back to `~` if APPDATA is
unset; see [CLI Reference](CLI-Reference) and [Running on Windows](Running-on-Windows))
through `parse_config(config_arg)`:

- If `config_arg` is a directory, it is loaded by `_parse_config_dir`
  (directory mode, below).
- Otherwise it is loaded as a single file by `parse_config_file`. `parse_config`
  wraps that call in a `try/except OSError`, so a missing or unreadable file
  (`OSError`) is re-raised as a `ConfigError` and the CLI reports it cleanly
  rather than surfacing a bare `OSError`.

A single file is read as UTF-8, validated against `CONFIG_SCHEMA` with
strictyaml, and parsed by `parse_config_string`. The top level accepts an empty
document (`EmptyDict()`) or a mapping with the optional keys `defaults`, `jobs`,
`web`, `include`, and `logging` (all `Opt(...)`, none is required). See the
[Configuration Reference](Configuration-Reference) for the per-job and `web`
schemas, and [Logging Configuration](Logging-Configuration) for the `logging`
schema.

## Directory mode

When `-c` points at a directory, `_parse_config_dir` enumerates the directory's
entries with `os.scandir` and processes them in sorted filename order (sorting
makes job ordering and "first config found" error messages deterministic rather
than dependent on filesystem order).

For each entry, the name is split into base and extension:

- The entry is **skipped** if the first character of its base name is `_` or
  `.`. This lets you keep shared include fragments (e.g. `_inc.yaml`) and dotted
  files in the same directory without them being loaded as standalone configs.
- The entry is **skipped** unless its extension is `.yml` or `.yaml`, or its
  name marks it as a classic crontab (`*.crontab`, `*.cron`, or a file named
  `crontab`; see [Classic Crontabs](Classic-Crontabs)).

Each remaining file is parsed independently with `parse_config_file`, then its
results are **aggregated** across the directory:

| Aggregate | Behavior across files |
| --- | --- |
| `jobs` | Concatenated in sorted-filename order (`jobs.extend(...)`). |
| `defaults` | Merged across files via `mergedicts` into a single directory-wide `job_defaults` (used only as the returned `job_defaults`; see the caveat below). |
| `web` | At most one. A second file containing a `web` block raises `ConfigError("Multiple 'web' configurations found: first in <file>, now in <file>")`. |
| `logging` | At most one. A second file containing a `logging` block raises `ConfigError("Multiple 'logging' configurations found: ...")`. |

Per-file parse errors are collected (keyed by path) and, if any occurred,
raised together as a single `ConfigError` whose message joins the individual
errors with `\n---`. An empty directory, or one where every entry is skipped,
yields an empty `CronstableConfig` (empty `jobs`, no `web`, empty `job_defaults`,
no `logging`) rather than an error.

### Defaults are scoped per YAML file in directory mode

This is the most important caveat of directory mode: **the `defaults` section in
each file applies only to the jobs defined in that same file.** Each file is
parsed by its own `parse_config_string` call, where that file's `defaults` are
merged into its own jobs before the files are aggregated. The directory-wide
`job_defaults` returned by `_parse_config_dir` is the merge of every file's
defaults, but it is *not* applied retroactively to jobs that were already
constructed in other files. To share defaults across files in a directory, put
them in an include fragment and `include` it from each file (see below).

## The `defaults` section

A `defaults` mapping supplies inherited values for the jobs defined alongside
it. It accepts the same options as a job (`_job_defaults_common`) except for the
job-only required keys `name`, `command`, and `schedule`. Every option in
`defaults` is optional. Jobs may override any inherited value.

```yaml
defaults:
  shell: /bin/bash
  utc: false
  environment:
    - key: PATH
      value: /bin:/usr/bin
jobs:
  - name: test-01
    command: echo "foobar"   # inherits /bin/bash, utc:false, PATH
    schedule: "*/5 * * * *"
  - name: test-02
    command: echo "zbr"
    shell: /bin/sh           # overrides the inherited shell
    schedule: "*/5 * * * *"
```

The `shell` field works on every OS; the `/bin/...` paths above are POSIX
examples. On Windows you'd set e.g. `shell: powershell`, leave it empty to use
cmd.exe (via %ComSpec%), or pass `command` as a list to bypass the shell
entirely (see [Running on Windows](Running-on-Windows)).

### Merge precedence

Within a single parsed file (`parse_config_string`), each job's effective
configuration is built by successively merging with `mergedicts`, in this order
(later wins):

1. `DEFAULT_CONFIG`: the built-in defaults (e.g. `shell: /bin/sh` on POSIX
   (empty on Windows, which routes a string `command` through %ComSpec%/cmd.exe;
   see [Running on Windows](Running-on-Windows)), `captureStderr: true`,
   `utc: true`, `killTimeout: 30`; full list in the
   [Configuration Reference](Configuration-Reference)).
2. **Included files' defaults**: the `defaults` blocks of any files named by
   this file's `include` directive, merged together in include order.
3. **This file's `defaults` block.**
4. **Per-job overrides**: the keys set on the individual job.

Each job dict is `mergedicts(defaults, config_job)`, where `defaults` is
`mergedicts(mergedicts(DEFAULT_CONFIG, included_defaults), this_files_defaults)`.
The resulting per-job dict is then passed to `JobConfig`, which validates it
(numeric ranges, timezone, user/group). On Windows, user/group switching is
unsupported: a job with `user` or `group` set raises a configuration error
(`Job <name>: changing user/group is not supported on Windows`); see
[Running on Windows](Running-on-Windows). The merged `defaults` (steps 1–3) is
also returned as the file's `job_defaults`.

## Merge semantics (`mergedicts`)

`mergedicts(dict1, dict2)` is a recursive deep merge where, on key collision,
`dict2` takes precedence. Its rules:

| Case | Behavior |
| --- | --- |
| Both values are mappings | Recurse (deep merge). |
| `dict1` value is a mapping, `dict2` value is `None` | Keep `dict1`'s mapping (treat `None` as "not overridden"). |
| Both values are lists, key is `environment` | **Merge by key** (see below). |
| Both values are lists, key is `secrets` | **Merge by name** (see below). |
| Both values are lists, key is `fingerprint` | **Replace** with `dict2`'s list (no concatenation). |
| Both values are lists, any other key | **Concatenate** (`v1 + v2`). |
| Otherwise (scalars, type mismatch) | Take `dict2`'s value. |
| Key only in `dict1` | Keep `dict1`'s value. |
| Key only in `dict2` | Keep `dict2`'s value. |

### `environment` merges by key

`environment` is a list of `{key, value}` mappings. When a job and a default
both define `environment`, they are merged into a dictionary keyed by `key`
(default entries first, then the job's), so a job's variable **overrides** the
default with the same name instead of producing two list entries with the same
key. This is a behavior change from yacron (which concatenated the lists,
yielding duplicate-keyed entries). See [Commands and Environment](Commands-and-Environment)
for `environment` and `env_file`.

```yaml
defaults:
  environment:
    - key: FOO
      value: foo
    - key: BAR
      value: bar
jobs:
  - name: test-01
    command: env
    schedule: "* * * * *"
    environment:
      - key: FOO        # overrides the default FOO -> single FOO=xpto entry
        value: xpto
      - key: ZBR        # added alongside the inherited BAR
        value: blah
```

The job above runs with `FOO=xpto`, `BAR=bar`, `ZBR=blah`.

### `secrets` merges by name

Run-scoped `secrets` are a list of `{name, ...}` mappings and merge the same
way `environment` does: entries are keyed by `name` (default entries first,
then the job's), so a job's secret **overrides** a same-named default instead
of staging two secrets under one name. The job's entry wins wholesale,
including its `value`/`fromFile`/`fromEnvVar` source. See
[Durable State](Durable-State#run-scoped-secrets) for what a secret block
does and how a job reads one.

### `fingerprint` replaces, does not append

The Sentry `fingerprint` (a list of strings, default
`["cronstable", "{{ environment.HOSTNAME }}", "{{ name }}"]`) is a
replace-not-append setting: a job or `defaults` block that supplies its own
`fingerprint` overrides the default list entirely. Plain list concatenation
would silently prepend the three default entries, making custom Sentry issue
grouping impossible. See [Reporting](Reporting) for the Sentry reporter.

`environment`, `secrets`, and `fingerprint` are the only exceptions; all
other list-valued options concatenate.

## The `include` directive

`include` is an optional list of file paths. Each path is resolved
relative to the directory of the **including** file
(`os.path.join(os.path.dirname(path), include)`). An included file may be
YAML or a [classic crontab](Classic-Crontabs) (same recognition rules as
`-c`); note that crontab entries always carry the built-in defaults, so, like
any included file's jobs, they do not pick up the including file's
`defaults`. Included files are parsed recursively with `parse_config_file`,
and their results are merged into the including file as follows:

- **Jobs** from included files are appended to this file's job list. Crucially,
  these jobs arrive **already fully constructed** by the included file's own
  `parse_config_string`, so they carry only **their own file's defaults** (and
  `DEFAULT_CONFIG`). A `defaults` block in the *including* file does **not**
  retro-apply to jobs that came from an included file.
- **Defaults** from included files are merged together (in include order) into
  `inc_defaults_merged`, which is folded into the merge precedence at step 2
  above, i.e. included defaults affect only this file's **inline** jobs, not
  the included files' own jobs.
- **`web`** from an included file is adopted if this file has none; if both
  define `web`, parsing raises `ConfigError("multiple web configs")`.
- **`logging`** from an included file is adopted if this file has none; if both
  define `logging`, parsing raises `ConfigError("multiple logging configs")`.

The intended use is to put common definitions (reporting defaults, shell,
environment, etc.) in a fragment named so directory mode skips it (e.g. with a
leading underscore), and `include` it from each real config file. This mirrors
`example/adhoc.cronstable.d`:

`_inc.yaml` (skipped by directory mode; provides shared `defaults`, `web`, and
`logging`):

```yaml
defaults:
  shell: /bin/bash
  concurrencyPolicy: Allow
  environment:
    - key: FOO
      value: foo
    - key: BAR
      value: bar
```

`test.yaml` (a real config file that includes the fragment):

```yaml
include:
  - _inc.yaml
jobs:
  - name: test-01
    command: echo "foobar"   # inherits shell:/bin/bash and FOO/BAR from _inc.yaml
    schedule: "@reboot"
    captureStdout: true
```

Because `_inc.yaml` contains only `defaults` (no `jobs`), its defaults flow into
`test.yaml`'s inline jobs through the include-defaults merge. If `_inc.yaml`
also defined jobs, those jobs would carry `_inc.yaml`'s own defaults and would
*not* pick up `test.yaml`'s `defaults`.

### Include cycle detection

`parse_config_file` tracks visited files by absolute path in a `_seen` set that
is threaded through the recursive include parse. A file that includes itself,
directly or transitively, raises a clear
`ConfigError("include cycle detected at <path>")` instead of recursing until a
`RecursionError`. The `_seen` set is scoped to a single
top-level `parse_config_file` call (it starts empty), so two independent files
that both include a common fragment are **not** flagged as a cycle: diamond
includes are allowed; only true cycles are rejected.

In directory mode, `_seen` is *not* shared across the directory's files: each
file in the directory is parsed with its own fresh cycle-detection state.

## Related pages

- [Configuration Reference](Configuration-Reference): full option schema and
  built-in defaults.
- [Commands and Environment](Commands-and-Environment): `environment` and
  `env_file`.
- [Durable State](Durable-State#run-scoped-secrets): run-scoped `secrets`.
- [Reporting](Reporting): Sentry `fingerprint` and the report blocks.
- [Logging Configuration](Logging-Configuration): the `logging` section.
- [CLI Reference](CLI-Reference): the `-c` argument and `--validate-config`.
