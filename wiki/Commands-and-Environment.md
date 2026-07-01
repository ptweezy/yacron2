# Commands and Environment

This page documents how a job's command is invoked (shell vs. direct exec), how
its environment is constructed from `environment`, `env_file`, and inherited
defaults, and how privilege switching with `user`/`group` works. For schedule
syntax see [Schedules and Timezones](Schedules-and-Timezones); for how defaults
merge into jobs see [Includes, Defaults, and Multi-File Config](Includes-and-Defaults).

## Options

These options are members of each job (and may also be set in a `defaults` block).
Types and defaults are taken from the strictyaml schema and `DEFAULT_CONFIG`.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `command` | `Str` or `Seq(Str)` | required | The program to run. A string is run through a shell (on Windows, with the default empty `shell`, through the native command processor `cmd.exe` via `%ComSpec%`); a list is executed directly with no shell on every platform. |
| `shell` | `Str` | `/bin/sh` (POSIX) / empty (Windows) | Shell used when `command` is a string. The default is platform-specific: `/bin/sh` on POSIX, empty on Windows (an empty default routes a string command through the native command processor `%ComSpec%` / `cmd.exe`). To use PowerShell or another interpreter, set `shell:` explicitly, or pass `command` as a list (which bypasses the shell on every platform). See [Running on Windows](Running-on-Windows). |
| `environment` | `Seq(Map({key, value}))` | `[]` | Environment variables (each an item with `key` and `value`, both `Str`) added to the subprocess environment. |
| `env_file` | `Str` | `None` | Path to a `KEY=VALUE` file whose variables are merged into `environment`. |
| `user` | `Str` or `Int` | unset | User (login name or numeric uid) to run the subprocess as. POSIX-only; a job setting it raises a configuration error on Windows (see [Running on Windows](Running-on-Windows)). |
| `group` | `Str` or `Int` | unset | Group (group name or numeric gid) to run the subprocess as. POSIX-only; a job setting it raises a configuration error on Windows (see [Running on Windows](Running-on-Windows)). |

`command` is required on every job. `shell` has a platform-specific
schema/`DEFAULT_CONFIG` default: `/bin/sh` on POSIX and an empty string on
Windows (`DEFAULT_SHELL` in `yacron2/platform.py`), which makes a string command
run via `cmd.exe`. `environment` defaults to an empty list. `env_file`,
`user`, and `group` are optional (`Opt(...)` in the schema) and unset by default;
`environment` and `env_file` are also inheritable via `defaults`, but `user` and
`group` are job-only fields in the schema (they appear in `_job_defaults_common`,
so they are technically accepted in `defaults`, but resolution and the
root-required check happen per job).

## command and shell

`command` may be either a string or a list of strings, and the form determines
how the process is launched (`RunningJob.start` in `yacron2/job.py`):

- **String**: run through a shell.
  - If `shell` is set, yacron2 launches `asyncio.create_subprocess_exec` with
    argv `[shell, "-c", command]`. With the default `shell` on POSIX, that is
    `["/bin/sh", "-c", command]`.
  - If `shell` is falsy, yacron2 instead uses `asyncio.create_subprocess_shell`
    with the bare command string. On POSIX the default `/bin/sh` makes the
    `exec`-with-`-c` path the one that runs; on Windows the default `shell` is
    empty (`DEFAULT_SHELL` in `yacron2/platform.py`), so the
    `create_subprocess_shell` path is the default: the command is handed to the
    native command processor `cmd.exe` via `%ComSpec%`. Setting `shell:`
    explicitly on Windows takes the `exec`-with-`-c` path with that interpreter.
    See [Running on Windows](Running-on-Windows).
- **List**: executed directly with `asyncio.create_subprocess_exec`, with no
  shell involved. The argv is taken verbatim from the list; no word splitting,
  globbing, quoting, or `$VAR` expansion is performed.

In all cases the argv elements are encoded to bytes (`c.encode()`) before the
subprocess is created.

```yaml
jobs:
  # string form: run via /bin/bash -c "..."
  - name: via-shell
    command: echo "$HOME" && date
    shell: /bin/bash
    schedule: "*/5 * * * *"

  # list form: executed directly, no shell, no variable expansion
  - name: direct-exec
    command:
      - echo
      - foobar
    schedule: "*/5 * * * *"
```

### Launch failures

If the process cannot be launched at all (for example, a list-form `command`
whose executable does not exist, raising `FileNotFoundError`, or a
`SubprocessError`/`UnicodeEncodeError`), the launch error is logged,
`start_failed` is set, and the run is treated as a normal job failure with exit
code `127` rather than crashing the scheduler. See
[Failure Detection and Retries](Failure-Detection-and-Retries).

## environment

`environment` is a list of `{key, value}` maps; both `key` and `value` are
strings in the schema. When `environment` is non-empty, the subprocess
environment is built from the **full current process environment**
(`dict(os.environ)`), the PyInstaller fixup is applied (see below), and then each
configured variable is set/overwritten by key:

```python
env = dict(os.environ)
fixup_pyinstaller_env(env)
for envvar in self.config.environment:
    env[envvar["key"]] = envvar["value"]
```

If `environment` is empty (the default) **and** there is no `env_file`, no `env`
is passed to the subprocess, so it inherits yacron2's environment unchanged.

```yaml
jobs:
  - name: with-env
    command: printenv PATH
    schedule: "*/5 * * * *"
    environment:
      - key: PATH
        value: /bin:/usr/bin
```

### HOSTNAME injection

When the `yacron2.job` module is imported, if `HOSTNAME` is not already present
in `os.environ`, it is set to `socket.gethostname()`:

```python
if "HOSTNAME" not in os.environ:
    os.environ["HOSTNAME"] = gethostname()
```

Because this mutates the process environment, `HOSTNAME` is therefore present in
the inherited environment of every job. Note that the reporting templates'
`environment` variable is the constructed subprocess `env` dict (`self.env`),
which is only populated when the job has a non-empty `environment` or an
`env_file`; for a job with neither, `environment` is `None` in templates and
`{{ environment.HOSTNAME }}` renders empty. See
[Reporting (Mail, Sentry, Shell)](Reporting).

## env_file

`env_file` names a file of `KEY=VALUE` lines. Parsing is done by
`parse_environment_file` in `yacron2/config.py`:

- The file is opened as **UTF-8**.
- Each line is stripped of surrounding spaces and a trailing newline.
- Lines beginning with `#` and blank lines are **ignored**.
- A line without an `=` raises `ConfigError` (`"Invalid line in env_file: ..."`).
- Each remaining line is split on the **first** `=` into key and value; both key
  and value are then space-stripped. There is no quote handling and no `#`
  inline-comment handling beyond whole-line comments.
- `parse_environment_file` itself raises a bare `OSError` if the file cannot be
  opened; the caller `_merge_env_file` wraps that as
  `ConfigError("Could not load env_file: ...")`.

### environment overrides env_file

When `env_file` is set, it is merged at config-parse time (`_merge_env_file`):
the file is parsed into a dict, then the job's own `environment` entries are
applied on top, so **`environment` overrides `env_file` per key**. The merged
result replaces `self.environment` as a list of `{key, value}` items, which is
then applied to the subprocess as described in [environment](#environment)
above. (This means that when `env_file` is set, an `env` is always passed to the
subprocess even if `environment` was originally empty.)

```yaml
jobs:
  - name: with-env-file
    command: printenv
    schedule: "*/5 * * * *"
    env_file: /etc/yacron2/job.env
    environment:
      - key: LOG_LEVEL
        value: debug   # overrides LOG_LEVEL from job.env, if present
```

Example `env_file` contents:

```
# comment lines and blank lines are ignored

PATH=/bin:/usr/bin
LOG_LEVEL=info
```

## defaults.environment merge semantics

`environment` set in a `defaults` block merges into each job by key, not by list
concatenation. In `mergedicts` (`yacron2/config.py`), the `environment` list is
special-cased: the default's entries and the job's entries are folded into a
single key-to-value mapping, with the job's value winning on conflict, then
re-expanded to a `{key, value}` list. The result has **no duplicate keys**: a
job variable overrides the same-named default rather than appearing twice.

```yaml
defaults:
  environment:
    - key: PATH
      value: /bin:/usr/bin
    - key: LANG
      value: C
jobs:
  - name: job-a
    command: printenv
    schedule: "*/5 * * * *"
    environment:
      - key: PATH
        value: /usr/local/bin:/bin   # overrides the default PATH
        # LANG=C is inherited from defaults
```

The precedence chain for a variable is therefore: inherited
process environment (including injected `HOSTNAME`) < `env_file` < merged
`environment` (defaults then job, job winning). See
[Includes, Defaults, and Multi-File Config](Includes-and-Defaults) for how
`defaults` and includes are merged overall.

## user and group (privilege switching)

`user` and `group` request that the subprocess run under a different identity.
Resolution happens in `JobConfig._resolve_user_group` (`yacron2/config.py`):

> **Windows:** this whole feature is POSIX-only. Windows has no setuid/setgid
> model, so a job with `user` or `group` set raises the configuration error
> `Job <name>: changing user/group is not supported on Windows`
> (`config.py` `_resolve_user_group`). The Root requirement, Resolution rules,
> and Demotion ordering below all apply to POSIX only. See
> [Running on Windows](Running-on-Windows).

### Resolution rules

- **`user` as a name (`Str`)**: looked up with `getpwnam`. Sets `uid` from
  `pw_uid`, `gid` from `pw_gid` (the user's primary group), and the resolved
  login name (`pw_name`). A missing user raises `ConfigError("User not found: ...")`.
- **`user` as a number (`Int`)**: `uid` is set to the number directly. yacron2
  additionally looks the uid up with `getpwuid` to derive the user's **primary
  gid** and **login name**; if `group` was not given, the derived primary gid is
  used (so a numeric `user` without `group` does not silently keep yacron2's
  gid 0). If the uid is not in the passwd database, no login name or derived gid
  is available (and that is not an error here).
- **`group` as a name (`Str`)**: looked up with `getgrnam`; `gid` set from
  `gr_gid`. A missing group raises `ConfigError("Group not found: ...")`.
- **`group` as a number (`Int`)**: `gid` is set to the number directly.
- If only `user` is given, the group defaults to the main group of that user.
  An explicit `group` overrides any gid derived from `user`.

The resolved login name (`username`) matters for supplementary-group handling in
`_demote` (below); it is `None` when the user is unknown.

### Root requirement

If, after resolution, either `uid` or `gid` is set and the yacron2 process is not
running as root (`os.geteuid() != 0`), config parsing fails with:

```
Job <name> wants to change user or group, but yacron2 is not running as superuser
```

On POSIX, any use of `user` **or** `group` therefore requires yacron2 to run as
root. yacron2 needs no special privileges otherwise; `user`/`group` switching is
the only feature that requires root. (On Windows `user`/`group` are rejected
outright with a configuration error, so this root requirement is a POSIX-only
statement; see the Windows note above.)

```yaml
jobs:
  - name: as-www-data
    command: id
    schedule:
      minute: "*"
    captureStderr: true
    user: www-data        # group defaults to www-data's primary group
```

### Demotion ordering (_demote)

When `uid` or `gid` is set, `start` passes `preexec_fn=self._demote`, which runs
in the **child process** after fork while still privileged. The order is
deliberate and is required for safety:

1. **Supplementary groups first.** If both a login name and a gid are known,
   `os.initgroups(username, gid)` gives the child exactly the target user's
   supplementary groups. Otherwise `os.setgroups([])` drops all supplementary
   groups. A failure raises `RuntimeError("setgroups/initgroups: ...")`.
2. **Primary gid next.** If `gid` is set, `os.setgid(gid)`. A failure raises
   `RuntimeError("setgid: ...")`.
3. **uid last.** If `uid` is set, `os.setuid(uid)`. A failure raises
   `RuntimeError("setuid: ...")`.

Supplementary groups and the gid must be changed **before** `setuid`, because
once the process drops root via `setuid` it can no longer call
`setgroups`/`setgid`. Performing them in the other order would leave the child
holding root's supplementary group memberships (the classic
"forgot `setgroups()` before `setuid()`" privilege-escalation bug).

## PyInstaller environment fixup

`fixup_pyinstaller_env` is applied to the subprocess environment (only when an
`env` is being constructed, i.e. when `environment`/`env_file` produced
variables). It only does anything when running as a frozen PyInstaller binary
(`getattr(sys, "frozen", False)`):

```python
for env_var in "LD_LIBRARY_PATH", "LIBPATH":
    env[env_var] = env.get(f"{env_var}_ORIG", "")
```

PyInstaller's bootloader overwrites `LD_LIBRARY_PATH` and `LIBPATH` so the
bundled binary can find its own libraries, saving the caller's original values in
`LD_LIBRARY_PATH_ORIG`/`LIBPATH_ORIG`. This fixup restores those originals (or
empties the variable if there was no `_ORIG`) for the subprocess, so a child
process does not inherit the frozen interpreter's library paths. See
[Production and Container Deployment](Production-Deployment) for the frozen-binary
build. (Because the fixup is only applied when an `env` is constructed, jobs with
no `environment` and no `env_file` inherit the process environment as-is,
including any PyInstaller-clobbered values.)
