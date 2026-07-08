# Migration from yacron

yacron2 is a fork of [yacron](https://github.com/gjcarneiro/yacron) continuing
from upstream 0.19. This page enumerates every breaking change introduced in
yacron2 relative to yacron 0.19, and gives an operator checklist for
moving an existing yacron 0.19 deployment to yacron2.

yacron2 carries forward all of upstream yacron's functionality: scheduling,
reporting, retries, concurrency, metrics, and the HTTP API all behave as in
0.19 except where a breaking change below says otherwise, and yacron2 adds
new options on top (e.g. the `web.authToken` and `web.socketMode` keys;
`web.socketMode` only applies to `unix://` listeners and is therefore
irrelevant on Windows, where unix-socket listeners are unsupported (see
[Running on Windows](Running-on-Windows))). The
breaking changes below are packaging renames, an interpreter floor, two
security-relevant default/behavior changes, one merge semantics change, and
dependency-pin changes. No per-job YAML key was removed, renamed, or retyped;
the only user-visible config change is the mail `validate_certs` default flipping
from `False` to `True`.

## Breaking changes

### Command and distribution renamed `yacron` -> `yacron2`

| Old (yacron 0.19) | New (yacron2) |
| --- | --- |
| `pip install yacron` | `pip install yacron2` |
| `yacron` command | `yacron2` command |
| `import yacron` | `import yacron2` |
| entry point `yacron.__main__:main` | `yacron2.__main__:main` |

The console script is declared as `yacron2 = "yacron2.__main__:main"` in
`pyproject.toml`. The internal logger name and the argparse program name are now
`yacron2`, so CLI error and `--version` output read `yacron2`. See the
[Command-Line Reference](CLI-Reference).

### Default config directory `/etc/yacron.d` -> `/etc/yacron2.d`

The built-in default for `-c`/`--config` is now `/etc/yacron2.d`
(`CONFIG_DEFAULT` in `yacron2/__main__.py`). Operators who relied on the old
default path must move their configuration directory, or pass the old path
explicitly with `-c /etc/yacron.d`. The published container image and the
example Dockerfile read from `/etc/yacron2.d`. See
[Includes, Defaults, and Multi-File Config](Includes-and-Defaults).

This default is platform-specific: it is `/etc/yacron2.d` only on POSIX. On
Windows the default is instead `%APPDATA%\yacron2` (e.g.
`C:\Users\<you>\AppData\Roaming\yacron2`), falling back to the user profile
(`~`) when `APPDATA` is unset. The "config not found -> exit 1" special case
keys off whichever platform default applies (not the literal `/etc/yacron2.d`
string). See [Running on Windows](Running-on-Windows).

### Minimum Python is now 3.10

`requires-python` is `>=3.10`; Python 3.10 through 3.14 are supported. Python
3.9 and earlier are no longer supported. If your host runs an older
interpreter, use the self-contained binary (which embeds Python) or the
container image instead of a `pip` install. `pip install yacron2`
and the self-contained binaries also work natively on Windows (amd64/arm64);
see [Running on Windows](Running-on-Windows). See [Installation](Installation).

### Reporter shell environment variables renamed `YACRON_*` -> `YACRON2_*`

The shell reporter exports its job-state variables with the `YACRON2_` prefix.
Any `onFailure`/`onSuccess`/`onPermanentFailure` shell-reporter scripts that
reference the old `YACRON_*` names must be updated. The current variable set
exported by the shell reporter is:

| Variable | Description |
| --- | --- |
| `YACRON2_FAIL_REASON` | Failure reason string |
| `YACRON2_FAILED` | `"1"` if the job failed, `"0"` otherwise |
| `YACRON2_JOB_NAME` | Job name |
| `YACRON2_JOB_COMMAND` | Job command |
| `YACRON2_JOB_SCHEDULE` | Job schedule (unparsed) |
| `YACRON2_RETCODE` | Process exit code as a string |
| `YACRON2_STDERR` | Captured standard error |
| `YACRON2_STDOUT` | Captured standard output |
| `YACRON2_STDERR_TRUNCATED` | Whether stderr was truncated |
| `YACRON2_STDOUT_TRUNCATED` | Whether stdout was truncated |

The `*_TRUNCATED` variables are exported by the shell reporter
(`yacron2/job.py`) but are not listed in `README.md`; the other eight match the
README. See [Reporting (Mail, Sentry, Shell, Webhook)](Reporting).

### Mail `validate_certs` now defaults to `True`

SMTP TLS certificate validation is now enabled by default. In the strictyaml
schema `validate_certs` is `Opt("validate_certs"): Bool()` (optional), and the
mail reporter default in `_REPORT_DEFAULTS` is `True`. The value is passed
straight through to `aiosmtplib.SMTP(validate_certs=...)`.

Delivery to SMTP servers with self-signed or otherwise invalid certificates
that previously worked silently under yacron 0.19 (where validation was off)
will now fail. Set `validate_certs: false` explicitly to restore the old
behavior:

```yaml
jobs:
  - name: test-01
    command: echo "hello"
    schedule: "@reboot"
    onFailure:
      report:
        mail:
          from: example@foo.com
          to: example@bar.com
          smtpHost: smtp.internal
          tls: true
          validate_certs: false
```

See [Reporting (Mail, Sentry, Shell, Webhook)](Reporting).

### Privilege drop: supplementary groups and derived gid

The per-job `user`/`group` switch now performs the privilege drop in the
correct order in the child process (`_demote` in `yacron2/job.py`):

1. Supplementary groups first. With a known login name and gid,
   `os.initgroups(username, gid)` gives the child exactly the target user's
   supplementary groups; otherwise `os.setgroups([])` drops all supplementary
   groups.
2. Primary gid next: `os.setgid(gid)`.
3. uid last: `os.setuid(uid)`.

This fixes a privilege-escalation bug in yacron 0.19 where root's supplementary
group memberships leaked into the child (the classic "forgot `setgroups()`
before `setuid()`" bug). Additionally, a numeric `user` given without an
explicit `group` now derives its primary gid from the passwd database, instead
of silently keeping yacron's gid `0`. If you previously relied on a numeric
`user` retaining gid `0`, set `group` explicitly. See
[Commands and Environment](Commands-and-Environment).

Per-job `user`/`group` switching is POSIX-only (Windows has no setuid/setgid
model). On Windows a job with `user` or `group` set raises a configuration
error, `Job <name>: changing user/group is not supported on Windows`, so the
entire privilege-drop discussion above (and the related checklist item about a
numeric `user` retaining gid `0`) applies only on POSIX. See
[Running on Windows](Running-on-Windows).

### `defaults.environment` merges by key instead of concatenating

When a `defaults` block and a job both define `environment` entries, yacron2
merges them by key: a job overriding a default variable yields a single entry
for that key (the job's value wins), rather than concatenating both into the
list with a duplicate key. This is implemented in `mergedicts`
(`yacron2/config.py`), which special-cases the `environment` list.

Configurations that relied on the old duplicate-key concatenation behavior will
behave differently. In practice the effective value of an overridden variable
is unchanged (the later/job entry took precedence at process-launch time
either way), but the merged `environment` list no longer contains duplicate
keys.

### Dependency pin changes

| Dependency | yacron 0.19 | yacron2 |
| --- | --- | --- |
| `crontab` | `==0.22.8` | dropped (cron expressions parsed by yacron2's built-in engine, same dialect) |
| `strictyaml` | (older pin) | `>=1.7,<2` |
| `aiohttp` | (older pin) | `>=3.10,<4` |
| `aiosmtplib` | (older pin) | `>=3,<6` (v2+ login API) |
| `sentry-sdk` | (older pin) | `>=2,<3` |
| `pytz` | required | dropped (replaced by stdlib `zoneinfo`) |
| `ruamel.yaml` | direct pin | dropped |
| `tzdata` | — | added, `>=2024.1` |

`tzdata` is added so `zoneinfo` can resolve timezones on slim/minimal container
images that do not ship the system tz database. Timezone handling migrated from
third-party `pytz` to the standard-library `zoneinfo`; an invalid `timezone`
now raises `ConfigError`. The new-version pins matter mainly if you install
yacron2 into a shared environment alongside other packages that constrain these
same libraries. See [Schedules and Timezones](Schedules-and-Timezones).

## Migration checklist

- [ ] Install the new distribution: `pip install yacron2` (or `pipx install
  yacron2`), or switch to the container image / standalone binary. `pip` and
  the standalone binaries (amd64/arm64) also work on Windows; see
  [Running on Windows](Running-on-Windows). See [Installation](Installation).
- [ ] Ensure the target interpreter is Python 3.10 or newer. On older hosts, use
  the binary or container image.
- [ ] Update any code/scripts that `import yacron` to `import yacron2`, and any
  service unit or wrapper invoking the `yacron` command to invoke `yacron2`.
- [ ] Move your config directory from `/etc/yacron.d` to `/etc/yacron2.d`, or
  pass `-c /etc/yacron.d` explicitly. Update systemd units, Dockerfiles, and
  Kubernetes manifests accordingly. On Windows the platform default is
  `%APPDATA%\yacron2` instead; see [Running on Windows](Running-on-Windows).
- [ ] In every shell reporter script, rename `YACRON_*` references to
  `YACRON2_*`.
- [ ] For each `mail` reporter targeting a server with a self-signed or invalid
  certificate that previously worked, add `validate_certs: false` (or fix the
  server certificate). All other mail reporters now validate certificates by
  default.
- [ ] (POSIX only; on Windows remove `user`/`group`) If any job uses a numeric
  `user` without a `group` and relied on keeping gid `0`, add an explicit
  `group`. Re-test per-job privilege dropping; the child now also gets the
  target user's supplementary groups instead of root's. On Windows `user`/
  `group` is unsupported and raises a config error, so any job carrying
  `user`/`group` must have it removed (or be run on a POSIX host) before
  migration. See [Running on Windows](Running-on-Windows).
- [ ] Review `defaults.environment` overrides; the merged environment list no
  longer contains duplicate keys (the effective value of each variable is
  unchanged).
- [ ] If you install into a shared environment, reconcile the new dependency
  pins (`strictyaml>=1.7,<2`, `aiohttp>=3.10,<4`, `aiosmtplib>=3,<6`,
  `sentry-sdk>=2,<3`, `tzdata>=2024.1`; `pytz`, the `ruamel.yaml` pin and
  the `crontab` package removed — cron expressions are parsed by yacron2's
  built-in engine).
- [ ] Validate the migrated configuration before starting the scheduler:
  `yacron2 -v -c <path>`. See [Command-Line Reference](CLI-Reference).

For the full set of options and their current types and defaults, see the
[Configuration Reference](Configuration-Reference).
