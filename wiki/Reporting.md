# Reporting (Mail, Sentry, Shell)

yacron2 can report a job's outcome through three reporters - Sentry, e-mail
(SMTP), and an arbitrary shell command - configured under the `report` block of
the `onFailure`, `onPermanentFailure`, and `onSuccess` hooks. This page documents
every reporter option, its type and default, the secret-resolution rules, the
jinja2 template variables, and the environment variables passed to the shell
reporter.

## Hooks and when each fires

Each job has three reporting hooks, each containing a `report` block with the
same schema:

| Hook | Fires when |
| --- | --- |
| `onFailure` | A job run is detected as failed (see [Failure Detection and Retries](Failure-Detection-and-Retries)). When retries are configured, this fires on each failed attempt. |
| `onPermanentFailure` | A job has failed and all configured retries are exhausted. |
| `onSuccess` | A job run is detected as succeeded. |

All three hooks accept the identical `report` block (`sentry`, `mail`, `shell`).
The default report configuration is applied independently to each of the three
hooks (`_REPORT_DEFAULTS` is deep-copied into each), so configuring one hook does
not affect the others.

A run that is deliberately terminated to make way for a newer instance
(`concurrencyPolicy: Replace`) is not treated as a failure and is neither
reported nor retried. See [Concurrency and Timeouts](Concurrency-and-Timeouts).

### All three reporters always run

For any given hook, yacron2 always invokes all three reporters concurrently
(`asyncio.gather` with `return_exceptions=True`). A reporter that is not
configured returns early and does nothing:

- Sentry returns if no `dsn` source is set.
- Mail returns if `to` or `from` is unset.
- Shell returns if `command` is unset (`None`).

An exception raised by one reporter is logged at `ERROR` level (with traceback)
and does not prevent the other reporters from running, nor does it propagate to
the scheduler.

## Templating

`subject`, `body` (mail), `body` and each `fingerprint` entry (sentry) are
[jinja2](https://jinja.palletsprojects.com/) templates. Each distinct template
source is compiled once and cached for the process lifetime (`lru_cache`), so the
same template string is not recompiled on every report.

The following variables are available when rendering any report template:

| Variable | Type | Description |
| --- | --- | --- |
| `name` | str | Job name. |
| `success` | bool | `True` when the job is considered successful (i.e. no fail reason). |
| `fail_reason` | str or None | Human-readable reason the job failed, or `None` on success. |
| `stdout` | str or None | Captured standard output (`None` if `captureStdout` is off). |
| `stderr` | str or None | Captured standard error (`None` if `captureStderr` is off). |
| `exit_code` | int or None | Process exit code (`retcode`). |
| `command` | str or list | The job's command. |
| `shell` | str | The job's shell. |
| `environment` | dict or None | The subprocess environment (`None` when the job defines no `environment`). |

The README's variable list omits `fail_reason`; the code provides it (it is
also used by the default body template). To capture output for inclusion in reports, enable `captureStderr`
(on by default) and/or `captureStdout`. See [Output Capturing](Output-Capturing).

### Default templates

The mail subject and (prepended to the) sentry body use:

```text
Cron job '{{name}}' {% if success %}completed{% else %}failed{% endif %}
```

The default body (`DEFAULT_BODY_TEMPLATE`) prints the fail reason (when set)
followed by captured stdout/stderr, or `(no output was captured)`:

```text
{% if fail_reason -%}
(job failed because {{fail_reason}})
{% endif %}
{% if stdout and stderr -%}
STDOUT:
---
{{stdout}}
---
STDERR:
{{stderr}}
{% elif stdout -%}
{{stdout}}
{% elif stderr -%}
{{stderr}}
{% else -%}
(no output was captured)
{% endif %}
```

The default sentry `body` is the default subject template, a newline, and the
default body template, concatenated.

## Mail reporter

Sends an e-mail via SMTP using `aiosmtplib`. Reporting is enabled only when both
`to` and `from` are set; otherwise the mail reporter returns without sending.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `from` | str (required in block) | `None` | Envelope/`From` header. Required for mail reporting to occur. |
| `to` | str (required in block) | `None` | Comma-separated recipient list, used directly as the `To` header. Required for mail reporting to occur. |
| `smtpHost` | str (Opt) | `None` | SMTP server hostname. |
| `smtpPort` | int (Opt) | `25` | SMTP server port. |
| `tls` | bool (Opt) | `false` | Use TLS for the connection (`aiosmtplib` `use_tls`). |
| `starttls` | bool (Opt) | `false` | Issue `STARTTLS` after connecting. |
| `validate_certs` | bool (Opt) | `true` | Validate the server's TLS certificate. See note below. |
| `html` | bool (Opt) | `false` | Send the body as `text/html` (`set_content` subtype `html`) instead of plain text. |
| `username` | str (Opt) | `None` | SMTP login username. Login is attempted only when both `username` and a resolved `password` are present. |
| `password` | secret block (Opt) | unset | SMTP login password; see [Secrets](#secrets). |
| `subject` | str (Opt) | default subject template | jinja2 template for the `Subject` header. |
| `body` | str (Opt) | default body template | jinja2 template for the message body. |

In the strictyaml schema, `from` and `to` are required keys when a `mail` block
is present (they accept an empty value, mapping to `None`), while the remaining
keys are optional. Behaviorally, mail reporting is skipped unless both resolve to
a non-empty value.

Notes on behavior:

- **`validate_certs` defaults to `true`** (changed so that SMTP TLS certificate
  validation is on by default). This breaks connections to servers with
  self-signed or otherwise untrusted certificates unless you set
  `validate_certs: false`.
- An `RFC 5322` `Date` header is set
  (`email.utils.format_datetime(datetime.now(timezone.utc))`), e.g.
  `Wed, 18 Jun 2026 12:34:56 +0000` - not ISO-8601.
- **Empty-body success e-mails are skipped.** On a success report, if the
  rendered body is empty after stripping whitespace, no e-mail is sent. (Failure
  reports are sent even with an empty body.)
- The SMTP connection is always closed, even if `STARTTLS`, login, or sending
  raises, so a misbehaving server cannot leak one connection per report.
- `html: true` uses `set_content(body, subtype="html")`, which sets the correct
  charset and transfer-encoding for non-ASCII HTML.

Minimal failure-mail example:

```yaml
jobs:
  - name: backup
    command: /usr/local/bin/backup.sh
    schedule: "0 3 * * *"
    captureStderr: true
    onFailure:
      report:
        mail:
          from: cron@example.com
          to: ops@example.com, oncall@example.com
          smtpHost: 127.0.0.1
          smtpPort: 25
          starttls: false
          validate_certs: false
```

HTML success mail with login (password from environment):

```yaml
jobs:
  - name: report-build
    command: render-report
    schedule: "@reboot"
    captureStdout: true
    onSuccess:
      report:
        mail:
          from: cron@example.com
          to: team@example.com
          smtpHost: smtp.example.com
          smtpPort: 587
          starttls: true
          username: cron
          password:
            fromEnvVar: SMTP_PASSWORD
          html: true
          subject: "Build report for {{ name }}"
          body: "{{ stdout }}"
```

## Sentry reporter

Captures a message to [Sentry](https://sentry.io/) via `sentry-sdk`. Reporting is
enabled only when a `dsn` source resolves to a value; otherwise the reporter
returns early.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `dsn` | secret block (Opt) | all sources `None` | Sentry DSN; see [Secrets](#secrets). No DSN means Sentry reporting is disabled. |
| `fingerprint` | list of str (Opt) | `["yacron2", "{{ environment.HOSTNAME }}", "{{ name }}"]` | jinja2-templated fingerprint lines controlling Sentry issue grouping. **Replaces, not appends, on merge** - see note. |
| `level` | str (Opt) | unset (effective `error`) | Sentry severity level (e.g. `error`, `warning`, `info`). When unset, `sentry_sdk.capture_message` is called with `level="error"`. |
| `extra` | map of str to (str/int/bool) (Opt) | unset | Additional key/value context attached to the event. Your map is merged on top of yacron2's always-attached `job`/`exit_code`/`command`/`shell`/`success` context. |
| `body` | str (Opt) | default subject + body template | jinja2 template for the captured message text. |
| `environment` | str (Opt) | `None` | Sentry environment tag. |
| `maxStringLength` | int (Opt) | `8192` | Sets `sentry_sdk.utils.MAX_STRING_LENGTH` (max length before Sentry truncates strings). |

Notes on behavior:

- The Sentry client is initialized once per `(dsn, environment)` pair and cached;
  it is rebuilt only when one of those changes, not on every report.
- `maxStringLength` mutates the process-global `sentry_sdk.utils.MAX_STRING_LENGTH`
  when set (and truthy).
- In addition to any `extra` you supply, yacron2 always attaches `job`,
  `exit_code`, `command`, `shell`, and `success` to the event's extra context.
  Your `extra` map is merged on top of these.
- Capture uses an isolated scope (`sentry_sdk.new_scope()`); the configured
  `fingerprint` and extras are applied per event.

**Fingerprint merge semantics:** `fingerprint` is a replace-not-append setting.
When a `defaults` block or a job supplies its own `fingerprint`, it overrides the
default list entirely rather than being concatenated onto the three default
entries. This is a deliberate special case in the config merge so that custom
Sentry issue grouping is possible. All other list-valued options merge by
concatenation; `environment` (the job env list) merges by key. See
[Includes, Defaults, and Multi-File Config](Includes-and-Defaults).

Example:

```yaml
jobs:
  - name: ingest
    command: run-ingest
    schedule: "*/5 * * * *"
    captureStderr: true
    onFailure:
      report:
        sentry:
          dsn:
            fromEnvVar: SENTRY_DSN
          level: warning
          environment: production
          fingerprint:
            - ingest-job
            - "{{ name }}"
          extra:
            datacenter: dc1
            shard: 3
```

## Shell reporter

Runs a user-supplied command, passing job state through `YACRON2_*` environment
variables.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `shell` | str (Opt) | `/bin/sh` (POSIX); empty (Windows) | Shell used when `command` is a string. The default is platform-specific (`platform.DEFAULT_SHELL`), mirroring the job-level `shell` default: `/bin/sh` on POSIX, but empty (`""`) on Windows, where an empty default routes a string `command` through the native command processor (%ComSpec% / cmd.exe). See [Running on Windows](Running-on-Windows). |
| `command` | str or list of str (required in block) | `None` | The command to run. A list is executed directly (argv); a string is run via `shell -c` when `shell` is set, or through the system default shell when `shell` is empty (the Windows default - see the execution model below). Required key when a `shell` block is present; reporting is skipped if it resolves to nothing. |

Execution model:

- If `command` is a **list**, it is executed directly with
  `asyncio.create_subprocess_exec` (no shell).
- If `command` is a **string** and `shell` is set (on POSIX the default
  `/bin/sh` applies), it is executed as `[shell, "-c", command]` with
  `asyncio.create_subprocess_exec`.
- If `command` is a **string** and `shell` resolves to a falsy value (e.g.
  `shell: ""`), the string is passed to `asyncio.create_subprocess_shell`
  (run by the system default shell). **This is the Windows default**, where
  the empty `shell` makes the string run through the native command processor
  (cmd.exe via %ComSpec%). To use PowerShell or another interpreter on
  Windows, set `shell:` explicitly, or pass `command` as a list to bypass the
  shell entirely. See [Running on Windows](Running-on-Windows).
- The reporter does not fail the job. A failure to launch the command is logged
  (with traceback) and the reporter returns; a nonzero exit code from the command
  is logged at `ERROR` level (without a spurious traceback).

### Environment variables

The shell command inherits the full environment of the yacron2 process, plus the
following variables describing the job outcome:

| Variable | Value |
| --- | --- |
| `YACRON2_FAIL_REASON` | The fail reason string, or empty string on success. |
| `YACRON2_FAILED` | `"1"` if the job failed, `"0"` otherwise. |
| `YACRON2_JOB_NAME` | The job name. |
| `YACRON2_JOB_COMMAND` | The job command; a list command is joined with spaces. |
| `YACRON2_JOB_SCHEDULE` | The job's unparsed schedule string. |
| `YACRON2_RETCODE` | The process exit code, as a string. |
| `YACRON2_STDERR` | Captured stderr (possibly truncated; see below). |
| `YACRON2_STDOUT` | Captured stdout (possibly truncated; see below). |
| `YACRON2_STDERR_TRUNCATED` | `"1"` if `YACRON2_STDERR` was truncated, `"0"` otherwise. |
| `YACRON2_STDOUT_TRUNCATED` | `"1"` if `YACRON2_STDOUT` was truncated, `"0"` otherwise. |

**Truncation:** stdout and stderr can be large, and there are OS limits on
argument/environment sizes. yacron2 truncates each stream to a maximum of
**16 KiB** (`1024 * 16`) when either stream individually, or the two combined,
exceeds that limit. `YACRON2_STDERR_TRUNCATED` / `YACRON2_STDOUT_TRUNCATED`
indicate per-stream whether truncation occurred. The README lists the first eight
variables but omits the `*_TRUNCATED` pair; both are set by the code.

Example:

```yaml
jobs:
  - name: ping-job
    command: do-work
    shell: /bin/bash
    schedule: "* * * * *"
    onFailure:
      report:
        shell:
          shell: /bin/bash
          command: echo "job $YACRON2_JOB_NAME failed with code $YACRON2_RETCODE"
```

This POSIX-shaped example (a `/bin/bash` shell and `$VAR` syntax) won't run as
written on Windows. There, either leave `shell` unset (the command runs via
cmd.exe, using `%VAR%` syntax) or set `shell:` to a PowerShell path. See
[Running on Windows](Running-on-Windows).

List form (no shell):

```yaml
        shell:
          command:
            - /usr/local/bin/notify
            - --job
            - "failed"
```

## Secrets

The `mail.password` and `sentry.dsn` options are secret blocks with three
mutually-exclusive sources. Each is an optional key accepting a string or empty
value:

| Source | Description |
| --- | --- |
| `value` | The secret inline in the config. |
| `fromFile` | Path to a file whose contents (stripped of surrounding whitespace) are the secret. |
| `fromEnvVar` | Name of an environment variable holding the secret. |

Resolution order is `value`, then `fromFile`, then `fromEnvVar` - the first
non-empty source wins. If none is set, the reporter treats the secret as absent
(Sentry: disabled; mail: no login).

If `fromEnvVar` is set but the named environment variable is unset/empty, the
report is **skipped** and an error is logged - yacron2 no longer raises
`KeyError` in this case. For mail, the password env-var *name* is not echoed to
the logs (it is tied to a secret); for sentry, the DSN env-var name is logged.

```yaml
        sentry:
          dsn:
            fromFile: /etc/secrets/sentry-dsn
        mail:
          from: cron@example.com
          to: ops@example.com
          smtpHost: 127.0.0.1
          username: cron
          password:
            fromEnvVar: SMTP_PASSWORD
```

## Related pages

- [Configuration Reference](Configuration-Reference)
- [Failure Detection and Retries](Failure-Detection-and-Retries)
- [Output Capturing](Output-Capturing)
- [Metrics with statsd](Metrics-with-Statsd)
- [Includes, Defaults, and Multi-File Config](Includes-and-Defaults)
- [Running on Windows](Running-on-Windows)
