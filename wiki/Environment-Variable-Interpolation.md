# Environment-Variable Interpolation

cronstable expands `${VAR}` references in a configuration's **string values**
against its own process environment when it loads the file. One config can then
be deployed unchanged to several environments, with the per-environment pieces
(a listen address, a state path, a timezone, a webhook URL) supplied as
environment variables rather than templated in by a wrapper script. The
expansion is implemented in `cronstable/config.py` and runs on every config
load, including a hot reload.

**On this page:**
[Syntax](#syntax) ·
[What gets expanded](#what-gets-expanded) ·
[Skipped fields](#skipped-fields) ·
[Unset variables](#unset-variables) ·
[Multi-file config](#multi-file-config) ·
[Effect on the job-set id](#effect-on-the-job-set-id) ·
[Worked example](#worked-example)

## Syntax

| Form | Meaning |
| --- | --- |
| `${VAR}` | The value of environment variable `VAR`. A variable that is set but empty expands to the empty string. An **unset** `VAR` is a load-time error (see [Unset variables](#unset-variables)). |
| `${VAR:-default}` | The value of `VAR` when it is set and non-empty, otherwise the literal `default`. This mirrors the shell's `:-`: an unset **or** empty `VAR` yields `default`. The default runs to the closing `}` and may contain any character except `}`. |
| `$$` | A literal `$`. Use it to write a value that should contain a real dollar sign, or to keep a literal `${...}` (`$${VAR}` stays `${VAR}`). |

A variable name is a letter or underscore followed by letters, digits, or
underscores. Only these braced forms are recognized: a lone `$`, a bare
`$VAR` without braces, and a malformed `${...}` (unclosed, or a name with
unsupported characters) are all left exactly as written, so a value that never
used the syntax is passed through verbatim.

```yaml
web:
  listen:
    - "0.0.0.0:${PORT}"            # PORT from the environment
    - "${BIND:-127.0.0.1}:8080"    # a default when BIND is unset
state:
  path: ${STATE_DIR}/cronstable    # a whole path segment from the environment
jobs:
  - name: report-${REGION}         # names, timezones, prefixes, ... all expand
    command: run-report
    schedule:
      minute: "0"
    timezone: ${TZ:-UTC}
    streamPrefix: "cost was $$5"    # a literal dollar sign
```

## What gets expanded

Expansion runs **after** the document is validated against the schema, over the
parsed values, so it applies to every field the schema accepts as a string in
the `jobs`, `dags`, `web`, `state`, `cluster`, `mcp`, `defaults`, and `include`
sections (the `logging` section is skipped, see below). A `${VAR}` may sit
anywhere inside such a string.

Because it runs post-validation, a numeric key cannot itself be a bare
`${VAR}`: `smtpPort: ${PORT}` fails schema validation before expansion is ever
reached, since `${PORT}` is not an integer. Put the variable inside a string
instead. A listen address carries its port inside a string
(`"0.0.0.0:${PORT}"`), which is why the port can come from the environment.

## Skipped fields

Some subtrees are left untouched, because their `${...}` is another layer's
expansion syntax rather than cronstable's.

**Commands and shells** reach the process unchanged:

- a job's `command` and `shell`,
- a DAG task's `command` and `shell`,
- a shell reporter's whole `report.shell` block (its `command` and `shell`).

These are handed to a shell at run time, and their `${VAR}` is meant to be
expanded by that shell against the **job's** environment (its
[`environment` / `env_file`](Commands-and-Environment) and staged
[secrets](Durable-State)), which is assembled per run and is not the daemon's
environment. So `command: echo ${HOME}` prints the home directory of the user
the job runs as, exactly as under `/bin/sh`, and is never touched at load.
To use a load-time environment variable inside a command, set it as a job
`environment` value (which does expand) and reference that from the command.

**The whole `logging` section** is skipped, because it is passed to Python's
[`logging.config`](Logging-Configuration) verbatim and a `$`-style formatter
(`style: "$"`) legitimately writes `${asctime}` / `${message}` in its `format`
string. Interpolating them would treat those as environment variables and fail
an otherwise valid config to load, so cronstable leaves the section for
`logging.config`. A log path that needs an environment variable can be supplied
through the process environment that `logging.config` itself reads, or by
templating the file outside cronstable.

## Unset variables

A `${VAR}` with no `:-default` whose variable is unset is a hard `ConfigError`
that names the variable, the config value it appeared in, and the file:

```text
prod.yaml: config value web.listen[0] references environment variable
${NEEDED_PORT}, which is not set; export it, or write ${NEEDED_PORT:-default}
to supply a fallback
```

The failure happens at load, so [`cronstable --validate-config`](CLI-Reference)
catches a missing variable in CI or a deploy check and exits non-zero before the
scheduler ever starts. Supply a `:-default` for anything that has a sensible
fallback, and leave it off for the variables a deployment must provide.

## Multi-file config

Each file is expanded against the environment as it is parsed, so an
[included](Includes-and-Defaults) file resolves its own `${VAR}` references.
Because an `include:` entry is itself an ordinary string value, an include path
can also be built from the environment:

```yaml
include:
  - ${ENVIRONMENT:-prod}.yaml
```

The environment is read at load time. A running daemon reloads its config
periodically and on the usual triggers, but the reload is skipped when no source
file has changed on disk; a change to an environment variable alone does not
have a file to notice, so restart (or touch the config) to pick it up.

## Effect on the job-set id

The [job-set id](Job-Set-ID) is a fingerprint of the **effective** config, and
expansion happens before that fingerprint is taken, so it hashes the expanded
values. A config that interpolates environment variables into
fingerprinted fields therefore produces a **different id per environment**: two
deployments that resolve `${REGION}` to different values are, by design, running
different job sets. Keep environment variables out of fingerprinted job fields
if you need one id to compare across environments, or use them freely and expect
the id to track the environment.

## Worked example

A single `platform.yaml` deployed to every region, with the per-region values
injected as environment variables at the container or unit level:

```yaml
state:
  path: ${STATE_DIR}
web:
  listen:
    - "0.0.0.0:${WEB_PORT:-8080}"
jobs:
  - name: nightly-rollup
    command: rollup --region "$REGION"   # $REGION expanded by the job's shell
    schedule:
      minute: "0"
      hour: "2"
    timezone: ${TZ:-UTC}
    onFailure:
      report:
        webhook:
          url:
            value: ${ALERT_WEBHOOK}
```

`STATE_DIR` and `ALERT_WEBHOOK` are required (an unset one fails
`--validate-config`); `WEB_PORT` and `TZ` fall back to their defaults; and
`$REGION` inside `command` is left for the job's shell to expand at run time.

## See also

- [Configuration Reference](Configuration-Reference): the full schema and every string-typed field.
- [Commands and Environment](Commands-and-Environment): the job `environment`, `env_file`, and how a command's own `${VAR}` is expanded at run time.
- [Includes, Defaults, and Multi-File Config](Includes-and-Defaults): per-file parsing and the merge order.
- [Job-Set ID](Job-Set-ID): the fingerprint that expansion feeds into.
- [Command-Line Reference](CLI-Reference): `--validate-config`, which surfaces an unset-variable error.
