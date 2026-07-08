# Logging Configuration

This page documents how cronstable produces its own diagnostic log output: the
default behavior driven by `-l/--log-level`, and the optional `logging:` config
section that applies a full Python `logging.config` dictionary schema. It does
not cover capturing a job's stdout/stderr (see
[Output Capturing](Output-Capturing)), nor sending notifications on
job success/failure (see [Reporting](Reporting)).

## Default logging (no `logging:` section)

When the configuration contains no `logging:` section, cronstable's log output is
governed entirely by the CLI. At startup, `__main__.py` calls:

```python
logging.basicConfig(level=getattr(logging, args.log_level))
```

The level comes from `-l/--log-level` (default `INFO`). `logging.basicConfig`
installs a single `StreamHandler` on the root logger that writes to **stderr**
with the standard library default format
(`LEVEL:logger_name:message`). There is no timestamp in this default format.

`-l/--log-level` is passed through `getattr(logging, ...)` unchanged, so its
value must be a valid uppercase Python level name (`DEBUG`, `INFO`, `WARNING`,
`ERROR`, `CRITICAL`); any other value raises `AttributeError` at startup. See
[Command-Line Reference](CLI-Reference) for the full CLI.

This default applies whether or not the run later loads a `logging:` section:
`basicConfig` always runs first, and a `logging:` section (if present) is applied
afterwards during the scheduler loop, overriding it.

## The `logging:` section

The `logging:` section is a Python `logging.config` *dictionary schema*
(the same structure accepted by `logging.config.dictConfig`). cronstable validates
its top-level shape with strictyaml and then hands the whole dictionary to
`logging.config.dictConfig`.

```yaml
logging:
  version: 1
  disable_existing_loggers: false
  formatters:
    simple:
      format: '%(asctime)s [%(processName)s/%(threadName)s] %(levelname)s (%(name)s): %(message)s'
      datefmt: '%Y-%m-%d %H:%M:%S'
  handlers:
    console:
      class: logging.StreamHandler
      level: DEBUG
      formatter: simple
      stream: ext://sys.stdout
  root:
    level: INFO
    handlers:
      - console
```

The example above (taken from `README.md`) displays each log line with an
embedded timestamp, routing all root-logger output to stdout via a `simple`
formatter.

> The ability to configure yacron's own logging was added in yacron 0.19.0
> (upstream issues #81/#82/#83). The `datefmt` line in the README example was a
> later fix.

### Top-level keys

strictyaml only validates the *top-level* keys of the `logging:` map (and the
types of `version`, `incremental`, and `disable_existing_loggers`). The contents
of `formatters`, `filters`, `handlers`, `loggers`, and `root` are accepted as
arbitrary YAML (strictyaml `Any`) and are validated only later by
`dictConfig`. An error inside one of those nested mappings is therefore not
caught at config-parse time; it surfaces when `dictConfig` runs (see
[Reload and error handling](#reload-and-error-handling)).

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `version` | int | (required) | dictConfig schema version. Must be present. The only value `logging.config.dictConfig` currently accepts is `1`. |
| `incremental` | bool | optional (dictConfig default `false`) | If `true`, the configuration is interpreted incrementally: existing loggers/handlers are kept and only handler/logger *levels* and `propagate` flags are adjusted; `formatters`/`filters` and handler creation are ignored. See the dictConfig docs. |
| `disable_existing_loggers` | bool | optional (dictConfig default `true`) | If `true` (the dictConfig default), loggers that exist at the time `dictConfig` runs but are not named in this config are disabled. The README example sets this to `false` so previously-created loggers (e.g. `cronstable`) keep working. Ignored when `incremental` is `true`. |
| `formatters` | mapping | optional | Named formatter definitions (`format`, `datefmt`, etc.), as in dictConfig. Contents unvalidated by strictyaml. |
| `filters` | mapping | optional | Named filter definitions, as in dictConfig. Contents unvalidated by strictyaml. |
| `handlers` | mapping | optional | Named handler definitions (`class`, `level`, `formatter`, `stream`, etc.). Contents unvalidated by strictyaml. |
| `loggers` | mapping | optional | Per-logger configuration (`level`, `handlers`, `propagate`). Contents unvalidated by strictyaml. |
| `root` | mapping | optional | Configuration of the root logger (`level`, `handlers`). Contents unvalidated by strictyaml. |

The defaults shown for `incremental` and `disable_existing_loggers` are the
defaults of `logging.config.dictConfig` itself; they are *not* defined in
cronstable's `DEFAULT_CONFIG`. cronstable supplies no values for any logging key; what
you write is passed through verbatim. Only `version` is required by the schema;
all other keys are optional (strictyaml `Opt(...)`).

### Logger names used by cronstable

cronstable emits log records under these logger names. Target them in `loggers:` to
tune their levels independently, or rely on `root:` to catch them all:

| Logger | Source module | Emits |
| --- | --- | --- |
| `cronstable` | `cron.py`, `job.py` | Scheduler lifecycle, job start/spawn/exit, retries, web server start/stop, shutdown, and most operational messages. |
| `cronstable.config` | `config.py` | Configuration parsing diagnostics (e.g. the converted schedule string at `DEBUG`). |
| `statsd` | `statsd.py` | statsd metric-writer diagnostics. See [Metrics with statsd](Metrics-with-Statsd). |
| `prometheus` | `prometheus.py` | Prometheus `/metrics` endpoint diagnostics (e.g. a cluster-backend read failing during a scrape). See [Metrics with Prometheus](Metrics-with-Prometheus). |

Because `cronstable.config` is a child of `cronstable`, configuring the `cronstable`
logger affects it too (subject to `propagate`). The `statsd` logger is a
separate top-level logger.

## Reload and error handling

cronstable re-reads its configuration on every scheduler tick (roughly once per
minute; see [Architecture and Internals](Architecture-and-Internals)). The
`logging:` section participates in this reload with specific rules, implemented
in `cron.py`:

- The logging config is applied via `logging.config.dictConfig`.
- It is **only re-applied when it changes.** The scheduler keeps the
  last-successfully-applied logging dictionary and compares the freshly-loaded
  one against it; if they are equal, `dictConfig` is not called again.
- It is **only marked as applied on success.** If `dictConfig` raises, the
  scheduler logs an error (`Error while configuring logging: ...`, pointing at
  the dictConfig schema documentation, and including the offending config) and
  does **not** record it as applied.
- Consequently, a `logging:` section that was **broken and then fixed** is
  picked up on the next reload **without restarting cronstable**, because the
  broken version was never marked applied, the corrected version still counts as
  "changed" and is retried.

This behavior (re-apply on change, mark applied only on success) was
introduced as a fix so a logging section fixed after an error, or changed at
runtime, is picked up without a restart.

If the loaded config has no `logging:` section, `dictConfig` is never called and
whatever logging configuration is currently in effect (the startup `basicConfig`,
or a previously-applied `logging:` section) remains active.

## One logging block per configuration

At most **one** `logging:` block may exist across an entire configuration:

- Within a single file plus its `include:`s, a second `logging:` block raises
  `ConfigError("multiple logging configs")`.
- Across a configuration *directory* (multiple `.yml`/`.yaml` files), a second
  file containing a `logging:` block raises
  `ConfigError("Multiple 'logging' configurations found: first in <file>, now
  in <file>")`.

See [Includes, Defaults, and Multi-File Config](Includes-and-Defaults) for how
files in a directory are aggregated and for the matching rule that applies to the
`web:` block.

## Notes

- The `logging:` section configures **cronstable's own** logging only. It has no
  effect on how a job's captured output is stored or reported.
- Validating the configuration with `-v/--validate-config` checks the top-level
  schema of the `logging:` section but does **not** call `dictConfig`, so a
  nested error (e.g. an unknown handler class) is not detected by validation; it
  only surfaces when the daemon actually applies the config.
- For the complete configuration schema, see
  [Configuration Reference](Configuration-Reference).
