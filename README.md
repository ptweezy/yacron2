# yacron2 (Yet Another Cron 2)

A modern, rootless-container-friendly cron replacement, written in Rust.

yacron2 is a Rust reimplementation of [yacron](https://github.com/gjcarneiro/yacron)
(by Gustavo Carneiro). The YAML configuration format is unchanged, so existing
crontabs keep working; this rewrite trades the Python runtime for a single,
dependency-free static-ish binary that starts instantly and is trivial to ship
in a scratch/slim container. Documentation that refers to "since version X.Y"
describes behaviour inherited from the original yacron's version history.

## Features

* "Crontab" is in YAML format;
* Builtin sending of e-mail and shell-command reports when cron jobs fail
  (Sentry config is accepted but reporting is not yet implemented — see
  [Differences from the Python version](#differences-from-the-python-version));
* Flexible configuration: you decide how to determine if a cron job fails or not;
* Designed for running in Docker, Kubernetes, or 12-factor environments:
  * Runs in the foreground;
  * Logs everything to stdout/stderr;
* Option to automatically retry failing cron jobs, with exponential backoff;
* Optional HTTP REST API, to fetch status and start jobs on demand;
* Arbitrary timezone support (the IANA database is bundled into the binary, so
  timezones resolve even on minimal images with no system tzdata);
* A single native binary — no interpreter, no runtime dependencies to install.

## Installation

### Run with Docker

```shell
docker build -t yacron2 .
docker run --rm -v "$PWD/my-crontab.yaml:/etc/yacron2.d/crontab.yaml" yacron2
```

The image is built from `Dockerfile` (a Debian-slim runtime containing just the
binary plus the OpenSSL/CA bundle needed for SMTP TLS). See
[`example/docker/`](example/docker/) for a fuller example, including a
Kubernetes deployment.

### Prebuilt binary

A self-contained binary can be downloaded from the GitHub releases page:
https://github.com/ptweezy/yacron2/releases. No runtime is required on the
target system.

### Build from source

yacron2 builds on Linux, macOS, Windows, and WSL with a stable Rust toolchain
(1.74+). Install Rust from [rustup.rs](https://rustup.rs), then:

```shell
# install into ~/.cargo/bin
cargo install --path .

# or just build a release binary at target/release/yacron2
cargo build --release
```

On Linux/WSL the e-mail reporter links against the system OpenSSL, so install
the development headers first (`apt-get install pkg-config libssl-dev` on
Debian/Ubuntu). On Windows the SChannel TLS backend is used, so no extra
packages are needed.

## Usage

Configuration is in YAML format. To start yacron2, give it a configuration file
or directory path as the `-c` argument. For example:

```
yacron2 -c /tmp/my-crontab.yaml
```

This starts yacron2 (always in the foreground!), reading
`/tmp/my-crontab.yaml` as configuration file. If the path is a directory,
any `*.yaml` or `*.yml` files inside this directory are taken as
configuration files.

Useful flags:

* `-c, --config FILE-OR-DIR` — configuration file or directory
  (default `/etc/yacron2.d`);
* `-l, --log-level LEVEL` — `DEBUG`, `INFO` (default), `WARNING`, or `ERROR`.
  The `RUST_LOG` environment variable, if set, overrides this and accepts the
  full [`tracing` filter syntax](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html);
* `-v, --validate-config` — validate the configuration and exit;
* `--version` — print the version and exit.

### Configuration basics

This configuration runs a command every 5 minutes:

```yaml
jobs:
  - name: test-01
    command: echo "foobar"
    shell: /bin/bash
    schedule: "*/5 * * * *"
```

The command can be a string or a list of strings. If command is a string,
yacron2 runs it through a shell, which is `/bin/bash` in the above example, but
is `/bin/sh` by default.

If the command is a list of strings, the command is executed directly, without a
shell. The ARGV of the command to execute is extracted directly from the
configuration:

```yaml
jobs:
  - name: test-01
    command:
      - echo
      - foobar
    schedule: "*/5 * * * *"
```

The `schedule` option can be a string in crontab format. The fields are
`minute hour day-of-month month day-of-week`, with an optional 6th field for the
`year`. Steps (`*/5`), ranges (`1-5`), lists (`1,3,5`), month/day names
(`JAN`, `MON`), `L` (last day of month / last weekday), and the
`@yearly`/`@monthly`/`@weekly`/`@daily`/`@hourly` aliases are all supported.
`@reboot` is special-cased to run a job once, when yacron2 starts.

> **Note on semantics:** like the `parse-crontab` library the Python version
> used, day-of-month and day-of-week are combined with **AND** (a job with both
> fields restricted runs only when *both* match) — this differs from Vixie
> cron's "OR" rule. Sunday may be written as `0` or `7`.

`schedule` can also be an object with named fields. The following runs a command
every 5 minutes, but only on the specific date 2017-07-19:

```yaml
jobs:
  - name: test-01
    command: echo "foobar"
    schedule:
      minute: "*/5"
      dayOfMonth: 19
      month: 7
      year: 2017
      dayOfWeek: "*"
```

Important: by default all time is interpreted to be in UTC, but you can
request to use local time instead. For instance, the cron job below runs
every day at 19h27 *local time* because of the `utc: false` option:

```yaml
jobs:
  - name: test-01
    command: echo "hello"
    schedule: "27 19 * * *"
    utc: false
    captureStdout: true
```

You can also request that the schedule be interpreted in an arbitrary timezone,
using the `timezone` attribute:

```yaml
jobs:
  - name: test-01
    command: echo "hello"
    schedule: "27 19 * * *"
    timezone: America/Los_Angeles
    captureStdout: true
```

You can ask for environment variables to be defined for command execution:

```yaml
jobs:
  - name: test-01
    command: echo "foobar"
    shell: /bin/bash
    schedule: "*/5 * * * *"
    environment:
      - key: PATH
        value: /bin:/usr/bin
```

You can also provide an environment file to define environments for command execution:

```yaml
jobs:
  - name: test-01
    command: echo "foobar"
    shell: /bin/bash
    schedule: "*/5 * * * *"
    env_file: .env
```

The env file must be a list of `KEY=VALUE` pairs. Empty lines and lines starting with `#` will be ignored.

Variables declared in the `environment` option will override those found in the `env_file`.

### Specifying defaults

There can be a special `defaults` section in the config. Any attributes
defined in this section provide default values for cron jobs to inherit.
Although cron jobs can still override the defaults, as needed:

```yaml
defaults:
    environment:
      - key: PATH
        value: /bin:/usr/bin
    shell: /bin/bash
    utc: false
jobs:
  - name: test-01
    command: echo "foobar"  # runs with /bin/bash as shell
    schedule: "*/5 * * * *"
  - name: test-02  # runs with /bin/sh as shell
    command: echo "zbr"
    shell: /bin/sh
    schedule: "*/5 * * * *"
```

Note: if the configuration option is a directory and there are multiple configuration files in that directory, then the `defaults` section in each configuration file provides default options only for cron jobs inside that same file; the defaults have no effect beyond any individual YAML file.

### Reporting

yacron2 has builtin support for reporting job failure (more on that below) by
e-mail and shell command:

```yaml
- name: test-01
  command: |
    echo "hello" 1>&2
    sleep 1
    exit 10
  schedule:
    minute: "*/2"
  captureStderr: true
  onFailure:
    report:
      mail:
        from: example@foo.com
        to: example@bar.com
        smtpHost: 127.0.0.1
        # optional fields:
        username: "username1"  # set username and password to enable login
        password:
          value: example
          # Alternatively:
          # fromFile: /etc/secrets/my-secret-password
          # fromEnvVar: MAIL_PASSWORD
        tls: false  # set to true to enable implicit TLS
        starttls: false  # set to true to enable StartTLS
      shell:
        shell: /bin/bash
        command: ...
```

Here, the `onFailure` object indicates what to do when a job failure is
detected. The `captureStderr: true` part instructs yacron2 to capture output
from the program's *standard error*, so that it can be included in the report.
We could also turn on *standard output* capturing via the `captureStdout: true`
option. By default, yacron2 captures only standard error. If a cron job's
standard error or standard output capturing is not enabled, these streams will
simply write to the same standard output and standard error as yacron2 itself.

Both *stdout* and *stderr* stream lines are by default prefixed with
`[{job_name} {stream_name}]`, i.e. `[test-01 stdout]`. If for any reason you
need to change this, provide the option `streamPrefix` with your own custom
string:

```yaml
- name: test-01
  command: echo "hello world"
  schedule:
    minute: "*/2"
  captureStdout: true
  streamPrefix: "[{job_name} job]"
```

In some cases, for instance when you're logging JSON objects, you might want to
completely get rid of the prefix altogether:

```yaml
- name: test-01
  command: echo "hello world"
  schedule:
    minute: "*/2"
  captureStdout: true
  streamPrefix: ""
```

It is possible also to report job success, as well as failure, via the
`onSuccess` option.

```yaml
- name: test-01
  command: echo "hello world"
  schedule:
    minute: "*/2"
  captureStdout: true
  onSuccess:
    report:
      mail:
        from: example@foo.com
        to: example@bar.com
        smtpHost: 127.0.0.1
```

It is possible to customise the format of the report. For `mail` reporting, the
option `subject` indicates the subject of the email, while `body` formats the
email body. The values of those options are strings that are processed by a
Jinja2-compatible templating engine. The following variables are available in
templating:

* name(str): name of the cron job
* success(bool): whether or not the cron job succeeded
* stdout(str): standard output of the process
* stderr(str): standard error of the process
* exit_code(int): process exit code
* command(str): cron job command
* shell(str): cron job shell
* environment(dict): subprocess environment variables

Example:

```yaml
- name: test-01
  command: |
    echo "hello" 1>&2
    sleep 1
    exit 10
  schedule:
    minute: "*/2"
  captureStderr: true
  onFailure:
    report:
      mail:
        from: example@foo.com
        to: example@bar.com
        smtpHost: 127.0.0.1
        subject: Cron job '{{name}}' {% if success %}completed{% else %}failed{% endif %}
        body: |
          {{stderr}}
          (exit code: {{exit_code}})
```

The shell reporter executes a user-given shell command. It passes all of
yacron2's environment variables to the child and adds some that describe the
state of the job:

* YACRON2_FAIL_REASON (str)
* YACRON2_FAILED ("1" or "0")
* YACRON2_JOB_NAME (str)
* YACRON2_JOB_COMMAND (str)
* YACRON2_JOB_SCHEDULE (str)
* YACRON2_RETCODE (str)
* YACRON2_STDERR (str)
* YACRON2_STDOUT (str)
* YACRON2_STDERR_TRUNCATED ("1" or "0")
* YACRON2_STDOUT_TRUNCATED ("1" or "0")

A simple example configuration:

```yaml
- name: test-01
  command: echo "foobar" && exit 123
  shell: /bin/bash
  schedule: "* * * * *"
  onFailure:
    report:
      shell:
        shell: /bin/bash
        command: echo "Error code $YACRON2_RETCODE"
```

It is possible to send e-mail formatted as HTML by adding the `html: true`
property:

```yaml
- name: test-01
  command: echo "hello <b>world</b>"
  schedule: "@reboot"
  captureStdout: true
  onSuccess:
    report:
      mail:
        from: example@foo.com
        to: example@bar.com, zzz@sleep.com
        html: true
        smtpHost: 127.0.0.1
        smtpPort: 1025
        subject: This is a cron job with html body
```

### Metrics

yacron2 has builtin support for writing job metrics to
[Statsd](https://github.com/etsy/statsd):

```yaml
jobs:
  - name: test01
    command: echo "hello"
    schedule: "* * * * *"
    statsd:
      host: my-statsd.example.com
      port: 8125
      prefix: my.cron.jobs.prefix.test01
```

With this config yacron2 will write the following metrics over UDP
to the Statsd listening on `my-statsd.example.com:8125`:

```
my.cron.jobs.prefix.test01.start:1|g  # this one is sent when the job starts
my.cron.jobs.prefix.test01.stop:1|g   # the rest are sent when the job stops
my.cron.jobs.prefix.test01.success:1|g
my.cron.jobs.prefix.test01.duration:3|ms|@0.1
```

### Handling failure

By default, yacron2 considers that a job has *failed* if either the process
returns a non-zero code or if it generates output to *standard error* (and
standard error capturing is enabled, of course).

You can instruct yacron2 how to determine if a job has failed or not via the
`failsWhen` option:

```yaml
failsWhen:
  producesStdout: false
  producesStderr: true
  nonzeroReturn: true
  always: false
```

producesStdout
: If true, any captured standard output causes yacron2 to consider the job
as failed. This is false by default.

producesStderr
: If true, any captured standard error causes yacron2 to consider the job
as failed. This is true by default.

nonzeroReturn
: If true, if the job process returns a code other than zero causes yacron2
to consider the job as failed. This is true by default.

always
: If true, if the job process exits that causes yacron2 to consider the job as
failed. This is false by default.

It is possible to instruct yacron2 to retry failing cron jobs by adding a
`retry` option inside `onFailure`:

```yaml
- name: test-01
  command: |
    echo "hello" 1>&2
    sleep 1
    exit 10
  schedule:
    minute: "*/10"
  captureStderr: true
  onFailure:
    report:
      mail:
        from: example@foo.com
        to: example@bar.com
        smtpHost: 127.0.0.1
    retry:
      maximumRetries: 10
      initialDelay: 1
      maximumDelay: 30
      backoffMultiplier: 2
```

The above settings tell yacron2 to retry the job up to 10 times, with the delay
between retries defined by an exponential backoff process: initially 1 second,
doubling for every retry up to a maximum of 30 seconds. A value of -1 for
maximumRetries will mean yacron2 will keep retrying forever, this is mostly
useful with a schedule of "@reboot" to restart a long running process when it
has failed.

If the cron job is expected to fail sometimes, you may wish to report only in
the case the cron job ultimately fails after all retries and we give up on it.
For that situation, you can use the `onPermanentFailure` option:

```yaml
- name: test-01
  command: |
    echo "hello" 1>&2
    sleep 1
    exit 10
  schedule:
    minute: "*/10"
  captureStderr: true
  onFailure:
    retry:
      maximumRetries: 10
      initialDelay: 1
      maximumDelay: 30
      backoffMultiplier: 2
  onPermanentFailure:
    report:
      mail:
        from: example@foo.com
        to: example@bar.com
        smtpHost: 127.0.0.1
```

### Concurrency

Sometimes it may happen that a cron job takes so long to execute that the moment its next scheduled execution is reached a previous instance may still be running. How yacron2 handles this situation is controlled by the option `concurrencyPolicy`, which takes one of the following values:

Allow
: allows concurrently running jobs (default)

Forbid
: forbids concurrent runs, skipping next run if previous hasn't finished yet

Replace
: cancels currently running job and replaces it with a new one

### Execution timeout

If you have a cron job that may possibly hang sometimes, you can instruct yacron2
to terminate the process after N seconds if it's still running by then, via the
`executionTimeout` option. For example, the following cron job takes 2
seconds to complete, yacron2 will terminate it after 1 second:

```yaml
- name: test-03
  command: |
    echo "starting..."
    sleep 2
    echo "all done."
  schedule:
    minute: "*"
  captureStderr: true
  executionTimeout: 1  # in seconds
```

When terminating a job, it is always a good idea to give that job process some
time to terminate properly. The option `killTimeout` indicates how many seconds
to wait for the process to gracefully terminate before killing it more
forcefully. On Unix systems, we first send a SIGTERM, but if the process doesn't
exit after `killTimeout` seconds (30 by default) then we send SIGKILL:

```yaml
- name: test-03
  command: |
    trap "echo '(ignoring SIGTERM)'" TERM
    echo "starting..."
    sleep 10
    echo "all done."
  schedule:
    minute: "*"
  captureStderr: true
  executionTimeout: 1
  killTimeout: 0.5
```

### Change to another user/group

You can request that yacron2 change to another user and/or group for a specific
cron job (Unix only). The field `user` indicates the user (uid or username)
under which the subprocess must be executed. The field `group` (gid or group
name) indicates the group id. If only `user` is given, the group defaults to the
main group of that user. Example:

```yaml
- name: test-03
  command: id
  schedule:
    minute: "*"
  captureStderr: true
  user: www-data
```

Naturally, yacron2 must be running as root in order to have permissions to
change to another user.

### Remote web/HTTP interface

If you wish to remotely control yacron2, you can optionally enable an HTTP REST
interface, with the following configuration (example):

```yaml
web:
  listen:
     - http://127.0.0.1:8080
     - unix:///tmp/yacron2.sock
```

You may optionally require bearer-token authentication and set custom response
headers:

```yaml
web:
  listen:
    - http://127.0.0.1:8080
  authToken:
    value: my-secret-token
    # Alternatively:
    # fromFile: /etc/secrets/web-token
    # fromEnvVar: YACRON2_WEB_TOKEN
  headers:
    x-custom-header: value
  # octal permissions to apply to a unix:// socket
  socketMode: "660"
```

If `authToken` is configured but resolves to an empty value, yacron2 refuses to
start the web API (fail-closed), rather than serving it unauthenticated.

The following endpoints are available:

#### Get the version of yacron2

```shell
$ curl http://127.0.0.1:8080/version
1.0.4
```

#### Get the status of cron jobs

```shell
$ curl http://127.0.0.1:8080/status
test-01: scheduled (in 14 seconds)
test-02: scheduled (in 74 seconds)
test-03: running (pid: 12345)
```

You may also get status info in JSON format:

```shell
$ curl -H 'Accept: application/json' http://127.0.0.1:8080/status
[
  {"job": "test-01", "status": "scheduled", "scheduled_in": 6.16},
  {"job": "test-02", "status": "scheduled", "scheduled_in": 66.16}
]
```

#### Start a job right now

```shell
$ curl -X POST http://127.0.0.1:8080/jobs/test-02/start
```

### Includes

You may have a use case where it's convenient to have multiple config files,
and choose at runtime which one to use. In that case, it might be useful if
you can put common definitions (such as defaults for reporting, shell, etc.)
in a separate file, that is included by the other files via the `include`
directive. It takes a list of file names:

```yaml
include:
  - _inc.yaml

jobs:
  - name: my job
    ...
```

And your included `_inc.yaml` file could contain some useful defaults:

```yaml
defaults:
  shell: /bin/bash
  onPermanentFailure:
    report:
      mail:
        from: example@foo.com
        to: example@bar.com
        smtpHost: 127.0.0.1
```

### Obscure configuration options

#### enabled: true|false (default true)

It is possible to disable a specific cron job by adding an `enabled: false` option. Jobs
with `enabled: false` will simply be skipped, as if they aren't there, apart from
validating the configuration.

```yaml
jobs:
  - name: test-01
    enabled: false  # this cron job will not run until you change this to `true`
    command: echo "foobar"
    shell: /bin/bash
    schedule: "* * * * *"
```

## Differences from the Python version

The Rust port aims for full configuration and behavioural parity, with a few
deliberate exceptions:

* **Sentry reporting is not yet implemented.** The `sentry` report block is
  still parsed and validated (so existing configs load), but no event is sent —
  yacron2 logs a one-time warning if a DSN is configured. The `mail` and `shell`
  reporters are fully implemented.
* **The `logging:` section is informational only.** Python's `logging.dictConfig`
  has no equivalent here; logging is controlled by `--log-level` and the
  `RUST_LOG` environment variable. A present `logging:` block is accepted (for
  config compatibility) but not applied.
* **`user`/`group` are Unix-only.** On non-Unix platforms a job that requests a
  user or group change is a configuration error.
* **The `year` field works in the object schedule form.** The Python version
  silently dropped `year` when assembling an object-form schedule; here it is
  honoured, matching what the documentation always advertised.

## Contributing

Development setup, the test workflow, and the release process are documented in
[CONTRIBUTING.md](CONTRIBUTING.md).
