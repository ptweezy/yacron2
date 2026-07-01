# Output Capturing

This page documents how yacron2 handles a job's standard output and standard
error: which streams are captured, how captured output is prefixed and
re-emitted, how much is retained for reports, and the line-length limit applied
to the underlying reader.

## Overview

For each job, yacron2 decides per stream whether to *capture* it. The decision
is made independently for stdout (`captureStdout`, default `false`) and stderr
(`captureStderr`, default `true`).

- A **captured** stream is read line-by-line by yacron2. Each line is decoded as
  UTF-8, re-emitted to yacron2's own stdout/stderr (with a configurable prefix),
  and retained in memory (subject to `saveLimit`) so it can be included in
  [reports](Reporting) and exposed to [failure detection](Failure-Detection-and-Retries).
- An **uncaptured** stream is not piped through yacron2: the child process
  inherits yacron2's own stdout/stderr file descriptors, so its output passes
  through directly. Such output is *not* retained, *not* prefixed, and *not*
  available to reporters or to the `producesStdout`/`producesStderr` failure
  checks.

Whether a captured stream is re-emitted to yacron2's stdout or stderr depends on
the original stream, not on which stream was captured: captured stdout lines are
written to yacron2's stdout, captured stderr lines to yacron2's stderr.

## Options

These options are per-job and may also be set in a `defaults` block (see
[Includes, Defaults, and Multi-File Config](Includes-and-Defaults)). All are
optional (`Opt(...)` in the schema). Types and defaults are taken from the
strictyaml schema and `DEFAULT_CONFIG`.

| Option | Type | Default | Description |
|---|---|---|---|
| `captureStdout` | boolean | `false` | Capture the job's standard output: read, prefix, re-emit to yacron2's stdout, and retain for reports/failure checks. |
| `captureStderr` | boolean | `true` | Capture the job's standard error: read, prefix, re-emit to yacron2's stderr, and retain for reports/failure checks. |
| `streamPrefix` | string | `"[{job_name} {stream_name}] "` | Format string prepended to each re-emitted captured line. Supports `{job_name}` and `{stream_name}`. Set to `""` to disable. |
| `saveLimit` | integer | `4096` | Maximum number of lines retained per captured stream for reporting. Must be `>= 0`; `0` retains nothing but still counts discarded lines. |
| `maxLineLength` | integer | `16777216` (16 MiB) | Maximum length, in bytes, of a single line the underlying asyncio reader will buffer. Must be `> 0`. Lines exceeding it are skipped with a warning. |

`saveLimit` and `maxLineLength` are validated at config load time: a non-integer
fails the strictyaml schema, and `saveLimit < 0` or `maxLineLength <= 0` raises
a `ConfigError`.

## What "capture" means

When a stream is captured, yacron2 launches the subprocess with that stream
connected to a pipe (`asyncio.subprocess.PIPE`) and starts a `StreamReader` task
that loops over `readline()`. For each line:

1. The raw bytes are decoded with `"utf-8"` and `errors="replace"`, so a job
   that emits non-UTF-8 bytes does not crash the reader; invalid sequences
   become the Unicode replacement character.
2. The decoded line, with `streamPrefix` formatted and prepended, is written to
   yacron2's own stdout (for stdout lines) or stderr (for stderr lines) and
   flushed. Job stderr is written to yacron2's stderr, never to stdout.
3. The (unprefixed) line is retained according to `saveLimit`.

If a stream is not captured, no pipe is created for it and no `StreamReader` is
started; the child inherits yacron2's corresponding file descriptor.

### Encoding of re-emitted lines

Re-emitted lines are written as encoded bytes to the underlying buffer so
yacron2 controls the encoding. If the console encoding cannot represent the text
(`UnicodeEncodeError`), yacron2 falls back to encoding as ASCII with
replacement. Retained output (used in reports) is the UTF-8/`replace`-decoded
string and is unaffected by this console fallback.

## streamPrefix

`streamPrefix` is a Python `str.format` template applied once per
`StreamReader`. Two placeholders are substituted:

- `{job_name}`: the job's `name`.
- `{stream_name}`: `"stdout"` or `"stderr"`.

With the default `"[{job_name} {stream_name}] "`, a job named `test-01` emits
lines such as `[test-01 stdout] hello`.

To change the prefix:

```yaml
jobs:
  - name: test-01
    command: echo "hello world"
    schedule:
      minute: "*/2"
    captureStdout: true
    streamPrefix: "[{job_name} job] "
```

To remove the prefix entirely (for example when the job emits structured JSON
log lines that should pass through unmodified), set it to the empty string:

```yaml
jobs:
  - name: test-01
    command: echo '{"msg":"hello world"}'
    schedule:
      minute: "*/2"
    captureStdout: true
    streamPrefix: ""
```

Note the trailing space in the default prefix; a custom prefix is concatenated
directly with the line, so include your own separator if you want one.

## saveLimit and discarded-line accounting

`saveLimit` bounds how many lines per captured stream are retained for reporting.
The `StreamReader` does not keep the most recent N lines; it keeps the **first
half and the last half**, so both the beginning and the end of long output
survive while the middle is dropped:

- The first `saveLimit // 2` lines are stored in a top buffer.
- After the top buffer is full, subsequent lines go into a bottom buffer holding
  at most `saveLimit - saveLimit // 2` lines. When that bottom buffer is full,
  the oldest line in it is evicted and a discard counter is incremented.

When the retained output is assembled, if any lines were discarded a marker line
is inserted between the top and bottom halves:

```
   [.... N lines discarded ...]
```

where `N` is the number of discarded lines. The marker is only present when
discards occurred and when the bottom buffer is non-empty.

### saveLimit = 0

`saveLimit` may be set to `0`. With `saveLimit: 0`, no
lines are retained at all: every line is counted as discarded. The lines are
still decoded and re-emitted with their prefix as usual; only the in-memory
retention for reports is suppressed. The discard count is preserved, which
matters for failure detection (below).

## maxLineLength

`maxLineLength` (default 16 MiB) is passed as the `limit` to the asyncio stream
reader when either stream is captured. It bounds how many bytes the reader will
buffer for a single line. If a line exceeds this limit, `readline()` raises a
`ValueError`, which the `StreamReader` catches: it logs a warning

```
job <name>: ignored a very long line
```

and continues reading the next line. The oversized line is neither retained nor
re-emitted, and is **not** counted as a discarded line.

## Interaction with failure detection

The `producesStdout` and `producesStderr` checks in `failsWhen` (see
[Failure Detection and Retries](Failure-Detection-and-Retries)) consider a
stream non-empty if it has **either** retained output **or** a non-zero discard
count. Consequently, output that was produced but discarded (including all
output when `saveLimit: 0`) still triggers these failure conditions. Lines
skipped because they exceeded `maxLineLength` are not counted as discards and
therefore do not, on their own, satisfy these checks.

Because these checks operate only on captured streams, `producesStdout` has no
effect unless `captureStdout` is enabled, and `producesStderr` has no effect
unless `captureStderr` is enabled.

## Examples

Capture both streams with the default prefix and retain up to 1000 lines each:

```yaml
jobs:
  - name: report-builder
    command: ./build-report.sh
    schedule:
      minute: "0"
      hour: "6"
    captureStdout: true
    captureStderr: true
    saveLimit: 1000
```

Let stdout pass through to yacron2's stdout unmodified while capturing stderr for
failure reports:

```yaml
jobs:
  - name: importer
    command: ./import.sh
    schedule:
      minute: "*/15"
    captureStdout: false
    captureStderr: true
```

## See also

- [Reporting (Mail, Sentry, Shell)](Reporting): how captured `stdout`/`stderr`
  appear in report templates and shell-reporter environment variables.
- [Failure Detection and Retries](Failure-Detection-and-Retries):
  `failsWhen.producesStdout` / `producesStderr`.
- [Configuration Reference](Configuration-Reference): full option list.
- [Logging Configuration](Logging-Configuration): yacron2's own logging, which
  is separate from job output capturing.
