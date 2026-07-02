# Failure Detection and Retries

This page documents how yacron2 decides whether a job run failed (`failsWhen`), the exact precedence order of failure reasons, the retry mechanism with exponential backoff (`onFailure.retry`), and when each of the three report hooks (`onFailure`, `onPermanentFailure`, `onSuccess`) fires.

## Overview

After a job process exits, yacron2 computes a single failure reason from the run's exit code and captured output. If the reason is non-empty the run is *failed*; otherwise it *succeeded*. Failure triggers `onFailure` reporting and, if a retry is configured and not yet exhausted, schedules another run after a backoff delay. When retries are exhausted (or none was configured) `onPermanentFailure` reporting fires. Success cancels any pending retry and fires `onSuccess` reporting.

## Determining failure: `failsWhen`

`failsWhen` is a per-job (or per-`defaults`) block of four booleans. It is evaluated by `RunningJob.fail_reason` (`yacron2/job.py`) after the process exits and its streams have been read.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `producesStdout` | Bool | `false` | If true, any captured standard output marks the run as failed. |
| `producesStderr` | Bool | `true` | If true, any captured standard error marks the run as failed. |
| `nonzeroReturn` | Bool | `true` | If true, an exit code other than `0` marks the run as failed. |
| `always` | Bool | `false` | If true, the run is always considered failed regardless of exit code or output. |

In the strictyaml schema (`yacron2/config.py`), only `producesStdout` is required within a `failsWhen` map; `producesStderr`, `nonzeroReturn`, and `always` are `Opt(...)`. Defaults come from `DEFAULT_CONFIG["failsWhen"]` and are merged in before a `failsWhen` block is applied, so a partial `failsWhen` block inherits the defaults for the keys it omits.

Output detection considers both retained and discarded lines. A stream is treated as non-empty if it has saved content *or* if any lines were discarded (`saveLimit` exhausted, or `saveLimit: 0`). See [Output Capturing](Output-Capturing) for how `captureStdout`/`captureStderr` and `saveLimit` govern what is captured. If a stream is not captured, it cannot produce a failure reason: `producesStderr` only fires when `captureStderr` is enabled, and `producesStdout` only fires when `captureStdout` is enabled.

### Precedence order

`fail_reason` returns the first matching condition, in this fixed order, and `None` if none match:

1. `always` is true -> `"failsWhen=always"`.
2. `nonzeroReturn` is true and `retcode != 0` -> `"failsWhen=nonzeroReturn and retcode={retcode}"`.
3. `producesStdout` is true and stdout is non-empty or any stdout lines were discarded -> `"failsWhen=producesStdout and stdout is not empty"`.
4. `producesStderr` is true and stderr is non-empty or any stderr lines were discarded -> `"failsWhen=producesStderr and stderr is not empty"`.

The first match wins; later conditions are not evaluated. The resulting string is exposed to report templates as the `fail_reason` variable and to the shell reporter as `YACRON2_FAIL_REASON`. The boolean `failed` is simply `fail_reason is not None`.

### Special exit codes

Two synthetic exit codes are set by the runtime rather than the child process:

- **`127`**: the subprocess could not be launched at all (e.g. the command does not exist, or the argv could not be encoded). `RunningJob` sets `start_failed` and, in `wait()`, assigns `retcode = 127` so the run is treated as an ordinary failure rather than raising an internal error. With the default `nonzeroReturn: true`, this is a failure.
- **`-100`**: the run exceeded its `executionTimeout` and was cancelled. `wait()` sets `retcode = -100` before terminating the process. With the default `nonzeroReturn: true`, this is a failure. See [Concurrency and Timeouts](Concurrency-and-Timeouts).

A run cancelled to make way for a newer instance (`concurrencyPolicy: Replace`) is marked `replaced` and is *not* evaluated for failure, reported, or retried.

### Example

```yaml
jobs:
  - name: strict-job
    command: ./run.sh
    schedule: "*/5 * * * *"
    captureStdout: true
    captureStderr: true
    failsWhen:
      producesStdout: false
      producesStderr: true
      nonzeroReturn: true
      always: false
```

## Retries: `onFailure.retry`

Retries are configured under `onFailure.retry`. Retry orchestration lives in `yacron2/cron.py` (`launch_scheduled_job`, `handle_job_failure`, `schedule_retry_job`, `cancel_job_retries`); per-job backoff state is `JobRetryState` in `yacron2/job.py`.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `maximumRetries` | Int | `0` | Number of retries after the initial failed run. `0` disables retrying. `-1` retries forever. |
| `initialDelay` | Float | `1` | Delay in seconds before the first retry. |
| `maximumDelay` | Float | `300` | Upper bound in seconds on the backoff delay. |
| `backoffMultiplier` | Float | `2` | Factor the delay is multiplied by after each retry. |

In the schema, all four keys are required *within* a `retry` map (no `Opt(...)`), but the entire `retry` map is optional and the `DEFAULT_CONFIG` values above are merged in, so a job that omits `retry` entirely gets these defaults. If a `retry` map *is* given, strictyaml requires all four keys to be present (a partial `retry` block is a validation error). Numeric ranges are validated in `JobConfig._validate_numeric_ranges` and raise `ConfigError` on violation:

- `maximumRetries >= -1`
- `initialDelay >= 0`
- `maximumDelay > 0`
- `backoffMultiplier > 0`

### Exponential backoff

`JobRetryState.next_delay()` returns the current delay, then advances it for the next retry:

```
delay      = current delay (returned, used to sleep)
next delay = min(current delay * backoffMultiplier, maximumDelay)
```

The first retry waits `initialDelay`; each subsequent retry waits the previous delay times `backoffMultiplier`, capped at `maximumDelay`. With `initialDelay: 1`, `backoffMultiplier: 2`, `maximumDelay: 30`, the delay sequence is 1, 2, 4, 8, 16, 30, 30, ... seconds. The retry counter (`count`) increments on each `next_delay()` call.

### Retry lifecycle

- A retry state is created only when `maximumRetries` is truthy (non-zero). With `maximumRetries: 0` no state is created and a failed run goes straight to permanent failure.
- `launch_scheduled_job` calls `cancel_job_retries(name)` before starting a scheduled run, then creates a fresh `JobRetryState`. A scheduled run therefore resets any in-progress retry sequence for that job. A manually triggered run (`POST /jobs/{name}/start`, see [HTTP Control API](HTTP-API)) goes through `maybe_launch_job` directly and does *not* reset or create retry state; it reuses whatever retry state currently exists.
- On each failed run, `handle_job_failure` fires `onFailure` reporting, then: if no retry state exists or it was cancelled, fires `onPermanentFailure` and stops; otherwise, if `count >= maximumRetries` and `maximumRetries != -1`, cancels the retry state and fires `onPermanentFailure`; otherwise schedules the next retry after `next_delay()` seconds.
- A success (`handle_job_success`) calls `cancel_job_retries` and fires `onSuccess`, ending the sequence.
- If a job is removed from the configuration while a retry is pending, `schedule_retry_job` logs a warning, discards the stale retry state, and skips the run cleanly (no exception).
- When leader election is enabled (`cluster.electLeader`), `schedule_retry_job` re-checks the cluster gate before relaunching. A transient fail-closed condition (lost quorum, a detected conflict, a rebuilt gossip manager's still-converging view, a backend read error) does *not* end the sequence: the retry state is kept and the gate is re-checked after another delay of the same length (floored at one second; the first deferral of a wait is logged at INFO, repeats at DEBUG), so a keep-alive job survives the blip. Only when another node is *positively* identified as the job's owner is the pending retry **abandoned**: the retry state is cancelled and discarded, a WARNING is logged, and the abandonment is recorded in the run history as `cancelled`. An abandoned sequence ends without firing `onPermanentFailure`, and the failed attempt is not re-run elsewhere: the new owner only picks up the job's *future scheduled firings*, which an `@reboot` one-shot does not have (its boot run is already recorded, so an abandoned `@reboot` keep-alive ends cluster-wide). See [Clustering and Leader Election](Clustering-and-Leader-Election).
- On shutdown, all pending retries are cancelled before yacron2 exits.

### Retry example

```yaml
jobs:
  - name: flaky-job
    command: ./flaky.sh
    schedule: "*/10 * * * *"
    captureStderr: true
    onFailure:
      report:
        mail:
          from: cron@example.com
          to: ops@example.com
          smtpHost: 127.0.0.1
      retry:
        maximumRetries: 10
        initialDelay: 1
        maximumDelay: 30
        backoffMultiplier: 2
```

### Restart a long-running process

A schedule of `@reboot` runs the job once at yacron2 startup. Combined with `maximumRetries: -1`, this re-launches the process whenever it exits with a failure, indefinitely: a way to keep a long-running process alive under yacron2.

```yaml
jobs:
  - name: keep-alive
    command: ./long-running-server
    schedule: "@reboot"
    onFailure:
      retry:
        maximumRetries: -1
        initialDelay: 1
        maximumDelay: 30
        backoffMultiplier: 2
```

See [Schedules and Timezones](Schedules-and-Timezones) for `@reboot` semantics.

## Report hooks

Each hook has its own independent `report` block (Sentry, mail, shell, webhook), defaulted from `_REPORT_DEFAULTS` (deep-copied per hook so they do not alias). All four reporters in a block run for the relevant outcome; reporting errors are logged and do not abort the others. See [Reporting (Mail, Sentry, Shell, Webhook)](Reporting) for the report block options.

| Hook | Fires when | Frequency |
| --- | --- | --- |
| `onFailure.report` | Every failed run. | Once per failed attempt (including each retry that fails). |
| `onPermanentFailure.report` | Retries are exhausted, or no retry was configured, or the retry state was cancelled. | Once, at the end of a failing sequence. |
| `onSuccess.report` | The run succeeded (`fail_reason is None`). | Once per successful run. |

With no retry configured, a single failed run fires both `onFailure.report` (always) and then `onPermanentFailure.report` (because there is no retry state). To report only after all retries are exhausted, leave `onFailure.report` empty and configure `onPermanentFailure.report` instead, as in the example below.

```yaml
jobs:
  - name: eventually-consistent
    command: ./run.sh
    schedule: "*/10 * * * *"
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
          from: cron@example.com
          to: ops@example.com
          smtpHost: 127.0.0.1
```

A note on mail reporting: an `onSuccess` mail whose rendered body is empty (after `strip()`) is suppressed (no email is sent). This applies only to success reports.

## Notes

- `nonzeroReturn` checks `retcode != 0`, so both the synthetic `127` (launch failure) and `-100` (timeout) codes count as non-zero failures under the default.
- The `failsWhen` evaluation runs once per completed run, including each retried run, so a retry that still produces stderr (with `producesStderr: true`) fails again and continues the backoff sequence.
- Output-based failure (`producesStdout`/`producesStderr`) depends on stream capturing; without `captureStdout`/`captureStderr` the corresponding condition can never trigger because nothing is captured.
- During shutdown, `handle_job_failure` returns early if the stop event is set: a job that finishes failing while yacron2 is shutting down is *not* reported (`onFailure`/`onPermanentFailure` do not fire) and is not retried. A job that finishes successfully during shutdown still cancels its retries and fires `onSuccess`.
