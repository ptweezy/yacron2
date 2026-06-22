# History

yacron2 is a fork of [yacron](https://github.com/gjcarneiro/yacron),
continuing from yacron 0.19.  The 1.0.x entries below document the fork; the
entries from 0.19.0 onward document the history of the original yacron
project, on which yacron2 is based.

## 1.1.5 (2026-06-22)

This is a documentation release; there are no changes to the `yacron2`
package itself.

### Documentation

- README changes
- Add an `Architectures` badge to the README summarising the binary and
  container targets (`amd64`, `arm64`, `i686`, `armv7`, `ppc64le`,
  `s390x`).

### Release automation

- Default the manual (`workflow_dispatch`) release to a `patch` bump and
  list `patch` first in the bump options, since patch releases are the
  common case.


## 1.1.4 (2026-06-22)

- Add self-contained binaries for two more Linux architectures to every
  release, in both glibc and musl flavors: little-endian POWER (`ppc64le`)
  and IBM Z (`s390x`) — `yacron2-linux-ppc64le`, `yacron2-linux-s390x`, and
  their `-musl` variants — alongside the existing `amd64`, `arm64`, `i686`
  and `armv7` builds. As with the other binaries, Python is not required on
  the target system. Neither arch has a native GitHub runner, so they build
  inside a container via `docker run --platform` under QEMU emulation; both
  have prebuilt manylinux and musllinux wheels for the aiohttp dependency
  stack, so nothing compiles from source.
- The published container image now covers them too: the multi-arch image
  adds `linux/ppc64le` and `linux/s390x` (to `linux/amd64`, `linux/arm64`,
  `linux/386` and `linux/arm/v7`), and is build-checked at that full arch
  coverage on every commit.


## 1.1.3 (2026-06-22)

- Add self-contained binaries for two more Linux architectures to every
  release, in both glibc and musl flavors: 32-bit x86 (`yacron2-linux-i686`
  and `yacron2-linux-i686-musl`) and 32-bit ARM (`yacron2-linux-armv7` and
  `yacron2-linux-armv7-musl`), alongside the existing 64-bit `amd64` and
  `arm64` builds. As with the other binaries, Python is not required on the
  target system. The 32-bit binaries are built inside a 32-bit container
  (`i686` natively on the x86-64 runner, `armv7` under QEMU emulation).
- The published container image now covers those architectures too: the
  multi-arch image is built for `linux/amd64`, `linux/arm64`, `linux/386`
  and `linux/arm/v7`, and is build-checked at that full arch coverage on
  every commit.


## 1.1.2 (2026-06-21)

This is a documentation release; there are no changes to the `yacron2`
package itself.

### Documentation

- Add a project wiki (under `wiki/`) covering installation, the
  configuration reference, the HTTP API, the web dashboard, schedules and
  timezones, reporting, statsd metrics, output capturing, concurrency and
  timeouts, failure detection and retries, includes and defaults, logging,
  the CLI, architecture and internals, production deployment, migration
  from yacron, contributing/releasing, and troubleshooting.
- Showcase the web dashboard near the top of the README with annotated
  screenshots of the overview, live log tail, run history, schedule
  preview, command palette, keyboard-shortcut reference, and the
  green-phosphor and flat modern themes, linking the dashboard tour in the
  wiki.
- Slim the README's web-server section to an "Enabling the web dashboard"
  pointer to that showcase and the wiki, removing the duplicated feature
  list.


## 1.1.1 (2026-06-21)

### Features

- Add a built-in web dashboard, served at the root path (`/`) of any
  `http://` listener. It shows each job's latest status with a live
  countdown to the next run and a trend sparkline, tails job logs live
  (with in-log search, ANSI-colour rendering, optional timestamps, a
  line-wrap toggle, and a download button), runs or cancels jobs on
  demand, and reports each job's run history, success rate, and a
  plain-English schedule with a preview of upcoming run times. It is
  keyboard-first (`?` for shortcuts, `Ctrl-K`/`⌘K` command palette, `/`
  to filter), with configurable themes, a compact density mode, polling
  interval, and optional desktop failure notifications, all remembered
  in the browser.
- Cancel running jobs over the REST API with `POST /jobs/{name}/cancel`,
  using the same graceful SIGTERM-then-SIGKILL sequence (honouring
  `killTimeout`) as elsewhere. A cancelled run is recorded with a
  `cancelled` outcome and is neither reported nor retried; the endpoint
  returns `409 Conflict` if the job is not running and `404 Not Found`
  for an unknown job.
- `GET /jobs` now returns detailed per-job information — schedule,
  timezone, enabled/running state, time until the next run, a summary of
  the most recent finished run, and a compact recent-outcome history.
- Read a job's retained run history and aggregate statistics (success
  rate and average/min/max duration) with `GET /jobs/{name}/runs`.
- Tail a job's captured output live over Server-Sent Events with
  `GET /jobs/{name}/logs`, replaying the most recent run's buffered
  output before streaming new lines.
- Add a `web.ui` option; set `ui: false` to expose only the REST API and
  disable the dashboard.
- Keep run history and live logs in memory only, so the dashboard does
  not change yacron2's read-only-filesystem deployment story; history
  resets when yacron2 restarts.
- Ship a `docker-compose.yml` and a demo crontab for trying the
  dashboard against a set of varied example jobs.

### Security

- Serve the dashboard with a strict `Content-Security-Policy` and
  additional hardening headers (`X-Content-Type-Options`,
  `X-Frame-Options`, `Referrer-Policy`); each can be overridden via
  `web.headers` while unset defaults are still applied.
- When bearer-token authentication (`web.authToken`) is enabled, the
  dashboard page loads without a token and then prompts for one, storing
  it only in that browser tab; every data request it makes is
  authenticated with that token.


## 1.0.16 (2026-06-21)

- Publish container images to Docker Hub as `docker.io/ptweezy/yacron2`
  on every release, in addition to GHCR. The two registries carry the
  same multi-arch (`linux/amd64` + `linux/arm64`) image, so you can
  pull from whichever you prefer.
- Document the Docker Hub images in the README and add a quick-start
  `docker run` example and a Docker Hub badge.
- Harden the release workflow so Docker Hub publishing is enabled only
  when both `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` are configured.


## 1.0.15 (2026-06-21)

- Lower the minimum required Python version from 3.13 to 3.10;
  yacron2 now supports Python 3.10, 3.11, 3.12, 3.13, and 3.14.
- Add PyPI trove classifiers for Python 3.10, 3.11, and 3.12 so the
  expanded support is reflected on the package page.
- Expand the test matrix (`tox` and CI) to run across all five
  supported interpreters (3.10â€“3.14).
- Type-check with `mypy` against Python 3.10 so stdlib APIs that are
  unavailable on the lowest supported interpreter are caught at lint
  time rather than at runtime.


## 1.0.14 (2026-06-21)

Since 1.0.13, the net changes are entirely build/CI hardening (a new `build.yml`, an `arm64` addition to `docker.yml`). Here's the changelog body:

- Add a per-commit build-verification workflow that builds the wheel,
  `sdist`, and self-contained PyInstaller binaries for Linux (both
  glibc and musl/Alpine, on `amd64` and `arm64`) and macOS (`amd64`
  and `arm64`) on every push, without publishing, so a broken build or
  bundle is caught at commit time instead of only at release.
- Build-verify the Docker image for both `linux/amd64` and
  `linux/arm64` on every commit, catching arm64-only breakage (such as
  a dependency with no arm64 wheel) that the previous amd64-only check
  would miss.

## 1.0.13 (2026-06-20)

### Improvements

- Update the bundled Python runtime in the standalone binaries to
  `3.13.14` (from `3.13.5`), picking up the latest upstream bug and
  security fixes.
- Expand the PyPI package metadata with additional keywords, trove
  classifiers, and project links (`Documentation`, `Source`,
  `Changelog`, `Issues`, and `Container`) for easier discovery.

### Documentation

- Tidy up `README.md`, trimming redundant badges and condensing the
  macOS code-signing notes.


## 1.0.12 (2026-06-20)

- Update the GitHub Actions used to build and publish Docker images
  (`docker/metadata-action`, `docker/login-action`,
  `docker/setup-qemu-action`, `docker/setup-buildx-action`, and
  `docker/build-push-action`) to their latest major versions.
- Update the release workflow's `actions/upload-artifact`,
  `actions/download-artifact`, and `softprops/action-gh-release`
  actions to their latest major versions.


## 1.0.11 (2026-06-20)

- The macOS binaries are now Developer ID code-signed and notarized by
  Apple, so Gatekeeper accepts them and they run without first clearing
  the quarantine attribute (`xattr -d com.apple.quarantine` is no longer
  needed).


## 1.0.10 (2026-06-20)

- Release binaries now include macOS builds for both Apple Silicon
  (`yacron2-macos-arm64`) and Intel (`yacron2-macos-amd64`),
  alongside the existing Linux glibc and musl binaries. As with the
  Linux binaries, Python is not required on the target machine.
- Document clearing the macOS Gatekeeper quarantine with
  `xattr -d com.apple.quarantine` before first running the macOS
  binaries, which are unsigned and unnotarized.
- Fix a typo in the README fork attribution.


## 1.0.9 (2026-06-20)

### Documentation

- Document that the standalone binary is self-extracting: on each
  start it unpacks its embedded Python runtime into a temporary
  directory, so it requires a temp directory that is both writable
  and executable.
- Add guidance for running the binary under a read-only root
  filesystem â€” mount a small `rw,exec` tmpfs at `/tmp` (Docker's
  `--tmpfs` defaults to `noexec`, which fails), use a Kubernetes
  `emptyDir`, or point `TMPDIR` at a writable, executable directory.
- Clarify that this temp-directory requirement is unique to the
  standalone binary: the published container image and `pip`/`pipx`
  installs run yacron2 as a normal Python package and need no
  writable temp directory.

### Container image

- The official multi-arch (`linux/amd64` + `linux/arm64`) container
  image is now built and published to GHCR automatically as part of
  every release, and is build-checked on every commit so a broken
  `Dockerfile` fails fast.


## 1.0.8 (2026-06-20)

- Add self-contained musl binaries to every release for Alpine and
  other musl-based systems: `yacron2-linux-amd64-musl` and
  `yacron2-linux-arm64-musl`, alongside the existing glibc
  `yacron2-linux-amd64` and `yacron2-linux-arm64` builds. Python is
  not required on the target system.
- Build the release binaries with Python 3.14.


## 1.0.7 (2026-06-20)

- GitHub Releases now use the curated `HISTORY.md` section for the
  release as the body of the release notes. The matching `## X.Y.Z`
  entry is extracted and shown above GitHub's auto-generated "What's
  Changed" list and changelog compare link, so each release page leads
  with the human-written changelog instead of only auto-generated
  notes.


## 1.0.6 (2026-06-20)

- Release binaries are now published for both `linux/amd64` and
  `linux/arm64`. Every GitHub Release attaches a self-contained
  `yacron2-linux-amd64` and `yacron2-linux-arm64` executable, each built
  natively on its target architecture (previously only a single binary
  was provided).
- The downloadable binaries embed Python, so none is required on the
  target system, and run on any Linux host with glibc 2.39 or newer
  (e.g. Ubuntu 24.04) matching the CPU architecture.
- Each binary is smoke-tested with `--version` and built before
  publishing


## 1.0.5 (2026-06-20)

- docker builds

## 1.0.4 (2026-06-19)

### Reliability fixes

- Config reload failures no longer risk crashing the scheduler: if
  re-reading the configuration fails (for example, a YAML error introduced
  while yacron2 is running), the previously-loaded jobs keep running
  instead of the main loop failing on an unset `config` reference.
- A job whose command cannot be launched (for example, the executable does
  not exist) is now reported as an ordinary failure with exit code `127`,
  instead of raising `RuntimeError("process is not running")` and being
  logged as an internal "please report this as a bug" error.
- statsd reporting is now strictly best-effort: a failure to send the
  `job_started`/`job_stopped` metrics (for example, an unresolvable statsd
  host) is logged as a warning instead of propagating out of job
  start/stop.
- The mail reporter now always closes its SMTP connection, even when
  `STARTTLS`, login, or sending fails, so a misbehaving mail server can no
  longer leak one connection per report.
- Sentry and e-mail reporting no longer raise `KeyError` when the DSN or
  password is configured with `fromEnvVar` but the environment variable is
  unset; yacron2 logs an error and skips that report instead.

### Configuration

- The Sentry `fingerprint` setting now replaces rather than appends when
  merging `defaults`: a job (or `defaults` block) that defines its own
  `fingerprint` overrides the default entirely, so custom Sentry issue
  grouping works as configured (previously the three default entries were
  silently prepended).
- `include` cycles are now detected and rejected with a clear `ConfigError`
  ("include cycle detected") instead of recursing until a `RecursionError`.
- Jobs loaded from a configuration directory are now processed in sorted
  filename order, so job ordering and "first config found" messages are
  deterministic rather than dependent on the filesystem's directory order.
- Environment files (`env_file`) are now read as UTF-8.

### Security

- The web API's `Authorization` check now treats the `Bearer` auth scheme
  as case-insensitive (per RFC 7235), while still comparing the token
  itself in constant time.
- The mail reporter no longer logs the name of the configured password
  environment variable.

### Internal

- Refactored `JobConfig` construction into focused helper methods and
  switched `send_to_statsd` to `asyncio.get_running_loop()`; no behavioural
  change.
- Added a `.github/CODEOWNERS` file.

## 1.0.3 (2026-06-19)

This is a tooling and documentation release; there are no changes to the
`yacron2` package itself.

### Release automation & CI

- Added an opt-in, marker-driven `release` GitHub Actions workflow: a push to
  `main` whose commit message carries a release marker on its own line
  (`[release]` / `[release:major|minor|patch]`), or a manual run, gates on
  `tox`, builds at the next version, publishes to PyPI via Trusted Publishing
  (OIDC), and only after a successful publish tags the commit and cuts a GitHub
  Release.
- Hardened the release trigger to match a whole-line marker, so a `[release]`
  mention inside prose never triggers a publish, and added a local `commit-msg`
  hook (`scripts/gen_changelog_entry.py`) that drafts a changelog entry for
  release commits.
- Set least-privilege `permissions: contents: read` defaults on the `tox` and
  `release` workflows.

### Docs

- Added `CONTRIBUTING.md` documenting the development setup, the
  test/lint/type-check workflow, and the release process, and linked it from the
  README.
- Converted the changelog from reStructuredText to Markdown (`HISTORY.rst` ->
  `HISTORY.md`) and pointed the changelog generator, the `commit-msg` hook, and
  `CONTRIBUTING.md` at the Markdown changelog.

### Packaging

- Promote the PyPI `Development Status` classifier from `4 - Beta` to
  `5 - Production/Stable` to reflect the stable 1.0 release series. No code
  changes.

## 1.0.1 (2026-06-19)

### Security & behavior fixes

- The web API now fails closed when `web.authToken` is configured but
  resolves to an empty token (an unset `fromEnvVar`, or an empty/missing
  `fromFile`): yacron2 raises a `ConfigError` and refuses to start the
  HTTP server, instead of silently serving the control API without
  authentication.
- The web API now honours `enabled: false`. `POST /jobs/<name>/start`
  returns `409 Conflict` for a disabled job rather than launching it, and
  `GET /status` reports such jobs as `disabled` instead of an
  inapplicable `scheduled (in N seconds)`.
- Invalid `web.listen` URLs (an unsupported scheme, or an `http` url
  missing host/port) are now logged as a warning and skipped, instead of being
  surfaced as an internal "please report this as a bug" error; a bind failure
  (`OSError`) on one address likewise no longer aborts the whole config
  update. The "started listening" message is logged only after the bind
  actually succeeds.
- `concurrencyPolicy: Replace` no longer reports the replaced (cancelled)
  job instance as a failure and no longer schedules retries for it; the forced
  termination is treated as a replacement, not a job failure.

### Cleanups

- Removed a dead Windows event-loop branch from `main()` (yacron2 is
  POSIX-only because it imports `grp`/`pwd` at load time).
- `naturaltime` no longer relies on an `assert` for control flow (which
  would be stripped under `python -O`).
- The concurrency-policy test was rewritten to be deterministic (it was
  previously an `xfail` that could never exercise a second job instance).

## 1.0.0 (2026-06-19)

### About this release

- yacron2 1.0.0 is the first release of the yacron2 fork, based on
  gjcarneiro/yacron 0.19. It carries forward all of upstream yacron's
  functionality and adds modernized packaging, a Python 3.13+ runtime, new
  web-API authentication, and a set of security and correctness fixes.
- The project, package, command, config directory, and reporter environment
  variables have all been renamed from `yacron` to `yacron2` (see Breaking
  changes for migration steps).

### Breaking changes

- The installed command and PyPI distribution are renamed `yacron` ->
  `yacron2` (install with `pip install yacron2`; run `yacron2`). The
  Python import package is now `yacron2` and the entry point is
  `yacron2.__main__:main`.
- The default config directory changed from `/etc/yacron.d` to
  `/etc/yacron2.d`; operators relying on the default path must move their
  config directory.
- Minimum Python is now 3.13 (`requires-python >=3.13`); only Python 3.13 and
  3.14 are supported. Python 3.7 through 3.12 are no longer supported.
- Reporter shell environment variables were renamed `YACRON_*` ->
  `YACRON2_*` (e.g. `YACRON2_JOB_NAME`, `YACRON2_RETCODE`). Existing
  `onFailure`/`onSuccess` shell scripts must be updated.
- mail `validate_certs` now defaults to `True`, so SMTP TLS certificate
  validation is enabled unless explicitly disabled. Delivery to servers with
  self-signed/invalid certificates that previously worked silently will now
  fail unless `validate_certs: false` is set.
- Privilege drop now drops/sets supplementary groups (`os.initgroups` /
  `os.setgroups`) before `setuid`, fixing a privilege-escalation bug where
  root's supplementary group memberships leaked into the child. A numeric
  `user` without an explicit `group` now derives its primary gid from the
  passwd database instead of silently keeping yacron's gid 0.
- `defaults.environment` now merges by key instead of concatenating: a job
  overriding a default variable yields a single entry. Configs relying on the
  old duplicate-key concatenation behave differently.
- Dependency pins changed: `crontab` jumped from `==0.22.8` to `>=1,<2`
  (major version change), `strictyaml` to `>=1.7,<2`, `aiohttp` to
  `>=3.10,<4`, `aiosmtplib` to `>=3,<6` (v2+ login API), `sentry-sdk`
  to `>=2,<3`. `pytz` and the direct `ruamel.yaml` pin were dropped;
  `tzdata>=2024.1` was added.

### Features & behavior

- New `web.authToken` option adds opt-in bearer-token authentication to the
  HTTP API (literal `value`, `fromFile`, or `fromEnvVar`); when set, an
  aiohttp middleware requires `Authorization: Bearer <token>` on every route,
  compares it in constant time (`hmac.compare_digest`), and returns 401
  otherwise.
- New `web.socketMode` option sets octal permissions on `unix://` listen
  sockets, logging a warning rather than failing on invalid values; non-unix
  schemes are ignored.
- Job stderr is now written to the process's stderr instead of stdout, so
  operators separating yacron2's own stdout/stderr streams get correctly routed
  output.
- Config now validates numeric ranges at load time and raises a clear
  `ConfigError` for invalid values (`saveLimit>=0`, `maxLineLength>0`,
  `killTimeout>=0`, `executionTimeout>0`, and `onFailure.retry`
  constraints) instead of failing obscurely at runtime.
- Multi-file config directories now aggregate jobs, defaults, and logging
  across all files instead of using only the last file's settings. Duplicate
  `web` or `logging` blocks across the directory raise a `ConfigError`,
  an empty/all-skipped directory yields an empty config (no
  `UnboundLocalError`), and a missing/unreadable single config file now
  raises a clear `ConfigError`.
- Logging configuration is now re-applied on reload when it changes and is only
  marked applied on success, so a logging section fixed after an error or
  changed at runtime is picked up without a restart.
- Scheduling a retry for a job that was removed from the configuration
  mid-retry no longer crashes; the stale retry state is cleared and the retry
  is skipped.
- Job stop metrics (statsd `job_stopped`) are now emitted exactly once per
  run; a guard makes `_on_stop` idempotent, preventing duplicate metrics when
  `cancel` races `wait` (e.g. `concurrencyPolicy=Replace`).
- Non-UTF-8 job output no longer crashes the stream reader (output is decoded
  with `errors='replace'`).
- A job with an empty environment list now gets its environment assigned
  correctly (previously left `None`).
- Email reports now set an RFC 5322 `Date` header
  (`email.utils.format_datetime`), encode HTML bodies with the correct
  charset/transfer-encoding (`set_content` subtype `html`), and call
  `smtp.login` positionally for aiosmtplib v2+ compatibility.
- The Sentry client is now initialised once per `(dsn, environment)` and
  cached instead of on every report, and uses `sentry_sdk.new_scope()`
  (replacing the deprecated `push_scope()`).
- Report templates (sentry/mail body, subject, fingerprint) are now compiled
  and cached via an `lru_cache`, and the three report blocks (`onFailure`,
  `onPermanentFailure`, `onSuccess`) deep-copy their defaults so they no
  longer alias one shared mutable object.
- The shell reporter now logs a nonzero reporter exit code via `logger.error`
  (clean message) instead of `logger.exception` (which logged a bogus
  `NoneType: None` traceback).
- statsd UDP errors are now logged with their detail (`UDP error received:
  %s`) instead of being dropped due to a missing format placeholder.

### Python & runtime

- Timezone handling migrated from third-party `pytz` to the standard-library
  `zoneinfo`; invalid timezones now raise `ConfigError`.
- Added `tzdata>=2024.1` so `zoneinfo` can resolve timezones on
  slim/minimal container images that don't ship the system tz database.
- The asyncio event loop is now created with `asyncio.new_event_loop()`
  instead of the deprecated `asyncio.get_event_loop()` (carried from
  upstream).
- Internal logger and argparse program name updated to `yacron2`; CLI
  error/version output now reads `yacron2`.

### Packaging & build

- Migrated from legacy `setup.py`/`setup.cfg` to a PEP 621
  `pyproject.toml` using the setuptools build backend (`setuptools>=77`,
  `setuptools_scm>=8`); `setup.py` and `setup.cfg` were removed.
- Versioning continues via setuptools_scm, now configured under
  `[tool.setuptools_scm]` writing `yacron2/version.py`.
- Adopted a PEP 639 SPDX license expression (`license = "MIT"`) with
  `license-files`, and updated the LICENSE with a `Copyright (c) 2026, the
  yacron2 developers` line alongside the original 2019 copyright.
- Added a `[project.optional-dependencies]` `dev` extra (mypy,
  mypy-extensions, pytest, pytest-asyncio, pytest-cov, ruff, tox) and trimmed
  `requirements_dev.txt` to match (dropped flake8, types-pytz, and stale
  pins; added ruff).
- Consolidated mypy and pytest configuration into `pyproject.toml` and bumped
  the black/ruff target-version to `py313`.
- `MANIFEST.in` and packaging metadata updated for the `README.rst` ->
  `README.md` switch.

### CI & tooling

- Removed Travis CI configuration (`.travis.yml`).
- Switched linting from black + flake8 to ruff (`ruff check` + `ruff
  format`) with bugbear/mccabe/pycodestyle/pyflakes/import-sorting rules and a
  mccabe complexity limit; added a bandit config and a
  `.pre-commit-config.yaml` running bandit and ruff hooks (carried from
  upstream).
- Modernized the GitHub Actions tox workflow: bumped `actions/checkout` (v3
  -> v7) and `actions/setup-python` (v3 -> v6.2.0), renamed the lint job, and
  trimmed the test matrix to Python 3.13 and 3.14.
- Modernized `tox.ini` (envlist `py313, py314, lint, mypy`), removed the
  Travis mapping section, added `skip_install` to the lint/mypy envs, dropped
  `types-pytz` from the mypy env, and pointed lint/mypy commands at the
  `yacron2` package.
- Bumped pre-commit hook revisions (ruff-pre-commit and bandit).

### Docs & examples

- Converted the README from reStructuredText to Markdown (`README.rst` ->
  `README.md`) and rebranded it to yacron2, with a new intro noting it is a
  fork of gjcarneiro/yacron continuing from 0.19. The content is otherwise the
  same as upstream 0.19, not a rewrite; install docs now require Python >=
  3.13, the prebuilt binary targets glibc 2.39 / Ubuntu 24.04, and releases
  come from github.com/ptweezy/yacron2.
- `HISTORY.rst` gained a fork-attribution preamble; older entries are
  retained as upstream yacron history.
- Modernized the Docker example: base image `python:3.14-slim` with `pip
  install yacron2` (replacing ubuntu:xenial + virtualenv), config copied into
  `/etc/yacron2.d`, and `ENTRYPOINT ['yacron2']`.
- Updated the Kubernetes example to the `apps/v1` Deployment API with the
  now-required selector, rebranded `yacrondemo` -> `yacron2demo`.
- Rebranded the ad-hoc example config directory, example tab file, PyInstaller
  spec/launcher, and listen socket paths (`/tmp/yacron.sock` ->
  `/tmp/yacron2.sock`) to yacron2.

### Credits (trailing upstream changes)

- `web.headers` option to control HTTP response headers on all web
  endpoints — Gustavo Carneiro (gjcarneiro), commit bde0f0b; merged upstream
  but never released in yacron 0.19.0.
- Python 3.14 compatibility, including `asyncio.new_event_loop()` and
  modern-Python lint/format fixes — Gustavo J. A. M. Carneiro (gjcarneiro),
  commit 27a32bc (#100).
- Switch from black/flake8 to ruff, plus bandit and pre-commit configuration —
  Gustavo Carneiro (gjcarneiro), commits c656fa6 and 4f7936a.
- Removal of Travis CI and modernization of the Python/PyInstaller version
  matrices — upstream yacron (gjcarneiro), commits d9b1ca6, 8d28816, 4e6892a,
  2941dcf.
- README logging example fix adding `datefmt: '%Y-%m-%d %H:%M:%S'` to the
  custom-logging formatter — andreas-wittig, commit 931b186.

## 0.19.0 (2023-03-11)

- Add ability to configure yacron's own logging (#81 #82 #83, gjcarneiro, bdamian)
- Add config value for SMTP(validate_certs=False) (David Batley)

## 0.18.0 (2023-01-01)

- fixes "Job is always executed immediately on yacron start" (#67)
- add an `enabled` option in jobs (#73)
- give a better error message when no configuration file is provided or exists (#72)

## 0.17.0 (2022-06-26)

- Support Additional Shell Report Vars (RJ Garcia)
- Shell reporter: handle long lines truncatation (Hannes Hergeth)
- exe: undo pyinstaller LD_LIBRARY_PATH changes in subprocesses (#68, Gustavo Carneiro)

## 0.16.0 (2021-12-05)

- make the capture max line length configurable and change the default
  from 64K to 16M (#56)
- Add config option to change prefix of subprocess stream lines (#58, eelkeh)

## 0.15.1 (2021-11-19)

- Fix a bug in the --validate option (#57, Leonid Repin)

## 0.15.0 (2021-11-10)

- Allow emails to be html formatted
- Fix an error when reading cmd output with huge lines (#56)

## 0.14.0 (2021-10-04)

- Sentry: increase the size of messages before getting truncated #54
- Sentry: allow specifying the environment option #53
- Minor fixes

## 0.13.1 (2021-08-10)

- unicode fixes for the exe binary version

## 0.13.0 (2021-06-28)

- Add ability for one config file to include another one #38
- Add shell command reporting ability (Hannes Hergeth, #50)

## 0.12.2 (2021-05-31)

- constrain ruamel.yaml to version 0.17.4 or below, later versions are buggy

## 0.12.1 (2021-05-30)

- blacklist ruamel.yaml version 0.17.5 in requirements #47

## 0.12.0 (2021-04-22)

- web: don't crash when receiving a web request without Accept header (#45)
- add env_file configuration option (Alessandro Romani, #43)
- email: add missing Date header (#39)

## 0.11.2 (2020-11-29)

- Add back a self contained binary, this time based on PyInstaller

## 0.11.1 (2020-07-29)

- Fix email reporting when multiple recipients given

## 0.11.0 (2020-07-20)

- reporting: add a failure reason line at the top of sentry/email (#36)
- mail: new tls, startls, username, and password options (#21)
- allow jobs to run as a different user (#18)
- Support timezone schedule (#26)

## 0.10.1 (2020-06-02)

- Minor bugfixes

## 0.10.0 (2019-11-03)

- HTTP remote interface, allowing to get job status and start jobs on demand
- Simple Linux binary including all dependencies (built using PyOxidizer)

## 0.10.0b2 (2019-10-26)

- Build Linux binary inside Docker Ubuntu 16.04, so that it is compatible with
  older glibc systems

## 0.10.0b1 (2019-10-13)

- Build a standalone Linux binary, using PyOxidizer
- Switch from raven to sentry-sdk

## 0.9.0 (2019-04-03)

- Added an option to just check if the yaml file is valid without running the scheduler.
- Fix missing `body` in the schema for sentry config

## 0.8.1 (2018-10-16)

- Fix a bug handling `@reboot` in schedule (#22)

## 0.8.0 (2018-05-14)

- Sentry: add new `extra` and `level` options.

## 0.7.0 (2018-03-21)

- Added the `utc` option and document that times are utc by default (#17);
- If an email body is empty, skip sending it;
- Added docker and k8s example.

## 0.6.0 (2017-11-24)

- Add custom Sentry fingerprint support
- Ability to send job metrics to statsd (thanks bofm)
- `always` flag to consider any cron job that exits to be failed
  (thanks evanjardineskinner)
- `maximumRetries` can now be `-1` to never stop retrying (evanjardineskinner)
- `schedule` can be the string `@reboot` to always run that cron job on startup
  (evanjardineskinner)
- `saveLimit` can be set to zero (evanjardineskinner)

## 0.5.0

- Templating support for reports
- Remove deprecated smtp_host/smtp_port

## 0.4.3 (2017-09-13)

- Bug fixes

## 0.4.2 (2017-09-07)

- Bug fixes

## 0.4.1 (2017-08-03)

- More polished handling of configuration errors;
- Unit tests;
- Bug fixes.

## 0.4.0 (2017-07-24)

- New option `executionTimeout`, to terminate jobs that get stuck;
- If a job doesn't terminate gracefully kill it.  New option `killTimeout`
  controls how much time to wait for graceful termination before killing it;
- Switch parsing to strictyaml, for more user friendly parsing validation error
  messages.
