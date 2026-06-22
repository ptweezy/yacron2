# Contributing and Releasing

This page covers the yacron2 developer workflow (environment, tests, linters, type checks, pre-commit) and the fully automated GitHub Actions release pipeline that builds, publishes, tags, and containerizes each version. Version numbers are derived from git tags via `setuptools_scm` and are never hand-edited.

## Development environment

yacron2 targets **Python 3.13+**; 3.13 and 3.14 are the tested interpreters (`pyproject.toml` `requires-python = ">=3.13"`, classifiers for 3.13 and 3.14).

yacron2 is **POSIX-only**: it imports `grp` and `pwd` at load time, so the package and its test suite must run on Linux or macOS (or WSL on Windows). Linting and type checking do not import the package and run on any platform.

Clone and install the editable package with the `dev` extra:

```sh
git clone https://github.com/ptweezy/yacron2
cd yacron2
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"                         # or: pip install -r requirements_dev.txt
```

The `dev` optional-dependency group (`pyproject.toml`) and the equivalent `requirements_dev.txt` both pull in: `mypy`, `mypy-extensions`, `pytest`, `pytest-asyncio`, `pytest-cov`, `ruff`, and `tox`. The console entry point `yacron2 = yacron2.__main__:main` is installed by the editable install (see [Command-Line Reference](CLI-Reference)).

## Running the checks

All CI checks are driven by `tox` (`tox.ini`). The default `envlist` is `py313, py314, lint, mypy`.

```sh
tox            # all envs: py313, py314, lint, mypy
tox -e lint    # ruff check + ruff format --check
tox -e mypy    # mypy
tox -e py      # pytest on the current interpreter
```

| Env | Installs package | What it runs |
| --- | --- | --- |
| `py313`, `py314` | yes (`-rrequirements_dev.txt`, `PYTHONPATH={toxinidir}`) | `pytest --color=yes -vv` |
| `lint` | no (`skip_install = true`) | `ruff check yacron2` then `ruff format --check yacron2` |
| `mypy` | no (`skip_install = true`, `basepython=python3`) | `mypy -p yacron2 --ignore-missing-imports` |

The `lint` and `mypy` envs deliberately skip installing the package — ruff and mypy analyze the source tree directly, so they avoid imposing the project's `requires-python` on the lint/type-check interpreter.

### Tool configuration

`pyproject.toml` configures the tooling:

- **ruff**: `target-version = "py313"`, `line-length = 79`. Lint rule sets selected: `B`, `B9` (bugbear), `C` (mccabe complexity), `E` (pycodestyle errors), `F` (pyflakes), `W` (pycodestyle warnings), `I` (import sorting). `pyupgrade` (`UP`) is present but commented out. `max-complexity = 20`.
- **mypy**: `no_implicit_optional = true`, `warn_no_return = true`, `warn_return_any = true`, `strict_optional = true`.
- **pytest**: `asyncio_mode = "auto"`, `testpaths = ["tests"]`.
- **bandit**: `exclude_dirs = ["tests"]`.

### pre-commit

`pre-commit` runs ruff and bandit on staged changes (`.pre-commit-config.yaml`):

```sh
pip install pre-commit
pre-commit install
```

Configured hooks:

| Repo | Rev | Hook(s) | Args |
| --- | --- | --- | --- |
| `PyCQA/bandit` | `1.9.4` | `bandit` | `-c pyproject.toml --severity-level=medium`, with `bandit[toml]` |
| `astral-sh/ruff-pre-commit` | `v0.15.18` | `ruff` (lint), `ruff-format` | `ruff` runs with `--fix` |

Note `pre-commit`'s ruff runs with `--fix` (auto-applies fixes), whereas `tox -e lint` runs `ruff check` (no fix) plus `ruff format --check` (verify only). pre-commit is not pinned in the `dev` extra; install it separately as shown.

### CI for every commit

`.github/workflows/tox.yml` runs on every `push` and `pull_request` (read-only permissions). It has three jobs on `ubuntu-latest`: `tox-lint` (`tox -e lint`), `tox-mypy` (`tox -e mypy`), and a `tox` matrix over Python `3.13` and `3.14` running `tox -e py` (`fail-fast: false`). The matrix `os` is currently `ubuntu-latest` only; macOS and Windows are commented out in the matrix.

A second per-commit gate is `.github/workflows/docker.yml`, which builds the container image on **every commit on every branch** (`push: branches: ["**"]`) as `linux/amd64` build-only (no push, tagged `ci-build`), so a broken `Dockerfile` fails CI before a release. See [Production and Container Deployment](Production-Deployment).

## Releasing

Releases are fully automated by `.github/workflows/release.yml`. You never edit a version by hand; `setuptools_scm` derives the version from git tags (`version_file = "yacron2/version.py"`).

### Triggering a release

A release runs when **either**:

1. A **push to `main`** in which **any** commit introduced by the push has a release marker anywhere in its message — not just the tip commit. The scanned range is `BEFORE..AFTER` (the commits new in the push); on a brand-new branch where `BEFORE` is all-zeros (or unresolvable) it falls back to the tip commit only.
2. A **manual `workflow_dispatch`** run, choosing the bump level from a dropdown (`minor` default, or `major` / `patch`).

Valid markers (case-insensitive; the bump level is optional):

| Marker | Bump | 1.0.5 → |
| --- | --- | --- |
| `[release]` | minor | 1.1.0 |
| `[release:major]` | major | 2.0.0 |
| `[release:minor]` | minor | 1.1.0 |
| `[release:patch]` | patch | 1.0.6 |

If several markers appear across the pushed commits, the **most significant bump wins** (major > minor > patch); a bare `[release]` counts as minor.

The marker match is performed in the `decide` job with `grep -oiE '\[release(:(major|minor|patch))?\]'` over the commit message bodies.

> **Footgun — literal-marker substring match.** The match is a plain substring against commit message text (not anchored to its own line, not requiring any surrounding structure). Writing a literal `[release:patch]` (or `[release]`) **anywhere** in a commit message — even in prose describing the release process — **will trigger a publish**. Do not quote a marker verbatim in a commit message unless you mean it. Only commit messages are scanned; **file contents are never scanned** (this page can name the markers freely).

### What the pipeline does

The `release.yml` jobs run in dependency order. Top-level `permissions` default to `contents: read`; only the `release` job (`contents: write` + `id-token: write`) and the `docker` job (`packages: write`) opt up to the write scopes they need.

1. **`decide`** — Determines `release` (true/false) and `bump`. Trigger logic lives in a real shell script rather than a fuzzy `contains()` expression. All downstream jobs are gated on `needs.decide.outputs.release == 'true'`.
2. **`version`** — Computes the next version once, so every builder and the publish job build at the same number. Finds the latest tag matching `^[0-9]+\.[0-9]+\.[0-9]+$` (via `git tag -l | … | sort -V | tail -n1`, defaulting to `0.0.0`), applies the bump, and **refuses with an error if the computed tag already exists** (`refs/tags/$new`).
3. **`gate`** — Checks out full history and runs `tox` on Python 3.13 + 3.14 (`tox` with no `-e` runs `py313, py314, lint, mypy`). A red build means no release.
4. **Native binary builds** (run before publishing, so a broken build fails the run instead of producing a half-finished release). Each job pins `pyinstaller==6.21.0`, runs `pip install .` to bake `SETUPTOOLS_SCM_PRETEND_VERSION` (the computed version) into `yacron2/version.py`, runs `pyinstaller pyinstaller/yacron2.spec`, and smoke-tests the bundle with `dist/yacron2 --version`:
   - **`binaries`** — Linux **glibc**, `amd64` on `ubuntu-24.04` and `arm64` on `ubuntu-24.04-arm` (native runners, no QEMU). Built on Python 3.14. Artifacts `yacron2-linux-amd64`, `yacron2-linux-arm64`.
   - **`binaries-musl`** — Linux **musl/Alpine**, `amd64` and `arm64`, built **inside a `python:3.14-alpine` container via `docker run`** (so checkout/upload stay on the glibc host; arm64 uses the native arm64 runner). Installs `build-base libffi-dev zlib-dev` (the spec sets `strip=True`, and headers cover any dep that compiles from sdist). Artifacts `yacron2-linux-amd64-musl`, `yacron2-linux-arm64-musl`.
   - **`binaries-macos`** — macOS, `arm64` on `macos-15` (Apple Silicon) and `amd64` on `macos-15-intel`. Built on Python 3.14. After the smoke test it asserts the native arch with `file` (so Rosetta cannot let a mislabelled x86_64 build pass on the arm64 runner). Artifacts `yacron2-macos-arm64`, `yacron2-macos-amd64`.
5. **`release`** — Runs only after all builders succeed, with `permissions: contents: write` and `id-token: write`. In order:
   - Builds the wheel + sdist with `python -m build` (at `SETUPTOOLS_SCM_PRETEND_VERSION`) and validates with `twine check`.
   - **Publishes the wheel + sdist to PyPI** via Trusted Publishing / OIDC (`pypa/gh-action-pypi-publish@release/v1`) — no API token.
   - **Only after a successful publish**: creates an annotated tag `X.Y.Z` and pushes it; downloads every per-arch binary artifact (pattern `yacron2-*`, `merge-multiple: true`); extracts the release notes; and creates the GitHub Release.
6. **`docker`** — After `release`, calls `docker.yml` via `workflow_call` with the new version to build and push the multi-arch image (see below).

Because no file is committed back to the repo, a release never re-triggers the workflow. Because the tag is created **after** publishing, a failed publish leaves no orphan tag and a re-run cleanly retries the same version.

### macOS signing and notarization

The macOS binaries are Developer ID signed (hardened runtime) and notarized **when the signing secrets are configured**; if absent, the "Sign and notarize" step warns and exits 0, shipping an unsigned binary (a release is never blocked on signing setup). The secrets are `MACOS_CERT_P12_BASE64`, `MACOS_CERT_PASSWORD`, `MACOS_SIGN_IDENTITY`, `MACOS_NOTARY_KEY_BASE64`, `MACOS_NOTARY_KEY_ID`, `MACOS_NOTARY_ISSUER_ID`.

Signing imports the cert into a throwaway randomly-keyed keychain, signs with `codesign --options runtime --timestamp --entitlements pyinstaller/entitlements.plist`, verifies, then notarizes via `xcrun notarytool submit … --wait`. Because a one-file binary cannot be stapled, notarization publishes the ticket online and Gatekeeper validates on first run — end users do not need `xattr -d com.apple.quarantine`.

`pyinstaller/entitlements.plist` enables the three hardened-runtime entitlements a PyInstaller one-file binary needs (`com.apple.security.cs.allow-unsigned-executable-memory`, `…allow-jit`, `…disable-library-validation`) so the unpacked CPython runtime can load and execute its embedded `.so`/`.dylib` files.

### Release notes

The "Build release notes from HISTORY.md" step extracts this version's section from `HISTORY.md` — everything between its `## X.Y.Z (…)` header and the next `## ` header, with leading blank lines stripped — into `release-notes.md`. If there is no matching section it warns and the body is auto-generated only. The Release uses that section as `body_path` with `generate_release_notes: true` (the curated notes are prepended above GitHub's auto-generated "What's Changed" / compare link). Keep [HISTORY.md](https://github.com/ptweezy/yacron2/blob/main/HISTORY.md) entries headed exactly `## X.Y.Z (date)` so the matcher (`index($0, "## " ver " ") == 1`) finds them.

### Release assets

The GitHub Release (`softprops/action-gh-release@v3`) attaches:

- `dist/*.whl`, `dist/*.tar.gz`
- `yacron2-linux-amd64`, `yacron2-linux-arm64`
- `yacron2-linux-amd64-musl`, `yacron2-linux-arm64-musl`
- `yacron2-macos-amd64`, `yacron2-macos-arm64`

The download-artifact pattern `yacron2-*` must stay broad enough to match all of them — a too-narrow pattern silently drops artifacts it misses rather than erroring.

## Container image release

The official image is built and pushed by `.github/workflows/docker.yml`, built from the top-level `Dockerfile`. It runs in three modes:

- **Per-commit gate** (`push` to any branch): builds `linux/amd64` only, does **not** push (tagged `ci-build`). Catches a broken `Dockerfile` before a release.
- **On release** (invoked by `release.yml` via `workflow_call` with the version): builds **multi-arch** `linux/amd64,linux/arm64` and pushes to GHCR as `ghcr.io/ptweezy/yacron2:<version>` and `:latest` (and Docker Hub if `DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN` secrets are set). `workflow_call` is used rather than `on: release` because a Release created by the default `GITHUB_TOKEN` does not emit a triggering `release: published` event.
- **Manual** (`workflow_dispatch`): (re)builds and pushes any existing release tag (defaults to the latest release), e.g. to backfill an image or retry a failed push.

The image build passes the computed version with `--build-arg VERSION=X.Y.Z`; a plain local `docker build .` leaves it empty and `setuptools_scm` reads the version from `.git`. See [Production and Container Deployment](Production-Deployment).

## The PyInstaller build

The self-contained binaries are produced from `pyinstaller/yacron2.spec`. The spec analyzes the entry script `pyinstaller/yacron2` (which simply calls `yacron2.__main__:main`) and emits a single-file console executable named `yacron2` with `strip=True`, `upx=False`, `debug=False`, `console=True`. PyInstaller is **pinned to `6.21.0`** consistently across the release jobs and the local Dockerfile.

The version is baked in by installing the package under `SETUPTOOLS_SCM_PRETEND_VERSION` before running PyInstaller, so the bundled `yacron2/version.py` carries the release version (verified by the `--version` smoke test). PyInstaller is not a cross-compiler, so each architecture/libc is built on a matching native runner or container.

### Building a binary locally

`pyinstaller/Dockerfile` builds a glibc binary reproducibly on `ubuntu:24.04`: it installs build deps and `upx-ucl`, uses `pyenv` to install CPython `3.13.14` with `--enable-shared`, creates a venv, `pip install pyinstaller==6.21.0`, installs the package, runs the entry script (`python pyinstaller/yacron2 --version`), runs `pyinstaller pyinstaller/yacron2.spec`, and smoke-tests `dist/yacron2 --version`.

`pyinstaller/Makefile` wraps that: `make` (target `all`) builds the image, copies `dist/yacron2` out of the container, and runs `dist/yacron2 --version`.

> The standalone binaries unpack their embedded runtime to a temp directory at startup; the temp directory must be writable and executable. See [Installation](Installation) and [Troubleshooting and FAQ](Troubleshooting).

## Related pages

- [Installation](Installation)
- [Command-Line Reference](CLI-Reference)
- [Production and Container Deployment](Production-Deployment)
- [Architecture and Internals](Architecture-and-Internals)
