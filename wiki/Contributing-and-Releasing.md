# Contributing and Releasing

This page covers the cronstable developer workflow (environment, tests, linters, type checks, pre-commit) and the fully automated GitHub Actions release pipeline that builds, publishes, tags, and containerizes each version. Version numbers are derived from git tags via `setuptools_scm` and are never hand-edited.

## Development environment

cronstable targets **Python 3.10+**; 3.10, 3.11, 3.12, 3.13 and 3.14 are the tested interpreters (`pyproject.toml` `requires-python = ">=3.10"`, classifiers for 3.10 through 3.14).

cronstable runs **natively on Windows, Linux, and macOS** (WSL is not required). All OS-specific behavior is isolated in `cronstable/platform.py` (`grp`/`pwd` are guarded there, not imported unconditionally at load time on Windows), so the package and its full test suite run natively on every supported OS, and `pip install cronstable` works on Windows. See [Running on Windows](Running-on-Windows) for the platform-specific details.

Linting and type checking do not import the package and run on any platform. mypy is pinned to the `linux` platform (`pyproject.toml` `[tool.mypy]` `platform = "linux"`), so type-checking is identical on every OS: it type-checks the POSIX API surface, and the Windows branches are runtime-guarded.

Clone and install the editable package with the `dev` extra. cronstable uses
[uv](https://docs.astral.sh/uv/) for a fast dev loop (`tox` also runs through uv
via `tox-uv`, and uv can fetch the 3.10–3.14 interpreters the matrix needs):

```sh
git clone https://github.com/ptweezy/cronstable
cd cronstable
uv venv                                         # create .venv (uv picks a suitable Python)
uv pip install -e ".[dev]"                      # editable install with the dev extra
```

The classic virtualenv+pip path still works unchanged:

```sh
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"                         # or: pip install -r requirements_dev.txt
```

The editable dev install (`pip install -e ".[dev]"`) and the checks (`pytest`, `ruff`, `mypy`) all run natively on Windows too. Use `.venv\Scripts\activate` to enter the venv as shown above.

The `dev` optional-dependency group (`pyproject.toml`) and the equivalent `requirements_dev.txt` both pull in: `mypy`, `mypy-extensions`, `pytest`, `pytest-asyncio`, `pytest-cov`, `ruff`, `tox`, and `tox-uv` (the plugin that makes `tox` provision and install with uv). The console entry point `cronstable = cronstable.__main__:main` is installed by the editable install (see [Command-Line Reference](CLI-Reference)).

## Running the checks

All CI checks are driven by `tox` (`tox.ini`). The default `envlist` is `py310, py311, py312, py313, py314, lint, mypy`.

```sh
tox            # all envs: py310-py314, lint, mypy
tox -e lint    # ruff check + ruff format --check
tox -e mypy    # mypy
tox -e py      # pytest on the current interpreter
```

| Env | Installs package | What it runs |
| --- | --- | --- |
| `py313`, `py314` | yes (`-rrequirements_dev.txt`, `PYTHONPATH={toxinidir}`) | `pytest --color=yes -vv` |
| `lint` | no (`skip_install = true`) | `ruff check cronstable` then `ruff format --check cronstable` |
| `mypy` | no (`skip_install = true`, `basepython=python3`) | `mypy -p cronstable --ignore-missing-imports` |

`tox.ini` declares `requires = tox-uv`, so `tox` provisions its environments and installs dependencies with uv automatically (much faster; behavior-identical). Force the legacy virtualenv+pip path with `tox --runner virtualenv` if ever needed.

The `lint` and `mypy` envs deliberately skip installing the package: ruff and mypy analyze the source tree directly, so they avoid imposing the project's `requires-python` on the lint/type-check interpreter.

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

There is **one** workflow, `.github/workflows/release.yml` (named `CI`), and it runs on every `push` (any branch) and every `pull_request`. On an ordinary commit it builds and tests the whole product in parallel and stops there; only a release (see below) proceeds to publish. The test half is unchanged from before: `tox-lint` (`tox -e lint`) and `tox-mypy` (`tox -e mypy`) on `ubuntu-latest`, plus a `tox` matrix running `tox -e py` (`fail-fast: false`) across `os` `[ubuntu-latest, windows-latest]` × Python `3.10`–`3.14`, with an experimental `ubuntu-latest`/`3.15` row (`continue-on-error`, never gates) and a `windows-11-arm`/`3.14` row for **Windows ARM64**.

Alongside the tests, the same run builds every release artifact at the computed version (all the PyInstaller binaries, the wheel + sdist) and does a **build-only pass over every Docker image** (the `docker` job — all 8 distros at their full published arch sets, no push), so a broken `Dockerfile` fails CI before a release. On an ordinary commit the version is the natural `setuptools_scm` dev version; nothing is published, pushed, tagged, or signed. See [Production and Container Deployment](Production-Deployment).

## Releasing

Releases are fully automated by `.github/workflows/release.yml`. You never edit a version by hand; `setuptools_scm` derives the version from git tags (`version_file = "cronstable/version.py"`).

### Triggering a release

A release runs when **either**:

1. A **push to `main`** in which **any** commit introduced by the push has a release marker at the **start of its subject line**, not just the tip commit. The scanned range is `BEFORE..AFTER` (the commits new in the push); on a brand-new branch where `BEFORE` is all-zeros (or unresolvable) it falls back to the tip commit only.
2. A **manual `workflow_dispatch`** run, choosing the bump level from a dropdown (`minor` default, or `major` / `patch`).

Valid markers (case-insensitive; the bump level is optional):

| Marker | Bump | 1.0.5 → |
| --- | --- | --- |
| `[release]` | minor | 1.1.0 |
| `[release:major]` | major | 2.0.0 |
| `[release:minor]` | minor | 1.1.0 |
| `[release:patch]` | patch | 1.0.6 |

If several pushed commits carry a marker, the **latest such commit wins**; a bare `[release]` counts as minor.

The marker match is performed in the `decide` job with `grep -oiE '^\[release(:(major|minor|patch))?\]'` over the commit **subject lines** (`git log --pretty=%s`), taking the newest matching commit.

> **Why subjects only, anchored:** the original trigger substring-matched whole commit messages, so a commit *body* that merely discussed the bare `[release]` marker out-bumped an explicit `[release:patch]` and shipped 1.3.0 instead of 1.2.15. Bodies are no longer scanned at all, and a marker only counts when it begins the subject line. File contents are never scanned (this page can name the markers freely).

### What the pipeline does

The `release.yml` jobs run in dependency order. Top-level `permissions` default to `contents: read`; only the `release` job (`contents: write` + `id-token: write`) and the `docker-push` job (`packages: write`) opt up to the write scopes they need. `decide` / `version` and the whole build+test gate run on **every** event; only the `release`, `docker-push` and `homebrew` publish jobs are guarded by `needs.decide.outputs.release == 'true'`.

1. **`decide`**: determines `release` (true/false) and `bump`. Trigger logic lives in a real shell script rather than a fuzzy `contains()` expression, and it releases **only** on a `workflow_dispatch` or a push to `main` carrying a marker (a marker on any other branch, or in a PR, never releases).
2. **`version`**: computes the version once, so every builder (and the publish job) use the same number. On a release it finds the latest tag matching `^[0-9]+\.[0-9]+\.[0-9]+$` (via `git tag -l | … | sort -V | tail -n1`, defaulting to `0.0.0`), applies the bump, and **refuses with an error if the computed tag already exists** (`refs/tags/$new`); otherwise it emits the natural `setuptools_scm` dev version for the build-only run.
3. **Tests** (`tox-lint`, `tox-mypy`, and the `tox` matrix) run **in parallel with** the binary and Docker builds, not before them — the whole matrix together is the gate. A red anywhere means no release.
4. **Binary builds** (run in parallel with the tests; the publish jobs need them, so a broken build fails the run instead of producing a half-finished release). Each job pins `pyinstaller==6.21.0`, installs the project to bake `SETUPTOOLS_SCM_PRETEND_VERSION` (the computed version) into `cronstable/version.py`, runs `pyinstaller pyinstaller/cronstable.spec`, and smoke-tests the bundle with `dist/cronstable --version`. The **runner-native** jobs (`binaries`, `binaries-macos`, `binaries-windows`) install with **uv** (`uv venv` + `uv pip install` + `uv run pyinstaller`, via `astral-sh/setup-uv`); the **emulated foreign-arch** jobs (`binaries-glibc-extra`, `binaries-musl`) stay on **pip** inside their `docker run` containers, since uv's official image is amd64/arm64 only and it publishes no musl `ppc64le`/`s390x` wheels — pip is the arch-portable choice there:
   - **`binaries`**: Linux **glibc, 64-bit**, `amd64` on `ubuntu-24.04` and `arm64` on `ubuntu-24.04-arm` (native runners, no QEMU). Built on Python 3.14. Artifacts `cronstable-linux-amd64`, `cronstable-linux-arm64`.
   - **`binaries-glibc-extra`**: Linux **glibc** for the arches with no native GitHub runner (`i686`, `armv7`, `ppc64le`, `s390x`, `riscv64`), built **inside a `python:3.14-slim` (Debian) container via `docker run --platform`** (the native `binaries` job covers only `amd64`/`arm64` and PyInstaller is not a cross-compiler, so these need a foreign-arch container). `i686` (`linux/386`) runs natively on the `ubuntu-24.04` runner; `armv7` (`linux/arm/v7`), `ppc64le` (`linux/ppc64le`), `s390x` (`linux/s390x`) and `riscv64` (`linux/riscv64`) run under QEMU (`docker/setup-qemu-action`). Installs `build-essential libffi-dev zlib1g-dev` (the spec sets `strip=True`; `ppc64le`/`s390x` have full manylinux wheels, while the i686 aiohttp stack, propcache-on-`armv7`, and multidict/frozenlist/ruamel.yaml.clib on `riscv64` compile from sdist). Artifacts `cronstable-linux-{i686,armv7,ppc64le,s390x,riscv64}` (no `-musl` suffix; they sit beside the 64-bit glibc binaries).
   - **`binaries-musl`**: Linux **musl/Alpine**, `amd64`, `arm64`, `i686`, `armv7`, `ppc64le`, `s390x`, `riscv64` and `armv6`, built **inside a `python:3.14-alpine` container via `docker run`** (so checkout/upload stay on the glibc host). `amd64`/`arm64` use their native runners; `i686` (`linux/386`) runs natively on the `ubuntu-24.04` runner and `armv7`/`ppc64le`/`s390x`/`riscv64`/`armv6` under QEMU. `armv6` is **musl-only** (the Debian/glibc image ships no arm32v6, so it has no `binaries-glibc-extra` counterpart). Installs `build-base libffi-dev zlib-dev` (the spec sets `strip=True`, and headers cover any dep that compiles from sdist, notably the i686 aiohttp stack, which ships no `musllinux_i686` wheels, and the whole C-ext stack on `armv6`). Artifacts `cronstable-linux-{amd64,arm64,i686,armv7,ppc64le,s390x,riscv64,armv6}-musl`.
   - **`binaries-macos`**: macOS, `arm64` on `macos-15` (Apple Silicon) and `amd64` on `macos-15-intel`. Built on Python 3.14. After the smoke test it asserts the native arch with `file` (so Rosetta cannot let a mislabelled x86_64 build pass on the arm64 runner). Artifacts `cronstable-macos-arm64`, `cronstable-macos-amd64` — these are the **shipped** pair (signed + notarized on a release, see below). The same job also builds macOS 26 (Tahoe) `arm64`/`amd64` rows as **CI-only** coverage (`ship: false`, `continue-on-error` so a flaky Tahoe build never blocks a release); those upload as `cronstable-macos26-{arch}` and are neither signed nor attached to the Release.
   - **`binaries-windows`**: Windows, `amd64` on `windows-latest` and `arm64` on the `windows-11-arm` runner (both native; PyInstaller is not a cross-compiler). Built on Python 3.14 with the same `pyinstaller==6.21.0` pin and `dist/cronstable.exe --version` smoke test as the other jobs; any C-extension dep lacking a `win_arm64` wheel compiles from sdist via the runner's Visual Studio ARM64 toolchain. There is **no Windows code-signing step** (the binaries ship unsigned, like the Linux binaries). Artifacts `cronstable-windows-amd64.exe`, `cronstable-windows-arm64.exe`. See [Running on Windows](Running-on-Windows).
5. **`release`**: runs only after the **entire** gate succeeds — every test job, every binary job (`binaries`, `binaries-musl`, `binaries-glibc-extra`, `binaries-macos`, `binaries-windows`) **and** the `docker` build-only job — with `permissions: contents: write` and `id-token: write`. In order:
   - Downloads the `dist` artifact (the wheel + sdist the `dist` job already built and `twine check`ed) and **publishes it to PyPI** via Trusted Publishing / OIDC (`pypa/gh-action-pypi-publish`, `skip-existing: true`), no API token.
   - **Only after a successful publish**: creates an annotated tag `X.Y.Z` and pushes it (with `RELEASE_TOKEN` so the tag can point at a commit that touches `.github/workflows/`); downloads every per-arch binary artifact (pattern `cronstable-*`, `merge-multiple: true`); generates one `SHA256SUMS` over the shipped set; extracts the release notes; and creates the GitHub Release with **all** binaries + `SHA256SUMS` attached in a single step (there is no separate later attach step, so nothing ever collides with immutable-release protection).
6. **`docker-push`**: after `release` (so the tag it checks out exists, and it is thus gated on the whole gate including the `docker` build), builds and pushes every distro's multi-arch image at the released version (see below).
7. **`homebrew`**: after `release`, re-renders and pushes the tap formula from the published `SHA256SUMS`.

Because no file is committed back to the repo, a release never re-triggers the workflow. Because the tag is created **after** publishing, a failed publish leaves no orphan tag and a re-run cleanly retries the same version.

### macOS signing and notarization

The macOS binaries are Developer ID signed (hardened runtime) and notarized **when the signing secrets are configured**; if absent, the "Sign and notarize" step warns and exits 0, shipping an unsigned binary (a release is never blocked on signing setup). The secrets are `MACOS_CERT_P12_BASE64`, `MACOS_CERT_PASSWORD`, `MACOS_SIGN_IDENTITY`, `MACOS_NOTARY_KEY_BASE64`, `MACOS_NOTARY_KEY_ID`, `MACOS_NOTARY_ISSUER_ID`.

Signing imports the cert into a throwaway randomly-keyed keychain, signs with `codesign --options runtime --timestamp --entitlements pyinstaller/entitlements.plist`, verifies, then notarizes via `xcrun notarytool submit … --wait`. Because a one-file binary cannot be stapled, notarization publishes the ticket online and Gatekeeper validates on first run, so end users do not need `xattr -d com.apple.quarantine`.

`pyinstaller/entitlements.plist` enables the three hardened-runtime entitlements a PyInstaller one-file binary needs (`com.apple.security.cs.allow-unsigned-executable-memory`, `…allow-jit`, `…disable-library-validation`) so the unpacked CPython runtime can load and execute its embedded `.so`/`.dylib` files.

### Release notes

The "Build release notes from HISTORY.md" step extracts this version's section from `HISTORY.md` (everything between its `## X.Y.Z (…)` header and the next `## ` header, with leading blank lines stripped) into `release-notes.md`. If there is no matching section it warns and the body is auto-generated only. The Release uses that section as `body_path` with `generate_release_notes: true` (the curated notes are prepended above GitHub's auto-generated "What's Changed" / compare link). Keep [HISTORY.md](https://github.com/ptweezy/cronstable/blob/main/HISTORY.md) entries headed exactly `## X.Y.Z (date)` so the matcher (`index($0, "## " ver " ") == 1`) finds them.

### Release assets

The GitHub Release (`softprops/action-gh-release@v3`) attaches:

- `dist/*.whl`, `dist/*.tar.gz`
- `cronstable-linux-{amd64,arm64,i686,armv7,ppc64le,s390x,riscv64}` (glibc)
- the same seven arches with a `-musl` suffix, e.g. `cronstable-linux-amd64-musl` … `cronstable-linux-riscv64-musl`, **plus** `cronstable-linux-armv6-musl` (armv6 is musl-only)
- `cronstable-macos-amd64`, `cronstable-macos-arm64`
- `cronstable-windows-amd64.exe`, `cronstable-windows-arm64.exe`

The download-artifact pattern `cronstable-*` must stay broad enough to match all of them: a too-narrow pattern silently drops artifacts it misses rather than erroring.

## Container image release

The official images are built and pushed by the single `release.yml` pipeline (the former standalone `docker.yml` is folded into it), from the top-level `Dockerfile` and the per-distro `docker/Dockerfile.*`. Two jobs cover them:

- **`docker` (build-only gate)** runs on **every** push and PR: it builds all 8 distro images at their full published arch sets (non-`amd64` arches via QEMU) and does **not** push. This is part of the gate, so an arch-specific `Dockerfile` or dependency breakage fails CI *before* anything is published, and it warms the per-distro GHA build cache.
- **`docker-push`** runs **only on a release**, after `release` has published PyPI and pushed the tag (so it is transitively gated on the whole build+test gate, the `docker` build included). It checks out the tag and pushes each distro's multi-arch image to GHCR as `ghcr.io/ptweezy/cronstable:<version>` and `:latest` — the Debian base owns the bare tags, each variant gets a `-<distro>` suffix (`-alpine`, `-ubuntu`, `-rhel`, `-fedora`, `-opensuse`, `-amazonlinux`, `-distroless`) — and to Docker Hub when `DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN` are set.

The image build passes the computed version with `--build-arg VERSION=X.Y.Z`; a plain local `docker build .` leaves it empty and `setuptools_scm` reads the version from `.git`. See [Production and Container Deployment](Production-Deployment).

## The PyInstaller build

The self-contained binaries are produced from `pyinstaller/cronstable.spec`. The spec analyzes the entry script `pyinstaller/cronstable` (which simply calls `cronstable.__main__:main`) and emits a single-file console executable named `cronstable` with `strip=True`, `upx=False`, `debug=False`, `console=True`. PyInstaller is **pinned to `6.21.0`** consistently across the release jobs and the local Dockerfile.

The version is baked in by installing the package under `SETUPTOOLS_SCM_PRETEND_VERSION` before running PyInstaller, so the bundled `cronstable/version.py` carries the release version (verified by the `--version` smoke test). PyInstaller is not a cross-compiler, so each architecture/libc is built on a matching native runner or container.

### Building a binary locally

`pyinstaller/Dockerfile` builds a glibc binary reproducibly on `ubuntu:24.04`: it installs build deps and `upx-ucl`, uses `pyenv` to install CPython `3.13.14` with `--enable-shared`, creates a venv, installs `pyinstaller==6.21.0` and the package with **uv** (copied in via `COPY --from=ghcr.io/astral-sh/uv` — an amd64-only local build, so the clean image-copy pattern is arch-safe here, unlike the multi-arch release `Dockerfile`), runs the entry script (`python pyinstaller/cronstable --version`), runs `pyinstaller pyinstaller/cronstable.spec`, and smoke-tests `dist/cronstable --version`.

`pyinstaller/Makefile` wraps that: `make` (target `all`) builds the image, copies `dist/cronstable` out of the container, and runs `dist/cronstable --version`.

> The standalone binaries unpack their embedded runtime to a temp directory at startup; the temp directory must be writable and executable. See [Installation](Installation) and [Troubleshooting and FAQ](Troubleshooting).

## Related pages

- [Installation](Installation)
- [Running on Windows](Running-on-Windows)
- [Command-Line Reference](CLI-Reference)
- [Production and Container Deployment](Production-Deployment)
- [Architecture and Internals](Architecture-and-Internals)
