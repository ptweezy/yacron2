# Contributing to cronstable

Thanks for working on cronstable! This document covers local development and,
importantly, how releases are cut.

## Development setup

cronstable targets **Python 3.10+** (3.10, 3.11, 3.12, 3.13 and 3.14 are tested)
and runs on **Linux, macOS and Windows** (the test suite runs on all three in
CI, including Windows ARM64).

cronstable uses [uv](https://docs.astral.sh/uv/) for a fast dev loop (`tox` also
runs through uv via `tox-uv`, and uv can fetch the 3.10–3.14 interpreters the
test matrix needs). With uv installed:

```sh
git clone https://github.com/ptweezy/cronstable
cd cronstable
uv venv                                         # create .venv (uv picks a suitable Python)
uv pip install -e ".[dev]"                      # editable install with the dev extra
```

Prefer stock tooling? The classic path still works unchanged:

```sh
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"                         # or: pip install -r requirements_dev.txt
```

> **Note:** all OS-specific behavior lives in
> [`cronstable/platform.py`](cronstable/platform.py) (default shell, default config
> location, unix-socket support, and shutdown-signal wiring). The POSIX-only
> `user`/`group` feature imports `grp`/`pwd` lazily and is rejected on Windows.
> mypy is pinned to the `linux` platform (it type-checks the POSIX API surface;
> the Windows branches are runtime-guarded), so type-checking is identical on
> every OS.

## Running the checks

Everything CI runs is driven by `tox`:

```sh
tox            # all envs: py310, py311, py312, py313, py314, lint, mypy
tox -e lint    # ruff check + ruff format --check
tox -e mypy    # mypy
tox -e py      # pytest on the current interpreter
```

`tox.ini` declares `requires = tox-uv`, so `tox` provisions its environments and
installs dependencies with uv automatically (much faster; behavior-identical).
Force the legacy virtualenv+pip path with `tox --runner virtualenv` if you ever
need to.

`pre-commit` runs ruff and bandit on staged changes:

```sh
uv tool install pre-commit   # or: pip install pre-commit
pre-commit install
```

## Releasing

Releases are **automated** by the single [`CI`](.github/workflows/release.yml)
GitHub Actions pipeline (one workflow builds and tests everything on every
commit and, on a release, publishes it). Version numbers come from git tags via
`setuptools_scm`; you never edit a version by hand.

### Cutting a release

A release happens when **any commit in a push to `main`** has a release marker
at the **start of its subject line** (the first line of the commit message):

```text
[release:minor] Add retry backoff to the HTTP reporter
```

It does not need to be the latest commit in the push, but only subject lines
are scanned, and only a marker that begins the subject counts. Prose that
mentions a marker in a commit body (or anywhere else in a subject) never
triggers or escalates a release.

Valid markers (the bump level is optional; case is ignored):

| Marker             | Bump  | 1.0.5 → |
| ------------------ | ----- | ------- |
| `[release]`        | minor | 1.1.0   |
| `[release:major]`  | major | 2.0.0   |
| `[release:minor]`  | minor | 1.1.0   |
| `[release:patch]`  | patch | 1.0.6   |

If more than one commit in the push carries a marker, the **latest** such
commit wins. (File contents like this document are never scanned; only commit
subjects are.)

You can also release manually without a marker: **Actions → release → Run
workflow**, then pick the bump level from the dropdown.

### What the pipeline does

The same pipeline runs on every commit and PR; only the publish steps are
gated behind the release check. On a release it, in order:

1. **decides** whether to release and at what level (the strict marker check,
   which only fires on a push to `main` or a manual dispatch);
2. **computes** the next version from the latest `X.Y.Z` tag (refusing if that
   tag already exists);
3. **builds and tests everything in parallel** — `tox` (py310–py314, lint,
   mypy), the wheel + sdist, the self-contained PyInstaller binaries for Linux
   (`amd64`, `arm64`, `i686`, `armv7`, `armv6`, `ppc64le`, `s390x` and
   `riscv64`, glibc and musl), macOS (`arm64` + `amd64`) and Windows (`amd64` +
   `arm64`), each smoke-tested with `--version`, plus a build-only pass over
   every Docker image — all at the computed version. This whole matrix is the
   **gate**: a red anywhere (a failed test, a broken binary, or a broken
   `Dockerfile`) means no release;
4. **only once the entire gate is green**, publishes the wheel + sdist to PyPI
   via [Trusted Publishing (OIDC)](https://docs.pypi.org/trusted-publishers/):
   there is no API token to manage or leak;
5. **after a successful publish**, creates and pushes the `X.Y.Z` tag and a
   GitHub Release with the wheel, sdist, and all the binaries
   (`cronstable-linux-{amd64,arm64,i686,armv7,ppc64le,s390x,riscv64}`, their
   `-musl` variants plus `cronstable-linux-armv6-musl`, `cronstable-macos-{arm64,amd64}`,
   and `cronstable-windows-{amd64,arm64}.exe`) plus a single `SHA256SUMS`
   attached, then pushes the multi-arch container images and updates the
   Homebrew tap.

Because no file is committed back to the repo, a release never re-triggers the
workflow. Because the tag is created *after* publishing, a failed publish leaves
no orphan tag and a re-run cleanly retries the same version.

## Container image

The official image is built and published by the single
[`CI`](.github/workflows/release.yml) pipeline, from the top-level
[`Dockerfile`](Dockerfile) (and the per-distro `docker/Dockerfile.*`):

- **On every commit and PR** it builds every image *without* pushing (the
  `docker` gate job), across their full published arch sets, so a broken
  `Dockerfile` fails CI before a release.
- **On a release**, once the whole gate is green, the `docker-push` job builds
  and pushes each distro's multi-arch image, tagged `<version>` and `:latest`
  (the Debian base owns the bare tags; variants get a `-<distro>` suffix), to
  both `ghcr.io/ptweezy/cronstable` and `docker.io/ptweezy/cronstable`. GHCR
  authenticates with the built-in `GITHUB_TOKEN`; Docker Hub uses the
  `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` repository secrets (skipped if
  unset).

Build it locally the same way CI does (the version is read from git, or pass
`--build-arg VERSION=X.Y.Z`):

```sh
docker build -t cronstable .
docker run --rm -v "$PWD/example/docker/cronstable.yaml:/etc/cronstable.d/cronstable.yaml:ro" cronstable
```
