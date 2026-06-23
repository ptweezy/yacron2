# Contributing to yacron2

Thanks for working on yacron2! This document covers local development and,
importantly, how releases are cut.

## Development setup

yacron2 targets **Python 3.10+** (3.10, 3.11, 3.12, 3.13 and 3.14 are tested)
and runs on **Linux, macOS and Windows** (the test suite runs on all three in
CI, including Windows ARM64).

```sh
git clone https://github.com/ptweezy/yacron2
cd yacron2
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"                         # or: pip install -r requirements_dev.txt
```

> **Note:** all OS-specific behaviour lives in
> [`yacron2/platform.py`](yacron2/platform.py) (default shell, default config
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

`pre-commit` runs ruff and bandit on staged changes:

```sh
pip install pre-commit
pre-commit install
```

## Releasing

Releases are **automated** by the [`release`](.github/workflows/release.yml)
GitHub Actions workflow. Version numbers come from git tags via
`setuptools_scm`; you never edit a version by hand.

### Cutting a release

A release happens when **any commit in a push to `main`** has a release marker
anywhere in its message:

```
Add retry backoff to the HTTP reporter

[release:minor]
```

It does not need to be the latest commit in the push, and the marker does not
need to be on its own line.

Valid markers (the bump level is optional; case is ignored):

| Marker             | Bump  | 1.0.5 → |
| ------------------ | ----- | ------- |
| `[release]`        | minor | 1.1.0   |
| `[release:major]`  | major | 2.0.0   |
| `[release:minor]`  | minor | 1.1.0   |
| `[release:patch]`  | patch | 1.0.6   |

If more than one marker appears across the pushed commits, the most significant
bump wins (major > minor > patch).

> **Caution:** the match is a plain substring, so writing a literal
> `[release:patch]` (or `[release]`) anywhere in a commit message — even in prose
> describing the release process — **will trigger a publish**. Don't quote the
> marker verbatim in a commit message unless you mean it. (File contents like
> this document are never scanned — only commit messages are.)

You can also release manually without a marker: **Actions → release → Run
workflow**, then pick the bump level from the dropdown.

### What the pipeline does

On a release the workflow, in order:

1. **decides** whether to release and at what level (the strict marker check);
2. **computes** the next version from the latest `X.Y.Z` tag (refusing if that
   tag already exists);
3. **gates** on `tox` (py310, py311, py312, py313, py314, lint, mypy) — a red build means no release;
4. **builds** the wheel + sdist *and* the self-contained PyInstaller binaries
   for Linux (`amd64`, `arm64`, `i686`, `armv7`, `armv6`, `ppc64le`, `s390x`
   and `riscv64`, glibc and musl), macOS (`arm64` + `amd64`) and Windows
   (`amd64` + `arm64`), each on a matching runner (the non-native Linux arches
   inside a container under QEMU; Windows ARM64 on the `windows-11-arm`
   runner), smoke-tested with `--version` — all at the computed version,
   *before* publishing, so a broken build fails the run instead of producing a
   half-finished release;
5. **publishes the wheel + sdist to PyPI** via [Trusted Publishing
   (OIDC)](https://docs.pypi.org/trusted-publishers/) — there is no API token to
   manage or leak;
6. **only after a successful publish**, creates and pushes the `X.Y.Z` tag and a
   GitHub Release with the wheel, sdist, and all the binaries
   (`yacron2-linux-{amd64,arm64,i686,armv7,ppc64le,s390x,riscv64}`, their
   `-musl` variants plus `yacron2-linux-armv6-musl`, `yacron2-macos-{arm64,amd64}`,
   and `yacron2-windows-{amd64,arm64}.exe`) attached.

Because no file is committed back to the repo, a release never re-triggers the
workflow. Because the tag is created *after* publishing, a failed publish leaves
no orphan tag and a re-run cleanly retries the same version.

## Container image

The official image is built and published by the
[`docker`](.github/workflows/docker.yml) workflow, from the top-level
[`Dockerfile`](Dockerfile):

- **On each published release** it builds one multi-arch (`linux/amd64`,
  `linux/arm64`, `linux/386`, `linux/arm/v7`, `linux/ppc64le` and `linux/s390x`)
  image and pushes it, tagged `<version>` and `:latest`, to both
  `ghcr.io/ptweezy/yacron2` and `docker.io/ptweezy/yacron2`. GHCR authenticates
  with the built-in `GITHUB_TOKEN`; Docker Hub uses the `DOCKERHUB_USERNAME` and
  `DOCKERHUB_TOKEN` repository secrets.
- **On pull requests / `main` pushes** that touch the `Dockerfile`, the package,
  or the workflow, it builds the image *without* pushing, so a broken
  `Dockerfile` fails CI before a release.
- **Manually** (Actions → docker → Run workflow) you can (re)build any existing
  release tag — useful to backfill an image for a release cut before this
  workflow existed, or to retry a failed push.

Build it locally the same way CI does (the version is read from git, or pass
`--build-arg VERSION=X.Y.Z`):

```sh
docker build -t yacron2 .
docker run --rm -v "$PWD/example/docker/yacron2tab.yaml:/etc/yacron2.d/yacron2tab.yaml:ro" yacron2
```
