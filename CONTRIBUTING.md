# Contributing to yacron2

Thanks for working on yacron2! This document covers local development and,
importantly, how releases are cut.

## Development setup

yacron2 targets **Python 3.13+** (3.13 and 3.14 are tested).

```sh
git clone https://github.com/ptweezy/yacron2
cd yacron2
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"                         # or: pip install -r requirements_dev.txt
```

> **Note:** yacron2 is POSIX-only — it imports `grp`/`pwd` at load time, so the
> package and its test suite must run on Linux/macOS (or WSL on Windows).
> Linting and type-checking work anywhere.

## Running the checks

Everything CI runs is driven by `tox`:

```sh
tox            # all envs: py313, py314, lint, mypy
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
3. **gates** on `tox` (py313, py314, lint, mypy) — a red build means no release;
4. **builds** the wheel + sdist *and* the self-contained PyInstaller binaries
   for `linux/amd64` + `linux/arm64` (each on a native runner, smoke-tested with
   `--version`) — all at the computed version, *before* publishing, so a broken
   build fails the run instead of producing a half-finished release;
5. **publishes the wheel + sdist to PyPI** via [Trusted Publishing
   (OIDC)](https://docs.pypi.org/trusted-publishers/) — there is no API token to
   manage or leak;
6. **only after a successful publish**, creates and pushes the `X.Y.Z` tag and a
   GitHub Release with the wheel, sdist, and both binaries
   (`yacron2-linux-amd64`, `yacron2-linux-arm64`) attached.

Because no file is committed back to the repo, a release never re-triggers the
workflow. Because the tag is created *after* publishing, a failed publish leaves
no orphan tag and a re-run cleanly retries the same version.

### Important: releases are irreversible

- Every release marker that lands on `main` **publishes to PyPI**. There is no
  staging step — treat a marked commit as "ship it."
- **PyPI versions are immutable.** A version, once uploaded, can never be
  re-uploaded, even after it is deleted or yanked. Pick the bump level
  deliberately.

## Container image

The official image is built and published by the
[`docker`](.github/workflows/docker.yml) workflow, from the top-level
[`Dockerfile`](Dockerfile):

- **On each published release** it builds a multi-arch (`linux/amd64` +
  `linux/arm64`) image and pushes it to GHCR as
  `ghcr.io/ptweezy/yacron2:<version>` and `:latest`.
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

> The first GHCR push creates the package as **private**. In the repository's
> *Packages* settings, mark it public and link it to this repo so the image is
> discoverable and `docker pull` works without authentication.

### Publishing to Docker Hub (optional)

GHCR needs no setup — it authenticates with the built-in `GITHUB_TOKEN`. To
*also* publish to Docker Hub, add two repository secrets and the workflow picks
them up automatically (no code change needed):

- `DOCKERHUB_USERNAME` — your Docker Hub account / namespace
- `DOCKERHUB_TOKEN` — a Docker Hub access token with read/write scope

The image is then pushed to `docker.io/<DOCKERHUB_USERNAME>/yacron2` alongside
GHCR.
