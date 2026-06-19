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

A release happens when a commit lands on `main` with a **release marker on its
own line** in the commit message:

```
Add retry backoff to the HTTP reporter

[release:minor]
```

Valid markers (the bump level is optional; whitespace and case are ignored):

| Marker             | Bump  | 1.0.2 → |
| ------------------ | ----- | ------- |
| `[release]`        | minor | 1.1.0   |
| `[release:major]`  | major | 2.0.0   |
| `[release:minor]`  | minor | 1.1.0   |
| `[release:patch]`  | patch | 1.0.3   |

> **The marker must be on a line by itself.** This is deliberate: a mention of
> `[release]` *inside* a sentence (for example, in this very document, or in a
> commit body) must never trigger a publish. The workflow matches a whole line,
> not a substring.

You can also release manually without a marker: **Actions → release → Run
workflow**, then pick the bump level from the dropdown.

### What the pipeline does

On a release the workflow, in order:

1. **decides** whether to release and at what level (the strict marker check);
2. **gates** on `tox` (py313, py314, lint, mypy) — a red build means no release;
3. **builds** the wheel + sdist at the computed version;
4. **publishes to PyPI** via [Trusted Publishing
   (OIDC)](https://docs.pypi.org/trusted-publishers/) — there is no API token to
   manage or leak;
5. **only after a successful publish**, creates and pushes the `X.Y.Z` tag and a
   GitHub Release with the artifacts attached.

Because no file is committed back to the repo, a release never re-triggers the
workflow. Because the tag is created *after* publishing, a failed publish leaves
no orphan tag and a re-run cleanly retries the same version.

### Important: releases are irreversible

- Every release marker that lands on `main` **publishes to PyPI**. There is no
  staging step — treat a marked commit as "ship it."
- **PyPI versions are immutable.** A version, once uploaded, can never be
  re-uploaded, even after it is deleted or yanked. Pick the bump level
  deliberately.

### Changelog (HISTORY.rst)

A local `commit-msg` hook drafts a `HISTORY.rst` entry whenever you make a
release commit, from the commits since the last tag. It is best-effort and
never blocks a commit. Install it once per clone:

```sh
cp .githooks/commit-msg .git/hooks/commit-msg
# Windows (PowerShell):
# Copy-Item .githooks/commit-msg .git/hooks/commit-msg -Force
```

All the logic lives in
[`scripts/gen_changelog_entry.py`](scripts/gen_changelog_entry.py). If a
[`claude`](https://docs.anthropic.com/en/docs/claude-code) CLI is on your
`PATH`, the hook uses it to write a grouped, prose changelog entry; otherwise it
falls back to a plain list of commit subjects. Review/edit the generated entry
before pushing — it ships inside the published sdist.
