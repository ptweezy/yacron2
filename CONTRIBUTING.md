# Contributing to yacron2

Thanks for working on yacron2! This document covers local development and how
releases are cut. yacron2 is written in **Rust** (stable, 1.74+).

## Development setup

Install a Rust toolchain from [rustup.rs](https://rustup.rs), then:

```sh
git clone https://github.com/ptweezy/yacron2
cd yacron2
cargo build
```

On Linux/WSL the e-mail reporter links against the system OpenSSL, so install
the development headers first:

```sh
sudo apt-get install -y pkg-config libssl-dev   # Debian/Ubuntu/WSL
```

On Windows the SChannel TLS backend is used, so no extra packages are needed.

> **Tests that spawn processes are Unix-only** (they run `echo`, `sleep`, etc.).
> They run on Linux, macOS, and WSL; the rest of the suite runs everywhere.

## Running the checks

```sh
cargo test               # unit + integration tests
cargo clippy --all-targets   # lints
cargo fmt --all -- --check   # formatting
```

If you don't have a local toolchain (for example, on a Windows box without
Rust installed), everything can be run in Docker:

```sh
docker run --rm -v "$PWD:/work" -w /work rust:1-bookworm \
  bash -c "apt-get update && apt-get install -y pkg-config libssl-dev && cargo test"
```

To build and try the container image:

```sh
docker build -t yacron2 .
docker run --rm -v "$PWD/example/docker/yacron2tab.yaml:/etc/yacron2.d/crontab.yaml" yacron2 -l DEBUG
```

## Releasing

Releases are **automated** by the [`release`](.github/workflows/release.yml)
GitHub Actions workflow. The crate version in `Cargo.toml` is the source of
truth; the workflow computes the next version from the latest `X.Y.Z` git tag
and builds the release binaries at that version (without committing a change
back, so a release never re-triggers the workflow).

### Cutting a release

A release happens when a commit lands on `main` with a **release marker on its
own line** in the commit message:

```
Add retry backoff to the shell reporter

[release:minor]
```

Valid markers (the bump level is optional; whitespace and case are ignored):

| Marker             | Bump  | 1.0.4 → |
| ------------------ | ----- | ------- |
| `[release]`        | minor | 1.1.0   |
| `[release:major]`  | major | 2.0.0   |
| `[release:minor]`  | minor | 1.1.0   |
| `[release:patch]`  | patch | 1.0.5   |

> **The marker must be on a line by itself.** A mention of `[release]` *inside*
> a sentence (for example, in this document or in a commit body) must never
> trigger a publish. The workflow matches a whole line, not a substring.

You can also release manually: **Actions → release → Run workflow**, then pick
the bump level from the dropdown.

### What the pipeline does

On a release the workflow, in order:

1. **decides** whether to release and at what level (the strict marker check);
2. **gates** on `cargo test` / `clippy` / `fmt` — a red build means no release;
3. **builds** a release binary for each target platform at the computed version;
4. **only after a successful build**, creates and pushes the `X.Y.Z` tag and a
   GitHub Release with the binaries attached.

Because the tag is created *after* the build, a failed build leaves no orphan
tag and a re-run cleanly retries the same version.

### Changelog (HISTORY.md)

`HISTORY.md` is maintained by hand. Add an entry under the top heading
describing user-visible changes before cutting a release.
