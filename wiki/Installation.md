# Installation

This page covers every way to install yacron2: the published container image,
`pip`, `pipx`, and the self-contained PyInstaller binaries. It documents the
Python and platform requirements, the runtime dependencies, the exact binary
release assets, and the writable-and-executable temp-directory requirement that
applies to the standalone binary only.

## Requirements

| Requirement | Value |
| --- | --- |
| Python (pip/pipx) | `>= 3.13`; only 3.13 and 3.14 are supported (`requires-python = ">=3.13"`). Python 3.7–3.12 are not supported. |
| Operating system | POSIX only (Linux, macOS). yacron2 imports `grp`/`pwd` at module load; there is no Windows support. |
| CPU architectures | `amd64` (x86_64) and `arm64` for the image and the prebuilt binaries. |

Python is required only for the `pip`/`pipx` installs. The container image
bundles its own interpreter, and the standalone binaries embed Python, so
neither needs Python on the target host.

### Runtime dependencies (pip/pipx)

Installing the `yacron2` distribution pulls in the following, taken from
`pyproject.toml`:

| Dependency | Version constraint |
| --- | --- |
| `strictyaml` | `>=1.7,<2` |
| `crontab` | `>=1,<2` |
| `aiohttp` | `>=3.10,<4` |
| `sentry-sdk` | `>=2,<3` |
| `aiosmtplib` | `>=3,<6` |
| `jinja2` | `>=3,<4` |
| `tzdata` | `>=2024.1` |

`tzdata` ships the IANA time-zone database so `zoneinfo` resolves time zones on
minimal/slim images that do not include the system tz data. See
[Schedules and Timezones](Schedules-and-Timezones).

## Install methods at a glance

| Method | Source | Embeds Python? | Self-extracts at startup? |
| --- | --- | --- | --- |
| Container image | `ghcr.io/ptweezy/yacron2` | Yes (in-image interpreter) | No |
| pip | PyPI (`yacron2`) | No (uses your interpreter) | No |
| pipx | PyPI (`yacron2`) | No (uses your interpreter) | No |
| Standalone binary | GitHub Releases | Yes (embedded) | **Yes** |

Only the standalone binary self-extracts at startup and therefore needs a
writable and executable temp directory (see
[Standalone binary temp-directory requirement](#standalone-binary-temp-directory-requirement)).
The image and the `pip`/`pipx` installs run yacron2 as a normal Python package
with the interpreter on disk and never self-extract.

## Run with Docker

Prebuilt, multi-architecture (`linux/amd64` + `linux/arm64`) images are
published to the GitHub Container Registry on every release. Mount your crontab
and run:

```shell
docker run --rm \
  -v "$PWD/yacron2tab.yaml:/etc/yacron2.d/yacron2tab.yaml:ro" \
  ghcr.io/ptweezy/yacron2:latest
```

The image runs as the non-root user `65534:65534` and its entrypoint is
`yacron2` with default arguments `-c /etc/yacron2.d`, so it reads configuration
from `/etc/yacron2.d` unless you override the arguments. For production, pin a
specific version instead of `latest` (e.g. `ghcr.io/ptweezy/yacron2:1.0.4`).

To bake configuration into your own image, base it on the published image:

```dockerfile
FROM ghcr.io/ptweezy/yacron2:latest

# The base image already runs as the non-root user 65534.
COPY yacron2tab.yaml /etc/yacron2.d/yacron2tab.yaml
```

The image is built from `python:3.14-slim` (a multi-stage build that copies a
self-contained venv into the runtime stage) and sets `PYTHONUNBUFFERED=1` and
`PYTHONDONTWRITEBYTECODE=1`. It requires no writable paths at runtime. See
[Production and Container Deployment](Production-Deployment) for the hardened
Kubernetes/Docker setup (read-only root filesystem, dropped capabilities,
`fsGroup`).

## Install using pip

yacron2 requires Python >= 3.13. Install it in a virtual environment:

```shell
python3 -m venv yacron2env
. yacron2env/bin/activate
pip install yacron2
```

This installs the `yacron2` console script (entry point
`yacron2.__main__:main`). For systems with an older Python, use the standalone
binary instead.

## Install using pipx

[pipx](https://github.com/pipxproject/pipx) creates the virtualenv and installs
the program into it:

```shell
pipx install yacron2
```

pipx still requires a supported Python (3.13 or 3.14) available to build the
isolated environment.

## Install using a binary

A self-contained binary can be downloaded from
<https://github.com/ptweezy/yacron2/releases>. Python is not required on the
target system; it is embedded in the executable. Every release attaches the
following assets, built natively on a matching runner:

| Asset | Platform | libc / arch | Notes |
| --- | --- | --- | --- |
| `yacron2-linux-amd64` | Linux | glibc, x86_64 | Runs on any Linux with glibc 2.39 or newer (e.g. Ubuntu 24.04). |
| `yacron2-linux-arm64` | Linux | glibc, arm64 | Runs on any Linux with glibc 2.39 or newer on arm64. |
| `yacron2-linux-amd64-musl` | Linux | musl, x86_64 | For Alpine and other musl-based systems. |
| `yacron2-linux-arm64-musl` | Linux | musl, arm64 | For Alpine and other musl-based systems. |
| `yacron2-macos-arm64` | macOS | Apple Silicon (arm64) | Developer ID signed and notarized. |
| `yacron2-macos-amd64` | macOS | Intel (x86_64) | Developer ID signed and notarized. |

The glibc Linux builds target glibc 2.39 (the Ubuntu 24.04 runner's libc) and
work on any Linux host with glibc 2.39 or newer on the matching CPU. The musl builds
(added in 1.0.8) are built inside an Alpine container for musl/Alpine hosts.
macOS builds (added in 1.0.10) cover both Apple Silicon and Intel.

Download and run (glibc amd64 Linux shown — append `-musl` on Alpine, or use
`yacron2-macos-<arch>` on a Mac):

```shell
curl -fsSL -o yacron2 \
  https://github.com/ptweezy/yacron2/releases/latest/download/yacron2-linux-amd64
chmod +x yacron2
./yacron2 --version
```

### macOS signing and notarization

Since 1.0.11 the macOS binaries are Developer ID code-signed (hardened runtime)
and notarized by Apple, so Gatekeeper accepts them and they run without first
clearing the quarantine attribute. The earlier 1.0.10 macOS binaries were
unsigned and required `xattr -d com.apple.quarantine` before first run; that
step is no longer needed.

### Standalone binary temp-directory requirement

The standalone binary is a self-extracting executable: on each start it unpacks
its embedded Python runtime into a temporary directory and loads shared
libraries from there. It therefore needs a temporary directory that is both
**writable and executable** (documented in 1.0.9). On an ordinary system the
default `/tmp` already satisfies this, so no extra setup is required.

This matters only when you run the binary under a **read-only root filesystem**
(for example, a hardened container). With the root filesystem read-only, `/tmp`
is read-only too, and the binary aborts at startup — `Could not create temporary
directory`, or `Error loading shared library …: Operation not permitted`. Give
it a small writable *and executable* temp mount and it runs:

```shell
# Note `exec`: Docker's --tmpfs defaults to `noexec`, but the binary must be
# able to execute the libraries it unpacks.
docker run --rm --read-only \
  --tmpfs /tmp:rw,exec,nosuid,nodev,size=64m \
  -v "$PWD/yacron2tab.yaml:/etc/yacron2.d/yacron2tab.yaml:ro" \
  your-image-with-the-binary -c /etc/yacron2.d
```

Remedies:

* **Docker** — mount an `rw,exec` tmpfs at `/tmp`. `--tmpfs` defaults to
  `noexec`, which fails; pass `exec` explicitly as above.
* **Kubernetes** — mount an `emptyDir` at `/tmp` (writable and executable by
  default; use `medium: Memory` for a tmpfs).
* **Any host** — point the binary at another writable, executable directory
  with `TMPDIR=/path`.

This requirement is unique to the standalone binary. The published container
image and the `pip`/`pipx` installs run yacron2 as a normal Python package with
the interpreter on disk, so they never self-extract and need no writable temp
directory. See [Production and Container Deployment](Production-Deployment).

## After installation

Start yacron2 by giving it a configuration file or directory with `-c`; it
always runs in the foreground:

```shell
yacron2 -c /etc/yacron2.d
```

See [Command-Line Reference](CLI-Reference) for all flags, and
[Configuration Reference](Configuration-Reference) for the config schema. If you
are coming from the original yacron, see
[Migration from yacron](Migration-from-yacron).
