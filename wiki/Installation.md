# Installation

This page covers every way to install yacron2: the published container image,
`pip`, `pipx`, and the self-contained PyInstaller binaries. It documents the
Python and platform requirements, the runtime dependencies, the exact binary
release assets, and the writable-and-executable temp-directory requirement that
applies to the standalone binary only. yacron2 runs natively on
Windows in addition to Linux and macOS; see [Running on Windows](Running-on-Windows)
for the Windows-specific details.

## Requirements

| Requirement | Value |
| --- | --- |
| Python (pip/pipx) | `>= 3.10`; 3.10, 3.11, 3.12, 3.13 and 3.14 are supported and tested (`requires-python = ">=3.10"`). For an older Python, use the standalone binary instead. |
| Operating system | Linux, macOS, and Windows. OS-specific behavior is isolated in `yacron2/platform.py`; `grp`/`pwd` are only imported on POSIX. A few features differ on Windows; see [Running on Windows](Running-on-Windows). |
| CPU architectures | Linux: `amd64` (x86_64), `arm64`, `i686` (32-bit x86), `armv7` (32-bit ARM), `ppc64le` (POWER) and `s390x` (IBM Z), both the container image and the prebuilt binaries; the prebuilt binaries also cover `riscv64` (glibc and musl) and `armv6` (musl-only). macOS: `amd64` and `arm64` (prebuilt binaries). Windows: `amd64` (x64) and `arm64` (ARM64) (prebuilt binaries). |

Python is required only for the `pip`/`pipx` installs. The container image
bundles its own interpreter, and the standalone binaries embed Python, so
neither needs Python on the target host.

### Runtime dependencies (pip/pipx)

Installing the `yacron2` distribution pulls in the following, taken from
`pyproject.toml`:

| Dependency | Version constraint |
| --- | --- |
| `strictyaml` | `>=1.7,<2` |
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

Prebuilt, multi-architecture (`linux/amd64`, `linux/arm64`, `linux/386`,
`linux/arm/v7`, `linux/ppc64le`, `linux/s390x` and `linux/riscv64`) images are
published on every release to two registries: the GitHub Container Registry
(`ghcr.io/ptweezy/yacron2`) and Docker Hub (`docker.io/ptweezy/yacron2`). The
images are identical; pull from whichever you prefer. Mount your crontab
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

### Distro variants

The default `latest` (and `<version>`) image is built on **Debian** (slim). The
same release is also published on several other bases, so you can match a
specific one to your environment: a familiar userland, an image-provenance
policy that mandates a particular vendor, or the smallest possible image. Each
variant adds a `-<distro>` suffix to the tag (and the default Debian image is
also available explicitly as `-debian`):

| Tag suffix | Base image | Python | Notes |
| --- | --- | --- | --- |
| *(none)* / `-debian` | `python:3.14-slim` | 3.14 | Default. Widest architecture coverage. |
| `-alpine` | `python:3.14-alpine` | 3.14 | musl libc; smallest image. |
| `-ubuntu` | `ubuntu:24.04` | 3.12 | Ubuntu LTS userland. |
| `-rhel` | UBI 9 (`ubi-minimal`) | 3.12 | Red Hat base for RHEL / OpenShift. |
| `-fedora` | `fedora:41` | 3.13 | Leading-edge RPM userland. |
| `-opensuse` | `opensuse/leap:15.6` | 3.11 | SUSE / SLES family. |
| `-amazonlinux` | `amazonlinux:2023` | 3.11 | AWS-centric deployments. |
| `-distroless` | `gcr.io/distroless/python3` | 3.11 | No shell or package manager; minimal attack surface. |

```shell
# e.g. the Alpine variant, pinned to a version:
docker run --rm \
  -v "$PWD/yacron2tab.yaml:/etc/yacron2.d/yacron2tab.yaml:ro" \
  ghcr.io/ptweezy/yacron2:1.0.14-alpine
```

yacron2 is a pure-Python app that supports any Python >= 3.10, so behavior is
identical across variants. Pick the base, not the interpreter version. The
Debian default covers the most architectures; each variant covers the arches
its base image publishes (Alpine matches Debian's full set; RHEL, Fedora,
openSUSE and distroless cover `amd64`, `arm64`, `ppc64le` and `s390x`; Amazon
Linux covers `amd64` and `arm64`). All variants share the same non-root,
read-only-friendly hardening as the default image.

## Install using pip

yacron2 requires Python >= 3.10. Install it in a virtual environment:

```shell
python3 -m venv yacron2env
. yacron2env/bin/activate
pip install yacron2
```

This installs the `yacron2` console script (entry point
`yacron2.__main__:main`). For systems with an older Python, use the standalone
binary instead.

If you plan to use the Kubernetes leadership backend with the optional native
client library (`cluster.kubernetes.clientLibrary: native`), install the extra:
`pip install "yacron2[kubernetes]"`. The default HTTP transport needs no extra
dependency; see
[Clustering and Leader Election](Clustering-and-Leader-Election).

## Install using pipx

[pipx](https://github.com/pipxproject/pipx) creates the virtualenv and installs
the program into it:

```shell
pipx install yacron2
```

pipx still requires a supported Python (3.10 or newer) available to build the
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
| `yacron2-linux-i686` | Linux | glibc, 32-bit x86 | 32-bit x86 (i686) for glibc-based systems. |
| `yacron2-linux-armv7` | Linux | glibc, 32-bit ARM | 32-bit ARM (armv7, e.g. older Raspberry Pi) for glibc-based systems. |
| `yacron2-linux-ppc64le` | Linux | glibc, ppc64le | 64-bit little-endian POWER (IBM POWER) for glibc-based systems. |
| `yacron2-linux-s390x` | Linux | glibc, s390x | IBM Z (s390x, big-endian) for glibc-based systems. |
| `yacron2-linux-riscv64` | Linux | glibc, riscv64 | 64-bit RISC-V for glibc-based systems. |
| `yacron2-linux-amd64-musl` | Linux | musl, x86_64 | For Alpine and other musl-based systems. |
| `yacron2-linux-arm64-musl` | Linux | musl, arm64 | For Alpine and other musl-based systems. |
| `yacron2-linux-i686-musl` | Linux | musl, 32-bit x86 | 32-bit x86 (i686) for Alpine and other musl-based systems. |
| `yacron2-linux-armv7-musl` | Linux | musl, 32-bit ARM | 32-bit ARM (armv7) for Alpine and other musl-based systems. |
| `yacron2-linux-ppc64le-musl` | Linux | musl, ppc64le | 64-bit little-endian POWER for Alpine and other musl-based systems. |
| `yacron2-linux-s390x-musl` | Linux | musl, s390x | IBM Z (s390x) for Alpine and other musl-based systems. |
| `yacron2-linux-riscv64-musl` | Linux | musl, riscv64 | 64-bit RISC-V for Alpine and other musl-based systems. |
| `yacron2-linux-armv6-musl` | Linux | musl, 32-bit ARM | 32-bit ARM (armv6, e.g. Raspberry Pi 1/Zero); musl-only, no glibc build. |
| `yacron2-macos-arm64` | macOS | Apple Silicon (arm64) | Developer ID signed and notarized. |
| `yacron2-macos-amd64` | macOS | Intel (x86_64) | Developer ID signed and notarized. |
| `yacron2-windows-amd64.exe` | Windows | x64 (amd64) | Self-contained `.exe`; Python not required on the target. |
| `yacron2-windows-arm64.exe` | Windows | ARM64 | Self-contained `.exe`; Python not required on the target. |

The glibc Linux builds target glibc 2.39 (the Ubuntu 24.04 runner's libc) and
work on any Linux host with glibc 2.39 or newer on the matching CPU. The musl builds
are built inside an Alpine container for musl/Alpine hosts.
The `i686` and `armv7` builds and the `ppc64le` and `s390x`
builds, both glibc and musl, extend the 64-bit `amd64`/`arm64`
binaries to 32-bit x86, 32-bit ARM, POWER and IBM Z hosts; they build inside a
container (`i686` natively on the x86-64 runner, the rest under QEMU emulation).
The `riscv64` builds cover 64-bit RISC-V for both glibc and
musl, and the musl-only `armv6` build extends to older 32-bit ARM (e.g.
Raspberry Pi 1/Zero); there is no glibc `armv6` build. macOS builds cover both
Apple Silicon and Intel. The Windows binaries are
self-contained `.exe` files for x64 (`amd64`) and ARM64; like the other
binaries they embed Python, so Python is not required on the target.

Download and run (glibc amd64 Linux shown; append `-musl` on Alpine, or use
`yacron2-macos-<arch>` on a Mac):

```shell
curl -fsSL -o yacron2 \
  https://github.com/ptweezy/yacron2/releases/latest/download/yacron2-linux-amd64
chmod +x yacron2
./yacron2 --version
```

On Windows, download `yacron2-windows-amd64.exe` (or `yacron2-windows-arm64.exe`
on ARM64) and run it directly; no `chmod` is needed:

```powershell
.\yacron2-windows-amd64.exe --version
```

### macOS signing and notarization

The macOS binaries are Developer ID code-signed (hardened runtime)
and notarized by Apple, so Gatekeeper accepts them and they run without first
clearing the quarantine attribute; no `xattr -d com.apple.quarantine` step is
needed before first run.

### Standalone binary temp-directory requirement

The standalone binary is a self-extracting executable: on each start it unpacks
its embedded Python runtime into a temporary directory and loads shared
libraries from there. It therefore needs a temporary directory that is both
**writable and executable**. On an ordinary system the
default `/tmp` already satisfies this, so no extra setup is required.

This matters only when you run the binary under a **read-only root filesystem**
(for example, a hardened container). With the root filesystem read-only, `/tmp`
is read-only too, and the binary aborts at startup: `Could not create temporary
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

* **Docker**: mount an `rw,exec` tmpfs at `/tmp`. `--tmpfs` defaults to
  `noexec`, which fails; pass `exec` explicitly as above.
* **Kubernetes**: mount an `emptyDir` at `/tmp` (writable and executable by
  default; use `medium: Memory` for a tmpfs).
* **Any host**: point the binary at another writable, executable directory
  with `TMPDIR=/path`.

This requirement is unique to the standalone binary. The published container
image and the `pip`/`pipx` installs run yacron2 as a normal Python package with
the interpreter on disk, so they never self-extract and need no writable temp
directory. See [Production and Container Deployment](Production-Deployment).

On Windows the self-extracting `.exe` uses the standard Windows temp directory
(`%TEMP%`), which is writable and executable by default; the read-only-rootfs and
`noexec` caveats above are Linux-container concerns only.

## After installation

Start yacron2 by giving it a configuration file or directory with `-c`; it
always runs in the foreground:

```shell
yacron2 -c /etc/yacron2.d
```

The `-c` default is platform-specific: `/etc/yacron2.d` on POSIX, and
`%APPDATA%\yacron2` on Windows (e.g. `C:\Users\<you>\AppData\Roaming\yacron2`,
falling back to the user profile `~` if `APPDATA` is unset). The default `shell`
also differs: `/bin/sh` on POSIX, and on Windows an empty default that runs a
string `command` through `%ComSpec%` (`cmd.exe`). On Windows, press Ctrl-C (or
Ctrl-Break) to stop yacron2 gracefully; it finishes running jobs first, just as
SIGTERM does on POSIX. Note that per-job `user`/`group` switching and `unix://`
web listeners are not available on Windows; see
[Running on Windows](Running-on-Windows) for the full details.

See [Command-Line Reference](CLI-Reference) for all flags, and
[Configuration Reference](Configuration-Reference) for the config schema. For
Windows-specific behavior, see [Running on Windows](Running-on-Windows). If you
are coming from the original yacron, see
[Migration from yacron](Migration-from-yacron).
