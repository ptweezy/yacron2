# syntax=docker/dockerfile:1
#
# Official cronstable image: a minimal, non-root, multi-arch build
#
# Build locally:
#   docker build -t cronstable .
# The version is read from git during the build; CI passes the released version
# explicitly via --build-arg VERSION=X.Y.Z.

# ---- build stage --------------------------------------------------------
FROM python:3.14-slim AS builder

# In CI we pass the already-computed release version so the build is
# deterministic and needs no git history. A plain `docker build .` leaves it
# empty and setuptools_scm reads the version from .git (which .dockerignore
# deliberately keeps in the build context).
ARG VERSION=""

WORKDIR /src
COPY . .

# Install into a self-contained venv so the runtime stage can copy just that,
# leaving the build toolchain behind. The venv lives at the same path in both
# stages (both are python:3.14-slim), so its interpreter symlinks stay valid.
#
# build-essential + libffi/zlib headers let pip source-compile the C-extension
# deps that ship no wheel on some targets — notably the aiohttp stack on 32-bit
# x86 (linux/386), propcache on 32-bit ARM (linux/arm/v7), and
# multidict/frozenlist/ruamel.yaml.clib on linux/riscv64 (no riscv64 wheels yet).
# On amd64/arm64 the whole stack is prebuilt wheels, so the toolchain goes
# unused; either way it stays in this builder stage and never reaches the slim
# runtime image.
#
# When VERSION is given we hand it to setuptools_scm directly — no git needed.
# Otherwise (a plain `docker build .`) setuptools_scm reads the version from
# .git, which requires the git binary the slim image does not ship; git is
# installed only in that case.
# retry() re-runs a network step (package install, pip download) a few times
# with backoff, so a transient mirror/index hiccup does not fail the build.
RUN set -eux; \
    retry() { n=0; until "$@"; do n=$((n+1)); if [ "$n" -ge 5 ]; then return 1; fi; echo "retry $n: $*"; sleep $((n*5)); done; }; \
    pkgs="build-essential libffi-dev zlib1g-dev"; \
    if [ -z "$VERSION" ]; then pkgs="$pkgs git"; fi; \
    retry apt-get -o Acquire::Retries=5 update; \
    retry apt-get -o Acquire::Retries=5 install -y --no-install-recommends $pkgs; \
    rm -rf /var/lib/apt/lists/*; \
    if [ -n "$VERSION" ]; then export SETUPTOOLS_SCM_PRETEND_VERSION="$VERSION"; fi; \
    python -m venv /opt/venv; \
    retry /opt/venv/bin/pip install --no-cache-dir --upgrade pip; \
    retry /opt/venv/bin/pip install --no-cache-dir --timeout 60 .

# Best-effort orjson (the `speedups` extra) to accelerate the durable-state and
# cluster-gossip JSON paths; cronstable/_json falls back to the stdlib json when it
# is absent, so this never fails the build -- it just logs, per arch, which way
# this image went. Almost every arch (amd64/arm64/386/armv7/ppc64le/s390x)
# installs a manylinux wheel and needs no toolchain; only riscv64 has no orjson
# wheel, so it installs a CURRENT Rust via rustup (Debian's packaged rustc is
# older than orjson's MSRV) and source-builds. A source build (especially under
# QEMU) can import yet be miscompiled, so orjson_ok() round-trips it and a
# failure uninstalls it -- a broken orjson is never shipped. The toolchain and
# apt lists live only in this builder stage; the runtime image carries just the
# small compiled orjson .so inside /opt/venv when the build succeeded.
RUN set -eux; \
    orjson_ok() { \
        /opt/venv/bin/python -c 'import orjson,sys; s={"v":1,"a":[1,2.5,True,None],"z":"x"}; b=orjson.dumps(s,option=orjson.OPT_SORT_KEYS); sys.exit(0 if isinstance(b,bytes) and orjson.loads(b)==s else 1)'; \
    }; \
    if /opt/venv/bin/pip install --no-cache-dir --timeout 120 "orjson>=3.9" && orjson_ok; then \
        echo "orjson: bundled (wheel)"; \
    elif apt-get -o Acquire::Retries=5 update \
        && apt-get -o Acquire::Retries=5 install -y --no-install-recommends curl ca-certificates \
        && curl --proto =https --tlsv1.2 -sSf https://sh.rustup.rs | env CARGO_HOME=/opt/cargo RUSTUP_HOME=/opt/rustup sh -s -- -y --default-toolchain stable --profile minimal --no-modify-path \
        && env PATH="/opt/cargo/bin:$PATH" CARGO_HOME=/opt/cargo RUSTUP_HOME=/opt/rustup /opt/venv/bin/pip install --no-cache-dir --timeout 300 "orjson>=3.9" && orjson_ok; then \
        echo "orjson: bundled (source build)"; \
    else \
        echo "orjson: unavailable on this arch; using stdlib json"; \
        /opt/venv/bin/pip uninstall -y orjson || true; \
    fi; \
    rm -rf /var/lib/apt/lists/*

# ---- runtime stage ------------------------------------------------------
FROM python:3.14-slim

LABEL org.opencontainers.image.title="cronstable" \
      org.opencontainers.image.description="A modern, rootless-container-friendly cron replacement." \
      org.opencontainers.image.source="https://github.com/ptweezy/cronstable" \
      org.opencontainers.image.licenses="MIT"

# Flush stdout/stderr immediately (cronstable logs to them) and never write .pyc
# files — because remember....the read-only root filesystem.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH"

COPY --from=builder /opt/venv /opt/venv

# Run as an unprivileged, non-root user (65534 = "nobody"). Per-job user/group
# switching is unavailable in this mode; dropping root gives a fully
# locked-down container.
USER 65534:65534

ENTRYPOINT ["cronstable"]
CMD ["-c", "/etc/cronstable.d"]
