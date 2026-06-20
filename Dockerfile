# syntax=docker/dockerfile:1
#
# Official yacron2 image: a minimal, non-root, multi-arch build suitable for the
# hardened (read-only root filesystem, dropped-capabilities) deployments
# described in the README. Built and published to GHCR by
# .github/workflows/docker.yml.
#
# Build locally:
#   docker build -t yacron2 .
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
# When VERSION is given we hand it to setuptools_scm directly — no git needed.
# Otherwise (a plain `docker build .`) setuptools_scm reads the version from
# .git, which requires the git binary the slim image does not ship.
RUN set -eux; \
    if [ -n "$VERSION" ]; then \
      export SETUPTOOLS_SCM_PRETEND_VERSION="$VERSION"; \
    else \
      apt-get update; \
      apt-get install -y --no-install-recommends git; \
      rm -rf /var/lib/apt/lists/*; \
    fi; \
    python -m venv /opt/venv; \
    /opt/venv/bin/pip install --no-cache-dir --upgrade pip; \
    /opt/venv/bin/pip install --no-cache-dir .

# ---- runtime stage ------------------------------------------------------
FROM python:3.14-slim

LABEL org.opencontainers.image.title="yacron2" \
      org.opencontainers.image.description="A modern, rootless-container-friendly cron replacement." \
      org.opencontainers.image.source="https://github.com/ptweezy/yacron2" \
      org.opencontainers.image.licenses="MIT"

# Flush stdout/stderr immediately (yacron2 logs to them) and never write .pyc
# files — both matter under a read-only root filesystem.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH"

COPY --from=builder /opt/venv /opt/venv

# Run as an unprivileged, non-root user (65534 = "nobody"). Per-job user/group
# switching is unavailable in this mode; dropping root gives the fully
# locked-down container the README documents.
USER 65534:65534

ENTRYPOINT ["yacron2"]
CMD ["-c", "/etc/yacron2.d"]
