#!/bin/sh
# Best-effort: bundle orjson (the `speedups` extra) into the current build env
# so the frozen binary / container image ships it. cronstable falls back to the
# stdlib json whenever orjson is absent (cronstable/_json), so EVERY path here is
# non-fatal -- it just records, per arch, which way this build went (grep the
# build log for "orjson"). The script always exits 0.
#
# orjson is a Rust extension with a fairly new MSRV. amd64/arm64 (and macOS/
# Windows), plus most manylinux/musllinux arches, install a prebuilt wheel and
# need no toolchain. The wheel-less arches (riscv64, armv6) source-build it, so
# RUST_SETUP installs a CURRENT stable Rust via rustup into /opt/cargo +
# /opt/rustup -- the distro rustc packages are often older than orjson's MSRV
# (Debian's riscv64 rustc is 1.85; orjson 3.11 needs 1.95). A source build
# (especially under QEMU) can import yet be miscompiled, so verify_orjson.py
# round-trips the result and this script uninstalls it on failure: a broken
# orjson is never shipped, and the fallback is byte-compatible anyway.
#
# Optional env knobs:
#   PIP        install command    (default: pip)              e.g. "uv pip"
#   PIPUNINST  uninstall command  (default: "$PIP uninstall -y")
#   PY         python command     (default: python)           e.g. "uv run python"
#   RUST_SETUP shell run, when the wheel is missing, that installs a CURRENT
#              Rust via rustup into /opt/cargo + /opt/rustup (default: none;
#              see the workflow callers for the per-distro command)
#
# Deliberately NOT `set -e`: every step is best-effort and handled inline. $PIP
# is left unquoted on purpose so "uv pip" word-splits into two tokens.
set -u

PIP="${PIP:-pip}"
PY="${PY:-python}"
PIPUNINST="${PIPUNINST:-$PIP uninstall -y}"
RUST_SETUP="${RUST_SETUP:-}"
here=$(dirname "$0")

if $PIP install "orjson>=3.9"; then
    :  # a prebuilt wheel (or a toolchain already present) installed it
elif [ -n "$RUST_SETUP" ] && sh -c "$RUST_SETUP" \
    && env PATH="/opt/cargo/bin:$PATH" CARGO_HOME=/opt/cargo \
        RUSTUP_HOME=/opt/rustup $PIP install "orjson>=3.9"; then
    :  # no wheel for this arch; a current Rust (rustup) source-built it
else
    echo "orjson: no wheel and no working source build here; using stdlib json"
fi

# Prove it actually round-trips -- a miscompiled source build can import yet be
# broken -- and drop it if not, so only a known-good orjson is ever shipped.
if $PY "$here/verify_orjson.py"; then
    :
else
    echo "orjson: verification failed; uninstalling, using stdlib json"
    $PIPUNINST orjson || true
fi
