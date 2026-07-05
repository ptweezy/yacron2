#!/bin/sh
# Best-effort: bundle orjson (the `speedups` extra) into the current build env
# so the frozen binary / container image ships it. yacron2 falls back to the
# stdlib json whenever orjson is absent (yacron2/_json), so EVERY path here is
# non-fatal -- it just records, per arch, which way this build went (grep the
# build log for "orjson"). The script always exits 0.
#
# orjson is a Rust extension. amd64/arm64 (and macOS/Windows) install a
# prebuilt wheel and need no toolchain; the other arches have no wheel and
# source-build it, which needs Rust -- pass RUST_SETUP so the toolchain is
# installed only when the wheel is missing. A source build (especially under
# QEMU) can import yet be subtly miscompiled, so verify_orjson.py round-trips
# the result and this script uninstalls it on failure: a broken orjson is
# never shipped, and the fallback is byte-compatible anyway.
#
# Optional env knobs:
#   PIP        install command    (default: pip)              e.g. "uv pip"
#   PIPUNINST  uninstall command  (default: "$PIP uninstall -y")
#   PY         python command     (default: python)           e.g. "uv run python"
#   RUST_SETUP shell run to install a Rust toolchain when the wheel is missing
#              (default: none)     e.g. "apk add --no-cache rust cargo"
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
elif [ -n "$RUST_SETUP" ] && sh -c "$RUST_SETUP" && $PIP install "orjson>=3.9"; then
    :  # no wheel for this arch, but the Rust source build succeeded
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
