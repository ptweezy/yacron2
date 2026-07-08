#!/usr/bin/env bash
# Render the Homebrew formula for a cronstable release by substituting the version
# and the per-artifact SHA256 sums into cronstable.rb.tmpl.
#
# Checksums are read from a published SHA256SUMS manifest (the one attached to
# the GitHub Release), so the formula always pins the exact bytes users get.
#
# Usage: render-formula.sh <version> <sha256sums-file> <template> > cronstable.rb
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <version> <sha256sums-file> <template>" >&2
  exit 2
fi

version="$1"
sums="$2"
template="$3"

sha_for() {
  # Pull the checksum for one asset filename out of a `sha256sum`-format file.
  # Handles both text mode ("<hash>  name") and binary mode ("<hash> *name",
  # which some sha256sum builds emit) by stripping a leading "*" from the name.
  # Fail loudly if the asset is missing.
  local name="$1" hash
  hash="$(awk -v n="$name" '{ f = $2; sub(/^\*/, "", f); if (f == n) print $1 }' "$sums")"
  if [ -z "$hash" ]; then
    echo "error: no SHA256 for '$name' in $sums" >&2
    exit 1
  fi
  printf '%s' "$hash"
}

sed \
  -e "s/@VERSION@/${version}/g" \
  -e "s/@SHA_MACOS_ARM64@/$(sha_for cronstable-macos-arm64)/g" \
  -e "s/@SHA_MACOS_AMD64@/$(sha_for cronstable-macos-amd64)/g" \
  -e "s/@SHA_LINUX_ARM64@/$(sha_for cronstable-linux-arm64)/g" \
  -e "s/@SHA_LINUX_AMD64@/$(sha_for cronstable-linux-amd64)/g" \
  "$template"
