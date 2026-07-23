#!/usr/bin/env python3
"""Fail CI if docs/openapi.yaml is missing or not a valid OpenAPI document.

The spec is the contract a generated client (the iOS app, via
swift-openapi-generator, and any other consumer) is built from. cronstable
releases move fast, so the spec is easy to break by accident -- a duplicated
key, a dangling `$ref`, a mistyped schema -- in a way that only surfaces when a
client build fails downstream. This guard parses the spec and validates it
against the OpenAPI schema on every CI run, so it can never drift into being
malformed while still looking fine to a human skim.

It intentionally validates only that the document is a well-formed OpenAPI spec
(structure, refs, schema shapes); it does not diff the spec against the live
routes -- that belongs to a future contract test, and the spec deliberately
leaves fast-moving nested payloads open (additionalProperties).

Usage:
    python .github/scripts/check_openapi.py [path/to/openapi.yaml]
"""

from __future__ import annotations

import sys
from pathlib import Path

# repo root is two levels up from .github/scripts/
_DEFAULT_SPEC = Path(__file__).resolve().parents[2] / "docs" / "openapi.yaml"


def main(argv: list[str]) -> int:
    spec_path = Path(argv[1]) if len(argv) > 1 else _DEFAULT_SPEC
    if not spec_path.is_file():
        print(f"OpenAPI spec not found: {spec_path}", file=sys.stderr)
        return 1

    try:
        from openapi_spec_validator import validate
        from openapi_spec_validator.readers import read_from_filename
    except ImportError:
        print(
            "openapi-spec-validator is required to validate the spec "
            "(pip install openapi-spec-validator, or run via `tox -e openapi`)",
            file=sys.stderr,
        )
        return 1

    try:
        spec_dict, _base_uri = read_from_filename(str(spec_path))
    except Exception as ex:  # noqa: BLE001 - surface any parse error cleanly
        print(f"Could not read {spec_path}: {ex}", file=sys.stderr)
        return 1

    try:
        validate(spec_dict)
    except Exception as ex:  # noqa: BLE001 - the validator raises many types
        print(f"OpenAPI spec is INVALID ({spec_path}):", file=sys.stderr)
        print(f"  {ex}", file=sys.stderr)
        return 1

    paths = spec_dict.get("paths", {})
    print(
        "OpenAPI spec OK: {} ({} paths, OpenAPI {})".format(
            spec_path, len(paths), spec_dict.get("openapi", "?")
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
