#!/usr/bin/env python3
"""Fail CI if docs/openapi.yaml is missing, malformed, or self-inconsistent.

The spec is the contract a generated client (the iOS app, via
swift-openapi-generator, and any other consumer) is built from. cronstable
releases move fast, so the spec is easy to break by accident in a way that
only surfaces when a client build fails downstream. This guard checks, on
every CI run (`tox -e openapi`):

* the document parses and validates against the OpenAPI 3.0 schema
  (structure, resolvable `$ref`s, path-template/parameter agreement);
* no mapping key is duplicated anywhere in the document. YAML resolves a
  duplicate silently (last one wins), so a re-added path or field makes one
  definition vanish while the spec still validates; the standard loader
  cannot see this, so a dedicated pass walks the raw mapping nodes.

It intentionally checks only the DOCUMENT. Whether the spec matches the
routes the daemon actually serves is the other half of the contract, and
lives in tests/test_openapi.py: it diffs the spec's paths and methods against
cronstable.cron.WEB_ROUTES (the table the aiohttp app is built from) in both
directions, in every test environment.

Usage:
    python .github/scripts/check_openapi.py [path/to/openapi.yaml]
"""

from __future__ import annotations

import sys
from pathlib import Path

# repo root is two levels up from .github/scripts/
_DEFAULT_SPEC = Path(__file__).resolve().parents[2] / "docs" / "openapi.yaml"


def _duplicate_keys(spec_path: Path) -> list[str]:
    """Every duplicated mapping key in the document, with its line number.

    PyYAML (a dependency of openapi-spec-validator, which already parsed the
    document by the time this runs) exposes the raw mapping nodes; the
    default constructor folds duplicates silently, so this walks them itself.
    """
    import yaml

    problems: list[str] = []

    class _DupLoader(yaml.SafeLoader):
        pass

    def _construct_mapping(loader, node, deep=False):
        seen = set()
        for key_node, _value_node in node.value:
            key = loader.construct_object(key_node, deep=deep)
            try:
                dup = key in seen
                seen.add(key)
            except TypeError:  # complex key; OpenAPI keys are scalars
                continue
            if dup:
                problems.append(
                    "duplicated key {!r} (line {})".format(
                        key, key_node.start_mark.line + 1
                    )
                )
        return yaml.SafeLoader.construct_mapping(loader, node, deep)

    _DupLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping
    )
    with open(spec_path, "rt", encoding="utf-8") as handle:
        yaml.load(handle, Loader=_DupLoader)
    return problems


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

    duplicates = _duplicate_keys(spec_path)
    if duplicates:
        print(
            f"OpenAPI spec has duplicated keys ({spec_path}):",
            file=sys.stderr,
        )
        for problem in duplicates:
            print(f"  {problem}", file=sys.stderr)
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
