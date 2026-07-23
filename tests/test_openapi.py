"""docs/openapi.yaml stays in lockstep with the served route table.

The structural half of the contract lives in ``.github/scripts/check_openapi.py``
(schema validity, resolvable refs, path-template/parameter agreement, duplicate
path keys). THIS file is the drift half: it diffs the spec's paths and methods
against :data:`cronstable.cron.WEB_ROUTES`, the declarative table
``start_stop_web_app`` builds the aiohttp app from, so a route added, removed,
renamed, or re-methoded in the code without a matching spec edit fails the
suite in every test environment. It also pins the scope-override keys to real
routes: an override whose path drifted from the registration would silently
degrade the DAG approval gate to the `control` scope.
"""

import pathlib

# strictyaml's VENDORED ruamel (the same import cronstable.backends.kubernetes
# uses): guaranteed present wherever cronstable is, unlike the standalone
# ruamel.yaml distribution, which is NOT a dependency (strictyaml >= 1.5
# vendors its copy) and is only ever importable here by accident of the
# developer's global site-packages.
from strictyaml.ruamel import YAML

from cronstable.cron import _WEB_SCOPE_OVERRIDES, WEB_ROUTES, Cron

_SPEC_PATH = pathlib.Path(__file__).parent.parent / "docs" / "openapi.yaml"

# every method key OpenAPI 3.0 allows under a path item
_OPENAPI_METHODS = frozenset(
    {"get", "put", "post", "delete", "options", "head", "patch", "trace"}
)


def _spec_pairs():
    spec = YAML(typ="safe").load(_SPEC_PATH.read_text(encoding="utf-8"))
    pairs = set()
    for path, item in spec["paths"].items():
        for key in item:
            if key in _OPENAPI_METHODS:
                pairs.add((key.upper(), path))
    return pairs


def test_spec_paths_and_methods_match_the_route_table():
    served = {(method, path) for method, path, _handler, _gate in WEB_ROUTES}
    documented = _spec_pairs()
    missing = served - documented
    extra = documented - served
    assert not missing, (
        "routes served but absent from docs/openapi.yaml "
        "(document them, conditional registration included): {}".format(
            sorted(missing)
        )
    )
    assert not extra, (
        "docs/openapi.yaml documents routes the app does not "
        "register: {}".format(sorted(extra))
    )


def test_route_table_handlers_exist():
    # a renamed handler must fail here, not 500 at app build time.
    for _method, path, handler_name, gate in WEB_ROUTES:
        if gate == "mcp":
            from cronstable.mcp import MCPHandler

            owner = MCPHandler
        else:
            owner = Cron
        assert hasattr(owner, handler_name), (
            "route {} names missing handler {}.{}".format(
                path, owner.__name__, handler_name
            )
        )


def test_scope_overrides_name_registered_routes():
    served_paths = {path for _m, path, _h, _g in WEB_ROUTES}
    for override_path in _WEB_SCOPE_OVERRIDES:
        assert override_path in served_paths, (
            "_WEB_SCOPE_OVERRIDES names {!r}, which is not a registered "
            "route; the override would silently stop binding".format(
                override_path
            )
        )
