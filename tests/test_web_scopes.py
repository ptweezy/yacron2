"""Scoped web bearer tokens (web.authTokens).

Covers the three layers of the feature:
  * ``_effective_web_scopes`` / ``_required_web_scope``: the scope model.
  * ``Cron._resolve_web_tokens``: config -> token table (fail-closed).
  * ``Cron._make_auth_middleware``: 401 (unrecognised token) vs 403
    (recognised token, insufficient scope), plus the .ics carve-out and the
    backward-compatible scalar path, as fast fake-request unit tests and one
    real-aiohttp end-to-end boot.

Mirrors the bearer-auth unit tests in tests/test_ui_endpoints.py (fake request
+ sentinel-return / pytest.raises) and the app-boot test in tests/test_cron.py.
"""

import asyncio

import pytest
from aiohttp import web

import cronstable.cron
from cronstable.config import ConfigError
from cronstable.cron import (
    _WEB_ALL_SCOPES,
    Cron,
    _effective_web_scopes,
    _required_web_scope,
    _WebToken,
)

# --------------------------------------------------------------------------
# fake request plumbing for the middleware unit tests
# --------------------------------------------------------------------------


class _FakeResource:
    def __init__(self, canonical):
        self.canonical = canonical


class _FakeRoute:
    def __init__(self, canonical):
        self.resource = _FakeResource(canonical) if canonical else None


class _FakeMatchInfo:
    def __init__(self, canonical):
        self.route = _FakeRoute(canonical)


class _ScopedReq:
    """A minimal stand-in for aiohttp's Request carrying just what the auth
    middleware and _required_web_scope read: path, method, headers, query,
    and a match_info whose route resource has a canonical path."""

    def __init__(
        self,
        path,
        method="GET",
        canonical="__self__",
        headers=None,
        query=None,
    ):
        self.path = path
        self.method = method
        self.headers = headers or {}
        self.query = query or {}
        self.match_info = _FakeMatchInfo(
            path if canonical == "__self__" else canonical
        )


async def _run(middleware, request):
    async def handler(_request):
        return "ok"

    return await middleware(request, handler)


def _bearer(tok):
    return {"Authorization": "Bearer " + tok}


def _table(*entries):
    """Build a token table from (secret, scopes, label) triples, expanding
    implied scopes exactly as _resolve_web_tokens does."""
    return [
        _WebToken(secret.encode("utf-8"), _effective_web_scopes(scopes), label)
        for secret, scopes, label in entries
    ]


# --------------------------------------------------------------------------
# the scope model: _effective_web_scopes / _required_web_scope
# --------------------------------------------------------------------------


def test_effective_scopes_view_only():
    assert _effective_web_scopes(["view"]) == frozenset({"view"})


def test_effective_scopes_control_implies_view():
    assert _effective_web_scopes(["control"]) == frozenset({"control", "view"})


def test_effective_scopes_approve_implies_view():
    assert _effective_web_scopes(["approve"]) == frozenset({"approve", "view"})


def test_effective_scopes_approve_does_not_imply_control():
    assert "control" not in _effective_web_scopes(["approve"])


def test_required_scope_get_is_view():
    assert _required_web_scope(_ScopedReq("/status")) == "view"


def test_required_scope_post_is_control():
    req = _ScopedReq(
        "/jobs/x/start", method="POST", canonical="/jobs/{name}/start"
    )
    assert _required_web_scope(req) == "control"


def test_required_scope_decision_is_approve():
    req = _ScopedReq(
        "/dags/d/runs/r/tasks/t/decision",
        method="POST",
        canonical="/dags/{name}/runs/{run_key}/tasks/{taskkey}/decision",
    )
    assert _required_web_scope(req) == "approve"


def test_required_scope_mcp_is_control():
    req = _ScopedReq("/mcp", method="POST", canonical="/mcp")
    assert _required_web_scope(req) == "control"


def test_required_scope_unmatched_route_falls_back_to_method():
    # a 404-bound request has no resource; still classified by method.
    assert _required_web_scope(_ScopedReq("/nope", canonical=None)) == "view"


# --------------------------------------------------------------------------
# middleware: 401 vs 403 and the scope matrix
# --------------------------------------------------------------------------


async def test_unknown_token_is_401():
    mw = Cron._make_auth_middleware(_table(("viewtok", ["view"], "phone")))
    with pytest.raises(web.HTTPUnauthorized):
        await _run(mw, _ScopedReq("/status", headers=_bearer("nope")))


async def test_view_token_allowed_on_get():
    mw = Cron._make_auth_middleware(_table(("viewtok", ["view"], "phone")))
    assert (
        await _run(mw, _ScopedReq("/status", headers=_bearer("viewtok")))
        == "ok"
    )


async def test_view_token_forbidden_on_control_post():
    mw = Cron._make_auth_middleware(_table(("viewtok", ["view"], "phone")))
    req = _ScopedReq(
        "/jobs/x/start",
        method="POST",
        canonical="/jobs/{name}/start",
        headers=_bearer("viewtok"),
    )
    with pytest.raises(web.HTTPForbidden):
        await _run(mw, req)


async def test_control_token_allowed_on_control_post():
    mw = Cron._make_auth_middleware(_table(("ctl", ["control"], "ci")))
    req = _ScopedReq(
        "/jobs/x/start",
        method="POST",
        canonical="/jobs/{name}/start",
        headers=_bearer("ctl"),
    )
    assert await _run(mw, req) == "ok"


async def test_control_token_allowed_on_get_via_implied_view():
    mw = Cron._make_auth_middleware(_table(("ctl", ["control"], "ci")))
    assert (
        await _run(mw, _ScopedReq("/status", headers=_bearer("ctl"))) == "ok"
    )


async def test_control_token_forbidden_on_approve_route():
    mw = Cron._make_auth_middleware(_table(("ctl", ["control"], "ci")))
    req = _ScopedReq(
        "/dags/d/runs/r/tasks/t/decision",
        method="POST",
        canonical="/dags/{name}/runs/{run_key}/tasks/{taskkey}/decision",
        headers=_bearer("ctl"),
    )
    with pytest.raises(web.HTTPForbidden):
        await _run(mw, req)


async def test_approve_token_allowed_on_approve_route():
    mw = Cron._make_auth_middleware(_table(("apr", ["approve"], "oncall")))
    req = _ScopedReq(
        "/dags/d/runs/r/tasks/t/decision",
        method="POST",
        canonical="/dags/{name}/runs/{run_key}/tasks/{taskkey}/decision",
        headers=_bearer("apr"),
    )
    assert await _run(mw, req) == "ok"


async def test_approve_token_forbidden_on_control_post():
    # approve implies view but NOT control.
    mw = Cron._make_auth_middleware(_table(("apr", ["approve"], "oncall")))
    req = _ScopedReq(
        "/jobs/x/start",
        method="POST",
        canonical="/jobs/{name}/start",
        headers=_bearer("apr"),
    )
    with pytest.raises(web.HTTPForbidden):
        await _run(mw, req)


async def test_view_token_forbidden_on_mcp():
    mw = Cron._make_auth_middleware(_table(("viewtok", ["view"], "phone")))
    req = _ScopedReq(
        "/mcp", method="POST", canonical="/mcp", headers=_bearer("viewtok")
    )
    with pytest.raises(web.HTTPForbidden):
        await _run(mw, req)


async def test_control_token_allowed_on_mcp():
    mw = Cron._make_auth_middleware(_table(("ctl", ["control"], "ci")))
    req = _ScopedReq(
        "/mcp", method="POST", canonical="/mcp", headers=_bearer("ctl")
    )
    assert await _run(mw, req) == "ok"


async def test_scoped_view_token_on_ics_query():
    # a view (or control, via implication) token may ride ?token= on .ics.
    mw = Cron._make_auth_middleware(_table(("viewtok", ["view"], "phone")))
    assert (
        await _run(mw, _ScopedReq("/calendar.ics", query={"token": "viewtok"}))
        == "ok"
    )


async def test_scoped_wrong_token_on_ics_query_is_401():
    mw = Cron._make_auth_middleware(_table(("viewtok", ["view"], "phone")))
    with pytest.raises(web.HTTPUnauthorized):
        await _run(mw, _ScopedReq("/calendar.ics", query={"token": "wrong"}))


async def test_multiple_tokens_each_match_own_scope():
    mw = Cron._make_auth_middleware(
        _table(("viewtok", ["view"], "phone"), ("ctl", ["control"], "ci"))
    )
    # the view token still cannot control...
    start = _ScopedReq(
        "/jobs/x/start",
        method="POST",
        canonical="/jobs/{name}/start",
        headers=_bearer("viewtok"),
    )
    with pytest.raises(web.HTTPForbidden):
        await _run(mw, start)
    # ...but the control token can, on the very same middleware.
    start_ctl = _ScopedReq(
        "/jobs/x/start",
        method="POST",
        canonical="/jobs/{name}/start",
        headers=_bearer("ctl"),
    )
    assert await _run(mw, start_ctl) == "ok"


async def test_scalar_string_token_is_full_scope():
    # backward-compat: a bare string is an all-scopes token and clears every
    # route without a per-route scope lookup.
    mw = Cron._make_auth_middleware("god")
    req = _ScopedReq(
        "/dags/d/runs/r/tasks/t/decision",
        method="POST",
        canonical="/dags/{name}/runs/{run_key}/tasks/{taskkey}/decision",
        headers=_bearer("god"),
    )
    assert await _run(mw, req) == "ok"


# --------------------------------------------------------------------------
# _resolve_web_tokens: config -> table, fail-closed
# --------------------------------------------------------------------------


def test_resolve_tokens_none_when_unconfigured():
    assert Cron._resolve_web_tokens({"listen": []}) is None


def test_resolve_tokens_scalar_is_full_scope():
    table = Cron._resolve_web_tokens({"authToken": {"value": "god"}})
    assert table is not None and len(table) == 1
    assert table[0].scopes == _WEB_ALL_SCOPES
    assert table[0].label == "authToken"
    assert table[0].token_bytes == b"god"


def test_resolve_tokens_scoped_entry():
    table = Cron._resolve_web_tokens(
        {"authTokens": [{"value": "t", "scopes": ["control"], "label": "ci"}]}
    )
    assert table is not None and len(table) == 1
    assert table[0].scopes == frozenset({"control", "view"})
    assert table[0].label == "ci"


def test_resolve_tokens_default_label():
    table = Cron._resolve_web_tokens(
        {"authTokens": [{"value": "t", "scopes": ["view"]}]}
    )
    assert table is not None
    assert table[0].label == "web.authTokens[0]"


def test_resolve_tokens_scalar_and_scoped_combine():
    table = Cron._resolve_web_tokens(
        {
            "authToken": {"value": "god"},
            "authTokens": [{"value": "t", "scopes": ["view"], "label": "ph"}],
        }
    )
    assert table is not None and len(table) == 2
    assert table[0].label == "authToken"
    assert table[1].label == "ph"


def test_resolve_tokens_empty_source_fails_closed():
    # a scoped entry that names no resolvable source must refuse to start,
    # never silently mint an empty (match-anything-empty) token.
    with pytest.raises(ConfigError):
        Cron._resolve_web_tokens(
            {"authTokens": [{"scopes": ["view"], "label": "broken"}]}
        )


def test_resolve_tokens_explicit_empty_value_fails_closed():
    # `value: ""` rides the same fail-closed branch as a missing source.
    with pytest.raises(ConfigError):
        Cron._resolve_web_tokens(
            {"authTokens": [{"value": "", "scopes": ["view"], "label": "e"}]}
        )


def test_resolve_tokens_scoped_duplicate_of_scalar_refused():
    # matching is by secret: a scoped entry repeating the scalar authToken
    # would silently downgrade the all-scopes token to the entry's scopes
    # (the middleware's no-early-return loop keeps the LAST match). Refused
    # at resolve time instead; the error names labels, never the secret.
    with pytest.raises(ConfigError) as exc:
        Cron._resolve_web_tokens(
            {
                "authToken": {"value": "sekrit-dup-9x"},
                "authTokens": [
                    {
                        "value": "sekrit-dup-9x",
                        "scopes": ["view"],
                        "label": "wall",
                    }
                ],
            }
        )
    assert "'wall'" in str(exc.value)
    assert "'authToken'" in str(exc.value)
    assert "sekrit-dup-9x" not in str(exc.value)


def test_resolve_tokens_two_scoped_duplicates_refused():
    with pytest.raises(ConfigError):
        Cron._resolve_web_tokens(
            {
                "authTokens": [
                    {"value": "s", "scopes": ["view"], "label": "a"},
                    {"value": "s", "scopes": ["control"], "label": "b"},
                ]
            }
        )


# --------------------------------------------------------------------------
# end-to-end over a real aiohttp app
# --------------------------------------------------------------------------

_DISABLED_JOB = """
jobs:
  - name: test
    command: echo hi
    schedule: "* * * * *"
    enabled: false
"""


@pytest.mark.asyncio
async def test_scoped_tokens_end_to_end():
    import aiohttp

    from cronstable.config import _build_mcp_config

    cron = cronstable.cron.Cron(None, config_yaml=_DISABLED_JOB)
    web_config = {
        "listen": ["http://127.0.0.1:0"],
        "authTokens": [
            {"value": "viewtok", "scopes": ["view"], "label": "phone"},
            {"value": "ctltok", "scopes": ["control"], "label": "ci"},
            {"value": "apprtok", "scopes": ["approve"], "label": "lead"},
        ],
    }
    await cron.start_stop_web_app(
        web_config, _build_mcp_config({"enabled": True})
    )
    try:
        port = cron.web_runner.addresses[0][1]
        base = "http://127.0.0.1:{}".format(port)
        async with aiohttp.ClientSession() as session:
            # no token -> 401
            async with session.get(base + "/status") as resp:
                assert resp.status == 401
            # view token reads
            async with session.get(
                base + "/status", headers=_bearer("viewtok")
            ) as resp:
                assert resp.status == 200
            # view token cannot control -> 403 (Origin-less POST passes the
            # CSRF gate, so this isolates the scope check)
            async with session.post(
                base + "/jobs/test/start", headers=_bearer("viewtok")
            ) as resp:
                assert resp.status == 403
            # control token controls -> reaches the handler (409: disabled)
            async with session.post(
                base + "/jobs/test/start", headers=_bearer("ctltok")
            ) as resp:
                assert resp.status == 409
            # control implies view
            async with session.get(
                base + "/status", headers=_bearer("ctltok")
            ) as resp:
                assert resp.status == 200
            # the /mcp override binds on the real registered route, on every
            # method: view is refused, control reaches the handler (whose GET
            # answer is 405: the stateless transport has no SSE stream).
            async with session.get(
                base + "/mcp", headers=_bearer("viewtok")
            ) as resp:
                assert resp.status == 403
            async with session.get(
                base + "/mcp", headers=_bearer("ctltok")
            ) as resp:
                assert resp.status == 405
            # the decision route's `approve` override, on the real route:
            # approve passes the scope gate and reaches the handler (409, no
            # such run); control and view are 403 despite outranking approve
            # elsewhere; approve does not leak into control routes but does
            # imply view.
            decision = base + "/dags/d/runs/r/tasks/t/decision"
            body = {"decision": "approve"}
            async with session.post(
                decision, json=body, headers=_bearer("apprtok")
            ) as resp:
                assert resp.status == 409
            for tok in ("ctltok", "viewtok"):
                async with session.post(
                    decision, json=body, headers=_bearer(tok)
                ) as resp:
                    assert resp.status == 403, tok
            async with session.post(
                base + "/jobs/test/start", headers=_bearer("apprtok")
            ) as resp:
                assert resp.status == 403
            async with session.get(
                base + "/status", headers=_bearer("apprtok")
            ) as resp:
                assert resp.status == 200
    finally:
        await cron.start_stop_web_app(None)
        await asyncio.sleep(0.25)
