"""The loopback job-state API and its cron wiring.

Where test_state_job_primitives.py exercises the backend primitives and the
pure logic layer, this file exercises the *server*: it starts a real
:class:`cronstable.jobapi.JobStateAPI` (which binds an ephemeral 127.0.0.1
port) and drives it over real HTTP with an aiohttp client -- the same wire the
`cronstable` job CLI speaks -- covering auth, every primitive's endpoint, the
lease-backed lock manager, and run-scoped secrets.  It then checks the cron
wiring end to end: env injection at launch and token/lock cleanup at finish.

Style: bare ``async def`` tests, real temp store via ``_backend``, no frozen
clock (lock TTLs are kept at the floor and never waited on for a renewal).
"""

import asyncio
import os
import sys

import aiohttp
import pytest
from aiohttp import web

from cronstable import jobapi
from cronstable.config import parse_config_string
from cronstable.cron import Cron
from cronstable.jobapi import (
    MAX_LOCK_PERMITS,
    JobLockManager,
    JobStateAPI,
    RunContext,
    _bracket_host,
    run_environment,
)
from cronstable.jobstate import JobStateError
from cronstable.state import Lease
from tests.test_state import _backend, _state_cfg

_ONE_JOB = (
    "state:\n  path: {path}\n"
    "jobs:\n  - name: j\n    command: 'true'\n    schedule: '* * * * *'\n"
)


def _ctx(token="tok", job="job", secrets=None, allowed_scopes=None):
    return RunContext(
        token=token,
        run_id="rid-" + token,
        job_name=job,
        attempt=0,
        scheduled_at=None,
        host="h",
        default_scope=job,
        allowed_scopes=set(allowed_scopes or ()),
        secrets=secrets or {},
    )


async def _make_api(tmp_path, **cfg_over):
    backend = _backend(tmp_path)
    await backend.start()
    config = {"maxValueBytes": 0, "maxArtifactBytes": 0, "lockTtlSeconds": 5}
    config.update(cfg_over)
    api = JobStateAPI(
        lambda: backend, host="h", base_holder="h#proc", config=config
    )
    await api.start()
    return api, backend


def _auth(token="tok"):
    return {"Authorization": "Bearer " + token}


# --------------------------------------------------------------------------
# Auth
# --------------------------------------------------------------------------


async def test_auth_required(tmp_path):
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx())
    try:
        async with aiohttp.ClientSession() as s:
            r = await s.get(api.base_url + "/v1/run")
            assert r.status == 401
            r = await s.get(api.base_url + "/v1/run", headers=_auth("wrong"))
            assert r.status == 401
            r = await s.get(api.base_url + "/v1/run", headers=_auth())
            assert r.status == 200
            body = await r.json()
            assert body["job"] == "job"
    finally:
        await api.stop()
        await backend.stop()


async def test_auth_non_ascii_token_is_401_not_500(tmp_path):
    # compare_digest raises TypeError for a non-ASCII str, which used to
    # escape the auth check as a 500 + logged traceback; a garbage token can
    # never validate, so the answer on this boundary must be a clean 401.
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx())
    try:
        async with aiohttp.ClientSession() as s:
            r = await s.get(
                api.base_url + "/v1/run",
                headers={"Authorization": "Bearer t\xf6k"},
            )
            assert r.status == 401
    finally:
        await api.stop()
        await backend.stop()


# --------------------------------------------------------------------------
# KV over HTTP
# --------------------------------------------------------------------------


async def test_kv_http_roundtrip(tmp_path):
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx())
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            r = await s.post(
                api.base_url + "/v1/kv/set", json={"key": "k", "value": "v"}
            )
            assert r.status == 200
            r = await s.get(api.base_url + "/v1/kv/get?key=k")
            assert (await r.json())["value"] == "v"
            r = await s.get(api.base_url + "/v1/kv/get?key=absent")
            assert r.status == 404
            r = await s.get(api.base_url + "/v1/kv/list")
            assert [k["key"] for k in (await r.json())["keys"]] == ["k"]
            r = await s.post(api.base_url + "/v1/kv/delete", json={"key": "k"})
            assert (await r.json())["existed"] is True
            r = await s.get(api.base_url + "/v1/kv/get?key=k")
            assert r.status == 404
    finally:
        await api.stop()
        await backend.stop()


async def test_kv_default_scope_is_job_name(tmp_path):
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx(job="alpha"))
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            await s.post(
                api.base_url + "/v1/kv/set", json={"key": "k", "value": "v"}
            )
        # landed in the job's own scope, not some global default.
        from cronstable import jobstate

        assert (await jobstate.kv_get(backend, "alpha", "k"))["value"] == "v"
        assert await jobstate.kv_get(backend, "global", "k") is None
    finally:
        await api.stop()
        await backend.stop()


async def test_kv_explicit_scope_shared(tmp_path):
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx(token="a", job="alpha"))
    api.register_run(_ctx(token="b", job="beta"))
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(
                api.base_url + "/v1/kv/set",
                json={"scope": "global", "key": "shared", "value": "x"},
                headers=_auth("a"),
            )
            r = await s.get(
                api.base_url + "/v1/kv/get?scope=global&key=shared",
                headers=_auth("b"),
            )
            assert (await r.json())["value"] == "x"
    finally:
        await api.stop()
        await backend.stop()


async def test_kv_scope_naming_another_jobs_scope_is_forbidden(tmp_path):
    # "beta" is job alpha's own private default scope: without an explicit
    # allowlist entry, alpha may not name it (would let one job read/write/
    # destroy an unrelated job's state -- see cronstable.jobapi._scope).
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx(token="a", job="alpha"))
    api.register_run(_ctx(token="b", job="beta"))
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(
                api.base_url + "/v1/kv/set",
                json={"key": "k", "value": "secret"},
                headers=_auth("b"),
            )
            r = await s.get(
                api.base_url + "/v1/kv/get?scope=beta&key=k",
                headers=_auth("a"),
            )
            assert r.status == 403
            r = await s.post(
                api.base_url + "/v1/kv/delete",
                json={"scope": "beta", "key": "k"},
                headers=_auth("a"),
            )
            assert r.status == 403
        # beta's value survived the attempted cross-job reach-in.
        from cronstable import jobstate

        got = await jobstate.kv_get(backend, "beta", "k")
        assert got["value"] == "secret"
    finally:
        await api.stop()
        await backend.stop()


async def test_kv_scope_allowlisted_explicitly_permitted(tmp_path):
    api, backend = await _make_api(tmp_path)
    api.register_run(
        _ctx(token="a", job="alpha", allowed_scopes=["shared-team"])
    )
    try:
        async with aiohttp.ClientSession(headers=_auth("a")) as s:
            r = await s.post(
                api.base_url + "/v1/kv/set",
                json={"scope": "shared-team", "key": "k", "value": "v"},
            )
            assert r.status == 200
            r = await s.get(
                api.base_url + "/v1/kv/get?scope=shared-team&key=k"
            )
            assert (await r.json())["value"] == "v"
    finally:
        await api.stop()
        await backend.stop()


async def test_kv_value_size_limit_413(tmp_path):
    api, backend = await _make_api(tmp_path, maxValueBytes=8)
    api.register_run(_ctx())
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            r = await s.post(
                api.base_url + "/v1/kv/set",
                json={"key": "k", "value": "x" * 100},
            )
            assert r.status == 413
    finally:
        await api.stop()
        await backend.stop()


# --------------------------------------------------------------------------
# Cursor over HTTP
# --------------------------------------------------------------------------


async def test_cursor_http_monotonic(tmp_path):
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx())
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            r = await s.post(
                api.base_url + "/v1/cursor/advance",
                json={"name": "wm", "value": 100},
            )
            assert (await r.json()) == {"value": 100, "advanced": True}
            r = await s.post(
                api.base_url + "/v1/cursor/advance",
                json={"name": "wm", "value": 50},
            )
            assert (await r.json()) == {"value": 100, "advanced": False}
            r = await s.get(api.base_url + "/v1/cursor/get?name=wm")
            assert (await r.json())["value"] == 100
    finally:
        await api.stop()
        await backend.stop()


# --------------------------------------------------------------------------
# Idempotency over HTTP
# --------------------------------------------------------------------------


async def test_idempotency_http(tmp_path):
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx())
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            r = await s.post(
                api.base_url + "/v1/idempotency/claim", json={"key": "order-1"}
            )
            assert (await r.json())["fresh"] is True
            r = await s.post(
                api.base_url + "/v1/idempotency/claim", json={"key": "order-1"}
            )
            assert (await r.json())["fresh"] is False
    finally:
        await api.stop()
        await backend.stop()


# --------------------------------------------------------------------------
# Artifact over HTTP
# --------------------------------------------------------------------------


async def test_artifact_http(tmp_path):
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx())
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            r = await s.post(
                api.base_url + "/v1/artifact/put?name=report.csv",
                data=b"a,b,c\n",
            )
            assert r.status == 200
            assert (await r.json())["size"] == 6
            r = await s.get(api.base_url + "/v1/artifact/get?name=report.csv")
            assert await r.read() == b"a,b,c\n"
            assert r.headers["X-Cronstable-Size"] == "6"
            r = await s.get(api.base_url + "/v1/artifact/list")
            assert [a["name"] for a in (await r.json())["artifacts"]] == [
                "report.csv"
            ]
            r = await s.get(api.base_url + "/v1/artifact/get?name=nope")
            assert r.status == 404
    finally:
        await api.stop()
        await backend.stop()


async def test_artifact_size_limit_413(tmp_path):
    api, backend = await _make_api(tmp_path, maxArtifactBytes=4)
    api.register_run(_ctx())
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            r = await s.post(
                api.base_url + "/v1/artifact/put?name=big", data=b"x" * 100
            )
            assert r.status == 413
    finally:
        await api.stop()
        await backend.stop()


async def test_artifact_put_2mib_with_no_limit(tmp_path):
    # maxArtifactBytes 0 is the documented "no limit": aiohttp's default
    # 1 MiB client_max_size must not override it with a spurious 413.
    api, backend = await _make_api(tmp_path)  # _make_api sets both limits 0
    api.register_run(_ctx())
    payload = b"x" * (2 * 1024 * 1024)
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            r = await s.post(
                api.base_url + "/v1/artifact/put?name=big", data=payload
            )
            assert r.status == 200
            assert (await r.json())["size"] == len(payload)
    finally:
        await api.stop()
        await backend.stop()


async def test_artifact_and_kv_2mib_within_raised_limits(tmp_path):
    # with finite limits the transport cap is derived from them: a payload
    # under the configured maxArtifactBytes/maxValueBytes but over aiohttp's
    # 1 MiB default must succeed (xcom push rides the same artifact route).
    api, backend = await _make_api(
        tmp_path,
        maxArtifactBytes=8 * 1024 * 1024,
        maxValueBytes=8 * 1024 * 1024,
    )
    api.register_run(_ctx())
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            r = await s.post(
                api.base_url + "/v1/artifact/put?name=big",
                data=b"x" * (2 * 1024 * 1024),
            )
            assert r.status == 200
            r = await s.post(
                api.base_url + "/v1/kv/set",
                json={"key": "k", "value": "v" * (2 * 1024 * 1024)},
            )
            assert r.status == 200
    finally:
        await api.stop()
        await backend.stop()


async def test_json_body_over_transport_cap_is_413_not_400(tmp_path):
    # a JSON body larger than the derived transport cap is a 413 and must
    # surface as one: _json_body used to swallow HTTPRequestEntityTooLarge
    # and mislabel it 400 "request body is not valid JSON".
    api, backend = await _make_api(
        tmp_path, maxValueBytes=1024, maxArtifactBytes=1024
    )
    api.register_run(_ctx())
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            r = await s.post(
                api.base_url + "/v1/kv/set",
                json={"key": "k", "value": "v" * (512 * 1024)},
            )
            assert r.status == 413
            assert "not valid JSON" not in (await r.text())
    finally:
        await api.stop()
        await backend.stop()


# --------------------------------------------------------------------------
# Secrets (run-scoped, in memory)
# --------------------------------------------------------------------------


async def test_secret_http(tmp_path):
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx(secrets={"TOKEN": "hunter2"}))
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            r = await s.get(api.base_url + "/v1/secret/get?name=TOKEN")
            assert (await r.json())["value"] == "hunter2"
            r = await s.get(api.base_url + "/v1/secret/list")
            assert (await r.json())["names"] == ["TOKEN"]
            r = await s.get(api.base_url + "/v1/secret/get?name=OTHER")
            assert r.status == 404
    finally:
        await api.stop()
        await backend.stop()


async def test_secret_is_run_scoped(tmp_path):
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx(token="a", secrets={"S": "sekret"}))
    api.register_run(_ctx(token="b", secrets={}))
    try:
        async with aiohttp.ClientSession() as s:
            r = await s.get(
                api.base_url + "/v1/secret/get?name=S", headers=_auth("a")
            )
            assert (await r.json())["value"] == "sekret"
            # run b never had it staged.
            r = await s.get(
                api.base_url + "/v1/secret/get?name=S", headers=_auth("b")
            )
            assert r.status == 404
    finally:
        await api.stop()
        await backend.stop()


# --------------------------------------------------------------------------
# Locks (mutex / semaphore) over HTTP
# --------------------------------------------------------------------------


async def test_lock_mutex_excludes(tmp_path):
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx(token="a", job="alpha"))
    api.register_run(_ctx(token="b", job="alpha"))
    try:
        async with aiohttp.ClientSession() as s:
            r = await s.post(
                api.base_url + "/v1/lock/acquire",
                json={"scope": "global", "name": "L"},
                headers=_auth("a"),
            )
            got_a = await r.json()
            assert got_a["acquired"] is True
            # a second run cannot take the held mutex.
            r = await s.post(
                api.base_url + "/v1/lock/acquire",
                json={"scope": "global", "name": "L"},
                headers=_auth("b"),
            )
            assert (await r.json())["acquired"] is False
            # once released, it is free again.
            r = await s.post(
                api.base_url + "/v1/lock/release",
                json={"token": got_a["token"]},
                headers=_auth("a"),
            )
            assert (await r.json())["released"] is True
            r = await s.post(
                api.base_url + "/v1/lock/acquire",
                json={"scope": "global", "name": "L"},
                headers=_auth("b"),
            )
            assert (await r.json())["acquired"] is True
    finally:
        await api.stop()
        await backend.stop()


async def test_lock_semaphore_two_permits(tmp_path):
    api, backend = await _make_api(tmp_path)
    for t in ("a", "b", "c"):
        api.register_run(_ctx(token=t, job="alpha"))
    try:
        async with aiohttp.ClientSession() as s:

            async def acq(token):
                r = await s.post(
                    api.base_url + "/v1/lock/acquire",
                    json={"scope": "global", "name": "S", "permits": 2},
                    headers=_auth(token),
                )
                return await r.json()

            a, b = await acq("a"), await acq("b")
            assert a["acquired"] and b["acquired"]
            assert {a["slot"], b["slot"]} == {0, 1}
            # both permits taken: the third is refused.
            assert (await acq("c"))["acquired"] is False
    finally:
        await api.stop()
        await backend.stop()


async def test_lock_released_on_finish_run(tmp_path):
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx(token="a", job="alpha"))
    api.register_run(_ctx(token="b", job="alpha"))
    try:
        async with aiohttp.ClientSession() as s:
            r = await s.post(
                api.base_url + "/v1/lock/acquire",
                json={"scope": "global", "name": "L"},
                headers=_auth("a"),
            )
            assert (await r.json())["acquired"] is True
            # run a ends without releasing: finish_run must free its lock.
            await api.finish_run("a")
            r = await s.post(
                api.base_url + "/v1/lock/acquire",
                json={"scope": "global", "name": "L"},
                headers=_auth("b"),
            )
            assert (await r.json())["acquired"] is True
    finally:
        await api.stop()
        await backend.stop()


async def test_lock_acquire_after_run_ended_does_not_leak(tmp_path):
    # a blocking acquire that lands AFTER its run was already finished must not
    # record a hold or start a renewer (that would pin the lease forever).
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx(token="a", job="alpha"))
    api.register_run(_ctx(token="b", job="alpha"))
    try:
        await api.finish_run("a")  # run a ends before its acquire lands
        result = await api.locks.acquire("a", "global", "L")
        assert result["acquired"] is False
        assert result.get("runEnded") is True
        assert api.locks._holds == {}  # nothing recorded, no renewer
        # the lease was handed straight back, so a live run can take it.
        r = await api.locks.acquire("b", "global", "L")
        assert r["acquired"] is True
    finally:
        await api.stop()
        await backend.stop()


async def test_lock_per_acquire_ttl_used_for_hold(tmp_path):
    # a per-acquire --ttl must drive the hold (and thus the renewer), not the
    # manager default -- otherwise a short lease lapses before its first renew.
    api, backend = await _make_api(tmp_path, lockTtlSeconds=30)
    api.register_run(_ctx(token="a"))
    try:
        r = await api.locks.acquire("a", "s", "L", ttl=6)
        assert r["acquired"] is True
        assert r["ttl"] == 6  # reply reports the actual ttl, not the default
        hold = api.locks._holds[r["token"]]
        assert hold.ttl == 6  # the renewer renews on 6, not 30
    finally:
        await api.stop()
        await backend.stop()


async def test_lock_acquire_non_numeric_fields_400(tmp_path):
    # ttl/blockSeconds that cannot convert are the caller's bad input: a
    # clean 400 (like permits two lines above), not ValueError -> 500.
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx())
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            for body in (
                {"name": "L", "ttl": "abc"},
                {"name": "L", "blockSeconds": "zz"},
                {"name": "L", "ttl": {"nested": 1}},
            ):
                r = await s.post(api.base_url + "/v1/lock/acquire", json=body)
                assert r.status == 400, body
    finally:
        await api.stop()
        await backend.stop()


async def test_lock_acquire_permits_over_cap_400(tmp_path):
    # permits comes straight from the request body, and every permit is a
    # SEQUENTIALLY probed lease per acquire pass: an absurd count
    # (--permits 1000000000) must be the caller's clean 400 up front, not a
    # store-hammering scan.
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx())
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            body = {"name": "L", "permits": MAX_LOCK_PERMITS + 1}
            r = await s.post(api.base_url + "/v1/lock/acquire", json=body)
            assert r.status == 400
            # the ceiling itself is accepted (slot 0 is free: instant grant).
            body = {"name": "L", "permits": MAX_LOCK_PERMITS}
            r = await s.post(api.base_url + "/v1/lock/acquire", json=body)
            assert (await r.json())["acquired"] is True
    finally:
        await api.stop()
        await backend.stop()


def test_bracket_host_formats_ipv6_authority():
    # the bound host goes into CRONSTABLE_STATE_URL: an IPv6 literal must be
    # bracketed or "http://::1:8080" is unparseable to every consumer.
    assert _bracket_host("127.0.0.1") == "127.0.0.1"
    assert _bracket_host("localhost") == "localhost"
    assert _bracket_host("::1") == "[::1]"
    assert _bracket_host("fe80::1%eth0") == "[fe80::1%eth0]"


async def test_idempotency_claim_non_numeric_ttl_400(tmp_path):
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx())
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            r = await s.post(
                api.base_url + "/v1/idempotency/claim",
                json={"key": "k", "ttl": "abc"},
            )
            assert r.status == 400
    finally:
        await api.stop()
        await backend.stop()


async def test_cursor_value_size_limit_413(tmp_path):
    api, backend = await _make_api(tmp_path, maxValueBytes=8)
    api.register_run(_ctx())
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            r = await s.post(
                api.base_url + "/v1/cursor/advance",
                json={"name": "wm", "value": "x" * 100},
            )
            assert r.status == 413
    finally:
        await api.stop()
        await backend.stop()


async def test_finish_run_revokes_token(tmp_path):
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx(token="a"))
    try:
        await api.finish_run("a")
        async with aiohttp.ClientSession() as s:
            r = await s.get(api.base_url + "/v1/run", headers=_auth("a"))
            assert r.status == 401  # token no longer valid
    finally:
        await api.stop()
        await backend.stop()


# --------------------------------------------------------------------------
# run_environment
# --------------------------------------------------------------------------


def test_run_environment_keys():
    ctx = _ctx()
    env = run_environment(ctx, "http://127.0.0.1:9999")
    assert env["CRONSTABLE_STATE_URL"] == "http://127.0.0.1:9999"
    assert env["CRONSTABLE_STATE_TOKEN"] == "tok"
    assert env["CRONSTABLE_JOB_NAME"] == "job"
    assert env["CRONSTABLE_ATTEMPT"] == "0"
    # None scheduled time -> empty string, not absent
    assert env["CRONSTABLE_SCHEDULED_AT"] == ""
    assert all(isinstance(v, str) for v in env.values())


# --------------------------------------------------------------------------
# Cron wiring: injection at launch, cleanup at finish
# --------------------------------------------------------------------------


async def test_cron_starts_job_api_and_injects_env(tmp_path):
    cron = Cron(None, config_yaml=_ONE_JOB.format(path=tmp_path))
    await cron.start_stop_state(_state_cfg(_ONE_JOB.format(path=tmp_path)))
    try:
        assert cron._job_api is not None
        job = parse_config_string(_ONE_JOB.format(path=tmp_path), "").jobs[0]
        token, env = cron._prepare_job_api_run(job, None)
        assert token is not None
        assert env["CRONSTABLE_STATE_URL"].startswith("http://127.0.0.1:")
        assert env["CRONSTABLE_STATE_TOKEN"] == token
        assert env["CRONSTABLE_JOB_NAME"] == "j"
        # the run is registered and reachable; finish revokes it.
        async with aiohttp.ClientSession(
            headers={"Authorization": "Bearer " + token}
        ) as s:
            r = await s.get(env["CRONSTABLE_STATE_URL"] + "/v1/run")
            assert r.status == 200
        await cron._job_api.finish_run(token)
    finally:
        await cron._stop_job_api()
        if cron.state_backend is not None:
            await cron.state_backend.stop()


async def test_cron_jobapi_disabled(tmp_path):
    cron = Cron(None, config_yaml=_ONE_JOB.format(path=tmp_path))
    await cron.start_stop_state(
        _state_cfg(
            "state:\n  path: {}\n  jobApi:\n    enabled: false\n".format(
                tmp_path
            )
        )
    )
    try:
        assert cron._job_api is None
        job = parse_config_string(_ONE_JOB.format(path=tmp_path), "").jobs[0]
        token, env = cron._prepare_job_api_run(job, None)
        assert token is None
        assert env == {}
    finally:
        if cron.state_backend is not None:
            await cron.state_backend.stop()


async def test_end_to_end_real_subprocess(tmp_path):
    # The full chain in one process: the daemon injects the env, a REAL child
    # process runs the CLI, the CLI reaches the loopback endpoint over TCP, and
    # the write lands in the backend. This is the one seam the other tests
    # split across the server, the CLI parser, and env injection.
    cron = Cron(None, config_yaml=_ONE_JOB.format(path=tmp_path))
    await cron.start_stop_state(_state_cfg(_ONE_JOB.format(path=tmp_path)))
    try:
        job = parse_config_string(_ONE_JOB.format(path=tmp_path), "").jobs[0]
        token, env = cron._prepare_job_api_run(job, None)
        child_env = {**os.environ, **env}
        # run via create_subprocess_exec (not blocking subprocess.run) so the
        # daemon's event loop stays free to serve the child's loopback request.
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "cronstable",
            "state",
            "set",
            "greeting",
            "hi",
            env=child_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _out, err = await proc.communicate()
        assert proc.returncode == 0, err.decode(errors="replace")
        # it landed in the job's own default scope (the job name), through the
        # endpoint the child reached over the injected CRONSTABLE_STATE_URL.
        from cronstable import jobstate

        body = await jobstate.kv_get(cron.state_backend, "j", "greeting")
        assert body is not None and body["value"] == "hi"
        await cron._job_api.finish_run(token)
    finally:
        await cron._stop_job_api()
        if cron.state_backend is not None:
            await cron.state_backend.stop()


async def test_cli_subprocess_ignores_proxy_env(tmp_path):
    # http_proxy in the job's environment (inherited from the host, common
    # behind corporate proxies) must not reroute loopback state calls: the
    # CLI pins a proxy-free opener, so the request still lands on the
    # daemon instead of shipping the bearer run token to an external proxy.
    # Nothing listens on the proxy address, so a proxied request would fail.
    cron = Cron(None, config_yaml=_ONE_JOB.format(path=tmp_path))
    await cron.start_stop_state(_state_cfg(_ONE_JOB.format(path=tmp_path)))
    try:
        job = parse_config_string(_ONE_JOB.format(path=tmp_path), "").jobs[0]
        token, env = cron._prepare_job_api_run(job, None)
        proxy = "http://127.0.0.1:1"
        child_env = {
            **os.environ,
            **env,
            "http_proxy": proxy,
            "HTTP_PROXY": proxy,
            "https_proxy": proxy,
            "HTTPS_PROXY": proxy,
            "all_proxy": proxy,
            "ALL_PROXY": proxy,
            "no_proxy": "",
            "NO_PROXY": "",
        }
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "cronstable",
            "state",
            "set",
            "via",
            "loopback",
            env=child_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _out, err = await proc.communicate()
        assert proc.returncode == 0, err.decode(errors="replace")
        from cronstable import jobstate

        body = await jobstate.kv_get(cron.state_backend, "j", "via")
        assert body is not None and body["value"] == "loopback"
        await cron._job_api.finish_run(token)
    finally:
        await cron._stop_job_api()
        if cron.state_backend is not None:
            await cron.state_backend.stop()


async def test_cron_stages_secrets(tmp_path):
    yaml = (
        "state:\n  path: {path}\n"
        "jobs:\n  - name: j\n    command: 'true'\n"
        "    schedule: '* * * * *'\n"
        "    secrets:\n      - name: TOKEN\n        value: s3cr3t\n"
    ).format(path=tmp_path)
    cron = Cron(None, config_yaml=yaml)
    await cron.start_stop_state(_state_cfg(yaml))
    try:
        job = parse_config_string(yaml, "").jobs[0]
        token, env = cron._prepare_job_api_run(job, None)
        async with aiohttp.ClientSession(
            headers={"Authorization": "Bearer " + token}
        ) as s:
            url = env["CRONSTABLE_STATE_URL"] + "/v1/secret/get?name=TOKEN"
            r = await s.get(url)
            assert (await r.json())["value"] == "s3cr3t"
        await cron._job_api.finish_run(token)
    finally:
        await cron._stop_job_api()
        if cron.state_backend is not None:
            await cron.state_backend.stop()

# ===========================================================================
# Degraded paths: a vanished backend, probe/renewal timeouts, lease
# takeover, junk request bodies, and the auth/bind parsers.
# ===========================================================================

def _lease(name="lock/s/l#0", holder="h#x", fence=1):
    return Lease(name, holder, fence, 9e18)


class _ScriptedBackend:
    """A lease backend whose renew outcomes are scripted per call."""

    def __init__(self, renew_outcomes=()):
        self.renews = list(renew_outcomes)
        self.released = []

    async def acquire_lease(self, name, holder, ttl):
        return Lease(name, holder, 1, 9e18)

    async def renew_lease(self, lease, ttl):
        outcome = self.renews.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def release_lease(self, lease):
        self.released.append(lease.name)


def _manager(backend, ttl=5.0, live=True):
    return JobLockManager(
        lambda: backend,
        "h#proc",
        ttl,
        is_run_live=lambda token: live,
    )


def _instant_sleep(monkeypatch):
    real_sleep = asyncio.sleep

    async def fast(_delay):
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fast)


# ---------------------------------------------------------------------------
# acquire: validation + degraded backends
# ---------------------------------------------------------------------------


async def test_acquire_permit_bounds():
    mgr = _manager(_ScriptedBackend())
    with pytest.raises(JobStateError, match="permits must be >= 1"):
        await mgr.acquire("run", "s", "l", permits=0)
    with pytest.raises(JobStateError, match="permits must be <="):
        await mgr.acquire("run", "s", "l", permits=10**9)


async def test_acquire_without_backend_is_503():
    mgr = _manager(None)
    with pytest.raises(JobStateError, match="backend is unavailable"):
        await mgr.acquire("run", "s", "l")


async def test_acquire_probe_timeout_skips_slot(monkeypatch):
    class _Hanging(_ScriptedBackend):
        async def acquire_lease(self, name, holder, ttl):
            await asyncio.Event().wait()  # never resolves

    monkeypatch.setattr(jobapi, "STATE_OP_TIMEOUT", 0.05)
    mgr = _manager(_Hanging())
    result = await mgr.acquire("run", "s", "l")
    assert result == {"acquired": False}


async def test_blocking_acquire_retries_until_deadline(monkeypatch):
    class _Denying(_ScriptedBackend):
        def __init__(self):
            super().__init__()
            self.attempts = 0

        async def acquire_lease(self, name, holder, ttl):
            self.attempts += 1
            return None  # held by someone else

    backend = _Denying()
    mgr = _manager(backend, ttl=5.0)
    _instant_sleep(monkeypatch)
    result = await mgr.acquire(
        "run", "s", "l", wait=True, block_seconds=0.05
    )
    assert result == {"acquired": False}
    assert backend.attempts > 1  # it kept retrying between sleeps


async def test_acquire_after_run_ended_hands_lease_back():
    backend = _ScriptedBackend()
    mgr = _manager(backend, live=False)
    result = await mgr.acquire("run", "s", "l")
    assert result == {"acquired": False, "runEnded": True}
    assert backend.released  # the just-won lease went straight back


async def test_safe_release_swallows_backend_errors():
    class _Broken(_ScriptedBackend):
        async def release_lease(self, lease):
            raise RuntimeError("store on fire")

    await JobLockManager._safe_release(_Broken(), _lease())


# ---------------------------------------------------------------------------
# release
# ---------------------------------------------------------------------------


async def test_release_wrong_run_or_unknown_hold():
    backend = _ScriptedBackend()
    mgr = _manager(backend)
    got = await mgr.acquire("run", "s", "l")
    token = got["token"]
    assert await mgr.release("someone-else", token) is False
    assert await mgr.release("run", "no-such-hold") is False
    assert await mgr.release("run", token) is True


async def test_release_swallows_backend_errors():
    class _Broken(_ScriptedBackend):
        async def release_lease(self, lease):
            raise RuntimeError("boom")

    mgr = _manager(_Broken())
    got = await mgr.acquire("run", "s", "l")
    assert await mgr.release("run", got["token"]) is True


async def test_release_all_tolerates_stale_hold_tokens():
    backend = _ScriptedBackend()
    mgr = _manager(backend)
    first = await mgr.acquire("run", "s", "l")
    second = await mgr.acquire("run", "s", "l2")
    # releasing one of two holds keeps the run's hold-set for the other
    assert await mgr.release("run", first["token"]) is True
    assert mgr._run_holds["run"] == {second["token"]}
    # a stale token (hold already gone) is skipped, the live one released
    mgr._run_holds["run"].add("ghost-token")
    await mgr.release_all("run")
    assert mgr._holds == {}


async def test_finish_run_without_token_is_noop(tmp_path):
    api, backend = await _make_api(tmp_path)
    try:
        await api.finish_run("")  # a run that never registered
    finally:
        await api.stop()
        await backend.stop()


# ---------------------------------------------------------------------------
# the renewal loop
# ---------------------------------------------------------------------------


async def _acquired(mgr):
    got = await mgr.acquire("run", "s", "l")
    assert got["acquired"] is True
    return got["token"]


async def test_renew_updates_then_flags_takeover(monkeypatch):
    fresh = _lease(fence=2)
    backend = _ScriptedBackend(renew_outcomes=[fresh, None])
    mgr = _manager(backend)
    _instant_sleep(monkeypatch)
    token = await _acquired(mgr)
    hold = mgr._holds[token]
    await asyncio.wait_for(hold.renewer, 5)
    # first tick adopted the renewed lease, second saw the takeover
    assert hold.lease is fresh
    assert hold.lost is True
    # a lost hold is not released back (a peer holds it now)
    assert await mgr.release("run", token) is True
    assert backend.released == []


async def test_renew_retries_through_transient_errors(monkeypatch):
    backend = _ScriptedBackend(
        renew_outcomes=[TimeoutError(), RuntimeError("blip"), None]
    )
    mgr = _manager(backend)
    _instant_sleep(monkeypatch)
    token = await _acquired(mgr)
    hold = mgr._holds[token]
    await asyncio.wait_for(hold.renewer, 5)
    assert backend.renews == []  # all three outcomes were consumed
    assert hold.lost is True


async def test_renew_stops_when_hold_vanishes(monkeypatch):
    backend = _ScriptedBackend(renew_outcomes=[])
    mgr = _manager(backend)
    _instant_sleep(monkeypatch)
    token = await _acquired(mgr)
    renewer = mgr._holds[token].renewer
    del mgr._holds[token]  # dropped without cancelling the renewer
    await asyncio.wait_for(renewer, 5)  # returns instead of renewing


# ---------------------------------------------------------------------------
# HTTP guards over the real server
# ---------------------------------------------------------------------------


async def test_body_guards_over_http(tmp_path):
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx())
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            # no body at all: an empty payload, so `key` is missing
            r = await s.post(api.base_url + "/v1/kv/set")
            assert r.status == 400
            assert "key is required" in (await r.json())["error"]
            # malformed JSON
            r = await s.post(
                api.base_url + "/v1/kv/set",
                data=b"{nope",
                headers={"Content-Type": "application/json"},
            )
            assert r.status == 400
            assert "not valid JSON" in (await r.json())["error"]
            # valid JSON, wrong shape
            r = await s.post(
                api.base_url + "/v1/kv/set",
                data=b"[1, 2]",
                headers={"Content-Type": "application/json"},
            )
            assert r.status == 400
            assert "JSON object" in (await r.json())["error"]
            # a non-portable value fails closed as the caller's 400
            r = await s.post(
                api.base_url + "/v1/kv/set",
                data=b'{"key": "k", "value": Infinity}',
                headers={"Content-Type": "application/json"},
            )
            assert r.status == 400
            assert "not portable" in (await r.json())["error"]
    finally:
        await api.stop()
        await backend.stop()


async def test_cursor_guards_over_http(tmp_path):
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx())
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            r = await s.get(api.base_url + "/v1/cursor/get?name=missing")
            assert r.status == 404
            r = await s.post(
                api.base_url + "/v1/cursor/advance", json={"name": "c"}
            )
            assert r.status == 400
            assert "value is required" in (await r.json())["error"]
    finally:
        await api.stop()
        await backend.stop()


async def test_idempotency_release_over_http(tmp_path):
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx())
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            r = await s.post(
                api.base_url + "/v1/idempotency/claim",
                json={"key": "k", "ttl": 60},
            )
            assert (await r.json())["fresh"] is True
            r = await s.post(
                api.base_url + "/v1/idempotency/release", json={"key": "k"}
            )
            assert (await r.json())["released"] is True
            r = await s.post(
                api.base_url + "/v1/idempotency/release",
                json={"key": "never-claimed"},
            )
            assert (await r.json())["released"] is False
    finally:
        await api.stop()
        await backend.stop()


async def test_lock_permits_must_be_integer_over_http(tmp_path):
    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx())
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            r = await s.post(
                api.base_url + "/v1/lock/acquire",
                json={"name": "l", "permits": "many"},
            )
            assert r.status == 400
            assert "permits must be an integer" in (await r.json())["error"]
    finally:
        await api.stop()
        await backend.stop()


async def test_backend_gone_is_503_over_http(tmp_path):
    api = JobStateAPI(
        lambda: None,
        host="h",
        base_holder="h#proc",
        config={"maxValueBytes": 0, "maxArtifactBytes": 0},
    )
    await api.start()
    api.register_run(_ctx())
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            r = await s.get(api.base_url + "/v1/kv/get?key=k")
            assert r.status == 503
            assert "unavailable" in (await r.json())["error"]
    finally:
        await api.stop()


async def test_backend_document_error_is_503_over_http(tmp_path, monkeypatch):
    from cronstable.state import _DocumentUnreadable

    api, backend = await _make_api(tmp_path)
    api.register_run(_ctx())

    async def broken(*args, **kwargs):
        raise _DocumentUnreadable("kv/x", "corrupt")

    monkeypatch.setattr(jobapi.jobstate, "kv_get", broken)
    try:
        async with aiohttp.ClientSession(headers=_auth()) as s:
            r = await s.get(api.base_url + "/v1/kv/get?key=k")
            assert r.status == 503
    finally:
        await api.stop()
        await backend.stop()


# ---------------------------------------------------------------------------
# auth + bind-target parsing (direct)
# ---------------------------------------------------------------------------


class _HeaderReq:
    def __init__(self, header):
        self.headers = {"Authorization": header}


def test_auth_surrogate_token_is_401(tmp_path):
    api = JobStateAPI(
        lambda: None,
        host="h",
        base_holder="h#proc",
        config={},
    )
    # a raw-bytes header decodes to surrogates, which cannot re-encode; that
    # must be a clean 401, not a UnicodeEncodeError 500.
    with pytest.raises(web.HTTPUnauthorized):
        api._run(_HeaderReq("Bearer t\udc80k"))


def _bind_of(listen):
    api = JobStateAPI(
        lambda: None,
        host="h",
        base_holder="h#proc",
        config={"listen": listen} if listen else {},
    )
    return api._bind_target()


def test_bind_target_forms():
    assert _bind_of(None) == ("127.0.0.1", 0)
    assert _bind_of("127.0.0.1:9123") == ("127.0.0.1", 9123)
    assert _bind_of("http://10.0.0.5:8099") == ("10.0.0.5", 8099)
    assert _bind_of("http://10.0.0.5") == ("10.0.0.5", 0)
