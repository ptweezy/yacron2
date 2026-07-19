"""Job-state API edges: lock-manager failure paths, body/auth guards.

``test_state_job_api.py`` covers the happy paths over real HTTP; this file
adds the degraded ones -- a vanished backend, probe/renewal timeouts, lease
takeover, junk request bodies -- driving :class:`JobLockManager` directly
with a scripted backend stub (real sleeps replaced per-test) and the HTTP
guards through the same real-server harness.
"""

import asyncio

import aiohttp
import pytest
from aiohttp import web

from cronstable import jobapi
from cronstable.jobapi import JobLockManager, JobStateAPI
from cronstable.jobstate import JobStateError
from cronstable.state import Lease
from tests.test_state import _backend
from tests.test_state_job_api import _auth, _ctx, _make_api


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
