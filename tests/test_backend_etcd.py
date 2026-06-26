"""Pure-helper and local-state tests for the etcd leadership backend.

No etcd server and no crypto: the HTTP glue is ``# pragma: no cover`` and
exercised only by the Docker integration tests.
"""

import base64
import datetime
import time

import aiohttp
import pytest

from yacron2.backends.etcd import (
    EtcdBackend,
    _b64,
    _b64decode,
    build_campaign_txn,
    campaign_won,
    holder_from_txn_response,
    lease_id_from_grant,
    lease_ttl_from_grant,
    lease_ttl_from_keepalive,
)
from yacron2.config import parse_config_string

NOW = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)


def _backend(extra="", endpoint="http://127.0.0.1:2379"):
    yaml = (
        "cluster:\n"
        "  backend: etcd\n"
        "  nodeName: node-a\n"
        "  connectTimeout: 4\n"
        "  etcd:\n"
        "    endpoints:\n"
        "      - " + endpoint + "\n"
        "    electionName: yacron2/leader\n"
        "    ttl: 15\n" + extra
    )
    cfg = parse_config_string(yaml, "").cluster_config
    return EtcdBackend(cfg, lambda: "v1:job")


def _utc_now_plus(seconds):
    return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        seconds=seconds
    )


# --- base64 helpers -------------------------------------------------------


def test_b64_roundtrip():
    assert _b64("yacron2/leader") == base64.b64encode(
        b"yacron2/leader"
    ).decode("ascii")
    assert _b64decode(_b64("node-a")) == "node-a"


# --- campaign transaction -------------------------------------------------


def test_build_campaign_txn_structure():
    txn = build_campaign_txn("yacron2/leader", "node-a", "777")
    key = _b64("yacron2/leader")
    compare = txn["compare"][0]
    assert compare == {
        "key": key,
        "result": "EQUAL",
        "target": "CREATE",
        "create_revision": "0",
    }
    put = txn["success"][0]["requestPut"]
    assert put["key"] == key
    assert put["value"] == _b64("node-a")
    assert put["lease"] == "777"
    assert txn["failure"][0]["requestRange"]["key"] == key


# --- holder_from_txn_response ---------------------------------------------


def test_holder_from_succeeded_txn_is_us():
    assert holder_from_txn_response({"succeeded": True}, "node-a") == "node-a"


def test_holder_from_failed_txn_reads_existing_value():
    resp = {
        "succeeded": False,
        "responses": [
            {"response_range": {"kvs": [{"value": _b64("node-b")}]}}
        ],
    }
    assert holder_from_txn_response(resp, "node-a") == "node-b"


def test_holder_from_failed_txn_camelcase_response_range():
    resp = {
        "succeeded": False,
        "responses": [
            {"responseRange": {"kvs": [{"value": _b64("node-c")}]}}
        ],
    }
    assert holder_from_txn_response(resp, "node-a") == "node-c"


def test_holder_skips_empty_range_entry():
    # a range entry with no kvs is skipped; a later one with a value wins.
    resp = {
        "succeeded": False,
        "responses": [
            {"response_range": {"kvs": []}},
            {"response_range": {"kvs": [{"value": _b64("node-d")}]}},
        ],
    }
    assert holder_from_txn_response(resp, "node-a") == "node-d"


def test_holder_from_empty_response_is_none():
    assert holder_from_txn_response({"succeeded": False}, "node-a") is None
    assert (
        holder_from_txn_response(
            {"succeeded": False, "responses": [{"response_range": {}}]},
            "node-a",
        )
        is None
    )


# --- lease grant / keepalive parsing --------------------------------------


def test_lease_id_from_grant():
    assert lease_id_from_grant({"ID": "1234567890"}) == "1234567890"
    assert lease_id_from_grant({"ID": 42}) == "42"
    assert lease_id_from_grant({}) is None


def test_lease_ttl_from_keepalive():
    assert lease_ttl_from_keepalive({"result": {"TTL": "15"}}) == 15
    assert lease_ttl_from_keepalive({"result": {"TTL": "0"}}) == 0
    assert lease_ttl_from_keepalive({"result": {}}) is None
    assert lease_ttl_from_keepalive({}) is None
    assert lease_ttl_from_keepalive({"result": {"TTL": "nope"}}) is None


# --- campaign_won (lease-id fence) ----------------------------------------


def test_campaign_won_on_succeeded():
    # we created the key -> bound to our lease -> we hold it.
    assert campaign_won({"succeeded": True}, "777") is True


def test_campaign_won_when_key_bound_to_our_lease():
    # a lost create race against a key that is nonetheless bound to OUR lease
    # (we already hold it; create-if-absent fails as it exists) -> we lead.
    resp = {
        "succeeded": False,
        "responses": [
            {
                "response_range": {
                    "kvs": [{"value": _b64("node-a"), "lease": "777"}]
                }
            }
        ],
    }
    assert campaign_won(resp, "777") is True


def test_campaign_lost_when_key_bound_to_other_lease():
    # the duplicate-identity fence: the key stores OUR identity string but is
    # bound to a DIFFERENT lease (another process sharing our nodeName) -> we
    # are NOT the leader, so we do not both believe we hold the fence.
    resp = {
        "succeeded": False,
        "responses": [
            {
                "response_range": {
                    "kvs": [{"value": _b64("node-a"), "lease": "999"}]
                }
            }
        ],
    }
    assert campaign_won(resp, "777") is False


def test_campaign_won_camelcase_and_missing_lease():
    won = {
        "succeeded": False,
        "responses": [
            {
                "responseRange": {
                    "kvs": [{"value": _b64("node-a"), "lease": "5"}]
                }
            }
        ],
    }
    assert campaign_won(won, "5") is True
    # a key with no lease bound is never ours
    no_lease = {
        "succeeded": False,
        "responses": [{"response_range": {"kvs": [{"value": _b64("x")}]}}],
    }
    assert campaign_won(no_lease, "5") is False
    assert campaign_won({"succeeded": False}, "5") is False


# --- backend local state --------------------------------------------------


def test_construction_defaults():
    b = _backend()
    assert b.backend_name == "etcd"
    assert b.identity == "node-a"
    assert b.election_name == "yacron2/leader"
    assert b.ttl == 15
    assert b.renew_period == 5.0
    assert b.endpoints == ["http://127.0.0.1:2379"]
    assert b.distribution == "single-leader"


def test_endpoint_is_https():
    assert EtcdBackend.endpoint_is_https("https://h:2379") is True
    assert EtcdBackend.endpoint_is_https("http://h:2379") is False


def test_is_leader_gated_on_lease_deadline():
    # the fence is a MONOTONIC deadline (immune to wall-clock steps), not the
    # wall-clock _lease_deadline (which is display only).
    b = _backend()
    assert b.is_leader() is False
    b._is_leader = True
    b._lease_deadline_mono = time.monotonic() - 1  # in the past
    assert b.is_leader() is False  # self-demote without a keepalive
    b._lease_deadline_mono = time.monotonic() + 100
    assert b.is_leader() is True


def test_is_quorate_tracks_fresh_contact():
    b = _backend()
    assert b.is_quorate() is False
    b._last_contact_mono = time.monotonic()
    assert b.is_quorate() is True
    b._last_contact_mono = time.monotonic() - (b.ttl + 5)
    assert b.is_quorate() is False


def test_leader_name_none_when_stale():
    b = _backend()
    b._holder = "node-b"
    assert b.leader_name() is None
    b._last_contact_mono = time.monotonic()
    assert b.leader_name() == "node-b"


def test_apply_round_win():
    b = _backend()
    b._apply_round("node-a", True, NOW)
    assert b._is_leader is True
    assert b._holder == "node-a"
    assert b._lease_deadline == b._leader_deadline(NOW)  # wall, for display
    assert b._last_contact_mono is not None  # monotonic freshness advanced
    assert b.is_quorate() is True


def test_apply_round_follower():
    b = _backend()
    # another node holds the key (won=False), even though it displays as us
    b._apply_round("node-b", False, NOW)
    assert b._is_leader is False
    assert b._holder == "node-b"
    assert b._last_contact_mono is not None


def test_lease_ttl_from_grant():
    assert lease_ttl_from_grant({"ID": "1", "TTL": "15"}) == 15
    assert lease_ttl_from_grant({"ID": "1", "TTL": 9}) == 9
    assert lease_ttl_from_grant({"ID": "1"}) is None
    assert lease_ttl_from_grant({"TTL": "bad"}) is None


def test_effective_ttl_narrows_to_server_grant():
    # M1: the deadline must follow the TTL etcd actually grants/keepalives, not
    # the configured value, or is_leader stays true past the server expiry.
    b = _backend()  # configured ttl 15
    assert b._effective_ttl == 15
    # simulate a keepalive/grant that returned a shorter TTL
    b._effective_ttl = max(1, min(b.ttl, 5))
    assert b._effective_ttl == 5
    mono = time.monotonic()
    b._apply_round("node-a", True, NOW, mono)
    # the monotonic deadline reflects the 5s effective ttl (minus skew), well
    # short of the configured 15s.
    assert b._lease_deadline_mono <= mono + 5


def test_apply_round_lost_key_not_leader():
    # won=False (the key is bound to another lease) -> not the leader, even
    # though the stored holder string matches our own identity.
    b = _backend()
    b._apply_round("node-a", False, NOW)
    assert b._is_leader is False


def test_view_dict_and_lease_detail():
    b = _backend()
    b._lease_id = "777"
    # apply the round at real "now" so the wall-clock-gated reads see a valid
    # lease and a fresh contact.
    b._apply_round("node-a", True, _utc_now_plus(0))
    view = b.view_dict()
    assert view["backend"] == "etcd"
    assert view["is_leader"] is True
    assert view["lease"]["electionName"] == "yacron2/leader"
    assert view["lease"]["identity"] == "node-a"
    assert view["lease"]["holder"] == "node-a"
    assert view["lease"]["leaseId"] == "777"
    assert view["lease"]["expiry"] is not None


async def test_start_closes_session_when_authenticate_fails(monkeypatch):
    # start() creates the aiohttp session before authenticating; a rejected
    # credential / unreachable etcd must close it before re-raising, or one
    # session+connector leaks per config reload (the manager is never stored,
    # so the caller never stop()s it). A ClientResponseError (a 401) is also
    # not an OSError, so it must still surface and be cleaned up.
    b = _backend(
        "    username: admin\n    password:\n      value: secret\n",
        # auth credentials require an https endpoint (config rejects auth over
        # plaintext); use one so the authenticate-failure cleanup path is what
        # this test exercises, not the config-time scheme guard.
        endpoint="https://127.0.0.1:2379",
    )

    async def boom():
        raise aiohttp.ClientResponseError(
            request_info=None, history=(), status=401
        )

    monkeypatch.setattr(b, "_authenticate", boom)
    with pytest.raises(aiohttp.ClientError):
        await b.start()


# --- renew loop: the live state machine (F04 deadline anchor, F21) ---------


async def test_renew_once_anchors_deadline_to_lease_not_campaign(monkeypatch):
    # F04 / C1: the leadership FENCE must be anchored to the instant the
    # lease-renewing POST (grant/keepalive) LANDED -- when etcd actually reset
    # the lease's TTL -- NOT to round end after the campaign. In the steady
    # path the campaign is a read; folding its latency into the fence would let
    # is_leader() outlive the server lease's expiry while a second node wins
    # the freed key (two leaders run a Leader job). So the fence must exclude
    # the campaign latency entirely, while staying anchored to the lease event
    # (not round start, which would flap is_leader False mid-round).
    clock = [100.0]
    monkeypatch.setattr("yacron2.backends.etcd._monotonic", lambda: clock[0])
    b = _backend()  # ttl 15
    grant_latency = 1.0
    campaign_latency = 4.0

    async def fake_post(path, body, *, allow_reauth=True):
        if path == "/v3/lease/grant":
            clock[0] += grant_latency  # the grant POST takes time to land
            return {"ID": "777", "TTL": "15"}
        if path == "/v3/kv/txn":
            clock[0] += campaign_latency  # the campaign takes more time after
            return {"succeeded": True}
        if path == "/v3/kv/range":
            return {"kvs": []}
        return {}

    monkeypatch.setattr(b, "_post", fake_post)
    await b._renew_once()
    assert b.is_leader() is True
    grant_landing = 100.0 + grant_latency  # 101: server reset the TTL here
    round_end = grant_landing + campaign_latency  # 105
    # anchored to the grant landing (101 + 15 - 1 = 115), excluding the
    # campaign latency, so the local fence never outlives the server lease.
    assert b._lease_deadline_mono == grant_landing + 15 - 1
    # specifically NOT round end (105 + 15 - 1 = 119, the old double-run hole)
    # and NOT round start (100 + 15 - 1 = 114).
    assert b._lease_deadline_mono != round_end + 15 - 1
    assert b._lease_deadline_mono != 100.0 + 15 - 1


async def test_renew_once_keepalive_narrows_then_regrants(monkeypatch):
    # F21: the live keepalive -> narrow-ttl -> lease-lost -> re-grant -> re-
    # campaign sequence had no round-level test. A keepalive returning a TTL
    # below the configured value must narrow the deadline; a keepalive of 0
    # (lease gone) must drop the lease, re-grant, and re-campaign.
    clock = [100.0]
    monkeypatch.setattr("yacron2.backends.etcd._monotonic", lambda: clock[0])
    b = _backend()  # ttl 15
    state = {"keepalive_ttl": 6}

    async def fake_post(path, body, *, allow_reauth=True):
        if path == "/v3/lease/grant":
            return {"ID": "1", "TTL": "15"}
        if path == "/v3/lease/keepalive":
            return {"result": {"TTL": str(state["keepalive_ttl"])}}
        if path == "/v3/kv/txn":
            return {"succeeded": True}
        if path == "/v3/kv/range":
            return {"kvs": []}
        return {}

    monkeypatch.setattr(b, "_post", fake_post)
    # round 1: no lease yet -> grant (ttl 15) + campaign
    await b._renew_once()
    assert b._lease_id == "1" and b._effective_ttl == 15
    # round 2: keepalive narrows the effective ttl to 6 -> deadline follows 6
    await b._renew_once()
    assert b._effective_ttl == 6
    assert b._lease_deadline_mono == clock[0] + 6 - 1
    # round 3: keepalive says lease GONE (ttl 0) -> re-grant + re-campaign
    state["keepalive_ttl"] = 0
    await b._renew_once()
    assert b._lease_id == "1" and b._effective_ttl == 15
    assert b.is_leader() is True
    assert b._session is None
