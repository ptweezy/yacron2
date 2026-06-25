"""Pure-helper and local-state tests for the etcd leadership backend.

No etcd server and no crypto: the HTTP glue is ``# pragma: no cover`` and
exercised only by the Docker integration tests.
"""

import base64
import datetime

from yacron2.backends.etcd import (
    EtcdBackend,
    _b64,
    _b64decode,
    build_campaign_txn,
    holder_from_txn_response,
    is_leader_from_lock_state,
    lease_id_from_grant,
    lease_ttl_from_keepalive,
)
from yacron2.config import parse_config_string

NOW = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)


def _backend(extra=""):
    yaml = (
        "cluster:\n"
        "  backend: etcd\n"
        "  nodeName: node-a\n"
        "  connectTimeout: 4\n"
        "  etcd:\n"
        "    endpoints:\n"
        "      - http://127.0.0.1:2379\n"
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


# --- is_leader_from_lock_state --------------------------------------------


def test_is_leader_from_lock_state():
    assert is_leader_from_lock_state("node-a", "node-a", True) is True
    assert is_leader_from_lock_state("node-a", "node-a", False) is False
    assert is_leader_from_lock_state("node-b", "node-a", True) is False
    assert is_leader_from_lock_state(None, "node-a", True) is False


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
    b = _backend()
    assert b.is_leader() is False
    b._is_leader = True
    b._lease_deadline = NOW  # in the past
    assert b.is_leader() is False  # self-demote without a keepalive
    b._lease_deadline = _utc_now_plus(100)
    assert b.is_leader() is True


def test_is_quorate_tracks_fresh_contact():
    b = _backend()
    assert b.is_quorate() is False
    b._last_contact = _utc_now_plus(0)
    assert b.is_quorate() is True
    b._last_contact = _utc_now_plus(-(b.ttl + 5))
    assert b.is_quorate() is False


def test_leader_name_none_when_stale():
    b = _backend()
    b._holder = "node-b"
    assert b.leader_name() is None
    b._last_contact = _utc_now_plus(0)
    assert b.leader_name() == "node-b"


def test_apply_round_win():
    b = _backend()
    b._apply_round("node-a", True, NOW)
    assert b._is_leader is True
    assert b._holder == "node-a"
    assert b._lease_deadline == b._leader_deadline(NOW)
    assert b._last_contact == NOW


def test_apply_round_follower():
    b = _backend()
    b._apply_round("node-b", True, NOW)
    assert b._is_leader is False
    assert b._holder == "node-b"
    assert b._last_contact == NOW


def test_apply_round_dead_lease_not_leader():
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
