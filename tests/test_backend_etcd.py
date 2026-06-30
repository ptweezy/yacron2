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
    _MIN_USABLE_TTL,
    EtcdBackend,
    _b64,
    _b64decode,
    build_campaign_txn,
    build_reboot_ran_cas_txn,
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


def test_renew_cadence_fits_lease_window():
    # The etcd analogue of the Kubernetes renew+retry<duration invariant: a
    # round (round_deadline) plus the inter-round sleep (renew_period) must fit
    # inside the effective lease window minus the 1s clock-skew margin, so a
    # holder cannot lapse out of its own lease between rounds (which would
    # collapse Leader at-most-once to at-most-zero and double-run
    # PreferLeader). Holds by construction across the supported ttl range.
    for ttl in (3, 5, 15, 60, 600):
        b = _backend()
        b._effective_ttl = ttl
        assert b.round_deadline + b.renew_period <= ttl - 1 + 1e-9
        # F09: a round's worst-case sequential lease POSTs must still fit the
        # deadline at EVERY ttl (so a single slow/half-open endpoint cannot
        # make every round overrun). With no per-request floor this holds even
        # at the minimum ttl=3; assert it unconditionally -- the old
        # `request_timeout > 0.5` guard skipped exactly the small-ttl boundary
        # a reintroduced floor would break (the regression the no-floor fix is
        # for).
        assert b.request_timeout * 5 <= b.round_deadline + 1e-9
        # never tighter than connectTimeout asked for
        assert b.request_timeout <= b.connect_timeout


def test_renew_cadence_tracks_narrowed_effective_ttl():
    # etcd may grant a shorter lease than requested; the cadence is derived
    # from the EFFECTIVE ttl (not the configured one), so the round budget
    # tightens with the real window rather than overrunning it.
    b = _backend()  # ttl 15
    assert b.renew_period == 5.0 and b.round_deadline == 9.0
    b._effective_ttl = 6
    assert b.renew_period == 2.0
    assert b.round_deadline == 3.0  # 6 - 2 - 1


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


def test_apply_round_lost_with_unparseable_holder_fences_closed():
    # Defensive: a lost campaign (won=False) whose holder could not be parsed
    # (holder=None -- only via a non-conformant gateway that drops the
    # failure-branch range value) must NOT report leader_name()=None while
    # quorate. None there is read by is_available_leader() as "holder unknown
    # -> run anyway", double-running PreferLeader on every quorate replica
    # alongside the real holder. _apply_round substitutes a non-None sentinel
    # so a follower defers instead.
    b = _backend()
    b._apply_round(None, False, NOW)  # contacted, lost, holder unparseable
    assert b.is_quorate() is True
    assert b.is_leader() is False
    assert b.leader_name() is not None  # sentinel, not None
    assert b.is_available_leader() is False  # so a follower defers


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


async def test_renew_once_anchors_deadline_to_presend_lower_bound(monkeypatch):
    # H1: the leadership FENCE must be anchored to a monotonic instant captured
    # BEFORE the lease-renewing POST (grant/keepalive) is SENT -- a guaranteed
    # LOWER BOUND on when etcd reset the lease's TTL. etcd resets the TTL when
    # it *processes* the request (at or after we send it), so the response
    # landing is only an UPPER bound: anchoring to it over-extends the fence by
    # the request round-trip, and on a slow-but-reachable etcd (RTT > the 1s
    # skew margin) is_leader() outlives the server lease while a second node
    # wins the freed key (two leaders run a Leader job). The campaign read's
    # latency must likewise be excluded.
    clock = [100.0]
    monkeypatch.setattr("yacron2.backends.etcd._monotonic", lambda: clock[0])
    b = _backend()  # ttl 15
    grant_latency = 2.0  # a slow grant RTT, larger than the 1s skew margin
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
    pre_send = clock[0]  # 100: lower bound on the server's TTL reset
    await b._renew_once()
    assert b.is_leader() is True
    # anchored to the pre-send lower bound (100 + 15 - 1 = 114), excluding BOTH
    # the grant RTT and the campaign latency, so the local fence can never
    # outlive the server lease no matter how slow the round.
    assert b._lease_deadline_mono == pre_send + 15 - 1
    grant_landing = 100.0 + grant_latency  # 102: only an UPPER bound on reset
    round_end = grant_landing + campaign_latency  # 106
    # specifically NOT the grant landing (the old over-extension -- the H1
    # double-run hole) and NOT round end (the even larger campaign-inclusive
    # hole).
    assert b._lease_deadline_mono != grant_landing + 15 - 1
    assert b._lease_deadline_mono != round_end + 15 - 1


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


async def test_renew_once_keepalive_anchors_fence_to_presend(monkeypatch):
    # TEST-ETCD-KEEPALIVE-FENCE: the grant-path fence anchor is tested above,
    # but steady state renews via KEEPALIVE -- the dominant path, which fires
    # every round after the first. Its lease_mono is sampled BEFORE the
    # keepalive POST (a lower bound on the server's TTL reset); a regression
    # sampling it AFTER the response would push is_leader() past the server
    # lease on a slow keepalive RTT (RTT > the skew margin -> two leaders). The
    # narrows-then-regrants test pins the clock so it cannot catch that; this
    # injects keepalive latency and asserts the fence excludes it.
    clock = [100.0]
    monkeypatch.setattr("yacron2.backends.etcd._monotonic", lambda: clock[0])
    b = _backend()  # ttl 15
    keepalive_latency = 3.0  # a slow keepalive RTT, larger than the 1s skew

    async def fake_post(path, body, *, allow_reauth=True):
        if path == "/v3/lease/grant":
            return {"ID": "1", "TTL": "15"}
        if path == "/v3/lease/keepalive":
            clock[0] += keepalive_latency  # the keepalive POST takes time
            return {"result": {"TTL": "15"}}
        if path == "/v3/kv/txn":
            return {"succeeded": True}
        if path == "/v3/kv/range":
            return {"kvs": []}
        return {}

    monkeypatch.setattr(b, "_post", fake_post)
    # round 1 grants + campaigns (establishes the lease; no keepalive yet)
    await b._renew_once()
    # round 2 renews via KEEPALIVE; sample the pre-keepalive monotonic instant
    pre_keepalive = clock[0]
    await b._renew_once()
    assert b.is_leader() is True
    # anchored to the pre-send lower bound, NOT the keepalive landing
    assert b._lease_deadline_mono == pre_keepalive + 15 - 1
    keepalive_landing = pre_keepalive + keepalive_latency
    assert b._lease_deadline_mono != keepalive_landing + 15 - 1


async def test_sync_reboot_ran_swallows_wrong_shape_response(monkeypatch):
    # ETCD-SHAPE-CRASH: a non-conformant gateway can answer a 200 whose body is
    # the wrong SHAPE (here a JSON list for /v3/kv/range). _post's dict guard
    # turns a top-level non-dict into a ClientError, but a nested non-dict can
    # still raise AttributeError in a parser. Reached from start()'s
    # initial round and cron's eager reboot-ran persist (both OUTSIDE the run
    # loop's broad guard), so _sync_reboot_ran must SWALLOW it, not let it
    # escape and abort manager start / mislog as an internal bug.
    b = _backend()
    b._reboot_ran_local = {"mine"}
    b._reboot_ran_local_job_set_id = "v1:job"

    async def fake_post(path, body, *, allow_reauth=True):
        if path == "/v3/kv/range":
            return ["not", "a", "dict"]  # nested wrong shape -> kvs.get() boom
        return {}

    monkeypatch.setattr(b, "_post", fake_post)
    # must not raise (the AttributeError is caught and logged at debug)
    await b._sync_reboot_ran()


# --- F03: effective-ttl narrowing is safe (honoured, never inflated) -------


def test_narrow_effective_ttl_honours_server_value_without_inflating():
    # F03: a server-granted ttl below the configured one is honoured for the
    # fence and NOT floored back up to the config minimum -- inflating it would
    # keep is_leader() true past the real (short) server lease (two leaders).
    b = _backend()  # configured ttl 15
    b._narrow_effective_ttl(6)
    assert b._effective_ttl == 6
    assert b._ttl_collapsed is False


def test_narrow_effective_ttl_below_min_is_honoured_not_inflated(caplog):
    # F03: below _MIN_USABLE_TTL the leader window collapses; the fence is kept
    # at the small (safe) value -- Leader fails closed -- rather than inflated
    # to the configured minimum (which would risk two leaders). Warns, not
    # silent.
    import logging

    b = _backend()
    with caplog.at_level(logging.WARNING, logger="yacron2.backends.etcd"):
        b._narrow_effective_ttl(1)
    assert b._effective_ttl == 1  # honoured, NOT inflated to _MIN_USABLE_TTL
    assert b._effective_ttl < _MIN_USABLE_TTL
    assert b._ttl_collapsed is True
    assert any(
        "usable leader window" in r.getMessage() for r in caplog.records
    )


def test_narrow_effective_ttl_warns_once_then_recovers(caplog):
    # F03: the collapse warning is per-transition (not every renew round); a
    # later recovery to a usable ttl clears the collapsed flag.
    import logging

    b = _backend()
    with caplog.at_level(logging.WARNING, logger="yacron2.backends.etcd"):
        b._narrow_effective_ttl(1)
        b._narrow_effective_ttl(2)  # still collapsed -> no second warning
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    b._narrow_effective_ttl(15)  # recovered
    assert b._ttl_collapsed is False
    assert b._effective_ttl == 15


def test_etcd_is_self_demoted_holder():
    # F14: raw win flag set but the monotonic fence lapsed = the self-demotion
    # window (still quorate, still names self). is_available_leader uses this
    # so the lapsed-but-quorate former holder keeps running PreferLeader.
    b = _backend()
    assert b._is_self_demoted_holder() is False  # fresh: never won
    b._is_leader = True
    b._lease_deadline_mono = time.monotonic() - 1  # fence lapsed
    assert b.is_leader() is False
    assert b._is_self_demoted_holder() is True
    b._lease_deadline_mono = time.monotonic() + 100  # fence valid: real leader
    assert b._is_self_demoted_holder() is False
    b._is_leader = False
    assert b._is_self_demoted_holder() is False


# --- F02/F12: the @reboot-ran compare-and-swap (union + revision spelling) --


def test_build_reboot_ran_cas_txn_structure():
    # F02/F12: structurally untested before. Compares MOD == the revision read
    # this round and PUTs the merged value on success; the re-read on a lost
    # CAS is done by the caller's loop, so the txn's failure branch is empty.
    txn = build_reboot_ran_cas_txn(
        "yacron2/leader/reboot-ran", "payload", "42"
    )
    key = _b64("yacron2/leader/reboot-ran")
    assert txn["compare"][0] == {
        "key": key,
        "result": "EQUAL",
        "target": "MOD",
        "mod_revision": "42",
    }
    assert txn["success"][0]["requestPut"] == {
        "key": key,
        "value": _b64("payload"),
    }
    assert txn["failure"] == []


async def test_cas_write_reboot_ran_unions_on_contention(monkeypatch):
    # F02 (TEST-ETCD-CAS): two concurrent writers must UNION their @reboot-ran
    # marks, not last-writer-wins clobber. This drives the retry against a
    # STATEFUL fake etcd that HONOURS the txn compare (a CAS submitted against
    # a stale mod_revision FAILS); a concurrent writer bumps the revision
    # once between our first read and our first txn. So the retry only wins by
    # RE-READING the new revision and re-merging -- a regression that hoisted
    # the read out of the loop, or reused the stale revision, would contend out
    # and persist nothing (re-running a deferred one-shot on failover).
    # The previous version of this test let the txn succeed purely on attempt
    # count and returned a static revision, so it passed under that bug.
    from yacron2.leadership import decode_reboot_ran, encode_reboot_ran

    b = _backend()  # get_job_set_id -> "v1:job"
    b._reboot_ran_local = {"mine"}
    b._reboot_ran_local_job_set_id = "v1:job"

    # a single key with a value + mod_revision the txn compare is checked
    # against; reads["n"] counts range re-reads, compares records each txn's
    # submitted compare revision.
    server = {"value": encode_reboot_ran("v1:job", {"theirs"}), "rev": 5}
    reads = {"n": 0}
    compares = []

    async def fake_post(path, body, *, allow_reauth=True):
        if path == "/v3/kv/range":
            reads["n"] += 1
            resp = {
                "kvs": [
                    {
                        "value": _b64(server["value"]),
                        "mod_revision": str(server["rev"]),
                    }
                ]
            }
            if reads["n"] == 1:
                # a concurrent writer moves the key AFTER our first read but
                # BEFORE our first txn lands: it adds its own mark "other" and
                # bumps the revision, so our first CAS (rev 5) must fail
                # and we must re-read rev 6.
                server["value"] = encode_reboot_ran(
                    "v1:job", {"theirs", "other"}
                )
                server["rev"] = 6
            return resp
        if path == "/v3/kv/txn":
            cmp_rev = body["compare"][0]["mod_revision"]
            compares.append(cmp_rev)
            if cmp_rev == str(server["rev"]):
                server["value"] = _b64decode(
                    body["success"][0]["requestPut"]["value"]
                )
                server["rev"] += 1
                return {"succeeded": True}
            return {"succeeded": False}  # stale revision -> CAS rejected
        return {}

    monkeypatch.setattr(b, "_post", fake_post)
    await b._cas_write_reboot_ran()

    # the first CAS used the stale revision and was REJECTED; the retry RE-READ
    # the new revision and won.
    assert reads["n"] >= 2  # a re-read happened (not hoisted out of the loop)
    assert compares[0] == "5" and compares[-1] == "6"  # used the NEW revision
    _jsid, jobs = decode_reboot_ran(server["value"])
    # all three marks survive: ours, the prior holder's, and the concurrent
    # writer's -- a true union, no lost update.
    assert jobs == {"mine", "theirs", "other"}


async def test_cas_write_reboot_ran_reads_camelcase_mod_revision(monkeypatch):
    # F12: the etcd gRPC-gateway may marshal mod_revision as camelCase. Reading
    # only snake_case would fall back to "0", and the MOD==0 compare against an
    # EXISTING key never succeeds -> the mark is never persisted -> a deferred
    # one-shot re-runs after failover.
    from yacron2.leadership import encode_reboot_ran

    b = _backend()
    b._reboot_ran_local = {"mine"}
    b._reboot_ran_local_job_set_id = "v1:job"
    compares = []

    async def fake_post(path, body, *, allow_reauth=True):
        if path == "/v3/kv/range":
            return {
                "kvs": [
                    {
                        "value": _b64(encode_reboot_ran("v1:job", set())),
                        "modRevision": "9",  # camelCase ONLY
                    }
                ]
            }
        compares.append(body["compare"][0]["mod_revision"])
        return {"succeeded": True}

    monkeypatch.setattr(b, "_post", fake_post)
    await b._cas_write_reboot_ran()
    assert compares == ["9"]  # the camelCase modRevision was read, not "0"


def test_holder_from_txn_response_malformed_base64_raises_valueerror():
    # F13: a malformed base64 value (a non-conformant gateway) makes the holder
    # decode raise binascii.Error (a ValueError subclass). start()'s initial
    # round and the renew loop both catch ValueError so it cannot wedge the
    # manager start; pin that the decode does raise ValueError (what those
    # catches rely on).
    resp = {
        "succeeded": False,
        "responses": [{"response_range": {"kvs": [{"value": "Z"}]}}],
    }
    with pytest.raises(ValueError):
        holder_from_txn_response(resp, "node-a")
