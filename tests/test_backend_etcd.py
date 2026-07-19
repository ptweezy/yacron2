"""Pure-helper and local-state tests for the etcd leadership backend.

No etcd server and no crypto: the HTTP glue is ``# pragma: no cover`` and
exercised only by the Docker integration tests.
"""

import base64
import datetime
import time

import aiohttp
import pytest

from cronstable.backends.etcd import (
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
from cronstable.config import parse_config_string

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
        "    electionName: cronstable/leader\n"
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
    assert _b64("cronstable/leader") == base64.b64encode(
        b"cronstable/leader"
    ).decode("ascii")
    assert _b64decode(_b64("node-a")) == "node-a"


# --- campaign transaction -------------------------------------------------


def test_build_campaign_txn_structure():
    txn = build_campaign_txn("cronstable/leader", "node-a", "777")
    key = _b64("cronstable/leader")
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
    assert b.election_name == "cronstable/leader"
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


def test_tls_files_changed_detects_in_place_rotation(tmp_path):
    # F-CERT-ROTATE: the etcd SSLContext is built once at start() and never
    # reloaded, so an in-place client-cert/CA rotation (same paths, new bytes
    # from cert-manager / Vault) is invisible unless tls_files_changed()
    # reports it, as gossip and kubernetes already do. Without it the
    # fleet silently loses leadership once the old client cert expires.
    ca = tmp_path / "ca.crt"
    cert = tmp_path / "client.crt"
    key = tmp_path / "client.key"
    for f in (ca, cert, key):
        f.write_text("x")
    extra = (
        "    tls:\n"
        "      ca: " + str(ca).replace("\\", "/") + "\n"
        "      cert: " + str(cert).replace("\\", "/") + "\n"
        "      key: " + str(key).replace("\\", "/") + "\n"
    )
    b = _backend(extra=extra, endpoint="https://127.0.0.1:2379")
    b._record_tls_files()  # snapshot, as start() does after _build_ssl
    assert b.tls_files_changed() is False
    # an in-place rotation: same path, new (longer) bytes -> new size/mtime
    cert.write_text("new-client-cert-material-much-longer")
    assert b.tls_files_changed() is True


def test_tls_files_changed_false_for_plain_http():
    # plain-http endpoints load no client cert, so there is nothing on disk to
    # rotate and tls_files_changed stays False (the inherited lease default),
    # never spuriously rebuilding the backend.
    b = _backend()  # http endpoint, no tls material
    b._record_tls_files()
    assert b._tls_files == []
    assert b.tls_files_changed() is False


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


def test_is_leader_fenced_closed_on_known_lease_loss():
    # F-LEASE-LOST (unit): a keepalive that found the lease already gone sets
    # _lease_lost, which forces is_leader() False even while the monotonic
    # deadline is still in the future (etcd may have freed the key, so a second
    # node could win it). _is_self_demoted_holder() stays True so a never-skip
    # PreferLeader job keeps running on this former holder; _apply_round (the
    # single writer) clears the flag once a round re-establishes state.
    b = _backend()
    b._is_leader = True
    b._lease_deadline_mono = time.monotonic() + 100  # fence still ahead
    assert b.is_leader() is True
    b._lease_lost = True
    assert b.is_leader() is False  # fenced closed despite the live deadline
    assert b._is_self_demoted_holder() is True  # PreferLeader still runs here
    b._apply_round("node-a", True, NOW)  # a fresh round re-establishes state
    assert b._lease_lost is False
    assert b.is_leader() is True


def test_is_quorate_tracks_fresh_contact():
    # quorum freshness is a monotonic DEADLINE fixed at contact time (see
    # test_cadence_widening_does_not_resurrect_quorum for why), so staleness is
    # simply the deadline lapsing.
    b = _backend()
    assert b.is_quorate() is False
    b._quorum_deadline_mono = time.monotonic() + b.ttl
    assert b.is_quorate() is True
    b._quorum_deadline_mono = time.monotonic() - 5
    assert b.is_quorate() is False


def test_leader_name_none_when_stale():
    b = _backend()
    b._holder = "node-b"
    assert b.leader_name() is None
    b._quorum_deadline_mono = time.monotonic() + b.ttl
    assert b.leader_name() == "node-b"


def test_apply_round_win():
    b = _backend()
    b._apply_round("node-a", True, NOW)
    assert b._is_leader is True
    assert b._holder == "node-a"
    assert b._lease_deadline == b._leader_deadline(NOW)  # wall, for display
    assert b._quorum_deadline_mono is not None  # monotonic freshness advanced
    assert b.is_quorate() is True


def test_apply_round_follower():
    b = _backend()
    # another node holds the key (won=False), even though it displays as us
    b._apply_round("node-b", False, NOW)
    assert b._is_leader is False
    assert b._holder == "node-b"
    assert b._quorum_deadline_mono is not None


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
    assert view["lease"]["electionName"] == "cronstable/leader"
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
    monkeypatch.setattr(
        "cronstable.backends.etcd._monotonic", lambda: clock[0]
    )
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
    monkeypatch.setattr(
        "cronstable.backends.etcd._monotonic", lambda: clock[0]
    )
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
    monkeypatch.setattr(
        "cronstable.backends.etcd._monotonic", lambda: clock[0]
    )
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


async def test_renew_once_lease_lost_then_regrant_fails_self_demotes(
    monkeypatch,
):
    # F-LEASE-LOST (round): when a keepalive reports the lease gone (ttl<=0)
    # and the re-grant then RAISES this round, the former holder must fence
    # is_leader() closed (the freed key may already be another node's) yet stay
    # _is_self_demoted_holder() True, so a never-skip PreferLeader job keeps
    # running on it this cycle instead of dropping to zero-run fleet-wide.
    # Previously the keepalive branch cleared _is_leader directly, which made
    # _is_self_demoted_holder() False -> the PreferLeader job ran nowhere.
    clock = [100.0]
    monkeypatch.setattr(
        "cronstable.backends.etcd._monotonic", lambda: clock[0]
    )
    b = _backend()  # ttl 15
    fail_grant = {"on": False}

    async def fake_post(path, body, *, allow_reauth=True):
        if path == "/v3/lease/grant":
            if fail_grant["on"]:
                raise aiohttp.ClientError("etcd unreachable")
            return {"ID": "1", "TTL": "15"}
        if path == "/v3/lease/keepalive":
            return {"result": {"TTL": "0"}}  # lease GONE
        if path == "/v3/kv/txn":
            return {"succeeded": True}
        if path == "/v3/kv/range":
            return {"kvs": []}
        return {}

    monkeypatch.setattr(b, "_post", fake_post)
    # round 1: grant + campaign -> we lead and are quorate
    await b._renew_once()
    assert b.is_leader() is True
    # round 2: keepalive says the lease is gone, and the re-grant then fails.
    fail_grant["on"] = True
    with pytest.raises(aiohttp.ClientError):
        await b._renew_once()
    assert b.is_leader() is False  # fenced closed: the freed key may be taken
    # ...but still the self-demoted former holder, still quorate from round 1,
    # so a PreferLeader job runs here this cycle (never-skip), not nowhere.
    assert b._is_self_demoted_holder() is True
    assert b.is_quorate() is True
    assert b.is_available_leader() is True


async def test_renew_once_recovers_collapsed_cadence_when_not_quorate(
    monkeypatch,
):
    # F-TTL-WEDGE: a once-narrowed effective ttl shrinks request_timeout, and
    # that tight per-POST budget can itself prevent the contact needed to widen
    # the ttl again -- a self-sustaining wedge off a since-recovered etcd.
    # While NOT quorate (fence already lapsed, no two-leaders risk) the cadence
    # widened back to the configured ttl at the START of the round, so the
    # reconnect POSTs run at the full timeout budget rather than the collapsed
    # ~0.2s a narrowed ttl=3 would give.
    clock = [100.0]
    monkeypatch.setattr(
        "cronstable.backends.etcd._monotonic", lambda: clock[0]
    )
    b = _backend()  # ttl 15
    # simulate a prior narrowing to the minimum AND lost contact (not quorate)
    b._effective_ttl = 3
    b._quorum_deadline_mono = 100.0 - 100  # long lapsed -> not quorate
    assert b.is_quorate() is False
    assert b.request_timeout < 0.3  # the collapsed per-POST budget at ttl 3
    budget_seen = []

    async def fake_post(path, body, *, allow_reauth=True):
        budget_seen.append(b.request_timeout)  # the budget the round runs at
        if path == "/v3/lease/grant":
            return {"ID": "1", "TTL": "15"}  # etcd has recovered
        if path == "/v3/kv/txn":
            return {"succeeded": True}
        if path == "/v3/kv/range":
            return {"kvs": []}
        return {}

    monkeypatch.setattr(b, "_post", fake_post)
    await b._renew_once()
    # the grant POST (the first of the round) ran at the widened budget, so a
    # real reconnect would not have timed out under the collapsed ~0.2s.
    assert budget_seen[0] >= 1.0
    assert b._effective_ttl == 15  # widened, then re-narrowed to granted 15
    assert b.is_leader() is True  # reconnected and re-acquired


async def test_cadence_widening_does_not_resurrect_quorum(monkeypatch):
    # F-QUORUM-WIDEN: is_quorate() must check a deadline FIXED at the last
    # successful contact (contact + the ttl in effect at THAT contact), not a
    # live last-contact + current-_effective_ttl computation. Otherwise the
    # not-quorate cadence widening at the top of _renew_once (narrowed server
    # ttl -> configured ttl, the F-TTL-WEDGE reconnect-budget fix above)
    # retroactively re-extends the freshness window around the OLD contact and
    # flips is_quorate() back to True with ZERO store contact -- re-deferring
    # never-skip PreferLeader jobs to a possibly-dead stale holder for up to
    # (configured - granted) extra seconds of a store outage, while /cluster
    # falsely reports the node quorate with that stale leader.
    clock = [1000.0]
    monkeypatch.setattr(
        "cronstable.backends.etcd._monotonic", lambda: clock[0]
    )
    b = _backend()  # configured ttl 15
    # a successful FOLLOWER round under a server-narrowed ttl of 5 (node-b
    # holds the key): the freshness window is 5s from this contact, not 15.
    b._narrow_effective_ttl(5)
    b._apply_round("node-b", False, NOW, clock[0])
    assert b.is_quorate() is True
    assert b.is_available_leader() is False  # defer to the live holder
    # 6s later the granted window has lapsed: not quorate, so the never-skip
    # PreferLeader gate correctly opens on this surviving follower.
    clock[0] += 6.0
    assert b.is_quorate() is False
    assert b.is_available_leader() is True
    # the renew loop then runs a round against a still-unreachable etcd: the
    # reconnect-budget widening runs first, then every POST fails.
    down = {"on": True}

    async def fake_post(path, body, *, allow_reauth=True):
        if down["on"]:
            raise aiohttp.ClientError("connection refused")
        if path == "/v3/lease/grant":
            return {"ID": "1", "TTL": "15"}
        if path == "/v3/kv/txn":
            return {"succeeded": True}
        if path == "/v3/kv/range":
            return {"kvs": []}
        return {}

    monkeypatch.setattr(b, "_post", fake_post)
    with pytest.raises(aiohttp.ClientError):
        await b._renew_once()
    assert b._effective_ttl == 15  # the cadence widening itself still ran
    # ...but with zero store contact quorum stays expired (no retroactive
    # resurrection), the stale holder is not reported, and the never-skip
    # gate stays open here instead of re-deferring to a possibly-dead node-b.
    assert b.is_quorate() is False
    assert b.leader_name() is None
    assert b.is_available_leader() is True
    # and stays that way across the whole window the live computation wrongly
    # restored (old contact + configured ttl, i.e. until T+15).
    clock[0] = 1000.0 + 14.0
    with pytest.raises(aiohttp.ClientError):
        await b._renew_once()
    assert b.is_quorate() is False
    assert b.is_available_leader() is True
    # only a REAL successful round re-establishes quorum.
    down["on"] = False
    await b._renew_once()
    assert b.is_quorate() is True


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
    with caplog.at_level(logging.WARNING, logger="cronstable.backends.etcd"):
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
    with caplog.at_level(logging.WARNING, logger="cronstable.backends.etcd"):
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
        "cronstable/leader/reboot-ran", "payload", "42"
    )
    key = _b64("cronstable/leader/reboot-ran")
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
    from cronstable.leadership import decode_reboot_ran, encode_reboot_ran

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
    from cronstable.leadership import encode_reboot_ran

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


# --- @reboot-ran conservative gate (failover double-fire guard) -------------
#
# Leadership and the reboot-ran key live at SEPARATE etcd keys, read by
# separate requests -- unlike kubernetes, where the ran-set rides the Lease
# annotations of the very read that wins leadership. So a failover leader
# whose reboot-ran read blips used to answer reboot_ran() "not ran" from its
# stale cache and re-run a one-shot the previous leader marked moments before
# dying. The gate mirrors FilesystemBackend: between gaining leadership (or a
# known lease loss) and a completed read-back, reboot_ran() raises
# RebootRanUnknownError and cron keeps the one-shot pending.


def test_reboot_ran_raises_while_leader_unsynced():
    from cronstable.leadership import RebootRanUnknownError

    b = _backend()
    b._is_leader = True
    assert b._reboot_ran_synced is False  # False from construction
    with pytest.raises(RebootRanUnknownError):
        b.reboot_ran("boot-job")


def test_reboot_ran_positive_answer_is_safe_while_unsynced():
    # marks are append-only within a job set: a stale cache can only be
    # MISSING marks, never carry false ones, so True is always safe.
    b = _backend()
    b._is_leader = True
    b._reboot_ran_local = {"mine"}
    b._reboot_ran_local_job_set_id = "v1:job"
    assert b.reboot_ran("mine") is True


def test_reboot_ran_follower_answers_false_while_unsynced():
    # a non-leader never launches the one-shot (cron gates on ownership after
    # this check), so it may answer False without the conservative raise.
    b = _backend()
    assert b.reboot_ran("boot-job") is False


@pytest.mark.asyncio
async def test_completed_read_back_opens_reboot_gate(monkeypatch):
    b = _backend()
    b._is_leader = True

    async def fake_post(path, body, *, allow_reauth=True):
        assert path == "/v3/kv/range"
        return {"kvs": []}  # absent key: a completed, authoritative read

    monkeypatch.setattr(b, "_post", fake_post)
    await b._cas_write_reboot_ran()
    assert b._reboot_ran_synced is True
    assert b.reboot_ran("boot-job") is False  # now safe to answer


@pytest.mark.asyncio
async def test_failed_read_back_keeps_reboot_gate_closed(monkeypatch):
    from cronstable.leadership import RebootRanUnknownError

    b = _backend()
    b._is_leader = True

    async def fake_post(path, body, *, allow_reauth=True):
        raise aiohttp.ClientError("etcd unreachable")

    monkeypatch.setattr(b, "_post", fake_post)
    await b._sync_reboot_ran()  # swallowed, as on any renew round
    assert b._reboot_ran_synced is False
    with pytest.raises(RebootRanUnknownError):
        b.reboot_ran("boot-job")


@pytest.mark.asyncio
async def test_mark_reboot_ran_eager_persist_opens_gate(monkeypatch):
    # the eager mark-persist path reads the key back too (CAS read-modify-
    # write), so a leader that just recorded its own one-shot is synced.
    b = _backend()
    b._is_leader = True

    async def fake_post(path, body, *, allow_reauth=True):
        if path == "/v3/kv/range":
            return {"kvs": []}
        return {"succeeded": True}

    monkeypatch.setattr(b, "_post", fake_post)
    await b.mark_reboot_ran("mine")
    assert b._reboot_ran_synced is True
    assert b.reboot_ran("mine") is True


@pytest.mark.asyncio
async def test_renew_gain_syncs_before_leadership_applies(monkeypatch):
    # on a takeover the forced read-back runs BEFORE _apply_round, so a
    # healthy gain never exposes is_leader()==True next to a stale set (and
    # a deferred one-shot is not delayed a round); a FAILED read-back still
    # applies leadership -- reboot_ran() just keeps deferring.
    from cronstable.leadership import RebootRanUnknownError

    b = _backend()
    sync_calls = []

    async def grant():
        return "222", 15

    async def campaign(lease_id):
        return "node-a", True

    async def sync(*, leaderish=None):
        sync_calls.append(("leader-at-sync", b._is_leader))
        # simulate the read-back failing: synced stays False

    monkeypatch.setattr(b, "_grant_lease", grant)
    monkeypatch.setattr(b, "_campaign", campaign)
    monkeypatch.setattr(b, "_sync_reboot_ran", sync)
    await b._renew_once()

    assert sync_calls == [("leader-at-sync", False)]  # sync BEFORE apply
    assert b._is_leader is True  # failed read-back still applied leadership
    with pytest.raises(RebootRanUnknownError):
        b.reboot_ran("boot-job")


@pytest.mark.asyncio
async def test_renew_steady_state_syncs_after_apply(monkeypatch):
    # an established leader (no gain) keeps the old order: apply, then the
    # best-effort sync; an open gate stays open across the round.
    b = _backend()
    b._is_leader = True
    b._lease_id = "111"
    b._reboot_ran_synced = True
    sync_calls = []

    async def keepalive(lease_id):
        return 15

    async def campaign(lease_id):
        return "node-a", True

    async def sync(*, leaderish=None):
        sync_calls.append(("leader-at-sync", b._is_leader))

    monkeypatch.setattr(b, "_keepalive", keepalive)
    monkeypatch.setattr(b, "_campaign", campaign)
    monkeypatch.setattr(b, "_sync_reboot_ran", sync)
    await b._renew_once()

    assert sync_calls == [("leader-at-sync", True)]
    assert b._reboot_ran_synced is True
    assert b.reboot_ran("boot-job") is False


@pytest.mark.asyncio
async def test_known_lease_loss_closes_reboot_gate(monkeypatch):
    # a lost lease can be re-won within a single round with _is_leader never
    # observed False -- another node may have led, run and marked a one-shot
    # entirely between our two rounds. The loss itself must force a fresh
    # read-back before "not ran" may be answered again.
    from cronstable.leadership import RebootRanUnknownError

    b = _backend()
    b._is_leader = True
    b._lease_id = "111"
    b._reboot_ran_synced = True  # was synced while leading

    async def keepalive(lease_id):
        return 0  # lease already expired server-side

    async def grant():
        return "222", 15

    async def campaign(lease_id):
        return "node-a", True  # immediately re-won

    async def sync(*, leaderish=None):
        pass  # read-back does NOT complete this round

    monkeypatch.setattr(b, "_keepalive", keepalive)
    monkeypatch.setattr(b, "_grant_lease", grant)
    monkeypatch.setattr(b, "_campaign", campaign)
    monkeypatch.setattr(b, "_sync_reboot_ran", sync)
    await b._renew_once()

    assert b._is_leader is True
    assert b._reboot_ran_synced is False
    with pytest.raises(RebootRanUnknownError):
        b.reboot_ran("boot-job")
