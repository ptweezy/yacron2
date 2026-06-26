"""Pure-helper and local-state tests for the Kubernetes Lease backend.

No apiserver and no crypto: the network glue is ``# pragma: no cover`` and
exercised only by the Docker integration tests; everything here is the decision
logic plus the locally-computed leader/quorum state.
"""

import datetime

import pytest

from yacron2.backends import (
    TRANSPORT_HTTP,
    TRANSPORT_LIBRARY,
    select_transport,
)
from yacron2.backends.kubernetes import (
    ACTION_ACQUIRE,
    ACTION_CREATE,
    ACTION_RENEW,
    ACTION_WAIT,
    KubernetesBackend,
    LeaseState,
    _format_microtime,
    _parse_microtime,
    build_lease_body,
    decide_lease_action,
    display_holder,
    lease_is_expired,
    parse_lease,
    plan_lease_write,
    resolve_namespace,
)
from yacron2.config import ConfigError, parse_config_string

NOW = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)


def _backend(extra=""):
    yaml = (
        "cluster:\n"
        "  backend: kubernetes\n"
        "  nodeName: node-a\n"
        "  connectTimeout: 4\n"
        "  kubernetes:\n"
        "    leaseName: yl\n"
        "    leaseNamespace: ns\n"
        "    leaseDurationSeconds: 15\n"
        "    renewDeadlineSeconds: 10\n"
        "    retryPeriodSeconds: 2\n" + extra
    )
    cfg = parse_config_string(yaml, "").cluster_config
    return KubernetesBackend(cfg, lambda: "v1:job")


# --- time parsing ---------------------------------------------------------


def test_parse_microtime_canonical():
    parsed = _parse_microtime("2026-01-01T12:00:00.000000Z")
    assert parsed == NOW


def test_parse_microtime_fewer_fractional_digits():
    parsed = _parse_microtime("2026-01-01T12:00:00.5Z")
    assert parsed == NOW.replace(microsecond=500000)


def test_parse_microtime_no_fraction_and_offset():
    assert _parse_microtime("2026-01-01T12:00:00Z") == NOW
    assert _parse_microtime("2026-01-01T12:00:00+00:00") == NOW


def test_parse_microtime_naive_assumes_utc():
    # no trailing Z and no offset: fromisoformat yields a naive datetime, which
    # we stamp as UTC.
    assert _parse_microtime("2026-01-01T12:00:00.5") == NOW.replace(
        microsecond=500000
    )


def test_parse_microtime_garbage_is_none():
    assert _parse_microtime("not-a-time") is None
    assert _parse_microtime(None) is None
    assert _parse_microtime("") is None
    assert _parse_microtime(12345) is None


def test_format_microtime_roundtrip():
    text = _format_microtime(NOW)
    assert text == "2026-01-01T12:00:00.000000Z"
    assert _parse_microtime(text) == NOW


# --- parse_lease ----------------------------------------------------------


def test_parse_lease_full():
    obj = {
        "metadata": {"resourceVersion": "42"},
        "spec": {
            "holderIdentity": "node-b",
            "renewTime": "2026-01-01T12:00:00.000000Z",
            "acquireTime": "2026-01-01T11:00:00.000000Z",
            "leaseDurationSeconds": 15,
            "leaseTransitions": 3,
        },
    }
    state = parse_lease(obj)
    assert state.holder == "node-b"
    assert state.renew_time == NOW
    assert state.acquire_time == NOW.replace(hour=11)
    assert state.duration == 15
    assert state.transitions == 3
    assert state.resource_version == "42"


def test_parse_lease_empty_object():
    state = parse_lease({})
    assert state.holder is None
    assert state.renew_time is None
    assert state.duration is None
    assert state.transitions == 0
    assert state.resource_version is None


def test_parse_lease_non_int_fields_ignored():
    state = parse_lease(
        {"spec": {"leaseDurationSeconds": "15", "leaseTransitions": None}}
    )
    assert state.duration is None
    assert state.transitions == 0


# --- lease_is_expired -----------------------------------------------------


def _after(seconds):
    return NOW + datetime.timedelta(seconds=seconds)


def test_lease_is_expired_fresh():
    # observed_at == renewTime simulates synced clocks
    state = LeaseState("node-b", NOW, NOW, 15, 0, "1")
    assert lease_is_expired(state, _after(10), NOW) is False


def test_lease_is_expired_lapsed():
    state = LeaseState("node-b", NOW, NOW, 15, 0, "1")
    assert lease_is_expired(state, _after(15), NOW) is True
    assert lease_is_expired(state, _after(20), NOW) is True


def test_lease_is_expired_uses_observed_anchor_not_renewtime():
    # skew immunity: a record OBSERVED 15s ago is expired on our clock even
    # though the holder's renewTime (NOW) claims it is fresh...
    state = LeaseState("node-b", NOW, NOW, 15, 0, "1")
    assert lease_is_expired(state, _after(5), _after(-15)) is True
    # ...and a record we only just observed is NOT stolen even if the holder's
    # renewTime is ancient (a slow/skewed holder clock keeps its lease).
    old = NOW - datetime.timedelta(seconds=100)
    stale_renew = LeaseState("node-b", old, old, 15, 0, "1")
    assert lease_is_expired(stale_renew, _after(5), NOW) is False


def test_lease_is_expired_anchor_fallback_and_no_duration():
    # no duration -> expired regardless of anchor
    assert lease_is_expired(LeaseState("x", NOW, NOW, None, 0, "1"), NOW, NOW)
    # no anchor at all (observed_at None and renew_time None) -> expired
    none_state = LeaseState(None, None, None, 15, 0, "1")
    assert lease_is_expired(none_state, NOW, None)
    # observed_at None falls back to the holder's renewTime
    fresh = LeaseState("x", NOW, NOW, 15, 0, "1")
    assert lease_is_expired(fresh, _after(10), None) is False
    assert lease_is_expired(fresh, _after(20), None) is True


# --- decide_lease_action --------------------------------------------------


def test_decide_create_when_no_lease():
    assert decide_lease_action(None, "node-a", NOW, None) == ACTION_CREATE


def test_decide_renew_when_we_hold_it():
    state = LeaseState("node-a", NOW, NOW, 15, 0, "1")
    assert decide_lease_action(state, "node-a", NOW, NOW) == ACTION_RENEW
    # reclaim even if our own lease lapsed
    late = NOW + datetime.timedelta(seconds=99)
    assert decide_lease_action(state, "node-a", late, NOW) == ACTION_RENEW


def test_decide_acquire_when_empty_or_expired():
    empty = LeaseState(None, NOW, NOW, 15, 0, "1")
    assert decide_lease_action(empty, "node-a", NOW, NOW) == ACTION_ACQUIRE
    expired = LeaseState("node-b", NOW, NOW, 15, 0, "1")
    late = _after(30)
    assert decide_lease_action(expired, "node-a", late, NOW) == ACTION_ACQUIRE


def test_decide_wait_when_other_holds_valid_lease():
    state = LeaseState("node-b", NOW, NOW, 15, 0, "1")
    assert decide_lease_action(state, "node-a", NOW, NOW) == ACTION_WAIT


# --- build_lease_body -----------------------------------------------------


def test_build_lease_body_create():
    body = build_lease_body("yl", "ns", "node-a", NOW, 15, None, ACTION_CREATE)
    assert body["apiVersion"] == "coordination.k8s.io/v1"
    assert body["metadata"] == {"name": "yl", "namespace": "ns"}
    spec = body["spec"]
    assert spec["holderIdentity"] == "node-a"
    assert spec["leaseDurationSeconds"] == 15
    assert spec["leaseTransitions"] == 0
    assert spec["renewTime"] == _format_microtime(NOW)
    assert spec["acquireTime"] == _format_microtime(NOW)


def test_build_lease_body_renew_preserves_acquire_and_transitions():
    acq = NOW.replace(hour=11)
    state = LeaseState("node-a", NOW, acq, 15, 5, "42")
    later = _after(5)
    body = build_lease_body(
        "yl", "ns", "node-a", later, 15, state, ACTION_RENEW
    )
    assert body["metadata"]["resourceVersion"] == "42"
    assert body["spec"]["acquireTime"] == _format_microtime(acq)
    assert body["spec"]["leaseTransitions"] == 5
    assert body["spec"]["renewTime"] == _format_microtime(later)


def test_build_lease_body_acquire_bumps_transitions():
    state = LeaseState("node-b", NOW, NOW, 15, 5, "42")
    later = NOW + datetime.timedelta(seconds=30)
    body = build_lease_body(
        "yl", "ns", "node-a", later, 15, state, ACTION_ACQUIRE
    )
    assert body["spec"]["leaseTransitions"] == 6
    assert body["spec"]["acquireTime"] == _format_microtime(later)
    assert body["metadata"]["resourceVersion"] == "42"


def test_build_lease_body_omits_namespace_when_absent():
    body = build_lease_body("yl", None, "node-a", NOW, 15, None, ACTION_CREATE)
    assert "namespace" not in body["metadata"]


# --- plan_lease_write -----------------------------------------------------


def test_plan_wait_has_no_body():
    obj = {
        "metadata": {"resourceVersion": "1"},
        "spec": {
            "holderIdentity": "node-b",
            "renewTime": _format_microtime(NOW),
            "leaseDurationSeconds": 15,
        },
    }
    action, body, state = plan_lease_write(
        obj, "yl", "ns", "node-a", NOW, 15, NOW
    )
    assert action == ACTION_WAIT
    assert body is None
    assert state.holder == "node-b"


def test_plan_create_when_absent():
    action, body, state = plan_lease_write(
        None, "yl", "ns", "node-a", NOW, 15, None
    )
    assert action == ACTION_CREATE
    assert body is not None
    assert state is None


# --- backend local state --------------------------------------------------


def test_construction_defaults_and_identity():
    b = _backend()
    assert b.backend_name == "kubernetes"
    assert b.display_identity == "node-a"  # defaulted from nodeName
    # the written holderIdentity carries a per-process token for uniqueness
    assert b.identity.startswith("node-a#")
    assert b.namespace == "ns"
    assert b.lease_duration == 15
    assert b.distribution == "single-leader"


def test_identity_can_be_overridden():
    b = _backend(extra="    identity: custom-id\n")
    assert b.display_identity == "custom-id"
    assert b.identity.startswith("custom-id#")


def test_identity_is_unique_per_process():
    # two nodes sharing a nodeName (a duplicate identity) still write DISTINCT
    # holderIdentity strings, so neither sees the other's holder as "us" and
    # both cannot believe they hold the Lease -- the fenced-guarantee fix.
    b1, b2 = _backend(), _backend()
    assert b1.display_identity == b2.display_identity == "node-a"
    assert b1.identity != b2.identity
    # the loser observes the winner's full identity holding a valid lease and
    # decides to WAIT, not RENEW (a bare-nodeName match would wrongly renew).
    held = LeaseState(b1.identity, NOW, NOW, 15, 0, "1")
    assert decide_lease_action(held, b2.identity, NOW, NOW) == ACTION_WAIT
    # and the true holder still renews its own lease.
    assert decide_lease_action(held, b1.identity, NOW, NOW) == ACTION_RENEW


def test_display_holder_strips_instance_token():
    assert display_holder("node-a#deadbeef0000") == "node-a"
    assert display_holder("yacron2-0#abc123") == "yacron2-0"
    # no suffix (a foreign / older holder) -> shown unchanged
    assert display_holder("legacy-holder") == "legacy-holder"
    assert display_holder(None) is None


def test_client_library_default_and_override():
    assert _backend().client_library == "auto"
    assert _backend(extra="    clientLibrary: http\n").client_library == "http"


def test_select_transport_auto_prefers_native_else_http():
    assert select_transport("auto", True, "kubernetes") == TRANSPORT_LIBRARY
    assert select_transport("auto", False, "kubernetes") == TRANSPORT_HTTP


def test_select_transport_http_forces_http():
    assert select_transport("http", True, "kubernetes") == TRANSPORT_HTTP


def test_select_transport_library_requires_native():
    assert select_transport("library", True, "kubernetes") == TRANSPORT_LIBRARY
    with pytest.raises(ConfigError, match="not importable"):
        select_transport("library", False, "kubernetes")


def test_resolve_namespace_precedence():
    # regression: both transports must converge on the SAME namespace, and it
    # must never be None -- if the in-cluster HTTP path leaves it None while
    # the native path falls back to "default", a mixed-transport fleet runs two
    # leaders (one per namespace). Explicit leaseNamespace wins over all.
    assert resolve_namespace("explicit", "ctx", "incluster") == "explicit"
    # then the kubeconfig context's namespace (kubeconfig path only)
    assert resolve_namespace(None, "ctx", "incluster") == "ctx"
    # then the in-cluster service-account namespace
    assert resolve_namespace(None, None, "incluster") == "incluster"
    # finally "default" -- crucially never None, even when the SA namespace
    # file is absent/unreadable (the trigger for the HTTP/library divergence)
    assert resolve_namespace(None, None, None) == "default"


def test_namespace_ignores_incluster_file_when_kubeconfig_set(monkeypatch):
    # H1 regression: when a kubeconfig is configured the in-cluster
    # service-account namespace file must NOT be consulted -- by EITHER
    # transport. If the native client transport consulted
    # _incluster_namespace() while the HTTP transport passed None, a
    # mixed-transport in-pod fleet
    # (native client on one arch, HTTP fallback on another) would elect the
    # Lease in two different namespaces (the SA file's value vs "default") -- a
    # cross-namespace split-brain. Both resolve through
    # KubernetesBackend._resolve_namespace identically.
    import yacron2.backends.kubernetes as k8s_mod

    monkeypatch.setattr(k8s_mod, "_incluster_namespace", lambda: "prod")

    def _mk(extra):
        yaml = (
            "cluster:\n"
            "  backend: kubernetes\n"
            "  nodeName: node-a\n"
            "  connectTimeout: 4\n"
            "  kubernetes:\n"
            "    leaseName: yl\n"
            "    leaseDurationSeconds: 15\n"
            "    renewDeadlineSeconds: 10\n"
            "    retryPeriodSeconds: 2\n" + extra
        )
        cfg = parse_config_string(yaml, "").cluster_config
        return KubernetesBackend(cfg, lambda: "v1:job")

    # kubeconfig set, no leaseNamespace, no kubeconfig-context namespace ->
    # "default", NEVER the SA file's "prod" (the split-brain trigger)
    kc = _mk("    kubeconfig: /tmp/kc\n")
    assert kc._resolve_namespace(None) == "default"
    # a kubeconfig context namespace is honoured (matches the HTTP path)
    assert kc._resolve_namespace("ctxns") == "ctxns"
    # truly in-cluster (no kubeconfig) DOES consult the SA namespace file
    incluster = _mk("")
    assert incluster._resolve_namespace(None) == "prod"
    # an explicit leaseNamespace always wins, kubeconfig or not
    explicit = _mk("    leaseNamespace: chosen\n    kubeconfig: /tmp/kc\n")
    assert explicit._resolve_namespace("ctxns") == "chosen"


def test_is_leader_gated_on_local_expiry():
    b = _backend()
    assert b.is_leader() is False  # fresh: never renewed
    b._is_leader = True
    b._leader_until = NOW  # in the past relative to real now
    assert b.is_leader() is False  # self-demote without a network call
    b._leader_until = _utc_now_plus(100)
    assert b.is_leader() is True


def test_is_quorate_tracks_fresh_contact():
    b = _backend()
    assert b.is_quorate() is False  # never contacted
    b._last_contact = _utc_now_plus(0)
    assert b.is_quorate() is True
    b._last_contact = _utc_now_plus(-(b.lease_duration + 5))
    assert b.is_quorate() is False  # stale -> Leader fails closed


def test_leader_name_none_when_not_quorate():
    b = _backend()
    b._holder = "node-b"
    assert b.leader_name() is None  # stale read -> unknown
    b._last_contact = _utc_now_plus(0)
    assert b.leader_name() == "node-b"


def test_track_observation_resets_clock_on_record_change():
    b = _backend()
    s1 = LeaseState("node-b", NOW, NOW, 15, 0, "1")
    t0 = _after(0)
    assert b._track_observation(s1, t0) == t0  # first sight anchors at t0
    # same (holder, renewTime) record a bit later: anchor stays at t0
    assert b._track_observation(s1, _after(3)) == t0
    # a renewed record (new renewTime) re-anchors to the new observation time
    s2 = LeaseState("node-b", _after(2), NOW, 15, 0, "1")
    t1 = _after(4)
    assert b._track_observation(s2, t1) == t1
    # a different holder also re-anchors
    s3 = LeaseState("node-c", _after(2), NOW, 15, 0, "1")
    t2 = _after(6)
    assert b._track_observation(s3, t2) == t2
    # no lease observed (None) re-anchors to that observation too
    assert b._track_observation(None, _after(8)) == _after(8)


def test_apply_round_win_acquire():
    b = _backend()
    b._apply_round(ACTION_ACQUIRE, True, None, NOW)
    assert b._is_leader is True
    assert b._holder == "node-a"
    assert b._leader_until == b._leader_deadline(NOW)
    assert b._last_contact == NOW


def test_apply_round_wait_records_other_holder():
    b = _backend()
    state = LeaseState("node-b", NOW, NOW, 15, 0, "1")
    b._apply_round(ACTION_WAIT, False, state, NOW)
    assert b._is_leader is False
    assert b._holder == "node-b"
    assert b._last_contact == NOW


def test_apply_round_lost_race_not_leader():
    b = _backend()
    b._is_leader = True
    state = LeaseState("node-b", NOW, NOW, 15, 0, "1")
    b._apply_round(ACTION_ACQUIRE, False, state, NOW)
    assert b._is_leader is False
    assert b._holder == "node-b"


def test_view_dict_and_lease_detail():
    b = _backend()
    # apply the round at real "now" so is_leader()/is_quorate() (which read the
    # wall clock) see a still-valid lease and a fresh contact.
    b._apply_round(ACTION_CREATE, True, None, _utc_now_plus(0))
    view = b.view_dict()
    assert view["backend"] == "kubernetes"
    assert view["is_leader"] is True
    assert view["lease"]["name"] == "yl"
    assert view["lease"]["namespace"] == "ns"
    assert view["lease"]["holder"] == "node-a"
    assert view["lease"]["identity"] == "node-a"
    assert view["lease"]["expiry"] is not None


def _utc_now_plus(seconds):
    return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        seconds=seconds
    )
