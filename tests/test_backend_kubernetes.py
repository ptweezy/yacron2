"""Pure-helper and local-state tests for the Kubernetes Lease backend.

No apiserver and no crypto: the network glue is ``# pragma: no cover`` and
exercised only by the Docker integration tests; everything here is the decision
logic plus the locally-computed leader/quorum state.
"""

import copy
import datetime
import time

import pytest

from yacron2.backends import (
    TRANSPORT_HTTP,
    TRANSPORT_LIBRARY,
    select_transport,
)
from yacron2.backends.kubernetes import (
    _UNKNOWN_HOLDER,
    ACTION_ACQUIRE,
    ACTION_CREATE,
    ACTION_RENEW,
    ACTION_WAIT,
    KubernetesBackend,
    LeaseState,
    _format_microtime,
    _join_host_port,
    _K8sHttpTransport,
    _kubeconfig_cert_files,
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
    assert state.annotations == {}


def test_parse_and_build_lease_annotations_roundtrip():
    # H2: the @reboot-ran set is persisted in metadata.annotations; parse_lease
    # must surface them and build_lease_body must write them back.
    obj = {
        "metadata": {
            "resourceVersion": "9",
            "annotations": {"yacron2.io/reboot-ran": "blob"},
        },
        "spec": {"holderIdentity": "node-a", "leaseDurationSeconds": 15},
    }
    state = parse_lease(obj)
    assert state.annotations == {"yacron2.io/reboot-ran": "blob"}
    body = build_lease_body(
        "yl", "ns", "node-a", NOW, 15, state, ACTION_RENEW,
        {"yacron2.io/reboot-ran": "blob2"},
    )
    assert body["metadata"]["annotations"] == {
        "yacron2.io/reboot-ran": "blob2"
    }
    # no annotations passed -> the block is omitted
    bare = build_lease_body("yl", "ns", "node-a", NOW, 15, None, ACTION_CREATE)
    assert "annotations" not in bare["metadata"]


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
    # the fence is a MONOTONIC deadline (immune to wall-clock steps); the
    # wall-clock _leader_until is display only.
    b = _backend()
    assert b.is_leader() is False  # fresh: never renewed
    b._is_leader = True
    b._leader_until_mono = time.monotonic() - 1  # in the past
    assert b.is_leader() is False  # self-demote without a network call
    b._leader_until_mono = time.monotonic() + 100
    assert b.is_leader() is True


def test_is_quorate_tracks_fresh_contact():
    b = _backend()
    assert b.is_quorate() is False  # never contacted
    b._last_contact_mono = time.monotonic()
    assert b.is_quorate() is True
    b._last_contact_mono = time.monotonic() - (b.lease_duration + 5)
    assert b.is_quorate() is False  # stale -> Leader fails closed


def test_leader_name_none_when_not_quorate():
    b = _backend()
    b._holder = "node-b"
    assert b.leader_name() is None  # stale read -> unknown
    b._last_contact_mono = time.monotonic()
    assert b.leader_name() == "node-b"


def test_lease_detail_expiry_only_while_leader():
    # L4: a former holder must not keep advertising a stale expiry. Expiry is
    # populated only while is_leader() is true.
    b = _backend()
    # leader, fresh deadline -> expiry shown
    b._apply_round(ACTION_CREATE, True, None, _utc_now_plus(0))
    assert b.lease_detail()["expiry"] is not None
    # a later WAIT round (a peer took over) -> not leader -> no expiry
    state = LeaseState("node-b#x", NOW, NOW, 15, 0, "1")
    b._apply_round(ACTION_WAIT, False, state, _utc_now_plus(0))
    assert b.lease_detail()["expiry"] is None


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
    # no lease observed (None, a 404) PRESERVES the last real anchor rather
    # than re-anchoring: a 404 carries no timing, and resetting here is what
    # let a node recreate a deleted Lease as itself while a prior holder was
    # still inside its local fence -- two leaders (see decide_lease_action's
    # recreate-race guard / F06).
    assert b._track_observation(None, _after(8)) == t2


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


def test_apply_round_create_race_loser_reports_holder_not_none():
    # K8S-409-HOLDER: two replicas cold-start with no Lease, both plan CREATE
    # (plan_lease_write passes state=None). The loser's POST 409s -> write_ok
    # False with state=None. It is still quorate (it reached the apiserver), so
    # leaving _holder None would make leader_name() None, which
    # is_available_leader() reads "holder unknown -> run anyway": the quorate
    # loser would run every PreferLeader (and spread) job alongside the real
    # holder, a double-run with NO partition. A 409 proves a holder won, so
    # cold start (no observed holder) report a non-None sentinel and DEFER.
    b = _backend()
    assert b._observed_holder is None  # cold start, never observed a holder
    b._apply_round(ACTION_CREATE, False, None, _utc_now_plus(0))
    assert b.is_quorate() is True  # we did reach the apiserver this round
    assert b._is_leader is False
    assert b.is_leader() is False
    assert b._holder == _UNKNOWN_HOLDER  # non-None sentinel, NOT None
    assert b.leader_name() is not None
    assert b.is_available_leader() is False  # the loser defers (no double-run)


def test_apply_round_create_race_loser_prefers_observed_holder():
    # K8S-409-HOLDER: a later create race (not the very first round) where we
    # DID previously observe a holder prefers that real display name over the
    # bare sentinel -- still non-None either way, so a follower still defers.
    b = _backend()
    b._observed_holder = "node-b#tok"  # the holder we last observed
    b._apply_round(ACTION_CREATE, False, None, _utc_now_plus(0))
    assert b._holder == "node-b"  # display name of the observed holder
    assert b.leader_name() is not None
    assert b.is_available_leader() is False


def test_join_host_port_brackets_ipv6():
    # K8S-IPV6-URL: KUBERNETES_SERVICE_HOST is unbracketed on an IPv6
    # single-stack cluster; without bracketing the URL is ambiguous and yarl
    # rejects it. IPv4 and hostnames are left alone; an already-bracketed host
    # is not double-bracketed.
    assert _join_host_port("10.0.0.1", "443") == "10.0.0.1:443"
    assert _join_host_port("kubernetes.default", "443") == (
        "kubernetes.default:443"
    )
    assert _join_host_port("fd00:10:96::1", "443") == "[fd00:10:96::1]:443"
    assert _join_host_port("[fd00::1]", "443") == "[fd00::1]:443"
    # the built URL parses (yarl accepts the bracketed authority)
    from yarl import URL

    url = URL("https://" + _join_host_port("fd00:10:96::1", "443"))
    assert url.host == "fd00:10:96::1" and url.port == 443


def test_kubeconfig_cert_files_extracts_referenced_paths(tmp_path):
    # K8S-NATIVE-CERT-ROTATE: the native transport must track a kubeconfig's
    # FILE-referenced CA/cert/key (not just the kubeconfig path) so an in-place
    # cert rotation rebuilds the backend. _kubeconfig_cert_files returns the
    # active context's referenced files; embedded -data forms resolve to None.
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text(
        "apiVersion: v1\n"
        "current-context: ctx\n"
        "contexts:\n"
        "  - name: ctx\n"
        "    context:\n"
        "      cluster: c\n"
        "      user: u\n"
        "clusters:\n"
        "  - name: c\n"
        "    cluster:\n"
        "      server: https://1.2.3.4:6443\n"
        "      certificate-authority: /etc/certs/ca.crt\n"
        "users:\n"
        "  - name: u\n"
        "    user:\n"
        "      client-certificate: /etc/certs/tls.crt\n"
        "      client-key: /etc/certs/tls.key\n"
    )
    assert _kubeconfig_cert_files(str(kubeconfig)) == [
        "/etc/certs/ca.crt",
        "/etc/certs/tls.crt",
        "/etc/certs/tls.key",
    ]


def test_kubeconfig_cert_files_tolerates_embedded_and_malformed(tmp_path):
    # embedded -data certs resolve to None (a -data rotation rewrites the
    # kubeconfig itself, tracked via its path); a malformed/absent kubeconfig
    # yields [] rather than raising (the kubeconfig path stays tracked anyway).
    embedded = tmp_path / "embedded"
    embedded.write_text(
        "apiVersion: v1\n"
        "current-context: ctx\n"
        "contexts:\n"
        "  - name: ctx\n"
        "    context: {cluster: c, user: u}\n"
        "clusters:\n"
        "  - name: c\n"
        "    cluster:\n"
        "      server: https://1.2.3.4:6443\n"
        "      certificate-authority-data: Zm9v\n"
        "users:\n"
        "  - name: u\n"
        "    user:\n"
        "      client-certificate-data: YmFy\n"
    )
    assert _kubeconfig_cert_files(str(embedded)) == [None, None, None]
    assert _kubeconfig_cert_files(str(tmp_path / "missing")) == []


def test_kubeconfig_cert_files_tolerates_ruamel_yaml_errors(tmp_path):
    # Finding 16: the tolerance must cover the ruamel error family, not just
    # OSError/structural errors. A duplicated mapping key raises
    # DuplicateKeyError -- descending from Warning, NOT from any class the old
    # tuple caught -- while PyYAML (what the native client's load_kube_config
    # parses with) ACCEPTS the same file, so the official client succeeds and
    # the unguarded _setup_sync call reaches this helper. An escape here
    # aborts the whole backend ("please report this as a bug"): Leader jobs
    # run on NO replica, PreferLeader on EVERY replica.
    dup = tmp_path / "dup"
    dup.write_text(
        "apiVersion: v1\n"
        "current-context: ctx\n"
        "contexts:\n"
        "  - name: ctx\n"
        "    context: {cluster: c, user: u}\n"
        "clusters:\n"
        "  - name: c\n"
        "    cluster:\n"
        "      server: https://1.2.3.4:6443\n"
        "      certificate-authority: /a.crt\n"
        "      certificate-authority: /b.crt\n"
        "users:\n"
        "  - name: u\n"
        "    user: {token: tok}\n"
    )
    assert _kubeconfig_cert_files(str(dup)) == []
    # syntax-broken (a tab indent -> ScannerError, a YAMLError subclass)
    tabbed = tmp_path / "tabbed"
    tabbed.write_text("contexts:\n\t- broken\n")
    assert _kubeconfig_cert_files(str(tabbed)) == []
    # truncated mid-rotation (ParserError, also a YAMLError subclass)
    truncated = tmp_path / "truncated"
    truncated.write_text("clusters:\n  - name: c\n    cluster: {server: h\n")
    assert _kubeconfig_cert_files(str(truncated)) == []


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


async def test_renew_once_anchors_steal_at_observe_time(monkeypatch):
    # The steal anchor (_observed_at) must be the MONOTONIC instant we actually
    # read the Lease, not a timestamp captured before observe() returns.
    # Anchoring it before the await would back-date the record by the observe
    # latency (bounded by renewDeadline, not the 1s skew budget), shrinking the
    # steal window and risking two simultaneous leaders on a slow apiserver.
    # Mirrors client-go's observedTime = now() after the Get.
    b = _backend()
    mono = [100.0]

    def fake_mono():
        return mono[0]

    monkeypatch.setattr("yacron2.backends.kubernetes._monotonic", fake_mono)

    lease_obj = {
        "metadata": {"name": "yl", "namespace": "ns", "resourceVersion": "7"},
        "spec": {
            "holderIdentity": "other#tok",
            "renewTime": _format_microtime(NOW),
            "leaseDurationSeconds": 15,
            "leaseTransitions": 0,
        },
    }
    observe_latency = 5.0

    class _FakeTransport:
        async def observe(self):
            mono[0] += observe_latency  # the GET takes monotonic time
            return lease_obj

        async def write(self, body, *, create):  # pragma: no cover - not hit
            return True

        async def setup(self):
            pass

        async def close(self):
            pass

    b._transport = _FakeTransport()
    await b._renew_once()

    # the anchor is sampled AFTER observe() returns (100 + 5), not before.
    assert b._observed_at == 105.0


# --- HTTP transport: rotating service-account token -----------------------


def test_http_transport_rereads_rotating_token(tmp_path):
    # Kubernetes rotates the projected SA token on disk (~hourly); the lease
    # backend is never rebuilt on rotation, so _auth_headers must re-read the
    # token each request or a stale frozen token 401s the node into a
    # permanent, cluster-wide loss of leadership.
    token = tmp_path / "token"
    token.write_text("tok-1")
    t = _K8sHttpTransport(_backend())
    t._token_path = str(token)
    assert t._auth_headers() == {"Authorization": "Bearer tok-1"}
    # the kubelet replaces the file with a fresh token before the old expires.
    token.write_text("tok-2")
    assert t._auth_headers() == {"Authorization": "Bearer tok-2"}
    # a transient read failure keeps the last good token (the round may fail,
    # which fails Leader closed -- it does not drop the credential).
    token.unlink()
    assert t._auth_headers() == {"Authorization": "Bearer tok-2"}


def test_http_transport_static_token_and_no_token(tmp_path):
    # a kubeconfig bearer token has no _token_path and is used as-is; with no
    # token at all (client-cert auth) there is no Authorization header.
    t = _K8sHttpTransport(_backend())
    t._token_path = None
    t._auth_token = "static-tok"
    assert t._auth_headers() == {"Authorization": "Bearer static-tok"}
    t._auth_token = None
    assert t._auth_headers() == {}


def _write_kubeconfig(tmp_path, server="https://10.0.0.5:6443"):
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text(
        "apiVersion: v1\n"
        "current-context: ctx\n"
        "contexts:\n"
        "  - name: ctx\n"
        "    context: {cluster: c, user: u}\n"
        "clusters:\n"
        "  - name: c\n"
        "    cluster:\n"
        "      server: " + server + "\n"
        "users:\n"
        "  - name: u\n"
        "    user: {token: tok}\n"
    )
    return kubeconfig


def test_http_kubeconfig_honours_apiserver_override(tmp_path):
    # Finding 1: cluster.kubernetes.apiServer must override the kubeconfig's
    # embedded server URL on the HTTP transport too, matching the native
    # client (_setup_sync applies it after load_kube_config) and the
    # documented override-wins semantics. Silently dropping it points the two
    # transports at DIFFERENT apiservers from the same config under
    # clientLibrary auto (two lease stores each granting a lease -> two
    # leaders), and even in a single-transport fleet pins every round to the
    # kubeconfig's (often unreachable) embedded URL -- permanently non-quorate.
    kubeconfig = _write_kubeconfig(tmp_path)
    b = _backend(
        extra=(
            "    kubeconfig: '{}'\n"
            "    apiServer: https://mgmt-vip.example.com:8443/\n"
        ).format(kubeconfig)
    )
    t = _K8sHttpTransport(b)
    t._load_connection()
    # the override wins (and is rstripped like the native transport's)
    assert t._base_url == "https://mgmt-vip.example.com:8443"
    # without the override the kubeconfig's embedded server is used
    plain = _backend(extra="    kubeconfig: '{}'\n".format(kubeconfig))
    t2 = _K8sHttpTransport(plain)
    t2._load_connection()
    assert t2._base_url == "https://10.0.0.5:6443"


def test_http_load_kubeconfig_yaml_errors_become_configerror(tmp_path):
    # Finding 16: a kubeconfig the ruamel loader rejects must surface as
    # ConfigError -- which start_stop_cluster catches and logs as "cluster:
    # failed to start" -- not escape as a raw ScannerError/ParserError/
    # DuplicateKeyError to the run loop's "please report this as a bug"
    # handler. The YAML load sat OUTSIDE the try that does the conversion, and
    # DuplicateKeyError descends from Warning so it needs naming explicitly.
    t = _K8sHttpTransport(_backend())
    # duplicated mapping key (e.g. a hand-merged kubeconfig kubectl accepts)
    dup = tmp_path / "dup"
    dup.write_text(
        "apiVersion: v1\n"
        "current-context: ctx\n"
        "contexts:\n"
        "  - name: ctx\n"
        "    context: {cluster: c, user: u}\n"
        "clusters:\n"
        "  - name: c\n"
        "    cluster:\n"
        "      server: https://1.2.3.4:6443\n"
        "      certificate-authority: /a.crt\n"
        "      certificate-authority: /b.crt\n"
        "users:\n"
        "  - name: u\n"
        "    user: {token: tok}\n"
    )
    with pytest.raises(ConfigError, match="malformed kubeconfig"):
        t._load_kubeconfig(str(dup))
    # truncated mid-rotation / syntax-broken
    truncated = tmp_path / "truncated"
    truncated.write_text("clusters:\n  - name: c\n    cluster: {server: h\n")
    with pytest.raises(ConfigError, match="malformed kubeconfig"):
        t._load_kubeconfig(str(truncated))
    tabbed = tmp_path / "tabbed"
    tabbed.write_text("contexts:\n\t- broken\n")
    with pytest.raises(ConfigError, match="malformed kubeconfig"):
        t._load_kubeconfig(str(tabbed))


# --- shared-store fence: the at-most-one guarantee end-to-end --------------
#
# A minimal in-memory "apiserver" Lease object with the optimistic-concurrency
# semantics the fence relies on: create fails (409) if the object exists, and
# replace fails (409) on a stale resourceVersion. This exercises the actual
# write()->fence path the production transports drive, which is otherwise only
# covered by (absent) Docker integration tests.


class _StoreNotFound(Exception):
    """The fake apiserver's 404 on a replace of a deleted Lease.

    Both production transports call ``resp.raise_for_status()`` on a 404, so a
    PUT against a Lease deleted between observe and write RAISES (the round
    fails and ``_last_contact_mono`` is left unadvanced -> the holder soon
    goes non-quorate -> never-skip PreferLeader double-runs). Only a *409* is
    the soft "lost the resourceVersion race" that returns False. Modelling the
    404 as a raise (not False) keeps the fence test honest about that path.
    """


class _FakeApiStore:
    def __init__(self):
        self.obj = None
        self._rv = 0

    def get(self):
        return copy.deepcopy(self.obj)

    def create(self, body):
        if self.obj is not None:
            return False  # 409 AlreadyExists -- the fence
        self._rv += 1
        body = copy.deepcopy(body)
        body.setdefault("metadata", {})["resourceVersion"] = str(self._rv)
        self.obj = body
        return True

    def replace(self, body):
        if self.obj is None:
            # 404: the object was deleted between observe and write. The real
            # transports raise_for_status() here (only a 409 is the soft lost
            # race), so RAISE rather than returning False -- returning False
            # would model a deleted-during-renew Lease as a clean self-demote
            # that KEEPS quorum, hiding the split-brain-relevant path.
            raise _StoreNotFound()
        rv = (body.get("metadata") or {}).get("resourceVersion")
        if rv != self.obj["metadata"]["resourceVersion"]:
            return False  # 409 conflict -- the fence
        self._rv += 1
        body = copy.deepcopy(body)
        body["metadata"]["resourceVersion"] = str(self._rv)
        self.obj = body
        return True


class _StoreTransport:
    def __init__(self, store):
        self.store = store

    async def setup(self):  # pragma: no cover - not exercised
        pass

    async def observe(self):
        return self.store.get()

    async def write(self, body, *, create):
        return self.store.create(body) if create else self.store.replace(body)

    async def close(self):  # pragma: no cover - not exercised
        pass


def _store_backend(store, identity, extra=""):
    b = _backend(extra)
    b.identity = identity
    b._transport = _StoreTransport(store)
    return b


async def test_two_nodes_concurrent_create_only_one_wins():
    # F08/F19: two nodes observe the (absent) Lease in the SAME round and both
    # try to create it; the apiserver's AlreadyExists 409 must let exactly one
    # win. (No test previously drove the fence against a shared store; the old
    # tests hand-fed write_ok.)
    store = _FakeApiStore()
    a = _store_backend(store, "node-a#1")
    b = _store_backend(store, "node-b#2")
    la, lb = await a._transport.observe(), await b._transport.observe()
    assert la is None and lb is None  # both see no lease
    _, abody, _ = plan_lease_write(
        la, a.lease_name, a.namespace, a.identity, NOW, a.lease_duration, None
    )
    _, bbody, _ = plan_lease_write(
        lb, b.lease_name, b.namespace, b.identity, NOW, b.lease_duration, None
    )
    aok = await a._transport.write(abody, create=True)
    bok = await b._transport.write(bbody, create=True)
    assert aok != bok  # exactly one create succeeded


async def test_renew_loses_resourceversion_race_not_leader():
    # F08/F19: a holder whose observed resourceVersion is stale (another writer
    # moved the Lease on) must get a 409 on replace and self-demote.
    store = _FakeApiStore()
    a = _store_backend(store, "node-a#1")
    await a._renew_once()  # a creates + holds the lease
    assert a.is_leader() is True
    # someone else bumps the object (rv changes) between a's observe and write
    stale = store.get()
    store._rv += 1
    store.obj["metadata"]["resourceVersion"] = str(store._rv)
    _, body, state = plan_lease_write(
        stale, a.lease_name, a.namespace, a.identity, NOW, a.lease_duration,
        None,
    )
    ok = await a._transport.write(body, create=False)
    assert ok is False  # fenced by resourceVersion
    a._apply_round(ACTION_RENEW, ok, state, NOW)
    assert a.is_leader() is False


def test_wait_with_deleted_lease_reports_remembered_holder_not_none():
    # M1: on the deleted-lease WAIT path (state is None because the Lease
    # object was deleted, but decide_lease_action returned WAIT to keep
    # deferring to a holder we recently saw whose fence has not expired),
    # _apply_round must report the REMEMBERED holder, not None. Reporting None
    # makes leader_name() None, which is_available_leader() reads as "holder
    # unknown -> run anyway" -- so a quorate follower would run PreferLeader
    # alongside the still-fenced prior holder, a double-run with NO partition.
    b = _backend()
    b._observed_holder = "node-b#held"  # the holder we last observed
    b._apply_round(ACTION_WAIT, False, None, NOW)  # state None: lease deleted
    assert b.is_quorate() is True  # we did reach the apiserver this round
    assert b.is_leader() is False
    assert b.leader_name() is not None  # remembered holder, NOT None
    assert b.is_available_leader() is False  # so a follower defers (no run)


async def test_replace_against_deleted_lease_raises_not_silent_false():
    # M9: a replace (RENEW/ACQUIRE PUT) against a Lease deleted between observe
    # and write must RAISE the apiserver 404, NOT return False. Both real
    # transports raise_for_status() on a 404 and treat only a 409 as the soft
    # lost race; returning False here would model a deleted-during-renew Lease
    # as a clean self-demote that keeps quorum, hiding the path that actually
    # produces a stale (-> non-quorate -> PreferLeader never-skip) holder.
    store = _FakeApiStore()
    a = _store_backend(store, "node-a#1")
    await a._renew_once()  # create + hold the lease
    observed = store.get()  # a valid lease object (holder == a.identity)
    store.obj = None  # deleted out from under us, after we observed it
    _, body, _ = plan_lease_write(
        observed, a.lease_name, a.namespace, a.identity, NOW, a.lease_duration,
        None,
    )
    with pytest.raises(_StoreNotFound):
        await a._transport.write(body, create=False)


async def test_deleted_lease_not_recreated_under_live_holder(monkeypatch):
    # F06: deleting the Lease object must NOT let a node that recently saw a
    # different, still-valid holder immediately recreate it as itself -- that
    # would make two leaders until the prior holder's local fence lapses.
    clock = [1000.0]
    monkeypatch.setattr(
        "yacron2.backends.kubernetes._monotonic", lambda: clock[0]
    )
    store = _FakeApiStore()
    holder = _store_backend(store, "node-b#held")
    other = _store_backend(store, "node-a#other")
    await holder._renew_once()  # B creates + holds
    assert holder.is_leader() is True
    await other._renew_once()  # A observes B's valid lease -> waits
    assert other.is_leader() is False
    # the Lease object is deleted out from under the live holder.
    store.obj = None
    clock[0] += 1.0  # well within the 15s lease duration
    await other._renew_once()  # A sees 404 but remembers B's unexpired lease
    assert other.is_leader() is False  # A must NOT recreate as itself yet
    assert holder.is_leader() is True  # B still leads (its fence is unexpired)
    # once B's remembered lease has expired from A's view, A may recreate.
    clock[0] += holder.lease_duration + 1
    await other._renew_once()
    assert other.is_leader() is True


async def test_persist_reboot_ran_eagerly_writes_annotation():
    # F03/F07: mark_reboot_ran on the kubernetes backend must persist the
    # @reboot-ran set to the Lease IMMEDIATELY (cron records intent-to-run
    # before launching, relying on this), not only on the next periodic round.
    store = _FakeApiStore()
    b = _store_backend(store, "node-a#1")
    await b._renew_once()  # acquire the lease
    await b.mark_reboot_ran("migrate")  # eager persist
    stored = parse_lease(store.get())
    from yacron2.leadership import REBOOT_RAN_KEY, decode_reboot_ran

    jsid, jobs = decode_reboot_ran(stored.annotations.get(REBOOT_RAN_KEY))
    assert jsid == "v1:job" and jobs == {"migrate"}


async def test_release_preserves_reboot_ran_annotation():
    # F03: a graceful release (config reload / stop) must hand the Lease back
    # WITHOUT erasing the @reboot-ran annotation, or a peer that takes over
    # re-runs the one-shot.
    store = _FakeApiStore()
    b = _store_backend(store, "node-a#1")
    await b._renew_once()
    await b.mark_reboot_ran("migrate")
    await b._release()
    state = parse_lease(store.get())
    from yacron2.leadership import REBOOT_RAN_KEY, decode_reboot_ran

    assert state.holder is None  # released
    _, jobs = decode_reboot_ran(state.annotations.get(REBOOT_RAN_KEY))
    assert jobs == {"migrate"}  # annotation preserved


async def test_release_does_not_restamp_stale_reboot_ran_under_new_id():
    # Finding 8: one config edit both redefines the @reboot job (job-set id
    # v1 -> v2) and touches the cluster section (or a kubeconfig cert rotation
    # lands the same reload), so start_stop_cluster stops the holder in the
    # SAME iteration the live id changed -- no renew round in between
    # re-scopes the cache. _release must not launder the ran-set observed
    # under v1 by re-stamping it with the live v2 id: every later observe
    # would adopt that pairing as genuine and the redefined one-shot would be
    # retired without ever running, on every node, forever.
    from yacron2.leadership import REBOOT_RAN_KEY, decode_reboot_ran

    live = {"id": "v1:job"}
    store = _FakeApiStore()
    b = _store_backend(store, "node-a#1")
    b.get_job_set_id = lambda: live["id"]
    await b._renew_once()  # acquire the lease under v1
    await b.mark_reboot_ran("migrate")  # ran + persisted under v1
    # a later renew round re-observes the persisted set from the store, so it
    # now lives in _reboot_ran scoped to v1 (the finding's precondition: the
    # one-shot "already ran", recorded in the annotation under the old id).
    await b._renew_once()
    live["id"] = "v2:new"  # reload redefines the job...
    await b._release()  # ...and stops the manager before any renew round
    state = parse_lease(store.get())
    assert state.holder is None  # released
    jsid, jobs = decode_reboot_ran(state.annotations.get(REBOOT_RAN_KEY))
    # the stored (v1, {migrate}) pairing is preserved VERBATIM, not re-stamped
    # under v2 -- so an observer under v2 ignores it as an older config's.
    assert jsid == "v1:job" and jobs == {"migrate"}
    # a peer booting under v2 must see the one-shot as NOT run yet.
    peer = _store_backend(store, "node-b#2")
    peer.get_job_set_id = lambda: "v2:new"
    await peer._renew_once()  # takes over the released lease, folds the store
    assert peer.reboot_ran("migrate") is False


# --- F10: @reboot-ran job-set re-stamping through the real _renew_once -----


async def test_renew_once_rescopes_reboot_ran_on_job_set_change():
    # F10: drive the @reboot-ran job-set scoping through the REAL _renew_once /
    # Lease write path (the base LeaseBackend scoping is unit-tested in
    # isolation; this covers the kubernetes wiring). A reload that redefines an
    # @reboot job changes the live id WITHOUT rebuilding the backend; the next
    # round must NOT re-stamp the stale mark under the new id (which would
    # suppress the redefined one-shot cluster-wide).
    from yacron2.leadership import REBOOT_RAN_KEY, decode_reboot_ran

    live = {"id": "v1:job"}
    store = _FakeApiStore()
    b = _store_backend(store, "node-a#1")
    b.get_job_set_id = lambda: live["id"]
    await b._renew_once()  # acquire the lease under v1
    await b.mark_reboot_ran("migrate")  # record + eager-persist under v1
    stored = parse_lease(store.get())
    jsid, jobs = decode_reboot_ran(stored.annotations.get(REBOOT_RAN_KEY))
    assert jsid == "v1:job" and jobs == {"migrate"}
    # reload redefines the @reboot job -> new id; run another round.
    live["id"] = "v2:new"
    await b._renew_once()
    stored = parse_lease(store.get())
    jsid, _jobs = decode_reboot_ran(stored.annotations.get(REBOOT_RAN_KEY))
    # the stale v1 mark is carried forward UNCHANGED (still tagged v1:job), NOT
    # re-stamped under v2 -- so a node observing it under v2 ignores it.
    assert jsid == "v1:job"
    # and the read path reports the redefined one-shot as NOT run under v2.
    assert b.reboot_ran("migrate") is False


# --- F08: a failed start() leaks nothing (cleans up its half-started state) -


async def test_start_cleans_up_transport_when_setup_fails(monkeypatch):
    # F08: a failed start() (the transport's setup() raises -- a transient
    # apiserver/credential error) must cancel any task and close the transport,
    # leaving NOTHING for the caller to leak: start() never returns, so
    # start_stop_cluster never stores the manager to stop() it. Mirrors the
    # etcd test_start_closes_session_when_authenticate_fails.
    b = _backend()
    monkeypatch.setattr(b, "_native_available", lambda: False)  # -> HTTP

    async def boom(self):
        raise RuntimeError("setup failed")

    monkeypatch.setattr(_K8sHttpTransport, "setup", boom)
    with pytest.raises(RuntimeError):
        await b.start()
    assert b._transport is None  # closed and cleared: no leaked session/task
    assert b._task is None


# --- F05: an in-place TLS cert/CA rotation is detected for a rebuild --------


def test_tls_files_changed_detects_in_place_rotation(tmp_path):
    # F05: the SSLContext is built once at setup() and never reloaded, so the
    # backend snapshots its on-disk TLS files and reports a change -- letting
    # start_stop_cluster rebuild it on a cert-manager/Vault in-place rotation.
    b = _backend()
    ca = tmp_path / "ca.crt"
    ca.write_text("old-ca")
    b._record_tls_files([str(ca)])
    assert b.tls_files_changed() is False
    ca.write_text("rotated-new-ca-bytes")  # in-place rotation (size/mtime)
    assert b.tls_files_changed() is True


def test_tls_files_changed_false_when_nothing_tracked():
    # embedded -data creds / insecure mode: nothing on disk to rotate.
    b = _backend()
    assert b.tls_files_changed() is False  # nothing recorded yet
    b._record_tls_files([None, ""])  # None/empty entries dropped
    assert b._tls_files == []
    assert b.tls_files_changed() is False


def test_k8s_is_self_demoted_holder():
    # F14: raw leadership flag set but the monotonic fence lapsed = the
    # self-demotion window (still quorate, still names self), which
    # is_available_leader treats as the never-skip owner.
    b = _backend()
    assert b._is_self_demoted_holder() is False  # fresh: never held
    b._is_leader = True
    b._leader_until_mono = time.monotonic() - 1  # fence lapsed
    assert b.is_leader() is False
    assert b._is_self_demoted_holder() is True
    b._leader_until_mono = time.monotonic() + 100  # fence valid: real leader
    assert b._is_self_demoted_holder() is False
    b._is_leader = False
    assert b._is_self_demoted_holder() is False
