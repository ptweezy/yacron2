"""Config parsing/validation for the pluggable leadership backends.

Covers the loosened cluster schema (listen/tls/peers no longer hard-required),
the backend dispatch in _build_cluster_config, and each lease backend's
defaults, validation, and secret resolution.
"""

import os

import pytest

from yacron2.config import (
    DEFAULT_ETCD,
    DEFAULT_FILESYSTEM,
    DEFAULT_K8S,
    ConfigError,
    _redact_userinfo,
    _validate_cross_sections,
    cluster_config_warnings,
    parse_config_string,
    parse_config_with_sources,
)


def _cluster(yaml):
    return parse_config_string(yaml, "").cluster_config


def _gossip(peers_yaml, listen="0.0.0.0:8443", node="node-a", extra=""):
    return _cluster(
        "cluster:\n"
        "  listen: '" + listen + "'\n"
        "  nodeName: " + node + "\n"
        "  tls:\n    ca: /ca\n    cert: /cert\n    key: /key\n" + extra
        + "  peers:\n" + peers_yaml
    )


# --- backend defaulting ---------------------------------------------------


def test_backend_defaults_to_gossip():
    cfg = _cluster(
        "cluster:\n"
        "  listen: '0.0.0.0:8443'\n"
        "  tls:\n"
        "    ca: /ca\n"
        "    cert: /cert\n"
        "    key: /key\n"
        "  peers:\n"
        "    - host: b:8443\n"
    )
    assert cfg["backend"] == "gossip"


def test_gossip_requires_transport_keys():
    # listen/tls/peers are schema-optional now, but gossip still requires them
    for missing in ("listen", "tls", "peers"):
        lines = {
            "listen": "  listen: '0.0.0.0:8443'\n",
            "tls": "  tls:\n    ca: /ca\n    cert: /cert\n    key: /key\n",
            "peers": "  peers:\n    - host: b:8443\n",
        }
        del lines[missing]
        yaml = "cluster:\n  backend: gossip\n" + "".join(lines.values())
        with pytest.raises(ConfigError, match=missing):
            _cluster(yaml)


# --- kubernetes -----------------------------------------------------------

_K8S = "cluster:\n  backend: kubernetes\n  nodeName: node-a\n"


def test_kubernetes_defaults_filled():
    cfg = _cluster(_K8S)
    k8s = cfg["kubernetes"]
    assert k8s["leaseName"] == DEFAULT_K8S["leaseName"]
    assert k8s["leaseDurationSeconds"] == DEFAULT_K8S["leaseDurationSeconds"]
    assert k8s["identity"] == "node-a"  # defaulted from nodeName
    assert cfg["electLeader"] is True  # lease backend implies leadership


def test_kubernetes_identity_override():
    cfg = _cluster(_K8S + "  kubernetes:\n    identity: pod-7\n")
    assert cfg["kubernetes"]["identity"] == "pod-7"


def test_kubernetes_rejects_spread():
    with pytest.raises(ConfigError, match="spread"):
        _cluster(_K8S + "  distribution: spread\n")


def test_kubernetes_duration_must_exceed_renew():
    yaml = _K8S + (
        "  kubernetes:\n"
        "    leaseDurationSeconds: 10\n"
        "    renewDeadlineSeconds: 10\n"
    )
    with pytest.raises(ConfigError, match="greater than"):
        _cluster(yaml)


def test_kubernetes_renew_must_be_positive():
    yaml = _K8S + (
        "  kubernetes:\n"
        "    leaseDurationSeconds: 5\n"
        "    renewDeadlineSeconds: 0\n"
    )
    with pytest.raises(ConfigError, match="renewDeadlineSeconds"):
        _cluster(yaml)


def test_kubernetes_retry_must_be_positive():
    yaml = _K8S + "  kubernetes:\n    retryPeriodSeconds: 0\n"
    with pytest.raises(ConfigError, match="retryPeriodSeconds"):
        _cluster(yaml)


def test_kubernetes_retry_must_be_less_than_renew():
    # client-go's third leaderelection invariant: with retry >= renew a holder
    # cannot renew before the next attempt is due, so it lapses out of the
    # lease every cycle and no Leader job runs stably. retry == renew is the
    # boundary case and must also be rejected.
    yaml = _K8S + (
        "  kubernetes:\n"
        "    leaseDurationSeconds: 15\n"
        "    renewDeadlineSeconds: 10\n"
        "    retryPeriodSeconds: 10\n"
    )
    with pytest.raises(ConfigError, match="retryPeriodSeconds"):
        _cluster(yaml)


def test_kubernetes_default_retry_renew_valid():
    # the shipped defaults (retry 2 < renew 10 < duration 15) must pass.
    cfg = _cluster(_K8S)
    k8s = cfg["kubernetes"]
    assert k8s["retryPeriodSeconds"] < k8s["renewDeadlineSeconds"]


# --- etcd -----------------------------------------------------------------

_ETCD = (
    "cluster:\n"
    "  backend: etcd\n"
    "  nodeName: node-a\n"
    "  etcd:\n"
    "    endpoints:\n"
    "      - http://127.0.0.1:2379\n"
)

# auth credentials must never cross a plaintext wire, so a config that sets a
# username/password is only valid with https endpoints (see
# _build_etcd_cluster_config); the password-resolution tests use this base.
_ETCD_TLS = (
    "cluster:\n"
    "  backend: etcd\n"
    "  nodeName: node-a\n"
    "  etcd:\n"
    "    endpoints:\n"
    "      - https://127.0.0.1:2379\n"
)


def test_etcd_defaults_filled():
    cfg = _cluster("cluster:\n  backend: etcd\n  nodeName: node-a\n")
    etcd = cfg["etcd"]
    assert etcd["endpoints"] == DEFAULT_ETCD["endpoints"]
    assert etcd["electionName"] == DEFAULT_ETCD["electionName"]
    assert etcd["ttl"] == DEFAULT_ETCD["ttl"]
    assert etcd["resolved_password"] is None
    assert cfg["electLeader"] is True


def test_etcd_rejects_spread():
    with pytest.raises(ConfigError, match="spread"):
        _cluster(_ETCD + "  distribution: spread\n")


def test_etcd_ttl_must_be_positive():
    yaml = (
        "cluster:\n"
        "  backend: etcd\n"
        "  etcd:\n"
        "    endpoints:\n"
        "      - http://127.0.0.1:2379\n"
        "    ttl: 0\n"
    )
    with pytest.raises(ConfigError, match="ttl"):
        _cluster(yaml)


def test_etcd_ttl_floor_rejects_unleadable_small_ttl():
    # A ttl below the floor passes the > 0 check but makes the leader deadline
    # (ttl - 1s skew) collapse to <= the keepalive period, so a node that wins
    # the campaign immediately treats its own lease as expired and is_leader()
    # is permanently False -- at-most-once silently degrades to at-most-zero.
    # ttl 1 and 2 must be rejected; 3 (the floor) must be accepted.
    def _yaml(ttl):
        return (
            "cluster:\n"
            "  backend: etcd\n"
            "  etcd:\n"
            "    endpoints:\n"
            "      - http://127.0.0.1:2379\n"
            "    ttl: {}\n".format(ttl)
        )

    for bad in (1, 2):
        with pytest.raises(ConfigError, match="ttl must be >= 3"):
            _cluster(_yaml(bad))
    assert _cluster(_yaml(3))["etcd"]["ttl"] == 3


def test_etcd_rejects_malformed_endpoint():
    # missing host, bad scheme, non-numeric port, out-of-range port, and port 0
    # must all raise a clean ConfigError (never a raw ValueError from
    # urlparse().port). A MISSING port is allowed (defaults to scheme port);
    # see test_etcd_accepts_portless_endpoint.
    bad_endpoints = (
        "127.0.0.1:2379",  # no scheme
        "ftp://h:2379",
        "http://h:notaport",
        "http://h:99999",
        "http://h:0",
    )
    for bad in bad_endpoints:
        yaml = (
            "cluster:\n"
            "  backend: etcd\n"
            "  etcd:\n"
            "    endpoints:\n"
            "      - " + bad + "\n"
        )
        with pytest.raises(ConfigError, match="endpoints"):
            _cluster(yaml)


def test_etcd_accepts_portless_endpoint():
    # a host without an explicit port is valid (it defaults to the scheme's
    # port, e.g. https behind a 443 ingress); the old validation wrongly
    # required a port.
    yaml = (
        "cluster:\n"
        "  backend: etcd\n"
        "  etcd:\n"
        "    endpoints:\n"
        "      - https://etcd.svc.cluster.local\n"
    )
    cfg = _cluster(yaml)
    assert cfg["etcd"]["endpoints"] == ["https://etcd.svc.cluster.local"]


def test_etcd_rejects_url_embedded_credentials():
    # credentials in the URL would be logged in cleartext and sent as Basic
    # auth, bypassing the username/password https-only guard.
    yaml = (
        "cluster:\n"
        "  backend: etcd\n"
        "  etcd:\n"
        "    endpoints:\n"
        "      - https://user:s3cret@etcd:2379\n"
    )
    with pytest.raises(ConfigError, match="credentials"):
        _cluster(yaml)


def test_etcd_credentialed_bad_port_does_not_leak_password():
    # M6: an endpoint with BOTH embedded credentials AND a bad port must be
    # rejected with the password REDACTED. It previously short-circuited into
    # the scheme/port branch, which printed the raw endpoint (cleartext
    # password) into the ConfigError that reaches stderr/logs/CI.
    yaml = (
        "cluster:\n"
        "  backend: etcd\n"
        "  etcd:\n"
        "    endpoints:\n"
        "      - https://user:s3cretpw@etcd:70000\n"
    )
    with pytest.raises(ConfigError) as exc:
        _cluster(yaml)
    assert "s3cretpw" not in str(exc.value)
    assert "***" in str(exc.value)


def test_kubernetes_rejects_stray_etcd_store_block():
    # M5: an etcd: store block under backend: kubernetes is silently ignored by
    # the k8s builder, discarding the operator's intended endpoints/TLS/creds
    # and arbitrating leadership against the default store. Reject it loudly.
    yaml = (
        "cluster:\n"
        "  backend: kubernetes\n"
        "  etcd:\n"
        "    endpoints:\n"
        "      - https://etcd:2379\n"
    )
    with pytest.raises(ConfigError, match="cluster.etcd is configured"):
        _cluster(yaml)


def test_etcd_rejects_stray_kubernetes_store_block():
    # M5: the symmetric case -- a kubernetes: block under backend: etcd.
    yaml = (
        "cluster:\n"
        "  backend: etcd\n"
        "  etcd:\n"
        "    endpoints:\n"
        "      - https://etcd:2379\n"
        "  kubernetes:\n"
        "    leaseName: yacron2-leader\n"
    )
    with pytest.raises(ConfigError, match="cluster.kubernetes is configured"):
        _cluster(yaml)


def test_kubernetes_rejects_lease_name_with_path_chars():
    # L2: leaseName is spliced into the apiserver URL path; a '/' (or other
    # path metacharacter) would retarget the request to a different resource.
    # Reject non-RFC1123 names at config load, not silently at runtime.
    yaml = (
        "cluster:\n"
        "  backend: kubernetes\n"
        "  kubernetes:\n"
        "    leaseName: team/leader\n"
    )
    with pytest.raises(ConfigError, match="leaseName"):
        _cluster(yaml)


def test_etcd_password_from_value():
    yaml = _ETCD_TLS + (
        "    username: root\n"
        "    password:\n"
        "      value: s3cret\n"
    )
    cfg = _cluster(yaml)
    assert cfg["etcd"]["resolved_password"] == "s3cret"
    assert cfg["etcd"]["username"] == "root"


def test_etcd_password_from_env(monkeypatch):
    monkeypatch.setenv("ETCD_PW", "from-env")
    yaml = _ETCD_TLS + "    password:\n      fromEnvVar: ETCD_PW\n"
    assert _cluster(yaml)["etcd"]["resolved_password"] == "from-env"


def test_etcd_auth_requires_https():
    # credentials over a plaintext endpoint would be sniffable: reject the
    # combination at load time (the default endpoint is http:// but needs no
    # auth, so it stays valid without credentials).
    yaml = _ETCD + "    username: root\n    password:\n      value: s3cret\n"
    with pytest.raises(ConfigError, match="cleartext"):
        _cluster(yaml)


def test_etcd_auth_rejects_mixed_scheme_endpoints():
    # a mixed http/https list still lets the _post failover loop POST
    # credentials over the plaintext member, so it is rejected too.
    yaml = (
        "cluster:\n"
        "  backend: etcd\n"
        "  nodeName: node-a\n"
        "  etcd:\n"
        "    endpoints:\n"
        "      - https://etcd-1:2379\n"
        "      - http://etcd-2:2379\n"
        "    username: root\n"
        "    password:\n"
        "      value: s3cret\n"
    )
    with pytest.raises(ConfigError, match="http://etcd-2:2379"):
        _cluster(yaml)


def test_etcd_password_empty_source_fails_closed(monkeypatch):
    monkeypatch.delenv("ETCD_PW_MISSING", raising=False)
    yaml = _ETCD + "    password:\n      fromEnvVar: ETCD_PW_MISSING\n"
    with pytest.raises(ConfigError, match="empty secret"):
        _cluster(yaml)


def test_etcd_password_from_file(tmp_path):
    secret = tmp_path / "pw"
    secret.write_text("file-secret\n")
    yaml = _ETCD_TLS + "    password:\n      fromFile: {}\n".format(secret)
    assert _cluster(yaml)["etcd"]["resolved_password"] == "file-secret"


def test_etcd_password_from_missing_file_fails(tmp_path):
    missing = tmp_path / "nope"
    yaml = _ETCD + "    password:\n      fromFile: {}\n".format(missing)
    with pytest.raises(ConfigError, match="could not be read"):
        _cluster(yaml)


def test_etcd_tls_cert_requires_key():
    # a client cert without its key silently degrades mTLS to one-way TLS.
    yaml = _ETCD_TLS + "    tls:\n      cert: /c\n"
    with pytest.raises(ConfigError, match="cert and .*key must be set"):
        _cluster(yaml)
    yaml2 = _ETCD_TLS + "    tls:\n      key: /k\n"
    with pytest.raises(ConfigError, match="cert and .*key must be set"):
        _cluster(yaml2)
    # both together is fine
    both = _ETCD_TLS + "    tls:\n      cert: /c\n      key: /k\n"
    assert _cluster(both)["etcd"]["tls"]["cert"] == "/c"


def test_etcd_tls_without_https_endpoint_rejected():
    # TLS material set but every endpoint is plaintext -> silently ignored;
    # refuse rather than send cleartext.
    yaml = _ETCD + "    tls:\n      ca: /ca\n"
    with pytest.raises(ConfigError, match="no endpoint is https"):
        _cluster(yaml)


def test_etcd_username_requires_password():
    yaml = _ETCD_TLS + "    username: root\n"
    with pytest.raises(ConfigError, match="no password is configured"):
        _cluster(yaml)


def test_kubernetes_apiserver_must_be_https():
    # an http:// apiserver would leak the ServiceAccount bearer token.
    yaml = _K8S + "  kubernetes:\n    apiServer: http://kube-proxy:8080\n"
    with pytest.raises(ConfigError, match="apiServer must be an https"):
        _cluster(yaml)
    # https is accepted
    ok = _K8S + "  kubernetes:\n    apiServer: https://api:6443\n"
    assert _cluster(ok)["kubernetes"]["apiServer"] == "https://api:6443"


def test_gossip_fqdn_self_listing_warns_degenerate_size():
    # a 3-node config where one peer is this node by FQDN (short nodeName) is
    # really 2 nodes at runtime; warn so it is not discovered as flapping.
    cfg = _cluster(
        "cluster:\n"
        "  listen: '0.0.0.0:8443'\n"
        "  nodeName: node-a\n"
        "  electLeader: true\n"
        "  tls:\n    ca: /ca\n    cert: /cert\n    key: /key\n"
        "  peers:\n"
        "    - host: node-a.internal:8443\n"
        "    - host: node-b:8443\n"
    )
    warnings = cluster_config_warnings(cfg)
    assert any("FQDN" in w for w in warnings)


def test_gossip_localhost_self_listing_warns_degenerate_size():
    # SELF-BY-IP hardening (config-time half): a "localhost" peer on our own
    # port cannot be dropped without DNS resolution, but it can never be
    # another host either -- a 3-node config carrying it is really 2 nodes at
    # runtime, the degenerate mode the size==2 refusal exists to catch. Warn,
    # like the FQDN self-listing.
    cfg = _cluster(
        "cluster:\n"
        "  listen: '0.0.0.0:8443'\n"
        "  nodeName: node-a\n"
        "  electLeader: true\n"
        "  tls:\n    ca: /ca\n    cert: /cert\n    key: /key\n"
        "  peers:\n"
        "    - host: localhost:8443\n"
        "    - host: node-b:8443\n"
    )
    warnings = cluster_config_warnings(cfg)
    assert any("loopback" in w for w in warnings)


def test_gossip_family_mismatched_loopback_warns():
    # a loopback literal that _is_self_listed could not drop as unambiguous
    # (here: v6 loopback under a v4-only wildcard bind) still points at this
    # host at best, so it gets the same advisory.
    cfg = _cluster(
        "cluster:\n"
        "  listen: '0.0.0.0:8443'\n"
        "  nodeName: node-a\n"
        "  electLeader: true\n"
        "  tls:\n    ca: /ca\n    cert: /cert\n    key: /key\n"
        "  peers:\n"
        "    - host: '[::1]:8443'\n"
        "    - host: node-b:8443\n"
    )
    warnings = cluster_config_warnings(cfg)
    assert any("loopback" in w for w in warnings)


def test_gossip_loopback_advisory_scope():
    # no advisory for a loopback peer on a DIFFERENT port (a colocated second
    # daemon is a legitimate member -- the pattern the test suite itself
    # uses), nor when enough real peers remain (> 2 nodes after discounting).
    off_port = _cluster(
        "cluster:\n"
        "  listen: '0.0.0.0:8443'\n"
        "  nodeName: node-a\n"
        "  electLeader: true\n"
        "  tls:\n    ca: /ca\n    cert: /cert\n    key: /key\n"
        "  peers:\n"
        "    - host: localhost:9443\n"
        "    - host: node-b:8443\n"
    )
    assert not any(
        "loopback" in w for w in cluster_config_warnings(off_port)
    )
    big = _cluster(
        "cluster:\n"
        "  listen: '0.0.0.0:8443'\n"
        "  nodeName: node-a\n"
        "  electLeader: true\n"
        "  tls:\n    ca: /ca\n    cert: /cert\n    key: /key\n"
        "  peers:\n"
        "    - host: localhost:8443\n"
        "    - host: node-b:8443\n"
        "    - host: node-c:8443\n"
        "    - host: node-d:8443\n"
    )
    assert not any("loopback" in w for w in cluster_config_warnings(big))


# --- warnings -------------------------------------------------------------


def test_no_warnings_for_lease_backends():
    # the gossip-only even-size / distribution advisories must not fire (and
    # must not KeyError on the absent peers list) for a lease backend.
    assert cluster_config_warnings(_cluster(_K8S)) == []
    assert cluster_config_warnings(_cluster(_ETCD)) == []


def test_etcd_warns_on_small_ttl_tight_request_budget():
    # A small etcd ttl collapses the per-request renew timeout (request_timeout
    # ~= round_deadline/5); below ~1s it can fall under a real cross-AZ/region
    # round-trip, so every renew POST times out and the node treats a reachable
    # etcd as unreachable (Leader jobs fail closed, never recovering at boot).
    # It is the operator's explicit ttl choice and a local etcd is fine, so
    # warn rather than reject.
    cfg = _cluster(
        "cluster:\n"
        "  backend: etcd\n"
        "  nodeName: node-a\n"
        "  etcd:\n"
        "    ttl: 5\n"
        "    endpoints:\n"
        "      - http://127.0.0.1:2379\n"
    )
    assert any(
        "per-request timeout" in w and "cluster.etcd.ttl" in w
        for w in cluster_config_warnings(cfg)
    )


def test_etcd_no_tight_budget_warning_at_default_ttl():
    # the default ttl (15) leaves a comfortable ~1.8s per-POST budget; silent.
    assert not any(
        "per-request timeout" in w
        for w in cluster_config_warnings(_cluster(_ETCD))
    )


def test_etcd_warns_on_gossip_only_keys():
    # a lease config carrying gossip transport keys (e.g. copied from a
    # gossip example) silently ignores them; warn -- and call out that
    # cluster.tls does NOT secure the lease store (the dangerous false belief).
    cfg = _cluster(
        "cluster:\n"
        "  backend: etcd\n"
        "  nodeName: node-a\n"
        "  listen: '0.0.0.0:8443'\n"
        "  tls:\n    ca: /ca\n    cert: /cert\n    key: /key\n"
        "  peers:\n    - host: b:8443\n"
        "  etcd:\n    endpoints:\n      - http://127.0.0.1:2379\n"
    )
    warnings = cluster_config_warnings(cfg)
    assert any("ignored by the 'etcd' backend" in w for w in warnings)
    assert any("does NOT secure the lease store" in w for w in warnings)
    # the backend still builds and is unaffected by the inert keys.
    assert cfg["etcd"]["endpoints"] == ["http://127.0.0.1:2379"]


def test_kubernetes_warns_on_gossip_only_keys():
    cfg = _cluster(
        "cluster:\n"
        "  backend: kubernetes\n"
        "  nodeName: node-a\n"
        "  peers:\n    - host: b:8443\n"
    )
    warnings = cluster_config_warnings(cfg)
    assert any(
        "cluster.peers" in w and "ignored by the 'kubernetes' backend" in w
        for w in warnings
    )


def test_lease_warns_on_explicit_elect_leader_false():
    # a lease backend always implies leadership; an explicit electLeader:false
    # is contradictory and silently overridden -- surface the swallowed value
    # while still honouring the override (the gate stays on).
    cfg = _cluster(
        "cluster:\n"
        "  backend: kubernetes\n"
        "  nodeName: node-a\n"
        "  electLeader: false\n"
    )
    assert cfg["electLeader"] is True  # override still wins
    assert any(
        "electLeader: false is ignored" in w
        for w in cluster_config_warnings(cfg)
    )


def test_unknown_backend_rejected():
    with pytest.raises(ConfigError):
        _cluster("cluster:\n  backend: zookeeper\n")


# --- F17: kubernetes lease-timing sum invariant ---------------------------


def test_kubernetes_rejects_renew_plus_retry_ge_duration():
    # renewDeadline + retryPeriod must fit inside leaseDuration, or the
    # holder's worst-case refresh gap exceeds the lease lifetime and it lapses
    # out of the lease every cycle even when sole and healthy (no stable
    # leader). 11+10=21 >= 12 passes the pairwise checks (10<11, 11<12) but not
    # the sum.
    with pytest.raises(ConfigError, match="must be less than"):
        _cluster(
            "cluster:\n  backend: kubernetes\n  nodeName: node-a\n"
            "  kubernetes:\n"
            "    leaseDurationSeconds: 12\n"
            "    renewDeadlineSeconds: 11\n"
            "    retryPeriodSeconds: 10\n"
        )


def test_kubernetes_defaults_satisfy_sum_invariant():
    # the shipped defaults (15 / 10 / 2 -> 12 < 15) must pass.
    cfg = _cluster(_K8S)
    assert cfg["kubernetes"]["leaseDurationSeconds"] == 15


# --- F11: IPv6 host:port validation ---------------------------------------


def test_gossip_rejects_bare_ipv6_peer():
    # a bare (unbracketed) IPv6 peer host would be mis-split (host=2001:db8:,
    # port=1) and silently dropped from quorum; require the bracketed form.
    with pytest.raises(ConfigError, match="IPv6"):
        _gossip("    - host: 2001:db8::5\n")


def test_gossip_rejects_bare_ipv6_listen():
    with pytest.raises(ConfigError, match="IPv6"):
        _gossip("    - host: node-b:8443\n", listen="2001:db8::1")


def test_gossip_accepts_bracketed_ipv6():
    cfg = _gossip(
        "    - host: '[2001:db8::5]:8443'\n", listen="[2001:db8::1]:8443"
    )
    assert cfg["peers"][0]["host"] == "[2001:db8::5]:8443"


# --- F10: userinfo redaction with '@' in the password ---------------------


def test_redact_userinfo_splits_on_last_at():
    # a password containing '@' must not leak its tail: split on the LAST '@'
    # (as urlparse does), not the first.
    redacted = _redact_userinfo("https://user:p@ss@host:2379")
    assert redacted == "https://***@host:2379"
    assert "p@ss" not in redacted and "ss@host" not in redacted


def test_redact_userinfo_no_userinfo_unchanged():
    assert _redact_userinfo("https://host:2379") == "https://host:2379"


# --- F04: scheme-less userinfo + apiServer redaction ----------------------


def test_redact_userinfo_schemeless_is_redacted():
    # F04: a scheme-less user:pass@host -- which urlparse misreads as scheme
    # 'user' with no username -- must still be redacted, not echoed verbatim.
    assert (
        _redact_userinfo("user:s3cret@etcd.internal:2379")
        == "***@etcd.internal:2379"
    )
    assert "s3cret" not in _redact_userinfo("user:s3cret@etcd.internal:2379")
    # the LAST '@' rule still applies without a scheme
    assert _redact_userinfo("user:p@ss@host:2379") == "***@host:2379"


def test_etcd_rejects_schemeless_credentialed_endpoint_without_leak():
    # F04: a scheme-less endpoint with embedded credentials must be rejected
    # AND its password must not appear in the ConfigError (the reload loop logs
    # str(err); the old parsed.username check missed the scheme-less form and
    # the fall-through scheme error leaked cleartext).
    yaml = (
        "cluster:\n"
        "  backend: etcd\n"
        "  etcd:\n"
        "    endpoints:\n"
        "      - user:s3cret@etcd.internal:2379\n"
    )
    with pytest.raises(ConfigError) as ei:
        _cluster(yaml)
    assert "s3cret" not in str(ei.value)


def test_k8s_apiserver_credentialed_error_redacts_password():
    # F04: a non-https apiServer is rejected; if it carries embedded userinfo
    # the password must be redacted from the ConfigError (it was echoed raw via
    # {!r} before).
    yaml = _K8S + (
        "  kubernetes:\n    apiServer: http://tok:s3cret@kube-proxy:8080\n"
    )
    with pytest.raises(ConfigError) as ei:
        _cluster(yaml)
    assert "apiServer must be an https" in str(ei.value)
    assert "s3cret" not in str(ei.value)


# --- filesystem (shared-mount) backend --------------------------------------

_FS = (
    "cluster:\n"
    "  backend: filesystem\n"
    "  nodeName: node-a\n"
    "  filesystem:\n"
    "    path: /mnt/shared/yacron2\n"
)


def test_filesystem_defaults_filled():
    cfg = _cluster(_FS)
    fsb = cfg["filesystem"]
    assert fsb["path"] == "/mnt/shared/yacron2"
    assert fsb["electionName"] == DEFAULT_FILESYSTEM["electionName"]
    assert fsb["ttl"] == DEFAULT_FILESYSTEM["ttl"]
    assert fsb["deploymentId"] is None
    assert fsb["topology"] == "auto"
    assert cfg["electLeader"] is True  # lease backend implies leadership


def test_filesystem_requires_path():
    with pytest.raises(ConfigError, match="filesystem.path"):
        _cluster("cluster:\n  backend: filesystem\n")
    with pytest.raises(ConfigError, match="filesystem.path"):
        _cluster(
            "cluster:\n  backend: filesystem\n"
            "  filesystem:\n    path: '  '\n"
        )


def test_filesystem_ttl_floor():
    # same rationale as etcd's: below 3s the leader window collapses under
    # the renew cadence plus the clock-skew margin.
    with pytest.raises(ConfigError, match="ttl must be >= 3"):
        _cluster(_FS + "    ttl: 2\n")


def test_filesystem_election_name_nonempty():
    with pytest.raises(ConfigError, match="electionName"):
        _cluster(_FS + "    electionName: ' '\n")


def test_filesystem_rejects_spread():
    with pytest.raises(ConfigError, match="spread"):
        _cluster(_FS + "  distribution: spread\n")


def test_filesystem_block_rejected_under_other_backends():
    # a filesystem: block under another backend would be silently ignored,
    # arbitrating leadership against an unintended store -- refused loudly.
    yaml = (
        "cluster:\n"
        "  backend: etcd\n"
        "  filesystem:\n"
        "    path: /mnt/shared\n"
    )
    with pytest.raises(ConfigError, match="cluster.filesystem"):
        _cluster(yaml)


def test_filesystem_gossip_only_keys_are_advisories():
    cfg = _cluster(_FS + "  interval: 5\n  electLeader: false\n")
    warnings = cluster_config_warnings(cfg)
    assert any("interval" in w for w in warnings)
    assert any("electLeader" in w for w in warnings)
    assert cfg["electLeader"] is True  # the override stands


# --- non-finite floats in the state / filesystem range checks ---------------
# strictyaml's Float() accepts '.nan' and overflow literals like '1e309'
# (== inf), and 'nan < floor' is False, so a floor-only check waves them
# through into the lease/TTL arithmetic (a nan lease is never validly held; an
# inf one never expires). The validators must reject non-finite values.


def test_filesystem_ttl_rejects_non_finite():
    for bad in (".nan", "1e309"):
        with pytest.raises(ConfigError, match="ttl must be >= 3"):
            _cluster(_FS + "    ttl: {}\n".format(bad))


def test_state_floats_reject_non_finite():
    cases = (
        "  slotTtlSeconds: {}\n",
        "  maxOpsPerSecond: {}\n",
        "  jobApi:\n    lockTtlSeconds: {}\n",
    )
    for extra in cases:
        for bad in (".nan", "1e309"):
            with pytest.raises(ConfigError, match="finite"):
                parse_config_string(
                    "state:\n  path: /x\n" + extra.format(bad), ""
                )


def test_state_floats_accept_finite_values():
    # the pre-existing valid range must stay valid (no over-tightening).
    cfg = parse_config_string(
        "state:\n"
        "  path: /x\n"
        "  slotTtlSeconds: 30\n"
        "  maxOpsPerSecond: 0\n"
        "  jobApi:\n"
        "    lockTtlSeconds: 5.5\n",
        "",
    ).state_config
    assert cfg["slotTtlSeconds"] == 30
    assert cfg["jobApi"]["lockTtlSeconds"] == 5.5


# --- state.jobApi.listen port validation ------------------------------------
# an unvalidated port passed config validation and then blew up the API
# startup at runtime (urlparse().port raises), permanently disabling the
# loopback endpoint; the config load must catch it instead.


def test_jobapi_listen_bad_port_rejected():
    for bad in ("127.0.0.1:99999", "127.0.0.1:abc"):
        with pytest.raises(ConfigError, match="0-65535"):
            parse_config_string(
                "state:\n  path: /x\n  jobApi:\n"
                "    listen: '{}'\n".format(bad),
                "",
            )


def test_jobapi_listen_valid_forms_parse():
    # bare host:port, a portless host (ephemeral bind, same as the default)
    # and the http:// URL form must all stay valid.
    for good in ("127.0.0.1:8080", "127.0.0.1", "http://localhost:9000"):
        cfg = parse_config_string(
            "state:\n  path: /x\n  jobApi:\n"
            "    listen: '{}'\n".format(good),
            "",
        ).state_config
        assert cfg["jobApi"]["listen"] == good


def test_jobapi_listen_explicit_port_zero_parses():
    # an explicit :0 is the ephemeral-bind idiom, identical at runtime to
    # omitting the port (jobapi._bind_target maps a missing port to 0);
    # rejecting it broke configs that worked before the port validation.
    for good in ("127.0.0.1:0", "http://127.0.0.1:0"):
        cfg = parse_config_string(
            "state:\n  path: /x\n  jobApi:\n"
            "    listen: '{}'\n".format(good),
            "",
        ).state_config
        assert cfg["jobApi"]["listen"] == good


# --- DAG config validation (schedule form, retries, name collisions) --------

_STATE = "state:\n  path: /tmp/x\n"


def _xsect(yaml):
    _validate_cross_sections(parse_config_string(_STATE + yaml, ""))


def test_dag_reboot_schedule_rejected():
    # "@reboot" survives _parse_schedule as a plain string, but every DAG
    # scheduling path computes next-fire instants from a CronTab; reject at
    # load rather than crash the scheduler.
    with pytest.raises(ConfigError, match="not supported for dags"):
        parse_config_string(
            "dags:\n  - name: d\n    schedule: '@reboot'\n    tasks:\n"
            "      - id: t\n        command: 'echo'\n",
            "",
        )


def test_dag_alias_schedules_parse():
    # aliases the crontab library expands into a real CronTab stay valid --
    # the rejection is structural (whatever stays a string), not a blacklist.
    for alias in ("@daily", "@hourly"):
        cfg = parse_config_string(
            "dags:\n  - name: d\n    schedule: '{}'\n    tasks:\n"
            "      - id: t\n        command: 'echo'\n".format(alias),
            "",
        )
        assert cfg.dags[0].schedule_job is not None


def test_dag_task_negative_retries_rejected():
    # the job-level maximumRetries documents -1 as retry-forever; on a dag
    # task a negative value would silently mean ZERO retries, so reject it.
    for bad in (-1, -5):
        with pytest.raises(ConfigError, match="not supported for dag tasks"):
            parse_config_string(
                "dags:\n  - name: d\n    tasks:\n"
                "      - id: t\n        command: 'echo'\n"
                "        retries: {}\n".format(bad),
                "",
            )


def test_dag_task_zero_retries_parses():
    cfg = parse_config_string(
        "dags:\n  - name: d\n    tasks:\n"
        "      - id: t\n        command: 'echo'\n        retries: 0\n",
        "",
    )
    assert cfg.dags[0].tasks[0].spec.max_attempts == 1


def test_job_name_colliding_with_dag_task_template_rejected():
    # dag tasks launch under '<dag>.<taskId>' and share the scheduler's
    # per-name bookkeeping with jobs: a job named 'etl.extract' with
    # concurrencyPolicy Replace would cancel the unrelated in-flight dag
    # task (Forbid silently skips). Rejected at load, naming both parties.
    with pytest.raises(ConfigError, match="collides with dag 'etl'"):
        _xsect(
            "jobs:\n  - name: etl.extract\n    command: echo\n"
            "    schedule: '* * * * *'\n"
            "dags:\n  - name: etl\n    tasks:\n"
            "      - id: extract\n        command: 'echo'\n"
        )


def test_dag_dot_task_ids_colliding_across_dags_rejected():
    # task ids may contain '.', so dag 'a' task 'b.c' and dag 'a.b' task 'c'
    # both mint the template name 'a.b.c'.
    with pytest.raises(ConfigError, match="both launch"):
        _xsect(
            "dags:\n"
            "  - name: a\n    tasks:\n"
            "      - id: b.c\n        command: 'echo'\n"
            "  - name: a.b\n    tasks:\n"
            "      - id: c\n        command: 'echo'\n"
        )


def test_near_miss_job_and_dag_names_still_parse():
    # near-misses must not be caught: a job 'etl-extract' next to dag 'etl'
    # task 'extract', and a job sharing the BARE dag name (the synthetic
    # schedule job is 'dag:'-prefixed and never launched, and dag run/XCom
    # state lives under its own scopes, so bare names never collide).
    _xsect(
        "jobs:\n"
        "  - name: etl-extract\n    command: echo\n"
        "    schedule: '* * * * *'\n"
        "  - name: etl\n    command: echo\n"
        "    schedule: '* * * * *'\n"
        "dags:\n  - name: etl\n    tasks:\n"
        "      - id: extract\n        command: 'echo'\n"
    )


# --- DAG task env_file paths belong to the reload source set ----------------


def test_dag_task_env_file_in_reload_sources(tmp_path):
    # the scheduler stats the source set to skip an unchanged reparse; a dag
    # task's env_file missing from it meant edits (e.g. a rotated credential)
    # never triggered a reload.
    job_env = tmp_path / "job.env"
    task_env = tmp_path / "task.env"
    job_env.write_text("A=1\n")
    task_env.write_text("B=2\n")
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(
        "state:\n  path: /tmp/x\n"
        "jobs:\n  - name: j\n    command: echo\n"
        "    schedule: '* * * * *'\n"
        "    env_file: '{}'\n"
        "dags:\n  - name: d\n    tasks:\n"
        "      - id: t\n        command: 'echo'\n"
        "        env_file: '{}'\n".format(
            job_env.as_posix(), task_env.as_posix()
        )
    )
    _, sources = parse_config_with_sources(str(cfg_file))
    assert os.path.abspath(str(job_env)) in sources
    assert os.path.abspath(str(task_env)) in sources
