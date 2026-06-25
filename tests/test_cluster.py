import asyncio
import datetime
import json
import socket

import aiohttp
import pytest

from yacron2.cluster import (
    STATUS_AGREED,
    STATUS_CONFLICT,
    STATUS_DRIFTED,
    STATUS_SELF,
    STATUS_SYNCING,
    STATUS_UNREACHABLE,
    STATUS_UNTRUSTED,
    ClusterManager,
    ClusterView,
    _hrw_owner,
    _split_host_port,
    elect_available_job_owner,
    elect_available_leader,
    elect_job_owner,
    elect_leader,
    quorum_size,
)
from yacron2.config import DEFAULT_CLUSTER, ConfigError, parse_config_string
from yacron2.fingerprint import SCHEME_VERSION

NOW = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)


# --------------------------------------------------------------------------
# config parsing
# --------------------------------------------------------------------------

CLUSTER_YAML = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
cluster:
  listen: "0.0.0.0:8443"
  tls:
    ca: /etc/yacron2/ca.pem
    cert: /etc/yacron2/node.pem
    key: /etc/yacron2/node.key
  peers:
    - host: yacron-b:8443
    - host: yacron-c:8443
"""


def test_cluster_config_parsed_with_defaults():
    cfg = parse_config_string(CLUSTER_YAML, "").cluster_config
    assert cfg is not None
    assert cfg["listen"] == "0.0.0.0:8443"
    assert [p["host"] for p in cfg["peers"]] == [
        "yacron-b:8443",
        "yacron-c:8443",
    ]
    assert cfg["interval"] == DEFAULT_CLUSTER["interval"]
    assert cfg["driftAfter"] == DEFAULT_CLUSTER["driftAfter"]
    assert cfg["nodeName"]  # defaulted to the hostname


def test_no_cluster_section_is_none():
    y = 'jobs:\n  - name: a\n    command: echo a\n    schedule: "* * * * *"\n'
    assert parse_config_string(y, "").cluster_config is None


def test_cluster_config_rejects_bad_numbers():
    bad = CLUSTER_YAML + "  interval: 0\n"
    with pytest.raises(ConfigError):
        parse_config_string(bad, "")
    bad = CLUSTER_YAML + "  driftAfter: 0\n"
    with pytest.raises(ConfigError):
        parse_config_string(bad, "")


def test_cluster_config_rejects_bad_peer_address():
    # #11: a portless / malformed peer host fails the load with a pointer to
    # the offending value, instead of surfacing later as an opaque poll error.
    bad = CLUSTER_YAML.replace("yacron-b:8443", "yacron-b")  # no port
    with pytest.raises(ConfigError, match="host:port"):
        parse_config_string(bad, "")
    bad = CLUSTER_YAML.replace("yacron-c:8443", "yacron-c:notaport")
    with pytest.raises(ConfigError, match="host:port"):
        parse_config_string(bad, "")


def test_cluster_config_rejects_bad_listen_address():
    bad = CLUSTER_YAML.replace('"0.0.0.0:8443"', '"0.0.0.0"')
    with pytest.raises(ConfigError, match="host:port"):
        parse_config_string(bad, "")


def test_cluster_config_dedups_peers():
    # #10: a repeated peer host collapses to one (ClusterView keys by host), so
    # cluster_size / quorum reflect distinct members rather than the raw count.
    dup = CLUSTER_YAML + "    - host: yacron-b:8443\n"
    cfg = parse_config_string(dup, "").cluster_config
    assert cfg is not None
    assert [p["host"] for p in cfg["peers"]] == [
        "yacron-b:8443",
        "yacron-c:8443",
    ]


def test_cluster_config_excludes_self_listed_peer():
    # a peer entry equal to our own listen address never counts toward
    # agreement, so it is dropped (it would only inflate the quorum threshold).
    y = CLUSTER_YAML + '    - host: "0.0.0.0:8443"\n'
    cfg = parse_config_string(y, "").cluster_config
    assert cfg is not None
    assert [p["host"] for p in cfg["peers"]] == [
        "yacron-b:8443",
        "yacron-c:8443",
    ]


def test_cluster_config_excludes_self_listed_by_hostname_behind_wildcard():
    # the common uniform-peer-list mistake: a node bound to a wildcard listen
    # (0.0.0.0) and self-listed by its nodeName. The literal listen string
    # ("0.0.0.0:8443") does not match "yacron-a:8443", so this used to survive
    # config-time dedup and inflate N -- which, if the self-poll never
    # succeeded (cert SAN / loopback quirk), permanently pinned Leader jobs
    # closed cluster-wide. It is now recognised structurally at load.
    y = CLUSTER_YAML + '    - host: "yacron-a:8443"\n  nodeName: yacron-a\n'
    cfg = parse_config_string(y, "").cluster_config
    assert cfg is not None
    assert [p["host"] for p in cfg["peers"]] == [
        "yacron-b:8443",
        "yacron-c:8443",
    ]


def test_self_listed_behind_wildcard_does_not_bypass_two_node_guard():
    # the same wildcard self-listing must not sneak a 2-effective-node
    # electLeader cluster past the guard: after dropping self there is one real
    # peer (quorum 2 -> both must be up -> strictly worse than one replica).
    y = (
        TWO_NODE_YAML
        + '    - host: "yacron-a:8443"\n'
        + "  nodeName: yacron-a\n"
        + "  electLeader: true\n"
    )
    with pytest.raises(ConfigError, match="strictly worse than a single"):
        parse_config_string(y, "")


def test_is_self_listed_edge_cases():
    from yacron2.config import _is_self_listed

    # exact listen match (any host form)
    assert _is_self_listed("0.0.0.0:8443", "0.0.0.0:8443", "node-a") is True
    assert _is_self_listed("host:8443", "host:8443", "whatever") is True
    # wildcard listen + host == nodeName on the same port -> self
    assert _is_self_listed("node-a:8443", "0.0.0.0:8443", "node-a") is True
    assert _is_self_listed("node-a:8443", "::8443", "node-a") is False  # bare
    assert _is_self_listed("node-a:8443", "[::]:8443", "node-a") is True
    # wildcard listen but a DIFFERENT host -> a real distinct peer, kept
    assert _is_self_listed("node-b:8443", "0.0.0.0:8443", "node-a") is False
    # right name, WRONG port -> a genuinely different endpoint, not self
    assert _is_self_listed("node-a:9443", "0.0.0.0:8443", "node-a") is False
    # non-wildcard listen only matches the literal string, never by nodeName
    assert _is_self_listed("node-a:8443", "10.0.0.5:8443", "node-a") is False
    # FQDN vs short nodeName escapes structural detection (documented residual,
    # falls back to runtime STATUS_SELF) -- not a false positive here
    assert (
        _is_self_listed("node-a.internal:8443", "0.0.0.0:8443", "node-a")
        is False
    )


def test_distinct_peer_behind_wildcard_is_kept():
    # only the node's *own* name is treated as self; a genuinely different peer
    # on the same wildcard-listen port is still a real member.
    y = CLUSTER_YAML + "  nodeName: some-other-name\n"
    cfg = parse_config_string(y, "").cluster_config
    assert cfg is not None
    assert [p["host"] for p in cfg["peers"]] == [
        "yacron-b:8443",
        "yacron-c:8443",
    ]


# --------------------------------------------------------------------------
# ClusterView state machine (pure, no network)
# --------------------------------------------------------------------------


def _view(drift_after=3):
    return ClusterView(["peer:8443"], drift_after)


def _ok(view, *, name="peer-b", pid="v1:same", scheme=SCHEME_VERSION):
    view.record_success("peer:8443", name, pid, scheme, "v1:same", NOW, "me")
    return view.peers["peer:8443"]


def test_agreed_when_id_matches():
    p = _ok(_view())
    assert p.status == STATUS_AGREED
    assert p.job_set_id == "v1:same"
    assert p.node_name == "peer-b"
    assert p.last_seen == NOW
    assert p.mismatch_streak == 0


def test_mismatch_debounces_then_drifts():
    view = _view(drift_after=3)
    p = _ok(view, pid="v1:other")
    assert p.status == STATUS_SYNCING and p.mismatch_streak == 1
    _ok(view, pid="v1:other")
    assert view.peers["peer:8443"].status == STATUS_SYNCING
    _ok(view, pid="v1:other")  # third consecutive -> drift
    assert view.peers["peer:8443"].status == STATUS_DRIFTED
    assert view.peers["peer:8443"].mismatch_streak == 3


def test_recovery_resets_streak():
    view = _view(drift_after=2)
    _ok(view, pid="v1:other")
    _ok(view, pid="v1:other")
    assert view.peers["peer:8443"].status == STATUS_DRIFTED
    p = _ok(view, pid="v1:same")  # back in agreement
    assert p.status == STATUS_AGREED and p.mismatch_streak == 0


def test_drift_after_one_is_immediate():
    p = _ok(_view(drift_after=1), pid="v1:other")
    assert p.status == STATUS_DRIFTED


def test_self_detection():
    p = _ok(_view(), name="me")  # peer reports our own node name
    assert p.status == STATUS_SELF


def test_scheme_mismatch_is_drift():
    p = _ok(_view(), scheme="v999")
    assert p.status == STATUS_DRIFTED
    assert "scheme mismatch" in p.last_error


def test_record_failure_classifies():
    view = _view()
    view.record_failure("peer:8443", "boom", untrusted=False)
    assert view.peers["peer:8443"].status == STATUS_UNREACHABLE
    view.record_failure("peer:8443", "bad cert", untrusted=True)
    assert view.peers["peer:8443"].status == STATUS_UNTRUSTED


def test_failure_resets_mismatch_streak():
    view = _view(drift_after=5)
    _ok(view, pid="v1:other")  # streak 1
    view.record_failure("peer:8443", "down", untrusted=False)
    assert view.peers["peer:8443"].mismatch_streak == 0


def test_to_dict_shape():
    d = _ok(_view()).to_dict()
    assert set(d) == {
        "host",
        "status",
        "job_set_id",
        "node_name",
        "last_seen",
        "last_error",
        "mismatch_streak",
    }
    # instance_id is an internal liveness token, deliberately not surfaced
    assert "instance_id" not in d
    assert d["last_seen"] == NOW.isoformat()


# --------------------------------------------------------------------------
# duplicate-nodeName detection (the instance-id guard)
# --------------------------------------------------------------------------


def test_self_listing_with_matching_instance_is_self():
    # the operator listed this node's own address: the peer reports our name
    # AND our own instance id (we polled ourselves) -> the benign self case.
    view = _view()
    view.record_success(
        "peer:8443",
        "me",
        "v1:same",
        SCHEME_VERSION,
        "v1:same",
        NOW,
        "me",
        peer_instance="inst-1",
        my_instance="inst-1",
    )
    assert view.peers["peer:8443"].status == STATUS_SELF


def test_same_name_different_instance_is_conflict():
    # a DIFFERENT process announcing our nodeName is a duplicate, not self.
    view = _view()
    view.record_success(
        "peer:8443",
        "me",
        "v1:same",
        SCHEME_VERSION,
        "v1:same",
        NOW,
        "me",
        peer_instance="inst-2",
        my_instance="inst-1",
    )
    p = view.peers["peer:8443"]
    assert p.status == STATUS_CONFLICT
    assert "duplicate nodeName" in p.last_error


def test_self_name_without_instance_stays_self():
    # an older peer reporting no instance id cannot be proven a duplicate, so
    # we keep the historical benign 'self' classification (name match only).
    view = _view()
    view.record_success(
        "peer:8443",
        "me",
        "v1:same",
        SCHEME_VERSION,
        "v1:same",
        NOW,
        "me",
    )
    assert view.peers["peer:8443"].status == STATUS_SELF


# --------------------------------------------------------------------------
# real mTLS round-trip with generated certs
# --------------------------------------------------------------------------


def _gen_ca(cn):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(NOW - datetime.timedelta(days=1))
        .not_valid_after(NOW + datetime.timedelta(days=3650))
        .add_extension(
            x509.BasicConstraints(ca=True, path_length=None), critical=True
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    return key, cert


def _gen_leaf(ca_key, ca_cert, hostname):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    cert = (
        x509.CertificateBuilder()
        .subject_name(
            x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, hostname)])
        )
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(NOW - datetime.timedelta(days=1))
        .not_valid_after(NOW + datetime.timedelta(days=3650))
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName(hostname)]),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(
                ca_key.public_key()
            ),
            critical=False,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage(
                [
                    ExtendedKeyUsageOID.SERVER_AUTH,
                    ExtendedKeyUsageOID.CLIENT_AUTH,
                ]
            ),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    return key, cert


def _write_tls(dirpath, cn="cluster-ca"):
    # cryptography has no win-arm64 wheel and can't build from source on that
    # CI runner, so it isn't installed there; skip the cert-minting mTLS tests
    # rather than error. The cluster logic itself is covered by the pure tests
    # above and the mTLS round-trips run on every other platform.
    pytest.importorskip(
        "cryptography",
        reason="cryptography unavailable on this platform (e.g. win-arm64)",
    )
    from cryptography.hazmat.primitives import serialization

    ca_key, ca_cert = _gen_ca(cn)
    leaf_key, leaf_cert = _gen_leaf(ca_key, ca_cert, "localhost")
    ca_path = dirpath / (cn + "-ca.pem")
    cert_path = dirpath / (cn + "-node.pem")
    key_path = dirpath / (cn + "-node.key")
    ca_path.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    cert_path.write_bytes(leaf_cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        leaf_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return {
        "ca": str(ca_path),
        "cert": str(cert_path),
        "key": str(key_path),
    }


def _free_port():
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


def _cfg(tls, listen, peers, node, drift_after=3):
    cfg = dict(DEFAULT_CLUSTER)
    cfg.update(
        {
            "listen": listen,
            "tls": tls,
            "peers": [{"host": h} for h in peers],
            "nodeName": node,
            "driftAfter": drift_after,
            "interval": 3600,  # never auto-poll during the test
            "connectTimeout": 5,
        }
    )
    return cfg


@pytest.mark.asyncio
async def test_tls_files_changed_detects_in_place_rotation(tmp_path):
    # the SSL contexts load the cert+key once at construction, so an in-place
    # rotation (cert-manager / Vault / a k8s secret refresh -- same paths, new
    # bytes) is otherwise invisible and the cluster keeps serving the old cert
    # until it expires, then loses quorum fleet-wide. tls_files_changed is the
    # signal Cron.start_stop_cluster uses to restart and rebuild the contexts.
    tls = _write_tls(tmp_path)
    mgr = ClusterManager(
        _cfg(tls, "127.0.0.1:18443", ["localhost:18444"], "node-a"),
        lambda: "v1:x",
    )
    assert mgr.tls_files_changed() is False
    # simulate the rotation by changing the cert file's bytes in place (size
    # changes, so detection is robust to coarse mtime resolution).
    with open(tls["cert"], "ab") as fh:
        fh.write(b"\n# rotated\n")
    assert mgr.tls_files_changed() is True


@pytest.mark.asyncio
async def test_mtls_round_trip_agreed(tmp_path):
    tls = _write_tls(tmp_path)
    pa, pb = _free_port(), _free_port()
    a = ClusterManager(
        _cfg(tls, f"127.0.0.1:{pa}", [f"localhost:{pb}"], "node-a"),
        lambda: "v1:same",
    )
    b = ClusterManager(
        _cfg(tls, f"127.0.0.1:{pb}", [f"localhost:{pa}"], "node-b"),
        lambda: "v1:same",
    )
    await b.start()
    await a.start()
    try:
        await a._poll_all()
        peer = a.view.peers[f"localhost:{pb}"]
        assert peer.status == STATUS_AGREED
        assert peer.node_name == "node-b"
        assert peer.job_set_id == "v1:same"
    finally:
        await a.stop()
        await b.stop()


@pytest.mark.asyncio
async def test_mtls_round_trip_drift(tmp_path):
    tls = _write_tls(tmp_path)
    pa, pb = _free_port(), _free_port()
    a = ClusterManager(
        _cfg(
            tls,
            f"127.0.0.1:{pa}",
            [f"localhost:{pb}"],
            "node-a",
            drift_after=1,
        ),
        lambda: "v1:aaa",
    )
    b = ClusterManager(
        _cfg(tls, f"127.0.0.1:{pb}", [f"localhost:{pa}"], "node-b"),
        lambda: "v1:bbb",  # different id
    )
    await b.start()
    await a.start()
    try:
        await a._poll_all()
        assert a.view.peers[f"localhost:{pb}"].status == STATUS_DRIFTED
    finally:
        await a.stop()
        await b.stop()


@pytest.mark.asyncio
async def test_mtls_untrusted_peer(tmp_path):
    # peer presents a cert from a DIFFERENT CA than we trust -> untrusted
    mine = _write_tls(tmp_path, cn="mine")
    rogue = _write_tls(tmp_path, cn="rogue")
    pa, pb = _free_port(), _free_port()
    a = ClusterManager(
        _cfg(mine, f"127.0.0.1:{pa}", [f"localhost:{pb}"], "node-a"),
        lambda: "v1:same",
    )
    rogue_b = ClusterManager(
        _cfg(rogue, f"127.0.0.1:{pb}", [f"localhost:{pa}"], "rogue-b"),
        lambda: "v1:same",
    )
    await rogue_b.start()
    await a.start()
    try:
        await a._poll_all()
        assert a.view.peers[f"localhost:{pb}"].status == STATUS_UNTRUSTED
    finally:
        await a.stop()
        await rogue_b.stop()


# --------------------------------------------------------------------------
# /cluster web endpoint
# --------------------------------------------------------------------------


class _Req:
    headers: dict = {}


@pytest.mark.asyncio
async def test_web_cluster_endpoint_disabled():
    import json

    import yacron2.cron

    cron = yacron2.cron.Cron(None)
    cron.web_config = {}
    resp = await cron._web_get_cluster(_Req())
    assert json.loads(resp.text) == {"enabled": False, "peers": []}


@pytest.mark.asyncio
async def test_web_cluster_endpoint_enabled():
    import json

    import yacron2.cron

    class StubManager:
        def view_dict(self):
            return {
                "node_name": "n",
                "job_set_id": "v1:x",
                "peers": [{"host": "p:1", "status": "agreed"}],
            }

    cron = yacron2.cron.Cron(None)
    cron.web_config = {}
    cron.cluster_manager = StubManager()
    resp = await cron._web_get_cluster(_Req())
    data = json.loads(resp.text)
    assert data["enabled"] is True
    assert data["node_name"] == "n"
    assert data["peers"][0]["status"] == "agreed"


# --------------------------------------------------------------------------
# quorum + leader election (pure)
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "size,want",
    [(1, 1), (2, 2), (3, 2), (4, 3), (5, 3), (6, 4), (7, 4)],
)
def test_quorum_size(size, want):
    # even N needs the same quorum as N+1 odd above it -> use odd sizes
    assert quorum_size(size) == want


def test_single_node_always_leads():
    # degenerate 1-node "cluster": quorum is 1, so it always leads itself
    assert elect_leader("a", [], 1) == "a"


def test_lowest_name_in_full_quorum_leads():
    # all 3 agree: every node independently elects the same lowest name
    assert elect_leader("a", ["b", "c"], 3) == "a"
    assert elect_leader("b", ["a", "c"], 3) == "a"
    assert elect_leader("c", ["a", "b"], 3) == "a"


def test_quorum_with_one_peer_down_still_elects_one_leader():
    # 3-node cluster, node c down: a and b each see {self, other} == quorum 2,
    # and both elect "a" -> exactly one leader, no double-run.
    assert elect_leader("a", ["b"], 3) == "a"
    assert elect_leader("b", ["a"], 3) == "a"


def test_minority_has_no_leader():
    # only self reachable in a 3-node cluster: below quorum -> stand down
    assert elect_leader("a", [], 3) is None
    # 5-node cluster split 3/2: the 2-side is a minority -> no leader there
    assert elect_leader("d", ["e"], 5) is None
    # ...while the 3-side elects its lowest name
    assert elect_leader("a", ["b", "c"], 5) == "a"


def test_two_node_cluster_needs_both():
    assert elect_leader("a", ["b"], 2) == "a"  # both up
    assert elect_leader("b", ["a"], 2) == "a"
    assert elect_leader("a", [], 2) is None  # peer down -> nobody leads


def test_available_leader_ignores_quorum():
    # lowest name among self + agreeing peers, regardless of cluster size
    assert elect_available_leader("a", ["b", "c"]) == "a"
    assert elect_available_leader("c", ["a", "b"]) == "a"
    # isolated node (no reachable peers) still elects itself -> never skips,
    # which is the whole point of PreferLeader; contrast elect_leader -> None.
    assert elect_available_leader("b", []) == "b"
    assert elect_leader("b", [], 3) is None
    # a partitioned 5-node cluster: each side elects its own available leader
    # (so the job may run on both sides) -- the liveness/safety trade.
    assert elect_available_leader("a", ["b"]) == "a"  # majority side
    assert elect_available_leader("d", ["e"]) == "d"  # minority side


def test_elect_leader_uses_candidate_names_for_the_winner():
    # the quorum gate is on live_peer_names; the winner is min(self, *cands) --
    # candidate_names are the names this node may actually elect (confirmed
    # quorate). Defaults to live when omitted.
    assert (
        elect_leader("b", ["c", "d"], 4) == "b"
    )  # no candidates -> min(live)
    # 'a' is an eligible (confirmed-quorate) candidate -> defer to it
    assert elect_leader("b", ["c", "d"], 4, ["a", "c", "d"]) == "a"
    # only c,d eligible (a smaller node was excluded as sub-quorum) -> b leads
    assert elect_leader("b", ["c", "d"], 4, ["c", "d"]) == "b"
    # candidate_names NEVER relax the quorum gate (on live): below -> None
    assert elect_leader("b", [], 4, ["a"]) is None


def test_elect_job_owner_uses_candidate_names():
    # the spread analogue: the rendezvous is over self + candidate_names
    base = elect_job_owner("job-x", "b", ["c", "d"], 4)
    assert base == _hrw_owner("job-x", ["b", "c", "d"])
    withcand = elect_job_owner("job-x", "b", ["c", "d"], 4, ["a", "c", "d"])
    assert withcand == _hrw_owner("job-x", ["b", "a", "c", "d"])
    # quorum gate still on live, regardless of candidate_names
    assert elect_job_owner("job-x", "b", [], 4, ["a"]) is None


@pytest.mark.asyncio
async def test_mtls_round_trip_elects_single_leader(tmp_path):
    # two agreeing nodes (cluster_size 2, quorum 2): once each has polled the
    # other, exactly one of them (the lowest name) is leader.
    tls = _write_tls(tmp_path)
    pa, pb = _free_port(), _free_port()
    a = ClusterManager(
        _cfg(tls, f"127.0.0.1:{pa}", [f"localhost:{pb}"], "node-a"),
        lambda: "v1:same",
    )
    b = ClusterManager(
        _cfg(tls, f"127.0.0.1:{pb}", [f"localhost:{pa}"], "node-b"),
        lambda: "v1:same",
    )
    await a.start()
    await b.start()
    try:
        # mutual attestation needs each node to have polled the other AND been
        # polled back, so converge over a couple of rounds.
        for _ in range(2):
            await a._poll_all()
            await b._poll_all()
        assert a.cluster_size() == 2 and a.quorum() == 2
        assert a.is_leader() is True  # "node-a" < "node-b"
        assert b.is_leader() is False
        assert a.leader_name() == "node-a"
        assert b.leader_name() == "node-a"
        # both expose the same election view over /cluster
        assert a.view_dict()["is_leader"] is True
        assert b.view_dict()["leader"] == "node-a"
    finally:
        await a.stop()
        await b.stop()


@pytest.mark.asyncio
async def test_is_leader_false_without_quorum(tmp_path):
    # node-a in a 3-node cluster whose two peers are unreachable: it sees only
    # itself (1 < quorum 2) and must not lead.
    tls = _write_tls(tmp_path)
    pa = _free_port()
    a = ClusterManager(
        _cfg(
            tls,
            f"127.0.0.1:{pa}",
            ["localhost:1", "localhost:2"],  # nothing listening
            "node-a",
        ),
        lambda: "v1:same",
    )
    await a.start()
    try:
        await a._poll_all()
        assert a.cluster_size() == 3 and a.quorum() == 2
        assert a.is_leader() is False
        assert a.leader_name() is None
    finally:
        await a.stop()


def test_elect_leader_parsed_from_config():
    cfg = parse_config_string(
        CLUSTER_YAML + "  electLeader: true\n", ""
    ).cluster_config
    assert cfg is not None and cfg["electLeader"] is True


def test_elect_leader_defaults_false():
    cfg = parse_config_string(CLUSTER_YAML, "").cluster_config
    assert cfg is not None and cfg["electLeader"] is False


# a cluster declaring a single peer -> 2 nodes total
TWO_NODE_YAML = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
cluster:
  listen: "0.0.0.0:8443"
  tls:
    ca: /etc/yacron2/ca.pem
    cert: /etc/yacron2/node.pem
    key: /etc/yacron2/node.key
  peers:
    - host: yacron-b:8443
"""


def test_elect_leader_rejects_two_node_cluster():
    # quorum of 2 needs both up -> strictly worse than a single replica
    with pytest.raises(ConfigError, match="strictly worse than a single"):
        parse_config_string(TWO_NODE_YAML + "  electLeader: true\n", "")


def test_two_node_cluster_ok_without_election():
    # the same 2-node cluster is fine for attestation-only (no electLeader)
    cfg = parse_config_string(TWO_NODE_YAML, "").cluster_config
    assert cfg is not None and cfg["electLeader"] is False


def test_dedup_makes_padded_two_node_cluster_rejected():
    # #10: listing the single real peer twice is really a 2-node cluster.
    # After de-duplication the electLeader 2-node guard correctly fires, where
    # the raw count would have masqueraded as 3 nodes and slipped through.
    y = TWO_NODE_YAML + "    - host: yacron-b:8443\n  electLeader: true\n"
    with pytest.raises(ConfigError, match="strictly worse than a single"):
        parse_config_string(y, "")


def test_job_cluster_policy_parsed():
    y = (
        "jobs:\n"
        "  - name: a\n"
        "    command: echo a\n"
        '    schedule: "* * * * *"\n'
        "    clusterPolicy: EveryNode\n"
        "  - name: b\n"
        "    command: echo b\n"
        '    schedule: "* * * * *"\n'
    )
    jobs = {j.name: j for j in parse_config_string(y, "").jobs}
    assert jobs["a"].clusterPolicy == "EveryNode"
    assert jobs["b"].clusterPolicy == "Leader"  # default


def test_elect_leader_warns_on_even_size():
    # CLUSTER_YAML has 2 peers -> 3 nodes; add one more for an even 4. The
    # warning is computed (not logged) by cluster_config_warnings so the daemon
    # can emit it once on (re)start instead of on every config reload.
    from yacron2.config import cluster_config_warnings

    even_yaml = (
        CLUSTER_YAML + "    - host: yacron-d:8443\n  electLeader: true\n"
    )
    cfg = parse_config_string(even_yaml, "").cluster_config
    assert cfg is not None and cfg["electLeader"] is True
    assert any("even cluster size" in w for w in cluster_config_warnings(cfg))


# --------------------------------------------------------------------------
# distribution: spread (per-job rendezvous ownership)
# --------------------------------------------------------------------------


def test_distribution_defaults_single_leader():
    cfg = parse_config_string(CLUSTER_YAML, "").cluster_config
    assert cfg is not None and cfg["distribution"] == "single-leader"


def test_distribution_spread_parsed():
    cfg = parse_config_string(
        CLUSTER_YAML + "  electLeader: true\n  distribution: spread\n", ""
    ).cluster_config
    assert cfg is not None and cfg["distribution"] == "spread"


def test_distribution_spread_without_electleader_warns():
    from yacron2.config import cluster_config_warnings

    cfg = parse_config_string(
        CLUSTER_YAML + "  distribution: spread\n", ""
    ).cluster_config
    assert cfg is not None and cfg["distribution"] == "spread"
    assert any(
        "no effect without electLeader" in w
        for w in cluster_config_warnings(cfg)
    )


def test_job_owner_quorum_gated():
    # below quorum -> nobody owns it (stands down), exactly like elect_leader
    assert elect_job_owner("job1", "a", [], 3) is None
    # at quorum -> a deterministic owner from the live set is chosen
    owner = elect_job_owner("job1", "a", ["b"], 3)
    assert owner in {"a", "b"}


def test_job_owner_deterministic_and_consistent_across_nodes():
    # every node in one quorum computes the SAME owner for a given job: that is
    # what keeps spread mode at-most-once under a clean partition.
    members = ["a", "b", "c"]
    for job in ["alpha", "beta", "gamma", "delta", "epsilon"]:
        owners = {
            elect_job_owner(job, me, [p for p in members if p != me], 3)
            for me in members
        }
        assert len(owners) == 1  # all three agree
        assert owners.pop() in members


def test_job_owner_spreads_load_across_nodes():
    # over many jobs, ownership fans out across all three nodes (rendezvous
    # hashing is well-mixed) rather than concentrating on one.
    members = ["a", "b", "c"]
    counts = dict.fromkeys(members, 0)
    for i in range(300):
        owner = elect_job_owner("job-%d" % i, "a", ["b", "c"], 3)
        counts[owner] += 1
    assert all(c > 0 for c in counts.values())  # every node owns some jobs
    # roughly balanced: no node owns more than ~half (sanity, not exact)
    assert max(counts.values()) < 200


def test_available_job_owner_no_quorum_gate():
    # isolated node still owns all its jobs (never skips) -- the PreferLeader
    # contract; contrast elect_job_owner which would return None.
    for job in ["a", "b", "c", "d"]:
        assert elect_available_job_owner(job, "solo", []) == "solo"
    assert elect_job_owner("a", "solo", [], 3) is None


def test_available_job_owner_matches_quorate_owner_when_all_agree():
    # with the full set agreeing, the quorum-gated and ungated owners coincide
    for job in ["w", "x", "y", "z"]:
        gated = elect_job_owner(job, "a", ["b", "c"], 3)
        ungated = elect_available_job_owner(job, "a", ["b", "c"])
        assert gated == ungated


@pytest.mark.asyncio
async def test_mtls_spread_assigns_distinct_owners(tmp_path):
    # two agreeing nodes in spread mode: each job is owned by exactly one of
    # them, and both compute the same owner for the same job.
    tls = _write_tls(tmp_path)
    pa, pb = _free_port(), _free_port()
    cfg_a = _cfg(tls, f"127.0.0.1:{pa}", [f"localhost:{pb}"], "node-a")
    cfg_a["distribution"] = "spread"
    cfg_b = _cfg(tls, f"127.0.0.1:{pb}", [f"localhost:{pa}"], "node-b")
    cfg_b["distribution"] = "spread"
    a = ClusterManager(cfg_a, lambda: "v1:same")
    b = ClusterManager(cfg_b, lambda: "v1:same")
    await a.start()
    await b.start()
    try:
        # mutual attestation needs both directions confirmed -> a few rounds.
        for _ in range(2):
            await a._poll_all()
            await b._poll_all()
        assert a.distribution == "spread"
        for job in ["one", "two", "three", "four", "five"]:
            # exactly one of the two nodes owns each job...
            assert a.is_job_owner(job) != b.is_job_owner(job)
            # ...and they agree on which one
            assert a.job_owner(job) == b.job_owner(job)
        # and there is no single "leader" reported in spread mode
        assert a.view_dict()["leader"] is None
        assert a.view_dict()["distribution"] == "spread"
        assert a.view_dict()["quorate"] is True
    finally:
        await a.stop()
        await b.stop()


@pytest.mark.asyncio
async def test_mtls_reboot_ran_push_propagates(tmp_path):
    # the eager push: node-a runs a deferred @reboot job and pushes the fact to
    # node-b over real mTLS, so node-b learns it ran without waiting to poll.
    tls = _write_tls(tmp_path)
    pa, pb = _free_port(), _free_port()
    a = ClusterManager(
        _cfg(tls, f"127.0.0.1:{pa}", [f"localhost:{pb}"], "node-a"),
        lambda: "v1:same",
    )
    b = ClusterManager(
        _cfg(tls, f"127.0.0.1:{pb}", [f"localhost:{pa}"], "node-b"),
        lambda: "v1:same",
    )
    await a.start()
    await b.start()
    try:
        assert b.reboot_ran("boot") is False
        await a.mark_reboot_ran("boot")  # records + eager-pushes to b
        assert b.reboot_ran("boot") is True  # b absorbed the push (same id)
    finally:
        await a.stop()
        await b.stop()


# --------------------------------------------------------------------------
# ClusterManager I/O + accessors WITHOUT cryptography
#
# The mTLS round-trip tests above mint real certs, so they self-skip where
# `cryptography` ships no wheel (e.g. win-arm64) -- and they are the *only*
# tests that touch ClusterManager's network surface, so on that platform
# cluster.py coverage collapses and the --cov-fail-under gate would fail.
# These tests stub the TLS context builders and fake the aiohttp session to
# drive the same paths (init, the /peer handler, start/stop + poll loop,
# _poll_peer's success/untrusted/unreachable branches, and the election
# accessors) with no certs and no real handshake, keeping the cluster code
# covered on every platform. They complement -- not replace -- the
# real-handshake mTLS tests above.
# --------------------------------------------------------------------------

_DUMMY_TLS = {"ca": "ca", "cert": "cert", "key": "key"}


@pytest.fixture
def no_tls(monkeypatch):
    # make ClusterManager constructible without real certs; start() then serves
    # plaintext (ssl_context=None), which is all these tests need.
    monkeypatch.setattr(
        "yacron2.cluster.build_client_ssl_context", lambda tls: None
    )
    monkeypatch.setattr(
        "yacron2.cluster.build_server_ssl_context", lambda tls: None
    )


def test_split_host_port_ok():
    assert _split_host_port("host:8443") == ("host", 8443)
    assert _split_host_port("127.0.0.1:1") == ("127.0.0.1", 1)


def test_split_host_port_rejects_bad_input():
    with pytest.raises(ValueError):
        _split_host_port("noport")


@pytest.mark.asyncio
async def test_handle_peer_payload(no_tls):
    import json

    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", [], "node-a"), lambda: "v1:mine"
    )
    resp = await mgr._handle_peer(_Req())
    payload = json.loads(resp.text)
    assert payload["node_name"] == "node-a"
    assert payload["job_set_id"] == "v1:mine"
    assert payload["scheme_version"] == SCHEME_VERSION
    # the per-process instance id, used to tell a self-listing from a duplicate
    assert payload["instance_id"] == mgr.instance_id and payload["instance_id"]
    # our declared cluster size N (no peers here -> just this node)
    assert payload["cluster_size"] == 1


def _seed_agree(mgr, host, name, instance=None, mutual=None):
    # mark a configured peer AGREED, as a successful poll round would --
    # including the mutual-attestation members list showing the peer sees US
    # agreed too (without it, _agreeing_peer_names no longer counts the peer).
    # `mutual` is the peer's gossiped mutual_agreeing set (the names IT
    # mutually agrees with): None means "not reported" -> the peer gets the
    # benefit of the doubt and stays electable; a set lets a test mark the peer
    # quorate (>= quorum-1 names) or sub-quorum, and name bridge targets.
    if instance is None:
        instance = "inst-" + host
    peer = mgr.view.peers[host]
    peer.status = STATUS_AGREED
    peer.node_name = name
    peer.instance_id = instance
    peer.job_set_id = "v1:mine"
    peer.members = [
        (mgr.node_name, mgr.instance_id, True),
        (name, instance, True),
    ]
    peer.mutual_agreeing = mutual


def test_manager_accessors_single_leader_quorate(no_tls):
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "c:1"], "node-a"),
        lambda: "v1:mine",
    )
    _seed_agree(mgr, "b:1", "node-b")
    _seed_agree(mgr, "c:1", "node-c")
    assert mgr.cluster_size() == 3
    assert mgr.quorum() == 2
    assert mgr._agreeing_peer_names() == ["node-b", "node-c"]
    assert mgr.leader_name() == "node-a"
    assert mgr.is_leader() is True
    assert mgr.is_quorate() is True
    assert mgr.available_leader_name() == "node-a"
    assert mgr.is_available_leader() is True
    view = mgr.view_dict()
    assert view["is_leader"] is True
    assert view["leader"] == "node-a"
    assert view["distribution"] == "single-leader"
    assert view["quorate"] is True
    assert len(view["peers"]) == 2


def test_manager_accessors_minority_stands_down(no_tls):
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "c:1"], "node-a"),
        lambda: "v1:mine",
    )
    # no peer agreeing -> below quorum -> no leader
    assert mgr.leader_name() is None
    assert mgr.is_leader() is False
    assert mgr.is_quorate() is False
    # available_* ignores quorum: an isolated node still leads itself
    assert mgr.is_available_leader() is True


def test_manager_accessors_spread(no_tls):
    cfg = _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "c:1"], "node-a")
    cfg["distribution"] = "spread"
    mgr = ClusterManager(cfg, lambda: "v1:mine")
    _seed_agree(mgr, "b:1", "node-b")
    _seed_agree(mgr, "c:1", "node-c")
    members = {"node-a", "node-b", "node-c"}
    owner = mgr.job_owner("job-x")
    assert owner in members
    assert mgr.is_job_owner("job-x") == (owner == "node-a")
    assert mgr.available_job_owner("job-x") in members
    assert isinstance(mgr.is_available_job_owner("job-x"), bool)
    view = mgr.view_dict()
    assert view["leader"] is None  # spread mode has no single leader
    assert view["is_leader"] is False
    assert view["distribution"] == "spread"


def test_manager_spread_job_owner_none_without_quorum(no_tls):
    cfg = _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "c:1"], "node-a")
    cfg["distribution"] = "spread"
    mgr = ClusterManager(cfg, lambda: "v1:mine")
    assert mgr.job_owner("job-x") is None
    assert mgr.is_job_owner("job-x") is False


def test_bridge_discovery_makes_larger_node_defer(no_tls):
    # node-b mutually agrees with c and d; c and d EACH mutually agree with
    # node-a too (a two-way edge they witness, but node-a is unreachable from
    # b). N=4, quorum=3. b would naively elect itself (min of {b,c,d}); via the
    # bridge it confirms a is quorate (two witnessed mutual edges c<->a, d<->a)
    # and defers to it, closing the asymmetric double-run.
    cfg = _cfg(_DUMMY_TLS, "127.0.0.1:1", ["a:1", "c:1", "d:1"], "node-b")
    mgr = ClusterManager(cfg, lambda: "v1:mine")
    # c and d each mutually agree with node-b and node-a (quorate; witness a)
    _seed_agree(mgr, "c:1", "node-c", mutual={"node-a", "node-b"})
    _seed_agree(mgr, "d:1", "node-d", mutual={"node-a", "node-b"})
    # node-a itself is left unreachable from b (never polled)
    assert mgr.cluster_size() == 4 and mgr.quorum() == 3
    assert mgr._bridge_candidates() == ["node-a"]
    assert mgr.leader_name() == "node-a"  # defers to the smaller bridged node
    assert mgr.is_leader() is False
    assert mgr.is_quorate() is True  # b is still quorate ({b,c,d})


def test_thin_bridge_is_not_confirmed_so_node_leads(no_tls):
    # only ONE witness (c) mutually agrees with node-a, so b sees just 2 nodes
    # mutually agree with a (c + a) < quorum 3: it cannot confirm a is quorate,
    # does NOT defer (the liveness choice -- it may double-run rather than risk
    # standing down behind a node it cannot confirm), and leads itself.
    cfg = _cfg(_DUMMY_TLS, "127.0.0.1:1", ["a:1", "c:1", "d:1"], "node-b")
    mgr = ClusterManager(cfg, lambda: "v1:mine")
    _seed_agree(
        mgr, "c:1", "node-c", mutual={"node-a", "node-b"}
    )  # witnesses a
    _seed_agree(mgr, "d:1", "node-d", mutual={"node-b", "node-c"})  # not a
    assert mgr._bridge_candidates() == []
    assert mgr.leader_name() == "node-b"
    assert mgr.is_leader() is True


def test_no_standdown_when_min_neighbor_is_sub_quorum(no_tls):
    # the N>=5 stand-down the v2 fix eliminates. node-1 (N=5, quorum=3)
    # mutually agrees with {0,3,4} -> quorate, but its lowest peer node-0 is
    # sub-quorum (mutual only with node-1). The base min-of-live would defer
    # node-1 to the non-runnable node-0 and stand the whole healthy majority
    # down; v2 excludes node-0 (confirmed sub-quorum) so node-1 leads.
    cfg = _cfg(
        _DUMMY_TLS, "127.0.0.1:1", ["n0:1", "n2:1", "n3:1", "n4:1"], "node-1"
    )
    mgr = ClusterManager(cfg, lambda: "v1:mine")
    _seed_agree(mgr, "n0:1", "node-0", mutual={"node-1"})  # sub-quorum (2 < 3)
    _seed_agree(mgr, "n3:1", "node-3", mutual={"node-1", "node-2"})  # quorate
    _seed_agree(mgr, "n4:1", "node-4", mutual={"node-1", "node-2"})  # quorate
    # node-2 is not mutually agreeing with node-1 (no direct edge); unseeded
    assert mgr.cluster_size() == 5 and mgr.quorum() == 3
    assert mgr.is_quorate() is True
    # node-0 is confirmed sub-quorum -> excluded; node-1 does NOT defer to it
    assert "node-0" not in mgr._eligible_candidates()
    assert mgr.is_leader() is True
    assert mgr.leader_name() == "node-1"


def test_unconfirmed_peer_is_not_electable(no_tls):
    # we only elect a peer we can CONFIRM is quorate. A peer that does not
    # report mutual_agreeing (mutual=None -- e.g. an older build) is NOT
    # confirmed, so node-b does not defer to the smaller node-a; it leans to
    # leading itself (the liveness choice -- never defer to a node that might
    # stand down). The price is a possible double-run during a rolling upgrade.
    cfg = _cfg(_DUMMY_TLS, "127.0.0.1:1", ["a:1", "c:1"], "node-b")
    mgr = ClusterManager(cfg, lambda: "v1:mine")
    _seed_agree(mgr, "a:1", "node-a")  # mutual=None -> unconfirmed
    _seed_agree(mgr, "c:1", "node-c")
    assert "node-a" not in mgr._eligible_candidates()
    assert mgr.is_leader() is True
    assert mgr.leader_name() == "node-b"


def test_bridged_pair_exactly_one_leads(no_tls):
    # the motivating scenario, BOTH sides: a and b each mutually agree with c
    # and d but NOT each other. Build both managers and confirm exactly ONE
    # leads (safety) and it is the global-min name.
    def _bridged(name, peers):
        cfg = _cfg(_DUMMY_TLS, "127.0.0.1:1", peers, name)
        mgr = ClusterManager(cfg, lambda: "v1:mine")
        # c and d each mutually agree with both a and b (the shared bridge)
        _seed_agree(
            mgr, "c:1", "node-c", mutual={"node-a", "node-b", "node-d"}
        )
        _seed_agree(
            mgr, "d:1", "node-d", mutual={"node-a", "node-b", "node-c"}
        )
        return mgr

    a = _bridged("node-a", ["b:1", "c:1", "d:1"])
    b = _bridged("node-b", ["a:1", "c:1", "d:1"])
    assert a.is_leader() is True
    assert b.is_leader() is False
    assert a.leader_name() == "node-a" and b.leader_name() == "node-a"


def test_bridge_discovery_spread_owner_consistent(no_tls):
    # spread mode: the bridged node joins the rendezvous candidate set, so b
    # computes each job's owner over the same {a,b,c,d} that a quorate a would,
    # converging on one owner per job instead of double-running.
    cfg = _cfg(_DUMMY_TLS, "127.0.0.1:1", ["a:1", "c:1", "d:1"], "node-b")
    cfg["distribution"] = "spread"
    mgr = ClusterManager(cfg, lambda: "v1:mine")
    _seed_agree(mgr, "c:1", "node-c", mutual={"node-a", "node-b"})
    _seed_agree(mgr, "d:1", "node-d", mutual={"node-a", "node-b"})
    full = ["node-a", "node-b", "node-c", "node-d"]
    for job in ("j1", "j2", "j3", "j4"):
        assert mgr.job_owner(job) == _hrw_owner(job, full)


def test_bridge_discovery_ignores_direct_and_underwitnessed(no_tls):
    # a name we already count directly, or one witnessed by fewer than quorum-1
    # mutual edges, is never a bridge candidate.
    cfg = _cfg(_DUMMY_TLS, "127.0.0.1:1", ["a:1", "c:1", "d:1"], "node-b")
    mgr = ClusterManager(cfg, lambda: "v1:mine")
    # c witnesses node-a (only 1 witness -> underwitnessed); d does not.
    # Both peers stay quorate/electable; no node is confirmed via the bridge.
    _seed_agree(mgr, "c:1", "node-c", mutual={"node-a", "node-b"})
    _seed_agree(mgr, "d:1", "node-d", mutual={"node-b", "node-c"})
    assert mgr._bridge_candidates() == []  # a underwitnessed; c is direct
    assert mgr.leader_name() == "node-b"  # b is the min of {b,c,d}


def _seed_conflict(mgr, host, name, instance):
    # a peer announcing `name` from a specific instance id
    peer = mgr.view.peers[host]
    peer.status = STATUS_AGREED
    peer.node_name = name
    peer.instance_id = instance
    peer.job_set_id = "v1:mine"


def test_manager_no_conflict_when_names_distinct(no_tls):
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "c:1"], "node-a"),
        lambda: "v1:mine",
    )
    _seed_agree(mgr, "b:1", "node-b")
    _seed_agree(mgr, "c:1", "node-c")
    assert mgr.has_conflict() is False
    assert mgr.conflict_names() == []


def test_conflict_detection_unaffected_by_bridge_data(no_tls):
    # a duplicate nodeName must still be detected (so the Leader gate fails
    # closed) even when bridge witnesses are present -- bridge discovery and
    # conflict detection are independent, and conflict wins (checked first in
    # cron._cluster_allows). c and d agree and both witness node-a; d also
    # announces OUR nodeName from a different instance (the duplicate).
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["a:1", "c:1", "d:1"], "node-b"),
        lambda: "v1:mine",
    )
    # c and d mutually agree with node-b and node-a -> both witness node-a
    _seed_agree(mgr, "c:1", "node-c", mutual={"node-a", "node-b"})
    _seed_agree(mgr, "d:1", "node-d", mutual={"node-a", "node-b"})
    # d's gossip also reveals a second instance of our own name (a duplicate)
    mgr.view.peers["d:1"].members.append(
        ("node-b", "some-other-instance", True)
    )
    assert mgr._bridge_candidates() == ["node-a"]  # bridge still works
    assert mgr.has_conflict() is True  # ...but the conflict is still detected
    assert "node-b" in mgr.conflict_names()


def test_manager_detects_duplicate_of_own_name(no_tls):
    # a peer announces OUR nodeName from a different instance -> conflict
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "c:1"], "node-a"),
        lambda: "v1:mine",
    )
    p = mgr.view.peers["b:1"]
    p.status = STATUS_CONFLICT
    p.node_name = "node-a"
    p.instance_id = "some-other-instance"
    assert mgr.has_conflict() is True
    assert mgr.conflict_names() == ["node-a"]


def test_manager_detects_cross_peer_duplicate(no_tls):
    # two DIFFERENT peers report the same name (neither is ours) -> still a
    # duplicate: a third node can spot the collision its peers cannot.
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "c:1"], "node-a"),
        lambda: "v1:mine",
    )
    _seed_conflict(mgr, "b:1", "dup", "inst-b")
    _seed_conflict(mgr, "c:1", "dup", "inst-c")
    assert mgr.conflict_names() == ["dup"]
    assert mgr.has_conflict() is True


def test_manager_self_listing_is_not_conflict(no_tls):
    # the 'self' peer carries our name AND our own instance id -> benign
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "self:1"], "node-a"),
        lambda: "v1:mine",
    )
    _seed_agree(mgr, "b:1", "node-b")
    selfp = mgr.view.peers["self:1"]
    selfp.status = STATUS_SELF
    selfp.node_name = "node-a"
    selfp.instance_id = mgr.instance_id
    assert mgr.has_conflict() is False


def test_view_dict_reports_conflict(no_tls):
    cfg = _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "c:1"], "node-a")
    cfg["electLeader"] = True
    mgr = ClusterManager(cfg, lambda: "v1:mine")
    p = mgr.view.peers["b:1"]
    p.status = STATUS_CONFLICT
    p.node_name = "node-a"
    p.instance_id = "other"
    view = mgr.view_dict()
    assert view["conflict"] is True
    assert view["conflict_names"] == ["node-a"]


# --- cluster-size (membership) divergence ---------------------------------


def test_manager_detects_cluster_size_divergence(no_tls):
    # the headline split-brain: an agreeing peer declares a different cluster
    # size N. Two nodes quorate under different Ns would each elect themselves
    # (two majorities of *different* Ns can be disjoint), so a size mismatch is
    # a first-class conflict, exactly like a duplicate nodeName.
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "c:1"], "node-a"),
        lambda: "v1:mine",
    )
    _seed_agree(mgr, "b:1", "node-b")
    _seed_agree(mgr, "c:1", "node-c")
    assert mgr.cluster_size() == 3
    assert mgr.conflicting_sizes() == []
    assert mgr.has_conflict() is False
    # c is mid-resize and now declares N=5
    mgr.view.peers["c:1"].declared_size = 5
    assert mgr.conflicting_sizes() == [5]
    assert mgr.has_conflict() is True


def test_manager_no_size_conflict_when_sizes_match(no_tls):
    # agreeing peers reporting our own N (the healthy case) is no conflict.
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "c:1"], "node-a"),
        lambda: "v1:mine",
    )
    _seed_agree(mgr, "b:1", "node-b")
    _seed_agree(mgr, "c:1", "node-c")
    mgr.view.peers["b:1"].declared_size = 3
    mgr.view.peers["c:1"].declared_size = 3
    assert mgr.conflicting_sizes() == []
    assert mgr.has_conflict() is False


def test_self_listing_does_not_inflate_size_or_conflict(no_tls):
    # a benign self-listing (this node's own address in its peers, escaping the
    # config-load string dedup because `listen` is a wildcard) is recognised at
    # runtime as STATUS_SELF and excluded from N -- so it neither raises this
    # node's quorum nor makes agreeing peers see a divergent size (the cluster-
    # wide false conflict a naive len(peers)+1 size would cause).
    mgr = ClusterManager(
        _cfg(
            _DUMMY_TLS,
            "0.0.0.0:8443",
            ["b:1", "c:1", "myhost:8443"],
            "node-a",
        ),
        lambda: "v1:mine",
    )
    # before this node has polled itself, the self entry is still counted (N=4)
    assert mgr.cluster_size() == 4
    # once it polls its own self-listed address it is marked SELF...
    selfp = mgr.view.peers["myhost:8443"]
    selfp.status = STATUS_SELF
    selfp.node_name = "node-a"
    selfp.instance_id = mgr.instance_id
    # ...and N drops back to the real member count, matching its peers
    assert mgr.cluster_size() == 3
    _seed_agree(mgr, "b:1", "node-b")
    _seed_agree(mgr, "c:1", "node-c")
    mgr.view.peers["b:1"].declared_size = 3
    mgr.view.peers["c:1"].declared_size = 3
    assert mgr.conflicting_sizes() == []
    assert mgr.has_conflict() is False


def test_manager_size_divergence_ignores_non_agreed_and_unknown(no_tls):
    # only AGREED peers are size-checked: a peer on a different job set never
    # joins our quorum (and a real resize keeps the job set unchanged), and a
    # peer too old to report a size (None) is simply skipped -- neither is a
    # false conflict.
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "c:1"], "node-a"),
        lambda: "v1:mine",
    )
    pb = mgr.view.peers["b:1"]
    pb.status = STATUS_DRIFTED
    pb.node_name = "node-b"
    pb.declared_size = 99  # different N, but drifted -> ignored
    _seed_agree(mgr, "c:1", "node-c")
    mgr.view.peers["c:1"].declared_size = None  # too old to report -> skipped
    assert mgr.conflicting_sizes() == []
    assert mgr.has_conflict() is False


def test_view_dict_reports_size_conflict(no_tls):
    cfg = _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "c:1"], "node-a")
    cfg["electLeader"] = True
    mgr = ClusterManager(cfg, lambda: "v1:mine")
    _seed_agree(mgr, "b:1", "node-b")
    mgr.view.peers["b:1"].declared_size = 5
    view = mgr.view_dict()
    assert view["conflict"] is True  # umbrella flag (either kind)
    assert view["size_conflict"] is True
    assert view["conflicting_sizes"] == [5]
    assert view["conflict_names"] == []  # not a nodeName conflict


@pytest.mark.asyncio
async def test_mtls_cluster_size_divergence_detected(tmp_path):
    # end-to-end repro of the headline trace over real mTLS: mid 3->5 resize,
    # node a still declares N=3 while node c declares N=5. They share the job
    # set (a resize touches only `peers`), so each sees the other AGREED -- but
    # the size mismatch makes BOTH fail Leader closed instead of both leading.
    # The extra peers are dead 127.0.0.1 ports (fast connection-refused) that
    # only pad each node's declared N.
    tls = _write_tls(tmp_path)
    pa, pc = _free_port(), _free_port()
    a = ClusterManager(
        _cfg(
            tls,
            f"127.0.0.1:{pa}",
            [f"localhost:{pc}", "127.0.0.1:2"],
            "node-a",  # N = 3
        ),
        lambda: "v1:same",
    )
    c = ClusterManager(
        _cfg(
            tls,
            f"127.0.0.1:{pc}",
            [f"localhost:{pa}", "127.0.0.1:2", "127.0.0.1:3", "127.0.0.1:4"],
            "node-c",  # N = 5
        ),
        lambda: "v1:same",
    )
    await a.start()
    await c.start()
    try:
        await a._poll_all()
        await c._poll_all()
        assert a.cluster_size() == 3 and c.cluster_size() == 5
        # each saw the other agree on the job set but declare a different N
        assert a.conflicting_sizes() == [5]
        assert c.conflicting_sizes() == [3]
        assert a.has_conflict() and c.has_conflict()
    finally:
        await a.stop()
        await c.stop()


@pytest.mark.asyncio
async def test_mtls_duplicate_nodename_detected(tmp_path):
    # two nodes accidentally share a nodeName: each sees the other announce
    # that name from a different instance id -> conflict on both sides.
    tls = _write_tls(tmp_path)
    pa, pb = _free_port(), _free_port()
    a = ClusterManager(
        _cfg(tls, f"127.0.0.1:{pa}", [f"localhost:{pb}"], "dup"),
        lambda: "v1:same",
    )
    b = ClusterManager(
        _cfg(tls, f"127.0.0.1:{pb}", [f"localhost:{pa}"], "dup"),
        lambda: "v1:same",
    )
    await a.start()
    await b.start()
    try:
        await a._poll_all()
        await b._poll_all()
        assert a.has_conflict() and b.has_conflict()
        assert a.view.peers[f"localhost:{pb}"].status == STATUS_CONFLICT
        assert a.conflict_names() == ["dup"]
    finally:
        await a.stop()
        await b.stop()


# fake aiohttp session for _poll_peer / _poll_all (no sockets, no certs) -----


class _FakeContent:
    # minimal stand-in for aiohttp's StreamReader: yields the whole body in one
    # chunk, which is all _read_capped needs to exercise.
    def __init__(self, body):
        self._body = body

    async def iter_chunked(self, n):
        if self._body:
            yield self._body


class _FakeResp:
    def __init__(self, payload=None, *, body=None):
        if body is None:
            body = json.dumps(payload).encode("utf-8")
        self.content = _FakeContent(body)

    def raise_for_status(self):
        pass


class _PushReq:
    # minimal stand-in for an aiohttp request body, for _handle_reboot_ran
    def __init__(self, payload):
        self.content = _FakeContent(json.dumps(payload).encode("utf-8"))


class _FakeGet:
    def __init__(self, resp=None, exc=None):
        self._resp = resp
        self._exc = exc

    async def __aenter__(self):
        if self._exc is not None:
            raise self._exc
        return self._resp

    async def __aexit__(self, *exc):
        return False


class _FakeSession:
    def __init__(self, get_result):
        self._get_result = get_result
        self.calls = []

    def get(self, url, ssl=None):
        self.calls.append((url, ssl))
        return self._get_result


class _FakeSSLError(aiohttp.ClientSSLError):
    # the real __init__ wants a connection key / os error; bypass it and give
    # str() (which record_failure logs) something printable.
    def __init__(self):
        pass

    def __str__(self):
        return "fake ssl handshake failure"


@pytest.mark.asyncio
async def test_poll_peer_success_records_agreement(no_tls):
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1"], "node-a"), lambda: "v1:mine"
    )
    session = _FakeSession(
        _FakeGet(
            resp=_FakeResp(
                {
                    "node_name": "node-b",
                    "job_set_id": "v1:mine",
                    "scheme_version": SCHEME_VERSION,
                }
            )
        )
    )
    await mgr._poll_peer(session, "b:1", "v1:mine")
    peer = mgr.view.peers["b:1"]
    assert peer.status == STATUS_AGREED
    assert peer.node_name == "node-b"
    assert session.calls[0][0] == "https://b:1/peer"


@pytest.mark.asyncio
async def test_poll_peer_ssl_error_is_untrusted(no_tls):
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1"], "node-a"), lambda: "v1:mine"
    )
    session = _FakeSession(_FakeGet(exc=_FakeSSLError()))
    await mgr._poll_peer(session, "b:1", "v1:mine")
    assert mgr.view.peers["b:1"].status == STATUS_UNTRUSTED


@pytest.mark.asyncio
async def test_poll_peer_client_error_is_unreachable(no_tls):
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1"], "node-a"), lambda: "v1:mine"
    )
    session = _FakeSession(_FakeGet(exc=aiohttp.ClientError("boom")))
    await mgr._poll_peer(session, "b:1", "v1:mine")
    assert mgr.view.peers["b:1"].status == STATUS_UNREACHABLE


@pytest.mark.asyncio
async def test_poll_peer_records_declared_size(no_tls):
    # a successful poll stores the peer's declared cluster size for the
    # size-divergence gate.
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1"], "node-a"), lambda: "v1:mine"
    )
    session = _FakeSession(
        _FakeGet(
            resp=_FakeResp(
                {
                    "node_name": "node-b",
                    "job_set_id": "v1:mine",
                    "scheme_version": SCHEME_VERSION,
                    "cluster_size": 5,
                }
            )
        )
    )
    await mgr._poll_peer(session, "b:1", "v1:mine")
    assert mgr.view.peers["b:1"].declared_size == 5


@pytest.mark.asyncio
async def test_poll_peer_omitted_size_is_none(no_tls):
    # a peer too old to report cluster_size leaves declared_size None (the size
    # check is then skipped for it), not a failed observation.
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1"], "node-a"), lambda: "v1:mine"
    )
    session = _FakeSession(
        _FakeGet(
            resp=_FakeResp(
                {
                    "node_name": "node-b",
                    "job_set_id": "v1:mine",
                    "scheme_version": SCHEME_VERSION,
                }
            )
        )
    )
    await mgr._poll_peer(session, "b:1", "v1:mine")
    peer = mgr.view.peers["b:1"]
    assert peer.status == STATUS_AGREED
    assert peer.declared_size is None


@pytest.mark.asyncio
async def test_poll_peer_rejects_malformed_size(no_tls):
    # a non-int, non-positive, or bool cluster_size from a CA-trusted but buggy
    # peer is a malformed observation (bool is an int subclass -> rejected too)
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1"], "node-a"), lambda: "v1:mine"
    )
    for bad in ("five", 0, -1, True, 1.5):
        session = _FakeSession(
            _FakeGet(
                resp=_FakeResp(
                    {
                        "node_name": "node-b",
                        "job_set_id": "v1:mine",
                        "scheme_version": SCHEME_VERSION,
                        "cluster_size": bad,
                    }
                )
            )
        )
        await mgr._poll_peer(session, "b:1", "v1:mine")
        peer = mgr.view.peers["b:1"]
        assert peer.status == STATUS_UNREACHABLE, bad
        assert "cluster_size" in peer.last_error


@pytest.mark.asyncio
async def test_poll_all_with_no_peers_is_noop(no_tls):
    # exercises _poll_all's session setup + empty gather with no network
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", [], "node-a"), lambda: "v1:mine"
    )
    await mgr._poll_all()  # must not raise


@pytest.mark.asyncio
async def test_start_stop_lifecycle_plaintext(no_tls):
    # no peers -> the poll loop's _poll_all is a no-op (no peer sockets), so
    # this drives start()/_poll_loop/stop() over a plaintext listener with no
    # certs. stop() is called twice to cover the already-stopped path.
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, f"127.0.0.1:{_free_port()}", [], "node-a"),
        lambda: "v1:mine",
    )
    await mgr.start()
    try:
        await asyncio.sleep(0)  # let the poll loop reach its first wait
    finally:
        await mgr.stop()
    await mgr.stop()  # idempotent: nothing running


# --------------------------------------------------------------------------
# adversarial-review regressions: mutual attestation, transitive conflict,
# untrusted-input validation, and lifecycle cleanup
# --------------------------------------------------------------------------


def test_quorum_requires_mutual_agreement(no_tls):
    # #1: seeing a peer AGREED is not enough -- it must also report seeing US
    # agreed (by our instance_id), else a one-way link would let both ends
    # self-elect. The guard: an un-attesting peer does not count toward quorum.
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "c:1"], "node-a"),
        lambda: "v1:mine",
    )
    peer = mgr.view.peers["b:1"]
    peer.status = STATUS_AGREED
    peer.node_name = "node-b"
    peer.instance_id = "inst-b"
    peer.members = [("node-b", "inst-b", True)]  # does NOT list node-a
    assert mgr._agreeing_peer_names() == []  # one-way -> not counted
    assert mgr.leader_name() is None  # 1 < quorum 2
    assert mgr.is_leader() is False
    # once the peer attests us back, it counts and we lead our quorum
    peer.members = [
        ("node-b", "inst-b", True),
        (mgr.node_name, mgr.instance_id, True),
    ]
    assert mgr._agreeing_peer_names() == ["node-b"]
    assert mgr.leader_name() == "node-a"
    assert mgr.is_leader() is True


def test_asymmetric_reachability_never_double_leads(no_tls):
    # the split-brain repro from review finding #1: a<b<c, quorum 2, with
    # a->b reachable, b->a NOT, a<->c down, b->c up + back. Pre-fix BOTH a and
    # b self-elected; mutual attestation must leave at most one leader.
    a = ClusterManager(
        _cfg(_DUMMY_TLS, "a:1", ["b:1", "c:1"], "node-a"), lambda: "v1:x"
    )
    b = ClusterManager(
        _cfg(_DUMMY_TLS, "b:1", ["a:1", "c:1"], "node-b"), lambda: "v1:x"
    )
    # a polled b OK, but b never reached a -> b's members omit node-a
    pa_b = a.view.peers["b:1"]
    pa_b.status = STATUS_AGREED
    pa_b.node_name = "node-b"
    pa_b.instance_id = b.instance_id
    pa_b.members = [("node-b", b.instance_id, True)]
    # b and c attest each other; b never reached a
    pb_c = b.view.peers["c:1"]
    pb_c.status = STATUS_AGREED
    pb_c.node_name = "node-c"
    pb_c.instance_id = "inst-c"
    pb_c.members = [
        ("node-c", "inst-c", True),
        ("node-b", b.instance_id, True),
    ]
    assert a.is_leader() is False  # b does not attest a back -> a not quorate
    assert b.is_leader() is True  # {b,c} mutually agree -> b leads
    assert not (a.is_leader() and b.is_leader())  # never both


def test_transitive_conflict_via_gossip(no_tls):
    # #2: two nodes share a nodeName but we can only reach one of them. A
    # reachable peer's reported members carries the second instance, so we
    # still detect the duplicate (and the Leader gate then fails closed).
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "c:1"], "node-a"),
        lambda: "v1:mine",
    )
    # we directly see one "dup" (instance i1)
    p = mgr.view.peers["b:1"]
    p.status = STATUS_AGREED
    p.node_name = "dup"
    p.instance_id = "i1"
    p.members = [("dup", "i1", True), (mgr.node_name, mgr.instance_id, True)]
    # peer c gossips a DIFFERENT instance of "dup" we cannot reach directly
    pc = mgr.view.peers["c:1"]
    pc.status = STATUS_AGREED
    pc.node_name = "node-c"
    pc.instance_id = "ic"
    pc.members = [("dup", "i2", True), (mgr.node_name, mgr.instance_id, True)]
    assert mgr.conflict_names() == ["dup"]
    assert mgr.has_conflict() is True


@pytest.mark.asyncio
async def test_poll_peer_records_peer_members(no_tls):
    # a successful poll stores the peer's reported members, and an attesting
    # peer is counted toward quorum (the mutual-agreement happy path).
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1"], "node-a"), lambda: "v1:mine"
    )
    session = _FakeSession(
        _FakeGet(
            resp=_FakeResp(
                {
                    "node_name": "node-b",
                    "job_set_id": "v1:mine",
                    "scheme_version": SCHEME_VERSION,
                    "instance_id": "inst-b",
                    "members": [
                        {
                            "node_name": "node-a",
                            "instance_id": mgr.instance_id,
                            "agreed": True,
                        },
                        {
                            "node_name": "node-b",
                            "instance_id": "inst-b",
                            "agreed": True,
                        },
                    ],
                }
            )
        )
    )
    await mgr._poll_peer(session, "b:1", "v1:mine")
    peer = mgr.view.peers["b:1"]
    assert peer.status == STATUS_AGREED
    assert (mgr.node_name, mgr.instance_id, True) in peer.members
    assert mgr._agreeing_peer_names() == ["node-b"]


@pytest.mark.asyncio
async def test_handle_peer_includes_members(no_tls):
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1"], "node-a"), lambda: "v1:mine"
    )
    _seed_agree(mgr, "b:1", "node-b")
    resp = await mgr._handle_peer(_Req())
    payload = json.loads(resp.text)
    by_name = {m["node_name"]: m for m in payload["members"]}
    assert "node-a" in by_name and "node-b" in by_name
    me = by_name["node-a"]
    assert me["agreed"] is True and me["instance_id"] == mgr.instance_id


@pytest.mark.asyncio
async def test_handle_peer_includes_mutual_agreeing(no_tls):
    # the /peer response must publish our mutual_agreeing set (the confirmed
    # two-way agreers) so pollers can drive bridge confirmation off it.
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "c:1"], "node-a"),
        lambda: "v1:mine",
    )
    _seed_agree(mgr, "b:1", "node-b")
    _seed_agree(mgr, "c:1", "node-c")
    resp = await mgr._handle_peer(_Req())
    payload = json.loads(resp.text)
    assert payload["mutual_agreeing"] == ["node-b", "node-c"]


@pytest.mark.asyncio
async def test_poll_peer_round_trips_mutual_agreeing(no_tls):
    # end to end: a polled mutual_agreeing is parsed, stored, and drives a
    # bridge decision. node-a polls node-b (N=4, quorum 3); b reports it
    # mutually agrees with node-a and node-x. After also learning node-c
    # mutually agrees with node-x, node-a confirms node-x as a bridge node.
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "c:1", "x:1"], "node-a"),
        lambda: "v1:mine",
    )
    me = {
        "node_name": "node-a",
        "instance_id": mgr.instance_id,
        "agreed": True,
    }
    for host, nm, inst in (("b:1", "node-b", "ib"), ("c:1", "node-c", "ic")):
        session = _FakeSession(
            _FakeGet(
                resp=_FakeResp(
                    {
                        "node_name": nm,
                        "job_set_id": "v1:mine",
                        "scheme_version": SCHEME_VERSION,
                        "instance_id": inst,
                        "members": [
                            me,
                            {
                                "node_name": nm,
                                "instance_id": inst,
                                "agreed": True,
                            },
                        ],
                        # b and c each mutually agree with node-a and node-x
                        "mutual_agreeing": ["node-a", "node-x"],
                    }
                )
            )
        )
        await mgr._poll_peer(session, host, "v1:mine")
    assert mgr.view.peers["b:1"].mutual_agreeing == {"node-a", "node-x"}
    # node-x is witnessed by both b and c (2) + itself = quorum 3 -> confirmed
    assert mgr._bridge_candidates() == ["node-x"]


@pytest.mark.asyncio
async def test_poll_peer_omitted_mutual_agreeing_is_unconfirmed(no_tls):
    # mixed-version: a peer on an older build omits mutual_agreeing -> parses
    # to an empty set, so it is NOT confirmed quorate and not electable. The
    # node leans to leading rather than deferring to an unconfirmed peer.
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "c:1"], "node-a"),
        lambda: "v1:mine",
    )
    me = {
        "node_name": "node-a",
        "instance_id": mgr.instance_id,
        "agreed": True,
    }
    session = _FakeSession(
        _FakeGet(
            resp=_FakeResp(
                {
                    "node_name": "node-b",
                    "job_set_id": "v1:mine",
                    "scheme_version": SCHEME_VERSION,
                    "instance_id": "ib",
                    "members": [
                        me,
                        {
                            "node_name": "node-b",
                            "instance_id": "ib",
                            "agreed": True,
                        },
                    ],
                    # NO mutual_agreeing key (older peer)
                }
            )
        )
    )
    await mgr._poll_peer(session, "b:1", "v1:mine")
    assert mgr.view.peers["b:1"].mutual_agreeing == set()  # omitted -> empty
    assert "node-b" not in mgr._eligible_candidates()  # unconfirmed: excluded


def test_poll_failure_resets_mutual_agreeing(no_tls):
    # a failed poll drops the now-stale gossip, including mutual_agreeing, so a
    # witness gone unreachable can no longer vouch for a bridge candidate.
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1"], "node-a"), lambda: "v1:mine"
    )
    _seed_agree(mgr, "b:1", "node-b", mutual={"node-a", "node-x"})
    assert mgr.view.peers["b:1"].mutual_agreeing == {"node-a", "node-x"}
    mgr.view.record_failure("b:1", "boom", untrusted=False)
    assert mgr.view.peers["b:1"].mutual_agreeing is None


@pytest.mark.asyncio
async def test_poll_peer_neutralizes_hostile_mutual_agreeing(no_tls):
    # a malformed/hostile mutual_agreeing (non-list, or non-string items) is
    # neutralized by _parse_str_list, never reaching min()/sorted().
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1"], "node-a"), lambda: "v1:mine"
    )
    me = {
        "node_name": "node-a",
        "instance_id": mgr.instance_id,
        "agreed": True,
    }
    session = _FakeSession(
        _FakeGet(
            resp=_FakeResp(
                {
                    "node_name": "node-b",
                    "job_set_id": "v1:mine",
                    "scheme_version": SCHEME_VERSION,
                    "instance_id": "ib",
                    "members": [
                        me,
                        {
                            "node_name": "node-b",
                            "instance_id": "ib",
                            "agreed": True,
                        },
                    ],
                    "mutual_agreeing": ["node-a", 123, None, {"x": 1}],
                }
            )
        )
    )
    await mgr._poll_peer(session, "b:1", "v1:mine")
    # only the valid string survives; no crash
    assert mgr.view.peers["b:1"].mutual_agreeing == {"node-a"}


@pytest.mark.asyncio
async def test_poll_peer_rejects_non_string_node_name(no_tls):
    # #3: a CA-trusted-but-misbehaving peer returning a non-string node_name is
    # rejected (not stored), so it can never reach min()/sorted() and crash the
    # scheduler. Election keeps working.
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1"], "node-a"), lambda: "v1:mine"
    )
    session = _FakeSession(
        _FakeGet(
            resp=_FakeResp(
                {
                    "node_name": 12345,  # not a string
                    "job_set_id": "v1:mine",
                    "scheme_version": SCHEME_VERSION,
                }
            )
        )
    )
    await mgr._poll_peer(session, "b:1", "v1:mine")
    peer = mgr.view.peers["b:1"]
    assert peer.status == STATUS_UNREACHABLE
    assert peer.node_name is None
    assert mgr.leader_name() is None  # election does not crash


@pytest.mark.asyncio
async def test_poll_peer_rejects_oversized_body(no_tls):
    # #4: an over-cap body is refused rather than buffered (OOM guard).
    from yacron2.cluster import MAX_PEER_RESPONSE_BYTES

    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1"], "node-a"), lambda: "v1:mine"
    )
    big = b"x" * (MAX_PEER_RESPONSE_BYTES + 1)
    session = _FakeSession(_FakeGet(resp=_FakeResp(body=big)))
    await mgr._poll_peer(session, "b:1", "v1:mine")
    peer = mgr.view.peers["b:1"]
    assert peer.status == STATUS_UNREACHABLE
    assert "oversized" in (peer.last_error or "")


@pytest.mark.asyncio
async def test_poll_peer_rejects_non_dict_and_invalid_json(no_tls):
    # #5: valid-but-non-object JSON and unparseable bodies are classified as
    # failed observations instead of raising AttributeError/ValueError.
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1"], "node-a"), lambda: "v1:mine"
    )
    for body in (b"[1, 2, 3]", b'"a string"', b"not json{"):
        session = _FakeSession(_FakeGet(resp=_FakeResp(body=body)))
        await mgr._poll_peer(session, "b:1", "v1:mine")
        assert mgr.view.peers["b:1"].status == STATUS_UNREACHABLE


@pytest.mark.asyncio
async def test_start_cleans_up_runner_on_bind_failure(no_tls):
    # #7: a bind failure (port already in use) after AppRunner.setup() must not
    # leak the runner; start() cleans up after itself and leaves the manager
    # un-started so the caller's "log and keep running" handler is clean.
    port = _free_port()
    blocker = ClusterManager(
        _cfg(_DUMMY_TLS, f"127.0.0.1:{port}", [], "node-a"), lambda: "v1:x"
    )
    await blocker.start()
    try:
        clash = ClusterManager(
            _cfg(_DUMMY_TLS, f"127.0.0.1:{port}", [], "node-b"), lambda: "v1:x"
        )
        with pytest.raises(OSError):
            await clash.start()
        assert clash._runner is None  # cleaned up, not leaked
        assert clash._poll_task is None  # never reached task creation
    finally:
        await blocker.stop()


# --------------------------------------------------------------------------
# deferred @reboot "already ran" gossip-ack (prevents re-run on failover)
# --------------------------------------------------------------------------


def test_reboot_ran_self_is_advertised(no_tls):
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1"], "node-a"), lambda: "v1:mine"
    )
    assert mgr.reboot_ran("boot") is False
    mgr._ran_reboot_jobs.add("boot")
    assert mgr.reboot_ran("boot") is True
    assert "boot" in mgr.advertised_ran_jobs()


def test_reboot_ran_transitive_from_agreed_peer_only(no_tls):
    # a job an AGREED peer reports as run counts (transitive), but the same
    # report from a non-agreed peer (different job set) is ignored -- scoping.
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "c:1"], "node-a"),
        lambda: "v1:mine",
    )
    _seed_agree(mgr, "b:1", "node-b")
    mgr.view.peers["b:1"].ran_reboot_jobs = {"boot"}
    assert mgr.reboot_ran("boot") is True
    # c is reachable but NOT agreed (e.g. syncing/drifted): its claim is moot
    pc = mgr.view.peers["c:1"]
    pc.status = STATUS_SYNCING
    pc.node_name = "node-c"
    pc.ran_reboot_jobs = {"other"}
    assert mgr.reboot_ran("other") is False


@pytest.mark.asyncio
async def test_poll_peer_records_ran_reboot_jobs(no_tls):
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1"], "node-a"), lambda: "v1:mine"
    )
    session = _FakeSession(
        _FakeGet(
            resp=_FakeResp(
                {
                    "node_name": "node-b",
                    "job_set_id": "v1:mine",
                    "scheme_version": SCHEME_VERSION,
                    "instance_id": "inst-b",
                    "members": [
                        {
                            "node_name": "node-a",
                            "instance_id": mgr.instance_id,
                            "agreed": True,
                        }
                    ],
                    "ran_reboot_jobs": ["boot"],
                }
            )
        )
    )
    await mgr._poll_peer(session, "b:1", "v1:mine")
    assert mgr.view.peers["b:1"].ran_reboot_jobs == {"boot"}
    # b is agreed (mutual), so its run is trusted transitively
    assert mgr.reboot_ran("boot") is True


@pytest.mark.asyncio
async def test_handle_reboot_ran_absorbs_only_matching_job_set(no_tls):
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1"], "node-a"), lambda: "v1:mine"
    )
    # a push for a DIFFERENT job set is ignored (stale config)
    resp = await mgr._handle_reboot_ran(
        _PushReq({"job_set_id": "v1:other", "names": ["boot"]})
    )
    assert resp.status == 204
    assert mgr.reboot_ran("boot") is False
    # a push for our current job set is absorbed
    await mgr._handle_reboot_ran(
        _PushReq({"job_set_id": "v1:mine", "names": ["boot", 123]})
    )
    assert mgr.reboot_ran("boot") is True  # the int entry is dropped


@pytest.mark.asyncio
async def test_push_reboot_ran_fans_out_to_peers(no_tls, monkeypatch):
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", ["b:1", "c:1"], "node-a"),
        lambda: "v1:mine",
    )
    mgr._ran_reboot_jobs.add("boot")
    sent = []

    async def fake_one(session, host, payload):
        sent.append((host, payload))

    monkeypatch.setattr(mgr, "_push_reboot_ran_one", fake_one)
    await mgr.mark_reboot_ran("boot")  # records + eager-pushes
    assert {h for h, _ in sent} == {"b:1", "c:1"}
    assert all(
        p["names"] == ["boot"] and p["job_set_id"] == "v1:mine"
        for _, p in sent
    )


@pytest.mark.asyncio
async def test_ran_jobs_cleared_on_job_set_change(no_tls):
    # a config reload (job_set_id change) forgets prior runs: a still-deferred
    # job may then re-run (safe), never silently skip a job whose def changed.
    job_set = {"id": "v1:a"}
    mgr = ClusterManager(
        _cfg(_DUMMY_TLS, "127.0.0.1:1", [], "node-a"), lambda: job_set["id"]
    )
    mgr._ran_reboot_jobs.add("boot")
    await mgr._poll_all()  # same id -> kept
    assert mgr.reboot_ran("boot") is True
    job_set["id"] = "v2:b"  # config reload changes the job-set id
    await mgr._poll_all()  # id changed -> cleared
    assert mgr.reboot_ran("boot") is False
