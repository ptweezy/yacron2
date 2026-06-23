import datetime
import socket

import pytest

from yacron2.cluster import (
    STATUS_AGREED,
    STATUS_DRIFTED,
    STATUS_SELF,
    STATUS_SYNCING,
    STATUS_UNREACHABLE,
    STATUS_UNTRUSTED,
    ClusterManager,
    ClusterView,
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
    assert d["last_seen"] == NOW.isoformat()


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


def test_elect_leader_warns_on_even_size(caplog):
    import logging

    # CLUSTER_YAML has 2 peers -> 3 nodes; add one more for an even 4
    even_yaml = (
        CLUSTER_YAML + "    - host: yacron-d:8443\n  electLeader: true\n"
    )
    with caplog.at_level(logging.WARNING, logger="yacron2.config"):
        cfg = parse_config_string(even_yaml, "").cluster_config
    assert cfg is not None and cfg["electLeader"] is True
    assert any("even cluster size" in r.message for r in caplog.records)
