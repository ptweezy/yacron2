"""Config parsing/validation for the pluggable leadership backends.

Covers the loosened cluster schema (listen/tls/peers no longer hard-required),
the backend dispatch in _build_cluster_config, and each lease backend's
defaults, validation, and secret resolution.
"""

import pytest

from yacron2.config import (
    DEFAULT_ETCD,
    DEFAULT_K8S,
    ConfigError,
    cluster_config_warnings,
    parse_config_string,
)


def _cluster(yaml):
    return parse_config_string(yaml, "").cluster_config


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


# --- etcd -----------------------------------------------------------------

_ETCD = (
    "cluster:\n"
    "  backend: etcd\n"
    "  nodeName: node-a\n"
    "  etcd:\n"
    "    endpoints:\n"
    "      - http://127.0.0.1:2379\n"
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


def test_etcd_rejects_malformed_endpoint():
    for bad in ("127.0.0.1:2379", "http://nohost", "ftp://h:2379"):
        yaml = (
            "cluster:\n"
            "  backend: etcd\n"
            "  etcd:\n"
            "    endpoints:\n"
            "      - " + bad + "\n"
        )
        with pytest.raises(ConfigError, match="endpoints"):
            _cluster(yaml)


def test_etcd_password_from_value():
    yaml = _ETCD + (
        "    username: root\n"
        "    password:\n"
        "      value: s3cret\n"
    )
    cfg = _cluster(yaml)
    assert cfg["etcd"]["resolved_password"] == "s3cret"
    assert cfg["etcd"]["username"] == "root"


def test_etcd_password_from_env(monkeypatch):
    monkeypatch.setenv("ETCD_PW", "from-env")
    yaml = _ETCD + "    password:\n      fromEnvVar: ETCD_PW\n"
    assert _cluster(yaml)["etcd"]["resolved_password"] == "from-env"


def test_etcd_password_empty_source_fails_closed(monkeypatch):
    monkeypatch.delenv("ETCD_PW_MISSING", raising=False)
    yaml = _ETCD + "    password:\n      fromEnvVar: ETCD_PW_MISSING\n"
    with pytest.raises(ConfigError, match="empty secret"):
        _cluster(yaml)


def test_etcd_password_from_file(tmp_path):
    secret = tmp_path / "pw"
    secret.write_text("file-secret\n")
    yaml = _ETCD + "    password:\n      fromFile: {}\n".format(secret)
    assert _cluster(yaml)["etcd"]["resolved_password"] == "file-secret"


def test_etcd_password_from_missing_file_fails(tmp_path):
    missing = tmp_path / "nope"
    yaml = _ETCD + "    password:\n      fromFile: {}\n".format(missing)
    with pytest.raises(ConfigError, match="could not be read"):
        _cluster(yaml)


# --- warnings -------------------------------------------------------------


def test_no_warnings_for_lease_backends():
    # the gossip-only even-size / distribution advisories must not fire (and
    # must not KeyError on the absent peers list) for a lease backend.
    assert cluster_config_warnings(_cluster(_K8S)) == []
    assert cluster_config_warnings(_cluster(_ETCD)) == []


def test_unknown_backend_rejected():
    with pytest.raises(ConfigError):
        _cluster("cluster:\n  backend: zookeeper\n")
