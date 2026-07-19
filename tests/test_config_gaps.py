"""Config-loading edges not covered by the main ``test_config.py`` suite.

Small, targeted regressions: per-section include/directory merge conflicts,
timezone and schedule type failures, cluster/state/mcp validation guards, and
the pure address/secret helpers.  Everything goes through the public parse
entry points where a YAML form exists; only genuinely YAML-unreachable
branches call the helper directly.
"""

import pytest

from cronstable import config
from cronstable.config import (
    DEFAULT_CONFIG,
    ConfigError,
    JobConfig,
    mergedicts,
    parse_config_string,
)

# ---------------------------------------------------------------------------
# schedule / timezone edges
# ---------------------------------------------------------------------------


def test_schedule_has_seconds_non_schedule_type():
    assert config.schedule_has_seconds(None) is False
    assert config.schedule_has_seconds(1234) is False


def test_schedule_invalid_type_is_config_error():
    job_dict = mergedicts(
        DEFAULT_CONFIG,
        {"name": "j", "command": "true", "schedule": 1234},
    )
    with pytest.raises(ConfigError, match="invalid schedule"):
        JobConfig(job_dict)


def test_unknown_timezone_is_config_error():
    yaml = (
        "jobs:\n"
        "  - name: j\n"
        "    command: true\n"
        "    schedule: '* * * * *'\n"
        "    timezone: Not/AZone\n"
    )
    with pytest.raises(ConfigError, match="unknown timezone"):
        parse_config_string(yaml, "")


def test_dag_level_job_error_names_the_dag():
    yaml = (
        "dags:\n"
        "  - name: etl\n"
        "    schedule: '0 2 * * *'\n"
        "    timezone: Not/AZone\n"
        "    tasks:\n"
        "      - id: a\n"
        "        command: 'true'\n"
    )
    with pytest.raises(ConfigError, match="dag 'etl'.*unknown timezone"):
        parse_config_string(yaml, "")


# ---------------------------------------------------------------------------
# cluster validation guards
# ---------------------------------------------------------------------------

_TLS = "  tls:\n    ca: /ca\n    cert: /cert\n    key: /key\n"


def _cluster_yaml(extra):
    return (
        "cluster:\n"
        "  listen: '0.0.0.0:8443'\n" + _TLS + "  peers:\n"
        "    - host: b:8443\n" + extra
    )


def test_cluster_connect_timeout_must_be_positive():
    with pytest.raises(ConfigError, match="connectTimeout must be > 0"):
        parse_config_string(_cluster_yaml("  connectTimeout: 0\n"), "")


def test_cluster_ipv6_peer_needs_valid_port():
    yaml = (
        "cluster:\n"
        "  listen: '0.0.0.0:8443'\n" + _TLS + "  peers:\n"
        "    - host: '[::1]:notaport'\n"
    )
    with pytest.raises(ConfigError, match=r"must be \[ipv6\]:port"):
        parse_config_string(yaml, "")


def test_observability_mesh_tuning_keys_forwarded():
    yaml = (
        "cluster:\n"
        "  backend: kubernetes\n"
        "  nodeName: node-a\n"
        "  observability:\n"
        "    listen: '0.0.0.0:8140'\n"
        "    tls:\n      ca: /oca\n      cert: /ocert\n      key: /okey\n"
        "    peers:\n      - host: b:8140\n"
        "    nodeName: obs-a\n"
        "    interval: 7\n"
    )
    cfg = parse_config_string(yaml, "").cluster_config
    mesh = cfg["observabilityMesh"]
    assert mesh["nodeName"] == "obs-a"
    assert mesh["interval"] == 7


def test_kubernetes_lease_namespace_charset():
    yaml = (
        "cluster:\n"
        "  backend: kubernetes\n"
        "  nodeName: node-a\n"
        "  kubernetes:\n"
        "    leaseNamespace: 'Bad_NS'\n"
    )
    with pytest.raises(ConfigError, match="leaseNamespace must be a valid"):
        parse_config_string(yaml, "")


# ---------------------------------------------------------------------------
# state.jobApi validation guards
# ---------------------------------------------------------------------------


def _state_yaml(extra):
    return "state:\n  path: /tmp/st\n  jobApi:\n    enabled: true\n" + extra


def test_job_api_max_value_bytes_negative():
    with pytest.raises(ConfigError, match="maxValueBytes must be >= 0"):
        parse_config_string(_state_yaml("    maxValueBytes: -1\n"), "")


def test_job_api_max_artifact_bytes_negative():
    with pytest.raises(ConfigError, match="maxArtifactBytes must be >= 0"):
        parse_config_string(_state_yaml("    maxArtifactBytes: -1\n"), "")


def test_job_api_listen_port_out_of_range():
    with pytest.raises(ConfigError, match="invalid port"):
        parse_config_string(
            _state_yaml("    listen: 'http://127.0.0.1:70000'\n"), ""
        )


# ---------------------------------------------------------------------------
# web metrics / mcp cross-checks
# ---------------------------------------------------------------------------


def test_web_metrics_map_without_buckets_keeps_defaults():
    yaml = (
        "web:\n"
        "  listen:\n    - http://127.0.0.1:8080\n"
        "  metrics:\n    public: true\n"
    )
    conf = parse_config_string(yaml, "")
    assert conf.web_config["metrics"]["public"] is True


def test_mcp_act_with_read_only_warns_but_loads(caplog):
    yaml = (
        "web:\n  listen:\n    - http://127.0.0.1:8080\n"
        "mcp:\n  enabled: true\n"
        "  toolsets:\n    - observe\n    - act\n"
    )
    with caplog.at_level("WARNING", logger="cronstable.config"):
        conf = parse_config_string(yaml, "")
        config._validate_cross_sections(conf)
    assert conf.mcp_config["enabled"] is True
    assert "mutating tools stay suppressed" in caplog.text


# ---------------------------------------------------------------------------
# pure helpers
# ---------------------------------------------------------------------------


def test_resolve_secret_unset_is_none():
    assert config._resolve_secret(None, "x") is None
    assert config._resolve_secret({}, "x") is None


def test_likely_self_fqdn_exact_match_is_not_degenerate():
    # an exact self-listing was already dropped by _is_self_listed; the
    # fuzzy FQDN heuristic must not re-flag it.
    assert (
        config._likely_self_fqdn("node1:8443", "0.0.0.0:8443", "node1")
        is False
    )


def test_is_local_listener_forms():
    assert config._is_local_listener("unix:///tmp/x.sock") is True
    assert config._is_local_listener("localhost:8080") is True
    assert config._is_local_listener("127.0.0.1:8080") is True
    # a hostname is not an IP literal: fails the loopback check closed
    assert config._is_local_listener("example.com:8080") is False


# ---------------------------------------------------------------------------
# include: per-section merge + conflicts
# ---------------------------------------------------------------------------

_CHILD_ALL = (
    "web:\n  listen:\n    - http://127.0.0.1:8080\n"
    "cluster:\n"
    "  listen: '0.0.0.0:8443'\n" + _TLS + "  peers:\n"
    "    - host: b:8443\n"
    "state:\n  path: /tmp/st\n"
    "mcp:\n  enabled: true\n"
    "logging:\n  version: 1\n"
)


def _write(path, text):
    path.write_text(text)
    return str(path)


def test_include_adopts_child_sections(tmp_path):
    _write(tmp_path / "child.yaml", _CHILD_ALL)
    parent = _write(tmp_path / "parent.yaml", "include:\n  - child.yaml\n")
    conf = config.parse_config_file(parent)
    assert conf.web_config is not None
    assert conf.cluster_config is not None
    assert conf.state_config is not None
    assert conf.mcp_config["enabled"] is True
    assert conf.logging_config is not None


@pytest.mark.parametrize(
    "section,inline",
    [
        ("web", "web:\n  listen:\n    - http://127.0.0.1:9090\n"),
        (
            "cluster",
            "cluster:\n"
            "  listen: '0.0.0.0:9443'\n" + _TLS + "  peers:\n"
            "    - host: c:9443\n",
        ),
        ("mcp", "mcp:\n  enabled: false\n"),
        ("logging", "logging:\n  version: 1\n"),
    ],
)
def test_include_duplicate_section_conflicts(tmp_path, section, inline):
    _write(tmp_path / "child.yaml", _CHILD_ALL)
    parent = _write(
        tmp_path / "parent.yaml", inline + "include:\n  - child.yaml\n"
    )
    with pytest.raises(
        ConfigError, match="multiple {} configs".format(section)
    ):
        config.parse_config_file(parent)


# ---------------------------------------------------------------------------
# directory loading: per-section conflicts + unreadable files
# ---------------------------------------------------------------------------


def test_parse_config_dir_multiple_cluster(tmp_path):
    cluster = (
        "cluster:\n"
        "  listen: '0.0.0.0:8443'\n" + _TLS + "  peers:\n"
        "    - host: b:8443\n"
    )
    _write(tmp_path / "a.yaml", cluster)
    _write(tmp_path / "b.yaml", cluster)
    with pytest.raises(ConfigError, match="Multiple 'cluster'"):
        config.parse_config(str(tmp_path))


def test_parse_config_dir_multiple_mcp(tmp_path):
    _write(tmp_path / "a.yaml", "mcp:\n  enabled: true\n")
    _write(tmp_path / "b.yaml", "mcp:\n  enabled: false\n")
    with pytest.raises(ConfigError, match="Multiple 'mcp'"):
        config.parse_config(str(tmp_path))


def test_parse_config_dir_multiple_logging(tmp_path):
    _write(tmp_path / "a.yaml", "logging:\n  version: 1\n")
    _write(tmp_path / "b.yaml", "logging:\n  version: 1\n")
    with pytest.raises(ConfigError, match="Multiple 'logging'"):
        config.parse_config(str(tmp_path))


def test_parse_config_dir_records_oserror(tmp_path, monkeypatch):
    _write(tmp_path / "a.yaml", "jobs: []\n")
    real = config.parse_config_file

    def flaky(path, *args, **kwargs):
        if path.endswith("a.yaml"):
            raise OSError("disk on fire")
        return real(path, *args, **kwargs)

    monkeypatch.setattr(config, "parse_config_file", flaky)
    # the collected per-file errors surface as one aggregate ConfigError
    with pytest.raises(ConfigError, match="disk on fire"):
        config.parse_config(str(tmp_path))
