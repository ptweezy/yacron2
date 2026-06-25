"""The leadership seam: ABC defaults, never-skip semantics, factory dispatch.

These exercise :mod:`yacron2.leadership` with tiny in-test backends (no
network, no crypto), which is what carries its coverage in CI.
"""

import pytest

from yacron2.config import parse_config_string
from yacron2.leadership import (
    LeadershipBackend,
    LeaseBackend,
    make_backend,
)


class _FakeBackend(LeadershipBackend):
    """Minimal concrete backend for exercising the ABC's defaulted methods."""

    def __init__(self, *, quorate=True, leader=None, node_name="node-a"):
        self.config = {}  # type: ignore[assignment]
        self.node_name = node_name
        self.distribution = "single-leader"
        self._quorate = quorate
        self._leader = leader

    async def start(self):  # pragma: no cover - not exercised
        ...

    async def stop(self):  # pragma: no cover - not exercised
        ...

    def is_leader(self):
        return self._leader == self.node_name

    def leader_name(self):
        return self._leader

    def is_quorate(self):
        return self._quorate

    def view_dict(self):
        return {"backend": "fake"}


def test_defaulted_per_job_collapses_to_leader():
    leader = _FakeBackend(leader="node-a")
    assert leader.is_job_owner("j") is True
    assert leader.job_owner("j") == "node-a"
    follower = _FakeBackend(leader="node-b")
    assert follower.is_job_owner("j") is False
    assert follower.job_owner("j") == "node-b"


def test_defaulted_single_holder_invariants():
    b = _FakeBackend()
    assert b.has_conflict() is False
    assert b.conflict_names() == []
    assert b.conflicting_sizes() == []
    assert b.cluster_size() == 1
    assert b.quorum() == 1
    assert b.reboot_ran("j") is False
    assert b.tls_files_changed() is False


async def test_mark_reboot_ran_is_a_noop():
    assert await _FakeBackend().mark_reboot_ran("j") is None


def test_never_skip_quorate_leader_runs():
    b = _FakeBackend(quorate=True, leader="node-a")
    assert b.is_available_leader() is True
    assert b.available_leader_name() == "node-a"


def test_never_skip_quorate_follower_defers():
    # a healthy follower that can see the holder defers (does not double-run)
    b = _FakeBackend(quorate=True, leader="node-b")
    assert b.is_available_leader() is False
    assert b.available_leader_name() == "node-b"


def test_never_skip_unreachable_runs_self():
    # store unreachable -> run anyway (the locked PreferLeader decision)
    b = _FakeBackend(quorate=False, leader=None)
    assert b.is_available_leader() is True
    assert b.available_leader_name() == "node-a"


def test_never_skip_quorate_unknown_leader_falls_back_to_self():
    # quorate but holder unknown (e.g. a lost create race): the name-returning
    # and boolean gates must AGREE that this node runs, so a scheduled
    # PreferLeader job and its @reboot equivalent never diverge.
    b = _FakeBackend(quorate=True, leader=None)
    assert b.available_leader_name() == "node-a"
    assert b.is_available_leader() is True
    assert b.available_job_owner("j") == "node-a"
    assert b.is_available_job_owner("j") is True


def test_available_job_owner_mirrors_leader_paths():
    unreachable = _FakeBackend(quorate=False)
    assert unreachable.is_available_job_owner("j") is True
    assert unreachable.available_job_owner("j") == "node-a"
    own = _FakeBackend(quorate=True, leader="node-a")
    assert own.is_available_job_owner("j") is True
    assert own.available_job_owner("j") == "node-a"
    other = _FakeBackend(quorate=True, leader="node-b")
    assert other.is_available_job_owner("j") is False
    assert other.available_job_owner("j") == "node-b"
    # quorate but holder unknown -> name self for the never-skip owner
    unknown = _FakeBackend(quorate=True, leader=None)
    assert unknown.available_job_owner("j") == "node-a"


class _BareLease(LeaseBackend):
    """A LeaseBackend that does not override lease_detail (covers the base)."""

    backend_name = "bare"

    def __init__(self):
        super().__init__({"nodeName": "node-a"}, lambda: "v1:x")  # type: ignore[arg-type]
        self._leader = None
        self._quorate = False

    async def start(self):  # pragma: no cover - not exercised
        ...

    async def stop(self):  # pragma: no cover - not exercised
        ...

    def is_leader(self):
        return False

    def leader_name(self):
        return self._leader

    def is_quorate(self):
        return self._quorate


def test_lease_backend_view_dict_shape():
    b = _BareLease()
    view = b.view_dict()
    assert view["backend"] == "bare"
    assert view["peers"] == []
    assert view["lease"] == {}  # the base lease_detail default
    assert view["elect_leader"] is True
    assert view["distribution"] == "single-leader"
    assert view["cluster_size"] == 1
    assert view["quorum"] == 1
    assert view["conflict"] is False
    assert view["conflict_names"] == []
    assert view["size_conflict"] is False
    assert view["quorate"] is False
    assert view["leader"] is None
    assert view["is_leader"] is False
    assert view["node_name"] == "node-a"
    assert view["job_set_id"] == "v1:x"


# --- make_backend dispatch ------------------------------------------------

_K8S_YAML = """
cluster:
  backend: kubernetes
  nodeName: node-a
  kubernetes:
    leaseName: yl
    leaseNamespace: ns
"""

_ETCD_YAML = """
cluster:
  backend: etcd
  nodeName: node-a
  etcd:
    endpoints:
      - http://127.0.0.1:2379
"""

# fake (nonexistent) cert paths: the config builds (it only validates the
# host:port form, not the files); make_backend then builds a ClusterManager,
# which loads the missing TLS files at construction.
_GOSSIP_YAML = """
cluster:
  listen: "0.0.0.0:8443"
  tls:
    ca: /nonexistent/ca.pem
    cert: /nonexistent/node.pem
    key: /nonexistent/node.key
  peers:
    - host: yacron-b:8443
"""


def test_make_backend_kubernetes():
    from yacron2.backends.kubernetes import KubernetesBackend

    cfg = parse_config_string(_K8S_YAML, "").cluster_config
    backend = make_backend(cfg, lambda: "v1:x")
    assert isinstance(backend, KubernetesBackend)
    assert backend.backend_name == "kubernetes"


def test_make_backend_etcd():
    from yacron2.backends.etcd import EtcdBackend

    cfg = parse_config_string(_ETCD_YAML, "").cluster_config
    backend = make_backend(cfg, lambda: "v1:x")
    assert isinstance(backend, EtcdBackend)
    assert backend.backend_name == "etcd"


def test_make_backend_gossip_dispatch():
    cfg = parse_config_string(_GOSSIP_YAML, "").cluster_config
    # the gossip branch builds a ClusterManager, which loads the (missing) TLS
    # files in __init__ -> OSError. We only assert the dispatch path is taken.
    with pytest.raises(OSError):
        make_backend(cfg, lambda: "v1:x")
