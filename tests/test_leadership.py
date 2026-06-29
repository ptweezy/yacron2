"""The leadership seam: ABC defaults, never-skip semantics, factory dispatch.

These exercise :mod:`yacron2.leadership` with tiny in-test backends (no
network, no crypto), which is what carries its coverage in CI.
"""

import pytest

from yacron2.config import parse_config_string
from yacron2.leadership import (
    REBOOT_RAN_KEY,
    LeadershipBackend,
    LeaseBackend,
    decode_reboot_ran,
    encode_reboot_ran,
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


class _IdentityLease(LeadershipBackend):
    """A lease-style backend whose ``leader_name`` is the holder's display
    *identity* (which may differ from ``node_name``, as KubernetesBackend's
    does), with an independent ``is_leader`` flag -- the H1 shape.
    """

    def __init__(self, *, quorate, is_leader, holder, node_name="node-a"):
        self.config = {}  # type: ignore[assignment]
        self.node_name = node_name
        self.distribution = "single-leader"
        self._quorate = quorate
        self._is_leader = is_leader
        self._holder = holder

    async def start(self):  # pragma: no cover - not exercised
        ...

    async def stop(self):  # pragma: no cover - not exercised
        ...

    def is_leader(self):
        return self._is_leader

    def leader_name(self):
        return self._holder if self._quorate else None

    def is_quorate(self):
        return self._quorate

    def view_dict(self):  # pragma: no cover - not exercised
        return {"backend": "idlease"}


def test_never_skip_holder_runs_when_identity_differs_from_node_name():
    # H1 regression: a lease backend reports the holder's display *identity*
    # (cluster.kubernetes.identity), which may differ from node_name. The
    # holder must still recognise itself and run PreferLeader, else every node
    # defers and the job is silently skipped cluster-wide while quorate.
    holder = _IdentityLease(
        quorate=True, is_leader=True, holder="pod-xyz", node_name="node-a"
    )
    assert holder.is_available_leader() is True
    assert holder.available_leader_name() == "node-a"
    assert holder.is_available_job_owner("j") is True
    assert holder.available_job_owner("j") == "node-a"
    # a healthy follower (sees the holder identity, is not the holder) defers
    follower = _IdentityLease(
        quorate=True, is_leader=False, holder="pod-xyz", node_name="node-b"
    )
    assert follower.is_available_leader() is False
    assert follower.available_leader_name() == "pod-xyz"
    assert follower.is_available_job_owner("j") is False
    # store unreachable -> run anyway, regardless of identity
    isolated = _IdentityLease(
        quorate=False, is_leader=False, holder=None, node_name="node-a"
    )
    assert isolated.is_available_leader() is True
    assert isolated.available_leader_name() == "node-a"


class _FakeLease(LeaseBackend):
    """A minimal concrete lease backend for the @reboot-ran cache tests."""

    def __init__(self, job_set_id="v1:js"):
        super().__init__({"nodeName": "node-a"}, lambda: job_set_id)  # type: ignore[arg-type]

    async def start(self):  # pragma: no cover - not exercised
        ...

    async def stop(self):  # pragma: no cover - not exercised
        ...

    def is_leader(self):
        return False

    def leader_name(self):
        return None

    def is_quorate(self):
        return False


def test_encode_decode_reboot_ran_roundtrip():
    jsid, jobs = decode_reboot_ran(encode_reboot_ran("v1:js", {"b", "a"}))
    assert jsid == "v1:js"
    assert jobs == {"a", "b"}


def test_decode_reboot_ran_tolerates_garbage():
    assert decode_reboot_ran(None) == (None, set())
    assert decode_reboot_ran("") == (None, set())
    assert decode_reboot_ran("not json") == (None, set())
    assert decode_reboot_ran("[1,2]") == (None, set())  # not a dict
    jsid, jobs = decode_reboot_ran('{"jobSetId": 5, "jobs": "x"}')
    assert jsid is None and jobs == set()  # wrong types ignored


def test_decode_reboot_ran_tolerates_deeply_nested_json():
    # H1/C1 regression: a deeply-nested value makes json.loads raise
    # RecursionError (a RuntimeError subclass, NOT a ValueError/TypeError). A
    # junk store value must never escape decode and crash a backend's renew
    # loop -- for the kubernetes backend that would wedge the cluster
    # permanently non-quorate (decode runs before _apply_round advances the
    # quorum-freshness clock, and the poison annotation is never overwritten).
    poison = "[" * 100_000  # a valid prefix of a deeply-nested JSON array
    assert decode_reboot_ran(poison) == (None, set())
    nested_obj = '{"a":' * 50_000 + "1" + "}" * 50_000
    assert decode_reboot_ran(nested_obj) == (None, set())


async def test_lease_backend_reboot_ran_cache():
    # H2: mark_reboot_ran records locally (so this node won't re-run), and the
    # _persist_reboot_ran is a harmless no-op for the base test backend.
    b = _FakeLease()
    assert b.reboot_ran("j") is False
    await b.mark_reboot_ran("j")
    assert b.reboot_ran("j") is True


async def test_local_reboot_ran_cleared_on_job_set_change():
    # F02/F13: a mark recorded under one job set must NOT survive (and be
    # re-stamped under) a NEW job set -- a redefined @reboot one-shot must be
    # allowed to run again. The lease base drops _reboot_ran_local when the
    # live job-set id changes, mirroring gossip's whole-set reconcile.
    live = {"id": "v1:old"}
    b = _FakeLease()
    b.get_job_set_id = lambda: live["id"]
    await b.mark_reboot_ran("migrate")
    assert b.reboot_ran("migrate") is True
    # the annotation is stamped under the CURRENT job set
    jsid, jobs = decode_reboot_ran(b.reboot_ran_annotation()[REBOOT_RAN_KEY])
    assert jsid == "v1:old" and jobs == {"migrate"}
    # config reload redefines the job -> new job-set id: the stale local mark
    # is dropped (so the redefined one-shot runs again) and is NOT re-published
    # under the new id (which would suppress it cluster-wide).
    live["id"] = "v2:new"
    assert b.reboot_ran("migrate") is False
    assert b.reboot_ran_annotation() is None


def test_observe_reboot_ran_is_job_set_scoped():
    b = _FakeLease(job_set_id="v1:js")
    b._observe_reboot_ran("v1:js", {"a"})
    assert b.reboot_ran("a") is True
    # a stored set tagged with a DIFFERENT job set belongs to an older config
    # and is ignored, so the one-shot runs again under the new job set.
    b._observe_reboot_ran("v0:old", {"b"})
    assert b.reboot_ran("b") is False


def test_reboot_ran_annotation_carries_forward_and_encodes():
    b = _FakeLease(job_set_id="v1:js")
    # a mark recorded under the current job set: mark_reboot_ran stamps the
    # job-set id alongside the local set (see _reconcile_local_reboot_ran), so
    # simulate that here rather than poking _reboot_ran_local raw (which the
    # reconcile would otherwise drop as belonging to an unknown job set).
    b._reboot_ran_local.add("a")
    b._reboot_ran_local_job_set_id = "v1:js"
    ann = b.reboot_ran_annotation({"other/key": "keep"})
    assert ann["other/key"] == "keep"  # someone else's annotation preserved
    jsid, jobs = decode_reboot_ran(ann[REBOOT_RAN_KEY])
    assert jsid == "v1:js" and jobs == {"a"}
    # nothing to write and no existing annotations -> None (skip the block)
    assert _FakeLease().reboot_ran_annotation(None) is None


def test_never_skip_duplicate_nodename_follower_does_not_self_elect():
    # H3 regression: two replicas share the same nodeName (a Kubernetes
    # Deployment / shared HOSTNAME). The follower observes the holder's DISPLAY
    # identity, which -- because the names collide -- equals its OWN node_name.
    # A name-comparison gate (available_leader_name() == node_name) would make
    # the follower think it is the available leader and run all PreferLeader
    # job on every replica, silently. The self-state gate must keep it
    # deferring: it is quorate, it is not the lease holder, and the holder is
    # known, so it stands down.
    follower = _IdentityLease(
        quorate=True, is_leader=False, holder="node-a", node_name="node-a"
    )
    assert follower.is_available_leader() is False
    assert follower.is_available_job_owner("j") is False


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
