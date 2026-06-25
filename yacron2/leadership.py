"""Pluggable leadership backends behind one interface.

All of yacron2's leader-gating funnels through a small, stable seam: the
scheduler (:mod:`yacron2.cron`) only ever asks *am I allowed to run this job?*
through a handful of methods on whatever object ``cluster.backend`` selected.
This module defines that seam as :class:`LeadershipBackend` and the
:func:`make_backend` factory that builds the chosen one.

Three backends share the seam, each a different point on the CAP trade-off:

* **gossip** (default) -- the original mTLS, no-shared-state, best-effort
  quorum election in :mod:`yacron2.cluster`.  Zero new dependencies; can only
  ever be best-effort (see that module's docstring).
* **kubernetes** -- a ``coordination.k8s.io/v1`` ``Lease`` (see
  :mod:`yacron2.backends.kubernetes`).  Fenced, exactly-once while the lease
  store is reachable.
* **etcd** -- a lease-backed key/election (see :mod:`yacron2.backends.etcd`).
  Same fenced guarantee, against an etcd cluster.

The two lease backends talk to their store over plain HTTP via the core
``aiohttp`` dependency (the Kubernetes apiserver's REST API, etcd's v3
gRPC-gateway JSON API) -- *not* the heavyweight official client libraries.
That keeps the core install zero-new-dep and, by avoiding grpc/protobuf wheels,
keeps yacron2's wide architecture coverage intact.

The surface is split three ways so a new lease backend stays tiny:

* **core abstract** -- :meth:`~LeadershipBackend.start`,
  :meth:`~LeadershipBackend.stop`, :meth:`~LeadershipBackend.is_leader`,
  :meth:`~LeadershipBackend.leader_name`,
  :meth:`~LeadershipBackend.is_quorate`,
  :meth:`~LeadershipBackend.view_dict`.  Every backend implements these.
* **defaulted** -- concrete bodies on the ABC that a single-holder lease
  backend inherits unchanged (per-job ownership collapses to the leader, there
  are no gossip-style conflicts, the cluster is logically size 1, ``@reboot``
  gossip is a no-op, TLS rotation does not apply).  Gossip overrides every one
  of them with its real, richer behaviour, so the gossip refactor is
  byte-identical.
* **never-skip (PreferLeader)** -- the ``available_*`` family, defaulted here
  to the locked decision for lease backends: a node that currently *cannot*
  reach the store runs a ``PreferLeader`` job anyway (it may double-run), while
  a node that can see the holder defers.  ``Leader`` stays fail-closed.
"""

import abc
from typing import Any, Callable, Dict, List, Optional

from yacron2.config import ClusterConfig, ConfigError


class LeadershipBackend(abc.ABC):
    """The seam every leader-gating call in :mod:`yacron2.cron` goes through.

    A backend owns the question "may this node run leader-gated work now",
    exposed through the methods below.  Concrete subclasses must set the three
    attributes (``config``, ``node_name``, ``distribution``) and implement the
    *core abstract* methods; the rest have defaults suited to a single-holder
    lease backend, which gossip overrides.
    """

    #: the resolved cluster config block this backend was built from
    config: ClusterConfig
    #: a stable, human-readable identity for this node (defaults to hostname)
    node_name: str
    #: "single-leader" or "spread"; lease backends are always single-leader
    distribution: str

    # --- core: every backend implements these ----------------------------

    @abc.abstractmethod
    async def start(self) -> None:
        """Begin maintaining leadership (launch renew loops, listeners)."""

    @abc.abstractmethod
    async def stop(self) -> None:
        """Stop and release leadership, best-effort, for fast failover."""

    @abc.abstractmethod
    def is_leader(self) -> bool:
        """Whether this node currently holds leadership (quorum-gated)."""

    @abc.abstractmethod
    def leader_name(self) -> Optional[str]:
        """The current leader as this node sees it, or ``None`` if unknown."""

    @abc.abstractmethod
    def is_quorate(self) -> bool:
        """Whether this node currently has a trustworthy view of leadership.

        For gossip this is the quorum gate; for a lease backend it is whether
        the node has a *fresh* successful read of the lease store.  When false,
        ``Leader`` jobs fail closed and the never-skip ``available_*`` defaults
        below let ``PreferLeader`` jobs run anyway.
        """

    @abc.abstractmethod
    def view_dict(self) -> Dict[str, Any]:
        """The cluster view for ``GET /cluster`` and the dashboard.

        Always carries a ``"backend"`` key naming the active backend.
        """

    # --- defaulted: a single-holder lease backend inherits these unchanged;
    #     gossip (ClusterManager) overrides every one ----------------------

    def is_job_owner(self, job_name: str) -> bool:
        """Per-job ownership collapses to leadership for a single holder."""
        return self.is_leader()

    def job_owner(self, job_name: str) -> Optional[str]:
        """Per-job owner collapses to the single leader."""
        return self.leader_name()

    def has_conflict(self) -> bool:
        """A lease store is authoritative: no split-identity gate applies."""
        return False

    def conflict_names(self) -> List[str]:
        return []

    def conflicting_sizes(self) -> List[int]:
        return []

    def cluster_size(self) -> int:
        """A lease backend is logically a single holder (size 1, quorum 1)."""
        return 1

    def quorum(self) -> int:
        return 1

    def reboot_ran(self, job_name: str) -> bool:
        """No cross-node ``@reboot`` gossip: the holder runs the one-shot."""
        return False

    async def mark_reboot_ran(self, job_name: str) -> None:  # noqa: B027
        """No-op: there is no peer set to gossip the run to."""

    def tls_files_changed(self) -> bool:
        """Lease backends are not restarted on an mTLS cert rotation."""
        return False

    # --- never-skip PreferLeader defaults (the locked lease semantics) ----

    def available_leader_name(self) -> str:
        """Leader for the ``PreferLeader`` policy, never returning ``None``.

        When the store is unreachable (not quorate) this node names *itself* --
        the never-skip choice (it runs, and may double-run).  Otherwise it
        defers to the observed leader (falling back to itself if unknown).
        """
        if not self.is_quorate():
            return self.node_name
        return self.leader_name() or self.node_name

    def is_available_leader(self) -> bool:
        """Whether this node should run a ``PreferLeader`` job this cycle.

        Derived from :meth:`available_leader_name` so the boolean gate and the
        name it would resolve to can never disagree -- in particular in the
        quorate-but-unknown-holder window (a lost ``create`` race), where the
        never-skip rule names *this* node and so must also run it.  ``True``
        when the store is unreachable (run anyway, never skip) or this node is
        the available leader.
        """
        return self.available_leader_name() == self.node_name

    def available_job_owner(self, job_name: str) -> str:
        """``available_leader_name`` for the per-job (spread) shape.

        Lease backends reject ``distribution: spread`` at config time, so this
        simply mirrors the single-leader path.
        """
        if not self.is_quorate():
            return self.node_name
        return self.job_owner(job_name) or self.node_name

    def is_available_job_owner(self, job_name: str) -> bool:
        return self.available_job_owner(job_name) == self.node_name


class LeaseBackend(LeadershipBackend):
    """Shared base for the single-holder lease backends (kubernetes, etcd).

    It pins ``distribution`` to ``"single-leader"`` (lease backends reject
    ``spread``) and provides the common lease-shaped :meth:`view_dict`; the
    never-skip ``available_*`` semantics are inherited from
    :class:`LeadershipBackend`.  Subclasses implement :meth:`start`,
    :meth:`stop`, and the three live-state reads (:meth:`is_leader`,
    :meth:`leader_name`, :meth:`is_quorate`), plus :meth:`lease_detail` for the
    backend-specific block in the view.
    """

    #: backend name surfaced in view_dict (subclasses set this)
    backend_name: str = "lease"

    def __init__(
        self,
        config: ClusterConfig,
        get_job_set_id: Callable[[], str],
    ) -> None:
        self.config = config
        self.get_job_set_id = get_job_set_id
        self.node_name = config["nodeName"]
        # lease backends are always a single holder; spread is rejected at
        # config time, so per-job ownership never diverges from leadership.
        self.distribution = "single-leader"

    def lease_detail(self) -> Dict[str, Any]:
        """Backend-specific ``"lease"`` block for :meth:`view_dict`.

        Default is empty; subclasses surface holder/expiry/name details.
        """
        return {}

    def view_dict(self) -> Dict[str, Any]:
        leader = self.leader_name()
        return {
            "backend": self.backend_name,
            "node_name": self.node_name,
            "job_set_id": self.get_job_set_id(),
            "cluster_size": self.cluster_size(),
            "quorum": self.quorum(),
            "elect_leader": True,
            "distribution": self.distribution,
            # a lease store is authoritative: there is no gossip-style
            # duplicate-name or size-divergence conflict to report.
            "conflict": False,
            "conflict_names": [],
            "size_conflict": False,
            "conflicting_sizes": [],
            "quorate": self.is_quorate(),
            "leader": leader,
            "is_leader": self.is_leader(),
            # no static peer set; the lease store is the source of truth.
            "peers": [],
            "lease": self.lease_detail(),
        }


def make_backend(
    cluster_config: ClusterConfig,
    get_job_set_id: Callable[[], str],
) -> LeadershipBackend:
    """Build the leadership backend named by ``cluster.backend``.

    Imports are deferred so the lease backends (and any future heavyweight
    dependency) never enter the import graph for the common gossip case.  The
    schema only ever admits the three known names, so the final ``raise`` is a
    defensive backstop, not a user-facing path.
    """
    backend = cluster_config.get("backend", "gossip")
    if backend == "gossip":
        from yacron2.cluster import ClusterManager

        return ClusterManager(cluster_config, get_job_set_id)
    if backend == "kubernetes":
        from yacron2.backends.kubernetes import KubernetesBackend

        return KubernetesBackend(cluster_config, get_job_set_id)
    if backend == "etcd":
        from yacron2.backends.etcd import EtcdBackend

        return EtcdBackend(cluster_config, get_job_set_id)
    raise ConfigError(  # pragma: no cover - unreachable; schema-validated
        "unknown cluster.backend {!r}".format(backend)
    )
