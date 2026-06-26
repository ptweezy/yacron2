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
import json
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from yacron2.config import ClusterConfig, ConfigError

#: the key (kubernetes Lease annotation / etcd sibling key) under which a
#: lease backend persists the set of @reboot one-shots already run in the
#: cluster, so a failover holder does not re-run them.
REBOOT_RAN_KEY = "yacron2.io/reboot-ran"


def encode_reboot_ran(job_set_id: str, jobs: Set[str]) -> str:
    """Encode the @reboot-ran set + its job-set fingerprint for the store."""
    return json.dumps(
        {"jobSetId": job_set_id, "jobs": sorted(jobs)},
        separators=(",", ":"),
    )


def decode_reboot_ran(raw: Optional[str]) -> Tuple[Optional[str], Set[str]]:
    """Decode a stored @reboot-ran blob to ``(job_set_id, jobs)``.

    Tolerant of any malformed/absent value (returns ``(None, set())``), since a
    backend must never crash its renew loop on a junk annotation/key written by
    something else.
    """
    if not raw:
        return None, set()
    try:
        data = json.loads(raw)
    except (ValueError, TypeError):
        return None, set()
    if not isinstance(data, dict):
        return None, set()
    job_set_id = data.get("jobSetId")
    jobs = data.get("jobs")
    if not isinstance(jobs, list):
        jobs = []
    return (
        job_set_id if isinstance(job_set_id, str) else None,
        {j for j in jobs if isinstance(j, str)},
    )


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

    def tls_files_loadable(self) -> bool:
        """Whether the current on-disk TLS material can be loaded right now.

        Used by :meth:`yacron2.cron.Cron.start_stop_cluster` as a cheap,
        bind-free dry-run *before* it tears the manager down to apply an
        in-place cert rotation, so a half-written / briefly-absent cert seen
        mid-rotation cannot leave the node with no manager (which would wedge
        ``Leader`` / ``PreferLeader`` jobs closed for up to one reload).  Lease
        backends have no per-node mTLS material to pre-validate -- and never
        report :meth:`tls_files_changed` -- so this is never consulted for
        them; the default is always ``True``.  The gossip backend overrides it.
        """
        return True

    # --- never-skip PreferLeader defaults (the locked lease semantics) ----

    def available_leader_name(self) -> str:
        """The ``PreferLeader`` owner's *display* name, never ``None``.

        Used only to *show* the owner (``GET /cluster`` / the dashboard); the
        run/skip decision is :meth:`is_available_leader`, computed from
        authoritative self-state, not from this string.  When the store is
        unreachable (not quorate) or this node is itself the holder, it names
        *itself*; otherwise it reports the observed holder's display identity
        (falling back to itself when the holder is unknown).  Because this can
        return a display identity that coincides with another node's
        ``node_name`` (a duplicate ``nodeName``), it must **not** be string-
        compared against ``node_name`` to gate a run -- see
        :meth:`is_available_leader`.
        """
        if not self.is_quorate():
            return self.node_name
        if self.is_leader():
            return self.node_name
        return self.leader_name() or self.node_name

    def is_available_leader(self) -> bool:
        """Whether this node should run a ``PreferLeader`` job this cycle.

        Decided from this node's own *authoritative* state, never from a
        comparison of two display names.  ``True`` exactly when:

        * the store is unreachable (not quorate) -- the never-skip choice: run
          anyway, may double-run; or
        * this node holds the lease (:meth:`is_leader`); or
        * the store is reachable but the holder is genuinely unknown
          (:meth:`leader_name` is ``None`` -- a lost ``create`` race), where
          never-skip names *this* node so the job still runs somewhere.

        Otherwise a different node holds the lease and this node defers.  This
        deliberately does **not** string-compare :meth:`available_leader_name`
        (the holder's *display* identity) against ``node_name``: a lease
        backend's display identity can legitimately equal another node's
        ``node_name`` (a duplicate ``nodeName`` -- the default for a Kubernetes
        Deployment -- or an ``identity`` set to a peer's name), which would
        otherwise make a quorate *follower* believe it is the available leader
        and run every ``PreferLeader`` job, silently, on every replica.  The
        fence itself is per-process unique (a lease id / ``#<token>`` suffix),
        so :meth:`is_leader` self-recognises correctly regardless of any
        display-name collision.
        """
        if not self.is_quorate():
            return True
        if self.is_leader():
            return True
        return self.leader_name() is None

    def available_job_owner(self, job_name: str) -> str:
        """``available_leader_name`` for the per-job (spread) shape.

        Lease backends reject ``distribution: spread`` at config time, so this
        simply mirrors the single-leader path -- including recognising this
        node as the owner via :meth:`is_job_owner` (which for a single holder
        collapses to :meth:`is_leader`), so an identity that differs from
        ``node_name`` cannot make the owner skip its own job (see
        :meth:`available_leader_name`).
        """
        if not self.is_quorate():
            return self.node_name
        if self.is_job_owner(job_name):
            return self.node_name
        return self.job_owner(job_name) or self.node_name

    def is_available_job_owner(self, job_name: str) -> bool:
        """Per-job (spread) analogue of :meth:`is_available_leader`.

        Decided from authoritative self-state, not a display-name comparison,
        for the same duplicate-identity reason.  Lease backends reject
        ``distribution: spread`` at config time, so for them per-job ownership
        collapses to leadership and this mirrors :meth:`is_available_leader`.
        """
        if not self.is_quorate():
            return True
        if self.is_job_owner(job_name):
            return True
        return self.job_owner(job_name) is None


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
        # @reboot "already ran" tracking, persisted in the store scoped to
        # the current job-set so a FAILOVER holder does not re-run a
        # deferred @reboot Leader one-shot (the gossip backend gossips this; a
        # lease backend has no peer set, so it persists it instead).
        # ``_reboot_ran`` is what we have read back from the store;
        # ``_reboot_ran_local`` is what this node has run but not yet confirmed
        # persisted. reboot_ran checks the union (see cron's deferred-reboot
        # handling). Note the semantics differ slightly from gossip: keyed by
        # job-set id, an @reboot Leader job runs once per job CONFIG on a lease
        # cluster (persisted across restarts), not once per process boot.
        self._reboot_ran: Set[str] = set()
        self._reboot_ran_local: Set[str] = set()
        # the job-set id under which the current ``_reboot_ran_local`` marks
        # were recorded, so they can be dropped when the job set changes (see
        # _reconcile_local_reboot_ran). None until the first mark/read.
        self._reboot_ran_local_job_set_id: Optional[str] = None

    def _reconcile_local_reboot_ran(self) -> None:
        """Drop our own @reboot-ran marks when the job set changed.

        ``_reboot_ran_local`` records the one-shots *this process* ran, keyed
        implicitly by the job set live when it ran them.  A config reload that
        redefines an @reboot job changes the job-set id, and such a job must be
        allowed to run again -- so a mark made under an older config must not
        survive (and, worse, be re-persisted to the store stamped with the new
        id, which would suppress the genuinely-new one-shot cluster-wide).
        Mirrors the gossip backend's
        :meth:`yacron2.cluster.ClusterManager._reconcile_job_set_id`, which
        clears its whole ran-set on a job-set
        change for exactly the same reason.  ``_reboot_ran`` (the store-read
        set) is reconciled separately by :meth:`_observe_reboot_ran`.
        """
        current = self.get_job_set_id()
        if self._reboot_ran_local_job_set_id != current:
            self._reboot_ran_local = set()
            self._reboot_ran_local_job_set_id = current

    def reboot_ran(self, job_name: str) -> bool:
        self._reconcile_local_reboot_ran()
        return (
            job_name in self._reboot_ran or job_name in self._reboot_ran_local
        )

    async def mark_reboot_ran(self, job_name: str) -> None:
        self._reconcile_local_reboot_ran()
        self._reboot_ran_local.add(job_name)
        await self._persist_reboot_ran()

    async def _persist_reboot_ran(self) -> None:  # pragma: no cover
        """Eagerly persist the local @reboot-ran set to the store.

        Default no-op (the kubernetes backend folds the set into its periodic
        Lease write instead); the etcd backend overrides this to write its
        sibling key at once.
        """

    def _observe_reboot_ran(
        self, stored_job_set_id: Optional[str], stored: Set[str]
    ) -> None:
        """Fold a store-read @reboot-ran set into the cache, job-set-scoped.

        A stored set tagged with a DIFFERENT job-set id belongs to an older
        configuration, so it is ignored (the @reboot job runs again under the
        new job set), matching gossip's job-set scoping of ``advertised_ran``.
        """
        if stored_job_set_id == self.get_job_set_id():
            self._reboot_ran = set(stored)
        else:
            self._reboot_ran = set()

    def reboot_ran_annotation(
        self, existing: Optional[Dict[str, str]] = None
    ) -> Optional[Dict[str, str]]:
        """The annotations to write back: carry ``existing`` forward, set ours.

        Returns ``None`` when there is nothing to write and no existing
        annotations to preserve, so a backend can skip an empty annotations
        block. Used by the kubernetes backend's Lease write.
        """
        self._reconcile_local_reboot_ran()
        combined = self._reboot_ran | self._reboot_ran_local
        annotations = dict(existing or {})
        if combined:
            annotations[REBOOT_RAN_KEY] = encode_reboot_ran(
                self.get_job_set_id(), combined
            )
        return annotations or None

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
            # duplicate-name, size-divergence, or coordination-policy conflict
            # to report (a single holder, distribution pinned single-leader).
            "conflict": False,
            "conflict_names": [],
            "size_conflict": False,
            "conflicting_sizes": [],
            "policy_conflict": False,
            "conflicting_policies": [],
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
