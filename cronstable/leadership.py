"""Pluggable leadership backends behind one interface.

All of cronstable's leader-gating funnels through a small, stable seam: the
scheduler (:mod:`cronstable.cron`) only ever asks *am I allowed to run this
job?*
through a handful of methods on whatever object ``cluster.backend`` selected.
This module defines that seam as :class:`LeadershipBackend` and the
:func:`make_backend` factory that builds the chosen one.

Four backends share the seam, each a different point on the CAP trade-off:

* **gossip** (default) -- the original mTLS, no-shared-state, best-effort
  quorum election in :mod:`cronstable.cluster`.  Zero new dependencies; can
  only ever be best-effort (see that module's docstring).
* **kubernetes** -- a ``coordination.k8s.io/v1`` ``Lease`` (see
  :mod:`cronstable.backends.kubernetes`).  Fenced, exactly-once while the lease
  store is reachable.
* **etcd** -- a lease-backed key/election (see
:mod:`cronstable.backends.etcd`).
  Same fenced guarantee, against an etcd cluster.
* **filesystem** -- a flock-guarded TTL lease on a shared POSIX mount (see
  :mod:`cronstable.backends.filesystem`).  Fenced under NTP-bounded clock skew;
  no coordination service at all -- the mount is the store.

The kubernetes/etcd backends talk to their store over plain HTTP via the core
``aiohttp`` dependency (the Kubernetes apiserver's REST API, etcd's v3
gRPC-gateway JSON API) -- *not* the heavyweight official client libraries --
and the filesystem backend needs only the standard library.  That keeps the
core install zero-new-dep and, by avoiding grpc/protobuf wheels, keeps
cronstable's wide architecture coverage intact.

The surface is split three ways so a new lease backend stays tiny:

* **core abstract** -- :meth:`~LeadershipBackend.start`,
  :meth:`~LeadershipBackend.stop`, :meth:`~LeadershipBackend.is_leader`,
  :meth:`~LeadershipBackend.leader_name`,
  :meth:`~LeadershipBackend.is_quorate`,
  :meth:`~LeadershipBackend.view_dict`.  Every backend implements these.
* **defaulted** -- concrete bodies on the ABC that a single-holder lease
  backend inherits unchanged (per-job ownership collapses to the leader, there
  are no gossip-style conflicts, the cluster is logically size 1, the view is
  never mid-convergence, TLS rotation does not apply).  The ``@reboot``
  "already ran" defaults are the one pair :class:`LeaseBackend` replaces: it
  persists the ran-set in the lease store (:data:`REBOOT_RAN_KEY`), scoped to
  the job-set id, so a *failover* holder does not re-run a one-shot.  Gossip
  overrides every one of them with its real, richer behaviour, so the gossip
  refactor is byte-identical.
* **never-skip (PreferLeader)** -- the ``available_*`` family, defaulted here
  to the locked decision for lease backends: a node that currently *cannot*
  reach the store runs a ``PreferLeader`` job anyway (it may double-run), while
  a node that can see the holder defers.  ``Leader`` stays fail-closed.
"""

import abc
import json
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from cronstable.config import ClusterConfig, ConfigError

#: the key (kubernetes Lease annotation / etcd sibling key) under which a
#: lease backend persists the set of @reboot one-shots already run in the
#: cluster, so a failover holder does not re-run them.
REBOOT_RAN_KEY = "cronstable.io/reboot-ran"


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
    except (ValueError, TypeError, RecursionError):
        # RecursionError (a RuntimeError subclass, NOT a ValueError) is what
        # CPython's json decoder raises on a deeply-nested value; a junk
        # annotation/key written by something else must never crash the renew
        # loop (mirrors cluster.py's (ValueError, RecursionError) guards).
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


class RebootRanUnknownError(RuntimeError):
    """The @reboot-ran answer is not yet safe to give.

    Raised by a lease backend's ``reboot_ran`` between GAINING leadership
    and the first completed re-read of the persisted ran-set: until that
    read lands, "not ran" may just mean "not read back yet", and the
    consumer launches the deferred one-shot on a ``False`` -- the failover
    double-fire the persisted set exists to prevent.  The one consumer,
    ``cron._process_pending_reboots``, treats any raise from ``reboot_ran``
    as "not known to have run" and keeps the one-shot PENDING -- never
    launched, never retired -- re-evaluating on the next wakeup.  That is
    the fail-safe direction: delaying an ``@reboot`` is acceptable;
    re-running one the previous leader marked moments before failover is
    not.  Shared by the filesystem and etcd backends (kubernetes needs no
    gate: its ran-set rides the Lease annotations, so the very read that
    wins leadership delivers it).
    """


class LeadershipBackend(abc.ABC):
    """The seam each leader-gating call in :mod:`cronstable.cron` goes through.

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

    def conflicting_policies(self) -> List[str]:
        """Coordination-policy divergences among peers (gossip only)."""
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

    def view_settled(self) -> bool:
        """Whether a ``False`` from the never-skip ``available_*`` gates
        positively identifies another node as the owner.

        The gossip backend holds those gates closed while a freshly built
        manager's view is still *converging* (peers not yet re-attesting its
        new ``instance_id``; see
        :meth:`cronstable.cluster.ClusterManager._view_settled`) -- even on the
        rightful owner.  During that hold a ``False`` is a transient
        fail-closed denial, not an observed ownership move, so
        :meth:`cronstable.cron.Cron._cluster_owner_moved` must defer a pending
        retry instead of abandoning it (abandonment would end an ``@reboot``
        keep-alive sequence cluster-wide).  Lease backends decide the
        ``available_*`` family from live store reads with no convergence
        window, so the default is always ``True``; gossip overrides it.
        """
        return True

    def tls_files_loadable(self) -> bool:
        """Whether the current on-disk TLS material can be loaded right now.

        Used by :meth:`cronstable.cron.Cron.start_stop_cluster` as a cheap,
        bind-free dry-run *before* it tears the manager down to apply an
        in-place cert rotation, so a half-written / briefly-absent cert seen
        mid-rotation cannot leave the node with no manager (which would wedge
        ``Leader`` / ``PreferLeader`` jobs closed for up to one reload).  Lease
        backends have no per-node mTLS material to pre-validate -- and never
        report :meth:`tls_files_changed` -- so this is never consulted for
        them; the default is always ``True``.  The gossip backend overrides it.
        """
        return True

    def set_job_summaries_provider(  # noqa: B027
        self, provider: Callable[[], Dict[str, Any]]
    ) -> None:
        """Install the scheduler's per-job run-summary snapshot callable.

        The provider returns ``{job_name: summary}`` for this node's own jobs
        (see :meth:`cronstable.cron.Cron.fleet_job_summaries`).  The gossip
        backend overrides this to piggyback the snapshot on its ``/peer``
        response, which is what makes the dashboard's fleet view possible.
        Default no-op: a lease backend has no node-to-node channel to carry
        summaries on (it only ever talks to its lease store), so there is
        nothing to install.
        """

    def set_node_stats_provider(  # noqa: B027
        self,
        provider: Callable[[], Optional[Dict[str, Any]]],
        share: bool = True,
    ) -> None:
        """Install the scheduler's whole-node CPU/memory snapshot callable.

        The provider returns this node's live load (see
        :meth:`cronstable.resources.NodeResourceSampler.snapshot`). The gossip
        backend overrides this: installing it makes this node's own load
        available in its ``/cluster`` and ``/fleet`` self readouts, while
        ``share`` gates whether it is also gossiped to peers (so a cluster can
        show its local load without adding any gossip traffic). Default no-op:
        a lease backend has no node-to-node channel, so there is nothing to
        install (a lease cluster shares node load through a separate gossip
        observability overlay instead -- see ``cluster.observability``).
        """

    def fleet_view(self) -> Optional[Dict[str, Any]]:
        """The merged per-node job-summary view for ``GET /fleet``.

        ``None`` means this backend cannot provide one -- the lease backends
        know only the lease holder, not what any node is running -- and the
        endpoint then reports the feature unavailable so the dashboard hides
        its fleet view (the same gate as the gossip-only swimlane/ring
        panels).  The gossip backend overrides it.
        """
        return None

    # --- never-skip PreferLeader defaults (the locked lease semantics) ----

    def _is_self_demoted_holder(self) -> bool:
        """Whether this node is the lease holder in its self-demotion window.

        A lease holder stops calling itself :meth:`is_leader` a clock-skew
        margin BEFORE its lease actually expires server-side, and only learns a
        peer took over on its next renew round.  In that brief window it is
        still :meth:`is_quorate`, still observes ITSELF as holder, yet
        :meth:`is_leader` is already ``False`` -- a state a genuine follower
        never reaches.  Returns ``True`` only for that lapsed-but-still-quorate
        former holder, decided from authoritative self-state (never a
        display-name compare), so :meth:`is_available_leader` can keep running
        ``PreferLeader`` work there instead of dropping it to at-most-zero on
        every node.  Default ``False``; the lease backends override it.  Gossip
        recomputes leadership from live gossip on every call, so it has no such
        window and leaves this ``False``.
        """
        return False

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
        # The former holder in its self-demotion window (fence lapsed, next
        # renew not yet landed) is still quorate and still names ITSELF holder;
        # treat it as the never-skip owner so a PreferLeader job is not dropped
        # to at-most-zero on every node for that sub-second window (a follower
        # also still sees the old holder and defers, so nobody else runs it).
        # Decided from authoritative self-state, not a display-name compare.
        if self._is_self_demoted_holder():
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
    """Shared base for the single-holder lease backends (kubernetes, etcd,
    filesystem).

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
        # the job-set id under which ``_reboot_ran`` was last observed from the
        # store (set by _observe_reboot_ran). The READ path (reboot_ran) gates
        # on it so a reload that redefines an @reboot job -- changing the
        # job-set id WITHOUT rebuilding this backend, since the cluster section
        # is unchanged -- does not let a stale store-read mark suppress the
        # genuinely-new one-shot before the next renew round re-observes it.
        # None until the first observe.
        self._reboot_ran_job_set_id: Optional[str] = None
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
        :meth:`cronstable.cluster.ClusterManager._reconcile_job_set_id`, which
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
        self._reconcile_observed_reboot_ran()
        return (
            job_name in self._reboot_ran or job_name in self._reboot_ran_local
        )

    def _reconcile_observed_reboot_ran(self) -> None:
        """Drop the store-read @reboot-ran set when the live job set changed.

        ``_reboot_ran`` is scoped to the job-set id current at OBSERVE time
        (:meth:`_observe_reboot_ran`, run only inside the renew loop).  A
        config reload that redefines an @reboot job changes the job-set id but
        leaves the cluster section unchanged, so
        :meth:`cronstable.cron.Cron.start_stop_cluster` reuses this backend
        instance and the next renew round (which would re-observe under the new
        id) may not have run yet.  Reading the stale set on the READ path would
        make :meth:`reboot_ran` report the redefined one-shot as already-run
        and silently drop it.  Gate on the live id here, mirroring the gossip
        backend's ``advertised_ran_jobs`` read-path guard (and
        :meth:`_reconcile_local_reboot_ran` for the local set); the next
        observe re-populates it correctly under the new id.
        """
        if self._reboot_ran_job_set_id != self.get_job_set_id():
            self._reboot_ran = set()
            self._reboot_ran_job_set_id = None

    async def mark_reboot_ran(self, job_name: str) -> None:
        self._reconcile_local_reboot_ran()
        self._reboot_ran_local.add(job_name)
        await self._persist_reboot_ran()

    async def _persist_reboot_ran(self) -> None:  # pragma: no cover
        """Eagerly persist the local @reboot-ran set to the store.

        Default no-op. Both shipping lease backends override it to persist
        BEFORE the deferred @reboot job launches (cron records-then-spawns), so
        a failover holder does not re-run the one-shot: etcd writes its sibling
        key via a CAS, and kubernetes runs a renew round folding the set into
        the Lease annotation. The no-op default is the fallback for a lease
        backend that has no eager-write path (it would then persist on its next
        periodic round).
        """

    def _observe_reboot_ran(
        self, stored_job_set_id: Optional[str], stored: Set[str]
    ) -> None:
        """Fold a store-read @reboot-ran set into the cache, job-set-scoped.

        A stored set tagged with a DIFFERENT job-set id belongs to an older
        configuration, so it is ignored (the @reboot job runs again under the
        new job set), matching gossip's job-set scoping of ``advertised_ran``.
        Records the id this set was observed under so the read path
        (:meth:`_reconcile_observed_reboot_ran`) can drop it if the live job
        set changes before the next observe.
        """
        if stored_job_set_id == self.get_job_set_id():
            self._reboot_ran = set(stored)
        else:
            self._reboot_ran = set()
        self._reboot_ran_job_set_id = self.get_job_set_id()

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
    schema only ever admits the four known names, so the final ``raise`` is a
    defensive backstop, not a user-facing path.
    """
    backend = cluster_config.get("backend", "gossip")
    if backend == "gossip":
        from cronstable.cluster import ClusterManager

        return ClusterManager(cluster_config, get_job_set_id)
    if backend == "kubernetes":
        from cronstable.backends.kubernetes import KubernetesBackend

        return KubernetesBackend(cluster_config, get_job_set_id)
    if backend == "etcd":
        from cronstable.backends.etcd import EtcdBackend

        return EtcdBackend(cluster_config, get_job_set_id)
    if backend == "filesystem":
        from cronstable.backends.filesystem import FilesystemBackend

        return FilesystemBackend(cluster_config, get_job_set_id)
    raise ConfigError(  # pragma: no cover - unreachable; schema-validated
        "unknown cluster.backend {!r}".format(backend)
    )
