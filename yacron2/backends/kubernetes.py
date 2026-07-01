"""Kubernetes ``Lease`` leadership backend.

A single ``coordination.k8s.io/v1`` ``Lease`` object is the fence: at most one
node holds it (``spec.holderIdentity``), and a holder must keep renewing
``spec.renewTime`` or the lease is considered expired and another node may take
it.  This is the standard client-go leader-election algorithm, ported
faithfully.

It runs over one of **two interchangeable transports** (see
:class:`_K8sTransport`), chosen by ``cluster.kubernetes.clientLibrary`` via
:func:`yacron2.backends.select_transport`:

* the official **``kubernetes`` client** when it is installed and importable on
  this architecture (``pip install yacron2[kubernetes]``); or
* a **hand-rolled apiserver REST transport** over the core ``aiohttp``
  dependency (the default fallback) -- no ``kubernetes`` client, no grpc, so it
  runs on every architecture yacron2 targets.

The decision logic -- parsing a Lease, deciding whether it is expired, choosing
the action (create / acquire / renew / wait), and building the object to write
back -- lives in the small pure helpers below and is fully unit-tested; both
transports feed it the same JSON dict shape.  The transport layer that performs
the calls and loads credentials is ``# pragma: no cover`` (exercised only by
the Docker integration tests).

The safety property is *local*: :meth:`KubernetesBackend.is_leader` is gated on
a locally-computed expiry (``renew time + leaseDurationSeconds`` minus a small
clock-skew margin), so a stalled renew loop self-demotes with no network call.
``is_quorate`` reflects whether we have a *fresh* successful read of the lease
store; when it is false ``Leader`` jobs fail closed and -- per the locked
PreferLeader decision -- the never-skip defaults on
:class:`yacron2.leadership.LeadershipBackend` let ``PreferLeader`` jobs run
anyway (and possibly double-run).
"""

import asyncio
import base64
import datetime
import logging
import os
import ssl
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import quote

import aiohttp

from yacron2.backends import TRANSPORT_LIBRARY, select_transport
from yacron2.config import ClusterConfig, ConfigError
from yacron2.leadership import (
    REBOOT_RAN_KEY,
    LeaseBackend,
    decode_reboot_ran,
)

logger = logging.getLogger("yacron2.backends.kubernetes")

# How far in advance of the computed lease expiry a holder stops calling itself
# leader, so a node whose clock runs slightly fast self-demotes *before* a peer
# would be entitled to steal the lease -- erring is_leader toward False.
_CLOCK_SKEW = datetime.timedelta(seconds=1)
_SKEW_SECONDS = _CLOCK_SKEW.total_seconds()


def _monotonic() -> float:
    """A monotonic clock for the lease fence, steal anchor, and quorum window.

    These must never ride the wall clock: a same-node forward step would make a
    follower's ``observed_at + duration`` steal deadline fire early (stealing a
    still-valid lease -> two leaders), and a backward step would keep a former
    holder ``is_leader`` past expiry. ``time.monotonic`` cannot jump, so the
    timing stays correct across any wall-clock correction (NTP, VM resume). The
    wall clock is used only for the RFC3339 ``renewTime``/``acquireTime`` we
    write and the human-readable expiry shown in the dashboard.
    """
    return time.monotonic()


_API_GROUP = "coordination.k8s.io/v1"
_SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"

# decide_lease_action outcomes.
ACTION_CREATE = "create"  # no Lease exists -> create one with us as holder
ACTION_ACQUIRE = "acquire"  # exists but free/expired -> take it over
ACTION_RENEW = "renew"  # we already hold it -> refresh renewTime
ACTION_WAIT = "wait"  # someone else holds a still-valid lease -> not us

# Reported as the holder when we know another node won (we lost an optimistic-
# concurrency write) but cannot name it -- a 409 on a CREATE (the Lease did not
# exist when we planned, so we carry no observed holder) and we have no prior
# observation to fall back to. Reporting a non-None holder keeps leader_name()
# non-None so a quorate follower defers its PreferLeader jobs (is_available_
# leader stays False) instead of reading "holder unknown" as "run anyway" and
# double-running fleet-wide with NO partition. Mirrors etcd's _UNKNOWN_HOLDER.
# See _apply_round's write_ok==False branch.
_UNKNOWN_HOLDER = "<unknown holder>"


def _join_host_port(host: str, port: str) -> str:
    """Build a ``host:port`` authority, bracketing a bare IPv6 literal.

    ``KUBERNETES_SERVICE_HOST`` is an *unbracketed* literal on an IPv6 single-
    stack cluster (e.g. ``fd00:10:96::1``); ``"{}:{}".format`` of it yields an
    ambiguous ``fd00:10:96::1:443`` that the URL parser (yarl) rejects, so the
    in-cluster HTTP transport could never reach the apiserver on such a cluster
    (the native transport, via ``load_incluster_config``, brackets it and
    works). Bracket a host that contains a ``:`` and is not already bracketed,
    matching client-go's in-cluster loader.
    """
    if ":" in host and not host.startswith("["):
        host = "[{}]".format(host)
    return "{}:{}".format(host, port)


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def display_holder(raw: Optional[str]) -> Optional[str]:
    """The human-readable holder name from a (possibly suffixed) identity.

    yacron2 writes ``spec.holderIdentity`` as ``<name>#<instance token>`` (see
    :class:`KubernetesBackend`) so that two nodes sharing a configured
    ``identity`` / ``nodeName`` still write *distinct* holder identities and
    cannot both believe they hold the ``Lease`` -- the fence is the
    ``holderIdentity`` string, so it must be per-process unique.  For display
    we strip the ``#<token>`` suffix back to ``<name>``; a holder identity
    written by some other tool (no ``#``) is shown unchanged.
    """
    if raw is None:
        return None
    return raw.rpartition("#")[0] or raw


@dataclass
class LeaseState:
    """The fields of an observed ``Lease`` the election cares about."""

    holder: Optional[str]
    renew_time: Optional[datetime.datetime]
    acquire_time: Optional[datetime.datetime]
    duration: Optional[int]
    transitions: int
    resource_version: Optional[str]
    # the Lease's metadata.annotations, where the @reboot-ran set is persisted
    # (see yacron2.leadership.REBOOT_RAN_KEY); default {} so positional
    # LeaseState(...) constructions in the tests are unaffected.
    annotations: Dict[str, str] = field(default_factory=dict)


def _parse_microtime(value: Any) -> Optional[datetime.datetime]:
    """Parse a Kubernetes ``MicroTime`` (RFC3339 with a ``Z``) to a datetime.

    Tolerant of fewer/more fractional digits than the canonical six, and of an
    explicit numeric offset, so it survives apiserver formatting variations.
    Returns ``None`` for anything unparseable (treated as "no time observed").
    """
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "." in text:
        date_part, _, frac_and_tz = text.partition(".")
        frac, tzsep = frac_and_tz, ""
        for sep in ("+", "-"):
            idx = frac_and_tz.find(sep)
            if idx != -1:
                frac, tzsep = frac_and_tz[:idx], frac_and_tz[idx:]
                break
        frac = (frac + "000000")[:6]
        text = "{}.{}{}".format(date_part, frac, tzsep)
    try:
        parsed = datetime.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed


def _format_microtime(when: datetime.datetime) -> str:
    """Format a datetime as a Kubernetes ``MicroTime`` string."""
    return when.astimezone(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )


def parse_lease(obj: Optional[Dict[str, Any]]) -> LeaseState:
    """Extract a :class:`LeaseState` from a decoded ``Lease`` JSON object."""
    spec = (obj or {}).get("spec") or {}
    meta = (obj or {}).get("metadata") or {}
    duration = spec.get("leaseDurationSeconds")
    transitions = spec.get("leaseTransitions")
    annotations = meta.get("annotations")
    return LeaseState(
        holder=spec.get("holderIdentity"),
        renew_time=_parse_microtime(spec.get("renewTime")),
        acquire_time=_parse_microtime(spec.get("acquireTime")),
        duration=duration if isinstance(duration, int) else None,
        transitions=transitions if isinstance(transitions, int) else 0,
        resource_version=meta.get("resourceVersion"),
        annotations=annotations if isinstance(annotations, dict) else {},
    )


def lease_is_expired(
    state: LeaseState,
    now: Any,
    observed_at: Optional[Any],
) -> bool:
    """Whether another holder's lease has lapsed, from *our* clock's view.

    The deadline is anchored to ``observed_at`` -- the local instant we first
    saw this lease record -- not to the holder's own ``renewTime``.  This is
    client-go's ``observedTime + leaseDurationSeconds`` rule.  Both the anchor
    and ``now`` are *this node's* clock, so the steal decision is immune to
    clock skew **between** us and the holder (judging the holder's own
    ``renewTime`` against our clock could otherwise make a fast clock steal a
    freshly-renewed lease and elect a second leader).  The live caller passes a
    **monotonic** ``now``/``observed_at`` (see
    :meth:`KubernetesBackend._renew_once`), so the decision is also immune to a
    discontinuous jump in *our own* wall clock -- a forward NTP/VM step cannot
    make ``now`` leap past ``observed_at + duration`` and steal a still-valid
    lease.  This function itself only does the arithmetic, so it is agnostic to
    which clock the caller uses (the unit tests pass wall datetimes).
    ``observed_at`` is ``None`` only before the first observation is recorded,
    where we fall back to ``renewTime``; a lease with no usable anchor or no
    duration is treated as expired.
    """
    if not state.duration:
        return True
    if isinstance(now, (int, float)):
        # live path: a monotonic clock (seconds). The anchor is the monotonic
        # observed_at, in the same units; there is no renewTime fallback here
        # (renewTime is wall-clock and would not be comparable). No monotonic
        # anchor yet -> treat as expired.
        if observed_at is None:
            return True
        return bool(now >= observed_at + state.duration)
    # test path: wall-clock datetimes, with the renewTime fallback.
    anchor = observed_at if observed_at is not None else state.renew_time
    if anchor is None:
        return True
    return bool(now >= anchor + datetime.timedelta(seconds=state.duration))


def _deadline_passed(
    now: Any, anchor: Optional[Any], duration: Optional[int]
) -> bool:
    """Whether ``anchor + duration`` has elapsed on whichever clock ``now`` is.

    ``now``/``anchor`` are both monotonic floats (the live path) or both wall
    datetimes (the unit tests).  True when there is no usable anchor/duration.
    """
    if anchor is None or not duration:
        return True
    if isinstance(now, (int, float)):
        return bool(now >= anchor + duration)
    return bool(now >= anchor + datetime.timedelta(seconds=duration))


def _file_signature(path: str) -> Optional[Tuple[int, int]]:
    """A cheap ``(st_mtime_ns, st_size)`` fingerprint of one file, or ``None``.

    ``os.stat`` follows symlinks, so the atomic symlink swap Kubernetes uses
    for a mounted/projected secret is picked up too.  A stat error (e.g. a file
    briefly absent mid-rotation) is recorded as ``None`` and simply compares
    unequal once the file is back -- the safe direction (a spurious rebuild,
    never a missed one).  Mirrors the gossip backend's ``_tls_file_signature``.
    """
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (st.st_mtime_ns, st.st_size)


def decide_lease_action(
    state: Optional[LeaseState],
    identity: str,
    now: Any,
    observed_at: Optional[Any],
    *,
    last_holder: Optional[str] = None,
    last_observed_at: Optional[Any] = None,
    duration: Optional[int] = None,
) -> str:
    """Choose the action for this round given the observed lease.

    * no lease -> ``create`` (but ``wait`` first if a *different* holder we
      recently saw still has an unexpired lease from our view -- see below);
    * we are the holder -> ``renew`` (reclaim even if it lapsed);
    * holder is empty, or *our* observation of this record has aged past the
      lease duration (see :func:`lease_is_expired`) -> ``acquire`` (take over);
    * someone else holds a still-valid lease -> ``wait``.

    The ``last_holder``/``last_observed_at``/``duration`` guard narrows a
    two-leaders race when the Lease *object* is deleted out from under a live
    holder (``kubectl delete lease`` / a GC or namespace controller): the
    deletion does not touch the prior holder's local monotonic fence, so it
    keeps returning :meth:`KubernetesBackend.is_leader` ``True`` until its next
    round observes the recreated lease.  If we recreated the lease as ourselves
    the instant we saw the 404, both nodes would lead for up to a retry period.
    So when the object is absent but we remember a *different* holder whose
    lease has not yet expired from our clock, we ``wait`` it out first.  (Two
    nodes both creating from a genuinely-absent lease stay fenced by the
    apiserver's ``AlreadyExists`` 409 -> ``write_ok`` False -> not leader.)

    RESIDUAL (by design): this guard only fires for a node that REMEMBERS a
    prior holder.  A fresh process (no observation history -> ``last_holder``
    is ``None``) cannot tell a genuinely-empty cluster from a Lease just
    deleted out from under a live holder, so it creates immediately; if a prior
    holder is still inside its local fence the two briefly co-lead until that
    holder's next round observes the recreated Lease and stands down (bounded
    by its retry period, self-healing).  This residual is inherent to the lease
    model -- deleting the Lease *object* out-of-band can always produce two
    momentary leaders -- so the Lease object should not be deleted out of band
    (its absence is recovered automatically).  It is deliberately NOT closed by
    making a fresh node defer, which would delay every cold-start election by a
    full lease duration for a rare, self-healing, externally-triggered window.
    """
    if state is None:
        if (
            last_holder
            and last_holder != identity
            and not _deadline_passed(now, last_observed_at, duration)
        ):
            return ACTION_WAIT
        return ACTION_CREATE
    if state.holder == identity:
        return ACTION_RENEW
    if not state.holder or lease_is_expired(state, now, observed_at):
        return ACTION_ACQUIRE
    return ACTION_WAIT


def build_lease_body(
    name: str,
    namespace: Optional[str],
    identity: str,
    now: datetime.datetime,
    duration: int,
    state: Optional[LeaseState],
    action: str,
    annotations: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Build the ``Lease`` object to POST (create) or PUT (acquire/renew).

    ``acquireTime`` and ``leaseTransitions`` follow client-go: a fresh acquire
    (taking over from another holder or an empty/expired lease) stamps a new
    ``acquireTime`` and bumps ``leaseTransitions``; a renew preserves both.  A
    replace (acquire/renew) carries the observed ``resourceVersion`` so the
    apiserver rejects the write (HTTP 409) if another node raced us.
    ``annotations`` (when non-empty) is written to ``metadata.annotations`` --
    this is where the @reboot-ran set is persisted so it survives failover.
    """
    if action == ACTION_RENEW and state is not None:
        acquire_time = state.acquire_time or now
        transitions = state.transitions
    elif action == ACTION_ACQUIRE and state is not None:
        acquire_time = now
        transitions = state.transitions + 1
    else:  # create
        acquire_time = now
        transitions = 0
    metadata: Dict[str, Any] = {"name": name}
    if namespace:
        metadata["namespace"] = namespace
    if annotations:
        metadata["annotations"] = annotations
    if (
        action != ACTION_CREATE
        and state is not None
        and state.resource_version
    ):
        metadata["resourceVersion"] = state.resource_version
    return {
        "apiVersion": _API_GROUP,
        "kind": "Lease",
        "metadata": metadata,
        "spec": {
            "holderIdentity": identity,
            "leaseDurationSeconds": duration,
            "acquireTime": _format_microtime(acquire_time),
            "renewTime": _format_microtime(now),
            "leaseTransitions": transitions,
        },
    }


def plan_lease_write(
    lease_obj: Optional[Dict[str, Any]],
    name: str,
    namespace: Optional[str],
    identity: str,
    now: datetime.datetime,
    duration: int,
    observed_at: Optional[Any],
    mono_now: Optional[float] = None,
    annotations: Optional[Dict[str, str]] = None,
    last_holder: Optional[str] = None,
    last_observed_at: Optional[Any] = None,
) -> Tuple[str, Optional[Dict[str, Any]], Optional[LeaseState]]:
    """Pure planning step: observed Lease -> (action, body, state).

    ``observed_at`` is the local instant we first saw the current lease record
    (the steal anchor; see :func:`lease_is_expired`).  Timing and body-stamping
    use *different* clocks: the steal/expiry **decision** runs on a monotonic
    clock (``mono_now`` and the monotonic ``observed_at``) so a wall-clock jump
    cannot mis-time a takeover, while the ``renewTime``/``acquireTime`` written
    into the object must be wall-clock RFC3339 (``now``).  When ``mono_now`` is
    omitted the decision falls back to ``now`` (the unit tests pass one wall
    clock for both, which is internally consistent).  ``last_holder`` /
    ``last_observed_at`` are the holder we remembered *before* this round's
    observation, for the deleted-lease recreate-race guard in
    :func:`decide_lease_action`.  ``body`` is ``None`` for ``wait`` (nothing to
    write).
    """
    state = parse_lease(lease_obj) if lease_obj is not None else None
    decide_now = now if mono_now is None else mono_now
    action = decide_lease_action(
        state,
        identity,
        decide_now,
        observed_at,
        last_holder=last_holder,
        last_observed_at=last_observed_at,
        duration=duration,
    )
    if action == ACTION_WAIT:
        return action, None, state
    body = build_lease_body(
        name, namespace, identity, now, duration, state, action, annotations
    )
    return action, body, state


class KubernetesBackend(LeaseBackend):
    """Leadership via a single ``coordination.k8s.io/v1`` ``Lease``."""

    backend_name = "kubernetes"

    def __init__(
        self,
        config: ClusterConfig,
        get_job_set_id: Callable[[], str],
    ) -> None:
        super().__init__(config, get_job_set_id)
        k8s = config["kubernetes"]
        self.lease_name: str = k8s["leaseName"]
        # The configured/defaulted human name (defaults to nodeName), shown in
        # the dashboard and GET /cluster.
        self.display_identity: str = k8s["identity"]
        # A random per-process token appended to the holderIdentity actually
        # written to the Lease, so two nodes that share a configured identity
        # (a duplicate nodeName: a Deployment, a shared HOSTNAME) still write
        # DISTINCT holder identities and cannot both believe they hold the
        # fence.  decide_lease_action compares this full string, so only the
        # exact process that wrote it renews; a same-name peer sees a different
        # holder and waits.  Stripped back to display_identity for display (see
        # display_holder / lease_detail).
        self.instance_id: str = uuid.uuid4().hex[:12]
        self.identity: str = "{}#{}".format(
            self.display_identity, self.instance_id
        )
        self.lease_duration: int = k8s["leaseDurationSeconds"]
        self.renew_deadline: int = k8s["renewDeadlineSeconds"]
        self.retry_period: int = k8s["retryPeriodSeconds"]
        self._configured_namespace: Optional[str] = k8s["leaseNamespace"]
        self.kubeconfig: Optional[str] = k8s["kubeconfig"]
        self.api_server_override: Optional[str] = k8s["apiServer"]
        self.client_library: str = k8s["clientLibrary"]
        self.connect_timeout: int = config["connectTimeout"]

        # resolved at start() from the in-cluster files or a kubeconfig
        self.namespace: Optional[str] = self._configured_namespace

        # live state, written by the renew loop and read by the sync methods.
        # _leader_until / _last_contact are WALL-CLOCK, for display only; the
        # load-bearing fence and freshness gates use the monotonic deadlines
        # below (immune to wall-clock steps; see _monotonic).
        self._is_leader = False
        self._holder: Optional[str] = None
        self._leader_until: Optional[datetime.datetime] = None
        self._last_contact: Optional[datetime.datetime] = None
        self._leader_until_mono: Optional[float] = None
        self._last_contact_mono: Optional[float] = None

        # client-go's observedTime: the (holder, renewTime) record we last saw
        # and the MONOTONIC clock when we first saw it. The steal decision is
        # anchored to _observed_at (our monotonic clock), not the holder's
        # renewTime, so it is immune both to skew between us and the holder and
        # to a discontinuous jump in our own wall clock.
        self._observed_holder: Optional[str] = None
        self._observed_renew: Optional[datetime.datetime] = None
        self._observed_at: Optional[float] = None

        # the chosen transport (native client or hand-rolled HTTP), bound in
        # start(); see select_transport.
        self._transport: Optional["_K8sTransport"] = None
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        # Serialises the renew loop's lease write with an eager reboot-ran
        # persist (see _persist_reboot_ran), so the two cannot interleave and
        # 409 each other on the shared resourceVersion (a transient false
        # demotion).
        self._lease_write_lock = asyncio.Lock()
        # on-disk TLS files this backend's transport loaded (an in-cluster
        # ca.crt, a kubeconfig's CA / client cert / key, the kubeconfig file
        # itself). Snapshotted at setup() so an in-place cert/CA rotation
        # (cert-manager / Vault / a projected secret refresh) is detected and
        # the backend rebuilt via start_stop_cluster, mirroring the gossip
        # backend -- the SSLContext is built once at setup(), never reloaded,
        # so without this a rotated client cert/CA silently and permanently
        # loses leadership. The bearer token is NOT tracked (it self-heals,
        # re-read per request; see _auth_headers). Empty until setup() records
        # them, which keeps tls_files_changed() False (nothing on disk to
        # rotate: embedded -data creds / insecure mode).
        self._tls_files: List[str] = []
        self._tls_signature: Dict[str, Optional[Tuple[int, int]]] = {}

    def _record_tls_files(self, paths: List[Optional[str]]) -> None:
        """Snapshot the on-disk TLS files the transport loaded.

        Called from a transport's ``setup()``; ``None``/empty entries (embedded
        ``-data`` creds, ``insecure-skip-tls-verify``) are dropped, so nothing
        on disk to rotate leaves :meth:`tls_files_changed` ``False``.
        """
        self._tls_files = [p for p in paths if p]
        self._tls_signature = {p: _file_signature(p) for p in self._tls_files}

    def tls_files_changed(self) -> bool:
        """Whether any tracked on-disk TLS file changed since ``setup()``.

        The SSLContext is built once at setup() and never reloaded, so -- as
        for the gossip backend -- an in-place cert/CA rotation (same paths, new
        bytes) is otherwise invisible until the process restarts.  Reporting
        the change lets :meth:`yacron2.cron.Cron.start_stop_cluster` rebuild
        this backend with the fresh material.  ``False`` when nothing was
        tracked (embedded creds / insecure mode: nothing on disk to rotate).
        """
        if not self._tls_files:
            return False
        current = {p: _file_signature(p) for p in self._tls_files}
        return current != self._tls_signature

    # --- pure local-state reads (no I/O) ---------------------------------

    def _leader_deadline(self, now: datetime.datetime) -> datetime.datetime:
        """Wall-clock lease expiry, for display only (see ``_apply_round``)."""
        return (
            now + datetime.timedelta(seconds=self.lease_duration) - _CLOCK_SKEW
        )

    def is_leader(self) -> bool:
        if not self._is_leader or self._leader_until_mono is None:
            return False
        # local-expiry safety on a MONOTONIC deadline: a stalled renew loop
        # self-demotes with no network call, and no wall-clock step can keep us
        # leader past the point the apiserver has expired the lease.
        return _monotonic() < self._leader_until_mono

    def _is_self_demoted_holder(self) -> bool:
        # raw leadership flag still set (we hold/held the Lease and have not
        # observed a takeover) but the monotonic fence has lapsed -- the brief
        # self-demotion window. See LeadershipBackend._is_self_demoted_holder.
        return self._is_leader and not self.is_leader()

    def leader_name(self) -> Optional[str]:
        if not self.is_quorate():
            return None
        return self._holder

    def is_quorate(self) -> bool:
        """Whether we have a *fresh* successful read of the lease store.

        Stale (or never-contacted) -> not quorate, so ``Leader`` fails closed
        and the never-skip ``PreferLeader`` default runs the job anyway.
        """
        if self._last_contact_mono is None:
            return False
        return _monotonic() < self._last_contact_mono + self.lease_duration

    def lease_detail(self) -> Dict[str, Any]:
        return {
            "name": self.lease_name,
            "namespace": self.namespace,
            "identity": self.display_identity,
            "holder": self._holder,
            # only advertise an expiry while we actually hold the lease, so a
            # former holder's view does not keep showing a stale deadline.
            "expiry": (
                _format_microtime(self._leader_until)
                if self.is_leader() and self._leader_until is not None
                else None
            ),
        }

    def _resolve_namespace(self, context_namespace: Optional[str]) -> str:
        """The Lease namespace, resolved identically for *both* transports.

        Centralised here (not duplicated per transport) so the HTTP and native
        paths can never elect the Lease in *different* namespaces -- a
        cross-namespace split-brain (two leaders).  The load-bearing subtlety:
        when a ``kubeconfig`` is configured the in-cluster service-account
        namespace file must **not** be consulted by *either* transport --
        otherwise a mixed-transport fleet (native client on one arch, HTTP
        fallback on another) running in-pod with a kubeconfig would resolve two
        different namespaces (the SA file's value vs ``"default"``).  See
        :func:`resolve_namespace`.
        """
        incluster = None if self.kubeconfig else _incluster_namespace()
        return resolve_namespace(
            self._configured_namespace, context_namespace, incluster
        )

    # --- the renew loop's per-round local-state update (pure) ------------

    def _track_observation(
        self, state: Optional[LeaseState], mono_now: float
    ) -> Optional[float]:
        """Record client-go's observedTime and return the steal anchor.

        Whenever the observed ``(holder, renewTime)`` record changes, reset the
        local observation clock to ``mono_now`` (a *monotonic* timestamp); an
        unchanged record keeps the original ``_observed_at``.  The returned
        anchor is what :func:`lease_is_expired` measures the lease duration
        from, so a peer's lease is only ever stolen after *we* have watched the
        same record for a full monotonic duration -- which no wall-clock
        jump can fast-forward.
        """
        if state is None:
            # An absent lease (404) carries no new timing. PRESERVE the last
            # real observation so the steal/expiry timer -- and the deleted-
            # lease recreate-race guard in decide_lease_action -- keep counting
            # against the holder we last saw, instead of resetting the anchor
            # (which would let us recreate the lease as ourselves immediately
            # and lead alongside a prior holder still inside its local fence).
            return self._observed_at
        holder = state.holder
        renew = state.renew_time
        if holder != self._observed_holder or renew != self._observed_renew:
            self._observed_holder = holder
            self._observed_renew = renew
            self._observed_at = mono_now
        return self._observed_at

    def _apply_round(
        self,
        action: str,
        write_ok: bool,
        state: Optional[LeaseState],
        now: datetime.datetime,
        mono: Optional[float] = None,
    ) -> None:
        """Update the live leader state from this round's outcome.

        Separated from the I/O so it is unit-tested: ``write_ok`` is whether
        the create/replace succeeded (a 409 conflict is ``False`` -- another
        node won). Always advances the contact clocks (we *did* reach apiserver
        this round, which is what ``is_quorate`` tracks). ``now`` is wall-clock
        (display); ``mono`` is the monotonic instant the fence/freshness
        gates use (defaulted to the current monotonic clock for unit tests).
        """
        if mono is None:
            mono = _monotonic()
        self._last_contact = now
        self._last_contact_mono = mono
        if action == ACTION_WAIT:
            self._is_leader = False
            if state is not None:
                self._holder = display_holder(state.holder)
            else:
                # Deleted-lease WAIT: the Lease object is gone (404) but we are
                # deferring to a holder we recently saw whose local fence has
                # not yet expired (decide_lease_action's recreate-race guard).
                # Report THAT remembered holder, not None -- leaving _holder
                # None makes leader_name() None, which is_available_leader()
                # reads as "holder unknown -> run anyway", so a follower
                # would run PreferLeader jobs alongside the still-fenced prior
                # holder: a double-run with NO partition. The remembered holder
                # keeps leader_name() non-None so the follower defers until the
                # lease reappears.
                self._holder = display_holder(self._observed_holder)
            return
        if write_ok:
            self._is_leader = True
            self._holder = self.display_identity
            self._leader_until = self._leader_deadline(now)
            self._leader_until_mono = (
                mono + self.lease_duration - _SKEW_SECONDS
            )
        else:
            # lost the optimistic-concurrency race (a 409): not leader now.
            # NEVER leave _holder None here: leader_name() None reads in
            # is_available_leader() as "holder unknown -> run anyway", so a
            # quorate follower would run every PreferLeader (and spread) job
            # alongside the real holder, a double-run with NO partition. The
            # 409 proves another node won, so report a non-None holder: the one
            # from the observed Lease if we have it, else the holder we last
            # observed, else a sentinel. This is the CREATE-race twin of the
            # deleted-lease WAIT branch above -- on a CREATE, plan_lease_write
            # passes state=None, so display_holder(state.holder) would be None
            # exactly when two replicas cold-start and the loser 409s. Mirrors
            # etcd's _UNKNOWN_HOLDER fence.
            self._is_leader = False
            holder: Optional[str] = None
            if state is not None:
                holder = display_holder(state.holder)
            if holder is None and self._observed_holder is not None:
                holder = display_holder(self._observed_holder)
            self._holder = holder if holder is not None else _UNKNOWN_HOLDER

    # --- transport selection + renew loop (integration-only) -------------

    def _native_available(self) -> bool:  # pragma: no cover - import probe
        try:
            import kubernetes  # noqa: F401

            return True
        except ImportError:
            return False

    async def start(self) -> None:  # pragma: no cover - network/credential I/O
        # `import kubernetes` pulls in urllib3/requests etc. -- a heavy
        # one-time import that must not block the run loop start_stop_cluster
        # awaits us from; probe it in a worker thread.
        native = await asyncio.to_thread(self._native_available)
        kind = select_transport(self.client_library, native, "kubernetes")
        self._transport = (
            _K8sLibraryTransport(self)
            if kind == TRANSPORT_LIBRARY
            else _K8sHttpTransport(self)
        )
        try:
            await self._transport.setup()
            # Run one round up front so is_quorate/is_leader reflect a read
            # of the Lease BEFORE the first spawn_jobs. Without it a
            # lease backend is "never contacted" for one cycle, which makes
            # every PreferLeader job run on every node at boot (and on every
            # reload that rebuilds the manager). Best-effort: a failed/slow
            # round is swallowed (the loop retries), leaving the not-quorate
            # state -- the genuine "apiserver unreachable" case.
            try:
                await asyncio.wait_for(self._renew_once(), self.renew_deadline)
            except Exception as ex:
                logger.warning(
                    "cluster: kubernetes initial round failed: %s", ex
                )
            logger.info(
                "cluster: kubernetes backend (%s transport), identity %r, "
                "lease %s/%s (duration %ds, renew %ds, retry %ds)",
                kind,
                self.identity,
                self.namespace,
                self.lease_name,
                self.lease_duration,
                self.renew_deadline,
                self.retry_period,
            )
            self._stop.clear()
            # create the renew task INSIDE the try so a failure here is cleaned
            # up like any other -- it must not leak the open session/task.
            self._task = asyncio.create_task(self._renew_loop())
        except BaseException:
            # clean up half-started state (an open session, temp cert files, a
            # created renew task) so a failed start leaks nothing, honouring
            # the caller's contract.
            if self._task is not None:
                self._task.cancel()
                self._task = None
            await self._transport.close()
            self._transport = None
            raise

    async def _renew_loop(self) -> None:  # pragma: no cover - network loop
        assert self._transport is not None
        while not self._stop.is_set():
            try:
                # client-go's renewDeadline: bound each renew/observe round so
                # a stuck call is abandoned (and retried next round) before the
                # lease can expire, rather than blocking the whole duration.
                await asyncio.wait_for(self._renew_once(), self.renew_deadline)
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                # could not complete the round (apiserver unreachable, a
                # transport/library error): do NOT advance _last_contact, so
                # is_quorate goes stale and Leader fails closed while
                # PreferLeader runs (never-skip).
                logger.warning(
                    "cluster: kubernetes lease round failed: %s", ex
                )
            try:
                await asyncio.wait_for(self._stop.wait(), self.retry_period)
            except asyncio.TimeoutError:
                pass

    async def _renew_once(self) -> None:  # pragma: no cover - network
        assert self._transport is not None
        # Serialise with the eager reboot-ran persist so the two writes do not
        # race on resourceVersion (see _persist_reboot_ran / the write lock).
        async with self._lease_write_lock:
            lease_obj = await self._transport.observe()
            # Capture the clocks AFTER observe() returns, so the steal anchor
            # (and the renewTime we write) reflect the instant we actually read
            # the record -- client-go's observedTime = now() *after* the Get.
            # Sampling before the await would anchor whatever (possibly newer)
            # record GET returns to a time up to one observe-latency earlier
            # (bounded by renewDeadline, not the skew budget), shrinking the
            # steal window and risking two leaders at once on a slow apiserver.
            # `mono` drives the steal/expiry timing (jump-proof); `now` (wall)
            # only stamps written renewTime/acquireTime and the displayed
            # expiry.
            now = _utcnow()
            mono = _monotonic()
            observed = (
                parse_lease(lease_obj) if lease_obj is not None else None
            )
            # remember the holder we saw BEFORE this round overwrites it, for
            # the deleted-lease recreate-race guard (see decide_lease_action).
            prev_holder = self._observed_holder
            prev_observed_at = self._observed_at
            observed_at = self._track_observation(observed, mono)
            # Fold the @reboot-ran set persisted in the Lease annotations into
            # our cache (so a node that just acquired the lease learns what
            # already ran and does not re-run it), then carry those annotations
            # forward and add our own pending marks for the write below.
            observed_annotations = observed.annotations if observed else {}
            stored_jsid, stored_jobs = decode_reboot_ran(
                observed_annotations.get(REBOOT_RAN_KEY)
            )
            self._observe_reboot_ran(stored_jsid, stored_jobs)
            write_annotations = self.reboot_ran_annotation(
                observed_annotations
            )
            action, body, state = plan_lease_write(
                lease_obj,
                self.lease_name,
                self.namespace,
                self.identity,
                now,
                self.lease_duration,
                observed_at,
                mono,
                write_annotations,
                last_holder=prev_holder,
                last_observed_at=prev_observed_at,
            )
            write_ok = False
            if body is not None:
                write_ok = await self._transport.write(
                    body, create=(action == ACTION_CREATE)
                )
            self._apply_round(action, write_ok, state, now, mono)

    async def _persist_reboot_ran(self) -> None:  # pragma: no cover - network
        """Eagerly write the @reboot-ran set into the Lease annotations.

        cron records a deferred @reboot one-shot as run via ``mark_reboot_ran``
        *before* launching it, and both the cron and leadership docstrings
        promise a lease backend persists that immediately so a failover holder
        cannot re-run it.  The default
        :class:`~yacron2.leadership.LeaseBackend` ``mark_reboot_ran`` only
        updates the in-memory set and calls this no-op, leaving the annotation
        to the *next* periodic renew round -- a window in which a crash (or a
        graceful stop that PUT-replaces the Lease) loses the record and the
        one-shot double-runs.  Override it to run a renew round
        now, which carries the mark into the Lease.  Best-effort and bounded
        (the periodic round retries on failure); serialised against the loop by
        ``_renew_once``'s ``_lease_write_lock``.
        """
        if self._transport is None:
            return
        try:
            await asyncio.wait_for(self._renew_once(), self.renew_deadline)
        except Exception as ex:
            logger.debug(
                "cluster: kubernetes reboot-ran eager persist failed: %s", ex
            )

    async def stop(self) -> None:  # pragma: no cover - network
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._transport is not None:
            # Bound the release + close so a hung apiserver cannot wedge the
            # inline start_stop_cluster reload (the native client calls run in
            # an uncancellable worker thread; a deadline-less _release/close
            # would block the whole reload, and the lease would not be handed
            # back -- a peer then waits out the full leaseDuration). A failed
            # release safely falls back to TTL expiry.
            if self._is_leader:
                try:
                    await asyncio.wait_for(
                        self._release(), self.renew_deadline
                    )
                except Exception as ex:
                    logger.debug(
                        "cluster: kubernetes lease release timed out: %s", ex
                    )
            try:
                await asyncio.wait_for(
                    self._transport.close(), self.connect_timeout
                )
            except Exception as ex:
                logger.debug(
                    "cluster: kubernetes transport close timed out: %s", ex
                )
            self._transport = None
        self._is_leader = False

    async def _release(self) -> None:  # pragma: no cover - network
        """Best-effort: clear ``holderIdentity`` so a peer can take over."""
        assert self._transport is not None
        try:
            lease_obj = await self._transport.observe()
            if lease_obj is None:
                return
            state = parse_lease(lease_obj)
            if state.holder != self.identity:
                return
            metadata: Dict[str, Any] = {
                "name": self.lease_name,
                "namespace": self.namespace,
                "resourceVersion": state.resource_version,
            }
            # Preserve the @reboot-ran annotation when handing the lease back:
            # a graceful stop (e.g. a config reload that rebuilds the manager)
            # must NOT erase the record of which one-shots already ran, or a
            # peer that takes over would re-run them. Observe the stored
            # pairing FIRST, as the renew round does: reboot_ran_annotation
            # stamps the cached set with the LIVE job-set id, and a reload can
            # change that id in the same iteration that stops this manager
            # (cluster section changed / a kubeconfig cert rotation), with no
            # renew round in between to re-scope the cache -- composing
            # without observing would re-stamp the OLD config's ran-set under
            # the NEW id, a toxic pairing every later observe adopts as
            # genuine, retiring a redefined @reboot one-shot cluster-wide
            # without ever running it (the exact laundering gossip's
            # advertised_ran_jobs and etcd's _cas_write_reboot_ran guard
            # against). Carry the stored set (plus any of our own pending
            # marks) forward.
            stored_jsid, stored_jobs = decode_reboot_ran(
                state.annotations.get(REBOOT_RAN_KEY)
            )
            self._observe_reboot_ran(stored_jsid, stored_jobs)
            annotations = self.reboot_ran_annotation(state.annotations)
            if annotations:
                metadata["annotations"] = annotations
            body = {
                "apiVersion": _API_GROUP,
                "kind": "Lease",
                "metadata": metadata,
                "spec": {
                    "holderIdentity": None,
                    "leaseDurationSeconds": self.lease_duration,
                    "leaseTransitions": state.transitions,
                },
            }
            await self._transport.write(body, create=False)
        except Exception as ex:
            logger.debug("cluster: kubernetes lease release failed: %s", ex)


def _incluster_namespace() -> Optional[str]:  # pragma: no cover - file I/O
    try:
        with open(os.path.join(_SA_DIR, "namespace")) as ns_file:
            return ns_file.read().strip()
    except OSError:
        return None


def resolve_namespace(
    configured: Optional[str],
    context_namespace: Optional[str],
    incluster_namespace: Optional[str],
) -> str:
    """Resolve the Lease namespace identically for both transports.

    Precedence: an explicit ``cluster.kubernetes.leaseNamespace``, then the
    kubeconfig context's namespace (the kubeconfig path only), then the
    in-cluster service-account namespace, then ``"default"``.  Centralised, and
    never ``None``, so the HTTP and native transports can never elect the Lease
    in *different* namespaces -- a cross-namespace split-brain (two leaders).
    Falling back to ``"default"`` (never ``None``) is what keeps the two
    transports in agreement when the service-account ``namespace`` file is
    unreadable (a custom projected-token mount): both resolve ``"default"``
    rather than one running a second leader in another namespace.
    """
    return configured or context_namespace or incluster_namespace or "default"


def _kubeconfig_cert_files(path: str) -> List[Optional[str]]:
    """File-referenced CA / client-cert / client-key of a kubeconfig's active
    context, for TLS-rotation tracking (:meth:`KubernetesBackend.tls_files_
    changed`).

    Both transports build their TLS material once at setup and never reload it,
    so an in-place cert/CA rotation is invisible until the backend is rebuilt.
    The HTTP transport tracks the kubeconfig PLUS these referenced files
    (:meth:`_K8sHttpTransport._load_kubeconfig`); the native transport tracked
    only the kubeconfig, so a path-referenced cert rotated in place (the
    kubeconfig's own mtime unchanged) was missed and the frozen client kept
    presenting the expired cert -> permanent loss of leadership.  Pull the
    same referenced paths so the native transport can track them too.

    Embedded ``*-data`` forms resolve to ``None`` here and are dropped (a -data
    rotation rewrites the kubeconfig itself, tracked via its own path).  Any
    parse error yields ``[]`` -- the kubeconfig path stays tracked regardless,
    so the worst case is the pre-existing under-tracking, never a crash.
    """
    from strictyaml.ruamel import YAML
    from strictyaml.ruamel.error import YAMLError, YAMLFutureWarning

    try:
        with open(path) as cfg_file:
            data = YAML(typ="safe").load(cfg_file)
        contexts = {c["name"]: c["context"] for c in data.get("contexts", [])}
        ctx = contexts[data["current-context"]]
        clusters = {c["name"]: c["cluster"] for c in data.get("clusters", [])}
        users = {u["name"]: u["user"] for u in data.get("users", [])}
        cluster = clusters[ctx["cluster"]]
        user = users.get(ctx["user"], {})
        return [
            cluster.get("certificate-authority"),
            user.get("client-certificate"),
            user.get("client-key"),
        ]
    except (
        OSError,
        KeyError,
        TypeError,
        AttributeError,
        ValueError,
        # the ruamel parse-error family: ScannerError/ParserError subclass
        # YAMLError, but DuplicateKeyError descends from Warning via
        # YAMLFutureWarning, so BOTH roots must be named. PyYAML -- what the
        # native client's load_kube_config parses with -- silently accepts a
        # duplicated mapping key (last-wins), so a kubeconfig the official
        # client loads fine still reaches the unguarded call in _setup_sync;
        # raising here would abort the whole backend over cert-file TRACKING.
        YAMLError,
        YAMLFutureWarning,
    ):
        return []


class _K8sTransport:
    """The observe/write/close surface the renew loop drives a Lease over."""

    async def setup(self) -> None:  # pragma: no cover - integration
        raise NotImplementedError

    async def observe(
        self,
    ) -> Optional[Dict[str, Any]]:  # pragma: no cover - integration
        raise NotImplementedError

    async def write(
        self, body: Dict[str, Any], *, create: bool
    ) -> bool:  # pragma: no cover - integration
        raise NotImplementedError

    async def close(self) -> None:  # pragma: no cover - integration
        raise NotImplementedError


class _K8sHttpTransport(_K8sTransport):  # pragma: no cover - network I/O
    """Hand-rolled apiserver REST transport (core aiohttp; no client library).

    Resolves credentials from the in-cluster service-account files (or a
    kubeconfig for out-of-cluster / local testing) and drives the
    ``coordination.k8s.io/v1`` Lease endpoints directly over HTTPS.
    """

    def __init__(self, backend: "KubernetesBackend") -> None:
        self.b = backend
        self._base_url: Optional[str] = None
        self._auth_token: Optional[str] = None
        # the on-disk path of a *rotating* in-cluster service-account token,
        # re-read before every request (see _auth_headers). None when the
        # token is static (a kubeconfig bearer token) or absent (client-cert
        # auth), in which case _auth_token alone is used.
        self._token_path: Optional[str] = None
        self._ssl: Optional[ssl.SSLContext] = None
        self._tempfiles: List[str] = []
        self._session: Optional[aiohttp.ClientSession] = None

    async def setup(self) -> None:
        self._load_connection()
        timeout = aiohttp.ClientTimeout(total=self.b.connect_timeout)
        # The Authorization header is set *per request* (see _auth_headers),
        # not frozen on the session: a projected service-account token is
        # rotated on disk by the kubelet and a token baked in here would 401
        # after the first rotation with no way to recover.
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        self._session = aiohttp.ClientSession(timeout=timeout, headers=headers)

    @staticmethod
    def _read_token(path: str) -> str:
        with open(path) as token_file:
            return token_file.read().strip()

    def _auth_headers(self) -> Dict[str, str]:
        """Per-request ``Authorization`` header, refreshing a rotating token.

        Kubernetes >= 1.22 projects *bound* service-account tokens that the
        kubelet rotates on disk roughly hourly. A lease backend is never
        rebuilt on a token rotation (``cron.start_stop_cluster`` only rebuilds
        on a config-byte or gossip-TLS change), so a token frozen at
        :meth:`setup` would 401 once the first rotation lands and the node
        would silently lose leadership cluster-wide for good. For the
        in-cluster path we therefore re-read the (kubelet-kept-fresh) token
        before each request; a static kubeconfig token has no ``_token_path``
        and is used as-is. A transient read failure keeps the last good token
        (a stale token simply fails the round, which fails ``Leader`` closed).
        """
        if self._token_path is not None:
            try:
                self._auth_token = self._read_token(self._token_path)
            except OSError:
                pass
        if self._auth_token:
            return {"Authorization": "Bearer " + self._auth_token}
        return {}

    def _load_connection(self) -> None:
        """Resolve the apiserver URL, auth token, CA and namespace.

        In-cluster (the default) reads the service-account token/CA/namespace
        files and the ``KUBERNETES_SERVICE_*`` env vars; a configured
        ``kubeconfig`` is used instead for out-of-cluster / local testing.
        """
        if self.b.kubeconfig:
            self._load_kubeconfig(self.b.kubeconfig)
            return
        host = os.environ.get("KUBERNETES_SERVICE_HOST")
        port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
        if self.b.api_server_override:
            # apiServer is an IN-CLUSTER url override (e.g. a kube-rbac-proxy
            # sidecar), still authenticated with the ServiceAccount token/CA
            # below; not an out-of-cluster mode (use kubeconfig for that).
            # Config validation already requires https, so the token below is
            # never sent in cleartext.
            self._base_url = self.b.api_server_override.rstrip("/")
        elif host:
            self._base_url = "https://" + _join_host_port(host, port)
        else:
            raise ConfigError(
                "kubernetes backend: not running in a cluster and no "
                "cluster.kubernetes.kubeconfig or apiServer configured"
            )
        # Defence in depth: never attach the SA bearer token to a non-https
        # target (config already rejects an http apiServer; this catches any
        # path that slips through before the token is read).
        if not self._base_url.lower().startswith("https://"):
            raise ConfigError(
                "kubernetes backend: refusing to send the ServiceAccount "
                "bearer token to a non-https apiserver ({})".format(
                    self._base_url
                )
            )
        try:
            # remember the path so _auth_headers can re-read the rotating
            # token on every request rather than freezing it here.
            self._token_path = os.path.join(_SA_DIR, "token")
            self._auth_token = self._read_token(self._token_path)
            ca_path = os.path.join(_SA_DIR, "ca.crt")
            self._ssl = ssl.create_default_context(cafile=ca_path)
            # track the in-cluster CA so a projected-secret CA rotation
            # rebuilds the backend (token rotates per-request; see above).
            self.b._record_tls_files([ca_path])
            self.b.namespace = self.b._resolve_namespace(None)
        except OSError as ex:
            raise ConfigError(
                "kubernetes backend: could not read in-cluster service "
                "account credentials ({}); these are required for the "
                "in-cluster and apiServer-override paths. For out-of-cluster "
                "use set cluster.kubernetes.kubeconfig instead".format(ex)
            ) from ex

    def _load_kubeconfig(self, path: str) -> None:
        """Minimal kubeconfig loader (server, CA, token or client cert).

        Uses the bundled ruamel YAML (a strictyaml transitive dependency) so no
        new dependency is pulled in.  Supports the common shapes used by k3s /
        kind for local testing: a bearer token, or client-certificate(+key)
        data/files, with an embedded or referenced CA (or ``insecure``).
        """
        from strictyaml.ruamel import YAML
        from strictyaml.ruamel.error import YAMLError, YAMLFutureWarning

        # A syntactically-broken kubeconfig (truncated mid-rotation, a tab
        # indent) raises the ruamel ScannerError/ParserError family (YAMLError
        # subclasses), and a duplicated mapping key raises DuplicateKeyError,
        # which descends from Warning via YAMLFutureWarning, NOT YAMLError --
        # so both roots must be named. A well-formed-YAML but structurally-
        # broken kubeconfig (empty file, no current-context, a context naming
        # an undefined cluster/user) raises KeyError/TypeError/AttributeError.
        # start_stop_cluster catches none of those, so without this they
        # escape as a "please report a bug" crash; convert to ConfigError so a
        # bad kubeconfig is logged as "cluster: failed to start" and the
        # daemon survives, mirroring the native transport's _setup_sync.
        try:
            with open(path) as cfg_file:
                data = YAML(typ="safe").load(cfg_file)
            contexts = {
                c["name"]: c["context"] for c in data.get("contexts", [])
            }
            ctx = contexts[data["current-context"]]
            clusters = {
                c["name"]: c["cluster"] for c in data.get("clusters", [])
            }
            users = {u["name"]: u["user"] for u in data.get("users", [])}
            cluster = clusters[ctx["cluster"]]
            user = users.get(ctx["user"], {})
            self._base_url = cluster["server"].rstrip("/")
        except (
            KeyError,
            TypeError,
            AttributeError,
            YAMLError,
            YAMLFutureWarning,
        ) as ex:
            raise ConfigError(
                "kubernetes backend: malformed kubeconfig {!r} ({})".format(
                    path, ex
                )
            ) from ex
        if self.b.api_server_override:
            # cluster.kubernetes.apiServer overrides the kubeconfig's embedded
            # server URL (a reachable proxy/VIP when the kubeconfig embeds an
            # internal address), matching the native transport (_setup_sync
            # applies it after load_kube_config) and the documented
            # override-wins semantics. Silently keeping the kubeconfig's URL
            # here would point the HTTP and native transports at DIFFERENT
            # apiservers from the same config -- two lease stores each
            # granting a lease (two leaders).
            self._base_url = self.b.api_server_override.rstrip("/")
        self.b.namespace = self.b._resolve_namespace(ctx.get("namespace"))

        if cluster.get("insecure-skip-tls-verify"):
            self._ssl = ssl.create_default_context()
            self._ssl.check_hostname = False
            self._ssl.verify_mode = ssl.CERT_NONE
            # Loud, every-start warning: with verification off the apiserver
            # cert is not checked, so the lease store can be MITM'd and any
            # bearer token sent to it captured (-> token theft + a forged
            # holderIdentity -> two leaders). Intended for local testing only;
            # the silent default is a real footgun in production.
            logger.warning(
                "cluster: kubernetes kubeconfig sets insecure-skip-tls-verify "
                "-- the apiserver certificate is NOT verified, exposing the "
                "lease store (and any bearer token) to MITM. Use only for "
                "local testing; prefer a real CA."
            )
        else:
            self._ssl = ssl.create_default_context()
            ca_data = cluster.get("certificate-authority-data")
            if ca_data:
                self._ssl.load_verify_locations(
                    cadata=base64.b64decode(ca_data).decode("utf-8")
                )
            elif cluster.get("certificate-authority"):
                self._ssl.load_verify_locations(
                    cafile=cluster["certificate-authority"]
                )

        if user.get("token"):
            if self._base_url.lower().startswith("http://"):
                # Refuse to send the bearer token over cleartext http,
                # matching the hard ConfigError on an http:// apiServer
                # override (and the in-cluster defence-in-depth above). A
                # captured token lets an attacker forge a Lease holderIdentity
                # (-> two leaders) or revoke the real holder, so warn-only is
                # not enough.
                raise ConfigError(
                    "kubernetes backend: kubeconfig server is http:// but the "
                    "user has a bearer token; refusing to send it in "
                    "cleartext. Use an https:// server ({})".format(
                        self._base_url
                    )
                )
            self._auth_token = user["token"]
        cert = self._material(
            user.get("client-certificate"),
            user.get("client-certificate-data"),
        )
        key = self._material(
            user.get("client-key"), user.get("client-key-data")
        )
        if cert and key and self._ssl is not None:
            self._ssl.load_cert_chain(cert, key)
        # Track the on-disk TLS material a cert-manager/Vault rotation would
        # renew so start_stop_cluster rebuilds the backend: the kubeconfig
        # itself (an embedded -data cert rotated by rewriting it) plus any
        # file-referenced CA / client cert / key. Embedded -data forms resolve
        # to None paths here and are dropped (a -data change is a kubeconfig
        # rewrite, caught via the kubeconfig path).
        self.b._record_tls_files(
            [
                self.b.kubeconfig,
                cluster.get("certificate-authority"),
                user.get("client-certificate"),
                user.get("client-key"),
            ]
        )
        # This hand-rolled HTTP transport understands only a static bearer
        # token or a client certificate. exec-credential plugins (EKS
        # aws-iam-authenticator, GKE gke-gcloud-auth-plugin) and the legacy
        # auth-provider must be EXECUTED, which only the official client can.
        # Without this check such a kubeconfig yields no Authorization header,
        # 401s every round, and leaves the node PERMANENTLY non-quorate (Leader
        # jobs never run; never-skip PreferLeader jobs double-run on every
        # replica) -- mislogged as a transient "apiserver unreachable". Fail
        # fast and loudly instead of silently never-leading.
        if (
            not self._auth_token
            and not (cert and key)
            and (user.get("exec") or user.get("auth-provider"))
        ):
            raise ConfigError(
                "kubernetes backend: kubeconfig user {!r} authenticates via "
                "an exec-credential plugin or auth-provider, which the "
                "built-in HTTP transport cannot run. Install the optional "
                "native client (pip install 'yacron2[kubernetes]') so the "
                "credential plugin can execute, or use a kubeconfig with a "
                "static token or client certificate.".format(ctx.get("user"))
            )

    def _material(
        self, path: Optional[str], data: Optional[str]
    ) -> Optional[str]:
        """Return a filesystem path for cert/key material given as a path or
        as base64 ``*-data`` (written to a tracked temp file, cleaned on stop).
        """
        if path:
            return path
        if data:
            fd, tmp = tempfile.mkstemp(prefix="yacron2-k8s-")
            with os.fdopen(fd, "wb") as handle:
                handle.write(base64.b64decode(data))
            self._tempfiles.append(tmp)
            return tmp
        return None

    def _lease_url(self, *, collection: bool = False) -> str:
        # URL-encode the namespace and lease name (config validates leaseName/
        # leaseNamespace against the RFC1123 charset, but the namespace may
        # come from a kubeconfig context, and the native client percent-encodes
        # its path params too -- so encoding here keeps both transports pointed
        # at the SAME apiserver resource and stops a stray '/', '?' or '#' from
        # retargeting the request).
        namespace = quote(self.b.namespace or "", safe="")
        base = "{}/apis/coordination.k8s.io/v1/namespaces/{}/leases".format(
            self._base_url, namespace
        )
        if collection:
            return base
        return "{}/{}".format(base, quote(self.b.lease_name, safe=""))

    async def observe(self) -> Optional[Dict[str, Any]]:
        assert self._session is not None
        async with self._session.get(
            self._lease_url(),
            ssl=self._ssl,
            headers=self._auth_headers(),
            # don't follow a redirect to an attacker-chosen target (SSRF) or a
            # plaintext downgrade; the apiserver answers directly. Matches the
            # gossip transport's allow_redirects=False.
            allow_redirects=False,
        ) as resp:
            if resp.status == 404:
                return None
            resp.raise_for_status()
            data: Dict[str, Any] = await resp.json()
            return data

    async def write(self, body: Dict[str, Any], *, create: bool) -> bool:
        """POST (create) or PUT (replace) the Lease; ``False`` on 409 race."""
        assert self._session is not None
        if create:
            url, method = self._lease_url(collection=True), self._session.post
        else:
            url, method = self._lease_url(), self._session.put
        async with method(
            url,
            json=body,
            ssl=self._ssl,
            headers=self._auth_headers(),
            # see observe(): never follow a redirect (SSRF / plaintext
            # downgrade) on a credentialed lease write.
            allow_redirects=False,
        ) as resp:
            if resp.status == 409:
                return False
            resp.raise_for_status()
            return True

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None
        for tmp in self._tempfiles:
            try:
                os.unlink(tmp)
            except OSError:
                pass
        self._tempfiles = []


class _K8sLibraryTransport(_K8sTransport):  # pragma: no cover - client library
    """Native ``kubernetes`` client transport (used when the lib is present).

    The official client is synchronous, so its short, infrequent Lease calls
    run in a worker thread via ``asyncio.to_thread``.  Read leases are
    normalised to the same JSON dict shape the REST transport returns, so the
    pure planning helpers are transport-agnostic.
    """

    def __init__(self, backend: "KubernetesBackend") -> None:
        self.b = backend
        self._api: Any = None
        self._api_client: Any = None

    async def setup(self) -> None:
        # The native client's config loading is fully synchronous and can be
        # SLOW (disk reads, and for an exec-credential kubeconfig an external
        # subprocess that may contact a metadata endpoint). start() is awaited
        # inline from the daemon's single run loop, so run it in a worker
        # thread rather than freezing the whole scheduler/web API meanwhile.
        context_namespace = await asyncio.to_thread(self._setup_sync)
        self.b.namespace = self.b._resolve_namespace(context_namespace)

    def _setup_sync(self) -> Optional[str]:
        from kubernetes import client
        from kubernetes import config as kube_config
        from kubernetes.config.config_exception import ConfigException

        context_namespace: Optional[str] = None
        try:
            if self.b.kubeconfig:
                kube_config.load_kube_config(config_file=self.b.kubeconfig)
                # honour the active context's namespace, matching the HTTP
                # transport's kubeconfig handling so the two transports cannot
                # contend for the Lease in two namespaces (a split-brain).
                _contexts, active = kube_config.list_kube_config_contexts(
                    config_file=self.b.kubeconfig
                )
                if active:
                    context_namespace = (active.get("context") or {}).get(
                        "namespace"
                    )
            else:
                kube_config.load_incluster_config()
        except ConfigException as ex:
            # a malformed/absent kubeconfig or missing in-cluster files is an
            # operational misconfiguration, not a yacron2 bug. ConfigException
            # is NOT in start_stop_cluster's caught tuple, so without this it
            # escapes to the run loop's "please report this as a bug" handler;
            # re-raise as ConfigError so it is logged as "cluster: failed to
            # start" and the daemon keeps running.
            raise ConfigError(
                "kubernetes backend: could not load client configuration "
                "({})".format(ex)
            ) from ex
        # honour cluster.kubernetes.apiServer here too, so the override is not
        # silently dropped when the native client is selected.
        config_obj = client.Configuration.get_default_copy()
        if self.b.api_server_override:
            config_obj.host = self.b.api_server_override.rstrip("/")
        if not getattr(config_obj, "verify_ssl", True):
            # Match the HTTP transport's loud warning: load_kube_config honours
            # insecure-skip-tls-verify silently, so without this a node that
            # selected the native client (an arch where the lib is installed)
            # would disable apiserver cert verification with no operator signal
            # -- the bearer token then travels over an unverified TLS link.
            logger.warning(
                "cluster: kubernetes kubeconfig disables TLS verification "
                "(insecure-skip-tls-verify) -- the apiserver certificate is "
                "NOT verified, exposing the lease store (and any bearer "
                "token) to MITM. Use only for local testing; prefer a real CA."
            )
        self._api_client = client.ApiClient(config_obj)
        self._api = client.CoordinationV1Api(self._api_client)
        # Track the kubeconfig AND any cert/CA/key it references BY FILE PATH,
        # or the in-cluster CA, so an in-place cert/CA rotation rebuilds the
        # backend via start_stop_cluster (the native client freezes its
        # ApiClient cert material at construction, like the HTTP transport's
        # SSLContext). The kubeconfig path alone is NOT enough: a path-
        # referenced cert rotated in place leaves the kubeconfig's own
        # (mtime,size) unchanged, so tracking only it would miss the rotation
        # and the frozen client would keep presenting the expired cert. Mirror
        # the HTTP transport's set (embedded -data forms resolve to None
        # and are dropped: a -data rotation rewrites the kubeconfig itself).
        if self.b.kubeconfig:
            self.b._record_tls_files(
                [self.b.kubeconfig, *_kubeconfig_cert_files(self.b.kubeconfig)]
            )
        else:
            self.b._record_tls_files([os.path.join(_SA_DIR, "ca.crt")])
        return context_namespace

    async def observe(self) -> Optional[Dict[str, Any]]:
        from kubernetes.client.exceptions import ApiException

        def _read() -> Optional[Dict[str, Any]]:
            try:
                lease = self._api.read_namespaced_lease(
                    self.b.lease_name,
                    self.b.namespace,
                    # bound the urllib3 socket so a black-holed apiserver
                    # aborts the call instead of blocking this worker thread
                    # forever: the renew loop's asyncio.wait_for can cancel the
                    # awaiting coroutine but NOT the thread, so without this a
                    # hung round permanently consumes a to_thread worker and
                    # eventually starves the whole process's executor (the
                    # aiohttp transports get this from their ClientTimeout).
                    _request_timeout=self.b.renew_deadline,
                )
            except ApiException as ex:
                if ex.status == 404:
                    return None
                raise
            sanitize = self._api_client.sanitize_for_serialization
            result: Dict[str, Any] = sanitize(lease)
            return result

        return await asyncio.to_thread(_read)

    async def write(self, body: Dict[str, Any], *, create: bool) -> bool:
        from kubernetes.client.exceptions import ApiException

        def _write() -> bool:
            try:
                if create:
                    self._api.create_namespaced_lease(
                        self.b.namespace,
                        body,
                        # see observe(): bound the socket so a hung write
                        # cannot leak the worker thread.
                        _request_timeout=self.b.renew_deadline,
                    )
                else:
                    self._api.replace_namespaced_lease(
                        self.b.lease_name,
                        self.b.namespace,
                        body,
                        _request_timeout=self.b.renew_deadline,
                    )
            except ApiException as ex:
                if ex.status == 409:
                    return False
                raise
            return True

        return await asyncio.to_thread(_write)

    async def close(self) -> None:
        if self._api_client is not None:
            await asyncio.to_thread(self._api_client.close)
            self._api_client = None
