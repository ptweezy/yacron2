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
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp

from yacron2.backends import TRANSPORT_LIBRARY, select_transport
from yacron2.config import ClusterConfig, ConfigError
from yacron2.leadership import LeaseBackend

logger = logging.getLogger("yacron2.backends.kubernetes")

# How far in advance of the computed lease expiry a holder stops calling itself
# leader, so a node whose clock runs slightly fast self-demotes *before* a peer
# would be entitled to steal the lease -- erring is_leader toward False.
_CLOCK_SKEW = datetime.timedelta(seconds=1)

_API_GROUP = "coordination.k8s.io/v1"
_SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"

# decide_lease_action outcomes.
ACTION_CREATE = "create"  # no Lease exists -> create one with us as holder
ACTION_ACQUIRE = "acquire"  # exists but free/expired -> take it over
ACTION_RENEW = "renew"  # we already hold it -> refresh renewTime
ACTION_WAIT = "wait"  # someone else holds a still-valid lease -> not us


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


@dataclass
class LeaseState:
    """The fields of an observed ``Lease`` the election cares about."""

    holder: Optional[str]
    renew_time: Optional[datetime.datetime]
    acquire_time: Optional[datetime.datetime]
    duration: Optional[int]
    transitions: int
    resource_version: Optional[str]


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
    return LeaseState(
        holder=spec.get("holderIdentity"),
        renew_time=_parse_microtime(spec.get("renewTime")),
        acquire_time=_parse_microtime(spec.get("acquireTime")),
        duration=duration if isinstance(duration, int) else None,
        transitions=transitions if isinstance(transitions, int) else 0,
        resource_version=meta.get("resourceVersion"),
    )


def lease_is_expired(state: LeaseState, now: datetime.datetime) -> bool:
    """Whether ``state``'s lease has lapsed by ``now``.

    Expired iff ``now`` is at or past ``renewTime + leaseDurationSeconds``.  A
    lease with no renew time or duration is treated as expired (free to take).
    """
    if state.renew_time is None or not state.duration:
        return True
    expiry = state.renew_time + datetime.timedelta(seconds=state.duration)
    return now >= expiry


def decide_lease_action(
    state: Optional[LeaseState],
    identity: str,
    now: datetime.datetime,
) -> str:
    """Choose the action for this round given the observed lease.

    * no lease -> ``create``;
    * we are the holder -> ``renew`` (reclaim even if it lapsed);
    * holder is empty or the lease has expired -> ``acquire`` (take over);
    * someone else holds a still-valid lease -> ``wait``.
    """
    if state is None:
        return ACTION_CREATE
    if state.holder == identity:
        return ACTION_RENEW
    if not state.holder or lease_is_expired(state, now):
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
) -> Dict[str, Any]:
    """Build the ``Lease`` object to POST (create) or PUT (acquire/renew).

    ``acquireTime`` and ``leaseTransitions`` follow client-go: a fresh acquire
    (taking over from another holder or an empty/expired lease) stamps a new
    ``acquireTime`` and bumps ``leaseTransitions``; a renew preserves both.  A
    replace (acquire/renew) carries the observed ``resourceVersion`` so the
    apiserver rejects the write (HTTP 409) if another node raced us.
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
) -> Tuple[str, Optional[Dict[str, Any]], Optional[LeaseState]]:
    """Pure planning step: observed Lease -> (action, body, state).

    ``body`` is ``None`` for ``wait`` (nothing to write).  This is the whole
    per-round decision with no I/O, so the renew loop reduces to "observe,
    plan, maybe write, update local state".
    """
    state = parse_lease(lease_obj) if lease_obj is not None else None
    action = decide_lease_action(state, identity, now)
    if action == ACTION_WAIT:
        return action, None, state
    body = build_lease_body(
        name, namespace, identity, now, duration, state, action
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
        self.identity: str = k8s["identity"]
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

        # live state, written by the renew loop and read by the sync methods
        self._is_leader = False
        self._holder: Optional[str] = None
        self._leader_until: Optional[datetime.datetime] = None
        self._last_contact: Optional[datetime.datetime] = None

        # the chosen transport (native client or hand-rolled HTTP), bound in
        # start(); see select_transport.
        self._transport: Optional["_K8sTransport"] = None
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    # --- pure local-state reads (no I/O) ---------------------------------

    def _leader_deadline(self, now: datetime.datetime) -> datetime.datetime:
        """The instant a successful renew at ``now`` keeps us leader until."""
        return (
            now + datetime.timedelta(seconds=self.lease_duration) - _CLOCK_SKEW
        )

    def is_leader(self) -> bool:
        if not self._is_leader or self._leader_until is None:
            return False
        # local-expiry safety: a stalled renew loop self-demotes with no call.
        return _utcnow() < self._leader_until

    def leader_name(self) -> Optional[str]:
        if not self.is_quorate():
            return None
        return self._holder

    def is_quorate(self) -> bool:
        """Whether we have a *fresh* successful read of the lease store.

        Stale (or never-contacted) -> not quorate, so ``Leader`` fails closed
        and the never-skip ``PreferLeader`` default runs the job anyway.
        """
        if self._last_contact is None:
            return False
        freshness = datetime.timedelta(seconds=self.lease_duration)
        return _utcnow() < self._last_contact + freshness

    def lease_detail(self) -> Dict[str, Any]:
        return {
            "name": self.lease_name,
            "namespace": self.namespace,
            "identity": self.identity,
            "holder": self._holder,
            "expiry": (
                _format_microtime(self._leader_until)
                if self._leader_until is not None
                else None
            ),
        }

    # --- the renew loop's per-round local-state update (pure) ------------

    def _apply_round(
        self,
        action: str,
        write_ok: bool,
        state: Optional[LeaseState],
        now: datetime.datetime,
    ) -> None:
        """Update the live leader state from this round's outcome.

        Separated from the I/O so it is unit-tested: ``write_ok`` is whether
        the create/replace succeeded (a 409 conflict is ``False`` -- another
        node won).  Always advances ``_last_contact`` (we *did* reach apiserver
        this round, which is what ``is_quorate`` tracks).
        """
        self._last_contact = now
        if action == ACTION_WAIT:
            self._is_leader = False
            self._holder = state.holder if state is not None else None
            return
        if write_ok:
            self._is_leader = True
            self._holder = self.identity
            self._leader_until = self._leader_deadline(now)
        else:
            # lost the optimistic-concurrency race (409): not leader now.
            self._is_leader = False
            self._holder = state.holder if state is not None else None

    # --- transport selection + renew loop (integration-only) -------------

    def _native_available(self) -> bool:  # pragma: no cover - import probe
        try:
            import kubernetes  # noqa: F401

            return True
        except ImportError:
            return False

    async def start(self) -> None:  # pragma: no cover - network/credential I/O
        kind = select_transport(
            self.client_library, self._native_available(), "kubernetes"
        )
        self._transport = (
            _K8sLibraryTransport(self)
            if kind == TRANSPORT_LIBRARY
            else _K8sHttpTransport(self)
        )
        await self._transport.setup()
        logger.info(
            "cluster: kubernetes backend (%s transport), identity %r, lease "
            "%s/%s (duration %ds, renew %ds, retry %ds)",
            kind,
            self.identity,
            self.namespace,
            self.lease_name,
            self.lease_duration,
            self.renew_deadline,
            self.retry_period,
        )
        self._stop.clear()
        self._task = asyncio.create_task(self._renew_loop())

    async def _renew_loop(self) -> None:  # pragma: no cover - network loop
        assert self._transport is not None
        while not self._stop.is_set():
            try:
                await self._renew_once()
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
        now = _utcnow()
        lease_obj = await self._transport.observe()
        action, body, state = plan_lease_write(
            lease_obj,
            self.lease_name,
            self.namespace,
            self.identity,
            now,
            self.lease_duration,
        )
        write_ok = False
        if body is not None:
            write_ok = await self._transport.write(
                body, create=(action == ACTION_CREATE)
            )
        self._apply_round(action, write_ok, state, now)

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
            if self._is_leader:
                await self._release()
            await self._transport.close()
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
            body = {
                "apiVersion": _API_GROUP,
                "kind": "Lease",
                "metadata": {
                    "name": self.lease_name,
                    "namespace": self.namespace,
                    "resourceVersion": state.resource_version,
                },
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
        self._ssl: Optional[ssl.SSLContext] = None
        self._tempfiles: List[str] = []
        self._session: Optional[aiohttp.ClientSession] = None

    async def setup(self) -> None:
        self._load_connection()
        timeout = aiohttp.ClientTimeout(total=self.b.connect_timeout)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if self._auth_token:
            headers["Authorization"] = "Bearer " + self._auth_token
        self._session = aiohttp.ClientSession(timeout=timeout, headers=headers)

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
            self._base_url = self.b.api_server_override.rstrip("/")
        elif host:
            self._base_url = "https://{}:{}".format(host, port)
        else:
            raise ConfigError(
                "kubernetes backend: not running in a cluster and no "
                "cluster.kubernetes.kubeconfig or apiServer configured"
            )
        try:
            with open(os.path.join(_SA_DIR, "token")) as token_file:
                self._auth_token = token_file.read().strip()
            ca_path = os.path.join(_SA_DIR, "ca.crt")
            self._ssl = ssl.create_default_context(cafile=ca_path)
            if self.b.namespace is None:
                self.b.namespace = _incluster_namespace()
        except OSError as ex:
            raise ConfigError(
                "kubernetes backend: could not read in-cluster service "
                "account credentials ({}); set cluster.kubernetes.kubeconfig "
                "for out-of-cluster use".format(ex)
            ) from ex

    def _load_kubeconfig(self, path: str) -> None:
        """Minimal kubeconfig loader (server, CA, token or client cert).

        Uses the bundled ruamel YAML (a strictyaml transitive dependency) so no
        new dependency is pulled in.  Supports the common shapes used by k3s /
        kind for local testing: a bearer token, or client-certificate(+key)
        data/files, with an embedded or referenced CA (or ``insecure``).
        """
        from strictyaml.ruamel import YAML

        with open(path) as cfg_file:
            data = YAML(typ="safe").load(cfg_file)
        contexts = {c["name"]: c["context"] for c in data.get("contexts", [])}
        ctx = contexts[data["current-context"]]
        clusters = {c["name"]: c["cluster"] for c in data.get("clusters", [])}
        users = {u["name"]: u["user"] for u in data.get("users", [])}
        cluster = clusters[ctx["cluster"]]
        user = users.get(ctx["user"], {})
        self._base_url = cluster["server"].rstrip("/")
        if self.b.namespace is None:
            self.b.namespace = ctx.get("namespace") or "default"

        if cluster.get("insecure-skip-tls-verify"):
            self._ssl = ssl.create_default_context()
            self._ssl.check_hostname = False
            self._ssl.verify_mode = ssl.CERT_NONE
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
        base = "{}/apis/coordination.k8s.io/v1/namespaces/{}/leases".format(
            self._base_url, self.b.namespace
        )
        return base if collection else "{}/{}".format(base, self.b.lease_name)

    async def observe(self) -> Optional[Dict[str, Any]]:
        assert self._session is not None
        async with self._session.get(self._lease_url(), ssl=self._ssl) as resp:
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
        async with method(url, json=body, ssl=self._ssl) as resp:
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
        from kubernetes import client
        from kubernetes import config as kube_config

        if self.b.kubeconfig:
            kube_config.load_kube_config(config_file=self.b.kubeconfig)
        else:
            kube_config.load_incluster_config()
        self._api_client = client.ApiClient()
        self._api = client.CoordinationV1Api(self._api_client)
        if self.b.namespace is None:
            self.b.namespace = _incluster_namespace() or "default"

    async def observe(self) -> Optional[Dict[str, Any]]:
        from kubernetes.client.exceptions import ApiException

        def _read() -> Optional[Dict[str, Any]]:
            try:
                lease = self._api.read_namespaced_lease(
                    self.b.lease_name, self.b.namespace
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
                    self._api.create_namespaced_lease(self.b.namespace, body)
                else:
                    self._api.replace_namespaced_lease(
                        self.b.lease_name, self.b.namespace, body
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
