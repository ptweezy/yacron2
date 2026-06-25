"""etcd leadership backend (hand-rolled against the v3 JSON gateway).

A single etcd key (``cluster.etcd.electionName``) is the fence: the node that
creates it -- holding our identity as the value, bound to a short-TTL lease --
is the leader.  The key is created with a *create-if-absent* transaction
(compare ``CREATE`` revision ``== 0``), so at most one node ever wins; the
holder keeps the lease alive, and if it dies the lease expires and etcd deletes
the key, letting another node's transaction win.

It talks to etcd over the **v3 gRPC-gateway JSON/HTTP API** (``/v3/kv/txn``,
``/v3/lease/grant``, ``/v3/lease/keepalive``, ``/v3/lease/revoke``) using the
core ``aiohttp`` dependency -- *not* the ``etcd3`` client -- so it adds no
dependency and, by avoiding grpc/protobuf, keeps yacron2's wide architecture
coverage.  Keys and values are base64-encoded per that API.

As with the Kubernetes backend, the decision logic (building the campaign
transaction, reading the holder out of a transaction/keepalive response,
deciding leadership) is in pure, unit-tested helpers; the HTTP glue is
``# pragma: no cover`` and exercised only by the Docker integration tests.  The
local-expiry safety is the same: :meth:`EtcdBackend.is_leader` is gated on a
locally-computed lease deadline, so a stalled keepalive self-demotes without a
network call, and ``is_quorate`` reflects a fresh successful call (stale ->
``Leader`` fails closed, never-skip ``PreferLeader`` runs anyway).
"""

import asyncio
import base64
import datetime
import logging
import ssl
from typing import Any, Callable, Dict, List, Optional

import aiohttp

from yacron2.config import ClusterConfig
from yacron2.leadership import LeaseBackend

logger = logging.getLogger("yacron2.backends.etcd")

# See yacron2.backends.kubernetes._CLOCK_SKEW.
_CLOCK_SKEW = datetime.timedelta(seconds=1)


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _b64(text: str) -> str:
    """Base64-encode a string for the etcd JSON API."""
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def _b64decode(text: str) -> str:
    """Decode a base64 string from an etcd JSON response (lossy-safe)."""
    return base64.b64decode(text).decode("utf-8", "replace")


def build_campaign_txn(
    key: str, identity: str, lease_id: str
) -> Dict[str, Any]:
    """A create-if-absent transaction that campaigns for ``key``.

    *If* ``key`` has never been created (``CREATE`` revision ``0``) *then* put
    our identity there bound to ``lease_id`` (we become leader); *else* read
    the current value (the existing holder) so the caller learns who leads.
    """
    encoded = _b64(key)
    return {
        "compare": [
            {
                "key": encoded,
                "result": "EQUAL",
                "target": "CREATE",
                "create_revision": "0",
            }
        ],
        "success": [
            {
                "requestPut": {
                    "key": encoded,
                    "value": _b64(identity),
                    "lease": str(lease_id),
                }
            }
        ],
        "failure": [{"requestRange": {"key": encoded}}],
    }


def holder_from_txn_response(
    resp: Dict[str, Any], identity: str
) -> Optional[str]:
    """The current holder implied by a campaign transaction's response.

    A succeeded transaction means *we* created the key, so the holder is us; a
    failed one carries the existing key's value in its range response.  Returns
    ``None`` if neither is present (an empty/odd response).

    This is the *display* name (the human ``nodeName`` stored at the key); the
    leadership decision uses :func:`campaign_won`, which fences on the bound
    lease id, not on this string.
    """
    if resp.get("succeeded"):
        return identity
    for entry in resp.get("responses", []) or []:
        rng = entry.get("response_range") or entry.get("responseRange")
        if rng:
            kvs = rng.get("kvs") or []
            if kvs and kvs[0].get("value") is not None:
                return _b64decode(kvs[0]["value"])
    return None


def campaign_won(resp: Dict[str, Any], my_lease_id: str) -> bool:
    """Whether *we* hold the election key after this campaign transaction.

    ``True`` iff we created the key (the transaction succeeded) **or** the
    existing key is bound to *our* lease id.  Fencing leadership on the lease
    id -- not merely on the stored identity string -- is what makes the
    election safe even when two nodes share an identity (a duplicate
    ``nodeName``): each node grants its *own* lease, so only the node whose
    lease actually backs the key is the leader.  A lost campaign against a key
    bound to someone else's lease (even one storing our own identity string)
    returns ``False``, so the loser stands down instead of both believing they
    hold the fence.
    """
    if resp.get("succeeded"):
        return True
    for entry in resp.get("responses", []) or []:
        rng = entry.get("response_range") or entry.get("responseRange")
        if rng:
            kvs = rng.get("kvs") or []
            if kvs:
                bound = kvs[0].get("lease")
                return bound is not None and str(bound) == str(my_lease_id)
    return False


def lease_id_from_grant(resp: Dict[str, Any]) -> Optional[str]:
    """The lease id from a ``/v3/lease/grant`` response (stringified int64)."""
    lease_id = resp.get("ID")
    return str(lease_id) if lease_id is not None else None


def lease_ttl_from_keepalive(resp: Dict[str, Any]) -> Optional[int]:
    """Remaining TTL from a ``/v3/lease/keepalive`` response.

    ``0`` (or a missing/odd value) means the lease is gone -- the holder must
    re-grant and re-campaign.
    """
    result = resp.get("result") or {}
    ttl = result.get("TTL")
    if ttl is None:
        return None
    try:
        return int(ttl)
    except (TypeError, ValueError):
        return None


class EtcdBackend(LeaseBackend):
    """Leadership via a lease-bound etcd key (the v3 JSON gateway)."""

    backend_name = "etcd"

    def __init__(
        self,
        config: ClusterConfig,
        get_job_set_id: Callable[[], str],
    ) -> None:
        super().__init__(config, get_job_set_id)
        etcd = config["etcd"]
        self.endpoints: List[str] = list(etcd["endpoints"])
        self.election_name: str = etcd["electionName"]
        # the value written at the election key; nodeName is the identity.
        self.identity: str = config["nodeName"]
        self.ttl: int = etcd["ttl"]
        self.username: Optional[str] = etcd["username"]
        self.password: Optional[str] = etcd.get("resolved_password")
        self._tls: Dict[str, Optional[str]] = etcd["tls"]
        self.connect_timeout: int = config["connectTimeout"]
        # keepalive cadence: comfortably within the TTL (>= once a second)
        self.renew_period: float = max(1.0, self.ttl / 3)

        # live state, written by the renew loop and read by the sync methods
        self._is_leader = False
        self._holder: Optional[str] = None
        self._lease_id: Optional[str] = None
        self._lease_deadline: Optional[datetime.datetime] = None
        self._last_contact: Optional[datetime.datetime] = None

        self._auth_token: Optional[str] = None
        self._ssl: Optional[ssl.SSLContext] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    # --- pure local-state reads (no I/O) ---------------------------------

    def _leader_deadline(self, now: datetime.datetime) -> datetime.datetime:
        return now + datetime.timedelta(seconds=self.ttl) - _CLOCK_SKEW

    def is_leader(self) -> bool:
        if not self._is_leader or self._lease_deadline is None:
            return False
        return _utcnow() < self._lease_deadline

    def leader_name(self) -> Optional[str]:
        if not self.is_quorate():
            return None
        return self._holder

    def is_quorate(self) -> bool:
        if self._last_contact is None:
            return False
        freshness = datetime.timedelta(seconds=self.ttl)
        return _utcnow() < self._last_contact + freshness

    def lease_detail(self) -> Dict[str, Any]:
        from yacron2.backends.kubernetes import _format_microtime

        return {
            "electionName": self.election_name,
            "identity": self.identity,
            "holder": self._holder,
            "leaseId": self._lease_id,
            "expiry": (
                _format_microtime(self._lease_deadline)
                if self._lease_deadline is not None
                else None
            ),
        }

    def _apply_round(
        self,
        holder: Optional[str],
        is_leader: bool,
        now: datetime.datetime,
    ) -> None:
        """Update live leader state from a round's outcome (pure, tested).

        ``is_leader`` is whether *we* hold the election key via *our* lease
        (see :func:`campaign_won`); ``holder`` is the display name stored at
        the key (whoever currently holds it).
        """
        self._last_contact = now
        self._holder = holder
        self._is_leader = is_leader
        if self._is_leader:
            self._lease_deadline = self._leader_deadline(now)

    # --- network glue (integration-only) ---------------------------------

    async def start(self) -> None:  # pragma: no cover - network/credential I/O
        self._ssl = self._build_ssl()
        timeout = aiohttp.ClientTimeout(total=self.connect_timeout)
        self._session = aiohttp.ClientSession(timeout=timeout)
        if self.username:
            self._auth_token = await self._authenticate()
        logger.info(
            "cluster: etcd backend, identity %r, election key %r, ttl %ds, "
            "endpoints %s",
            self.identity,
            self.election_name,
            self.ttl,
            ", ".join(self.endpoints),
        )
        self._stop.clear()
        self._task = asyncio.create_task(self._renew_loop())

    def _build_ssl(self) -> Optional[ssl.SSLContext]:  # pragma: no cover
        if not any(self.endpoint_is_https(e) for e in self.endpoints):
            return None
        ctx = ssl.create_default_context(cafile=self._tls.get("ca") or None)
        cert, key = self._tls.get("cert"), self._tls.get("key")
        if cert and key:
            ctx.load_cert_chain(cert, key)
        return ctx

    @staticmethod
    def endpoint_is_https(endpoint: str) -> bool:
        return endpoint.lower().startswith("https://")

    async def _authenticate(self) -> Optional[str]:  # pragma: no cover
        resp = await self._post(
            "/v3/auth/authenticate",
            {"name": self.username, "password": self.password},
            allow_reauth=False,
        )
        return resp.get("token")

    async def _post(
        self, path: str, body: Dict[str, Any], *, allow_reauth: bool = True
    ) -> Dict[str, Any]:  # pragma: no cover - network
        """POST ``body`` to ``path`` on the first responsive endpoint.

        etcd auth tokens have a TTL, so on a ``401`` from an auth-enabled
        cluster the token has expired: re-authenticate once and retry, so the
        backend recovers on its own instead of failing every round against a
        healthy etcd until the process is restarted.  The retry passes
        ``allow_reauth=False`` (and ``_authenticate`` itself does too) to bound
        recursion to a single refresh -- a persistent ``401`` (e.g. bad
        credentials) then surfaces as a normal failed round.
        """
        assert self._session is not None
        headers = {}
        if self._auth_token:
            headers["Authorization"] = self._auth_token
        last_error: Optional[Exception] = None
        for endpoint in self.endpoints:
            url = endpoint.rstrip("/") + path
            try:
                async with self._session.post(
                    url, json=body, ssl=self._ssl, headers=headers
                ) as resp:
                    if resp.status == 401 and self.username and allow_reauth:
                        self._auth_token = await self._authenticate()
                        return await self._post(path, body, allow_reauth=False)
                    resp.raise_for_status()
                    data: Dict[str, Any] = await resp.json()
                    return data
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as ex:
                last_error = ex
                continue
        raise aiohttp.ClientError(
            "all etcd endpoints failed: {}".format(last_error)
        )

    async def _grant_lease(self) -> Optional[str]:  # pragma: no cover
        resp = await self._post("/v3/lease/grant", {"TTL": str(self.ttl)})
        return lease_id_from_grant(resp)

    async def _keepalive(
        self, lease_id: str
    ) -> Optional[int]:  # pragma: no cover - network
        resp = await self._post("/v3/lease/keepalive", {"ID": str(lease_id)})
        return lease_ttl_from_keepalive(resp)

    async def _campaign(
        self, lease_id: str
    ) -> "tuple[Optional[str], bool]":  # pragma: no cover - network
        """Campaign for the election key; return ``(holder, won)``.

        ``holder`` is the display name stored at the key; ``won`` is whether
        the key is bound to *our* lease (the fence; see :func:`campaign_won`).
        """
        resp = await self._post(
            "/v3/kv/txn",
            build_campaign_txn(self.election_name, self.identity, lease_id),
        )
        return (
            holder_from_txn_response(resp, self.identity),
            campaign_won(resp, lease_id),
        )

    async def _renew_loop(self) -> None:  # pragma: no cover - network loop
        while not self._stop.is_set():
            try:
                await self._renew_once()
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as ex:
                # could not reach etcd this round: leave _last_contact alone so
                # is_quorate goes stale (Leader closed, PreferLeader runs).
                logger.warning("cluster: etcd round failed: %s", ex)
            except Exception:
                logger.exception("cluster: unexpected etcd error")
            try:
                await asyncio.wait_for(self._stop.wait(), self.renew_period)
            except asyncio.TimeoutError:
                pass

    async def _renew_once(self) -> None:  # pragma: no cover - network
        now = _utcnow()
        if self._lease_id is not None:
            ttl = await self._keepalive(self._lease_id)
            if ttl is None or ttl <= 0:
                self._lease_id = None  # lease expired; re-grant below
                self._is_leader = False
        if self._lease_id is None:
            self._lease_id = await self._grant_lease()
        if self._lease_id is None:
            raise aiohttp.ClientError("etcd lease grant returned no id")
        holder, won = await self._campaign(self._lease_id)
        self._apply_round(holder, won, now)

    async def stop(self) -> None:  # pragma: no cover - network
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._lease_id is not None:
            # revoking the lease deletes the election key at once -> immediate
            # failover (no waiting out the TTL).
            try:
                await self._post(
                    "/v3/lease/revoke", {"ID": str(self._lease_id)}
                )
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as ex:
                logger.debug("cluster: etcd lease revoke failed: %s", ex)
            self._lease_id = None
        if self._session is not None:
            await self._session.close()
            self._session = None
        self._is_leader = False
