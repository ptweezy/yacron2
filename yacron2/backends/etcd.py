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
import json
import logging
import ssl
import time
from typing import Any, Callable, Dict, List, Optional

import aiohttp

from yacron2.config import ClusterConfig, _redact_userinfo
from yacron2.leadership import (
    LeaseBackend,
    decode_reboot_ran,
    encode_reboot_ran,
)

logger = logging.getLogger("yacron2.backends.etcd")

# See yacron2.backends.kubernetes._CLOCK_SKEW.
_CLOCK_SKEW = datetime.timedelta(seconds=1)
_SKEW_SECONDS = _CLOCK_SKEW.total_seconds()


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _monotonic() -> float:
    """A monotonic clock for lease/quorum *deadlines*.

    Lease fences must never be judged on the wall clock: a backward NTP/VM step
    would keep ``is_leader`` true past the server's lease expiry (a second node
    has by then won the campaign); a forward step would expire quorum early.
    ``time.monotonic`` cannot jump, so deadlines anchored to it stay correct
    across any wall-clock correction. The wall clock is used only for the
    human-readable expiry shown in the dashboard.
    """
    return time.monotonic()


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


def lease_ttl_from_grant(resp: Dict[str, Any]) -> Optional[int]:
    """The *server-chosen* TTL from a ``/v3/lease/grant`` response, if any.

    etcd may grant a TTL lower than requested; the local lease deadline must be
    derived from what etcd granted, not the configured value, or a node
    would call itself leader past the point etcd has already expired the key.
    ``None`` if the field is absent or unparseable (the caller keeps the
    configured ttl).
    """
    ttl = resp.get("TTL")
    if ttl is None:
        return None
    try:
        return int(ttl)
    except (TypeError, ValueError):
        return None


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
        # a sibling, non-lease-bound key holding the @reboot-ran set (persisted
        # so a failover holder does not re-run a deferred one-shot; see
        # LeaseBackend). Separate from the election key, so it survives the
        # election lease expiring on a leader change.
        self.reboot_ran_key: str = self.election_name + "/reboot-ran"
        # the value written at the election key; nodeName is the identity.
        self.identity: str = config["nodeName"]
        self.ttl: int = etcd["ttl"]
        # the TTL the lease deadline is actually computed from -- etcd may
        # grant/refresh a shorter one (see lease_ttl_from_grant); starts at the
        # configured value and is narrowed by each grant/keepalive.
        self._effective_ttl: int = self.ttl
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
        # wall-clock expiry, for the dashboard/lease_detail display ONLY
        self._lease_deadline: Optional[datetime.datetime] = None
        # monotonic deadlines: the load-bearing fence/freshness gates (immune
        # wall-clock steps; see _monotonic)
        self._lease_deadline_mono: Optional[float] = None
        self._last_contact_mono: Optional[float] = None

        self._auth_token: Optional[str] = None
        self._ssl: Optional[ssl.SSLContext] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    # --- pure local-state reads (no I/O) ---------------------------------

    def _leader_deadline(self, now: datetime.datetime) -> datetime.datetime:
        """Wall-clock lease expiry, for display only (see ``_apply_round``)."""
        return (
            now + datetime.timedelta(seconds=self._effective_ttl) - _CLOCK_SKEW
        )

    def is_leader(self) -> bool:
        if not self._is_leader or self._lease_deadline_mono is None:
            return False
        # gated on a MONOTONIC deadline so a backward wall-clock step cannot
        # keep us "leader" past the point etcd has expired the key.
        return _monotonic() < self._lease_deadline_mono

    def leader_name(self) -> Optional[str]:
        if not self.is_quorate():
            return None
        return self._holder

    def is_quorate(self) -> bool:
        if self._last_contact_mono is None:
            return False
        return _monotonic() < self._last_contact_mono + self._effective_ttl

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
        mono: Optional[float] = None,
        lease_mono: Optional[float] = None,
    ) -> None:
        """Update live leader state from a round's outcome (pure, tested).

        ``is_leader`` is whether *we* hold the election key via *our* lease
        (see :func:`campaign_won`); ``holder`` is the display name stored at
        the key (whoever currently holds it).  ``now`` is wall-clock (for the
        displayed expiry).  ``mono`` is the matching monotonic instant the
        freshness gate (``is_quorate``) uses, captured at round END -- the safe
        (fresher) direction for freshness.  ``lease_mono`` is a monotonic
        instant captured just BEFORE the lease-renewing keepalive/grant POST is
        sent -- a guaranteed lower bound on when the server reset our lease's
        TTL; the leadership FENCE is anchored to *that*, never to the later
        round-end ``mono``, so neither the keepalive/grant round-trip nor a
        slow campaign can push our local ``is_leader`` deadline past the
        server's lease expiry and let a second node win the freed key (two
        leaders).
        Both default to the current monotonic clock for the pure unit tests.
        """
        if mono is None:
            mono = _monotonic()
        self._last_contact_mono = mono
        self._holder = holder
        self._is_leader = is_leader
        if self._is_leader:
            # the fence MUST NOT outlive the server lease (server expiry >=
            # lease_mono + effective_ttl, since lease_mono is a pre-send lower
            # bound on the server's TTL reset); anchoring to it keeps the fence
            # conservative regardless of keepalive/grant or campaign latency.
            fence_anchor = lease_mono if lease_mono is not None else mono
            self._lease_deadline = self._leader_deadline(now)
            self._lease_deadline_mono = (
                fence_anchor + self._effective_ttl - _SKEW_SECONDS
            )

    # --- network glue (integration-only) ---------------------------------

    async def start(self) -> None:  # pragma: no cover - network/credential I/O
        self._ssl = self._build_ssl()
        timeout = aiohttp.ClientTimeout(total=self.connect_timeout)
        self._session = aiohttp.ClientSession(timeout=timeout)
        try:
            if self.username:
                self._auth_token = await self._authenticate()
            # Run one round up front so is_quorate/is_leader reflect a read
            # of the store BEFORE the first spawn_jobs. Without it a
            # lease backend is "never contacted" for one cycle, which makes
            # every PreferLeader job run on every node at boot (and on every
            # reload that rebuilds the manager). Best-effort: a failed/slow
            # round is swallowed (the loop retries), leaving the not-quorate
            # state -- the genuine "store unreachable" case.
            try:
                await asyncio.wait_for(self._renew_once(), self.ttl)
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as ex:
                logger.warning("cluster: etcd initial round failed: %s", ex)
        except BaseException:
            # Honour the "a backend cleans up its own half-started state on
            # failure" contract (as KubernetesBackend.start already does): an
            # unreachable etcd or a rejected credential raises here; without
            # this the open ClientSession/connector leaks -- one per reload --
            # and is never closed (start() never returns, so the caller never
            # stores the manager to stop() it). BaseException also covers a
            # cancellation mid-handshake.
            await self._session.close()
            self._session = None
            raise
        logger.info(
            "cluster: etcd backend, identity %r, election key %r, ttl %ds, "
            "endpoints %s",
            self.identity,
            self.election_name,
            self.ttl,
            # redact any userinfo (config rejects it, but never log a secret).
            ", ".join(_redact_userinfo(e) for e in self.endpoints),
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
        endpoints = self.endpoints
        if self.username or self.password:
            # Defence in depth: never transmit credentials (the cleartext
            # password to /v3/auth/authenticate, or the bearer token on every
            # other call) over a plaintext endpoint. Config validation already
            # rejects auth combined with any http:// endpoint, so in practice
            # this filters nothing -- but it guarantees that even a mixed list
            # that somehow reached here cannot leak the secret over its http
            # member. With no auth configured there is nothing to protect, so
            # all endpoints stay eligible.
            endpoints = [e for e in endpoints if self.endpoint_is_https(e)]
        last_error: Optional[Exception] = None
        for endpoint in endpoints:
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
            # json.JSONDecodeError (a ValueError) is raised by resp.json() when
            # the body has a json-ish content-type but an invalid/truncated
            # body -- a misbehaving proxy/gateway in front of etcd. Treat it as
            # a failed endpoint like any transport error so it (a) tries the
            # next endpoint, (b) surfaces as a normal "etcd round failed"
            # rather than escaping the renew loop's network-error tuple and
            # either killing the scheduler (the eager reboot-ran persist path
            # runs outside the run-loop guard) or being mislogged as an
            # unexpected internal bug.
            except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                OSError,
                json.JSONDecodeError,
            ) as ex:
                last_error = ex
                continue
        raise aiohttp.ClientError(
            "all etcd endpoints failed: {}".format(last_error)
        )

    async def _grant_lease(
        self,
    ) -> "tuple[Optional[str], Optional[int]]":  # pragma: no cover
        """Grant a lease; return ``(lease_id, server_granted_ttl)``.

        The granted TTL may be lower than requested, so the caller narrows the
        local deadline to it (see :func:`lease_ttl_from_grant`).
        """
        resp = await self._post("/v3/lease/grant", {"TTL": str(self.ttl)})
        return lease_id_from_grant(resp), lease_ttl_from_grant(resp)

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
                # Bound the whole round by the ttl: _renew_once makes several
                # sequential POSTs, each iterating every endpoint, so on
                # slow/half-open endpoints an unbounded round could block for
                # far longer than the lease lifetime (N_endpoints x
                # connectTimeout per call). After ttl the lease has lapsed
                # anyway (is_leader self-demotes on its monotonic deadline), so
                # abandon and retry rather than wedge the loop.
                await asyncio.wait_for(self._renew_once(), self.ttl)
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as ex:
                # could not reach etcd this round: leave the monotonic contact
                # deadline alone so is_quorate goes stale (Leader closed,
                # PreferLeader runs).
                logger.warning("cluster: etcd round failed: %s", ex)
            except Exception:
                logger.exception("cluster: unexpected etcd error")
            try:
                await asyncio.wait_for(self._stop.wait(), self.renew_period)
            except asyncio.TimeoutError:
                pass

    async def _renew_once(self) -> None:  # pragma: no cover - network
        # Anchor the local leadership fence to a monotonic instant captured
        # BEFORE the lease-renewing POST (keepalive or grant) is sent. etcd
        # resets our lease's TTL when it *processes* that request, which is at
        # or after we send it, so a pre-send sample is a guaranteed LOWER BOUND
        # on the server's TTL reset -- the conservative anchor. Sampling AFTER
        # the response (the naive choice) would inflate the anchor by the
        # request round-trip time; on a slow-but-reachable etcd whose RTT
        # exceeds the _SKEW_SECONDS margin that pushes is_leader() past the
        # point etcd has already expired and deleted the key, letting a second
        # node's create-if-absent campaign win while we still believe we lead
        # (two leaders running a Leader job). The campaign read below is
        # likewise excluded: its latency must not extend the fence either.
        lease_mono: Optional[float] = None
        if self._lease_id is not None:
            lease_mono = _monotonic()
            ttl = await self._keepalive(self._lease_id)
            if ttl is None or ttl <= 0:
                self._lease_id = None  # lease expired; re-grant below
                self._is_leader = False
                lease_mono = None  # re-anchored before the grant POST below
            else:
                # honour the TTL etcd refreshed to (may be < requested)
                self._effective_ttl = max(1, min(self.ttl, ttl))
        if self._lease_id is None:
            lease_mono = _monotonic()
            self._lease_id, granted = await self._grant_lease()
            if granted is not None:
                self._effective_ttl = max(1, min(self.ttl, granted))
        if self._lease_id is None:
            raise aiohttp.ClientError("etcd lease grant returned no id")
        holder, won = await self._campaign(self._lease_id)
        # ``now``/``mono`` are sampled at round END -- the safe (fresher)
        # direction for the is_quorate freshness gate (_last_contact_mono). The
        # leadership fence is anchored to ``lease_mono`` above (the keepalive/
        # grant landing) instead, so the campaign's latency cannot push it past
        # the server lease's expiry. See _apply_round.
        now = _utcnow()
        mono = _monotonic()
        self._apply_round(holder, won, now, mono, lease_mono)
        await self._sync_reboot_ran()

    # --- @reboot-ran persistence (H2: no peer set, so persist to the store) --

    async def _sync_reboot_ran(self) -> None:  # pragma: no cover - network
        """Best-effort: read the @reboot-ran key, fold it in, re-persist marks.

        Wrapped so an auxiliary read/write failure never fails the leadership
        round (the local set still prevents this node re-running its own one-
        shot); the next round retries.
        """
        try:
            # drop our own marks first if the job set changed, so a redefined
            # @reboot one-shot is not re-suppressed by a stale local mark being
            # re-published under the new job-set id (see
            # LeaseBackend._reconcile_local_reboot_ran).
            self._reconcile_local_reboot_ran()
            resp = await self._post(
                "/v3/kv/range", {"key": _b64(self.reboot_ran_key)}
            )
            kvs = resp.get("kvs") or []
            raw = (
                _b64decode(kvs[0]["value"])
                if kvs and kvs[0].get("value") is not None
                else None
            )
            stored_jsid, stored_jobs = decode_reboot_ran(raw)
            self._observe_reboot_ran(stored_jsid, stored_jobs)
            # re-persist any local marks the store has not yet recorded (a
            # failed eager _persist_reboot_ran is retried here).
            if self._reboot_ran_local - self._reboot_ran:
                await self._write_reboot_ran()
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as ex:
            logger.debug("cluster: etcd reboot-ran sync failed: %s", ex)

    async def _write_reboot_ran(self) -> None:  # pragma: no cover - network
        combined = self._reboot_ran | self._reboot_ran_local
        value = encode_reboot_ran(self.get_job_set_id(), combined)
        await asyncio.wait_for(
            self._post(
                "/v3/kv/put",
                {"key": _b64(self.reboot_ran_key), "value": _b64(value)},
            ),
            self.connect_timeout,
        )

    async def _persist_reboot_ran(self) -> None:  # pragma: no cover - network
        # eager write on mark_reboot_ran (before the deferred job launches);
        # best-effort -- _sync_reboot_ran retries it each round if it fails.
        try:
            await self._write_reboot_ran()
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as ex:
            logger.debug("cluster: etcd reboot-ran persist failed: %s", ex)

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
            # failover (no waiting out the TTL). Bound the whole attempt by
            # connectTimeout: _post iterates every endpoint, so an unreachable
            # etcd at shutdown/reload time would otherwise stall the inline
            # start_stop_cluster reload for N_endpoints x connectTimeout. A
            # failed revoke is harmless -- the lease still expires by its TTL.
            try:
                await asyncio.wait_for(
                    self._post("/v3/lease/revoke", {"ID": self._lease_id}),
                    self.connect_timeout,
                )
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as ex:
                logger.debug("cluster: etcd lease revoke failed: %s", ex)
            self._lease_id = None
        if self._session is not None:
            await self._session.close()
            self._session = None
        self._is_leader = False
