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
dependency and, by avoiding grpc/protobuf, keeps cronstable's wide architecture
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
import os
import ssl
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp

from cronstable.config import ClusterConfig, _redact_userinfo
from cronstable.leadership import (
    LeaseBackend,
    decode_reboot_ran,
    encode_reboot_ran,
)

logger = logging.getLogger("cronstable.backends.etcd")

# See cronstable.backends.kubernetes._CLOCK_SKEW.
_CLOCK_SKEW = datetime.timedelta(seconds=1)
_SKEW_SECONDS = _CLOCK_SKEW.total_seconds()

# The smallest *effective* lease ttl that still leaves a usable leader window.
# The fence is (effective_ttl - clock skew), so below this the window collapses
# toward zero and a node that wins the campaign immediately self-demotes
# (Leader -> at-most-zero: fail-closed/safe, but no Leader job runs on this
# node). config.py rejects a CONFIGURED ttl below this; etcd can still GRANT or
# keepalive a shorter one, which the backend honours for the fence -- never
# inflating it back up (which would keep is_leader() True past the real server
# lease and let a second node win the freed key) -- while warning. Kept in sync
# with config.py's etcd ttl floor.
_MIN_USABLE_TTL = 3

# Worst-case number of sequential HTTP POSTs a single renew *cycle* can make,
# each of which may waste a full per-request timeout on a half-open endpoint
# before failing over: keepalive (or grant), campaign, then the reboot-ran
# range + CAS txn, plus the re-grant a lease-expiry round adds. The per-request
# timeout is sized off this (see EtcdBackend.request_timeout) so the whole
# cycle -- and so the gap between two successful contacts -- stays inside the
# lease window even when one endpoint is slow.
_ETCD_POSTS_PER_CYCLE = 5

# Bounded retries for the @reboot-ran compare-and-swap (see
# _cas_write_reboot_ran): a contended key is re-read and re-merged so two
# concurrent writers UNION their marks instead of last-writer-wins clobbering.
_REBOOT_RAN_CAS_ATTEMPTS = 3

# Reported as the holder when we lost a campaign (so a real holder exists) but
# its identity could not be parsed from the txn response -- only reachable via
# a non-conformant gateway that drops the failure-branch range value. Reporting
# a non-None holder keeps leader_name() non-None so a quorate follower defers
# its PreferLeader jobs (is_available_leader stays False) instead of reading
# "holder unknown" as "run anyway" and double-running fleet-wide. See
# _apply_round.
_UNKNOWN_HOLDER = "<unknown holder>"


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


def _file_signature(path: str) -> Optional[Tuple[int, int]]:
    """A cheap ``(st_mtime_ns, st_size)`` fingerprint of one file, or ``None``.

    Mirrors the kubernetes backend's helper: ``os.stat`` follows symlinks, so
    the atomic symlink swap cert-manager / a projected secret uses is picked
    up too.  A stat error (a file briefly absent mid-rotation) records ``None``
    and simply compares unequal once the file is back -- the safe direction (a
    spurious rebuild, never a missed one).
    """
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (st.st_mtime_ns, st.st_size)


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


def build_reboot_ran_cas_txn(
    key: str, value: str, mod_revision: str
) -> Dict[str, Any]:
    """A compare-and-swap ``put`` on the @reboot-ran key.

    Puts ``value`` only if the key's ``MOD`` revision still equals the one we
    read this round (``mod_revision``; ``"0"`` matches an absent key, as in
    :func:`build_campaign_txn`).  If another node wrote the key in between the
    compare fails and the caller re-reads + re-merges + retries, so two
    concurrent writers UNION their @reboot-ran marks rather than the later
    blind ``put`` silently dropping the earlier writer's mark (a lost update).
    """
    encoded = _b64(key)
    return {
        "compare": [
            {
                "key": encoded,
                "result": "EQUAL",
                "target": "MOD",
                "mod_revision": str(mod_revision),
            }
        ],
        "success": [{"requestPut": {"key": encoded, "value": _b64(value)}}],
        "failure": [],
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
        # configured value and is narrowed by each grant/keepalive (see
        # _narrow_effective_ttl).
        self._effective_ttl: int = self.ttl
        # whether the effective ttl is currently below _MIN_USABLE_TTL, so the
        # warning/recovery in _narrow_effective_ttl is logged once per
        # transition rather than every renew round.
        self._ttl_collapsed: bool = False
        self.username: Optional[str] = etcd["username"]
        self.password: Optional[str] = etcd.get("resolved_password")
        self._tls: Dict[str, Optional[str]] = etcd["tls"]
        # on-disk client-TLS files (ca / cert / key) the SSLContext is built
        # from. Snapshotted in start() so an in-place cert/CA rotation
        # (cert-manager / Vault: same paths, new bytes) is detected by
        # tls_files_changed() and the backend rebuilt via start_stop_cluster --
        # the context is built once in start() and never reloaded, so without
        # this a rotated client cert/CA silently and permanently loses
        # leadership fleet-wide once the old cert expires. Mirrors the
        # kubernetes and gossip backends. Empty (-> tls_files_changed False)
        # for plain-http endpoints: nothing on disk to rotate.
        self._tls_files: List[str] = []
        self._tls_signature: Dict[str, Optional[Tuple[int, int]]] = {}
        self.connect_timeout: int = config["connectTimeout"]
        # renew_period / round_deadline / request_timeout are derived from the
        # *effective* ttl (properties below), so they tighten automatically if
        # etcd grants a shorter lease than requested.
        # rotates the endpoint probe order each round (see _post), so a single
        # persistently slow/half-open endpoint is not always tried first.
        self._endpoint_offset = 0

        # live state, written by the renew loop and read by the sync methods
        self._is_leader = False
        # set when a keepalive round finds our lease already gone (ttl<=0)
        # before the re-grant/re-campaign lands: forces is_leader() closed at
        # once (the local fence must not be trusted past a KNOWN lease loss)
        # WITHOUT clearing _is_leader, so _is_self_demoted_holder() stays True
        # and a never-skip PreferLeader job keeps running on this former holder
        # this cycle rather than dropping to zero-run if the re-grant/re-
        # campaign raises before _apply_round can run. Cleared by _apply_round
        # (the single writer of leadership state) once a round re-establishes
        # it.
        self._lease_lost = False
        self._holder: Optional[str] = None
        self._lease_id: Optional[str] = None
        # wall-clock expiry, for the dashboard/lease_detail display ONLY
        self._lease_deadline: Optional[datetime.datetime] = None
        # monotonic deadlines: the load-bearing fence/freshness gates (immune
        # wall-clock steps; see _monotonic)
        self._lease_deadline_mono: Optional[float] = None
        # quorum freshness deadline, FIXED at each successful round's contact
        # instant (contact + the effective ttl in effect at THAT contact; see
        # _apply_round). A deadline -- not a live contact + current-ttl
        # computation -- so the not-quorate cadence widening in _renew_once
        # cannot retroactively resurrect is_quorate() with zero store contact.
        self._quorum_deadline_mono: Optional[float] = None

        self._auth_token: Optional[str] = None
        self._ssl: Optional[ssl.SSLContext] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    # --- derived renew cadence (all track the *effective* lease ttl) ------

    @property
    def renew_period(self) -> float:
        """Sleep between renew rounds (the keepalive cadence).

        Tracks the *effective* ttl (etcd may grant a shorter lease than
        requested), so the cadence follows the real lease window.
        """
        return max(1.0, self._effective_ttl / 3)

    @property
    def round_deadline(self) -> float:
        """Wall-time bound on a single renew round.

        Mirrors the Kubernetes backend's ``renewDeadline``: a round is
        abandoned (and retried) before the lease window can close, leaving room
        for the inter-round :attr:`renew_period` sleep *inside* the effective
        ttl.  While the effective ttl is at or above ``_MIN_USABLE_TTL``,
        ``round_deadline + renew_period <= effective_ttl - clock_skew`` by
        construction, so the gap between two successive successful contacts
        stays within the lease window and a slow round cannot make the holder
        lapse out of its own lease -- the at-most-once -> at-most-zero collapse
        the Kubernetes ``renew + retry < duration`` invariant guards against.
        (At a smaller server-granted ttl the 1s floors here can exceed that
        budget, but the fence has by then already collapsed to ~zero so the
        node is not leading anyway -- fail-closed, never two leaders; see
        :meth:`_narrow_effective_ttl`.)  :attr:`request_timeout` is in turn
        derived from this round deadline (no absolute floor), so the per-cycle
        POST budget always fits regardless of ttl.
        """
        return max(
            1.0, self._effective_ttl - self.renew_period - _SKEW_SECONDS
        )

    @property
    def request_timeout(self) -> float:
        """Per-endpoint request timeout for a renew POST.

        Sized so a round's sequential lease POSTs still fit
        :attr:`round_deadline` when one endpoint is half-open and wastes a full
        attempt before failing over to a healthy peer.  Previously each POST
        was bounded only by the much larger ``connectTimeout``, so on the
        default ttl a single slow-but-quorate endpoint made every round overrun
        the lease window and the leader self-demote every cycle.

        It is exactly ``round_deadline / _ETCD_POSTS_PER_CYCLE`` (capped at
        ``connectTimeout`` so an explicitly lower value still applies), with NO
        absolute floor: an earlier ``max(0.5, ...)`` floor broke the invariant
        at the minimum ttl (and whenever etcd grants a ttl smaller than
        configured), letting 5 sequential 0.5s POSTs overrun a ~1s round
        deadline and self-demote a healthy leader each cycle. Without the floor
        the budget always holds (5 x request_timeout == round_deadline), at the
        cost of a tighter per-POST timeout at very small ttl -- which is the
        operator's explicit aggressive choice.  Two safety nets bound that
        cost: while a node is not quorate the cadence is widened back to the
        configured ttl (see :meth:`_renew_once`), so a once-narrowed effective
        ttl cannot wedge the per-POST budget below a reconnectable value
        indefinitely; and a configured ttl whose derived budget is below a
        realistic round-trip is warned about at load time (see
        :func:`cronstable.config.cluster_config_warnings`).
        """
        return min(
            float(self.connect_timeout),
            self.round_deadline / _ETCD_POSTS_PER_CYCLE,
        )

    # --- pure local-state reads (no I/O) ---------------------------------

    def _leader_deadline(self, now: datetime.datetime) -> datetime.datetime:
        """Wall-clock lease expiry, for display only (see ``_apply_round``)."""
        return (
            now + datetime.timedelta(seconds=self._effective_ttl) - _CLOCK_SKEW
        )

    def is_leader(self) -> bool:
        if (
            not self._is_leader
            or self._lease_deadline_mono is None
            # a keepalive round that found the lease gone (ttl<=0) fences us
            # closed immediately: the old monotonic deadline may still be in
            # the future, but etcd has already freed the key, so trusting it
            # would let a second node win it (two leaders). See _lease_lost.
            or self._lease_lost
        ):
            return False
        # gated on a MONOTONIC deadline so a backward wall-clock step cannot
        # keep us "leader" past the point etcd has expired the key.
        return _monotonic() < self._lease_deadline_mono

    def _is_self_demoted_holder(self) -> bool:
        # raw win flag still set (we won the campaign and have not observed a
        # loss) but the monotonic fence has lapsed -- the brief self-demotion
        # window. See LeadershipBackend._is_self_demoted_holder.
        return self._is_leader and not self.is_leader()

    def leader_name(self) -> Optional[str]:
        if not self.is_quorate():
            return None
        return self._holder

    def is_quorate(self) -> bool:
        if self._quorum_deadline_mono is None:
            return False
        # The deadline was FIXED when the last successful round landed (see
        # _apply_round): recomputing it live from the mutable _effective_ttl
        # would let the not-quorate cadence widening in _renew_once (narrowed
        # server ttl -> configured ttl) re-extend an already-lapsed freshness
        # window around the OLD contact, flipping this back to True with zero
        # store contact and re-deferring never-skip PreferLeader jobs to a
        # possibly-dead stale holder.
        return _monotonic() < self._quorum_deadline_mono

    def lease_detail(self) -> Dict[str, Any]:
        from cronstable.backends.kubernetes import _format_microtime

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

    def _narrow_effective_ttl(self, ttl: int) -> None:
        """Adopt a server-granted/keepalived ttl for the cadence and fence.

        etcd may grant or refresh a lease ttl below the requested one; honour
        it so the local fence never outlives the real server lease.  We do NOT
        floor it back up to the configured minimum: inflating the fence past
        the server's lease would keep :meth:`is_leader` ``True`` after etcd has
        freed the key, letting a second node win it (two leaders).  When the
        honoured ttl is too small to leave a usable leader window (below
        ``_MIN_USABLE_TTL``) the fence collapses to ~zero and Leader jobs fail
        closed on this node -- safe, but otherwise silent -- so surface a
        warning once per transition (and a recovery once it returns).
        """
        effective = max(1, min(self.ttl, ttl))
        usable = effective >= _MIN_USABLE_TTL
        if not usable and not self._ttl_collapsed:
            logger.warning(
                "cluster: etcd granted a lease ttl of %ds, below the %ds "
                "needed for a usable leader window; Leader jobs will not run "
                "on this node until etcd returns a larger ttl (check etcd's "
                "--min-lease-ttl and load)",
                effective,
                _MIN_USABLE_TTL,
            )
        elif usable and self._ttl_collapsed:
            logger.info(
                "cluster: etcd lease ttl recovered to %ds; leader window "
                "restored",
                effective,
            )
        self._ttl_collapsed = not usable
        self._effective_ttl = effective

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
        quorum freshness deadline (``is_quorate``) is fixed from, captured at
        round END -- the safe (fresher) direction for freshness.
        ``lease_mono`` is a monotonic instant captured just BEFORE the
        lease-renewing keepalive/grant POST is sent -- a guaranteed lower
        bound on when the server reset our lease's TTL; the leadership FENCE
        is anchored to *that*, never to the later round-end ``mono``, so
        neither the keepalive/grant round-trip nor a slow campaign can push
        our local ``is_leader`` deadline past the server's lease expiry and
        let a second node win the freed key (two leaders).
        Both default to the current monotonic clock for the pure unit tests.
        """
        if mono is None:
            mono = _monotonic()
        # Fix the quorum freshness deadline HERE, from the effective ttl in
        # effect at THIS contact. It must never be recomputed later from the
        # mutable _effective_ttl (see is_quorate): only a successful round may
        # extend it, so no post-hoc ttl change can move an established
        # deadline.
        self._quorum_deadline_mono = mono + self._effective_ttl
        if holder is None and not is_leader:
            # We lost the campaign (a holder exists, bound to another node's
            # lease) but its identity was unparseable -- only reachable via a
            # non-conformant gateway that dropped the failure-branch range
            # value. Reporting holder=None would make leader_name() None, which
            # is_available_leader() reads as "holder unknown -> run anyway",
            # double-running each quorate replica's PreferLeader jobs alongside
            # the real holder. Fence closed instead: keep the last known holder
            # (or a sentinel) so leader_name() stays non-None and followers
            # defer. (A genuinely absent key makes the campaign SUCCEED, so
            # is_leader is True there and this branch is not taken.)
            holder = self._holder or _UNKNOWN_HOLDER
        self._holder = holder
        self._is_leader = is_leader
        # A completed round re-establishes leadership state, so clear the
        # mid-round lease-lost flag (see is_leader / _renew_once): if we
        # re-acquired, is_leader() trusts the fresh fence below again; if we
        # lost the campaign, _is_leader is now False and the flag is moot.
        self._lease_lost = False
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
        self._record_tls_files()
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
                await asyncio.wait_for(self._renew_once(), self.round_deadline)
            except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                OSError,
                # ValueError covers the binascii.Error a base64 decode of a
                # malformed election-key value (from a non-conformant gateway)
                # raises in _campaign, OUTSIDE _post's catch. The renew LOOP
                # already swallows it via its broad `except Exception`; the
                # initial round must too, or it hits `except BaseException`
                # below and aborts the whole manager start -- wedging leader
                # gating off until the next reload. Keeps start() best-effort.
                ValueError,
                # belt-and-suspenders for a non-conformant gateway whose body
                # parses as JSON but is the wrong SHAPE (a nested non-dict the
                # top-level _post dict guard does not reach): a parser's
                # resp.get() then raises AttributeError/TypeError, which must
                # not abort the manager start either.
                AttributeError,
                TypeError,
            ) as ex:
                logger.warning("cluster: etcd initial round failed: %s", ex)
            logger.info(
                "cluster: etcd backend, identity %r, election key %r, "
                "ttl %ds, endpoints %s",
                self.identity,
                self.election_name,
                self.ttl,
                # redact any userinfo (config rejects it, never log a secret).
                ", ".join(_redact_userinfo(e) for e in self.endpoints),
            )
            self._stop.clear()
            # create the renew task INSIDE the try so a failure here is cleaned
            # up like any other -- it must not leak the open session/task.
            self._task = asyncio.create_task(self._renew_loop())
        except BaseException:
            # Honour the "a backend cleans up its own half-started state on
            # failure" contract (as KubernetesBackend.start already does): an
            # unreachable etcd or a rejected credential raises here; without
            # this the open ClientSession/connector leaks -- one per reload --
            # and is never closed (start() never returns, so the caller never
            # stores the manager to stop() it). BaseException also covers a
            # cancellation mid-handshake.
            if self._task is not None:
                self._task.cancel()
                self._task = None
            await self._session.close()
            self._session = None
            raise

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

    def _tls_file_paths(self) -> List[str]:
        """The on-disk client-TLS files in use (empty for plain-http only).

        Only meaningful when at least one endpoint is https -- ``_build_ssl``
        builds no context otherwise, so there is nothing on disk to rotate.
        """
        if not any(self.endpoint_is_https(e) for e in self.endpoints):
            return []
        paths: List[str] = []
        for key in ("ca", "cert", "key"):
            value = self._tls.get(key)
            if value:
                paths.append(value)
        return paths

    def _record_tls_files(self) -> None:  # pragma: no cover - file stat
        """Snapshot the on-disk client-TLS files the SSLContext was built from.

        Called from :meth:`start` right after ``_build_ssl`` so
        :meth:`tls_files_changed` can later detect an in-place rotation.  No
        https endpoint (or no TLS material) leaves the snapshot empty, so
        :meth:`tls_files_changed` stays ``False``.
        """
        self._tls_files = self._tls_file_paths()
        self._tls_signature = {p: _file_signature(p) for p in self._tls_files}

    def tls_files_changed(self) -> bool:
        """Whether any tracked on-disk client-TLS file changed since ``start``.

        The SSLContext is built once in :meth:`start` and never reloaded, so --
        as for the gossip and kubernetes backends -- an in-place cert/CA
        rotation (same paths, new bytes from cert-manager / Vault) is otherwise
        invisible until the process restarts, and the fleet silently loses
        leadership once the old client cert expires.  Reporting the change lets
        :meth:`cronstable.cron.Cron.start_stop_cluster` rebuild this backend
        with
        the fresh material.  ``False`` when nothing was tracked (plain http, or
        no client cert/CA: nothing on disk to rotate).
        """
        if not self._tls_files:
            return False
        current = {p: _file_signature(p) for p in self._tls_files}
        return current != self._tls_signature

    def tls_files_loadable(self) -> bool:  # pragma: no cover - ssl file I/O
        """Dry-run-load the current on-disk client-TLS material.

        :meth:`cronstable.cron.Cron.start_stop_cluster` consults this before
        tearing the running backend down to apply a detected rotation: a
        cert-manager / Vault refresh is not atomic across ca/cert/key, so a
        reload can observe a half-written or briefly-absent file.  If the new
        material cannot be loaded yet, keep the running backend (still using
        the valid old context) and retry next reload, rather than rebuilding
        into a load failure that would leave no manager and wedge ``Leader`` /
        ``PreferLeader`` closed for up to a reload -- make-before-break, as the
        gossip backend does.  (Kubernetes inherits the always-``True`` default;
        etcd validates because ``_build_ssl`` loads the client chain eagerly,
        so a half-written key would otherwise abort the rebuild.)
        """
        try:
            self._build_ssl()
        except (OSError, ssl.SSLError):
            return False
        return True

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
        # Rotate the probe order each round (see _endpoint_offset) so a single
        # persistently slow/half-open endpoint is not always tried -- and timed
        # out on -- first, which would add request_timeout to every POST of
        # every round. Combined with the per-request timeout below this keeps
        # the worst-case round inside round_deadline.
        if len(endpoints) > 1:
            off = self._endpoint_offset % len(endpoints)
            endpoints = endpoints[off:] + endpoints[:off]
        # Bound EACH endpoint attempt (overriding the session-wide default), so
        # the round's sequential POSTs fail over fast and still fit the round
        # deadline when one endpoint is half-open; see request_timeout.
        timeout = aiohttp.ClientTimeout(total=self.request_timeout)
        last_error: Optional[Exception] = None
        for endpoint in endpoints:
            url = endpoint.rstrip("/") + path
            try:
                async with self._session.post(
                    url,
                    json=body,
                    ssl=self._ssl,
                    headers=headers,
                    timeout=timeout,
                    # never follow a redirect to an attacker-chosen target
                    # (SSRF) or a plaintext downgrade; a real etcd endpoint
                    # answers directly. Matches the gossip transport.
                    allow_redirects=False,
                ) as resp:
                    if resp.status == 401 and self.username and allow_reauth:
                        self._auth_token = await self._authenticate()
                        return await self._post(path, body, allow_reauth=False)
                    resp.raise_for_status()
                    data = await resp.json()
                    if not isinstance(data, dict):
                        # A 200 with a non-object body (null / list / scalar)
                        # from a non-conformant L7 proxy/gateway: every parser
                        # below assumes a dict and does resp.get(), so a bare
                        # list/scalar raises AttributeError in a parser.
                        # That escapes the network-only catch tuples (start()'s
                        # initial round, the eager persist) and either
                        # aborts manager start or is mislogged as an internal
                        # bug. Treat a non-object body as a failed endpoint so
                        # fails over, shown as a normal "etcd round failed"
                        # (trailing raise covers "every endpoint did this").
                        raise aiohttp.ClientError(
                            "non-object etcd response from {}".format(endpoint)
                        )
                    return data
            # A bad body from a misbehaving proxy/gateway in front of etcd
            # makes resp.json() raise a ValueError: json.JSONDecodeError on
            # invalid/truncated JSON, but ALSO UnicodeDecodeError when the body
            # declares a charset its bytes are not valid for (aiohttp decodes
            # before parsing). Both subclass ValueError, so catch ValueError to
            # cover the whole family (and the binascii.Error a base64 decode of
            # a malformed value can raise downstream). Treat it as a failed
            # endpoint like any transport error so it (a) tries the next
            # endpoint, (b) shows as a normal "etcd round failed" rather than
            # escaping the renew loop's network-error tuple and either killing
            # the scheduler (the eager reboot-ran persist path runs outside the
            # run-loop guard) or being mislogged as an unexpected internal bug.
            except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                OSError,
                ValueError,
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
                # Bound each round by round_deadline (< the effective ttl,
                # leaving room for the renew_period sleep inside the lease
                # window) rather than by the full ttl. _renew_once makes a few
                # sequential POSTs, each bounded by request_timeout; abandoning
                # at round_deadline and retrying keeps the gap between two
                # successful contacts inside the lease window, so a slow/half-
                # open-but-quorate endpoint cannot make the holder self-demote
                # every cycle (Leader to at-most-zero, PreferLeader doubles).
                await asyncio.wait_for(self._renew_once(), self.round_deadline)
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
        #
        # Advance the endpoint probe rotation for this round (see _post).
        self._endpoint_offset += 1
        # Recover a collapsed cadence: while we are NOT quorate the lease
        # window (and so the fence) has already lapsed -- is_leader() is False,
        # so there is no live fence to over-extend here (no two-leaders risk).
        # A previous round may have narrowed _effective_ttl to a small server-
        # granted value, which shrinks request_timeout/round_deadline; that
        # tight per-POST budget is itself what can then prevent the very
        # contact needed to observe a larger ttl again -- a self-sustaining
        # wedge that keeps a node off a since-recovered etcd until restart.
        # Widen the cadence back to the configured ttl so this round's
        # reconnect POSTs get the full budget; a successful keepalive/grant
        # re-narrows to whatever etcd actually grants, so the fence is never
        # computed from this widened value while we hold leadership. Quorum is
        # likewise immune: is_quorate() checks a deadline FIXED at the last
        # successful contact (see _apply_round), so this widening cannot
        # retroactively resurrect an already-expired quorum -- and so re-close
        # the never-skip PreferLeader gate -- without a real contact.
        if not self.is_quorate() and self._effective_ttl < self.ttl:
            self._effective_ttl = self.ttl
        lease_mono: Optional[float] = None
        if self._lease_id is not None:
            lease_mono = _monotonic()
            ttl = await self._keepalive(self._lease_id)
            if ttl is None or ttl <= 0:
                self._lease_id = None  # lease expired; re-grant below
                # Mark the lease lost rather than clearing _is_leader:
                # is_leader() gates on _lease_lost so it fences closed at once
                # (we must not trust the old monotonic deadline past a known
                # lease loss), but _is_leader stays True so
                # _is_self_demoted_holder() is True and a never-skip
                # PreferLeader job keeps running on this former holder this
                # cycle instead of dropping to zero-run if the re-grant/re-
                # campaign below raises before _apply_round runs.
                self._lease_lost = True
                lease_mono = None  # re-anchored before the grant POST below
            else:
                # honour the TTL etcd refreshed to (may be < requested)
                self._narrow_effective_ttl(ttl)
        if self._lease_id is None:
            lease_mono = _monotonic()
            self._lease_id, granted = await self._grant_lease()
            if granted is not None:
                self._narrow_effective_ttl(granted)
        if self._lease_id is None:
            raise aiohttp.ClientError("etcd lease grant returned no id")
        holder, won = await self._campaign(self._lease_id)
        # ``now``/``mono`` are sampled at round END -- the safe (fresher)
        # direction for the is_quorate deadline (_quorum_deadline_mono). The
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
        shot); the next round retries. ValueError is caught too so a malformed
        stored value (a non-UTF-8 / bad-base64 body from a misbehaving gateway)
        cannot escape -- this path is reached from start()'s initial round,
        which catches only the network tuple.
        """
        try:
            # drop our own marks first if the job set changed, so a redefined
            # @reboot one-shot is not re-suppressed by a stale local mark being
            # re-published under the new job-set id (see
            # LeaseBackend._reconcile_local_reboot_ran).
            self._reconcile_local_reboot_ran()
            await self._cas_write_reboot_ran()
        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            OSError,
            ValueError,
            # a wrong-shape gateway body that slips past _post's dict guard
            # (a nested non-dict) raises AttributeError/TypeError in a parser;
            # swallow it like any failed auxiliary round (the local set still
            # stops this node re-running its own one-shot; the next round
            # retries) so it cannot escape _sync_reboot_ran -- which start()'s
            # initial round runs.
            AttributeError,
            TypeError,
        ) as ex:
            logger.debug("cluster: etcd reboot-ran sync failed: %s", ex)

    async def _cas_write_reboot_ran(self) -> None:  # pragma: no cover
        """Read-modify-write the @reboot-ran key with optimistic concurrency.

        Reads the sibling key (and its ``mod_revision``), folds the stored set
        into our cache, then writes the union back in a txn guarded on that
        revision (see :func:`build_reboot_ran_cas_txn`). If a concurrent writer
        moved the key the compare fails, so we re-read, re-merge and retry --
        UNIONing both writers' marks instead of the blind ``put`` silently
        dropping the earlier one's (the lost update a plain put allowed). The
        write is skipped entirely when the store already holds everything we
        would write. Bounded retries; on persistent contention the local set
        still stops this node re-running its own one-shot and the next round
        retries.
        """
        for _attempt in range(_REBOOT_RAN_CAS_ATTEMPTS):
            resp = await self._post(
                "/v3/kv/range", {"key": _b64(self.reboot_ran_key)}
            )
            kvs = resp.get("kvs") or []
            if kvs and kvs[0].get("value") is not None:
                raw: Optional[str] = _b64decode(kvs[0]["value"])
                # accept BOTH wire spellings: the etcd gRPC-gateway JSON can
                # marshal multi-word KV fields as snake_case OR camelCase (the
                # same reason holder_from_txn_response/campaign_won accept
                # response_range AND responseRange). Reading only mod_revision
                # would fall back to "0" on a camelCase gateway, making the
                # MOD==0 compare against an EXISTING key never succeed, so the
                # CAS contends out and the @reboot-ran mark is never persisted
                # -- re-running a deferred one-shot after a failover.
                mod_revision = str(
                    kvs[0].get("mod_revision")
                    or kvs[0].get("modRevision")
                    or "0"
                )
            else:
                raw = None
                mod_revision = "0"  # absent key: compare MOD == 0 succeeds
            stored_jsid, stored_jobs = decode_reboot_ran(raw)
            self._observe_reboot_ran(stored_jsid, stored_jobs)
            combined = self._reboot_ran | self._reboot_ran_local
            if not (combined - self._reboot_ran):
                # the store already holds every mark we would write (after
                # folding in what we just read): nothing to persist.
                return
            value = encode_reboot_ran(self.get_job_set_id(), combined)
            result = await asyncio.wait_for(
                self._post(
                    "/v3/kv/txn",
                    build_reboot_ran_cas_txn(
                        self.reboot_ran_key, value, mod_revision
                    ),
                ),
                self.connect_timeout,
            )
            if result.get("succeeded"):
                return
            # lost the CAS race: another moved the key between our read and
            # write. Loop to re-read, re-merge, and retry so we union.
        logger.debug(
            "cluster: etcd reboot-ran CAS contended out after %d attempts",
            _REBOOT_RAN_CAS_ATTEMPTS,
        )

    async def _persist_reboot_ran(self) -> None:  # pragma: no cover - network
        # eager write on mark_reboot_ran (before the deferred job launches);
        # best-effort -- _sync_reboot_ran retries it each round if it fails.
        try:
            await self._cas_write_reboot_ran()
        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            OSError,
            ValueError,
            # see _sync_reboot_ran: a wrong-shape gateway body can raise
            # AttributeError/TypeError in a parser. Runs from cron's
            # _process_pending_reboots, OUTSIDE the run loop's guard, so must
            # never escape -- mirror _sync_reboot_ran's tuple exactly.
            AttributeError,
            TypeError,
        ) as ex:
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
