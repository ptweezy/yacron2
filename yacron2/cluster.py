"""Peer attestation: confirm a static set of peers run the same job set.

Each instance is configured with a list of peer ``host:port`` addresses and a
mutual-TLS identity (a cluster CA plus this node's certificate/key).  It serves
a tiny ``GET /peer`` endpoint on a dedicated mTLS listener, and periodically
polls every configured peer's ``/peer`` over mTLS to compare job-set ids (see
:mod:`yacron2.fingerprint`).

The trust model is deliberately simple and keeps no shared state:

* **mTLS is the membership boundary.**  A peer's certificate must chain to the
  configured cluster CA, and (client side) match the host we connected to, so
  only nodes the CA vouches for are ever attested.  Standard TLS hostname
  verification gives us that SAN pinning for free.
* **Each node keeps its own view.**  ``ClusterView`` is just this node's table
  of what it last observed per peer; two healthy nodes converge to the same
  picture, and any disagreement is itself the signal.  Nobody is authoritative.
* **Drift is debounced.**  A reachable peer whose id differs is only reported
  as ``drifted`` after ``driftAfter`` consecutive rounds, so a rolling deploy
  (a transient, legitimate mismatch) does not raise a false alarm.

When ``electLeader`` is set, the same attestation drives a **quorum-gated
leader election** (see :func:`elect_leader`): each node independently elects,
as leader, the lowest ``nodeName`` among the *agreeing* members it can see, but
only if that set is a strict majority (a *quorum*) of the configured cluster.
Only the leader runs scheduled jobs, so replicas deployed from one config no
longer double-run.  The quorum gate is what makes this safe with no shared
state: two strict majorities of N cannot be disjoint, so under a clean
partition at most one side is quorate and at most one leader exists.  The
trade-off is liveness: a minority partition deliberately goes idle rather than
risk a second leader, and because the view is only as fresh as the last poll,
the guarantee is best-effort across membership changes (a brief window after a
leader dies may skip a firing; asymmetric/flapping reachability may briefly
double-elect).  It is *not* a fenced, exactly-once guarantee; for that you
would need a lease/consensus store, which this design intentionally avoids.

When ``distribution`` is ``"spread"`` the single elected leader is replaced by
**per-job ownership** via rendezvous (highest-random-weight) hashing (see
:func:`elect_job_owner`): each job is independently assigned to one member
of the quorate set, so leader-gated work fans out roughly evenly across the
cluster instead of piling onto one node.  This is purely a load optimization:
it keeps
the same quorum gate and therefore the same safety guarantee (under a clean
partition all quorate nodes see the same member set and compute the same owner
for each job, so still at most one node runs it).
"""

import asyncio
import datetime
import hashlib
import logging
import ssl
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional

import aiohttp
from aiohttp import web

from yacron2.config import ClusterConfig
from yacron2.fingerprint import SCHEME_VERSION

logger = logging.getLogger("yacron2.cluster")

# Per-peer status, as reported in the /cluster view.
STATUS_UNKNOWN = "unknown"  # not yet contacted
STATUS_SELF = "self"  # the peer reported our own node name
STATUS_AGREED = "agreed"  # reachable, same job-set id
STATUS_SYNCING = "syncing"  # reachable, id differs but within driftAfter
STATUS_DRIFTED = "drifted"  # reachable, id has differed >= driftAfter rounds
STATUS_UNREACHABLE = "unreachable"  # connect/timeout failure
STATUS_UNTRUSTED = "untrusted"  # TLS/cert verification failed


def quorum_size(cluster_size: int) -> int:
    """The strict majority of ``cluster_size`` nodes.

    A quorum requires *more than half* the cluster, so no two quorums can be
    disjoint; that is the property the leader gate relies on for safety.  Note
    this favours odd cluster sizes: N=3 and N=4 both need 3 and both tolerate
    only one failure, so the even node buys nothing.
    """
    return cluster_size // 2 + 1


def elect_leader(
    node_name: str,
    agreeing_peer_names: Iterable[str],
    cluster_size: int,
) -> Optional[str]:
    """Pure, deterministic leader election from one node's point of view.

    The *live set* is this node plus every peer it currently sees agreeing on
    the job-set id.  If that set is at least a quorum of ``cluster_size`` the
    leader is its lowest ``nodeName`` (so every node in one quorum elects the
    same single leader); otherwise there is no leader and ``None`` is returned,
    which is how a minority partition is made to stand down.
    """
    live = [node_name, *agreeing_peer_names]
    if len(live) < quorum_size(cluster_size):
        return None
    return min(live)


def elect_available_leader(
    node_name: str,
    agreeing_peer_names: Iterable[str],
) -> str:
    """Leaderless election *without* the quorum gate (favours liveness).

    Returns the lowest ``nodeName`` among this node and the peers it sees
    agreeing — and, since this node is always in that set, it always returns a
    name (never ``None``).  Dropping the quorum requirement means a node
    isolated from the rest still elects itself and runs, so a job never skips
    while any node is up; the price is that two sides of a partition may each
    elect their own leader and double-run.  Used by the ``PreferLeader`` job
    policy; contrast :func:`elect_leader`.
    """
    return min([node_name, *agreeing_peer_names])


def _hrw_score(job_name: str, node_name: str) -> int:
    """Rendezvous (highest-random-weight) score for one (job, node) pair.

    A stable hash of ``job_name`` + ``node_name``: deterministic across nodes
    and processes (so every node computes the same scores), and well-mixed, so
    different jobs favour different nodes.  Only the *ordering* of scores
    matters, not their magnitude.
    """
    digest = hashlib.sha256(
        job_name.encode("utf-8") + b"\x00" + node_name.encode("utf-8")
    ).digest()
    return int.from_bytes(digest[:8], "big")


def _hrw_owner(job_name: str, members: List[str]) -> str:
    """The rendezvous winner for ``job_name`` among ``members``.

    The member with the highest score owns the job; ties (astronomically
    unlikely with a 64-bit score) break on the node name so the choice stays
    deterministic.  This is what spreads jobs ~evenly and, crucially, only
    reassigns a leaving/joining node's *own* share on a membership change
    (the defining property of rendezvous hashing) rather than reshuffling
    everything the way ``hash % N`` would.
    """
    return max(members, key=lambda n: (_hrw_score(job_name, n), n))


def elect_job_owner(
    job_name: str,
    node_name: str,
    agreeing_peer_names: Iterable[str],
    cluster_size: int,
) -> Optional[str]:
    """Quorum-gated per-job owner (the ``distribution: spread`` analogue of
    :func:`elect_leader`).

    The live set is this node plus the peers it sees agreeing.  If that set is
    at least a quorum of ``cluster_size`` the owner is its rendezvous winner
    for ``job_name`` (so every node in one quorum picks the same owner); else
    ``None`` is returned, which is how a minority partition is made to stand
    down, exactly as in :func:`elect_leader`, just per job.
    """
    live = [node_name, *agreeing_peer_names]
    if len(live) < quorum_size(cluster_size):
        return None
    return _hrw_owner(job_name, live)


def elect_available_job_owner(
    job_name: str,
    node_name: str,
    agreeing_peer_names: Iterable[str],
) -> str:
    """Per-job owner *without* the quorum gate (spread-mode ``PreferLeader``).

    The rendezvous winner among this node and the peers it sees agreeing.  As
    with :func:`elect_available_leader`, this node is always a candidate, so a
    value is always returned (never ``None``): an isolated node owns all its
    jobs and never skips, at the cost of a possible double-run on partition.
    """
    return _hrw_owner(job_name, [node_name, *agreeing_peer_names])


@dataclass
class PeerState:
    """This node's last observation of one configured peer."""

    host: str
    status: str = STATUS_UNKNOWN
    job_set_id: Optional[str] = None  # peer's last-reported id
    node_name: Optional[str] = None  # peer's last-reported node name
    last_seen: Optional[datetime.datetime] = None  # last successful contact
    last_error: Optional[str] = None
    # consecutive reachable-but-mismatched rounds, for the drift hysteresis
    mismatch_streak: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "host": self.host,
            "status": self.status,
            "job_set_id": self.job_set_id,
            "node_name": self.node_name,
            "last_seen": (
                self.last_seen.isoformat()
                if self.last_seen is not None
                else None
            ),
            "last_error": self.last_error,
            "mismatch_streak": self.mismatch_streak,
        }


class ClusterView:
    """This node's peer table and the rules that update it.

    Pure (no I/O): the networking layer feeds it observations and reads back
    the table, which keeps the drift/state logic trivially testable.
    """

    def __init__(self, hosts: List[str], drift_after: int) -> None:
        self.drift_after = drift_after
        # preserve configured order for a stable view
        self.peers: "Dict[str, PeerState]" = {
            host: PeerState(host=host) for host in hosts
        }

    def record_success(
        self,
        host: str,
        peer_name: Optional[str],
        peer_id: Optional[str],
        peer_scheme: Optional[str],
        my_id: str,
        now: datetime.datetime,
        my_name: str,
    ) -> None:
        peer = self.peers[host]
        peer.last_seen = now
        peer.last_error = None
        peer.job_set_id = peer_id
        peer.node_name = peer_name

        if peer_name is not None and peer_name == my_name:
            # the peer recognised itself: an operator listed this node's own
            # address. Not a real peer, so it never counts toward agreement.
            peer.status = STATUS_SELF
            peer.mismatch_streak = 0
            return

        if peer_scheme is not None and peer_scheme != SCHEME_VERSION:
            # different fingerprint scheme: the ids are not comparable, so this
            # is a (non-debounced) disagreement rather than transient skew.
            peer.status = STATUS_DRIFTED
            peer.last_error = (
                "fingerprint scheme mismatch: {!r} != {!r}".format(
                    peer_scheme, SCHEME_VERSION
                )
            )
            return

        if peer_id == my_id:
            peer.status = STATUS_AGREED
            peer.mismatch_streak = 0
        else:
            # debounce: a mismatch is "syncing" until it persists, so a rolling
            # deploy does not immediately read as drift.
            peer.mismatch_streak += 1
            peer.status = (
                STATUS_DRIFTED
                if peer.mismatch_streak >= self.drift_after
                else STATUS_SYNCING
            )

    def record_failure(
        self, host: str, error: str, *, untrusted: bool
    ) -> None:
        peer = self.peers[host]
        peer.last_error = error
        peer.status = STATUS_UNTRUSTED if untrusted else STATUS_UNREACHABLE
        # we could not observe the id this round, so the drift streak (which
        # only counts *reachable* mismatches) is reset.
        peer.mismatch_streak = 0

    def to_list(self) -> List[Dict[str, Any]]:
        return [peer.to_dict() for peer in self.peers.values()]


def build_client_ssl_context(tls: Dict[str, str]) -> ssl.SSLContext:
    """Client context: verify peer certs vs the CA, pin the hostname."""
    ctx = ssl.create_default_context(cafile=tls["ca"])
    ctx.load_cert_chain(tls["cert"], tls["key"])
    # create_default_context already sets check_hostname=True and
    # verify_mode=CERT_REQUIRED for the client purpose; be explicit anyway.
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


def build_server_ssl_context(tls: Dict[str, str]) -> ssl.SSLContext:
    """Server context: require and verify a CA-signed client cert (mTLS)."""
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH, cafile=tls["ca"])
    ctx.load_cert_chain(tls["cert"], tls["key"])
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


def _split_host_port(addr: str) -> "tuple[str, int]":
    host, _, port = addr.rpartition(":")
    if not host or not port:
        raise ValueError("expected host:port, got {!r}".format(addr))
    return host, int(port)


class ClusterManager:
    """Owns the mTLS ``/peer`` listener and the periodic peer-poll loop."""

    def __init__(
        self,
        config: ClusterConfig,
        get_job_set_id: Callable[[], str],
    ) -> None:
        self.config = config
        self.get_job_set_id = get_job_set_id
        self.node_name: str = config["nodeName"]
        # "single-leader" (one leader runs all Leader jobs) or "spread"
        # (per-job ownership via rendezvous hashing); see _cluster_allows.
        self.distribution: str = config.get("distribution", "single-leader")
        self.view = ClusterView(
            [peer["host"] for peer in config["peers"]],
            config["driftAfter"],
        )
        self._client_ssl = build_client_ssl_context(config["tls"])
        self._server_ssl = build_server_ssl_context(config["tls"])
        self._runner: Optional[web.AppRunner] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    # --- the mTLS /peer server -------------------------------------------

    async def _handle_peer(self, request: web.Request) -> web.Response:
        return web.json_response(
            {
                "node_name": self.node_name,
                "job_set_id": self.get_job_set_id(),
                "scheme_version": SCHEME_VERSION,
            }
        )

    async def start(self) -> None:
        app = web.Application()
        app.add_routes([web.get("/peer", self._handle_peer)])
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        host, port = _split_host_port(self.config["listen"])
        site = web.TCPSite(
            self._runner, host, port, ssl_context=self._server_ssl
        )
        await site.start()
        logger.info(
            "cluster: node %r serving mTLS /peer on %s, polling %d peer(s) "
            "every %ds",
            self.node_name,
            self.config["listen"],
            len(self.config["peers"]),
            self.config["interval"],
        )
        self._poll_task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        self._stop.set()
        if self._poll_task is not None:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    # --- the peer-poll loop ----------------------------------------------

    async def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._poll_all()
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive
                logger.exception("cluster: unexpected error in poll loop")
            try:
                await asyncio.wait_for(
                    self._stop.wait(), self.config["interval"]
                )
            except asyncio.TimeoutError:
                pass

    async def _poll_all(self) -> None:
        my_id = self.get_job_set_id()
        timeout = aiohttp.ClientTimeout(total=self.config["connectTimeout"])
        async with aiohttp.ClientSession(timeout=timeout) as session:
            await asyncio.gather(
                *(
                    self._poll_peer(session, peer["host"], my_id)
                    for peer in self.config["peers"]
                )
            )

    async def _poll_peer(
        self, session: aiohttp.ClientSession, host: str, my_id: str
    ) -> None:
        url = "https://{}/peer".format(host)
        now = datetime.datetime.now(datetime.timezone.utc)
        try:
            async with session.get(url, ssl=self._client_ssl) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientSSLError as ex:
            # cert chain / hostname verification failure: the peer is not (or
            # not provably) a cluster member.
            self.view.record_failure(host, str(ex), untrusted=True)
            return
        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            OSError,
        ) as ex:
            self.view.record_failure(host, str(ex), untrusted=False)
            return
        self.view.record_success(
            host,
            data.get("node_name"),
            data.get("job_set_id"),
            data.get("scheme_version"),
            my_id,
            now,
            self.node_name,
        )

    # --- leader election --------------------------------------------------

    def cluster_size(self) -> int:
        """Total number of cluster members.

        ``peers`` lists every *other* member, so the cluster is those plus this
        node.  (Listing this node in its own peer list is a misconfiguration;
        it is reported as ``self`` and never counts toward agreement, so it
        only makes quorum harder to reach, never easier.)
        """
        return len(self.config["peers"]) + 1

    def quorum(self) -> int:
        return quorum_size(self.cluster_size())

    def _agreeing_peer_names(self) -> List[str]:
        """Names of peers currently agreeing on our job-set id over mTLS."""
        return [
            peer.node_name
            for peer in self.view.peers.values()
            if peer.status == STATUS_AGREED and peer.node_name is not None
        ]

    def leader_name(self) -> Optional[str]:
        """Elected leader as this node sees it, or ``None`` if not quorate."""
        return elect_leader(
            self.node_name, self._agreeing_peer_names(), self.cluster_size()
        )

    def is_leader(self) -> bool:
        """Whether this node is the elected leader (quorate, lowest name)."""
        return self.leader_name() == self.node_name

    def available_leader_name(self) -> str:
        """Elected leader ignoring quorum (for the ``PreferLeader`` policy)."""
        return elect_available_leader(
            self.node_name, self._agreeing_peer_names()
        )

    def is_available_leader(self) -> bool:
        """Whether this node leads its reachable set, quorum or not."""
        return self.available_leader_name() == self.node_name

    def is_quorate(self) -> bool:
        """Whether this node currently sees a quorum (so it may run jobs)."""
        return self.leader_name() is not None

    # --- per-job ownership (distribution: spread) -------------------------

    def job_owner(self, job_name: str) -> Optional[str]:
        """Quorum-gated owner of ``job_name`` (spread mode), else ``None``."""
        return elect_job_owner(
            job_name,
            self.node_name,
            self._agreeing_peer_names(),
            self.cluster_size(),
        )

    def is_job_owner(self, job_name: str) -> bool:
        """Whether this node owns ``job_name`` (quorate, rendezvous winner)."""
        return self.job_owner(job_name) == self.node_name

    def available_job_owner(self, job_name: str) -> str:
        """Owner of ``job_name`` ignoring quorum (spread ``PreferLeader``)."""
        return elect_available_job_owner(
            job_name, self.node_name, self._agreeing_peer_names()
        )

    def is_available_job_owner(self, job_name: str) -> bool:
        """Whether this node owns ``job_name`` in its reachable set."""
        return self.available_job_owner(job_name) == self.node_name

    def view_dict(self) -> Dict[str, Any]:
        leader = self.leader_name()
        spread = self.distribution == "spread"
        return {
            "node_name": self.node_name,
            "job_set_id": self.get_job_set_id(),
            "cluster_size": self.cluster_size(),
            "quorum": self.quorum(),
            "elect_leader": bool(self.config.get("electLeader")),
            "distribution": self.distribution,
            "quorate": leader is not None,
            # In spread mode there is no single leader: ownership is per job,
            # so leader/is_leader are not meaningful (reported null/false).
            "leader": None if spread else leader,
            "is_leader": (
                False
                if spread
                else (leader is not None and leader == self.node_name)
            ),
            "peers": self.view.to_list(),
        }
