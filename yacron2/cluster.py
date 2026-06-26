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
* **Identities must be distinct.**  The election's safety rests on every node
  having a unique ``nodeName``; two nodes sharing one would *both* elect
  themselves (each is the ``min`` of its own live set) and double-run.  Each
  process therefore mints a random ``instance_id`` at startup and reports it
  alongside its name, so a node can tell a benign self-listing (same name *and*
  instance id) from a genuine duplicate (same name, *different* instance).
  Each /peer response also carries the responder's own observations, so a node
  detects a duplicate *transitively* -- even when it cannot reach both copies
  directly, two peers that each see one copy let it union the two instance ids
  for that name.  A detected duplicate is reported as ``conflict`` and makes
  the quorum-gated leader gate fail closed (see
  :meth:`ClusterManager.has_conflict`), so a misconfiguration pauses ``Leader``
  jobs rather than silently double-running them.
* **Membership must be uniform.**  The safety proof below ("two strict
  majorities of N cannot be disjoint") holds only if every node uses the same
  cluster size N.  But N is each node's own ``len(peers) + 1`` and the job-set
  fingerprint deliberately ignores the peer list, so two nodes with divergent
  peer lists (e.g. mid-resize) still see each other ``AGREED`` yet are each
  quorate under a *different* N -- a split-brain.  So every node also reports
  its declared N on /peer, and a peer that agrees on the job set but declares a
  different N is a ``conflict`` exactly like a duplicate ``nodeName``: the
  ``Leader`` gate fails closed until the cluster reconverges on one N (see
  :meth:`ClusterManager.conflicting_sizes`).  A size-divergent peer is *also*
  dropped from the mutual-agreement set (see
  :meth:`ClusterManager._agreeing_peers`), so it is neither counted toward
  quorum nor gossiped as a node we vouch for -- otherwise a third node that
  cannot itself see the divergence could bridge-confirm the stale-N node as
  quorate and defer to it (a node that is itself failing closed), stranding the
  whole cluster mid-resize.  This catches every *resize* (a changed N); it does
  **not** catch a same-N change of *membership* (swapping one peer for another
  keeps N, so two disjoint quorate groups could each lead).  Change membership
  one node at a time so the old and new majorities always overlap.
* **Drift is debounced.**  A reachable peer whose id differs is only reported
  as ``drifted`` after ``driftAfter`` consecutive rounds, so a rolling deploy
  (a transient, legitimate mismatch) does not raise a false alarm.

When ``electLeader`` is set, the same attestation drives a **quorum-gated
leader election** (see :func:`elect_leader`): each node independently elects,
as leader, the lowest ``nodeName`` among the *agreeing* members it can see, but
only if that set is a strict majority (a *quorum*) of the configured cluster.
Only the leader runs scheduled jobs, so replicas deployed from one config no
longer double-run.  Agreement is counted *mutually*: a peer joins the live set
only when both directions are confirmed -- we see it agreeing on the job-set id
*and* its /peer response shows it sees us agreeing too (matched by our unique
``instance_id``; see :meth:`ClusterManager._agreeing_peer_names`).  That, plus
the quorum gate, is what makes this safe with no shared state: two strict
majorities of N cannot be disjoint (for a *single* shared N -- divergence is
caught by the membership-uniformity gate above), so under a **clean partition**
at most one side is quorate and **at most one leader exists**.  The mutual
requirement closes the obvious one-way-link loophole: two nodes joined *only*
by a one-way link can no longer each count the other and both reach a majority.
The harder case is *bridged* asymmetry -- two nodes that never agree with each
other (a<->{c,d}, b<->{c,d}, with a and b not mutually agreeing) can each still
reach a quorum through the *shared* members that bridge them, and would each
elect itself.  Here the bridge is turned from cause into cure: each node
reports the set it *mutually* agrees with, so a node ``n`` reached only
transitively is confirmed quorate when a quorum -- ``n`` plus the shared
members we see two-way-agreeing with it -- vouches for it, and is then folded
into the election as an electable candidate (see
:meth:`ClusterManager._bridge_candidates` and
:meth:`ClusterManager._eligible_candidates`); the lower name wins on both
sides, so only one leads, whenever the two share at least ``quorum - 1``
mutually-agreeing members.
A node only ever elects a candidate it can *confirm is itself quorate* (a
direct peer whose gossiped ``mutual_agreeing`` is at or above quorum, or a
witnessed bridge node) -- never a node that would itself stand down.  This is
deliberate and is the design's liveness choice: in a **uniform-version**,
*converged* cluster a healthy majority is not stood down (electing the lowest
of a confirmed-quorate set lands on a node that actually runs).  That
confirmation is only as fresh as the witnesses' last gossip, though: a
bridge-confirmed candidate proves it *had* a quorum of mutual agreers when the
witnesses last saw it, not that it is still reachable now.  So if such a
candidate becomes isolated, the witnesses keep advertising it as quorate for up
to ~1--2 poll ``interval``s, and during that window the majority can briefly
*all* defer to that now-sub-quorum candidate and skip a firing -- the
liveness-side mirror of the double-run window below, and like it transient and
self-healing once the stale gossip ages out.  The accepted steady-state cost is
the converse -- two quorate nodes whose bridge is too thin to confirm
each other (fewer than ``quorum - 1`` shared members), are more than one gossip
hop apart, or are still converging may *each* elect itself and **double-run** a
``Leader`` job.  We trade the (fail-closed) risk of a missed firing for never
silently halting a healthy cluster; ``spread`` makes the same trade per job
(see :func:`elect_job_owner`).
During a **rolling upgrade** old and new builds run *different* election logic
and cannot fully agree: a node that has not yet learned a peer's
``mutual_agreeing`` simply does not elect that peer, which leans the new nodes
toward running (a possible double-run) rather than standing down -- though a
rare bridged mix can still transiently stand down until the upgrade completes.
The remaining liveness costs are mild: a true minority partition still goes
idle (it is below quorum, by design), mutual agreement and bridge discovery
cost an extra poll round to converge, and a brief window after a leader dies
may skip a firing.  It is *not* a fenced, exactly-once guarantee; for that you
would need a lease/consensus store, which this design intentionally avoids.
(The election trusts peers' gossiped ``mutual_agreeing`` like the rest of the
protocol: a CA-vouched but *hostile* peer could fabricate it to force a
stand-down or suppress a defer -- the same Byzantine class out of scope above.)

When ``distribution`` is ``"spread"`` the single elected leader is replaced by
**per-job ownership** via rendezvous (highest-random-weight) hashing (see
:func:`elect_job_owner`): each job is independently assigned to one member
of the quorate set, so leader-gated work fans out roughly evenly across the
cluster instead of piling onto one node.  This is purely a load optimization:
it keeps the same quorum gate and therefore the **same** safety guarantee as
single-leader -- no stronger and no weaker, including the bridge-discovery
mitigation above: bridged quorate nodes fold the same confirmed candidates
into the rendezvous set, so a quorum-strong shared bridge makes them agree on
one owner per job.  The same residuals apply -- a thin bridge, a >1-hop gossip
distance, or the convergence window can still double-run a job.
"""

import asyncio
import datetime
import hashlib
import json
import logging
import os
import ssl
import uuid
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Set

import aiohttp
from aiohttp import web

from yacron2.config import ClusterConfig
from yacron2.fingerprint import SCHEME_VERSION
from yacron2.leadership import LeadershipBackend

logger = logging.getLogger("yacron2.cluster")

# Per-peer status, as reported in the /cluster view.
STATUS_UNKNOWN = "unknown"  # not yet contacted
STATUS_SELF = "self"  # the peer reported our own node name AND instance id
STATUS_AGREED = "agreed"  # reachable, same job-set id
STATUS_SYNCING = "syncing"  # reachable, id differs but within driftAfter
STATUS_DRIFTED = "drifted"  # reachable, id has differed >= driftAfter rounds
STATUS_UNREACHABLE = "unreachable"  # connect/timeout failure
STATUS_UNTRUSTED = "untrusted"  # TLS/cert verification failed
# A *different* running instance is announcing our own nodeName: a duplicate
# nodeName, which breaks the election's distinct-identity assumption. Never
# counts toward agreement, and makes the leader gate fail closed (see
# ClusterManager.has_conflict / yacron2.cron._cluster_allows).
STATUS_CONFLICT = "conflict"

# Statuses for which we hold no fresh observation of the peer's identity this
# round, so the peer is ignored when detecting nodeName collisions.
_STALE_STATUSES = frozenset(
    {STATUS_UNKNOWN, STATUS_UNREACHABLE, STATUS_UNTRUSTED}
)

# Cap on the peer /peer response we will buffer per poll. The legitimate
# payload is a small JSON object (a fixed header plus one short member entry
# per node), so this is generous for clusters into the hundreds of nodes while
# bounding the memory a misbehaving-but-CA-trusted peer can force us to
# allocate each round (see _read_capped / _poll_peer).
MAX_PEER_RESPONSE_BYTES = 256 * 1024
_READ_CHUNK = 8192


def _parse_members(raw: Any) -> List["tuple[str, str, bool]"]:
    """Validate a peer's reported ``members`` list, dropping malformed entries.

    A peer is CA-vouched but otherwise untrusted input, so anything that is not
    a list of ``{node_name: str, instance_id: str, agreed: bool}`` objects is
    ignored: a malformed or hostile payload degrades to "no mutual/transitive
    information" rather than poisoning the election (see the type checks in
    :meth:`ClusterManager._poll_peer`).
    """
    members: List["tuple[str, str, bool]"] = []
    if not isinstance(raw, list):
        return members
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        name = entry.get("node_name")
        instance = entry.get("instance_id")
        agreed = entry.get("agreed")
        if (
            isinstance(name, str)
            and isinstance(instance, str)
            and isinstance(agreed, bool)
        ):
            members.append((name, instance, agreed))
    return members


def _parse_str_list(raw: Any) -> "set[str]":
    """Validate an untrusted JSON value as a set of strings, dropping the rest.

    Used for the gossiped ``ran_reboot_jobs`` set and the ``mutual_agreeing``
    set (the latter feeds bridge confirmation in
    :meth:`ClusterManager._bridge_candidates`); like _parse_members, hostile or
    malformed input degrades to an empty set rather than raising, and a peer
    that omits the field (an older build) parses to an empty set -- the safe
    direction (it simply contributes no evidence).
    """
    if not isinstance(raw, list):
        return set()
    return {item for item in raw if isinstance(item, str)}


def _peer_sees_me_agreed(
    peer_members: Optional[List["tuple[str, str, bool]"]],
    my_instance: str,
) -> bool:
    """Whether a peer's reported member list shows *us* — matched by our unique
    per-process ``instance_id`` — as one of the nodes it currently sees AGREED.

    This is the receiver half of the mutual-attestation gate: we count a peer
    toward quorum only when it confirms it sees us agreeing too (see
    :meth:`ClusterManager._agreeing_peer_names`).
    """
    if not peer_members:
        return False
    for _name, instance, agreed in peer_members:
        if agreed and instance == my_instance:
            return True
    return False


async def _read_capped(resp: Any, limit: int) -> "tuple[bytes, bool]":
    """Read a response body, refusing to buffer more than ``limit`` bytes.

    Returns ``(body, too_large)``.  Iterating (rather than ``resp.read()`` /
    ``resp.json()``, which buffer the whole body unconditionally) bounds memory
    even when the peer streams a huge or chunked response, and because aiohttp
    decompresses as we read, it also caps the *decompressed* size (a gzip-bomb
    guard).
    """
    chunks: List[bytes] = []
    total = 0
    async for chunk in resp.content.iter_chunked(_READ_CHUNK):
        total += len(chunk)
        if total > limit:
            return b"", True
        chunks.append(chunk)
    return b"".join(chunks), False


def quorum_size(cluster_size: int) -> int:
    """The strict majority of ``cluster_size`` nodes.

    A quorum requires *more than half* the cluster, so no two quorums can be
    disjoint; that is the property the leader gate relies on for safety.  Note
    this favours odd cluster sizes: N=3 needs 2 (tolerates one failure) and N=4
    needs 3 (still tolerates only one), so the even node buys nothing, while
    N=5 needs 3 and tolerates two.
    """
    return cluster_size // 2 + 1


def elect_leader(
    node_name: str,
    live_peer_names: Iterable[str],
    cluster_size: int,
    candidate_names: Optional[Iterable[str]] = None,
) -> Optional[str]:
    """Pure, deterministic leader election from one node's point of view.

    ``live_peer_names`` is this node's *mutual live set* -- the peers it sees
    agreeing on the job-set id.  The quorum gate is on that set (this node plus
    them): below a quorum of ``cluster_size`` there is no leader and ``None``
    is returned, which is how a minority partition is made to stand down.

    When quorate, the leader is the lowest ``nodeName`` among this node and
    ``candidate_names`` -- the names this node may actually *elect*.  The
    caller passes only candidates it can confirm are themselves quorate (live
    peers not known to be sub-quorum, plus bridge-discovered quorate nodes; see
    :meth:`ClusterManager._eligible_candidates`).  Two consequences:

    * electing the lowest among a set that spans a bridge makes two would-be
      leaders joined only by shared members defer to the same node, so only one
      leads (closing the asymmetric double-run); and
    * because every candidate is confirmed quorate *as of the witnesses' last
      gossip*, this node defers only to a node that was runnable then -- so in
      a converged view a healthy majority is not stood down (the liveness
      choice).  The residuals: two quorate nodes whose bridge is too thin to
      confirm each
      other may both run, and a candidate confirmed from now-stale gossip that
      has since become isolated can briefly draw the majority into deferring to
      it (a transient skip until the gossip ages out).

    ``candidate_names`` defaults to ``live_peer_names`` (the simple, no-
    confirmation behaviour) and never affects the quorum gate, which is always
    on ``live_peer_names``.
    """
    live = [node_name, *live_peer_names]
    if len(live) < quorum_size(cluster_size):
        return None
    if candidate_names is None:
        return min(live)
    return min([node_name, *candidate_names])


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
    live_peer_names: Iterable[str],
    cluster_size: int,
    candidate_names: Optional[Iterable[str]] = None,
) -> Optional[str]:
    """Quorum-gated per-job owner (the ``distribution: spread`` analogue of
    :func:`elect_leader`).

    The quorum gate is on the mutual live set (this node plus
    ``live_peer_names``); ``None`` below quorum stands a minority down.  When
    quorate the owner is the rendezvous winner for ``job_name`` over this node
    and ``candidate_names`` -- the confirmed-quorate names this node may elect
    (see :meth:`ClusterManager._eligible_candidates`), exactly as in
    :func:`elect_leader`.  Every quorate node sharing one bridged set computes
    the winner over the same candidates and so picks the same owner; and since
    each candidate is confirmed quorate, the per-job owner is never a node that
    would itself stand down.  ``candidate_names`` defaults to
    ``live_peer_names`` and never affects the quorum gate.
    """
    live = [node_name, *live_peer_names]
    if len(live) < quorum_size(cluster_size):
        return None
    names = live_peer_names if candidate_names is None else candidate_names
    return _hrw_owner(job_name, [node_name, *names])


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
    # peer's last-reported per-process instance id, used to distinguish a
    # benign self-listing from a duplicate nodeName (see record_success).
    # Deliberately not surfaced in to_dict (it is an internal liveness token).
    instance_id: Optional[str] = None
    # whether a successful poll of this host has positively identified it as
    # THIS node (it returned our own instance id, or our name with no instance
    # id). A node's identity at an address it once answered does not change
    # because a later poll fails, so once set this keeps the entry STATUS_SELF
    # across transient self-poll failures (a hairpin/NAT quirk) -- otherwise
    # cluster_size would flap N<->N+1 on the poll interval. Re-evaluated on
    # every successful poll. Internal, like instance_id; not in to_dict.
    self_confirmed: bool = False
    last_seen: Optional[datetime.datetime] = None  # last successful contact
    last_error: Optional[str] = None
    # consecutive reachable-but-mismatched rounds, for the drift hysteresis
    mismatch_streak: int = 0
    # the peer's own reported observations (node_name, instance_id, agreed)
    # from its last /peer response, feeding mutual-agreement and transitive
    # conflict detection (see ClusterManager._agreeing_peer_names /
    # conflict_names). None when we hold no fresh response. Internal, like
    # instance_id, so deliberately not surfaced in to_dict.
    members: Optional[List["tuple[str, str, bool]"]] = None
    # the cluster size (len(peers)+1) the peer last declared. The election's
    # safety rests on every node sharing one N, but N is each node's *local*
    # count and nothing reconciles it, so a divergence is a first-class
    # conflict (see ClusterManager.conflicting_sizes). None when no fresh
    # result, or a peer too old to report it. Internal, like instance_id; not
    # surfaced in to_dict.
    declared_size: Optional[int] = None
    # the coordination policy the peer last declared: its cluster.distribution
    # ("single-leader"/"spread") and whether it has electLeader on. Like
    # declared_size these are behaviour-affecting and NOT part of the job-set
    # fingerprint, so two nodes that differ only here still see each other
    # AGREED; a divergence is a first-class conflict (see
    # ClusterManager.conflicting_policies). None when no fresh result, or a
    # peer too old to report it. Internal, like instance_id; not in to_dict.
    declared_distribution: Optional[str] = None
    declared_elect_leader: Optional[bool] = None
    # the @reboot job names the peer reports as already run in the cluster
    # (its own runs plus what it learned), used to retire our matching deferred
    # one-shots without re-running them (see ClusterManager.reboot_ran). Only
    # trusted from an AGREED peer (same job-set id). None when no fresh result.
    ran_reboot_jobs: Optional["set[str]"] = None
    # the names the peer reports it *mutually* agrees with (its own
    # _agreeing_peer_names). Unlike ``members`` -- whose ``agreed`` flag is
    # one-directional (the peer merely reached that node) -- this is the peer's
    # confirmed two-way set, so it is the only sound evidence that a node we
    # reach only transitively is itself quorate (see _bridge_candidates). A
    # one-directional flag would let a node reached one-way by a quorum be
    # mistaken for quorate and pull every node into deferring to it -- a
    # cluster-wide stand-down. None when no fresh result (or an older peer that
    # does not report it: it then contributes no bridge evidence, which is the
    # safe direction).
    mutual_agreeing: Optional["set[str]"] = None

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
        peer_instance: Optional[str] = None,
        my_instance: Optional[str] = None,
        peer_members: Optional[List["tuple[str, str, bool]"]] = None,
        peer_ran_reboot_jobs: Optional["set[str]"] = None,
        peer_size: Optional[int] = None,
        peer_mutual_agreeing: Optional["set[str]"] = None,
        peer_distribution: Optional[str] = None,
        peer_elect_leader: Optional[bool] = None,
    ) -> None:
        peer = self.peers[host]
        peer.last_seen = now
        peer.last_error = None
        peer.job_set_id = peer_id
        peer.node_name = peer_name
        peer.instance_id = peer_instance
        peer.members = peer_members
        peer.ran_reboot_jobs = peer_ran_reboot_jobs
        peer.declared_size = peer_size
        peer.mutual_agreeing = peer_mutual_agreeing
        peer.declared_distribution = peer_distribution
        peer.declared_elect_leader = peer_elect_leader
        # re-determine self-ness on every successful poll: an address that no
        # longer answers as us (reassigned) must be able to lose the flag.
        peer.self_confirmed = False

        if peer_name is not None and peer_name == my_name:
            if peer_instance is not None and peer_instance != my_instance:
                # A *different* running instance is announcing our own
                # nodeName. That is a duplicate nodeName, which silently breaks
                # the election's core assumption (distinct identities -> a
                # single leader). Surface it as a hard conflict instead of
                # masking it as 'self'; the leader gate then fails closed.
                peer.status = STATUS_CONFLICT
                peer.mismatch_streak = 0
                peer.last_error = (
                    "duplicate nodeName {!r}: peer is a different "
                    "instance".format(peer_name)
                )
                return
            # Same name *and* same instance id (the operator listed this node's
            # own address), or a peer too old to report an instance id: the
            # benign self case. Never counts toward agreement.
            peer.status = STATUS_SELF
            peer.self_confirmed = True  # latch: keep SELF across poll failures
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
        if peer.self_confirmed:
            # this host has been positively identified as THIS node; a failed
            # poll (a hairpin/NAT quirk where we cannot dial our own advertised
            # address) does not change that. Keep it SELF rather than flapping
            # to UNREACHABLE, which would oscillate cluster_size (and so the
            # quorum threshold and the size-divergence gate) on the poll
            # interval, in turn flapping Leader-gated jobs.
            peer.status = STATUS_SELF
        else:
            peer.status = STATUS_UNTRUSTED if untrusted else STATUS_UNREACHABLE
        # we could not observe the id this round, so drop the peer's last
        # reported view as stale (no mutual/conflict info this time). The drift
        # streak is deliberately NOT reset here: it counts *reachable*
        # mismatches, and zeroing it on every unreachable round means an
        # intermittently-reachable but genuinely drifted peer never accumulates
        # driftAfter consecutive mismatches, so the drift alarm never fires for
        # exactly the flaky case it exists to catch. It is reset only by a
        # confirmed AGREED (or SELF) observation in record_success.
        peer.members = None
        peer.ran_reboot_jobs = None
        peer.mutual_agreeing = None

    def to_list(self) -> List[Dict[str, Any]]:
        return [peer.to_dict() for peer in self.peers.values()]

    def local_members(
        self, my_name: str, my_instance: str
    ) -> List[Dict[str, Any]]:
        """This node's current observations, for the /peer response body.

        Lists this node (always agreeing with itself) plus every peer we hold a
        fresh observation of, each tagged with whether we currently see it
        AGREED.  A polling peer uses this two ways: to confirm *mutual*
        agreement (does this list carry the poller, agreed?) and to detect a
        duplicate nodeName transitively (does any name appear with two distinct
        instance ids once everyone's lists are unioned?).
        """
        members: List[Dict[str, Any]] = [
            {
                "node_name": my_name,
                "instance_id": my_instance,
                "agreed": True,
            }
        ]
        for peer in self.peers.values():
            if peer.status in _STALE_STATUSES or peer.node_name is None:
                continue
            members.append(
                {
                    "node_name": peer.node_name,
                    "instance_id": peer.instance_id,
                    "agreed": peer.status == STATUS_AGREED,
                }
            )
        return members


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
    """Server context: require and verify a CA-signed client cert (mTLS).

    SECURITY: this is the cluster's membership boundary. A server cannot do
    hostname verification, so it accepts *any* client cert the configured
    ``cluster.tls.ca`` signed -- the CA file IS the allowlist. Point it at a
    **dedicated, single-purpose cluster CA**, never a shared organisational CA:
    with a shared CA, any holder of any cert that CA ever signed (an unrelated
    web service, say) can speak to ``/peer`` and ``/reboot-ran`` as a member.
    Peer-reported state is corroboration-checked (see
    :meth:`ClusterManager.conflict_names`) so one such cert cannot fabricate a
    cluster-wide ``Leader`` stand-down, but it can still push ``reboot-ran``
    suppression and read topology. (A future opt-in could pin the client cert
    SAN/CN against an allowed-name list; not enabled by default because a
    peer's cert SAN has no required relationship to its listed address.)
    """
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH, cafile=tls["ca"])
    ctx.load_cert_chain(tls["cert"], tls["key"])
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


def _tls_file_signature(tls: Dict[str, str]) -> Dict[str, Any]:
    """A cheap on-disk fingerprint of the CA / cert / key files.

    The SSL contexts are built once and load the cert+key into memory, so an
    *in-place* rotation -- same file paths, new bytes, which is exactly how
    cert-manager, Vault, and Kubernetes secret refreshes renew -- is otherwise
    invisible to a long-running process: every node keeps serving its old cert
    until it expires, then peers reject each other and the cluster loses quorum
    fleet-wide.  Comparing ``(st_mtime_ns, st_size)`` per file lets the daemon
    notice a rotation and rebuild the contexts (see
    :meth:`ClusterManager.tls_files_changed` and
    :meth:`yacron2.cron.Cron.start_stop_cluster`).  ``os.stat`` follows
    symlinks, so the atomic symlink swap Kubernetes uses for mounted secrets is
    picked up too.  A stat error (e.g. a file briefly absent mid-rotation) is
    recorded as ``None`` and simply compares unequal once the file is back,
    which is the safe direction -- a spurious restart, not a missed one.
    """
    signature: Dict[str, Any] = {}
    for key in ("ca", "cert", "key"):
        try:
            st = os.stat(tls[key])
            signature[key] = (st.st_mtime_ns, st.st_size)
        except OSError:
            signature[key] = None
    return signature


def _split_host_port(addr: str) -> "tuple[str, int]":
    # Bracketed IPv6 (``[2001:db8::1]:8900``): the host is inside the brackets;
    # split only on the final ``:`` after the closing ``]``. A bare unbracketed
    # IPv6 literal is rejected at config load (see config._require_host_port),
    # so anything reaching here with multiple colons is bracketed.
    if addr.startswith("["):
        bracket, sep, port = addr.rpartition("]:")
        host = bracket[1:]  # strip the leading "["
        if not sep or not host or not port:
            raise ValueError("expected [ipv6]:port, got {!r}".format(addr))
        return host, int(port)
    host, _, port = addr.rpartition(":")
    if not host or not port:
        raise ValueError("expected host:port, got {!r}".format(addr))
    return host, int(port)


class ClusterManager(LeadershipBackend):
    """Owns the mTLS ``/peer`` listener and the periodic peer-poll loop.

    The default, best-effort gossip leadership backend (see
    :class:`yacron2.leadership.LeadershipBackend`).  It defines real bodies for
    every method on the seam -- core, defaulted, and the never-skip
    ``available_*`` family -- so subclassing the ABC is purely a conformance
    declaration and leaves behaviour byte-identical.
    """

    def __init__(
        self,
        config: ClusterConfig,
        get_job_set_id: Callable[[], str],
    ) -> None:
        self.config = config
        self.get_job_set_id = get_job_set_id
        self.node_name: str = config["nodeName"]
        # A random per-process identity, reported alongside node_name so peers
        # can tell a benign self-listing from a duplicate nodeName (a different
        # process claiming the same name); see ClusterView.record_success and
        # has_conflict. Changes every restart, which is fine: it only ever
        # distinguishes "is this the same running process as me".
        self.instance_id: str = uuid.uuid4().hex
        # "single-leader" (one leader runs all Leader jobs) or "spread"
        # (per-job ownership via rendezvous hashing); see _cluster_allows.
        self.distribution: str = config.get("distribution", "single-leader")
        self.view = ClusterView(
            [peer["host"] for peer in config["peers"]],
            config["driftAfter"],
        )
        self._client_ssl = build_client_ssl_context(config["tls"])
        self._server_ssl = build_server_ssl_context(config["tls"])
        # snapshot the TLS material as loaded, so an in-place cert rotation can
        # be detected and the contexts rebuilt via a restart (see
        # tls_files_changed); the contexts themselves are never reloaded.
        self._tls_signature = _tls_file_signature(config["tls"])
        self._runner: Optional[web.AppRunner] = None
        self._poll_task: Optional[asyncio.Task] = None
        # one client session for the lifetime of the manager, so peer polls and
        # reboot-ran pushes reuse connections instead of re-handshaking mTLS
        # every round; created in start(), closed in stop().
        self._session: Optional[aiohttp.ClientSession] = None
        self._stop = asyncio.Event()
        # @reboot one-shots THIS node has run as the elected owner (plus any it
        # learned ran via push) -- gossiped so peers retire their matching
        # deferred jobs without re-running them on failover. Scoped to the
        # current job-set: cleared when our job_set_id changes (see _poll_all),
        # so a config change cannot carry a stale "already ran" across it.
        self._ran_reboot_jobs: Set[str] = set()
        self._ran_jobs_job_set_id: Optional[str] = None

    # --- the mTLS /peer server -------------------------------------------

    async def _handle_peer(self, request: web.Request) -> web.Response:
        return web.json_response(
            {
                "node_name": self.node_name,
                "job_set_id": self.get_job_set_id(),
                "scheme_version": SCHEME_VERSION,
                "instance_id": self.instance_id,
                # our declared cluster size (len(peers)+1). The election's
                # safety assumes every node shares one N; a polling peer that
                # declares a different one treats it as a conflict and fails
                # Leader closed, mirroring a duplicate nodeName (see
                # ClusterManager.conflicting_sizes).
                "cluster_size": self.cluster_size(),
                # our coordination policy: distribution and electLeader pick
                # *which* node runs a Leader job (single-leader elects the
                # min live name; spread picks a per-job rendezvous owner;
                # electLeader off runs every job ungated). Neither is in the
                # job-set fingerprint, so a peer that declares a different one
                # is treated as a conflict and fails Leader closed (see
                # ClusterManager.conflicting_policies).
                "distribution": self.distribution,
                "elect_leader": bool(self.config.get("electLeader")),
                # our current observations, so a polling peer can confirm we
                # see it too (mutual agreement) and spot a duplicate nodeName
                # transitively; see ClusterView.local_members.
                "members": self.view.local_members(
                    self.node_name, self.instance_id
                ),
                # @reboot one-shots already run in the cluster (ours + learned
                # from agreed peers), so a poller can retire its matching
                # deferred job without re-running it; see advertised_ran_jobs.
                "ran_reboot_jobs": sorted(self.advertised_ran_jobs()),
                # the peers we *mutually* agree with: a poller uses this as the
                # sound evidence that a node it reaches only transitively is
                # itself quorate (a witnessed two-way edge), driving the
                # bridge-discovery deferral; see _bridge_candidates. Distinct
                # from the one-directional ``agreed`` flags in ``members``.
                "mutual_agreeing": sorted(self._agreeing_peer_names()),
            }
        )

    async def _handle_reboot_ran(self, request: web.Request) -> web.Response:
        """Receive an eager push of @reboot jobs a peer just ran.

        The pull-poll already carries this set, but a push shrinks the window
        in which an owner could run a one-shot and then die before any peer
        polled it (so a new leader would re-run it).  Best-effort: we accept it
        only when the sender's job_set_id matches ours (an agreed peer, same
        config), and any malformed body is ignored.

        Trust scope: the never-re-run guarantee holds against benign failures
        (crashes, partitions).  A CA-vouched but *hostile* peer could push a
        fabricated "ran X" to make others retire a job that never ran -- the
        same Byzantine class as a member lying about its job_set_id to skew the
        election, which this design already does not defend against.
        """
        try:
            raw, too_large = await asyncio.wait_for(
                _read_capped(request, MAX_PEER_RESPONSE_BYTES),
                self.config["connectTimeout"],
            )
        except asyncio.TimeoutError:
            # a slow/stalled body read (a hung but CA-vouched peer): bound it
            # by the same per-request timeout the client side uses, rather than
            # letting it pin a handler coroutine indefinitely (the size cap in
            # _read_capped bounds bytes, not time).
            return web.Response(status=408)
        if too_large:
            return web.Response(status=413)
        try:
            data = json.loads(raw)
        except (ValueError, RecursionError):
            # unparseable (ValueError) or too deeply nested for the JSON
            # scanner (RecursionError, not a ValueError); either is a malformed
            # push from a CA-trusted-but-buggy/hostile peer -> reject cleanly
            # rather than 500 on an escaped exception.
            return web.Response(status=400)
        if (
            isinstance(data, dict)
            and data.get("job_set_id") == self.get_job_set_id()
        ):
            # Reconcile our recorded runs to the current job set *before*
            # absorbing, mirroring _poll_all.  A reload may have changed the
            # job set while _ran_jobs_job_set_id still lags; without this the
            # names would be seeded under the stale id and wiped by the next
            # poll.  Reconciling first records them under the live id so they
            # survive (and clears a stale set rather than carry it across).
            self._reconcile_job_set_id(self.get_job_set_id())
            self._ran_reboot_jobs |= _parse_str_list(data.get("names"))
        return web.Response(status=204)

    async def start(self) -> None:
        app = web.Application()
        app.add_routes(
            [
                web.get("/peer", self._handle_peer),
                web.post("/reboot-ran", self._handle_reboot_ran),
            ]
        )
        runner = web.AppRunner(app)
        await runner.setup()
        try:
            host, port = _split_host_port(self.config["listen"])
            site = web.TCPSite(
                runner, host, port, ssl_context=self._server_ssl
            )
            await site.start()
        except BaseException:
            # bad listen address (ValueError) or bind failure (OSError, e.g.
            # the port is already in use) after the runner was set up -- and
            # cancellation -- must not leak the half-started runner.
            await runner.cleanup()
            raise
        self._runner = runner
        # one session for the manager's lifetime: peer polls and reboot-ran
        # pushes reuse it (and its kept-alive mTLS connections) instead of
        # opening a fresh session -- and re-handshaking -- every round.
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config["connectTimeout"])
        )
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
        if self._session is not None:
            # close after the poll task is cancelled, so no in-flight request
            # is using it; before the runner, mirroring teardown order.
            await self._session.close()
            self._session = None
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    def tls_files_changed(self) -> bool:
        """Whether the CA/cert/key files differ from what we loaded at startup.

        True after an in-place cert rotation, so the daemon can restart the
        manager to rebuild the SSL contexts before the old cert expires
        cluster-wide (the contexts are otherwise built once and never
        reloaded).  See :func:`_tls_file_signature`.
        """
        return _tls_file_signature(self.config["tls"]) != self._tls_signature

    def tls_files_loadable(self) -> bool:
        """Whether the *current* on-disk CA/cert/key load into contexts now.

        Re-runs exactly the work :meth:`__init__` did at startup
        (:func:`build_client_ssl_context` / :func:`build_server_ssl_context`)
        against the live files but *without binding the listener*, so it is a
        side-effect-free dry-run of the rebuild that
        :meth:`yacron2.cron.Cron.start_stop_cluster` is about to attempt on a
        cert rotation.  The built contexts are discarded -- the real swap
        happens by reconstructing the manager once validation passes.  Returns
        ``False`` if any file is missing, unreadable, or a half-written/invalid
        PEM (the same ``OSError`` / ``ssl.SSLError`` the real rebuild would
        raise), so the caller can keep the running manager -- still serving the
        valid old cert -- until the rotation settles, instead of tearing it
        down and then failing to rebuild.
        """
        tls = self.config["tls"]
        try:
            build_client_ssl_context(tls)
            build_server_ssl_context(tls)
        except (OSError, ssl.SSLError):
            return False
        return True

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

    def _reconcile_job_set_id(self, my_id: str) -> None:
        """Align the recorded-@reboot-runs set with the current job set.

        Clears ``_ran_reboot_jobs`` when our job set CHANGED (a config reload):
        runs recorded under the old set no longer apply to the new one, so a
        still-deferred @reboot may run again -- the safe direction; we never
        silently skip a job whose definition changed.  The first observation
        only *establishes* the id (no clear), so a push that arrived before the
        first poll is not wiped.

        The poll loop calls this each round, but :meth:`mark_reboot_ran` and
        :meth:`_handle_reboot_ran` call it too, immediately before they add to
        the set: that records their entries under the live id so the loop's
        next reconcile (same id -> no clear) cannot discard them, closing the
        window where an add raced a reload-driven clear.  It is idempotent and
        await-free, so calling it from those paths interleaves safely.
        """
        if (
            self._ran_jobs_job_set_id is not None
            and my_id != self._ran_jobs_job_set_id
        ):
            self._ran_reboot_jobs.clear()
        self._ran_jobs_job_set_id = my_id

    async def _poll_all(self) -> None:
        my_id = self.get_job_set_id()
        self._reconcile_job_set_id(my_id)
        peers = self.config["peers"]
        session = self._session
        if not peers or session is None:
            # no peers to poll, or the manager is not running (e.g. _poll_all
            # invoked directly in a test): the reconcile above is the only work
            # this round.
            return
        # return_exceptions so one peer raising an *unexpected* error (a
        # bug, not a network failure -- those are handled inside _poll_peer)
        # cannot abort the whole round and leave the other peers' coroutines
        # detached. Surface such errors, don't swallow.
        results = await asyncio.gather(
            *(self._poll_peer(session, peer["host"], my_id) for peer in peers),
            return_exceptions=True,
        )
        # gather preserves order, so results[i] corresponds to peers[i].
        for index, result in enumerate(results):
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, BaseException):
                logger.error(
                    "cluster: unexpected error polling %s: %r",
                    peers[index]["host"],
                    result,
                )

    async def _poll_peer(
        self, session: aiohttp.ClientSession, host: str, my_id: str
    ) -> None:
        """Observe one peer, then log any status transition (once per change).

        The observation lives in :meth:`_observe_peer`; this thin wrapper diffs
        the peer's status across it so a reachability, cert, or drift change
        gets a log line at the manager seam -- ``ClusterView`` itself stays
        pure (no I/O, no logging) so its state machine remains trivially
        testable.
        """
        prev_status = self.view.peers[host].status
        await self._observe_peer(session, host, my_id)
        self._log_peer_status_change(host, prev_status)

    def _log_peer_status_change(self, host: str, prev: str) -> None:
        """Log a peer's status transition once, where the manager has a seam.

        Cert failures are the highest-value signal: a botched in-place rotation
        otherwise turns peers ``untrusted`` one by one in silence until enough
        fall off to break quorum.  A first contact going *unreachable* out of
        ``unknown`` (and any no-op transition) is not logged, so a cluster
        coming up does not emit a startup burst while peers are still binding;
        a first *successful* contact does log a single ``now agreed``.
        """
        peer = self.view.peers[host]
        new = peer.status
        if new == prev:
            return
        if new == STATUS_UNTRUSTED:
            logger.warning(
                "cluster: peer %s is untrusted -- TLS/cert verification "
                "failed: %s",
                host,
                peer.last_error,
            )
        elif new == STATUS_UNREACHABLE and prev not in _STALE_STATUSES:
            # only warn when a peer we had previously reached drops; a peer
            # that was never contacted (unknown / already stale) is startup
            # noise.
            logger.warning(
                "cluster: peer %s became unreachable: %s",
                host,
                peer.last_error,
            )
        elif new == STATUS_DRIFTED:
            logger.warning(
                "cluster: peer %s drifted -- its job-set id has differed for "
                ">= driftAfter rounds (or reports a different fingerprint "
                "scheme)",
                host,
            )
        elif new == STATUS_CONFLICT:
            # the cluster-wide conflict is logged loudly by
            # Cron._log_cluster_role; a per-peer line at INFO just pinpoints
            # which peer collided.
            logger.info(
                "cluster: peer %s reports a duplicate nodeName: %s",
                host,
                peer.last_error,
            )
        elif new == STATUS_AGREED and prev != STATUS_SELF:
            logger.info("cluster: peer %s now agreed", host)

    async def _observe_peer(
        self, session: aiohttp.ClientSession, host: str, my_id: str
    ) -> None:
        url = "https://{}/peer".format(host)
        now = datetime.datetime.now(datetime.timezone.utc)
        try:
            async with session.get(url, ssl=self._client_ssl) as resp:
                resp.raise_for_status()
                raw, too_large = await _read_capped(
                    resp, MAX_PEER_RESPONSE_BYTES
                )
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
        if too_large:
            self.view.record_failure(
                host,
                "oversized /peer response (> {} bytes)".format(
                    MAX_PEER_RESPONSE_BYTES
                ),
                untrusted=False,
            )
            return
        try:
            data = json.loads(raw)
        except (ValueError, RecursionError):
            # invalid/truncated JSON (JSONDecodeError and UnicodeDecodeError
            # both subclass ValueError), or a deeply-nested document the JSON
            # scanner refuses (RecursionError, a RuntimeError -- NOT a
            # ValueError -- reachable under the size cap): a CA-trusted peer
            # can still be buggy or hostile, so treat any unparseable body as a
            # failed observation. Letting RecursionError escape here would skip
            # record_failure and freeze the peer's last (stale) observation in
            # the view, since _poll_all only logs the stray exception.
            self.view.record_failure(
                host, "invalid JSON in /peer response", untrusted=False
            )
            return
        if not isinstance(data, dict):
            self.view.record_failure(
                host,
                "malformed /peer response (not a JSON object)",
                untrusted=False,
            )
            return
        # Type-validate the scalar identity fields: a non-string node_name from
        # a CA-trusted-but-misbehaving peer would otherwise flow into
        # min()/sorted()/dict keys during election and crash the scheduler.
        fields: Dict[str, Optional[str]] = {}
        for key in (
            "node_name",
            "job_set_id",
            "scheme_version",
            "instance_id",
            "distribution",
        ):
            value = data.get(key)
            if value is not None and not isinstance(value, str):
                self.view.record_failure(
                    host,
                    "malformed /peer response: {!r} is not a string".format(
                        key
                    ),
                    untrusted=False,
                )
                return
            fields[key] = value
        # cluster_size is an int, validated separately (bool is an int
        # subclass, so reject it explicitly). A peer too old to report it sends
        # None, which skips the size check for that peer. Unlike the
        # instance_id fail-open (a missing one only forgoes *extra* conflict
        # evidence), a missing declared_size forgoes a fail-*closed* guard:
        # such a peer is neither flagged in conflicting_sizes nor dropped from
        # the mutual set, so a genuinely divergent-but-silent peer (only
        # possible pre-size-gate builds) is trusted. That is the version-skew
        # residual, not a normal resize -- every current build reports its N.
        size = data.get("cluster_size")
        if size is not None and (
            not isinstance(size, int) or isinstance(size, bool) or size < 1
        ):
            self.view.record_failure(
                host,
                "malformed /peer response: cluster_size is not a positive "
                "integer",
                untrusted=False,
            )
            return
        # elect_leader is a bool; a peer too old to report it sends None, which
        # forgoes the policy-conflict guard for that peer (the safe direction:
        # it is simply not compared). Reject a non-bool from a misbehaving peer
        # before it reaches conflicting_policies.
        elect = data.get("elect_leader")
        if elect is not None and not isinstance(elect, bool):
            self.view.record_failure(
                host,
                "malformed /peer response: elect_leader is not a boolean",
                untrusted=False,
            )
            return
        self.view.record_success(
            host,
            fields["node_name"],
            fields["job_set_id"],
            fields["scheme_version"],
            my_id,
            now,
            self.node_name,
            peer_instance=fields["instance_id"],
            my_instance=self.instance_id,
            peer_members=_parse_members(data.get("members")),
            peer_ran_reboot_jobs=_parse_str_list(data.get("ran_reboot_jobs")),
            peer_size=size,
            peer_distribution=fields["distribution"],
            peer_elect_leader=elect,
            # An older build that omits the field, or a peer reporting an empty
            # set, both parse to an empty set here: either way it is not
            # confirmed quorate, so _eligible_candidates won't elect it. (The
            # PeerState default None -- never polled -- is treated the same.)
            peer_mutual_agreeing=_parse_str_list(data.get("mutual_agreeing")),
        )

    # --- deferred @reboot "already ran" gossip ---------------------------

    def advertised_ran_jobs(self) -> Set[str]:
        """@reboot one-shots known to have run under our *current* job set.

        Our own runs plus those reported by every peer we currently agree with
        (same job_set_id).  Re-advertising what we learned makes the fact
        survive the original owner's death (one-hop gossip).

        A peer's contribution is gated on its last-reported ``job_set_id``
        matching our *live* id, not merely on the cached ``STATUS_AGREED``.
        ``STATUS_AGREED`` only proves the ids matched at the *last poll*; a
        local config reload changes our live id immediately, yet a peer keeps
        its stale AGREED status (and its old-config ran-set) until its next
        poll round (up to one ``interval``).  Without the live-id check a stale
        set could mask a *redefined* @reboot one-shot and make
        :meth:`yacron2.cron.Cron._process_pending_reboots` retire it without
        running the new definition -- a silent skip the local-set reconcile in
        :meth:`_reconcile_job_set_id` already prevents for our own runs.  The
        live check mirrors the gate :meth:`_handle_reboot_ran` puts on pushes.
        """
        my_id = self.get_job_set_id()
        # Gate our OWN recorded runs on the live id too, not just peers'. They
        # were recorded under _ran_jobs_job_set_id, which the periodic poll
        # reconciles (and clears on change) only lazily. Between an in-place
        # reload and the next poll the live id is already the new one while
        # _ran_reboot_jobs still holds the old set, and /peer (_handle_peer
        # never reconciles, unlike the push paths) would otherwise advertise
        # that stale set under the *new* id -- a toxic pairing an agreed peer
        # trusts, retiring its redefined @reboot one-shot without running it.
        # (None means "no runs recorded yet": the set is empty, so it is the
        # safe direction -- it only establishes the id, never clears.)
        jobs = (
            set(self._ran_reboot_jobs)
            if self._ran_jobs_job_set_id in (None, my_id)
            else set()
        )
        for peer in self.view.peers.values():
            if (
                peer.status == STATUS_AGREED
                and peer.job_set_id == my_id
                and peer.ran_reboot_jobs
            ):
                jobs |= peer.ran_reboot_jobs
        return jobs

    def reboot_ran(self, job_name: str) -> bool:
        """Whether ``job_name`` already ran in the cluster (this config)."""
        return job_name in self.advertised_ran_jobs()

    async def mark_reboot_ran(self, job_name: str) -> None:
        """Record that we ran ``job_name`` as owner, and eagerly tell peers.

        The push is best-effort (the periodic pull carries the same set as a
        backstop); it just shrinks the window in which we could run the job and
        then die before any peer observed it.
        """
        # Reconcile to the live job set *before* adding, so the entry lands
        # under the current id.  Otherwise the poll loop, waking during the
        # push below, could observe a reloaded id and clear() this just-added
        # entry.  _reconcile_job_set_id is await-free, so the reconcile+add
        # cannot itself be interleaved, and the loop's next same-id reconcile
        # is then a no-op.
        self._reconcile_job_set_id(self.get_job_set_id())
        self._ran_reboot_jobs.add(job_name)
        await self._push_reboot_ran()

    async def _push_reboot_ran(self) -> None:
        peers = self.config["peers"]
        names = sorted(self.advertised_ran_jobs())
        session = self._session
        if not peers or not names or session is None:
            return
        payload = {"job_set_id": self.get_job_set_id(), "names": names}
        await asyncio.gather(
            *(
                self._push_reboot_ran_one(session, peer["host"], payload)
                for peer in peers
            ),
            return_exceptions=True,
        )

    async def _push_reboot_ran_one(
        self,
        session: aiohttp.ClientSession,
        host: str,
        payload: Dict[str, Any],
    ) -> None:
        url = "https://{}/reboot-ran".format(host)
        try:
            async with session.post(
                url, json=payload, ssl=self._client_ssl
            ) as resp:
                resp.raise_for_status()
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
            # best-effort: a delivery failure is fine, the periodic pull-poll
            # carries the same set; don't let it disturb the run loop.
            pass

    # --- leader election --------------------------------------------------

    def cluster_size(self) -> int:
        """Total number of cluster members.

        ``peers`` lists every *other* member, so the cluster is those plus this
        node -- minus any entry that turns out to be *this* node listed in its
        own peer list (status ``self``).  The config-load dedup
        (:func:`yacron2.config._is_self_listed`) already drops the literal
        ``listen`` match *and* the common wildcard case (a ``0.0.0.0:...``
        listen self-listed by ``nodeName``), so for a correctly-configured node
        N already matches what its peers declare.  This runtime subtraction is
        the backstop for the residue config can't catch without resolving
        addresses (e.g. a self-listing by an FQDN when ``nodeName`` is the
        short name): such an entry is recognised here as ``self`` once it
        successfully self-polls.  Excluding it keeps N equal to what
        correctly-configured peers declare, so a benign self-listing stays
        harmless rather than declaring a larger N and tripping the size-
        divergence gate cluster-wide (see :meth:`conflicting_sizes`).
        """
        self_listed = sum(
            1
            for peer in self.view.peers.values()
            if peer.status == STATUS_SELF
        )
        # Two peer entries that resolve to the SAME running node -- the same
        # observed per-process instance_id, e.g. one physical node listed at
        # two addresses / an IP and a DNS name -- are one fault domain, not
        # two. Counting both inflates N (and so the quorum threshold and the
        # size-divergence gate), eroding fault tolerance below the declared
        # size and silently enabling the degenerate 2-real-node mode the
        # electLeader 2-node refusal exists to forbid. Subtract the duplicates:
        # count each observed instance_id at most once (an entry not yet
        # identified, instance_id None, can't be deduped, so it counts -- the
        # safe, higher-quorum direction, matching the brief STATUS_SELF
        # inflation before a self-poll). A peer answering with OUR instance_id
        # is STATUS_SELF (already excluded above), so it never lands here.
        seen_instances: Set[str] = set()
        duplicate_instances = 0
        for peer in self.view.peers.values():
            if peer.status in _STALE_STATUSES or peer.status == STATUS_SELF:
                continue
            if peer.instance_id is None:
                continue
            if peer.instance_id in seen_instances:
                duplicate_instances += 1
            else:
                seen_instances.add(peer.instance_id)
        return (
            len(self.config["peers"]) + 1 - self_listed - duplicate_instances
        )

    def quorum(self) -> int:
        return quorum_size(self.cluster_size())

    def _agreeing_peers(self) -> List[PeerState]:
        """Peers we *mutually* agree with on our job-set id *and* cluster size.

        A peer counts only when both directions are confirmed: we see it AGREED
        *and* its last /peer response lists us (by our unique ``instance_id``)
        as a node it sees AGREED too.  The mutual requirement is what keeps the
        quorum gate sound under asymmetric reachability -- two nodes joined by
        a one-way link can no longer each count the other and both reach a
        bogus majority (which would let both self-elect and double-run a Leader
        job).  The price is one extra poll round to converge after a membership
        change, and that a purely one-way-reachable peer is treated as
        unreachable for quorum purposes.

        A peer that agrees on the job set but declares a *different* cluster
        size N is also excluded here -- it is a size conflict
        (:meth:`conflicting_sizes`), and that disagreement already fails our
        own ``Leader`` gate closed.  Dropping it from the mutual set is the
        load-bearing half of that gate's safety under asymmetric reachability:
        the names we gossip as ``mutual_agreeing`` are exactly these peers, so
        a node stuck on the old N is never *vouched for* to a third node that
        cannot see its divergent N -- which would otherwise let that third node
        bridge-confirm it as quorate and defer to a node that is itself failing
        closed (a cluster-wide stand-down for the whole resize).  Detection in
        :meth:`conflicting_sizes` is independent of this (it scans every peer),
        so the conflict is still surfaced and the gate still fails closed; a
        peer too old to declare a size (``declared_size is None``) is *not*
        excluded -- it contributes no divergence evidence, the safe direction.
        """
        my_size = self.cluster_size()
        agreeing: List[PeerState] = []
        # Dedup by per-process instance_id: a node reachable at two listed
        # addresses answers both with one identity and must count ONCE toward
        # the live set, or elect_leader's quorum check (len(live) >= quorum)
        # could be met by a single physical peer counted twice. Mirrors the
        # cluster_size() dedup so N and the live count stay consistent.
        seen_instances: Set[str] = set()
        for peer in self.view.peers.values():
            if not (
                peer.status == STATUS_AGREED
                and peer.node_name is not None
                and _peer_sees_me_agreed(peer.members, self.instance_id)
                and not (
                    peer.declared_size is not None
                    and peer.declared_size != my_size
                )
            ):
                continue
            if peer.instance_id is not None:
                if peer.instance_id in seen_instances:
                    continue
                seen_instances.add(peer.instance_id)
            agreeing.append(peer)
        return agreeing

    def _agreeing_peer_names(self) -> List[str]:
        """Names of the peers we mutually agree with.

        See :meth:`_agreeing_peers`.
        """
        return [
            peer.node_name for peer in self._agreeing_peers() if peer.node_name
        ]

    def _bridge_candidates(self) -> List[str]:
        """Nodes we reach only *transitively* that we can confirm are quorate.

        The quorum gate alone keeps the election safe under a clean partition,
        and the mutual requirement closes the direct one-way-link loophole --
        but two nodes that never agree with each other can each still reach a
        quorum through a set of *shared* members that bridges them, and would
        then both elect themselves (an at-most-once violation under asymmetric
        reachability; see the module docstring).

        This turns that bridge from the cause into the cure.  Each agreeing
        peer reports the set it *mutually* agrees with (``mutual_agreeing``).
        For a node ``n`` we do not count directly, we tally how many of *our*
        mutually agreeing peers also mutually agree with ``n`` -- each is a
        *witnessed two-way edge* into ``n`` -- and confirm ``n`` only when that
        tally plus ``n`` itself reaches a quorum.  That is sound evidence that
        ``n`` has a quorum of mutual agreers, i.e. ``n`` is itself quorate and
        *will* run if elected.  Folded into the election (see
        :func:`elect_leader`), this
        makes the larger of two bridged would-be leaders defer to the smaller,
        closing the steady-state double-run whenever the two share at least a
        ``quorum - 1`` mutually-agreeing members.

        Using mutual edges -- not the one-directional ``agreed`` flag in
        ``members`` -- is what keeps it *live*: a node merely *reached* one-way
        by a quorum is not quorate, and deferring to it would stand every node
        down (it cannot run).  By requiring a witnessed two-way edge we never
        confirm such a node.  The monotonicity argued on :func:`elect_leader`
        (this only ever *adds* candidates) soundly gives the **no-double-run**
        half: adding candidates can only make this node defer to a
        confirmed-quorate node, never lead more.  The **no-stand-down** half is
        weaker -- it holds only in a *converged* view, because confirmation
        proves a candidate *had* a quorum of mutual agreers as of the
        witnesses' last gossip, not that it is still reachable now.  A
        candidate that has
        since become isolated can therefore briefly pull the majority into
        deferring to it (a transient skip until the stale gossip ages out; see
        the module docstring).  Residual gaps stay best-effort: a pair sharing
        fewer than ``quorum - 1`` mutual
        bridges cannot be confirmed (so may still double-run), a node more than
        one gossip hop away is invisible until it propagates, and a stale view
        converges only as fast as the poll ``interval``.  A hard exactly-once
        guarantee still needs a lease/consensus store.
        """
        agreeing = self._agreeing_peers()
        # nodes we already count directly: never "bridge" candidates
        direct = {self.node_name} | {
            peer.node_name for peer in agreeing if peer.node_name
        }
        quorum = self.quorum()
        # per transitively-discovered node, the set of our mutually-agreeing
        # peers that *also* mutually agree with it (a witnessed two-way edge)
        witnesses: Dict[str, Set[str]] = defaultdict(set)
        for peer in agreeing:
            witness = peer.node_name
            if witness is None:  # _agreeing_peers filters these out already
                continue
            for name in peer.mutual_agreeing or ():
                if name not in direct:
                    witnesses[name].add(witness)
        # confirmed quorate iff we witness >= quorum mutual agreers of it
        # (the witnessing peers plus the node itself).
        return sorted(
            name for name, seen in witnesses.items() if len(seen) + 1 >= quorum
        )

    def _eligible_candidates(self) -> List[str]:
        """The names this node may actually *elect* as leader / job owner.

        The quorum gate (in :func:`elect_leader`) decides whether *this* node
        is quorate; this decides which OTHER names it will defer to.  We must
        not defer to a node that cannot itself run, or a healthy majority would
        stand down: e.g. our lowest-named mutual peer might itself be below
        quorum (reachable from us but isolated from the rest), and electing it
        would leave nobody running.  So we only ever elect a candidate we can
        *confirm is quorate*:

        * a directly mutually-agreeing peer whose own gossiped
          ``mutual_agreeing`` shows it at or above quorum
          (``len + 1 >= quorum``); and
        * a :meth:`_bridge_candidates` node, already confirmed quorate by
          witnessed mutual edges.

        A peer we cannot confirm quorate -- one reporting a sub-quorum set, or
        an older build that does not report the set at all -- is *not* elected.
        For a uniform-version cluster this means that, *in a converged view*, a
        quorate node elects a runnable leader, so a healthy majority is not
        stood down (the liveness choice).  The accepted residuals are the
        converse: two quorate nodes whose bridge is too thin to confirm each
        other may each elect itself and double-run a ``Leader`` job, and --
        symmetrically -- a candidate confirmed from now-stale bridge gossip
        that has since become isolated can briefly draw the majority into
        deferring to it (a transient skip until the gossip ages out; see the
        module docstring).  During a *rolling upgrade*
        (old and new builds) the two builds run different election logic and
        cannot agree, so excluding the unconfirmable old peers leans the new
        nodes toward running (a possible double-run) rather than standing down;
        a rare bridged topology can still transiently stand down until the
        upgrade completes.  See the module docstring.
        """
        quorum = self.quorum()
        eligible = [
            peer.node_name
            for peer in self._agreeing_peers()
            if peer.node_name and len(peer.mutual_agreeing or ()) + 1 >= quorum
        ]
        return eligible + self._bridge_candidates()

    # --- duplicate-nodeName detection ------------------------------------

    def conflict_names(self) -> List[str]:
        """nodeNames currently claimed by more than one distinct instance.

        Non-empty means a duplicate ``nodeName`` is present, which makes the
        quorum election unsafe (two nodes would each elect themselves), so the
        ``Leader`` gate treats it as fail-closed.

        The view is built by unioning *our own* fresh observations with every
        reachable peer's reported observations (the ``members`` list from its
        /peer response -- one-hop gossip).  That transitivity closes the gap
        where the duplicates are not both visible to us directly: two peers
        that each see only one copy of the duplicated name still let us spot
        the collision.  ``identity`` is the per-process ``instance_id``
        (falling back to a peer's host if it somehow reported none), and benign
        self-listing (same name *and* same instance id) is not a conflict.
        Stale peers (unreachable/untrusted/never-contacted) contribute nothing.

        Because a peer's ``members`` list is CA-vouched but otherwise untrusted
        input, an instance only counts toward a conflict when it is *credible*:

        * **first-party** -- our own identity, or a peer's identity as *we
          directly observed it* when we polled that peer (a peer describing
          itself, including a same-name ``STATUS_CONFLICT`` peer); or
        * **corroborated** -- a purely *transitive* (members-reported) instance
          is credible only when **at least two distinct peers** report it.

        A name is in conflict when it has two or more credible instances.  This
        keeps a genuine duplicate detectable -- in the usual full-mesh cluster
        we poll both copies directly (two first-party instances), and in a
        partial mesh two peers corroborate the copy we cannot reach -- while
        stopping a single misbehaving or buggy member from fabricating a
        conflict (two instances for one name, or a foreign instance of *our
        own* name) and wedging every node's ``Leader`` gate closed cluster-wide
        indefinitely (an availability DoS from one peer).  The residual: a real
        duplicate that only a *single* peer can witness transitively is not
        flagged -- the same best-effort limit the module documents (a hard
        guarantee needs a lease/consensus store).  ``identity`` is the
        per-process ``instance_id`` (falling back to a peer's host if it
        somehow reported none); a benign self-listing (same name *and* same
        instance id) is not a conflict, and stale peers contribute nothing.
        """
        # name -> instance -> set(first-party sources: "self" or a peer host)
        first_party: Dict[str, Dict[str, Set[str]]] = defaultdict(
            lambda: defaultdict(set)
        )
        # name -> instance -> set(distinct peers reporting it transitively)
        transitive: Dict[str, Dict[str, Set[str]]] = defaultdict(
            lambda: defaultdict(set)
        )
        first_party[self.node_name][self.instance_id].add("self")
        for peer in self.view.peers.values():
            if peer.status in _STALE_STATUSES:
                continue
            if peer.node_name is not None:
                # our own DIRECT observation of this peer's identity (the peer
                # describing itself when we polled it -- first-party evidence).
                first_party[peer.node_name][
                    peer.instance_id or "host:" + peer.host
                ].add(peer.host)
            # the peer's one-hop (transitive) view of the cluster -- weaker
            # evidence: credited only when a second distinct peer corroborates.
            for name, instance, _agreed in peer.members or ():
                transitive[name][instance].add(peer.host)
        conflicted: List[str] = []
        for name in set(first_party) | set(transitive):
            credible = set(first_party.get(name, {}))
            for instance, reporters in transitive.get(name, {}).items():
                if instance not in credible and len(reporters) >= 2:
                    credible.add(instance)
            if len(credible) >= 2:
                conflicted.append(name)
        return sorted(conflicted)

    # --- cluster-size (membership) divergence ----------------------------

    def conflicting_sizes(self) -> List[int]:
        """Cluster sizes declared by agreeing peers that differ from ours.

        The election's safety rests on every node sharing one cluster size N:
        "two strict majorities of N cannot be disjoint" holds *only* for a
        single N.  But N is each node's own ``len(peers) + 1`` and the job-set
        fingerprint deliberately ignores the peer list, so two nodes with
        divergent peer lists still see each other ``AGREED`` -- each then
        reaches a quorum under its *own* N and both elect a leader (a
        split-brain an ordinary resize, touching only ``peers``, can trigger).
        A peer that agrees on the job set but declares a different N is
        therefore a first-class conflict, handled exactly like a duplicate
        ``nodeName``: the ``Leader`` gate fails closed until the cluster
        reconverges on one N (see :meth:`has_conflict` /
        :func:`yacron2.cron.Cron._cluster_allows`).

        Only ``AGREED`` peers are compared: a differing N matters precisely
        for the members the quorum would otherwise count, and because a resize
        keeps the job set unchanged the divergent nodes *are* agreed and
        observe the mismatch symmetrically (each side fails closed).  Stale or
        job-set-drifted peers, and peers too old to report a size, contribute
        nothing.

        This compares the declared *size* N, so it catches every resize.  It
        does **not** catch a same-N change of *membership* (e.g. swapping one
        peer for another while keeping the count): two disjoint groups that
        each declare the same N would each reach a quorum with no conflict
        flagged.  The mitigation is operational -- change membership one node
        at a time so the old and new majorities always overlap (see the module
        docstring).
        """
        my_size = self.cluster_size()
        return sorted(
            {
                peer.declared_size
                for peer in self.view.peers.values()
                if peer.status == STATUS_AGREED
                and peer.declared_size is not None
                and peer.declared_size != my_size
            }
        )

    def conflicting_policies(self) -> List[str]:
        """Coordination-policy divergences declared by agreeing peers.

        The leader gate's safety assumes every node coordinates the same way:
        the same ``distribution`` and the same ``electLeader``.  These pick
        *which* node runs a ``Leader`` job -- ``single-leader`` elects
        ``min(live)`` while ``spread`` picks a per-job rendezvous owner (two
        independent selectors that name different nodes for most jobs), and a
        node with ``electLeader`` off runs *every* job ungated.  Neither field
        is part of the job-set fingerprint (it is deliberately per-*job*) nor
        of the ``cluster_size`` gate, so two nodes identical but for these
        still see each other ``AGREED`` -- and would then either double-run a
        ``Leader`` job (two different owners each fire it) or drop it entirely
        (each defers to the other).  A divergence is therefore a first-class
        conflict, handled exactly like a duplicate ``nodeName`` or a size
        disagreement: the ``Leader`` gate fails *closed* on every divergent
        node until the cluster reconverges (see :meth:`has_conflict` /
        :func:`yacron2.cron.Cron._cluster_allows`).

        Only ``AGREED`` peers that actually reported a value are compared; a
        peer too old to declare these contributes nothing (the safe
        direction).  Returns human-readable ``"field theirs != ours"``
        descriptors, sorted and de-duplicated, for the dashboard / view.
        """
        my_elect = bool(self.config.get("electLeader"))
        conflicts: Set[str] = set()
        for peer in self.view.peers.values():
            if peer.status != STATUS_AGREED:
                continue
            if (
                peer.declared_distribution is not None
                and peer.declared_distribution != self.distribution
            ):
                conflicts.add(
                    "distribution {!r} != {!r}".format(
                        peer.declared_distribution, self.distribution
                    )
                )
            if (
                peer.declared_elect_leader is not None
                and peer.declared_elect_leader != my_elect
            ):
                conflicts.add(
                    "electLeader {!r} != {!r}".format(
                        peer.declared_elect_leader, my_elect
                    )
                )
        return sorted(conflicts)

    def has_conflict(self) -> bool:
        """Whether any conflict that makes the election unsafe is visible here.

        A duplicate ``nodeName`` (two nodes would each elect themselves; see
        :meth:`conflict_names`), a cluster-size disagreement (two nodes quorate
        under different Ns; see :meth:`conflicting_sizes`), or a
        coordination-policy divergence (agreeing peers running a different
        ``distribution`` / ``electLeader`` and so picking different owners; see
        :meth:`conflicting_policies`).  All three fail the ``Leader`` gate
        closed.
        """
        return (
            bool(self.conflict_names())
            or bool(self.conflicting_sizes())
            or bool(self.conflicting_policies())
        )

    def leader_name(self) -> Optional[str]:
        """Elected leader as this node sees it, or ``None`` if not quorate.

        The quorum gate uses our full mutual live set; the elected name is the
        lowest among ourselves and the confirmed-quorate candidates
        (:meth:`_eligible_candidates`) -- bridge-discovered nodes so we defer
        across a bridge instead of double-leading, and never a peer we can tell
        is itself sub-quorum (which would stand a healthy majority down).
        """
        return elect_leader(
            self.node_name,
            self._agreeing_peer_names(),
            self.cluster_size(),
            self._eligible_candidates(),
        )

    def is_leader(self) -> bool:
        """Whether this node is the elected leader (quorate, lowest name)."""
        return self.leader_name() == self.node_name

    def available_leader_name(self) -> str:
        """Elected leader ignoring quorum (for the ``PreferLeader`` policy)."""
        return elect_available_leader(
            self.node_name, self._agreeing_peer_names()
        )

    def _instances_claiming(self, name: str) -> Set[str]:
        """Per-process ``instance_id``s we currently see claiming ``name``.

        Our own (when ``name`` is our nodeName) plus every non-stale peer that
        directly answers as ``name`` -- including a same-name
        ``STATUS_CONFLICT`` peer (a duplicate nodeName), whose instance id we
        recorded when we polled it.  Used to break a duplicate-nodeName tie in
        the never-skip ``available_*`` path.
        """
        instances: Set[str] = set()
        if name == self.node_name:
            instances.add(self.instance_id)
        for peer in self.view.peers.values():
            if peer.status in _STALE_STATUSES:
                continue
            if peer.node_name == name and peer.instance_id:
                instances.add(peer.instance_id)
        return instances

    def _wins_available_tiebreak(self) -> bool:
        """Whether *this process* wins its own nodeName among same-name peers.

        ``available_*`` decides the never-skip ``PreferLeader`` run on the
        lowest *nodeName*, which two processes sharing a duplicate nodeName
        both match -- so without a tiebreak both would run every
        ``PreferLeader`` job on a healthy, quorate cluster (the duplicate-name
        conflict gate does *not* protect ``PreferLeader``; it accepts double-
        runs only across a partition).  Break the tie on the per-process
        ``instance_id`` -- the same fence the lease backends self-recognise by
        -- so exactly one of the same-named processes runs.  The two duplicates
        poll each other (each sees the other as ``STATUS_CONFLICT`` carrying
        its instance id), so both compute the same lowest instance and agree on
        the winner; the lowest runs, the other defers.  (Residual: two same-
        named processes that cannot see each other -- an asymmetric peer list
        -- each believe they win and may double-run, the accepted
        ``PreferLeader``-across-a-partition behaviour.)
        """
        instances = self._instances_claiming(self.node_name)
        return not instances or self.instance_id == min(instances)

    def is_available_leader(self) -> bool:
        """Whether this node leads its reachable set, quorum or not."""
        return (
            self.available_leader_name() == self.node_name
            and self._wins_available_tiebreak()
        )

    def is_quorate(self) -> bool:
        """Whether this node currently sees a quorum (so it may run jobs)."""
        return self.leader_name() is not None

    # --- per-job ownership (distribution: spread) -------------------------

    def job_owner(self, job_name: str) -> Optional[str]:
        """Quorum-gated owner of ``job_name`` (spread mode), else ``None``.

        Like :meth:`leader_name`, the owner is the rendezvous winner over
        ourselves and the confirmed-quorate candidates
        (:meth:`_eligible_candidates`): bridged nodes agree on one owner
        instead of double-running it, and the owner is never a node that would
        itself stand down.
        """
        return elect_job_owner(
            job_name,
            self.node_name,
            self._agreeing_peer_names(),
            self.cluster_size(),
            self._eligible_candidates(),
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
        """Whether this node owns ``job_name`` in its reachable set.

        Like :meth:`is_available_leader`, this never-skip (spread
        ``PreferLeader``) path is broken on the per-process ``instance_id``
        when a duplicate nodeName would otherwise make two same-named processes
        both win the per-job rendezvous owner and double-run the job on a
        healthy cluster (the conflict gate does not protect ``PreferLeader``).
        """
        return (
            self.available_job_owner(job_name) == self.node_name
            and self._wins_available_tiebreak()
        )

    def view_dict(self) -> Dict[str, Any]:
        leader = self.leader_name()
        spread = self.distribution == "spread"
        conflicts = self.conflict_names()
        size_conflicts = self.conflicting_sizes()
        policy_conflicts = self.conflicting_policies()
        return {
            "backend": "gossip",
            "node_name": self.node_name,
            "job_set_id": self.get_job_set_id(),
            "cluster_size": self.cluster_size(),
            "quorum": self.quorum(),
            "elect_leader": bool(self.config.get("electLeader")),
            "distribution": self.distribution,
            # a conflict was detected: Leader jobs fail closed until it clears
            # (see has_conflict / cron._cluster_allows). "conflict" is the
            # umbrella flag (any kind); the lists below say which.
            "conflict": (
                bool(conflicts)
                or bool(size_conflicts)
                or bool(policy_conflicts)
            ),
            # a duplicate nodeName (two nodes would each elect themselves)
            "conflict_names": conflicts,
            # peers that agree on the job set but declare a different cluster
            # size N (two nodes quorate under different Ns -> split-brain)
            "size_conflict": bool(size_conflicts),
            "conflicting_sizes": size_conflicts,
            # agreeing peers running a different distribution / electLeader
            # (independent owner selectors -> double-run or lost-run)
            "policy_conflict": bool(policy_conflicts),
            "conflicting_policies": policy_conflicts,
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
