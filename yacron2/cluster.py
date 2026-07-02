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
cluster instead of piling onto one node.  It keeps the same quorum gate, so a
minority partition still stands down and two clean-partition majorities cannot
both own a job.  The bridge mitigation needs one extra step here, though,
because rendezvous breaks the property that saves single-leader: single-
leader's winner is always the one global-``min`` node, which everyone can see,
so a *thin* bridge (a pair sharing fewer than ``quorum - 1`` witnesses) rarely
hides it; ``spread``'s winner is *per job* and can be exactly such a thin-
bridged node, which some other quorate node cannot see and so self-owns -- a
double-run in a converged topology where single-leader elects exactly one
leader.  To close that gap a ``spread`` owner folds the co-owners a witness
*vouches quorate* (the nodes an agreeing peer reports in its
``quorate_vouched`` set -- its own ``_eligible_candidates`` -- that this node
cannot itself confirm; see :meth:`ClusterManager._unconfirmed_contenders`) into
the rendezvous set and defers to any that out-score it.  Two strict majorities
of one ``N`` always overlap, so a quorate node it cannot see is still vouched
to it by a shared member -- which is why this is sound: ``spread`` is then
at-most-once for ``Leader`` jobs, no weaker than single-leader.  The trade is
the fail-closed one single-leader already makes: a job whose rightful owner no
quorate peer can currently confirm stands down until the view converges.  The
fold is gated on ``quorate_vouched`` rather than the raw two-way edge for a
reason: a witness may have a single edge to a *sub-quorum* node, and deferring
the per-job owner to a node that then stands itself down on its own quorum gate
would run the job on *no* node -- a silent cluster-wide zero-run that the
raw-edge fold once caused.

``PreferLeader`` ``spread`` (the never-skip, quorum-less owner path) needs the
*same* convergence step, or two quorate nodes sharing a majority core but blind
to each other each self-own the per-job winner and double-run it on a healthy
cluster -- weaker than single-leader ``PreferLeader``, whose global-``min``
winner both can see.  It folds the reachable co-owners a witness vouches a
two-way edge to (:meth:`ClusterManager._available_contenders`), using the *raw*
edge, not ``quorate_vouched``: with no quorum gate a sub-quorum node still runs
the jobs it owns, so it is a legitimate co-owner, and a rendezvous winner has
no gate to stand itself down on (the global-max node always self-owns), so this
fold can only de-duplicate a double-run, never cause a zero-run.
"""

import asyncio
import datetime
import hashlib
import json
import logging
import math
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

# Completed poll rounds after which the never-skip available_* gates stop
# holding for a reachable AGREED peer that has not yet attested this (new)
# instance back (see ClusterManager._view_settled). Mutual attestation after
# a (re)start needs the peer to poll our fresh instance_id and us to then
# re-poll the peer, which completes within two full intervals; a peer still
# not attesting after three rounds is a genuinely one-way link, and the
# never-skip contract then leans toward running (a possible double-run)
# rather than standing this node down indefinitely.
_SETTLE_ROUNDS = 3

# Cap on the peer /peer response we will buffer per poll. The legitimate
# payload is a small JSON object (a fixed header plus one short member entry
# per node), so this is generous for clusters into the hundreds of nodes while
# bounding the memory a misbehaving-but-CA-trusted peer can force us to
# allocate each round (see _read_capped / _poll_peer).
MAX_PEER_RESPONSE_BYTES = 256 * 1024
_READ_CHUNK = 8192

# Per-field bounds on a CA-vouched-but-untrusted peer's /peer payload. The byte
# cap above bounds a single response, but the @reboot-ran set is PERSISTENT and
# re-advertised (advertised_ran_jobs), so without a per-set bound a peer could
# push names that accumulate and re-broadcast until OUR /peer response exceeds
# the byte cap -- which honest peers then reject as oversized, dropping us from
# their quorum (a cluster-wide availability DoS). These cap the cardinality and
# per-string length of the absorbed/re-emitted sets so a node never emits a
# response that overflows the cap, and reject over-long / control-character
# scalar identity fields (which would otherwise be reflected verbatim into the
# /cluster JSON and logs).
MAX_PEER_FIELD_LEN = 256  # node_name, job_set_id, instance_id, ...
MAX_MEMBER_ENTRIES = 4096  # members[] / mutual_agreeing[] cardinality
MAX_REBOOT_JOB_NAME_LEN = 128  # a single @reboot job name
MAX_ADVERTISED_REBOOT_JOBS = 512  # ran-set cardinality stored + re-advertised

# Bounds on the fleet-view job_summaries block gossiped in /peer. Unlike
# ran_reboot_jobs, absorbed peer summaries are never re-advertised (each node
# only ever emits its OWN scheduler's snapshot), so these bound two different
# things: what we EMIT -- a node with a huge job set must not push its own
# /peer response past MAX_PEER_RESPONSE_BYTES, which honest peers reject as
# oversized and would drop it from THEIR quorum -- and what we STORE from a
# CA-vouched-but-untrusted peer. The budget: a summary entry is the job name
# plus ~150 bytes of fixed-shape fields, so 512 entries of <=128-char names is
# comfortably under half the byte cap even before compression, leaving the
# members list (one short entry per node) plenty of headroom.
MAX_JOB_SUMMARY_NAME_LEN = 128  # a single job name in job_summaries
MAX_ADVERTISED_JOB_SUMMARIES = 512  # per-node job_summaries cardinality
MAX_JOB_SUMMARY_TS_LEN = 64  # an ISO-8601 finished_at timestamp

# the only run outcomes a peer summary may carry (mirrors JobRunInfo.outcome)
_SUMMARY_OUTCOMES = frozenset({"success", "failure", "cancelled"})


def _parse_members(
    raw: Any,
    *,
    max_len: Optional[int] = None,
    max_items: Optional[int] = None,
) -> List["tuple[str, str, bool]"]:
    """Validate a peer's reported ``members`` list, dropping malformed entries.

    A peer is CA-vouched but otherwise untrusted input, so anything that is not
    a list of ``{node_name: str, instance_id: str, agreed: bool}`` objects is
    ignored: a malformed or hostile payload degrades to "no mutual/transitive
    information" rather than poisoning the election (see the type checks in
    :meth:`ClusterManager._poll_peer`). ``max_len`` drops entries whose
    name/instance exceeds it, and ``max_items`` caps the list length, so a
    hostile peer cannot force unbounded conflict-detection work.
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
            if max_len is not None and (
                len(name) > max_len or len(instance) > max_len
            ):
                continue
            # Drop control-character names/instances: a transitive member's
            # node_name flows (via conflict_names) into operator-facing log
            # lines, so a newline/ANSI-bearing value from a CA-vouched-but-
            # hostile peer is a log-injection vector. Mirror the isprintable()
            # guard _poll_peer applies to a peer's own scalar identity fields.
            if not (name.isprintable() and instance.isprintable()):
                continue
            members.append((name, instance, agreed))
            if max_items is not None and len(members) >= max_items:
                break
    return members


def _parse_str_list(
    raw: Any,
    *,
    max_len: Optional[int] = None,
    max_items: Optional[int] = None,
) -> "set[str]":
    """Validate an untrusted JSON value as a set of strings, dropping the rest.

    Used for the gossiped ``ran_reboot_jobs`` set and the ``mutual_agreeing``
    set (the latter feeds bridge confirmation in
    :meth:`ClusterManager._bridge_candidates`); like _parse_members, hostile or
    malformed input degrades to an empty set rather than raising, and a peer
    that omits the field (an older build) parses to an empty set -- the safe
    direction (it simply contributes no evidence). ``max_len`` drops over-long
    strings and ``max_items`` caps the set size, bounding what a CA-vouched
    peer can make us store and re-broadcast.
    """
    if not isinstance(raw, list):
        return set()
    out: "set[str]" = set()
    for item in raw:
        if not isinstance(item, str):
            continue
        if max_len is not None and len(item) > max_len:
            continue
        # Drop control-character entries: a gossiped ran_reboot_jobs name can
        # reach operator logs, so reject a newline/ANSI-bearing value from a
        # CA-vouched-but-hostile peer (see _parse_members / _poll_peer).
        if not item.isprintable():
            continue
        out.add(item)
        if max_items is not None and len(out) >= max_items:
            break
    return out


def _finite_number(value: Any) -> Optional[float]:
    """An untrusted JSON value as a finite float, else ``None``.

    Rejects bools (an int subclass) and non-finite floats: Python's json
    module happily parses ``Infinity``/``NaN`` AND re-emits them, but they are
    not valid JSON -- a hostile peer could otherwise plant one in a summary
    and make our /fleet response unparseable to every browser (JSON.parse
    rejects it), blanking the dashboard's fleet view cluster-wide.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    out = float(value)
    return out if math.isfinite(out) else None


def _parse_job_summaries(raw: Any) -> Optional[Dict[str, Dict[str, Any]]]:
    """Validate a peer's gossiped ``job_summaries`` block, field by field.

    A peer is CA-vouched but otherwise untrusted, so every field is
    type-checked and re-built into a fresh dict of the exact expected shape
    (never stored as-received): a malformed entry is dropped, a malformed
    field degrades to its absent value, and anything else -- extra keys,
    nested junk -- is simply not copied. Job names are length-capped,
    control-character-free (they are reflected into the authenticated
    GET /fleet JSON) and the entry count is capped, mirroring the other
    absorbed peer sets. Returns ``None`` (not ``{}``) when the field is
    absent or not an object, so "an older build that gossips no summaries"
    stays distinguishable from "a node with zero jobs" in the fleet view.
    """
    if not isinstance(raw, dict):
        return None
    out: Dict[str, Dict[str, Any]] = {}
    for name, entry in raw.items():
        if (
            not isinstance(name, str)
            or not name
            or len(name) > MAX_JOB_SUMMARY_NAME_LEN
            or not name.isprintable()
            or not isinstance(entry, dict)
        ):
            continue
        summary: Dict[str, Any] = {
            "running": entry.get("running") is True,
            "enabled": entry.get("enabled") is not False,
            "scheduled_in": _finite_number(entry.get("scheduled_in")),
            "last": None,
        }
        last = entry.get("last")
        if isinstance(last, dict):
            outcome = last.get("outcome")
            finished_at = last.get("finished_at")
            exit_code = last.get("exit_code")
            if (
                outcome in _SUMMARY_OUTCOMES
                and isinstance(finished_at, str)
                and len(finished_at) <= MAX_JOB_SUMMARY_TS_LEN
                and finished_at.isprintable()
            ):
                summary["last"] = {
                    "outcome": outcome,
                    "finished_at": finished_at,
                    "duration": _finite_number(last.get("duration")),
                    "exit_code": (
                        exit_code
                        if isinstance(exit_code, int)
                        and not isinstance(exit_code, bool)
                        else None
                    ),
                }
        out[name] = summary
        if len(out) >= MAX_ADVERTISED_JOB_SUMMARIES:
            break
    return out


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
    # whether the peer's last /peer response actually carried a ``members``
    # list (a current build) rather than omitting the field (a legacy build
    # from before mutual attestation -- mid rolling upgrade). A legacy peer
    # cannot report whether it sees us, so requiring the mutual gate for it
    # would drop it from our live set and stand a NEW node DOWN among legacy
    # peers -- a cluster-wide Leader halt. We instead fall back to one-
    # directional agreement for such a peer (see _agreeing_peers): the
    # documented "lean toward running" rolling-upgrade behaviour rather than a
    # silent stand-down. Defaults True -- the STRICT direction (mutual
    # gate) -- so a peer is only ever treated as legacy when _observe_peer
    # POSITIVELY sees no members list; record_failure resets it. Internal, like
    # ``members``; not surfaced in to_dict.
    reports_members: bool = True
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
    # the names the peer reports it can itself *confirm are quorate* -- its own
    # _eligible_candidates (the nodes it would elect / defer to). Unlike
    # ``mutual_agreeing`` (every node the peer has a two-way edge with, quorate
    # or not), this is the peer's vouch that the named node has a quorum of its
    # own mutual agreers, so it will actually run if elected. It is the load-
    # bearing input to the ``spread`` Leader-path owner fold
    # (_unconfirmed_contenders): folding a node a single peer merely has an
    # edge to -- but that is itself sub-quorum -- would make every quorate node
    # defer to a node that then stands down, a silent cluster-wide zero-run.
    # Folding only quorate-vouched names keeps the per-job owner runnable. None
    # when no fresh result, or an older peer that omits it (which then vouches
    # nothing -- the safe direction: it cannot cause a zero-run, only forgo a
    # deferral, i.e. lean toward running like the rest of the upgrade path).
    quorate_vouched: Optional["set[str]"] = None
    # the peer's advertised per-job run summaries (its scheduler's snapshot),
    # feeding the fleet view (GET /fleet). Observability only -- never an
    # election or safety input. Unlike members/mutual_agreeing (which a stale
    # read could poison), this is deliberately NOT cleared on a failed poll:
    # the fleet view shows a briefly-unreachable node's last-known state aged
    # by last_seen rather than blanking it. None = never reported (an older
    # build, or never successfully polled). Internal; not in to_dict (the
    # fleet view has its own shape, see ClusterManager.fleet_view).
    job_summaries: Optional[Dict[str, Dict[str, Any]]] = None
    # whether the peer said it truncated its advertised summaries at its cap
    # (a node with more jobs than MAX_ADVERTISED_JOB_SUMMARIES), so the fleet
    # view can label that node's column as partial instead of implying the
    # missing jobs do not exist there.
    job_summaries_truncated: bool = False

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
        peer_quorate_vouched: Optional["set[str]"] = None,
        peer_distribution: Optional[str] = None,
        peer_elect_leader: Optional[bool] = None,
        peer_reports_members: bool = True,
        peer_job_summaries: Optional[Dict[str, Dict[str, Any]]] = None,
        peer_job_summaries_truncated: bool = False,
    ) -> None:
        peer = self.peers[host]
        peer.last_seen = now
        peer.last_error = None
        peer.job_set_id = peer_id
        peer.node_name = peer_name
        peer.instance_id = peer_instance
        peer.members = peer_members
        # fleet-view summaries: None (an older build that gossips none) leaves
        # any previously-absorbed snapshot in place -- like a failed poll, the
        # fleet view prefers last-known-aged-by-last_seen over blanking -- so
        # only a real report overwrites.
        if peer_job_summaries is not None:
            peer.job_summaries = peer_job_summaries
            peer.job_summaries_truncated = peer_job_summaries_truncated
        # whether this response actually carried a members list (current build)
        # or omitted it (a legacy peer mid rolling upgrade); drives the one-
        # directional fallback in _agreeing_peers. Defaults True so existing
        # callers/tests keep the mutual gate; _observe_peer passes the real
        # value (isinstance(data["members"], list)).
        peer.reports_members = peer_reports_members
        peer.ran_reboot_jobs = peer_ran_reboot_jobs
        peer.declared_size = peer_size
        peer.mutual_agreeing = peer_mutual_agreeing
        peer.quorate_vouched = peer_quorate_vouched
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
                # Do NOT reset mismatch_streak here: it counts reachable
                # mismatches for the drift hysteresis, and the invariant (see
                # record_failure) is that only a confirmed AGREED/SELF
                # observation clears it. CONFLICT is neither, so a transient
                # same-name/different-instance answer at one address must not
                # zero a genuinely-drifting peer's streak and delay its
                # STATUS_DRIFTED label by up to driftAfter rounds. The conflict
                # itself fails the Leader gate closed regardless of the streak.
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
        peer.quorate_vouched = None
        # job_summaries is deliberately KEPT: it is observability-only (never
        # an election input), and the fleet view shows a briefly-unreachable
        # node's last-known state aged by last_seen instead of blanking it.
        # no fresh response this round, so make no legacy/current claim about
        # the peer's members field either (it is not AGREED while failed, so
        # _agreeing_peers skips it regardless; reset for tidiness).
        peer.reports_members = False

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

    A holder of a CA-signed cert is a trusted *member*; defending against a
    hostile or buggy member is out of scope (the Byzantine note in the module
    docstring). Such a member CAN force a fail-closed ``Leader`` stand-down:
    the conflict gates (duplicate ``nodeName``, a divergent cluster size or
    coordination policy) deliberately fail *closed* on any divergence so two
    nodes never both lead, and a single member declaring a divergent size or
    policy -- a first-party report about *itself* -- trips them. The
    ``nodeName`` collision gate corroborates a purely *transitive* (hearsay)
    report across two peers (see :meth:`ClusterManager.conflict_names`), but a
    first-party divergence is credited from that one member by design: gating
    it on corroboration would re-open the split-brain the gate exists to close
    (two members each declaring a different N, each seeing only the other,
    would then neither stand down). So the trade-off is a member-level
    availability DoS, never a correctness break (never a double-run); a hostile
    member can likewise push ``reboot-ran`` suppression and read topology. (A
    future opt-in could pin the client cert SAN/CN against an allowed-name
    list; not enabled by default because a peer's cert SAN has no required
    relationship to its listed address.)
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


def gossip_tls_loadable(cluster_config: ClusterConfig) -> bool:
    """Whether the gossip backend's TLS material in ``cluster_config`` loads.

    A side-effect-free dry-run of what :meth:`ClusterManager.__init__` does
    (build the client + server SSL contexts from the on-disk CA/cert/key), used
    by :meth:`yacron2.cron.Cron.start_stop_cluster` BEFORE it tears the running
    manager down for a CONFIG change. It covers a config edit (peers/listen)
    that coincides with an in-flight cert rotation (cert-manager / Vault /
    Kubernetes secret refresh briefly leaves a half-written or absent file):
    tearing the old manager down and then failing to rebuild the new one on the
    bad file would wedge ``Leader`` / ``PreferLeader`` closed for up to a
    reload, the very window the cert-only make-before-break already guards.

    Returns ``True`` for any non-gossip backend, and a gossip config with no
    ``tls`` block (nothing on disk to pre-validate), so it only ever defers a
    gossip cert-rotation race. Unlike :meth:`ClusterManager.tls_files_loadable`
    (which validates the *running* manager's paths), this validates the
    *incoming* config, since a config edit can repoint at different cert files
    the old manager cannot speak to.
    """
    if cluster_config.get("backend", "gossip") != "gossip":
        return True
    tls = cluster_config.get("tls")
    if not tls:
        return True
    try:
        build_client_ssl_context(tls)
        build_server_ssl_context(tls)
    except (OSError, ssl.SSLError):
        return False
    return True


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
        # completed peer-poll rounds since this manager was built. A rebuilt
        # manager mints a fresh instance_id, so peers cannot attest it back
        # until they have re-polled it (~1-2 intervals); this counter bounds
        # the convergence hold _view_settled() places on the never-skip
        # available_* gates during that window.
        self._poll_rounds = 0
        # emit-once latch for the degenerate 2-of-2 self-listing warning
        # (see _maybe_warn_degenerate_self_listing).
        self._warned_degenerate_self = False
        # the scheduler's per-job run-summary snapshot callable, piggybacked
        # on the /peer response for the fleet view (installed by
        # Cron.start_stop_cluster before start(); None until then, and /peer
        # then simply advertises no summaries).
        self._job_summaries_provider: Optional[
            Callable[[], Dict[str, Any]]
        ] = None

    def set_job_summaries_provider(
        self, provider: Callable[[], Dict[str, Any]]
    ) -> None:
        self._job_summaries_provider = provider

    def _advertised_job_summaries(
        self,
    ) -> "tuple[Dict[str, Any], bool]":
        """Our own gossiped per-job summaries: ``(block, truncated)``.

        The provider's snapshot is local, trusted input (it comes from our own
        config and scheduler state), so no field validation happens here --
        only the emit-side caps that keep our /peer response under
        MAX_PEER_RESPONSE_BYTES: entries beyond MAX_ADVERTISED_JOB_SUMMARIES
        are dropped (deterministically, by sorted name, so the advertised
        subset is stable across rounds rather than flapping) and over-long
        names are skipped. Truncation is flagged so the fleet view can label
        this node's data as partial.
        """
        provider = self._job_summaries_provider
        if provider is None:
            return {}, False
        summaries = provider()
        names = sorted(
            name for name in summaries if len(name) <= MAX_JOB_SUMMARY_NAME_LEN
        )
        truncated = len(names) > MAX_ADVERTISED_JOB_SUMMARIES or len(
            names
        ) < len(summaries)
        return {
            name: summaries[name]
            for name in names[:MAX_ADVERTISED_JOB_SUMMARIES]
        }, truncated

    # --- the mTLS /peer server -------------------------------------------

    async def _handle_peer(self, request: web.Request) -> web.Response:
        job_summaries, summaries_truncated = self._advertised_job_summaries()
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
                # Capped so this response can never exceed MAX_PEER_RESPONSE_
                # BYTES even if an upstream peer's set was inflated (the
                # membership test reboot_ran() still uses the full union).
                "ran_reboot_jobs": sorted(self.advertised_ran_jobs())[
                    :MAX_ADVERTISED_REBOOT_JOBS
                ],
                # the peers we *mutually* agree with: a poller uses this as the
                # sound evidence that a node it reaches only transitively is
                # itself quorate (a witnessed two-way edge), driving the
                # bridge-discovery deferral; see _bridge_candidates. Distinct
                # from the one-directional ``agreed`` flags in ``members``.
                "mutual_agreeing": sorted(self._agreeing_peer_names()),
                # the names WE can confirm are themselves quorate (our
                # _eligible_candidates): the nodes we would elect / defer to.
                # A poller folds these -- not the raw mutual_agreeing -- into
                # its ``spread`` Leader-path owner set, so it only ever defers
                # a job to a node vouched able to run it (see PeerState.
                # quorate_vouched / _unconfirmed_contenders). Stronger than
                # mutual_agreeing, which lists every two-way edge including
                # ones to sub-quorum nodes that stand a deferred job down.
                "quorate_vouched": sorted(self._eligible_candidates()),
                # this node's per-job run summaries (the scheduler's snapshot:
                # running/enabled/next-fire plus the last finished run), for
                # the polling peer's fleet view. Observability only -- a peer
                # never feeds these into election or run/skip decisions --
                # and capped so a huge job set cannot push this response past
                # MAX_PEER_RESPONSE_BYTES (see _advertised_job_summaries).
                "job_summaries": job_summaries,
                "job_summaries_truncated": summaries_truncated,
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
            self._ran_reboot_jobs |= _parse_str_list(
                data.get("names"),
                max_len=MAX_REBOOT_JOB_NAME_LEN,
                max_items=MAX_ADVERTISED_REBOOT_JOBS,
            )
            # Bound the PERSISTENT set so a peer cannot grow it (and the /peer
            # response that re-advertises it) past the byte cap and collapse
            # quorum cluster-wide. Dropping excess marks only risks rerunning a
            # one-shot (the documented PreferLeader envelope), never an outage.
            if len(self._ran_reboot_jobs) > MAX_ADVERTISED_REBOOT_JOBS:
                self._ran_reboot_jobs = set(
                    sorted(self._ran_reboot_jobs)[:MAX_ADVERTISED_REBOOT_JOBS]
                )
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
            self._runner = runner
            # one session for the manager's lifetime: peer polls and reboot-ran
            # pushes reuse it (and its kept-alive mTLS connections) instead of
            # opening a fresh session -- and re-handshaking -- every round.
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(
                    total=self.config["connectTimeout"]
                )
            )
            logger.info(
                "cluster: node %r serving mTLS /peer on %s, polling %d "
                "peer(s) every %ds",
                self.node_name,
                self.config["listen"],
                len(self.config["peers"]),
                self.config["interval"],
            )
            # Run one full poll round up front so the never-skip available_*
            # gates and reboot_ran() reflect a real read of the peers BEFORE
            # the first spawn_jobs, mirroring the lease backends' inline
            # store round (see backends/etcd.py / backends/kubernetes.py
            # start()). Without it the view is never-polled for the whole
            # startup pass: every node sees only itself, so every PreferLeader
            # job runs on every node at boot (and on every reload that
            # rebuilds the manager), and a restarted node re-runs deferred
            # @reboot one-shots its peers' ran_reboot_jobs gossip would have
            # retired. Bounded by connectTimeout (the per-peer polls run
            # concurrently) and best-effort: a failed round records the peers
            # unreachable -- the genuine "peer down" state the gates already
            # price in.
            try:
                await self._poll_all()
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive, as _poll_loop
                logger.exception("cluster: initial peer poll round failed")
            self._poll_task = asyncio.create_task(self._poll_loop())
        except BaseException:
            # bad listen address (ValueError) or bind failure (OSError, e.g.
            # the port is already in use) after the runner was set up -- and a
            # failure creating the session or poll task, and cancellation --
            # must not leak the half-started runner/session/task.
            if self._poll_task is not None:
                self._poll_task.cancel()
                self._poll_task = None
            if self._session is not None:
                await self._session.close()
                self._session = None
            await runner.cleanup()
            self._runner = None
            raise

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
        # one full round completed: every configured peer now carries a real
        # observation (success or failure); feeds _view_settled()'s bound.
        self._poll_rounds += 1
        # Re-run the degenerate-self check with the round's full information:
        # at the SELF transition itself a coexisting multi-homed duplicate
        # may not have been deduped yet (cluster_size() one larger for that
        # instant), and a transition-only check would then never fire.
        self._maybe_warn_degenerate_self_listing()

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
        elif new == STATUS_SELF:
            # a self-listing config-time dedup could not catch (e.g. this node
            # listed by its own IP under a wildcard listen; see
            # config._is_self_listed) was just identified by its self-poll.
            # The entry is excluded from cluster_size(), so the declared N was
            # one larger than the real cluster -- which, at the boundary,
            # means the config sailed past the electLeader size==2 refusal and
            # the cluster is really the degenerate 2-real-node mode it exists
            # to forbid. That case gets a prominent warning; any other
            # self-listing is benign and logged once at INFO. The degenerate
            # check also re-runs at the end of every poll round (_poll_all):
            # at this instant a coexisting multi-homed duplicate peer may not
            # be deduped yet (cluster_size() transiently one larger), and a
            # transition-only check would downgrade the warning to this INFO
            # line forever.
            if not self._maybe_warn_degenerate_self_listing():
                logger.info(
                    "cluster: peer %s is this node itself (a self-listing); "
                    "excluded from the cluster size",
                    host,
                )
        elif new == STATUS_AGREED and prev != STATUS_SELF:
            logger.info("cluster: peer %s now agreed", host)

    def _maybe_warn_degenerate_self_listing(self) -> bool:
        """Warn (once) when a runtime-identified self-listing leaves the
        effective ``electLeader`` cluster at 2 real nodes -- the degenerate
        quorum-2-of-2 mode the config-time size==2 refusal exists to forbid
        (both nodes must be up; any single failure stops all Leader jobs
        cluster-wide, strictly worse than a single replica).

        Returns whether the warning fired *now* (the caller then skips its
        benign INFO line).  Evaluated at the SELF transition AND at the end
        of every poll round: at the transition instant a coexisting
        multi-homed duplicate peer may not be deduped yet, so
        ``cluster_size()`` can transiently read one larger; the round-end
        re-check sees the fully-deduped size and still fires.  Emit-once via
        ``_warned_degenerate_self`` (the ``self_confirmed`` status latch
        alone is not enough once the check re-runs every round).
        """
        if self._warned_degenerate_self:
            return False
        if not bool(self.config.get("electLeader")):
            return False
        self_hosts = [
            host
            for host, peer in self.view.peers.items()
            if peer.status == STATUS_SELF
        ]
        if not self_hosts or self.cluster_size() != 2:
            return False
        self._warned_degenerate_self = True
        logger.warning(
            "cluster: peer %s is this node itself (a self-listing "
            "not recognisable at config load), so the effective "
            "electLeader cluster is 2 nodes with a quorum of 2 -- "
            "the degenerate mode the 2-node refusal exists to "
            "forbid: BOTH nodes must be up for either to run, so "
            "any single failure stops all Leader jobs cluster-wide "
            "(strictly worse than a single replica). Remove the "
            "self entry from cluster.peers, or grow the cluster to "
            "3+ nodes.",
            ", ".join(self_hosts),
        )
        return True

    async def _observe_peer(
        self, session: aiohttp.ClientSession, host: str, my_id: str
    ) -> None:
        url = "https://{}/peer".format(host)
        now = datetime.datetime.now(datetime.timezone.utc)
        try:
            # allow_redirects=False: a legitimate peer endpoint never
            # redirects; following one would let a CA-vouched-but-hostile
            # peer pivot us into an attacker-chosen target (SSRF) or a
            # plaintext http:// downgrade where the mTLS client context
            # (ssl=) no longer applies.
            async with session.get(
                url, ssl=self._client_ssl, allow_redirects=False
            ) as resp:
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
            # Bound the length and reject control characters: these strings are
            # stored in PeerState and reflected verbatim into the authenticated
            # GET /cluster JSON and into log lines, so an over-long or
            # newline-bearing value from a CA-vouched-but-hostile peer is a
            # payload-bloat / log-injection vector. Legitimate identities
            # (hostnames, hashes, uuids, "single-leader"/"spread", scheme
            # tokens like "v2") are short and printable, so this rejects
            # nothing real.
            if value is not None and (
                len(value) > MAX_PEER_FIELD_LEN or not value.isprintable()
            ):
                self.view.record_failure(
                    host,
                    "malformed /peer response: {!r} is over {} chars or "
                    "contains control characters".format(
                        key, MAX_PEER_FIELD_LEN
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
            peer_members=_parse_members(
                data.get("members"),
                max_len=MAX_PEER_FIELD_LEN,
                max_items=MAX_MEMBER_ENTRIES,
            ),
            # whether the peer sent a members list (a current build) or
            # omitted the field (a legacy build, pre-mutual-attestation). A
            # legacy peer cannot confirm it sees us, so _agreeing_peers falls
            # back to one-directional agreement rather than standing this
            # node down mid rolling upgrade. A non-list (null / hostile) is
            # treated as "not reported" -- a CA-vouched peer's junk is the
            # documented out-of-scope Byzantine case either way.
            peer_reports_members=isinstance(data.get("members"), list),
            peer_ran_reboot_jobs=_parse_str_list(
                data.get("ran_reboot_jobs"),
                max_len=MAX_REBOOT_JOB_NAME_LEN,
                max_items=MAX_ADVERTISED_REBOOT_JOBS,
            ),
            peer_size=size,
            peer_distribution=fields["distribution"],
            peer_elect_leader=elect,
            # An older build that omits the field, or a peer reporting an empty
            # set, both parse to an empty set here: either way it is not
            # confirmed quorate, so _eligible_candidates won't elect it. (The
            # PeerState default None -- never polled -- is treated the same.)
            peer_mutual_agreeing=_parse_str_list(
                data.get("mutual_agreeing"),
                max_len=MAX_PEER_FIELD_LEN,
                max_items=MAX_MEMBER_ENTRIES,
            ),
            # the peer's vouch of which nodes it confirms quorate (its
            # _eligible_candidates). An older build that omits the field, or a
            # peer reporting an empty set, both parse to an empty set: it then
            # vouches no transitive owner, so _unconfirmed_contenders folds
            # nothing from it -- the safe direction (lean toward running, never
            # a zero-run). Capped like the other absorbed identity sets.
            peer_quorate_vouched=_parse_str_list(
                data.get("quorate_vouched"),
                max_len=MAX_PEER_FIELD_LEN,
                max_items=MAX_MEMBER_ENTRIES,
            ),
            # the peer's per-job run summaries for the fleet view, re-built
            # field-by-field from the untrusted payload (see
            # _parse_job_summaries). None (absent/malformed -- an older build)
            # keeps any previously-absorbed snapshot rather than blanking the
            # node in the fleet view.
            peer_job_summaries=_parse_job_summaries(data.get("job_summaries")),
            peer_job_summaries_truncated=(
                data.get("job_summaries_truncated") is True
            ),
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
        # capped like the /peer serialization so a push body cannot exceed the
        # receiver's MAX_PEER_RESPONSE_BYTES (else rejected as oversized).
        names = sorted(self.advertised_ran_jobs())[:MAX_ADVERTISED_REBOOT_JOBS]
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
            # allow_redirects=False: see _observe_peer -- a peer redirect would
            # replay this payload (job_set_id + names) to an attacker-chosen
            # target over a possibly-plaintext connection.
            async with session.post(
                url,
                json=payload,
                ssl=self._client_ssl,
                allow_redirects=False,
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
        # An address->instance binding learned while the peer was alive keeps
        # deduping while the peer is UNREACHABLE/UNTRUSTED (record_failure
        # retains instance_id): a node's identity at an address it once
        # answered does not change because it stopped answering, exactly the
        # rationale behind the STATUS_SELF self_confirmed latch. Skipping
        # stale entries here would drop the dedup the moment the multi-homed
        # node dies, inflating N -- and quorum_size(N) -- in lockstep on every
        # survivor for the whole outage (no size conflict is surfaced, they
        # all inflate identically), standing a healthy true majority down
        # precisely when the declared fault tolerance is being exercised. The
        # binding self-corrects: a successful poll of a reassigned address
        # records the new instance and stops the dedup.
        seen_instances: Set[str] = set()
        duplicate_instances = 0
        for peer in self.view.peers.values():
            if peer.status == STATUS_SELF:
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

        A peer that agrees on the job set but declares a *different*
        coordination policy (``distribution`` or ``electLeader``) is excluded
        for exactly the same reason, symmetric with the size gate.  Such a peer
        is a policy conflict (:meth:`conflicting_policies`) that already fails
        our *direct*-witness ``Leader`` gate closed -- but that gate only fires
        for a peer we see directly.  If we did *not* drop it from the gossiped
        ``mutual_agreeing`` set, a third node that reaches it only across a
        bridge (and so never sees the policy conflict itself) would
        bridge-confirm it as quorate and elect/defer across a node coordinating
        by different rules, the very double-run / silent-skip that
        :meth:`conflicting_policies` exists to prevent.  As with size, a peer
        too old to declare a policy field (``None``) contributes no divergence
        evidence and is not excluded -- the safe direction.
        """
        my_size = self.cluster_size()
        my_elect = bool(self.config.get("electLeader"))
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
                # mutual gate: count a current peer only if its response shows
                # it sees us AGREED too. A LEGACY peer (no members field, mid
                # rolling upgrade) cannot report this, so fall back to one-
                # directional agreement for it -- otherwise a new node among
                # legacy peers counts zero agreers, drops below quorum, and
                # stands every Leader job down cluster-wide. Counting it here
                # leans the node toward running (it still won't *defer* to a
                # legacy peer -- _eligible_candidates needs mutual_agreeing it
                # also lacks -- so elects itself: the documented "lean toward
                # running, possible double-run" upgrade behaviour, not a halt).
                and (
                    _peer_sees_me_agreed(peer.members, self.instance_id)
                    or not peer.reports_members
                )
                and not (
                    peer.declared_size is not None
                    and peer.declared_size != my_size
                )
                and not (
                    peer.declared_distribution is not None
                    and peer.declared_distribution != self.distribution
                )
                and not (
                    peer.declared_elect_leader is not None
                    and peer.declared_elect_leader != my_elect
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

    def _unconfirmed_contenders(self) -> List[str]:
        """Quorate co-owners a peer *vouches* for that we cannot confirm
        ourselves -- folded into the ``spread`` ``Leader`` owner gate so it
        never double-runs (nor zero-runs) a job across a *thin* bridge.

        We fold a peer's :attr:`PeerState.quorate_vouched` set (the nodes that
        peer can confirm quorate -- its own :meth:`_eligible_candidates`,
        gossiped on ``/peer``), **not** its raw ``mutual_agreeing``.  That
        distinction is load-bearing.  ``mutual_agreeing`` lists every node a
        peer has a two-way edge with, quorate or not; a witness ``W`` may have
        a single edge to a *sub-quorum* node ``S`` (``S``'s own live set is
        below quorum, so ``S`` never runs a ``Leader`` job).  Folding ``S`` in
        -- as the older raw-edge version did -- lets ``_hrw_owner`` pick ``S``
        as the per-job owner, so every quorate node defers to ``S`` and ``S``
        then stands itself down on its own quorum gate: the job runs on *no*
        node, a silent cluster-wide zero-run, in a converged topology where
        single-leader runs it exactly once.  ``quorate_vouched`` is ``W``'s
        vouch that it has seen ``S`` carry a quorum of mutual agreers, so a
        folded contender is always a node that will actually run if it wins.

        :meth:`_eligible_candidates` only *adds* a transitively-reached node
        when a quorum of witnesses vouches for it; a node witnessed by fewer (a
        thin bridge) is dropped, and :func:`elect_leader` then leans this node
        toward leading rather than standing down behind an unconfirmable peer
        (the single-leader liveness choice).  That under-counting is exactly
        what made ``spread`` *strictly weaker* than single-leader: single-
        leader's winner is always the one global-``min`` node, reachable by
        everyone, so a thin bridge rarely hides it; ``spread``'s rendezvous
        winner is *per job* and can be precisely such a thin-bridged node,
        which some other quorate node then cannot see -- so it self-owns and
        the job double-runs even where single-leader elects exactly one leader.

        The cure reuses the bridge gossip, now quorum-qualified.  Two strict
        majorities of one ``N`` cannot be disjoint, so any *other* quorate node
        ``Z`` we cannot see still shares a mutually-agreeing member ``W`` with
        us, and ``W`` -- polling ``Z`` directly -- sees ``Z`` carry a quorum
        of mutual agreers and so lists ``Z`` in the ``quorate_vouched`` set it
        gossips.  We therefore treat every name an agreeing peer *vouches
        quorate* -- that we do not already account for (ourselves, a directly-
        agreeing peer, or a confirmed bridge node) -- as a *possible* co-owner.
        Folded into :meth:`job_owner`'s rendezvous set, such a possible that
        out-scores us for a job makes us *defer* (fail closed) rather than risk
        double-running it; one that scores below us cannot displace us, so the
        only liveness lost is for a job whose owner no quorate peer can
        currently confirm -- the same fail-closed trade single-leader already
        makes.  Because the fold is gated on ``quorate_vouched`` (not the raw
        two-way edge), the node we defer to is one a witness confirmed quorate,
        so it *will* run -- closing the zero-run the raw-edge fold opened.  A
        node *no* agreeing peer vouches quorate is deliberately omitted: by the
        disjoint-majorities argument a quorate ``Z`` is always vouched by a
        shared witness, so the only names dropped are sub-quorum ones that
        would have stood a job down (a crashed or partitioned node never stands
        our jobs down).
        """
        agreeing = self._agreeing_peers()
        # Names we have already placed: ourselves, every directly-agreeing
        # peer (whether we confirmed it quorate or saw it sub-quorum -- either
        # way it is in our view and will not silently out-own us), and every
        # bridge-confirmed candidate.  Anything left a peer vouches quorate
        # is a node we cannot ourselves confirm but a witness can, so it is
        # treated as a possible co-owner.
        accounted = {self.node_name}
        accounted |= {peer.node_name for peer in agreeing if peer.node_name}
        accounted |= set(self._eligible_candidates())
        possible: Set[str] = set()
        for peer in agreeing:
            for name in peer.quorate_vouched or ():
                if name and name not in accounted:
                    possible.add(name)
        return sorted(possible)

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

        RESIDUAL (restart-window false *positive*): the symmetric case also
        exists. A node restart mints a new ``instance_id`` at the same address,
        so just after node-x restarts (x1 -> x2) a peer we already re-polled
        reports x2 first-party while two peers not yet re-polled still
        gossip stale x1 transitively -- two credible instances for one name,
        flagged as a conflict that fails this node's ``Leader`` gate closed for
        up to ~1--2 poll intervals until those peers refresh. It is transient,
        self-healing, and fail-closed (a missed firing, never a double-run). It
        is deliberately NOT "fixed" by letting a fresh first-party observation
        suppress a stale transitive instance of the same name: that same rule
        would suppress a *genuine* partial-mesh duplicate (the copy we reach
        first-party shadowing the copy only peers can witness), regressing the
        corroboration detection above. Distinguishing them needs per-instance
        recency the one-hop ``members`` gossip does not carry, so the transient
        false positive is accepted as the price of keeping the partial-mesh
        true positive. (A lease/consensus backend avoids both.)
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
            # Skip STALE peers (no fresh identity this round) AND STATUS_SELF:
            # a SELF peer is THIS node answering its own listed address, so it
            # is not independent evidence. record_success classifies a
            # self-listing that reports no instance_id (an older same-named
            # build, or a round-robined endpoint during a rolling upgrade) as
            # the *benign* STATUS_SELF case, but if processed here the block
            # below would synthesise a second "host:"+host instance key for our
            # OWN nodeName (peer.instance_id is None) on top of the self seed
            # above, fabricating a phantom duplicate-nodeName conflict that
            # fails every Leader job closed cluster-wide. Excluding SELF
            # mirrors how _agreeing_peers / cluster_size already treat it.
            if peer.status in _STALE_STATUSES or peer.status == STATUS_SELF:
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

        RESIDUAL (version skew, pre-release only): the declared N is the
        instance-id-deduped :meth:`cluster_size`, so a node on a build from
        *before* that dedup landed declares the raw ``len(peers)+1`` while a
        current build declares the deduped value.  With a multi-homed peer
        (one node listed at two addresses) the two builds therefore declare a
        different N for the *same, correct* config and each flags the other a
        size conflict -- failing ``Leader`` closed cluster-wide for upgrade.
        This is fail-closed (no double-run) and self-heals once every node runs
        a build that dedups; it can only arise *during* a rolling upgrade
        between two unreleased dev builds (every shipped build dedups), so is
        accepted rather than worked around in the size comparison (decoupling
        the advertised N from the quorum N would weaken the single-N safety
        proof for a window that does not survive release).  Change membership /
        upgrade one node at a time, as above.
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
        node with ``electLeader`` off runs *every* job ungated.  Neither
        ``distribution`` nor ``electLeader`` is part of the job-set fingerprint
        (they are cluster-level coordination settings, not job identity; the
        genuinely per-job ``clusterPolicy`` *is* fingerprinted) nor of the
        ``cluster_size`` gate, so two nodes identical but for these
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

        The size/policy gates fail closed on a SINGLE agreeing peer's
        divergent declaration (no corroboration), unlike the ``nodeName`` gate
        which corroborates a purely *transitive* report.  This is deliberate:
        a divergent size/policy is a first-party report by that member about
        itself, and requiring corroboration would re-open the split-brain the
        gate closes (two members each declaring a different N, each seeing only
        the other, would neither stand down).  The accepted cost is that a
        single hostile/buggy CA-vouched member can wedge the ``Leader`` gate
        closed cluster-wide (a member-level availability DoS, never a
        double-run) -- the same out-of-scope Byzantine class the module
        docstring notes, and why a dedicated single-purpose cluster CA matters
        (see :func:`build_server_ssl_context`).
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

    def _view_settled(self) -> bool:
        """Whether the never-skip ``available_*`` gates may trust this view.

        A freshly built manager (a cold boot, or a reload / in-place TLS
        rotation that rebuilds it) starts from a blank view and a new
        ``instance_id``.  Against that view every peer is ``unknown`` and
        nobody attests us, so the quorum-less election reduces to
        ``min([self])`` and EVERY node claims available leadership / job
        ownership at once -- running each PreferLeader job (and each deferred
        @reboot one-shot) on every node of a *healthy* cluster, the misfire
        the deferral in :meth:`yacron2.cron.Cron.spawn_jobs` exists to
        prevent.  The quorum-gated paths (:meth:`is_leader` /
        :meth:`is_job_owner`) already fail closed there; this is the
        fail-closed analogue for the ``available`` family, held only while
        the view is still *converging*:

        * a peer never polled at all (``unknown``) carries no information --
          hold until the first round completes (:meth:`start` runs one
          inline, so this clears within ``connectTimeout``); and
        * a current-build peer we see AGREED whose ``members`` do not mention
          our ``instance_id`` has not re-polled this incarnation of us yet.
          The mutual gate keeps such a peer out of the agreeing set, so we
          would elect ourselves alongside the true owner for the ~1-2
          intervals re-attestation takes.  This hold is BOUNDED by
          ``_SETTLE_ROUNDS`` completed rounds: a peer still not attesting us
          after that is a genuinely one-way link, and the never-skip contract
          then leans toward running (a possible double-run) rather than
          standing this node down indefinitely.  A *legacy* peer (no
          ``members`` field at all) never attests anyone, so it is exempt,
          matching the one-directional fallback in :meth:`_agreeing_peers`.

        Unreachable / untrusted / self / conflict / drifted / syncing peers
        are real observations, not convergence: a genuinely isolated node
        settles on its first round and keeps the never-skip guarantee.  The
        cost of the hold is a scheduled PreferLeader firing skipped for the
        window (<= ~2 intervals, self-healing) -- and when the held node is
        the rightful owner, skipped on EVERY node for that window, since its
        peers still defer to it -- the same transient fail-closed convergence
        trade the module docstring already makes elsewhere.
        """
        for peer in self.view.peers.values():
            if peer.status == STATUS_UNKNOWN:
                return False
        if self._poll_rounds >= _SETTLE_ROUNDS:
            return True
        for peer in self.view.peers.values():
            if (
                peer.status == STATUS_AGREED
                and peer.reports_members
                and not any(
                    instance == self.instance_id
                    for _name, instance, _agreed in peer.members or ()
                )
            ):
                return False
        return True

    def view_settled(self) -> bool:
        """Seam read of :meth:`_view_settled` (see the leadership ABC).

        While the hold is on, :meth:`is_available_leader` /
        :meth:`is_available_job_owner` return ``False`` even on the rightful
        owner -- a *quorate* node can be held (quorum needs only a majority
        attesting us; the hold waits for EVERY current-build agreeing peer),
        so :meth:`yacron2.cron.Cron._cluster_owner_moved` must read the hold
        as a transient fail-closed denial, never as another node positively
        owning the job (which would abandon a rightful owner's pending retry
        -- fatal for an @reboot keep-alive, whose reboot_ran record means no
        node ever restarts it).
        """
        return self._view_settled()

    def available_leader_name(self) -> str:
        """Elected leader ignoring quorum (for the ``PreferLeader`` policy).

        The lowest ``nodeName`` over ourselves, the peers we see agreeing,
        *and* the reachable co-owners a witness vouches a two-way edge to but
        we cannot reach directly (:meth:`_available_contenders`) -- the same
        fold :meth:`available_job_owner` applies for ``spread``.  Without it,
        two ``PreferLeader`` nodes blind to each other but sharing a witness
        each elect *themselves* and both run the job on a converged cluster;
        folding the contenders in makes them agree on one owner.  It cannot
        zero-run: the election is a ``min`` and the global-minimum node is the
        minimum of any set containing it, so it always self-elects and runs
        (the never-skip guarantee), mirroring :meth:`_available_contenders`'s
        ``max`` argument for spread.
        """
        return elect_available_leader(
            self.node_name,
            [*self._agreeing_peer_names(), *self._available_contenders()],
        )

    def _cedes_to_lower_instance(
        self, owns_for: "Callable[[str, set[str]], bool]"
    ) -> bool:
        """Whether a *lower-instance* twin sharing our nodeName would itself
        run this, so we defer to it -- the duplicate-nodeName tiebreak on the
        never-skip ``available_*`` path.

        A duplicate ``nodeName`` is a misconfiguration the conflict gate does
        *not* protect ``PreferLeader`` against (it accepts double-runs only
        across a partition), so two processes sharing one name would otherwise
        *both* run every job they own on a healthy cluster.  We break the tie
        on the per-process ``instance_id`` (the same fence the lease backends
        self-recognise by): the lowest instance runs, the rest defer.  The two
        duplicates poll each other (each sees the other as ``STATUS_CONFLICT``
        carrying its instance id), so on a converged cluster both agree the
        lowest runs.

        The deferral is gated on the lower twin *actually owning* this job /
        leadership in its own gossiped view (``owns_for`` recomputes the
        election over the twin's ``mutual_agreeing``), not merely on its
        existence.  A blunt "a lower instance exists, so stand down" -- the
        older rule -- *zero-runs* a never-skip job when the twins' views are
        asymmetric: the lower twin can self-own a name we do not, so it defers
        to a *different* node while we defer to it, and the job runs nowhere.
        By ceding only to a twin we can see will run it, an asymmetric
        duplicate-name view degrades to a (``PreferLeader``-accepted)
        double-run rather than a zero-run.  When the twin's view is unknown (no
        ``mutual_agreeing``) it trivially self-owns its own name, so we cede --
        the converged healthy case.  (Residual: a twin that defers via a folded
        contender we cannot ourselves see is mis-read as a self-owner; that
        needs a duplicate nodeName *and* an asymmetric view, and biases to the
        accepted double-run only when our own folded set already names us
        owner.)
        """
        for peer in self.view.peers.values():
            if peer.status in _STALE_STATUSES:
                continue
            if peer.node_name != self.node_name or not peer.instance_id:
                continue
            if peer.instance_id >= self.instance_id:
                continue  # not a strictly-lower-instance twin
            if owns_for(peer.node_name, peer.mutual_agreeing or set()):
                return True
        return False

    def is_available_leader(self) -> bool:
        """Whether this node leads its reachable set, quorum or not."""
        if not self._view_settled():
            # never-polled / still-converging view: hold (fail closed) rather
            # than claim leadership of a cluster we have not really looked at
            # yet; see _view_settled.
            return False
        if self.available_leader_name() != self.node_name:
            return False
        return not self._cedes_to_lower_instance(
            lambda name, view: elect_available_leader(name, view) == name
        )

    def is_quorate(self) -> bool:
        """Whether this node currently sees a quorum (so it may run jobs)."""
        return self.leader_name() is not None

    # --- per-job ownership (distribution: spread) -------------------------

    def job_owner(self, job_name: str) -> Optional[str]:
        """Quorum-gated owner of ``job_name`` (spread mode), else ``None``.

        Like :meth:`leader_name`, the owner is the rendezvous winner over
        ourselves and the confirmed-quorate candidates
        (:meth:`_eligible_candidates`), so bridged nodes agree on one owner and
        the owner is never a node that would itself stand down.  Unlike
        single-leader, the rendezvous set *also* includes the contenders a
        witness vouches quorate (:meth:`_unconfirmed_contenders`): a
        thin-bridged node we cannot confirm but a peer vouches quorate would
        otherwise self-own a job we also claim (the rendezvous winner is
        per-job, so a thin bridge that single-leader's global-``min`` shrugs
        off can split ``spread``).  Folding those in makes us defer to a
        higher-ranked one we
        cannot rule out, so ``spread`` keeps the same at-most-once guarantee as
        single-leader rather than double-running across a thin bridge -- at the
        cost (the same fail-closed trade) of standing a job down while its
        owner is unconfirmable.  Folding only *quorate-vouched* contenders (not
        every two-way edge) is what keeps that fail-closed case a rare,
        converging skip rather than a permanent zero-run behind a sub-quorum
        node (see :meth:`_unconfirmed_contenders`).
        """
        return elect_job_owner(
            job_name,
            self.node_name,
            self._agreeing_peer_names(),
            self.cluster_size(),
            [*self._eligible_candidates(), *self._unconfirmed_contenders()],
        )

    def is_job_owner(self, job_name: str) -> bool:
        """Whether this node owns ``job_name`` (quorate rendezvous winner).

        At-most-once: a node defers (this returns ``False``) when a possible
        co-owner it cannot confirm out-scores it for the job, so two quorate
        nodes never both own one ``Leader`` job across a thin bridge (see
        :meth:`job_owner` / :meth:`_unconfirmed_contenders`).
        """
        return self.job_owner(job_name) == self.node_name

    def _available_contenders(self) -> List[str]:
        """Reachable co-owners a peer vouches a two-way edge to that we cannot
        reach directly -- folded into the never-skip ``available`` owner set so
        two ``PreferLeader`` nodes blind to each other (but sharing a witness)
        agree on one owner per job instead of both running it.

        This is the ``PreferLeader`` analogue of
        :meth:`_unconfirmed_contenders`, but it folds the *raw*
        ``mutual_agreeing`` edge, not the quorate-vouched set.  The available
        path has **no quorum gate**: a sub-quorum node still runs the jobs it
        owns, so it is a legitimate co-owner and must be in
        everyone's rendezvous set or it would be double-run.  Folding raw edges
        cannot zero-run here -- a rendezvous winner has no quorum gate to stand
        itself down on, and the global-max node is the max of *any* set
        containing it, so it always self-owns and runs.  Without this fold, two
        quorate nodes sharing a majority core but not seeing each other each
        self-own the per-job winner (the rendezvous winner is per job, so the
        global ``min`` that saves single-leader ``PreferLeader`` does not), and
        a converged cluster double-runs the job -- weaker than single-leader,
        which the docstring once wrongly claimed it never was.
        """
        agreeing = self._agreeing_peers()
        accounted = {self.node_name}
        accounted |= {peer.node_name for peer in agreeing if peer.node_name}
        possible: Set[str] = set()
        for peer in agreeing:
            for name in peer.mutual_agreeing or ():
                if name and name not in accounted:
                    possible.add(name)
        return sorted(possible)

    def available_job_owner(self, job_name: str) -> str:
        """Owner of ``job_name`` ignoring quorum (spread ``PreferLeader``).

        The rendezvous winner over ourselves, the peers we see agreeing, *and*
        the reachable co-owners a witness vouches for but we cannot reach
        (:meth:`_available_contenders`).  Folding the contenders in makes two
        never-skip nodes blind to each other converge on one owner per job
        (no double-run), while the absent quorum gate guarantees the winner
        still runs (no zero-run); see :meth:`_available_contenders`.
        """
        return elect_available_job_owner(
            job_name,
            self.node_name,
            [*self._agreeing_peer_names(), *self._available_contenders()],
        )

    def is_available_job_owner(self, job_name: str) -> bool:
        """Whether this node owns ``job_name`` in its reachable set.

        Like :meth:`is_available_leader`, this never-skip (spread
        ``PreferLeader``) path breaks a duplicate-nodeName tie on the
        per-process ``instance_id`` so two same-named processes do not both run
        the job on a healthy cluster -- ceding only to a lower-instance twin
        that would itself own the job, so an asymmetric duplicate-name view
        never zero-runs it (see :meth:`_cedes_to_lower_instance`).
        """
        if not self._view_settled():
            # never-polled / still-converging view: hold (fail closed) rather
            # than claim ownership out of a blank view; see _view_settled.
            return False
        if self.available_job_owner(job_name) != self.node_name:
            return False
        return not self._cedes_to_lower_instance(
            lambda name, view: (
                elect_available_job_owner(job_name, name, view) == name
            )
        )

    def fleet_view(self) -> Dict[str, Any]:
        """The merged per-node job-summary view for ``GET /fleet``.

        One entry per distinct node: this node first (live scheduler state,
        stamped now), then every configured peer with whatever snapshot its
        last successful poll absorbed, aged by ``as_of`` (= last_seen) so the
        dashboard can show data freshness per node. Peer freshness is bounded
        by the poll ``interval``: the summaries ride the existing gossip
        round, no extra fan-out happens here. Self-listings are skipped and
        peers are deduped by instance_id (two configured addresses answering
        as the same process appear once), mirroring cluster_size's dedup.
        ``jobs: null`` means no snapshot was ever absorbed (never reached, or
        a build that predates fleet gossip); the dashboard renders that as
        "no data" rather than "no jobs".
        """
        job_summaries, summaries_truncated = self._advertised_job_summaries()
        now = datetime.datetime.now(datetime.timezone.utc)
        nodes: List[Dict[str, Any]] = [
            {
                "node_name": self.node_name,
                "host": None,
                "self": True,
                "status": STATUS_SELF,
                "as_of": now.isoformat(),
                "jobs": job_summaries,
                "truncated": summaries_truncated,
            }
        ]
        seen_instances = {self.instance_id}
        for peer in self.view.peers.values():
            if peer.status == STATUS_SELF or peer.self_confirmed:
                continue
            if peer.instance_id is not None:
                if peer.instance_id in seen_instances:
                    continue
                seen_instances.add(peer.instance_id)
            nodes.append(
                {
                    "node_name": peer.node_name,
                    "host": peer.host,
                    "self": False,
                    "status": peer.status,
                    "as_of": (
                        peer.last_seen.isoformat()
                        if peer.last_seen is not None
                        else None
                    ),
                    "jobs": peer.job_summaries,
                    "truncated": peer.job_summaries_truncated,
                }
            )
        return {
            "enabled": True,
            "backend": "gossip",
            "node_name": self.node_name,
            "distribution": self.distribution,
            "elect_leader": bool(self.config.get("electLeader")),
            # the peer-poll cadence, so the dashboard can set expectations
            # for how stale a healthy peer's as_of may legitimately be
            "interval": self.config["interval"],
            "nodes": nodes,
        }

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
