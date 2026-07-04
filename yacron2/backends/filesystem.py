"""Leader election over a shared POSIX mount -- no coordination service.

``cluster.backend: filesystem`` elects a single leader through the same
flock-guarded, fence-counted TTL lease :class:`yacron2.state
.FilesystemStateBackend` provides for durable job state: one small lease
file on a mount every node shares (an Amazon S3 Files / EFS / NFSv4 mount),
taken and renewed under an advisory lock, with a monotonic fence counter
that survives release/re-acquire cycles.  The mount *is* the store; there
is nothing else to deploy.

Safety story, and how it differs from the etcd/kubernetes backends:

* **Fencing.**  The lease holder string is ``<nodeName>#<12-hex token>``,
  unique per process, so duplicate ``nodeName`` s (or a restarted daemon)
  can never adopt each other's lease; the store's fence counter bumps on
  every takeover, so a stale holder's late writes are detectable.  As with
  etcd, ``is_leader()`` is additionally gated on a LOCAL monotonic
  deadline anchored *before* the renewing write is sent, so a stalled
  renew loop self-demotes with no I/O and a wall-clock step can neither
  extend leadership nor steal a valid lease.
* **Clocks.**  Unlike etcd (one server clock) the lease expiry here is
  compared across *N participating wall clocks* (the store judges takeover
  by the challenger's clock against the holder's written expiry).  Two
  margins make that safe under NTP-bounded skew: the holder stops calling
  itself leader :data:`_SKEW_SECONDS` before its lease really expires, and
  a challenger refuses to take over until the observed expiry is
  :data:`_SKEW_SECONDS` in the past *by its own clock*.  Two leaders would
  need inter-host clock skew above the SUM of the margins (~2s); NTP keeps
  real fleets orders of magnitude below that.  Run NTP on every node --
  the same requirement the durable state store documents for shared
  mounts.
* **Capability probe.**  ``start()`` hard-refuses a store whose locks are
  demonstrably fiction: a functional probe (two descriptors of one file
  must contend) catches no-op lock implementations, and a Linux mount-
  option sniff catches NFS ``nolock`` / ``local_lock=flock|all`` mounts
  whose flock never reaches the server.  Both checks are same-host by
  construction, so a mount whose locks are real locally but not propagated
  across hosts is NOT detectable here -- on platforms without
  ``/proc/mounts`` (Windows, macOS) that residual rests entirely on the
  operator's ``topology`` assertion, and start() says so loudly.
* **Quorum.**  ``is_quorate()`` means "this node has a FRESH, positive
  observation of the lease store": only an operation that returned an
  actual lease (a renew, an acquire, or a read that parsed a live holder)
  extends the freshness deadline.  A ``None`` from the lease API is
  deliberately NOT contact -- the state backend's lease API conflates
  "denied" with "store unreadable" (it fails closed), so counting it would
  keep a node on a sick store quorate with no holder visible, and the
  never-skip PreferLeader rule would then run the job on every such node.

The embedded state backend instance is private to this election: it runs
none of the scheduler's durable-state chores (no manifests, no GC, no
counters), writes only under the ``cluster/`` stream prefix and the
election lease name, and may safely share a directory (same ``path`` and
``deploymentId``) with a ``state:`` section -- the stream namespaces are
disjoint, lease files are never garbage-collected, and the scheduler's GC
never touches streams outside its managed prefixes.

The ``@reboot``-ran set is persisted as APPEND-ONLY records (one per
newly-ran job, tagged with the job-set id) in the ``cluster/reboot-ran``
stream: append-only makes concurrent writers union by construction, where
a single read-modify-write blob would need the CAS loop etcd uses.  Reads
fold only records tagged with the LIVE job-set id, so a reconfigured
one-shot runs again, and the stream is pruned to the newest
:data:`_REBOOT_RAN_KEEP` records (a documented bound: a deployment cannot
track more marked one-shots than that).
"""

import asyncio
import contextlib
import datetime
import logging
import time
import uuid
from typing import Any, Callable, Dict, Optional, Set

from yacron2.config import ClusterConfig, ConfigError, StateConfig
from yacron2.leadership import LeaseBackend
from yacron2.platform import IS_WINDOWS
from yacron2.state import FilesystemStateBackend, Lease

logger = logging.getLogger("yacron2.backends.filesystem")

# The clock budget applied TWICE (see the module docstring): the holder
# self-demotes this early, and a challenger waits this long past the
# observed expiry, so leadership only overlaps when inter-host skew
# exceeds their sum.  Matches etcd's margin, and stays under the
# config-floored ttl (3s) so the leader window never collapses at the
# minimum ttl.
_SKEW_SECONDS = 1.0

# Kept in sync with config.py's cluster.filesystem.ttl floor (>= 3).
_MIN_USABLE_TTL = 3

# Worst-case number of sequential store operations one renew round makes
# (renew-or-acquire, a confirming read, and the occasional reboot-ran
# refresh/append); the per-op timeout is sized off this so a whole round
# fits its deadline even when every op is slow.
_OPS_PER_CYCLE = 4

# Reported as the holder's display name when a lease exists but its holder
# string is empty/unparseable.  Reporting a non-None holder keeps
# leader_name() non-None so a quorate follower defers its PreferLeader
# jobs instead of reading "holder unknown" as "run anyway" (see
# LeadershipBackend.is_available_leader).
_UNKNOWN_HOLDER = "<unknown holder>"

# Stream (inside the embedded store) holding the @reboot-ran records, and
# the newest-N bound both the reader and the pruner use.
_REBOOT_RAN_STREAM = "cluster/reboot-ran"
_REBOOT_RAN_KEEP = 512

# How often (seconds, monotonic) the reboot-ran stream is re-read.  It
# changes only when a one-shot runs, so re-listing it every renew round
# would be pointless mount traffic; a takeover forces an immediate
# refresh so a new leader never acts on a stale set.
_REBOOT_RAN_REFRESH = 60.0


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _monotonic() -> float:
    """A monotonic clock for lease/quorum *deadlines*.

    Lease fences must never be judged on the wall clock: a backward NTP/VM
    step would keep ``is_leader`` true past the lease's real expiry (a
    second node has by then taken it over); a forward step would expire
    quorum early.  ``time.monotonic`` cannot jump, so deadlines anchored
    to it stay correct across any wall-clock correction.  The wall clock
    is used only for the human-readable expiry in the dashboard and for
    the challenger-side takeover margin (which is exactly the cross-host
    comparison the margins exist to bound).
    """
    return time.monotonic()


def _wallclock() -> float:
    """Wall-clock epoch seconds, seam-patchable in tests.

    Used ONLY for the challenger-side takeover margin (comparing another
    host's written ``expires_at``) and never for our own fence.
    """
    return time.time()


def display_name(holder: Optional[str]) -> Optional[str]:
    """The human identity in a ``<nodeName>#<token>`` holder string.

    ``None`` passes through (no holder).  An empty display part maps to
    the :data:`_UNKNOWN_HOLDER` sentinel rather than ``None``: a lease
    that exists but cannot be named still names *someone*, and reporting
    ``None`` would make every quorate follower treat the job as unowned
    and run it (see the module docstring).  Display only -- the run/skip
    decision never string-compares this (see
    :meth:`yacron2.leadership.LeadershipBackend.is_available_leader`).
    """
    if holder is None:
        return None
    name = holder.rsplit("#", 1)[0]
    return name or _UNKNOWN_HOLDER


class FilesystemBackend(LeaseBackend):
    """Leader election through a TTL lease on a shared POSIX mount."""

    backend_name = "filesystem"

    def __init__(
        self,
        config: ClusterConfig,
        get_job_set_id: Callable[[], str],
    ) -> None:
        super().__init__(config, get_job_set_id)
        fsb = config["filesystem"]
        self.election_name: str = fsb["electionName"]
        # display identity (the nodeName); the LEASE holder string below is
        # per-process unique so duplicate nodeNames can never adopt each
        # other's lease (see the module docstring).
        self.identity: str = config["nodeName"]
        self._holder_token: str = "{}#{}".format(
            self.identity, uuid.uuid4().hex[:12]
        )
        self.ttl: float = float(fsb["ttl"])
        self.connect_timeout: int = config["connectTimeout"]
        # the private store this election runs over; no scheduler chores
        # ever run on it (see the module docstring).
        self._store = FilesystemStateBackend(
            StateConfig(
                {
                    "path": fsb["path"],
                    "topology": fsb.get("topology", "auto"),
                    "deploymentId": fsb.get("deploymentId"),
                }
            ),
            get_job_set_id,
        )

        # live state, written by the renew loop and read by the sync methods
        self._is_leader = False
        # set when a renew is positively refused (taken over, released, or
        # an unreadable-lease blip -- the store fails closed): fences
        # is_leader() closed at once WITHOUT clearing _is_leader, so
        # _is_self_demoted_holder() stays True and a never-skip
        # PreferLeader job keeps running on this former holder rather than
        # dropping to zero-run while every follower still sees (and defers
        # to) the on-disk lease. Cleared by _apply_round (the single writer
        # of leadership state) once a round re-establishes the picture.
        self._lease_lost = False
        # the lease we hold (None when not holding); carries the fence.
        self._lease: Optional[Lease] = None
        self._holder: Optional[str] = None
        # wall-clock expiry, for the dashboard/lease_detail display ONLY
        self._lease_deadline: Optional[datetime.datetime] = None
        # monotonic deadlines: the load-bearing fence/freshness gates
        self._lease_deadline_mono: Optional[float] = None
        self._quorum_deadline_mono: Optional[float] = None
        self._observed_fence: Optional[int] = None

        # reboot-ran refresh bookkeeping (see _refresh_reboot_ran)
        self._reboot_refresh_next = 0.0
        self._reboot_persisted: Set[str] = set()
        self._reboot_persisted_job_set_id: Optional[str] = None

        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    # --- derived renew cadence --------------------------------------------

    @property
    def renew_period(self) -> float:
        """Sleep between renew rounds (a third of the lease ttl)."""
        return max(1.0, self.ttl / 3)

    @property
    def round_deadline(self) -> float:
        """Wall-time bound on a single renew round.

        ``round_deadline + renew_period <= ttl - skew`` by construction
        (at ttl >= the config floor), so the gap between two successful
        renews stays inside the lease window and a slow round cannot make
        the holder lapse out of its own lease.
        """
        return max(1.0, self.ttl - self.renew_period - _SKEW_SECONDS)

    @property
    def op_timeout(self) -> float:
        """Per-operation timeout for one store call inside a round.

        Sized so a round's worst-case sequential ops still fit
        :attr:`round_deadline` (capped at ``connectTimeout`` so an
        explicitly lower value still applies).  A store op runs on a
        daemon worker thread; timing out abandons the thread, never
        blocks the loop.
        """
        return min(
            float(self.connect_timeout),
            max(0.5, self.round_deadline / _OPS_PER_CYCLE),
        )

    # --- pure local-state reads (no I/O) -----------------------------------

    def is_leader(self) -> bool:
        if (
            not self._is_leader
            or self._lease_deadline_mono is None
            # a renew that was positively refused fences us closed at once:
            # the old monotonic deadline may still be in the future, but the
            # on-disk lease is no longer provably ours. See _lease_lost.
            or self._lease_lost
        ):
            return False
        # gated on a MONOTONIC deadline so a backward wall-clock step
        # cannot keep us "leader" past the real lease expiry.
        return _monotonic() < self._lease_deadline_mono

    def _is_self_demoted_holder(self) -> bool:
        # raw win flag still set (we acquired and have not observed another
        # holder) but the monotonic fence lapsed or a renew was refused --
        # the brief self-demotion window. See the LeadershipBackend base.
        return self._is_leader and not self.is_leader()

    def leader_name(self) -> Optional[str]:
        if not self.is_quorate():
            return None
        return self._holder

    def is_quorate(self) -> bool:
        if self._quorum_deadline_mono is None:
            return False
        # FIXED at the last positive store observation (see _apply_round);
        # only a successful round may extend it.
        return _monotonic() < self._quorum_deadline_mono

    def lease_detail(self) -> Dict[str, Any]:
        return {
            "path": self._store.root,
            "electionName": self.election_name,
            "identity": self.identity,
            "holder": self._holder,
            "fence": self._observed_fence,
            "expiry": (
                self._lease_deadline.isoformat()
                if self._lease_deadline is not None
                else None
            ),
        }

    def _apply_round(
        self,
        holder: Optional[str],
        is_leader: bool,
        expires_at: Optional[float],
        fence: Optional[int],
        mono: Optional[float] = None,
        lease_mono: Optional[float] = None,
    ) -> None:
        """Update live leader state from a round's POSITIVE outcome.

        Called only when the round actually observed the store (a lease
        was renewed, acquired, or read); an ambiguous round (timeouts,
        ``None`` everywhere) changes nothing and lets the deadlines lapse.
        ``mono`` fixes the quorum freshness deadline (captured at round
        end -- the fresher, safe direction); ``lease_mono`` is captured
        just BEFORE the renewing/acquiring write is sent -- a lower bound
        on when the lease's TTL was reset -- and anchors the leadership
        fence, so a slow locked write cannot push our local deadline past
        the lease's real expiry.  Both default to now for pure unit tests.
        """
        if mono is None:
            mono = _monotonic()
        self._quorum_deadline_mono = mono + self.ttl
        self._holder = holder if holder is not None else _UNKNOWN_HOLDER
        self._is_leader = is_leader
        self._lease_lost = False
        self._observed_fence = fence
        if is_leader:
            fence_anchor = lease_mono if lease_mono is not None else mono
            self._lease_deadline_mono = fence_anchor + self.ttl - _SKEW_SECONDS
        else:
            self._lease_deadline_mono = None
            self._lease = None
        if expires_at is not None and expires_at > 0.0:
            self._lease_deadline = datetime.datetime.fromtimestamp(
                expires_at, datetime.timezone.utc
            )
        else:
            self._lease_deadline = None

    # --- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        try:
            await asyncio.wait_for(
                self._store.start(), timeout=self.connect_timeout
            )
            reason = await asyncio.wait_for(
                self._store.verify_locking(), timeout=self.connect_timeout
            )
            if reason is not None:
                # the roadmap posture: hard-refuse a store that fakes its
                # locks rather than silently electing two leaders.
                # start_stop_cluster logs this and leaves the manager
                # unbuilt, so Leader jobs fail closed.
                raise ConfigError(
                    "cluster.backend filesystem: refusing to elect over "
                    "{}: {}".format(self._store.root, reason)
                )
            if not self._store.supports_shared_locking():
                logger.warning(
                    "cluster: the filesystem election store at %s resolved "
                    "topology %r, so its locks only exclude processes on "
                    "THIS host%s; if the directory really is a shared "
                    "network mount, set cluster.filesystem.topology: shared",
                    self._store.root,
                    self._store.topology,
                    (
                        " (Windows/macOS cannot probe the mount)"
                        if IS_WINDOWS
                        else ""
                    ),
                )
            elif IS_WINDOWS:  # pragma: no cover - Windows-only advisory
                logger.warning(
                    "cluster: filesystem election on a Windows shared "
                    "mount: cross-host lock fidelity cannot be verified on "
                    "this platform (no /proc/mounts); the election is safe "
                    "only if the mount honours byte-range locks across "
                    "hosts"
                )
            # one bounded, best-effort round so quorate/leader state is
            # real before the first spawn_jobs; a store that cannot answer
            # yet just leaves the node not-quorate (Leader fails closed,
            # PreferLeader runs -- the documented posture).
            try:
                await asyncio.wait_for(
                    self._renew_once(), timeout=self.round_deadline
                )
            except (OSError, asyncio.TimeoutError) as ex:
                logger.warning(
                    "cluster: filesystem election: first round did not "
                    "complete (%s); starting unquorate",
                    ex,
                )
            self._task = asyncio.create_task(self._renew_loop())
        except BaseException:
            # clean up our own half-started state; the caller
            # (start_stop_cluster) logs the failure and keeps running.
            with contextlib.suppress(Exception):
                await self._store.stop()
            raise
        logger.info(
            "cluster: filesystem election ready at %s (election=%s, "
            "identity=%s, ttl=%.0fs)",
            self._store.root,
            self.election_name,
            self.identity,
            self.ttl,
        )

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        lease = self._lease
        self._lease = None
        self._is_leader = False
        if lease is not None:
            # best-effort release for immediate failover; TTL expiry is
            # the fallback. (A store acquire abandoned by an earlier
            # timeout could in principle land after this and re-take the
            # released lease for a process that has exited -- bounded by
            # one ttl, availability-only, and vanishingly rare.)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    self._store.release_lease(lease),
                    timeout=self.connect_timeout,
                )
        await self._store.stop()

    # --- the renew loop -----------------------------------------------------

    async def _renew_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(
                    self._renew_once(), timeout=self.round_deadline
                )
            except asyncio.CancelledError:
                raise
            except (OSError, asyncio.TimeoutError) as ex:
                # transient store trouble: nothing to change -- the fixed
                # quorum deadline simply lapses (Leader closes, PreferLeader
                # runs), and the next round retries.
                logger.warning(
                    "cluster: filesystem election round failed: %s", ex
                )
            except Exception:  # noqa: BLE001 - keep the loop alive
                logger.exception(
                    "cluster: unexpected error in the filesystem election loop"
                )
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self.renew_period
                )
            except asyncio.TimeoutError:
                pass

    async def _bounded(self, coro: Any) -> Any:
        return await asyncio.wait_for(coro, timeout=self.op_timeout)

    async def _renew_once(self) -> None:
        """One election round: renew (or read/acquire), then reboot-ran.

        State changes happen ONLY on positive observations; every
        ambiguous outcome (a timeout, a ``None`` whose confirming read
        also answers nothing) leaves the previous state to lapse on its
        deadlines.  See the module docstring for the quorum rule.
        """
        was_leader = self.is_leader()
        if self._lease is not None:
            lease_mono: Optional[float] = _monotonic()
            try:
                renewed = await self._bounded(
                    self._store.renew_lease(self._lease, self.ttl)
                )
            except asyncio.TimeoutError:
                # UNKNOWN: the abandoned worker may still land the renew.
                # Change nothing; the fence self-demotes if this persists.
                renewed = None
                lease_mono = None
                logger.debug(
                    "cluster: filesystem lease renew timed out; leaving "
                    "state to its deadlines"
                )
            if renewed is not None:
                self._lease = renewed
                self._apply_round(
                    display_name(renewed.holder),
                    True,
                    renewed.expires_at,
                    renewed.fence,
                    lease_mono=lease_mono,
                )
                await self._maintain_reboot_ran(gained=False)
                return
            if lease_mono is not None:
                # positively refused (taken over / released / unreadable):
                # fence closed NOW, raw win flag kept for the never-skip
                # self-demotion window; then fall through and look at the
                # store like any non-holder.
                self._lease_lost = True
                self._lease = None
        # not holding (or just lost): observe, then maybe take over.
        try:
            observed = await self._bounded(
                self._store.read_lease(self.election_name)
            )
        except asyncio.TimeoutError:
            return
        if observed is not None and observed.holder == self._holder_token:
            # our own lease -- an acquire abandoned by an earlier timeout
            # landed after all (the documented UNKNOWN case). Adopt the
            # Lease object so the next round renews it under the lock;
            # leadership itself waits for that locked confirmation.
            # Adopted AFTER the round is applied: the non-leader branch of
            # _apply_round clears the held lease, and this adoption must
            # survive it.
            self._apply_round(
                display_name(observed.holder),
                self._is_leader and not self._lease_lost,
                observed.expires_at,
                observed.fence,
            )
            self._lease = observed
            await self._maintain_reboot_ran(gained=False)
            return
        now_wall = _wallclock()
        if (
            observed is not None
            and observed.expires_at > now_wall - _SKEW_SECONDS
        ):
            # a live (or too-recently-expired) foreign holder: defer.  The
            # challenger-side margin: we do not try to take over until the
            # written expiry is a full skew margin in the past BY OUR
            # CLOCK, so a holder whose clock trails ours keeps its lease.
            self._apply_round(
                display_name(observed.holder),
                False,
                observed.expires_at,
                observed.fence,
            )
            await self._maintain_reboot_ran(gained=False)
            return
        # absent, released, or expired-beyond-margin: campaign.
        lease_mono = _monotonic()
        try:
            acquired = await self._bounded(
                self._store.acquire_lease(
                    self.election_name, self._holder_token, self.ttl
                )
            )
        except asyncio.TimeoutError:
            # UNKNOWN, not denied: the write may still land. The
            # own-holder adoption branch above self-heals next round.
            return
        if acquired is not None:
            self._lease = acquired
            self._apply_round(
                display_name(acquired.holder),
                True,
                acquired.expires_at,
                acquired.fence,
                lease_mono=lease_mono,
            )
            await self._maintain_reboot_ran(gained=not was_leader)
            return
        # denied -- EITHER a rival won the race OR the store failed closed
        # (unreadable/unwritable). A confirming read tells them apart; a
        # read that answers nothing extends no deadline (not contact).
        try:
            confirm = await self._bounded(
                self._store.read_lease(self.election_name)
            )
        except asyncio.TimeoutError:
            return
        if confirm is not None:
            self._apply_round(
                display_name(confirm.holder),
                confirm.holder == self._holder_token,
                confirm.expires_at,
                confirm.fence,
            )
            if confirm.holder == self._holder_token:
                self._lease = confirm
            await self._maintain_reboot_ran(gained=False)

    # --- @reboot-ran persistence ---------------------------------------------

    async def _maintain_reboot_ran(self, *, gained: bool) -> None:
        """Refresh the store-read ran-set (throttled) and flush local marks.

        Runs inside the renew round, after the leadership decision.  The
        stream is re-listed only every :data:`_REBOOT_RAN_REFRESH` seconds
        -- it changes rarely -- except that GAINING leadership forces an
        immediate refresh, so a failover leader never re-runs a one-shot
        the old leader marked moments ago.  Best-effort throughout: a
        failure here must never cost the round its election work.
        """
        mono = _monotonic()
        if gained or mono >= self._reboot_refresh_next:
            self._reboot_refresh_next = mono + _REBOOT_RAN_REFRESH
            try:
                await self._refresh_reboot_ran()
            except (OSError, asyncio.TimeoutError) as ex:
                logger.debug(
                    "cluster: could not refresh the @reboot-ran set: %s", ex
                )
        with contextlib.suppress(OSError, asyncio.TimeoutError):
            await self._persist_reboot_ran()

    async def _refresh_reboot_ran(self) -> None:
        records = await self._bounded(
            self._store.list_records(
                _REBOOT_RAN_STREAM,
                limit=_REBOOT_RAN_KEEP,
                newest_first=True,
            )
        )
        live_id = self.get_job_set_id()
        jobs = {
            str(rec["job"])
            for rec in records
            if isinstance(rec.get("job"), str)
            and rec.get("jobSetId") == live_id
        }
        self._observe_reboot_ran(live_id, jobs)
        # the store now carries these; no need to re-append them.
        self._note_persisted(live_id, jobs)

    def _note_persisted(self, job_set_id: str, jobs: Set[str]) -> None:
        if self._reboot_persisted_job_set_id != job_set_id:
            self._reboot_persisted = set()
            self._reboot_persisted_job_set_id = job_set_id
        self._reboot_persisted |= jobs

    async def _persist_reboot_ran(self) -> None:
        """Append any locally-marked one-shots the store does not carry yet.

        Called eagerly by :meth:`mark_reboot_ran` (cron records-then-
        spawns, so the mark must land BEFORE the launch) and again each
        maintenance pass as the retry path.  Appends are idempotent in
        effect (readers union), so a duplicate append after a lost refresh
        is harmless; the per-id ``_reboot_persisted`` set just keeps the
        stream from growing one record per renew round.
        """
        self._reconcile_local_reboot_ran()
        live_id = self.get_job_set_id()
        if self._reboot_persisted_job_set_id != live_id:
            self._reboot_persisted = set()
            self._reboot_persisted_job_set_id = live_id
        missing = self._reboot_ran_local - self._reboot_persisted
        if not missing:
            return
        for job in sorted(missing):
            await self._bounded(
                self._store.append_record(
                    _REBOOT_RAN_STREAM,
                    {"jobSetId": live_id, "job": job},
                )
            )
            self._reboot_persisted.add(job)
        with contextlib.suppress(OSError, asyncio.TimeoutError):
            await self._bounded(
                self._store.prune_records(
                    _REBOOT_RAN_STREAM, keep=_REBOOT_RAN_KEEP
                )
            )

    async def mark_reboot_ran(self, job_name: str) -> None:
        self._reconcile_local_reboot_ran()
        self._reboot_ran_local.add(job_name)
        # eager, bounded, best-effort: the caller launches right after, so
        # a store that cannot answer must not stall the launch -- the
        # maintenance pass retries the append.
        with contextlib.suppress(OSError, asyncio.TimeoutError):
            await self._persist_reboot_ran()
