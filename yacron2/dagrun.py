"""The DAG runtime: the driver that turns the pure state machine durable.

:mod:`yacron2.dag` is the pure, I/O-free state machine; this module is the
daemon-side driver that gives it a store, a clock, leases and subprocesses.  It
is the DAG analogue of the retry / catch-up / slot machinery on
:class:`yacron2.cron.Cron`, kept in its own module so the (large) orchestration
surface does not bloat cron.py; a :class:`DagScheduler` holds a back-reference
to the owning ``Cron`` and reuses its seams (the state backend, the loopback
job-state API, ``_compute_next_fire``, ``_cluster_allows``, ``running_jobs``,
the ``_proc_token`` / ``_state_host`` identity, ``_track_state_write``).

The durable model, in one paragraph: each ``dag_run`` is a single mutable
*document* (``dagrun/<dag>`` keyed by a run key), advanced only by the node
that holds that run's advance *lease* (``dagadvance/<dag>/<key>``).  An advance
is one flock-guarded read-modify-write that atomically claims every ready task
``pending -> running``; the driver then launches a real subprocess per claimed
task (through the same :class:`~yacron2.job.RunningJob` path a job uses, with
the durable env injected so the task can call ``yacron2 xcom`` / ``artifact`` /
``state``), and records the pid in a second RMW.  A task's completion is routed
back here by the reaper, recorded, and triggers a fresh advance.  Because the
lease gates who advances *and* who reconciles, and the RMW claim is atomic, the
fleet never double-advances or double-launches a task; on a crash the durable
per-task state is the source of truth and a resumed (or adopting) node
reconciles interrupted tasks from it -- at-least-once, never at-most-once.
"""

import asyncio
import datetime
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from yacron2 import dag, platform
from yacron2.dag import DagSpec
from yacron2.job import RunningJob
from yacron2.state import DOC_KEEP, Lease, StateBackend

logger = logging.getLogger("yacron2.dagrun")

# Every awaited backend op is capped so a wedged store cannot hang the advance
# loop forever (mirrors cron.STATE_OP_TIMEOUT; kept local so this module does
# not import cron, which imports it).
STATE_OP_TIMEOUT = 10.0

# TTL of the per-run advance lease; renewed at a third of it while the run is
# active on this node, so only its owner advances/reconciles it.  A lapse
# (owner stopped renewing == owner gone) lets a peer adopt the run.
DAG_LEASE_TTL = 30.0

# How often the scheduler re-checks for due scheduled runs and orphaned runs to
# adopt (leaderless per-node chore; the per-run lease does the real gating).
SCHEDULE_CHECK_INTERVAL = 20.0
ADOPT_SCAN_INTERVAL = 30.0
GC_INTERVAL = 3600.0

# How often the owner re-advances a run that is blocked on an approval gate, so
# a decision recorded on a peer node (which cannot advance a run it does not
# own) is acted on within a few seconds rather than a full idle re-advance.
APPROVAL_POLL_INTERVAL = 5.0

# Hard cap on how many missed occurrences a single catch-up replays, mirroring
# cron.MAX_CATCHUP_OCCURRENCES so a long outage cannot stampede.
DAG_MAX_CATCHUP = 100

RunRef = Tuple[str, str]  # (dag_name, run_key)


def _now() -> float:
    """Wall-clock epoch seconds for document timestamps and poke schedules.

    A seam (like :func:`yacron2.jobstate._now`) so tests drive poke/retry
    timing without touching the lease clock; plain ``time.time`` in production.
    """
    return time.time()


def _jitter(max_jitter: float) -> float:
    """A random poke jitter in ``[0, max_jitter]`` (0 when disabled)."""
    if max_jitter <= 0:
        return 0.0
    return random.uniform(0.0, max_jitter)  # noqa: S311 - not cryptographic


@dataclass
class _DagRef:
    """The marker a launched DAG-task :class:`RunningJob` carries.

    Lets the reaper route the task's completion back to the right run/task
    without the scheduler having to track every live subprocess itself.
    """

    dag_name: str
    run_key: str
    run_id: str
    task_id: str
    taskkey: str


class DagScheduler:
    """Drives every DAG's runs: schedules, advances, launches, reconciles."""

    def __init__(self, cron: Any) -> None:
        self._cron = cron
        # runs this node owns (holds the advance lease for) -> the held lease.
        self._owned: Dict[RunRef, Lease] = {}
        self._renewers: Dict[RunRef, asyncio.Task] = {}
        self._locks: Dict[RunRef, asyncio.Lock] = {}
        # soonest wall-clock instant an owned run wants another advance (a due
        # sensor poke or task retry); drives the loop's sleep cap.
        self._wake: Dict[RunRef, float] = {}
        # in-memory forward next-fire index per scheduled dag (like the job
        # next-fire index); catch-up of missed runs is a one-time seed step.
        self._next_logical: Dict[str, datetime.datetime] = {}
        # dag name -> the schedule signature it was seeded under, so a reload
        # that changes a schedule (or disables a dag) re-seeds strictly-future
        # instead of replaying the gap (mirrors the job _refresh_schedule).
        self._seeded: Dict[str, str] = {}
        self._service_task: Optional[asyncio.Task] = None
        self._next_sched_check = 0.0
        self._next_adopt = 0.0
        self._next_gc = 0.0

    # --- accessors -------------------------------------------------------

    def _backend(self) -> Optional[StateBackend]:
        backend: Optional[StateBackend] = self._cron.state_backend
        return backend

    def _dags(self) -> Dict[str, Any]:
        return getattr(self._cron, "cron_dags", {})

    def has_dags(self) -> bool:
        return bool(self._dags())

    @staticmethod
    def _ns(dag_name: str) -> str:
        return dag.DAG_RUN_NS_PREFIX + dag_name

    @staticmethod
    def _lease_name(ref: RunRef) -> str:
        return "{}{}/{}".format(dag.DAG_LEASE_PREFIX, ref[0], ref[1])

    def _wrap(self, transform):
        """Adapt a :mod:`yacron2.dag` transform to the backend sentinel.

        The pure module returns its own keep sentinel (to stay import-free of
        :mod:`yacron2.state`); translate it to the real ``DOC_KEEP`` the
        backend compares by identity.
        """

        def wrapped(body):
            new_body, result = transform(body)
            if dag.is_keep(new_body):
                return DOC_KEEP, result
            return new_body, result

        return wrapped

    # --- backend op helpers (all bounded) --------------------------------

    async def _mutate(self, dag_name: str, key: str, transform):
        backend = self._backend()
        if backend is None:
            return None, None
        return await asyncio.wait_for(
            backend.mutate_document(self._ns(dag_name), key, transform),
            timeout=STATE_OP_TIMEOUT,
        )

    async def _read(self, dag_name: str, key: str) -> Optional[Dict[str, Any]]:
        backend = self._backend()
        if backend is None:
            return None
        return await asyncio.wait_for(
            backend.read_document(self._ns(dag_name), key),
            timeout=STATE_OP_TIMEOUT,
        )

    # =====================================================================
    # Periodic entry point (called each scheduling tick from cron)
    # =====================================================================

    def service(self) -> None:
        """Spawn a single-flight service pass if there is DAG work to do.

        Synchronous and cheap, like ``Cron._state_periodic``: it only decides
        whether to spawn the async pass (scheduling due, an owned run's wake
        due, or an adoption/GC interval elapsed), never blocks the loop.
        """
        if self._backend() is None or not self.has_dags():
            return
        if self._service_task is not None and not self._service_task.done():
            return
        now = _now()
        due = (
            now >= self._next_sched_check
            or now >= self._next_adopt
            or now >= self._next_gc
            or any(w <= now for w in self._wake.values())
            or any(w.timestamp() <= now for w in self._next_logical.values())
        )
        if not due:
            return
        self._service_task = self._cron._track_state_write(self._run_service())

    async def _run_service(self) -> None:
        try:
            now = _now()
            # (re)seed new/changed dags + run one-time catch-up on the coarse
            # cadence (the seed is the one expensive durable read).
            if now >= self._next_sched_check:
                self._next_sched_check = now + SCHEDULE_CHECK_INTERVAL
                await self._seed_dags(now)
            # fire due scheduled runs EVERY pass (a cheap in-memory index
            # walk), so a fire lands at its instant, not a cadence late.
            await self._fire_scheduled(now)
            if now >= self._next_adopt:
                self._next_adopt = now + ADOPT_SCAN_INTERVAL
                await self._adopt_orphans()
            await self._advance_owned(now)
            if now >= self._next_gc:
                self._next_gc = now + GC_INTERVAL
                await self._gc_runs()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a bad pass must not kill the loop
            logger.exception("dag: unexpected error in the service pass")

    def next_wake_delay(self) -> Optional[float]:
        """Seconds until the scheduler next wants to run, or ``None``.

        Caps the main loop's sleep so a due sensor poke, task retry, or the
        next scheduled run is serviced on time even when no job is due.
        """
        if self._backend() is None or not self.has_dags():
            return None
        now = _now()
        candidates = [self._next_sched_check]
        candidates.extend(self._wake.values())
        for when in self._next_logical.values():
            candidates.append(when.timestamp())
        if self._owned:
            candidates.append(self._next_adopt)
        soonest = min(candidates)
        return max(0.0, soonest - now)

    # =====================================================================
    # Scheduling: create due runs (forward firing + one-time catch-up)
    # =====================================================================

    @staticmethod
    def _sched_sig(dagcfg: Any) -> str:
        """A signature of a dag's schedule + resolved timezone.

        Two configs fire on the same instants iff this matches (mirrors the job
        ``_same_schedule``), so a reload that changes the schedule re-seeds and
        one that leaves it alone keeps the existing next-fire (never skipping a
        fire on the reload's own boundary).
        """
        sched = dagcfg.schedule_job
        return "{}|{}".format(sched.schedule, sched.timezone)

    async def _seed_dags(self, now: float) -> None:
        """Reconcile the next-fire index with the (reloaded) dag set.

        Drops the index for a removed or disabled dag (so a later re-enable
        seeds strictly-future rather than backfilling the disabled gap), and
        (re)seeds a new dag or one whose schedule changed, running its one-time
        missed-run catch-up.
        """
        now_dt = datetime.datetime.now(datetime.timezone.utc)
        live = self._dags()
        for name in list(self._seeded):
            dagcfg = live.get(name)
            if (
                dagcfg is None
                or dagcfg.schedule_job is None
                or not dagcfg.enabled
                or self._seeded.get(name) != self._sched_sig(dagcfg)
            ):
                self._next_logical.pop(name, None)
                self._seeded.pop(name, None)
        for name, dagcfg in live.items():
            sched = dagcfg.schedule_job
            if sched is None or not dagcfg.enabled:
                continue
            if not self._cron._cluster_allows(sched):
                continue
            if name not in self._seeded:
                await self._seed_dag(dagcfg, now_dt)

    async def _seed_dag(self, dagcfg: Any, now_dt: datetime.datetime) -> None:
        sched = dagcfg.schedule_job
        nxt = self._cron._compute_next_fire(sched, now_dt)
        if nxt is not None:
            self._next_logical[dagcfg.name] = nxt
        self._seeded[dagcfg.name] = self._sched_sig(dagcfg)
        if sched.onMissed != "skip":
            try:
                await self._catch_up(dagcfg, now_dt)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("dag %s: catch-up seed failed", dagcfg.name)

    async def _fire_scheduled(self, now: float) -> None:
        now_dt = datetime.datetime.now(datetime.timezone.utc)
        for name, dagcfg in self._dags().items():
            sched = dagcfg.schedule_job
            if sched is None or not dagcfg.enabled:
                continue
            if name not in self._seeded:
                continue  # not seeded yet (waits for the next seed cadence)
            if not self._cron._cluster_allows(sched):
                continue
            await self._fire_forward(dagcfg, now_dt)

    async def _fire_forward(
        self, dagcfg: Any, now_dt: datetime.datetime
    ) -> None:
        sched = dagcfg.schedule_job
        fired = 0
        while fired < DAG_MAX_CATCHUP:
            nxt = self._next_logical.get(dagcfg.name)
            if nxt is None or nxt > now_dt:
                break
            await self._create_run(dagcfg, nxt, "scheduled")
            following = self._cron._compute_next_fire(sched, nxt)
            if following is None:
                # the schedule has no further occurrence (a fixed past year):
                # drop the index rather than poisoning it with None, which the
                # loop's ``.timestamp()`` sleep/due candidates would crash on
                # (mirrors the job next-fire index dropping an exhausted job).
                self._next_logical.pop(dagcfg.name, None)
                break
            self._next_logical[dagcfg.name] = following
            fired += 1

    async def _catch_up(self, dagcfg: Any, now_dt: datetime.datetime) -> None:
        sched = dagcfg.schedule_job
        after = await self._durable_watermark(dagcfg)
        if after is None:
            return  # never ran: nothing missed to replay
        deadline = sched.startingDeadlineSeconds
        if deadline:
            cutoff = now_dt - datetime.timedelta(seconds=deadline)
            if cutoff > after:
                after = cutoff
        missed: List[datetime.datetime] = []
        nxt = self._cron._compute_next_fire(sched, after)
        while (
            nxt is not None and nxt <= now_dt and len(missed) < DAG_MAX_CATCHUP
        ):
            missed.append(nxt)
            nxt = self._cron._compute_next_fire(sched, nxt)
        if not missed:
            return
        targets = missed[-1:] if sched.onMissed == "run-once" else missed
        logger.info(
            "dag %s: catch-up replaying %d missed run(s)",
            dagcfg.name,
            len(targets),
        )
        for when in targets:
            await self._create_run(dagcfg, when, "catchup")

    async def _durable_watermark(
        self, dagcfg: Any
    ) -> Optional[datetime.datetime]:
        backend = self._backend()
        if backend is None:
            return None
        docs = await asyncio.wait_for(
            backend.list_documents(self._ns(dagcfg.name)),
            timeout=STATE_OP_TIMEOUT,
        )
        latest: Optional[datetime.datetime] = None
        for body in docs:
            iso = body.get("logicalDate")
            when = _parse_iso(iso) if isinstance(iso, str) else None
            if when is not None and (latest is None or when > latest):
                latest = when
        return latest

    async def _create_run(
        self, dagcfg: Any, logical_dt: datetime.datetime, kind: str
    ) -> Optional[RunRef]:
        run_key = dag.run_key_for_logical(logical_dt.isoformat())
        created = await self._create_doc(
            dagcfg, run_key, logical_dt.isoformat(), kind
        )
        ref = (dagcfg.name, run_key)
        if created:
            await self._try_own(dagcfg, ref)
        return ref

    async def _create_doc(
        self, dagcfg: Any, run_key: str, logical_iso: Optional[str], kind: str
    ) -> bool:
        run_id = os.urandom(16).hex()
        now = _now()
        spec = dagcfg.spec

        def _create(current):
            if current is not None:
                return DOC_KEEP, False
            body = dag.new_run_body(
                dag=dagcfg.name,
                run_key=run_key,
                run_id=run_id,
                logical_date=logical_iso,
                kind=kind,
                now=now,
                spec=spec,
            )
            return body, True

        _stored, created = await self._mutate(dagcfg.name, run_key, _create)
        return bool(created)

    # =====================================================================
    # Ownership: the per-run advance lease (the TTL lease trio, per run)
    # =====================================================================

    async def _try_own(self, dagcfg: Any, ref: RunRef) -> bool:
        """Take ``ref``'s advance lease; on success reconcile + advance it.

        A ``None`` from ``acquire_lease`` (held elsewhere, or the store could
        not answer) means "not mine" -- fail closed and do not advance, exactly
        like the cluster slot claim.
        """
        if ref in self._owned:
            return True
        backend = self._backend()
        if backend is None:
            return False
        holder = self._cron._slot_holder()
        try:
            lease = await asyncio.wait_for(
                backend.acquire_lease(
                    self._lease_name(ref), holder, DAG_LEASE_TTL
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.TimeoutError:
            return False
        if lease is None:
            return False
        self._owned[ref] = lease
        self._locks.setdefault(ref, asyncio.Lock())
        self._renewers[ref] = asyncio.ensure_future(self._renew_loop(ref))
        self._wake[ref] = _now()  # advance promptly
        await self._reconcile_run(dagcfg, ref)
        await self.advance_one(ref)
        return True

    async def _renew_loop(self, ref: RunRef) -> None:
        period = max(1.0, DAG_LEASE_TTL / 3)
        while True:
            await asyncio.sleep(period)
            lease = self._owned.get(ref)
            backend = self._backend()
            if lease is None or backend is None:
                return
            try:
                renewed = await asyncio.wait_for(
                    backend.renew_lease(lease, DAG_LEASE_TTL),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.TimeoutError:
                continue  # unknown: retry next period
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - renewal is best-effort
                continue
            if renewed is not None:
                self._owned[ref] = renewed
                continue
            # positively taken over: a peer adopted the run (our lease lapsed).
            # Stop advancing it; in-flight tasks keep running and their
            # completions RMW the doc harmlessly (the new owner's state wins).
            logger.warning(
                "dag run %s/%s: advance lease was taken over; stopping "
                "advancement here (at-least-once)",
                ref[0],
                ref[1],
            )
            self._drop_owned(ref)
            return

    def _drop_owned(self, ref: RunRef) -> None:
        self._owned.pop(ref, None)
        self._wake.pop(ref, None)
        renewer = self._renewers.pop(ref, None)
        if renewer is not None and not renewer.done():
            renewer.cancel()

    async def _release(self, ref: RunRef) -> None:
        lease = self._owned.get(ref)
        self._drop_owned(ref)
        backend = self._backend()
        if lease is not None and backend is not None:
            try:
                await asyncio.wait_for(
                    backend.release_lease(lease), timeout=STATE_OP_TIMEOUT
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - the TTL frees it regardless
                pass

    async def _adopt_orphans(self) -> None:
        """Adopt active runs whose owner is gone (its lease lapsed)."""
        backend = self._backend()
        if backend is None:
            return
        for name, dagcfg in list(self._dags().items()):
            try:
                docs = await asyncio.wait_for(
                    backend.list_documents(self._ns(name)),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.TimeoutError:
                continue
            for body in docs:
                if dag.is_terminal_run(body):
                    continue
                run_key = body.get("runKey")
                if not isinstance(run_key, str):
                    continue
                ref = (name, run_key)
                if ref in self._owned:
                    continue
                await self._try_own(dagcfg, ref)

    # =====================================================================
    # Advancing an owned run
    # =====================================================================

    async def _advance_owned(self, now: float) -> None:
        for ref in list(self._owned):
            if self._wake.get(ref, 0.0) <= now:
                await self.advance_one(ref)

    async def advance_one(self, ref: RunRef) -> None:
        lock = self._locks.setdefault(ref, asyncio.Lock())
        async with lock:
            if ref not in self._owned:
                return
            dagcfg = self._dags().get(ref[0])
            if dagcfg is None:
                await self._release(ref)  # dag removed on reload
                return
            try:
                await self._do_advance(dagcfg, ref)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - never kill the loop
                logger.exception(
                    "dag run %s/%s: advance failed", ref[0], ref[1]
                )

    async def _do_advance(self, dagcfg: Any, ref: RunRef) -> None:
        spec = dagcfg.spec
        # 1. reconcile first: fail any task a crash left running with a dead
        # process (protects our own live tasks by proc token).
        await self._reconcile_run(dagcfg, ref)
        # 2. read the doc to see which mapped tasks are ready to expand, and
        # pre-read their upstream XCom lists (outside the RMW).
        body = await self._read(ref[0], ref[1])
        if body is None:
            await self._release(ref)
            return
        if dag.is_terminal_run(body):
            await self._on_terminal(ref)
            return
        run_id = str(body.get("runId"))
        expansions = await self._read_expansions(dagcfg, run_id, body)
        # 3. claim: atomically expand + propagate + claim ready tasks.
        now = _now()
        proc = self._cron._proc_token
        host = self._cron._state_host
        transform = self._wrap(
            dag.plan_and_claim(spec, now, proc, host, expansions)
        )
        stored, result = await self._mutate(ref[0], ref[1], transform)
        if result is None:
            return
        # 4. launch each claimed task (subprocess); record pid or fail. Each
        # launch is independent -- one failing must not skip the rest of the
        # batch (which would strand them claimed-but-unlaunched).
        for intent in result.launches:
            try:
                await self._launch_task(dagcfg, ref, run_id, intent)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - fail just this task, keep going
                logger.exception(
                    "dag run %s/%s: launching task %s failed",
                    ref[0],
                    ref[1],
                    intent.taskkey,
                )
                await self._finish_task(
                    dagcfg,
                    ref,
                    intent.taskkey,
                    intent.task_id,
                    success=False,
                    exit_code=127,
                    fail_reason="launch error",
                )
        # 5. terminal? release the lease; else schedule the next wake.
        final = stored if stored is not None else body
        if dag.is_terminal_run(final):
            await self._on_terminal(ref)
        else:
            self._wake[ref] = self._compute_wake(spec, final, now)

    async def _read_expansions(
        self, dagcfg: Any, run_id: str, body: Dict[str, Any]
    ) -> Dict[str, Optional[List[Any]]]:
        expansions: Dict[str, Optional[List[Any]]] = {}
        for tid, from_task, key in dag.tasks_awaiting_expansion(
            dagcfg.spec, body
        ):
            expansions[tid] = await self._read_xcom_list(
                run_id, dagcfg.name, from_task, key
            )
        return expansions

    async def _read_xcom_list(
        self, run_id: str, dag_name: str, taskkey: str, key: str
    ) -> Optional[List[Any]]:
        """The JSON list an upstream published, for a mapped task to fan out.

        Only ever read after the upstream has *succeeded*, so its output is
        final: a genuine list expands to itself; a definitively absent or
        non-list output expands to the **empty list** (a mapped task with no
        items -> success), so a mis-publishing upstream cannot wedge the run
        forever.  Only a transient store failure (timeout / error / a swept
        blob) returns ``None``, which leaves the task unexpanded to retry on a
        later pass rather than guessing an empty fan-out.
        """
        backend = self._backend()
        if backend is None:
            return None
        from yacron2 import jobstate

        scope = dag.xcom_scope(dag_name, run_id)
        name = dag.xcom_name(taskkey, key)
        try:
            got = await asyncio.wait_for(
                jobstate.artifact_get(backend, scope, name),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.TimeoutError:
            return None  # transient: retry next pass
        except jobstate.JobStateError:
            # the record survives but its blob is gone (410): definitively
            # unrecoverable, so map to empty rather than retry forever (blobs
            # are never swept in practice, so this is a belt-and-braces guard).
            logger.warning(
                "dag %s: xcom %r from %r has a missing blob; mapping to an "
                "empty fan-out",
                dag_name,
                key,
                taskkey,
            )
            return []
        except OSError:
            return None  # transient store error
        if got is None:
            # upstream succeeded without publishing this key: no items to map.
            return []
        _record, data = got
        try:
            parsed = json.loads(data.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            logger.warning(
                "dag %s: xcom %r from %r is not valid JSON; mapping it to an "
                "empty fan-out",
                dag_name,
                key,
                taskkey,
            )
            return []
        if isinstance(parsed, list):
            return parsed
        logger.warning(
            "dag %s: xcom %r from %r is a %s, not a list; mapping it to an "
            "empty fan-out",
            dag_name,
            key,
            taskkey,
            type(parsed).__name__,
        )
        return []

    def _compute_wake(
        self, spec: DagSpec, body: Dict[str, Any], now: float
    ) -> float:
        """The soonest instant this run wants advancing again.

        The nearest due sensor poke or task retry; a short poll while a gate is
        awaiting a decision (so an approval made on a *different* node -- which
        cannot advance a run it does not own -- is picked up by the owner in a
        few seconds, not a full idle cycle); a longer floor otherwise (each
        task completion on the owning node also forces an advance).
        """
        soonest = now + 60.0
        for entry in body.get("tasks", {}).values():
            state = entry.get("state")
            if state == dag.RUNNING and entry.get("awaitingApproval"):
                soonest = min(soonest, now + APPROVAL_POLL_INTERVAL)
            elif state == dag.RUNNING and entry.get("nextPokeAt") is not None:
                soonest = min(soonest, float(entry["nextPokeAt"]))
            elif state == dag.UP_FOR_RETRY and entry.get("nextRetryAt"):
                soonest = min(soonest, float(entry["nextRetryAt"]))
        return soonest

    async def _on_terminal(self, ref: RunRef) -> None:
        logger.info("dag run %s/%s reached a terminal state", ref[0], ref[1])
        await self._release(ref)

    # =====================================================================
    # Launching a task instance (reuses the RunningJob/job-API machinery)
    # =====================================================================

    async def _launch_task(
        self, dagcfg: Any, ref: RunRef, run_id: str, intent
    ) -> None:
        template = dagcfg.task_templates[intent.task_id]
        taskkey = intent.taskkey
        token, env = self._prepare_task_run(
            dagcfg, run_id, ref[1], intent, template
        )
        dref = _DagRef(
            dag_name=dagcfg.name,
            run_key=ref[1],
            run_id=run_id,
            task_id=intent.task_id,
            taskkey=taskkey,
        )
        running = RunningJob(
            template,
            None,
            extra_env=env,
            state_token=token,
            run_id=env.get(dag.ENV_DAG_RUN_ID),
            dag_ref=dref,
        )
        try:
            await running.start()
        except BaseException:  # noqa: BLE001 - mirror maybe_launch_job cleanup
            if token is not None and self._cron._job_api is not None:
                await self._cron._job_api.finish_run(token)
            await self._finish_task(
                dagcfg,
                ref,
                taskkey,
                intent.task_id,
                success=False,
                exit_code=127,
                fail_reason="launch failed",
            )
            return
        self._cron.running_jobs[template.name].append(running)
        self._cron._jobs_running.set()
        pid = running.proc.pid if running.proc is not None else None
        # best-effort: the task already owns its slot (proc was set at claim,
        # so reconciliation protects it even without a pid), and the reaper
        # will record its completion; a failed pid write must not abort the
        # launch batch or, worse, fail an already-running task.
        try:
            await self._set_pid(dagcfg, ref, taskkey, pid)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - the pid is an optimisation
            logger.warning(
                "dag run %s/%s: could not record pid for task %s",
                ref[0],
                ref[1],
                taskkey,
            )

    def _prepare_task_run(
        self, dagcfg: Any, run_id: str, run_key: str, intent, template
    ) -> Tuple[Optional[str], Dict[str, str]]:
        """Register the task run with the loopback API; return its env.

        Mirrors ``Cron._prepare_job_api_run`` but scopes the run's default
        namespace to the DAG run's XCom scope and injects the ``YACRON2_DAG_*``
        vars, so ``yacron2 xcom`` / ``artifact`` land in the run scope.
        """
        scope = dag.xcom_scope(dagcfg.name, run_id)
        dag_env = {
            dag.ENV_DAG_NAME: dagcfg.name,
            dag.ENV_DAG_RUN_ID: run_id,
            dag.ENV_DAG_RUN_KEY: run_key,
            dag.ENV_DAG_TASK: intent.task_id,
            dag.ENV_DAG_TASKKEY: intent.taskkey,
            dag.ENV_DAG_MAP_INDEX: (
                "" if intent.map_index is None else str(intent.map_index)
            ),
            dag.ENV_DAG_MAP_ITEM: (
                "" if intent.map_item is None else json.dumps(intent.map_item)
            ),
            dag.ENV_DAG_XCOM_SCOPE: scope,
        }
        api = self._cron._job_api
        if api is None or api.base_url is None:
            return None, dag_env
        from yacron2.config import _resolve_secret
        from yacron2.jobapi import RunContext, run_environment

        secrets: Dict[str, str] = {}
        for spec in template.secrets:
            name = spec.get("name")
            try:
                value = _resolve_secret(
                    spec,
                    "dag {} task {} secret {}".format(
                        dagcfg.name, intent.task_id, name
                    ),
                )
            except Exception:  # noqa: BLE001 - a bad secret is skipped, 404s
                continue
            if name and value is not None:
                secrets[name] = value
        ctx = RunContext(
            token=os.urandom(32).hex(),
            run_id=os.urandom(16).hex(),
            job_name=template.name,
            attempt=intent.attempt,
            scheduled_at=None,
            host=self._cron._state_host,
            default_scope=scope,
            secrets=secrets,
        )
        api.register_run(ctx)
        env = run_environment(ctx, api.base_url)
        env.update(dag_env)
        return ctx.token, env

    async def _set_pid(
        self, dagcfg: Any, ref: RunRef, taskkey: str, pid: Optional[int]
    ) -> None:
        transform = self._wrap(
            dag.set_task_pid(taskkey, self._cron._proc_token, pid, _now())
        )
        await self._mutate(ref[0], ref[1], transform)

    # =====================================================================
    # Completion (called by the reaper via cron._handle_finished_job)
    # =====================================================================

    async def on_task_finished(self, running: RunningJob) -> None:
        dref = running.dag_ref
        assert dref is not None  # only called for a DAG-task RunningJob
        dagcfg = self._dags().get(dref.dag_name)
        if dagcfg is None:
            return  # dag removed mid-run; the doc is orphaned, GC handles it
        success = running.fail_reason is None
        await self._finish_task(
            dagcfg,
            (dref.dag_name, dref.run_key),
            dref.taskkey,
            dref.task_id,
            success=success,
            exit_code=running.retcode,
            fail_reason=running.fail_reason,
        )

    async def _finish_task(
        self,
        dagcfg: Any,
        ref: RunRef,
        taskkey: str,
        task_id: str,
        *,
        success: bool,
        exit_code: Optional[int],
        fail_reason: Optional[str],
    ) -> None:
        task = dagcfg.spec.by_id.get(task_id)
        if task is None:
            # the task was removed from the DAG (a config reload) while its
            # instance was running: drop the stale completion.
            return
        jitter = _jitter(task.poke_jitter) if task.type == dag.SENSOR else 0.0
        transform = self._wrap(
            dag.mark_task_finished(
                taskkey,
                success=success,
                exit_code=exit_code,
                fail_reason=fail_reason,
                now=_now(),
                task=task,
                jitter=jitter,
            )
        )
        await self._mutate(ref[0], ref[1], transform)
        # trigger a fresh advance without blocking the reaper on the per-run
        # lock (a concurrent periodic advance may hold it).
        self._wake[ref] = 0.0
        self._cron._track_state_write(self.advance_one(ref))

    # =====================================================================
    # Crash reconciliation
    # =====================================================================

    async def reconcile_on_boot(self) -> None:
        """Adopt and reconcile this node's active runs after a restart.

        Called from rehydration (after the job reconciler).  Lists every dag's
        active runs and tries to take each one's lease; a run still owned by a
        live peer stays with it, one whose owner is gone is adopted here and
        its interrupted tasks reconciled from durable state -- the DAG analogue
        of ``Cron._reconcile_inflight``.
        """
        backend = self._backend()
        if backend is None or not self.has_dags():
            return
        for name, dagcfg in list(self._dags().items()):
            try:
                docs = await asyncio.wait_for(
                    backend.list_documents(self._ns(name)),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "dag %s: boot reconciliation timed out reading runs", name
                )
                continue
            for body in docs:
                if dag.is_terminal_run(body):
                    continue
                run_key = body.get("runKey")
                if not isinstance(run_key, str):
                    continue
                await self._try_own(dagcfg, (name, run_key))

    async def _reconcile_run(self, dagcfg: Any, ref: RunRef) -> None:
        transform = self._wrap(
            dag.reconcile_crashed(
                dagcfg.spec,
                _now(),
                self._cron._proc_token,
                self._cron._state_host,
                platform.pid_alive,
            )
        )
        _stored, changed = await self._mutate(ref[0], ref[1], transform)
        if changed:
            logger.info(
                "dag run %s/%s: reconciled %d interrupted task(s)",
                ref[0],
                ref[1],
                changed,
            )

    # =====================================================================
    # Control-API surface: approvals, introspection, manual trigger, backfill
    # =====================================================================

    async def approve(
        self,
        dag_name: str,
        run_key: str,
        taskkey: str,
        *,
        approved: bool,
        by: str,
    ) -> Dict[str, Any]:
        """Record an approval-gate decision, then advance the run."""
        dagcfg = self._dags().get(dag_name)
        if dagcfg is None:
            return {"ok": False, "reason": "no such dag"}
        task_id = taskkey.split("#", 1)[0]
        task = dagcfg.spec.by_id.get(task_id)
        on_reject = task.on_reject if task is not None else dag.FAILED
        transform = self._wrap(
            dag.apply_approval(
                taskkey,
                approved=approved,
                by=by,
                now=_now(),
                on_reject=on_reject,
            )
        )
        _stored, result = await self._mutate(dag_name, run_key, transform)
        if result and result.get("ok"):
            self._wake[(dag_name, run_key)] = 0.0
            self._cron._track_state_write(
                self.advance_one((dag_name, run_key))
            )
        return result or {"ok": False, "reason": "no such run"}

    async def trigger_run(
        self, dag_name: str, *, logical_date: Optional[str] = None
    ) -> Optional[str]:
        """Create a manual run of ``dag_name`` now; return its run key."""
        dagcfg = self._dags().get(dag_name)
        if dagcfg is None:
            return None
        run_key = "manual-" + os.urandom(6).hex()
        created = await self._create_doc(
            dagcfg, run_key, logical_date, "manual"
        )
        if created:
            await self._try_own(dagcfg, (dag_name, run_key))
        return run_key

    async def backfill(
        self, dag_name: str, start_iso: str, end_iso: str
    ) -> Dict[str, Any]:
        """Create runs for every scheduled instant in ``[start, end]``.

        A deliberate replay: it is bounded by ``DAG_MAX_CATCHUP`` but ignores
        the automatic catch-up deadline (the operator asked for it).
        Idempotent -- each date's run key create-if-absents, so re-running a
        backfill does not duplicate runs.
        """
        dagcfg = self._dags().get(dag_name)
        if dagcfg is None or dagcfg.schedule_job is None:
            return {"ok": False, "reason": "no such scheduled dag"}
        start = _parse_iso(start_iso)
        end = _parse_iso(end_iso)
        if start is None or end is None or end < start:
            return {"ok": False, "reason": "bad date range"}
        sched = dagcfg.schedule_job
        created = 0
        # step from just before start so an instant exactly at start counts
        cursor = start - datetime.timedelta(seconds=1)
        nxt = self._cron._compute_next_fire(sched, cursor)
        while nxt is not None and nxt <= end and created < DAG_MAX_CATCHUP:
            await self._create_run(dagcfg, nxt, "backfill")
            created += 1
            nxt = self._cron._compute_next_fire(sched, nxt)
        return {"ok": True, "created": created}

    def list_dags(self) -> List[Dict[str, Any]]:
        out = []
        for name, dagcfg in self._dags().items():
            out.append(
                {
                    "name": name,
                    "enabled": dagcfg.enabled,
                    "scheduled": dagcfg.schedule_job is not None,
                    "tasks": [
                        {
                            "id": t.spec.id,
                            "type": t.spec.type,
                            "dependsOn": list(t.spec.depends_on),
                        }
                        for t in dagcfg.tasks
                    ],
                }
            )
        return out

    async def get_run(
        self, dag_name: str, run_key: str
    ) -> Optional[Dict[str, Any]]:
        if dag_name not in self._dags():
            return None
        return await self._read(dag_name, run_key)

    async def list_runs(
        self, dag_name: str, *, limit: int = 50
    ) -> Optional[List[Dict[str, Any]]]:
        backend = self._backend()
        if backend is None or dag_name not in self._dags():
            return None
        docs = await asyncio.wait_for(
            backend.list_documents(self._ns(dag_name)),
            timeout=STATE_OP_TIMEOUT,
        )
        docs.sort(key=lambda b: float(b.get("createdAt") or 0), reverse=True)
        return [_run_summary(b) for b in docs[:limit]]

    # =====================================================================
    # Retention GC (DAG-owned; dag_run documents live outside the record GC)
    # =====================================================================

    async def _gc_runs(self) -> None:
        backend = self._backend()
        if backend is None:
            return
        for name, dagcfg in list(self._dags().items()):
            try:
                await self._gc_one_dag(backend, name, dagcfg)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("dag %s: run GC failed", name)

    async def _gc_one_dag(
        self, backend: StateBackend, name: str, dagcfg: Any
    ) -> None:
        docs = await asyncio.wait_for(
            backend.list_documents(self._ns(name)),
            timeout=STATE_OP_TIMEOUT,
        )
        terminal = [b for b in docs if dag.is_terminal_run(b)]
        terminal.sort(key=lambda b: float(b.get("createdAt") or 0))
        excess = len(terminal) - dagcfg.retain_runs
        if excess <= 0:
            return
        for body in terminal[:excess]:
            run_key = body.get("runKey")
            run_id = body.get("runId")
            if not run_key:
                continue
            await asyncio.wait_for(
                backend.delete_document(self._ns(name), run_key),
                timeout=STATE_OP_TIMEOUT,
            )
            if run_id:
                # drop the run's XCom record stream too (its blobs are
                # content-addressed and never swept, so no XCom can ever
                # reference a collected blob).
                from yacron2.jobstate import ARTIFACT_STREAM_PREFIX

                scope = dag.xcom_scope(name, str(run_id))
                try:
                    await asyncio.wait_for(
                        backend.prune_records(
                            ARTIFACT_STREAM_PREFIX + scope, keep=0
                        ),
                        timeout=STATE_OP_TIMEOUT,
                    )
                except Exception:  # noqa: BLE001 - best effort
                    pass

    # =====================================================================
    # Shutdown
    # =====================================================================

    async def shutdown(self) -> None:
        """Release every held advance lease and stop the renewers."""
        if self._service_task is not None and not self._service_task.done():
            self._service_task.cancel()
        for ref in list(self._owned):
            await self._release(ref)

    def forget(self) -> None:
        """Drop all in-memory run ownership after a backend swap.

        The old store's advance leases lapse by TTL (their renewers are
        cancelled here, since renewing them against a dead store is pointless);
        the new store's active runs are re-adopted from scratch by
        :meth:`reconcile_on_boot`, which reruns because the backend swap
        cleared ``_state_rehydrated``.  No store ops here -- the old backend is
        already gone.
        """
        if self._service_task is not None and not self._service_task.done():
            self._service_task.cancel()
        self._service_task = None
        for renewer in list(self._renewers.values()):
            if not renewer.done():
                renewer.cancel()
        self._renewers.clear()
        self._owned.clear()
        self._wake.clear()
        self._locks.clear()
        self._next_logical.clear()
        self._seeded.clear()
        self._next_sched_check = 0.0
        self._next_adopt = 0.0
        self._next_gc = 0.0


# --------------------------------------------------------------------------
# module helpers
# --------------------------------------------------------------------------


def _parse_iso(value: Optional[str]) -> Optional[datetime.datetime]:
    if not value:
        return None
    try:
        dt = datetime.datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt


def _run_summary(body: Dict[str, Any]) -> Dict[str, Any]:
    tasks = body.get("tasks", {})
    counts: Dict[str, int] = {}
    for entry in tasks.values():
        st = entry.get("state", "unknown")
        counts[st] = counts.get(st, 0) + 1
    return {
        "runKey": body.get("runKey"),
        "runId": body.get("runId"),
        "state": body.get("state"),
        "kind": body.get("kind"),
        "logicalDate": body.get("logicalDate"),
        "createdAt": body.get("createdAt"),
        "updatedAt": body.get("updatedAt"),
        "taskStates": counts,
    }
