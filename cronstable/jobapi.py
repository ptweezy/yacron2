"""The loopback endpoint that hands the durable store to job commands.

cronstable exposes its durable state to the *jobs it runs*, not just to
the scheduler.  The mechanism is a small HTTP server bound to loopback that
the daemon stands up alongside the dashboard, plus a per-run bearer token the
daemon injects into every job's environment.  A job's ``cronstable
state|cursor|
lock|artifact|idempotent|secret`` command (see :mod:`cronstable.jobcli`) is a
thin client of this endpoint; the identical logic is reachable offline against
the store directly, so this server is a *front-end*, not a second source of
truth.

Why route the primitives through the daemon at all, rather than let each job
open the store itself?  Three of the six need the live daemon:

* a **mutex / semaphore** is a lease that must be *renewed* for as long as the
  job holds it and *released* the instant the run ends (even on a crash) --
  the daemon already runs exactly this machinery for cluster concurrency slots
  (:mod:`cronstable.cron`), and a short-lived ``cronstable lock`` subprocess
  cannot;
* **run-scoped secrets** are resolved fresh per run and staged *in memory*, so
  they never touch the store and vanish when the run ends -- only the daemon
  holds them;
* every primitive is scoped and authorised by *which run is calling*, which
  the per-run token establishes without the job proving anything.

The KV / cursor / idempotency / artifact primitives themselves are pure
functions over the backend (:mod:`cronstable.jobstate`); this module adds the
per-run token registry, the secret staging, the lease-backed lock manager, and
the HTTP surface over them.  It is imported only when a ``state`` section with
``jobApi.enabled`` is configured, so the stateless install pays nothing.
"""

import asyncio
import hmac
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set
from urllib.parse import urlparse

from aiohttp import web

from cronstable import _json, jobstate
from cronstable.jobstate import GLOBAL_SCOPE, JobStateError
from cronstable.state import Lease, StateBackend, _DocumentUnreadable

logger = logging.getLogger("cronstable.jobapi")

# Every awaited backend call is capped so a wedged store cannot hang a job's
# loopback request forever (mirrors cron.STATE_OP_TIMEOUT; kept local to avoid
# importing cron, which imports this module).
STATE_OP_TIMEOUT = 10.0

# The lease-name prefix for job mutex/semaphore permits.  A permit slot is
# ``lock/<scope>/<name>#<i>``; distinct from every scheduler lease name.
LOCK_LEASE_PREFIX = "lock/"

# Environment variables injected into every job when the endpoint is enabled.
# CRONSTABLE_JOB_NAME deliberately matches the key ShellReporter already sets.
ENV_URL = "CRONSTABLE_STATE_URL"
ENV_TOKEN = "CRONSTABLE_STATE_TOKEN"
ENV_RUN_ID = "CRONSTABLE_RUN_ID"
ENV_JOB_NAME = "CRONSTABLE_JOB_NAME"
ENV_ATTEMPT = "CRONSTABLE_ATTEMPT"
ENV_SCHEDULED_AT = "CRONSTABLE_SCHEDULED_AT"
ENV_HOST = "CRONSTABLE_HOST"


@dataclass
class RunContext:
    """One running job's identity, as the loopback endpoint sees it.

    Keyed by its opaque ``token`` (the bearer secret injected into the job's
    environment).  ``default_scope`` is the job's own name, so a job's KV /
    cursor / artifact calls land in its private namespace unless it names a
    shared scope.  ``allowed_scopes`` is the operator-configured allowlist
    (``job.stateAllowedScopes``) of additional scope names this run may name
    explicitly -- without it, a run could pass ANY string as ``scope`` and
    reach straight into another job's private namespace (which is simply that
    job's name), reading, overwriting or destroying its state.  ``secrets`` is
    the run-scoped, in-memory staging table -- resolved by the daemon at
    launch and dropped when the run ends.
    """

    token: str
    run_id: str
    job_name: str
    attempt: int
    scheduled_at: Optional[str]
    host: str
    default_scope: str
    allowed_scopes: Set[str] = field(default_factory=set)
    secrets: Dict[str, str] = field(default_factory=dict)


@dataclass
class _LockHold:
    """One held mutex/semaphore permit, on a run's behalf."""

    hold_token: str
    run_token: str
    lease_name: str
    lease: Lease
    scope: str
    name: str
    slot: int
    # the TTL this hold's lease was actually taken with (a per-acquire --ttl
    # may differ from the manager default), so the renewer renews on the right
    # schedule -- renewing a short lease on the default cadence would let it
    # lapse before its first renewal and be stolen.
    ttl: float = 0.0
    renewer: Optional[asyncio.Task] = None
    lost: bool = False


def run_environment(ctx: RunContext, base_url: str) -> Dict[str, str]:
    """The ``CRONSTABLE_*`` env the daemon injects for one run.

    All values are ``str`` (Windows and POSIX both reject non-str env values);
    an unknown scheduled time is the empty string rather than absent, so a job
    can test the variable rather than guess whether it was set.
    """
    return {
        ENV_URL: base_url,
        ENV_TOKEN: ctx.token,
        ENV_RUN_ID: ctx.run_id,
        ENV_JOB_NAME: ctx.job_name,
        ENV_ATTEMPT: str(ctx.attempt),
        ENV_SCHEDULED_AT: ctx.scheduled_at or "",
        ENV_HOST: ctx.host,
    }


class JobLockManager:
    """Holds job mutex/semaphore leases on the run's behalf, renewing them.

    A job acquires a lock by name; the daemon takes a TTL lease per permit
    slot, renews it at a third of the TTL while the job holds it, and releases
    it the instant the job releases OR the run ends -- so a job that crashes or
    forgets to unlock never leaks a lock (the lease also self-frees by TTL as
    the ultimate backstop).  This mirrors the cluster concurrency-slot
    machinery in :mod:`cronstable.cron`; it is the same lease API and the same
    at-least-once honesty (a holder that loses its lease to a store outage is
    warned but its job keeps running -- the fence token in the acquire reply
    is there for a job that needs true fencing).

    Each hold takes a *unique* lease holder string, so two runs on the *same*
    daemon contending for one mutex exclude each other exactly as two nodes
    do: the second's acquire sees the slot held by a different holder and is
    denied, rather than silently renewing the first's lease.
    """

    def __init__(
        self,
        backend_getter: Callable[[], Optional[StateBackend]],
        base_holder: str,
        ttl: float,
        is_run_live: Callable[[str], bool],
    ) -> None:
        self._backend_getter = backend_getter
        self._base_holder = base_holder
        self._ttl = max(5.0, float(ttl))
        # whether a run token is still a live, registered run.  A blocking
        # acquire can win its lease *after* its run has already ended (the run
        # was killed while the acquire request sat in the wait loop); recording
        # a hold + renewer then would pin the lease forever, since finish_run
        # already ran and released nothing.  Checked before every hold is
        # recorded, with no await between the check and the record so the run
        # cannot end in the gap.
        self._is_run_live = is_run_live
        self._holds: Dict[str, _LockHold] = {}
        self._run_holds: Dict[str, Set[str]] = {}

    @staticmethod
    def _slot_lease_name(scope: str, name: str, slot: int) -> str:
        return "{}{}/{}#{}".format(LOCK_LEASE_PREFIX, scope, name, slot)

    async def acquire(
        self,
        run_token: str,
        scope: str,
        name: str,
        *,
        permits: int = 1,
        ttl: Optional[float] = None,
        wait: bool = False,
        block_seconds: float = 0.0,
    ) -> Dict[str, Any]:
        """Acquire one of ``permits`` slots of lock ``name``.

        Returns ``{"acquired": bool, ...}``; on success also ``token`` (the
        opaque hold id to release with), ``slot`` (which permit) and ``fence``
        (the lease's monotonic fence, for a job that wants to fence its own
        writes).  With ``wait`` it retries up to ``block_seconds`` before
        giving up; without, it makes a single pass over the slots.
        """
        if permits < 1:
            raise JobStateError("permits must be >= 1")
        backend = self._backend_getter()
        if backend is None:
            raise JobStateError("state backend is unavailable", status=503)
        ttl = self._ttl if ttl is None else max(5.0, float(ttl))
        hold_token = os.urandom(16).hex()
        holder = "{}#{}".format(self._base_holder, hold_token)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, block_seconds) if wait else None
        while True:
            for slot in range(permits):
                lease_name = self._slot_lease_name(scope, name, slot)
                try:
                    lease = await asyncio.wait_for(
                        backend.acquire_lease(lease_name, holder, ttl),
                        timeout=STATE_OP_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    # unknown, not denied: skip this slot for now (a later
                    # pass, or the TTL, sorts out an acquire that did land).
                    lease = None
                if lease is not None:
                    if not self._is_run_live(run_token):
                        # the run ended while we were blocked here: do NOT
                        # record a hold or start a renewer for a dead run (it
                        # would pin the lease until the daemon restarts, since
                        # finish_run already ran and found nothing to release).
                        # Hand the lease straight back and report not-acquired.
                        await self._safe_release(backend, lease)
                        return {"acquired": False, "runEnded": True}
                    return await self._record_hold(
                        backend,
                        run_token,
                        hold_token,
                        lease,
                        scope,
                        name,
                        slot,
                        ttl,
                    )
            if deadline is None or loop.time() >= deadline:
                return {"acquired": False}
            await asyncio.sleep(min(1.0, max(0.05, self._ttl / 10)))

    @staticmethod
    async def _safe_release(backend: StateBackend, lease: Lease) -> None:
        try:
            await asyncio.wait_for(
                backend.release_lease(lease), timeout=STATE_OP_TIMEOUT
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - the TTL frees it regardless
            pass

    async def _record_hold(
        self,
        backend: StateBackend,
        run_token: str,
        hold_token: str,
        lease: Lease,
        scope: str,
        name: str,
        slot: int,
        ttl: float,
    ) -> Dict[str, Any]:
        hold = _LockHold(
            hold_token=hold_token,
            run_token=run_token,
            lease_name=lease.name,
            lease=lease,
            scope=scope,
            name=name,
            slot=slot,
            ttl=ttl,
        )
        self._holds[hold_token] = hold
        self._run_holds.setdefault(run_token, set()).add(hold_token)
        hold.renewer = asyncio.ensure_future(self._renew_loop(hold_token))
        return {
            "acquired": True,
            "token": hold_token,
            "slot": slot,
            "fence": lease.fence,
            "ttl": ttl,
        }

    async def _renew_loop(self, hold_token: str) -> None:
        # renew on THIS hold's TTL (a per-acquire --ttl may be shorter than the
        # manager default); a third of it leaves headroom for renew latency.
        hold = self._holds.get(hold_token)
        ttl = hold.ttl if hold is not None else self._ttl
        period = max(1.0, ttl / 3)
        while True:
            await asyncio.sleep(period)
            hold = self._holds.get(hold_token)
            backend = self._backend_getter()
            if hold is None or backend is None:
                return
            try:
                renewed = await asyncio.wait_for(
                    backend.renew_lease(hold.lease, hold.ttl),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.TimeoutError:
                continue  # unknown: try again next period
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - renewal is best-effort
                continue
            if renewed is not None:
                hold.lease = renewed
                continue
            # renew positively refused: the lease was taken over (our hold
            # lapsed and a peer grabbed it).  Stop renewing but leave the hold
            # recorded and flag it lost -- the job believes it still holds the
            # lock, which is the documented at-least-once overlap.
            hold.lost = True
            logger.warning(
                "job lock %s (%s slot %d) was taken over after its lease "
                "lapsed; the job still believes it holds it (at-least-once)",
                hold.name,
                hold.scope,
                hold.slot,
            )
            return

    async def release(self, run_token: str, hold_token: str) -> bool:
        """Release a hold this run owns; return whether it was held."""
        hold = self._holds.get(hold_token)
        if hold is None or hold.run_token != run_token:
            return False
        return await self._release_hold(hold)

    async def _release_hold(self, hold: _LockHold) -> bool:
        self._holds.pop(hold.hold_token, None)
        held = self._run_holds.get(hold.run_token)
        if held is not None:
            held.discard(hold.hold_token)
            if not held:
                self._run_holds.pop(hold.run_token, None)
        if hold.renewer is not None and not hold.renewer.done():
            hold.renewer.cancel()
        backend = self._backend_getter()
        if backend is not None and not hold.lost:
            try:
                await asyncio.wait_for(
                    backend.release_lease(hold.lease), timeout=STATE_OP_TIMEOUT
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - the TTL frees it regardless
                pass
        return True

    async def release_all(self, run_token: str) -> None:
        """Release every lock a finishing (or gone) run still holds."""
        for hold_token in list(self._run_holds.get(run_token, ())):
            hold = self._holds.get(hold_token)
            if hold is not None:
                await self._release_hold(hold)


class JobStateAPI:
    """The loopback HTTP server plus its per-run token and secret registry.

    Constructed with a *getter* for the current state backend (which the
    scheduler may swap on a config reload), the host name, a base lease-holder
    string, and the resolved ``state.jobApi`` config.  Owned by
    :class:`cronstable.cron.Cron`, which registers a run before launching it
    and
    finishes it (dropping its token and secrets, releasing its locks) when it
    ends.
    """

    def __init__(
        self,
        backend_getter: Callable[[], Optional[StateBackend]],
        *,
        host: str,
        base_holder: str,
        config: Dict[str, Any],
    ) -> None:
        self._backend_getter = backend_getter
        self._host = host
        self._config = config
        self._runs: Dict[str, RunContext] = {}
        self._runner: Optional[web.AppRunner] = None
        self._base_url: Optional[str] = None
        self._max_value_bytes = int(config.get("maxValueBytes") or 0)
        self._max_artifact_bytes = int(config.get("maxArtifactBytes") or 0)
        self.locks = JobLockManager(
            backend_getter,
            base_holder,
            float(config.get("lockTtlSeconds") or 30),
            is_run_live=lambda token: token in self._runs,
        )

    # --- lifecycle -------------------------------------------------------

    @property
    def base_url(self) -> Optional[str]:
        return self._base_url

    def _bind_target(self) -> "tuple[str, int]":
        """``(host, port)`` to bind: the configured listen, or ephemeral."""
        listen = self._config.get("listen")
        if not listen:
            return "127.0.0.1", 0
        text = str(listen)
        if "://" not in text:
            text = "http://" + text
        parsed = urlparse(text)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port if parsed.port is not None else 0
        return host, port

    def _client_max_size(self) -> int:
        """The transport-level body cap, derived from the configured limits.

        aiohttp defaults ``client_max_size`` to 1 MiB, which would silently
        override ``maxArtifactBytes``/``maxValueBytes`` (a put under the
        64 MiB artifact default would 413 at 1 MiB regardless).  0 in either
        configured limit is the documented "no limit", so the transport cap
        is lifted too (0 disables aiohttp's check); otherwise allow the
        larger limit plus JSON-envelope headroom (string escaping can
        inflate a maxValueBytes value up to 6x on the wire).  The handlers
        still enforce the configured limits with truthful 413s.
        """
        if self._max_value_bytes <= 0 or self._max_artifact_bytes <= 0:
            return 0
        return (
            max(self._max_artifact_bytes, self._max_value_bytes * 6)
            + 64 * 1024
        )

    async def start(self) -> None:
        app = web.Application(
            middlewares=self._middlewares(),
            client_max_size=self._client_max_size(),
        )
        app.add_routes(self._routes())
        runner = web.AppRunner(app)
        await runner.setup()
        host, port = self._bind_target()
        site = web.TCPSite(runner, host, port)
        await site.start()
        # read the actually-bound address (the port is OS-assigned when 0).
        bound_host, bound_port = "127.0.0.1", port
        if runner.addresses:
            addr = runner.addresses[0]
            bound_host, bound_port = str(addr[0]), int(addr[1])
        self._base_url = "http://{}:{}".format(bound_host, bound_port)
        self._runner = runner
        logger.info("state job API listening on %s", self._base_url)

    async def stop(self) -> None:
        # release every still-held lock before dropping the server, so a
        # shutdown does not leave the fleet's locks pinned for a whole TTL.
        for token in list(self._runs):
            await self.locks.release_all(token)
        self._runs.clear()
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
            self._base_url = None

    # --- run registry ----------------------------------------------------

    def register_run(self, ctx: RunContext) -> None:
        self._runs[ctx.token] = ctx

    async def finish_run(self, token: Optional[str]) -> None:
        """Drop a finished run's token/secrets and release its locks."""
        if not token:
            return
        self._runs.pop(token, None)
        await self.locks.release_all(token)

    # --- backend access --------------------------------------------------

    def _backend(self) -> StateBackend:
        backend = self._backend_getter()
        if backend is None:
            raise JobStateError("state backend is unavailable", status=503)
        return backend

    # --- middlewares -----------------------------------------------------

    def _middlewares(self) -> List[Any]:
        @web.middleware
        async def error_mw(request: web.Request, handler: Any) -> Any:
            try:
                return await handler(request)
            except web.HTTPException:
                raise
            except JobStateError as ex:
                return web.json_response({"error": str(ex)}, status=ex.status)
            except _json.UnsupportedValue as ex:
                # defence in depth: the value primitives pre-validate via
                # _check_size, but any handler that writes a client value
                # without it would otherwise let a non-portable value surface
                # as a 500.  It is the caller's bad input -> a clean 400.
                return web.json_response({"error": str(ex)}, status=400)
            except (
                asyncio.TimeoutError,
                OSError,
                _DocumentUnreadable,
            ) as ex:
                logger.warning("state job API: backend error: %s", ex)
                return web.json_response(
                    {"error": "state backend unavailable: {}".format(ex)},
                    status=503,
                )

        return [error_mw]

    def _run(self, request: web.Request) -> RunContext:
        """The authenticated run for ``request``, or raise 401.

        Every handler resolves its caller here rather than through middleware
        request-storage: the bearer token is matched in constant time against
        the (tiny) live run set, so a forged or stale token is rejected before
        any state is touched.
        """
        header = request.headers.get("Authorization", "")
        scheme, _, presented = header.partition(" ")
        if scheme.lower() != "bearer" or not presented:
            raise web.HTTPUnauthorized()
        try:
            # compare as bytes: compare_digest raises TypeError on any
            # non-ASCII str (turning a garbage token into a 500, not a 401),
            # and a header that cannot even encode (surrogates from raw
            # header bytes) can never match a real token.
            presented_bytes = presented.encode("utf-8")
        except UnicodeEncodeError:
            raise web.HTTPUnauthorized() from None
        for token, ctx in self._runs.items():
            if hmac.compare_digest(presented_bytes, token.encode("utf-8")):
                return ctx
        raise web.HTTPUnauthorized()

    def _routes(self) -> List[Any]:
        return [
            web.get("/v1/run", self._h_run),
            web.get("/v1/kv/get", self._h_kv_get),
            web.post("/v1/kv/set", self._h_kv_set),
            web.post("/v1/kv/delete", self._h_kv_delete),
            web.get("/v1/kv/list", self._h_kv_list),
            web.get("/v1/cursor/get", self._h_cursor_get),
            web.post("/v1/cursor/advance", self._h_cursor_advance),
            web.post("/v1/idempotency/claim", self._h_idem_claim),
            web.post("/v1/idempotency/release", self._h_idem_release),
            web.post("/v1/artifact/put", self._h_artifact_put),
            web.get("/v1/artifact/get", self._h_artifact_get),
            web.get("/v1/artifact/list", self._h_artifact_list),
            web.post("/v1/lock/acquire", self._h_lock_acquire),
            web.post("/v1/lock/release", self._h_lock_release),
            web.get("/v1/secret/get", self._h_secret_get),
            web.get("/v1/secret/list", self._h_secret_list),
        ]

    # --- request helpers -------------------------------------------------

    @staticmethod
    async def _json_body(request: web.Request) -> Dict[str, Any]:
        if not request.can_read_body:
            return {}
        try:
            body = await request.json()
        except web.HTTPRequestEntityTooLarge:
            # the transport cap fired while reading the body: that is a
            # truthful 413, not "not valid JSON" -- let it out unmasked.
            raise
        except Exception as ex:  # noqa: BLE001 - a malformed body is a 400
            raise JobStateError("request body is not valid JSON") from ex
        if not isinstance(body, dict):
            raise JobStateError("request body must be a JSON object")
        return body

    @staticmethod
    def _scope(ctx: RunContext, given: Optional[str]) -> str:
        """The scope a call may act in, authorising any explicitly-named one.

        A run always reaches its own ``default_scope`` and the conventional
        shared ``global`` namespace (the documented ``--global`` coordination
        path); any OTHER name must be in the job's configured
        ``stateAllowedScopes`` -- otherwise it is most likely another job's
        own name, i.e. that job's private scope, and letting it through would
        let one job read, overwrite or destroy an unrelated job's state.
        """
        if not given:
            return ctx.default_scope
        if (
            given != ctx.default_scope
            and given != GLOBAL_SCOPE
            and given not in ctx.allowed_scopes
        ):
            raise JobStateError(
                "scope {!r} is not permitted for job {!r}; add it to "
                "stateAllowedScopes to allow this job to use it".format(
                    given, ctx.job_name
                ),
                status=403,
            )
        return given

    @staticmethod
    def _require(value: Optional[str], what: str) -> str:
        if not value:
            raise JobStateError("{} is required".format(what))
        return value

    # --- handlers: run ---------------------------------------------------

    async def _h_run(self, request: web.Request) -> web.Response:
        ctx = self._run(request)
        return web.json_response(
            {
                "runId": ctx.run_id,
                "job": ctx.job_name,
                "attempt": ctx.attempt,
                "scheduledAt": ctx.scheduled_at,
                "host": ctx.host,
                "defaultScope": ctx.default_scope,
            }
        )

    # --- handlers: kv ----------------------------------------------------

    async def _h_kv_get(self, request: web.Request) -> web.Response:
        ctx = self._run(request)
        scope = self._scope(ctx, request.query.get("scope"))
        key = self._require(request.query.get("key"), "key")
        body = await jobstate.kv_get(self._backend(), scope, key)
        if body is None:
            raise web.HTTPNotFound()
        return web.json_response(
            {"value": body.get("value"), "updatedAt": body.get("updatedAt")}
        )

    async def _h_kv_set(self, request: web.Request) -> web.Response:
        ctx = self._run(request)
        payload = await self._json_body(request)
        scope = self._scope(ctx, payload.get("scope"))
        key = self._require(payload.get("key"), "key")
        body = await jobstate.kv_set(
            self._backend(),
            scope,
            key,
            payload.get("value"),
            max_bytes=self._max_value_bytes,
        )
        return web.json_response({"ok": True, "updatedAt": body["updatedAt"]})

    async def _h_kv_delete(self, request: web.Request) -> web.Response:
        ctx = self._run(request)
        payload = await self._json_body(request)
        scope = self._scope(ctx, payload.get("scope"))
        key = self._require(payload.get("key"), "key")
        existed = await jobstate.kv_delete(self._backend(), scope, key)
        return web.json_response({"existed": existed})

    async def _h_kv_list(self, request: web.Request) -> web.Response:
        ctx = self._run(request)
        scope = self._scope(ctx, request.query.get("scope"))
        bodies = await jobstate.kv_list(self._backend(), scope)
        return web.json_response(
            {
                "scope": scope,
                "keys": [
                    {
                        "key": b.get("key"),
                        "value": b.get("value"),
                        "updatedAt": b.get("updatedAt"),
                    }
                    for b in bodies
                ],
            }
        )

    # --- handlers: cursor ------------------------------------------------

    async def _h_cursor_get(self, request: web.Request) -> web.Response:
        ctx = self._run(request)
        scope = self._scope(ctx, request.query.get("scope"))
        name = self._require(request.query.get("name"), "name")
        body = await jobstate.cursor_get(self._backend(), scope, name)
        if body is None:
            raise web.HTTPNotFound()
        return web.json_response(
            {"value": body.get("value"), "updatedAt": body.get("updatedAt")}
        )

    async def _h_cursor_advance(self, request: web.Request) -> web.Response:
        ctx = self._run(request)
        payload = await self._json_body(request)
        scope = self._scope(ctx, payload.get("scope"))
        name = self._require(payload.get("name"), "name")
        if "value" not in payload:
            raise JobStateError("value is required")
        result = await jobstate.cursor_advance(
            self._backend(),
            scope,
            name,
            payload["value"],
            force=bool(payload.get("force")),
            max_bytes=self._max_value_bytes,
        )
        return web.json_response(result)

    # --- handlers: idempotency -------------------------------------------

    async def _h_idem_claim(self, request: web.Request) -> web.Response:
        ctx = self._run(request)
        payload = await self._json_body(request)
        scope = self._scope(ctx, payload.get("scope"))
        key = self._require(payload.get("key"), "key")
        try:
            ttl = float(payload.get("ttl") or 0.0)
        except (TypeError, ValueError) as ex:
            raise JobStateError("ttl must be a number") from ex
        result = await jobstate.idempotency_claim(
            self._backend(), scope, key, ttl=ttl
        )
        return web.json_response(result)

    async def _h_idem_release(self, request: web.Request) -> web.Response:
        ctx = self._run(request)
        payload = await self._json_body(request)
        scope = self._scope(ctx, payload.get("scope"))
        key = self._require(payload.get("key"), "key")
        released = await jobstate.idempotency_release(
            self._backend(), scope, key
        )
        return web.json_response({"released": released})

    # --- handlers: artifact ----------------------------------------------

    async def _h_artifact_put(self, request: web.Request) -> web.Response:
        ctx = self._run(request)
        scope = self._scope(ctx, request.query.get("scope"))
        name = self._require(request.query.get("name"), "name")
        data = await request.read()
        record = await jobstate.artifact_put(
            self._backend(),
            scope,
            name,
            data,
            max_bytes=self._max_artifact_bytes,
        )
        return web.json_response(
            {"sha256": record["sha256"], "size": record["size"]}
        )

    async def _h_artifact_get(
        self, request: web.Request
    ) -> web.StreamResponse:
        ctx = self._run(request)
        scope = self._scope(ctx, request.query.get("scope"))
        name = self._require(request.query.get("name"), "name")
        got = await jobstate.artifact_get(self._backend(), scope, name)
        if got is None:
            raise web.HTTPNotFound()
        record, data = got
        return web.Response(
            body=data,
            content_type="application/octet-stream",
            headers={
                "X-Cronstable-Sha256": str(record.get("sha256", "")),
                "X-Cronstable-Size": str(record.get("size", len(data))),
            },
        )

    async def _h_artifact_list(self, request: web.Request) -> web.Response:
        ctx = self._run(request)
        scope = self._scope(ctx, request.query.get("scope"))
        listing = await jobstate.artifact_list(self._backend(), scope)
        return web.json_response(
            {
                "scope": scope,
                "artifacts": [
                    {
                        "name": r.get("name"),
                        "sha256": r.get("sha256"),
                        "size": r.get("size"),
                        "at": r.get("at"),
                    }
                    for r in listing
                ],
            }
        )

    # --- handlers: lock --------------------------------------------------

    async def _h_lock_acquire(self, request: web.Request) -> web.Response:
        ctx = self._run(request)
        payload = await self._json_body(request)
        scope = self._scope(ctx, payload.get("scope"))
        name = self._require(payload.get("name"), "name")
        try:
            permits = int(payload.get("permits", 1))
        except (TypeError, ValueError) as ex:
            raise JobStateError("permits must be an integer") from ex
        ttl = payload.get("ttl")
        try:
            ttl_seconds = float(ttl) if ttl is not None else None
            block_seconds = float(payload.get("blockSeconds") or 0.0)
        except (TypeError, ValueError) as ex:
            raise JobStateError("ttl and blockSeconds must be numbers") from ex
        result = await self.locks.acquire(
            ctx.token,
            scope,
            name,
            permits=permits,
            ttl=ttl_seconds,
            wait=bool(payload.get("wait")),
            block_seconds=block_seconds,
        )
        return web.json_response(result)

    async def _h_lock_release(self, request: web.Request) -> web.Response:
        ctx = self._run(request)
        payload = await self._json_body(request)
        token = self._require(payload.get("token"), "token")
        released = await self.locks.release(ctx.token, token)
        return web.json_response({"released": released})

    # --- handlers: secret ------------------------------------------------

    async def _h_secret_get(self, request: web.Request) -> web.Response:
        ctx = self._run(request)
        name = self._require(request.query.get("name"), "name")
        if name not in ctx.secrets:
            raise web.HTTPNotFound()
        return web.json_response({"value": ctx.secrets[name]})

    async def _h_secret_list(self, request: web.Request) -> web.Response:
        ctx = self._run(request)
        return web.json_response({"names": sorted(ctx.secrets)})
