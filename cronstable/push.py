"""End-to-end encrypted push alerts: the ``push`` reporter's engine.

The daemon seals a compact alert payload to each paired device's X25519
public key (libsodium sealed boxes via PyNaCl) and hands the ciphertext,
plus an opaque coalescing id, to a hosted relay that forwards it to the
platform push service (APNs).  The relay never sees plaintext: it learns
only a device token, a ciphertext and a hash, so a self-hosted daemon
can use a shared relay without trusting it with job names, log lines or
hostnames.  See wiki/Push-Notifications.md and docs/relay-protocol.md.

Three pieces live here:

- sealing and payload sizing (:func:`seal_to_device`,
  :func:`build_payload`, :func:`fit_payload`), bounded so the relay's
  final APNs JSON stays under the 4096-byte APNs cap;
- the paired-device registry: :class:`FileDeviceStore` (a small local
  JSON file, atomic replace-on-write) and :class:`StateDeviceStore`
  (one document per device on the durable state store, so every node
  sharing the store sees the same pairings);
- :class:`PushService`: the daemon-global object shared by the
  ``push`` reporter, the ``notify:`` fan-out and the ``/push/devices``
  web handlers, published through :func:`set_service` /
  :func:`get_service` (reporters are stateless singletons, so they
  reach the daemon's service through this module seam, the same way
  the loop reaches the daemon's config).

PyNaCl is an optional extra (``pip install "cronstable[push]"``): the
import is guarded, and config validation refuses a ``push:`` block when
the library is absent.  Fail closed on purpose: an alerting channel
that silently self-disables is a missed page, the one failure mode a
paging feature must never have.
"""

import asyncio
import base64
import binascii
import datetime
import hashlib
import json
import logging
import os
import secrets
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp

try:
    from nacl.public import PublicKey, SealedBox

    HAVE_PYNACL = True
except ImportError:  # pragma: no cover - exercised on the no-push baseline
    HAVE_PYNACL = False

logger = logging.getLogger("cronstable")

#: Version stamp inside every sealed plaintext and relay envelope, so the
#: app and relay can evolve the format without guessing.
PUSH_PROTOCOL_VERSION = 1

#: APNs rejects notifications whose final JSON exceeds 4096 bytes.  The
#: relay wraps our ciphertext in its own envelope (alert stub, headers,
#: the mutable-content marker), so the base64 ciphertext we hand it is
#: capped well below that, leaving the relay ~1 KB of headroom.
CIPHERTEXT_B64_MAX = 3000

#: A libsodium sealed box adds an ephemeral X25519 public key (32 bytes)
#: and a Poly1305 MAC (16 bytes) to the plaintext.
_SEALED_OVERHEAD = 48

#: The largest plaintext whose sealed, base64-encoded form fits the cap.
MAX_PLAINTEXT_BYTES = CIPHERTEXT_B64_MAX // 4 * 3 - _SEALED_OVERHEAD

#: X25519 public keys are exactly 32 bytes.
DEVICE_PUBLIC_KEY_BYTES = 32

#: Durable-state document namespace holding one document per paired
#: device, keyed by device id.  Documents are never swept by state GC,
#: so a pairing survives until it is explicitly revoked.
PUSH_DOC_NAMESPACE = "pushdevice"

#: How long the in-memory device mirror is trusted before the next send
#: or listing re-reads the store (a pairing made on another node sharing
#: the state store becomes visible within this window).
REGISTRY_REFRESH_SECONDS = 60.0

#: Bound every store operation the same way cron's state writes are
#: bounded, so a wedged shared mount cannot stall a report fan-out or a
#: pairing request forever.
STORE_OP_TIMEOUT = 10.0

#: How many trailing captured output lines a job alert starts from
#: before size trimming; the fit loop drops oldest-first from there.
LOG_TAIL_MAX_LINES = 40

_FIELD_LIMITS = {"name": 64, "platform": 32, "pushToken": 512}


class PushError(Exception):
    """A push operation failed (bad device material, store trouble)."""


def _utcnow_iso() -> str:
    return (
        datetime.datetime.now(datetime.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
    )


def validate_public_key(value: Any) -> str:
    """Normalize and validate a device public key (base64 X25519).

    Returns the canonical (re-encoded) base64 form; raises
    :class:`PushError` with an operator-readable reason otherwise.
    """
    if not isinstance(value, str) or not value.strip():
        raise PushError("publicKey is required (base64 X25519 key)")
    try:
        raw = base64.b64decode(value.strip(), validate=True)
    except (binascii.Error, ValueError):
        raise PushError("publicKey is not valid base64") from None
    if len(raw) != DEVICE_PUBLIC_KEY_BYTES:
        raise PushError(
            "publicKey must decode to exactly {} bytes, got {}".format(
                DEVICE_PUBLIC_KEY_BYTES, len(raw)
            )
        )
    return base64.b64encode(raw).decode("ascii")


def _validate_field(payload: Dict[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise PushError("{} is required".format(field))
    value = value.strip()
    if len(value) > _FIELD_LIMITS[field]:
        raise PushError(
            "{} is longer than {} characters".format(
                field, _FIELD_LIMITS[field]
            )
        )
    return value


def validate_pairing(payload: Any) -> Dict[str, str]:
    """Validate a ``POST /push/devices`` body into a clean field dict.

    Raises :class:`PushError` with a message safe to return in a 400.
    """
    if not isinstance(payload, dict):
        raise PushError("body must be a JSON object")
    return {
        "name": _validate_field(payload, "name"),
        "platform": _validate_field(payload, "platform"),
        "pushToken": _validate_field(payload, "pushToken"),
        "publicKey": validate_public_key(payload.get("publicKey")),
    }


def seal_to_device(public_key_b64: str, plaintext: bytes) -> str:
    """Seal ``plaintext`` to a device public key; return base64 text.

    Anonymous-sender sealed box: an ephemeral key pair per message, so
    the daemon holds no long-lived sending secret and only the device's
    private key (which never leaves the phone) can open it.
    """
    if not HAVE_PYNACL:  # pragma: no cover - config validation gates this
        raise PushError(
            "PyNaCl is not installed; install the push extra "
            '(pip install "cronstable[push]")'
        )
    try:
        raw = base64.b64decode(public_key_b64, validate=True)
        key = PublicKey(raw)
    except Exception as exc:
        raise PushError(
            "device public key is unusable: {}".format(exc)
        ) from None
    sealed = SealedBox(key).encrypt(plaintext)
    return base64.b64encode(sealed).decode("ascii")


def _encode(payload: Dict[str, Any]) -> bytes:
    return json.dumps(
        payload, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def build_payload(
    ctx: Any, success: bool, include_log_tail: bool
) -> Dict[str, Any]:
    """The sealed plaintext for one alert, before size fitting.

    ``ctx`` is duck-typed exactly as the other reporters take it: a
    :class:`~cronstable.job.RunningJob`, an SLA breach context or a
    ``notify:`` event context.  All three expose ``template_vars`` with
    the standard key set; the event and SLA contexts are recognized by
    their ``event`` / ``sla_check`` attributes.
    """
    tv = ctx.template_vars
    event = getattr(ctx, "event", None)
    sla_check = getattr(ctx, "sla_check", None)
    if event is not None:
        kind = "event"
    elif sla_check is not None:
        kind = "sla"
    else:
        kind = "success" if success else "failure"
    payload: Dict[str, Any] = {
        "v": PUSH_PROTOCOL_VERSION,
        "kind": kind,
        "name": tv.get("name"),
        "success": bool(success),
        "host": tv.get("host"),
        "ts": _utcnow_iso(),
    }
    for field in (
        "run_id",
        "schedule",
        "started_at",
        "exit_code",
        "fail_reason",
    ):
        value = tv.get(field)
        if value not in (None, ""):
            payload[field] = value
    if kind == "event":
        payload["event"] = event
        payload["subject"] = tv.get("subject")
        payload["message"] = tv.get("message")
        for field in ("dag", "run_key", "taskkey", "role", "leader"):
            value = tv.get(field)
            if value not in (None, ""):
                payload[field] = value
    elif kind == "sla":
        payload["sla_check"] = sla_check
        for field in (
            "threshold_seconds",
            "observed_seconds",
            "last_success_at",
        ):
            value = tv.get(field)
            if value is not None:
                payload[field] = value
    elif include_log_tail:
        # stderr first: it is captured by default and is where a failing
        # job's reason usually lives; stdout only when that's all there
        # is.  Event/SLA alerts have no process, so no tail.
        text = tv.get("stderr") or tv.get("stdout")
        if text:
            payload["log_tail"] = text.splitlines()[-LOG_TAIL_MAX_LINES:]
    return payload


def fit_payload(payload: Dict[str, Any]) -> bytes:
    """Shrink ``payload`` in place until it seals under the APNs cap.

    Trim order: oldest log-tail lines first (the newest lines carry the
    failure), then the long free-text fields by halving, so the alert
    always keeps its identity (name, kind, host) intact.  Returns the
    encoded plaintext.
    """
    data = _encode(payload)
    while len(data) > MAX_PLAINTEXT_BYTES:
        tail = payload.get("log_tail")
        if tail:
            del tail[0]
            if not tail:
                del payload["log_tail"]
        else:
            for field in ("message", "fail_reason", "subject"):
                value = payload.get(field)
                if isinstance(value, str) and len(value) > 64:
                    payload[field] = value[: max(64, len(value) // 2)]
                    break
            else:
                # Nothing long is left; the residual overage can only
                # come from many short fields, so drop the optional
                # context ones until the identity core fits.
                for field in ("schedule", "started_at", "run_id"):
                    if field in payload:
                        del payload[field]
                        break
                else:  # pragma: no cover - identity core is tiny
                    break
        data = _encode(payload)
    return data


def collapse_id(payload: Dict[str, Any]) -> str:
    """An opaque coalescing key for the relay: same alert, same id.

    A hash of the alert's identity fields, so the relay can deduplicate
    the same (job, run) reported by several nodes without learning the
    job name or run id.
    """
    ident = {
        key: payload[key]
        for key in (
            "kind",
            "name",
            "run_id",
            "event",
            "dag",
            "run_key",
            "taskkey",
            "sla_check",
        )
        if payload.get(key) not in (None, "")
    }
    blob = json.dumps(ident, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


def public_device(device: Dict[str, Any]) -> Dict[str, Any]:
    """A device record as served by ``GET /push/devices``.

    The push token is redacted to its tail: it is not key material, but
    it is the one field that lets a third party address this device
    through the platform push service, so the listing never echoes it
    whole.  The public key is public by definition and returned intact
    (the app re-checks it against its own on screen).
    """
    token = device.get("pushToken") or ""
    return {
        "id": device.get("id"),
        "name": device.get("name"),
        "platform": device.get("platform"),
        "publicKey": device.get("publicKey"),
        "pushToken": "…" + token[-6:] if token else "",
        "createdAt": device.get("createdAt"),
        "createdBy": device.get("createdBy"),
    }


class FileDeviceStore:
    """Paired devices in one local JSON file (stateless installs).

    Writes are atomic (temp file + ``os.replace``) and serialized by an
    in-process lock; the file is created with owner-only permissions
    where the platform honors them.  A corrupt file refuses writes
    instead of clobbering what might still be recoverable pairings.
    """

    kind = "file"

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = asyncio.Lock()
        self._corrupt: Optional[str] = None

    def describe(self) -> str:
        return "file:{}".format(self.path)

    def _read(self) -> List[Dict[str, Any]]:
        try:
            with open(self.path, "rt", encoding="utf-8") as stream:
                doc = json.load(stream)
        except FileNotFoundError:
            return []
        except (OSError, ValueError) as exc:
            self._corrupt = str(exc)
            raise PushError(
                "push devices file {} is unreadable: {}".format(self.path, exc)
            ) from None
        devices = doc.get("devices") if isinstance(doc, dict) else None
        if not isinstance(devices, list):
            self._corrupt = "not a {version, devices} object"
            raise PushError(
                "push devices file {} has an unexpected shape".format(
                    self.path
                )
            )
        self._corrupt = None
        return [d for d in devices if isinstance(d, dict)]

    def _write(self, devices: List[Dict[str, Any]]) -> None:
        if self._corrupt is not None:
            # Never overwrite a file we could not parse: the operator
            # may still recover pairings from it by hand.
            raise PushError(
                "refusing to overwrite unreadable devices file {} "
                "({}); fix or remove it first".format(self.path, self._corrupt)
            )
        doc = {"version": 1, "devices": devices}
        tmp = self.path + ".tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(fd, "wt", encoding="utf-8") as stream:
                json.dump(doc, stream, indent=2, sort_keys=True)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        os.replace(tmp, self.path)

    async def load(self) -> List[Dict[str, Any]]:
        async with self._lock:
            return self._read()

    async def upsert(self, device: Dict[str, Any]) -> None:
        async with self._lock:
            devices = self._read()
            devices = [d for d in devices if d.get("id") != device.get("id")]
            devices.append(device)
            self._write(devices)

    async def remove(self, device_id: str) -> bool:
        async with self._lock:
            devices = self._read()
            kept = [d for d in devices if d.get("id") != device_id]
            if len(kept) == len(devices):
                return False
            self._write(kept)
            return True


class StateDeviceStore:
    """Paired devices as durable-state documents (one per device).

    One document per device in the :data:`PUSH_DOC_NAMESPACE` namespace,
    so register and revoke are per-key atomic operations (no read-
    modify-write races between nodes pairing at once) and every node
    sharing the store sees the same registry.  Documents are never
    swept by state GC, so a pairing lives until explicitly revoked.

    ``get_backend`` is a callable, not a backend reference: the state
    backend is torn down and rebuilt on config reloads, and the store
    must always talk to the current one (or fail loudly when there is
    none, e.g. mid-reload).
    """

    kind = "state"

    def __init__(self, get_backend: Callable[[], Optional[Any]]) -> None:
        self._get_backend = get_backend

    def describe(self) -> str:
        return "state:{}/".format(PUSH_DOC_NAMESPACE)

    def _backend(self) -> Any:
        backend = self._get_backend()
        if backend is None:
            raise PushError(
                "the durable state store is not available; device "
                "pairing needs it (or configure push.devicesFile)"
            )
        return backend

    async def load(self) -> List[Dict[str, Any]]:
        backend = self._backend()
        try:
            docs = await asyncio.wait_for(
                backend.list_documents(PUSH_DOC_NAMESPACE),
                timeout=STORE_OP_TIMEOUT,
            )
        except asyncio.TimeoutError:
            raise PushError(
                "timed out listing paired devices from the state store"
            ) from None
        return [d for d in docs if isinstance(d, dict) and d.get("id")]

    async def upsert(self, device: Dict[str, Any]) -> None:
        backend = self._backend()

        def _put(_current: Optional[Dict[str, Any]]) -> Tuple[Any, None]:
            # Pure and idempotent: mutate_document may retry it on a
            # torn read, and it runs on the store's worker thread.
            return dict(device), None

        try:
            await asyncio.wait_for(
                backend.mutate_document(
                    PUSH_DOC_NAMESPACE, device["id"], _put
                ),
                timeout=STORE_OP_TIMEOUT,
            )
        except asyncio.TimeoutError:
            raise PushError(
                "timed out writing the device pairing to the state store"
            ) from None

    async def remove(self, device_id: str) -> bool:
        backend = self._backend()
        try:
            return bool(
                await asyncio.wait_for(
                    backend.delete_document(PUSH_DOC_NAMESPACE, device_id),
                    timeout=STORE_OP_TIMEOUT,
                )
            )
        except asyncio.TimeoutError:
            raise PushError(
                "timed out revoking the device in the state store"
            ) from None


class PushService:
    """The daemon-global push engine: registry mirror + relay client.

    Interactive paths (the ``/push/devices`` handlers) await the store
    directly so a pairing either durably happened or the caller gets an
    error.  The reporting path reads only the in-memory mirror,
    refreshed at most every :data:`REGISTRY_REFRESH_SECONDS`, so a slow
    or absent store can delay a *new* pairing taking effect but can
    never stall a report fan-out.
    """

    def __init__(
        self,
        *,
        relay_url: str,
        relay_timeout: float,
        store: Any,
        host: str,
    ) -> None:
        self.relay_url = relay_url
        self.relay_timeout = relay_timeout
        self.store = store
        self.host = host
        self._devices: Dict[str, Dict[str, Any]] = {}
        self._mirror_fresh_until = 0.0
        self._refresh_lock = asyncio.Lock()

    async def start(self) -> None:
        """Warm the device mirror; never fatal (the store may be down)."""
        try:
            await self.refresh(force=True)
        except PushError as exc:
            logger.warning(
                "push: could not load the device registry yet (%s); "
                "will keep retrying on demand",
                exc,
            )
        else:
            logger.info(
                "push: %d paired device(s) loaded from %s",
                len(self._devices),
                self.store.describe(),
            )

    async def refresh(self, force: bool = False) -> None:
        """Re-read the registry unless the mirror is still fresh."""
        now = asyncio.get_running_loop().time()
        if not force and now < self._mirror_fresh_until:
            return
        async with self._refresh_lock:
            now = asyncio.get_running_loop().time()
            if not force and now < self._mirror_fresh_until:
                return
            devices = await self.store.load()
            self._devices = {d["id"]: d for d in devices if d.get("id")}
            self._mirror_fresh_until = (
                asyncio.get_running_loop().time() + REGISTRY_REFRESH_SECONDS
            )

    def devices_payload(self) -> List[Dict[str, Any]]:
        devices = sorted(
            self._devices.values(), key=lambda d: d.get("createdAt") or ""
        )
        return [public_device(d) for d in devices]

    def get_device(self, device_id: str) -> Optional[Dict[str, Any]]:
        return self._devices.get(device_id)

    async def pair(
        self, fields: Dict[str, str], created_by: Optional[str]
    ) -> Tuple[Dict[str, Any], bool]:
        """Register (or re-register) a device; returns (record, created).

        Re-pairing is keyed on the public key: the same device pairing
        again (APNs tokens rotate; people rename phones) updates its
        record in place instead of accumulating duplicates, keeping its
        id and createdAt so revocation references stay stable.
        """
        await self.refresh(force=True)
        existing = next(
            (
                d
                for d in self._devices.values()
                if d.get("publicKey") == fields["publicKey"]
            ),
            None,
        )
        if existing is not None:
            record = dict(existing)
            record.update(fields)
            created = False
        else:
            record = dict(fields)
            record["id"] = secrets.token_hex(8)
            record["createdAt"] = _utcnow_iso()
            record["createdBy"] = created_by
            created = True
        await self.store.upsert(record)
        self._devices[record["id"]] = record
        return record, created

    async def revoke(self, device_id: str) -> bool:
        await self.refresh(force=True)
        removed = await self.store.remove(device_id)
        self._devices.pop(device_id, None)
        return bool(removed)

    async def send_report(
        self, ctx: Any, success: bool, push_config: Dict[str, Any]
    ) -> None:
        """Fan one alert out to every paired device (the reporter path).

        Failures are logged per device and never raised: this runs
        inside the reporter gather, and a relay outage must not look
        like a reporting crash.
        """
        try:
            await self.refresh()
        except PushError as exc:
            logger.warning(
                "push: device registry unavailable (%s); sending to the "
                "%d last-known device(s)",
                exc,
                len(self._devices),
            )
        if not self._devices:
            logger.warning(
                "push: report is enabled but no device is paired; "
                "dropping alert for %s (pair one at POST /push/devices)",
                getattr(getattr(ctx, "config", None), "name", "?"),
            )
            return
        payload = build_payload(
            ctx, success, bool(push_config.get("includeLogTail", True))
        )
        results = await self._send_payload(
            payload,
            priority=push_config.get("priority", "time-sensitive"),
        )
        for result in results:
            if result.get("error"):
                logger.error(
                    "push: delivery to device %s failed: %s",
                    result["device"],
                    result["error"],
                )

    async def send_test(self, device: Dict[str, Any]) -> Dict[str, Any]:
        """Send a test alert to one device; returns the relay outcome."""
        payload = {
            "v": PUSH_PROTOCOL_VERSION,
            "kind": "test",
            "name": "test",
            "success": True,
            "host": self.host,
            "message": "test alert from cronstable",
            "ts": _utcnow_iso(),
        }
        results = await self._send_payload(
            payload, priority="time-sensitive", only=device
        )
        return results[0]

    async def _send_payload(
        self,
        payload: Dict[str, Any],
        *,
        priority: str,
        only: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        plaintext = fit_payload(payload)
        coalesce = collapse_id(payload)
        is_event = payload.get("kind") == "event"
        targets = [only] if only else list(self._devices.values())
        results = []
        timeout = aiohttp.ClientTimeout(total=self.relay_timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for device in targets:
                outcome: Dict[str, Any] = {
                    "device": device.get("id"),
                    "status": None,
                    "error": None,
                }
                try:
                    ciphertext = seal_to_device(device["publicKey"], plaintext)
                except (PushError, KeyError) as exc:
                    outcome["error"] = "sealing failed: {}".format(exc)
                    results.append(outcome)
                    continue
                body = {
                    "v": PUSH_PROTOCOL_VERSION,
                    "device": device.get("pushToken"),
                    "ciphertext": ciphertext,
                    "collapseId": coalesce,
                    "priority": priority,
                    "event": is_event,
                }
                try:
                    async with session.post(self.relay_url, json=body) as resp:
                        outcome["status"] = resp.status
                        if resp.status >= 400:
                            # Body text only; the URL stays out of logs
                            # (webhook-reporter convention).
                            outcome["error"] = "relay HTTP {}: {}".format(
                                resp.status, (await resp.text())[:512]
                            )
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    outcome["error"] = "relay unreachable: {}".format(exc)
                results.append(outcome)
        return results


_service: Optional[PushService] = None


def get_service() -> Optional[PushService]:
    """The running daemon's push service, or None when unconfigured."""
    return _service


def set_service(service: Optional[PushService]) -> None:
    global _service
    _service = service
