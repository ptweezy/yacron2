"""Optional orjson acceleration for the hot JSON serialization paths.

orjson (a compiled Rust/pyo3 extension) serializes and parses JSON several
times faster than the stdlib and hands back ``bytes`` directly, saving the
separate ``.encode("utf-8")`` every persistence site does by hand. It is an
OPTIONAL speedup, wired exactly like the uvloop event-loop swap: it ships no
wheels for some of the leaner architectures cronstable targets (riscv64, armv6,
musl ppc64le/s390x), so it stays out of the core dependency set (install the
``speedups`` extra to pull it in) and this module transparently falls back to
the stdlib ``json`` when it is absent -- identical behaviour, just slower.

CONTRACT -- read before pointing anything new at these helpers:

Use them only where the serialized bytes are round-tripped back through
:func:`loads` (durable state records, leases, documents; parsing peer gossip
bodies). orjson always emits UTF-8 and does NOT ``ensure_ascii``, so for
non-ASCII input its bytes are NOT identical to
``json.dumps(...).encode("utf-8")``. Anywhere the exact bytes matter -- a value
fed into a hash / content-address, or compared across nodes or versions (the
job-set fingerprint in :mod:`cronstable.fingerprint`, the cluster peer ETag in
:mod:`cronstable.cluster`) -- keep the stdlib ``json`` directly, so the output
stays stable and backend-independent whether or not a given host has orjson.
"""

import json as _stdlib
import math
from typing import Any, Union, cast

try:
    import orjson
except ImportError:  # pragma: no cover - exercised on the no-orjson baseline
    orjson = None  # type: ignore[assignment]


# The portable-value contract shared by BOTH backends.  orjson and the stdlib
# are not interchangeable at the edges: orjson accepts only integers in the
# signed-64..unsigned-64 window (it raises on anything wider) and SILENTLY
# rewrites a non-finite float to ``null``; the stdlib accepts arbitrary-width
# ints and writes ``NaN`` / ``Infinity`` tokens that orjson then refuses to
# parse.  So a value outside this window is serialized DIFFERENTLY -- or
# unreadably -- depending on whether a given host installed the optional orjson
# extra.  On a mixed fleet sharing one store that is silent corruption: a node
# without orjson writes ``{"value":Infinity}`` that every orjson node's
# ``loads`` then rejects (a wedged cursor / a watermark read back as unset ->
# reprocessed backlog), or an orjson node coerces ``nan`` to ``null`` a stdlib
# node reads back as ``None``.  :func:`dumps_bytes` rejects such values
# UNIFORMLY, on every backend, so the durable store stays backend-independent:
# bytes any node writes, every node can read.
_INT_MIN = -(2**63)
_INT_MAX = 2**64 - 1


class UnsupportedValue(ValueError):
    """A value that cannot be encoded identically across a mixed-orjson fleet.

    Raised by :func:`dumps_bytes` (and available as a standalone pre-check via
    :func:`ensure_portable`) for a non-finite float (``NaN`` / ``Infinity``),
    an integer outside the 64-bit window orjson supports, or a non-string
    object key.  Rejecting at write time is what keeps a record readable by
    every node regardless of which ones have orjson.
    """


def _ensure_finite(obj: Any) -> None:
    """The non-finite-float half of :func:`ensure_portable`, standalone.

    Under orjson this is the ONLY portability hazard that needs a Python-level
    pre-walk: orjson itself already REJECTS out-of-window integers and
    non-string keys (its ``dumps_bytes`` translates those into
    :class:`UnsupportedValue` after the fact), but it silently rewrites a
    non-finite float to ``null``, the one corruption only a walk can catch
    before the bytes are written.  Kept float-only so the per-node work of the
    walk is a couple of isinstance checks, not the full rule set.
    """
    if isinstance(obj, float):
        if not math.isfinite(obj):
            raise UnsupportedValue(
                "non-finite float {!r} is not portable across the fleet "
                "(NaN/Infinity serializes differently with and without "
                "orjson)".format(obj)
            )
    elif isinstance(obj, dict):
        for value in obj.values():
            _ensure_finite(value)
    elif isinstance(obj, (list, tuple)):
        for value in obj:
            _ensure_finite(value)


def ensure_portable(obj: Any) -> None:
    """Raise :class:`UnsupportedValue` if ``obj`` is not fleet-portable JSON.

    A recursive, backend-independent pre-check so the accept/reject decision is
    identical on every host -- with or without orjson -- instead of one backend
    silently corrupting what the other rejects.  The stdlib flavour of
    :func:`dumps_bytes` runs it up front (the stdlib would otherwise accept
    everything it bans); the orjson flavour runs only the lighter
    :func:`_ensure_finite` walk up front and reaches for this one just to
    classify a failed serialize.  A boundary that wants the full check without
    serializing (to translate the failure into its own error type) invokes it
    directly.
    """
    if isinstance(obj, bool):
        return  # a bool is an int subclass but always in range and portable
    if isinstance(obj, float):
        if not math.isfinite(obj):
            raise UnsupportedValue(
                "non-finite float {!r} is not portable across the fleet "
                "(NaN/Infinity serializes differently with and without "
                "orjson)".format(obj)
            )
    elif isinstance(obj, int):
        if obj < _INT_MIN or obj > _INT_MAX:
            raise UnsupportedValue(
                "integer {} is outside the portable signed/unsigned 64-bit "
                "range [{}, {}]".format(obj, _INT_MIN, _INT_MAX)
            )
    elif isinstance(obj, dict):
        for key, value in obj.items():
            if not isinstance(key, str):
                raise UnsupportedValue(
                    "non-string object key {!r} is not portable across the "
                    "fleet (orjson rejects it, the stdlib coerces it)".format(
                        key
                    )
                )
            ensure_portable(value)
    elif isinstance(obj, (list, tuple)):
        for value in obj:
            ensure_portable(value)


if orjson is not None:

    def dumps_bytes(
        obj: Any, *, sort_keys: bool = False, trusted: bool = False
    ) -> bytes:
        """Serialize ``obj`` to compact UTF-8 JSON bytes.

        ``trusted`` skips the pre-serialize portability walk for payloads the
        daemon constructed entirely itself out of values it computed (a lease
        record written every few seconds is the motivating case): the caller
        vouches there is no non-finite float, out-of-window integer or
        non-string key in it.  Never pass it for anything that embeds
        job-supplied or store-read data.
        """
        if not trusted:
            # the one hazard orjson does not raise on itself; int-window and
            # key-type violations surface from orjson.dumps below and are
            # translated to UnsupportedValue there.
            _ensure_finite(obj)
        option = orjson.OPT_SORT_KEYS if sort_keys else 0
        try:
            # cast: the tox mypy env runs --ignore-missing-imports (orjson not
            # a dev dep), so orjson.dumps reads as Any and warn_return_any
            # fires.
            return cast(bytes, orjson.dumps(obj, option=option))
        except (TypeError, ValueError):
            # orjson.JSONEncodeError covers exactly the remaining portability
            # bans (a >64-bit integer, a non-string key). Re-run the full
            # walk so a PORTABILITY violation raises the same UnsupportedValue
            # every backend raises; anything else (a genuinely unserializable
            # object) re-raises as-is.
            ensure_portable(obj)
            raise

    def loads(data: Union[bytes, str]) -> Any:
        """Parse JSON from ``bytes`` or ``str``."""
        return orjson.loads(data)

    def deepcopy_json(obj: Any) -> Any:
        """Deep-copy a JSON-shaped value via a serialize+parse round trip.

        For in-process copies only (a transform needing a mutable working
        copy of a document already read from the store): the bytes never
        leave the process, so the fleet-portability gate that guards
        :func:`dumps_bytes` does not apply and is skipped.  Several times
        faster than both ``copy.deepcopy`` and a stdlib ``json`` round
        trip when orjson is installed.
        """
        return orjson.loads(orjson.dumps(obj))

else:

    def dumps_bytes(
        obj: Any, *, sort_keys: bool = False, trusted: bool = False
    ) -> bytes:
        """Serialize ``obj`` to compact UTF-8 JSON bytes.

        ``trusted`` skips the pre-serialize portability walk (see the orjson
        flavour for the contract).  ``allow_nan=False`` keeps even a trusted
        caller's non-finite float an error at zero extra cost (the stdlib
        would otherwise write a ``NaN`` token an orjson peer cannot parse),
        though it surfaces as the stdlib's ValueError, not UnsupportedValue.
        """
        if not trusted:
            ensure_portable(obj)
        text = _stdlib.dumps(
            obj,
            separators=(",", ":"),
            sort_keys=sort_keys,
            allow_nan=False,
        )
        return text.encode("utf-8")

    def loads(data: Union[bytes, str]) -> Any:
        """Parse JSON from ``bytes`` or ``str``."""
        return _stdlib.loads(data)

    def deepcopy_json(obj: Any) -> Any:
        """Deep-copy a JSON-shaped value via a serialize+parse round trip.

        For in-process copies only (a transform needing a mutable working
        copy of a document already read from the store): the bytes never
        leave the process, so the fleet-portability gate that guards
        :func:`dumps_bytes` does not apply and is skipped.  Several times
        faster than both ``copy.deepcopy`` and a stdlib ``json`` round
        trip when orjson is installed.
        """
        return _stdlib.loads(_stdlib.dumps(obj))
