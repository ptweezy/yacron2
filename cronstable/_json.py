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
import re
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

# Lone (unpaired) surrogate code points are the string-shaped portability
# hazard: the stdlib encoder happily writes ``"\ud800"`` (and its decoder
# reads it back), but orjson raises on dumps AND refuses to parse those same
# bytes -- so a stdlib node can persist a record no orjson node can ever read
# or rewrite.  They cannot appear in well-formed UTF-8 input; they arrive via
# surrogateescape-decoded OS data or a caller constructing them directly.
_SURROGATES = re.compile("[\ud800-\udfff]")

# The portable nesting-depth bound, enforced by :func:`ensure_portable` (and
# the orjson pre-walk) with an explicit counter.  Without one the accepted
# document set was whatever each backend's encoder happened to tolerate:
# orjson's encoder hard-fails at depth 256 while its parser reads to 1024 and
# the stdlib reaches ~1000 both ways -- so a stdlib node could persist a
# 256..1023-deep document every orjson node could READ but never WRITE BACK,
# permanently wedging any read-modify-write path (a DAG run advance, a kv
# update).  128 sits safely below orjson's 256 even after the store's own
# wrapper layers, and far above any real cronstable document.  The gate
# itself counts depth instead of recursing to a stack overflow, so an
# adversarial ~2KB deep-nested body is a clean UnsupportedValue, not a
# RecursionError.
MAX_DEPTH = 128

# Read-side guard for out-of-window INTEGER LITERALS.  The two parsers
# disagree about them: the stdlib preserves ``18446744073709551616`` as an
# exact (unportable) int that :func:`ensure_portable` then rejects, while
# orjson silently narrows it to a lossy finite FLOAT the gate happily
# accepts -- blinding the very pre-check built to catch the value, and
# making a mapped fan-out (or any consumer) see different data depending on
# which host parsed the bytes.  :func:`loads` therefore rejects such
# literals UNIFORMLY: a cheap prescan for a 19-digit run (the shortest an
# out-of-window literal can be: ``-(2**63)-1`` has 19 digits; every
# in-window literal of 19+ digits still passes the precise check) gates a
# stdlib verification parse whose ``parse_int`` hook applies the exact
# 64-bit-window rule.  Digit runs inside strings or float literals can trip
# the prescan; they only cost the verification parse, never a false reject
# (``parse_int`` sees integer tokens alone, and float literals already
# parse identically on both backends).
_WIDE_INT_RUN_B = re.compile(rb"\d{19}")
_WIDE_INT_RUN_S = re.compile(r"\d{19}")

# The bytes prescan runs on EVERY loads(), over the whole payload, and a
# ``\d{19}`` scan has no literal prefix for sre to memchr on, so it cost more
# than the parse it guards.  Translating digits to a single byte and
# everything else to a separator turns the same predicate into two
# memcpy-speed C loops (~21x faster): a 19-digit run exists iff the
# translated buffer contains nineteen ``0`` bytes in a row.
_DIGIT_RUN_TR = bytes.maketrans(
    bytes(range(256)),
    bytes((0x30 if 0x30 <= c <= 0x39 else 0x20) for c in range(256)),
)
_RUN19 = b"0" * 19


def _checked_parse_int(text: str) -> int:
    value = int(text)
    if value < _INT_MIN or value > _INT_MAX:
        raise UnsupportedValue(
            "integer literal {} is outside the portable signed/unsigned "
            "64-bit range [{}, {}] (the stdlib parser preserves it exactly "
            "while orjson silently narrows it to a lossy float)".format(
                value, _INT_MIN, _INT_MAX
            )
        )
    return value


def _has_wide_int_run(
    data: Union[bytes, bytearray, memoryview, str],
) -> bool:
    # Wider than :func:`loads`' own ``Union[bytes, str]`` on purpose: the
    # translate path below needs a real ``bytes``/``bytearray``, so the
    # buffer types that only the regex can take are spelled out rather than
    # left as a statically dead branch.
    if isinstance(data, str):
        return _WIDE_INT_RUN_S.search(data) is not None
    if isinstance(data, (bytes, bytearray)):
        return _RUN19 in data.translate(_DIGIT_RUN_TR)
    # Any other buffer (memoryview) has no ``.translate`` returning bytes;
    # the regex accepts it directly, so keep the original path for it.
    return _WIDE_INT_RUN_B.search(data) is not None


class UnsupportedValue(ValueError):
    """A value that cannot be encoded identically across a mixed-orjson fleet.

    Raised by :func:`dumps_bytes` (and available as a standalone pre-check via
    :func:`ensure_portable`) for a non-finite float (``NaN`` / ``Infinity``),
    an integer outside the 64-bit window orjson supports, a string (or object
    key) containing a lone surrogate, a non-string object key, or nesting
    deeper than :data:`MAX_DEPTH`.  Also raised by :func:`loads` for an
    out-of-window integer LITERAL, which the two parsers would otherwise
    read back differently (exact int vs. silently narrowed float).
    Rejecting at write time is what keeps a record readable by every node
    regardless of which ones have orjson.
    """


def _surrogate_error(found: str) -> "UnsupportedValue":
    return UnsupportedValue(
        "string containing lone surrogate {!r} is not portable "
        "across the fleet (the stdlib writes an escape orjson can "
        "neither emit nor parse)".format(found)
    )


def _depth_error(depth: int) -> "UnsupportedValue":
    return UnsupportedValue(
        "nesting depth exceeds the portable bound of {} (a deeper document "
        "is writable by the stdlib encoder but a hard error on orjson's, "
        "so it could never be rewritten by every node)".format(MAX_DEPTH)
    )


def _ensure_finite(obj: Any, _depth: int = 0) -> None:
    """The float-and-depth half of :func:`ensure_portable`, standalone.

    Under orjson these are the ONLY portability hazards that need a
    Python-level pre-walk: orjson itself already REJECTS out-of-window
    integers and non-string keys (its ``dumps_bytes`` translates those into
    :class:`UnsupportedValue` after the fact), but it silently rewrites a
    non-finite float to ``null``, and it happily WRITES a document between
    :data:`MAX_DEPTH` and its own 256-deep encoder limit that the full gate
    (and so every stdlib node) rejects -- the two corruptions only a walk
    can catch before the bytes are written.  Kept light so the per-node work
    is a couple of isinstance checks, not the full rule set.
    """
    if isinstance(obj, float):
        if not math.isfinite(obj):
            raise UnsupportedValue(
                "non-finite float {!r} is not portable across the fleet "
                "(NaN/Infinity serializes differently with and without "
                "orjson)".format(obj)
            )
    elif isinstance(obj, dict):
        if _depth >= MAX_DEPTH:
            raise _depth_error(_depth)
        _next = _depth + 1
        for value in obj.values():
            _ensure_finite(value, _next)
    elif isinstance(obj, (list, tuple)):
        if _depth >= MAX_DEPTH:
            raise _depth_error(_depth)
        _next = _depth + 1
        for value in obj:
            _ensure_finite(value, _next)


def ensure_portable(obj: Any, _depth: int = 0) -> None:
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

    Depth is bounded at :data:`MAX_DEPTH` by an explicit counter, so a
    too-deep value -- including one far beyond the interpreter's own stack --
    is a clean :class:`UnsupportedValue`, never a RecursionError out of the
    gate itself.
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
    elif isinstance(obj, str):
        # ``isascii`` is an O(1) flag read on CPython, and a lone surrogate
        # is never ASCII, so the scan below can only matter for a non-ASCII
        # string.  This walk visits every string node in the document; the
        # unconditional regex was a third of the gate's cost.
        if not obj.isascii():
            match = _SURROGATES.search(obj)
            if match is not None:
                raise _surrogate_error(match.group())
    elif isinstance(obj, dict):
        if _depth >= MAX_DEPTH:
            raise _depth_error(_depth)
        _next = _depth + 1
        for key, value in obj.items():
            if not isinstance(key, str):
                raise UnsupportedValue(
                    "non-string object key {!r} is not portable across the "
                    "fleet (orjson rejects it, the stdlib coerces it)".format(
                        key
                    )
                )
            # The str branch above, inlined: a proven-str can reach nothing
            # else (no depth guard applies to it), and recursing per KEY as
            # well as per value doubled the node count of the whole walk.
            if not key.isascii():
                match = _SURROGATES.search(key)
                if match is not None:
                    raise _surrogate_error(match.group())
            ensure_portable(value, _next)
    elif isinstance(obj, (list, tuple)):
        if _depth >= MAX_DEPTH:
            raise _depth_error(_depth)
        _next = _depth + 1
        for value in obj:
            ensure_portable(value, _next)


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
        """Parse JSON from ``bytes`` or ``str``.

        Raises :class:`UnsupportedValue` for an integer literal outside the
        portable 64-bit window, identically on both backends (orjson alone
        would silently narrow it to a lossy float -- see the prescan notes
        above).
        """
        result = orjson.loads(data)
        if _has_wide_int_run(data):
            # verification parse: only its parse_int hook can raise
            # UnsupportedValue; any OTHER disagreement with orjson (e.g. a
            # deeper nesting tolerance) must not turn a document orjson
            # already accepted into a new error, so it is swallowed.
            try:
                _stdlib.loads(data, parse_int=_checked_parse_int)
            except UnsupportedValue:
                raise
            except Exception:  # noqa: BLE001 - see comment above
                pass
        return result

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
        """Parse JSON from ``bytes`` or ``str``.

        Raises :class:`UnsupportedValue` for an integer literal outside the
        portable 64-bit window, identically on both backends (see the
        prescan notes above; without the check a stdlib host would hand
        back an exact big int where an orjson host hands back a lossy
        float).
        """
        if _has_wide_int_run(data):
            return _stdlib.loads(data, parse_int=_checked_parse_int)
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
