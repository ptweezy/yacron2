"""Optional orjson acceleration for the hot JSON serialization paths.

orjson (a compiled Rust/pyo3 extension) serializes and parses JSON several
times faster than the stdlib and hands back ``bytes`` directly, saving the
separate ``.encode("utf-8")`` every persistence site does by hand. It is an
OPTIONAL speedup, wired exactly like the uvloop event-loop swap: it ships no
wheels for some of the leaner architectures yacron2 targets (riscv64, armv6,
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
job-set fingerprint in :mod:`yacron2.fingerprint`, the cluster peer ETag in
:mod:`yacron2.cluster`) -- keep the stdlib ``json`` directly, so the output
stays stable and backend-independent whether or not a given host has orjson.
"""

import json as _stdlib
from typing import Any, Union, cast

try:
    import orjson
except ImportError:  # pragma: no cover - exercised on the no-orjson baseline
    orjson = None  # type: ignore[assignment]


if orjson is not None:

    def dumps_bytes(obj: Any, *, sort_keys: bool = False) -> bytes:
        """Serialize ``obj`` to compact UTF-8 JSON bytes."""
        option = orjson.OPT_SORT_KEYS if sort_keys else 0
        # cast: the tox mypy env runs --ignore-missing-imports (orjson not a
        # dev dep), so orjson.dumps reads as Any and warn_return_any fires.
        return cast(bytes, orjson.dumps(obj, option=option))

    def loads(data: Union[bytes, str]) -> Any:
        """Parse JSON from ``bytes`` or ``str``."""
        return orjson.loads(data)

else:

    def dumps_bytes(obj: Any, *, sort_keys: bool = False) -> bytes:
        """Serialize ``obj`` to compact UTF-8 JSON bytes."""
        text = _stdlib.dumps(obj, separators=(",", ":"), sort_keys=sort_keys)
        return text.encode("utf-8")

    def loads(data: Union[bytes, str]) -> Any:
        """Parse JSON from ``bytes`` or ``str``."""
        return _stdlib.loads(data)
