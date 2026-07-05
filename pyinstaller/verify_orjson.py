"""Verify a just-installed orjson actually works; exit nonzero if it does not.

The release binary jobs and the container image builds let orjson (a Rust
extension) compile from sdist on the arches that have no prebuilt wheel. A
source build -- especially under QEMU emulation -- can succeed yet be subtly
miscompiled, and yacron2 routes every durable-state and cluster-gossip
read/write through orjson whenever it is merely importable (yacron2/_json), so
an unusable orjson would corrupt the state store instead of falling back.
Round-tripping a small document here catches that: the caller uninstalls orjson
on a nonzero exit, so it is never frozen/shipped and yacron2 cleanly uses the
stdlib json.

Exit 0 (nothing to verify) when orjson is not installed at all -- the arch had
no wheel and the optional source build was skipped or failed, which is fine.
"""

import importlib.util
import sys

if importlib.util.find_spec("orjson") is None:
    print("orjson not installed; nothing to verify (using stdlib json)")
    sys.exit(0)

import orjson

# Exercise exactly what yacron2._json.dumps_bytes/loads depend on: compact
# bytes output, the OPT_SORT_KEYS path, and a lossless round-trip including
# non-ASCII (orjson emits raw UTF-8). A miscompiled build typically fails here.
sample = {
    "schemaVersion": "v1",
    "z": 1,
    "a": "café ☃ 日本",
    "n": [1, 2.5, True, None],
}
blob = orjson.dumps(sample, option=orjson.OPT_SORT_KEYS)
if not isinstance(blob, bytes) or orjson.loads(blob) != sample:
    print(
        "orjson: round-trip mismatch (miscompiled?); using stdlib json",
        file=sys.stderr,
    )
    sys.exit(1)
print("orjson verified: imports and round-trips")
