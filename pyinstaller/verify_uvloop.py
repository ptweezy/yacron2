"""Verify a just-installed uvloop actually works; exit nonzero if it does not.

The release binary jobs let uvloop compile from sdist on arches that have no
prebuilt wheel (see .github/workflows/release.yml). A source build can succeed
yet be subtly miscompiled -- especially under QEMU emulation -- and the frozen
binary prefers uvloop whenever it is merely importable (yacron2/__main__.
_new_event_loop), so an unusable uvloop would crash the daemon at start-up
instead of falling back. Running a real (no-op) uvloop loop here catches that:
the caller uninstalls uvloop on a nonzero exit, so the binary is frozen without
it and cleanly runs on stock asyncio.

Exit 0 (nothing to verify) when uvloop is not installed at all -- the arch had
no wheel and the optional source build was skipped or failed, which is fine.
"""

import importlib.util
import sys

if importlib.util.find_spec("uvloop") is None:
    print("uvloop not installed; nothing to verify")
    sys.exit(0)

import asyncio

import uvloop

loop = uvloop.new_event_loop()
try:
    loop.run_until_complete(asyncio.sleep(0))
finally:
    loop.close()
print("uvloop verified: imports and runs a loop")
