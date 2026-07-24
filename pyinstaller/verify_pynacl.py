"""Verify a just-installed PyNaCl actually works; exit nonzero if not.

The release binary jobs let PyNaCl compile from sdist on arches that have
no prebuilt wheel (its bundled libsodium is plain C, so unlike orjson it
needs no Rust). A source build can succeed yet be subtly miscompiled,
especially under QEMU emulation, and a broken libsodium in a frozen
binary would corrupt or crash every push alert. A real sealed-box
round-trip here catches that: the caller uninstalls PyNaCl on a nonzero
exit, so the binary is frozen without it and the daemon's fail-closed
config check reports push as unavailable instead of sealing garbage.

Exit 0 (nothing to verify) when PyNaCl is not installed at all: the arch
had no wheel and the optional source build was skipped or failed, which
is fine, that binary simply ships without the push extra.
"""

import importlib.util
import sys

if importlib.util.find_spec("nacl") is None:
    print("pynacl not installed; nothing to verify")
    sys.exit(0)

from nacl.public import PrivateKey, SealedBox  # noqa: E402

message = b"cronstable push self-test \xf0\x9f\x94\x94"
device = PrivateKey.generate()
sealed = SealedBox(device.public_key).encrypt(message)
opened = SealedBox(device).decrypt(sealed)
if opened != message:
    print("pynacl sealed-box round-trip MISMATCH")
    sys.exit(1)
print("pynacl verified: sealed-box round-trip ok")
