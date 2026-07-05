# -*- mode: python ; coding: utf-8 -*-

import sys

from PyInstaller.utils.hooks import collect_data_files

block_cipher = None

# strip is a Unix concept (ELF/Mach-O). On Windows the GNU `strip` that ships
# with git bash WILL corrupt the bundled PE DLLs (notably pythonXY.dll) if
# PyInstaller is allowed to run it -- the resulting .exe then fails to load the
# Python DLL ("Invalid access to memory location"). So strip only off Windows.
STRIP = sys.platform != "win32"

# bundle the single-page web UI (yacron2/web/index.html) so the binary serves
# it without needing any files on disk
datas = collect_data_files("yacron2")

# uvloop and orjson are optional runtime accelerators, each imported behind a
# try/except with a stdlib fallback: uvloop by yacron2/__main__._new_event_loop
# (else asyncio) and orjson by yacron2/_json (else stdlib json). A frozen binary
# only contains what is importable in the build environment, so bundle each (as
# a hidden import, since the guarded import is easy for the analysis to miss)
# exactly when the build env actually has it -- the binary CI jobs best-effort
# install a wheel, or source-build it, before freezing (see install_orjson.sh /
# the uvloop steps). Absent (an arch with no wheel and no working source build)
# the list stays empty and the binary simply runs on the stdlib equivalents.
hiddenimports = []
try:
    import uvloop  # noqa: F401

    hiddenimports.append("uvloop")
except ImportError:
    pass
try:
    import orjson  # noqa: F401

    hiddenimports.append("orjson")
except ImportError:
    pass


# optimize=2 compiles every bundled module at -OO: it strips assert statements
# AND docstrings from the frozen bytecode. yacron2's modules are deliberately
# docstring-dense (the rationale lives next to the code), and those strings
# otherwise ship in the archive and sit in resident memory for the life of the
# daemon; dropping them shrinks the binary and lowers RSS. Every assert in the
# tree is a type-narrowing / internal-invariant check (`x is not None`,
# `isinstance`, `not in`) with no side effects and no untrusted-input
# validation, so removing them does not change behavior on the correct path.
# The source-run test suite does not exercise the frozen -OO build; CI's
# per-arch `--version` smoke test is the backstop for a dependency that might
# misbehave without its docstrings/asserts.
a = Analysis(
    ["yacron2"],
    pathex=["."],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
    optimize=2,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="yacron2",
    debug=False,
    bootloader_ignore_signals=False,
    strip=STRIP,
    upx=False,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
)
