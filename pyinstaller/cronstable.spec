# -*- mode: python ; coding: utf-8 -*-

import sys

from PyInstaller.utils.hooks import collect_data_files

block_cipher = None

# strip is a Unix concept (ELF/Mach-O). On Windows the GNU `strip` that ships
# with git bash WILL corrupt the bundled PE DLLs (notably pythonXY.dll) if
# PyInstaller is allowed to run it -- the resulting .exe then fails to load the
# Python DLL ("Invalid access to memory location"). So strip only off Windows.
STRIP = sys.platform != "win32"

# bundle the single-page web UI (cronstable/web/index.html) so the binary serves
# it without needing any files on disk
datas = collect_data_files("cronstable")

# uvloop and orjson are optional runtime accelerators, each imported behind a
# try/except with a stdlib fallback: uvloop by cronstable/__main__._new_event_loop
# (else asyncio) and orjson by cronstable/_json (else stdlib json). A frozen binary
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


# Modules that are never reachable at runtime but that the analysis (or a
# dependency's optional `try: import ...` probe) could otherwise rake into the
# bundle. cronstable is a headless daemon with an ANSI TUI (raw termios/tty on
# POSIX, msvcrt on Windows) and an HTML/JSON web UI, so no GUI toolkit is ever
# imported; the TUI reads raw keypresses itself and never uses readline; durable
# state is JSON (orjson / stdlib json), never sqlite. Excluding a module that was
# never going to be collected is a harmless no-op, so this list is insurance
# against dead weight sneaking in. Verified against the tree and the runtime deps
# (aiohttp / jinja2 / strictyaml / sentry-sdk / aiosmtplib / psutil / tzdata);
# the per-arch `--version` smoke test is the build-time backstop.
excludes = [
    # GUI toolkits: never imported by a headless daemon.
    "tkinter",
    "_tkinter",
    "turtle",
    "turtledemo",
    "idlelib",
    # curses: the TUI drives the terminal through termios/tty directly.
    "curses",
    "_curses",
    "_curses_panel",
    # the TUI reads raw keypresses; nothing uses readline's line editor.
    "readline",
    # no sqlite anywhere in cronstable or its runtime deps.
    "sqlite3",
    "_sqlite3",
    # dev/tooling stdlib that never runs inside the frozen daemon.
    "test",
    "lib2to3",
    "ensurepip",
    "pydoc_data",
]


# optimize=2 compiles every bundled module at -OO: it strips assert statements
# AND docstrings from the frozen bytecode. cronstable's modules are deliberately
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
    ["cronstable"],
    pathex=["."],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=excludes,
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
    name="cronstable",
    debug=False,
    bootloader_ignore_signals=False,
    strip=STRIP,
    upx=False,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
)
