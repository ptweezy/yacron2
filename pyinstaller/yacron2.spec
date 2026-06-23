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


a = Analysis(
    ["yacron2"],
    pathex=["."],
    binaries=[],
    datas=datas,
    hiddenimports=[],
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
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
