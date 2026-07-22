# SPDX-License-Identifier: LicenseRef-cronstable-Proprietary
# Copyright (c) 2026 Parker Loflin. All rights reserved.
"""cronstable Pro: proprietary premium features built on the MIT cronstable core.

This package is NOT open source (see ../LICENSE and ../../LICENSING.md). It is a
separate distribution that depends on ``cronstable`` and uses its public API, so
proprietary code never lives inside the MIT package.

The module is a scaffold: it establishes the package, proves the boundary (it
imports the core at import time), and exposes the entitlement gate that premium
features check before doing paid work.
"""

from __future__ import annotations

from importlib import metadata

# Assert the boundary at import time: Pro builds on the MIT core. The core's
# __init__ is empty, so this is cheap; it exists to make the dependency real.
import cronstable as _core  # noqa: F401

__all__ = ["__version__", "core_version", "features"]

try:
    __version__ = metadata.version("cronstable-pro")
except metadata.PackageNotFoundError:  # running from a source tree, not installed
    __version__ = "0.0.0.dev0"


def core_version() -> str:
    """Version of the MIT cronstable core this Pro build runs against."""
    try:
        return metadata.version("cronstable")
    except metadata.PackageNotFoundError:
        return "unknown"


def features() -> list[str]:
    """Premium features exposed by this build.

    Empty in the scaffold. Real features register here and gate on
    :func:`cronstable_pro.licensing.is_entitled` as they are built.
    """
    return []
