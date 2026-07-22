# SPDX-License-Identifier: LicenseRef-cronstable-Proprietary
# Copyright (c) 2026 Parker Loflin. All rights reserved.
"""Boundary smoke tests for cronstable Pro.

Not part of the MIT core's tox suite; run with Pro's own tooling from the repo
root:

    pip install -e . -e "./pro[dev]"
    pytest pro/tests
"""

from __future__ import annotations

import cronstable_pro
from cronstable_pro import licensing


def test_pro_imports_the_core() -> None:
    # The boundary: Pro builds on the MIT core's public surface.
    assert isinstance(cronstable_pro.core_version(), str)


def test_scaffold_has_no_features_yet() -> None:
    assert cronstable_pro.features() == []


def test_entitlement_is_fail_closed() -> None:
    assert licensing.is_entitled() is False
    assert licensing.is_entitled("anything") is False
    assert licensing.verify(None).active is False
