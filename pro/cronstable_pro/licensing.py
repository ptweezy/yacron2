# SPDX-License-Identifier: LicenseRef-cronstable-Proprietary
# Copyright (c) 2026 Parker Loflin. All rights reserved.
"""Entitlement gate for cronstable Pro.

Premium features call :func:`is_entitled` before doing paid work. This module is
a SCAFFOLD with a fail-closed default (no entitlement until real verification is
wired in), so a half-built feature never hands out paid functionality for free.

Design intent (not yet implemented here):

- Verify entitlements SERVER-SIDE. The client presents a signed proof (an App
  Store transaction/receipt, a license key, a subscription token) and a server,
  or a signed offline-verifiable token, is the source of truth. Do NOT embed a
  secret in the client and "check the key locally": a bundled binary or an iOS
  app can be inspected, so client-only checks are trivially bypassed.
- Fail closed: any error, expiry, or missing proof yields NO entitlement.
- Keep this the ONLY gate, so there is one place to audit what paid work needs a
  valid entitlement.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Entitlement:
    """The result of verifying a customer's proof of purchase."""

    active: bool
    plan: str = "none"
    # e.g. account id, expiry, feature flags: added as the model firms up.


def verify(proof: str | None) -> Entitlement:
    """Verify a proof of purchase and return the resulting entitlement.

    SCAFFOLD: always returns an inactive entitlement. Replace the body with real
    server-side / signed-token verification (see the module docstring). Until
    then, premium features stay off by construction.
    """
    return Entitlement(active=False)


def is_entitled(proof: str | None = None) -> bool:
    """True iff *proof* grants an active entitlement. Fail-closed on any error."""
    try:
        return verify(proof).active
    except Exception:
        return False
