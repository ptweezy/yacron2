"""Zero-config LAN discovery: the opt-in Bonjour/mDNS advert.

With ``web.bonjour`` enabled, the daemon advertises its web control API
as a ``_cronstable._tcp`` service on the local network, so a companion
app (or ``dns-sd -B _cronstable._tcp``) finds it without a typed URL.
The advert carries no secrets: instance name, port, scheme and version
only; a client still needs a bearer token to read anything.

python-zeroconf is an optional extra (``pip install
"cronstable[discovery]"``); the import is guarded, and config validation
refuses ``web.bonjour`` when the library is absent.  Unlike push (an
alerting channel that must fail closed), a *runtime* advert failure is
logged and swallowed: discovery is a convenience, and an mDNS hiccup
must never take down a scheduler.
"""

import asyncio
import logging
import socket
from typing import Any, Dict, Optional

try:
    from zeroconf import ServiceInfo
    from zeroconf.asyncio import AsyncZeroconf

    HAVE_ZEROCONF = True
except ImportError:  # pragma: no cover - exercised on the bare baseline
    HAVE_ZEROCONF = False

logger = logging.getLogger("cronstable")

SERVICE_TYPE = "_cronstable._tcp.local."

#: Bound the register/unregister round-trips: mDNS involves the network,
#: and the callers (config apply, shutdown) must never hang on it.
_MDNS_OP_TIMEOUT = 10.0


def primary_address() -> Optional[str]:
    """This host's primary outbound IPv4 address, or ``None``.

    The connected-UDP trick: connecting a datagram socket selects the
    route (and thus the source address) without sending a packet.  The
    target is TEST-NET-1, never actually reached.  Falls back to the
    hostname's A record, then gives up (the caller skips the advert
    with a warning rather than advertising loopback).
    """
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect(("192.0.2.1", 9))
        address = str(probe.getsockname()[0])
    except OSError:
        address = None
    finally:
        probe.close()
    if address and not address.startswith("127."):
        return address
    try:
        address = socket.gethostbyname(socket.gethostname())
    except OSError:
        return None
    if address.startswith("127."):
        return None
    return address


def _instance_name(name: str) -> str:
    """A safe mDNS instance label: dots would split the service name."""
    cleaned = name.replace(".", "-").strip("-")
    return cleaned[:63] or "cronstable"


class BonjourAdvertiser:
    """Owns the registered mDNS service across config reloads.

    ``start_stop`` is the whole lifecycle, mirroring the daemon's other
    ``start_stop_*`` edges: call it with the desired advert (or ``None``
    to stop) on every config apply; it re-registers only when something
    the advert carries actually changed.
    """

    def __init__(self) -> None:
        self._zeroconf: Optional[Any] = None
        self._info: Optional[Any] = None
        self._signature: Optional[Dict[str, Any]] = None

    @property
    def active(self) -> bool:
        return self._info is not None

    async def start_stop(self, advert: Optional[Dict[str, Any]]) -> None:
        """Converge the running advert onto ``advert``.

        ``advert`` is ``{"name", "port", "properties"}`` (built by the
        caller from the web config) or ``None`` for off.  Never raises:
        a network/mDNS failure logs and leaves the advert off until the
        next config apply retries it.
        """
        if advert == self._signature and (
            advert is None or self._info is not None
        ):
            return
        await self._unregister()
        self._signature = advert
        if advert is None:
            return
        if not HAVE_ZEROCONF:  # pragma: no cover - config validation gates
            logger.error(
                "bonjour: python-zeroconf is not installed; not advertising"
            )
            return
        address = primary_address()
        if address is None:
            logger.warning(
                "bonjour: could not determine a non-loopback address to "
                "advertise; skipping the advert until the next reload"
            )
            self._signature = None
            return
        instance = _instance_name(advert["name"])
        properties = {
            key: str(value)
            for key, value in (advert.get("properties") or {}).items()
        }
        info = ServiceInfo(
            SERVICE_TYPE,
            "{}.{}".format(instance, SERVICE_TYPE),
            addresses=[socket.inet_aton(address)],
            port=int(advert["port"]),
            properties=properties,
            server="{}.local.".format(instance),
        )
        try:
            zeroconf = AsyncZeroconf()
            await asyncio.wait_for(
                zeroconf.async_register_service(info),
                timeout=_MDNS_OP_TIMEOUT,
            )
        except Exception as exc:
            logger.error(
                "bonjour: failed to register the %s advert: %s",
                SERVICE_TYPE,
                exc,
            )
            self._signature = None
            return
        self._zeroconf = zeroconf
        self._info = info
        logger.info(
            "bonjour: advertising %r on %s:%d",
            instance,
            address,
            int(advert["port"]),
        )

    async def _unregister(self) -> None:
        zeroconf, info = self._zeroconf, self._info
        self._zeroconf = None
        self._info = None
        if zeroconf is None:
            return
        try:
            if info is not None:
                await asyncio.wait_for(
                    zeroconf.async_unregister_service(info),
                    timeout=_MDNS_OP_TIMEOUT,
                )
        except Exception as exc:
            logger.warning("bonjour: unregister failed: %s", exc)
        try:
            await asyncio.wait_for(
                zeroconf.async_close(), timeout=_MDNS_OP_TIMEOUT
            )
        except Exception as exc:
            logger.warning("bonjour: close failed: %s", exc)

    async def stop(self) -> None:
        """Tear the advert down (shutdown path)."""
        self._signature = None
        await self._unregister()
