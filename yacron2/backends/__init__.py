"""Optional lease-store leadership backends (kubernetes, etcd).

Each module here implements :class:`yacron2.leadership.LeaseBackend` against a
real coordination store, over plain HTTP via the core ``aiohttp`` dependency --
so neither adds a runtime dependency and, by avoiding grpc/protobuf wheels,
both run on every architecture yacron2 targets.

* **kubernetes** drives a ``coordination.k8s.io/v1`` ``Lease`` and has **two
  interchangeable transports**: the official ``kubernetes`` client when it is
  installed (and importable on this architecture), or a hand-rolled apiserver
  REST transport otherwise.  ``cluster.kubernetes.clientLibrary`` chooses:
  ``auto`` (default) prefers the native client and falls back to HTTP;
  ``library`` requires it (a config error if absent); ``http`` forces the
  hand-rolled path.
* **etcd** uses etcd's own v3 gRPC-gateway JSON/HTTP API directly -- a single,
  fully-portable transport, with no optional client library (the gateway is
  etcd's first-class HTTP interface, so a native grpc client buys little).

The modules are imported lazily by :func:`yacron2.leadership.make_backend`, so
they never enter the import graph unless ``cluster.backend`` selects them.
"""

from yacron2.config import ConfigError

# transport kinds returned by select_transport.
TRANSPORT_HTTP = "http"
TRANSPORT_LIBRARY = "library"


def select_transport(
    client_library: str, native_available: bool, backend: str
) -> str:
    """Choose the transport for a lease backend (pure; unit-tested).

    ``client_library`` is the resolved ``cluster.<backend>.clientLibrary``
    setting, ``native_available`` whether the native client imported on this
    architecture.  ``auto`` prefers the native client when present; ``library``
    requires it (raising :class:`~yacron2.config.ConfigError` if absent);
    ``http`` always uses the hand-rolled transport.
    """
    if client_library == "http":
        return TRANSPORT_HTTP
    if client_library == "library":
        if not native_available:
            raise ConfigError(
                "cluster.{0}.clientLibrary is 'library' but the native client "
                "is not importable on this architecture; install the optional "
                "yacron2[{0}] extra, or use clientLibrary auto/http".format(
                    backend
                )
            )
        return TRANSPORT_LIBRARY
    # "auto": prefer the native client when present, else the HTTP fallback.
    return TRANSPORT_LIBRARY if native_available else TRANSPORT_HTTP
