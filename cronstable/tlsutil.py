"""Shared TLS plumbing for every listener and client cronstable speaks over.

The cluster's gossip mesh grew the first SSL contexts, the first on-disk
cert fingerprint and the first "is the new material loadable yet?" dry run
(see :mod:`cronstable.cluster`).  The web listeners, the job-facing state
API and the CLI clients all need the same three things, so they live here
instead of being reimplemented per call site:

* :func:`build_listener_ssl_context`, a server context requiring client
  certificates if and only if a client CA is configured;
* :func:`build_mutual_client_ssl_context` /
  :func:`build_verifying_client_ssl_context`, the two client postures: the
  strict mutual one the cluster peer channel needs and the softer
  pin-a-private-CA one the CLI clients need;
* :func:`tls_file_signature` and :func:`listener_tls_loadable`, noticing an
  in-place certificate rotation, and checking the new material loads before
  tearing a working listener down for it.

This module is a deliberate leaf: it imports nothing from ``cronstable`` and
nothing outside the standard library, so :mod:`cronstable.cluster`,
:mod:`cronstable.cron` and :mod:`cronstable.jobapi` can all import it at
module level without any of the import-cycle deferral those modules use for
each other.
"""

import os
import ssl
from typing import (
    Any,
    Callable,
    Dict,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

# The keys of a listener `tls:` block, in the order the rotation check stats
# them. `clientCa` is optional, which is why tls_file_signature tolerates a
# missing/None entry rather than indexing.
LISTENER_TLS_KEYS = ("cert", "key", "clientCa")

# The server-side files of a `state.jobApi.tls` block, in signature order.
# `ca` is deliberately excluded: unlike `cert`/`key` it is never loaded into
# the listener's SSLContext.  It is handed to jobs as a PATH (via
# CRONSTABLE_STATE_CACERT) and read fresh by each job, so the daemon caches
# nothing an in-place `ca` rotation would stale, and watching it here would
# restart the listener for no reason.  See
# cronstable.cron.Cron._job_api_tls_files_changed.
JOB_API_TLS_KEYS = ("cert", "key")


def listener_tls_configured(tls: Optional[Mapping[str, Any]]) -> bool:
    """Whether a ``tls`` block actually names material to serve.

    A `tls:` block can be PRESENT but empty of values: strictyaml maps a
    blank scalar (``cert:`` with nothing after it, which is what a template
    rendering an unset variable produces) to ``None``, giving a truthy dict
    of falsy values.  Callers must not treat that as "TLS is configured", so
    every path that reaches for ``tls["cert"]`` gates on this first rather
    than on the dict's own truthiness.
    """
    if not tls:
        return False
    return bool(tls.get("cert")) and bool(tls.get("key"))


# --------------------------------------------------------------------------
# server contexts
# --------------------------------------------------------------------------


def build_listener_ssl_context(
    cert: str,
    key: str,
    *,
    client_ca: Optional[str] = None,
) -> ssl.SSLContext:
    """A server context serving ``cert``/``key``.

    Client certificates are REQUIRED if and only if ``client_ca`` is given
    (mutual TLS); with it unset the context does no client authentication at
    all.  The tri-state is deliberate: "optional client CA" means the CA is
    optional, not that a presented client certificate is.
    :data:`ssl.CERT_OPTIONAL` would complete the handshake for a client that
    presents nothing, and aiohttp offers no ergonomic hook to reject it
    afterwards, so ``client_ca`` would silently become a no-op for an
    operator who set it expecting caller authentication.

    With ``client_ca`` set, that CA file IS the caller allowlist: a server
    cannot do hostname verification, so any certificate the CA ever signed is
    accepted.  Point it at a dedicated CA, never a shared organisational one.

    Raises :exc:`OSError` (missing or unreadable file) or
    :exc:`ssl.SSLError` (malformed PEM, certificate/key mismatch); callers
    decide whether that is fatal.  See :func:`listener_tls_loadable` for the
    side-effect-free dry run.
    """
    # The CA must go in through cafile= at CONSTRUCTION.  With cafile None,
    # create_default_context() calls load_default_certs() and the context
    # trusts the system root store; adding the CA afterwards with
    # load_verify_locations() would widen the trust set from "the configured
    # CA" to "the configured CA plus every public root on the box", which for
    # the cluster is a membership-boundary break (see
    # cronstable.cluster.build_server_ssl_context).
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH, cafile=client_ca)
    ctx.load_cert_chain(cert, key)
    # When client_ca is None the system roots create_default_context just
    # loaded are never consulted (verify_mode is CERT_NONE), so that load is
    # wasted work once per listener start, not a trust widening.  Do not
    # "fix" it by passing cafile=None and calling load_verify_locations
    # later: that is the dangerous shape the comment above describes.
    ctx.verify_mode = ssl.CERT_REQUIRED if client_ca else ssl.CERT_NONE
    return ctx


# --------------------------------------------------------------------------
# client contexts
# --------------------------------------------------------------------------


def build_mutual_client_ssl_context(
    ca: str, cert: str, key: str
) -> ssl.SSLContext:
    """A client context that verifies the peer against ``ca``, presents
    ``cert``/``key``, and pins the hostname.

    The strict mutual-TLS posture the cluster peer channel requires: every
    field mandatory, verification and hostname checking always on.
    """
    ctx = ssl.create_default_context(cafile=ca)
    ctx.load_cert_chain(cert, key)
    # create_default_context already sets check_hostname=True and
    # verify_mode=CERT_REQUIRED for the client purpose; be explicit anyway.
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


def build_verifying_client_ssl_context(
    *,
    ca: Optional[str] = None,
    cert: Optional[str] = None,
    key: Optional[str] = None,
    insecure: bool = False,
) -> Optional[ssl.SSLContext]:
    """A client context for the CLI clients, or ``None`` for "library default".

    ``ca`` pins a private trust anchor (an internally-issued or self-signed
    certificate); ``cert``/``key`` present a client certificate to a listener
    configured with ``web.tls.clientCa``; ``insecure`` disables verification
    entirely.

    Returns ``None`` when no option is set, so a caller keeps its existing
    default transport untouched rather than paying for a context that changes
    nothing.

    Raises :exc:`ValueError` for ``key`` without ``cert``. A private key
    alone cannot present an identity, so honouring it is impossible; failing
    is the only alternative to accepting the flag and silently ignoring it,
    which would leave the caller believing it had presented a client
    certificate to an mTLS listener that in fact refused it.
    """
    if key and not cert:
        raise ValueError(
            "a client key needs its certificate: pass both, or neither"
        )
    if not (ca or cert or key or insecure):
        return None
    ctx = ssl.create_default_context(cafile=ca)
    if insecure:
        # check_hostname must go first: setting verify_mode to CERT_NONE
        # while check_hostname is still True raises ValueError.
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    if cert:
        ctx.load_cert_chain(cert, key)
    return ctx


# --------------------------------------------------------------------------
# rotation detection
# --------------------------------------------------------------------------


def file_signature(path: str) -> Optional[Tuple[int, int]]:
    """``(st_mtime_ns, st_size)`` for ``path``, or ``None`` if it cannot be
    stat'ed.

    ``os.stat`` follows symlinks, so the atomic symlink swap Kubernetes uses
    for a mounted secret is picked up.  A stat error (a file briefly absent
    mid-rotation) records ``None`` and simply compares unequal once the file
    is back, which is the safe direction: a spurious restart, not a missed
    one.
    """
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (st.st_mtime_ns, st.st_size)


def tls_file_signature(
    tls: Mapping[str, Any], keys: Sequence[str]
) -> Dict[str, Optional[Tuple[int, int]]]:
    """A cheap on-disk fingerprint of the ``keys`` named in ``tls``.

    An SSL context is built once and loads the certificate and key into
    memory, so an *in-place* rotation (same paths, new bytes, which is
    exactly how cert-manager, Vault and Kubernetes secret refreshes renew) is
    otherwise invisible to a long-running process: it keeps serving the old
    certificate until that expires.  Comparing this mapping across reloads is
    how a listener notices.

    An absent or ``None`` entry (an optional ``clientCa``) records ``None``
    rather than raising, so an optional-material block is safe to pass whole.
    """
    out: Dict[str, Optional[Tuple[int, int]]] = {}
    for key in keys:
        path = tls.get(key)
        out[key] = file_signature(path) if path else None
    return out


def contexts_loadable(*builders: Callable[[], ssl.SSLContext]) -> bool:
    """True if every builder runs without :exc:`OSError` / :exc:`ssl.SSLError`.

    A side-effect-free dry run: the contexts are built and discarded.  Used
    before tearing a running listener down for new material, because a
    cert-manager / Vault / Kubernetes refresh is not atomic across the files
    and can be observed half-written.  Stopping and then failing to rebuild
    would leave nothing serving; retrying next reload leaves the valid old
    certificate up.
    """
    for build in builders:
        try:
            build()
        except (OSError, ssl.SSLError):
            return False
    return True


def listener_tls_loadable(tls: Optional[Mapping[str, Any]]) -> bool:
    """Whether a ``{cert, key, clientCa?}`` block loads into a server context
    right now.

    ``None`` or empty is vacuously loadable: there is nothing on disk to
    pre-validate, so a plaintext listener never defers a restart on this.
    """
    if not tls:
        return True
    if not listener_tls_configured(tls):
        # A half-configured or blank-valued block cannot build a context.
        # Answer "not loadable" rather than raising KeyError out of what is
        # only a "can we restart yet?" probe. A block in this shape never
        # gets as far as being served: listener_tls_configured also gates the
        # build path (see cronstable.cron.Cron.start_stop_web_app).
        return False
    return contexts_loadable(
        lambda: build_listener_ssl_context(
            tls["cert"], tls["key"], client_ca=tls.get("clientCa")
        )
    )
