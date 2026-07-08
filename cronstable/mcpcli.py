"""The ``cronstable mcp`` stdio-to-HTTP bridge for local MCP clients.

Desktop MCP clients (Claude Desktop, Cursor, VS Code) launch a *stdio* server:
a subprocess that speaks newline-delimited JSON-RPC on stdin/stdout.
cronstable already serves MCP over HTTP (``POST /mcp``) from the daemon, so
rather than
re-implement every tool for a second transport, this bridge is a thin frame
proxy: it reads each JSON-RPC frame from stdin, forwards it to a running
daemon's ``/mcp`` endpoint over stdlib ``urllib``, and writes the reply to
stdout.  Tool logic lives in exactly one place (the daemon, :mod:`cronstable.\
mcp`).

Like the other job-facing subcommands (:mod:`cronstable.jobcli`) it imports
**only the standard library** -- never aiohttp, strictyaml, or the ``Cron``
graph -- so it starts instantly and stays out of the daemon's import cost.  It
therefore requires a REACHABLE running daemon; that is the right model for an
ops tool (there is nothing to serve without one).

The stdio contract: **stdout carries only JSON-RPC frames; everything else
goes to stderr.**  A notification (a frame with no ``id``) gets no reply, so
nothing is written for it.  Being a synchronous line proxy with no
server->client channel, the bridge cannot carry elicitation/sampling/progress;
those work only against the endpoint directly.  The negotiated protocol version
is sniffed from the ``initialize`` reply and stamped on every later request.
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any, Optional, Tuple

# Hardcoded, NOT imported from cronstable.mcp: importing that module would pull
# aiohttp and the daemon graph into this featherweight CLI. This is only the
# wire default sent before initialize completes; the real negotiated version is
# learned from the initialize reply and used thereafter.
DEFAULT_PROTOCOL_VERSION = "2025-11-25"
DEFAULT_URL = "http://127.0.0.1:8080"
DEFAULT_TIMEOUT = 30.0
# The env var cronstable's own docs use for the web bearer token; consulted as
# a convenience when neither --token nor --token-env is given.
ENV_TOKEN = "CRONSTABLE_WEB_TOKEN"

# JSON-RPC codes used when the bridge itself must synthesize an error reply.
_PARSE_ERROR = -32700
_TRANSPORT_ERROR = -32001

# Loopback/control traffic must never be proxied (the daemon's endpoint is
# usually 127.0.0.1, which an external proxy cannot reach), matching jobcli.
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class _BridgeError(Exception):
    """A transport failure reaching the daemon's ``/mcp`` endpoint."""


def _resolve_token(args: argparse.Namespace) -> Optional[str]:
    if args.token:
        return str(args.token)
    env_name = args.token_env or ENV_TOKEN
    value = os.environ.get(env_name)
    return value or None


def _post(
    url: str,
    frame: bytes,
    token: Optional[str],
    protocol_version: str,
    timeout: float,
) -> Tuple[int, bytes]:
    """POST one JSON-RPC frame to ``<url>/mcp``; return ``(status, body)``."""
    endpoint = url.rstrip("/") + "/mcp"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "MCP-Protocol-Version": protocol_version,
    }
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(
        endpoint, data=frame, method="POST", headers=headers
    )
    try:
        with _OPENER.open(req, timeout=timeout) as resp:  # noqa: S310
            return resp.status, resp.read()
    except urllib.error.HTTPError as ex:
        return ex.code, ex.read()
    except urllib.error.URLError as ex:
        raise _BridgeError(
            "cannot reach the cronstable MCP endpoint at {}: {}".format(
                endpoint, ex.reason
            )
        ) from ex
    except (TimeoutError, OSError) as ex:
        raise _BridgeError(
            "cannot reach the cronstable MCP endpoint at {}: {}".format(
                endpoint, ex
            )
        ) from ex


def _emit(obj: Any) -> None:
    """Write one JSON frame to stdout (the only thing stdout ever carries)."""
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def _error_frame(msg_id: Any, code: int, message: str) -> None:
    _emit(
        {
            "jsonrpc": "2.0",
            "id": msg_id,
            "error": {"code": code, "message": message},
        }
    )


def _run_bridge(args: argparse.Namespace) -> int:
    token = _resolve_token(args)
    protocol_version = args.protocol_version or DEFAULT_PROTOCOL_VERSION
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except ValueError:
            _error_frame(None, _PARSE_ERROR, "parse error")
            continue
        is_request = isinstance(msg, dict) and "id" in msg
        msg_id = msg.get("id") if isinstance(msg, dict) else None
        method = msg.get("method") if isinstance(msg, dict) else None
        try:
            status, body = _post(
                args.url,
                line.encode("utf-8"),
                token,
                protocol_version,
                args.timeout,
            )
        except _BridgeError as ex:
            if is_request:
                _error_frame(msg_id, _TRANSPORT_ERROR, str(ex))
            else:
                print(str(ex), file=sys.stderr)
            continue
        # learn the negotiated protocol version from the initialize reply and
        # stamp it on every subsequent request (how a "dumb" proxy discovers
        # the value it must send).
        if method == "initialize" and status == 200 and body:
            sniffed = _sniff_protocol_version(body)
            if sniffed is not None:
                protocol_version = sniffed
        if not is_request:
            continue  # a notification gets no reply frame
        if status == 200 and body:
            sys.stdout.write(body.decode("utf-8").rstrip("\n") + "\n")
            sys.stdout.flush()
        else:
            _error_frame(
                msg_id, _TRANSPORT_ERROR, _http_error_message(status, body)
            )
    return 0


def _sniff_protocol_version(body: bytes) -> Optional[str]:
    try:
        parsed = json.loads(body)
    except ValueError:
        return None
    result = parsed.get("result") if isinstance(parsed, dict) else None
    pv = result.get("protocolVersion") if isinstance(result, dict) else None
    return pv if isinstance(pv, str) else None


def _http_error_message(status: int, body: bytes) -> str:
    message = "MCP endpoint returned HTTP {}".format(status)
    try:
        parsed = json.loads(body)
        if isinstance(parsed, dict) and parsed.get("error"):
            message = "{}: {}".format(message, parsed["error"])
    except ValueError:
        pass
    return message


def _check(args: argparse.Namespace) -> int:
    """Handshake self-test: initialize + tools/list, report on stderr."""
    token = _resolve_token(args)
    pv = args.protocol_version or DEFAULT_PROTOCOL_VERSION
    init = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": pv,
            "capabilities": {},
            "clientInfo": {"name": "cronstable-mcp-check", "version": "0"},
        },
    }
    try:
        status, body = _post(
            args.url, json.dumps(init).encode(), token, pv, args.timeout
        )
    except _BridgeError as ex:
        print("mcp check: {}".format(ex), file=sys.stderr)
        return 1
    if status != 200:
        print(
            "mcp check: initialize failed ({})".format(
                _http_error_message(status, body)
            ),
            file=sys.stderr,
        )
        return 1
    negotiated = _sniff_protocol_version(body) or pv
    try:
        _s, body2 = _post(
            args.url,
            json.dumps(
                {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}
            ).encode(),
            token,
            negotiated,
            args.timeout,
        )
        tools = json.loads(body2).get("result", {}).get("tools", [])
    except (_BridgeError, ValueError, AttributeError):
        tools = []
    print(
        "mcp check: ok - protocol {}, {} tool(s) at {}".format(
            negotiated, len(tools), args.url.rstrip("/") + "/mcp"
        ),
        file=sys.stderr,
    )
    return 0


def add_mcp_command(sub: Any) -> None:
    """Register the ``cronstable mcp`` subcommand on the subparsers."""
    parser = sub.add_parser(
        "mcp",
        help="run the MCP stdio bridge to a running daemon's /mcp endpoint "
        "(for desktop MCP clients)",
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        metavar="URL",
        help="daemon web base URL serving /mcp (default: %(default)s)",
    )
    parser.add_argument(
        "--token",
        default=None,
        metavar="TOKEN",
        help="web.authToken bearer value (prefer --token-env to keep it out "
        "of the process table)",
    )
    parser.add_argument(
        "--token-env",
        default=None,
        metavar="VAR",
        help="env var holding the bearer token (default: {} if set)".format(
            ENV_TOKEN
        ),
    )
    parser.add_argument(
        "--protocol-version",
        default=None,
        metavar="REV",
        help="pin the MCP-Protocol-Version sent before initialize "
        "(default: {})".format(DEFAULT_PROTOCOL_VERSION),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        metavar="SECONDS",
        help="per-request deadline (default: %(default)s)",
    )
    parser.add_argument(
        "--check",
        dest="mcp_check",
        default=False,
        action="store_true",
        help="handshake the endpoint (initialize + tools/list) and exit, "
        "instead of proxying stdin",
    )


def dispatch(args: argparse.Namespace) -> int:
    if getattr(args, "mcp_check", False):
        return _check(args)
    return _run_bridge(args)
