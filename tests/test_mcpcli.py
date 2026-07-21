"""Tests for the ``cronstable mcp`` stdio bridge (:mod:`cronstable.mcpcli`).

The bridge is a synchronous line proxy over two seams: ``_post`` (one HTTP
round-trip via the module-level ``_OPENER``) and stdin/stdout.  ``_post``
itself is tested against a fake opener; everything above it monkeypatches
``_post`` with a scripted recorder, in the same single-seam style as
``test_state_job_cli.py``.  Import isolation lives in ``test_mcp.py``.
"""

import argparse
import io
import json
import ssl
import sys
import urllib.error

import pytest

from cronstable import mcpcli


def _args(**overrides):
    ns = argparse.Namespace(
        url=mcpcli.DEFAULT_URL,
        token=None,
        token_env=None,
        protocol_version=None,
        timeout=1.0,
        mcp_check=False,
        cacert=None,
        client_cert=None,
        client_key=None,
        insecure=False,
    )
    for key, value in overrides.items():
        setattr(ns, key, value)
    return ns


# ---------------------------------------------------------------------------
# token resolution
# ---------------------------------------------------------------------------


def test_resolve_token_prefers_flag(monkeypatch):
    monkeypatch.setenv(mcpcli.ENV_TOKEN, "from-env")
    assert mcpcli._resolve_token(_args(token="flag")) == "flag"


def test_resolve_token_default_env(monkeypatch):
    monkeypatch.setenv(mcpcli.ENV_TOKEN, "from-env")
    assert mcpcli._resolve_token(_args()) == "from-env"


def test_resolve_token_custom_env(monkeypatch):
    monkeypatch.delenv(mcpcli.ENV_TOKEN, raising=False)
    monkeypatch.setenv("OTHER_TOKEN", "other")
    assert mcpcli._resolve_token(_args(token_env="OTHER_TOKEN")) == "other"


def test_resolve_token_absent(monkeypatch):
    monkeypatch.delenv(mcpcli.ENV_TOKEN, raising=False)
    assert mcpcli._resolve_token(_args()) is None


# ---------------------------------------------------------------------------
# TLS resolution and the opener it selects
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _no_tls_env(monkeypatch):
    """No CRONSTABLE_WEB_* TLS variable leaks in from the developer's shell.

    Autouse because the drivers now resolve TLS on every run: an exported
    CRONSTABLE_WEB_CACERT would otherwise fail tests that have nothing to do
    with TLS.  Tests that want a variable set request the fixture and use the
    monkeypatch it returns.
    """
    for name in (
        mcpcli.ENV_CACERT,
        mcpcli.ENV_CLIENT_CERT,
        mcpcli.ENV_CLIENT_KEY,
        mcpcli.ENV_INSECURE,
    ):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def test_resolve_tls_none_without_flags(_no_tls_env):
    # the plaintext default: no context at all, so the bridge keeps the
    # transport it had before TLS existed.
    assert mcpcli._resolve_tls(_args()) is None


def test_resolve_tls_insecure_warns_on_stderr(_no_tls_env, capsys):
    ctx = mcpcli._resolve_tls(_args(insecure=True))
    assert ctx is not None
    assert ctx.verify_mode == ssl.CERT_NONE
    # the warning is the point: verification is off while the token still
    # goes out, so this must never be silent.
    err = capsys.readouterr().err
    assert "--insecure" in err
    assert "bearer token" in err


def test_resolve_tls_insecure_via_env_also_warns(_no_tls_env, capsys):
    _no_tls_env.setenv(mcpcli.ENV_INSECURE, "yes")
    assert mcpcli._resolve_tls(_args()).verify_mode == ssl.CERT_NONE
    assert "--insecure" in capsys.readouterr().err


def test_resolve_tls_cacert_flag_beats_env(_no_tls_env, tmp_path):
    # flag-then-env precedence, asserted through the error: the flag's path is
    # the one that gets opened.
    _no_tls_env.setenv(mcpcli.ENV_CACERT, str(tmp_path / "from-env.pem"))
    with pytest.raises(mcpcli._BridgeError) as caught:
        mcpcli._resolve_tls(_args(cacert=str(tmp_path / "from-flag.pem")))
    assert "from-flag.pem" in str(caught.value)


def test_resolve_tls_bad_path_is_a_clean_error(_no_tls_env, tmp_path):
    # an unreadable CA must not exit with a traceback out of ssl.
    with pytest.raises(mcpcli._BridgeError, match="TLS material"):
        mcpcli._resolve_tls(_args(cacert=str(tmp_path / "absent.pem")))


def test_build_opener_without_context_is_the_shared_global():
    # identity, not equality: _OPENER is the monkeypatch seam, so the no-TLS
    # path must hand back that very object.
    assert mcpcli._build_opener(None) is mcpcli._OPENER


def test_build_opener_with_context_is_a_separate_opener():
    opener = mcpcli._build_opener(ssl.create_default_context())
    assert opener is not mcpcli._OPENER
    handlers = [type(h).__name__ for h in opener.handlers]
    assert "HTTPSHandler" in handlers
    # an empty ProxyHandler evicts urllib's default one and installs no
    # *_open method of its own, so proxy support ends up absent entirely:
    # the same shape _OPENER has, which is the point of passing it along.
    assert "ProxyHandler" not in handlers
    assert "ProxyHandler" not in [
        type(h).__name__ for h in mcpcli._OPENER.handlers
    ]


# ---------------------------------------------------------------------------
# _post: one HTTP round-trip through the proxy-free opener
# ---------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, status, body):
        self.status = status
        self._body = body

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeOpener:
    """Records the request and returns/raises a scripted outcome."""

    def __init__(self, outcome):
        self._outcome = outcome
        self.request = None
        self.timeout = None

    def open(self, req, timeout=None):
        self.request = req
        self.timeout = timeout
        if isinstance(self._outcome, Exception):
            raise self._outcome
        return self._outcome


def test_post_success_builds_request(monkeypatch):
    opener = _FakeOpener(_FakeResponse(200, b'{"ok": 1}'))
    monkeypatch.setattr(mcpcli, "_OPENER", opener)
    status, body = mcpcli._post(
        "http://127.0.0.1:9/", b'{"a":1}', "sekret", "2025-11-25", 3.0
    )
    assert (status, body) == (200, b'{"ok": 1}')
    req = opener.request
    assert req.full_url == "http://127.0.0.1:9/mcp"
    assert req.get_method() == "POST"
    assert req.get_header("Authorization") == "Bearer sekret"
    assert req.get_header("Mcp-protocol-version") == "2025-11-25"
    assert opener.timeout == 3.0


def test_post_without_token_sends_no_auth_header(monkeypatch):
    opener = _FakeOpener(_FakeResponse(200, b"{}"))
    monkeypatch.setattr(mcpcli, "_OPENER", opener)
    mcpcli._post("http://127.0.0.1:9", b"{}", None, "2025-11-25", 1.0)
    assert opener.request.get_header("Authorization") is None


def test_post_http_error_returns_status_and_body(monkeypatch):
    err = urllib.error.HTTPError(
        "http://x/mcp", 401, "unauthorized", {}, io.BytesIO(b'{"error":"no"}')
    )
    monkeypatch.setattr(mcpcli, "_OPENER", _FakeOpener(err))
    status, body = mcpcli._post(
        "http://127.0.0.1:9", b"{}", None, "2025-11-25", 1.0
    )
    assert status == 401
    assert body == b'{"error":"no"}'


@pytest.mark.parametrize(
    "raised",
    [
        urllib.error.URLError(ConnectionRefusedError(61, "refused")),
        TimeoutError("timed out"),
        OSError("broken"),
    ],
)
def test_post_transport_failures_raise_bridge_error(monkeypatch, raised):
    monkeypatch.setattr(mcpcli, "_OPENER", _FakeOpener(raised))
    with pytest.raises(mcpcli._BridgeError, match="cannot reach"):
        mcpcli._post("http://127.0.0.1:9", b"{}", None, "2025-11-25", 1.0)


def test_post_without_opener_uses_the_module_global(monkeypatch):
    # the five-argument call is the pre-TLS signature; it must still go
    # through _OPENER, read at call time so the monkeypatch takes.
    opener = _FakeOpener(_FakeResponse(200, b"{}"))
    monkeypatch.setattr(mcpcli, "_OPENER", opener)
    mcpcli._post("http://127.0.0.1:9", b"{}", None, "2025-11-25", 1.0)
    assert opener.request is not None


def test_post_uses_the_supplied_opener(monkeypatch):
    unused = _FakeOpener(_FakeResponse(500, b"nope"))
    monkeypatch.setattr(mcpcli, "_OPENER", unused)
    chosen = _FakeOpener(_FakeResponse(200, b'{"ok": 1}'))
    status, _body = mcpcli._post(
        "http://127.0.0.1:9", b"{}", None, "2025-11-25", 1.0, opener=chosen
    )
    assert status == 200
    assert chosen.request is not None
    assert unused.request is None


def test_post_tls_failure_names_cacert_not_unreachable(monkeypatch):
    # a verification failure arrives as URLError(reason=SSLError); reporting it
    # as "cannot reach" would send the operator after a network problem.
    failure = urllib.error.URLError(
        ssl.SSLCertVerificationError(1, "certificate verify failed")
    )
    monkeypatch.setattr(mcpcli, "_OPENER", _FakeOpener(failure))
    with pytest.raises(mcpcli._BridgeError) as caught:
        mcpcli._post("https://127.0.0.1:9", b"{}", None, "2025-11-25", 1.0)
    message = str(caught.value)
    assert "TLS verification failed" in message
    assert "--cacert" in message
    assert "cannot reach" not in message


# ---------------------------------------------------------------------------
# reply sniffing / error message helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "body,expected",
    [
        (b'{"result": {"protocolVersion": "2025-06-18"}}', "2025-06-18"),
        (b'{"result": {"protocolVersion": 7}}', None),
        (b'{"result": []}', None),
        (b"[1]", None),
        (b"not json", None),
    ],
)
def test_sniff_protocol_version(body, expected):
    assert mcpcli._sniff_protocol_version(body) == expected


def test_http_error_message_includes_body_error():
    msg = mcpcli._http_error_message(503, b'{"error": "leader only"}')
    assert msg == "MCP endpoint returned HTTP 503: leader only"


def test_http_error_message_plain_on_unparseable_body():
    assert (
        mcpcli._http_error_message(500, b"<html>")
        == "MCP endpoint returned HTTP 500"
    )


def test_http_error_message_ignores_json_without_error():
    assert (
        mcpcli._http_error_message(500, b'{"ok": true}')
        == "MCP endpoint returned HTTP 500"
    )


# ---------------------------------------------------------------------------
# the stdin -> _post -> stdout proxy loop
# ---------------------------------------------------------------------------


class _PostRecorder:
    """Scripted stand-in for ``mcpcli._post``: pops one outcome per call."""

    def __init__(self, outcomes):
        self._outcomes = list(outcomes)
        self.calls = []

    def __call__(
        self, url, frame, token, protocol_version, timeout, opener=None
    ):
        self.calls.append(
            {
                "url": url,
                "frame": frame,
                "token": token,
                "pv": protocol_version,
                "timeout": timeout,
                "opener": opener,
            }
        )
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def _run(monkeypatch, stdin_text, outcomes, **arg_overrides):
    recorder = _PostRecorder(outcomes)
    monkeypatch.setattr(mcpcli, "_post", recorder)
    monkeypatch.setattr(sys, "stdin", io.StringIO(stdin_text))
    code = mcpcli._run_bridge(_args(**arg_overrides))
    return code, recorder


def _frames(captured_out):
    return [json.loads(line) for line in captured_out.splitlines() if line]


def test_bridge_replies_to_a_request(monkeypatch, capsys):
    reply = b'{"jsonrpc": "2.0", "id": 1, "result": {}}\n'
    code, recorder = _run(
        monkeypatch,
        '{"jsonrpc": "2.0", "id": 1, "method": "ping"}\n',
        [(200, reply)],
    )
    assert code == 0
    out = capsys.readouterr().out
    assert _frames(out) == [{"jsonrpc": "2.0", "id": 1, "result": {}}]
    assert recorder.calls[0]["pv"] == mcpcli.DEFAULT_PROTOCOL_VERSION


def test_bridge_skips_blank_lines_and_parse_errors(monkeypatch, capsys):
    code, recorder = _run(monkeypatch, "\n   \nnot json\n", [])
    assert code == 0
    frames = _frames(capsys.readouterr().out)
    assert frames == [
        {
            "jsonrpc": "2.0",
            "id": None,
            "error": {"code": -32700, "message": "parse error"},
        }
    ]
    assert recorder.calls == []


def test_bridge_notification_gets_no_reply(monkeypatch, capsys):
    code, recorder = _run(
        monkeypatch,
        '{"jsonrpc": "2.0", "method": "notifications/initialized"}\n',
        [(202, b"")],
    )
    assert code == 0
    assert capsys.readouterr().out == ""
    assert len(recorder.calls) == 1


def test_bridge_forwards_non_object_frame_without_reply(monkeypatch, capsys):
    # a malformed-but-valid-JSON frame (an array) is proxied verbatim but can
    # carry no id, so nothing is written even for an error response.
    code, recorder = _run(monkeypatch, "[1, 2]\n", [(400, b'{"error":"x"}')])
    assert code == 0
    assert capsys.readouterr().out == ""
    assert recorder.calls[0]["frame"] == b"[1, 2]"


def test_bridge_sniffs_negotiated_version_from_initialize(monkeypatch, capsys):
    init_reply = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"protocolVersion": "2025-06-18"},
        }
    ).encode()
    ping_reply = b'{"jsonrpc": "2.0", "id": 2, "result": {}}'
    stdin_text = (
        '{"jsonrpc": "2.0", "id": 1, "method": "initialize"}\n'
        '{"jsonrpc": "2.0", "id": 2, "method": "ping"}\n'
    )
    code, recorder = _run(
        monkeypatch, stdin_text, [(200, init_reply), (200, ping_reply)]
    )
    assert code == 0
    assert recorder.calls[0]["pv"] == mcpcli.DEFAULT_PROTOCOL_VERSION
    assert recorder.calls[1]["pv"] == "2025-06-18"
    assert len(_frames(capsys.readouterr().out)) == 2


def test_bridge_keeps_pinned_version_when_sniff_fails(monkeypatch, capsys):
    stdin_text = (
        '{"jsonrpc": "2.0", "id": 1, "method": "initialize"}\n'
        '{"jsonrpc": "2.0", "id": 2, "method": "ping"}\n'
    )
    code, recorder = _run(
        monkeypatch,
        stdin_text,
        [(200, b'{"result": {}}'), (200, b'{"id": 2}')],
        protocol_version="2025-03-26",
    )
    assert code == 0
    assert [c["pv"] for c in recorder.calls] == [
        "2025-03-26",
        "2025-03-26",
    ]


def test_bridge_transport_error_on_request_emits_error_frame(
    monkeypatch, capsys
):
    code, _ = _run(
        monkeypatch,
        '{"jsonrpc": "2.0", "id": 5, "method": "ping"}\n',
        [mcpcli._BridgeError("cannot reach the cronstable MCP endpoint")],
    )
    assert code == 0
    captured = capsys.readouterr()
    frames = _frames(captured.out)
    assert frames[0]["id"] == 5
    assert frames[0]["error"]["code"] == -32001
    assert "cannot reach" in frames[0]["error"]["message"]


def test_bridge_transport_error_on_notification_goes_to_stderr(
    monkeypatch, capsys
):
    code, _ = _run(
        monkeypatch,
        '{"jsonrpc": "2.0", "method": "notifications/initialized"}\n',
        [mcpcli._BridgeError("daemon is down")],
    )
    assert code == 0
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "daemon is down" in captured.err


def test_bridge_http_error_becomes_error_frame(monkeypatch, capsys):
    code, _ = _run(
        monkeypatch,
        '{"jsonrpc": "2.0", "id": 9, "method": "ping"}\n',
        [(401, b'{"error": "authentication required"}')],
    )
    assert code == 0
    frames = _frames(capsys.readouterr().out)
    assert frames[0]["error"]["code"] == -32001
    assert "HTTP 401" in frames[0]["error"]["message"]
    assert "authentication required" in frames[0]["error"]["message"]


def test_bridge_threads_the_default_opener_into_post(monkeypatch, capsys):
    # with no TLS flags the bridge still resolves an opener; it must be the
    # shared global, so the plaintext path is byte-for-byte what it was.
    _code, recorder = _run(
        monkeypatch,
        '{"jsonrpc": "2.0", "id": 1, "method": "ping"}\n',
        [(200, b'{"id": 1}')],
    )
    assert recorder.calls[0]["opener"] is mcpcli._OPENER


def test_bridge_reports_bad_tls_material_and_exits_nonzero(
    monkeypatch, capsys, tmp_path
):
    # a bad path is fatal before the read loop, not an error frame per request.
    recorder = _PostRecorder([])
    monkeypatch.setattr(mcpcli, "_post", recorder)
    monkeypatch.setattr(sys, "stdin", io.StringIO('{"id": 1}\n'))
    code = mcpcli._run_bridge(_args(cacert=str(tmp_path / "absent.pem")))
    assert code == 1
    captured = capsys.readouterr()
    assert captured.out == ""  # stdout carries JSON-RPC frames only
    assert "TLS material" in captured.err
    assert recorder.calls == []


def test_bridge_empty_200_body_becomes_error_frame(monkeypatch, capsys):
    code, _ = _run(
        monkeypatch,
        '{"jsonrpc": "2.0", "id": 3, "method": "ping"}\n',
        [(200, b"")],
    )
    assert code == 0
    frames = _frames(capsys.readouterr().out)
    assert frames[0]["error"]["code"] == -32001
    assert "HTTP 200" in frames[0]["error"]["message"]


# ---------------------------------------------------------------------------
# --check: the initialize + tools/list handshake self-test
# ---------------------------------------------------------------------------


def _check(monkeypatch, outcomes, **arg_overrides):
    recorder = _PostRecorder(outcomes)
    monkeypatch.setattr(mcpcli, "_post", recorder)
    code = mcpcli._check(_args(**arg_overrides))
    return code, recorder


def test_check_happy_path(monkeypatch, capsys):
    init_reply = json.dumps(
        {"result": {"protocolVersion": "2025-06-18"}}
    ).encode()
    tools_reply = json.dumps(
        {"result": {"tools": [{"name": "cron_get_status"}]}}
    ).encode()
    code, recorder = _check(
        monkeypatch, [(200, init_reply), (200, tools_reply)]
    )
    assert code == 0
    err = capsys.readouterr().err
    assert "ok - protocol 2025-06-18, 1 tool(s)" in err
    # the second request must carry the negotiated version, and the first the
    # pre-initialize default.
    assert recorder.calls[0]["pv"] == mcpcli.DEFAULT_PROTOCOL_VERSION
    assert recorder.calls[1]["pv"] == "2025-06-18"
    assert json.loads(recorder.calls[1]["frame"])["method"] == "tools/list"


def test_check_threads_the_default_opener_into_post(monkeypatch, capsys):
    code, recorder = _check(
        monkeypatch, [(200, b'{"result": {}}'), (200, b'{"result": {}}')]
    )
    assert code == 0
    assert [c["opener"] for c in recorder.calls] == [mcpcli._OPENER] * 2


def test_check_bad_tls_material_fails_before_any_request(
    monkeypatch, capsys, tmp_path
):
    code, recorder = _check(monkeypatch, [], cacert=str(tmp_path / "no.pem"))
    assert code == 1
    assert "mcp check: " in capsys.readouterr().err
    assert recorder.calls == []


def test_check_unreachable_daemon(monkeypatch, capsys):
    code, _ = _check(monkeypatch, [mcpcli._BridgeError("connection refused")])
    assert code == 1
    assert "connection refused" in capsys.readouterr().err


def test_check_initialize_http_failure(monkeypatch, capsys):
    code, _ = _check(monkeypatch, [(401, b'{"error": "auth"}')])
    assert code == 1
    err = capsys.readouterr().err
    assert "initialize failed" in err
    assert "HTTP 401" in err


def test_check_tools_list_failure_still_ok(monkeypatch, capsys):
    # a broken tools/list downgrades the report to 0 tools, not a failure.
    code, _ = _check(
        monkeypatch,
        [
            (200, b'{"result": {"protocolVersion": "2025-11-25"}}'),
            mcpcli._BridgeError("flaky"),
        ],
    )
    assert code == 0
    assert "0 tool(s)" in capsys.readouterr().err


def test_check_tools_list_bad_json_still_ok(monkeypatch, capsys):
    code, _ = _check(
        monkeypatch,
        [(200, b'{"result": {}}'), (200, b"not json")],
    )
    assert code == 0
    err = capsys.readouterr().err
    # sniff fell back to the pre-initialize default version.
    assert mcpcli.DEFAULT_PROTOCOL_VERSION in err
    assert "0 tool(s)" in err


# ---------------------------------------------------------------------------
# dispatch through the real subcommand parser
# ---------------------------------------------------------------------------


def _parse_cli(argv):
    parser = argparse.ArgumentParser(prog="cronstable")
    sub = parser.add_subparsers(dest="subcommand")
    mcpcli.add_mcp_command(sub)
    return parser.parse_args(argv)


def test_dispatch_routes_check(monkeypatch):
    args = _parse_cli(["mcp", "--check", "--url", "http://127.0.0.1:1"])
    monkeypatch.setattr(mcpcli, "_check", lambda a: 42)
    assert mcpcli.dispatch(args) == 42


def test_dispatch_routes_bridge_by_default(monkeypatch):
    args = _parse_cli(["mcp", "--token", "sekret"])
    seen = {}

    def fake_bridge(a):
        seen["token"] = a.token
        return 0

    monkeypatch.setattr(mcpcli, "_run_bridge", fake_bridge)
    assert mcpcli.dispatch(args) == 0
    assert seen == {"token": "sekret"}


def test_parser_defaults(monkeypatch):
    args = _parse_cli(["mcp"])
    assert args.url == mcpcli.DEFAULT_URL
    assert args.timeout == mcpcli.DEFAULT_TIMEOUT
    assert args.mcp_check is False
    assert args.protocol_version is None
    # the TLS flags default to "untouched transport", matching _resolve_tls
    # returning None for them.
    assert args.cacert is None
    assert args.client_cert is None
    assert args.client_key is None
    assert args.insecure is False


def test_parser_accepts_the_tls_flags():
    args = _parse_cli(
        [
            "mcp",
            "--cacert",
            "ca.pem",
            "--client-cert",
            "c.pem",
            "--client-key",
            "k.pem",
            "--insecure",
        ]
    )
    assert (args.cacert, args.client_cert, args.client_key) == (
        "ca.pem",
        "c.pem",
        "k.pem",
    )
    assert args.insecure is True
