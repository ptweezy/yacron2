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

    def __call__(self, url, frame, token, protocol_version, timeout):
        self.calls.append(
            {
                "url": url,
                "frame": frame,
                "token": token,
                "pv": protocol_version,
                "timeout": timeout,
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
