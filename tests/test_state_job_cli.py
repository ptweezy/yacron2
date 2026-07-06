"""The job-facing CLI (`yacron2 state|cursor|lock|artifact|...`).

Drives the commands the way a shell in a job would: through
``yacron2.__main__.main_loop`` with a fake ``sys.argv`` and the injected
``YACRON2_STATE_*`` environment, so it exercises the real argument parsing and
the __main__ routing that tells a job-facing ``state get`` from an admin
``state gc``.  The one HTTP seam (``yacron2.jobcli._http``) is monkeypatched
with a recorder, so every verb's request-building, output, and exit code are
asserted deterministically without a live server (the real wire is covered in
test_state_job_api.py).
"""

import asyncio
import json
import sys
import urllib.request

import pytest

import yacron2.__main__
from yacron2 import jobcli


class ExitError(Exception):
    pass


def _exit(code=0):
    raise ExitError(code)


class _FakeHTTP:
    """Stand-in for jobcli._http: canned responses keyed by path, recorded."""

    def __init__(self, responses=None):
        self.responses = responses or {}
        self.calls = []

    def __call__(
        self,
        method,
        path,
        *,
        query=None,
        json_body=None,
        data=None,
        timeout=None,
    ):
        self.calls.append(
            {
                "method": method,
                "path": path,
                "query": query,
                "json": json_body,
                "data": data,
                "timeout": timeout,
            }
        )
        status, body = self.responses.get(path, (200, {}))
        payload = (
            body if isinstance(body, bytes) else json.dumps(body).encode()
        )
        return status, {}, payload


def _cli(monkeypatch, argv, http=None, stdin=b""):
    monkeypatch.setenv("YACRON2_STATE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("YACRON2_STATE_TOKEN", "tok")
    if http is not None:
        monkeypatch.setattr(jobcli, "_http", http)
    loop = asyncio.new_event_loop()
    try:
        monkeypatch.setattr(sys, "argv", ["yacron2"] + argv)
        monkeypatch.setattr(sys, "exit", _exit)
        with pytest.raises(ExitError) as excinfo:
            yacron2.__main__.main_loop(loop)
        return excinfo.value.args[0]
    finally:
        loop.close()


# --------------------------------------------------------------------------
# KV (state get/set/delete/keys) coexisting with the admin `state` actions
# --------------------------------------------------------------------------


def test_state_set_builds_request(monkeypatch, capsys):
    http = _FakeHTTP({"/v1/kv/set": (200, {"ok": True})})
    assert _cli(monkeypatch, ["state", "set", "k", "v"], http) == 0
    call = http.calls[0]
    assert call["method"] == "POST" and call["path"] == "/v1/kv/set"
    assert call["json"] == {"scope": None, "key": "k", "value": "v"}


def test_state_get_prints_value(monkeypatch, capsys):
    http = _FakeHTTP({"/v1/kv/get": (200, {"value": "hello"})})
    assert _cli(monkeypatch, ["state", "get", "k"], http) == 0
    assert capsys.readouterr().out == "hello\n"


def test_state_get_missing_exit_4(monkeypatch, capsys):
    http = _FakeHTTP({"/v1/kv/get": (404, {})})
    assert _cli(monkeypatch, ["state", "get", "k"], http) == 4


def test_state_set_json_flag(monkeypatch):
    http = _FakeHTTP({"/v1/kv/set": (200, {"ok": True})})
    _cli(monkeypatch, ["state", "set", "k", '{"a": 1}', "--json"], http)
    assert http.calls[0]["json"]["value"] == {"a": 1}


def test_state_global_scope(monkeypatch):
    http = _FakeHTTP({"/v1/kv/get": (200, {"value": "x"})})
    _cli(monkeypatch, ["state", "get", "k", "--global"], http)
    assert http.calls[0]["query"] == {"scope": "global", "key": "k"}


def test_state_keys_lists(monkeypatch, capsys):
    http = _FakeHTTP(
        {"/v1/kv/list": (200, {"keys": [{"key": "a"}, {"key": "b"}]})}
    )
    assert _cli(monkeypatch, ["state", "keys"], http) == 0
    assert capsys.readouterr().out == "a\nb\n"


def test_state_delete_absent_exit_4(monkeypatch):
    http = _FakeHTTP({"/v1/kv/delete": (200, {"existed": False})})
    assert _cli(monkeypatch, ["state", "delete", "k"], http) == 4


def test_error_surfaced_exit_1(monkeypatch, capsys):
    http = _FakeHTTP({"/v1/kv/set": (413, {"error": "value too large"})})
    assert _cli(monkeypatch, ["state", "set", "k", "v"], http) == 1
    assert "value too large" in capsys.readouterr().err


def test_state_admin_still_routes(monkeypatch, capsys, tmp_path):
    # a non-job `state` action must still reach the offline admin dispatcher,
    # not the job CLI. `check` on a missing state section exits 1 via admin.
    cfg = tmp_path / "c.yaml"
    cfg.write_text("jobs:\n  - name: j\n    command: 'true'\n"
                   "    schedule: '* * * * *'\n")
    code = _cli(monkeypatch, ["state", "check", "-c", str(cfg)])
    assert code == 1
    assert "no `state:` section" in capsys.readouterr().out


# --------------------------------------------------------------------------
# cursor
# --------------------------------------------------------------------------


def test_cursor_advance_typed_int(monkeypatch, capsys):
    http = _FakeHTTP({"/v1/cursor/advance": (200, {"value": 100})})
    assert _cli(monkeypatch, ["cursor", "advance", "wm", "100"], http) == 0
    assert http.calls[0]["json"]["value"] == 100  # parsed as int, not "100"
    assert capsys.readouterr().out == "100\n"


def test_cursor_advance_iso_string(monkeypatch):
    http = _FakeHTTP({"/v1/cursor/advance": (200, {"value": "x"})})
    _cli(monkeypatch, ["cursor", "advance", "ts", "2026-07-01T00:00:00"], http)
    assert http.calls[0]["json"]["value"] == "2026-07-01T00:00:00"


def test_cursor_get_missing_exit_4(monkeypatch):
    http = _FakeHTTP({"/v1/cursor/get": (404, {})})
    assert _cli(monkeypatch, ["cursor", "get", "wm"], http) == 4


def test_cursor_no_action_exit_2(monkeypatch):
    assert _cli(monkeypatch, ["cursor"], _FakeHTTP()) == 2


# --------------------------------------------------------------------------
# idempotent (exit 0 fresh / 5 duplicate / 1 error)
# --------------------------------------------------------------------------


def test_idempotent_fresh_exit_0(monkeypatch):
    http = _FakeHTTP({"/v1/idempotency/claim": (200, {"fresh": True})})
    assert _cli(monkeypatch, ["idempotent", "order-1"], http) == 0


def test_idempotent_duplicate_exit_5(monkeypatch):
    # "a prior caller did the work" must not share exit 1 with transport/
    # store errors, or an outage masquerades as "already done" to a guard
    # script; the duplicate outcome has its own code.
    http = _FakeHTTP({"/v1/idempotency/claim": (200, {"fresh": False})})
    assert _cli(monkeypatch, ["idempotent", "order-1"], http) == 5


def test_idempotent_store_error_exit_1(monkeypatch, capsys):
    # a state endpoint failure is a real error (1), distinct from the
    # duplicate skip (5), so `if yacron2 idempotent K; then ...` cannot
    # silently drop the side effect for the length of a store outage.
    http = _FakeHTTP(
        {"/v1/idempotency/claim": (503, {"error": "store down"})}
    )
    assert _cli(monkeypatch, ["idempotent", "order-1"], http) == 1
    assert "store down" in capsys.readouterr().err


def test_idempotent_release(monkeypatch):
    http = _FakeHTTP({"/v1/idempotency/release": (200, {"released": True})})
    assert _cli(monkeypatch, ["idempotent", "order-1", "--release"], http) == 0
    assert http.calls[0]["path"] == "/v1/idempotency/release"


# --------------------------------------------------------------------------
# lock
# --------------------------------------------------------------------------


def test_lock_acquire_prints_token(monkeypatch, capsys):
    http = _FakeHTTP(
        {"/v1/lock/acquire": (200, {"acquired": True, "token": "h1"})}
    )
    assert _cli(monkeypatch, ["lock", "acquire", "L"], http) == 0
    assert capsys.readouterr().out == "h1\n"


def test_lock_acquire_denied_exit_3(monkeypatch):
    http = _FakeHTTP({"/v1/lock/acquire": (200, {"acquired": False})})
    assert _cli(monkeypatch, ["lock", "acquire", "L"], http) == 3


def test_lock_run_wraps_command(monkeypatch):
    http = _FakeHTTP(
        {
            "/v1/lock/acquire": (200, {"acquired": True, "token": "h1"}),
            "/v1/lock/release": (200, {"released": True}),
        }
    )
    argv = [
        "lock", "run", "L", "--",
        sys.executable, "-c", "import sys; sys.exit(7)",
    ]
    assert _cli(monkeypatch, argv, http) == 7
    # the lock was released after the wrapped command.
    assert any(c["path"] == "/v1/lock/release" for c in http.calls)


def test_lock_run_denied_does_not_run(monkeypatch):
    http = _FakeHTTP({"/v1/lock/acquire": (200, {"acquired": False})})
    argv = [
        "lock", "run", "L", "--",
        sys.executable, "-c", "import sys; sys.exit(0)",
    ]
    assert _cli(monkeypatch, argv, http) == 3
    assert not any(c["path"] == "/v1/lock/release" for c in http.calls)


def test_lock_acquire_wait_passes_client_deadline(monkeypatch):
    # a --wait long poll is ended by the server (blockSeconds); the client
    # deadline is that plus margin, so a wedged daemon still cannot hang
    # the job forever while a healthy long poll is never cut short.
    http = _FakeHTTP(
        {"/v1/lock/acquire": (200, {"acquired": True, "token": "h1"})}
    )
    argv = ["lock", "acquire", "L", "--wait", "--timeout", "300"]
    assert _cli(monkeypatch, argv, http) == 0
    assert http.calls[0]["timeout"] == 300.0 + jobcli._DEFAULT_TIMEOUT


def test_lock_acquire_non_wait_uses_default_deadline(monkeypatch):
    # without --wait no explicit deadline is passed; _http applies
    # _DEFAULT_TIMEOUT (every request must have one).
    http = _FakeHTTP(
        {"/v1/lock/acquire": (200, {"acquired": True, "token": "h1"})}
    )
    assert _cli(monkeypatch, ["lock", "acquire", "L"], http) == 0
    assert http.calls[0]["timeout"] is None


def test_lock_run_parses_flags_before_command(monkeypatch):
    # the documented form puts our flags BEFORE the `--`:
    #   lock run NAME --wait --timeout 300 -- cmd
    # argparse must PARSE --wait/--timeout/--ttl (nargs=REMAINDER swallowed
    # them into the command, leaving the lock non-blocking and mis-execing
    # "--wait") AND still run the wrapped command after `--`.
    http = _FakeHTTP(
        {
            "/v1/lock/acquire": (200, {"acquired": True, "token": "h1"}),
            "/v1/lock/release": (200, {"released": True}),
        }
    )
    argv = [
        "lock", "run", "L", "--wait", "--timeout", "300", "--ttl", "42", "--",
        sys.executable, "-c", "import sys; sys.exit(7)",
    ]
    assert _cli(monkeypatch, argv, http) == 7  # the wrapped command ran
    acquire = next(c for c in http.calls if c["path"] == "/v1/lock/acquire")
    assert acquire["json"]["wait"] is True
    assert acquire["json"]["blockSeconds"] == 300.0
    assert acquire["json"]["ttl"] == 42.0


# --------------------------------------------------------------------------
# artifact + secret
# --------------------------------------------------------------------------


def test_artifact_put_from_file(monkeypatch, capsys, tmp_path):
    src = tmp_path / "a.txt"
    src.write_bytes(b"payload")
    http = _FakeHTTP({"/v1/artifact/put": (200, {"sha256": "abc", "size": 7})})
    assert _cli(
        monkeypatch, ["artifact", "put", "a.txt", str(src)], http
    ) == 0
    assert http.calls[0]["data"] == b"payload"
    assert capsys.readouterr().out == "abc\n"


def test_artifact_get_to_file(monkeypatch, tmp_path):
    http = _FakeHTTP({"/v1/artifact/get": (200, b"the-bytes")})
    out = tmp_path / "out.bin"
    assert _cli(
        monkeypatch, ["artifact", "get", "a", "-o", str(out)], http
    ) == 0
    assert out.read_bytes() == b"the-bytes"


def test_artifact_list(monkeypatch, capsys):
    http = _FakeHTTP(
        {"/v1/artifact/list": (200, {"artifacts": [{"name": "x"}]})}
    )
    assert _cli(monkeypatch, ["artifact", "list"], http) == 0
    assert capsys.readouterr().out == "x\n"


def test_secret_get(monkeypatch, capsys):
    http = _FakeHTTP({"/v1/secret/get": (200, {"value": "sekret"})})
    assert _cli(monkeypatch, ["secret", "get", "TOKEN"], http) == 0
    assert capsys.readouterr().out == "sekret\n"


def test_secret_get_missing_exit_4(monkeypatch):
    http = _FakeHTTP({"/v1/secret/get": (404, {})})
    assert _cli(monkeypatch, ["secret", "get", "NOPE"], http) == 4


# --------------------------------------------------------------------------
# no environment (not inside a job)
# --------------------------------------------------------------------------


def test_no_env_errors(monkeypatch, capsys):
    monkeypatch.delenv("YACRON2_STATE_URL", raising=False)
    monkeypatch.delenv("YACRON2_STATE_TOKEN", raising=False)
    loop = asyncio.new_event_loop()
    try:
        monkeypatch.setattr(sys, "argv", ["yacron2", "state", "get", "k"])
        monkeypatch.setattr(sys, "exit", _exit)
        with pytest.raises(ExitError) as ei:
            yacron2.__main__.main_loop(loop)
        assert ei.value.args[0] == 1
        assert "not running inside a yacron2 job" in capsys.readouterr().err
    finally:
        loop.close()


def test_typed_value_parsing():
    assert jobcli._typed_value("100") == 100
    assert jobcli._typed_value("1.5") == 1.5
    assert jobcli._typed_value("2026-07-01") == "2026-07-01"


def test_http_opener_carries_no_proxies():
    # the CLI's opener is built with ProxyHandler({}): urllib drops the
    # no-op handler entirely, so no handler in the chain carries a proxy
    # map -- the default opener would honor http_proxy env vars and route
    # loopback state calls (bearer run token included) through an external
    # proxy.  The live-wire proof is test_cli_subprocess_ignores_proxy_env
    # in test_state_job_api.py.
    assert not any(
        getattr(h, "proxies", None) for h in jobcli._OPENER.handlers
    )
    assert not any(
        isinstance(h, urllib.request.ProxyHandler)
        for h in jobcli._OPENER.handlers
    )


# --------------------------------------------------------------------------
# clean errors instead of raw tracebacks (review findings)
# --------------------------------------------------------------------------


def test_state_set_bad_json_clean_error(monkeypatch, capsys):
    http = _FakeHTTP()
    code = _cli(monkeypatch, ["state", "set", "k", "not json", "--json"], http)
    assert code == 1
    assert "not valid JSON" in capsys.readouterr().err
    assert http.calls == []  # failed before any request


def test_lock_run_no_command_exit_1(monkeypatch, capsys):
    http = _FakeHTTP(
        {"/v1/lock/acquire": (200, {"acquired": True, "token": "h"})}
    )
    # `lock run L --` with nothing after -- leaves an empty command.
    assert _cli(monkeypatch, ["lock", "run", "L", "--"], http) == 1
    assert "needs a command" in capsys.readouterr().err
    # rejected before taking the lock.
    assert not any(c["path"] == "/v1/lock/acquire" for c in http.calls)


def test_lock_run_bad_command_clean_error(monkeypatch, capsys):
    http = _FakeHTTP(
        {
            "/v1/lock/acquire": (200, {"acquired": True, "token": "h"}),
            "/v1/lock/release": (200, {"released": True}),
        }
    )
    argv = ["lock", "run", "L", "--", "/no/such/command-xyz"]
    assert _cli(monkeypatch, argv, http) == 1
    assert "cannot run" in capsys.readouterr().err
    # the lock is still released despite the failure.
    assert any(c["path"] == "/v1/lock/release" for c in http.calls)


def test_artifact_put_bad_file_clean_error(monkeypatch, capsys, tmp_path):
    http = _FakeHTTP()
    missing = str(tmp_path / "does-not-exist.bin")
    assert _cli(monkeypatch, ["artifact", "put", "n", missing], http) == 1
    assert "cannot read" in capsys.readouterr().err
    assert http.calls == []  # failed before any request
