"""End-to-end encrypted push alerts: sealing, registry, service, API.

Covers cronstable.push (payload build/fit, sealed-box round-trip, the two
device stores, PushService), the PushReporter edge in cronstable.job, the
fail-closed config validation, the /push/devices and /whoami handlers,
scope enforcement on the new routes, the start_stop_push lifecycle, and
the Bonjour advertiser (with a fake zeroconf).

PyNaCl is a dev dependency (wheels on every CI cell), but the module
still importorskips so a bare `pip install -e .` checkout runs the rest
of the suite.
"""

import asyncio
import base64
import copy
import json
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest
from aiohttp import web

import cronstable.config as config
import cronstable.cron as cron_mod
import cronstable.discovery as discovery
import cronstable.push as push
from cronstable.config import ConfigError, parse_config_string
from cronstable.cron import WEB_TOKEN_REQUEST_KEY, Cron, _WebToken
from cronstable.fingerprint import canonical_job
from cronstable.job import (
    NotifyEventContext,
    PushReporter,
    RunningJob,
    report_config_enabled,
)

nacl_public = pytest.importorskip(
    "nacl.public", reason="pynacl (the push extra) is not installed"
)


# ---------------------------------------------------------------- helpers


def _device_keypair():
    private = nacl_public.PrivateKey.generate()
    public_b64 = base64.b64encode(bytes(private.public_key)).decode()
    return private, public_b64


def _open_sealed(private, ciphertext_b64: str) -> Dict[str, Any]:
    sealed = base64.b64decode(ciphertext_b64)
    plaintext = nacl_public.SealedBox(private).decrypt(sealed)
    return json.loads(plaintext.decode("utf-8"))


class _FakeJobCtx:
    """Quacks like RunningJob as far as build_payload reads one."""

    def __init__(self, name="backup", stderr="boom\nworse", **overrides):
        self.config = SimpleNamespace(name=name)
        self.template_vars = {
            "name": name,
            "success": False,
            "fail_reason": "exit code 1",
            "stdout": None,
            "stderr": stderr,
            "exit_code": 1,
            "host": "node-a",
            "schedule": "*/5 * * * *",
            "started_at": "2026-07-23T01:00:00+00:00",
            "run_id": "run-123",
        }
        self.template_vars.update(overrides)


class _RelayServer:
    """A local stand-in for the hosted push relay; records every POST."""

    def __init__(self, status: int = 200) -> None:
        self.status = status
        self.requests: List[Dict[str, Any]] = []
        self.url = ""
        self._runner: Optional[web.AppRunner] = None

    async def __aenter__(self) -> "_RelayServer":
        app = web.Application()
        app.router.add_post("/v1/notify", self._handle)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        port = self._runner.addresses[0][1]
        self.url = "http://127.0.0.1:{}/v1/notify".format(port)
        return self

    async def __aexit__(self, *exc) -> None:
        assert self._runner is not None
        await self._runner.cleanup()

    async def _handle(self, request: web.Request) -> web.Response:
        self.requests.append(await request.json())
        return web.json_response({"ok": True}, status=self.status)


def _service(store, relay_url="http://127.0.0.1:1/unused") -> push.PushService:
    return push.PushService(
        relay_url=relay_url, relay_timeout=5.0, store=store, host="node-a"
    )


async def _paired_service(tmp_path, relay_url, public_b64):
    service = _service(
        push.FileDeviceStore(str(tmp_path / "devices.json")), relay_url
    )
    record, created = await service.pair(
        {
            "name": "phone",
            "platform": "ios",
            "publicKey": public_b64,
            "pushToken": "tok-abcdef",
        },
        "authToken",
    )
    assert created
    return service, record


# ------------------------------------------------------- payload building


def test_build_payload_job_failure_carries_identity_and_tail():
    payload = push.build_payload(_FakeJobCtx(), False, True)
    assert payload["v"] == push.PUSH_PROTOCOL_VERSION
    assert payload["kind"] == "failure"
    assert payload["name"] == "backup"
    assert payload["host"] == "node-a"
    assert payload["run_id"] == "run-123"
    assert payload["fail_reason"] == "exit code 1"
    assert payload["log_tail"] == ["boom", "worse"]


def test_build_payload_success_and_no_tail_when_disabled():
    ctx = _FakeJobCtx(success=True, fail_reason=None, exit_code=0)
    payload = push.build_payload(ctx, True, False)
    assert payload["kind"] == "success"
    assert "log_tail" not in payload
    assert "fail_reason" not in payload


def test_build_payload_event_kind():
    ctx = NotifyEventContext(
        event="dag_failure",
        success=False,
        name="etl",
        subject="dag etl failed",
        message="task load failed",
        fields={"dag": "etl", "run_key": "sched-1"},
    )
    payload = push.build_payload(ctx, False, True)
    assert payload["kind"] == "event"
    assert payload["event"] == "dag_failure"
    assert payload["subject"] == "dag etl failed"
    assert payload["dag"] == "etl"
    assert payload["run_key"] == "sched-1"
    # events have no process, so never a log tail
    assert "log_tail" not in payload


def test_build_payload_sla_kind():
    yaml = """
jobs:
  - name: late
    command: "true"
    schedule: "* * * * *"
"""
    job_config = parse_config_string(yaml, "").jobs[0]
    from cronstable.job import SlaBreachContext

    ctx = SlaBreachContext(
        job_config,
        check="lateAfterSeconds",
        threshold_seconds=60.0,
        observed_seconds=190.0,
    )
    payload = push.build_payload(ctx, False, True)
    assert payload["kind"] == "sla"
    assert payload["sla_check"] == "lateAfterSeconds"
    assert payload["threshold_seconds"] == 60.0
    assert payload["observed_seconds"] == 190.0


def test_fit_payload_trims_oldest_tail_lines_first():
    ctx = _FakeJobCtx(
        stderr="\n".join("line-{:05d}".format(i) for i in range(5000))
    )
    payload = push.build_payload(ctx, False, True)
    data = push.fit_payload(payload)
    assert len(data) <= push.MAX_PLAINTEXT_BYTES
    fitted = json.loads(data.decode("utf-8"))
    tail = fitted["log_tail"]
    # newest lines survive; the identity core is intact
    assert tail[-1] == "line-04999"
    assert fitted["name"] == "backup"
    assert fitted["kind"] == "failure"


def test_fit_payload_truncates_long_text_without_tail():
    ctx = _FakeJobCtx(stderr=None, fail_reason="x" * 50_000)
    payload = push.build_payload(ctx, False, True)
    data = push.fit_payload(payload)
    assert len(data) <= push.MAX_PLAINTEXT_BYTES
    fitted = json.loads(data.decode("utf-8"))
    assert fitted["name"] == "backup"
    assert 64 <= len(fitted["fail_reason"]) < 50_000


def test_collapse_id_same_run_same_id_different_run_differs():
    a = push.build_payload(_FakeJobCtx(), False, True)
    b = push.build_payload(_FakeJobCtx(), False, True)
    c = push.build_payload(_FakeJobCtx(run_id="run-456"), False, True)
    assert push.collapse_id(a) == push.collapse_id(b)
    assert push.collapse_id(a) != push.collapse_id(c)
    assert len(push.collapse_id(a)) == 32


# ----------------------------------------------------------------- crypto


def test_seal_round_trip():
    private, public_b64 = _device_keypair()
    ciphertext = push.seal_to_device(public_b64, b'{"hello": "world"}')
    assert _open_sealed(private, ciphertext) == {"hello": "world"}


def test_seal_rejects_garbage_key():
    with pytest.raises(push.PushError):
        push.seal_to_device("not base64!!", b"x")


def test_validate_pairing_normalizes_and_rejects():
    _, public_b64 = _device_keypair()
    fields = push.validate_pairing(
        {
            "name": "  phone  ",
            "platform": "ios",
            "publicKey": public_b64,
            "pushToken": "tok",
        }
    )
    assert fields["name"] == "phone"
    with pytest.raises(push.PushError):
        push.validate_pairing("not a dict")
    with pytest.raises(push.PushError):
        push.validate_pairing(
            {"name": "p", "platform": "ios", "pushToken": "t",
             "publicKey": base64.b64encode(b"short").decode()}
        )
    with pytest.raises(push.PushError):
        push.validate_pairing(
            {"name": "x" * 65, "platform": "ios", "pushToken": "t",
             "publicKey": public_b64}
        )


# ---------------------------------------------------------- device stores


async def test_file_store_round_trip(tmp_path):
    store = push.FileDeviceStore(str(tmp_path / "devices.json"))
    assert await store.load() == []
    await store.upsert({"id": "d1", "name": "phone"})
    await store.upsert({"id": "d2", "name": "pad"})
    await store.upsert({"id": "d1", "name": "phone-renamed"})
    loaded = {d["id"]: d for d in await store.load()}
    assert loaded["d1"]["name"] == "phone-renamed"
    assert await store.remove("d2") is True
    assert await store.remove("d2") is False
    assert [d["id"] for d in await store.load()] == ["d1"]


async def test_file_store_corrupt_file_refuses_reads_and_writes(tmp_path):
    path = tmp_path / "devices.json"
    path.write_text("{not json", encoding="utf-8")
    store = push.FileDeviceStore(str(path))
    with pytest.raises(push.PushError):
        await store.load()
    with pytest.raises(push.PushError):
        await store.upsert({"id": "d1"})
    # the corrupt bytes are preserved for hand recovery
    assert path.read_text(encoding="utf-8") == "{not json"


class _FakeStateBackend:
    """The document slice of the state backend the registry uses."""

    def __init__(self) -> None:
        self.docs: Dict[str, Dict[str, Any]] = {}

    async def list_documents(self, namespace):
        assert namespace == push.PUSH_DOC_NAMESPACE
        return list(self.docs.values())

    async def mutate_document(self, namespace, key, transform):
        new, result = transform(self.docs.get(key))
        self.docs[key] = new
        return new, result

    async def delete_document(self, namespace, key):
        return self.docs.pop(key, None) is not None


async def test_state_store_round_trip_and_backend_loss():
    backend: List[Any] = [_FakeStateBackend()]
    store = push.StateDeviceStore(lambda: backend[0])
    await store.upsert({"id": "d1", "name": "phone"})
    assert [d["id"] for d in await store.load()] == ["d1"]
    assert await store.remove("d1") is True
    assert await store.remove("d1") is False
    backend[0] = None  # the backend went away (reload, outage)
    with pytest.raises(push.PushError):
        await store.load()
    with pytest.raises(push.PushError):
        await store.upsert({"id": "d2"})


# ------------------------------------------------------------ PushService


async def test_pair_revoke_and_repair_keeps_identity(tmp_path):
    _, public_b64 = _device_keypair()
    service, record = await _paired_service(tmp_path, "http://x/", public_b64)
    assert record["id"] and record["createdAt"]
    assert record["createdBy"] == "authToken"
    # same public key pairs again: same id/createdAt, fresh token+name
    repaired, created = await service.pair(
        {
            "name": "phone-2",
            "platform": "ios",
            "publicKey": public_b64,
            "pushToken": "tok-rotated",
        },
        "other",
    )
    assert created is False
    assert repaired["id"] == record["id"]
    assert repaired["createdAt"] == record["createdAt"]
    assert repaired["pushToken"] == "tok-rotated"
    listing = service.devices_payload()
    assert len(listing) == 1
    assert listing[0]["pushToken"].endswith("otated")
    assert listing[0]["pushToken"] != "tok-rotated"  # redacted
    assert await service.revoke(record["id"]) is True
    assert service.devices_payload() == []


async def test_send_report_seals_to_each_device_and_posts_relay(tmp_path):
    private, public_b64 = _device_keypair()
    async with _RelayServer() as relay:
        service, record = await _paired_service(
            tmp_path, relay.url, public_b64
        )
        await service.send_report(
            _FakeJobCtx(),
            False,
            {"enabled": True, "priority": "passive", "includeLogTail": True},
        )
        assert len(relay.requests) == 1
        body = relay.requests[0]
        assert body["v"] == push.PUSH_PROTOCOL_VERSION
        assert body["device"] == "tok-abcdef"
        assert body["priority"] == "passive"
        assert body["event"] is False
        assert len(body["collapseId"]) == 32
        opened = _open_sealed(private, body["ciphertext"])
        assert opened["name"] == "backup"
        assert opened["kind"] == "failure"
        assert opened["log_tail"] == ["boom", "worse"]
        # the relay never saw plaintext
        flat = json.dumps(body)
        assert "backup" not in flat and "boom" not in flat


async def test_send_report_with_no_devices_logs_and_returns(tmp_path, caplog):
    service = _service(push.FileDeviceStore(str(tmp_path / "d.json")))
    await service.send_report(_FakeJobCtx(), False, {"enabled": True})
    assert any("no device is paired" in r.message for r in caplog.records)


async def test_send_test_reports_relay_failure(tmp_path):
    private, public_b64 = _device_keypair()
    async with _RelayServer(status=429) as relay:
        service, record = await _paired_service(
            tmp_path, relay.url, public_b64
        )
        outcome = await service.send_test(record)
        assert outcome["status"] == 429
        assert "429" in outcome["error"]
        opened = _open_sealed(private, relay.requests[0]["ciphertext"])
        assert opened["kind"] == "test"


async def test_send_report_survives_unreachable_relay(tmp_path):
    _, public_b64 = _device_keypair()
    service, _ = await _paired_service(
        # nothing listens on port 9; must log, never raise
        tmp_path, "http://127.0.0.1:9/v1/notify", public_b64
    )
    await service.send_report(_FakeJobCtx(), False, {"enabled": True})


# ------------------------------------------------------------ PushReporter


class _StubService:
    def __init__(self) -> None:
        self.calls: List[Any] = []

    async def send_report(self, ctx, success, push_config):
        self.calls.append((ctx, success, push_config))


@pytest.fixture
def stub_service():
    stub = _StubService()
    push.set_service(stub)
    yield stub
    push.set_service(None)


async def test_push_reporter_disabled_never_touches_service(stub_service):
    reporter = PushReporter()
    await reporter.report(False, _FakeJobCtx(), {"push": {"enabled": False}})
    await reporter.report(False, _FakeJobCtx(), {})
    assert stub_service.calls == []


async def test_push_reporter_enabled_hands_off(stub_service):
    reporter = PushReporter()
    ctx = _FakeJobCtx()
    await reporter.report(
        False, ctx, {"push": {"enabled": True, "priority": "passive"}}
    )
    assert len(stub_service.calls) == 1
    handed_ctx, success, push_config = stub_service.calls[0]
    assert handed_ctx is ctx and success is False
    assert push_config["priority"] == "passive"


async def test_push_reporter_without_service_logs_not_raises(caplog):
    push.set_service(None)
    reporter = PushReporter()
    await reporter.report(False, _FakeJobCtx(), {"push": {"enabled": True}})
    assert any("alert dropped" in r.message for r in caplog.records)


def test_push_is_a_registered_reporter_and_gates_fanout():
    assert any(
        isinstance(r, PushReporter) for r in RunningJob.REPORTERS
    )
    defaults = parse_config_string(
        "jobs:\n  - name: j\n    command: \"true\"\n"
        "    schedule: \"* * * * *\"\n",
        "",
    ).jobs[0]
    report = defaults.onFailure["report"]
    assert report["push"]["enabled"] is False
    assert report_config_enabled(report) is False
    # deepcopy before mutating: mergedicts shares untouched subtrees with
    # the module-level DEFAULT_CONFIG, so an in-place edit here would
    # poison every job parsed later in this process.
    enabled = copy.deepcopy(report)
    enabled["push"]["enabled"] = True
    assert report_config_enabled(enabled) is True


def test_canonical_job_omits_all_default_push_block():
    plain = """
jobs:
  - name: plain
    command: "true"
    schedule: "* * * * *"
"""
    job = parse_config_string(plain, "").jobs[0]
    assert "push" not in canonical_job(job)["onFailure"]["report"]
    # a job that actually enables push gets it in identity (set at parse
    # time; never mutate the parsed dicts, they share subtrees with the
    # module-level defaults)
    pushy = """
jobs:
  - name: plain
    command: "true"
    schedule: "* * * * *"
    onFailure:
      report:
        push:
          enabled: true
"""
    job = parse_config_string(pushy, "").jobs[0]
    assert canonical_job(job)["onFailure"]["report"]["push"]["enabled"]


# ----------------------------------------------------- config validation


def _parse_validated(yaml: str):
    """Parse plus the cross-section pass the daemon's file entry runs.

    The push/mcp/state fail-closed checks live in
    ``_validate_cross_sections`` (sections may span config-dir files),
    which ``parse_config_string`` alone deliberately does not run.
    """
    cfg = parse_config_string(yaml, "")
    config._validate_cross_sections(cfg)
    return cfg


_PUSH_STATE_YAML = """
push:
  relay:
    url: https://relay.example.net/v1/notify
state:
  path: {path}
jobs:
  - name: j
    command: "true"
    schedule: "* * * * *"
    onFailure:
      report:
        push:
          enabled: true
"""


def test_push_config_parses_with_state(tmp_path):
    cfg = _parse_validated(
        _PUSH_STATE_YAML.format(path=(tmp_path / "state").as_posix())
    )
    assert cfg.push_config == {
        "relay": {
            "url": "https://relay.example.net/v1/notify",
            "timeout": 10.0,
        },
        "devicesFile": None,
    }


def test_push_enabled_without_section_is_refused():
    yaml = """
jobs:
  - name: j
    command: "true"
    schedule: "* * * * *"
    onFailure:
      report:
        push:
          enabled: true
"""
    with pytest.raises(ConfigError) as exc:
        _parse_validated(yaml)
    assert "job j" in str(exc.value)
    assert "push" in str(exc.value)


def test_notify_push_without_section_is_refused():
    yaml = """
notify:
  report:
    push:
      enabled: true
jobs:
  - name: j
    command: "true"
    schedule: "* * * * *"
"""
    with pytest.raises(ConfigError) as exc:
        _parse_validated(yaml)
    assert "notify" in str(exc.value)


def test_push_needs_state_or_devices_file():
    yaml = """
push:
  relay:
    url: https://relay.example.net/v1/notify
"""
    with pytest.raises(ConfigError) as exc:
        _parse_validated(yaml)
    assert "devicesFile" in str(exc.value)
    # devicesFile alone satisfies the storage requirement
    _parse_validated(
        yaml + "  devicesFile: /tmp/devices.json\n")


def test_push_without_pynacl_is_refused(monkeypatch):
    monkeypatch.setattr(push, "HAVE_PYNACL", False)
    yaml = """
push:
  relay:
    url: https://relay.example.net/v1/notify
  devicesFile: /tmp/devices.json
"""
    with pytest.raises(ConfigError) as exc:
        _parse_validated(yaml)
    assert "PyNaCl" in str(exc.value)
    assert "fail" in str(exc.value)


@pytest.mark.parametrize(
    "url", ["ftp://x/y", "relay.example.net", "unix:///tmp/x", ""]
)
def test_push_relay_url_must_be_http(url):
    yaml = (
        "push:\n  relay:\n    url: \"{}\"\n"
        "  devicesFile: /tmp/d.json\n".format(url)
    )
    with pytest.raises(ConfigError):
        _parse_validated(yaml)


def test_push_relay_timeout_must_be_positive():
    yaml = """
push:
  relay:
    url: https://relay.example.net/v1/notify
    timeout: 0
  devicesFile: /tmp/d.json
"""
    with pytest.raises(ConfigError) as exc:
        _parse_validated(yaml)
    assert "timeout" in str(exc.value)


# ------------------------------------------------------- bonjour config


_BONJOUR_YAML = """
web:
  listen:
    - {listen}
  bonjour: true
"""


def test_bonjour_without_zeroconf_is_refused(monkeypatch):
    monkeypatch.setattr(discovery, "HAVE_ZEROCONF", False)
    with pytest.raises(ConfigError) as exc:
        parse_config_string(
            _BONJOUR_YAML.format(listen="http://127.0.0.1:8080"), ""
        )
    assert "zeroconf" in str(exc.value)


def test_bonjour_with_zeroconf_and_tcp_listen_parses(monkeypatch):
    monkeypatch.setattr(discovery, "HAVE_ZEROCONF", True)
    cfg = parse_config_string(
        _BONJOUR_YAML.format(listen="http://127.0.0.1:8080"), ""
    )
    assert config.resolve_bonjour_config(cfg.web_config) == {"name": None}


def test_bonjour_unix_only_listen_is_refused(monkeypatch):
    monkeypatch.setattr(discovery, "HAVE_ZEROCONF", True)
    with pytest.raises(ConfigError) as exc:
        parse_config_string(
            _BONJOUR_YAML.format(listen="unix:///tmp/web.sock"), ""
        )
    assert "unix" in str(exc.value)


def test_bonjour_off_forms_need_no_library(monkeypatch):
    monkeypatch.setattr(discovery, "HAVE_ZEROCONF", False)
    cfg = parse_config_string(
        "web:\n  listen:\n    - http://127.0.0.1:8080\n  bonjour: false\n",
        "",
    )
    assert config.resolve_bonjour_config(cfg.web_config) is None
    cfg = parse_config_string(
        "web:\n  listen:\n    - http://127.0.0.1:8080\n"
        "  bonjour:\n    enabled: false\n    name: attic\n",
        "",
    )
    assert config.resolve_bonjour_config(cfg.web_config) is None


# --------------------------------------------------------- web handlers


class _Req:
    """The slice of aiohttp's Request the push/whoami handlers read."""

    def __init__(self, match=None, body=None, token=None):
        self.match_info = match or {}
        self._body = body
        self._store: Dict[str, Any] = {}
        if token is not None:
            self._store[WEB_TOKEN_REQUEST_KEY] = token

    def get(self, key, default=None):
        return self._store.get(key, default)

    async def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


_SEED_JOB = """
jobs:
  - name: seed
    command: "true"
    schedule: "* * * * *"
    enabled: false
"""


def _cron() -> Cron:
    cron = Cron(None, config_yaml=_SEED_JOB)
    cron.web_config = {}
    return cron


def _pair_body(public_b64: str) -> Dict[str, str]:
    return {
        "name": "phone",
        "platform": "ios",
        "publicKey": public_b64,
        "pushToken": "tok-abcdef",
    }


async def test_push_routes_404_without_push_section():
    cron = _cron()
    for call in (
        cron._web_push_devices(_Req()),
        cron._web_push_pair(_Req(body={})),
        cron._web_push_revoke(_Req(match={"id": "x"})),
        cron._web_push_test(_Req(match={"id": "x"})),
    ):
        with pytest.raises(web.HTTPNotFound):
            await call


async def test_pair_list_revoke_via_handlers(tmp_path):
    _, public_b64 = _device_keypair()
    cron = _cron()
    cron._push_service = _service(
        push.FileDeviceStore(str(tmp_path / "d.json"))
    )
    token = _WebToken(b"t", frozenset({"view", "control"}), "ops-phone")
    resp = await cron._web_push_pair(
        _Req(body=_pair_body(public_b64), token=token)
    )
    assert resp.status == 201
    created = json.loads(resp.body)
    assert created["created"] is True
    assert created["device"]["createdBy"] == "ops-phone"
    device_id = created["device"]["id"]

    # re-pair: 200, same id
    resp = await cron._web_push_pair(_Req(body=_pair_body(public_b64)))
    assert resp.status == 200
    assert json.loads(resp.body)["device"]["id"] == device_id

    resp = await cron._web_push_devices(_Req())
    devices = json.loads(resp.body)["devices"]
    assert [d["id"] for d in devices] == [device_id]

    resp = await cron._web_push_revoke(_Req(match={"id": device_id}))
    assert json.loads(resp.body)["revoked"] == device_id
    with pytest.raises(web.HTTPNotFound):
        await cron._web_push_revoke(_Req(match={"id": device_id}))


async def test_pair_rejects_bad_bodies(tmp_path):
    cron = _cron()
    cron._push_service = _service(
        push.FileDeviceStore(str(tmp_path / "d.json"))
    )
    with pytest.raises(web.HTTPBadRequest):
        await cron._web_push_pair(_Req(body=ValueError("bad json")))
    with pytest.raises(web.HTTPBadRequest):
        await cron._web_push_pair(_Req(body={"name": "x"}))


async def test_push_test_endpoint_round_trip(tmp_path):
    _, public_b64 = _device_keypair()
    async with _RelayServer() as relay:
        cron = _cron()
        service, record = await _paired_service(
            tmp_path, relay.url, public_b64
        )
        cron._push_service = service
        resp = await cron._web_push_test(_Req(match={"id": record["id"]}))
        assert resp.status == 200
        assert json.loads(resp.body)["status"] == 200
        with pytest.raises(web.HTTPNotFound):
            await cron._web_push_test(_Req(match={"id": "nope"}))


async def test_whoami_with_and_without_token():
    cron = _cron()
    token = _WebToken(b"t", frozenset({"view"}), "wallboard")
    body = json.loads((await cron._web_whoami(_Req(token=token))).body)
    assert body == {
        "authenticated": True,
        "label": "wallboard",
        "scopes": ["view"],
        "allScopes": False,
    }
    body = json.loads((await cron._web_whoami(_Req())).body)
    assert body["authenticated"] is False
    assert body["allScopes"] is True
    assert body["scopes"] == sorted(["view", "control", "approve"])


async def test_all_scopes_token_reports_all_scopes():
    cron = _cron()
    token = _WebToken(
        b"t", frozenset({"view", "control", "approve"}), "authToken"
    )
    body = json.loads((await cron._web_whoami(_Req(token=token))).body)
    assert body["allScopes"] is True


# --------------------------------------------------- scope enforcement


class _ScopeReq:
    def __init__(self, path, method, canonical, headers):
        self.path = path
        self.method = method
        self.headers = headers
        self.query: Dict[str, str] = {}
        resource = SimpleNamespace(canonical=canonical)
        route = SimpleNamespace(resource=resource)
        self.match_info = SimpleNamespace(route=route)
        self._store: Dict[str, Any] = {}

    def __setitem__(self, key, value):
        self._store[key] = value

    def get(self, key, default=None):
        return self._store.get(key, default)


async def _run_mw(middleware, request):
    async def handler(req):
        return "ok"

    return await middleware(request, handler)


def _mw(scopes):
    table = [
        _WebToken(
            b"tok", cron_mod._effective_web_scopes(scopes), "phone"
        )
    ]
    return Cron._make_auth_middleware(table)


async def test_view_token_lists_devices_but_cannot_pair():
    headers = {"Authorization": "Bearer tok"}
    mw = _mw(["view"])
    ok = await _run_mw(
        mw, _ScopeReq("/push/devices", "GET", "/push/devices", headers)
    )
    assert ok == "ok"
    with pytest.raises(web.HTTPForbidden):
        await _run_mw(
            mw, _ScopeReq("/push/devices", "POST", "/push/devices", headers)
        )
    with pytest.raises(web.HTTPForbidden):
        await _run_mw(
            mw,
            _ScopeReq(
                "/push/devices/x",
                "DELETE",
                "/push/devices/{id}",
                headers,
            ),
        )


async def test_control_token_can_pair_and_middleware_files_identity():
    headers = {"Authorization": "Bearer tok"}
    mw = _mw(["control"])
    req = _ScopeReq("/push/devices", "POST", "/push/devices", headers)
    assert await _run_mw(mw, req) == "ok"
    filed = req.get(WEB_TOKEN_REQUEST_KEY)
    assert filed is not None and filed.label == "phone"


# ------------------------------------------------------ lifecycle edges


async def test_start_stop_push_builds_and_tears_down(tmp_path):
    cron = _cron()
    push_config = {
        "relay": {"url": "http://127.0.0.1:1/unused", "timeout": 5.0},
        "devicesFile": str(tmp_path / "devices.json"),
    }
    await cron.start_stop_push(push_config)
    assert cron._push_service is not None
    assert push.get_service() is cron._push_service
    assert cron._push_service.store.kind == "file"
    first = cron._push_service
    # unchanged config: same service instance
    await cron.start_stop_push(dict(push_config))
    assert cron._push_service is first
    # section removed: service gone, module seam cleared
    await cron.start_stop_push(None)
    assert cron._push_service is None
    assert push.get_service() is None


async def test_start_stop_push_state_store_tracks_backend(tmp_path):
    cron = _cron()
    await cron.start_stop_push(
        {"relay": {"url": "http://x/", "timeout": 5.0}, "devicesFile": None}
    )
    service = cron._push_service
    assert service is not None and service.store.kind == "state"
    # no backend yet: interactive paths fail loudly, not silently
    with pytest.raises(push.PushError):
        await service.store.load()
    cron.state_backend = _FakeStateBackend()
    await service.store.upsert({"id": "d1", "name": "phone"})
    assert [d["id"] for d in await service.store.load()] == ["d1"]
    await cron.start_stop_push(None)


# ----------------------------------------------------- bonjour runtime


class _FakeAsyncZeroconf:
    instances: List["_FakeAsyncZeroconf"] = []

    def __init__(self) -> None:
        self.registered: List[Any] = []
        self.unregistered: List[Any] = []
        self.closed = False
        _FakeAsyncZeroconf.instances.append(self)

    async def async_register_service(self, info):
        self.registered.append(info)

    async def async_unregister_service(self, info):
        self.unregistered.append(info)

    async def async_close(self):
        self.closed = True


class _FakeServiceInfo:
    def __init__(self, type_, name, **kwargs):
        self.type = type_
        self.name = name
        self.kwargs = kwargs


@pytest.fixture
def fake_zeroconf(monkeypatch):
    _FakeAsyncZeroconf.instances = []
    monkeypatch.setattr(discovery, "HAVE_ZEROCONF", True)
    monkeypatch.setattr(
        discovery, "AsyncZeroconf", _FakeAsyncZeroconf, raising=False
    )
    monkeypatch.setattr(
        discovery, "ServiceInfo", _FakeServiceInfo, raising=False
    )
    monkeypatch.setattr(discovery, "primary_address", lambda: "192.0.2.7")
    return _FakeAsyncZeroconf


async def test_bonjour_register_converge_and_stop(fake_zeroconf):
    advertiser = discovery.BonjourAdvertiser()
    advert = {
        "name": "attic.local",
        "port": 8080,
        "properties": {"v": "1.2.30", "scheme": "http"},
    }
    await advertiser.start_stop(advert)
    assert advertiser.active
    (zc,) = fake_zeroconf.instances
    (info,) = zc.registered
    assert info.name == "attic-local._cronstable._tcp.local."
    assert info.kwargs["port"] == 8080
    assert info.kwargs["properties"] == {"v": "1.2.30", "scheme": "http"}
    # unchanged advert: no re-registration
    await advertiser.start_stop(dict(advert))
    assert len(fake_zeroconf.instances) == 1
    # changed advert: old one is torn down, a fresh one registered
    await advertiser.start_stop({**advert, "port": 9090})
    assert zc.closed and len(zc.unregistered) == 1
    assert len(fake_zeroconf.instances) == 2
    await advertiser.stop()
    assert not advertiser.active
    assert fake_zeroconf.instances[-1].closed


async def test_bonjour_register_failure_logs_and_stays_off(
    fake_zeroconf, monkeypatch, caplog
):
    async def boom(self, info):
        raise OSError("multicast is off")

    monkeypatch.setattr(
        _FakeAsyncZeroconf, "async_register_service", boom
    )
    advertiser = discovery.BonjourAdvertiser()
    await advertiser.start_stop({"name": "n", "port": 1, "properties": {}})
    assert not advertiser.active
    assert any("failed to register" in r.message for r in caplog.records)


async def test_notify_fanout_reaches_push(stub_service):
    from cronstable.job import report_event

    ctx = NotifyEventContext(
        event="quorum_loss",
        success=False,
        name="cluster",
        subject="quorum lost",
        message="2 of 5 peers visible",
    )
    report_config = config._build_notify_config(
        {"report": {"push": {"enabled": True}}}
    )["report"]
    await report_event(ctx, report_config)
    assert len(stub_service.calls) == 1
    handed_ctx, success, push_config = stub_service.calls[0]
    assert handed_ctx is ctx
    assert push_config["enabled"] is True
