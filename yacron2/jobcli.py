"""The job-facing state CLI: `yacron2 state|cursor|lock|artifact|...`.

These are the commands a job command line reaches for -- durable KV, an ETL
cursor, a distributed lock, the artifact store, idempotency keys, run-scoped
secrets.  Each is a thin client of the loopback endpoint the daemon injected
into the job's environment (:mod:`yacron2.jobapi`): it reads the injected
``YACRON2_STATE_URL`` / ``YACRON2_STATE_TOKEN`` and speaks HTTP over stdlib
``urllib`` (no aiohttp, no event loop, so the command starts instantly).

They coexist with the ``yacron2 state`` *admin* commands (backup / restore /
gc / check ...): the admin actions operate on the store file tree offline via
``-c``, while these job-facing actions (get / set / delete / keys, and the
other verbs) act through the running daemon.  ``yacron2.__main__`` routes by
action name, so ``yacron2 state check`` and ``yacron2 state get KEY`` reach the
right handler.

The default *scope* every KV / cursor / artifact / lock call lands in is the
calling job's own name (the daemon fills it from the run's identity), so one
job cannot read another's keys by accident; ``--global`` (or ``--scope NAME``)
opts into a shared namespace for deliberate cross-job coordination.
"""

import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional, Tuple

# The env vars the daemon injects (see yacron2.jobapi); the job CLI is the
# consumer.  Hardcoded here rather than imported so the CLI never pulls aiohttp
# into its import graph -- these three names are a stable wire contract.
ENV_URL = "YACRON2_STATE_URL"
ENV_TOKEN = "YACRON2_STATE_TOKEN"

# Exit codes: 0 success, 1 a real error, 2 usage, 3 lock not acquired, 4 the
# looked-up thing does not exist (so a script can branch on "missing").
EXIT_ERROR = 1
EXIT_NOT_ACQUIRED = 3
EXIT_NOT_FOUND = 4

# Every job-facing action name, so __main__ can tell a `state get` (this
# module) from a `state backup` (yacron2.state_admin) without guessing.
STATE_JOB_ACTIONS = frozenset({"get", "set", "delete", "keys"})


class _CliError(Exception):
    """A user-facing failure: printed to stderr, exits non-zero."""


# --------------------------------------------------------------------------
# HTTP transport (stdlib only, monkeypatchable in tests via _http)
# --------------------------------------------------------------------------


def _endpoint() -> Tuple[str, str]:
    url = os.environ.get(ENV_URL)
    token = os.environ.get(ENV_TOKEN)
    if not url or not token:
        raise _CliError(
            "not running inside a yacron2 job: {} is not set (these commands "
            "reach the daemon's loopback state endpoint, which is injected "
            "into a job's environment; is a `state` section with jobApi "
            "enabled configured?)".format(ENV_URL)
        )
    return url, token


def _http(
    method: str,
    path: str,
    *,
    query: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    data: Optional[bytes] = None,
) -> Tuple[int, Dict[str, str], bytes]:
    """One request to the endpoint; return ``(status, headers, body)``.

    The single seam the whole CLI goes through, so a test monkeypatches this
    to drive every verb without a live server.
    """
    url, token = _endpoint()
    full = url.rstrip("/") + path
    if query:
        pairs = {k: v for k, v in query.items() if v is not None}
        if pairs:
            full += "?" + urllib.parse.urlencode(pairs)
    headers = {"Authorization": "Bearer " + token}
    body = None
    if json_body is not None:
        body = json.dumps(json_body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    elif data is not None:
        body = data
        headers["Content-Type"] = "application/octet-stream"
    req = urllib.request.Request(
        full, data=body, method=method, headers=headers
    )
    try:
        with urllib.request.urlopen(req) as resp:  # noqa: S310 - loopback only
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as ex:
        return ex.code, dict(ex.headers or {}), ex.read()
    except urllib.error.URLError as ex:
        raise _CliError(
            "cannot reach the yacron2 state endpoint at {}: {}".format(
                url, ex.reason
            )
        ) from ex


def _json(
    method: str,
    path: str,
    *,
    query: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
) -> Tuple[int, Dict[str, Any]]:
    status, _headers, body = _http(
        method, path, query=query, json_body=json_body
    )
    try:
        parsed = json.loads(body) if body else {}
    except ValueError:
        parsed = {}
    if not isinstance(parsed, dict):
        parsed = {"value": parsed}
    return status, parsed


def _ok(status: int, data: Dict[str, Any]) -> Dict[str, Any]:
    """Return the body, or raise the endpoint's error for a 4xx/5xx."""
    if status >= 400:
        raise _CliError(
            data.get("error")
            or "the state endpoint returned HTTP {}".format(status)
        )
    return data


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------


def _scope_of(args: argparse.Namespace) -> Optional[str]:
    """The scope to send, or ``None`` to let the daemon default to the job."""
    if getattr(args, "use_global", False):
        return "global"
    return getattr(args, "scope", None)


def _emit(value: Any) -> None:
    """Print a value: strings verbatim, everything else as compact JSON."""
    if isinstance(value, str):
        sys.stdout.write(value + "\n")
    else:
        sys.stdout.write(json.dumps(value) + "\n")


def _typed_value(raw: str) -> Any:
    """Parse a cursor value: int, then float, else the raw string.

    So a numeric watermark compares numerically (``9 < 10``) and an ISO
    timestamp compares as the string it is (``2026-06 < 2026-07``).
    """
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        return raw


# --------------------------------------------------------------------------
# verb handlers
# --------------------------------------------------------------------------


def _cmd_state(args: argparse.Namespace) -> int:
    scope = _scope_of(args)
    action = args.state_command
    if action == "get":
        status, data = _json(
            "GET", "/v1/kv/get", query={"scope": scope, "key": args.key}
        )
        if status == 404:
            print("key not found: {}".format(args.key), file=sys.stderr)
            return EXIT_NOT_FOUND
        _emit(_ok(status, data).get("value"))
        return 0
    if action == "set":
        if args.json:
            try:
                value = json.loads(args.value)
            except ValueError as ex:
                raise _CliError(
                    "--json was given but VALUE is not valid JSON: {}".format(
                        ex
                    )
                ) from ex
        else:
            value = args.value
        status, data = _json(
            "POST",
            "/v1/kv/set",
            json_body={"scope": scope, "key": args.key, "value": value},
        )
        _ok(status, data)
        return 0
    if action == "delete":
        status, data = _json(
            "POST",
            "/v1/kv/delete",
            json_body={"scope": scope, "key": args.key},
        )
        return 0 if _ok(status, data).get("existed") else EXIT_NOT_FOUND
    if action == "keys":
        status, data = _json("GET", "/v1/kv/list", query={"scope": scope})
        for entry in _ok(status, data).get("keys", []):
            sys.stdout.write(str(entry.get("key")) + "\n")
        return 0
    raise _CliError("unknown state action {!r}".format(action))


def _cmd_cursor(args: argparse.Namespace) -> int:
    scope = _scope_of(args)
    if args.cursor_command == "get":
        status, data = _json(
            "GET", "/v1/cursor/get", query={"scope": scope, "name": args.name}
        )
        if status == 404:
            print("cursor not set: {}".format(args.name), file=sys.stderr)
            return EXIT_NOT_FOUND
        _emit(_ok(status, data).get("value"))
        return 0
    if args.cursor_command == "advance":
        status, data = _json(
            "POST",
            "/v1/cursor/advance",
            json_body={
                "scope": scope,
                "name": args.name,
                "value": _typed_value(args.value),
                "force": args.force,
            },
        )
        _emit(_ok(status, data).get("value"))
        return 0
    raise _CliError("unknown cursor action {!r}".format(args.cursor_command))


def _cmd_idempotent(args: argparse.Namespace) -> int:
    scope = _scope_of(args)
    if args.release:
        status, data = _json(
            "POST",
            "/v1/idempotency/release",
            json_body={"scope": scope, "key": args.key},
        )
        _ok(status, data)
        return 0
    status, data = _json(
        "POST",
        "/v1/idempotency/claim",
        json_body={"scope": scope, "key": args.key, "ttl": args.ttl},
    )
    # exit 0 when this caller won the claim (fresh: do the work), 1 when a
    # prior caller already claimed it (a duplicate: skip). Made for a shell
    # guard: `yacron2 idempotent "$KEY" && do-the-side-effect`.
    return 0 if _ok(status, data).get("fresh") else EXIT_ERROR


def _cmd_secret(args: argparse.Namespace) -> int:
    if args.secret_command == "get":
        status, data = _json(
            "GET", "/v1/secret/get", query={"name": args.name}
        )
        if status == 404:
            print("secret not staged: {}".format(args.name), file=sys.stderr)
            return EXIT_NOT_FOUND
        _emit(_ok(status, data).get("value"))
        return 0
    if args.secret_command == "list":
        status, data = _json("GET", "/v1/secret/list")
        for name in _ok(status, data).get("names", []):
            sys.stdout.write(str(name) + "\n")
        return 0
    raise _CliError("unknown secret action {!r}".format(args.secret_command))


def _cmd_artifact(args: argparse.Namespace) -> int:
    scope = _scope_of(args)
    if args.artifact_command == "put":
        if args.file in (None, "-"):
            payload = sys.stdin.buffer.read()
        else:
            try:
                with open(args.file, "rb") as fobj:
                    payload = fobj.read()
            except OSError as ex:
                raise _CliError(
                    "cannot read {}: {}".format(args.file, ex)
                ) from ex
        status, _headers, body = _http(
            "POST",
            "/v1/artifact/put",
            query={"scope": scope, "name": args.name},
            data=payload,
        )
        data = _ok(status, json.loads(body) if body else {})
        sys.stdout.write(str(data.get("sha256", "")) + "\n")
        return 0
    if args.artifact_command == "get":
        status, _headers, body = _http(
            "GET",
            "/v1/artifact/get",
            query={"scope": scope, "name": args.name},
        )
        if status == 404:
            print("artifact not found: {}".format(args.name), file=sys.stderr)
            return EXIT_NOT_FOUND
        if status >= 400:
            _ok(status, json.loads(body) if body else {})
        if args.output in (None, "-"):
            sys.stdout.buffer.write(body)
        else:
            try:
                with open(args.output, "wb") as fobj:
                    fobj.write(body)
            except OSError as ex:
                raise _CliError(
                    "cannot write {}: {}".format(args.output, ex)
                ) from ex
        return 0
    if args.artifact_command == "list":
        status, data = _json(
            "GET", "/v1/artifact/list", query={"scope": scope}
        )
        for entry in _ok(status, data).get("artifacts", []):
            sys.stdout.write(str(entry.get("name")) + "\n")
        return 0
    raise _CliError(
        "unknown artifact action {!r}".format(args.artifact_command)
    )


def _lock_acquire(args: argparse.Namespace) -> Tuple[bool, Optional[str]]:
    scope = _scope_of(args)
    status, data = _json(
        "POST",
        "/v1/lock/acquire",
        json_body={
            "scope": scope,
            "name": args.name,
            "permits": args.permits,
            "wait": args.wait,
            "blockSeconds": args.timeout,
            "ttl": args.ttl,
        },
    )
    data = _ok(status, data)
    return bool(data.get("acquired")), data.get("token")


def _lock_release(token: str) -> None:
    status, data = _json(
        "POST", "/v1/lock/release", json_body={"token": token}
    )
    _ok(status, data)


def _cmd_lock(args: argparse.Namespace) -> int:
    if args.lock_command == "acquire":
        acquired, token = _lock_acquire(args)
        if not acquired:
            print("lock not acquired: {}".format(args.name), file=sys.stderr)
            return EXIT_NOT_ACQUIRED
        # print the hold token so a later `yacron2 lock release TOKEN` (or a
        # wrapper script) can free it.
        sys.stdout.write(str(token) + "\n")
        return 0
    if args.lock_command == "release":
        _lock_release(args.token)
        return 0
    if args.lock_command == "run":
        # reject a missing command before taking the lock, so a usage mistake
        # does not needlessly acquire (and immediately release) it.
        if not args.run_command:
            raise _CliError(
                "lock run needs a command to run (put it after `--`)"
            )
        acquired, token = _lock_acquire(args)
        if not acquired:
            print("lock not acquired: {}".format(args.name), file=sys.stderr)
            return EXIT_NOT_ACQUIRED
        try:
            completed = subprocess.run(args.run_command)  # noqa: S603
            return completed.returncode
        except OSError as ex:
            # a bad argv (command not found, not executable): report it
            # cleanly rather than leaking the raw OSError traceback. The
            # finally below still releases the lock.
            raise _CliError(
                "cannot run {!r}: {}".format(args.run_command[0], ex)
            ) from ex
        finally:
            # release even if the wrapped command raised or was signalled;
            # the daemon would also free the lease when the run ends, but
            # prompt release lets a peer proceed at once.
            if token is not None:
                try:
                    _lock_release(token)
                except _CliError:
                    pass
    raise _CliError("unknown lock action {!r}".format(args.lock_command))


# --------------------------------------------------------------------------
# argparse wiring (called by yacron2.__main__)
# --------------------------------------------------------------------------


def _add_scope_flags(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--scope",
        metavar="NAME",
        help="the namespace to act in (default: this job's own name)",
    )
    group.add_argument(
        "--global",
        dest="use_global",
        action="store_true",
        help="act in the shared `global` scope (cross-job coordination)",
    )


def add_state_job_actions(actions: Any) -> None:
    """Add the job-facing KV actions to the existing `state` subparser.

    Coexists with yacron2.state_admin's backup/restore/gc/... actions under
    the same ``yacron2 state`` command; the action name disambiguates.
    """
    get = actions.add_parser("get", help="print a durable KV value")
    get.add_argument("key")
    _add_scope_flags(get)

    setp = actions.add_parser("set", help="set a durable KV value")
    setp.add_argument("key")
    setp.add_argument("value")
    setp.add_argument(
        "--json",
        action="store_true",
        help="parse VALUE as JSON instead of storing it as a string",
    )
    _add_scope_flags(setp)

    delete = actions.add_parser("delete", help="delete a durable KV value")
    delete.add_argument("key")
    _add_scope_flags(delete)

    keys = actions.add_parser("keys", help="list the keys in a scope")
    _add_scope_flags(keys)


def add_job_commands(sub: Any) -> None:
    """Add the top-level `cursor|lock|artifact|idempotent|secret` commands."""
    # cursor
    cursor = sub.add_parser(
        "cursor", help="read or advance a monotonic ETL cursor/watermark"
    )
    cursor_actions = cursor.add_subparsers(
        dest="cursor_command", metavar="ACTION"
    )
    cget = cursor_actions.add_parser("get", help="print a cursor's value")
    cget.add_argument("name")
    _add_scope_flags(cget)
    cadv = cursor_actions.add_parser(
        "advance", help="advance a cursor (monotonic unless --force)"
    )
    cadv.add_argument("name")
    cadv.add_argument("value")
    cadv.add_argument(
        "--force",
        action="store_true",
        help="set the value even if it moves the cursor backwards",
    )
    _add_scope_flags(cadv)

    # lock
    lock = sub.add_parser(
        "lock", help="a fleet-wide distributed mutex or semaphore"
    )
    lock_actions = lock.add_subparsers(dest="lock_command", metavar="ACTION")
    for verb, help_text in (
        ("acquire", "take the lock; print its hold token"),
        ("run", "hold the lock while running a command"),
    ):
        p = lock_actions.add_parser(verb, help=help_text)
        p.add_argument("name")
        p.add_argument(
            "--permits",
            type=int,
            default=1,
            help="semaphore capacity (default 1 = a mutex)",
        )
        p.add_argument(
            "--wait",
            action="store_true",
            help="block until the lock is free (up to --timeout)",
        )
        p.add_argument(
            "--timeout",
            type=float,
            default=0.0,
            metavar="SECONDS",
            help="how long --wait blocks before giving up",
        )
        p.add_argument(
            "--ttl",
            type=float,
            default=None,
            metavar="SECONDS",
            help="lease TTL (default: state.jobApi.lockTtlSeconds)",
        )
        _add_scope_flags(p)
        if verb == "run":
            # NOT dest "command": the root subparsers already store the
            # command name (state/lock/...) under args.command, and a same-
            # named REMAINDER here would clobber it and misroute the whole
            # invocation.
            p.add_argument(
                "run_command",
                nargs=argparse.REMAINDER,
                metavar="command",
                help="the command to run while holding the lock (after --)",
            )
    lrel = lock_actions.add_parser("release", help="release a held lock")
    lrel.add_argument("token")

    # artifact
    artifact = sub.add_parser(
        "artifact", help="publish or fetch a named artifact blob"
    )
    art_actions = artifact.add_subparsers(
        dest="artifact_command", metavar="ACTION"
    )
    aput = art_actions.add_parser(
        "put", help="publish an artifact (from FILE or stdin)"
    )
    aput.add_argument("name")
    aput.add_argument("file", nargs="?", default=None)
    _add_scope_flags(aput)
    aget = art_actions.add_parser(
        "get", help="fetch an artifact (to -o FILE or stdout)"
    )
    aget.add_argument("name")
    aget.add_argument("-o", "--output", default=None, metavar="FILE")
    _add_scope_flags(aget)
    alist = art_actions.add_parser("list", help="list artifact names")
    _add_scope_flags(alist)

    # idempotent
    idem = sub.add_parser(
        "idempotent",
        help="claim a key once fleet-wide (exit 0 fresh, 1 duplicate)",
    )
    idem.add_argument("key")
    idem.add_argument(
        "--ttl",
        type=float,
        default=0.0,
        metavar="SECONDS",
        help="expire the claim after N seconds (0 = permanent)",
    )
    idem.add_argument(
        "--release",
        action="store_true",
        help="drop the claim instead of making it",
    )
    _add_scope_flags(idem)

    # secret
    secret = sub.add_parser(
        "secret", help="read a run-scoped secret staged for this run"
    )
    secret_actions = secret.add_subparsers(
        dest="secret_command", metavar="ACTION"
    )
    sget = secret_actions.add_parser("get", help="print a secret's value")
    sget.add_argument("name")
    secret_actions.add_parser("list", help="list staged secret names")


_DISPATCH = {
    "state": _cmd_state,
    "cursor": _cmd_cursor,
    "lock": _cmd_lock,
    "artifact": _cmd_artifact,
    "idempotent": _cmd_idempotent,
    "secret": _cmd_secret,
}


def dispatch(args: argparse.Namespace) -> int:
    """Run a parsed job-facing command; return its exit code."""
    handler = _DISPATCH.get(args.command)
    if handler is None:  # pragma: no cover - routed by __main__
        print("yacron2: unknown command", file=sys.stderr)
        return 2
    # a bare `yacron2 cursor` (no action) prints help via a missing sub-action
    action_attr = {
        "cursor": "cursor_command",
        "lock": "lock_command",
        "artifact": "artifact_command",
        "secret": "secret_command",
    }.get(args.command)
    if action_attr is not None and getattr(args, action_attr, None) is None:
        print(
            "yacron2 {}: no action given (see --help)".format(args.command),
            file=sys.stderr,
        )
        return 2
    try:
        return handler(args)
    except _CliError as ex:
        print("yacron2 {}: {}".format(args.command, ex), file=sys.stderr)
        return EXIT_ERROR
