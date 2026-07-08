"""The job-facing state CLI: `cronstable state|cursor|lock|artifact|...`.

These are the commands a job command line reaches for -- durable KV, an ETL
cursor, a distributed lock, the artifact store, idempotency keys, run-scoped
secrets.  Each is a thin client of the loopback endpoint the daemon injected
into the job's environment (:mod:`cronstable.jobapi`): it reads the injected
``CRONSTABLE_STATE_URL`` / ``CRONSTABLE_STATE_TOKEN`` and speaks HTTP over
stdlib ``urllib`` (no aiohttp, no event loop, so the command starts instantly).

They coexist with the ``cronstable state`` *admin* commands (backup / restore /
gc / check ...): the admin actions operate on the store file tree offline via
``-c``, while these job-facing actions (get / set / delete / keys, and the
other verbs) act through the running daemon.  ``cronstable.__main__`` routes by
action name, so ``cronstable state check`` and ``cronstable state get KEY``
reach the right handler.

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

# The env vars the daemon injects (see cronstable.jobapi); the job CLI is the
# consumer.  Hardcoded here rather than imported so the CLI never pulls aiohttp
# into its import graph -- these three names are a stable wire contract.
ENV_URL = "CRONSTABLE_STATE_URL"
ENV_TOKEN = "CRONSTABLE_STATE_TOKEN"

# Injected into every DAG task so `cronstable xcom` knows this task's own
# key and the run's shared XCom scope (an artifact scope; XCom is a thin,
# task-keyed convention over the durable artifact store).  Hardcoded for the
# same reason as above -- a stable wire contract (see cronstable.dag).
ENV_DAG_XCOM_SCOPE = "CRONSTABLE_DAG_XCOM_SCOPE"
ENV_DAG_TASKKEY = "CRONSTABLE_DAG_TASKKEY"

# Exit codes: 0 success, 1 a real error, 2 usage, 3 lock not acquired, 4 the
# looked-up thing does not exist (so a script can branch on "missing"), 5 an
# idempotency key already claimed.  "Duplicate" gets its own code rather than
# sharing 1 with errors: a store outage must never masquerade as "a prior
# caller already did the work" to a guard script.
EXIT_ERROR = 1
EXIT_NOT_ACQUIRED = 3
EXIT_NOT_FOUND = 4
EXIT_DUPLICATE = 5

# Every job-facing action name, so __main__ can tell a `state get` (this
# module) from a `state backup` (cronstable.state_admin) without guessing.
STATE_JOB_ACTIONS = frozenset({"get", "set", "delete", "keys"})


class _CliError(Exception):
    """A user-facing failure: printed to stderr, exits non-zero."""


# Loopback control traffic must never be proxied: the default urllib opener
# honors http_proxy/HTTP_PROXY, which would route every state call -- bearer
# run token included -- to an external proxy that cannot reach the daemon's
# 127.0.0.1 endpoint anyway (CPython's bypass logic does not exempt loopback).
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

# Every request gets a deadline so a wedged daemon (state store on a dead
# mount) cannot hang the calling job forever.  Every verb but a blocking lock
# acquire is answered in milliseconds, so this is generous; the long poll
# passes its own deadline (blockSeconds plus this margin).
_DEFAULT_TIMEOUT = 30.0


# --------------------------------------------------------------------------
# HTTP transport (stdlib only, monkeypatchable in tests via _http)
# --------------------------------------------------------------------------


def _endpoint() -> Tuple[str, str]:
    url = os.environ.get(ENV_URL)
    token = os.environ.get(ENV_TOKEN)
    if not url or not token:
        raise _CliError(
            "not running inside a cronstable job: {} is not set (these "
            "commands reach the daemon's loopback state endpoint, which is "
            "injected into a job's environment; is a `state` section with "
            "jobApi enabled configured?)".format(ENV_URL)
        )
    return url, token


def _http(
    method: str,
    path: str,
    *,
    query: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    data: Optional[bytes] = None,
    timeout: Optional[float] = None,
) -> Tuple[int, Dict[str, str], bytes]:
    """One request to the endpoint; return ``(status, headers, body)``.

    The single seam the whole CLI goes through, so a test monkeypatches this
    to drive every verb without a live server.  ``timeout`` overrides the
    default deadline (the blocking lock acquire needs its long poll to be
    ended by the server, not the socket).
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
    if timeout is None:
        timeout = _DEFAULT_TIMEOUT
    try:
        with _OPENER.open(req, timeout=timeout) as resp:  # noqa: S310
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as ex:
        return ex.code, dict(ex.headers or {}), ex.read()
    except urllib.error.URLError as ex:
        raise _CliError(
            "cannot reach the cronstable state endpoint at {}: {}".format(
                url, ex.reason
            )
        ) from ex
    except (TimeoutError, OSError) as ex:
        # urllib wraps only errors from SENDING the request in URLError; a
        # deadline that fires while waiting for or reading the response
        # escapes as a raw TimeoutError (an OSError that is NOT a URLError
        # subclass).  Same transport failure, same clean error.  Ordered
        # after HTTPError/URLError on purpose: both subclass OSError, and
        # an HTTP error response must keep its status semantics.
        raise _CliError(
            "cannot reach the cronstable state endpoint at {}: {}".format(
                url, ex
            )
        ) from ex


def _json(
    method: str,
    path: str,
    *,
    query: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: Optional[float] = None,
) -> Tuple[int, Dict[str, Any]]:
    # timeout is forwarded only when explicitly set: _http applies the
    # default itself, and test fakes of the _http seam predate the kwarg.
    kwargs: Dict[str, Any] = {}
    if timeout is not None:
        kwargs["timeout"] = timeout
    status, _headers, body = _http(
        method, path, query=query, json_body=json_body, **kwargs
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
    # exit 0 when this caller won the claim (fresh: do the work), 5 when a
    # prior caller already claimed it (a duplicate: skip). Made for a shell
    # guard: `cronstable idempotent "$KEY" && do-the-side-effect`.  Distinct
    # from EXIT_ERROR (1, used for transport/store failures) so an outage
    # is detectable instead of reading as "already done".
    return 0 if _ok(status, data).get("fresh") else EXIT_DUPLICATE


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


def _xcom_scope() -> str:
    scope = os.environ.get(ENV_DAG_XCOM_SCOPE)
    if not scope:
        raise _CliError(
            "not running inside a cronstable DAG task: {} is not set (xcom "
            "publishes/reads task outputs within a dag_run; it only works "
            "for a task the DAG scheduler launched)".format(ENV_DAG_XCOM_SCOPE)
        )
    return scope


def _cmd_xcom(args: argparse.Namespace) -> int:
    scope = _xcom_scope()
    if args.xcom_command == "push":
        my = os.environ.get(ENV_DAG_TASKKEY)
        if not my:
            raise _CliError(
                "cannot determine this task's id ({} unset)".format(
                    ENV_DAG_TASKKEY
                )
            )
        payload = _read_input(args.file)
        status, _headers, body = _http(
            "POST",
            "/v1/artifact/put",
            query={"scope": scope, "name": my + "/" + args.key},
            data=payload,
        )
        _ok(status, json.loads(body) if body else {})
        return 0
    if args.xcom_command == "pull":
        upstream = args.task
        if args.map_index is not None:
            upstream = "{}#{}".format(upstream, args.map_index)
        status, _headers, body = _http(
            "GET",
            "/v1/artifact/get",
            query={"scope": scope, "name": upstream + "/" + args.key},
        )
        if status == 404:
            print(
                "no xcom {!r} from task {!r}".format(args.key, upstream),
                file=sys.stderr,
            )
            return EXIT_NOT_FOUND
        if status >= 400:
            _ok(status, json.loads(body) if body else {})
        _write_output(args.output, body)
        return 0
    if args.xcom_command == "list":
        status, data = _json(
            "GET", "/v1/artifact/list", query={"scope": scope}
        )
        for entry in _ok(status, data).get("artifacts", []):
            sys.stdout.write(str(entry.get("name")) + "\n")
        return 0
    raise _CliError("unknown xcom action {!r}".format(args.xcom_command))


def _read_input(path: Optional[str]) -> bytes:
    if not path or path == "-":
        return sys.stdin.buffer.read()
    try:
        with open(path, "rb") as fobj:
            return fobj.read()
    except OSError as ex:
        raise _CliError("cannot read {}: {}".format(path, ex)) from ex


def _write_output(path: Optional[str], data: bytes) -> None:
    if not path or path == "-":
        sys.stdout.buffer.write(data)
        return
    try:
        with open(path, "wb") as fobj:
            fobj.write(data)
    except OSError as ex:
        raise _CliError("cannot write {}: {}".format(path, ex)) from ex


def _lock_acquire(args: argparse.Namespace) -> Tuple[bool, Optional[str]]:
    scope = _scope_of(args)
    # a --wait long poll is server-bounded by blockSeconds: the client
    # deadline is that plus margin, so the server (not the socket) ends it.
    deadline = args.timeout + _DEFAULT_TIMEOUT if args.wait else None
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
        timeout=deadline,
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
        # print the hold token so a later `cronstable lock release TOKEN` (or a
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
# argparse wiring (called by cronstable.__main__)
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

    Coexists with cronstable.state_admin's backup/restore/gc/... actions under
    the same ``cronstable state`` command; the action name disambiguates.
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
                # The command after "--" is split off BEFORE argparse, in
                # __main__.main_loop (portable across Python versions; see
                # the note there -- argparse's own "--"/trailing handling is
                # inconsistent before 3.13, and REMAINDER would swallow our
                # own --wait/--timeout/--ttl). This positional only holds the
                # default [] and a command given WITHOUT a "--" separator.
                nargs="*",
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
        help="claim a key once fleet-wide (exit 0 fresh, 5 duplicate, "
        "1 error)",
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

    # xcom: cross-task data hand-off within a dag_run
    xcom = sub.add_parser(
        "xcom",
        help="publish or read a DAG task output (XCom) within a dag_run",
    )
    xcom_actions = xcom.add_subparsers(dest="xcom_command", metavar="ACTION")
    xpush = xcom_actions.add_parser(
        "push", help="publish this task's output under a key (FILE or stdin)"
    )
    xpush.add_argument("--key", required=True, help="the XCom key to publish")
    xpush.add_argument("file", nargs="?", default=None)
    xpull = xcom_actions.add_parser(
        "pull", help="read an upstream task's output by key"
    )
    xpull.add_argument(
        "--task", required=True, metavar="TASK", help="the upstream task id"
    )
    xpull.add_argument("--key", required=True, help="the XCom key to read")
    xpull.add_argument(
        "--map-index",
        type=int,
        default=None,
        metavar="I",
        help="read a specific mapped instance of the upstream task",
    )
    xpull.add_argument("-o", "--output", default=None, metavar="FILE")
    xcom_actions.add_parser("list", help="list XCom keys in this run")

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
    "xcom": _cmd_xcom,
}


def dispatch(args: argparse.Namespace) -> int:
    """Run a parsed job-facing command; return its exit code."""
    handler = _DISPATCH.get(args.command)
    if handler is None:  # pragma: no cover - routed by __main__
        print("cronstable: unknown command", file=sys.stderr)
        return 2
    # a bare `cronstable cursor` (no action) prints help via a missing
    # sub-action
    action_attr = {
        "cursor": "cursor_command",
        "lock": "lock_command",
        "artifact": "artifact_command",
        "secret": "secret_command",
        "xcom": "xcom_command",
    }.get(args.command)
    if action_attr is not None and getattr(args, action_attr, None) is None:
        print(
            "cronstable {}: no action given (see --help)".format(args.command),
            file=sys.stderr,
        )
        return 2
    try:
        return handler(args)
    except _CliError as ex:
        print("cronstable {}: {}".format(args.command, ex), file=sys.stderr)
        return EXIT_ERROR
