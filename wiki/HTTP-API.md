# HTTP Control API

yacron2 exposes an optional [aiohttp](https://docs.aiohttp.org/) REST control API,
enabled by adding a top-level `web` section to the configuration. It serves three
endpoints for querying the daemon version, inspecting job status, and triggering a
job on demand. This page documents the configuration schema, the endpoints, bearer-token
authentication, Unix-socket permissions, and lifecycle behavior.

The interface is *new in version 0.10*; `web.authToken` and `web.socketMode` are new
in yacron2 1.0.0.

> **Looking for the browser UI?** The same HTTP interface also serves the
> built-in **[Web Dashboard](Web-Dashboard)** at `/` on every `http://` listener
> (enabled by default; disable it with `ui: false`). This page documents the REST
> endpoints; the [Web Dashboard](Web-Dashboard) page is the visual tour.

## Enabling the API

Add a `web` section with at least one `listen` URL:

```yaml
web:
  listen:
    - http://127.0.0.1:8080
    - unix:///tmp/yacron2.sock
```

The server is created only when `web.listen` is non-empty. There must be exactly one
`web` block across the whole configuration: a duplicate `web` block in an included file
or a second file in a config directory raises a `ConfigError`. See
[Includes, Defaults, and Multi-File Config](Includes-and-Defaults).

## Configuration reference

The `web` section is parsed by the strictyaml `CONFIG_SCHEMA` in `yacron2/config.py`.
`listen` is required; the rest are optional (strictyaml `Opt(...)`).

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `listen` | sequence of strings | (required) | List of URLs to bind. Each is `http://host:port` or `unix:///path`. An empty list disables the server. |
| `headers` | map of string→string | (none) | Extra HTTP headers added to the success responses (including the 409 conflict body and the empty start-job response, but not the 404 or 401). |
| `authToken` | map (`value`/`fromFile`/`fromEnvVar`) | (none) | When set, requires bearer-token authentication on all routes (see [Authentication](#authentication)). |
| `socketMode` | string (octal) | (none) | File mode applied via `chmod` to `unix://` listen sockets (see [Unix socket permissions](#unix-socket-permissions)). |

### `listen` URL forms

| Scheme | Form | Requirements |
| --- | --- | --- |
| `http` | `http://host:port` | Both host and port are required. An `http` URL missing either is logged as a warning (`Ignoring web listen url ...: http url needs host and port`) and skipped. |
| `unix` | `unix:///path/to/socket` | Binds an `aiohttp` `UnixSite` at the given filesystem path. |

Any other scheme is logged (`scheme ... not supported`) and skipped. Binding maps to
`web.TCPSite` for `http` and `web.UnixSite` for `unix` (`web_site_from_url` in
`yacron2/cron.py`).

`https` is not a recognized scheme. To serve the API over TLS, bind to a loopback
`http` address or a `unix` socket and terminate TLS in a reverse proxy.

## Endpoints

All routes are registered in `start_stop_web_app`:

| Method | Path | Handler | Success status |
| --- | --- | --- | --- |
| `GET` | `/version` | `_web_get_version` | `200` |
| `GET` | `/status` | `_web_get_status` | `200` |
| `POST` | `/jobs/{name}/start` | `_web_start_job` | `200` |

The configured `headers` map is applied to the `200` responses of all three
handlers and to the `409` body of `/jobs/{name}/start`. The `404` (unknown job)
and `401` (authentication failure) responses are raised without it.

### `GET /version`

Returns the yacron2 version as `text/plain` (the value of `yacron2.version.version`).

```shell
$ http get http://127.0.0.1:8080/version
HTTP/1.1 200 OK
Content-Type: text/plain; charset=utf-8

1.0.13
```

### `GET /status`

Returns the status of every configured job. The response format depends on the
request's `Accept` header:

- If `Accept` is exactly `application/json`, the response is a JSON array
  (`application/json`).
- Otherwise the response is `text/plain`, one job per line.

Each job has one of three statuses, determined in this order:

| Status | When | Fields |
| --- | --- | --- |
| `running` | One or more instances are currently running. | `job`, `status`, `pid` (list of the PIDs of running instances whose subprocess has been started, i.e. `runjob.proc is not None`). |
| `disabled` | The job is not running and `enabled: false`. | `job`, `status`. |
| `scheduled` | The job is not running and is enabled. | `job`, `status`, `scheduled_in`. |

For `scheduled` jobs, `scheduled_in` is the number of seconds until the next run
(a float, computed from the job's crontab in the job's timezone). For an `@reboot`
schedule, `scheduled_in` is the literal string `"@reboot"`.

The `disabled` status (*new in 1.0.1*) is reported honestly instead of an
inapplicable `scheduled (in N seconds)`.

Text form:

```shell
$ http get http://127.0.0.1:8080/status
HTTP/1.1 200 OK
Content-Type: text/plain; charset=utf-8

test-01: scheduled (in 14 seconds)
test-02: running (pid: 12345)
test-03: disabled
```

In the text form, `scheduled_in` is rendered as a human-readable relative time
(`in N seconds` / `minutes` / `hours` / `days`), running jobs show
`running (pid: <comma-separated pids>)`, and disabled jobs show `disabled`.

JSON form:

```shell
$ http get http://127.0.0.1:8080/status Accept:application/json
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

[
    {"job": "test-01", "status": "scheduled", "scheduled_in": 6.16588},
    {"job": "test-02", "status": "running", "pid": [12345]},
    {"job": "test-03", "status": "disabled"}
]
```

### `POST /jobs/{name}/start`

Launches the named job immediately, regardless of its schedule. `{name}` is the
job's `name`.

| Condition | Response |
| --- | --- |
| No job with that name. | `404 Not Found`. |
| The job exists but has `enabled: false`. | `409 Conflict`, body `job '<name>' is disabled`. |
| Otherwise. | `200 OK`, empty body; the job is launched via the normal launch path. |

The `409` behavior is *new in 1.0.1*: a disabled job behaves as if it is not there,
so the API refuses to launch it manually rather than overriding the config.

Manual launch goes through `maybe_launch_job`, so the job's `concurrencyPolicy`
applies. If an instance is already running, `Allow` starts another, `Forbid` does
not start a new one (the `200` still returns), and `Replace` cancels the running
instance(s) first. See [Concurrency and Timeouts](Concurrency-and-Timeouts).

```shell
$ http post http://127.0.0.1:8080/jobs/test-02/start
HTTP/1.1 200 OK
Content-Length: 0
```

## Response headers

The `web.headers` map (*released in yacron2 1.0.0*; merged upstream but never
released in yacron 0.19) is a string→string map applied to the responses from
`/version`, `/status`, the `409` body of `/jobs/{name}/start`, and the `200` of
`/jobs/{name}/start`. It is not applied to the `404` (unknown job) or `401`
(authentication failure) responses, which are raised without the configured
headers. Example:

```yaml
web:
  listen:
    - http://127.0.0.1:8080
  headers:
    X-Frame-Options: DENY
    Cache-Control: no-store
```

## Authentication

By default the API is unauthenticated; anyone who can reach a `listen` address can
call every endpoint. Restrict access at the network or socket level, or enable
bearer-token authentication with `web.authToken`.

`authToken` resolves the token from exactly one source, in this precedence order
(`_resolve_web_token`):

| Sub-option | Type | Description |
| --- | --- | --- |
| `value` | string or null | Literal token value. |
| `fromFile` | string or null | Path to a file; the token is the file contents with surrounding whitespace stripped. |
| `fromEnvVar` | string or null | Name of an environment variable holding the token. |

When `authToken` is set, an aiohttp middleware (`_make_auth_middleware`) requires
`Authorization: Bearer <token>` on every route:

- The auth scheme is compared case-insensitively (`Bearer`, `bearer`, etc.) per
  RFC 7235 (*case-insensitive matching new in 1.0.4*).
- The presented token is compared against the configured token in constant time via
  `hmac.compare_digest`.
- A missing/malformed `Authorization` header, a wrong scheme, or a non-matching
  token returns `401 Unauthorized`.

```yaml
web:
  listen:
    - http://127.0.0.1:8080
  authToken:
    fromEnvVar: YACRON2_WEB_TOKEN
```

```shell
$ http get http://127.0.0.1:8080/status "Authorization:Bearer s3cr3t"
$ curl -H "Authorization: Bearer s3cr3t" http://127.0.0.1:8080/status
```

### Fail-closed behavior

If `authToken` is configured but resolves to an empty token, yacron2 raises a
`ConfigError` and refuses to start the web server, rather than silently serving
the control API with no authentication (*new in 1.0.1*). This happens when:

- `value`, `fromFile`, and `fromEnvVar` are all empty/absent;
- `fromEnvVar` names a variable that is unset (resolves to `""`);
- `fromFile` points to a file that is empty or contains only whitespace.

If `fromFile` cannot be read (`OSError`), yacron2 also raises a `ConfigError`
(`web.authToken.fromFile could not be read: ...`).

## Unix socket permissions

`web.socketMode` (*new in 1.0.0*) is an octal-string file mode applied with `chmod`
to each `unix://` listen socket after it starts (`_apply_socket_mode`):

```yaml
web:
  listen:
    - unix:///run/yacron2/yacron2.sock
  socketMode: "0660"
```

- The mode is parsed as base-8 (`int(socketMode, 8)`).
- It is applied only to `unix://` sockets; non-`unix` addresses are ignored.
- An invalid mode (not an octal integer, raising `ValueError`) or a `chmod`
  failure (`OSError`) is logged as a warning
  (`web: could not set socketMode <mode> on <path>: ...`) and does not abort
  startup.

When using a Unix socket on a read-only-root container, point the socket at a small
writable volume. See [Production and Container Deployment](Production-Deployment).

## Lifecycle and reload behavior

The control API lifecycle is driven by `start_stop_web_app`, called on each config
reload from the scheduler loop:

- If a server is running and the new `web` config is absent or differs from the
  running one, the running server is stopped (`web_runner.cleanup()`) before any new
  one is started. A change to any `web` field (including `headers`, `authToken`, or
  `socketMode`) thus triggers a restart of the server on reload.
- The server is (re)started only when `web` is present, `listen` is non-empty, and no
  server is currently running.
- Each `listen` address is bound independently. A bad URL (`ValueError`) or a bind
  failure (`OSError`, e.g. address already in use) on one address is logged as a
  warning (`web: could not listen on <addr>: ...`) and skipped; the remaining
  addresses still bind, and the config update is not aborted (*new in 1.0.1*).
- The `web: started listening on <addr>` log line is emitted only after the bind
  succeeds.
- On shutdown, the running server is stopped after currently running jobs finish.

A `ConfigError` raised while resolving `authToken` (empty token or unreadable
`fromFile`) propagates out of `start_stop_web_app` and is caught by the reload loop,
which logs the configuration error and keeps running the previously-loaded
configuration (the new config is not applied).

## See also

- [Web Dashboard](Web-Dashboard) — the built-in browser UI served by this interface.
- [Configuration Reference](Configuration-Reference)
- [CLI Reference](CLI-Reference)
- [Concurrency and Timeouts](Concurrency-and-Timeouts)
- [Production and Container Deployment](Production-Deployment)
- [Architecture and Internals](Architecture-and-Internals)
