# Listener TLS

cronstable's HTTP surfaces can serve TLS natively, with no reverse proxy in
front. The optional **`web.tls`** block turns the `https://` entries of
`web.listen` into real TLS listeners for the dashboard, the REST API, the
metrics endpoint and the MCP bridge; the separate **`state.jobApi.tls`** block
does the same for the job-facing durable-state endpoint, and hands every job
the trust anchor it needs to verify that endpoint back.

Both are ordinary listener TLS: a certificate, its private key, and (on
`web.tls` only) an optional client CA that turns the listener into a
**mutual-TLS** one, where a caller must present a certificate that CA signed.
The plumbing is shared with the cluster mesh through `cronstable/tlsutil.py`,
but the config blocks are separate:
[`cluster.tls`](Clustering-and-Leader-Election#cluster-peer-attestation) is its
own always-mutual block for the peer channel and is not affected by anything on
this page.

**On this page:**
[When you want it](#when-you-want-it-and-when-you-do-not) ·
[Quickstart](#quickstart-an-https-dashboard) ·
[The `web.tls` block](#the-webtls-block) ·
[Mutual TLS](#mutual-tls-clientca) ·
[Mixed listeners](#mixed-listeners-one-daemon-two-transports) ·
[The job state API over TLS](#the-job-state-api-over-tls) ·
[Client configuration](#client-configuration) ·
[Certificate rotation](#certificate-rotation) ·
[Troubleshooting](#troubleshooting)

## When you want it, and when you do not

Reach for `web.tls` when the dashboard or the API is reachable from anywhere
that is not this host: another machine on the network, a colleague's laptop, a
CI runner, a desktop MCP client. The API carries the bearer token
(`web.authToken`) in an `Authorization` header on every request, so a plaintext
listener on a routable interface puts that token, and every job name, command
and captured output the API returns, on the wire in the clear.

Three cases where it buys nothing:

* **A loopback-only dashboard.** `http://127.0.0.1:8080` never leaves the host,
  so wrapping it in TLS adds certificates to manage and protects nothing that
  was exposed.
* **A `unix://` listener.** The socket lives in the host filesystem and
  [`web.socketMode`](HTTP-API#unix-socket-permissions) is its access control.
  `unix://` entries stay plaintext even when `web.tls` is set.
* **TLS already terminated in front.** If an ingress, a service mesh sidecar or
  an nginx block already terminates TLS and forwards to a loopback `http://`
  listener, that is still a valid deployment. `web.tls` is the alternative for
  when you would rather not run that hop.

The one thing TLS does **not** do is authenticate the caller. Plain `https://`
proves to the *client* which server it reached; it says nothing about who is
connecting. Caller authentication is either
[`web.authToken`](HTTP-API#authentication) or
[mutual TLS](#mutual-tls-clientca).

## Quickstart: an https dashboard

### 1. Mint a certificate with the right SANs

The certificate must cover **the name the client actually dials**. This is the
single most common first-run failure: a certificate issued for `localhost` does
not match `https://127.0.0.1:8443`, because an IP literal is matched against IP
SANs, not DNS ones. Put both forms in, plus whatever routable name you will use:

```shell
openssl req -x509 -newkey rsa:2048 -sha256 -days 365 -nodes \
  -keyout /etc/cronstable/tls/web.key \
  -out    /etc/cronstable/tls/web.pem \
  -subj   "/CN=cronstable" \
  -addext "subjectAltName=DNS:localhost,DNS:cronstable.internal,IP:127.0.0.1,IP:::1" \
  -addext "extendedKeyUsage=serverAuth"
chmod 600 /etc/cronstable/tls/web.key
```

`IP:::1` is the IPv6 loopback (the `IP:` prefix followed by `::1`). Drop the
`DNS:cronstable.internal` entry if you only ever dial the host by address, and add
more `DNS:`/`IP:` entries for every other name that will appear in a client's
URL. In production, issue the same shape of certificate from your own internal
CA or cert-manager instead of self-signing; cronstable only consumes the files.

### 2. Point `web.tls` at it

```yaml
web:
  listen:
    - https://0.0.0.0:8443
  tls:
    cert: /etc/cronstable/tls/web.pem
    key:  /etc/cronstable/tls/web.key
  authToken:
    fromEnvVar: CRONSTABLE_WEB_TOKEN
```

Run `cronstable --validate-config` before deploying: the pairing rules below
are checked at parse time, so a `cert` with no `key`, or an `https://` listener
with no certificate, is caught at rest rather than at the first connection.

### 3. Point a client at it

A self-signed certificate is its own CA, so the file you just minted is what
clients verify against. For an internally-issued certificate, pass the issuing
CA's PEM instead:

```shell
cronstable tui --url https://cronstable.internal:8443 \
               --cacert /etc/cronstable/tls/web.pem

# same four flags on the MCP bridge
cronstable mcp --url https://cronstable.internal:8443 \
               --cacert /etc/cronstable/tls/web.pem \
               --token-env CRONSTABLE_WEB_TOKEN --check
```

`curl` needs the same anchor: `curl --cacert /etc/cronstable/tls/web.pem
https://cronstable.internal:8443/status`. A browser will show a trust warning
until the CA is added to the operating system or browser trust store.

## The `web.tls` block

`web.listen` accepts three schemes. `https://` entries are served by the
`web.tls` material:

| Scheme | Transport | Notes |
| --- | --- | --- |
| `http://host:port` | plaintext | Host and port both required. |
| `https://host:port` | TLS from `web.tls` | Host and port both required; an omitted port is not defaulted, it is skipped with a warning. |
| `unix:///path` | plaintext | Filesystem socket; [`socketMode`](HTTP-API#unix-socket-permissions) is the access control. POSIX only. |

The block itself has three keys:

| Key | Default | Meaning |
| --- | --- | --- |
| `cert` | *(none)* | PEM certificate (with any intermediates) served on every `https://` listen address. Required together with `key`. |
| `key` | *(none)* | PEM private key for `cert`. Required together with `cert`. |
| `clientCa` | *(none)* | Trust anchor for **client** certificates. When set, the listener requires one, signed by this CA, on every `https://` connection. See [Mutual TLS](#mutual-tls-clientca). |

One context is built per app start and shared by every `https://` site, so all
of them serve the same certificate.

### What is validated, and when

These combinations are `ConfigError`s at parse time, which means
`cronstable --validate-config` catches them:

| Configuration | Why it is refused |
| --- | --- |
| `cert` without `key`, or `key` without `cert` | A server certificate cannot be served without its private key. |
| `clientCa` without `cert`/`key` | A listener cannot demand client certificates without serving one of its own. |
| `web.tls` set, but no `https://` in `listen` | The material would be silently ignored and the API served in cleartext. |
| `https://` in `listen`, but no `cert`/`key` | Those listeners have no certificate to serve and would be skipped at startup. |

**Whether the files exist is deliberately not checked at parse time.** Config
parsing touches no filesystem, `--validate-config` is often run somewhere that
is not the deployment target, and a Kubernetes-mounted secret need not exist yet
at first boot. The files are read when the listener starts: if they will not
load, the web API does **not** start (it is never downgraded to plaintext), the
error is logged, and the daemon retries on the next config reload, which is at
most a minute away.

## Mutual TLS: `clientCa`

Setting `clientCa` promotes the listener from "encrypted" to "encrypted and
caller-authenticated":

```yaml
web:
  listen:
    - https://0.0.0.0:8443
  tls:
    cert:     /etc/cronstable/tls/web.pem
    key:      /etc/cronstable/tls/web.key
    clientCa: /etc/cronstable/tls/client-ca.pem
```

Every `https://` connection must now present a certificate that chains to
`client-ca.pem`. A client that presents nothing is refused during the handshake;
so is a certificate from any other CA. There is no middle setting: "optional
client CA" means the *key* is optional, never that a presented certificate is.
A mode that completed the handshake for a client presenting nothing would turn
`clientCa` into a no-op for the operator who set it precisely to authenticate
callers.

**The CA file is the caller allowlist.** A server cannot do hostname
verification against a client, so *any* certificate that CA has ever signed is
accepted, with no further check on which one it is. Point `clientCa` at a
**dedicated CA that issues to cronstable clients only**, never at a shared
service-mesh or organisation-wide CA: with an organisational CA, every workload
that CA admits can call the control API and start, pause or reconfigure jobs.

Clients answer the requirement with `--client-cert` and `--client-key`:

```shell
cronstable tui --url https://cronstable.internal:8443 \
               --cacert      /etc/cronstable/tls/web.pem \
               --client-cert /etc/cronstable/tls/ops.pem \
               --client-key  /etc/cronstable/tls/ops.key
```

### mTLS satisfies the MCP token gate

`mcp.enabled` with no `web.authToken` on a routable listener is normally a
`ConfigError`: with no token the web app installs no auth middleware at all, so
`/mcp` would be served unauthenticated. Plain `https://` does not change that
verdict, because encryption is not caller authentication.

`web.tls.clientCa` does change it. An mTLS listener authenticates its callers at
the transport, which is the same guarantee the gate already accepts from an
mTLS-terminating proxy, so `mcp.enabled` on an `https://` listener with
`clientCa` set is allowed with no token. See [MCP](MCP) for the endpoint itself.

## Mixed listeners: one daemon, two transports

`listen` is a list, and each entry becomes its own site on one runner, with its
own transport. So a single daemon can serve the same app plaintext on loopback
and over TLS on the routable interface:

```yaml
web:
  listen:
    - http://127.0.0.1:8080          # local curl, local tui, node-local scrapes
    - https://10.0.0.5:8443          # everything off-host
    - unix:///run/cronstable/web.sock   # plaintext, guarded by socketMode
  tls:
    cert: /etc/cronstable/tls/web.pem
    key:  /etc/cronstable/tls/web.key
  socketMode: "0660"
```

The `https://` entry is TLS, the `http://` and `unix://` entries are not, and
all three serve the same routes, dashboard and metrics. A plaintext request to
the TLS port fails, as it should.

Note that with `mcp.enabled` set, the MCP token gate reads the *routable*
addresses: a mixed list like this needs `web.authToken` (or `clientCa`) on
account of the `10.0.0.5` entry, even though the loopback entry alone would
not. Without `mcp.enabled`, no listener requires a token.

## The job state API over TLS

`state.jobApi` is the endpoint jobs reach for [durable
state](Durable-State#job-facing-state): KV, cursors, artifacts, mutexes and
run-scoped secrets. It defaults to an ephemeral loopback port, which needs no
TLS at all. It needs TLS when jobs run **off this host** and dial it across the
network, because it serves per-run bearer tokens and staged job secrets.

```yaml
state:
  path: /var/lib/cronstable
  jobApi:
    listen: https://10.0.0.5:9000
    allowNonLoopbackBind: true
    tls:
      cert: /etc/cronstable/tls/state.pem
      key:  /etc/cronstable/tls/state.key
      ca:   /etc/cronstable/tls/internal-ca.pem
```

| Key | Default | Meaning |
| --- | --- | --- |
| `cert` | *(none)* | PEM certificate served on an `https://` listen address. Required together with `key`. |
| `key` | *(none)* | PEM private key for `cert`. Required together with `cert`. |
| `ca` | *(none)* | The **client-side trust anchor** handed to every job as `CRONSTABLE_STATE_CACERT`, so the job CLI can verify a certificate no public root signed. Counts as TLS material: setting it without an `https://` listen is refused, since it would be injected into every job and used for nothing. |

`listen` accepts `https://host:port`, `http://host:port` or a bare `host:port`
(a `unix://` path is still refused; the job CLI speaks TCP only). The same
pairing rules as `web.tls` apply: `cert` and `key` together, TLS material with
no `https://` listen refused, an `https://` listen with no certificate refused.

Two rules are specific to this endpoint:

* **No wildcard host over `https://`.** `https://0.0.0.0:9000`,
  `https://[::]:9000` and every other spelling of the unspecified address are
  `ConfigError`s. Jobs dial the address they are handed, and no certificate
  carries a SAN for "every interface", so every job would fail hostname
  verification. Name the interface explicitly (`https://10.0.0.5:9000`).
* **The advertised URL is the configured host**, not the bound one. A job now
  receives `CRONSTABLE_STATE_URL=https://10.0.0.5:9000`, the address it can
  actually dial and the one the certificate covers. (The bound address is still
  used for the ephemeral loopback default, where it is the right answer.)

`allowNonLoopbackBind` remains required for any non-loopback `listen`. Pairing
it with a plaintext address now logs a warning at every load, naming what
crosses the network in cleartext. It is a warning and not an error: terminating
TLS in front of the endpoint is still a valid answer.

### `ca` here is not `clientCa` there

The third key means the **opposite direction** in the two blocks, so it is worth
reading twice:

| Key | Direction | What it does |
| --- | --- | --- |
| `web.tls.clientCa` | server verifies **clients** | Requires a client certificate signed by this CA. It is the caller allowlist. |
| `state.jobApi.tls.ca` | **jobs** verify the server | Path handed to jobs as `CRONSTABLE_STATE_CACERT` so they can verify the daemon's certificate. It authenticates nobody. |

Mutual TLS on the job state API is deliberately **not** in this release. The
per-run bearer token already authenticates the caller (it identifies which run
is calling, which a certificate would not), and requiring client certificates
would mean injecting client key material into every job's environment.

### `CRONSTABLE_STATE_CACERT`

When `state.jobApi.tls.ca` is set, the daemon adds one variable to every job's
environment alongside the existing `CRONSTABLE_STATE_*` set:

| Variable | Meaning |
| --- | --- |
| `CRONSTABLE_STATE_CACERT` | Path to the CA the job should verify the state endpoint against. Absent when `ca` is unset. |

**The job CLI picks it up with no configuration.** `cronstable state`,
`cronstable lock`, `cronstable secret` and the rest read it the same way they
read `CRONSTABLE_STATE_URL` and `CRONSTABLE_STATE_TOKEN`:

```shell
#!/bin/sh
cronstable state set last-cursor 12345     # verifies against CRONSTABLE_STATE_CACERT
```

The in-job CLI has **no TLS flags at all**, and there is deliberately no way to
switch verification off from inside a job: nothing running in a job's
environment should be able to downgrade the channel carrying that job's own
secrets.

## Client configuration

The TUI and the MCP bridge take the same four options, flag first and
environment variable second, so one exported set of variables serves every
client in a shell:

| Flag | Environment variable | Meaning |
| --- | --- | --- |
| `--cacert PATH` | `CRONSTABLE_WEB_CACERT` | Verify the listener against this CA file instead of the system trust store. Needed for an internally-issued or self-signed certificate. |
| `--client-cert PATH` | `CRONSTABLE_WEB_CLIENT_CERT` | Certificate to present to a listener configured with [`web.tls.clientCa`](#mutual-tls-clientca). |
| `--client-key PATH` | `CRONSTABLE_WEB_CLIENT_KEY` | Private key for `--client-cert`. Passing it without `--client-cert` is an error: a key alone cannot present an identity, so accepting it would leave you believing you had authenticated when the listener had refused you. |
| `--insecure` | `CRONSTABLE_WEB_INSECURE` (`1`/`true`/`yes`) | Skip verification entirely. Warns on stderr, because the bearer token is still sent and therefore goes to whoever answers the connection. |

Who takes what:

* **`cronstable tui`** and **`cronstable mcp`**: all four, identically named.
* **A publicly trusted certificate** needs none of them. With no option set, a
  client verifies against the system trust store and presents no certificate.
* **The in-job CLI takes none of them.** It reads `CRONSTABLE_STATE_CACERT` from
  the environment the daemon injects; see
  [`CRONSTABLE_STATE_CACERT`](#cronstable_state_cacert) above.

`--insecure` is for a broken-certificate emergency, not a deployment mode.
Verification off means an interceptor that answers the connection receives the
bearer token, which is exactly what the warning says.

## Certificate rotation

An **in-place rotation** (same paths, new bytes, which is how cert-manager,
Vault and a Kubernetes secret refresh renew) leaves the config bytes identical,
so the ordinary "did the config change?" check never fires for it. Meanwhile the
SSL context was built once at listener start and holds the old certificate in
memory, so without a dedicated check the daemon keeps serving the expired
certificate.

cronstable notices it by fingerprint. On each housekeeping pass (at most once
per wall-clock minute) it compares an `(mtime_ns, size)` signature of `cert`,
`key` and `clientCa` against what the running listener loaded. If any of them
changed, the web app is **restarted** so the context is rebuilt from the new
material. `os.stat` follows symlinks, so the atomic symlink swap behind a
Kubernetes mounted secret is picked up.

Two properties are worth planning around:

* **The restart is gated on the new material loading.** A rotation is not atomic
  across the files and can be observed half-written. The new `cert`/`key` are
  dry-run loaded into a throwaway context first; if that fails, the running
  listener is left up, a warning is logged, and the check runs again on the next
  reload. A half-written rotation therefore costs a log line, not an outage.
* **The restart briefly drops open connections.** Make-before-break is not
  possible here: the new runner binds the port the old one still holds, so the
  old listener must stop first. In-flight requests are dropped, and so are the
  long-lived **SSE log streams** the dashboard and the TUI hold open. They
  reconnect on their own; the visible effect is a tailing log view blipping and
  resuming. Time noisy rotations accordingly.

**The `state.jobApi` listener does not hot-reload.** It builds its context once
when it starts and has no rotation check, so renewing its certificate in place
changes nothing until the **daemon is restarted**. If that endpoint's
certificate is on a short automatic renewal cycle, schedule a daemon restart to
match it.

## Troubleshooting

**A client fails with `certificate verify failed: Hostname mismatch` (or
`IP address mismatch`).** The certificate does not cover the name in the URL.
Hostname verification is on by default in every client, so
`https://127.0.0.1:8443` needs an `IP:127.0.0.1` SAN and
`https://localhost:8443` needs a `DNS:localhost` SAN; one does not imply the
other. Check what the certificate actually carries:

```shell
openssl x509 -in /etc/cronstable/tls/web.pem -noout -text | grep -A1 "Subject Alternative Name"
```

Reissue with the missing SAN, or dial the name the certificate does cover.

**`certificate verify failed: unable to get local issuer certificate`.** The
client does not trust the issuing CA. Pass `--cacert` with the CA PEM (for a
self-signed certificate, the certificate file itself), or install the CA in the
system trust store. This is also what a browser's trust warning means.

**The log says `web: TLS material is not loadable, so the web API is not
starting`.** The certificate or key could not be loaded, so the whole web API
stays down rather than any port being served in cleartext. The rest of the line
carries the underlying error: a wrong path, a key that does not match the
certificate, or a file the daemon's user cannot read. Fix it and the next
config reload retries; no restart is needed.

**The log says `Ignoring web listen url ...: no usable web.tls material for an
https listener`.** A single `https://` address was skipped because no context
was available for it. Parse-time validation normally makes this unreachable (an
`https://` listen with no `web.tls` is a `ConfigError`), so in practice it means
`web.tls` is present but empty. Give it a `cert` and `key`, or make the address
`http://`.

**The log says `web: new TLS material is not yet loadable`.** A rotation was
detected but the new files do not load yet, typically a half-written renewal
caught mid-flight. The old listener is still up and still serving; the check
repeats on the next reload. If the message persists past a couple of minutes,
the new material is genuinely broken rather than half-written: check the renewal
job and that the certificate and key are a matching pair.

**An mTLS listener refuses a client during the handshake.** With `clientCa` set,
a client that sends no certificate, or one signed by a different CA, is rejected
before any HTTP request happens, so there is no status code to read, only a
handshake error. Confirm the client is passing both `--client-cert` and
`--client-key`, and that the certificate chains to the exact CA in `clientCa`:

```shell
openssl verify -CAfile /etc/cronstable/tls/client-ca.pem /etc/cronstable/tls/ops.pem
```

**A job reports a TLS handshake failure against the state endpoint.** The
message names `CRONSTABLE_STATE_CACERT`. Either `state.jobApi.tls.ca` points at
the wrong anchor for the certificate `state.jobApi.tls.cert` serves, or the
endpoint's certificate lacks a SAN for the host in `state.jobApi.listen`. That
URL is the configured host verbatim, so it is the name the certificate must
cover. A job that reports the endpoint as unreachable instead is a different
fault: that one really is a bind, port or network problem.

## See also

- [HTTP Control API](HTTP-API): the endpoints, `web.authToken`, `socketMode`, and the rest of the `web` section these listeners serve.
- [Durable State](Durable-State): the store behind `state.jobApi`, the injected `CRONSTABLE_STATE_*` environment, and the job-facing commands.
- [Clustering and Leader Election](Clustering-and-Leader-Election): `cluster.tls` is a separate, always-mutual block for the peer mesh, unaffected by anything on this page.
- [MCP](MCP): the `/mcp` endpoint and the `cronstable mcp` bridge, including the token gate mTLS relaxes.
- [Terminal Dashboard](Terminal-Dashboard): `cronstable tui`, which takes the same four client flags.
- [Configuration Reference](Configuration-Reference): the `web.tls` and `state.jobApi.tls` option schema.
- [Production and Container Deployment](Production-Deployment): mounting certificates into a container and the certificate operations runbooks.
