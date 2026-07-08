# Production and Container Deployment

This page covers running cronstable in hardened Linux container and Kubernetes
environments (the published Docker image is Linux-only): the published image, the
security context it is built to satisfy, the Kubernetes `Deployment` manifest, the
FROM-the-published-image build pattern, and the few cases that require a writable
path. See [Installation](Installation) for the package/binary install methods and
[HTTP Control API](HTTP-API) for the optional web interface. For native Windows
deployment with the `cronstable-windows-amd64.exe` / `cronstable-windows-arm64.exe`
binaries, see [Running on Windows](Running-on-Windows).

## Why cronstable fits a locked-down pod

At runtime the daemon only *reads* its configuration and secrets and writes its
output to stdout/stderr. It needs no writable working directory, no temp files,
and no log files. It runs in the foreground, makes no exotic syscalls, and
requires no special privileges. The published image runs under a restricted
Kubernetes/container security context with no writable paths or elevated
privileges required.

The single exception is the optional per-job `user`/`group` switching feature,
which calls `os.initgroups`/`os.setgid`/`os.setuid` (`cronstable/job.py`) and so
requires the daemon to run as root. It is **unavailable** in the non-root
published image. If you do not use that feature, drop root entirely. See
[Commands and Environment](Commands-and-Environment) for the feature itself.

## The published image

Prebuilt, multi-architecture (`linux/amd64`, `linux/arm64`, `linux/386`,
`linux/arm/v7`, `linux/ppc64le` and `linux/s390x`) images are published to the
GitHub Container Registry on every release.

| Property | Value |
| --- | --- |
| Registry/image | `ghcr.io/ptweezy/cronstable` |
| Tags | the release version (e.g. `1.0.4`), plus `latest` |
| Base | `python:3.14-slim` (multi-stage; runtime stage copies a self-contained venv) |
| User | `65534:65534` (`nobody`), set via `USER` in the `Dockerfile` |
| Entrypoint | `cronstable` |
| Default command | `-c /etc/cronstable.d` |
| Config path | `/etc/cronstable.d` (a file or a directory of `*.yaml`/`*.yml` files) |
| Env | `PYTHONUNBUFFERED=1`, `PYTHONDONTWRITEBYTECODE=1`, `PATH=/opt/venv/bin:$PATH` |

`PYTHONUNBUFFERED` flushes stdout/stderr immediately (where cronstable logs) and
`PYTHONDONTWRITEBYTECODE` prevents `.pyc` writes; both matter under a read-only
root filesystem.

> A second `python:3.14-slim`-based Dockerfile under `example/docker/` exists for
> demonstration; it `pip install`s cronstable and is **not** non-root. For production
> use the published GHCR image (or base your own image on it, below), not the
> example.

### Quick start

Mount a crontab read-only and run:

```shell
docker run --rm \
  -v "$PWD/cronstable.yaml:/etc/cronstable.d/cronstable.yaml:ro" \
  ghcr.io/ptweezy/cronstable:latest
```

For production, pin a specific version instead of `latest`
(e.g. `ghcr.io/ptweezy/cronstable:1.0.4`).

### Baking config into your own image

If you would rather bake the configuration into an image, base it on the
published image. The non-root user, entrypoint, and config path are inherited:

```dockerfile
FROM ghcr.io/ptweezy/cronstable:latest

# The base image already runs as the non-root user 65534.
COPY cronstable.yaml /etc/cronstable.d/cronstable.yaml
```

## Hardened security context

The published image satisfies a fully restricted pod security context out of the
box:

* **`runAsNonRoot` / non-root UID**: the daemon needs no privileges, so it runs
  as an unprivileged UID (`65534`). Only per-job `user`/`group` switching needs
  root, and that feature is not available here.
* **`RuntimeDefault` seccomp**: cronstable makes no exotic syscalls, so the default
  seccomp profile (or an equivalently strict custom one) works.
* **`readOnlyRootFilesystem`**: no runtime writes are required by the image (or a
  `pip`/`pipx` install). Mount the crontab read-only. See [Writable-path
  exceptions](#writable-path-exceptions) for the two features that need a small
  writable volume: a Unix-socket web listener (with the image) and the standalone
  binary's self-extraction temp directory (only if you deploy the binary instead
  of the image).
* **`fsGroup`-mounted config/secrets**: mount config and secret volumes with an
  `fsGroup` (e.g. `65534`) so the non-root process can read them.
* **`drop: [ALL]` capabilities and `allowPrivilegeEscalation: false`**: cronstable
  needs no Linux capabilities and never escalates privileges.

## Kubernetes Deployment

A `Deployment` mounting the crontab from a `ConfigMap`, read-only, under a fully
restricted security context:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cronstable
spec:
  replicas: 1
  selector:
    matchLabels:
      app: cronstable
  template:
    metadata:
      labels:
        app: cronstable
    spec:
      securityContext:           # pod-level
        runAsNonRoot: true
        runAsUser: 65534
        runAsGroup: 65534
        fsGroup: 65534           # lets the non-root process read mounted volumes
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: cronstable
          image: ghcr.io/ptweezy/cronstable:latest
          args: ["-c", "/etc/cronstable.d"]
          securityContext:       # container-level
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop:
                - ALL
          resources:
            limits:
              cpu: 200m
              memory: 128Mi
            requests:
              cpu: 10m
              memory: 64Mi
          volumeMounts:
            - name: crontab
              mountPath: /etc/cronstable.d
              readOnly: true
      volumes:
        - name: crontab
          configMap:
            name: cronstable
```

`replicas: 1` is the safe default: cronstable holds the schedule in-process, so
two replicas with no coordination each run every job independently.

To run more than one replica without double-running jobs, enable
[leader election](Clustering-and-Leader-Election) with a `cluster` section.
Pick a backend by whether you already run a coordination store:

* **`backend: kubernetes` (recommended on Kubernetes).** A
  `coordination.k8s.io/v1` `Lease` gives a **fenced, exactly-once** election
  while the apiserver is reachable. No mTLS, no peer list, no odd-replica
  rule: a plain `Deployment` with any replica count works; you only grant the
  `Lease` RBAC (`get`/`create`/`update`). Likewise `backend: etcd` if you run
  etcd. See
  [Operating the lease backends](Clustering-and-Leader-Election#operating-the-lease-backends-kubernetes-and-etcd)
  and [`example/kubernetes/`](https://github.com/ptweezy/cronstable/tree/develop/example/kubernetes).
* **`backend: gossip` (the default, no coordination store).** A quorum-gated,
  mutual-TLS election that keeps no shared state, so:
  * **Use an odd replica count.** 3 replicas tolerate one failure, 5 tolerate
    two; even counts buy no extra fault tolerance, and `replicas: 2` is worse
    than 1 (a majority of 2 is 2, so both must be up to run anything); cronstable
    refuses to start with `electLeader` and a 2-node cluster, and warns on even
    sizes. Spread replicas across nodes/zones with `topologySpreadConstraints`;
    correlated failures defeat quorum regardless of count.
  * **A minority partition stands down** (runs nothing) to guarantee at most one
    leader, and the view is only as fresh as the poll `interval`, so this is
    best-effort, not fenced exactly-once. If a job must *never* be skipped or
    doubled, use a lease backend above or keep `replicas: 1`.
  * Provision the per-pod certificates from your own PKI (e.g. cert-manager) and
    give each pod a stable `nodeName`; a StatefulSet's ordinal hostnames make
    both the cert SANs and the peer list straightforward.

See [Clustering and Leader Election](Clustering-and-Leader-Election) for the full
trust model, quorum math, per-job `clusterPolicy`, the lease backends, and a
runnable local example.

### mTLS listener and networking (gossip backend)

The `gossip` backend adds a second listener, the `cluster.listen` mTLS port
(`0.0.0.0:8443` in the examples), that peers poll over
[mutual TLS](Clustering-and-Leader-Election#cluster-peer-attestation). It is
separate from the optional web API port, so plan the pod network for it:

* **Open the port for pod-to-pod traffic.** Every node both serves `/peer` on
  `cluster.listen` and connects out to each peer's `cluster.listen`, so the port
  must be reachable *between* the replicas. Expose it on the container
  (`containerPort: 8443`).
* **Add a `NetworkPolicy` in a default-deny cluster.** If you run default-deny
  ingress, allow the `cluster.listen` port from the pods carrying the same
  `app` label (the peers), so the mTLS gossip is not silently dropped; a blocked
  port shows up as `unreachable` peers and a lost quorum, not a clear error.
* **Use a headless `Service` for peer DNS.** Pair a gossip StatefulSet with a
  headless `Service` (`clusterIP: None`) so each pod is addressable at a stable
  per-ordinal DNS name (`cronstable-0.<svc>.<ns>.svc`, `cronstable-1.<svc>…`). Those
  names are what you list under `cluster.peers` and what the certificate SANs
  must match. A lease backend needs none of this: it has no peer listener.

### Health checks

cronstable serves a `GET /version` on the [web API](HTTP-API) that returns `200`
once the daemon is up, which makes a cheap liveness probe. Enable an
`http://` [web listener](HTTP-API) (for example `web.listen: ["http://0.0.0.0:8080"]`)
and point the probe at it:

```yaml
          ports:
            - name: web
              containerPort: 8080
          livenessProbe:
            httpGet:
              path: /version
              port: web
            initialDelaySeconds: 5
            periodSeconds: 10
```

If you protect the web API with a `web.authToken`, only the dashboard page (`/`)
stays unauthenticated, so `/version` then returns `401` without the bearer
token; either add the `Authorization` header to the probe (`httpGet.httpHeaders`)
or point it at `/`.

For clustered deployments, do **not** turn quorum loss into a pod restart: a
`livenessProbe` that fails on lost quorum would restart exactly the nodes you
need to reform it. Keep the probe scoped to daemon liveness (`/version`) and
alert on cluster health separately. `quorate` on `GET /cluster` is the field to
alert on (a node that cannot see a majority, or, on a lease backend, cannot
reach the store, stands its `Leader` jobs down); see the
[Monitoring and alerting](Clustering-and-Leader-Election#monitoring-and-alerting)
table for the full signal set.

### Cluster certificate operations

This applies to the `gossip` backend only (the lease backends use no per-node
mTLS certs). The day-2 story is mostly hands-off, because cronstable reloads its
mTLS contexts **in place**: on each config-reload pass it compares the on-disk
`ca`/`cert`/`key` against what it loaded and, if any changed, rebuilds the TLS
contexts (dry-running the new material first so a half-written file is retried,
not fatal) within ~1 minute, with **no restart**. So the in-place renewals
cert-manager, Vault, and mounted-secret refreshes perform (same paths, new
bytes) are picked up automatically. The full mechanism is on the Clustering page
under [Certificate rotation](Clustering-and-Leader-Election#certificate-rotation);
the operational cases:

* **The cluster `ca` is the membership allowlist.** cronstable trusts *any* cert
  the configured `ca` signs to assert a node identity and gossip state, so the
  `ca` **must** be a dedicated, single-purpose CA issued only to cronstable nodes,
  **not** a shared service-mesh or organisation-wide CA. Any cert the CA admits
  could otherwise fabricate the `/peer` payload (fake agreement, trip the
  conflict gate, or suppress an `@reboot` job). Provision leaf certs from that
  dedicated PKI (a private cert-manager issuer, an internal CA); cronstable only
  consumes them.
* **Rotating a single node's cert.** A leaf renewed by your PKI needs no
  coordination as long as it still chains to the same `ca`: write the new
  `cert`/`key` in place, the node reloads within ~1 minute, and its peers keep
  trusting it. Issue the replacement with a comfortable overlap (well before the
  old cert expires) so a slow refresh never leaves the node serving an expired
  cert.
* **Rolling the cluster CA.** Changing the CA itself needs care, because a node
  only trusts peers whose certs chain to the CA bundle it currently holds. Roll
  it so trust always overlaps:
  1. Distribute a **bundle** file containing *both* the old and new CA as the
     `tls.ca` on **every** node first (an additional trust anchor). Each node
     reloads within ~1 minute and now trusts either CA.
  2. Confirm every node still shows its peers `agreed` on `GET /cluster` (no
     `untrusted`).
  3. Re-issue each node's leaf cert from the **new** CA, **one node at a time**,
     watching `GET /cluster` after each until the rotated node and its peers
     return to `agreed`.
  4. Once every node presents a new-CA cert, narrow the bundle back to the
     **new** CA alone and distribute it everywhere.

  Never cut the CA over in a single step: if some nodes trust only the new CA
  while others still present old-CA certs, they reject each other as `untrusted`
  and the cluster loses quorum until trust overlaps again.
* **Recovering from an `untrusted` cascade.** If peers start showing `untrusted`
  on `GET /cluster` (or the `peer … is untrusted` `WARNING` in the logs) after a
  rotation, certs and CA trust have diverged (typically a CA roll that skipped
  the overlap step, or a node whose refresh lagged). Recovery needs **no
  restart**: restore the trust overlap (push a CA bundle that includes whichever
  CA the still-`untrusted` peers were issued from), or finish rolling the lagging
  nodes onto the new CA. Each node reloads within ~1 minute and peers return to
  `agreed` automatically. Because `Leader` jobs stand down (fail closed) while
  quorum is lost, the cascade **skips** firings rather than double-running them:
  no split-brain during recovery, only missed firings until trust reconverges.

### Production clustering checklist

Before scaling past `replicas: 1`, walk this per your backend. Each row links to
the authoritative section on the Clustering page.

| Concern | `gossip` (default) | Lease (`kubernetes` / `etcd`) |
| --- | --- | --- |
| Trust / auth ([gossip mTLS](Clustering-and-Leader-Election#cluster-peer-attestation) / [lease store](Clustering-and-Leader-Election#operating-the-lease-backends-kubernetes-and-etcd)) | per-node certs from a **dedicated** cluster CA; open the `cluster.listen` mTLS port | apiserver `Lease` RBAC (`get`/`create`/`update`) or reachable etcd endpoints; no mTLS |
| Node identity ([gossip](Clustering-and-Leader-Election#unique-node-names) / [lease](Clustering-and-Leader-Election#node-identity-for-the-lease-backends)) | stable, **unique** `nodeName` (distinct cert SANs / stable hostnames) | stable name too, but the **lease** is the fence, so a duplicate name cannot double-lead |
| Listener / port ([attestation](Clustering-and-Leader-Election#cluster-peer-attestation)) | `cluster.listen` reachable pod-to-pod; headless `Service` for peer DNS | none (no peer listener) |
| Replica count ([sizing](Clustering-and-Leader-Election#sizing-the-cluster)) | **odd** (3/5/7); cronstable rejects an `electLeader` 2-node cluster and warns on even sizes | **any** count; the store fences, not a quorum |
| Rotation ([cert rotation](Clustering-and-Leader-Election#certificate-rotation)) | in-place cert/CA reload (above); roll the CA with an overlap | n/a (no per-node mTLS certs) |
| Preflight | `cronstable -c <path> --validate-config` (catches lease ordering, CA/cert paths, an `electLeader` 2-node cluster) | `cronstable -c <path> --validate-config` (same) |

`cronstable -c <path> --validate-config` parses the config and exits non-zero on any
error, so it belongs in CI before a cluster deploy: it enforces the lease timing
ordering (`leaseDurationSeconds > renewDeadlineSeconds`, `retryPeriodSeconds <
renewDeadlineSeconds`, and the like), the RFC1123 `leaseName`/`leaseNamespace`
shapes, the etcd endpoint/TLS rules, and the `electLeader` 2-node rejection. See
[Command-Line Reference](CLI-Reference).

## Writable-path exceptions

A read-only root filesystem is sufficient for the published image in the normal
case. Two features need a small writable mount.

### 1. Unix-socket web interface

If you enable the optional [HTTP Control API](HTTP-API) on a Unix socket
(`web.listen: [unix:///path/cronstable.sock]`), cronstable binds a `UnixSite` and
creates the socket file at that path, a write. Point the socket at a small
writable volume (a Kubernetes `emptyDir`) rather than the root filesystem:

```yaml
          volumeMounts:
            - name: crontab
              mountPath: /etc/cronstable.d
              readOnly: true
            - name: run
              mountPath: /run/cronstable
      volumes:
        - name: crontab
          configMap:
            name: cronstable
        - name: run
          emptyDir: {}
```

with `web.listen: [unix:///run/cronstable/cronstable.sock]` in your config. (TCP
listeners such as `http://0.0.0.0:8080` need no writable path.) The optional
`web.socketMode` config key, if set, `chmod`s the socket after bind.

> `unix://` web listeners are not supported on Windows (the Proactor event loop
> lacks `create_unix_server`); such a listen URL is skipped with the warning
> `Ignoring web listen url <url>: unix-socket listeners are not supported on this
> platform`. Use an `http://` listener instead. `web.socketMode` only applies to
> unix sockets, so it is irrelevant on Windows. See
> [Running on Windows](Running-on-Windows).

### 2. The standalone binary under a read-only rootfs

This applies only if you deploy the standalone *binary* (from the GitHub
releases) instead of the published image. The binary is a self-extracting
executable: on each start it unpacks its embedded Python runtime into a temporary
directory and loads shared libraries from there, so it needs a temp directory
that is both **writable and executable**. On an ordinary system `/tmp` already
satisfies this and no setup is needed.

Under a read-only root filesystem, `/tmp` is read-only too and the binary aborts
at startup with `Could not create temporary directory` or
`Error loading shared library …: Operation not permitted`. Provide a writable,
executable temp mount:

* **Docker**: `--tmpfs /tmp:rw,exec,nosuid,nodev,size=64m`. The `exec` is
  required: Docker's `--tmpfs` defaults to `noexec`, but the binary must execute
  the libraries it unpacks.

  ```shell
  docker run --rm --read-only \
    --tmpfs /tmp:rw,exec,nosuid,nodev,size=64m \
    -v "$PWD/cronstable.yaml:/etc/cronstable.d/cronstable.yaml:ro" \
    your-image-with-the-binary -c /etc/cronstable.d
  ```

* **Kubernetes**: mount an `emptyDir` at `/tmp` (writable and executable by
  default; use `medium: Memory` for a tmpfs).
* **Either**: point the binary at another writable, executable directory with
  `TMPDIR=/path`.

This requirement is unique to the standalone binary. The published image and
`pip`/`pipx` installs run cronstable as a normal Python package with the interpreter
on disk; they never self-extract and need no writable temp directory. See
[Installation](Installation) for the binary download.

## Operational notes

* **Logging**: cronstable logs to stdout/stderr; collect logs at the platform
  level (`kubectl logs`, the container runtime's log driver). Adjust verbosity
  with `-l/--log-level` (default `INFO`) or a `logging:` config section.
* **Shutdown**: on POSIX, cronstable installs handlers for `SIGINT` and `SIGTERM`
  and shuts down gracefully, so the default pod termination path works without
  extra configuration. On Windows (where the Proactor loop has no
  `add_signal_handler`) it instead handles `SIGINT`/`SIGBREAK` via
  `signal.signal` plus a heartbeat timer, so pressing Ctrl-C or Ctrl-Break stops
  it; either way it finishes the currently-running jobs first. See
  [Running on Windows](Running-on-Windows).
* **Validate before deploy**: `cronstable -c <path> --validate-config` parses the
  config and exits, useful as a CI/pre-deploy gate. See [Command-Line
  Reference](CLI-Reference).
* **Config not found**: the default config path is platform-specific:
  `/etc/cronstable.d` on POSIX, `%APPDATA%\cronstable` on Windows (falling back to the
  user profile `~` if `APPDATA` is unset). When `-c` is left at whichever is the
  platform default and that path does not exist, cronstable prints an error and exits
  non-zero. In the container, ensure the config volume is mounted at
  `/etc/cronstable.d`.
