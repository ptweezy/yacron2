# Production and Container Deployment

This page covers running yacron2 in hardened container environments: the published
image, the security context it is built to satisfy, the Kubernetes `Deployment`
manifest, the FROM-the-published-image build pattern, and the few cases that
require a writable path. See [Installation](Installation) for the package/binary
install methods and [HTTP Control API](HTTP-API) for the optional web interface.

## Why yacron2 fits a locked-down pod

At runtime the daemon only *reads* its configuration and secrets and writes its
output to stdout/stderr. It needs no writable working directory, no temp files,
and no log files. It runs in the foreground, makes no exotic syscalls, and
requires no special privileges. The published image runs under a restricted
Kubernetes/container security context with no writable paths or elevated
privileges required.

The single exception is the optional per-job `user`/`group` switching feature,
which calls `os.initgroups`/`os.setgid`/`os.setuid` (`yacron2/job.py`) and so
requires the daemon to run as root. It is **unavailable** in the non-root
published image. If you do not use that feature, drop root entirely. See
[Commands and Environment](Commands-and-Environment) for the feature itself.

## The published image

Prebuilt, multi-architecture (`linux/amd64` + `linux/arm64`) images are published
to the GitHub Container Registry on every release.

| Property | Value |
| --- | --- |
| Registry/image | `ghcr.io/ptweezy/yacron2` |
| Tags | the release version (e.g. `1.0.4`), plus `latest` |
| Base | `python:3.14-slim` (multi-stage; runtime stage copies a self-contained venv) |
| User | `65534:65534` (`nobody`), set via `USER` in the `Dockerfile` |
| Entrypoint | `yacron2` |
| Default command | `-c /etc/yacron2.d` |
| Config path | `/etc/yacron2.d` (a file or a directory of `*.yaml`/`*.yml` files) |
| Env | `PYTHONUNBUFFERED=1`, `PYTHONDONTWRITEBYTECODE=1`, `PATH=/opt/venv/bin:$PATH` |

`PYTHONUNBUFFERED` flushes stdout/stderr immediately (where yacron2 logs) and
`PYTHONDONTWRITEBYTECODE` prevents `.pyc` writes — both matter under a read-only
root filesystem.

> A second `python:3.14-slim`-based Dockerfile under `example/docker/` exists for
> demonstration; it `pip install`s yacron2 and is **not** non-root. For production
> use the published GHCR image (or base your own image on it, below), not the
> example.

### Quick start

Mount a crontab read-only and run:

```shell
docker run --rm \
  -v "$PWD/yacron2tab.yaml:/etc/yacron2.d/yacron2tab.yaml:ro" \
  ghcr.io/ptweezy/yacron2:latest
```

For production, pin a specific version instead of `latest`
(e.g. `ghcr.io/ptweezy/yacron2:1.0.4`).

### Baking config into your own image

If you would rather bake the configuration into an image, base it on the
published image. The non-root user, entrypoint, and config path are inherited:

```dockerfile
FROM ghcr.io/ptweezy/yacron2:latest

# The base image already runs as the non-root user 65534.
COPY yacron2tab.yaml /etc/yacron2.d/yacron2tab.yaml
```

## Hardened security context

The published image satisfies a fully restricted pod security context out of the
box:

* **`runAsNonRoot` / non-root UID** — the daemon needs no privileges, so it runs
  as an unprivileged UID (`65534`). Only per-job `user`/`group` switching needs
  root, and that feature is not available here.
* **`RuntimeDefault` seccomp** — yacron2 makes no exotic syscalls, so the default
  seccomp profile (or an equivalently strict custom one) works.
* **`readOnlyRootFilesystem`** — no runtime writes are required by the image (or a
  `pip`/`pipx` install). Mount the crontab read-only. See [Writable-path
  exceptions](#writable-path-exceptions) for the two features that need a small
  writable volume: a Unix-socket web listener (with the image) and the standalone
  binary's self-extraction temp directory (only if you deploy the binary instead
  of the image).
* **`fsGroup`-mounted config/secrets** — mount config and secret volumes with an
  `fsGroup` (e.g. `65534`) so the non-root process can read them.
* **`drop: [ALL]` capabilities and `allowPrivilegeEscalation: false`** — yacron2
  needs no Linux capabilities and never escalates privileges.

## Kubernetes Deployment

A `Deployment` mounting the crontab from a `ConfigMap`, read-only, under a fully
restricted security context:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: yacron2
spec:
  replicas: 1
  selector:
    matchLabels:
      app: yacron2
  template:
    metadata:
      labels:
        app: yacron2
    spec:
      securityContext:           # pod-level
        runAsNonRoot: true
        runAsUser: 65534
        runAsGroup: 65534
        fsGroup: 65534           # lets the non-root process read mounted volumes
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: yacron2
          image: ghcr.io/ptweezy/yacron2:latest
          args: ["-c", "/etc/yacron2.d"]
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
              mountPath: /etc/yacron2.d
              readOnly: true
      volumes:
        - name: crontab
          configMap:
            name: yacron2tab
```

`replicas: 1` is intentional: yacron2 holds the schedule in-process and has no
distributed leader election, so each replica runs every job independently.

## Writable-path exceptions

A read-only root filesystem is sufficient for the published image in the normal
case. Two features need a small writable mount.

### 1. Unix-socket web interface

If you enable the optional [HTTP Control API](HTTP-API) on a Unix socket
(`web.listen: [unix:///path/yacron2.sock]`), yacron2 binds a `UnixSite` and
creates the socket file at that path — a write. Point the socket at a small
writable volume (a Kubernetes `emptyDir`) rather than the root filesystem:

```yaml
          volumeMounts:
            - name: crontab
              mountPath: /etc/yacron2.d
              readOnly: true
            - name: run
              mountPath: /run/yacron2
      volumes:
        - name: crontab
          configMap:
            name: yacron2tab
        - name: run
          emptyDir: {}
```

with `web.listen: [unix:///run/yacron2/yacron2.sock]` in your config. (TCP
listeners such as `http://0.0.0.0:8080` need no writable path.) The optional
`web.socketMode` config key, if set, `chmod`s the socket after bind.

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

* **Docker** — `--tmpfs /tmp:rw,exec,nosuid,nodev,size=64m`. The `exec` is
  required: Docker's `--tmpfs` defaults to `noexec`, but the binary must execute
  the libraries it unpacks.

  ```shell
  docker run --rm --read-only \
    --tmpfs /tmp:rw,exec,nosuid,nodev,size=64m \
    -v "$PWD/yacron2tab.yaml:/etc/yacron2.d/yacron2tab.yaml:ro" \
    your-image-with-the-binary -c /etc/yacron2.d
  ```

* **Kubernetes** — mount an `emptyDir` at `/tmp` (writable and executable by
  default; use `medium: Memory` for a tmpfs).
* **Either** — point the binary at another writable, executable directory with
  `TMPDIR=/path`.

This requirement is unique to the standalone binary. The published image and
`pip`/`pipx` installs run yacron2 as a normal Python package with the interpreter
on disk; they never self-extract and need no writable temp directory. See
[Installation](Installation) for the binary download.

## Operational notes

* **Logging** — yacron2 logs to stdout/stderr; collect logs at the platform
  level (`kubectl logs`, the container runtime's log driver). Adjust verbosity
  with `-l/--log-level` (default `INFO`) or a `logging:` config section.
* **Shutdown** — yacron2 installs handlers for `SIGINT` and `SIGTERM` and shuts
  down gracefully, so the default pod termination path works without extra
  configuration.
* **Validate before deploy** — `yacron2 -c <path> --validate-config` parses the
  config and exits, useful as a CI/pre-deploy gate. See [Command-Line
  Reference](CLI-Reference).
* **Config not found** — when `-c` is left at its default `/etc/yacron2.d` and the
  path does not exist, yacron2 prints an error and exits non-zero. Ensure the
  config volume is mounted there.
