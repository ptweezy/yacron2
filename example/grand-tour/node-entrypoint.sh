#!/bin/sh
# Entry point for the Meridian grand-tour cluster: build this node's `cluster:`
# section from environment variables, assemble the config directory, then exec
# yacron2. Keeps the compose file small (no nine hand-written peer lists): every
# node shares one CLUSTER_HOSTS list and excludes itself by NODE_NAME.
#
# Env in:
#   NODE_NAME      this node's name (also its cert name and TLS SAN)
#   CLUSTER_HOSTS  comma/space list of ALL members as host:port (self included;
#                  it is filtered out below)
#   BACKEND        gossip | filesystem       (default gossip)
#   DISTRIBUTION   single-leader | spread    (default spread; gossip only)
#   ELECT_LEADER   true | false              (default true)
#   INTERVAL       poll seconds              (default 10)
#   DRIFT_AFTER    rounds before "drifted"   (default 2)
#
# BACKEND=gossip     each node serves a mutual-TLS /peer endpoint and the quorum
#                    elects a leader; distribution: spread gives each Leader job
#                    its own owner node. Needs the per-node certs from certgen.
# BACKEND=filesystem leader election is a single flock-guarded TTL lease file on
#                    the shared state volume — no /peer endpoint, no certs, no
#                    peer list. A single lease holder cannot fan jobs out, so
#                    this backend is single-leader only (no spread).
set -eu

# Validate every value spliced into a file path or the generated YAML, so a
# stray/hostile env value cannot traverse paths (NODE_NAME -> the cert/key path)
# or inject extra config keys (host tokens -> the cluster.yaml).
# Allowed: letters, digits, '.', '_', '-', and ':' (for host:port); reject '..'.
_valid() {
  case "$1" in
    "" | *..* | *[!A-Za-z0-9._:-]*) return 1 ;;
    *) return 0 ;;
  esac
}

if ! _valid "$NODE_NAME" || [ "${NODE_NAME#*:}" != "$NODE_NAME" ]; then
  echo "[entrypoint] invalid NODE_NAME: ${NODE_NAME}" >&2
  exit 1
fi

BACKEND="${BACKEND:-gossip}"
case "$BACKEND" in
  gossip | filesystem) ;;
  *) echo "[entrypoint] invalid BACKEND: ${BACKEND} (want gossip|filesystem)" >&2; exit 1 ;;
esac

DIR="${YACRON2_DIR:-/tmp/yacron2.d}"
mkdir -p "$DIR"
# The config is mounted read-only at /config; yacron2 needs every config file in
# one directory alongside the generated cluster.yaml, so copy the loadable files
# next to it. platform.yaml pulls in _defaults.yaml via `include:` (resolved
# relative to this directory), and legacy.crontab is loaded as a classic
# crontab. Absolute-path references (env_file /config/platform.env, the
# /config/secrets mount) are read straight from the mount and are not copied.
cp /config/platform.yaml "$DIR/platform.yaml"
cp /config/_defaults.yaml "$DIR/_defaults.yaml"
cp /config/legacy.crontab "$DIR/legacy.crontab"

if [ "$BACKEND" = "filesystem" ]; then
  # Shared-mount lease election: no listen/tls/peers, and single-leader only.
  # deploymentId matches state.deploymentId so the election lease and the state
  # store share one namespace on the same mount.
  {
    echo "cluster:"
    echo "  backend: filesystem"
    echo "  nodeName: \"${NODE_NAME}\""
    echo "  interval: ${INTERVAL:-10}"
    echo "  driftAfter: ${DRIFT_AFTER:-2}"
    echo "  filesystem:"
    echo "    path: /var/lib/yacron2/state"
    echo "    electionName: cluster/leader"
    echo "    ttl: 15"
    echo "    deploymentId: meridian-prod"
    echo "    topology: shared"
  } > "$DIR/cluster.yaml"
else
  # Gossip mesh: mutual-TLS /peer endpoint + quorum election, optionally spread.
  {
    echo "cluster:"
    echo "  listen: \"0.0.0.0:8443\""
    echo "  tls:"
    echo "    ca: /certs/ca.pem"
    echo "    cert: \"/certs/${NODE_NAME}.pem\""
    echo "    key: \"/certs/${NODE_NAME}.key\""
    echo "  nodeName: \"${NODE_NAME}\""
    echo "  electLeader: ${ELECT_LEADER:-true}"
    echo "  distribution: ${DISTRIBUTION:-spread}"
    echo "  interval: ${INTERVAL:-10}"
    echo "  driftAfter: ${DRIFT_AFTER:-2}"
    echo "  peers:"
    # split CLUSTER_HOSTS on commas/spaces, drop our own entry
    for hp in $(echo "$CLUSTER_HOSTS" | tr ',' ' '); do
      [ -z "$hp" ] && continue
      [ "${hp%%:*}" = "$NODE_NAME" ] && continue
      if ! _valid "$hp"; then
        echo "[entrypoint] skipping invalid CLUSTER_HOSTS entry: ${hp}" >&2
        continue
      fi
      echo "    - host: \"${hp}\""
    done
  } > "$DIR/cluster.yaml"
fi

echo "[entrypoint] ${NODE_NAME}: wrote $DIR/cluster.yaml (backend=${BACKEND})"
exec yacron2 -c "$DIR"
