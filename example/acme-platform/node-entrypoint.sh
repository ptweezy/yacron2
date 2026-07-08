#!/bin/sh
# Entry point for the "ACME Orders" cluster demo: build this node's cluster
# config from environment variables, then exec cronstable. Keeps the compose file
# small (no five hand-written peer lists): every node shares one CLUSTER_HOSTS
# list and excludes itself by NODE_NAME. Same shape as the cluster-large demo.
#
# Env in:
#   NODE_NAME      this node's name (also its cert name and TLS SAN)
#   CLUSTER_HOSTS  comma/space list of ALL members as host:port (self included;
#                  it is filtered out below)
#   DISTRIBUTION   single-leader | spread   (default spread)
#   ELECT_LEADER   true | false             (default true)
#   INTERVAL       poll seconds             (default 10)
#   DRIFT_AFTER    rounds before "drifted"  (default 2)
set -eu

# Validate every value that is spliced into a file path or the generated YAML,
# so a stray/hostile env value cannot traverse paths (NODE_NAME -> the loaded
# cert/key path) or inject extra config keys (host tokens -> the cluster.yaml).
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

DIR="${CRONSTABLE_DIR:-/tmp/cronstable.d}"
mkdir -p "$DIR"
# the job set is mounted read-only at /config/jobs.yaml; cronstable needs every
# config file in one directory, so copy it next to the generated cluster.yaml.
# (env_file / report sinks referenced by absolute path, e.g. /config/acme.env,
# are read straight from the mount and do not need copying here.)
cp /config/jobs.yaml "$DIR/jobs.yaml"

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

echo "[entrypoint] ${NODE_NAME}: wrote $DIR/cluster.yaml"
exec cronstable -c "$DIR"
