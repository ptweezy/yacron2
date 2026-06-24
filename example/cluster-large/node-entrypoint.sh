#!/bin/sh
# Entry point for the 10-node demo: build this node's cluster config from
# environment variables, then exec yacron2. This keeps the compose file small
# (no ten hand-written peer lists): every node shares one CLUSTER_HOSTS list and
# excludes itself by NODE_NAME.
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

DIR="${YACRON2_DIR:-/tmp/yacron2.d}"
mkdir -p "$DIR"
# the job set is mounted read-only at /config/jobs.yaml; yacron2 needs every
# config file in one directory, so copy it next to the generated cluster.yaml.
cp /config/jobs.yaml "$DIR/jobs.yaml"

{
  echo "cluster:"
  echo "  listen: \"0.0.0.0:8443\""
  echo "  tls:"
  echo "    ca: /certs/ca.pem"
  echo "    cert: /certs/${NODE_NAME}.pem"
  echo "    key: /certs/${NODE_NAME}.key"
  echo "  nodeName: ${NODE_NAME}"
  echo "  electLeader: ${ELECT_LEADER:-true}"
  echo "  distribution: ${DISTRIBUTION:-spread}"
  echo "  interval: ${INTERVAL:-10}"
  echo "  driftAfter: ${DRIFT_AFTER:-2}"
  echo "  peers:"
  # split CLUSTER_HOSTS on commas/spaces, drop our own entry
  for hp in $(echo "$CLUSTER_HOSTS" | tr ',' ' '); do
    [ -z "$hp" ] && continue
    [ "${hp%%:*}" = "$NODE_NAME" ] && continue
    echo "    - host: ${hp}"
  done
} > "$DIR/cluster.yaml"

echo "[entrypoint] ${NODE_NAME}: wrote $DIR/cluster.yaml"
exec yacron2 -c "$DIR"
