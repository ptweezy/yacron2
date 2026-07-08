#!/bin/sh
# Generate a throwaway cluster CA and one leaf cert per node for the
# docker-compose-pulse-cluster.yml demo. Run automatically by the `certgen`
# service into the shared `pulse-certs` volume before the nodes start.
#
# THESE CERTS ARE FOR LOCAL EXPERIMENTATION ONLY — a single CA key sitting in a
# volume, 10-year validity, owner-only keys. For real deployments provision
# per-node certificates from your own PKI (cert-manager, a service mesh, an
# internal CA); cronstable only consumes them.
#
# Each node's cert carries its service name as a Subject Alternative Name, which
# is what the mutual-TLS hostname check pins against when a peer connects to
# e.g. https://cronstable-b:8443/peer.
set -eu

CERTS=/certs
NODES="cronstable-a cronstable-b cronstable-c"

if [ -f "$CERTS/ca.pem" ]; then
  echo "certs already present in $CERTS — leaving them in place."
  echo "To regenerate: docker compose -f docker-compose-pulse-cluster.yml down -v"
  exit 0
fi

# Alpine ships without openssl; install it on first run.
if ! command -v openssl >/dev/null 2>&1; then
  echo "installing openssl..."
  apk add --no-cache openssl >/dev/null
fi

echo "generating cluster CA..."
# basicConstraints + keyUsage are required: OpenSSL 3.x strict verification
# rejects a CA cert that lacks the keyCertSign key-usage extension.
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
  -keyout "$CERTS/ca.key" -out "$CERTS/ca.pem" \
  -subj "/CN=cronstable-pulse-cluster-ca" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"

for n in $NODES; do
  echo "generating certificate for $n..."
  openssl req -newkey rsa:2048 -nodes \
    -keyout "$CERTS/$n.key" -out "/tmp/$n.csr" \
    -subj "/CN=$n"
  # SAN = the service name (peer hostname verification pins against it); the
  # cert is used both to serve /peer (serverAuth) and to authenticate as a
  # client when polling peers (clientAuth).
  cat > "/tmp/$n.ext" <<EOF
subjectAltName=DNS:$n
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
EOF
  openssl x509 -req -in "/tmp/$n.csr" \
    -CA "$CERTS/ca.pem" -CAkey "$CERTS/ca.key" -CAcreateserial \
    -out "$CERTS/$n.pem" -days 3650 -extfile "/tmp/$n.ext"
done

# The cronstable containers run as uid 65534 (nobody) and must read their own leaf
# key, so hand the private keys to that uid and keep them owner-only (0600). The
# CA signing key (ca.key) is the crown jewel — anyone who can read it can mint a
# cert for ANY node/SAN and impersonate a peer. In production never ship ca.key
# to the nodes; they need only their leaf key+cert and the CA public cert.
chmod 0644 "$CERTS"/*.pem
chown 65534:65534 "$CERTS"/*.key
chmod 0600 "$CERTS"/*.key

echo "done. generated:"
ls -l "$CERTS"
