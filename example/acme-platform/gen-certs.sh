#!/bin/sh
# Generate a throwaway cluster CA and one leaf cert per node for the "ACME
# Orders" cluster demo (docker-compose-acme.yml). Run by the `certgen` service
# into the shared volume before the nodes start. Same as the other cluster
# demos' gen-certs.sh, just with the five ACME node names.
#
# FOR LOCAL EXPERIMENTATION ONLY. Real deployments provision per-node certs from
# their own PKI; cronstable only consumes them.
set -eu

CERTS=/certs
NODES="cronstable-a cronstable-b cronstable-c cronstable-d cronstable-e"

if [ -f "$CERTS/ca.pem" ]; then
  echo "certs already present in $CERTS — leaving them in place."
  echo "To regenerate: docker compose -f docker-compose-acme.yml down -v"
  exit 0
fi

if ! command -v openssl >/dev/null 2>&1; then
  echo "installing openssl..."
  apk add --no-cache openssl >/dev/null
fi

echo "generating cluster CA..."
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
  -keyout "$CERTS/ca.key" -out "$CERTS/ca.pem" \
  -subj "/CN=cronstable-acme-ca" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"

for n in $NODES; do
  echo "generating certificate for $n..."
  openssl req -newkey rsa:2048 -nodes \
    -keyout "$CERTS/$n.key" -out "/tmp/$n.csr" \
    -subj "/CN=$n"
  cat > "/tmp/$n.ext" <<EOF
subjectAltName=DNS:$n
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
EOF
  openssl x509 -req -in "/tmp/$n.csr" \
    -CA "$CERTS/ca.pem" -CAkey "$CERTS/ca.key" -CAcreateserial \
    -out "$CERTS/$n.pem" -days 3650 -extfile "/tmp/$n.ext"
done

# Lock down permissions. cronstable runs as uid 65534 (nobody) and must read its
# own leaf key, so hand the private keys to that uid and keep them owner-only
# (0600) instead of world-readable. The CA SIGNING key (ca.key) is the sensitive
# one: whoever can read it can mint a cert for ANY node/SAN and impersonate a
# peer. Public material (ca.pem, node *.pem) is meant to be shared and stays
# 0644.
#
# DEMO-ONLY CAVEAT: ca.key lives in this shared volume only because the demo
# mints certs in place. In production never ship/mount the CA signing key to the
# nodes; they need only their own leaf key+cert and the CA *public* cert
# (ca.pem); ca.key belongs only on the (offline) signer.
chmod 0644 "$CERTS"/*.pem
chown 65534:65534 "$CERTS"/*.key
chmod 0600 "$CERTS"/*.key
echo "done. generated certs for: $NODES"
