#!/bin/sh
# Generate a throwaway cluster CA and one leaf cert per node for the 10-node
# docker-compose-cluster-large.yml demo. Run by the `certgen` service into the
# shared volume before the nodes start. Same as the 3-node demo's gen-certs.sh,
# just with ten node names.
#
# FOR LOCAL EXPERIMENTATION ONLY. Real deployments provision per-node certs from
# their own PKI; yacron2 only consumes them.
set -eu

CERTS=/certs
NODES="yacron-a yacron-b yacron-c yacron-d yacron-e \
       yacron-f yacron-g yacron-h yacron-i yacron-j"

if [ -f "$CERTS/ca.pem" ]; then
  echo "certs already present in $CERTS — leaving them in place."
  echo "To regenerate: docker compose -f docker-compose-cluster-large.yml down -v"
  exit 0
fi

if ! command -v openssl >/dev/null 2>&1; then
  echo "installing openssl..."
  apk add --no-cache openssl >/dev/null
fi

echo "generating cluster CA..."
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
  -keyout "$CERTS/ca.key" -out "$CERTS/ca.pem" \
  -subj "/CN=yacron2-cluster-ca" \
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

# yacron2 runs as uid 65534 (nobody) and must read these. World-readable is fine
# for this throwaway demo CA.
chmod 0644 "$CERTS"/*.pem "$CERTS"/*.key
echo "done. generated certs for: $NODES"
