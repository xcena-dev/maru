#!/bin/bash
# gen_test_certs.sh — Generate test CA + server/instance certs for mTLS
#
# Usage:
#   ./gen_test_certs.sh [output_dir]
#
# Generates:
#   ca.key / ca.cert                    — Root CA
#   server.key / server.cert            — Maru Server (SAN: spiffe://maru.cluster/role/server)
#   prefill.key / prefill.cert          — Prefill instance (SAN: spiffe://maru.cluster/node-1/role/prefill)
#   decode.key / decode.cert            — Decode instance (SAN: spiffe://maru.cluster/node-2/role/decode)
#
# All certs use spiffe:// URI SAN format for SPIRE compatibility.
# These are TEST ONLY certs — do not use in production.

set -euo pipefail

OUT_DIR="${1:-./test_certs}"
TRUST_DOMAIN="maru.cluster"
DAYS=365

mkdir -p "$OUT_DIR"

echo "=== Generating test certs in $OUT_DIR ==="

# --- CA ---
echo "[1/4] CA"
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 \
  -out "$OUT_DIR/ca.key" 2>/dev/null

openssl req -new -x509 -key "$OUT_DIR/ca.key" \
  -out "$OUT_DIR/ca.cert" \
  -days "$DAYS" \
  -subj "/CN=Maru Test CA" 2>/dev/null

# --- Helper function ---
gen_cert() {
  local name=$1
  local spiffe_id=$2
  local cn=$3

  echo "[*] $name (SAN: $spiffe_id)"

  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 \
    -out "$OUT_DIR/${name}.key" 2>/dev/null

  openssl req -new -key "$OUT_DIR/${name}.key" \
    -out "$OUT_DIR/${name}.csr" \
    -subj "/CN=${cn}" 2>/dev/null

  openssl x509 -req -in "$OUT_DIR/${name}.csr" \
    -CA "$OUT_DIR/ca.cert" -CAkey "$OUT_DIR/ca.key" \
    -CAcreateserial \
    -out "$OUT_DIR/${name}.cert" \
    -days "$DAYS" \
    -extfile <(cat <<EOF
subjectAltName = URI:${spiffe_id}, DNS:localhost, IP:127.0.0.1
keyUsage = digitalSignature
extendedKeyUsage = serverAuth, clientAuth
EOF
    ) 2>/dev/null

  rm -f "$OUT_DIR/${name}.csr"
}

# --- Server cert ---
echo "[2/4] Server"
gen_cert "server" "spiffe://${TRUST_DOMAIN}/role/server" "maru-server"

# --- Prefill instance cert ---
echo "[3/4] Prefill"
gen_cert "prefill" "spiffe://${TRUST_DOMAIN}/node-1/role/prefill" "prefill-0"

# --- Decode instance cert ---
echo "[4/4] Decode"
gen_cert "decode" "spiffe://${TRUST_DOMAIN}/node-2/role/decode" "decode-0"

# --- Cleanup serial file ---
rm -f "$OUT_DIR/ca.srl"

# --- Verify ---
echo ""
echo "=== Generated files ==="
ls -la "$OUT_DIR"/*.key "$OUT_DIR"/*.cert

echo ""
echo "=== SAN verification ==="
for cert in server prefill decode; do
  san=$(openssl x509 -in "$OUT_DIR/${cert}.cert" -noout -ext subjectAltName 2>/dev/null | grep -o 'URI:.*')
  echo "  ${cert}.cert: ${san}"
done

echo ""
echo "=== Chain verification ==="
for cert in server prefill decode; do
  if openssl verify -CAfile "$OUT_DIR/ca.cert" "$OUT_DIR/${cert}.cert" >/dev/null 2>&1; then
    echo "  ${cert}.cert: OK"
  else
    echo "  ${cert}.cert: FAILED"
  fi
done

echo ""
echo "Done. Trust bundle: $OUT_DIR/ca.cert"
