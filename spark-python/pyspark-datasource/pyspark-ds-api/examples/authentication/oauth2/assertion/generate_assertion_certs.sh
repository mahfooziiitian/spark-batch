#!/usr/bin/env bash
# Generates the RSA key pair used by the oauth2_assertion (RFC 7523 JWT
# bearer) example: a private key that signs the assertion and a self-signed
# public certificate the mock token endpoint uses to verify it.
set -euo pipefail

DATA_HOME="${DATA_HOME:-/tmp}"
CERTS_DIR="${DATA_HOME}/rest_api_ds/certs"
mkdir -p "${CERTS_DIR}"
cd "${CERTS_DIR}"

echo "[1/2] Generating assertion signing key..."
openssl genrsa -out assertion_client.key 2048

echo "[2/2] Generating self-signed public certificate..."
openssl req -x509 -new -nodes -key assertion_client.key -sha256 -days 365 \
  -out assertion_client.pem \
  -subj "/C=US/ST=CA/L=SanFrancisco/O=MyOrg/CN=assertion-client"

echo "✅ Assertion certs generated in ${CERTS_DIR}:"
ls -1 assertion_client.*
