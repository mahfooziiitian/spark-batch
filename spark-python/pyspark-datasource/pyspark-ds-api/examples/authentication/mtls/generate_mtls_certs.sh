#!/bin/bash

set -e

# Create output directory under DATA_HOME/rest_api_ds/certs (all example
# input/output data and databases live under this shared location).
DATA_HOME="${DATA_HOME:-/tmp}"
CERTS_DIR="${DATA_HOME}/rest_api_ds/certs"
mkdir -p "${CERTS_DIR}"
cd "${CERTS_DIR}"

echo "[1/7] Generating CA private key..."
openssl genrsa -out ca.key 4096

echo "[2/7] Generating CA certificate..."
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 -out ca.pem \
  -subj "/C=US/ST=CA/L=SanFrancisco/O=MyOrg/CN=MyRootCA"

# Config file for server SAN
cat > server-san.cnf <<EOF
[ req ]
default_bits       = 2048
prompt             = no
default_md         = sha256
distinguished_name = dn
req_extensions     = req_ext

[ dn ]
C = US
ST = CA
L = SanFrancisco
O = MyOrg
CN = localhost

[ req_ext ]
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
EOF

echo "[3/7] Generating server private key..."
openssl genrsa -out server.key 2048

echo "[4/7] Creating server CSR..."
openssl req -new -key server.key -out server.csr -config server-san.cnf

echo "[5/7] Signing server certificate with CA..."
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out server.pem -days 365 -sha256 -extfile server-san.cnf -extensions req_ext

echo "[6/7] Generating client private key..."
openssl genrsa -out client.key 2048

echo "[7/7] Creating and signing client certificate..."
openssl req -new -key client.key -out client.csr \
  -subj "/C=US/ST=CA/L=SanFrancisco/O=MyOrg/CN=client"

openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out client.pem -days 365 -sha256

# Clean up
rm *.csr *.srl server-san.cnf

echo "✅ Certificates generated in ${CERTS_DIR}:"
ls -1
