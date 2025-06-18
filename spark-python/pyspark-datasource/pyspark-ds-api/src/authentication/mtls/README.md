# Mtls

Mutual TLS (mTLS) authentication in Python is used to ensure both the client and server authenticate each other using certificates. This is useful in high-security contexts like internal microservices communication or financial APIs.

## 🔐 What You Need

1. Server certificate & private key
2. Client certificate & private key
3. CA certificate (used to sign the client and server certs)

## ✅ 1. Assumptions

You've already created:

1. ca.crt – CA certificate
2. server.crt – Server certificate
3. server.key – Server private key
4. client.crt, client.key – Client certificate & key

## 1. Generate CA certificates

This CA will be used to sign both server and client certificates.

### Generate private key for CA

```bash
openssl genrsa -out certificates/server.key 2048
```

### Generate a self-signed root CA certificate (valid for 10 years)

```bash
openssl req -x509 -new -nodes -key certificates/ca.key -sha256 -days 3650 -out certificates/ca.pem \
  -subj "/C=US/ST=California/L=San Francisco/O=MyOrg CA/OU=Dev/CN=MyRootCA"

# 2. Create CSR using the SAN config
openssl req -new -key certificates/server.key -out certificates/server.csr -config certificates/openssl-san.cnf
```

## 2. Create Server Private Key and Certificate Signing Request (CSR)

```bash

# Generate CSR
openssl req -new -key server.key -out server.csr \
  -subj "/C=US/ST=California/L=San Francisco/O=MyOrg Server/OU=Dev/CN=localhost"
```

## 3. Create a Server Certificate Signed by Your CA

This signs the server CSR with your CA key.

```bash
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out server.pem -days 365 -sha256
```

## Step 4: (Optional) Verify the Server Certificate

```bash
openssl verify -CAfile ca.pem server.pem
```

## 5. Create Client Certificate for mTLS

### ✅ Step 1: Generate Client Private Key

```bash
openssl genrsa -out client.key 2048
```

### ✅ Step 2: Generate a Certificate Signing Request (CSR) for the Client

```bash
openssl req -new -key client.key -out client.csr \
  -subj "/C=US/ST=California/L=San Francisco/O=MyOrg Client/OU=Dev/CN=client1"
```

### ✅ Step 3: Sign the Client Certificate with Your CA

```bash
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out client.pem -days 365 -sha256
```

### ✅ Step 4: (Optional) Verify the Client Certificate

```bash
openssl verify -CAfile ca.pem client.pem
```
