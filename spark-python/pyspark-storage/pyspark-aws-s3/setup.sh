#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Prerequisites ────────────────────────────────────────────────────────────
for cmd in terraform aws; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "ERROR: '$cmd' is required but not installed." >&2
    exit 1
  fi
done

# ── Provision infrastructure ─────────────────────────────────────────────────
echo "==> Initializing Terraform..."
terraform -chdir="$SCRIPT_DIR/infra" init

echo "==> Applying Terraform configuration..."
terraform -chdir="$SCRIPT_DIR/infra" apply -auto-approve

# ── Extract outputs ──────────────────────────────────────────────────────────
BUCKET_NAME=$(terraform -chdir="$SCRIPT_DIR/infra" output -raw bucket_name)
ACCESS_KEY_ID=$(terraform -chdir="$SCRIPT_DIR/infra" output -raw access_key_id)
SECRET_ACCESS_KEY=$(terraform -chdir="$SCRIPT_DIR/infra" output -raw secret_access_key)

# ── Upload sample data ──────────────────────────────────────────────────────
echo "==> Uploading sample.csv to s3://${BUCKET_NAME}/input/sample.csv ..."
aws s3 cp "$SCRIPT_DIR/data/sample.csv" "s3://${BUCKET_NAME}/input/sample.csv"

# ── Print env var exports ────────────────────────────────────────────────────
echo ""
echo "===== Setup complete! ====="
echo ""
echo "Set the following environment variables before running the PySpark scripts:"
echo ""
echo "  export AWS_ACCESS_KEY_ID=${ACCESS_KEY_ID}"
echo "  export AWS_SECRET_ACCESS_KEY=${SECRET_ACCESS_KEY}"
echo "  export INPUT_PATH=s3a://${BUCKET_NAME}/input/sample.csv"
echo "  export OUTPUT_PATH=s3a://${BUCKET_NAME}/output"
