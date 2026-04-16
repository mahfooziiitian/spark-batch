#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Prerequisites ────────────────────────────────────────────────────────────
for cmd in terraform az; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "ERROR: '$cmd' is not installed. Please install it before running this script."
    exit 1
  fi
done

# ── Terraform ────────────────────────────────────────────────────────────────
cd "$SCRIPT_DIR/infra"

echo "==> Running terraform init …"
terraform init -input=false

echo "==> Running terraform apply …"
terraform apply -auto-approve -input=false

# ── Extract outputs ──────────────────────────────────────────────────────────
STORAGE_ACCOUNT=$(terraform output -raw storage_account_name)
STORAGE_KEY=$(terraform output -raw storage_account_key)
CONTAINER=$(terraform output -raw container_name)
CLIENT_ID=$(terraform output -raw client_id)
CLIENT_SECRET=$(terraform output -raw client_secret)
TENANT_ID=$(terraform output -raw tenant_id)

# ── Upload sample data ──────────────────────────────────────────────────────
echo "==> Uploading sample data to Azure Blob Storage …"
az storage blob upload \
  --account-name "$STORAGE_ACCOUNT" \
  --account-key  "$STORAGE_KEY" \
  --container-name "$CONTAINER" \
  --name "input/sample.csv" \
  --file "$SCRIPT_DIR/data/sample.csv" \
  --overwrite

echo ""
echo "==> Setup complete. Export the following environment variables:"
echo ""
echo "export AZURE_STORAGE_ACCOUNT=\"$STORAGE_ACCOUNT\""
echo "export AZURE_STORAGE_KEY=\"$STORAGE_KEY\""
echo "export AZURE_CONTAINER=\"$CONTAINER\""
echo "export AZURE_CLIENT_ID=\"$CLIENT_ID\""
echo "export AZURE_CLIENT_SECRET=\"$CLIENT_SECRET\""
echo "export AZURE_TENANT_ID=\"$TENANT_ID\""
