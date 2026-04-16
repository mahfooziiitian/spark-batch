#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── prerequisites ───────────────────────────────────────────────
for cmd in terraform gcloud gsutil; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "ERROR: '$cmd' is not installed. Please install it first." >&2
    exit 1
  fi
done

# ── terraform ───────────────────────────────────────────────────
cd "$SCRIPT_DIR/infra"
terraform init
terraform apply

BUCKET_NAME=$(terraform output -raw bucket_name)
SA_KEY_B64=$(terraform output -raw service_account_key)

# ── save service account key ────────────────────────────────────
echo "$SA_KEY_B64" | base64 --decode > "$SCRIPT_DIR/infra/sa-key.json"
echo "Service account key saved to $SCRIPT_DIR/infra/sa-key.json"

# ── upload sample data ──────────────────────────────────────────
gsutil cp "$SCRIPT_DIR/data/sample.csv" "gs://${BUCKET_NAME}/input/sample.csv"
echo "Sample data uploaded to gs://${BUCKET_NAME}/input/sample.csv"

# ── print environment exports ───────────────────────────────────
cat <<EOF

Run the following to configure your environment:

  export GOOGLE_APPLICATION_CREDENTIALS="$SCRIPT_DIR/infra/sa-key.json"
  export GCS_BUCKET="$BUCKET_NAME"
  export INPUT_PATH="gs://${BUCKET_NAME}/input/sample.csv"
  export OUTPUT_PATH="gs://${BUCKET_NAME}/output/department_salaries"

EOF
