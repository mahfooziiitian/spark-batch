#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Empty the S3 bucket (including all versions & delete markers) ────────────
BUCKET_NAME=$(terraform -chdir="$SCRIPT_DIR/infra" output -raw bucket_name 2>/dev/null || true)

if [[ -n "$BUCKET_NAME" && "$BUCKET_NAME" != *"Warning"* ]]; then
  echo "==> Emptying bucket s3://${BUCKET_NAME} (all versions) ..."
  aws s3api list-object-versions \
    --bucket "$BUCKET_NAME" \
    --query '{Objects: [].{Key:Key,VersionId:VersionId}}' \
    --output json 2>/dev/null \
  | python3 -c "
import sys, json
data = json.load(sys.stdin)
objects = data.get('Objects')
if objects:
    print(json.dumps({'Objects': objects, 'Quiet': True}))
" > /tmp/_s3_delete.json 2>/dev/null && \
  if [[ -s /tmp/_s3_delete.json ]]; then
    aws s3api delete-objects --bucket "$BUCKET_NAME" --delete "file:///tmp/_s3_delete.json"
  fi
  rm -f /tmp/_s3_delete.json

  aws s3api list-object-versions \
    --bucket "$BUCKET_NAME" \
    --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' \
    --output json 2>/dev/null \
  | python3 -c "
import sys, json
data = json.load(sys.stdin)
objects = data.get('Objects')
if objects:
    print(json.dumps({'Objects': objects, 'Quiet': True}))
" > /tmp/_s3_delete.json 2>/dev/null && \
  if [[ -s /tmp/_s3_delete.json ]]; then
    aws s3api delete-objects --bucket "$BUCKET_NAME" --delete "file:///tmp/_s3_delete.json"
  fi
  rm -f /tmp/_s3_delete.json
fi

# ── Destroy infrastructure ───────────────────────────────────────────────────
echo "==> Destroying Terraform resources..."
terraform -chdir="$SCRIPT_DIR/infra" destroy -auto-approve
