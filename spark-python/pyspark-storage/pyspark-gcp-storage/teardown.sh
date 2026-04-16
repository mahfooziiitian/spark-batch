#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "$SCRIPT_DIR/infra"
terraform destroy -auto-approve

rm -f "$SCRIPT_DIR/infra/sa-key.json"
echo "Infrastructure destroyed and sa-key.json removed."
