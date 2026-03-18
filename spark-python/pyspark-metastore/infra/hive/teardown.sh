#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Tearing Down Hive Metastore ==="
cd "$SCRIPT_DIR"
docker compose down -v
echo "✓ All services stopped and volumes removed"
