#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Tearing Down External RDBMS ==="
cd "$SCRIPT_DIR"
docker compose --profile postgres --profile mysql down -v
echo "✓ All services stopped and volumes removed"
