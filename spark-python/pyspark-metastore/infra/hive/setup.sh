#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Hive Metastore Setup ==="

# Start services
cd "$SCRIPT_DIR"
docker compose up -d

# Wait for PostgreSQL
echo "Waiting for PostgreSQL..."
"$SCRIPT_DIR/../common/health-check.sh" postgres

# Wait for Hive Metastore
echo "Waiting for Hive Metastore..."
"$SCRIPT_DIR/../common/health-check.sh" hive-metastore

echo ""
echo "=== Hive Metastore Ready ==="
echo "Metastore URI:  thrift://localhost:${HIVE_METASTORE_PORT:-9083}"
echo "PostgreSQL:     localhost:${HIVE_METASTORE_DB_PORT:-5432}"
echo ""
echo "Run example:"
echo "  docker compose exec spark python src/metastore/hive/remote/hive_metastore.py"
echo ""
echo "Stop:"
echo "  docker compose down"
