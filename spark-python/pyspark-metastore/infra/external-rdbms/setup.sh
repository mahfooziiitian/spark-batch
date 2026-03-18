#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_TYPE="${1:-postgres}"

echo "=== External RDBMS Metastore Setup ($DB_TYPE) ==="

cd "$SCRIPT_DIR"

case "$DB_TYPE" in
    postgres)
        docker compose --profile postgres up -d
        echo "Waiting for PostgreSQL..."
        "$SCRIPT_DIR/../common/health-check.sh" postgres
        echo ""
        echo "Connection URL: jdbc:postgresql://localhost:5432/${METASTORE_DB_NAME:-metastore_db}"
        ;;
    mysql)
        docker compose --profile mysql up -d
        echo "Waiting for MySQL..."
        "$SCRIPT_DIR/../common/health-check.sh" mysql
        echo ""
        echo "Connection URL: jdbc:mysql://localhost:3306/${METASTORE_DB_NAME:-metastore_db}"
        ;;
    *)
        echo "Usage: $0 [postgres|mysql]"
        exit 1
        ;;
esac

echo ""
echo "Run example:"
echo "  docker compose exec spark python src/metastore/external/spark_external_metastore.py"
