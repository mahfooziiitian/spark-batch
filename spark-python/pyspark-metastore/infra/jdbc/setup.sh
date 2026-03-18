#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== JDBC (PostgreSQL) Setup ==="

docker compose up -d

echo "Waiting for PostgreSQL to be healthy..."
until docker inspect --format='{{.State.Health.Status}}' jdbc-postgres 2>/dev/null | grep -q "healthy"; do
    sleep 2
done

echo ""
echo "=== PostgreSQL Ready ==="
echo ""
echo "Connection info:"
echo "  Host:     localhost"
echo "  Port:     ${JDBC_PORT:-5432}"
echo "  Database: ${JDBC_DB_NAME:-appdb}"
echo "  User:     ${JDBC_USER:-appuser}"
echo "  Password: ${JDBC_PASSWORD:-apppassword}"
echo "  JDBC URL: jdbc:postgresql://localhost:${JDBC_PORT:-5432}/${JDBC_DB_NAME:-appdb}"
echo ""
echo "Example commands:"
echo "  psql -h localhost -p ${JDBC_PORT:-5432} -U ${JDBC_USER:-appuser} -d ${JDBC_DB_NAME:-appdb}"
echo "  docker compose exec spark bash"
echo "  docker compose exec spark python src/metastore/jdbc/jdbc_metastore.py"
