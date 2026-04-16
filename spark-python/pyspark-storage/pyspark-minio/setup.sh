#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v docker &>/dev/null; then
    echo "Error: docker is not installed or not in PATH" >&2
    exit 1
fi

echo "Starting MinIO..."
docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d

echo "Waiting for MinIO to be healthy..."
until docker compose -f "$SCRIPT_DIR/docker-compose.yml" ps minio --format json \
    | grep -q '"healthy"'; do
    sleep 2
done

echo ""
echo "MinIO is ready!"
echo ""
echo "Export the following environment variables:"
echo ""
echo "  export MINIO_ENDPOINT=http://localhost:9000"
echo "  export MINIO_ACCESS_KEY=minioadmin"
echo "  export MINIO_SECRET_KEY=minioadmin"
