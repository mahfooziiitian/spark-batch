#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check required tools
for cmd in docker aws; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "Error: '$cmd' is not installed or not in PATH." >&2
        exit 1
    fi
done

# Start LocalStack
echo "Starting LocalStack..."
docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d

# Wait for LocalStack to be healthy
echo "Waiting for LocalStack to be healthy..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:4566/_localstack/health &>/dev/null; then
        echo "LocalStack is healthy."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "Error: LocalStack did not become healthy in time." >&2
        exit 1
    fi
    sleep 2
done

# Set AWS credentials for LocalStack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

# Create S3 bucket
echo "Creating S3 bucket..."
aws --endpoint-url=http://localhost:4566 s3 mb s3://spark-demo

# Upload sample data
echo "Uploading sample data..."
aws --endpoint-url=http://localhost:4566 s3 cp "$SCRIPT_DIR/data/sample.csv" s3://spark-demo/input/sample.csv

echo ""
echo "Setup complete! Run the following exports in your shell before running PySpark scripts:"
echo ""
echo "  export AWS_ACCESS_KEY_ID=test"
echo "  export AWS_SECRET_ACCESS_KEY=test"
echo "  export AWS_DEFAULT_REGION=us-east-1"
