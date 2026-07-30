#!/usr/bin/env bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# Spark Connect: Start a local Spark Connect server for remote session access
#
# Usage:
#   ./scripts/spark-connect-server.sh          # Start server
#   ./scripts/spark-connect-server.sh stop     # Stop server
#
# After starting, connect from Python:
#   from pys_json import get_spark_connect
#   spark = get_spark_connect("my-app")
#
# Environment variables:
#   SPARK_HOME        - Path to Spark installation
#   JAVA_HOME_17      - Path to Java 17
#   CONNECT_PORT      - gRPC port (default: 15002)
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -n "${JAVA_HOME_17:-}" ]]; then
    export JAVA_HOME="${JAVA_HOME_17}"
fi

CONNECT_PORT="${CONNECT_PORT:-15002}"

if [[ -z "${SPARK_HOME:-}" ]]; then
    SPARK_HOME="$(python -c 'import pyspark; print(pyspark.__path__[0])' 2>/dev/null || echo "")"
fi

if [[ -z "${SPARK_HOME}" ]]; then
    echo "✗ SPARK_HOME not set and pyspark not found"
    exit 1
fi

# Stop command
if [[ "${1:-}" == "stop" ]]; then
    echo "Stopping Spark Connect server..."
    "${SPARK_HOME}/sbin/stop-connect-server.sh" 2>/dev/null || true
    echo "✓ Server stopped"
    exit 0
fi

echo "╭─────────────────────────────────────────────────╮"
echo "│  Spark Connect Server                           │"
echo "├─────────────────────────────────────────────────┤"
echo "│  SPARK_HOME: ${SPARK_HOME}"
echo "│  Port:       ${CONNECT_PORT}"
echo "│  URL:        sc://localhost:${CONNECT_PORT}"
echo "╰─────────────────────────────────────────────────╯"

exec "${SPARK_HOME}/sbin/start-connect-server.sh" \
    --packages org.apache.spark:spark-connect_2.13:4.2.0 \
    --conf "spark.connect.grpc.binding.port=${CONNECT_PORT}"
