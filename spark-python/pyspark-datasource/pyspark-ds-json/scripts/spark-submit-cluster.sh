#!/usr/bin/env bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# spark-submit: Run against a remote Spark cluster (standalone/YARN/K8s)
#
# Usage:
#   ./scripts/spark-submit-cluster.sh examples/06_schema/05_schema_inference.py
#   SPARK_MASTER=spark://master:7077 ./scripts/spark-submit-cluster.sh <file>
#   SPARK_MASTER=yarn ./scripts/spark-submit-cluster.sh <file>
#
# Environment variables:
#   SPARK_HOME      - Path to Spark installation
#   SPARK_MASTER    - Cluster master URL (required)
#   JAVA_HOME_17    - Path to Java 17
#   DATA_HOME       - Path to data directory
#   DEPLOY_MODE     - client (default) or cluster
#   DRIVER_MEMORY   - Driver memory (default: 2g)
#   EXECUTOR_MEMORY - Executor memory (default: 2g)
#   NUM_EXECUTORS   - Number of executors (default: 2)
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Java 17
if [[ -n "${JAVA_HOME_17:-}" ]]; then
    export JAVA_HOME="${JAVA_HOME_17}"
fi

# Spark home
if [[ -z "${SPARK_HOME:-}" ]]; then
    SPARK_SUBMIT="$(command -v spark-submit 2>/dev/null || echo "${PROJECT_ROOT}/.venv/bin/spark-submit")"
else
    SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"
fi

# Configuration
SPARK_MASTER="${SPARK_MASTER:?Set SPARK_MASTER (e.g., spark://host:7077 or yarn)}"
DEPLOY_MODE="${DEPLOY_MODE:-client}"
DRIVER_MEMORY="${DRIVER_MEMORY:-2g}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-2g}"
NUM_EXECUTORS="${NUM_EXECUTORS:-2}"
export DATA_HOME="${DATA_HOME:-${PROJECT_ROOT}/data}"

PY_FILE="${1:?Usage: $0 <python_file> [spark-submit args...]}"
shift

echo "╭─────────────────────────────────────────────────╮"
echo "│  spark-submit (cluster mode)                    │"
echo "├─────────────────────────────────────────────────┤"
echo "│  Script:      ${PY_FILE}"
echo "│  Master:      ${SPARK_MASTER}"
echo "│  Deploy mode: ${DEPLOY_MODE}"
echo "│  Driver mem:  ${DRIVER_MEMORY}"
echo "│  Exec mem:    ${EXECUTOR_MEMORY}"
echo "│  Executors:   ${NUM_EXECUTORS}"
echo "╰─────────────────────────────────────────────────╯"

exec "${SPARK_SUBMIT}" \
    --master "${SPARK_MASTER}" \
    --deploy-mode "${DEPLOY_MODE}" \
    --driver-memory "${DRIVER_MEMORY}" \
    --executor-memory "${EXECUTOR_MEMORY}" \
    --num-executors "${NUM_EXECUTORS}" \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
    --py-files "${PROJECT_ROOT}/dist/pyspark_ds_json-0.1.0-py3-none-any.whl" \
    "$@" \
    "${PY_FILE}"
