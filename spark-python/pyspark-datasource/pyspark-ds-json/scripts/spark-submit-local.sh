#!/usr/bin/env bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# spark-submit: Run a PySpark JSON example via spark-submit (local mode)
#
# Usage:
#   ./scripts/spark-submit-local.sh examples/06_schema/01_struct_type_schema.py
#   ./scripts/spark-submit-local.sh examples/05_error_handling/permissive_mode.py
#
# Environment variables:
#   SPARK_HOME      - Path to Spark installation (auto-detected if not set)
#   JAVA_HOME_17    - Path to Java 17 (required for PySpark 4)
#   DATA_HOME       - Path to data directory (defaults to ./data)
#   SPARK_MASTER    - Spark master URL (defaults to local[*])
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Java 17 (required for PySpark 4)
if [[ -n "${JAVA_HOME_17:-}" ]]; then
    export JAVA_HOME="${JAVA_HOME_17}"
fi

# Spark home detection
if [[ -z "${SPARK_HOME:-}" ]]; then
    # Try to find spark-submit in the venv (PySpark pip install)
    SPARK_SUBMIT="$(command -v spark-submit 2>/dev/null || echo "${PROJECT_ROOT}/.venv/bin/spark-submit")"
else
    SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"
fi

# Defaults
export DATA_HOME="${DATA_HOME:-${PROJECT_ROOT}/data}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
PY_FILE="${1:?Usage: $0 <python_file> [spark-submit args...]}"
shift

echo "╭─────────────────────────────────────────────────╮"
echo "│  spark-submit (local mode)                      │"
echo "├─────────────────────────────────────────────────┤"
echo "│  Script:  ${PY_FILE}"
echo "│  Master:  ${SPARK_MASTER}"
echo "│  Java:    ${JAVA_HOME:-system default}"
echo "│  Data:    ${DATA_HOME}"
echo "╰─────────────────────────────────────────────────╯"

exec "${SPARK_SUBMIT}" \
    --master "${SPARK_MASTER}" \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
    --py-files "${PROJECT_ROOT}/dist/pyspark_ds_json-0.1.0-py3-none-any.whl" \
    "$@" \
    "${PY_FILE}"
