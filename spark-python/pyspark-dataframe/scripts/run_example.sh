#!/usr/bin/env bash
# Run a single PySpark example script from src/.
#
# Usage:
#   ./scripts/run_example.sh src/data_frame/joins/inner/inner_equi_join.py
#   SPARK_MASTER=spark://master:7077 ./scripts/run_example.sh src/data_frame/etl/etl.py
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [ $# -lt 1 ]; then
  echo "Usage: $0 <path-to-script.py> [args...]" >&2
  echo ""
  echo "Example:"
  echo "  $0 src/data_frame/joins/inner/inner_equi_join.py"
  exit 1
fi

TARGET="$1"
shift

if [ ! -f "${PROJECT_ROOT}/${TARGET}" ]; then
  echo "ERROR: Script not found: ${PROJECT_ROOT}/${TARGET}" >&2
  exit 1
fi

export PYTHONPATH="${PROJECT_ROOT}/src:${PYTHONPATH:-}"
export SPARK_MASTER="${SPARK_MASTER:-local[*]}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-python3}"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"

echo "Running: ${TARGET}"
echo "  SPARK_MASTER=${SPARK_MASTER}"
echo ""

python3 "${PROJECT_ROOT}/${TARGET}" "$@"
