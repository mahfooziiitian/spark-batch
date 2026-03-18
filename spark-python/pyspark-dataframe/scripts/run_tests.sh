#!/usr/bin/env bash
# Run the pytest test suite.
#
# Usage:
#   ./scripts/run_tests.sh                     # all tests
#   ./scripts/run_tests.sh tests/data_frame/joins/   # specific directory
#   ./scripts/run_tests.sh -k "test_inner"     # filter by name
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

export PYTHONPATH="${PROJECT_ROOT}/src:${PYTHONPATH:-}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-python3}"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"

cd "${PROJECT_ROOT}"

echo "Running tests from: ${PROJECT_ROOT}/tests"
echo ""

python3 -m pytest "${@:-tests/}" -v --tb=short
