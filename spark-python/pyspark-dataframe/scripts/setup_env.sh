#!/usr/bin/env bash
# Set up the development environment: check Java, install Python deps, verify PySpark.
#
# This script is meant to be run once after cloning the repo.
# Usage:
#   ./scripts/setup_env.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=== PySpark DataFrame — environment setup ==="
echo ""

# --- Java ---
if ! command -v java > /dev/null 2>&1; then
  echo "ERROR: Java is not installed. Install Java 11 first." >&2
  echo "  Ubuntu:  sudo apt-get install -y openjdk-11-jdk" >&2
  echo "  macOS:   brew install openjdk@11" >&2
  exit 1
fi
echo "Java:    $(java -version 2>&1 | head -1)"

# --- Python ---
PYTHON_BIN="$(command -v python3 2>/dev/null || command -v python)"
if [ -z "$PYTHON_BIN" ]; then
  echo "ERROR: Python 3 is not installed." >&2
  exit 1
fi
echo "Python:  $("${PYTHON_BIN}" --version)"

# --- Install dependencies ---
cd "${PROJECT_ROOT}"

if command -v poetry > /dev/null 2>&1; then
  echo ""
  echo "Installing dependencies with Poetry..."
  poetry install
elif [ -f "requirements.txt" ]; then
  echo ""
  echo "Installing dependencies with pip..."
  pip install -r requirements.txt
else
  echo ""
  echo "Installing PySpark with pip..."
  pip install pyspark pytest chispa
fi

# --- Smoke test ---
echo ""
echo "Running PySpark smoke test..."
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
export PYSPARK_PYTHON="${PYTHON_BIN}"
export PYSPARK_DRIVER_PYTHON="${PYTHON_BIN}"
export PYTHONPATH="${PROJECT_ROOT}/src:${PYTHONPATH:-}"

python3 - <<'PYEOF'
from pyspark.sql import SparkSession
spark = (SparkSession.builder
         .appName("smoke-test")
         .master("local[2]")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
df = spark.createDataFrame([(1, "ok")], ["id", "status"])
assert df.count() == 1
spark.stop()
print("PySpark smoke test passed.")
PYEOF

echo ""
echo "=== Setup complete ==="
echo ""
echo "Run an example:  ./scripts/run_example.sh src/data_frame/joins/inner/inner_equi_join.py"
echo "Run tests:       ./scripts/run_tests.sh"
echo "Open notebooks:  jupyter lab notebooks/"
