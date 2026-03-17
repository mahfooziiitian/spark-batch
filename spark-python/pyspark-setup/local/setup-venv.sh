#!/usr/bin/env bash
# Set up a Python virtual environment for local PySpark development.
#
# Usage:
#   bash local/setup-venv.sh           # creates .venv in the current directory
#   bash local/setup-venv.sh myenv     # custom venv directory name

set -euo pipefail

VENV_DIR="${1:-.venv}"
PYTHON="${PYTHON:-python3}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Creating virtual environment in '${VENV_DIR}'..."
"$PYTHON" -m venv "$VENV_DIR"

# shellcheck disable=SC1090,SC1091
source "$VENV_DIR/bin/activate"

echo "==> Upgrading pip..."
pip install --quiet --upgrade pip

echo "==> Installing PySpark and data dependencies..."
pip install --quiet -r "$SCRIPT_DIR/requirements.txt"

PYTHON_BIN="$(which python)"

echo ""
echo "==> Add to your shell profile (or activate the venv first):"
echo ""
echo "  source ${VENV_DIR}/bin/activate"
echo "  export PYSPARK_PYTHON=${PYTHON_BIN}"
echo "  export PYSPARK_DRIVER_PYTHON=${PYTHON_BIN}"
echo ""

echo "==> Running smoke test..."
PYSPARK_PYTHON="$PYTHON_BIN" PYSPARK_DRIVER_PYTHON="$PYTHON_BIN" python - <<'PYEOF'
from pyspark.sql import SparkSession
spark = (SparkSession.builder
         .master("local[*]")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
print("  Spark version :", spark.version)
spark.stop()
print("  Smoke test     : OK")
PYEOF

echo ""
echo "Setup complete. Activate with:  source ${VENV_DIR}/bin/activate"
