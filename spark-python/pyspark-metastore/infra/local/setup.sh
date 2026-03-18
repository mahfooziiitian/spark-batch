#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "=== Local PySpark Metastore Setup ==="

# Check Java
if ! command -v java &>/dev/null; then
    echo "ERROR: Java is not installed." >&2
    echo "  macOS:   brew install openjdk@11" >&2
    echo "  Ubuntu:  sudo apt-get install -y openjdk-11-jdk" >&2
    exit 1
fi
echo "✓ Java: $(java -version 2>&1 | head -1)"

# Check Python
PYTHON_BIN="$(command -v python3 2>/dev/null || true)"
if [ -z "$PYTHON_BIN" ]; then
    echo "ERROR: Python 3 is not installed." >&2
    exit 1
fi
echo "✓ Python: $($PYTHON_BIN --version)"

# Set up environment
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_WAREHOUSE="${SPARK_WAREHOUSE:-/tmp/spark-warehouse}"

# Create warehouse directory
mkdir -p "$SPARK_WAREHOUSE"
echo "✓ Warehouse: $SPARK_WAREHOUSE"

# Install dependencies
cd "$PROJECT_ROOT"
if command -v uv &>/dev/null; then
    echo "Installing with uv..."
    uv sync
elif command -v pip &>/dev/null; then
    echo "Installing with pip..."
    pip install -e ".[dev]"
fi

echo "✓ Dependencies installed"

# Copy .env if not present
if [ ! -f "$PROJECT_ROOT/.env" ]; then
    cp "$SCRIPT_DIR/../common/.env.template" "$PROJECT_ROOT/.env"
    echo "✓ Created .env from template"
fi

# Run smoke test
echo ""
bash "$SCRIPT_DIR/../common/smoke-test.sh"

echo ""
echo "=== Setup Complete ==="
echo "Run examples:  uv run python src/metastore/memory/memory_metastore.py"
echo "Run tests:     uv run task test"
