#!/usr/bin/env bash
# PySpark Setup Script
# Installs PySpark and configures the environment for local development.
#
# Usage:
#   bash spark-python-setup.sh                           # default install (PySpark 3.5.0)
#   bash spark-python-setup.sh --version 3.4.1           # specific version
#   bash spark-python-setup.sh --proxy http://host:port  # install via corporate proxy

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
PYSPARK_VERSION="${PYSPARK_VERSION:-3.5.0}"
PROXY_URL=""

# ── Parse arguments ───────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) PYSPARK_VERSION="$2"; shift 2 ;;
    --proxy)   PROXY_URL="$2";       shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

echo "==> PySpark Setup — version ${PYSPARK_VERSION}"
echo ""

# ── Check Java ────────────────────────────────────────────────────────────────
echo "==> Checking Java..."
if ! command -v java &>/dev/null; then
  echo "ERROR: Java is not installed. Install Java 11 or 17 first." >&2
  echo "  macOS:   brew install openjdk@11" >&2
  echo "  Ubuntu:  sudo apt-get install -y openjdk-11-jdk" >&2
  echo "  Windows: winget install EclipseAdoptium.Temurin.11.JDK" >&2
  exit 1
fi
java -version 2>&1 | head -1
export JAVA_HOME="${JAVA_HOME:-$(dirname "$(dirname "$(readlink -f "$(which java)")")")}"
echo "   JAVA_HOME = $JAVA_HOME"
echo ""

# ── Check Python ──────────────────────────────────────────────────────────────
echo "==> Checking Python..."
python3 --version
echo ""

# ── Install PySpark ───────────────────────────────────────────────────────────
echo "==> Installing PySpark ${PYSPARK_VERSION} and common data dependencies..."

PIP_ARGS=(
  "pyspark==${PYSPARK_VERSION}"
  "pyarrow>=4.0.0"
  "pandas>=1.3.0"
  "numpy>=1.21.0"
)

if [[ -n "$PROXY_URL" ]]; then
  echo "   (using proxy: $PROXY_URL)"
  pip3 install "${PIP_ARGS[@]}" --proxy="$PROXY_URL"
else
  pip3 install "${PIP_ARGS[@]}"
fi
echo ""

# ── Configure environment variables ───────────────────────────────────────────
PYTHON_BIN="$(which python3)"
export PYSPARK_PYTHON="$PYTHON_BIN"
export PYSPARK_DRIVER_PYTHON="$PYTHON_BIN"

SHELL_RC=""
if [[ -f "$HOME/.zshrc" ]];    then SHELL_RC="$HOME/.zshrc"
elif [[ -f "$HOME/.bashrc" ]]; then SHELL_RC="$HOME/.bashrc"; fi

echo "==> Add the following exports to your shell profile (${SHELL_RC:-~/.bashrc or ~/.zshrc}):"
echo ""
echo "  export JAVA_HOME=${JAVA_HOME}"
echo "  export PYSPARK_PYTHON=${PYSPARK_PYTHON}"
echo "  export PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON}"
echo ""

# ── Smoke test ────────────────────────────────────────────────────────────────
echo "==> Running smoke test..."
python3 - <<'PYEOF'
from pyspark.sql import SparkSession
spark = (SparkSession.builder
         .master("local[*]")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
print("  Spark version :", spark.version)
print("  Master        :", spark.sparkContext.master)
spark.stop()
print("  Smoke test     : OK")
PYEOF

echo ""
echo "PySpark ${PYSPARK_VERSION} is ready."
