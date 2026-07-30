#!/usr/bin/env bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# Build the pys_json wheel for distribution / spark-submit --py-files
#
# Usage:
#   ./scripts/build-wheel.sh
#
# Output:
#   dist/pyspark_ds_json-0.1.0-py3-none-any.whl
#
# Use with spark-submit:
#   spark-submit --py-files dist/pyspark_ds_json-*.whl examples/...
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

echo "╭─────────────────────────────────────────────────╮"
echo "│  Building pys_json wheel                        │"
echo "╰─────────────────────────────────────────────────╯"

# Clean previous builds
rm -rf dist/ build/

# Build with uv
uv build

echo ""
echo "✓ Build complete:"
ls -lh dist/
echo ""
echo "Usage with spark-submit:"
echo "  spark-submit --py-files dist/pyspark_ds_json-*.whl your_script.py"
