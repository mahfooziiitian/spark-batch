#!/usr/bin/env bash
set -euo pipefail

echo "=== Databricks Unity Catalog Setup ==="

# Check Databricks CLI
if ! command -v databricks &>/dev/null; then
    echo "ERROR: Databricks CLI is not installed." >&2
    echo "  Install: https://docs.databricks.com/en/dev-tools/cli/install.html" >&2
    exit 1
fi

# Check workspace connection
echo "Verifying workspace connection..."
if ! databricks auth describe &>/dev/null; then
    echo "ERROR: Not connected to a Databricks workspace." >&2
    echo "  Run: databricks configure" >&2
    exit 1
fi

CATALOG_NAME="${UC_CATALOG:-demo_catalog}"
SCHEMA_NAME="${UC_SCHEMA:-demo_schema}"

echo "Catalog: $CATALOG_NAME"
echo "Schema:  $CATALOG_NAME.$SCHEMA_NAME"

# List existing catalogs
echo ""
echo "Existing catalogs:"
databricks catalogs list 2>/dev/null || echo "  (unable to list catalogs)"

# Create demo catalog
echo ""
echo "Creating catalog '$CATALOG_NAME'..."
databricks catalogs create --name "$CATALOG_NAME" --comment "PySpark Metastore demo catalog" \
    2>/dev/null || echo "Catalog already exists or insufficient permissions"

# Create demo schema
echo "Creating schema '$CATALOG_NAME.$SCHEMA_NAME'..."
databricks schemas create --catalog-name "$CATALOG_NAME" --name "$SCHEMA_NAME" --comment "PySpark Metastore demo schema" \
    2>/dev/null || echo "Schema already exists or insufficient permissions"

echo ""
echo "=== Unity Catalog Ready ==="
echo ""
echo "Connection info:"
echo "  Catalog: $CATALOG_NAME"
echo "  Schema:  $CATALOG_NAME.$SCHEMA_NAME"
echo ""
echo "In a Databricks notebook:"
echo "  spark.sql('USE CATALOG $CATALOG_NAME')"
echo "  spark.sql('USE SCHEMA $SCHEMA_NAME')"
echo ""
echo "Note: Unity Catalog configs are set automatically in Databricks Runtime."
