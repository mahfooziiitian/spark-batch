# Hive

This is the default catalog in older Databricks workspaces that don’t use Unity Catalog.

- `Managed by`: Workspace-local Hive Metastore
- `Namespace structure`: schema.table (no explicit catalog)
- `Limitations`:
    - No cross-workspace access
    - Coarse-grained permissions
    - No data lineage or centralized governance
- `Example`: default.sales_data
