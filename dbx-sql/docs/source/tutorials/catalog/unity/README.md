# Unity

The primary and recommended catalog type in modern Databricks environments.

- `Managed by`: Unity Catalog (Databricks' data governance layer)
- `Namespace structure`: catalog.schema.table
- `Features`:
  - Centralized metadata and access control across workspaces
  - Fine-grained data permissions (column, row level)
  - Audit logs for access
  - Data lineage tracking
  - Support for external tables, volumes, and managed tables

- `Example`: main.default.sales_data
