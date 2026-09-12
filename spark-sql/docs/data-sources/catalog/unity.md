# :material-unity: Unity Catalog

!!! note "[Databricks] Databricks-Only Feature"

    Unity Catalog is exclusive to Databricks. Open-source Spark uses Hive Metastore
    or third-party catalogs (e.g., Apache Polaris).

**Unity Catalog** is Databricks's centralized governance layer that provides
fine-grained access control, data lineage, and auditing across all workspaces in a
Databricks account. It extends Spark SQL with a three-level namespace:
`catalog.schema.table`.

______________________________________________________________________

## :material-sitemap: Architecture

```mermaid
flowchart TD
    ACC["Databricks Account\n(Unity Catalog Metastore)"] --> C1["catalog: sales"]
    ACC --> C2["catalog: marketing"]
    ACC --> C3["catalog: shared_data"]
    C1 --> S1["schema: raw"]
    C1 --> S2["schema: silver"]
    C1 --> S3["schema: gold"]
    S2 --> T1["table: orders"]
    S2 --> T2["table: customers"]
    S3 --> T3["view: revenue_summary"]
    C1 --> V1["volume: /files/docs"]
```

______________________________________________________________________

## :material-layers: Three-Level Namespace

```sql
-- Fully qualified: catalog.schema.table
SELECT * FROM sales.silver.orders;

-- Set active catalog and schema
USE CATALOG sales;
USE silver;

-- Now short form works
SELECT * FROM orders;
```

______________________________________________________________________

## :material-plus: Creating Catalog Objects

```sql
-- Catalog (account admin required)
CREATE CATALOG IF NOT EXISTS analytics
  COMMENT 'Analytics workspace catalog';

-- Schema
CREATE SCHEMA IF NOT EXISTS analytics.marts
  COMMENT 'Business-facing aggregated tables'
  MANAGED LOCATION 'abfss://container@storage.dfs.core.windows.net/marts';

-- Managed Delta table
CREATE TABLE IF NOT EXISTS analytics.marts.daily_revenue (
    report_date DATE,
    region      STRING,
    revenue     DOUBLE,
    order_count BIGINT
) USING DELTA
COMMENT 'Daily revenue aggregated by region'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- External table
CREATE TABLE analytics.raw.events
USING PARQUET
LOCATION 'abfss://container@storage.dfs.core.windows.net/raw/events'
COMMENT 'Raw click events from the web';
```

______________________________________________________________________

## :material-shield-account: Access Control — GRANT / REVOKE

Unity Catalog uses privilege-based access at every level of the hierarchy.

### Privilege Reference

| Privilege        | Applies to   | Description                      |
| ---------------- | ------------ | -------------------------------- |
| `USE CATALOG`    | Catalog      | Can reference the catalog        |
| `USE SCHEMA`     | Schema       | Can reference the schema         |
| `CREATE TABLE`   | Schema       | Can create tables in schema      |
| `CREATE VIEW`    | Schema       | Can create views                 |
| `SELECT`         | Table / View | Can read data                    |
| `MODIFY`         | Table        | Can INSERT/UPDATE/DELETE         |
| `ALL PRIVILEGES` | Any          | Grants all applicable privileges |
| `EXECUTE`        | Function     | Can call function                |

```sql
-- Grant a role access to a catalog
GRANT USE CATALOG ON CATALOG analytics TO `data_analyst_role`;
GRANT USE SCHEMA  ON SCHEMA analytics.marts TO `data_analyst_role`;

-- Allow analysts to read the daily_revenue table
GRANT SELECT ON TABLE analytics.marts.daily_revenue TO `data_analyst_role`;

-- Allow engineers to write
GRANT MODIFY ON TABLE analytics.marts.daily_revenue TO `data_engineer_role`;

-- Grant all privileges on a schema to a team
GRANT ALL PRIVILEGES ON SCHEMA analytics.staging TO `data_engineer_role`;

-- Revoke a privilege
REVOKE SELECT ON TABLE analytics.marts.daily_revenue FROM `contractor_role`;

-- Show current grants on an object
SHOW GRANTS ON TABLE analytics.marts.daily_revenue;
```

______________________________________________________________________

## :material-folder-multiple: Volumes — Managed File Storage

Volumes are Unity Catalog-governed directories for non-tabular files
(documents, images, raw files).

```sql
-- Create a managed volume
CREATE VOLUME IF NOT EXISTS analytics.raw.documents;

-- Create an external volume (maps to a cloud storage path)
CREATE EXTERNAL VOLUME analytics.raw.landing
  LOCATION 'abfss://container@storage.dfs.core.windows.net/landing';

-- Access files via /Volumes path
LIST '/Volumes/analytics/raw/documents';

-- Copy a file into a volume
COPY INTO analytics.raw.events
FROM '/Volumes/analytics/raw/landing/events/'
FILEFORMAT = PARQUET;
```

______________________________________________________________________

## :material-tag: Table Tags and Column Tags

Tags are key-value metadata labels used for discovery, classification, and compliance.

```sql
-- Tag a table as PII
ALTER TABLE analytics.silver.customers
  SET TAGS ('pii' = 'true', 'domain' = 'customer');

-- Tag individual columns
ALTER TABLE analytics.silver.customers
  ALTER COLUMN email SET TAGS ('pii_type' = 'email');

ALTER TABLE analytics.silver.customers
  ALTER COLUMN ssn   SET TAGS ('pii_type' = 'ssn', 'classification' = 'restricted');

-- Find all PII-tagged tables in a catalog
SELECT table_catalog, table_schema, table_name
FROM system.information_schema.table_tags
WHERE tag_name = 'pii' AND tag_value = 'true';
```

______________________________________________________________________

## :material-eye-lock: Column-Level Security — Column Masks

```sql
-- Mask SSN for non-privileged users
CREATE OR REPLACE FUNCTION analytics.security.mask_ssn(ssn STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('pii_access_role') THEN ssn
    ELSE CONCAT('***-**-', RIGHT(ssn, 4))
END;

ALTER TABLE analytics.silver.customers
  ALTER COLUMN ssn SET MASK analytics.security.mask_ssn;
```

______________________________________________________________________

## :material-table-lock: Row-Level Security — Row Filters

```sql
-- Users only see rows for their assigned region
CREATE OR REPLACE FUNCTION analytics.security.region_filter(region STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('global_data_access')
    OR region = current_user_region();   -- custom UDF returning the user's region

ALTER TABLE analytics.silver.orders
  ADD ROW FILTER analytics.security.region_filter ON (region);
```

______________________________________________________________________

## :material-history: Data Lineage

Unity Catalog automatically captures column-level lineage — no configuration needed.

```sql
-- View lineage in the Catalog Explorer UI, or query system tables:
SELECT *
FROM system.access.column_lineage
WHERE target_table_full_name = 'analytics.marts.daily_revenue'
ORDER BY event_time DESC
LIMIT 20;
```

______________________________________________________________________

## :material-information: system.information_schema — Catalog Introspection

```sql
-- All tables in a catalog
SELECT table_catalog, table_schema, table_name, table_type
FROM analytics.information_schema.tables
ORDER BY table_schema, table_name;

-- All columns with data types
SELECT table_name, column_name, data_type, is_nullable
FROM analytics.information_schema.columns
WHERE table_schema = 'marts'
ORDER BY table_name, ordinal_position;

-- All grants on a schema
SELECT *
FROM analytics.information_schema.schema_privileges
WHERE schema_name = 'marts';
```

______________________________________________________________________

## :material-database-search: Real-World System Catalog Queries

The `system` catalog is a Databricks-hosted analytical store of account-wide
operational data (cost, audit, lineage, query history). It requires Unity Catalog
and is populated automatically — no setup beyond `USE CATALOG`/`SELECT` grants.
The same tables back the `dbx_mcp` server's `query_system_table` tool (see
[MCP Server](../../mcp-server.md)).

!!! tip "Full runnable reference"

    [`sql/databricks/system_catalog/system_catalog_queries.sql`](https://github.com/mahfooziiitian/spark-batch/blob/main/spark-sql/sql/databricks/system_catalog/system_catalog_queries.sql)
    consolidates the snippets below plus `information_schema` catalog introspection,
    column-level lineage, and query-history cache/spill diagnostics into one aliased,
    lint-clean file.

### Cost monitoring — `system.billing.usage`

```sql
-- DBUs consumed per product this month
SELECT billing_origin_product,
       usage_date,
       SUM(usage_quantity) AS usage_quantity
FROM system.billing.usage
WHERE month(usage_date) = month(current_date())
  AND year(usage_date) = year(current_date())
GROUP BY billing_origin_product, usage_date
ORDER BY usage_date;

-- Which jobs consumed the most DBUs?
SELECT usage_metadata.job_id AS job_id,
       SUM(usage_quantity)   AS dbu_usage
FROM system.billing.usage
WHERE usage_metadata.job_id IS NOT NULL
GROUP BY usage_metadata.job_id
ORDER BY dbu_usage DESC
LIMIT 10;

-- Attribute cost to a team via a custom cluster/job tag
SELECT sku_name, usage_unit, SUM(usage_quantity) AS usage
FROM system.billing.usage
WHERE custom_tags['team'] = 'data-eng'
GROUP BY sku_name, usage_unit;
```

### Security & compliance auditing — `system.access.audit`

```sql
-- Who dropped tables in the analytics catalog this week? (actor pseudonymized — never
-- select user_identity.email directly; SHA2 keeps rows joinable without exposing PII)
SELECT event_time, action_name, request_params,
       SHA2(user_identity.email, 256) AS actor_hash
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name IN ('deleteTable', 'dropTable')
  AND request_params['full_name_arg'] LIKE 'analytics.%'
  AND event_time >= current_date() - INTERVAL 7 DAYS
ORDER BY event_time DESC;

-- Failed login attempts by source IP
SELECT source_ip_address, SHA2(user_identity.email, 256) AS actor_hash, COUNT(*) AS attempts
FROM system.access.audit
WHERE action_name = 'databricksAccountLogin'
  AND response.status_code != 200
GROUP BY source_ip_address, SHA2(user_identity.email, 256)
ORDER BY attempts DESC;
```

!!! warning "Never select identity columns raw"

    `user_identity.email`, `executed_by`, `owned_by`, `creator_user_name`,
    `run_as_user_name`, and `requester` are principal names/emails across the
    `system` catalog. Wrap them in `SHA2(<column>, 256)` (as above) for a stable,
    joinable pseudonym, or apply a Unity Catalog
    [column mask](#column-level-security-column-masks) directly on the source
    column so every consumer — including ad hoc `SELECT *` — is protected
    automatically.

### Query performance — `system.query.history`

```sql
-- Slowest queries in the last 24 hours on a given warehouse
-- (`compute` is a struct — `warehouse_id`/`cluster_id` are nested fields, not top-level columns)
SELECT statement_id, total_duration_ms, statement_text,
       SHA2(executed_by, 256) AS executed_by_hash
FROM system.query.history
WHERE compute.warehouse_id = '0123-456789-abcdefg'
  AND start_time >= current_timestamp() - INTERVAL 1 DAY
ORDER BY total_duration_ms DESC
LIMIT 20;
```

### Data lineage across a table's lifecycle — `system.access.table_lineage`

```sql
-- Everything downstream of a raw ingestion table (impact analysis)
SELECT DISTINCT target_table_full_name
FROM system.access.table_lineage
WHERE source_table_full_name = 'analytics.raw.events'
ORDER BY target_table_full_name;
```

### Compute inventory — `system.compute.clusters` / `system.compute.warehouses`

```sql
-- Active cluster footprint by runtime and creation source
SELECT cluster_source, dbr_version, data_security_mode,
       COUNT(DISTINCT cluster_id) AS cluster_count
FROM system.compute.clusters
WHERE delete_time IS NULL
GROUP BY cluster_source, dbr_version, data_security_mode
ORDER BY cluster_count DESC;

-- SQL warehouse fleet composition by type and size
SELECT warehouse_type, warehouse_size,
       COUNT(DISTINCT warehouse_id) AS warehouse_count
FROM system.compute.warehouses
WHERE delete_time IS NULL
GROUP BY warehouse_type, warehouse_size
ORDER BY warehouse_count DESC;
```

!!! example "Anonymized live snapshot (share of active fleet, rounded)"

    Aggregated from a running workspace; absolute counts are withheld and
    replaced with each category's share of the active fleet so no cost/scale
    information leaks — only the *shape* of the distribution is illustrative.

    | `warehouse_type` | `warehouse_size` | share of active warehouses |
    | ---------------- | ---------------- | --------------------------- |
    | SERVERLESS        | MEDIUM            | ~25%                        |
    | SERVERLESS        | SMALL             | ~25%                        |
    | PRO                | SMALL             | ~15%                        |
    | SERVERLESS        | 2X_SMALL          | ~8%                         |
    | PRO                | X_SMALL           | ~7%                         |
    | *(remaining sizes)*| —                 | ~20%                        |

### Job inventory — `system.lakeflow.jobs`

```sql
-- Active jobs by trigger type and paused state (scheduling health snapshot)
SELECT COALESCE(trigger_type, 'API_OR_LEGACY') AS trigger_type, paused,
       COUNT(DISTINCT job_id) AS job_count
FROM system.lakeflow.jobs
WHERE delete_time IS NULL
GROUP BY trigger_type, paused
ORDER BY job_count DESC;
```

!!! example "Anonymized live snapshot (share of active jobs, rounded)"

    | `trigger_type`   | `paused` | share of active jobs |
    | ---------------- | -------- | --------------------- |
    | API / legacy      | false    | ~50%                   |
    | API / legacy      | *(n/a)*  | ~34%                   |
    | CRON               | false    | ~6%                    |
    | CRON               | true     | ~4%                    |
    | *(other triggers)* | —        | ~6%                    |

### Model serving — `system.serving.endpoint_usage`

```sql
-- Endpoint traffic health by HTTP status code, with average token volume
SELECT status_code, COUNT(*) AS request_count,
       ROUND(AVG(input_token_count), 0)  AS avg_input_tokens,
       ROUND(AVG(output_token_count), 0) AS avg_output_tokens
FROM system.serving.endpoint_usage
WHERE request_time >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY status_code
ORDER BY request_count DESC;
```

!!! example "Anonymized live snapshot (share of requests, last 7 days, rounded)"

    | `status_code`      | share of requests |
    | ------------------ | ------------------ |
    | 200 (success)       | ~84%                |
    | 429 (rate limited)  | ~15%                |
    | 400 (bad request)   | ~2%                 |
    | 5xx (server error)  | <1%                 |

    `requester` is a principal name/email — always wrap it in
    `SHA2(requester, 256)` before grouping by caller (see
    [`system_catalog_queries.sql`](https://github.com/mahfooziiitian/spark-batch/blob/main/spark-sql/sql/databricks/system_catalog/system_catalog_queries.sql),
    section 9).

!!! tip "Combine system tables for richer answers"

    Join `system.billing.usage.usage_metadata.cluster_id` with
    `system.compute.clusters` to attribute DBU cost to a cluster's owner and
    node type, or join `system.access.audit` with `system.access.table_lineage`
    to see who read a table that later fed a downstream report.

______________________________________________________________________

## :material-magnify: Behavior Notes

1. **USE CATALOG is required** before `USE schema` when a non-default catalog is active.
2. **`spark_catalog`** is the legacy Hive Metastore catalog; Unity replaces it when enabled but both can coexist.
3. **Lineage is automatic** — Databricks captures it for all notebooks, jobs, and SQL warehouse queries.
4. **Managed tables** in Unity Catalog store data in the catalog's managed storage location — Databricks manages the lifecycle.
5. **External tables** in Unity require a storage credential and external location registered in the account.

______________________________________________________________________

## :material-brain: When to Use

| Scenario                           | Recommendation                      |
| ---------------------------------- | ----------------------------------- |
| Multi-workspace data sharing       | Unity Catalog with shared catalog   |
| GDPR / PII compliance              | Column masks + row filters          |
| Centralized RBAC                   | GRANT on catalog/schema/table level |
| Data discovery                     | Tags on tables and columns          |
| Audit who accessed what            | `system.access.audit` table         |
| File-based assets alongside tables | Volumes                             |
