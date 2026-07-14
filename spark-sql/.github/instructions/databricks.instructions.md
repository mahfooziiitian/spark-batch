---
applyTo: "src/**/*.sql,docs/**/*.md"
---

# Databricks-Specific Features

## Scope

This file covers features that **only work on Databricks Runtime** (not open-source Spark).
Always label these with `[Databricks]` in docs and `-- [Databricks]` in SQL comments.

## Delta Lake DML

Delta tables support `UPDATE`, `DELETE`, `MERGE INTO` — standard Spark SQL does not.

```sql
-- [Databricks] Delta MERGE — deduplicate source first
MERGE INTO target AS t
USING (SELECT * FROM source WHERE rn = 1) AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

## Table Optimization

```sql
-- [Databricks] Compact small files and co-locate by column
OPTIMIZE table_name ZORDER BY (join_key);

-- [Databricks] Remove old file versions
VACUUM table_name RETAIN 168 HOURS;
```

- Run `OPTIMIZE` after bulk writes (batch loads, SCD merges).
- `ZORDER` improves predicate pushdown on high-cardinality columns.

## COPY INTO

```sql
-- [Databricks] Idempotent file ingestion
COPY INTO target_table
FROM '/path/to/files'
FILEFORMAT = PARQUET
COPY_OPTIONS ('mergeSchema' = 'true');
```

## Unity Catalog

```sql
-- [Databricks] Three-level namespace
USE CATALOG my_catalog;
USE SCHEMA my_schema;

CREATE TABLE my_catalog.my_schema.dim_customer (...);
```

- Always use three-part names (`catalog.schema.table`) in production SQL.
- `GRANT` / `REVOKE` for access control.

## Databricks SQL Functions

Functions available only on Databricks Runtime:

| Function | Purpose |
|----------|---------|
| `ai_generate_text()` | LLM inference in SQL |
| `read_files()` | Dynamic file reader |
| `cloud_files()` | Auto Loader (streaming) |
| `h3_*()` | Geospatial H3 functions |

## Photon Engine

- Photon is Databricks' vectorized C++ execution engine.
- No SQL changes needed — it's transparent at runtime.
- Performance-sensitive docs may note "benefits from Photon" where relevant.

## Labeling Convention

In documentation:
```markdown
!!! note "[Databricks] Delta Lake Required"
    This pattern requires Delta tables (`USING DELTA`).
```

In SQL files:
```sql
-- [Databricks] Requires Delta Lake
OPTIMIZE dim_customer ZORDER BY (customer_id);
```

In Python:
```python
# [Databricks] Unity Catalog three-part name
spark.sql("SELECT * FROM catalog.schema.table")
```
