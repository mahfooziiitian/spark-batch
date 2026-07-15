# :material-table-split-cell: Partitioned Managed Tables

A partitioned managed table stores its data files under Spark's warehouse directory,
physically organised into subdirectories by the values of one or more partition columns.
Spark controls the full lifecycle — dropping the table removes both metadata and data.

---

## :material-code-tags: Syntax

```sql
CREATE TABLE [IF NOT EXISTS] table_name (
    col1  data_type,
    col2  data_type,
    ...
    -- partition columns are listed last in the schema
    part_col1  data_type,
    part_col2  data_type
)
USING { DELTA | PARQUET | ORC | AVRO }
[COMMENT 'description']
[TBLPROPERTIES ('key' = 'value')]
PARTITIONED BY (part_col1 [, part_col2, ...]);
```

| Clause | Required | Notes |
|--------|----------|-------|
| `USING` | Yes | `DELTA` preferred; `PARQUET` for Hive-compatible tables |
| `PARTITIONED BY` | Yes | Columns must appear in the schema; low-cardinality columns only |
| `COMMENT` | No | Table-level description stored in the metastore |
| `TBLPROPERTIES` | No | Key-value pairs for custom metadata or Delta feature flags |

!!! warning "Partition column cardinality"
    Never partition by a high-cardinality column (e.g., `order_id`, `user_id`).
    Each distinct value creates a directory — too many partitions cause metastore overload
    and small-file performance problems. Aim for **< 10,000 partitions** per table.

---

## :material-information-outline: Behavior

1. Partition columns are stored as **directory names** in Hive format: `region=EU/order_date=2024-01-01/`.
2. Queries with `WHERE` filters on partition columns skip unrelated directories entirely — this is called **partition pruning**.
3. `DROP TABLE` removes both the metastore entry **and** the underlying data files.
4. `TRUNCATE TABLE` removes all data files but retains the table definition and partition metadata.
5. For non-Delta tables, partitions added by external processes must be registered with `MSCK REPAIR TABLE` before Spark can see them.
6. For Delta tables, all partition directories are tracked in the transaction log automatically — no repair step is needed.
7. Writing with `INSERT OVERWRITE ... PARTITION (...)` replaces only the specified partition, leaving others untouched.

---

## :material-flask-outline: Practical Examples

### Create a partitioned Delta table

```sql
CREATE TABLE IF NOT EXISTS sales (
    order_id    BIGINT       NOT NULL,
    customer_id BIGINT,
    amount      DECIMAL(18, 2),
    region      STRING       NOT NULL,
    order_date  DATE         NOT NULL
)
USING DELTA
COMMENT 'Daily sales transactions partitioned by region and order date'
TBLPROPERTIES ('owner' = 'data-eng', 'delta.autoOptimize.autoCompact' = 'true')
PARTITIONED BY (region, order_date);
```

### Create a partitioned Parquet table (Hive-compatible)

```sql
CREATE TABLE IF NOT EXISTS events (
    event_id   BIGINT,
    user_id    BIGINT,
    event_type STRING,
    payload    STRING,
    event_date DATE
)
USING PARQUET
PARTITIONED BY (event_date);
```

### Insert with dynamic partitioning (default)

```sql
-- Spark infers partition values from the data
INSERT INTO sales
SELECT
    order_id,
    customer_id,
    amount,
    region,
    order_date
FROM staging_sales;
```

### Overwrite a single partition

```sql
-- Replaces only region='EU', order_date='2024-06-01' — other partitions unchanged
INSERT OVERWRITE sales
PARTITION (region = 'EU', order_date = '2024-06-01')
SELECT order_id, customer_id, amount
FROM staging_sales
WHERE region = 'EU'
  AND order_date = '2024-06-01';
```

### Confirm partitions were written

```sql
SHOW PARTITIONS sales;
-- region=APAC/order_date=2024-06-01
-- region=EU/order_date=2024-06-01
-- region=US/order_date=2024-06-01
```

### Repair missing partitions (non-Delta only)

```sql
-- Registers partition directories added externally (e.g., by Spark on another cluster)
MSCK REPAIR TABLE events;
```

### Truncate a single partition

```sql
-- Removes data from one partition without dropping the table
ALTER TABLE sales DROP IF EXISTS PARTITION (region = 'APAC', order_date = '2024-01-01');
```

### Optimize and compact a Delta partition

```sql
OPTIMIZE sales WHERE region = 'EU'
ZORDER BY (customer_id);
```

---

## :material-swap-horizontal: Managed vs External Partitioned Tables

| Aspect | Managed | External |
|--------|---------|----------|
| Data location | Spark warehouse directory | User-specified `LOCATION` path |
| `DROP TABLE` | Removes metadata **and** data | Removes metadata only; data survives |
| Best for | Spark-owned pipelines | Shared data lakes, multi-engine access |
| Delta support | Full | Full |

---

## :material-lightbulb-outline: When to Use

| Scenario | Recommendation |
|----------|----------------|
| Filter queries always include a date range | Partition by date column (`order_date`, `event_date`) |
| Multi-region data with region-scoped queries | Add `region` as a co-partition column |
| Full pipeline owned by Spark / Databricks | Use managed table |
| Data shared with Hive, Presto, or Flink | Use external table with Parquet |
| Need ACID upserts and time-travel | Use Delta (`USING DELTA`) |
| Replacing daily data idempotently | `INSERT OVERWRITE ... PARTITION (date = ...)` |
