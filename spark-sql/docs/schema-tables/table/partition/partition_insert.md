# :material-database-arrow-down-outline: Inserting into Partitioned Tables

Spark SQL supports two modes for writing into partitioned tables: **static** (partition
values are hardcoded in the statement) and **dynamic** (partition values are derived from
the data itself). Understanding when to use each avoids accidental full-table overwrites and
type-cast errors.

---

## :material-code-tags: Syntax

```sql
-- Append rows; Spark infers partition values from data (dynamic)
INSERT INTO table_name
SELECT col1, col2, ..., part_col
FROM source;

-- Append with explicit static partition
INSERT INTO table_name
PARTITION (part_col = 'value')
SELECT col1, col2, ...
FROM source
WHERE part_col = 'value';

-- Overwrite the entire table (all partitions)
INSERT OVERWRITE TABLE table_name
SELECT col1, col2, ..., part_col
FROM source;

-- Overwrite a single static partition only
INSERT OVERWRITE TABLE table_name
PARTITION (part_col = 'value')
SELECT col1, col2, ...
FROM source
WHERE part_col = 'value';

-- Mixed mode: static outer partition, dynamic inner partition
INSERT OVERWRITE TABLE table_name
PARTITION (region = 'EU', order_date)        -- order_date is dynamic
SELECT order_id, customer_id, amount, order_date
FROM source
WHERE region = 'EU';
```

---

## :material-information-outline: Behavior

1. **Dynamic partitioning** is enabled by default (`spark.sql.sources.partitionOverwriteMode`).
   Partition columns must be the **last columns** in the `SELECT` list.
2. **`INSERT INTO`** always appends; it never replaces existing data.
3. **`INSERT OVERWRITE` without a `PARTITION` clause** replaces the entire table.
4. **`INSERT OVERWRITE` with a static `PARTITION` clause** replaces only that one partition,
   leaving all other partitions untouched — this is the standard idempotent daily-load pattern.
5. Setting `spark.sql.sources.partitionOverwriteMode = DYNAMIC` (the default for Delta) means
   an `INSERT OVERWRITE` without a `PARTITION` clause **only replaces partitions that appear
   in the new data** — it does not wipe the entire table.
6. Mismatched column types between the `SELECT` list and the table schema cause a runtime
   `AnalysisException` — always cast explicitly.
7. For Delta tables, `INSERT OVERWRITE` is transactional and atomic; for Parquet/ORC it is not.

!!! warning "`INSERT OVERWRITE` without PARTITION on non-Delta tables"
    On Parquet/ORC tables with the default `STATIC` overwrite mode, omitting the
    `PARTITION` clause drops **all** existing data before writing. Always specify the
    partition when doing a targeted daily reload.

---

## :material-flask-outline: Practical Examples

### Daily append (dynamic partition)

```sql
-- Partition column order_date must be last in SELECT
INSERT INTO sales
SELECT
    order_id,
    customer_id,
    amount,
    region,
    order_date          -- partition column last
FROM staging_sales
WHERE order_date = CURRENT_DATE();
```

### Idempotent daily reload — overwrite one partition

```sql
-- Safe to re-run: only replaces region='EU', order_date='2024-06-01'
INSERT OVERWRITE TABLE sales
PARTITION (region = 'EU', order_date = '2024-06-01')
SELECT
    order_id,
    customer_id,
    amount
FROM staging_sales
WHERE region = 'EU'
  AND order_date = '2024-06-01';
```

### Dynamic overwrite (Delta default) — replace only touched partitions

```sql
-- With Delta + DYNAMIC mode: only replaces partitions present in staging data
INSERT OVERWRITE TABLE sales
SELECT
    order_id,
    customer_id,
    amount,
    region,
    order_date
FROM staging_sales;
```

### Mixed static/dynamic partition

```sql
-- Static: region = 'US'; dynamic: order_date derived from data
INSERT OVERWRITE TABLE sales
PARTITION (region = 'US', order_date)
SELECT
    order_id,
    customer_id,
    amount,
    order_date
FROM staging_sales
WHERE region = 'US';
```

### Explicit cast to avoid type mismatch

```sql
INSERT INTO events
PARTITION (event_date)
SELECT
    CAST(event_id   AS BIGINT)  AS event_id,
    CAST(user_id    AS BIGINT)  AS user_id,
    event_type,
    payload,
    CAST(event_date AS DATE)    AS event_date
FROM raw_events;
```

### Configure overwrite mode per session

```sql
-- DYNAMIC: only touched partitions are replaced (recommended for Delta)
SET spark.sql.sources.partitionOverwriteMode = DYNAMIC;

-- STATIC: entire table is replaced (Hive-compatible default)
SET spark.sql.sources.partitionOverwriteMode = STATIC;
```

---

## :material-swap-horizontal: Static vs Dynamic Partitioning

| Aspect | Static | Dynamic |
|--------|--------|---------|
| Partition value source | Hardcoded in `PARTITION (col = 'val')` | Derived from data column |
| Overwrites | Exactly the specified partition | All partitions present in source data |
| Performance | Faster (no shuffle to infer partitions) | May add a sort/shuffle step |
| Safety | Precise — accidental overwrites impossible | Risk of full-table overwrite if mode is `STATIC` |
| Typical use case | Idempotent daily reload of one partition | Full backfill, multi-partition writes |

---

## :material-lightbulb-outline: When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Append new daily data without touching history | `INSERT INTO ... (dynamic)` |
| Re-run a daily job idempotently | `INSERT OVERWRITE PARTITION (date = ...)` |
| Backfill multiple dates at once | `INSERT OVERWRITE` with `DYNAMIC` overwrite mode |
| Replace all data in the table | `INSERT OVERWRITE TABLE` (no `PARTITION`) |
| Write to Delta with ACID guarantees | `INSERT INTO` or `INSERT OVERWRITE` (both atomic) |

!!! tip "Idempotent pipelines"
    The standard pattern for a daily batch pipeline is `INSERT OVERWRITE ... PARTITION (date = current_date)`.
    Re-running the job produces the same result regardless of how many times it runs,
    because the overwrite replaces only that day's partition.
