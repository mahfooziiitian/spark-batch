# :material-information-outline: Table Metadata

Table metadata describes the structure, storage, and properties of a table.
Spark SQL exposes metadata through `DESCRIBE`, `SHOW`, and `ALTER TABLE` commands — no data is read.

---

## :material-code-tags: Syntax

```sql
-- Column schema only
DESCRIBE TABLE table_name;

-- Full metadata: storage, location, provider, properties, statistics
DESCRIBE TABLE EXTENDED table_name;

-- Column detail for a single column
DESCRIBE TABLE EXTENDED table_name col_name;

-- List all partition values present in the table
SHOW PARTITIONS table_name;

-- Filter partitions by one or more partition columns
SHOW PARTITIONS table_name PARTITION (region = 'EU');

-- All user-defined and system table properties
SHOW TBLPROPERTIES table_name;

-- A single property value
SHOW TBLPROPERTIES table_name ('delta.minReaderVersion');

-- Read / write column-level statistics
ANALYZE TABLE table_name COMPUTE STATISTICS FOR COLUMNS col1, col2;
```

| Command | Scope |
|---------|-------|
| `DESCRIBE TABLE` | Column names, types, nullable flag |
| `DESCRIBE TABLE EXTENDED` | All of the above plus location, format, provider, statistics, properties |
| `SHOW PARTITIONS` | Existing partition values (partitioned tables only) |
| `SHOW TBLPROPERTIES` | Key-value table properties |
| `ANALYZE TABLE` | Collects column-level statistics used by the query optimizer |

---

## :material-information-outline: Behavior

1. `DESCRIBE TABLE EXTENDED` returns rows for columns first, then a `# Detailed Table Information` section containing `Provider`, `Location`, `Type` (MANAGED/EXTERNAL), `Statistics`, and `Table Properties`.
2. `SHOW PARTITIONS` raises an `AnalysisException` when called on an unpartitioned table — always check `DESCRIBE TABLE EXTENDED` first.
3. `SHOW TBLPROPERTIES` returns user-set properties together with system properties (e.g., `delta.minReaderVersion`, `spark.sql.sources.schema.numParts`).
4. Column statistics written by `ANALYZE TABLE` are stored in the metastore and read automatically by the Spark AQE cost-based optimizer.
5. For Delta tables, the authoritative statistics come from the Delta transaction log, not the Hive metastore — `ANALYZE TABLE` is a no-op for row counts on Delta.

---

## :material-flask-outline: Practical Examples

### Inspect schema and storage details

```sql
DESCRIBE TABLE EXTENDED orders;
-- Returns: col_name, data_type, comment rows
--          then #Detailed Table Information rows:
--            Location: dbfs:/user/hive/warehouse/orders
--            Provider: delta
--            Type:     MANAGED
--            Statistics: 1234567 bytes, 48301 rows
```

### List all partition values

```sql
SHOW PARTITIONS sales;
-- Result:
-- region=APAC/order_date=2024-01-01
-- region=APAC/order_date=2024-01-02
-- region=EU/order_date=2024-01-01
```

### Filter partitions by column

```sql
SHOW PARTITIONS sales PARTITION (region = 'EU');
-- Result:
-- region=EU/order_date=2024-01-01
-- region=EU/order_date=2024-01-02
```

### Read a specific table property

```sql
SHOW TBLPROPERTIES orders ('delta.minReaderVersion');
-- Result:
-- key                        | value
-- delta.minReaderVersion     | 1
```

### Set a custom property

```sql
ALTER TABLE orders
SET TBLPROPERTIES ('owner' = 'data-eng', 'team.slack' = '#data-alerts');
```

### Collect optimizer statistics

```sql
ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS customer_id, order_date, amount;
```

### Read column-level statistics back

```sql
DESCRIBE TABLE EXTENDED orders customer_id;
-- Returns: min, max, num_nulls, distinct_count, avg_col_len, max_col_len
```

---

## :material-lightbulb-outline: When to Use

| Scenario | Recommended Command |
|----------|---------------------|
| Verify column types before a query | `DESCRIBE TABLE orders` |
| Debug a query reading the wrong location | `DESCRIBE TABLE EXTENDED orders` |
| Check whether a partition was written | `SHOW PARTITIONS sales PARTITION (...)` |
| Inspect Delta version / reader compatibility | `SHOW TBLPROPERTIES orders ('delta.minReaderVersion')` |
| Tag a table with ownership / team info | `ALTER TABLE ... SET TBLPROPERTIES (...)` |
| Improve join / filter cost estimates | `ANALYZE TABLE ... COMPUTE STATISTICS FOR COLUMNS` |

!!! note "Delta vs Hive tables"
    For **Delta** tables, prefer `DESCRIBE DETAIL table_name` (Databricks-specific) for richer metadata — it returns a single structured row with `numFiles`, `sizeInBytes`, `partitionColumns`, `location`, and `lastModified`.

!!! warning "SHOW PARTITIONS on unpartitioned tables"
    Always confirm the table is partitioned before calling `SHOW PARTITIONS`.
    An unpartitioned table raises `AnalysisException: table is not partitioned`.
