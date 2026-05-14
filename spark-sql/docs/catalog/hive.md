# :material-bee: Hive Catalog

The **Hive catalog** (`spark_catalog`) integrates Spark SQL with an external
**Hive Metastore** (HMS) for persistent, cross-session, cross-cluster metadata
storage. It is the default persistent catalog for on-prem and many cloud Spark deployments.

---

## :material-cog: Configuration

### Enable Hive Support

```ini
# spark-defaults.conf
spark.sql.catalogImplementation = hive
spark.hadoop.hive.metastore.uris = thrift://metastore-host:9083
```

Via `SparkSession` (PySpark):

```python
spark = (SparkSession.builder
         .appName("hive-job")
         .config("spark.sql.catalogImplementation", "hive")
         .config("spark.hadoop.hive.metastore.uris", "thrift://metastore-host:9083")
         .enableHiveSupport()
         .getOrCreate())
```

!!! note "Local Embedded Metastore"
    Without `hive.metastore.uris`, Spark creates a local Derby-backed metastore in
    `./metastore_db`. Suitable for testing only — not shared across processes.

---

## :material-table: Managed vs External Tables

```sql
-- Managed table: Spark controls the data location
-- DROP TABLE deletes both metadata and data
CREATE TABLE IF NOT EXISTS sales (
    order_id BIGINT,
    region   STRING,
    amount   DOUBLE
)
USING PARQUET
PARTITIONED BY (region);

-- External table: Spark only manages metadata
-- DROP TABLE deletes metadata only; data on disk survives
CREATE TABLE IF NOT EXISTS raw_logs
USING PARQUET
LOCATION '/mnt/raw/logs';
```

---

## :material-folder: Partitioned Tables and MSCK REPAIR

When partitions are added directly to storage (bypassing Spark DDL), the metastore
is unaware of them. `MSCK REPAIR TABLE` scans the storage path and registers
any new partitions.

```sql
-- Add partition explicitly
ALTER TABLE sales ADD PARTITION (region = 'APAC') LOCATION '/mnt/sales/region=APAC';

-- Or scan and auto-register all partitions on disk
MSCK REPAIR TABLE sales;

-- List registered partitions
SHOW PARTITIONS sales;
```

---

## :material-flask-outline: Practical Examples

### Full Lifecycle

```sql
-- Create database
CREATE DATABASE IF NOT EXISTS warehouse
  COMMENT 'Production warehouse'
  LOCATION '/mnt/warehouse.db';

USE warehouse;

-- Create managed table
CREATE TABLE orders (
    order_id BIGINT,
    customer STRING,
    amount   DOUBLE,
    order_date DATE
)
USING PARQUET
PARTITIONED BY (order_date);

-- Insert data
INSERT INTO orders PARTITION (order_date = '2024-06-01')
VALUES (1, 'Alice', 250.0),
       (2, 'Bob',   175.5);

-- Dynamic partitioning
INSERT INTO orders
SELECT order_id, customer, amount, order_date
FROM staging_orders;
```

### Inspect Table Metadata

```sql
-- Basic column info
DESCRIBE TABLE orders;

-- Full details: storage, SerDe, partition info
DESCRIBE EXTENDED orders;

-- Show table properties
SHOW TBLPROPERTIES orders;

-- Show DDL
SHOW CREATE TABLE orders;
```

### Schema Evolution

```sql
-- Add a column (non-breaking for Parquet)
ALTER TABLE orders ADD COLUMNS (discount DOUBLE COMMENT 'Applied discount');

-- Rename a column (Spark 3.x with Parquet only, not ORC)
ALTER TABLE orders RENAME COLUMN customer TO customer_name;

-- Change column comment
ALTER TABLE orders ALTER COLUMN amount COMMENT 'Order total in USD';
```

### Table Statistics

```sql
-- Collect stats for the query optimizer
ANALYZE TABLE orders COMPUTE STATISTICS;

-- Column-level stats for better join planning
ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS amount, order_date;

-- Partition-level stats
ANALYZE TABLE orders PARTITION (order_date = '2024-06-01') COMPUTE STATISTICS;
```

---

## :material-compare: Managed vs External — Key Differences

| Aspect | Managed | External |
|--------|:-------:|:--------:|
| Data location | `warehouse.dir/<db>/<table>` | User-specified `LOCATION` |
| `DROP TABLE` deletes data | Yes | No |
| `TRUNCATE TABLE` supported | Yes | No |
| Created by `CTAS` | Managed | N/A |
| Typical use | ETL outputs, marts | Raw landing zones |

---

## :material-magnify: Behavior Notes

1. **`spark_catalog`** — Hive-backed catalog is registered as `spark_catalog` internally; you usually omit it in SQL (`db.table` not `spark_catalog.db.table`).
2. **Dynamic partition overwrite** — set `spark.sql.sources.partitionOverwriteMode = dynamic` to overwrite only the affected partitions instead of the full table.
3. **SerDe support** — Hive tables with custom SerDes (ORC, Avro, custom) are readable via `USING HIVE OPTIONS (fileFormat 'ORC')`.
4. **Stats expire** — Spark does not auto-update statistics; re-run `ANALYZE` after large data changes for accurate query plans.

---

## :material-brain: When to Use

| Scenario | Recommendation |
|----------|----------------|
| On-prem Hadoop / multi-cluster sharing | Hive Metastore catalog |
| Legacy Hive pipelines | `spark_catalog` with `enableHiveSupport()` |
| Modern Databricks workload | Migrate to Unity Catalog |
| Partitioned tables added outside Spark | `MSCK REPAIR TABLE` |
| Large tables with complex joins | `ANALYZE TABLE` to collect statistics |
