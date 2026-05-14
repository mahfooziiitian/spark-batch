# :material-refresh: Refresh Table Metadata

`REFRESH TABLE` invalidates Spark's cached file listings and metadata for a table,
forcing the next query to re-read them from the underlying storage or metastore.

---

## :material-code-tags: Syntax

```sql
REFRESH TABLE [db_name.]table_name;
```

| Parameter | Description |
|-----------|-------------|
| `db_name` | Optional database (schema) qualifier. Defaults to the active database. |
| `table_name` | The table whose cached metadata should be invalidated. |

---

## :material-information-outline: Behavior

1. Clears Spark's in-memory cache of file paths and metadata for the table — **no data is modified**.
2. The next query against the table re-discovers all files from the storage location (HDFS, S3, ADLS, etc.).
3. Applies to both managed and external tables.
4. For **partitioned tables**, `REFRESH TABLE` re-discovers all partition directories — equivalent to running `MSCK REPAIR TABLE` for newly added partitions.
5. Does **not** clear results cached by `CACHE TABLE`; use `UNCACHE TABLE` for that.
6. For **Delta tables**, this is rarely needed — Delta's transaction log always reflects the true state. Use it only when metadata becomes inconsistent after a manual storage operation.

!!! note "REFRESH vs MSCK REPAIR TABLE"
    `MSCK REPAIR TABLE` adds missing partitions to the Hive metastore (DDL change).
    `REFRESH TABLE` only refreshes Spark's local cache without changing the metastore.
    Use `MSCK REPAIR TABLE` when new partition directories were added by an external process;
    use `REFRESH TABLE` when the metastore is up-to-date but Spark's cache is stale.

---

## :material-flask-outline: Practical Examples

### Basic refresh after external file update

```sql
-- Files in the sales table's S3 location were updated by an ETL job
REFRESH TABLE sales;
```

### Refresh a table in a non-default database

```sql
REFRESH TABLE warehouse.dim_customer;
```

### Refresh after landing new partition data externally

```sql
-- New files dropped into /data/events/event_date=2024-06-01/ by Kafka
REFRESH TABLE events;

-- Confirm partition is now visible
SHOW PARTITIONS events PARTITION (event_date = '2024-06-01');
```

### Invalidate and re-query to verify freshness

```sql
REFRESH TABLE inventory;

SELECT
    warehouse_id,
    SUM(quantity) AS total_qty
FROM inventory
GROUP BY warehouse_id
ORDER BY total_qty DESC;
```

---

## :material-lightbulb-outline: When to Use

| Scenario | Recommendation |
|----------|----------------|
| Files written to table location by a non-Spark process | Run `REFRESH TABLE` before querying |
| Table created or populated by another engine (Hive, Flink, etc.) | Refresh to pick up new data |
| Stale row counts or query results after an external update | Refresh and re-query |
| New partition directories added externally (non-Delta) | `REFRESH TABLE` or `MSCK REPAIR TABLE` |
| Cached query result is out of date | `UNCACHE TABLE` then re-cache |
| Delta table inconsistency after manual storage edit | `REFRESH TABLE` as a last resort |

!!! warning "Delta tables"
    Manual edits to Delta table storage (adding or removing Parquet files directly) corrupt the
    Delta transaction log and break ACID guarantees. `REFRESH TABLE` does **not** fix this.
    Use only official Delta operations (`INSERT`, `DELETE`, `MERGE`, `RESTORE`) to modify Delta data.

!!! tip "Automating refreshes in pipelines"
    Add `REFRESH TABLE <table>` at the start of any notebook or job step that reads a table
    populated by an upstream external process to ensure Spark always sees the latest files.
