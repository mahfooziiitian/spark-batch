# :material-file-cog-outline: File & I/O Settings

File and I/O settings control how Spark reads and writes Parquet, ORC, Delta, and
other formats — including compression, predicate pushdown, schema merging, and output
file sizing.

### :material-animation-play: Interactive Visualization — Codec & Scan Trade-Offs

Switch between the most common file-level settings to compare write speed, storage size,
and scan overhead. The visualization focuses on trade-offs already called out below:
codec choice for Parquet/ORC and the startup cost of schema merging.

<div id="viz-config-io" class="ts-viz"></div>

<script src="../assets/js/configuration-viz.js"></script>

______________________________________________________________________

## :material-code-tags: Key Settings

| Setting                                                         | Default     | Description                                                        |
| --------------------------------------------------------------- | ----------- | ------------------------------------------------------------------ |
| `spark.sql.parquet.compression.codec`                           | `snappy`    | Parquet write compression: `snappy`, `gzip`, `zstd`, `lz4`, `none` |
| `spark.sql.orc.compression.codec`                               | `snappy`    | ORC write compression                                              |
| `spark.sql.parquet.filterPushdown`                              | `true`      | Push predicates into the Parquet reader                            |
| `spark.sql.parquet.mergeSchema`                                 | `false`     | Merge schemas across Parquet files (slow; use only when needed)    |
| `spark.sql.files.maxPartitionBytes`                             | `128MB`     | Max bytes per input partition                                      |
| `spark.sql.files.openCostInBytes`                               | `4MB`       | Virtual cost to open a small file (for packing)                    |
| `spark.databricks.delta.optimizeWrite.enabled` **[Databricks]** | `false`     | Auto-optimize output file size on Delta writes                     |
| `spark.databricks.delta.autoCompact.enabled` **[Databricks]**   | `false`     | Auto-compact small Delta files after write                         |
| `spark.sql.parquet.int96RebaseModeInRead`                       | `CORRECTED` | Timestamp rebase mode for legacy Parquet                           |
| `spark.sql.ansi.enabled`                                        | `true`      | Strict SQL semantics during casts, arithmetic, and data ingestion  |

______________________________________________________________________

## :material-information-outline: Behavior

1. **Compression**: `snappy` is fast with moderate compression; `zstd` offers a better compression ratio with good read/write performance for colder data; `gzip` is usually the slowest to write.
2. **Predicate pushdown** (`filterPushdown = true`) lets the Parquet/ORC reader skip row groups based on file metadata. Keep it enabled unless you are diagnosing a reader bug.
3. **Schema merge** (`mergeSchema = true`) reads schema metadata from every file before processing — expensive on wide, heavily evolved tables. Use only when you truly need merged schemas.
4. `spark.sql.files.maxPartitionBytes` and `openCostInBytes` affect **scan task planning**, not the physical size of files already on disk. They control how many files Spark packs into each input partition.
5. **[Databricks]** Delta optimize write coalesces output files toward a target size during the write itself, which reduces small-file pressure before a later `OPTIMIZE` run.
6. `spark.sql.ansi.enabled = true` is the Spark 4 default, so malformed casts or overflow encountered during ingestion now raise errors unless you explicitly opt into `try_*` functions or disable ANSI mode.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Change Parquet compression for cold storage

```sql
-- zstd: better ratio than snappy, good for archival
SET spark.sql.parquet.compression.codec = zstd;

INSERT INTO archive_sales
SELECT * FROM sales WHERE order_date < '2023-01-01';

RESET spark.sql.parquet.compression.codec;
```

### Disable schema merge (default — leave it off)

```sql
SET spark.sql.parquet.mergeSchema = false;

-- Fast read: no per-file schema scan
SELECT * FROM events WHERE event_date = '2024-06-01';
```

### Enable schema merge for a schema-evolved table

```sql
-- Only when you know the table has files with different schemas
SET spark.sql.parquet.mergeSchema = true;

SELECT event_id, event_type, payload, new_column
FROM events;

RESET spark.sql.parquet.mergeSchema;
```

### Control output file size (non-Delta)

```sql
-- Reduce output partitions before writing to avoid many small files
INSERT INTO processed_events
SELECT /*+ COALESCE(10) */ *
FROM staged_events
WHERE event_date = CURRENT_DATE();
```

### [Databricks] Enable Delta optimize write

```sql
SET spark.databricks.delta.optimizeWrite.enabled = true;
SET spark.databricks.delta.optimizeWrite.binSize = 134217728;  -- 128 MB

INSERT INTO delta_sales
SELECT * FROM staging_sales;

RESET spark.databricks.delta.optimizeWrite.enabled;
```

### Verify predicate pushdown is active

```sql
SET spark.sql.parquet.filterPushdown = true;

EXPLAIN
SELECT order_id, amount
FROM parquet_orders
WHERE order_date = '2024-06-01' AND region = 'EU';
-- Confirm: PushedFilters: [IsNotNull(order_date), EqualTo(order_date,2024-06-01), ...]
```

### Tune file open cost to pack small files into larger partitions

```sql
SET spark.sql.files.openCostInBytes = 33554432;  -- 32 MB virtual cost per file open

SELECT *
FROM table_with_many_small_files
WHERE event_date = '2024-06-01';

RESET spark.sql.files.openCostInBytes;
```

### ANSI-safe ingestion when source quality is uneven

```sql
-- Spark 4 default: malformed integers now raise errors
SET spark.sql.ansi.enabled = true;

SELECT try_cast(raw_customer_id AS INT) AS customer_id
FROM landing_orders;
```

______________________________________________________________________

## :material-lightbulb-outline: When to Tune I/O Settings

| Scenario                                  | Setting                                                       |
| ----------------------------------------- | ------------------------------------------------------------- |
| Writing to cold or archival storage       | `parquet.compression.codec = zstd`                            |
| Schema evolution debugging                | `parquet.mergeSchema = true` (temporarily)                    |
| Many small output files                   | `optimizeWrite.enabled = true` **[Databricks]** or `COALESCE` |
| Slow reads on tables with many tiny files | Increase `openCostInBytes`                                    |
| Predicate not pushed to the reader        | Verify `filterPushdown = true` and inspect `EXPLAIN`          |
| Spark 4 ingestion now fails on bad input  | Use `try_cast` / `try_to_*` or disable ANSI deliberately      |
