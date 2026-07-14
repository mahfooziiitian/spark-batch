# :material-file-cog-outline: File & I/O Settings

File and I/O settings control how Spark reads and writes Parquet, ORC, Delta, and
other formats — including compression, predicate pushdown, schema merging, and output
file sizing.

---

## :material-code-tags: Key Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `spark.sql.parquet.compression.codec` | `snappy` | Parquet write compression: `snappy`, `gzip`, `zstd`, `lz4`, `none` |
| `spark.sql.orc.compression.codec` | `snappy` | ORC write compression |
| `spark.sql.parquet.filterPushdown` | `true` | Push predicates into the Parquet reader |
| `spark.sql.parquet.mergeSchema` | `false` | Merge schemas across Parquet files (slow; use only when needed) |
| `spark.sql.files.maxPartitionBytes` | `128MB` | Max bytes per input partition |
| `spark.sql.files.openCostInBytes` | `4MB` | Virtual cost to open a small file (for packing) |
| `spark.databricks.delta.optimizeWrite.enabled` | `false` | Auto-optimize output file size on Delta writes |
| `spark.databricks.delta.autoCompact.enabled` | `false` | Auto-compact small Delta files after write |
| `spark.sql.parquet.int96RebaseModeInRead` | `CORRECTED` | Timestamp rebase mode for legacy Parquet |
| `spark.sql.ansi.enabled` | `false` | Enable ANSI SQL mode (stricter type coercion, overflow errors) |

---

## :material-information-outline: Behavior

1. **Compression**: `snappy` is fast with moderate compression; `zstd` offers better compression ratio at similar speed (recommended for cold storage); `gzip` is slowest but highest ratio.
2. **Predicate pushdown** (`filterPushdown = true`) allows the Parquet/ORC reader to skip entire row groups based on column statistics. Always leave enabled.
3. **Schema merge** (`mergeSchema = true`) reads the schema from every file before processing — very expensive on large table with many files. Use only for schema evolution debugging.
4. **Optimize write** (Delta/Databricks) coalesces output files to the target size (default 128 MB) before writing — reduces the small-file problem without a separate `OPTIMIZE` run.
5. **ANSI mode** makes Spark SQL behave like standard SQL — integer overflow raises an error instead of wrapping, and invalid casts raise errors instead of returning `NULL`.

---

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

### Enable Delta optimize write (Databricks)

```sql
-- Auto-coalesce output to ~128 MB files on each write
SET spark.databricks.delta.optimizeWrite.enabled = true;
SET spark.databricks.delta.optimizeWrite.binSize = 134217728;  -- 128 MB

INSERT INTO delta_sales
SELECT * FROM staging_sales;

RESET spark.databricks.delta.optimizeWrite.enabled;
```

### Enable ANSI SQL mode

```sql
-- Strict type checks and overflow errors
SET spark.sql.ansi.enabled = true;

-- This now raises an error instead of silently overflowing
SELECT CAST(2147483648 AS INT);
-- ArithmeticException: integer overflow

RESET spark.sql.ansi.enabled;
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
-- Increase openCostInBytes to pack more small files per task
SET spark.sql.files.openCostInBytes = 33554432;  -- 32 MB virtual cost per file open

SELECT * FROM table_with_many_small_files WHERE event_date = '2024-06-01';

RESET spark.sql.files.openCostInBytes;
```

---

## :material-lightbulb-outline: When to Tune I/O Settings

| Scenario | Setting |
|----------|---------|
| Writing to cold/archival storage | `parquet.compression.codec = zstd` |
| Schema evolution debugging | `parquet.mergeSchema = true` (temporarily) |
| Many small output files | `optimizeWrite.enabled = true` (Delta) or `COALESCE` |
| Slow reads on table with many tiny files | Increase `openCostInBytes` |
| Strict SQL compliance needed | `ansi.enabled = true` |
| Predicate not pushed to reader | Verify `filterPushdown = true` and `EXPLAIN` |
