# :material-content-copy: COPY INTO

`COPY INTO` bulk-loads data from external file locations into a Delta Lake
table. It is **idempotent** — files that have already been loaded are
automatically skipped on subsequent runs, making it ideal for incremental
ingestion pipelines.

---

## :material-pin: Syntax

```sql
COPY INTO target_table
FROM 'source_path'
FILEFORMAT = format
[FILES = ('file1', 'file2', ...)]
[PATTERN = 'glob_pattern']
[FORMAT_OPTIONS (key = value, ...)]
[COPY_OPTIONS (key = value, ...)];
```

| Clause | Purpose |
|--------|---------|
| `FROM` | Cloud storage path (`s3://`, `abfss://`, `gs://`, `dbfs://`) or local path |
| `FILEFORMAT` | `CSV`, `JSON`, `PARQUET`, `AVRO`, `ORC`, `TEXT`, `BINARYFILE` |
| `FILES` | Explicit list of files to load |
| `PATTERN` | Glob pattern to select files (e.g., `'*.csv'`) |
| `FORMAT_OPTIONS` | Reader options — header, delimiter, schema hints, etc. |
| `COPY_OPTIONS` | Load behavior — `mergeSchema`, `force`, etc. |

---

## :material-magnify: Behavior

1. **Idempotent by default** — Delta tracks which files have been loaded in the
   table's transaction log. Re-running the same `COPY INTO` skips already-loaded
   files.
2. **Schema enforcement** — Incoming data must match the target table's schema.
   Mismatched columns raise errors unless `mergeSchema = true` is set.
3. **Atomic append** — Each `COPY INTO` is a single transaction. All files in
   the batch either succeed or fail together.
4. **No deduplication** — `COPY INTO` does not deduplicate within a file or
   across files. If a file contains duplicate rows, they are all loaded.
5. **Force reload** — Setting `force = true` in `COPY_OPTIONS` bypasses the
   idempotency check and reloads all matching files.

---

## :material-flask-outline: Practical Examples

### Load CSV Files

```sql
COPY INTO raw_events
FROM 's3://data-lake/events/2024/'
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header'    = 'true',
    'delimiter' = ',',
    'inferSchema' = 'true'
)
COPY_OPTIONS ('mergeSchema' = 'true');
```

### Load JSON with a Glob Pattern

```sql
COPY INTO api_responses
FROM 'abfss://landing@storage.dfs.core.windows.net/api/'
FILEFORMAT = JSON
PATTERN = '*/response_*.json'
FORMAT_OPTIONS (
    'multiLine' = 'true'
);
```

### Load Specific Parquet Files

```sql
COPY INTO fact_sales
FROM 'gs://warehouse/sales/'
FILEFORMAT = PARQUET
FILES = ('part-00000.parquet', 'part-00001.parquet');
```

### Force Reload (Bypass Idempotency)

```sql
COPY INTO staging_table
FROM 'dbfs:/mnt/landing/data/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
COPY_OPTIONS ('force' = 'true');
-- Re-loads all files even if previously ingested
```

### Load with Column Mapping

```sql
COPY INTO target_table
FROM (
    SELECT _c0 AS id, _c1 AS name, _c2 AS amount
    FROM 'dbfs:/mnt/raw/data/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'false');
```

---

## :material-content-copy: Format Options Reference

| Option | Formats | Description |
|--------|---------|-------------|
| `header` | CSV | `true` if first row is a header |
| `delimiter` | CSV | Column separator (default `,`) |
| `quote` | CSV | Quote character (default `"`) |
| `escape` | CSV | Escape character (default `\\`) |
| `multiLine` | JSON, CSV | Allow records to span multiple lines |
| `inferSchema` | CSV, JSON | Infer column types (slower, good for exploration) |
| `dateFormat` | CSV, JSON | Custom date parsing pattern |
| `timestampFormat` | CSV, JSON | Custom timestamp parsing pattern |

---

## :material-brain: When to Use

| Scenario | Recommended Approach |
|----------|---------------------|
| Incremental file ingestion | `COPY INTO` (built-in idempotency) |
| One-time bulk load | `COPY INTO` with all files |
| Re-process corrected files | `COPY INTO` with `force = true` |
| Complex transformations on load | Use `INSERT INTO ... SELECT` from a temp view instead |
| Streaming ingestion | Use Auto Loader (`readStream.format("cloudFiles")`) instead |

---

> **Tip:** For high-volume streaming ingestion, consider
> [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
> which provides file notification mode and better scalability than `COPY INTO`.

---

## :material-table-refresh: COPY_OPTIONS Reference

| Option | Default | Description |
|--------|---------|-------------|
| `mergeSchema` | `false` | Evolve target schema to accommodate new source columns |
| `force` | `false` | Re-load files even if already tracked as ingested |

---

## :material-table-eye: FORMAT_OPTIONS Extended Reference

| Option | Formats | Description |
|--------|---------|-------------|
| `header` | CSV | `true` if first row contains column names |
| `delimiter` | CSV | Field separator (default `,`) |
| `quote` | CSV | Quote character (default `"`) |
| `escape` | CSV | Escape character (default `\`) |
| `nullValue` | CSV | String representing NULL |
| `multiLine` | JSON, CSV | Records can span multiple lines |
| `inferSchema` | CSV, JSON | Infer types (slow — use explicit schema if possible) |
| `dateFormat` | CSV, JSON | Date parsing pattern (e.g., `yyyy-MM-dd`) |
| `timestampFormat` | CSV, JSON | Timestamp parsing pattern |
| `recordDelimiter` | AVRO | Delimiter between Avro records |
| `compression` | All | Override compression detection |

---

## :material-database-import: Advanced Patterns

### Load AVRO from DBFS

```sql
COPY INTO telemetry
FROM 'dbfs:/mnt/landing/telemetry/'
FILEFORMAT = AVRO
PATTERN = '*.avro';
```

### Load ORC from ADLS Gen2

```sql
COPY INTO fact_transactions
FROM 'abfss://raw@storage.dfs.core.windows.net/transactions/'
FILEFORMAT = ORC
COPY_OPTIONS ('mergeSchema' = 'true');
```

### Type-cast on load with a SELECT wrapper

```sql
COPY INTO orders (order_id, customer_id, amount, order_date)
FROM (
    SELECT
        CAST(_c0 AS INT)           AS order_id,
        _c1                        AS customer_id,
        CAST(_c2 AS DECIMAL(10,2)) AS amount,
        CAST(_c3 AS DATE)          AS order_date
    FROM 's3://landing/orders/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'false');
```

### Incremental daily ingestion pattern

```sql
-- Run nightly; only new files in today's folder are loaded
COPY INTO raw_events
FROM CONCAT('s3://landing/events/', DATE_FORMAT(current_date(), 'yyyy/MM/dd'), '/')
FILEFORMAT = JSON;
```

### Track load status

```sql
-- Check what has been loaded
DESCRIBE HISTORY raw_events;

-- Count ingested vs total files (approximate via table size)
SELECT COUNT(*) AS rows_loaded, MAX(_metadata.file_modification_time) AS last_file_ts
FROM raw_events;
```

---

## :material-compare: COPY INTO vs Auto Loader vs INSERT INTO

| Feature | `COPY INTO` | Auto Loader | `INSERT INTO … SELECT` |
|---------|:-----------:|:-----------:|:----------------------:|
| Idempotent | :material-check: | :material-check: | :material-close: |
| Streaming ingestion | :material-close: | :material-check: | :material-close: |
| File notification mode | :material-close: | :material-check: | :material-close: |
| Throughput (files/s) | Medium | High | N/A |
| Schema evolution | Via `mergeSchema` | Automatic | Manual |
| SQL-only (no Spark job) | :material-check: | :material-close: | :material-check: |
| Best for | Batch ingestion jobs | High-volume streaming | Ad-hoc or small loads |

!!! tip "When to use Auto Loader instead"
    Use [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
    (`readStream.format("cloudFiles")`) when:
    - You have **thousands of files per hour**
    - You need **file notification mode** (S3 Events / Event Hubs) for low-latency ingestion
    - You want **automatic schema evolution** without restarting the pipeline

---

## :material-speedometer: Performance Tips

| Tip | Reason |
|-----|--------|
| Use `PATTERN` to scope to a date prefix | Avoids listing the entire bucket |
| Set `inferSchema = 'false'` and provide schema | Skips the expensive schema-inference scan |
| Use `FILES` for small known batches | Avoids directory listing overhead |
| Run `OPTIMIZE` after large loads | Consolidates many small files written per batch |
| Avoid `force = true` in production pipelines | Reloads already-ingested data, causing duplicates |
