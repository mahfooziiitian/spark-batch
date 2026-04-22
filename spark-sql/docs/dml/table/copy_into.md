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
