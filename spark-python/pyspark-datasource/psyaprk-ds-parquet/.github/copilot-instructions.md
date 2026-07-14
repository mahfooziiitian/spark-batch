# Copilot Instructions — psyaprk-ds-parquet

This project demonstrates reading, writing, and managing **Parquet files**
using PySpark's built-in Parquet datasource (`spark.read.parquet` / `df.write.parquet`).

> **Note:** The directory name contains a typo (`psyaprk` → `pyspark`).

## Technology Stack

| Component | Version / Tool |
|-----------|---------------|
| Python | ≥ 3.11 |
| PySpark | 3.5.x |
| Package manager | uv (preferred) |
| Testing | pytest ≥ 8.0 |

## Parquet Datasource Patterns

### Reading Parquet

```python
df = spark.read.parquet("/path/to/data.parquet")
df = spark.read.parquet("/path/to/partitioned/")
df = spark.read.option("mergeSchema", "true").parquet("/path1", "/path2")
```

### Key Read Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `mergeSchema` | `bool` | `false` | Merge schemas from all Parquet part-files |
| `pathGlobFilter` | `str` | — | Glob pattern to filter files |
| `recursiveFileLookup` | `bool` | `false` | Walk subdirectories |
| `datetimeRebaseMode` | `str` | `EXCEPTION` | Rebase mode for dates (LEGACY, CORRECTED, EXCEPTION) |

### Writing Parquet

```python
df.write.mode("overwrite").parquet("/output/path")
df.write.partitionBy("year", "month").parquet("/output/partitioned/")
df.coalesce(1).write.mode("overwrite").parquet("/output/single/")
```

### Key Write Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `compression` | `str` | `snappy` | Codec: snappy, gzip, lzo, brotli, zstd, none |
| `partitionBy` | `list` | — | Partition columns |

### SQL Interface

```python
spark.sql("SELECT * FROM parquet.`/path/to/file.parquet`")
```

## Conventions

- Use `SPARK_MASTER` env var with `local[*]` fallback.
- `from pyspark.sql import functions as F` — never `import *`.
- Always call `spark.stop()` at the end of standalone scripts.
- Parquet is the preferred output format across the repository.

## Things to Avoid

- Do not use `from pyspark.sql.functions import *`.
- Do not omit `spark.stop()` in standalone scripts.
- Do not use `len(df.collect())` — use `df.count()`.
