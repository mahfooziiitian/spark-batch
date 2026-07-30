# Writing JSON Files

Complete reference for `df.write.json()` patterns and options.

## API Styles

=== "Direct Method (Preferred)"
    ```python
    df.write.json("/tmp/output")
    ```

=== "Format-Based"
    ```python
    df.write.format("json").save("/tmp/output")
    ```

## Write Modes

```python
# Overwrite existing data
df.write.mode("overwrite").json("/tmp/output")

# Append to existing data
df.write.mode("append").json("/tmp/output")

# Fail if path exists (default)
df.write.mode("error").json("/tmp/output")

# Ignore if path exists (no-op)
df.write.mode("ignore").json("/tmp/output")
```

| Mode | Behavior |
|------|----------|
| `error` / `errorifexists` | Throw exception if path exists (default) |
| `overwrite` | Delete existing data, write new |
| `append` | Add to existing data |
| `ignore` | No-op if path exists |

## Common Write Options

| Option | Default | Description |
|--------|---------|-------------|
| `compression` | `none` | Codec: gzip, bzip2, deflate, lz4, snappy |
| `dateFormat` | `yyyy-MM-dd` | Date column formatting |
| `timestampFormat` | ISO 8601 | Timestamp column formatting |
| `encoding` | `UTF-8` | Output encoding |
| `lineSep` | `\n` | Line separator between records |
| `ignoreNullFields` | `true` | Omit null fields from output |

## Compression

```python
# Gzip compression (best compression ratio)
df.write.option("compression", "gzip").json("/tmp/output")

# Snappy (fast compression, good for intermediate data)
df.write.option("compression", "snappy").json("/tmp/output")

# LZ4 (fastest compression)
df.write.option("compression", "lz4").json("/tmp/output")
```

!!! tip "Choosing Compression"
    - **gzip** — best ratio, slower; ideal for archival/export
    - **snappy** — balanced; good for intermediate Spark data
    - **lz4** — fastest; good for temporary/shuffle data
    - **none** — when downstream tools can't decompress

## Controlling Output

### Partitioning

```python
# Partition by columns (creates directory structure)
df.write.partitionBy("year", "month").json("/tmp/output")
# Output: /tmp/output/year=2024/month=01/part-*.json
```

### Number of Files

```python
# Single output file
df.coalesce(1).write.json("/tmp/output")

# Control parallelism
df.repartition(4).write.json("/tmp/output")
```

!!! warning "coalesce(1) Anti-Pattern"
    Using `coalesce(1)` forces all data through a single task. Acceptable for
    small datasets, but causes OOM errors on large data.

### Custom Timestamp Format

```python
df.write \
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX") \
    .option("dateFormat", "yyyy-MM-dd") \
    .json("/tmp/output")
```

### Omit Null Fields

```python
# Default: nulls are omitted
df.write.option("ignoreNullFields", "true").json("/tmp/output")
# Output: {"name": "Alice", "age": 30}  (email omitted if null)

# Include nulls explicitly
df.write.option("ignoreNullFields", "false").json("/tmp/output")
# Output: {"name": "Alice", "age": 30, "email": null}
```

## Output Format

Spark always writes **single-line JSON** (one record per line). Each output partition
produces one file:

```
/tmp/output/
├── part-00000-*.json
├── part-00001-*.json
├── part-00002-*.json
└── _SUCCESS
```

!!! note "No Pretty Print"
    Spark does not support writing multiline/pretty-printed JSON. If you need
    formatted output, post-process with `jq` or use `df.toPandas().to_json(indent=2)`
    for small datasets.

## Write + Read Round-Trip

```python
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Define schema
schema = StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
])

# Write
df.write.mode("overwrite").json("/tmp/roundtrip")

# Read back with same schema (guarantees consistency)
df_back = spark.read.schema(schema).json("/tmp/roundtrip")
```

## Run

```bash
python examples/03_dataframe/05_write_json.py
```
