# Multi-line JSON vs JSON Lines

Understanding the two JSON file formats and handling mixed folders.

## The Two Formats

=== "JSON Lines (Default)"
    One complete JSON object per line — Spark's default expectation:
    ```json
    {"id": 1, "name": "Alice", "skills": ["Spark"]}
    {"id": 2, "name": "Bob", "skills": ["SQL"]}
    ```
    ```python
    df = spark.read.json(path)  # Just works
    ```

=== "Multi-line JSON"
    Pretty-printed or array-rooted — requires `multiLine=true`:
    ```json
    {
      "id": 1,
      "name": "Mahfooz",
      "skills": ["Spark", "Databricks"]
    }
    ```
    ```python
    df = spark.read.option("multiLine", "true").json(path)
    ```

## Key Differences

| Aspect | JSON Lines | Multi-line |
|--------|-----------|------------|
| Read mode | Default | `multiLine=true` |
| Splittable | ✓ Yes (parallel) | ✗ No (single task) |
| Large files | ✓ Efficient | ✗ Memory-bound |
| Pretty-printed | ✗ Not supported | ✓ Yes |
| Array root `[...]` | ✗ Not supported | ✓ Yes |

## Common Mistakes

### multiLine on JSON Lines file

```python
# WRONG — reads whole file as one value, loses records!
spark.read.option("multiLine", "true").json(json_lines_file)
```

### Default mode on pretty-printed file

```python
# WRONG — each line treated as separate (corrupt) record
spark.read.json(pretty_printed_file)
```

## Mixed Folder Strategy

When a folder contains both formats:

```python
# 1. Read all as text
raw = spark.read.text(mixed_dir).withColumn("file", F.input_file_name())

# 2. Classify files by first line
file_types = raw.groupBy("file").agg(
    F.first(F.trim(F.col("value"))).alias("first_line"),
).withColumn(
    "format",
    F.when(F.col("first_line").startsWith("["), "multiline")
    .when(F.col("first_line") == "{", "multiline")
    .otherwise("json_lines"),
)

# 3. Process each format with correct mode
jsonl_files = [r.file for r in file_types.filter(col("format") == "json_lines").collect()]
multi_files = [r.file for r in file_types.filter(col("format") == "multiline").collect()]

df_lines = spark.read.schema(schema).json(jsonl_files)
df_multi = spark.read.option("multiLine", "true").schema(schema).json(multi_files)

# 4. Union
df_all = df_lines.unionByName(df_multi)
```

!!! tip "Best Practice"
    Separate formats into different directories at ingestion:
    ```
    /raw/json_lines/    ← default read
    /raw/multiline/     ← multiLine=true
    /raw/bad_records/   ← quarantine
    ```

## Full Demo

```python title="examples/05_error_handling/05_multiline_vs_jsonlines.py"
--8<-- "examples/05_error_handling/05_multiline_vs_jsonlines.py"
```

## Run

```bash
python examples/05_error_handling/05_multiline_vs_jsonlines.py
```

!!! success "Recommendation"
    Prefer **JSON Lines** for data pipelines (splittable, parallel).
    Use `multiLine=true` only for API responses and configuration files.
