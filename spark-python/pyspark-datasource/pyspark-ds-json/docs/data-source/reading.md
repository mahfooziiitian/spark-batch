# Reading JSON Files

Complete reference for all `spark.read.json()` patterns and options.

## API Styles

PySpark provides two equivalent ways to read JSON:

=== "Direct Method (Preferred)"
    ```python
    df = spark.read.json("path/to/file.json")
    ```

=== "Format-Based"
    ```python
    df = spark.read.format("json").load("path/to/file.json")
    ```

## Single-Line JSON (Default)

One JSON object per line — the most common format for log files and streaming data:

```json title="events.jsonl"
{"id": 1, "event": "click", "ts": "2024-01-15T10:30:00Z"}
{"id": 2, "event": "view", "ts": "2024-01-15T10:31:00Z"}
{"id": 3, "event": "purchase", "ts": "2024-01-15T10:32:00Z"}
```

```python
df = spark.read.json("events.jsonl")
```

## Multiline JSON

Pretty-printed JSON or JSON arrays require `multiline=true`:

```json title="users.json"
[
  {"id": 1, "name": "Alice", "age": 30},
  {"id": 2, "name": "Bob", "age": 25}
]
```

```python
df = spark.read.option("multiline", "true").json("users.json")
```

!!! warning "Performance Impact"
    Multiline mode reads the entire file into a **single partition**. For large files,
    prefer single-line JSON (one record per line) for parallel processing.

## Reading Multiple Files

```python
# Read from a directory (all .json files)
df = spark.read.json("path/to/json_dir/")

# Read from multiple explicit paths
df = spark.read.json(["file1.json", "file2.json", "dir/"])

# Read with glob pattern
df = spark.read.json("data/year=2024/month=*/events.json")

# Read with partition discovery
df = spark.read.json("data/year=*/month=*/")
# Adds year, month as partition columns automatically
```

## Schema Handling

### Schema Inference (Default)

```python
# Spark scans the data to infer types (slow for large datasets)
df = spark.read.json("data.json")
df.printSchema()
```

### Explicit Schema (Recommended for Production)

```python
from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
])

df = spark.read.schema(schema).json("data.json")
```

!!! tip "Why Explicit Schemas?"
    - **Faster** — skips inference scan
    - **Predictable** — no type surprises (e.g., `int` vs `long`)
    - **Safer** — catches missing/extra fields immediately

### DDL String Schema

```python
df = spark.read.schema("id LONG, name STRING, email STRING").json("data.json")
```

## Common Read Options

| Option | Default | Description |
|--------|---------|-------------|
| `multiline` | `false` | Parse entire file as one JSON record/array |
| `mode` | `PERMISSIVE` | Error handling: PERMISSIVE, DROPMALFORMED, FAILFAST |
| `columnNameOfCorruptRecord` | `_corrupt_record` | Column name for malformed rows |
| `primitivesAsString` | `false` | Read all primitives as strings |
| `prefersDecimal` | `false` | Prefer DecimalType over DoubleType |
| `allowComments` | `false` | Allow Java/C-style comments |
| `allowUnquotedFieldNames` | `false` | Allow unquoted JSON keys |
| `allowSingleQuotes` | `true` | Allow single-quoted strings |
| `allowNumericLeadingZeros` | `false` | Allow leading zeros in numbers |
| `allowNonNumericNumbers` | `false` | Allow NaN, Infinity, -Infinity |
| `dateFormat` | `yyyy-MM-dd` | Date parsing pattern |
| `timestampFormat` | ISO 8601 | Timestamp parsing pattern |
| `encoding` | `UTF-8` | File encoding (UTF-8, UTF-16, UTF-32) |
| `lineSep` | `\n`, `\r\n`, `\r` | Line separator |
| `samplingRatio` | `1.0` | Fraction of data to scan for inference |
| `dropFieldIfAllNull` | `false` | Drop fields that are always null |

## Example

```python title="examples/01_data_source/read_json_array.py"
--8<-- "examples/01_data_source/read_json_array.py"
```

## Run

```bash
python examples/01_data_source/read_json_array.py
```
