# Copilot Instructions — pyspark-ds-csv

This project demonstrates reading, writing, and managing **CSV files**
using PySpark's built-in CSV datasource.

## Technology Stack

| Component | Version / Tool |
|-----------|---------------|
| Python | ≥ 3.11 |
| PySpark | 3.5.x |
| Package manager | uv (preferred) |
| Testing | pytest ≥ 8.0 |

## CSV Datasource Patterns

### Reading CSV

```python
df = spark.read.csv("/path/to/data.csv")
df = spark.read.format("csv").load("/path/to/data.csv")

# Typical read with common options
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .csv("/path/to/data.csv")
)

# Read with explicit schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
])
df = spark.read.schema(schema).option("header", "true").csv("/path/to/data.csv")
```

### Key Read Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `header` | `bool` | `false` | First line is a header |
| `inferSchema` | `bool` | `false` | Infer column types (slow on large files) |
| `delimiter` / `sep` | `str` | `,` | Field separator |
| `quote` | `str` | `"` | Quote character |
| `escape` | `str` | `\` | Escape character |
| `multiLine` | `bool` | `false` | Allow fields spanning multiple lines |
| `encoding` | `str` | `UTF-8` | Character encoding |
| `nullValue` | `str` | — | String representation of null |
| `nanValue` | `str` | `NaN` | String representation of NaN |
| `dateFormat` | `str` | `yyyy-MM-dd` | Date parsing format |
| `timestampFormat` | `str` | `yyyy-MM-dd'T'HH:mm:ss` | Timestamp parsing format |
| `mode` | `str` | `PERMISSIVE` | Parse mode: PERMISSIVE, DROPMALFORMED, FAILFAST |
| `columnNameOfCorruptRecord` | `str` | `_corrupt_record` | Column to store malformed lines (PERMISSIVE) |
| `comment` | `str` | — | Comment line prefix character |
| `ignoreLeadingWhiteSpace` | `bool` | `false` | Trim leading whitespace from values |
| `ignoreTrailingWhiteSpace` | `bool` | `false` | Trim trailing whitespace from values |
| `schema` | `StructType` | — | Explicit schema (preferred over inferSchema) |

### Writing CSV

```python
df.write.mode("overwrite").csv("/output/path")

df.write \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("quote", '"') \
    .option("escape", "\\") \
    .option("compression", "gzip") \
    .option("nullValue", "") \
    .mode("overwrite") \
    .csv("/output/path")

# Single output file
df.coalesce(1).write.option("header", "true").mode("overwrite").csv("/output/single/")
```

### Key Write Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `header` | `bool` | `false` | Write header row |
| `delimiter` / `sep` | `str` | `,` | Field separator |
| `quote` | `str` | `"` | Quote character |
| `escape` | `str` | `\` | Escape character |
| `compression` | `str` | `none` | Codec: none, gzip, bzip2, lz4, snappy, deflate |
| `nullValue` | `str` | — | String to write for null values |

### SQL Interface

```python
spark.sql("SELECT * FROM csv.`/path/to/file.csv`")
```

## Conventions

- Use `SPARK_MASTER` env var with `local[*]` fallback.
- `from pyspark.sql import functions as F` — never `import *`.
- Always call `spark.stop()` at the end of standalone scripts.
- Prefer explicit schemas over `inferSchema` for production code.

## Things to Avoid

- Do not use `from pyspark.sql.functions import *`.
- Do not omit `spark.stop()` in standalone scripts.
- Do not use `len(df.collect())` — use `df.count()`.
- Do not rely on `inferSchema` in production — always supply an explicit schema.
