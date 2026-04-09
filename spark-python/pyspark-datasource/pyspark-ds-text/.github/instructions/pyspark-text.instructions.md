---
applyTo: "src/**/*.py"
---

# PySpark Text Datasource Patterns

## SparkSession

Every standalone script creates a SparkSession using the `SPARK_MASTER` env var
with a `local[*]` fallback:

```python
import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("text-descriptive-name")
    .master(os.environ.get("SPARK_MASTER", "local[*]"))
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
```

Always call `spark.stop()` at the end of standalone scripts.

## Text Datasource Schema

The text datasource always produces a single-column DataFrame:

```
root
 |-- value: string (nullable = true)
```

There is no way to change this schema at read time. Parse the `value` column
into structured columns using `F.split`, `F.substring`, `F.regexp_extract`, etc.

## Reading Text Files

### Basic Read

```python
df = spark.read.text("/path/to/file.txt")
```

### Read Options Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `wholetext` | `bool` | `false` | One row per file instead of one row per line |
| `lineSep` | `str` | `\n` | Custom line separator (single or multi-character) |
| `encoding` | `str` | `UTF-8` | Character encoding (ISO-8859-1, UTF-16, etc.) |
| `pathGlobFilter` | `str` | — | Glob pattern to filter files (e.g., `*.log`) |
| `recursiveFileLookup` | `bool` | `false` | Walk subdirectories |
| `compression` | `str` | auto | Force a decompression codec |

### Option Chaining Style

```python
df = (
    spark.read
    .option("wholetext", "true")
    .option("encoding", "ISO-8859-1")
    .text("/path/to/files/")
)
```

Or pass options as keyword arguments:

```python
df = spark.read.text("/path/to/files/", wholetext=True)
```

### Multi-File Reading

```python
# single file
spark.read.text("/data/file.txt")

# list of files
spark.read.text(["/data/file1.txt", "/data/file2.txt"])

# entire directory
spark.read.text("/data/logs/")

# glob pattern
spark.read.text("/data/logs_*.txt")
```

### Input File Name

Track which file each row came from:

```python
df = spark.read.text("/data/").withColumn("source", F.input_file_name())
```

### Compressed Files

Spark auto-detects compression by file extension (`.gz`, `.bz2`):

```python
spark.read.text("/data/logs.txt.gz")     # gzip
spark.read.text("/data/logs.txt.bz2")    # bzip2
spark.read.text("/data/mixed_dir/")      # mixed compressed + plain
```

### Recursive Directory Reading

```python
df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .text("/data/nested/")
)
```

### Whole-File Reading

One row per file — the entire file content is in the `value` column:

```python
df = spark.read.option("wholetext", "true").text("/data/docs/")
```

## Parsing Text into Columns

### Delimiter Split

```python
df_parsed = (
    df_raw.withColumn("parts", F.split(F.col("value"), "\\|"))
    .select(
        F.col("parts")[0].cast(IntegerType()).alias("id"),
        F.col("parts")[1].alias("name"),
        F.col("parts")[2].cast(DoubleType()).alias("price"),
    )
)
```

### Fixed-Width Parsing

```python
df_fixed = df.select(
    F.trim(F.substring(F.col("value"), 1, 5)).cast(IntegerType()).alias("id"),
    F.trim(F.substring(F.col("value"), 6, 15)).alias("name"),
    F.trim(F.substring(F.col("value"), 21, 11)).alias("department"),
)
```

### Regex Extraction

```python
df_parsed = df.select(
    F.regexp_extract("value", r"^(\S+)", 1).alias("ip"),
    F.regexp_extract("value", r"\[(.+?)\]", 1).alias("timestamp"),
    F.regexp_extract("value", r'"(\w+)\s', 1).alias("method"),
    F.regexp_extract("value", r'"\s(\d+)', 1).cast(IntegerType()).alias("status"),
)
```

### Key=Value Parsing

```python
df_kv = df.select(
    F.regexp_extract("value", r"host=(\S+)", 1).alias("host"),
    F.regexp_extract("value", r"cpu=(\S+)", 1).cast(DoubleType()).alias("cpu"),
    F.regexp_extract("value", r"mem=(\S+)", 1).cast(IntegerType()).alias("mem_mb"),
)
```

## Writing Text Files

The text writer requires exactly **one** `StringType` column:

```python
# concatenate fields into a single string column
df_text = df.select(F.concat_ws(",", F.col("name"), F.col("age")).alias("value"))
df_text.write.mode("overwrite").text("/output/path")
```

### Write with Compression

```python
df.coalesce(1).write.mode("overwrite").option("compression", "gzip").text("/output/gzip/")
df.coalesce(1).write.mode("overwrite").option("compression", "bzip2").text("/output/bzip2/")
```

Supported compression codecs: `none`, `gzip`, `bzip2`, `deflate`, `lz4`, `snappy`.

### Single Output File

Use `coalesce(1)` to produce a single part file:

```python
df.coalesce(1).write.mode("overwrite").text("/output/single/")
```

## SQL Interface

### Temp View

```python
df_parsed.createOrReplaceTempView("employees")
spark.sql("SELECT department, AVG(salary) FROM employees GROUP BY department").show()
```

### Direct Path Query

```python
spark.sql("SELECT * FROM text.`/path/to/file.txt`").show()
```

## RDD vs DataFrame

| Feature | RDD (`sc.textFile`) | DataFrame (`spark.read.text`) |
|---------|---------------------|-------------------------------|
| Return type | `RDD[str]` | `DataFrame` with `value` column |
| Catalyst optimizer | ❌ | ✅ |
| SQL support | ❌ | ✅ via temp views |
| Preferred | Legacy/low-level | Modern/recommended |

Prefer `spark.read.text` over `sc.textFile` in all new code.

## Sample Data Pattern

All examples create temporary sample data using `tempfile.mkdtemp()`:

```python
tmp = os.path.join(tempfile.mkdtemp(), "sample.txt")
with open(tmp, "w") as f:
    f.write("line one\nline two\nline three\n")

df = spark.read.text(tmp)
```

This avoids external file dependencies and makes every example self-contained.

## DataFrame Function Style

Use `from pyspark.sql import functions as F` and method chaining:

```python
result = (
    df
    .select(F.explode(F.split(F.col("value"), r"\s+")).alias("word"))
    .filter(F.col("word") != "")
    .groupBy("word")
    .count()
    .orderBy(F.desc("count"))
)
```
