---
applyTo: "**/src/**/*.py"
---

# PySpark Datasource Patterns

## SparkSession Creation

Every standalone script creates a SparkSession with `SPARK_MASTER` environment
variable support and `local[*]` fallback:

```python
import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("my-datasource-example")
    .master(os.environ.get("SPARK_MASTER", "local[*]"))
    .config("spark.ui.enabled", "false")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
```

### Key Configuration

| Config | Value | Purpose |
|--------|-------|---------|
| `spark.ui.enabled` | `false` | Skip Spark Web UI for local scripts |
| `spark.sql.adaptive.enabled` | `true` | Enable Adaptive Query Execution |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Auto-coalesce shuffle partitions |
| `spark.sparkContext.setLogLevel` | `"WARN"` | Suppress verbose INFO/DEBUG logs |

## Environment Variables for Paths

Use environment variables for input/output paths with `/tmp/...` fallbacks:

```python
INPUT_PATH = os.environ.get("INPUT_PATH", "/tmp/spark-input")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/spark-output")
```

Never hardcode absolute paths to cluster storage or user home directories.

## Datasource Read Patterns

### Generic format-based read

```python
df = (
    spark.read
    .format("parquet")
    .option("mergeSchema", "true")
    .load(INPUT_PATH)
)
```

### Shorthand methods

```python
df = spark.read.parquet(INPUT_PATH)
df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)
df = spark.read.json(INPUT_PATH)
df = spark.read.text(INPUT_PATH)
```

### With explicit schema

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("value", StringType(), True),
])

df = spark.read.format("csv").schema(schema).option("header", "true").load(INPUT_PATH)
```

### JDBC read

```python
df = (
    spark.read
    .format("jdbc")
    .option("url", os.environ.get("JDBC_URL", "jdbc:mysql://localhost:3306/mydb"))
    .option("dbtable", "employees")
    .option("user", os.environ.get("DB_USER", "root"))
    .option("password", os.environ.get("DB_PASSWORD", ""))
    .load()
)
```

## Datasource Write Patterns

```python
df.write.format("parquet").mode("overwrite").save(OUTPUT_PATH)
df.write.format("csv").mode("overwrite").option("header", "true").save(OUTPUT_PATH)
df.write.format("json").mode("overwrite").save(OUTPUT_PATH)
```

### Write modes

| Mode | Behaviour |
|------|-----------|
| `overwrite` | Replace existing data |
| `append` | Add to existing data |
| `ignore` | Skip if data exists |
| `errorifexists` | Fail if data exists (default) |

### Partitioned writes

```python
df.write.format("parquet").mode("overwrite").partitionBy("year", "month").save(OUTPUT_PATH)
```

## Preferred Output Format

Parquet is the preferred output format for all datasource examples unless the
example specifically demonstrates another format. Parquet provides columnar
storage, schema preservation, and compression out of the box.

## DataFrame Function Style

Use `from pyspark.sql import functions as F` and method chaining:

```python
from pyspark.sql import functions as F

result = (
    df
    .filter(F.col("status") == "active")
    .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
    .groupBy("department")
    .agg(
        F.count("*").alias("employee_count"),
        F.sum("salary").alias("total_salary"),
        F.avg("salary").alias("avg_salary"),
    )
    .orderBy(F.desc("employee_count"))
)
```

### Common function patterns

```python
# Column references
F.col("name"), F.lit("constant")

# String functions
F.upper("name"), F.trim("name"), F.concat_ws(",", "a", "b")

# Aggregations
F.count("*"), F.sum("amount"), F.avg("score"), F.max("date")

# Date/time
F.current_date(), F.date_format("ts", "yyyy-MM-dd")

# Null handling
F.coalesce("primary", "fallback"), F.when(F.col("x").isNull(), F.lit(0))
```

## Script Termination

Always call `spark.stop()` at the end of standalone scripts:

```python
if __name__ == "__main__":
    spark = create_spark_session()
    try:
        # ... processing logic ...
        result.show(truncate=False)
    finally:
        spark.stop()
```

## Sample Data Pattern

Examples that need sample data create it in-memory or with `tempfile`:

```python
import tempfile

data = [("Alice", 30), ("Bob", 25), ("Carol", 35)]
df = spark.createDataFrame(data, ["name", "age"])

tmp_dir = tempfile.mkdtemp()
df.write.format("parquet").mode("overwrite").save(tmp_dir)
```
