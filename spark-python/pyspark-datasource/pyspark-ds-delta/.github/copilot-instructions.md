# Copilot Instructions — pyspark-ds-delta

This project demonstrates reading, writing, and managing **Delta Lake** tables
using PySpark and the Delta Lake connector.

## Technology Stack

| Component | Version / Tool |
|-----------|---------------|
| Python | ≥ 3.11 |
| PySpark | 3.5.x |
| Delta Lake | 3.1.x (delta-spark) |
| Package manager | uv (preferred) |
| Testing | pytest ≥ 8.0 |

## Delta Lake SparkSession Configuration

Delta Lake requires specific Spark configuration at session creation:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("DeltaLakeExample")
    .master(os.environ.get("SPARK_MASTER", "local[*]"))
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)
```

## Delta Lake Datasource Patterns

### Reading Delta

```python
df = spark.read.format("delta").load("/path/to/delta-table")
df = spark.read.load("/path/to/delta-table")  # auto-detect if catalog configured
```

### Time Travel

```python
# Read a specific version
df = spark.read.format("delta").option("versionAsOf", 0).load("/path/to/delta-table")

# Read as of a timestamp
df = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load("/path/to/delta-table")
```

### Writing Delta

```python
df.write.format("delta").mode("overwrite").save("/output/delta-table")
df.write.format("delta").mode("append").save("/output/delta-table")

# Partitioned write
df.write.format("delta").partitionBy("year", "month").mode("overwrite").save("/output/delta-table")
```

### Schema Evolution

```python
df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("/output/delta-table")
```

### Merge (Upsert)

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/path/to/delta-table")

(
    delta_table.alias("target")
    .merge(source_df.alias("source"), "target.id = source.id")
    .whenMatchedUpdate(set={"value": "source.value"})
    .whenNotMatchedInsert(values={"id": "source.id", "value": "source.value"})
    .execute()
)
```

### Table Maintenance

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/path/to/delta-table")

# Remove old files (default retention: 7 days)
delta_table.vacuum()
delta_table.vacuum(168)  # hours

# Compact small files
spark.sql("OPTIMIZE delta.`/path/to/delta-table`")

# Z-ORDER for query performance
spark.sql("OPTIMIZE delta.`/path/to/delta-table` ZORDER BY (col1, col2)")
```

### History

```python
delta_table = DeltaTable.forPath(spark, "/path/to/delta-table")
delta_table.history().show(truncate=False)
```

## Conventions

- Use `SPARK_MASTER` env var with `local[*]` fallback.
- `from pyspark.sql import functions as F` — never `import *`.
- Always call `spark.stop()` at the end of standalone scripts.
- Always configure Delta-specific Spark settings at session creation.

## Things to Avoid

- Do not use `from pyspark.sql.functions import *`.
- Do not omit `spark.stop()` in standalone scripts.
- Do not use `len(df.collect())` — use `df.count()`.
- Do not forget to include `delta-spark` in `spark.jars.packages`.
- Do not run `VACUUM` with retention less than 7 days without disabling the safety check.
