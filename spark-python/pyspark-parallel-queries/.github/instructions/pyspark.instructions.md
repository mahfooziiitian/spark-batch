---
applyTo: "src/**/*.py"
---

# PySpark Code Instructions

## SparkSession

Every script must be environment-agnostic via the `SPARK_MASTER` env var:

```python
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("descriptive-job-name")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .config("spark.scheduler.mode", "FAIR")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

- `spark.scheduler.mode = FAIR` is **required** for all parallel-execution scripts.
- Set `spark.sql.shuffle.partitions` to `"4"` for local/test, `"200"` for cluster.
- Set `spark.ui.enabled` to `"false"` in test scripts to speed up fixture creation.
- Always wrap the session body in `try/finally` and call `spark.stop()` in the `finally` block for standalone scripts.

```python
try:
    # ... your work ...
finally:
    spark.stop()
```

## Imports

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F        # always alias as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, LongType,
)
```

Never use `from pyspark.sql.functions import *`.

## Environment Variables Pattern

All external paths and credentials come from env vars with safe fallbacks:

```python
JDBC_URL    = os.environ.get("JDBC_URL", "")
JDBC_USER   = os.environ.get("JDBC_USER", "")
JDBC_PASS   = os.environ.get("JDBC_PASS", "")
INPUT_PATH  = os.environ.get("INPUT_PATH", "")       # empty → use in-memory sample
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/output")
```

When `INPUT_PATH` is empty or the file does not exist, fall back to in-memory sample data so the script runs locally without any external dependencies.

## FAIR Scheduler + Pool Assignment

Enable the FAIR scheduler at session level:
```python
.config("spark.scheduler.mode", "FAIR")
```

Assign a pool inside each thread (thread-local property):
```python
def worker_fn() -> None:
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
    # ... Spark actions ...
```

To use named pools, point to `fairscheduler.xml`:
```python
.config("spark.scheduler.allocation.file", "/path/to/fairscheduler.xml")
```

## AQE & Performance

Always include for cluster-mode scripts:
```python
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

For S3 / EMRFS, add:
```python
.config("spark.speculation", "false")
```

## DataFrame Patterns

```python
# Prefer method chaining over intermediate variables
result = (df
          .filter(F.col("status") == "active")
          .groupBy("region")
          .agg(
              F.round(F.sum("revenue"), 2).alias("total_revenue"),
              F.countDistinct("customer_id").alias("unique_customers"),
          )
          .orderBy(F.desc("total_revenue")))
```

## Output

- Prefer Parquet: `df.write.mode("overwrite").parquet(path)`
- Partition large outputs: `.partitionBy("year", "month")`
- Use `df.count()` not `len(df.collect())` for row counts.

## Logging

```python
spark.sparkContext.setLogLevel("WARN")    # example scripts
spark.sparkContext.setLogLevel("ERROR")   # pytest fixtures
```
