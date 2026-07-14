---
applyTo: "spark-python/**/*.py"
---

# PySpark Code Instructions

## SparkSession

Always make scripts environment-agnostic using the `SPARK_MASTER` env var:

```python
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("descriptive-job-name")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.adaptive.enabled",                   "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled","true")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

- Omit `.master()` for jobs designed only for cluster submission via `spark-submit`.
- Set `spark.sql.shuffle.partitions` to `"4"` for local examples, `"200"` for cluster examples.
- Set `spark.ui.enabled` to `"false"` in scripts and tests to skip the Web UI.

## Imports

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F       # always alias as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
```

Never use `from pyspark.sql.functions import *`.

## Environment Variables Pattern

Scripts that read or write data use env vars with safe fallbacks:

```python
INPUT_PATH  = os.environ.get("INPUT_PATH")          # None → use in-memory sample
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/job_output")
YARN_QUEUE  = os.environ.get("YARN_QUEUE",  "default")
```

## AQE & Performance Configs

Include for all cluster-mode examples (YARN, K8s, EMR):

```python
.config("spark.sql.adaptive.enabled",                   "true")
.config("spark.sql.adaptive.coalescePartitions.enabled","true")
```

Disable speculative execution for S3 / EMRFS:
```python
.config("spark.speculation", "false")
```

## Functions & Aggregations

```python
# Prefer method chaining
result = (df
          .filter(F.col("status") == "active")
          .groupBy("region")
          .agg(
              F.round(F.sum("revenue"), 2).alias("total_revenue"),
              F.countDistinct("customer_id").alias("unique_customers"),
          )
          .orderBy(F.desc("total_revenue")))
```

## Window Functions

```python
from pyspark.sql.window import Window

w = (Window
     .partitionBy("region")
     .orderBy("date")
     .rowsBetween(Window.unboundedPreceding, 0))

df = df.withColumn("running_total", F.sum("revenue").over(w))
```

## Output

- Prefer Parquet: `df.write.mode("overwrite").parquet(path)`
- Partition large outputs: `.partitionBy("year_month")`
- Always call `spark.stop()` at the end of standalone scripts.

## Logging

```python
spark.sparkContext.setLogLevel("WARN")   # in example scripts
spark.sparkContext.setLogLevel("ERROR")  # in pytest fixtures
```

## Environment-Specific Notes

| Environment | Extra configs to set |
|-------------|---------------------|
| Local | `shuffle.partitions=4`, `ui.enabled=false` |
| YARN | `spark.yarn.queue`, `dynamicAllocation.*` |
| Kubernetes | `spark.kubernetes.container.image`, `spark.kubernetes.namespace` |
| EMR | `spark.speculation=false`, `fileoutputcommitter.algorithm.version=2` |
| Glue | Use `GlueContext` + `getResolvedOptions`; fallback to plain `SparkSession` when Glue libs absent |
