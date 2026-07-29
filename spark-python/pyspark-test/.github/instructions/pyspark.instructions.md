---
applyTo: "**/src/**/*.py"
---

# PySpark Source Code Instructions (Root-Level Defaults)

These are baseline source conventions for all child projects. Each child project
may override these via its own `.github/instructions/pyspark.instructions.md`.

## Imports

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
```

Never use `from pyspark.sql.functions import *`.

## SparkSession

Make scripts environment-agnostic:

```python
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("descriptive-job-name")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

## Type Hints

Add type hints to all function signatures:

```python
from pyspark.sql import Column, DataFrame

def my_column_func(col: Column) -> Column: ...
def my_df_func(df: DataFrame) -> DataFrame: ...
```

## Docstrings

Use Google-style docstrings on all public functions:

```python
def my_function(col: Column) -> Column:
    """One-line imperative summary.

    Args:
        col: Input column description.

    Returns:
        Description of return value.
    """
```

## DataFrame Transformations

Prefer method chaining for readability:

```python
result = (df
    .filter(F.col("status").isNotNull())
    .withColumn("clean", F.regexp_replace(F.col("text"), "\\s+", " "))
    .select("id", "clean"))
```

## Error Handling

Raise `ValueError` with descriptive messages for invalid arguments:

```python
if sort_order not in ("asc", "desc"):
    raise ValueError(
        f"['asc', 'desc'] are the only valid sort orders and you entered '{sort_order}'"
    )
```

## Output

- Prefer Parquet: `df.write.mode("overwrite").parquet(path)`
- Use environment variables for output paths: `os.environ.get("OUTPUT_PATH", "/tmp/output")`
- Always call `spark.stop()` at the end of standalone scripts.

## Performance Best Practices

- Use `spark.sql.adaptive.enabled=true` for Adaptive Query Execution.
- Set `spark.sql.shuffle.partitions` appropriately (default 200 is often too high for local mode).
- Avoid `collect()` on large DataFrames — use aggregations or `take(n)` instead.
- Prefer `F.col("name")` over `df["name"]` for column references in transformations.
