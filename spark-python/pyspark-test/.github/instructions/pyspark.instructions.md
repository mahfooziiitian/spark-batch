---
applyTo: "spark-python/pyspark-test/**/src/**/*.py"
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

## Output

- Prefer Parquet: `df.write.mode("overwrite").parquet(path)`
- Always call `spark.stop()` at the end of standalone scripts.
