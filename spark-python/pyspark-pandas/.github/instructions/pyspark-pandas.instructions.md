---
applyTo: "src/**/*.py"
---

# PySpark Pandas Code Instructions

## SparkSession

Always make scripts environment-agnostic using the `SPARK_MASTER` env var.
Enable Arrow for all pandas interop:

```python
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("descriptive-job-name")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

## Imports

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F       # always alias as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pyspark.pandas as ps                  # always alias as ps
import pandas as pd
```

Never use `from pyspark.sql.functions import *`.

## Pandas API on Spark

- Always import as `import pyspark.pandas as ps`.
- Convert between Spark and pandas-on-Spark DataFrames explicitly:

```python
# Spark → pandas-on-Spark
psdf = df.pandas_api()

# pandas-on-Spark → Spark
sdf = psdf.to_spark()

# pandas → pandas-on-Spark
psdf = ps.from_pandas(pdf)
```

## Arrow Optimization

Enable Arrow for efficient pandas ↔ Spark conversion:

```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

Always enable Arrow when calling `df.toPandas()` or `spark.createDataFrame(pdf)`.

## Pandas UDFs

Use the `@pandas_udf` decorator with explicit return type:

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

@pandas_udf(DoubleType())
def multiply(a: pd.Series, b: pd.Series) -> pd.Series:
    return a * b
```

Prefer pandas UDFs over regular Python UDFs — they leverage Arrow for vectorized execution.

## User-Defined Table Functions (UDTFs)

Use the `@udtf` decorator with explicit return type schema:

```python
from pyspark.sql.functions import udtf

@udtf(returnType="id: int, value: string")
class MyUDTF:
    def eval(self, x: int):
        for i in range(x):
            yield (i, f"value_{i}")
```

## Environment Variables Pattern

Scripts that read or write data use env vars with safe fallbacks:

```python
INPUT_PATH  = os.environ.get("INPUT_PATH")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/job_output")
```

## Output

- Prefer Parquet: `df.write.mode("overwrite").parquet(path)`
- Partition large outputs: `.partitionBy("year_month")`
- Always call `spark.stop()` at the end of standalone scripts.
