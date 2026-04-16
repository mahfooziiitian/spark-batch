---
applyTo: "spark-python/pyspark-test/pyspark-pytest/src/**/*.py"
---

# PySpark Source Code Instructions — pyspark-pytest

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
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

## Module Organisation

| Package            | Purpose                          | Spark dependency |
| ------------------ | -------------------------------- | ---------------- |
| `data_processing`  | Transaction classification       | Yes              |
| `reader/`          | CSV/file reader utilities        | Yes              |
| `transformation/`  | DataFrame text transformations   | Yes              |
| `utility/`         | Faker data generation scripts    | **No**           |

## Function Signatures

- DataFrame processing functions receive and return `DataFrame`:

```python
def normalise_transaction_information(transactions_df: DataFrame) -> DataFrame:
    ...
```

- Reader functions receive SparkSession and return DataFrame:

```python
def load_csv(spark: SparkSession, file: str) -> DataFrame:
    ...
```

## Type Hints

Add type hints to all function signatures:

```python
from pyspark.sql import Column, DataFrame

def remove_extra_spaces(df: DataFrame, column_name: str) -> DataFrame:
    ...
```

## Docstrings

Use Google-style docstrings on all public functions:

```python
def load_csv(spark: SparkSession, file: str) -> DataFrame:
    """Load a CSV file into a DataFrame.

    Args:
        spark: Active SparkSession.
        file: Path to the CSV file.

    Returns:
        DataFrame loaded from the CSV file.
    """
```

## DataFrame Transformations

Prefer method chaining:

```python
result = (df
    .filter(F.col("status").isNotNull())
    .withColumn("clean", F.regexp_replace(F.col("text"), "\\s+", " "))
    .select("id", "clean"))
```

## Faker Utility Scripts

Faker scripts in `src/utility/` are standalone generators. Each should:
- Be runnable with `python script.py`
- Print or write output to stdout / file
- Not require a SparkSession

## Cleanup

Always call `spark.stop()` at the end of standalone scripts.
