---
applyTo: "src/**/*.py,tests/**/*.py"
---

# Python — PySpark Conventions

## Tooling

All commands via `uv run task <name>`. Config lives exclusively in `pyproject.toml`.

## SparkSession

```python
import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("descriptive-job-name")
    .master(os.environ.get("SPARK_MASTER", "local[*]"))
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
```

- Never create SparkSession inside library functions — pass as parameter.
- Always call `spark.stop()` at end of standalone scripts.
- Use `if __name__ == "__main__":` guard in every script.

## Imports

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F        # always alias as F — never star-import
from pyspark.sql.window import Window
```

## Code Rules

- Type hints on **all** function signatures.
- Max line length: 128.
- No `print()` — use `logging.getLogger(__name__)`.
- No bare `except` — catch specific exceptions.
- No code at module level (no side effects on import).
- Prefer `F.col("name")` over `df["name"]`.
- Chain transformations; avoid reassigning variables.

## Environment Variables

```python
INPUT_PATH   = os.environ.get("INPUT_PATH")           # None → use in-memory sample
OUTPUT_PATH  = os.environ.get("OUTPUT_PATH", "/tmp/spark_output")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
```

## Schema Definition

Always define schemas explicitly — never `inferSchema=True` in production:

```python
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

ORDERS_SCHEMA = StructType([
    StructField("order_id",    LongType(),   nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("amount",      DoubleType(), nullable=True),
])
```

## Output

- Prefer Parquet. Partition large outputs: `.partitionBy("year_month")`.
- CSV only for non-technical audiences.
