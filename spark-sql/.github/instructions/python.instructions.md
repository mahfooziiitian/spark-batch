---
applyTo: "src/**/*.py,tests/**/*.py"
---

# Python Instructions — PySpark / Python

## Stack

- **Python**: `>=3.11`
- **PySpark**: `>=3.5.0,<4.0.0`
- **Package manager**: `uv` — use `uv run` for all tool invocations

## Formatters & Linters (all configured in `pyproject.toml`)

| Tool | Purpose | Command |
|------|---------|---------|
| `ruff format` | Code formatting | `uv run task format` |
| `ruff check` | Fast lint + auto-fix | `uv run task format_check` |
| `isort` | Import ordering (black profile) | `uv run task import` |
| `flake8` | Style lint (max-line 128) | `uv run task lint` |
| `mypy` | Static type checking | `uv run task type_check` |
| `radon` | Cyclomatic complexity | `uv run task complexity` |

Run the full pipeline:

```bash
uv run task quality   # import → format → format_check → lint → type_check → sql
```

## Code Style

- **Max line length**: 128 characters
- **Import order**: isort with `--profile black`
- **String quotes**: double quotes (Ruff/Black default)
- **Type hints**: required on **all** function signatures — no untyped `def`
- **No bare `except`**: always catch specific exceptions (`except ValueError:`, etc.)
- **No `print()`** in production code — use `logging.getLogger(__name__)`

```python
# ✅ CORRECT
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def compute_totals(df: DataFrame, group_col: str) -> DataFrame:
    """Return per-group revenue sums."""
    return df.groupBy(group_col).agg(F.sum("amount").alias("total"))


# ❌ WRONG — missing types, star import, bare except, print
def compute_totals(df, group_col):
    try:
        return df.groupBy(group_col).agg({"amount": "sum"})
    except:
        print("failed")
```

## SparkSession Pattern

Use the `SPARK_MASTER` env var so scripts run locally without modification:

```python
import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("descriptive-job-name")
    .master(os.environ.get("SPARK_MASTER", "local[*]"))
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.shuffle.partitions", "4")   # use 200 for cluster examples
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
```

- Omit `.master()` for jobs designed only for `spark-submit` cluster submission.
- Set `spark.ui.enabled = false` in tests and local scripts.
- Always call `spark.stop()` at the end of standalone scripts.
- Never create a new SparkSession inside a library function — receive it as a parameter.

## Imports

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F        # always alias as F — never star-import
from pyspark.sql.window import Window
from pyspark.sql.types import (
    DoubleType, LongType, StringType,
    StructField, StructType,
)
```

## Environment Variables Pattern

Scripts that read or write data use env vars with safe fallbacks:

```python
import os

INPUT_PATH  = os.environ.get("INPUT_PATH")           # None → use in-memory sample data
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/spark_output")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
```

## DataFrame Operations

- Chain transformations — avoid reassigning the same variable.
- Prefer `F.col("name")` over `df["name"]` for portability.
- Use `F.expr(...)` for complex SQL expressions inside the DataFrame API.

```python
result = (
    df
    .filter(F.col("status") == "active")
    .withColumn("amount_with_tax", F.col("amount") * 1.1)
    .groupBy("region")
    .agg(
        F.round(F.sum("amount_with_tax"), 2).alias("total_revenue"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
    .orderBy(F.desc("total_revenue"))
)
```

## SQL vs DataFrame API

- Use `spark.sql(...)` for multi-step analytical queries or when SQL is clearer.
- Use the DataFrame API for reusable transformation functions in library code.
- Do not mix both styles in the same function.

## Schema Definition

Always define schemas explicitly when reading external data:

```python
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

ORDERS_SCHEMA = StructType([
    StructField("order_id",    LongType(),   nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("amount",      DoubleType(), nullable=True),
])

df = spark.read.schema(ORDERS_SCHEMA).parquet(INPUT_PATH)
```

Never use `inferSchema=True` in production code — it triggers an extra full scan.

## Window Functions

```python
from pyspark.sql.window import Window

w = (
    Window
    .partitionBy("region")
    .orderBy("order_date")
    .rowsBetween(Window.unboundedPreceding, 0)
)

df = df.withColumn("running_total", F.sum("revenue").over(w))
```

## Output

- Prefer Parquet: `df.write.mode("overwrite").parquet(OUTPUT_PATH)`
- Partition large outputs: `.partitionBy("year_month")`
- Always call `spark.stop()` at the end of standalone scripts.

## Module Structure

```
src/
  <topic>/          # one directory per SQL/domain topic
    *.sql           # SQL examples
    *.py            # PySpark helpers (optional, one concern per file)
```

- No circular imports.
- No code executed at module level (no `spark.read.csv(...)` at import time).
- Keep files small — one logical concern per file.

## Logging

```python
import logging

logger = logging.getLogger(__name__)
logger.info("Processing %d rows from %s", df.count(), INPUT_PATH)
logger.warning("No data found for region: %s", region)
```

## Standalone Script Entry Point

Every standalone script must include a direct-run guard:

```python
if __name__ == "__main__":
    main()
    spark.stop()
```
