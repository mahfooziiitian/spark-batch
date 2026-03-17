---
applyTo: "src/**/*.py,tests/**/*.py"
---

# Python Instructions — PySpark / Python

## Stack

- **Python**: `>=3.11`
- **PySpark**: `3.5.2`
- **Databricks Connect**: `15.4.*`
- **Package manager**: `uv` — use `uv run` for all tool invocations

## Formatters & Linters (all configured in `pyproject.toml`)

| Tool | Purpose | Command |
|------|---------|---------|
| `ruff format` | Code formatting | `uv run task format` |
| `ruff check` | Fast lint + auto-fix | `uv run task format_check` |
| `isort` | Import ordering (black profile) | `uv run task import` |
| `flake8` | Style lint (max-line 128) | `uv run task lint` |
| `mypy` | Static type checking | `uv run task type_check` |

Run all quality checks together:

```bash
uv run task quality
```

## Code Style

- **Max line length**: 128 characters
- **Import order**: isort with `--profile black`
- **String quotes**: double quotes (Ruff/Black default)
- **Type hints**: required on all function signatures
- **No bare `except`**: always catch specific exceptions

```python
# ✅ CORRECT
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def compute_totals(df: DataFrame, group_col: str) -> DataFrame:
    """Return per-group sums of the amount column."""
    return df.groupBy(group_col).agg(F.sum("amount").alias("total"))


# ❌ WRONG — missing types, wrong imports, bare except
def compute_totals(df, group_col):
    try:
        return df.groupBy(group_col).agg({"amount": "sum"})
    except:
        pass
```

## PySpark Conventions

### SparkSession

- Never create a new SparkSession inside a function; receive it as a parameter or use `SparkSession.builder.getOrCreate()` in the entrypoint.
- In tests, use `SparkSession.builder.master("local[*]").getOrCreate()`.

```python
def transform(spark: SparkSession, df: DataFrame) -> DataFrame:
    ...
```

### DataFrame Operations

- Import `pyspark.sql.functions` as `F` — never use star imports
- Prefer SQL expressions (`F.expr(...)`) for complex logic
- Chain transformations; avoid reassigning the same variable

```python
import pyspark.sql.functions as F

result = (
    df
    .filter(F.col("amount") > 0)
    .withColumn("amount_tax", F.col("amount") * 1.1)
    .groupBy("region")
    .agg(F.sum("amount_tax").alias("total"))
)
```

### SQL vs DataFrame API

- Use `spark.sql(...)` for multi-step analytical queries or when a SQL equivalent is clearer
- Use the DataFrame API for reusable transformation functions in library code
- Do not mix both styles in the same function

### Schema Definition

- Define schemas explicitly when reading external data — never rely on `inferSchema=True` in production code

```python
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

ORDERS_SCHEMA = StructType([
    StructField("order_id", LongType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
])
```

## Module Structure

```
src/
  <topic>/          # One directory per SQL/domain topic
    *.sql           # SQL files for the topic
    *.py            # PySpark helpers (optional)
```

- Keep Python files small — one logical concern per file
- No circular imports
- No code execution at module level (no `spark.read.csv(...)` at the top level)

## Logging

Use Python's standard `logging` module — never `print()` in production code:

```python
import logging

logger = logging.getLogger(__name__)
logger.info("Processing %d rows", df.count())
```
