---
applyTo: "src/**/*.py,tests/**/*.py"
---

# Python & PySpark Conventions

## Role in This Repository

PySpark is used primarily to **execute and validate Spark SQL** — not as a DataFrame-first API.
Most logic lives in `.sql` files; Python wraps execution, testing, and orchestration.

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
spark.sparkContext.setLogLevel("WARN")   # use "ERROR" in tests
```

- Never create SparkSession inside library functions — pass as parameter.
- Always call `spark.stop()` at end of standalone scripts.
- Use `if __name__ == "__main__":` guard in every script.

## Executing SQL

```python
result = spark.sql("""
    SELECT customer_id, SUM(amount) AS total
    FROM orders
    GROUP BY customer_id
""")

# For file-based SQL
from pathlib import Path
sql_text = Path("src/scd/type2/expire.sql").read_text()
spark.sql(sql_text)
```

## Imports

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F        # always alias as F — never star-import
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType
```

- Prefer `F.col("name")` over `df["name"]`.

## Code Rules

- Type hints on **all** function signatures.
- Max line length: 128.
- No `print()` — use `logging.getLogger(__name__)`.
- No bare `except` — catch specific exceptions.
- No code at module level (no side effects on import).
- No `inferSchema=True` — define schemas explicitly.

## DataFrame API (when used)

- Chain transformations — don't reassign variables.
- Use `F.expr(...)` for complex SQL inside DataFrame API.
- Don't mix SQL and DataFrame styles in the same function.

## Environment Variables

```python
INPUT_PATH   = os.environ.get("INPUT_PATH")           # None → use in-memory sample
OUTPUT_PATH  = os.environ.get("OUTPUT_PATH", "/tmp/spark_output")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
```

## Output

- Prefer Parquet. Partition large outputs: `.partitionBy("year_month")`.
- CSV only for non-technical audiences.
