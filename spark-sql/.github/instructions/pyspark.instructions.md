---
applyTo: "src/**/*.py,tests/**/*.py"
---

# PySpark Conventions

## Role in This Repository

PySpark is used primarily to **execute and validate Spark SQL** — not as a DataFrame-first API.
Most logic lives in `.sql` files; Python wraps execution, testing, and orchestration.

## SparkSession

```python
import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("descriptive-name")
    .master(os.environ.get("SPARK_MASTER", "local[*]"))
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
```

- Receive `spark` as parameter in library functions — never create inside.
- Always `spark.stop()` at end of standalone scripts.
- Use `if __name__ == "__main__":` guard.

## Executing SQL

```python
# Preferred — run SQL directly
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
from pyspark.sql import functions as F        # always alias as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType
```

- Never `from pyspark.sql.functions import *`.
- Prefer `F.col("name")` over `df["name"]`.

## Rules

- Type hints on all function signatures.
- No `print()` — use `logging.getLogger(__name__)`.
- No bare `except`.
- No `inferSchema=True` — define schemas explicitly.
- Prefer Parquet output; CSV only for non-technical audiences.

## DataFrame API (when used)

- Chain transformations — don't reassign variables.
- Use `F.expr(...)` for complex SQL inside DataFrame API.
- Don't mix SQL and DataFrame styles in the same function.
