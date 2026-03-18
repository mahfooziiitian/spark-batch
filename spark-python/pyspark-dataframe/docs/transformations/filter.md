# Filter

Select rows that satisfy a condition using `filter` (alias `where`).

## Syntax

```python
df.filter(condition)     # preferred
df.where(condition)      # alias — identical behaviour
```

## Examples

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("filter")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

data = [
    (1, "Alice", "North", 999.99,  "active"),
    (2, "Bob",   "South", 1499.50, "active"),
    (3, "Carol", "East",  200.00,  "inactive"),
    (4, "Dave",  "North", 50.00,   "active"),
]
df = spark.createDataFrame(data, ["id", "name", "region", "revenue", "status"])

# Simple equality
active = df.filter(F.col("status") == "active")                    # (1)!

# Range condition
high_value = df.filter(F.col("revenue") > 500)

# Multiple conditions (AND)
north_active = df.filter(
    (F.col("region") == "North") & (F.col("status") == "active")  # (2)!
)

# IN list
target_regions = df.filter(F.col("region").isin("North", "East"))

# SQL expression string
sql_filter = df.filter("revenue > 500 AND status = 'active'")      # (3)!

active.show()
```
1. Always use `F.col()` — not `df["status"]` which binds to the specific object.
2. Use `&` for AND, `|` for OR, `~` for NOT — not Python `and`/`or`/`not`.
3. SQL strings work but lose IDE type checking; prefer `F.col()` expressions.

### Run

```bash
python src/data_frame/transformation/transformations.py
```

## Null-Safe Filter

```python
# Keep rows where discount IS null
df.filter(F.col("discount").isNull())

# Keep rows where discount IS NOT null
df.filter(F.col("discount").isNotNull())

# Null-safe equality (null == null → True)
df.filter(F.col("region").eqNullSafe("North"))
```

!!! tip "Push filters early"
    Filter as early as possible in your pipeline — before joins, groupBy, and
    withColumn — to reduce the data flowing through expensive operations.
    Spark's Catalyst optimizer usually handles this, but being explicit makes
    intent clear and helps when Catalyst cannot reorder (e.g., after a UDF).
