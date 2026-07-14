# Null Handling

Spark represents missing values as `null`. Many functions silently propagate or skip
nulls — understanding the behaviour prevents silent data quality issues.

## Null Behaviour Summary

| Operation | With nulls |
|-----------|-----------|
| `F.sum("col")` | Ignores nulls |
| `F.avg("col")` | Ignores nulls |
| `F.count(F.col("col"))` | Ignores nulls |
| `F.count("*")` | Counts every row, including nulls |
| `F.max / min` | Ignores nulls |
| `col == value` | `null == anything` → `null` (not `true`/`false`) |
| `col.eqNullSafe(value)` | `null == null` → `true` |

## Drop Null Rows

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("null-handling")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

data = [
    (1, "Alice", "North",  999.99),
    (2, "Bob",   None,    1499.50),
    (3, None,   "East",     None),
    (4, "Dave",  "South",  200.00),
]
df = spark.createDataFrame(data, ["id", "name", "region", "revenue"])

# Drop rows where ANY column is null
df.dropna().show()                           # rows 1 and 4

# Drop rows where ALL columns are null (extreme case)
df.dropna(how="all").show()

# Drop rows where specific columns are null
df.dropna(subset=["name", "revenue"]).show() # rows 1, 2, and 4
```

### Run

```bash
python src/data_frame/transformation/transformations.py
```

## Fill Nulls

```python
# Fill all string columns
df_filled = df.fillna("Unknown")

# Fill specific columns with typed defaults
df_filled = df.fillna({
    "name":    "Unknown",
    "region":  "Unknown",
    "revenue": 0.0,
})
df_filled.show()
```

## Coalesce — First Non-Null

```python
df = df.withColumn(
    "label",
    F.coalesce(F.col("label_override"), F.col("label_default"), F.lit("n/a"))   # (1)!
)
```
1. Returns the first non-null value from left to right. The `F.lit("n/a")` literal
   guarantees a non-null result.

## Null-Safe Equality

```python
# Standard equality — returns null (not false) when either side is null
df.filter(F.col("region") == "North")           # rows where region IS "North"

# Null-safe — null == null → true
df.filter(F.col("region").eqNullSafe("North"))  # same, but null region rows are excluded
df.filter(F.col("region").eqNullSafe(None))     # rows where region IS null
```

## Explicit Null Checks

```python
df.filter(F.col("revenue").isNull())    # rows with no revenue
df.filter(F.col("revenue").isNotNull()) # rows with a revenue value
```

## Replace Nulls in Output Columns

```python
# After a left outer join, fill right-side nulls
result = (employees
          .join(departments, on=["dept_id"], how="left")
          .withColumn("dept_name",
                      F.coalesce(F.col("dept_name"), F.lit("Unassigned"))))
```

!!! warning "Aggregation and nulls"
    `F.sum("revenue")` returns `null` — not `0` — when every row in the group is null.
    Use `F.coalesce(F.sum("revenue"), F.lit(0.0))` if you need a guaranteed numeric result.

!!! tip "Check null counts in your data quality checks"
    ```python
    null_counts = df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in df.columns
    ])
    null_counts.show()
    ```
