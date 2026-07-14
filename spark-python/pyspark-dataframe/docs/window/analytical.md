# Analytical Window Functions

Access values from **other rows** within the same window partition — the row before
(`lag`), the row after (`lead`), or the first/last value in the partition.

## Function Reference

| Function | Returns | Signature |
|----------|---------|-----------|
| `lag(col, offset, default)` | Value from `offset` rows **before** current row | `F.lag("revenue", 1, 0)` |
| `lead(col, offset, default)` | Value from `offset` rows **after** current row | `F.lead("revenue", 1, 0)` |
| `first(col)` | First value in the partition (or frame) | `F.first("revenue")` |
| `last(col)` | Last value in the partition (or frame) | `F.last("revenue")` |

## Lag and Lead

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("analytical-functions")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

data = [
    ("North", "2024-01", 100.0),
    ("North", "2024-02", 200.0),
    ("North", "2024-03", 150.0),
    ("South", "2024-01",  80.0),
    ("South", "2024-02", 120.0),
]
df = spark.createDataFrame(data, ["region", "month", "revenue"])

w = Window.partitionBy("region").orderBy("month")

result = (df
          .withColumn("prev_revenue",  F.lag("revenue",  1, 0.0).over(w))   # (1)!
          .withColumn("next_revenue",  F.lead("revenue", 1, 0.0).over(w))   # (2)!
          .withColumn("mom_change",    F.col("revenue") - F.col("prev_revenue")))
result.show()
```
1. `lag` with `default=0.0` — first row in each partition returns `0.0` instead of `null`.
2. `lead` with `default=0.0` — last row returns `0.0` instead of `null`.

### Run

```bash
python src/data_frame/analytical/window_function/analytical/analytical_function_lead_lag.py
```

## First and Last

```python
w_full = (Window
          .partitionBy("region")
          .orderBy("month")
          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))

result = (df
          .withColumn("first_revenue", F.first("revenue").over(w_full))   # (1)!
          .withColumn("last_revenue",  F.last("revenue").over(w_full)))
result.show()
```
1. Use `rowsBetween(unboundedPreceding, unboundedFollowing)` to look at the entire
   partition; without a frame, `first`/`last` only sees up to the current row.

### Run

```bash
python src/data_frame/analytical/window_function/analytical/analytical_function_first_last.py
```

## Revenue Difference from Previous Row

```python
result = (df
          .withColumn("prev",  F.lag("revenue", 1).over(w))
          .withColumn("delta", F.round(F.col("revenue") - F.col("prev"), 2))
          .fillna({"prev": 0.0, "delta": 0.0}))
```

!!! tip "Choose default vs fillna"
    Provide the `default` argument in `lag`/`lead` to substitute a value for
    boundary rows. Alternatively leave it `null` and use `fillna` or `coalesce`
    after the `withColumn` step.
