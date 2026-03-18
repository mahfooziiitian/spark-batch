# Aggregate Window Functions

Apply standard aggregation functions (`sum`, `avg`, `min`, `max`, `count`) over a
window frame, producing one value **per row** rather than one value per group.

## Common Patterns

| Pattern | Function + Frame |
|---------|-----------------|
| Running total | `F.sum("rev").over(rowsBetween(unboundedPreceding, currentRow))` |
| Cumulative max | `F.max("rev").over(rowsBetween(unboundedPreceding, currentRow))` |
| Moving average (3 rows) | `F.avg("rev").over(rowsBetween(-1, 1))` |
| Partition total | `F.sum("rev").over(rowsBetween(unboundedPreceding, unboundedFollowing))` |
| % of partition total | `col / F.sum(col).over(partition_window)` |

## Example — Running Total and Moving Average

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("window-aggregation")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

data = [
    ("North", "2024-01", 100.0),
    ("North", "2024-02", 200.0),
    ("North", "2024-03", 150.0),
    ("North", "2024-04", 300.0),
    ("South", "2024-01",  80.0),
    ("South", "2024-02", 120.0),
]
df = spark.createDataFrame(data, ["region", "month", "revenue"])

w_running = (Window
             .partitionBy("region")
             .orderBy("month")
             .rowsBetween(Window.unboundedPreceding, Window.currentRow))   # (1)!

w_moving = (Window
            .partitionBy("region")
            .orderBy("month")
            .rowsBetween(-1, 1))                                           # (2)!

w_partition = (Window
               .partitionBy("region")
               .rowsBetween(Window.unboundedPreceding,
                            Window.unboundedFollowing))                    # (3)!

result = (df
          .withColumn("running_total",    F.sum("revenue").over(w_running))
          .withColumn("moving_avg_3",     F.round(F.avg("revenue").over(w_moving), 2))
          .withColumn("partition_total",  F.sum("revenue").over(w_partition))
          .withColumn("pct_of_total",     F.round(
                                            F.col("revenue") /
                                            F.col("partition_total") * 100, 1)))
result.show()
```
1. Cumulative sum from start of partition to current row.
2. Rolling 3-row average: previous row, current, and next row.
3. Full partition sum — used as denominator for percentage calculation.

### Run

```bash
python src/data_frame/analytical/window_function/aggregate/window_aggregation_function.py
```

## Maximum Value per Partition

```python
w_full = Window.partitionBy("region").rowsBetween(
    Window.unboundedPreceding, Window.unboundedFollowing)

df = df.withColumn("max_revenue", F.max("revenue").over(w_full))
```

```bash
python src/data_frame/analytical/window_function/usage/maximum_value_per_partition.py
```

!!! note "Frame default with orderBy"
    When you add `.orderBy()` to a window, the implicit frame for aggregates changes
    from the whole partition to `rowsBetween(unboundedPreceding, currentRow)`.
    Always specify the frame explicitly to avoid this implicit behaviour.
