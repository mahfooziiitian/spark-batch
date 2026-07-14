# Pivoting

Transform rows into columns — rotate a categorical column into multiple value columns.

```mermaid
graph LR
    R[Row-oriented data\nregion  quarter  revenue] -->|pivot| C[Column-oriented\nregion  Q1  Q2  Q3  Q4]
```

## Explicit Pivot (Recommended)

Always specify pivot values explicitly to avoid an extra scan for value discovery:

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("pivot")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

data = [
    ("North", "Q1", 100.0), ("North", "Q2", 200.0),
    ("North", "Q3", 150.0), ("North", "Q4", 300.0),
    ("South", "Q1",  80.0), ("South", "Q2", 120.0),
    ("South", "Q3",  90.0), ("South", "Q4", 200.0),
]
df = spark.createDataFrame(data, ["region", "quarter", "revenue"])

pivot_values = ["Q1", "Q2", "Q3", "Q4"]                           # (1)!

result = (df
          .groupBy("region")                                       # (2)!
          .pivot("quarter", pivot_values)                          # (3)!
          .agg(F.round(F.sum("revenue"), 2).alias("revenue")))    # (4)!
result.show()
```
1. Explicit list avoids the discovery scan — Spark would otherwise scan the whole
   DataFrame to find distinct values.
2. `groupBy` defines the row dimension.
3. `pivot` defines the column dimension.
4. `agg` defines the cell value; prefix the alias to get cleaner column names.

### Run

```bash
python src/data_frame/analytical/pivoting/create_pivot_df.py
```

## Dynamic Pivot

When pivot values are unknown at write time:

```python
pivot_values = [r[0] for r in df.select("quarter").distinct().orderBy("quarter").collect()]
result = df.groupBy("region").pivot("quarter", pivot_values).agg(F.sum("revenue"))
```

!!! warning "Performance cost"
    Dynamic pivot collects distinct values to the driver and adds a full scan.
    Cache `df` before calling `.distinct().collect()` if it is computed from
    an expensive query.

## Multiple Aggregations

```python
result = (df
          .groupBy("region")
          .pivot("quarter", ["Q1", "Q2", "Q3", "Q4"])
          .agg(
              F.sum("revenue").alias("total"),
              F.count("revenue").alias("count"),
          ))
# Column names become Q1_total, Q1_count, Q2_total, …
```

## Unpivot (Wide to Long)

Convert pivoted columns back to rows using `stack`:

```python
result = result.select(
    "region",
    F.expr("stack(4, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3, 'Q4', Q4) AS (quarter, revenue)")
)
```

!!! tip "Spark 3.4+ unpivot"
    Spark 3.4 added a native `DataFrame.unpivot()` (alias `melt`) method:
    ```python
    result.unpivot(["region"], ["Q1","Q2","Q3","Q4"], "quarter", "revenue").show()
    ```

!!! success "Good fit for pivot"
    - Producing cross-tab or pivot-table reports
    - Reshaping time-series data for comparison (one column per period)

!!! failure "Avoid pivot when"
    - The number of pivot columns is very large (> 100) — the schema becomes unwieldy
    - Pivot values are user-supplied at runtime — validate them first to prevent injection
