# Aggregations

Group rows and compute summary statistics using `groupBy` and `agg`.

## API Quick Reference

| Method | Purpose |
|--------|---------|
| `groupBy(*cols)` | Define the grouping key |
| `agg(*exprs)` | Apply one or more aggregate functions |
| `rollup(*cols)` | Hierarchical subtotals |
| `cube(*cols)` | All combinations of subtotals |
| `F.sum / avg / min / max / count` | Basic aggregates |
| `F.countDistinct` | Distinct value count |
| `F.collect_list / collect_set` | Gather values into an array |
| `F.first / last` | First or last value in the group |
| `F.stddev / variance` | Statistical measures |

## groupBy + agg

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("aggregations")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

data = [
    ("North", "Widgets", "active",  999.99,  "C001"),
    ("North", "Gadgets", "active", 1499.50,  "C002"),
    ("South", "Widgets", "active",  750.00,  "C001"),
    ("South", "Gadgets", "inactive",200.00,  "C003"),
    ("East",  "Widgets", "active",  500.00,  "C004"),
]
df = spark.createDataFrame(data, ["region", "category", "status", "revenue", "customer_id"])

result = (df
          .filter(F.col("status") == "active")               # (1)!
          .groupBy("region", "category")                     # (2)!
          .agg(
              F.round(F.sum("revenue"), 2).alias("total_revenue"),
              F.round(F.avg("revenue"), 2).alias("avg_revenue"),
              F.countDistinct("customer_id").alias("unique_customers"),
              F.max("revenue").alias("max_revenue"),
          )
          .orderBy(F.desc("total_revenue")))                  # (3)!
result.show()
```
1. Filter before groupBy — reduces input data to the aggregation stage.
2. Multi-column grouping key.
3. Sort the result by the most important metric descending.

### Run

```bash
python src/data_frame/dataframe.py
```

## rollup — Hierarchical Subtotals

```python
result = (df
          .rollup("region", "category")    # (1)!
          .agg(F.sum("revenue").alias("total"))
          .orderBy("region", "category"))
result.show()
# Includes: (region, category), (region, null), (null, null)
```
1. Produces subtotals at each level of the hierarchy plus a grand total.

## cube — All Combinations

```python
result = (df
          .cube("region", "category")     # (1)!
          .agg(F.sum("revenue").alias("total"))
          .orderBy("region", "category"))
# Includes: all combinations — (r,c), (r,null), (null,c), (null,null)
```
1. Cube produces more rows than rollup — use `F.grouping(col)` to distinguish
   subtotal rows from genuine nulls.

## collect_list and collect_set

```python
result = (df
          .groupBy("region")
          .agg(
              F.collect_set("category").alias("categories"),     # unique values
              F.collect_list("customer_id").alias("all_customers"),
          ))
```

!!! warning "collect_list can be large"
    `collect_list` accumulates all values into a single array in memory on one
    executor. Avoid it on high-cardinality groups or large DataFrames.

!!! tip "countDistinct vs count(distinct col)"
    Use `F.countDistinct("col")` — it generates an optimised HyperLogLog plan.
    `F.count(F.expr("distinct col"))` is equivalent but less idiomatic.
