# Grouping ID — Identifying Subtotal Rows

`F.grouping()` and `F.grouping_id()` let you distinguish **aggregation-produced NULLs**
(subtotal / grand-total rows) from genuine NULL values in the source data.

## Functions

| Function | Returns | Use case |
|----------|---------|----------|
| `F.grouping(col)` | `1` if `col` is NULL due to aggregation; `0` otherwise | Single-column subtotal flag |
| `F.grouping_id(c1, c2, ...)` | Integer bitmask — bit `i` is `1` when column `i` is aggregated | Multi-column level identification |

## Bitmask Reference

For `grouping_id("region", "category")`:

| `gid` | Binary | `region` | `category` | Meaning |
|-------|--------|----------|------------|---------|
| `0` | `00` | present | present | Detail row |
| `1` | `01` | present | NULL | Region subtotal |
| `2` | `10` | NULL | present | Category subtotal *(CUBE only)* |
| `3` | `11` | NULL | NULL | Grand total |

The leftmost column maps to the highest bit.

## Example — grouping()

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("olap-grouping-id")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

data = [
    ("North", "Electronics", "2023", "Q1", 15000.0),
    ("North", "Apparel",     "2023", "Q1",  8000.0),
    ("South", "Electronics", "2023", "Q1", 12000.0),
    ("South", "Apparel",     "2023", "Q1",  6000.0),
]
df = spark.createDataFrame(data, ["region", "category", "year", "quarter", "revenue"])

result = (
    df.rollup("region", "category")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.grouping("region").alias("region_is_subtotal"),       # (1)!
        F.grouping("category").alias("category_is_subtotal"),
    )
    .orderBy(
        F.col("region").asc_nulls_last(),
        F.col("category").asc_nulls_last(),
    )
)
result.show(truncate=False)
```

1. `grouping("col")` returns `1` for every row where `col` is NULL because of
   rollup/cube/grouping sets — not because the source value was NULL.

## Example — grouping_id() with Level Labels

```python
result = (
    df.rollup("region", "category")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.grouping_id("region", "category").alias("gid"),       # (1)!
    )
    .withColumn(
        "level",
        F.when(F.col("gid") == 0, "detail")
         .when(F.col("gid") == 1, "region subtotal")
         .when(F.col("gid") == 3, "grand total"),
    )
    .orderBy(
        F.col("region").asc_nulls_last(),
        F.col("category").asc_nulls_last(),
    )
)
result.show(truncate=False)
```

1. Column order in `grouping_id()` must match the order used in `rollup()` / `cube()`.

### Run

```bash
python src/data_frame/analytical/olap/grouping_id/olap_grouping_id.py
```

## Filter by Aggregation Level

Use `gid` to extract only the rows you need without post-processing in the driver:

```python
# Grand total only
grand_total = result.filter(F.col("gid") == 3)

# Subtotal rows only (exclude detail)
subtotals = result.filter(F.col("gid") >= 1)

# Detail rows only
detail = result.filter(F.col("gid") == 0)
```

## Three-Dimension Bitmask

For `grouping_id("region", "category", "year")` with `rollup`:

| `gid` | Binary | Meaning |
|-------|--------|---------|
| `0` | `000` | Detail `(region, category, year)` |
| `1` | `001` | Year subtotal `(region, category)` |
| `3` | `011` | Category+year subtotal `(region)` |
| `7` | `111` | Grand total |

```python
result = (
    df.rollup("region", "category", "year")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.grouping_id("region", "category", "year").alias("gid"),
    )
    .withColumn(
        "level",
        F.when(F.col("gid") == 0, "detail")
         .when(F.col("gid") == 1, "region+category subtotal")
         .when(F.col("gid") == 3, "region subtotal")
         .when(F.col("gid") == 7, "grand total"),
    )
)
```

## SQL Equivalent

```sql
SELECT
    region,
    category,
    ROUND(SUM(revenue), 2)         AS total_revenue,
    GROUPING(region)               AS region_is_subtotal,
    GROUPING(category)             AS category_is_subtotal,
    GROUPING_ID(region, category)  AS gid
FROM sales
GROUP BY ROLLUP(region, category)
ORDER BY region ASC NULLS LAST, category ASC NULLS LAST
```

!!! tip "Always use grouping_id() when source data may contain NULLs"
    If your source data has `NULL` values in any dimension column, you cannot
    rely on `IS NULL` to detect subtotal rows — the grouping flags are the only
    reliable way.

!!! success "Good fit for grouping_id"
    - Pipelines that write rollup/cube output to a table and need a level column
    - Filtering summary output to a specific aggregation level in a single query
    - Replacing multiple union queries that each compute one level separately
