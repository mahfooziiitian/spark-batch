# Sort

Order DataFrame rows using `orderBy` (alias `sort`).

## API Reference

| Method | Scope | Shuffle? | Use Case |
|--------|-------|:--------:|----------|
| `df.orderBy(*cols)` | Global | ✅ | Final output ordering, top-N queries |
| `df.sort(*cols)` | Global | ✅ | Alias for `orderBy` |
| `df.sortWithinPartitions(*cols)` | Per-partition | ❌ | Intra-partition ordering before write |

### Null Ordering

| Expression | Null Placement |
|-----------|----------------|
| `F.col("c").asc()` | Nulls first (default) |
| `F.col("c").asc_nulls_last()` | Nulls last |
| `F.col("c").desc()` | Nulls last (default) |
| `F.col("c").desc_nulls_first()` | Nulls first |

## Examples

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("sort")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

data = [
    (3, "Carol", "East",  200.00),
    (1, "Alice", "North", 999.99),
    (4, "Dave",  "North",  50.00),
    (2, "Bob",   "South", 1499.50),
]
df = spark.createDataFrame(data, ["id", "name", "region", "revenue"])

# Ascending (default)
df.orderBy("revenue").show()

# Descending
df.orderBy(F.desc("revenue")).show()                          # (1)!

# Multiple keys — region ascending, then revenue descending
df.orderBy(F.asc("region"), F.desc("revenue")).show()        # (2)!

# Nulls first / nulls last
df.orderBy(F.col("revenue").asc_nulls_last()).show()          # (3)!
```
1. `F.desc("col")` is equivalent to `F.col("col").desc()`.
2. Sort stability: Spark's sort is not guaranteed to be stable across partitions.
3. `asc_nulls_last` / `desc_nulls_first` control null placement explicitly.

### Run

```bash
python src/data_frame/transformation/transformations.py
```

## Common Patterns

### Top-N Rows

```python
top_3 = df.orderBy(F.desc("revenue")).limit(3)
```

### Sort by Expression

```python
df.orderBy(F.abs(F.col("revenue") - 500)).show()
```

### Sort Within Partitions Before Write

```python
(df
 .repartition("region")
 .sortWithinPartitions("revenue")
 .write.mode("overwrite")
 .partitionBy("region")
 .parquet("/tmp/sorted_output"))
```

!!! warning "orderBy triggers a full shuffle"
    Sorting requires all data to flow through a single sort stage. Avoid
    unnecessary sorts in intermediate pipeline steps — only sort at the final
    output or when required by a downstream operation.

!!! tip "Use sortWithinPartitions for local ordering"
    `df.sortWithinPartitions("col")` sorts within each partition without a shuffle —
    useful when writing partitioned Parquet and you only need intra-partition order.

!!! note "Sort stability"
    Spark's sort is **not guaranteed to be stable** — rows with equal keys may
    appear in any order. Add a tiebreaker column (e.g., `id`) if deterministic
    ordering is required.
