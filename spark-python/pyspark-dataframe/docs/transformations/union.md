# Union

Combine rows from two DataFrames with the same schema into one.

## Methods

| Method | Alignment | Behaviour |
|--------|-----------|-----------|
| `df1.union(df2)` | By position | Columns matched left-to-right — schema names ignored |
| `df1.unionByName(df2)` | By name | Columns matched by name — order-independent *(preferred)* |
| `df1.unionByName(df2, allowMissingColumns=True)` | By name + fill | Fills missing columns with `null` |

## unionByName (Recommended)

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("union")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

q1 = spark.createDataFrame(
    [(1, "Alice", "North", 100.0), (2, "Bob", "South", 200.0)],
    ["id", "name", "region", "revenue"],
)
q2 = spark.createDataFrame(
    [(3, "Carol", "East", 150.0), (4, "Dave", "West", 300.0)],
    ["id", "name", "region", "revenue"],
)

combined = q1.unionByName(q2)   # (1)!
combined.show()
```
1. `unionByName` is safe even if the column order differs between DataFrames.

### Run

```bash
python src/data_frame/transformation/transformations.py
```

## Union with Different Schemas

```python
base = spark.createDataFrame([(1, "Alice")], ["id", "name"])
extra = spark.createDataFrame([(2, "Bob", "North")], ["id", "name", "region"])

# Fill missing columns with null instead of raising an error
combined = base.unionByName(extra, allowMissingColumns=True)   # (1)!
combined.show()
# Row 1 has region = null
```
1. Requires Spark 3.1+.

## union() — Position-Based (Use with Care)

```python
df1 = spark.createDataFrame([(1, "Alice")], ["id", "name"])
df2 = spark.createDataFrame([("Bob", 2)],   ["name", "id"])   # reversed order!

# union() matches by position — "Bob" ends up in the id column
df1.union(df2).show()   # produces wrong results silently
```

!!! warning "union() ignores column names"
    Always use `unionByName` unless you are certain both DataFrames have identical
    column order. `union()` with mis-ordered columns produces silently wrong data.

!!! tip "Combining many DataFrames"
    Use `functools.reduce` to union a list:
    ```python
    from functools import reduce
    result = reduce(lambda a, b: a.unionByName(b), df_list)
    ```
