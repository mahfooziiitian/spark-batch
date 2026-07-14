# Deduplication

Remove duplicate rows from a DataFrame using `dropDuplicates` or `distinct`.

## Methods

| Method | Deduplication Scope | Notes |
|--------|---------------------|-------|
| `df.distinct()` | All columns | Equivalent to SQL `SELECT DISTINCT *` |
| `df.dropDuplicates()` | All columns | Same as `distinct()` |
| `df.dropDuplicates(["col1", …])` | Subset of columns | Keeps first occurrence per key |
| Window `row_number` + filter | Subset — ordered | Choose **which** duplicate to keep |

## distinct() and dropDuplicates()

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("dedup")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

data = [
    (1, "Alice", "North"),
    (1, "Alice", "North"),   # exact duplicate
    (2, "Bob",   "South"),
    (2, "Bob",   "East"),    # same id, different region
]
df = spark.createDataFrame(data, ["id", "name", "region"])

# Remove exact row duplicates
deduped_all = df.distinct()
deduped_all.show()   # 3 rows

# Keep first occurrence per id (any region)
deduped_by_id = df.dropDuplicates(["id"])   # (1)!
deduped_by_id.show()  # 2 rows — order not guaranteed
```
1. Subset dedup keeps one arbitrary row per key — use window `row_number` when you
   need to control *which* duplicate is retained.

### Run

```bash
python src/data_frame/transformation/transformations.py
```

## Ordered Deduplication (Keep Latest)

Use `row_number` when the "best" duplicate must be chosen by an ordering criterion:

```python
from pyspark.sql.window import Window

w = Window.partitionBy("id").orderBy(F.desc("updated_at"))  # (1)!

deduped = (df
           .withColumn("rn", F.row_number().over(w))
           .filter(F.col("rn") == 1)
           .drop("rn"))
```
1. Descending `updated_at` — the most recent record gets `rn = 1`.

!!! tip "dropDuplicates is non-deterministic for subset keys"
    Which row is kept when using `dropDuplicates(["id"])` is not guaranteed.
    Use the window `row_number` pattern whenever order matters.

!!! warning "distinct() triggers a shuffle"
    `distinct()` and `dropDuplicates()` require a full shuffle to compare rows
    across partitions. Cache the input if it is reused after deduplication.
