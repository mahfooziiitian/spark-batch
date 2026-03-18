# Window Null Options

`first`, `last`, `lag`, and `lead` accept an `ignorenulls` parameter that controls
whether `null` values in the target column are skipped when scanning the window.

## API Reference

| Function | `ignorenulls=False` (default) | `ignorenulls=True` |
|----------|-------------------------------|---------------------|
| `F.first(col)` | Returns the first value, even if `null` | Skips nulls; returns the first non-null value |
| `F.last(col)` | Returns the last value, even if `null` | Skips nulls; returns the last non-null value |
| `F.lag(col, n)` | Returns `null` if the row `n` positions back is null | Looks further back to find a non-null value |
| `F.lead(col, n)` | Returns `null` if the row `n` positions ahead is null | Looks further ahead to find a non-null value |

## Behaviour

| Mode | Meaning |
|------|---------|
| `RESPECT NULLS` (default) | `null` in the column counts as a value; `lag`/`lead` return `null` if the offset row is null |
| `IGNORE NULLS` | `first`/`last` skip `null` rows; `lag`/`lead` look further back/forward to find a non-null value |

## Example

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("window-null-options")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

data = [
    ("A", 1, 10.0),
    ("A", 2, None),
    ("A", 3, None),
    ("A", 4, 40.0),
]
df = spark.createDataFrame(data, ["group", "step", "value"])

w = Window.partitionBy("group").orderBy("step")

result = (df
          .withColumn("last_respect",
                      F.last("value", ignorenulls=False).over(w))   # (1)!
          .withColumn("last_ignore",
                      F.last("value", ignorenulls=True).over(w))    # (2)!
          .withColumn("lag_ignore",
                      F.lag("value", 1, ignorenulls=True).over(w))) # (3)!
result.show()
```
1. Default — returns `null` for rows 2 and 3 because the running last is `null`.
2. `ignorenulls=True` — skips nulls, so rows 2 and 3 still return `10.0`.
3. `lag` with `ignorenulls=True` looks back past the null rows to find `10.0`.

### Expected Output

| group | step | value | last_respect | last_ignore | lag_ignore |
|-------|------|-------|-------------|-------------|------------|
| A     | 1    | 10.0  | 10.0        | 10.0        | null       |
| A     | 2    | null  | null        | 10.0        | 10.0       |
| A     | 3    | null  | null        | 10.0        | 10.0       |
| A     | 4    | 40.0  | 40.0        | 40.0        | 10.0       |

### Run

```bash
python src/data_frame/analytical/window_function/null_option/window_null_option.py
```

## Forward-Fill Pattern

Use `last(..., ignorenulls=True)` with an unbounded frame to forward-fill nulls:

```python
w_ff = (Window
        .partitionBy("group")
        .orderBy("step")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow))

df = df.withColumn("value_filled", F.last("value", ignorenulls=True).over(w_ff))
```

## Backward-Fill Pattern

Use `first(..., ignorenulls=True)` scanning from current row to end of partition:

```python
w_bf = (Window
        .partitionBy("group")
        .orderBy("step")
        .rowsBetween(Window.currentRow, Window.unboundedFollowing))

df = df.withColumn("value_backfilled", F.first("value", ignorenulls=True).over(w_bf))
```

!!! tip "Spark 3.2+ syntax"
    `ignorenulls` as a keyword argument was stabilised in Spark 3.0+.
    For older versions use the positional form: `F.last("value", True)`.

!!! warning "Aggregation functions ignore nulls by default"
    `F.sum()`, `F.avg()`, `F.min()`, `F.max()` over a window always skip nulls —
    they do not have an `ignorenulls` parameter. Only `first`, `last`, `lag`, and
    `lead` offer the choice.

## Full Source

```python title="src/data_frame/analytical/window_function/null_option/window_null_option.py"
--8<-- "src/data_frame/analytical/window_function/null_option/window_null_option.py"
```
