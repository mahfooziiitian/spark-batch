# Count

Return a single integer (or boolean) to the driver representing the number of rows
in a DataFrame. Count actions always trigger a full scan unless the DataFrame is
cached.

## API Reference

| Method | Returns | Description |
|--------|---------|-------------|
| `df.count()` | `int` | Total rows including those with NULLs |
| `df.distinct().count()` | `int` | Number of unique rows |
| `F.count("*")` | Column | Count all rows (use inside `agg()`) |
| `F.count("col")` | Column | Count non-NULL values in a column |
| `F.countDistinct("col")` | Column | Unique non-NULL values in a column |
| `F.approx_count_distinct("col")` | Column | HyperLogLog approximate distinct count |
| `df.isEmpty()` | `bool` | `True` when the DataFrame has zero rows (Spark 3.3+) |

## Examples

### Basic count

```python
total = df.count()                           # (1)!
print(f"Total rows: {total}")
```
1. `count()` includes rows that contain NULL values in any column.

### count(\*) vs count(col)

```python
from pyspark.sql import functions as F

count_star = df.select(F.count("*")).first()[0]       # includes NULLs
count_col  = df.select(F.count("customer_id")).first()[0]  # excludes NULLs
```

### Filtered count

```python
from pyspark.sql import functions as F

active   = df.filter(F.col("status") == "active").count()
null_ids = df.filter(F.col("customer_id").isNull()).count()
```

### Distinct counts

```python
from pyspark.sql import functions as F

distinct_rows    = df.distinct().count()
unique_products  = df.select(F.countDistinct("product")).first()[0]
unique_customers = (
    df.filter(F.col("customer_id").isNotNull())
    .select(F.countDistinct("customer_id"))
    .first()[0]
)
```

### isEmpty() — empty check (Spark 3.3+)

```python
print(df.isEmpty())                          # (1)!

# Pre-3.3 alternative
is_empty = len(df.take(1)) == 0
```
1. `isEmpty()` may short-circuit earlier than `count() == 0` because it only
   needs to verify that at least one row exists.

### Count-based pipeline guards

```python
from pyspark.sql import functions as F

row_count = df.count()
assert row_count > 0, "DataFrame must not be empty"

null_ids = df.filter(F.col("id").isNull()).count()
assert null_ids == 0, f"Found {null_ids} rows with NULL id"
```

### Run

```bash
cd spark-python/pyspark-dataframe
python src/data_frame/actions/count/action_count.py
```

!!! warning "count() triggers a full scan"
    Every call to `count()` re-executes the full DAG. If you need multiple
    counts over the same data, call `df.cache()` first.

!!! tip "Use approx_count_distinct for large datasets"
    `F.approx_count_distinct("col", rsd=0.05)` uses HyperLogLog and is
    significantly faster than an exact `countDistinct` on billions of rows.

!!! note "count(*) vs count(col)"
    `F.count("*")` counts every row including those with NULLs.
    `F.count("col")` counts only rows where `col` is not NULL.

## Full Source

```python title="src/data_frame/actions/count/action_count.py"
--8<-- "src/data_frame/actions/count/action_count.py"
```
