# DataFrame Assertions

DataFrame assertions compare **two entire DataFrames** — schema and data.
Use these when testing functions that return a new DataFrame.

## assert_df_equality

Compares two DataFrames for exact equality: same schema, same rows, same order.

```python
from chispa import assert_df_equality
```

### Basic Pattern

```python
def test_sort_columns_ascending(self, spark):
    source_df = spark.createDataFrame(
        [("jose", "oak", "switch")],
        ["name", "tree", "gaming_system"],
    )
    actual_df = sort_columns(source_df, "asc")
    expected_df = spark.createDataFrame(
        [("switch", "jose", "oak")],
        ["gaming_system", "name", "tree"],
    )
    assert_df_equality(actual_df, expected_df)
```

### Ignoring Row Order

When row order doesn't matter (e.g., after a `subtract` or `distinct`):

```python
def test_union_dedup(self, spark):
    df1 = spark.createDataFrame([(1, "a")], ["id", "val"])
    df2 = spark.createDataFrame([(2, "b")], ["id", "val"])
    result = union_dedup(df1, df2)
    expected = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    assert_df_equality(result, expected, ignore_row_order=True)  # (1)!
```

1. `ignore_row_order=True` sorts both DataFrames before comparing.

### Testing Transformations

**Column renaming:**

```python
def test_dots_to_underscores(self, spark):
    source_df = spark.createDataFrame([("jose", 8)], ["first.name", "person.favorite.number"])
    actual_df = modify_column_names(source_df, dots_to_underscores)
    expected_df = spark.createDataFrame([("jose", 8)], ["first_name", "person_favorite_number"])
    assert_df_equality(actual_df, expected_df)
```

**Deduplication:**

```python
def test_no_duplicates_unchanged(self, spark):
    data = [(1, "a", 1), (2, "b", 2)]
    df = spark.createDataFrame(data, ["id", "val", "ts"])
    result = deduplicate(df, subset=["id"], order_col="ts")
    assert_df_equality(result, df)
```

### Testing Empty DataFrames

Verify that functions handle zero rows correctly:

```python
def test_empty_dataframe(self, spark):
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("a.b", StringType()),
            StructField("c.d", LongType()),
        ]
    )
    source_df = spark.createDataFrame([], schema)
    actual_df = modify_column_names(source_df, dots_to_underscores)
    assert actual_df.columns == ["a_b", "c_d"]
    assert actual_df.count() == 0
```

## assert_approx_df_equality

For DataFrames with floating-point values where exact equality is impractical.

```python
from chispa.dataframe_comparer import assert_approx_df_equality
```

### Within Threshold

```python
def test_within_threshold(self, spark):
    df1 = spark.createDataFrame([(1.1, "a"), (2.2, "b")], ["num", "letter"])
    df2 = spark.createDataFrame([(1.05, "a"), (2.13, "b")], ["num", "letter"])
    assert_approx_df_equality(df1, df2, 0.1)  # (1)!
```

1. Each numeric value must be within ±0.1 of its counterpart.

### Exceeds Threshold (Expected Failure)

```python
def test_exceeds_threshold_raises(self, spark):
    df1 = spark.createDataFrame([(1.1, "a"), (2.2, "b")], ["num", "letter"])
    df2 = spark.createDataFrame([(1.1, "a"), (5.0, "b")], ["num", "letter"])
    with pytest.raises(Exception):
        assert_approx_df_equality(df1, df2, 0.1)
```

## Common Patterns

### Row diff for data quality

```python
def test_returns_rows_only_in_left(self, spark):
    left = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    right = spark.createDataFrame([(2,), (3,)], ["id"])
    result = row_diff(left, right)
    expected = spark.createDataFrame([(1,)], ["id"])
    assert_df_equality(result, expected)
```

### Asserting count and columns without chispa

For simple checks, standard assertions are fine:

```python
def test_descending_column_order(self, spark):
    df = spark.createDataFrame([("a", 1, True)], ["alpha", "beta", "gamma"])
    result = sort_columns(df, "desc")
    assert result.columns == ["gamma", "beta", "alpha"]
    assert result.count() == 1
```

## Run Tests

```bash
uv run pytest tests/equality/ tests/transformation/ -v
```
