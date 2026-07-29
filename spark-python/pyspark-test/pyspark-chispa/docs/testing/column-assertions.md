# Column Assertions

Column assertions compare two columns within the **same DataFrame**. This is the
most common pattern when testing column-level transformations.

## assert_column_equality

Compares two columns for exact equality, row by row, including null handling.

```python
from chispa.column_comparer import assert_column_equality
```

### Pattern

1. Create a DataFrame with an input column and an expected result column.
2. Apply your function to produce an actual result column.
3. Assert equality between actual and expected.

```python
def test_removes_special_characters(self, spark):
    data = [("jo&&se", "jose"), ("**li**", "li"), (None, None)]
    df = spark.createDataFrame(data, ["name", "expected"]).withColumn(
        "actual", remove_non_word_characters(F.col("name"))
    )
    assert_column_equality(df, "actual", "expected")
```

### Failure Output

When columns don't match, chispa produces a rich diff table:

```
+--------+----------+
|  actual | expected |
+--------+----------+
|  matt7 |     matt |  <-- MISMATCH
|   bill |     bill |
+--------+----------+
```

### Examples from This Project

**String cleaning:**

```python
def test_only_special_characters(self, spark):
    data = [("@#$%^&*", ""), ("!!!", "")]
    df = spark.createDataFrame(data, ["name", "expected"]).withColumn(
        "actual", remove_non_word_characters(F.col("name"))
    )
    assert_column_equality(df, "actual", "expected")
```

**Email domain extraction:**

```python
def test_extracts_domain(self, spark):
    data = [("alice@example.com", "example.com"), ("bob@work.org", "work.org")]
    df = spark.createDataFrame(data, ["email", "expected"]).withColumn("actual", extract_email_domain(F.col("email")))
    assert_column_equality(df, "actual", "expected")
```

**Title case:**

```python
def test_all_uppercase(self, spark):
    data = [("HELLO WORLD", "Hello World")]
    df = spark.createDataFrame(data, ["text", "expected"]).withColumn("actual", title_case(F.col("text")))
    assert_column_equality(df, "actual", "expected")
```

## assert_approx_column_equality

For floating-point results where exact equality is impractical.

```python
from chispa import assert_approx_column_equality
```

### Pattern

Same as `assert_column_equality`, but with a `precision` parameter:

```python
def test_divide_by_three(self, spark):
    data = [(1, 0.33), (2, 0.66), (3, 1.0)]
    df = spark.createDataFrame(data, ["num", "expected"]).withColumn("result", divide_by_three(F.col("num")))
    assert_approx_column_equality(df, "result", "expected", 0.01)  # (1)!
```

1. Tolerance of `0.01` — values within ±0.01 are considered equal.

### Null Handling with Explicit Schema

When all rows in a column are null, PySpark cannot infer the type. Provide an
explicit schema:

```python
def test_null_handling(self, spark):
    from pyspark.sql.types import DoubleType, StructField, StructType

    schema = StructType(
        [
            StructField("num", DoubleType()),
            StructField("expected", DoubleType()),
        ]
    )
    df = spark.createDataFrame([(None, None)], schema).withColumn("result", divide_by_three(F.col("num")))
    assert_approx_column_equality(df, "result", "expected", 0.01)
```

### Examples from This Project

**Null-safe division:**

```python
def test_zero_denominator_returns_null(self, spark):
    schema = StructType(
        [
            StructField("num", DoubleType()),
            StructField("denom", DoubleType()),
            StructField("expected", DoubleType()),
        ]
    )
    data = [(10.0, 0.0, None)]
    df = spark.createDataFrame(data, schema).withColumn("result", null_safe_divide(F.col("num"), F.col("denom")))
    assert_approx_column_equality(df, "result", "expected", 0.01)
```

**Clamping values:**

```python
def test_clamps_above_upper(self, spark):
    data = [(150.0, 100.0), (999.0, 100.0)]
    df = spark.createDataFrame(data, ["val", "expected"]).withColumn("result", clamp(F.col("val"), 0.0, 100.0))
    assert_approx_column_equality(df, "result", "expected", 0.01)
```

## Run Tests

```bash
uv run pytest tests/columns/ tests/functions/ -v
```
