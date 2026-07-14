# DataFrame Assertions

PySpark provides built-in assertion utilities for comparing DataFrames in tests.

## assertDataFrameEqual

The primary assertion function from `pyspark.testing`:

```python
from pyspark.testing.utils import assertDataFrameEqual

assertDataFrameEqual(actual_df, expected_df)
```

This checks both schema and data equality.

## Example: Transformation Test

```python
from pyspark.testing import assertDataFrameEqual
from transformation.df_transformation import remove_extra_spaces

def test_single_space(spark):
    sample_data = [
        {"name": "John    D.", "age": 30},
        {"name": "Alice   G.", "age": 25},
    ]
    original_df = spark.createDataFrame(sample_data)
    transformed_df = remove_extra_spaces(original_df, "name")

    expected_data = [
        {"name": "John D.", "age": 30},
        {"name": "Alice G.", "age": 25},
    ]
    expected_df = spark.createDataFrame(expected_data)

    assertDataFrameEqual(transformed_df, expected_df)
```

## Example: Row Count and Collect

For simpler checks, use `count()` and `collect()`:

```python
def test_classify_transactions(self, spark):
    output = classify_debit_credit_transactions(transactions_df, accounts_df)

    assert output.count() == 2
    assert [row.business_line for row in output.collect()] == ["credit", "debit"]
```

## Example: SQL Query Result

```python
def test_spark_sql(self, spark):
    df = spark.createDataFrame([("a", 1), ("b", 2)], ["letter", "number"])
    df.createOrReplaceTempView("my_table")

    result = spark.sql("SELECT * FROM my_table")
    assert result.collect()[0][1] == 1
```

## When to Use What

| Approach | Best for |
| --- | --- |
| `assertDataFrameEqual` | Full structural equality with detailed diffs |
| `df.count()` | Quick row count checks |
| `df.collect()` | Specific value assertions on small results |
| `df.first()["col"]` | Single-row value checks |
