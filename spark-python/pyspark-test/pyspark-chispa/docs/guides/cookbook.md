# Cookbook

Quick recipes for common chispa + PySpark testing patterns.

## Verify a Column Transformation

```python
from chispa.column_comparer import assert_column_equality
from pyspark.sql import functions as F


def test_upper_case(spark):
    data = [("alice", "ALICE"), ("bob", "BOB")]
    df = spark.createDataFrame(data, ["name", "expected"]).withColumn("actual", F.upper(F.col("name")))
    assert_column_equality(df, "actual", "expected")
```

## Verify Approximate Numeric Results

```python
from chispa import assert_approx_column_equality


def test_division(spark):
    data = [(10.0, 3.0, 3.33)]
    df = spark.createDataFrame(data, ["num", "denom", "expected"]).withColumn("actual", F.col("num") / F.col("denom"))
    assert_approx_column_equality(df, "actual", "expected", precision=0.01)
```

## Compare Two DataFrames Exactly

```python
from chispa import assert_df_equality


def test_filter(spark):
    source = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "val"])
    actual = source.filter(F.col("id") > 1)
    expected = spark.createDataFrame([(2, "b"), (3, "c")], ["id", "val"])
    assert_df_equality(actual, expected)
```

## Assert Schema Equality

```python
from chispa.schema_comparer import assert_schema_equality
from pyspark.sql.types import StructType, StructField, StringType, LongType


def test_schemas_match(spark):
    df1 = spark.createDataFrame([(1, "a")], ["id", "name"])
    expected_schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
        ]
    )
    assert_schema_equality(df1.schema, expected_schema)
```

## Test That a Transformation Raises on Invalid Input

```python
import pytest
from data_frame.equality.df_equality import sort_columns


def test_invalid_sort_raises(spark):
    df = spark.createDataFrame([("a",)], ["col"])
    with pytest.raises(ValueError, match="valid sort orders"):
        sort_columns(df, "invalid")
```

## Test with Explicit Null Schema

When testing null values, use explicit schemas to avoid type inference issues:

```python
from pyspark.sql.types import StringType, StructField, StructType


def test_null_handling(spark):
    schema = StructType(
        [
            StructField("input", StringType()),
            StructField("expected", StringType()),
        ]
    )
    df = spark.createDataFrame([(None, None)], schema).withColumn("actual", my_function(F.col("input")))
    assert_column_equality(df, "actual", "expected")
```

??? tip "Why explicit schemas for nulls?"
    Without a schema, `spark.createDataFrame([(None, None)])` can't infer
    column types and may default to `void`, causing unexpected test failures.

## Test Window Functions

```python
from data_frame.transformation.df_transformations import with_running_total


def test_running_total(spark):
    df = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["step", "val"])
    result = with_running_total(df, value_col="val", order_col="step")
    rows = result.orderBy("step").select("running_total").collect()
    assert [r["running_total"] for r in rows] == [10, 30, 60]
```

## Test Deduplication

```python
from data_frame.transformation.df_transformations import deduplicate


def test_keep_latest(spark):
    df = spark.createDataFrame(
        [(1, "old", 1), (1, "new", 2), (2, "only", 1)],
        ["id", "val", "ts"],
    )
    result = deduplicate(df, subset=["id"], order_col="ts", keep="last")
    assert result.count() == 2
    row = result.filter(F.col("id") == 1).first()
    assert row["val"] == "new"
```

## Rename Columns with a Helper Function

```python
from data_frame.helper.string_helper import snake_case
from data_frame.transformation.df_transformations import modify_column_names


def test_rename_columns(spark):
    df = spark.createDataFrame([("a",)], ["My Column.Name"])
    result = modify_column_names(df, snake_case)
    assert result.columns == ["my_column_name"]
```
