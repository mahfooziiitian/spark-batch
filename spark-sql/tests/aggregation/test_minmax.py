"""Tests for MIN / MAX aggregation and GREATEST / LEAST row-level functions in Spark SQL."""

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


@pytest.mark.unit
def test_min_basic(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(30.0,), (10.0,), (20.0,)],
        ["amount"],
    ).createOrReplaceTempView("minmax_min")

    row = spark.sql("SELECT MIN(amount) AS min_val FROM minmax_min").collect()[0]
    assert row["min_val"] == 10.0


@pytest.mark.unit
def test_max_basic(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(30.0,), (10.0,), (20.0,)],
        ["amount"],
    ).createOrReplaceTempView("minmax_max")

    row = spark.sql("SELECT MAX(amount) AS max_val FROM minmax_max").collect()[0]
    assert row["max_val"] == 30.0


@pytest.mark.unit
def test_min_ignores_nulls(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(None,), (5.0,), (None,), (15.0,)],
        ["amount"],
    ).createOrReplaceTempView("minmax_nulls")

    row = spark.sql("SELECT MIN(amount) AS min_val FROM minmax_nulls").collect()[0]
    assert row["min_val"] == 5.0


@pytest.mark.unit
def test_min_with_strings(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("banana",), ("apple",), ("cherry",), ("apricot",)],
        ["fruit"],
    ).createOrReplaceTempView("minmax_str")

    row = spark.sql("SELECT MIN(fruit) AS min_fruit FROM minmax_str").collect()[0]
    # Lexicographic minimum: "apple" < "apricot" < "banana" < "cherry"
    assert row["min_fruit"] == "apple"


@pytest.mark.unit
def test_max_with_filter(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(10.0, "A"), (50.0, "B"), (30.0, "A"), (20.0, "B")],
        ["amount", "category"],
    ).createOrReplaceTempView("minmax_filter")

    row = spark.sql(
        "SELECT MAX(amount) FILTER (WHERE category = 'A') AS max_a FROM minmax_filter"
    ).collect()[0]
    assert row["max_a"] == 30.0


@pytest.mark.unit
def test_greatest_least_row_level(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(3, 1, 2), (7, 9, 5), (4, 4, 4)],
        ["a", "b", "c"],
    ).createOrReplaceTempView("minmax_greatest_least")

    actual = spark.sql(
        """
        SELECT
            a, b, c,
            GREATEST(a, b, c) AS greatest_val,
            LEAST(a, b, c) AS least_val
        FROM minmax_greatest_least
        """
    )
    # createDataFrame infers integer literals as LongType — match that here
    expected = spark.createDataFrame(
        [(3, 1, 2, 3, 1), (7, 9, 5, 9, 5), (4, 4, 4, 4, 4)],
        StructType([
            StructField("a", LongType()),
            StructField("b", LongType()),
            StructField("c", LongType()),
            StructField("greatest_val", LongType()),
            StructField("least_val", LongType()),
        ]),
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)
