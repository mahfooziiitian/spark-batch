"""Tests for COUNT aggregation variants in Spark SQL."""

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType


@pytest.mark.unit
def test_count_star_includes_nulls(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, None), (2, "a"), (3, None), (4, "b")],
        ["id", "val"],
    ).createOrReplaceTempView("cnt_star")

    row = spark.sql("SELECT COUNT(*) AS cnt FROM cnt_star").collect()[0]
    assert row["cnt"] == 4


@pytest.mark.unit
def test_count_col_excludes_nulls(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, None), (2, "a"), (3, None), (4, "b")],
        ["id", "val"],
    ).createOrReplaceTempView("cnt_col")

    row = spark.sql("SELECT COUNT(val) AS cnt FROM cnt_col").collect()[0]
    assert row["cnt"] == 2


@pytest.mark.unit
def test_count_distinct(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("a",), ("b",), ("a",), ("c",), ("b",)],
        ["val"],
    ).createOrReplaceTempView("cnt_distinct")

    row = spark.sql("SELECT COUNT(DISTINCT val) AS cnt FROM cnt_distinct").collect()[0]
    assert row["cnt"] == 3


@pytest.mark.unit
def test_count_per_group(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("US", 1), ("US", 2), ("CA", 3)],
        ["region", "id"],
    ).createOrReplaceTempView("cnt_group")

    actual = spark.sql("SELECT region, COUNT(*) AS cnt FROM cnt_group GROUP BY region")
    expected = spark.createDataFrame(
        [("US", 2), ("CA", 1)],
        StructType(
            [
                StructField("region", StringType()),
                StructField("cnt", LongType()),
            ]
        ),
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_approx_count_distinct(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(i,) for i in range(1000)],
        ["val"],
    ).createOrReplaceTempView("cnt_approx")

    row = spark.sql("SELECT APPROX_COUNT_DISTINCT(val) AS approx FROM cnt_approx").collect()[0]
    # Result should be within 10% of the exact count (1000)
    assert 900 <= row["approx"] <= 1100
