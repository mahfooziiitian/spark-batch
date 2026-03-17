"""Tests for AVG aggregation in Spark SQL."""

import math

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


@pytest.mark.unit
def test_avg_basic(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(10.0,), (20.0,), (30.0,)],
        ["amount"],
    ).createOrReplaceTempView("avg_basic")

    row = spark.sql("SELECT AVG(amount) AS avg_val FROM avg_basic").collect()[0]
    assert math.isclose(row["avg_val"], 20.0, rel_tol=1e-9)


@pytest.mark.unit
def test_avg_ignores_nulls(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(10.0,), (None,), (30.0,)],
        ["amount"],
    ).createOrReplaceTempView("avg_nulls")

    row = spark.sql("SELECT AVG(amount) AS avg_val FROM avg_nulls").collect()[0]
    # AVG ignores NULLs: (10 + 30) / 2 = 20
    assert math.isclose(row["avg_val"], 20.0, rel_tol=1e-9)


@pytest.mark.unit
def test_avg_distinct(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(10.0,), (10.0,), (20.0,), (30.0,)],
        ["amount"],
    ).createOrReplaceTempView("avg_distinct")

    row = spark.sql("SELECT AVG(DISTINCT amount) AS avg_val FROM avg_distinct").collect()[0]
    # Distinct values: 10, 20, 30 → avg = 20
    assert math.isclose(row["avg_val"], 20.0, rel_tol=1e-9)


@pytest.mark.unit
def test_avg_per_group(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("US", 100.0), ("US", 200.0), ("CA", 50.0), ("CA", 150.0)],
        ["region", "amount"],
    ).createOrReplaceTempView("avg_group")

    actual = spark.sql(
        "SELECT region, AVG(amount) AS avg_amount FROM avg_group GROUP BY region"
    )
    expected = spark.createDataFrame(
        [("US", 150.0), ("CA", 100.0)],
        StructType([
            StructField("region", StringType()),
            StructField("avg_amount", DoubleType()),
        ]),
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)
