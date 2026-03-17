"""Tests for SUM aggregation in Spark SQL."""

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


@pytest.mark.unit
def test_sum_basic(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(10.0,), (20.0,), (30.0,)],
        ["amount"],
    ).createOrReplaceTempView("sum_basic")

    row = spark.sql("SELECT SUM(amount) AS total FROM sum_basic").collect()[0]
    assert row["total"] == 60.0


@pytest.mark.unit
def test_sum_ignores_nulls(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(10.0,), (None,), (30.0,)],
        ["amount"],
    ).createOrReplaceTempView("sum_nulls")

    row = spark.sql("SELECT SUM(amount) AS total FROM sum_nulls").collect()[0]
    assert row["total"] == 40.0


@pytest.mark.unit
def test_sum_distinct(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(10.0,), (10.0,), (20.0,), (30.0,), (30.0,)],
        ["amount"],
    ).createOrReplaceTempView("sum_distinct")

    row = spark.sql("SELECT SUM(DISTINCT amount) AS total FROM sum_distinct").collect()[0]
    # Distinct values: 10, 20, 30 → 60
    assert row["total"] == 60.0


@pytest.mark.unit
def test_sum_with_filter(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(10.0, "A"), (20.0, "B"), (30.0, "A"), (40.0, "B")],
        ["amount", "category"],
    ).createOrReplaceTempView("sum_filter")

    row = spark.sql(
        "SELECT SUM(amount) FILTER (WHERE category = 'A') AS total FROM sum_filter"
    ).collect()[0]
    assert row["total"] == 40.0


@pytest.mark.unit
def test_sum_with_case(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("shipped", 100.0), ("pending", 50.0), ("shipped", 200.0), ("cancelled", 75.0)],
        ["status", "amount"],
    ).createOrReplaceTempView("sum_case")

    row = spark.sql(
        """
        SELECT SUM(CASE WHEN status = 'shipped' THEN amount ELSE 0 END) AS shipped_total
        FROM sum_case
        """
    ).collect()[0]
    assert row["shipped_total"] == 300.0
