"""Tests for ANY_VALUE aggregation in Spark SQL."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType


@pytest.mark.unit
def test_any_value_basic(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(10,), (20,), (30,)],
        ["amount"],
    ).createOrReplaceTempView("av_basic")

    row = spark.sql("SELECT ANY_VALUE(amount) AS val FROM av_basic").collect()[0]
    assert row["val"] in {10, 20, 30}


@pytest.mark.unit
def test_any_value_per_group(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("US", "Alice"), ("US", "Bob"), ("CA", "Carol"), ("CA", "Dave")],
        ["region", "name"],
    ).createOrReplaceTempView("av_group")

    rows = spark.sql("SELECT region, ANY_VALUE(name) AS sample_name FROM av_group GROUP BY region").collect()

    lookup = {r["region"]: r["sample_name"] for r in rows}
    assert lookup["US"] in {"Alice", "Bob"}
    assert lookup["CA"] in {"Carol", "Dave"}


@pytest.mark.unit
def test_any_value_with_nulls_default(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(None,), (5,), (20,)],
        StructType([StructField("amount", IntegerType())]),
    ).createOrReplaceTempView("av_nulls")

    row = spark.sql("SELECT ANY_VALUE(amount) AS val FROM av_nulls").collect()[0]
    assert row["val"] in {None, 5, 20}


@pytest.mark.unit
def test_any_value_ignore_nulls(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(None,), (5,), (20,)],
        StructType([StructField("amount", IntegerType())]),
    ).createOrReplaceTempView("av_ignore_nulls")

    row = spark.sql("SELECT ANY_VALUE(amount, TRUE) AS val FROM av_ignore_nulls").collect()[0]
    assert row["val"] in {5, 20}


@pytest.mark.unit
def test_any_value_all_nulls(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(None,), (None,)],
        StructType([StructField("amount", IntegerType())]),
    ).createOrReplaceTempView("av_all_nulls")

    row = spark.sql("SELECT ANY_VALUE(amount, TRUE) AS val FROM av_all_nulls").collect()[0]
    assert row["val"] is None


@pytest.mark.unit
def test_any_value_single_value_group(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("X", 42)],
        ["grp", "val"],
    ).createOrReplaceTempView("av_single")

    row = spark.sql("SELECT grp, ANY_VALUE(val) AS sample_val FROM av_single GROUP BY grp").collect()[0]
    assert row["sample_val"] == 42


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
