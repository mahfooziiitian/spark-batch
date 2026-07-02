"""Tests for GROUP BY scenarios in Spark SQL."""

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


@pytest.mark.unit
def test_group_by_single_column(spark: SparkSession) -> None:
    spark.createDataFrame(
        [
            ("US", 100.0),
            ("US", 200.0),
            ("CA", 50.0),
            ("CA", 75.0),
        ],
        ["region", "amount"],
    ).createOrReplaceTempView("orders_grp_single")

    actual = spark.sql("SELECT region, SUM(amount) AS total FROM orders_grp_single GROUP BY region")
    expected = spark.createDataFrame(
        [("CA", 125.0), ("US", 300.0)],
        StructType(
            [
                StructField("region", StringType()),
                StructField("total", DoubleType()),
            ]
        ),
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_group_by_multiple_columns(spark: SparkSession) -> None:
    spark.createDataFrame(
        [
            ("US", "shipped", 100.0),
            ("US", "pending", 50.0),
            ("US", "shipped", 200.0),
            ("CA", "shipped", 75.0),
        ],
        ["region", "status", "amount"],
    ).createOrReplaceTempView("orders_grp_multi")

    actual = spark.sql(
        """
        SELECT region, status, SUM(amount) AS total
        FROM orders_grp_multi
        GROUP BY region, status
        """
    )
    expected = spark.createDataFrame(
        [
            ("US", "shipped", 300.0),
            ("US", "pending", 50.0),
            ("CA", "shipped", 75.0),
        ],
        StructType(
            [
                StructField("region", StringType()),
                StructField("status", StringType()),
                StructField("total", DoubleType()),
            ]
        ),
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_group_by_with_having(spark: SparkSession) -> None:
    spark.createDataFrame(
        [
            ("US", 100.0),
            ("US", 200.0),
            ("CA", 30.0),
            ("CA", 40.0),
        ],
        ["region", "amount"],
    ).createOrReplaceTempView("orders_grp_having")

    actual = spark.sql(
        """
        SELECT region, SUM(amount) AS total
        FROM orders_grp_having
        GROUP BY region
        HAVING SUM(amount) > 100
        """
    )
    expected = spark.createDataFrame(
        [("US", 300.0)],
        StructType(
            [
                StructField("region", StringType()),
                StructField("total", DoubleType()),
            ]
        ),
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_group_by_with_filter_clause(spark: SparkSession) -> None:
    spark.createDataFrame(
        [
            ("US", "shipped"),
            ("US", "pending"),
            ("US", "shipped"),
            ("CA", "pending"),
        ],
        ["region", "status"],
    ).createOrReplaceTempView("orders_grp_filter")

    actual = spark.sql(
        """
        SELECT
            region,
            COUNT(*) FILTER (WHERE status = 'shipped') AS shipped_count
        FROM orders_grp_filter
        GROUP BY region
        """
    )
    expected = spark.createDataFrame(
        [("US", 2), ("CA", 0)],
        StructType(
            [
                StructField("region", StringType()),
                StructField("shipped_count", LongType()),
            ]
        ),
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_group_by_nulls_grouped_together(spark: SparkSession) -> None:
    spark.createDataFrame(
        [
            ("US", 10.0),
            (None, 20.0),
            (None, 30.0),
            ("CA", 5.0),
        ],
        ["region", "amount"],
    ).createOrReplaceTempView("orders_grp_nulls")

    actual = spark.sql("SELECT region, SUM(amount) AS total FROM orders_grp_nulls GROUP BY region")
    # NULLs should collapse into one group with combined total
    rows = {r["region"]: r["total"] for r in actual.collect()}
    assert rows[None] == 50.0
    assert rows["US"] == 10.0
    assert rows["CA"] == 5.0
    assert len(rows) == 3


@pytest.mark.unit
def test_group_by_expression(spark: SparkSession) -> None:
    spark.createDataFrame(
        [
            ("2023-01-15", 100.0),
            ("2023-06-20", 200.0),
            ("2024-03-10", 50.0),
            ("2024-11-01", 75.0),
        ],
        ["order_date", "amount"],
    ).createOrReplaceTempView("orders_grp_expr")

    actual = spark.sql(
        """
        SELECT YEAR(order_date) AS yr, SUM(amount) AS total
        FROM orders_grp_expr
        GROUP BY YEAR(order_date)
        """
    )
    expected = spark.createDataFrame(
        [(2023, 300.0), (2024, 125.0)],
        StructType(
            [
                StructField("yr", IntegerType()),
                StructField("total", DoubleType()),
            ]
        ),
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)
