"""Tests for GROUPING SETS scenarios in Spark SQL."""

import pytest
from pyspark.sql import SparkSession


@pytest.mark.unit
def test_grouping_sets_custom(spark: SparkSession) -> None:
    spark.createDataFrame(
        [
            ("US", "widget", 100.0),
            ("US", "gadget", 200.0),
            ("CA", "widget", 50.0),
            ("CA", "gadget", 75.0),
        ],
        ["region", "product", "amount"],
    ).createOrReplaceTempView("sales_gsets_custom")

    # Request only (region) and (product) groupings — not (region, product) or ()
    actual = spark.sql(
        """
        SELECT region, product, SUM(amount) AS total
        FROM sales_gsets_custom
        GROUP BY GROUPING SETS ((region), (product))
        """
    )
    rows = actual.collect()
    # 2 region groups + 2 product groups = 4 rows
    assert len(rows) == 4

    # Every row must have exactly one of region/product NULL (the other grouping)
    for row in rows:
        assert (row["region"] is None) != (row["product"] is None), (
            f"Expected exactly one NULL per row, got region={row['region']}, product={row['product']}"
        )


@pytest.mark.unit
def test_grouping_sets_equivalent_to_union(spark: SparkSession) -> None:
    spark.createDataFrame(
        [
            ("US", "widget", 100.0),
            ("US", "gadget", 200.0),
            ("CA", "widget", 50.0),
        ],
        ["region", "product", "amount"],
    ).createOrReplaceTempView("sales_gsets_union")

    grouping_sets_df = spark.sql(
        """
        SELECT region, product, SUM(amount) AS total
        FROM sales_gsets_union
        GROUP BY GROUPING SETS ((region), (product))
        """
    )

    union_df = spark.sql(
        """
        SELECT region, CAST(NULL AS STRING) AS product, SUM(amount) AS total
        FROM sales_gsets_union
        GROUP BY region
        UNION ALL
        SELECT CAST(NULL AS STRING) AS region, product, SUM(amount) AS total
        FROM sales_gsets_union
        GROUP BY product
        """
    )

    assert_from_chispa(grouping_sets_df, union_df)


def assert_from_chispa(actual, expected):
    from chispa.dataframe_comparer import assert_df_equality

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)
