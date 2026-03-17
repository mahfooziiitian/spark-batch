"""Tests for GROUP BY ROLLUP scenarios in Spark SQL."""

import pytest
from pyspark.sql import SparkSession


@pytest.mark.unit
def test_rollup_hierarchical(spark: SparkSession) -> None:
    spark.createDataFrame(
        [
            ("US", "widget", 100.0),
            ("US", "gadget", 200.0),
            ("CA", "widget", 50.0),
        ],
        ["region", "product", "amount"],
    ).createOrReplaceTempView("sales_rollup_hier")

    actual = spark.sql(
        """
        SELECT region, product, SUM(amount) AS total
        FROM sales_rollup_hier
        GROUP BY ROLLUP(region, product)
        """
    )
    # ROLLUP(region, product) generates 3 levels:
    # (region, product), (region, NULL), (NULL, NULL)
    # 2 US rows + 1 CA row + 2 region subtotals + 1 grand total = 6
    assert actual.count() == 6

    # Verify all three grouping levels exist
    has_detail = actual.filter("region IS NOT NULL AND product IS NOT NULL").count() > 0
    has_region_subtotal = actual.filter("region IS NOT NULL AND product IS NULL").count() > 0
    has_grand_total = actual.filter("region IS NULL AND product IS NULL").count() > 0
    assert has_detail
    assert has_region_subtotal
    assert has_grand_total


@pytest.mark.unit
def test_rollup_grand_total(spark: SparkSession) -> None:
    spark.createDataFrame(
        [
            ("US", "widget", 100.0),
            ("CA", "gadget", 200.0),
            ("CA", "widget", 50.0),
        ],
        ["region", "product", "amount"],
    ).createOrReplaceTempView("sales_rollup_grand")

    actual = spark.sql(
        """
        SELECT region, product, SUM(amount) AS total
        FROM sales_rollup_grand
        GROUP BY ROLLUP(region, product)
        """
    )
    grand_total = actual.filter("region IS NULL AND product IS NULL").collect()
    assert len(grand_total) == 1
    assert grand_total[0]["total"] == 350.0


@pytest.mark.unit
def test_rollup_grouping_function(spark: SparkSession) -> None:
    spark.createDataFrame(
        [
            ("US", "widget", 100.0),
            ("US", "gadget", 200.0),
        ],
        ["region", "product", "amount"],
    ).createOrReplaceTempView("sales_rollup_grouping")

    actual = spark.sql(
        """
        SELECT
            region,
            product,
            SUM(amount) AS total,
            GROUPING(region) AS grp_region,
            GROUPING(product) AS grp_product
        FROM sales_rollup_grouping
        GROUP BY ROLLUP(region, product)
        """
    )
    # Grand total row: GROUPING(region) = 1, GROUPING(product) = 1
    grand = actual.filter("region IS NULL AND product IS NULL").collect()
    assert len(grand) == 1
    assert grand[0]["grp_region"] == 1
    assert grand[0]["grp_product"] == 1

    # Region-only subtotal: GROUPING(region) = 0, GROUPING(product) = 1
    region_sub = actual.filter("region = 'US' AND product IS NULL").collect()
    assert len(region_sub) == 1
    assert region_sub[0]["grp_region"] == 0
    assert region_sub[0]["grp_product"] == 1

    # Detail row: both GROUPING() = 0
    detail = actual.filter("region = 'US' AND product = 'widget'").collect()
    assert len(detail) == 1
    assert detail[0]["grp_region"] == 0
    assert detail[0]["grp_product"] == 0
