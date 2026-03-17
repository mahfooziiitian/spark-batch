"""Tests for GROUP BY CUBE scenarios in Spark SQL."""

import pytest
from pyspark.sql import SparkSession


@pytest.mark.unit
def test_cube_generates_all_combinations(spark: SparkSession) -> None:
    spark.createDataFrame(
        [
            ("US", "widget", 100.0),
            ("US", "gadget", 200.0),
            ("CA", "widget", 50.0),
            ("CA", "gadget", 75.0),
        ],
        ["region", "product", "amount"],
    ).createOrReplaceTempView("sales_cube_combo")

    actual = spark.sql(
        """
        SELECT region, product, SUM(amount) AS total
        FROM sales_cube_combo
        GROUP BY CUBE(region, product)
        """
    )
    # CUBE of 2 columns = 2^2 = 4 distinct grouping levels:
    # (region, product), (region, NULL), (NULL, product), (NULL, NULL)
    row_count = actual.count()
    # 2 regions × 2 products + 2 region subtotals + 2 product subtotals + 1 grand total = 9
    assert row_count == 9


@pytest.mark.unit
def test_cube_grand_total(spark: SparkSession) -> None:
    spark.createDataFrame(
        [
            ("US", "widget", 100.0),
            ("CA", "gadget", 200.0),
        ],
        ["region", "product", "amount"],
    ).createOrReplaceTempView("sales_cube_grand")

    actual = spark.sql(
        """
        SELECT region, product, SUM(amount) AS total
        FROM sales_cube_grand
        GROUP BY CUBE(region, product)
        """
    )
    grand_total_rows = actual.filter("region IS NULL AND product IS NULL").collect()
    assert len(grand_total_rows) == 1
    assert grand_total_rows[0]["total"] == 300.0


@pytest.mark.unit
def test_cube_grouping_function(spark: SparkSession) -> None:
    spark.createDataFrame(
        [
            ("US", "widget", 100.0),
            ("CA", "gadget", 200.0),
        ],
        ["region", "product", "amount"],
    ).createOrReplaceTempView("sales_cube_grouping")

    actual = spark.sql(
        """
        SELECT
            region,
            product,
            SUM(amount) AS total,
            GROUPING(region) AS grp_region,
            GROUPING(product) AS grp_product
        FROM sales_cube_grouping
        GROUP BY CUBE(region, product)
        """
    )
    # Grand total row: both GROUPING() = 1
    grand = actual.filter("region IS NULL AND product IS NULL").collect()
    assert len(grand) == 1
    assert grand[0]["grp_region"] == 1
    assert grand[0]["grp_product"] == 1

    # Fully-specified row: both GROUPING() = 0
    detail = actual.filter("region = 'US' AND product = 'widget'").collect()
    assert len(detail) == 1
    assert detail[0]["grp_region"] == 0
    assert detail[0]["grp_product"] == 0


@pytest.mark.unit
def test_cube_grouping_id(spark: SparkSession) -> None:
    spark.createDataFrame(
        [
            ("US", "widget", 100.0),
            ("CA", "gadget", 200.0),
        ],
        ["region", "product", "amount"],
    ).createOrReplaceTempView("sales_cube_gid")

    actual = spark.sql(
        """
        SELECT
            region,
            product,
            SUM(amount) AS total,
            GROUPING_ID(region, product) AS gid
        FROM sales_cube_gid
        GROUP BY CUBE(region, product)
        """
    )
    gid_map = {(r["region"], r["product"]): r["gid"] for r in actual.collect()}

    # (region, product) → GID 0 (binary 00)
    assert gid_map[("US", "widget")] == 0
    assert gid_map[("CA", "gadget")] == 0

    # (region, NULL) → GID 1 (binary 01 — product aggregated)
    assert gid_map[("US", None)] == 1
    assert gid_map[("CA", None)] == 1

    # (NULL, product) → GID 2 (binary 10 — region aggregated)
    assert gid_map[(None, "widget")] == 2
    assert gid_map[(None, "gadget")] == 2

    # Grand total → GID 3 (binary 11)
    assert gid_map[(None, None)] == 3
