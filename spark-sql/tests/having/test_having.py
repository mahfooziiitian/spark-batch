import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession


@pytest.mark.unit
def test_having_sum_threshold(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("US", 100.0), ("US", 200.0), ("UK", 50.0), ("UK", 30.0), ("CA", 500.0)],
        ["region", "amount"],
    ).createOrReplaceTempView("sales_having_sum")

    actual = spark.sql("""
        SELECT region, SUM(amount) AS total
        FROM sales_having_sum
        GROUP BY region
        HAVING SUM(amount) > 150
    """)
    # US=300, UK=80, CA=500 → US and CA exceed 150
    expected = spark.createDataFrame(
        [("US", 300.0), ("CA", 500.0)],
        ["region", "total"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_having_count(spark: SparkSession) -> None:
    spark.createDataFrame(
        [
            ("Alice", "US"),
            ("Bob", "US"),
            ("Carol", "UK"),
            ("Dave", "US"),
            ("Eve", "UK"),
        ],
        ["name", "region"],
    ).createOrReplaceTempView("users_having_count")

    actual = spark.sql("""
        SELECT region, COUNT(*) AS cnt
        FROM users_having_count
        GROUP BY region
        HAVING COUNT(*) >= 2
    """)
    # US=3, UK=2 → both qualify
    expected = spark.createDataFrame(
        [("US", 3), ("UK", 2)],
        ["region", "cnt"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_having_vs_where(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("US", 100.0), ("US", 200.0), ("UK", 50.0), ("UK", 400.0), ("CA", 500.0)],
        ["region", "amount"],
    ).createOrReplaceTempView("sales_having_vs_where")

    # WHERE filters individual rows before aggregation
    where_result = spark.sql("""
        SELECT region, SUM(amount) AS total
        FROM sales_having_vs_where
        WHERE amount > 100
        GROUP BY region
    """)
    # HAVING filters groups after aggregation
    having_result = spark.sql("""
        SELECT region, SUM(amount) AS total
        FROM sales_having_vs_where
        GROUP BY region
        HAVING SUM(amount) > 100
    """)

    where_map = {r["region"]: r["total"] for r in where_result.collect()}
    having_map = {r["region"]: r["total"] for r in having_result.collect()}

    # WHERE(amount>100) yields {US:200, UK:400, CA:500}
    # HAVING(sum>100)  yields {US:300, UK:450, CA:500}
    assert where_map != having_map


@pytest.mark.unit
def test_having_multiple_conditions(spark: SparkSession) -> None:
    spark.createDataFrame(
        [
            ("US", 100.0),
            ("US", 200.0),
            ("US", 300.0),
            ("UK", 50.0),
            ("UK", 60.0),
            ("CA", 500.0),
        ],
        ["region", "amount"],
    ).createOrReplaceTempView("sales_having_multi")

    actual = spark.sql("""
        SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
        FROM sales_having_multi
        GROUP BY region
        HAVING SUM(amount) > 200 AND COUNT(*) >= 2
    """)
    # US: total=600, cnt=3 → ✓
    # UK: total=110, cnt=2 → sum ≤ 200 ✗
    # CA: total=500, cnt=1 → cnt < 2 ✗
    expected = spark.createDataFrame(
        [("US", 600.0, 3)],
        ["region", "total", "cnt"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)
