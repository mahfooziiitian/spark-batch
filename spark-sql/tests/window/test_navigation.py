import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SALES_DATA = [
    ("North", "Alice", "2024-01-01", 100),
    ("North", "Alice", "2024-01-05", 200),
    ("North", "Alice", "2024-01-10", 300),
    ("North", "Bob", "2024-01-02", 150),
    ("North", "Bob", "2024-01-06", 250),
    ("South", "Alice", "2024-01-03", 400),
    ("South", "Alice", "2024-01-07", 500),
    ("South", "Bob", "2024-01-04", 180),
    ("South", "Bob", "2024-01-08", 220),
]
SALES_COLS = ["region", "rep", "sale_date", "amount"]


@pytest.fixture(scope="module")
def sales_view(spark: SparkSession):
    spark.createDataFrame(SALES_DATA, SALES_COLS).createOrReplaceTempView("nav_sales")
    return "nav_sales"


@pytest.mark.unit
def test_lag_returns_previous_value(spark: SparkSession, sales_view: str):
    result = spark.sql(
        """
        SELECT region, rep, sale_date, amount,
               LAG(amount, 1) OVER (PARTITION BY region, rep ORDER BY sale_date) AS prev_amount
        FROM nav_sales
        """
    )
    alice_north = (
        result.filter((F.col("region") == "North") & (F.col("rep") == "Alice"))
        .orderBy("sale_date")
        .collect()
    )
    assert alice_north[0].prev_amount is None, "First row LAG should be null"
    assert alice_north[1].prev_amount == 100
    assert alice_north[2].prev_amount == 200


@pytest.mark.unit
def test_lag_with_default(spark: SparkSession, sales_view: str):
    result = spark.sql(
        """
        SELECT region, rep, sale_date, amount,
               LAG(amount, 1, 0) OVER (PARTITION BY region, rep ORDER BY sale_date) AS prev_amount
        FROM nav_sales
        """
    )
    alice_north = (
        result.filter((F.col("region") == "North") & (F.col("rep") == "Alice"))
        .orderBy("sale_date")
        .collect()
    )
    assert alice_north[0].prev_amount == 0, "First row LAG default should be 0"
    assert alice_north[1].prev_amount == 100


@pytest.mark.unit
def test_lead_returns_next_value(spark: SparkSession, sales_view: str):
    result = spark.sql(
        """
        SELECT region, rep, sale_date, amount,
               LEAD(amount, 1) OVER (PARTITION BY region, rep ORDER BY sale_date) AS next_amount
        FROM nav_sales
        """
    )
    alice_north = (
        result.filter((F.col("region") == "North") & (F.col("rep") == "Alice"))
        .orderBy("sale_date")
        .collect()
    )
    assert alice_north[0].next_amount == 200
    assert alice_north[1].next_amount == 300
    assert alice_north[2].next_amount is None, "Last row LEAD should be null"


@pytest.mark.unit
def test_delta_calculation(spark: SparkSession, sales_view: str):
    result = spark.sql(
        """
        SELECT region, rep, sale_date, amount,
               amount - LAG(amount, 1) OVER (PARTITION BY region, rep ORDER BY sale_date) AS delta
        FROM nav_sales
        """
    )
    alice_north = (
        result.filter((F.col("region") == "North") & (F.col("rep") == "Alice"))
        .orderBy("sale_date")
        .collect()
    )
    assert alice_north[0].delta is None
    assert alice_north[1].delta == 100   # 200 - 100
    assert alice_north[2].delta == 100   # 300 - 200


@pytest.mark.unit
def test_first_value_per_partition(spark: SparkSession, sales_view: str):
    result = spark.sql(
        """
        SELECT region, rep, sale_date, amount,
               FIRST_VALUE(amount) OVER (
                   PARTITION BY region, rep ORDER BY sale_date
                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
               ) AS first_sale
        FROM nav_sales
        """
    )
    # For North/Alice ordered by date: first is 2024-01-01 → amount 100
    alice_north = result.filter(
        (F.col("region") == "North") & (F.col("rep") == "Alice")
    ).collect()
    assert all(row.first_sale == 100 for row in alice_north), (
        "FIRST_VALUE should be 100 for all North/Alice rows"
    )


@pytest.mark.unit
def test_last_value_with_full_frame(spark: SparkSession, sales_view: str):
    result = spark.sql(
        """
        SELECT region, rep, sale_date, amount,
               LAST_VALUE(amount) OVER (
                   PARTITION BY region, rep ORDER BY sale_date
                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
               ) AS last_sale
        FROM nav_sales
        """
    )
    # For North/Alice ordered by date: last is 2024-01-10 → amount 300
    alice_north = result.filter(
        (F.col("region") == "North") & (F.col("rep") == "Alice")
    ).collect()
    assert all(row.last_sale == 300 for row in alice_north), (
        "LAST_VALUE (full frame) should be 300 for all North/Alice rows"
    )


@pytest.mark.unit
def test_nth_value(spark: SparkSession, sales_view: str):
    result = spark.sql(
        """
        SELECT region, rep, sale_date, amount,
               NTH_VALUE(amount, 2) OVER (
                   PARTITION BY region, rep ORDER BY sale_date
                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
               ) AS second_sale
        FROM nav_sales
        """
    )
    # For North/Alice: order is 100, 200, 300 → 2nd value = 200
    alice_north = result.filter(
        (F.col("region") == "North") & (F.col("rep") == "Alice")
    ).collect()
    assert all(row.second_sale == 200 for row in alice_north), (
        "NTH_VALUE(2) should be 200 for all North/Alice rows"
    )
