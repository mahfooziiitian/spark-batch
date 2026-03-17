import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession

ORDERS = [
    (1, 101, 500.0, "shipped"),
    (2, 102, 300.0, "pending"),
    (3, 101, 200.0, "shipped"),
    (4, 999, 100.0, "cancelled"),
]
ORDERS_COLS = ["order_id", "customer_id", "amount", "status"]

CUSTOMERS = [(101, "Alice", "US"), (102, "Bob", "CA"), (103, "Charlie", "US")]
CUSTOMERS_COLS = ["customer_id", "name", "country"]

_MATCHED_ORDERS = [
    (1, 101, 500.0, "shipped"),
    (2, 102, 300.0, "pending"),
    (3, 101, 200.0, "shipped"),
]


@pytest.mark.unit
def test_left_semi_join_returns_only_left_columns(spark: SparkSession) -> None:
    spark.createDataFrame(ORDERS, ORDERS_COLS).createOrReplaceTempView("orders")
    spark.createDataFrame(CUSTOMERS, CUSTOMERS_COLS).createOrReplaceTempView("customers")

    actual = spark.sql("""
        SELECT *
        FROM orders
        LEFT SEMI JOIN customers
            ON orders.customer_id = customers.customer_id
    """)

    # Semi join must not leak customer columns into the result
    assert set(actual.columns) == set(ORDERS_COLS)


@pytest.mark.unit
def test_left_semi_join_existence_filter(spark: SparkSession) -> None:
    spark.createDataFrame(ORDERS, ORDERS_COLS).createOrReplaceTempView("orders")
    spark.createDataFrame(CUSTOMERS, CUSTOMERS_COLS).createOrReplaceTempView("customers")

    actual = spark.sql("""
        SELECT *
        FROM orders
        LEFT SEMI JOIN customers
            ON orders.customer_id = customers.customer_id
    """)

    # Only orders whose customer_id exists in the customers table (orders 1, 2, 3)
    expected = spark.createDataFrame(_MATCHED_ORDERS, ORDERS_COLS)

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_left_anti_join_returns_unmatched(spark: SparkSession) -> None:
    spark.createDataFrame(ORDERS, ORDERS_COLS).createOrReplaceTempView("orders")
    spark.createDataFrame(CUSTOMERS, CUSTOMERS_COLS).createOrReplaceTempView("customers")

    actual = spark.sql("""
        SELECT *
        FROM orders
        LEFT ANTI JOIN customers
            ON orders.customer_id = customers.customer_id
    """)

    # Only order 4 (customer_id=999) has no matching customer
    expected = spark.createDataFrame([(4, 999, 100.0, "cancelled")], ORDERS_COLS)

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_semi_vs_exists_equivalence(spark: SparkSession) -> None:
    """LEFT SEMI JOIN and WHERE EXISTS must return identical results."""
    spark.createDataFrame(ORDERS, ORDERS_COLS).createOrReplaceTempView("orders")
    spark.createDataFrame(CUSTOMERS, CUSTOMERS_COLS).createOrReplaceTempView("customers")

    semi_result = spark.sql("""
        SELECT *
        FROM orders
        LEFT SEMI JOIN customers
            ON orders.customer_id = customers.customer_id
    """)

    exists_result = spark.sql("""
        SELECT *
        FROM orders
        WHERE EXISTS (
            SELECT 1
            FROM customers
            WHERE customers.customer_id = orders.customer_id
        )
    """)

    assert_df_equality(semi_result, exists_result, ignore_row_order=True, ignore_nullable=True)
