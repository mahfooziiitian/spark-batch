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


@pytest.mark.unit
def test_inner_join_basic(spark: SparkSession) -> None:
    spark.createDataFrame(ORDERS, ORDERS_COLS).createOrReplaceTempView("orders")
    spark.createDataFrame(CUSTOMERS, CUSTOMERS_COLS).createOrReplaceTempView(
        "customers"
    )

    actual = spark.sql("""
        SELECT o.order_id, o.customer_id, o.amount, o.status, c.name, c.country
        FROM orders AS o
        INNER JOIN customers AS c
            ON o.customer_id = c.customer_id
    """)

    expected = spark.createDataFrame(
        [
            (1, 101, 500.0, "shipped", "Alice", "US"),
            (2, 102, 300.0, "pending", "Bob", "CA"),
            (3, 101, 200.0, "shipped", "Alice", "US"),
        ],
        ["order_id", "customer_id", "amount", "status", "name", "country"],
    )

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_inner_join_row_count(spark: SparkSession) -> None:
    spark.createDataFrame(ORDERS, ORDERS_COLS).createOrReplaceTempView("orders")
    spark.createDataFrame(CUSTOMERS, CUSTOMERS_COLS).createOrReplaceTempView(
        "customers"
    )

    actual = spark.sql("""
        SELECT o.order_id
        FROM orders AS o
        INNER JOIN customers AS c
            ON o.customer_id = c.customer_id
    """)

    # Orders 1, 2, 3 match customers; order 4 (customer_id=999) does not
    assert actual.count() == 3


@pytest.mark.unit
def test_inner_join_with_filter(spark: SparkSession) -> None:
    spark.createDataFrame(ORDERS, ORDERS_COLS).createOrReplaceTempView("orders")
    spark.createDataFrame(CUSTOMERS, CUSTOMERS_COLS).createOrReplaceTempView(
        "customers"
    )

    actual = spark.sql("""
        SELECT o.order_id, o.amount, c.name
        FROM orders AS o
        INNER JOIN customers AS c
            ON o.customer_id = c.customer_id
        WHERE o.status = 'shipped'
    """)

    expected = spark.createDataFrame(
        [(1, 500.0, "Alice"), (3, 200.0, "Alice")],
        ["order_id", "amount", "name"],
    )

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_inner_join_aggregation(spark: SparkSession) -> None:
    spark.createDataFrame(ORDERS, ORDERS_COLS).createOrReplaceTempView("orders")
    spark.createDataFrame(CUSTOMERS, CUSTOMERS_COLS).createOrReplaceTempView(
        "customers"
    )

    actual = spark.sql("""
        SELECT c.name, SUM(o.amount) AS total_spend
        FROM orders AS o
        INNER JOIN customers AS c
            ON o.customer_id = c.customer_id
        GROUP BY c.name
    """)

    expected = spark.createDataFrame(
        [("Alice", 700.0), ("Bob", 300.0)],
        ["name", "total_spend"],
    )

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_self_join(spark: SparkSession) -> None:
    """Customers with more than one order — found by joining orders to itself."""
    spark.createDataFrame(ORDERS, ORDERS_COLS).createOrReplaceTempView("orders")

    actual = spark.sql("""
        SELECT DISTINCT a.customer_id, a.order_id AS order1, b.order_id AS order2
        FROM orders AS a
        INNER JOIN orders AS b
            ON a.customer_id = b.customer_id
            AND a.order_id < b.order_id
    """)

    # Only customer 101 (Alice) has two orders: 1 and 3
    expected = spark.createDataFrame(
        [(101, 1, 3)],
        ["customer_id", "order1", "order2"],
    )

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_three_table_join(spark: SparkSession) -> None:
    spark.createDataFrame(ORDERS, ORDERS_COLS).createOrReplaceTempView("orders")
    spark.createDataFrame(CUSTOMERS, CUSTOMERS_COLS).createOrReplaceTempView(
        "customers"
    )

    order_items = [(1, "Widget", 2), (1, "Gadget", 1), (3, "Widget", 3)]
    spark.createDataFrame(
        order_items, ["order_id", "product", "qty"]
    ).createOrReplaceTempView("order_items")

    actual = spark.sql("""
        SELECT o.order_id, c.name, oi.product, oi.qty
        FROM orders AS o
        INNER JOIN customers AS c
            ON o.customer_id = c.customer_id
        INNER JOIN order_items AS oi
            ON o.order_id = oi.order_id
    """)

    expected = spark.createDataFrame(
        [
            (1, "Alice", "Widget", 2),
            (1, "Alice", "Gadget", 1),
            (3, "Alice", "Widget", 3),
        ],
        ["order_id", "name", "product", "qty"],
    )

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)
