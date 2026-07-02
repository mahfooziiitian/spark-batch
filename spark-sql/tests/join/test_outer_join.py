import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

ORDERS = [
    (1, 101, 500.0, "shipped"),
    (2, 102, 300.0, "pending"),
    (3, 101, 200.0, "shipped"),
    (4, 999, 100.0, "cancelled"),
]
ORDERS_COLS = ["order_id", "customer_id", "amount", "status"]

CUSTOMERS = [(101, "Alice", "US"), (102, "Bob", "CA"), (103, "Charlie", "US")]
CUSTOMERS_COLS = ["customer_id", "name", "country"]

_ORDER_CUSTOMER_NAME_SCHEMA = StructType(
    [
        StructField("order_id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField("name", StringType(), True),
    ]
)


@pytest.mark.unit
def test_left_join_preserves_all_left_rows(spark: SparkSession) -> None:
    spark.createDataFrame(ORDERS, ORDERS_COLS).createOrReplaceTempView("orders")
    spark.createDataFrame(CUSTOMERS, CUSTOMERS_COLS).createOrReplaceTempView("customers")

    actual = spark.sql("""
        SELECT o.order_id, o.customer_id, c.name
        FROM orders AS o
        LEFT JOIN customers AS c
            ON o.customer_id = c.customer_id
    """)

    # All 4 orders must appear — including unmatched order 4
    assert actual.count() == 4


@pytest.mark.unit
def test_left_join_null_for_unmatched(spark: SparkSession) -> None:
    spark.createDataFrame(ORDERS, ORDERS_COLS).createOrReplaceTempView("orders")
    spark.createDataFrame(CUSTOMERS, CUSTOMERS_COLS).createOrReplaceTempView("customers")

    actual = spark.sql("""
        SELECT o.order_id, o.customer_id, c.name
        FROM orders AS o
        LEFT JOIN customers AS c
            ON o.customer_id = c.customer_id
        WHERE o.order_id = 4
    """)

    expected = spark.createDataFrame([(4, 999, None)], _ORDER_CUSTOMER_NAME_SCHEMA)

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_right_join_preserves_all_right_rows(spark: SparkSession) -> None:
    spark.createDataFrame(ORDERS, ORDERS_COLS).createOrReplaceTempView("orders")
    spark.createDataFrame(CUSTOMERS, CUSTOMERS_COLS).createOrReplaceTempView("customers")

    actual = spark.sql("""
        SELECT o.order_id, c.customer_id, c.name
        FROM orders AS o
        RIGHT JOIN customers AS c
            ON o.customer_id = c.customer_id
    """)

    # All 3 customers appear; customer 101 (Alice) has 2 orders → 4 rows total
    # Charlie (103) has no orders → order_id is NULL
    expected = spark.createDataFrame(
        [(1, 101, "Alice"), (3, 101, "Alice"), (2, 102, "Bob"), (None, 103, "Charlie")],
        _ORDER_CUSTOMER_NAME_SCHEMA,
    )

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_full_outer_join_row_count(spark: SparkSession) -> None:
    spark.createDataFrame(ORDERS, ORDERS_COLS).createOrReplaceTempView("orders")
    spark.createDataFrame(CUSTOMERS, CUSTOMERS_COLS).createOrReplaceTempView("customers")

    actual = spark.sql("""
        SELECT
            o.order_id,
            COALESCE(o.customer_id, c.customer_id) AS customer_id,
            c.name
        FROM orders AS o
        FULL OUTER JOIN customers AS c
            ON o.customer_id = c.customer_id
    """)

    # Matched rows: orders 1, 2, 3 (Alice appears twice for orders 1+3) = 3 rows
    # Left-only:  order 4 (customer_id=999, no customer)            = 1 row
    # Right-only: Charlie (customer_id=103, no order)               = 1 row
    # Total: 5 rows
    assert actual.count() == 5


@pytest.mark.unit
def test_coalesce_after_outer_join(spark: SparkSession) -> None:
    spark.createDataFrame(ORDERS, ORDERS_COLS).createOrReplaceTempView("orders")
    spark.createDataFrame(CUSTOMERS, CUSTOMERS_COLS).createOrReplaceTempView("customers")

    actual = spark.sql("""
        SELECT o.order_id, COALESCE(c.name, 'Unknown') AS name
        FROM orders AS o
        LEFT JOIN customers AS c
            ON o.customer_id = c.customer_id
        WHERE o.order_id = 4
    """)

    expected = spark.createDataFrame([(4, "Unknown")], ["order_id", "name"])

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_anti_join_pattern(spark: SparkSession) -> None:
    """LEFT JOIN + WHERE right key IS NULL replicates an anti-join."""
    spark.createDataFrame(ORDERS, ORDERS_COLS).createOrReplaceTempView("orders")
    spark.createDataFrame(CUSTOMERS, CUSTOMERS_COLS).createOrReplaceTempView("customers")

    actual = spark.sql("""
        SELECT o.order_id, o.customer_id
        FROM orders AS o
        LEFT JOIN customers AS c
            ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """)

    expected = spark.createDataFrame(
        [(4, 999)],
        ["order_id", "customer_id"],
    )

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)
