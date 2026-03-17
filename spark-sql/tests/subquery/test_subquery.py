import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession


@pytest.mark.unit
def test_scalar_subquery_in_select(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, "Alice", 150.0), (2, "Bob", 80.0), (3, "Carol", 200.0)],
        ["id", "name", "amount"],
    ).createOrReplaceTempView("orders_scalar_sel")

    actual = spark.sql("""
        SELECT name, amount, (SELECT MAX(amount) FROM orders_scalar_sel) AS max_all
        FROM orders_scalar_sel
    """)
    expected = spark.createDataFrame(
        [("Alice", 150.0, 200.0), ("Bob", 80.0, 200.0), ("Carol", 200.0, 200.0)],
        ["name", "amount", "max_all"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_scalar_subquery_in_where(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, 150.0), (2, 80.0), (3, 200.0)],
        ["id", "amount"],
    ).createOrReplaceTempView("orders_scalar_where")

    actual = spark.sql("""
        SELECT id, amount
        FROM orders_scalar_where
        WHERE amount > (SELECT AVG(amount) FROM orders_scalar_where)
    """)
    # AVG([150, 80, 200]) ≈ 143.33 → 150 and 200 qualify
    expected = spark.createDataFrame(
        [(1, 150.0), (3, 200.0)],
        ["id", "amount"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_in_subquery(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, "Alice", "US"), (2, "Bob", "UK"), (3, "Carol", "US")],
        ["id", "name", "country"],
    ).createOrReplaceTempView("customers_in_subq")

    spark.createDataFrame(
        [(101, 1, 500.0), (102, 2, 300.0), (103, 3, 200.0)],
        ["order_id", "customer_id", "amount"],
    ).createOrReplaceTempView("orders_in_subq")

    actual = spark.sql("""
        SELECT order_id, customer_id, amount
        FROM orders_in_subq
        WHERE customer_id IN (
            SELECT id FROM customers_in_subq WHERE country = 'US'
        )
    """)
    # US customers: ids 1 and 3 → orders 101 and 103
    expected = spark.createDataFrame(
        [(101, 1, 500.0), (103, 3, 200.0)],
        ["order_id", "customer_id", "amount"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_not_in_subquery_without_nulls(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, "Alice", "US"), (2, "Bob", "UK"), (3, "Carol", "US")],
        ["id", "name", "country"],
    ).createOrReplaceTempView("customers_not_in")

    spark.createDataFrame(
        [(101, 1, 500.0), (102, 2, 300.0), (103, 3, 200.0)],
        ["order_id", "customer_id", "amount"],
    ).createOrReplaceTempView("orders_not_in")

    actual = spark.sql("""
        SELECT order_id, customer_id, amount
        FROM orders_not_in
        WHERE customer_id NOT IN (
            SELECT id FROM customers_not_in WHERE country = 'US'
        )
    """)
    # US ids are {1, 3}; no NULLs in subquery → only customer_id=2 survives
    expected = spark.createDataFrame(
        [(102, 2, 300.0)],
        ["order_id", "customer_id", "amount"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_exists_subquery(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, "Alice"), (2, "Bob"), (3, "Carol")],
        ["id", "name"],
    ).createOrReplaceTempView("customers_exists")

    spark.createDataFrame(
        [(101, 1), (102, 1)],
        ["order_id", "cust_id"],
    ).createOrReplaceTempView("orders_exists")

    actual = spark.sql("""
        SELECT c.id, c.name
        FROM customers_exists c
        WHERE EXISTS (
            SELECT 1 FROM orders_exists o WHERE o.cust_id = c.id
        )
    """)
    # Only Alice (id=1) has orders
    expected = spark.createDataFrame(
        [(1, "Alice")],
        ["id", "name"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_not_exists_subquery(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, "Alice"), (2, "Bob"), (3, "Carol")],
        ["id", "name"],
    ).createOrReplaceTempView("customers_not_exists")

    spark.createDataFrame(
        [(101, 1), (102, 1)],
        ["order_id", "cust_id"],
    ).createOrReplaceTempView("orders_not_exists")

    actual = spark.sql("""
        SELECT c.id, c.name
        FROM customers_not_exists c
        WHERE NOT EXISTS (
            SELECT 1 FROM orders_not_exists o WHERE o.cust_id = c.id
        )
    """)
    # Bob and Carol have no orders
    expected = spark.createDataFrame(
        [(2, "Bob"), (3, "Carol")],
        ["id", "name"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_correlated_subquery(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, 100.0), (1, 200.0), (2, 50.0), (2, 150.0), (2, 80.0)],
        ["customer_id", "amount"],
    ).createOrReplaceTempView("orders_correlated")

    actual = spark.sql("""
        SELECT o.customer_id, o.amount
        FROM orders_correlated o
        WHERE o.amount > (
            SELECT AVG(o2.amount)
            FROM orders_correlated o2
            WHERE o2.customer_id = o.customer_id
        )
    """)
    # Cust 1: avg=150 → 200 qualifies
    # Cust 2: avg≈93.33 → 150 qualifies
    expected = spark.createDataFrame(
        [(1, 200.0), (2, 150.0)],
        ["customer_id", "amount"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_derived_table_in_from(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, "US", 150.0), (2, "UK", 50.0), (3, "US", 300.0)],
        ["customer_id", "region", "amount"],
    ).createOrReplaceTempView("orders_derived")

    actual = spark.sql("""
        SELECT sub.region, sub.total
        FROM (
            SELECT region, SUM(amount) AS total
            FROM orders_derived
            GROUP BY region
        ) AS sub
        WHERE sub.total > 100
    """)
    # US: 150+300=450 > 100 ✓   UK: 50 ≤ 100 ✗
    expected = spark.createDataFrame(
        [("US", 450.0)],
        ["region", "total"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)
