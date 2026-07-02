import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession


@pytest.mark.unit
def test_equals(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("Alice", "F"), ("Bob", "M"), ("Carol", "F")],
        ["name", "gender"],
    ).createOrReplaceTempView("people_equals")

    actual = spark.sql("SELECT name FROM people_equals WHERE gender = 'F'")
    expected = spark.createDataFrame([("Alice",), ("Carol",)], ["name"])
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_not_equals(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("Alice", "F"), ("Bob", "M"), ("Carol", "F")],
        ["name", "gender"],
    ).createOrReplaceTempView("people_not_equals")

    actual = spark.sql("SELECT name FROM people_not_equals WHERE gender != 'F'")
    expected = spark.createDataFrame([("Bob",)], ["name"])
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_between_inclusive(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, 5), (2, 10), (3, 15), (4, 20), (5, 25)],
        ["id", "score"],
    ).createOrReplaceTempView("scores_between")

    actual = spark.sql("SELECT id, score FROM scores_between WHERE score BETWEEN 10 AND 20")
    # Both endpoints 10 and 20 are inclusive
    expected = spark.createDataFrame(
        [(2, 10), (3, 15), (4, 20)],
        ["id", "score"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_not_between(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, 5), (2, 10), (3, 15), (4, 20), (5, 25)],
        ["id", "score"],
    ).createOrReplaceTempView("scores_not_between")

    actual = spark.sql("SELECT id, score FROM scores_not_between WHERE score NOT BETWEEN 10 AND 20")
    expected = spark.createDataFrame(
        [(1, 5), (5, 25)],
        ["id", "score"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_in_list(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, "A"), (2, "B"), (3, "C"), (4, "D")],
        ["id", "category"],
    ).createOrReplaceTempView("items_in_list")

    actual = spark.sql("SELECT id, category FROM items_in_list WHERE category IN ('A', 'B')")
    expected = spark.createDataFrame(
        [(1, "A"), (2, "B")],
        ["id", "category"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_null_safe_equals_nulls(spark: SparkSession) -> None:
    result = spark.sql("SELECT (NULL <=> NULL) AS result").collect()[0]["result"]
    assert result is True


@pytest.mark.unit
def test_greater_than(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, 50), (2, 100), (3, 200)],
        ["id", "value"],
    ).createOrReplaceTempView("items_gt")

    actual = spark.sql("SELECT id, value FROM items_gt WHERE value > 50")
    expected = spark.createDataFrame(
        [(2, 100), (3, 200)],
        ["id", "value"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_between_dates(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("Alice", "2024-01-15"), ("Bob", "2024-04-01"), ("Carol", "2024-03-31")],
        ["name", "order_date"],
    ).createOrReplaceTempView("dated_orders")

    actual = spark.sql("""
        SELECT name
        FROM dated_orders
        WHERE CAST(order_date AS DATE) BETWEEN DATE '2024-01-01' AND DATE '2024-03-31'
    """)
    # Alice (Jan 15) and Carol (Mar 31, inclusive) qualify; Bob (Apr 1) does not
    expected = spark.createDataFrame(
        [("Alice",), ("Carol",)],
        ["name"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)
