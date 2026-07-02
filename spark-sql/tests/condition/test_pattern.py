import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession


@pytest.mark.unit
def test_like_starts_with(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("Alice",), ("Alice Smith",), ("Bob",), ("Alicia",)],
        ["name"],
    ).createOrReplaceTempView("names_like_starts")

    actual = spark.sql("SELECT name FROM names_like_starts WHERE name LIKE 'Alice%'")
    # 'Alicia' does not start with the exact prefix 'Alice'
    expected = spark.createDataFrame(
        [("Alice",), ("Alice Smith",)],
        ["name"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_like_contains(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("Alice",), ("Alice Smith",), ("ALICE",), ("bob",), ("alison",)],
        ["name"],
    ).createOrReplaceTempView("names_like_contains")

    actual = spark.sql("SELECT name FROM names_like_contains WHERE name ILIKE '%ali%'")
    expected = spark.createDataFrame(
        [("Alice",), ("Alice Smith",), ("ALICE",), ("alison",)],
        ["name"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_like_ends_with(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("Anderson",), ("Johnson",), ("Alice",), ("Richardson",)],
        ["name"],
    ).createOrReplaceTempView("names_like_ends")

    actual = spark.sql("SELECT name FROM names_like_ends WHERE name LIKE '%son'")
    # Anderson, Johnson, Richardson all end with 'son'
    expected = spark.createDataFrame(
        [("Anderson",), ("Johnson",), ("Richardson",)],
        ["name"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_not_like(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("Alice",), ("Bob",), ("Amy",), ("Charlie",)],
        ["name"],
    ).createOrReplaceTempView("names_not_like")

    actual = spark.sql("SELECT name FROM names_not_like WHERE name NOT LIKE 'A%'")
    expected = spark.createDataFrame(
        [("Bob",), ("Charlie",)],
        ["name"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_rlike_digits(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("12345",), ("abc",), ("007",), ("1a2b",)],
        ["code"],
    ).createOrReplaceTempView("codes_rlike")

    actual = spark.sql("SELECT code FROM codes_rlike WHERE code RLIKE '^[0-9]+$'")
    expected = spark.createDataFrame(
        [("12345",), ("007",)],
        ["code"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_rlike_email_format(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("alice@example.com",), ("not-an-email",), ("bob@test.com",), ("bad@",)],
        ["email"],
    ).createOrReplaceTempView("emails_rlike")

    actual = spark.sql(r"SELECT email FROM emails_rlike WHERE email RLIKE '^[a-z]+@[a-z]+\.com$'")
    expected = spark.createDataFrame(
        [("alice@example.com",), ("bob@test.com",)],
        ["email"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)
