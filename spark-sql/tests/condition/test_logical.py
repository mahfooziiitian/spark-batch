import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession


@pytest.mark.unit
def test_and_both_true(spark: SparkSession) -> None:
    result = spark.sql("SELECT (true AND true) AS result").collect()[0]["result"]
    assert result is True


@pytest.mark.unit
def test_and_one_false(spark: SparkSession) -> None:
    result = spark.sql("SELECT (true AND false) AS result").collect()[0]["result"]
    assert result is False


@pytest.mark.unit
def test_or_one_true(spark: SparkSession) -> None:
    result = spark.sql("SELECT (false OR true) AS result").collect()[0]["result"]
    assert result is True


@pytest.mark.unit
def test_not_true(spark: SparkSession) -> None:
    result = spark.sql("SELECT (NOT true) AS result").collect()[0]["result"]
    assert result is False


@pytest.mark.unit
def test_complex_and_or(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, 15, "X", None), (2, 5, "X", None), (3, 20, "Y", None), (4, 8, "Z", 42)],
        ["id", "a", "b", "c"],
    ).createOrReplaceTempView("complex_logic")

    actual = spark.sql("""
        SELECT id
        FROM complex_logic
        WHERE (a > 10 AND b = 'X') OR c IS NULL
    """)
    # id=1: (15>10 AND 'X'='X')=T  → T
    # id=2: (5>10 AND ...)=F, c IS NULL=T → T
    # id=3: (20>10 AND 'Y'='X')=F, c IS NULL=T → T
    # id=4: (8>10 AND ...)=F, c IS NULL=F → F
    expected = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_de_morgan_and(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, True, True), (2, True, False), (3, False, True), (4, False, False)],
        ["id", "a", "b"],
    ).createOrReplaceTempView("de_morgan_data")

    left = spark.sql("SELECT id, NOT (a AND b) AS result FROM de_morgan_data")
    right = spark.sql("SELECT id, ((NOT a) OR (NOT b)) AS result FROM de_morgan_data")
    # De Morgan: NOT(A AND B) ≡ (NOT A) OR (NOT B)
    assert_df_equality(left, right, ignore_row_order=True, ignore_nullable=True)
