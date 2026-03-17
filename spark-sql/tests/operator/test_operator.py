import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession


@pytest.mark.unit
def test_arithmetic_operators(spark: SparkSession) -> None:
    row = spark.sql("""
        SELECT
            10 + 3 AS add_result,
            10 - 3 AS sub_result,
            10 * 3 AS mul_result,
            10 / 3 AS div_result,
            10 % 3 AS mod_result
    """).collect()[0]

    assert row["add_result"] == 13
    assert row["sub_result"] == 7
    assert row["mul_result"] == 30
    assert abs(row["div_result"] - (10 / 3)) < 1e-9
    assert row["mod_result"] == 1


@pytest.mark.unit
def test_integer_division(spark: SparkSession) -> None:
    result = spark.sql("SELECT 7 DIV 2 AS result").collect()[0]["result"]
    assert result == 3


@pytest.mark.unit
def test_union_all_preserves_duplicates(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, "A"), (2, "B"), (3, "C")],
        ["id", "val"],
    ).createOrReplaceTempView("set_left_union_all")

    spark.createDataFrame(
        [(1, "A"), (2, "B"), (3, "C")],
        ["id", "val"],
    ).createOrReplaceTempView("set_right_union_all")

    actual = spark.sql("""
        SELECT id, val FROM set_left_union_all
        UNION ALL
        SELECT id, val FROM set_right_union_all
    """)
    # UNION ALL keeps all rows including duplicates → 3+3 = 6
    assert actual.count() == 6


@pytest.mark.unit
def test_union_deduplicates(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, "A"), (2, "B"), (3, "C")],
        ["id", "val"],
    ).createOrReplaceTempView("set_left_union")

    spark.createDataFrame(
        [(1, "A"), (4, "D"), (5, "E")],
        ["id", "val"],
    ).createOrReplaceTempView("set_right_union")

    actual = spark.sql("""
        SELECT id, val FROM set_left_union
        UNION
        SELECT id, val FROM set_right_union
    """)
    # {1,2,3} ∪ {1,4,5} = {1,2,3,4,5} → 5 distinct rows
    assert actual.count() == 5


@pytest.mark.unit
def test_intersect(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, "A"), (2, "B"), (3, "C")],
        ["id", "val"],
    ).createOrReplaceTempView("set_left_intersect")

    spark.createDataFrame(
        [(2, "B"), (3, "C"), (4, "D")],
        ["id", "val"],
    ).createOrReplaceTempView("set_right_intersect")

    actual = spark.sql("""
        SELECT id, val FROM set_left_intersect
        INTERSECT
        SELECT id, val FROM set_right_intersect
    """)
    expected = spark.createDataFrame(
        [(2, "B"), (3, "C")],
        ["id", "val"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_except(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, "A"), (2, "B"), (3, "C")],
        ["id", "val"],
    ).createOrReplaceTempView("set_left_except")

    spark.createDataFrame(
        [(2, "B"), (3, "C"), (4, "D")],
        ["id", "val"],
    ).createOrReplaceTempView("set_right_except")

    actual = spark.sql("""
        SELECT id, val FROM set_left_except
        EXCEPT
        SELECT id, val FROM set_right_except
    """)
    # {1,2,3} − {2,3,4} = {1}
    expected = spark.createDataFrame(
        [(1, "A")],
        ["id", "val"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_string_concat(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("Hello", "World"), ("foo", "bar")],
        ["col1", "col2"],
    ).createOrReplaceTempView("strings_concat")

    actual = spark.sql("SELECT col1 || ' ' || col2 AS combined FROM strings_concat")
    expected = spark.createDataFrame(
        [("Hello World",), ("foo bar",)],
        ["combined"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)
