import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


@pytest.mark.unit
def test_null_equality_returns_null(spark: SparkSession) -> None:
    result = spark.sql("SELECT (null = null) AS result").collect()[0]["result"]
    assert result is None


@pytest.mark.unit
def test_null_safe_equality(spark: SparkSession) -> None:
    result = spark.sql("SELECT (null <=> null) AS result").collect()[0]["result"]
    assert result is True


@pytest.mark.unit
def test_null_safe_inequality(spark: SparkSession) -> None:
    result = spark.sql("SELECT (1 <=> null) AS result").collect()[0]["result"]
    assert result is False


@pytest.mark.unit
def test_is_null_filter(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    spark.createDataFrame(
        [("Alice", 30), ("Bob", None), ("Carol", None)],
        schema,
    ).createOrReplaceTempView("people_null_filter")

    actual = spark.sql("SELECT name, age FROM people_null_filter WHERE age IS NULL")
    expected = spark.createDataFrame(
        [("Bob", None), ("Carol", None)],
        schema,
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_is_not_null_filter(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    spark.createDataFrame(
        [("Alice", 30), ("Bob", None), ("Carol", 25)],
        schema,
    ).createOrReplaceTempView("people_not_null_filter")

    actual = spark.sql("SELECT name FROM people_not_null_filter WHERE name IS NOT NULL")
    expected = spark.createDataFrame(
        [("Alice",), ("Bob",), ("Carol",)],
        ["name"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_count_star_includes_nulls(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    spark.createDataFrame(
        [("Alice", 30), ("Bob", None), ("Carol", None)],
        schema,
    ).createOrReplaceTempView("people_count_star")

    result = spark.sql("SELECT COUNT(*) AS cnt FROM people_count_star").collect()[0][
        "cnt"
    ]
    assert result == 3


@pytest.mark.unit
def test_count_col_excludes_nulls(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    spark.createDataFrame(
        [("Alice", 30), ("Bob", None), ("Carol", None)],
        schema,
    ).createOrReplaceTempView("people_count_col")

    result = spark.sql("SELECT COUNT(age) AS cnt FROM people_count_col").collect()[0][
        "cnt"
    ]
    # Only Alice has a non-NULL age
    assert result == 1


@pytest.mark.unit
def test_sum_ignores_nulls(spark: SparkSession) -> None:
    schema = StructType([StructField("val", IntegerType(), True)])
    spark.createDataFrame(
        [(1,), (None,), (3,)],
        schema,
    ).createOrReplaceTempView("vals_sum_null")

    result = spark.sql("SELECT SUM(val) AS total FROM vals_sum_null").collect()[0][
        "total"
    ]
    assert result == 4


@pytest.mark.unit
def test_coalesce_returns_first_non_null(spark: SparkSession) -> None:
    result = spark.sql("SELECT COALESCE(NULL, NULL, 5) AS result").collect()[0][
        "result"
    ]
    assert result == 5


@pytest.mark.unit
def test_nullif_returns_null_on_match(spark: SparkSession) -> None:
    result = spark.sql("SELECT NULLIF(5, 5) AS result").collect()[0]["result"]
    assert result is None


@pytest.mark.unit
def test_null_in_group_by(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("region", StringType(), True),
        ]
    )
    spark.createDataFrame(
        [("Alice", "US"), ("Bob", None), ("Carol", None), ("Dave", "UK")],
        schema,
    ).createOrReplaceTempView("people_null_group")

    actual = spark.sql("""
        SELECT region, COUNT(*) AS cnt
        FROM people_null_group
        GROUP BY region
    """)
    # Three groups: US=1, NULL=2, UK=1
    assert actual.count() == 3
    null_rows = actual.filter("region IS NULL").collect()
    assert len(null_rows) == 1
    assert null_rows[0]["cnt"] == 2


@pytest.mark.unit
def test_nulls_last_ordering(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("score", IntegerType(), True),
        ]
    )
    spark.createDataFrame(
        [("Alice", 90), ("Bob", None), ("Carol", 70)],
        schema,
    ).createOrReplaceTempView("scored_nulls_last")

    rows = spark.sql(
        "SELECT score FROM scored_nulls_last ORDER BY score ASC NULLS LAST"
    ).collect()
    scores = [r["score"] for r in rows]
    assert scores[-1] is None
    non_null = [s for s in scores if s is not None]
    assert non_null == sorted(non_null)


@pytest.mark.unit
def test_nulls_first_ordering(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("score", IntegerType(), True),
        ]
    )
    spark.createDataFrame(
        [("Alice", 90), ("Bob", None), ("Carol", 70)],
        schema,
    ).createOrReplaceTempView("scored_nulls_first")

    rows = spark.sql(
        "SELECT score FROM scored_nulls_first ORDER BY score ASC NULLS FIRST"
    ).collect()
    scores = [r["score"] for r in rows]
    assert scores[0] is None


@pytest.mark.unit
def test_not_in_with_null_returns_empty(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(2,), (3,), (4,)],
        ["val"],
    ).createOrReplaceTempView("vals_null_not_in")

    schema = StructType([StructField("val", IntegerType(), True)])
    spark.createDataFrame(
        [(1,), (None,)],
        schema,
    ).createOrReplaceTempView("exclusion_null_not_in")

    actual = spark.sql("""
        SELECT val
        FROM vals_null_not_in
        WHERE val NOT IN (SELECT val FROM exclusion_null_not_in)
    """)
    # Three-valued logic: NOT IN list containing NULL yields NULL for every row
    assert actual.count() == 0


@pytest.mark.unit
def test_and_null_with_false_is_false(spark: SparkSession) -> None:
    # NULL AND false = false (FALSE dominates AND in three-valued logic)
    result = spark.sql("SELECT (null AND false) AS result").collect()[0]["result"]
    assert result is False


@pytest.mark.unit
def test_or_null_with_true_is_true(spark: SparkSession) -> None:
    # NULL OR true = true (TRUE dominates OR in three-valued logic)
    result = spark.sql("SELECT (null OR true) AS result").collect()[0]["result"]
    assert result is True
