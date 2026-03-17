import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType


@pytest.mark.unit
def test_single_cte_filter(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("Alice", 30), ("Bob", 17), ("Carol", 25)],
        ["name", "age"],
    ).createOrReplaceTempView("people_cte_filter")

    actual = spark.sql("""
        WITH adults AS (
            SELECT name, age
            FROM people_cte_filter
            WHERE age >= 18
        )
        SELECT name, age FROM adults
    """)
    expected = spark.createDataFrame(
        [("Alice", 30), ("Carol", 25)],
        ["name", "age"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_multiple_chained_ctes(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("US", 100), ("US", 200), ("UK", 50), ("US", 80), ("UK", 300)],
        ["region", "amount"],
    ).createOrReplaceTempView("sales_chained")

    actual = spark.sql("""
        WITH raw AS (
            SELECT region, amount FROM sales_chained
        ),
        filtered AS (
            SELECT region, amount FROM raw WHERE amount >= 80
        ),
        aggregated AS (
            SELECT region, SUM(amount) AS total FROM filtered GROUP BY region
        )
        SELECT region, total FROM aggregated
    """)
    # US: 100+200+80=380  (50 is in UK, not US)
    # UK: 300 only (50 excluded)
    expected = spark.createDataFrame(
        [("US", 380), ("UK", 300)],
        ["region", "total"],
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_cte_referenced_twice(spark: SparkSession) -> None:
    actual = spark.sql("""
        WITH nums AS (
            SELECT 1 AS n
            UNION ALL
            SELECT 2
        )
        SELECT COUNT(*) AS cross_count
        FROM nums a
        CROSS JOIN nums b
    """)
    result = actual.collect()[0]["cross_count"]
    # 2-row CTE cross-joined with itself → 2×2 = 4
    assert result == 4


@pytest.mark.unit
def test_cte_replaces_subquery(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("Alice", 150.0), ("Bob", 80.0), ("Carol", 200.0)],
        ["name", "amount"],
    ).createOrReplaceTempView("orders_cte_subq")

    cte_result = spark.sql("""
        WITH high_value AS (
            SELECT name, amount FROM orders_cte_subq WHERE amount > 100
        )
        SELECT name, amount FROM high_value
    """)
    derived_result = spark.sql("""
        SELECT name, amount
        FROM (
            SELECT name, amount FROM orders_cte_subq WHERE amount > 100
        ) AS high_value
    """)
    assert_df_equality(cte_result, derived_result, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_recursive_cte(spark: SparkSession) -> None:
    # Spark SQL does not support WITH RECURSIVE; generate the sequence with
    # the built-in sequence() array function + explode instead.
    actual = spark.sql("""
        WITH numbers AS (
            SELECT explode(sequence(1, 5)) AS n
        )
        SELECT n FROM numbers ORDER BY n
    """)
    # sequence() yields IntegerType; match it explicitly to avoid LongType mismatch
    schema = StructType([StructField("n", IntegerType(), True)])
    expected = spark.createDataFrame([(1,), (2,), (3,), (4,), (5,)], schema)
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)
