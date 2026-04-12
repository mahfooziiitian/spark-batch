import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession

SIZES = [("S",), ("M",), ("L",)]
SIZES_COLS = ["size"]

COLORS = [("Red",), ("Green",), ("Blue",)]
COLORS_COLS = ["color"]


@pytest.mark.unit
def test_cross_join_row_count(spark: SparkSession) -> None:
    spark.createDataFrame(SIZES, SIZES_COLS).createOrReplaceTempView("sizes")
    spark.createDataFrame(COLORS, COLORS_COLS).createOrReplaceTempView("colors")

    actual = spark.sql("""
        SELECT s.size, c.color
        FROM sizes AS s
        CROSS JOIN colors AS c
    """)

    # 3 sizes × 3 colors = 9 combinations
    assert actual.count() == 9


@pytest.mark.unit
def test_cross_join_all_combinations_present(spark: SparkSession) -> None:
    spark.createDataFrame(SIZES, SIZES_COLS).createOrReplaceTempView("sizes")
    spark.createDataFrame(COLORS, COLORS_COLS).createOrReplaceTempView("colors")

    actual = spark.sql("""
        SELECT s.size, c.color
        FROM sizes AS s
        CROSS JOIN colors AS c
    """)

    expected = spark.createDataFrame(
        [
            ("S", "Red"),
            ("S", "Green"),
            ("S", "Blue"),
            ("M", "Red"),
            ("M", "Green"),
            ("M", "Blue"),
            ("L", "Red"),
            ("L", "Green"),
            ("L", "Blue"),
        ],
        ["size", "color"],
    )

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_cross_join_with_filter(spark: SparkSession) -> None:
    spark.createDataFrame(SIZES, SIZES_COLS).createOrReplaceTempView("sizes")
    spark.createDataFrame(COLORS, COLORS_COLS).createOrReplaceTempView("colors")

    actual = spark.sql("""
        SELECT s.size, c.color
        FROM sizes AS s
        CROSS JOIN colors AS c
        WHERE s.size = 'M'
    """)

    expected = spark.createDataFrame(
        [("M", "Red"), ("M", "Green"), ("M", "Blue")],
        ["size", "color"],
    )

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)
