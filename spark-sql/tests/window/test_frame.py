import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def tied_view(spark: SparkSession):
    """Three rows: two share the same order value (tie), one is distinct."""
    tied_data = [("A", "2024-01-01", 100), ("A", "2024-01-01", 200), ("A", "2024-01-03", 300)]
    spark.createDataFrame(tied_data, ["grp", "dt", "val"]).createOrReplaceTempView(
        "tied_frame"
    )
    return "tied_frame"


@pytest.fixture(scope="module")
def seq_view(spark: SparkSession):
    """Sequential 5-row dataset for sliding window tests."""
    seq_data = [("A", "2024-01-01", 10), ("A", "2024-01-02", 20), ("A", "2024-01-03", 30),
                ("A", "2024-01-04", 40), ("A", "2024-01-05", 50)]
    spark.createDataFrame(seq_data, ["grp", "dt", "val"]).createOrReplaceTempView(
        "seq_frame"
    )
    return "seq_frame"


@pytest.mark.unit
def test_rows_vs_range_with_ties(spark: SparkSession, tied_view: str):
    """ROWS counts physical rows; RANGE expands to include all rows with equal ORDER BY value."""
    result = spark.sql(
        """
        SELECT grp, dt, val,
               SUM(val) OVER (PARTITION BY grp ORDER BY dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rows_sum,
               SUM(val) OVER (PARTITION BY grp ORDER BY dt RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_sum
        FROM tied_frame
        ORDER BY val
        """
    )
    rows = result.collect()

    rows_sums = [r.rows_sum for r in rows]
    range_sums = [r.range_sum for r in rows]

    # ROWS: physical cumulative — 100, 300, 600
    assert rows_sums == [100, 300, 600]
    # RANGE: both tied rows (same dt) see the same running sum = 300; last = 600
    assert range_sums == [300, 300, 600]
    # They must differ (the tied rows prove ROWS != RANGE)
    assert rows_sums != range_sums


@pytest.mark.unit
def test_rows_2_preceding(spark: SparkSession, seq_view: str):
    """ROWS BETWEEN 2 PRECEDING AND CURRENT ROW: window grows until it reaches size 3."""
    result = spark.sql(
        """
        SELECT grp, dt, val,
               SUM(val) OVER (
                   PARTITION BY grp ORDER BY dt
                   ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
               ) AS sliding_sum
        FROM seq_frame
        ORDER BY dt
        """
    )
    sums = [row.sliding_sum for row in result.collect()]
    # pos1: 10; pos2: 10+20=30; pos3: 10+20+30=60; pos4: 20+30+40=90; pos5: 30+40+50=120
    assert sums == [10, 30, 60, 90, 120]


@pytest.mark.unit
def test_unbounded_following_suffix_sum(spark: SparkSession, seq_view: str):
    """SUM from CURRENT ROW to UNBOUNDED FOLLOWING produces a descending suffix sum."""
    result = spark.sql(
        """
        SELECT grp, dt, val,
               SUM(val) OVER (
                   PARTITION BY grp ORDER BY dt
                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
               ) AS suffix_sum
        FROM seq_frame
        ORDER BY dt
        """
    )
    sums = [row.suffix_sum for row in result.collect()]
    # 10+20+30+40+50=150; 20+30+40+50=140; 30+40+50=120; 40+50=90; 50
    assert sums == [150, 140, 120, 90, 50]
    assert sums == sorted(sums, reverse=True), "Suffix sum should be strictly descending"
