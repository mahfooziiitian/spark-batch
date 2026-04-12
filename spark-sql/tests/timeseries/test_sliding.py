"""Tests for sliding window aggregations (moving average, running total)."""

import pytest
from pyspark.sql import SparkSession


@pytest.mark.unit
def test_3row_moving_average(spark: SparkSession) -> None:
    """AVG over ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING (3-row centred window)."""
    sales = spark.createDataFrame(
        [
            (1, "2024-01-01", 10.0),
            (2, "2024-01-02", 20.0),
            (3, "2024-01-03", 30.0),
            (4, "2024-01-04", 40.0),
            (5, "2024-01-05", 50.0),
        ],
        ["id", "sale_date", "amount"],
    )
    sales.createOrReplaceTempView("tbl_moving_sales")
    result = spark.sql(
        """
        SELECT id, sale_date, amount,
               AVG(amount) OVER (
                   ORDER BY sale_date
                   ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
               ) AS moving_avg
        FROM tbl_moving_sales
        ORDER BY sale_date
        """
    )
    rows = result.collect()
    # id=1: avg(10, 20)       = 15.0   (no preceding row)
    # id=2: avg(10, 20, 30)   = 20.0
    # id=3: avg(20, 30, 40)   = 30.0
    # id=4: avg(30, 40, 50)   = 40.0
    # id=5: avg(40, 50)       = 45.0   (no following row)
    assert abs(rows[0]["moving_avg"] - 15.0) < 0.001
    assert abs(rows[1]["moving_avg"] - 20.0) < 0.001
    assert abs(rows[2]["moving_avg"] - 30.0) < 0.001
    assert abs(rows[3]["moving_avg"] - 40.0) < 0.001
    assert abs(rows[4]["moving_avg"] - 45.0) < 0.001


@pytest.mark.unit
def test_running_total_with_order(spark: SparkSession) -> None:
    """SUM OVER (ORDER BY date ROWS UNBOUNDED PRECEDING) produces a monotonically increasing total."""
    sales = spark.createDataFrame(
        [
            (1, "2024-01-01", 10.0),
            (2, "2024-01-02", 20.0),
            (3, "2024-01-03", 30.0),
            (4, "2024-01-04", 40.0),
            (5, "2024-01-05", 50.0),
        ],
        ["id", "sale_date", "amount"],
    )
    sales.createOrReplaceTempView("tbl_running_sales")
    result = spark.sql(
        """
        SELECT id, sale_date, amount,
               SUM(amount) OVER (
                   ORDER BY sale_date
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) AS running_total
        FROM tbl_running_sales
        ORDER BY sale_date
        """
    )
    rows = result.collect()
    totals = [r["running_total"] for r in rows]

    assert totals == [10.0, 30.0, 60.0, 100.0, 150.0]
    # Also verify the series is strictly increasing
    for i in range(1, len(totals)):
        assert totals[i] > totals[i - 1]
