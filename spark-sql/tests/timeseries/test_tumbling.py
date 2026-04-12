"""Tests for tumbling (fixed-size, non-overlapping) time window aggregations."""

import pytest
from pyspark.sql import SparkSession


@pytest.mark.unit
def test_daily_tumbling_window(spark: SparkSession) -> None:
    events = spark.createDataFrame(
        [
            ("2024-01-01 08:00:00", "login"),
            ("2024-01-01 14:30:00", "purchase"),
            ("2024-01-02 10:00:00", "login"),
        ],
        ["event_ts", "event_type"],
    )
    events.createOrReplaceTempView("tbl_daily_events")
    result = spark.sql(
        """
        SELECT CAST(event_ts AS DATE) AS day, COUNT(*) AS cnt
        FROM tbl_daily_events
        GROUP BY CAST(event_ts AS DATE)
        ORDER BY day
        """
    )
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["cnt"] == 2  # 2024-01-01: two events
    assert rows[1]["cnt"] == 1  # 2024-01-02: one event


@pytest.mark.unit
def test_hourly_tumbling_window(spark: SparkSession) -> None:
    events = spark.createDataFrame(
        [
            ("2024-01-01 08:05:00", "A"),
            ("2024-01-01 08:45:00", "B"),
            ("2024-01-01 09:15:00", "C"),
        ],
        ["event_ts", "event_type"],
    )
    events.createOrReplaceTempView("tbl_hourly_events")
    result = spark.sql(
        """
        SELECT DATE_TRUNC('HOUR', CAST(event_ts AS TIMESTAMP)) AS hour_bucket,
               COUNT(*) AS cnt
        FROM tbl_hourly_events
        GROUP BY DATE_TRUNC('HOUR', CAST(event_ts AS TIMESTAMP))
        ORDER BY hour_bucket
        """
    )
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["cnt"] == 2  # 08:xx bucket
    assert rows[1]["cnt"] == 1  # 09:xx bucket


@pytest.mark.unit
def test_15min_bucket(spark: SparkSession) -> None:
    events = spark.createDataFrame(
        [
            ("2024-01-01 08:00:00", "A"),
            ("2024-01-01 08:10:00", "B"),  # same 15-min bucket as A (08:00–08:14)
            ("2024-01-01 08:20:00", "C"),  # next bucket (08:15–08:29)
        ],
        ["event_ts", "event_type"],
    )
    events.createOrReplaceTempView("tbl_15min_events")
    result = spark.sql(
        """
        SELECT FLOOR(UNIX_TIMESTAMP(CAST(event_ts AS TIMESTAMP)) / 900) * 900 AS bucket_ts,
               COUNT(*) AS cnt
        FROM tbl_15min_events
        GROUP BY FLOOR(UNIX_TIMESTAMP(CAST(event_ts AS TIMESTAMP)) / 900) * 900
        ORDER BY bucket_ts
        """
    )
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["cnt"] == 2  # A and B share the first 15-min bucket
    assert rows[1]["cnt"] == 1  # C is in the next bucket


@pytest.mark.unit
def test_tumbling_count_per_bucket(spark: SparkSession) -> None:
    events = spark.createDataFrame(
        [
            ("2024-01-01", "A"),
            ("2024-01-01", "B"),
            ("2024-01-01", "C"),
            ("2024-01-02", "D"),
            ("2024-01-02", "E"),
            ("2024-01-03", "F"),
        ],
        ["event_date", "event_type"],
    )
    events.createOrReplaceTempView("tbl_count_events")
    result = spark.sql(
        """
        SELECT event_date, COUNT(*) AS cnt
        FROM tbl_count_events
        GROUP BY event_date
        ORDER BY event_date
        """
    )
    rows = result.collect()
    assert rows[0]["cnt"] == 3  # 2024-01-01
    assert rows[1]["cnt"] == 2  # 2024-01-02
    assert rows[2]["cnt"] == 1  # 2024-01-03
