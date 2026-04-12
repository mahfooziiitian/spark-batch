"""Tests for Spark SQL date and timestamp functions."""

import pytest
from pyspark.sql import SparkSession


@pytest.mark.unit
def test_date_add(spark: SparkSession) -> None:
    result = spark.sql("SELECT DATE_ADD(DATE '2024-01-01', 7) AS future_date")
    import datetime

    assert result.first()["future_date"] == datetime.date(2024, 1, 8)


@pytest.mark.unit
def test_date_sub(spark: SparkSession) -> None:
    result = spark.sql("SELECT DATE_SUB(DATE '2024-01-15', 1) AS yesterday")
    import datetime

    assert result.first()["yesterday"] == datetime.date(2024, 1, 14)


@pytest.mark.unit
def test_datediff(spark: SparkSession) -> None:
    result = spark.sql(
        "SELECT DATEDIFF(DATE '2024-01-15', DATE '2024-01-01') AS diff_days"
    )
    assert result.first()["diff_days"] == 14


@pytest.mark.unit
def test_date_format(spark: SparkSession) -> None:
    result = spark.sql("SELECT DATE_FORMAT(DATE '2024-03-15', 'yyyy-MM') AS month_str")
    assert result.first()["month_str"] == "2024-03"


@pytest.mark.unit
def test_year_month_day_extraction(spark: SparkSession) -> None:
    result = spark.sql(
        "SELECT YEAR(DATE '2024-03-15') AS yr,"
        "       MONTH(DATE '2024-03-15') AS mo,"
        "       DAY(DATE '2024-03-15') AS dy"
    )
    row = result.first()
    assert row["yr"] == 2024
    assert row["mo"] == 3
    assert row["dy"] == 15


@pytest.mark.unit
def test_date_trunc_month(spark: SparkSession) -> None:
    result = spark.sql("SELECT DATE_TRUNC('MONTH', DATE '2024-03-15') AS month_start")
    import datetime

    assert result.first()["month_start"] == datetime.datetime(2024, 3, 1, 0, 0, 0)


@pytest.mark.unit
def test_to_date_parsing(spark: SparkSession) -> None:
    result = spark.sql("SELECT TO_DATE('2024-01-15', 'yyyy-MM-dd') AS parsed_date")
    import datetime

    assert result.first()["parsed_date"] == datetime.date(2024, 1, 15)
