"""Tests for statistical aggregation functions in Spark SQL."""

import math

import pytest
from pyspark.sql import SparkSession


@pytest.mark.unit
def test_stddev_returns_correct_value(spark: SparkSession) -> None:
    # [1, 3, 5]: mean=3, sample variance = ((4+0+4)/2) = 4, sample stddev = 2.0 exactly
    spark.createDataFrame(
        [(v,) for v in [1, 3, 5]],
        ["val"],
    ).createOrReplaceTempView("stats_stddev")

    row = spark.sql("SELECT STDDEV(val) AS sd FROM stats_stddev").collect()[0]
    assert math.isclose(row["sd"], 2.0, rel_tol=1e-6)


@pytest.mark.unit
def test_corr_perfect_correlation(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1.0, 2.0), (2.0, 4.0), (3.0, 6.0), (4.0, 8.0), (5.0, 10.0)],
        ["x", "y"],
    ).createOrReplaceTempView("stats_corr")

    row = spark.sql("SELECT CORR(x, y) AS correlation FROM stats_corr").collect()[0]
    assert math.isclose(row["correlation"], 1.0, abs_tol=1e-9)


@pytest.mark.unit
def test_percentile_approx(spark: SparkSession) -> None:
    # Sorted values: 1..9 — median = 5
    spark.createDataFrame(
        [(v,) for v in range(1, 10)],
        ["val"],
    ).createOrReplaceTempView("stats_pct")

    row = spark.sql("SELECT PERCENTILE_APPROX(val, 0.5) AS median FROM stats_pct").collect()[0]
    assert row["median"] == 5


@pytest.mark.unit
def test_variance(spark: SparkSession) -> None:
    # [1, 3, 5]: mean=3, sample variance = ((4+0+4)/2) = 4.0 exactly
    spark.createDataFrame(
        [(v,) for v in [1, 3, 5]],
        ["val"],
    ).createOrReplaceTempView("stats_var")

    row = spark.sql("SELECT VAR_SAMP(val) AS variance FROM stats_var").collect()[0]
    assert math.isclose(row["variance"], 4.0, rel_tol=1e-6)


@pytest.mark.unit
def test_stats_with_filter_clause(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(10.0, True), (20.0, True), (30.0, False), (40.0, True)],
        ["val", "flag"],
    ).createOrReplaceTempView("stats_filter")

    # STDDEV only over rows where flag = true: [10, 20, 40]
    # Sample stddev of [10, 20, 40] = sqrt(((10-70/3)^2 + (20-70/3)^2 + (40-70/3)^2) / 2)
    # mean = 70/3 ≈ 23.333, deviations: -13.33, -3.33, 16.67 → var = (177.78+11.11+277.78)/2 = 233.33 → sd ≈ 15.275
    row = spark.sql(
        "SELECT STDDEV(val) FILTER (WHERE flag = true) AS sd FROM stats_filter"
    ).collect()[0]
    expected_sd = math.sqrt(((10 - 70 / 3) ** 2 + (20 - 70 / 3) ** 2 + (40 - 70 / 3) ** 2) / 2)
    assert math.isclose(row["sd"], expected_sd, rel_tol=1e-6)
