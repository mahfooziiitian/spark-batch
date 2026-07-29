"""Demonstrate PyDeequ ColumnProfilerRunner for column-level data profiling."""

import os
from typing import Any

os.environ["SPARK_VERSION"] = "3.5"

import pydeequ
from pydeequ.profiles import ColumnProfilerRunner
from pyspark.sql import DataFrame, Row, SparkSession


def run_profiling(spark: SparkSession, df: DataFrame) -> dict[str, Any]:
    """Profile all columns in the given DataFrame.

    Args:
        spark: Active SparkSession with Deequ JAR configured.
        df: Input DataFrame to profile.

    Returns:
        Dict mapping column names to their profile objects.
    """
    result = ColumnProfilerRunner(spark).onData(df).run()
    return result.profiles


def main() -> None:
    """Run the ColumnProfilerRunner demo."""
    spark = (
        SparkSession.builder.appName("deequ-profiling")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(
        [Row(a="foo", b=1, c=5), Row(a="bar", b=2, c=6), Row(a="baz", b=3, c=None)]
    )

    profiles = run_profiling(spark, df)
    for col_name, profile in profiles.items():
        print(f"--- {col_name} ---")
        print(profile)

    spark.stop()


if __name__ == "__main__":
    main()
