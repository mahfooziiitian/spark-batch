"""Demonstrate PyDeequ AnalysisRunner for computing data quality metrics."""

import os

os.environ["SPARK_VERSION"] = "3.5"

import pydeequ
from pydeequ.analyzers import (
    AnalysisRunner,
    AnalyzerContext,
    Completeness,
    Mean,
    Size,
)
from pyspark.sql import DataFrame, Row, SparkSession


def run_analysis(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Run size and completeness analyzers on the given DataFrame.

    Args:
        spark: Active SparkSession with Deequ JAR configured.
        df: Input DataFrame to analyze.

    Returns:
        DataFrame containing analysis metrics (entity, instance, name, value).
    """
    analysis_result = (
        AnalysisRunner(spark)
        .onData(df)
        .addAnalyzer(Size())
        .addAnalyzer(Completeness("b"))
        .addAnalyzer(Mean("b"))
        .run()
    )
    return AnalyzerContext.successMetricsAsDataFrame(spark, analysis_result)


def main() -> None:
    """Run the AnalysisRunner demo."""
    spark = (
        SparkSession.builder.appName("deequ-analyzers")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(
        [Row(a="foo", b=1, c=5), Row(a="bar", b=2, c=6), Row(a="baz", b=3, c=None)]
    )

    result_df = run_analysis(spark, df)
    result_df.show()

    spark.stop()


if __name__ == "__main__":
    main()
