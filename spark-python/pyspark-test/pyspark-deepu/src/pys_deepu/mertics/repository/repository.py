"""Demonstrate PyDeequ FileSystemMetricsRepository for persisting metrics."""

import os

os.environ["SPARK_VERSION"] = "3.5"

import pydeequ
from pydeequ.analyzers import AnalysisRunner, ApproxCountDistinct
from pydeequ.repository import FileSystemMetricsRepository, ResultKey
from pyspark.sql import DataFrame, Row, SparkSession


def save_metrics(
    spark: SparkSession,
    df: DataFrame,
    repository: FileSystemMetricsRepository,
    tags: dict[str, str],
) -> None:
    """Run analysis and save results to the metrics repository.

    Args:
        spark: Active SparkSession with Deequ JAR configured.
        df: Input DataFrame to analyze.
        repository: Metrics repository for persistence.
        tags: Key-value tags for the result entry.
    """
    result_key = ResultKey(spark, ResultKey.current_milli_time(), tags)

    AnalysisRunner(spark).onData(df).addAnalyzer(ApproxCountDistinct("b")).useRepository(
        repository
    ).saveOrAppendResult(result_key).run()


def load_metrics(
    spark: SparkSession,
    repository: FileSystemMetricsRepository,
) -> DataFrame:
    """Load historical metrics from the repository.

    Args:
        spark: Active SparkSession with Deequ JAR configured.
        repository: Metrics repository to query.

    Returns:
        DataFrame containing historical metric results.
    """
    return (
        repository.load()
        .before(ResultKey.current_milli_time())
        .forAnalyzers([ApproxCountDistinct("b")])
        .getSuccessMetricsAsDataFrame()
    )


def main() -> None:
    """Run the FileSystemMetricsRepository demo."""
    spark = (
        SparkSession.builder.appName("deequ-repository")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(
        [Row(a="foo", b=1, c=5), Row(a="bar", b=2, c=6), Row(a="baz", b=3, c=None)]
    )

    metrics_file = FileSystemMetricsRepository.helper_metrics_file(spark, "metrics.json")
    repository = FileSystemMetricsRepository(spark, metrics_file)

    save_metrics(spark, df, repository, {"tag": "pydeequ demo"})
    result_df = load_metrics(spark, repository)
    result_df.show()

    if spark.sparkContext._gateway:  # type: ignore[union-attr]
        spark.sparkContext._gateway.shutdown_callback_server()
    spark.stop()


if __name__ == "__main__":
    main()
