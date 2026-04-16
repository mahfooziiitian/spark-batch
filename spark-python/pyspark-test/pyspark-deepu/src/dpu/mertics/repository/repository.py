"""FileSystemMetricsRepository example — persist and query analyzer metrics."""

import os

from pydeequ.analyzers import AnalysisRunner, ApproxCountDistinct, Completeness, Size
from pydeequ.repository import FileSystemMetricsRepository, ResultKey

from dpu.sample_data import create_sample_df
from dpu.spark_session import create_spark


def save_metrics(spark, df, repository: FileSystemMetricsRepository) -> None:
    """Run analyzers and persist the results to the metrics repository.

    Args:
        spark: Active SparkSession.
        df: Input DataFrame to analyse.
        repository: Target FileSystemMetricsRepository.
    """
    key_tags = {"tag": "pydeequ-demo"}
    result_key = ResultKey(spark, ResultKey.current_milli_time(), key_tags)

    (
        AnalysisRunner(spark)
        .onData(df)
        .addAnalyzer(Size())
        .addAnalyzer(Completeness("b"))
        .addAnalyzer(ApproxCountDistinct("b"))
        .useRepository(repository)
        .saveOrAppendResult(result_key)
        .run()
    )


def load_metrics(repository: FileSystemMetricsRepository):
    """Load stored metrics from the repository.

    Args:
        repository: Source FileSystemMetricsRepository.

    Returns:
        DataFrame of previously stored metrics.
    """
    return (
        repository.load()
        .before(ResultKey.current_milli_time())
        .forAnalyzers([ApproxCountDistinct("b")])
        .getSuccessMetricsAsDataFrame()
    )


def main() -> None:
    """Run the metrics repository demo."""
    spark = create_spark("deequ-repository")

    metrics_path = os.environ.get("OUTPUT_PATH", "/tmp/deequ_metrics")
    metrics_file = FileSystemMetricsRepository.helper_metrics_file(spark, f"{metrics_path}/metrics.json")
    repository = FileSystemMetricsRepository(spark, metrics_file)

    df = create_sample_df(spark)

    print("=== Saving metrics ===")
    save_metrics(spark, df, repository)

    print("=== Loading metrics ===")
    result_df = load_metrics(repository)
    result_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
