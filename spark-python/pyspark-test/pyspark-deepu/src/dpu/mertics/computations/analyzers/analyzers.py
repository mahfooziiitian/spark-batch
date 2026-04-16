"""AnalysisRunner example — compute column-level metrics with PyDeequ analyzers."""

from pydeequ.analyzers import AnalysisRunner, AnalyzerContext, Completeness, Mean, Size
from pyspark.sql import DataFrame

from dpu.sample_data import create_retail_df, create_sample_df
from dpu.spark_session import create_spark


def run_basic_analysis(spark, df: DataFrame) -> DataFrame:
    """Run Size + Completeness analyzers and return the metrics DataFrame.

    Args:
        spark: Active SparkSession.
        df: Input DataFrame to analyse.

    Returns:
        DataFrame with one row per metric (entity, instance, name, value).
    """
    result = (
        AnalysisRunner(spark)
        .onData(df)
        .addAnalyzer(Size())
        .addAnalyzer(Completeness("b"))
        .addAnalyzer(Completeness("c"))
        .run()
    )
    return AnalyzerContext.successMetricsAsDataFrame(spark, result)


def run_retail_analysis(spark, df: DataFrame) -> DataFrame:
    """Run a broader set of analyzers on a retail DataFrame.

    Args:
        spark: Active SparkSession.
        df: Retail DataFrame with columns price, quantity, category.

    Returns:
        DataFrame with one row per metric.
    """
    result = (
        AnalysisRunner(spark)
        .onData(df)
        .addAnalyzer(Size())
        .addAnalyzer(Completeness("price"))
        .addAnalyzer(Completeness("quantity"))
        .addAnalyzer(Mean("price"))
        .addAnalyzer(Mean("quantity"))
        .run()
    )
    return AnalyzerContext.successMetricsAsDataFrame(spark, result)


def main() -> None:
    """Run the analyzer demo."""
    spark = create_spark("deequ-analyzers")

    print("=== Basic analysis ===")
    basic_df = create_sample_df(spark)
    run_basic_analysis(spark, basic_df).show(truncate=False)

    print("=== Retail analysis ===")
    retail_df = create_retail_df(spark)
    run_retail_analysis(spark, retail_df).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
