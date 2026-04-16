"""ColumnProfilerRunner example — generate statistical profiles for each column."""

from pydeequ.profiles import ColumnProfilerRunner

from dpu.sample_data import create_retail_df, create_sample_df
from dpu.spark_session import create_spark


def profile_columns(spark, df) -> dict:
    """Profile every column in the DataFrame.

    Args:
        spark: Active SparkSession.
        df: Input DataFrame to profile.

    Returns:
        A ColumnProfiles result whose ``.profiles`` dict maps
        column name → profile object.
    """
    return ColumnProfilerRunner(spark).onData(df).run()


def main() -> None:
    """Run the column profiling demo."""
    spark = create_spark("deequ-profiles")

    print("=== Basic sample profiles ===")
    basic_df = create_sample_df(spark)
    basic_result = profile_columns(spark, basic_df)
    for _col, profile in basic_result.profiles.items():
        print(profile)

    print("\n=== Retail data profiles ===")
    retail_df = create_retail_df(spark)
    retail_result = profile_columns(spark, retail_df)
    for _col, profile in retail_result.profiles.items():
        print(profile)

    spark.stop()


if __name__ == "__main__":
    main()
