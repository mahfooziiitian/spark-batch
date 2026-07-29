"""Demonstrate PyDeequ VerificationSuite for constraint validation."""

import os

os.environ["SPARK_VERSION"] = "3.5"

import pydeequ
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import DataFrame, Row, SparkSession


def run_verification(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Run constraint verification on the given DataFrame.

    Args:
        spark: Active SparkSession with Deequ JAR configured.
        df: Input DataFrame to verify.

    Returns:
        DataFrame containing verification results per constraint.
    """
    check = Check(spark, CheckLevel.Warning, "Data Quality Check")

    check_result = (
        VerificationSuite(spark)
        .onData(df)
        .addCheck(
            check.hasSize(lambda x: x >= 3)
            .hasMin("b", lambda x: x == 1)
            .isComplete("a")
            .isUnique("a")
            .isContainedIn("a", ["foo", "bar", "baz"])
            .isNonNegative("b")
        )
        .run()
    )
    return VerificationResult.checkResultsAsDataFrame(spark, check_result)


def main() -> None:
    """Run the VerificationSuite demo."""
    spark = (
        SparkSession.builder.appName("deequ-verification")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(
        [Row(a="foo", b=1, c=5), Row(a="bar", b=2, c=6), Row(a="baz", b=3, c=None)]
    )

    result_df = run_verification(spark, df)
    result_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
