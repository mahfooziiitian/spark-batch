"""VerificationSuite example — validate data against explicit constraints."""

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import DataFrame

from dpu.sample_data import create_retail_df, create_sample_df
from dpu.spark_session import create_spark


def verify_basic_constraints(spark, df: DataFrame) -> DataFrame:
    """Verify a set of basic constraints on the sample DataFrame.

    Args:
        spark: Active SparkSession.
        df: Input DataFrame to verify.

    Returns:
        DataFrame containing per-check verification results.
    """
    check = Check(spark, CheckLevel.Warning, "Basic quality checks")

    result = (
        VerificationSuite(spark)
        .onData(df)
        .addCheck(
            check.hasSize(lambda x: x >= 3)
            .isComplete("a")
            .isUnique("a")
            .isComplete("b")
            .isNonNegative("b")
            .isContainedIn("a", ["foo", "bar", "baz"])
        )
        .run()
    )
    return VerificationResult.checkResultsAsDataFrame(spark, result)


def verify_retail_constraints(spark, df: DataFrame) -> DataFrame:
    """Verify pricing and inventory constraints on the retail DataFrame.

    Args:
        spark: Active SparkSession.
        df: Retail DataFrame with price, quantity, category columns.

    Returns:
        DataFrame containing per-check verification results.
    """
    pricing_check = Check(spark, CheckLevel.Error, "Pricing checks")
    inventory_check = Check(spark, CheckLevel.Warning, "Inventory checks")

    result = (
        VerificationSuite(spark)
        .onData(df)
        .addCheck(
            pricing_check.isNonNegative("price").isContainedIn("category", ["electronics", "accessories", "hardware"])
        )
        .addCheck(inventory_check.isNonNegative("quantity").isComplete("region").hasSize(lambda x: x >= 5))
        .run()
    )
    return VerificationResult.checkResultsAsDataFrame(spark, result)


def main() -> None:
    """Run the verification demo."""
    spark = create_spark("deequ-verification")

    print("=== Basic constraint verification ===")
    basic_df = create_sample_df(spark)
    verify_basic_constraints(spark, basic_df).show(truncate=False)

    print("=== Retail constraint verification ===")
    retail_df = create_retail_df(spark)
    verify_retail_constraints(spark, retail_df).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
