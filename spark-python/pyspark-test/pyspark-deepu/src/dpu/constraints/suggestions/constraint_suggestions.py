"""ConstraintSuggestionRunner example — auto-suggest data quality rules."""

from pydeequ.suggestions import DEFAULT, ConstraintSuggestionRunner

from dpu.sample_data import create_retail_df, create_sample_df
from dpu.spark_session import create_spark


def suggest_constraints(spark, df) -> dict:
    """Run the ConstraintSuggestionRunner on a DataFrame.

    Args:
        spark: Active SparkSession.
        df: Input DataFrame to profile for constraint suggestions.

    Returns:
        dict containing the suggested constraints in JSON-like structure.
    """
    return ConstraintSuggestionRunner(spark).onData(df).addConstraintRule(DEFAULT()).run()


def main() -> None:
    """Run the constraint suggestion demo."""
    spark = create_spark("deequ-suggestions")

    print("=== Suggestions for basic sample ===")
    basic_df = create_sample_df(spark)
    basic_result = suggest_constraints(spark, basic_df)
    for suggestion in basic_result.get("constraint_suggestions", []):
        print(f"  - {suggestion.get('description', suggestion)}")

    print("\n=== Suggestions for retail data ===")
    retail_df = create_retail_df(spark)
    retail_result = suggest_constraints(spark, retail_df)
    for suggestion in retail_result.get("constraint_suggestions", []):
        print(f"  - {suggestion.get('description', suggestion)}")

    spark.stop()


if __name__ == "__main__":
    main()
