"""Demonstrate PyDeequ ConstraintSuggestionRunner for auto-suggesting constraints."""

import json
import os

os.environ["SPARK_VERSION"] = "3.5"

import pydeequ
from pydeequ.suggestions import DEFAULT, ConstraintSuggestionRunner
from pyspark.sql import DataFrame, Row, SparkSession


def run_suggestions(spark: SparkSession, df: DataFrame) -> dict:
    """Run constraint suggestion on the given DataFrame.

    Args:
        spark: Active SparkSession with Deequ JAR configured.
        df: Input DataFrame to suggest constraints for.

    Returns:
        Dict containing suggested constraints in JSON-like structure.
    """
    return ConstraintSuggestionRunner(spark).onData(df).addConstraintRule(DEFAULT()).run()


def main() -> None:
    """Run the ConstraintSuggestionRunner demo."""
    spark = (
        SparkSession.builder.appName("deequ-suggestions")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(
        [Row(a="foo", b=1, c=5), Row(a="bar", b=2, c=6), Row(a="baz", b=3, c=None)]
    )

    suggestion_result = run_suggestions(spark, df)
    print(json.dumps(suggestion_result, indent=2, default=str))

    spark.stop()


if __name__ == "__main__":
    main()
