"""Example: Column-level transformations with chispa assertions.

Demonstrates cleaning string columns using column functions and
verifying results with chispa's assert_column_equality.

Run:
    PYTHONPATH=src uv run python examples/column_transforms.py
"""

import os

from chispa.column_comparer import assert_column_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from data_frame.columns.column_equality import (
    extract_email_domain,
    normalize_whitespace,
    remove_non_word_characters,
    title_case,
)


def main() -> None:
    """Demonstrate column transformation functions with chispa verification."""
    spark = (
        SparkSession.builder.appName("example-column-transforms")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Clean messy names
    names_df = spark.createDataFrame(
        [("jo&&se  doe", "jose doe"), ("**li**  chen", "li chen"), ("#luisa   m.", "luisa m.")],
        ["raw_name", "expected_clean"],
    )
    cleaned = names_df.withColumn(
        "cleaned", title_case(normalize_whitespace(remove_non_word_characters(F.col("raw_name"))))
    )
    print("=== Name Cleaning Pipeline ===")
    cleaned.show(truncate=False)

    # Extract email domains
    emails_df = spark.createDataFrame(
        [("alice@gmail.com", "gmail.com"), ("bob@work.org", "work.org"), ("not-an-email", None)],
        ["email", "expected_domain"],
    )
    with_domain = emails_df.withColumn("domain", extract_email_domain(F.col("email")))
    print("=== Email Domain Extraction ===")
    with_domain.show(truncate=False)

    # Verify with chispa
    assert_column_equality(with_domain, "domain", "expected_domain")
    print("✅ chispa assertion passed: domains match expected values")

    spark.stop()


if __name__ == "__main__":
    main()
