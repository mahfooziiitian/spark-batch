"""Per-group ML preprocessing with ``applyInPandas``.

Demonstrates common ML preprocessing steps — missing value imputation,
outlier removal, and min-max scaling — applied per group using
``groupBy().applyInPandas()``.

Usage::

    python src/spp/integration/ml_preprocessing.py
"""

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

from spp.session import create_spark_session


def fill_missing(pdf: pd.DataFrame) -> pd.DataFrame:
    """Impute missing numeric values with the group median."""
    pdf = pdf.copy()
    for col in ["feature_a", "feature_b"]:
        pdf[col] = pdf[col].fillna(pdf[col].median())
    return pdf


FILL_SCHEMA = "group_id: long, feature_a: double, feature_b: double"


def remove_outliers(pdf: pd.DataFrame) -> pd.DataFrame:
    """Remove rows where ``feature_a`` is beyond 2 std from the group mean."""
    mean = pdf["feature_a"].mean()
    std = pdf["feature_a"].std() or 1.0
    z_scores = (pdf["feature_a"] - mean) / std
    return pdf[z_scores.abs() < 2]


OUTLIER_SCHEMA = "group_id: long, feature_a: double, feature_b: double"


def min_max_scale(pdf: pd.DataFrame) -> pd.DataFrame:
    """Min-max scale ``feature_a`` within each group to [0, 1]."""
    pdf = pdf.copy()
    fmin = pdf["feature_a"].min()
    fmax = pdf["feature_a"].max()
    span = fmax - fmin or 1.0
    pdf["scaled_a"] = (pdf["feature_a"] - fmin) / span
    return pdf


SCALE_SCHEMA = "group_id: long, feature_a: double, feature_b: double, scaled_a: double"


def full_preprocess(pdf: pd.DataFrame) -> pd.DataFrame:
    """Combined pipeline: fill → remove outliers → scale."""
    pdf = pdf.copy()

    # 1. Fill missing
    for col in ["feature_a", "feature_b"]:
        pdf[col] = pdf[col].fillna(pdf[col].median())

    # 2. Remove outliers (3-sigma)
    mean = pdf["feature_a"].mean()
    std = pdf["feature_a"].std() or 1.0
    pdf = pdf[(pdf["feature_a"] - mean).abs() / std < 3]

    # 3. Min-max scale
    fmin = pdf["feature_a"].min()
    fmax = pdf["feature_a"].max()
    span = fmax - fmin or 1.0
    pdf["scaled_a"] = (pdf["feature_a"] - fmin) / span

    return pdf


PREPROCESS_SCHEMA = (
    "group_id: long, feature_a: double, feature_b: double, scaled_a: double"
)


def main(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            (1, 10.0, 1.0),
            (1, 20.0, 2.0),
            (1, None, 3.0),
            (1, 15.0, None),
            (1, 100.0, 5.0),  # potential outlier
            (2, 5.0, 10.0),
            (2, 8.0, 12.0),
            (2, None, 11.0),
            (2, 6.0, None),
        ],
        ["group_id", "feature_a", "feature_b"],
    )

    print("=== Fill missing (group median) ===")
    df.groupBy("group_id").applyInPandas(
        fill_missing, schema=FILL_SCHEMA
    ).show()

    print("=== Remove outliers (2-sigma) ===")
    df.na.drop().groupBy("group_id").applyInPandas(
        remove_outliers, schema=OUTLIER_SCHEMA
    ).show()

    print("=== Min-max scale per group ===")
    df.na.drop().groupBy("group_id").applyInPandas(
        min_max_scale, schema=SCALE_SCHEMA
    ).show()

    print("=== Full preprocessing pipeline ===")
    df.groupBy("group_id").applyInPandas(
        full_preprocess, schema=PREPROCESS_SCHEMA
    ).show()


if __name__ == "__main__":
    spark = create_spark_session("ml-preprocessing")
    main(spark)
    spark.stop()
