"""Per-group feature engineering with ``applyInPandas``.

Demonstrates rolling windows, lag features, and cumulative statistics
computed within each group using pandas, distributed across the Spark
cluster via ``groupBy().applyInPandas()``.

Usage::

    python src/spp/integration/group_feature_engineering.py
"""

import pandas as pd
from pyspark.sql import SparkSession

from spp.session import create_spark_session


def engineer_features(pdf: pd.DataFrame) -> pd.DataFrame:
    """Add rolling mean, lag, and cumulative sum features per entity."""
    pdf = pdf.sort_values("ts").copy()
    pdf["rolling_mean_3"] = pdf["value"].rolling(window=3, min_periods=1).mean()
    pdf["lag_1"] = pdf["value"].shift(1)
    pdf["cumsum"] = pdf["value"].cumsum()
    return pdf


FEATURE_SCHEMA = (
    "entity_id: long, ts: long, value: double, "
    "rolling_mean_3: double, lag_1: double, cumsum: double"
)


def categorise_values(pdf: pd.DataFrame) -> pd.DataFrame:
    """Bin ``value`` into low / medium / high per entity."""
    pdf = pdf.copy()
    pdf["category"] = pd.cut(
        pdf["value"],
        bins=[float("-inf"), 10, 50, float("inf")],
        labels=["low", "medium", "high"],
    ).astype(str)
    return pdf


CATEGORY_SCHEMA = "entity_id: long, ts: long, value: double, category: string"


def pct_change_features(pdf: pd.DataFrame) -> pd.DataFrame:
    """Add period-over-period percentage change per entity."""
    pdf = pdf.sort_values("ts").copy()
    pdf["pct_change"] = pdf["value"].pct_change().fillna(0.0)
    return pdf


PCT_SCHEMA = "entity_id: long, ts: long, value: double, pct_change: double"


def main(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            (1, 1, 10.0), (1, 2, 20.0), (1, 3, 30.0), (1, 4, 25.0),
            (2, 1, 5.0), (2, 2, 15.0), (2, 3, 10.0), (2, 4, 20.0),
        ],
        ["entity_id", "ts", "value"],
    )

    print("=== Rolling / lag / cumsum features ===")
    df.groupBy("entity_id").applyInPandas(
        engineer_features, schema=FEATURE_SCHEMA
    ).show()

    print("=== Categorise values ===")
    df.groupBy("entity_id").applyInPandas(
        categorise_values, schema=CATEGORY_SCHEMA
    ).show()

    print("=== Percentage change ===")
    df.groupBy("entity_id").applyInPandas(
        pct_change_features, schema=PCT_SCHEMA
    ).show()


if __name__ == "__main__":
    spark = create_spark_session("group-feature-engineering")
    main(spark)
    spark.stop()
