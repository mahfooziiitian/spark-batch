"""Grouped Map UDF — ``applyInPandas``.

Applies a function to each group of a ``groupBy`` result, receiving and
returning a full ``pd.DataFrame``.  Useful when the transformation needs
access to multiple columns at once (e.g. normalisation within a group).

Usage::

    from spp.pandas_udf.grouped_map_udf import normalize_within_group
"""

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from spp.session import create_spark_session


def normalize_within_group(pdf: pd.DataFrame) -> pd.DataFrame:
    """Z-score normalise ``score`` within each group."""
    pdf = pdf.copy()
    mean = pdf["score"].mean()
    std = pdf["score"].std() or 1.0
    pdf["norm_score"] = (pdf["score"] - mean) / std
    return pdf


# Schema returned by the grouped-map function
NORMALIZE_SCHEMA = StructType(
    [
        StructField("group_id", IntegerType()),
        StructField("name", StringType()),
        StructField("score", DoubleType()),
        StructField("norm_score", DoubleType()),
    ]
)


def subtract_group_mean(pdf: pd.DataFrame) -> pd.DataFrame:
    """Subtract the group mean from every score (centering)."""
    pdf = pdf.copy()
    pdf["centered"] = pdf["score"] - pdf["score"].mean()
    return pdf


CENTERED_SCHEMA = StructType(
    [
        StructField("group_id", IntegerType()),
        StructField("name", StringType()),
        StructField("score", DoubleType()),
        StructField("centered", DoubleType()),
    ]
)


def main(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            (1, "alice", 80.0),
            (1, "bob", 90.0),
            (1, "carol", 100.0),
            (2, "dave", 60.0),
            (2, "eve", 80.0),
        ],
        ["group_id", "name", "score"],
    )

    print("=== Normalize within group (applyInPandas) ===")
    df.groupBy("group_id").applyInPandas(
        normalize_within_group, schema=NORMALIZE_SCHEMA
    ).show()

    print("=== Center within group (applyInPandas) ===")
    df.groupBy("group_id").applyInPandas(
        subtract_group_mean, schema=CENTERED_SCHEMA
    ).show()


if __name__ == "__main__":
    spark = create_spark_session("grouped-map-udf")
    main(spark)
    spark.stop()
