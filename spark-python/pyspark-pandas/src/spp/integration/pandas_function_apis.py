"""Pandas Function APIs — ``mapInPandas``, ``applyInPandas``, ``cogroup``.

PySpark 3.0+ provides three pandas function APIs that operate on entire
``pd.DataFrame`` batches rather than individual rows.  This module
demonstrates all three in a single runnable script.

Usage::

    python src/spp/integration/pandas_function_apis.py
"""

from typing import Iterator

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spp.session import create_spark_session


# ---------------------------------------------------------------------------
# mapInPandas — general-purpose per-batch transformation
# ---------------------------------------------------------------------------

def add_double_age(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Add ``double_age`` column to each batch."""
    for pdf in iterator:
        pdf["double_age"] = pdf["age"] * 2
        yield pdf


def demo_map_in_pandas(spark: SparkSession) -> None:
    """Demonstrate ``mapInPandas`` for row-wise transforms."""
    print("=== mapInPandas — add double_age ===")
    df = spark.createDataFrame(
        [(1, 21), (2, 30), (3, 25), (4, 35)],
        ["id", "age"],
    )
    result = df.mapInPandas(
        add_double_age,
        schema="id: bigint, age: bigint, double_age: bigint",
    )
    result.show()


# ---------------------------------------------------------------------------
# applyInPandas — grouped map (split-apply-combine)
# ---------------------------------------------------------------------------

def normalize_group(pdf: pd.DataFrame) -> pd.DataFrame:
    """Z-score normalise ``value`` within each group."""
    pdf = pdf.copy()
    std = pdf["value"].std() or 1.0
    pdf["normalized"] = (pdf["value"] - pdf["value"].mean()) / std
    return pdf


def mean_with_key(key: tuple, pdf: pd.DataFrame) -> pd.DataFrame:
    """Return the group mean, including the group key."""
    return pd.DataFrame([key + (pdf["value"].mean(),)])


def demo_apply_in_pandas(spark: SparkSession) -> None:
    """Demonstrate ``applyInPandas`` for per-group operations."""
    df = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (1, 3.0), (2, 4.0), (2, 5.0), (2, 10.0)],
        ["group_id", "value"],
    )

    print("=== applyInPandas — normalize within group ===")
    df.groupBy("group_id").applyInPandas(
        normalize_group,
        schema="group_id: long, value: double, normalized: double",
    ).show()

    print("=== applyInPandas — mean with group key ===")
    df.groupBy("group_id").applyInPandas(
        mean_with_key,
        schema="group_id: long, mean_value: double",
    ).show()


# ---------------------------------------------------------------------------
# cogroup().applyInPandas — joining two grouped datasets
# ---------------------------------------------------------------------------

def combine_groups(pdf1: pd.DataFrame, pdf2: pd.DataFrame) -> pd.DataFrame:
    """Left-merge two grouped DataFrames on ``id``."""
    return pdf1.merge(pdf2, on="id", how="left")


def demo_cogroup(spark: SparkSession) -> None:
    """Demonstrate ``cogroup().applyInPandas`` for cross-dataset logic."""
    df1 = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 4.0)],
        ["id", "value1"],
    )
    df2 = spark.createDataFrame(
        [(1, "A"), (2, "B"), (2, "C")],
        ["id", "value2"],
    )

    print("=== cogroup().applyInPandas — merge by key ===")
    df1.groupBy("id").cogroup(df2.groupBy("id")).applyInPandas(
        combine_groups,
        schema="id: long, value1: double, value2: string",
    ).show()


# ---------------------------------------------------------------------------

def main(spark: SparkSession) -> None:
    demo_map_in_pandas(spark)
    demo_apply_in_pandas(spark)
    demo_cogroup(spark)


if __name__ == "__main__":
    spark = create_spark_session("pandas-function-apis")
    main(spark)
    spark.stop()
