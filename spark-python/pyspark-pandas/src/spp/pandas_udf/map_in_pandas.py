"""``mapInPandas`` — general-purpose batch-wise transformation.

Processes each partition of a Spark DataFrame as an iterator of
``pd.DataFrame`` batches, yielding transformed batches back.  Useful for
complex row-wise transformations that benefit from pandas methods.

Available since Spark 3.0.0.

Usage::

    from spp.pandas_udf.map_in_pandas import add_double_age, add_bmi
"""

from typing import Iterator

import pandas as pd
from pyspark.sql import SparkSession

from spp.session import create_spark_session


def add_double_age(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Add a ``double_age`` column equal to ``age * 2``."""
    for pdf in iterator:
        pdf["double_age"] = pdf["age"] * 2
        yield pdf


def add_bmi(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Compute BMI from ``weight_kg`` and ``height_m`` columns."""
    for pdf in iterator:
        pdf["bmi"] = (pdf["weight_kg"] / (pdf["height_m"] ** 2)).round(1)
        yield pdf


def clean_text(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Lowercase and strip whitespace from the ``text`` column."""
    for pdf in iterator:
        pdf["text_clean"] = pdf["text"].str.lower().str.strip()
        yield pdf


def filter_positive(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Keep only rows where ``value`` is positive."""
    for pdf in iterator:
        yield pdf[pdf["value"] > 0]


def main(spark: SparkSession) -> None:
    print("=== mapInPandas — double age ===")
    df = spark.createDataFrame(
        [(1, 21), (2, 30), (3, 25), (4, 35)],
        ["id", "age"],
    )
    df.mapInPandas(
        add_double_age, schema="id: bigint, age: bigint, double_age: bigint"
    ).show()

    print("=== mapInPandas — BMI calculation ===")
    df2 = spark.createDataFrame(
        [(1, 70.0, 1.75), (2, 85.0, 1.80), (3, 60.0, 1.65)],
        ["id", "weight_kg", "height_m"],
    )
    df2.mapInPandas(
        add_bmi,
        schema="id: bigint, weight_kg: double, height_m: double, bmi: double",
    ).show()

    print("=== mapInPandas — text cleaning ===")
    df3 = spark.createDataFrame(
        [(1, "  Hello WORLD  "), (2, " PySpark  "), (3, "  pAnDaS ")],
        ["id", "text"],
    )
    df3.mapInPandas(
        clean_text, schema="id: bigint, text: string, text_clean: string"
    ).show(truncate=False)

    print("=== mapInPandas — filter positive ===")
    df4 = spark.createDataFrame(
        [(1, -5.0), (2, 3.0), (3, -1.0), (4, 7.0)],
        ["id", "value"],
    )
    df4.mapInPandas(
        filter_positive, schema="id: bigint, value: double"
    ).show()


if __name__ == "__main__":
    spark = create_spark_session("map-in-pandas")
    main(spark)
    spark.stop()
