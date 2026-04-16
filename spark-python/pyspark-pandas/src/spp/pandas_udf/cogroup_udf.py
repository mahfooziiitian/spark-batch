"""CoGrouped Map UDF — ``cogroup().applyInPandas``.

Joins two DataFrames by key and applies a function that receives both
groups as ``pd.DataFrame`` objects.  Useful for custom merge/diff logic
that is hard to express with Spark SQL joins.

Usage::

    from spp.pandas_udf.cogroup_udf import merge_scores
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


def merge_scores(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    """Merge two score tables and compute the difference."""
    merged = pd.merge(left, right, on="student", suffixes=("_mid", "_final"))
    merged["improvement"] = merged["score_final"] - merged["score_mid"]
    return merged[["student", "score_mid", "score_final", "improvement"]]


MERGE_SCHEMA = StructType(
    [
        StructField("student", StringType()),
        StructField("score_mid", DoubleType()),
        StructField("score_final", DoubleType()),
        StructField("improvement", DoubleType()),
    ]
)


def asof_match(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    """Match each trade to the nearest prior quote (as-of join)."""
    left = left.sort_values("ts")
    right = right.sort_values("ts")
    result = pd.merge_asof(left, right, on="ts", suffixes=("_trade", "_quote"))
    return result[["ts", "price_trade", "price_quote"]]


ASOF_SCHEMA = StructType(
    [
        StructField("ts", IntegerType()),
        StructField("price_trade", DoubleType()),
        StructField("price_quote", DoubleType()),
    ]
)


def main(spark: SparkSession) -> None:
    midterm = spark.createDataFrame(
        [("alice", 80.0), ("bob", 70.0), ("carol", 90.0)],
        ["student", "score"],
    )
    final = spark.createDataFrame(
        [("alice", 85.0), ("bob", 82.0), ("carol", 88.0)],
        ["student", "score"],
    )

    print("=== CoGrouped applyInPandas — score merge ===")
    midterm.groupBy("student").cogroup(final.groupBy("student")).applyInPandas(
        merge_scores, schema=MERGE_SCHEMA
    ).show()

    trades = spark.createDataFrame(
        [(1, 10, 100.0), (1, 30, 102.0)], ["sym", "ts", "price"]
    )
    quotes = spark.createDataFrame(
        [(1, 5, 99.5), (1, 15, 100.5), (1, 25, 101.0)], ["sym", "ts", "price"]
    )

    print("=== CoGrouped applyInPandas — as-of join ===")
    trades.groupBy("sym").cogroup(quotes.groupBy("sym")).applyInPandas(
        asof_match, schema=ASOF_SCHEMA
    ).show()


if __name__ == "__main__":
    spark = create_spark_session("cogroup-udf")
    main(spark)
    spark.stop()
