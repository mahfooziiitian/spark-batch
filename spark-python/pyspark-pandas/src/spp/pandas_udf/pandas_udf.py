"""Pandas UDFs — vectorized user-defined functions.

Demonstrates three pandas UDF flavours:
  1. Series → Series  (element-wise transform)
  2. Series → scalar  (grouped aggregate)
  3. Iterator[Series] → Iterator[Series]  (batched transform)
"""

from typing import Iterator

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, StringType

from spp.session import create_spark_session


def main(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            (1, "alice", 80.5),
            (1, "bob", 92.0),
            (2, "carol", 75.0),
            (2, "dave", 88.5),
            (2, "eve", 95.0),
        ],
        ["group_id", "name", "score"],
    )

    # --- 1. Series → Series (element-wise) ---
    @pandas_udf(StringType())
    def upper_name(s: pd.Series) -> pd.Series:
        return s.str.upper()

    print("=== Series → Series UDF ===")
    df.withColumn("upper", upper_name("name")).show()

    # --- 2. Series → scalar (grouped aggregate) ---
    @pandas_udf(DoubleType())
    def mean_score(v: pd.Series) -> float:
        return v.mean()

    print("=== Grouped Aggregate UDF ===")
    df.groupBy("group_id").agg(
        mean_score("score").alias("avg_score"),
    ).show()

    # --- 3. Iterator[Series] → Iterator[Series] (batched) ---
    @pandas_udf(DoubleType())
    def normalize(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
        for batch in batch_iter:
            yield (batch - batch.mean()) / batch.std()

    print("=== Iterator UDF (normalize) ===")
    df.withColumn("norm_score", normalize("score")).show()


if __name__ == "__main__":
    spark = create_spark_session("pandas-udf-examples")
    main(spark)
    spark.stop()
