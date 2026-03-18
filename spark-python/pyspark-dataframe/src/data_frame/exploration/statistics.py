"""
Descriptive statistics: describe(), summary() with extended percentiles,
and per-column min/max/mean for numeric columns.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType

from data_frame.sample_data import customer_orders
from data_frame.spark_utils import get_spark

_NUMERIC_TYPES = (IntegerType, LongType, DoubleType)


def numeric_columns(df):
    return [f.name for f in df.schema.fields if isinstance(f.dataType, _NUMERIC_TYPES)]


def main(spark) -> None:
    df = spark.createDataFrame(*customer_orders())

    print("=== describe() — count / mean / stddev / min / max ===")
    df.describe().show(truncate=False)

    print("=== summary() — adds 25%/50%/75% percentiles ===")
    df.summary().show(truncate=False)

    num_cols = numeric_columns(df)
    print(f"\n=== Custom per-column stats for: {num_cols} ===")
    aggs = []
    for col in num_cols:
        aggs += [
            F.min(col).alias(f"{col}_min"),
            F.max(col).alias(f"{col}_max"),
            F.round(F.avg(col), 2).alias(f"{col}_mean"),
            F.round(F.stddev(col), 2).alias(f"{col}_stddev"),
        ]
    df.agg(*aggs).show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("explore-statistics")
    main(spark)
    spark.stop()
