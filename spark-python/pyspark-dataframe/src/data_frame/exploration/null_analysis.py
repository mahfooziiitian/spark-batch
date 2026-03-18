"""
Null analysis: count nulls per column, null percentage, and rows with
any null — essential before deciding on imputation or filtering strategy.
"""

from pyspark.sql import functions as F

from data_frame.sample_data import customer_orders
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    df = spark.createDataFrame(*customer_orders())
    total = df.count()

    print(f"=== Null counts (total rows: {total}) ===")
    aggs = [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
    null_row = df.agg(*aggs).collect()[0]

    for col in df.columns:
        n = null_row[col]
        pct = round(n / total * 100, 1)
        bar = "█" * int(pct / 5)
        print(f"  {col:20s} {n:3d} nulls  ({pct:5.1f}%)  {bar}")

    print("\n=== Rows with at least one null ===")
    any_null_condition = (
        F.greatest(*[F.col(c).isNull().cast("int") for c in df.columns]) > 0
    )
    any_null = df.filter(any_null_condition)
    print(f"  {any_null.count()} rows contain at least one null")
    any_null.show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("explore-nulls")
    main(spark)
    spark.stop()
