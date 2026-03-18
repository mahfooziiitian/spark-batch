"""
Basic DataFrame inspection: shape, schema, preview, and column listing.
The starting point for every data exploration session.
"""

from pyspark.sql import functions as F

from data_frame.sample_data import customer_orders
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    df = spark.createDataFrame(*customer_orders())

    print("=== Schema ===")
    df.printSchema()

    print(f"=== Shape: {df.count()} rows × {len(df.columns)} columns ===")

    print("\n=== First 5 rows ===")
    df.show(5, truncate=False)

    print("\n=== Column names ===")
    print(df.columns)

    print("\n=== Data types ===")
    for name, dtype in df.dtypes:
        print(f"  {name:20s} {dtype}")

    print("\n=== Distinct row count ===")
    print(f"  Total rows:   {df.count()}")
    print(f"  Distinct rows:{df.distinct().count()}")


if __name__ == "__main__":
    spark = get_spark("explore-inspect")
    main(spark)
    spark.stop()
