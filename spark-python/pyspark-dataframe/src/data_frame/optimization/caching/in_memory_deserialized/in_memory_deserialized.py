"""
Demonstrates DataFrame caching with different StorageLevel options.
Cache a DataFrame when it is reused across multiple actions to avoid recomputation.
"""

from pyspark import StorageLevel
from pyspark.sql import functions as F

from data_frame.sample_data import regional_revenue
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    df = spark.createDataFrame(*regional_revenue())

    # Default cache — MEMORY_AND_DISK (deserialized in JVM heap when space allows)
    df.cache()

    # First action materialises and stores the cached data
    total = df.agg(F.sum("revenue")).first()[0]
    print(f"Total revenue: {total}")

    # Second action reads from cache — no recomputation
    by_region = (
        df.groupBy("region")
        .agg(F.round(F.sum("revenue"), 2).alias("total"))
        .orderBy(F.desc("total"))
    )
    by_region.show(truncate=False)

    df.unpersist()

    # Explicit storage level — serialised on disk when memory is tight
    df2 = spark.createDataFrame(*regional_revenue())
    df2.persist(StorageLevel.MEMORY_AND_DISK_DESER)
    print(f"Row count (from persisted): {df2.count()}")
    df2.unpersist()


if __name__ == "__main__":
    spark = get_spark("caching-example")
    main(spark)
    spark.stop()
