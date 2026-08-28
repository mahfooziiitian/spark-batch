"""Batch read — register and read from the in-memory `simple` data source.

Key concepts:
    - Registering a Python DataSource via spark.dataSource.register()
    - Reading with spark.read.format(...).option(...).load()
    - Partitioned generation controlled by `numRows` / `numPartitions` options
"""

from __future__ import annotations

from custom_ds import SimpleDataSource, create_spark_session

if __name__ == "__main__":
    spark = create_spark_session("simple-batch-read")

    spark.dataSource.register(SimpleDataSource)

    df = spark.read.format("simple").option("numRows", 20).option("numPartitions", 4).load()

    print(f"partitions: {df.rdd.getNumPartitions()}")
    df.show(10)
    print(f"row count: {df.count()}")

    spark.stop()
