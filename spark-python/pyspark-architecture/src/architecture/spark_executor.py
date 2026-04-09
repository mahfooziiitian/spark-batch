import os

from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel


def demo_partitioning(spark: SparkSession) -> None:
    """Show how data is distributed across Executor partitions."""
    df = spark.range(0, 20, numPartitions=4)

    def tag_partition(idx: int, rows):
        for row in rows:
            yield (idx, int(row["id"]))

    tagged = df.rdd.mapPartitionsWithIndex(tag_partition)
    for partition_id, value in tagged.collect():
        print(f"  Partition {partition_id}: id={value}")


def demo_caching(spark: SparkSession) -> None:
    """Demonstrate Executor-side caching with different storage levels."""
    df = spark.range(0, 10_000)

    # MEMORY_AND_DISK — spills to disk when Executor memory is insufficient
    df.persist(StorageLevel.MEMORY_AND_DISK)
    print(f"Count (materialise): {df.count()}")
    print(f"Count (from cache):  {df.count()}")
    df.unpersist()


def demo_repartition(spark: SparkSession) -> None:
    """repartition causes a full shuffle; coalesce avoids it when reducing partitions."""
    df = spark.range(0, 100, numPartitions=2)
    print(f"Original:           {df.rdd.getNumPartitions()} partitions")

    repartitioned = df.repartition(8)
    print(f"After repartition:  {repartitioned.rdd.getNumPartitions()} partitions")

    coalesced = repartitioned.coalesce(2)
    print(f"After coalesce:     {coalesced.rdd.getNumPartitions()} partitions")


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("executor-demo")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=== Partitioning ===")
    demo_partitioning(spark)
    print("\n=== Caching ===")
    demo_caching(spark)
    print("\n=== Repartition / Coalesce ===")
    demo_repartition(spark)
    spark.stop()
