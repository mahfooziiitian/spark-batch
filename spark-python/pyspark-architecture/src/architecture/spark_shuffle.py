import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def demo_narrow_vs_wide(spark: SparkSession) -> None:
    """Narrow transformations (map, filter) need no shuffle.
    Wide transformations (groupBy, join, repartition) trigger a shuffle.
    """
    df = spark.range(0, 100, numPartitions=4)

    # Narrow — each partition transforms independently, no data exchange
    narrow = df.filter(F.col("id") % 2 == 0).withColumn("doubled", F.col("id") * 2)
    print(f"Narrow — partitions before: {df.rdd.getNumPartitions()}, after: {narrow.rdd.getNumPartitions()}")

    # Wide — groupBy forces a shuffle to co-locate keys
    wide = df.withColumn("bucket", F.col("id") % 5).groupBy("bucket").count()
    print(f"Wide   — partitions after groupBy: {wide.rdd.getNumPartitions()}")

    print("\n--- Narrow plan (no Exchange) ---")
    narrow.explain()
    print("--- Wide plan (Exchange / shuffle) ---")
    wide.explain()


def demo_join_strategies(spark: SparkSession) -> None:
    """Spark chooses a join strategy based on table size and hints."""
    large = spark.range(0, 10_000).withColumn("key", (F.col("id") % 100).cast("int"))
    small = spark.createDataFrame([(i, f"val_{i}") for i in range(100)], ["key", "value"])

    # Broadcast hash join — small table is broadcast to all executors
    broadcast_join = large.join(F.broadcast(small), on="key")
    print("--- Broadcast Hash Join ---")
    broadcast_join.explain()
    print(f"Broadcast join count: {broadcast_join.count()}")

    # Sort-merge join — both sides are shuffled and sorted by key
    large2 = spark.range(0, 10_000).withColumn("key", (F.col("id") % 100).cast("int"))
    medium = spark.range(0, 5_000).withColumn("key", (F.col("id") % 100).cast("int"))
    smj = large2.hint("merge").join(medium, on="key")
    print("\n--- Sort-Merge Join ---")
    smj.explain()
    print(f"Sort-merge join count: {smj.count()}")


def demo_shuffle_partitions(spark: SparkSession) -> None:
    """spark.sql.shuffle.partitions controls the output partition count after a shuffle."""
    df = spark.range(0, 1000).withColumn("key", (F.col("id") % 10).cast("int"))

    current = spark.conf.get("spark.sql.shuffle.partitions")
    print(f"shuffle.partitions = {current}")

    grouped = df.groupBy("key").agg(F.count("id").alias("cnt"))
    print(f"Partitions after groupBy: {grouped.rdd.getNumPartitions()}")

    # Temporarily reduce shuffle partitions
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    grouped2 = df.groupBy("key").agg(F.count("id").alias("cnt"))
    print(f"Partitions with shuffle.partitions=2: {grouped2.rdd.getNumPartitions()}")

    spark.conf.set("spark.sql.shuffle.partitions", current or "4")


def demo_skew_handling(spark: SparkSession) -> None:
    """Demonstrate how data skew concentrates work on a few partitions."""
    # Skewed data — key 0 has 90% of the rows
    skewed_data = [(0, "skewed")] * 900 + [(i, f"val_{i}") for i in range(1, 101)]
    df = spark.createDataFrame(skewed_data, ["key", "value"])

    per_partition = df.withColumn("partition_id", F.spark_partition_id()).groupBy("partition_id").count()

    print("--- Partition distribution (before repartition) ---")
    per_partition.show()

    # Salt the skewed key to spread it across partitions
    salted = df.withColumn("salt", (F.rand() * 4).cast("int"))
    salted_agg = (
        salted.groupBy("key", "salt")
        .agg(F.count("value").alias("partial_count"))
        .groupBy("key")
        .agg(F.sum("partial_count").alias("total_count"))
    )

    print("--- Salted aggregation result ---")
    salted_agg.orderBy("key").show(5)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("shuffle-demo")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=== Narrow vs Wide Transformations ===")
    demo_narrow_vs_wide(spark)
    print("\n=== Join Strategies ===")
    demo_join_strategies(spark)
    print("\n=== Shuffle Partitions ===")
    demo_shuffle_partitions(spark)
    print("\n=== Skew Handling ===")
    demo_skew_handling(spark)
    spark.stop()
