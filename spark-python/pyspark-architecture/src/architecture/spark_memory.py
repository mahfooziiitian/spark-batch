import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


def demo_unified_memory(spark: SparkSession) -> None:
    """Spark's unified memory manager splits the JVM heap into execution and storage pools.

    Execution memory: joins, sorts, shuffles, aggregations.
    Storage memory:   cached DataFrames and broadcast variables.
    Both pools can borrow from each other when one is underutilised.
    """
    fraction = spark.conf.get("spark.memory.fraction", "0.6")
    storage_fraction = spark.conf.get("spark.memory.storageFraction", "0.5")
    print(f"spark.memory.fraction        = {fraction}")
    print(f"spark.memory.storageFraction  = {storage_fraction}")
    print(f"Execution pool = heap x {fraction} x (1 - {storage_fraction})")
    print(f"Storage pool   = heap x {fraction} x {storage_fraction}")


def demo_storage_levels(spark: SparkSession) -> None:
    """Compare all available storage levels and their trade-offs."""
    df = spark.range(0, 50_000)

    levels = {
        "MEMORY_ONLY": StorageLevel.MEMORY_ONLY,
        "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK,
        "MEMORY_AND_DISK_2": StorageLevel.MEMORY_AND_DISK_2,
        "DISK_ONLY": StorageLevel.DISK_ONLY,
    }

    for name, level in levels.items():
        cached = df.persist(level)
        cached.count()
        print(f"{name:20s}  storageLevel={cached.storageLevel}")
        cached.unpersist(blocking=True)


def demo_cache_vs_checkpoint(spark: SparkSession) -> None:
    """cache() keeps the lineage; checkpoint() truncates it for fault tolerance."""
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoint")  # noqa: S108  # nosec B108

    df = spark.range(0, 1000).withColumn("doubled", F.col("id") * 2)

    # Cache — keeps the full lineage (fast recovery by recomputation)
    cached = df.cache()
    cached.count()
    debug_str = cached.rdd.toDebugString()
    print(f"Cached — lineage depth: {debug_str.count(b'|') if debug_str else 0}")
    cached.unpersist(blocking=True)

    # Checkpoint — writes to reliable storage and truncates lineage
    df.rdd.checkpoint()
    df.count()
    debug_str = df.rdd.toDebugString()
    print(f"Checkpointed — lineage depth: {debug_str.count(b'|') if debug_str else 0}")


def demo_serialization(spark: SparkSession) -> None:
    """Serialisation affects both shuffle size and cache footprint.

    Java serialiser: default, human-debuggable, slower.
    Kryo serialiser: compact, faster, requires class registration for best results.
    """
    serializer = spark.conf.get("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    print(f"Active serializer: {serializer}")

    rdd = spark.sparkContext.parallelize([(i, f"value_{i}") for i in range(1000)], numSlices=4)

    # Persist with MEMORY_AND_DISK (serialised) to compare footprint
    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    rdd.count()
    print(f"Serialised RDD partitions: {rdd.getNumPartitions()}")
    rdd.unpersist(blocking=True)


def demo_spill_metrics(spark: SparkSession) -> None:
    """When execution memory is exhausted, Spark spills data to disk.

    Spills are visible in the Spark UI and through accumulators.
    This demo creates a workload that may spill with low executor memory.
    """
    df = spark.range(0, 100_000, numPartitions=4)

    # A groupBy + sort forces both execution memory (sort) and shuffle
    result = (
        df.withColumn("key", (F.col("id") % 50).cast("int"))
        .withColumn("payload", F.concat(F.lit("data_"), F.col("id").cast("string")))
        .groupBy("key")
        .agg(
            F.count("id").alias("cnt"),
            F.collect_list("payload").alias("payloads"),
        )
        .orderBy(F.desc("cnt"))
    )

    print(f"Groups: {result.count()}")
    result.select("key", "cnt").show(5)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("memory-demo")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=== Unified Memory Model ===")
    demo_unified_memory(spark)
    print("\n=== Storage Levels ===")
    demo_storage_levels(spark)
    print("\n=== Cache vs Checkpoint ===")
    demo_cache_vs_checkpoint(spark)
    print("\n=== Serialization ===")
    demo_serialization(spark)
    print("\n=== Spill / Heavy Workload ===")
    demo_spill_metrics(spark)
    spark.stop()
