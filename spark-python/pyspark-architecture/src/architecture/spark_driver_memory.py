import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def demo_driver_memory_config(spark: SparkSession) -> None:
    """Inspect the Driver's memory configuration at runtime."""
    configs = {
        "spark.driver.memory": spark.conf.get("spark.driver.memory", "1g"),
        "spark.driver.memoryOverhead": spark.conf.get("spark.driver.memoryOverhead", "(default)"),
        "spark.driver.maxResultSize": spark.conf.get("spark.driver.maxResultSize", "1g"),
        "spark.broadcast.blockSize": spark.conf.get("spark.broadcast.blockSize", "4m"),
    }
    print("Driver memory configuration:")
    for key, value in configs.items():
        print(f"  {key:40s} = {value}")


def demo_broadcast_memory_impact(spark: SparkSession) -> None:
    """Broadcast variables are serialised on the Driver before shipping to Executors.

    The Driver must hold the entire broadcast value in heap memory during
    serialisation.  Large broadcasts can cause Driver OOM.
    """
    # Small lookup table — safe to broadcast
    small_lookup = {i: f"region_{i}" for i in range(100)}
    bc_small = spark.sparkContext.broadcast(small_lookup)
    print(f"Small broadcast: {len(bc_small.value)} entries")
    print(f"  Estimated size: {sys.getsizeof(small_lookup)} bytes (Python obj)")
    bc_small.destroy()

    # Demonstrate broadcast join — Driver serialises the small side
    large = spark.range(0, 100_000).withColumn("key", (F.col("id") % 100).cast("int"))
    small = spark.createDataFrame(
        [(i, f"region_{i}", float(i * 1000)) for i in range(100)],
        ["key", "region", "budget"],
    )
    joined = large.join(F.broadcast(small), on="key")
    print(f"Broadcast join result: {joined.count()} rows")

    # Show the plan to confirm BroadcastHashJoin
    plan = joined._jdf.queryExecution().executedPlan().toString()
    has_broadcast = "Broadcast" in plan
    print(f"  Plan uses BroadcastHashJoin: {has_broadcast}")


def demo_collect_memory_pressure(spark: SparkSession) -> None:
    """collect() pulls all data from Executors into Driver memory.

    This is the most common cause of Driver OOM in production.
    Always limit or filter before collecting.
    """
    df = spark.range(0, 100_000).withColumn("payload", F.concat(F.lit("data_"), F.col("id").cast("string")))

    # SAFE: collect a small filtered subset
    small_result = df.filter(F.col("id") < 10).collect()
    print(f"Safe collect: {len(small_result)} rows")

    # SAFE: use .first() or .head() for single-row inspection
    first_row = df.first()
    assert first_row is not None  # nosec B101
    print(f"First row: id={first_row['id']}")

    head_rows = df.head(5)
    print(f"Head 5 rows: {len(head_rows)} rows")

    # SAFE: use .take() for a bounded number of rows
    sample = df.take(3)
    print(f"Take 3: {len(sample)} rows")

    # SAFE: aggregate on Executors, collect only the summary
    summary = df.agg(
        F.count("id").alias("total_rows"),
        F.min("id").alias("min_id"),
        F.max("id").alias("max_id"),
    ).collect()[0]
    print(f"Aggregated on Executors: rows={summary['total_rows']}, min={summary['min_id']}, max={summary['max_id']}")


def demo_max_result_size(spark: SparkSession) -> None:
    """spark.driver.maxResultSize caps the total serialised result per action.

    When exceeded, Spark aborts the job instead of crashing the Driver with OOM.
    """
    max_result = spark.conf.get("spark.driver.maxResultSize", "1g")
    print(f"spark.driver.maxResultSize = {max_result}")
    print("  This caps the total serialised bytes collected from all partitions.")
    print("  Exceeding it raises: 'Total size of serialized results … is bigger than maxResultSize'")

    # Show how to check result size before collecting
    df = spark.range(0, 10_000).withColumn("data", F.concat(F.lit("x" * 100), F.col("id").cast("string")))

    # Estimate: count rows and average row size
    stats = df.agg(
        F.count("id").alias("rows"),
        F.avg(F.length("data")).alias("avg_data_len"),
    ).first()
    assert stats is not None  # nosec B101
    estimated_bytes = (stats["rows"] or 0) * (stats["avg_data_len"] or 0)
    print(
        f"  Estimated collect size: ~{estimated_bytes / 1024:.0f} KB "
        f"({stats['rows']} rows x {stats['avg_data_len']:.0f} bytes avg)"
    )


def demo_accumulator_on_driver(spark: SparkSession) -> None:
    """Accumulators aggregate values from Executors back to the Driver.

    The Driver holds the accumulated value in memory.  Safe for counters
    and simple metrics; avoid accumulating large data structures.
    """
    row_counter = spark.sparkContext.accumulator(0)
    error_counter = spark.sparkContext.accumulator(0)

    rdd = spark.sparkContext.parallelize(range(1, 1001), numSlices=4)

    def process_record(x: int) -> int:
        row_counter.add(1)
        if x % 100 == 0:
            error_counter.add(1)
        return x * 2

    results = rdd.map(process_record)
    total = results.sum()

    # Only the Driver can read accumulator values
    print(f"Processed:    {row_counter.value} records")
    print(f"Errors found: {error_counter.value}")
    print(f"Total sum:    {total}")


def demo_topandas_safe(spark: SparkSession) -> None:
    """toPandas() transfers an entire DataFrame to Driver memory as a Pandas DataFrame.

    Always filter/limit before calling toPandas() in production.
    Requires pandas to be installed.
    """
    try:
        import pandas  # noqa: F401
    except ImportError:
        print("pandas not installed — skipping toPandas demo")
        return

    df = spark.range(0, 50_000).withColumn("value", F.rand())

    # Check Arrow optimisation status
    arrow_enabled = spark.conf.get("spark.sql.execution.arrow.pyspark.enabled", "false")
    print(f"Arrow enabled: {arrow_enabled}")

    # SAFE: limit before converting
    pdf = df.limit(100).toPandas()
    assert pdf is not None  # nosec B101
    print(f"toPandas() with limit: {len(pdf)} rows, columns={list(pdf.columns)}")
    print(f"  Pandas memory usage: {pdf.memory_usage(deep=True).sum()} bytes")


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("driver-memory-demo")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=== Driver Memory Configuration ===")
    demo_driver_memory_config(spark)
    print("\n=== Broadcast Memory Impact ===")
    demo_broadcast_memory_impact(spark)
    print("\n=== Collect Memory Pressure ===")
    demo_collect_memory_pressure(spark)
    print("\n=== Max Result Size ===")
    demo_max_result_size(spark)
    print("\n=== Accumulators on Driver ===")
    demo_accumulator_on_driver(spark)
    print("\n=== Safe toPandas() ===")
    demo_topandas_safe(spark)
    spark.stop()
