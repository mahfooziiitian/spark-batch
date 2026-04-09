import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_dag(spark: SparkSession) -> None:
    """Demonstrates how the Driver builds a logical plan (DAG) from transformations.

    Transformations are lazy — no work happens until an action is called.
    The Driver sends compiled tasks to Executors when an action triggers execution.
    """
    df = spark.range(0, 1_000_000, numPartitions=4)

    # Transformations — build the DAG; no data movement yet
    transformed = (
        df.withColumn("squared", F.col("id") * F.col("id"))
        .filter(F.col("squared") % 3 == 0)
        .withColumn("label", F.when(F.col("id") % 2 == 0, "even").otherwise("odd"))
    )

    # Action — triggers DAG execution on Executors
    result = transformed.agg(
        F.count("id").alias("count"),
        F.sum("squared").alias("total_squared"),
    )
    result.show()


def demo_broadcast(spark: SparkSession) -> None:
    """Broadcast variables avoid shipping large lookup tables with every task."""
    lookup = {1: "Alice", 2: "Bob", 3: "Charlie"}
    bc = spark.sparkContext.broadcast(lookup)

    df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    result = df.rdd.map(lambda row: (row["id"], bc.value.get(row["id"], "Unknown")))
    print(result.collect())
    bc.destroy()


def demo_accumulator(spark: SparkSession) -> None:
    """Accumulators let Executors report metrics back to the Driver."""
    counter = spark.sparkContext.accumulator(0)
    rdd = spark.sparkContext.parallelize(range(1, 101))
    rdd.foreach(lambda x: counter.add(1))
    print(f"Processed {counter.value} records")  # 100


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("driver-demo")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=== DAG / Lazy Evaluation ===")
    build_dag(spark)
    print("\n=== Broadcast Variable ===")
    demo_broadcast(spark)
    print("\n=== Accumulator ===")
    demo_accumulator(spark)
    spark.stop()
