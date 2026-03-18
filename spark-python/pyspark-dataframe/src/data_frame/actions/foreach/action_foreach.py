"""
foreach() and foreachPartition() apply a function to each row / partition
without returning a value — used for side-effecting operations such as
writing to external systems, publishing events, or accumulating metrics.

  foreach(f)             — f(Row) called once per row on an executor
  foreachPartition(f)    — f(Iterator[Row]) called once per partition
                           opens/closes connections per partition, not per row

foreachPartition is preferred over foreach when the function needs to
establish a connection (database, HTTP, file) because connections are
opened once per partition instead of once per row.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import Row

from data_frame.sample_data import customer_orders, product_revenue
from data_frame.spark_utils import get_spark


def demo_foreach(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    # ------------------------------------------------------------------
    # foreach(f) — f receives a single Row
    # Use an Accumulator to count from executors back to the driver
    # ------------------------------------------------------------------
    active_counter = spark.sparkContext.accumulator(0)  # (1)

    def count_active(row: Row) -> None:
        if row["status"] == "active":
            active_counter.add(1)

    df.foreach(count_active)
    print(f"foreach — active orders counted via accumulator: {active_counter.value}")


def demo_foreach_print(spark: SparkSession) -> None:
    df = spark.createDataFrame(*product_revenue()).limit(4)

    # ------------------------------------------------------------------
    # Simple foreach — print each row (output appears on executor stdout
    # in cluster mode, on driver stdout in local mode)
    # ------------------------------------------------------------------
    def print_row(row: Row) -> None:
        print(f"  product={row['product']:12s}  revenue={row['revenue']:.2f}")

    print("\nforeach — rows printed from executor:")
    df.foreach(print_row)


def demo_foreach_partition(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    # ------------------------------------------------------------------
    # foreachPartition(f) — f receives an Iterator[Row]
    # Simulates opening/closing a connection once per partition
    # ------------------------------------------------------------------
    written_counter = spark.sparkContext.accumulator(0)

    def write_partition(rows) -> None:
        # In production: open DB connection here
        batch = list(rows)
        # In production: bulk-insert `batch` into the target system
        written_counter.add(len(batch))
        # In production: close DB connection here

    df.foreachPartition(write_partition)
    print(f"\nforeachPartition — rows processed: {written_counter.value}")


def demo_foreach_partition_batched(spark: SparkSession) -> None:
    df = (
        spark.createDataFrame(*customer_orders())
        .filter(F.col("status") == "active")
        .select("order_id", "product", "quantity", "unit_price")
        .repartition(2)  # (2)
    )

    batch_log = spark.sparkContext.accumulator(0)

    def write_batch(rows) -> None:
        records = [row.asDict() for row in rows]
        if records:
            # Simulate a batched write (e.g. JDBC executeBatch)
            batch_log.add(len(records))

    df.foreachPartition(write_batch)
    print(f"\nforeachPartition batched — records written: {batch_log.value}")


def demo_foreach_rdd(spark: SparkSession) -> None:
    df = spark.createDataFrame(*product_revenue())

    # ------------------------------------------------------------------
    # .rdd.foreach() / .rdd.foreachPartition() — same semantics via RDD API
    # Gives access to rdd-level functions like glom() before forEach
    # ------------------------------------------------------------------
    total_acc = spark.sparkContext.accumulator(0.0)

    def accumulate_revenue(row: Row) -> None:
        total_acc.add(float(row["revenue"]))

    df.rdd.foreach(accumulate_revenue)
    print(f"\nrdd.foreach — total revenue accumulated: {total_acc.value:.2f}")


def main(spark: SparkSession) -> None:
    demo_foreach(spark)
    demo_foreach_print(spark)
    demo_foreach_partition(spark)
    demo_foreach_partition_batched(spark)
    demo_foreach_rdd(spark)


# (1) Accumulators are the only safe way to aggregate results from executor
#     functions back to the driver — do not use shared Python variables.
# (2) repartition() controls how many partitions (and therefore how many
#     connection-open/close cycles) foreachPartition will perform.

if __name__ == "__main__":
    spark = get_spark("action-foreach")
    main(spark)
    spark.stop()
