"""Spark Job → Stage → Task execution hierarchy.

Every Spark application is broken down into:

    Application → Jobs → Stages → Tasks

- **Job**: triggered by each *action* (count, collect, show, write).
- **Stage**: a job splits at shuffle boundaries (groupBy, join, repartition).
  Consecutive narrow transforms run in a single stage.
- **Task**: one task per partition per stage — the smallest unit of work.
"""

import os
import time

from pyspark import TaskContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- Jobs ------------------------------------------------------------------ #


def demo_actions_trigger_jobs(spark: SparkSession) -> None:
    """Each action on a DataFrame triggers a separate Spark job."""
    sc = spark.sparkContext
    df = spark.range(0, 1000, numPartitions=4)

    # Action 1 → Job
    count = df.count()
    print(f"Action 1 (count):   {count} rows")

    # Action 2 → another Job
    first_row = df.first()
    print(f"Action 2 (first):   {first_row}")

    # Action 3 → another Job
    total = df.agg(F.sum("id")).collect()[0][0]
    print(f"Action 3 (collect): sum = {total}")

    active_jobs = sc.statusTracker().getActiveJobsIds()
    print(f"Active jobs right now: {list(active_jobs)}")


def demo_job_groups(spark: SparkSession) -> None:
    """Job groups let you tag and cancel related jobs together."""
    sc = spark.sparkContext

    sc.setJobGroup("etl-pipeline", "Monthly ETL batch")
    df = spark.range(0, 500, numPartitions=2)
    result = df.filter(F.col("id") % 2 == 0).count()
    print(f"Job group 'etl-pipeline' completed: {result} even numbers")

    sc.setJobGroup("reporting", "Dashboard aggregations")
    agg = df.agg(F.avg("id").alias("mean")).collect()[0]["mean"]
    print(f"Job group 'reporting' completed: mean = {agg}")

    # Clear group so subsequent jobs aren't tagged
    sc.setJobGroup("", "")


def demo_job_description(spark: SparkSession) -> None:
    """Job descriptions appear in the Spark UI for easy identification."""
    sc = spark.sparkContext

    sc.setJobDescription("Step 1: Load and filter data")
    df = spark.range(0, 100, numPartitions=2)
    filtered = df.filter(F.col("id") > 50)
    count1 = filtered.count()
    print(f"Step 1: {count1} rows after filter")

    sc.setJobDescription("Step 2: Aggregate results")
    total = filtered.agg(F.sum("id")).collect()[0][0]
    print(f"Step 2: total = {total}")

    sc.setJobDescription("")


# --- Stages ---------------------------------------------------------------- #


def demo_stage_boundaries(spark: SparkSession) -> None:
    """Stages split at shuffle boundaries — wide transformations create new stages.

    Narrow transforms (filter, map, withColumn) stay within the same stage.
    Wide transforms (groupBy, join, repartition) force a shuffle → new stage.
    """
    df = spark.range(0, 1000, numPartitions=4)

    # Stage 1: narrow transforms — no shuffle
    narrow = df.filter(F.col("id") > 100).withColumn("doubled", F.col("id") * 2)

    # Stage boundary: groupBy triggers a shuffle
    wide = narrow.groupBy(F.col("id") % 10).agg(F.count("*").alias("cnt"))

    debug_str = wide.rdd.toDebugString()
    if debug_str is not None:
        decoded = debug_str.decode("utf-8") if isinstance(debug_str, bytes) else debug_str
        print("RDD lineage (indentation = stage boundaries):")
        print(decoded)


def demo_multi_stage_pipeline(spark: SparkSession) -> None:
    """A pipeline with multiple shuffle boundaries creates multiple stages."""
    orders = spark.createDataFrame(
        [(1, "A", 100.0), (2, "B", 200.0), (3, "A", 150.0), (4, "B", 300.0), (5, "A", 50.0)],
        ["order_id", "region", "amount"],
    )
    customers = spark.createDataFrame(
        [("A", "North"), ("B", "South")],
        ["region", "area"],
    )

    # Stage 1: scan + filter (narrow)
    filtered = orders.filter(F.col("amount") > 100)

    # Stage 2: shuffle for groupBy
    grouped = filtered.groupBy("region").agg(F.sum("amount").alias("total"))

    # Stage 3: shuffle for join
    result = grouped.join(customers, on="region")

    result.show()
    print(f"Final partitions: {result.rdd.getNumPartitions()}")

    debug_str = result.rdd.toDebugString()
    if debug_str is not None:
        decoded = debug_str.decode("utf-8") if isinstance(debug_str, bytes) else debug_str
        print("\nFull lineage:")
        print(decoded)


def demo_stage_skipping(spark: SparkSession) -> None:
    """Cached/persisted data lets Spark skip earlier stages on re-use."""
    df = spark.range(0, 10000, numPartitions=4)
    grouped = df.groupBy(F.col("id") % 5).agg(F.count("*").alias("cnt"))

    # First action: computes all stages
    grouped.cache()
    count1 = grouped.count()
    print(f"First action (full computation): {count1} rows")

    # Second action: stage before the cache is skipped
    count2 = grouped.filter(F.col("cnt") > 1000).count()
    print(f"Second action (skipped stage): {count2} rows with cnt > 1000")

    grouped.unpersist()


# --- Tasks ----------------------------------------------------------------- #


def demo_tasks_per_partition(spark: SparkSession) -> None:
    """One task per partition per stage — tasks are the unit of parallelism."""
    partitions_2 = spark.range(0, 100, numPartitions=2)
    partitions_8 = spark.range(0, 100, numPartitions=8)

    print(f"2-partition DataFrame → {partitions_2.rdd.getNumPartitions()} tasks per stage")
    print(f"8-partition DataFrame → {partitions_8.rdd.getNumPartitions()} tasks per stage")

    # Repartition changes task count for subsequent stages
    repartitioned = partitions_2.repartition(6)
    print(f"After repartition(6) → {repartitioned.rdd.getNumPartitions()} tasks per stage")


def demo_task_context(spark: SparkSession) -> None:
    """TaskContext provides per-task metadata inside executor-side code."""

    def get_task_info(iterator):
        ctx = TaskContext.get()
        if ctx is not None:
            info = {
                "partition_id": ctx.partitionId(),
                "stage_id": ctx.stageId(),
                "attempt_number": ctx.attemptNumber(),
                "task_attempt_id": ctx.taskAttemptId(),
            }
        else:
            info = {"partition_id": -1, "stage_id": -1, "attempt_number": -1, "task_attempt_id": -1}
        rows = list(iterator)
        yield (info["partition_id"], info["stage_id"], info["attempt_number"], len(rows))

    rdd = spark.sparkContext.parallelize(range(100), numSlices=4)
    results = rdd.mapPartitions(get_task_info).collect()
    print("Partition | Stage | Attempt | Rows")
    print("----------|-------|---------|-----")
    for partition_id, stage_id, attempt, row_count in results:
        print(f"    {partition_id:5d} | {stage_id:5d} | {attempt:7d} | {row_count}")


def demo_task_metrics_via_status_tracker(spark: SparkSession) -> None:
    """StatusTracker exposes job and stage IDs for monitoring."""
    sc = spark.sparkContext

    sc.setJobGroup("metrics-demo", "Show status tracker usage")
    df = spark.range(0, 1000, numPartitions=4)
    df.groupBy(F.col("id") % 10).count().collect()

    # Give the tracker a moment to update
    time.sleep(0.5)

    job_ids = sc.statusTracker().getJobIdsForGroup("metrics-demo")
    print(f"Jobs in 'metrics-demo' group: {list(job_ids)}")

    for job_id in job_ids:
        job_info = sc.statusTracker().getJobInfo(job_id)
        if job_info is not None:
            stage_ids = job_info.stageIds
            print(f"  Job {job_id} → stages: {list(stage_ids)}")
            for sid in stage_ids:
                stage_info = sc.statusTracker().getStageInfo(sid)
                if stage_info is not None:
                    print(
                        f"    Stage {sid}: "
                        f"tasks={stage_info.numTasks}, "
                        f"active={stage_info.numActiveTasks}, "
                        f"completed={stage_info.numCompletedTasks}, "
                        f"failed={stage_info.numFailedTasks}"
                    )

    sc.setJobGroup("", "")


def demo_partition_count_impact(spark: SparkSession) -> None:
    """Partition count directly controls the degree of parallelism.

    Too few partitions → under-utilised cores.
    Too many partitions → scheduling overhead, small task penalty.
    """
    data = [(i, f"item_{i}", float(i * 10)) for i in range(100)]
    df = spark.createDataFrame(data, ["id", "name", "value"])

    for n_parts in [1, 4, 8, 20]:
        repartitioned = df.repartition(n_parts)
        num_parts = repartitioned.rdd.getNumPartitions()
        rows_per_partition = [len(part) for part in repartitioned.rdd.glom().collect()]
        print(f"  {n_parts:2d} partitions → tasks={num_parts}, rows/partition={rows_per_partition}")


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("job-stage-task-demo")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("JOBS — Actions trigger jobs")
    print("=" * 60)
    demo_actions_trigger_jobs(spark)

    print("\n" + "=" * 60)
    print("JOBS — Job groups for tagging and cancellation")
    print("=" * 60)
    demo_job_groups(spark)

    print("\n" + "=" * 60)
    print("JOBS — Job descriptions for Spark UI")
    print("=" * 60)
    demo_job_description(spark)

    print("\n" + "=" * 60)
    print("STAGES — Shuffle boundaries create stage splits")
    print("=" * 60)
    demo_stage_boundaries(spark)

    print("\n" + "=" * 60)
    print("STAGES — Multi-stage pipeline")
    print("=" * 60)
    demo_multi_stage_pipeline(spark)

    print("\n" + "=" * 60)
    print("STAGES — Stage skipping with cache")
    print("=" * 60)
    demo_stage_skipping(spark)

    print("\n" + "=" * 60)
    print("TASKS — Tasks per partition")
    print("=" * 60)
    demo_tasks_per_partition(spark)

    print("\n" + "=" * 60)
    print("TASKS — TaskContext metadata")
    print("=" * 60)
    demo_task_context(spark)

    print("\n" + "=" * 60)
    print("TASKS — StatusTracker metrics")
    print("=" * 60)
    demo_task_metrics_via_status_tracker(spark)

    print("\n" + "=" * 60)
    print("TASKS — Partition count impact")
    print("=" * 60)
    demo_partition_count_impact(spark)

    spark.stop()
