import time

import pytest
from pyspark import TaskContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test-job-stage-task")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ── Jobs ─────────────────────────────────────────────────────────────────── #


class TestJobs:
    """Each action triggers a Spark job."""

    def test_count_produces_result(self, spark):
        df = spark.range(0, 100, numPartitions=2)
        assert df.count() == 100

    def test_collect_produces_result(self, spark):
        df = spark.range(0, 10, numPartitions=2)
        rows = df.collect()
        assert len(rows) == 10

    def test_first_returns_single_row(self, spark):
        df = spark.range(0, 10, numPartitions=2)
        row = df.first()
        assert row is not None
        assert row["id"] == 0

    def test_multiple_actions_all_succeed(self, spark):
        df = spark.range(0, 50, numPartitions=2)
        assert df.count() == 50
        assert df.first() is not None
        total = df.agg(F.sum("id")).collect()[0][0]
        assert total == 50 * 49 // 2


class TestJobGroups:
    """Job groups tag related jobs for monitoring and cancellation."""

    def test_set_and_clear_job_group(self, spark):
        sc = spark.sparkContext
        sc.setJobGroup("test-group-1", "Unit test group")
        spark.range(10).count()
        # Should not raise
        sc.setJobGroup("", "")

    def test_job_group_appears_in_tracker(self, spark):
        sc = spark.sparkContext
        group_id = "tracker-test-group"
        sc.setJobGroup(group_id, "Tracker test")
        spark.range(100, numPartitions=2).count()
        time.sleep(0.3)
        job_ids = sc.statusTracker().getJobIdsForGroup(group_id)
        assert len(job_ids) > 0
        sc.setJobGroup("", "")

    def test_job_description_does_not_raise(self, spark):
        sc = spark.sparkContext
        sc.setJobDescription("Test description")
        spark.range(10).count()
        sc.setJobDescription("")


# ── Stages ───────────────────────────────────────────────────────────────── #


class TestStages:
    """Stages split at shuffle boundaries."""

    def test_narrow_transforms_same_partitions(self, spark):
        df = spark.range(0, 100, numPartitions=4)
        filtered = df.filter(F.col("id") > 10).withColumn("x", F.col("id") * 2)
        assert filtered.rdd.getNumPartitions() == 4

    def test_groupby_triggers_shuffle(self, spark):
        df = spark.range(0, 100, numPartitions=4)
        grouped = df.groupBy(F.col("id") % 5).agg(F.count("*").alias("cnt"))
        # After shuffle, partitions <= spark.sql.shuffle.partitions (AQE may coalesce further)
        assert grouped.rdd.getNumPartitions() <= 2

    def test_repartition_triggers_shuffle(self, spark):
        df = spark.range(0, 100, numPartitions=2)
        repartitioned = df.repartition(6)
        assert repartitioned.rdd.getNumPartitions() == 6

    def test_debug_string_shows_lineage(self, spark):
        df = spark.range(0, 100, numPartitions=4)
        grouped = df.groupBy(F.col("id") % 3).count()
        debug_str = grouped.rdd.toDebugString()
        assert debug_str is not None
        decoded = debug_str.decode("utf-8") if isinstance(debug_str, bytes) else debug_str
        assert len(decoded) > 0

    def test_join_creates_stages(self, spark):
        left = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        right = spark.createDataFrame([(1, 100), (2, 200)], ["id", "score"])
        joined = left.join(right, on="id")
        assert joined.count() == 2


class TestMultiStagePipeline:
    """Complex pipelines with multiple shuffle boundaries."""

    def test_filter_groupby_join_pipeline(self, spark):
        orders = spark.createDataFrame(
            [(1, "A", 100.0), (2, "B", 200.0), (3, "A", 150.0), (4, "B", 300.0)],
            ["order_id", "region", "amount"],
        )
        regions = spark.createDataFrame([("A", "North"), ("B", "South")], ["region", "area"])

        result = (
            orders.filter(F.col("amount") > 100)
            .groupBy("region")
            .agg(F.sum("amount").alias("total"))
            .join(regions, on="region")
        )
        rows = result.collect()
        assert len(rows) == 2

    def test_union_does_not_shuffle(self, spark):
        df1 = spark.range(0, 50, numPartitions=2)
        df2 = spark.range(50, 100, numPartitions=2)
        unioned = df1.union(df2)
        # Union is a narrow transform — partitions add up
        assert unioned.rdd.getNumPartitions() == 4
        assert unioned.count() == 100


class TestStageSkipping:
    """Cached data allows stage skipping on subsequent actions."""

    def test_cache_enables_stage_reuse(self, spark):
        df = spark.range(0, 1000, numPartitions=4)
        grouped = df.groupBy(F.col("id") % 5).agg(F.count("*").alias("cnt"))
        grouped.cache()

        # First action materialises the cache
        count1 = grouped.count()
        assert count1 == 5

        # Second action reuses cached data (stage skipped)
        count2 = grouped.filter(F.col("cnt") > 100).count()
        assert count2 == 5

        grouped.unpersist()


# ── Tasks ────────────────────────────────────────────────────────────────── #


class TestTasks:
    """One task per partition per stage."""

    def test_partition_count_equals_task_count(self, spark):
        for n_parts in [1, 2, 4, 8]:
            df = spark.range(0, 100, numPartitions=n_parts)
            assert df.rdd.getNumPartitions() == n_parts

    def test_repartition_changes_task_count(self, spark):
        df = spark.range(0, 100, numPartitions=2)
        repartitioned = df.repartition(6)
        assert repartitioned.rdd.getNumPartitions() == 6

    def test_coalesce_reduces_task_count(self, spark):
        df = spark.range(0, 100, numPartitions=8)
        coalesced = df.coalesce(2)
        assert coalesced.rdd.getNumPartitions() == 2

    def test_glom_shows_rows_per_partition(self, spark):
        df = spark.range(0, 20, numPartitions=4)
        partitions = df.rdd.glom().collect()
        assert len(partitions) == 4
        total_rows = sum(len(p) for p in partitions)
        assert total_rows == 20


class TestTaskContext:
    """TaskContext provides per-task metadata on the executor side."""

    def test_partition_ids_are_sequential(self, spark):
        rdd = spark.sparkContext.parallelize(range(40), numSlices=4)

        def get_partition_id(iterator):
            ctx = TaskContext.get()
            pid = ctx.partitionId() if ctx is not None else -1
            yield pid

        partition_ids = sorted(rdd.mapPartitions(get_partition_id).collect())
        assert partition_ids == [0, 1, 2, 3]

    def test_stage_id_is_non_negative(self, spark):
        rdd = spark.sparkContext.parallelize(range(10), numSlices=2)

        def get_stage_id(iterator):
            ctx = TaskContext.get()
            sid = ctx.stageId() if ctx is not None else -1
            yield sid

        stage_ids = rdd.mapPartitions(get_stage_id).collect()
        for sid in stage_ids:
            assert sid >= 0

    def test_attempt_number_is_zero_on_success(self, spark):
        rdd = spark.sparkContext.parallelize(range(10), numSlices=2)

        def get_attempt(iterator):
            ctx = TaskContext.get()
            attempt = ctx.attemptNumber() if ctx is not None else -1
            yield attempt

        attempts = rdd.mapPartitions(get_attempt).collect()
        for a in attempts:
            assert a == 0

    def test_task_info_per_partition(self, spark):
        rdd = spark.sparkContext.parallelize(range(100), numSlices=4)

        def task_info(iterator):
            ctx = TaskContext.get()
            rows = list(iterator)
            pid = ctx.partitionId() if ctx is not None else -1
            yield (pid, len(rows))

        results = rdd.mapPartitions(task_info).collect()
        assert len(results) == 4
        assert sum(row_count for _, row_count in results) == 100


class TestStatusTracker:
    """StatusTracker exposes job and stage metadata."""

    def test_job_info_has_stage_ids(self, spark):
        sc = spark.sparkContext
        group = "tracker-job-info-test"
        sc.setJobGroup(group, "Test job info")
        spark.range(100, numPartitions=2).groupBy(F.col("id") % 3).count().collect()
        time.sleep(0.3)

        job_ids = sc.statusTracker().getJobIdsForGroup(group)
        assert len(job_ids) > 0

        job_info = sc.statusTracker().getJobInfo(job_ids[0])
        assert job_info is not None
        assert len(job_info.stageIds) > 0

        sc.setJobGroup("", "")

    def test_stage_info_has_task_count(self, spark):
        sc = spark.sparkContext
        group = "tracker-stage-info-test"
        sc.setJobGroup(group, "Test stage info")
        spark.range(100, numPartitions=4).count()
        time.sleep(0.3)

        job_ids = sc.statusTracker().getJobIdsForGroup(group)
        assert len(job_ids) > 0

        job_info = sc.statusTracker().getJobInfo(job_ids[0])
        assert job_info is not None

        for sid in job_info.stageIds:
            stage_info = sc.statusTracker().getStageInfo(sid)
            if stage_info is not None:
                assert stage_info.numTasks > 0

        sc.setJobGroup("", "")


class TestPartitionImpact:
    """Partition count controls parallelism and task granularity."""

    def test_single_partition_sequential(self, spark):
        df = spark.range(0, 100, numPartitions=1)
        assert df.rdd.getNumPartitions() == 1
        assert df.count() == 100

    def test_many_partitions_distributes_data(self, spark):
        df = spark.range(0, 100, numPartitions=10)
        partitions = df.rdd.glom().collect()
        assert len(partitions) == 10
        for part in partitions:
            assert len(part) == 10

    def test_uneven_distribution(self, spark):
        rdd = spark.sparkContext.parallelize(range(7), numSlices=3)
        partitions = rdd.glom().collect()
        assert len(partitions) == 3
        sizes = sorted([len(p) for p in partitions])
        assert sum(sizes) == 7
        # Distribution should be approximately even (2, 2, 3 or similar)
        assert max(sizes) - min(sizes) <= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
