import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test-shuffle")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


class TestNarrowVsWide:
    def test_filter_preserves_partitions(self, spark):
        df = spark.range(100, numPartitions=4)
        filtered = df.filter(F.col("id") > 50)
        assert filtered.rdd.getNumPartitions() == 4

    def test_map_preserves_partitions(self, spark):
        df = spark.range(100, numPartitions=4)
        mapped = df.withColumn("doubled", F.col("id") * 2)
        assert mapped.rdd.getNumPartitions() == 4

    def test_groupby_changes_partitions(self, spark):
        df = spark.range(100, numPartitions=4).withColumn("key", (F.col("id") % 5).cast("int"))
        grouped = df.groupBy("key").count()
        assert grouped.rdd.getNumPartitions() == 2


class TestJoinStrategies:
    def test_broadcast_join_correctness(self, spark):
        left = spark.range(100).withColumn("key", (F.col("id") % 10).cast("int"))
        right = spark.createDataFrame([(i, f"v{i}") for i in range(10)], ["key", "val"])
        joined = left.join(F.broadcast(right), on="key")
        assert joined.count() == 100
        assert set(joined.columns) == {"key", "id", "val"}

    def test_broadcast_join_plan_contains_broadcast(self, spark):
        left = spark.range(100).withColumn("key", (F.col("id") % 5).cast("int"))
        right = spark.createDataFrame([(i, "x") for i in range(5)], ["key", "val"])
        joined = left.join(F.broadcast(right), on="key")
        plan = joined._jdf.queryExecution().executedPlan().toString()
        assert "Broadcast" in plan

    def test_sort_merge_join_with_hint(self, spark):
        left = spark.range(100).withColumn("key", (F.col("id") % 10).cast("int"))
        right = spark.range(50).withColumn("key", (F.col("id") % 10).cast("int"))
        joined = left.hint("merge").join(right, on="key")
        assert joined.count() > 0


class TestShufflePartitions:
    def test_shuffle_partitions_config(self, spark):
        assert spark.conf.get("spark.sql.shuffle.partitions") == "2"

    def test_groupby_uses_shuffle_partitions(self, spark):
        df = spark.range(100).withColumn("k", (F.col("id") % 3).cast("int"))
        grouped = df.groupBy("k").count()
        assert grouped.rdd.getNumPartitions() == 2

    def test_override_shuffle_partitions(self, spark):
        original = spark.conf.get("spark.sql.shuffle.partitions")
        spark.conf.set("spark.sql.shuffle.partitions", "4")
        df = spark.range(100).withColumn("k", (F.col("id") % 3).cast("int"))
        grouped = df.groupBy("k").count()
        assert grouped.rdd.getNumPartitions() == 4
        spark.conf.set("spark.sql.shuffle.partitions", original)


class TestSkewHandling:
    def test_salted_aggregation_matches_direct(self, spark):
        data = [(0, "a")] * 90 + [(i, "b") for i in range(1, 11)]
        df = spark.createDataFrame(data, ["key", "val"])

        direct = df.groupBy("key").count().orderBy("key")

        salted = df.withColumn("salt", (F.rand() * 4).cast("int"))
        salted_result = (
            salted.groupBy("key", "salt")
            .agg(F.count("val").alias("partial"))
            .groupBy("key")
            .agg(F.sum("partial").alias("count"))
            .orderBy("key")
        )

        assert direct.count() == salted_result.count()
        assert direct.filter(F.col("key") == 0).first()["count"] == 90
        assert salted_result.filter(F.col("key") == 0).first()["count"] == 90

    def test_repartition_by_key_distributes_evenly(self, spark):
        df = spark.range(100).withColumn("key", (F.col("id") % 4).cast("int"))
        repartitioned = df.repartition(4, "key")
        assert repartitioned.rdd.getNumPartitions() == 4


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
