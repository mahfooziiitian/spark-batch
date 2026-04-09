import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test-driver-memory")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


class TestDriverMemoryConfig:
    def test_driver_memory_has_default(self, spark):
        mem = spark.conf.get("spark.driver.memory", "1g")
        assert mem is not None and len(mem) > 0

    def test_max_result_size_has_default(self, spark):
        max_result = spark.conf.get("spark.driver.maxResultSize", "1g")
        assert max_result is not None

    def test_broadcast_block_size_has_default(self, spark):
        block_size = spark.conf.get("spark.broadcast.blockSize", "4m")
        assert block_size is not None


class TestBroadcastMemory:
    def test_small_broadcast_creation(self, spark):
        lookup = {i: f"val_{i}" for i in range(50)}
        bc = spark.sparkContext.broadcast(lookup)
        assert len(bc.value) == 50
        bc.destroy()

    def test_broadcast_join_uses_broadcast_plan(self, spark):
        large = spark.range(1000).withColumn("key", (F.col("id") % 10).cast("int"))
        small = spark.createDataFrame([(i, f"v{i}") for i in range(10)], ["key", "val"])
        joined = large.join(F.broadcast(small), on="key")
        plan = joined._jdf.queryExecution().executedPlan().toString()
        assert "Broadcast" in plan
        assert joined.count() == 1000

    def test_broadcast_variable_accessible_in_rdd(self, spark):
        lookup = {"a": 1, "b": 2, "c": 3}
        bc = spark.sparkContext.broadcast(lookup)
        rdd = spark.sparkContext.parallelize(["a", "b", "c", "a"])
        result = rdd.map(lambda x: bc.value.get(x, 0)).collect()
        assert sorted(result) == [1, 1, 2, 3]
        bc.destroy()

    def test_broadcast_destroy_cleans_up(self, spark):
        data = list(range(100))
        bc = spark.sparkContext.broadcast(data)
        assert bc.value == data
        bc.destroy()


class TestCollectSafety:
    def test_collect_small_result(self, spark):
        df = spark.range(10)
        result = df.collect()
        assert len(result) == 10

    def test_first_returns_single_row(self, spark):
        df = spark.range(100)
        row = df.first()
        assert row is not None
        assert "id" in row.asDict()

    def test_head_returns_bounded_rows(self, spark):
        df = spark.range(1000)
        rows = df.head(5)
        assert len(rows) == 5

    def test_take_returns_bounded_rows(self, spark):
        df = spark.range(1000)
        rows = df.take(3)
        assert len(rows) == 3

    def test_aggregate_before_collect(self, spark):
        df = spark.range(10_000)
        summary = df.agg(
            F.count("id").alias("cnt"),
            F.min("id").alias("min_id"),
            F.max("id").alias("max_id"),
        ).collect()[0]
        assert summary["cnt"] == 10_000
        assert summary["min_id"] == 0
        assert summary["max_id"] == 9999

    def test_filter_before_collect_reduces_data(self, spark):
        df = spark.range(10_000)
        filtered = df.filter(F.col("id") < 5).collect()
        assert len(filtered) == 5


class TestAccumulators:
    def test_counter_accumulator(self, spark):
        counter = spark.sparkContext.accumulator(0)
        rdd = spark.sparkContext.parallelize(range(100), numSlices=2)
        rdd.foreach(lambda x: counter.add(1))
        assert counter.value == 100

    def test_sum_accumulator(self, spark):
        total = spark.sparkContext.accumulator(0)
        rdd = spark.sparkContext.parallelize(range(1, 11), numSlices=2)
        rdd.foreach(lambda x: total.add(x))
        assert total.value == 55

    def test_multiple_accumulators(self, spark):
        rows = spark.sparkContext.accumulator(0)
        errors = spark.sparkContext.accumulator(0)

        rdd = spark.sparkContext.parallelize(range(50), numSlices=2)

        def process(x: int) -> None:
            rows.add(1)
            if x % 10 == 0:
                errors.add(1)

        rdd.foreach(process)
        assert rows.value == 50
        assert errors.value == 5


pandas = pytest.importorskip("pandas")


class TestToPandas:
    def test_topandas_with_limit(self, spark):
        df = spark.range(1000).withColumn("val", F.rand())
        pdf = df.limit(50).toPandas()
        assert len(pdf) == 50
        assert list(pdf.columns) == ["id", "val"]

    def test_topandas_memory_usage_is_bounded(self, spark):
        df = spark.range(100)
        pdf = df.toPandas()
        mem_bytes = pdf.memory_usage(deep=True).sum()
        assert mem_bytes > 0
        assert mem_bytes < 1_000_000  # well under 1 MB for 100 rows

    def test_topandas_preserves_data(self, spark):
        data = [(1, "a"), (2, "b"), (3, "c")]
        df = spark.createDataFrame(data, ["id", "name"])
        pdf = df.toPandas()
        assert pdf["id"].tolist() == [1, 2, 3]
        assert pdf["name"].tolist() == ["a", "b", "c"]


class TestMaxResultSize:
    def test_estimate_collect_size(self, spark):
        df = spark.range(1000).withColumn("data", F.lit("x" * 50))
        stats = df.agg(
            F.count("id").alias("rows"),
            F.avg(F.length("data")).alias("avg_len"),
        ).first()
        assert stats["rows"] == 1000
        assert stats["avg_len"] == 50.0

    def test_small_collect_within_limit(self, spark):
        df = spark.range(100)
        result = df.collect()
        assert len(result) == 100


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
