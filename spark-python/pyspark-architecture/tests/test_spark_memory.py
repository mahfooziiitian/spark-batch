import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test-memory")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


class TestStorageLevels:
    def test_memory_only(self, spark):
        df = spark.range(1000).persist(StorageLevel.MEMORY_ONLY)
        assert df.count() == 1000
        assert df.storageLevel == StorageLevel.MEMORY_ONLY
        df.unpersist(blocking=True)

    def test_memory_and_disk(self, spark):
        df = spark.range(1000).persist(StorageLevel.MEMORY_AND_DISK)
        assert df.count() == 1000
        assert df.storageLevel == StorageLevel.MEMORY_AND_DISK
        df.unpersist(blocking=True)

    def test_disk_only(self, spark):
        df = spark.range(1000).persist(StorageLevel.DISK_ONLY)
        assert df.count() == 1000
        assert df.storageLevel == StorageLevel.DISK_ONLY
        df.unpersist(blocking=True)

    def test_memory_and_disk_serialized(self, spark):
        df = spark.range(1000).persist(StorageLevel.MEMORY_AND_DISK)
        assert df.count() == 1000
        assert df.storageLevel == StorageLevel.MEMORY_AND_DISK
        df.unpersist(blocking=True)

    def test_cache_is_memory_and_disk_deser(self, spark):
        df = spark.range(100).cache()
        df.count()
        assert df.storageLevel == StorageLevel.MEMORY_AND_DISK_DESER
        df.unpersist(blocking=True)


class TestCacheLifecycle:
    def test_unpersist_resets_storage_level(self, spark):
        df = spark.range(100).cache()
        df.count()
        assert df.storageLevel != StorageLevel.NONE
        df.unpersist(blocking=True)
        assert df.storageLevel == StorageLevel.NONE

    def test_cached_df_returns_same_result(self, spark):
        df = spark.range(50).withColumn("x", F.col("id") * 2)
        cached = df.cache()
        first = cached.collect()
        second = cached.collect()
        assert first == second
        cached.unpersist(blocking=True)

    def test_multiple_actions_on_cached_df(self, spark):
        df = spark.range(200).cache()
        assert df.count() == 200
        assert df.filter(F.col("id") < 100).count() == 100
        assert df.agg(F.max("id")).first()[0] == 199
        df.unpersist(blocking=True)


class TestCheckpoint:
    def test_checkpoint_truncates_lineage(self, spark):
        spark.sparkContext.setCheckpointDir("/tmp/spark-test-checkpoint")  # noqa: S108
        rdd = spark.sparkContext.parallelize(range(100), numSlices=2)
        transformed = rdd.map(lambda x: x * 2).filter(lambda x: x > 50)
        transformed.checkpoint()
        transformed.count()
        debug = transformed.toDebugString()
        assert b"ReliableCheckpointRDD" in debug

    def test_checkpoint_preserves_data(self, spark):
        spark.sparkContext.setCheckpointDir("/tmp/spark-test-checkpoint")  # noqa: S108
        rdd = spark.sparkContext.parallelize(range(50), numSlices=2)
        rdd.checkpoint()
        rdd.count()
        assert rdd.sum() == sum(range(50))


class TestMemoryConfig:
    def test_memory_fraction_default(self, spark):
        fraction = spark.conf.get("spark.memory.fraction", "0.6")
        assert float(fraction) > 0

    def test_storage_fraction_default(self, spark):
        storage = spark.conf.get("spark.memory.storageFraction", "0.5")
        assert float(storage) > 0

    def test_off_heap_disabled_by_default(self, spark):
        off_heap = spark.conf.get("spark.memory.offHeap.enabled", "false")
        assert off_heap == "false"


class TestSerialization:
    def test_default_serializer(self, spark):
        serializer = spark.conf.get("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
        assert "Serializer" in serializer

    def test_serialised_cache_works(self, spark):
        df = spark.range(500).persist(StorageLevel.MEMORY_AND_DISK)
        assert df.count() == 500
        assert df.filter(F.col("id") == 42).count() == 1
        df.unpersist(blocking=True)

    def test_rdd_serialised_persist(self, spark):
        rdd = spark.sparkContext.parallelize(range(200), numSlices=2)
        rdd.persist(StorageLevel.MEMORY_AND_DISK)
        assert rdd.count() == 200
        rdd.unpersist()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
