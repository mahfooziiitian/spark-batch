---
applyTo: "tests/**/*.py"
---

# Testing Instructions — PySpark Architecture

## SparkSession Fixture

Use a single **session-scoped** fixture to avoid JVM restart overhead:

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .appName("pyspark-architecture-tests")
               .master("local[2]")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

## SparkContext Fixture

When testing RDD / SparkContext behaviour directly:

```python
from pyspark import SparkConf, SparkContext

@pytest.fixture(scope="session")
def spark_context():
    conf = (SparkConf()
            .setAppName("sc-architecture-tests")
            .setMaster("local[2]")
            .set("spark.ui.enabled", "false"))
    sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("ERROR")
    yield sc
    sc.stop()
```

## Cluster Manager Tests

Tests that require a real YARN or Kubernetes cluster **must** be skipped in
CI — mark them explicitly:

```python
import pytest

@pytest.mark.skip(reason="Requires a live YARN cluster — run manually")
def test_yarn_cluster(spark_context_yarn):
    rdd = spark_context_yarn.parallelize([1, 2, 3, 4, 5])
    assert rdd.sum() == 15

@pytest.mark.skip(reason="Requires a live Kubernetes cluster — run manually")
def test_kubernetes_cluster(spark_context_kubernetes):
    rdd = spark_context_kubernetes.parallelize([1, 2, 3, 4, 5])
    assert rdd.sum() == 15
```

## Test Organisation

Group tests into classes by Spark component:

```python
class TestSparkSession:       ...  # lifecycle, singleton, newSession
class TestSparkContext:       ...  # RDD creation, partitions, getOrCreate
class TestDriver:             ...  # DAG construction, plan, actions
class TestExecutorPartitions: ...  # partitioning, caching, shuffle
class TestClusterManagers:    ...  # local, YARN (skipped), K8s (skipped)
```

## SparkSession Singleton Test Pattern

```python
class TestSparkSession:
    def test_get_or_create_returns_same_session(self):
        s1 = SparkSession.builder.appName("A").master("local[*]").getOrCreate()
        s2 = SparkSession.builder.appName("B").master("local[*]").getOrCreate()
        assert s1 is s2

    def test_new_session_shares_spark_context(self):
        s1 = SparkSession.builder.master("local[*]").getOrCreate()
        s2 = s1.newSession()
        assert s1.sparkContext is s2.sparkContext
        assert s1 != s2   # different session objects despite shared SC

    def test_shuffle_partitions_config(self, spark):
        assert spark.conf.get("spark.sql.shuffle.partitions") == "2"
```

## SparkContext & RDD Test Pattern

```python
class TestSparkContext:
    def test_parallelize_sum(self, spark_context):
        rdd = spark_context.parallelize([1, 2, 3, 4, 5])
        assert rdd.sum() == 15

    def test_partition_count(self, spark_context):
        rdd = spark_context.parallelize(range(10), numSlices=3)
        assert rdd.getNumPartitions() == 3

    def test_get_or_create_singleton(self):
        from pyspark import SparkConf, SparkContext
        sc1 = SparkContext.getOrCreate(SparkConf().setAppName("SC1").setMaster("local[*]"))
        sc2 = SparkContext.getOrCreate(SparkConf().setAppName("SC2").setMaster("local[*]"))
        assert sc1 == sc2
```

## Assertions

```python
# Row count — prefer count() over len(collect())
assert df.count() == 5

# Schema check
assert set(df.columns) == {"id", "name", "value"}

# Single-row assertion — collect minimally
row = df.filter(F.col("id") == 1).first()
assert row["name"] == "Alice"

# Config value assertion
assert spark.conf.get("spark.sql.shuffle.partitions") == "2"
```

## Entry Point

Always include a direct-run entry point:

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```

## CI Environment Variables

```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export SPARK_LOCAL_IP=127.0.0.1
```
