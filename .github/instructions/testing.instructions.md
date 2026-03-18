---
applyTo: "{**/test_*.py,**/*_test.py}"
---

# PySpark Test Instructions

## SparkSession Fixture

Always use a single session-scoped fixture to avoid JVM restart overhead:

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .appName("test-suite")
               .master("local[2]")                           # (1)
               .config("spark.sql.shuffle.partitions", "2")  # (2)
               .config("spark.ui.enabled", "false")          # (3)
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")                 # (4)
    yield session
    session.stop()
```

1. `local[2]` — two threads; deterministic and fast.
2. Default 200 is wasteful for test data.
3. Skip Spark Web UI to speed up fixture creation.
4. Suppress all output except actual errors.

## Test Organisation

Group tests into classes by capability area:

```python
class TestSparkSession:   ...  # version, master, app name
class TestDataFrame:      ...  # create, filter, join, withColumn, groupBy
class TestSQL:            ...  # temp views, aggregations, filters
class TestWindowFunctions:...  # rank, running total, lag, lead
class TestParquetIO:      ...  # write, read, partitioned write
```

## Assertions

Prefer `df.count()` over `len(df.collect())` — it avoids pulling all data to the driver:

```python
assert df.count() == 5                              # row count
assert set(df.columns) == {"id", "name", "score"}  # schema check
assert df.filter(F.col("id") > 3).count() == 2     # filter check
```

For single-row assertions, collect minimally:

```python
row = df.filter(F.col("region") == "North").first()
assert row["total_revenue"] == 1999.98
```

## File I/O Tests

Use pytest's `tmp_path` fixture — it is unique per test and cleaned up automatically:

```python
def test_write_and_read_parquet(self, spark, tmp_path):
    path = str(tmp_path / "output.parquet")
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    df.write.mode("overwrite").parquet(path)
    read_back = spark.read.parquet(path)
    assert read_back.count() == 2
```

## Window Function Tests

Always create a minimal, deterministic dataset:

```python
def test_running_total(self, spark):
    from pyspark.sql.window import Window
    data = [(1, 10), (2, 20), (3, 30)]
    df = spark.createDataFrame(data, ["step", "val"])
    w = Window.orderBy("step").rowsBetween(Window.unboundedPreceding, 0)
    result = df.withColumn("running", F.sum("val").over(w))
    assert result.orderBy(F.desc("step")).first()["running"] == 60
```

## Entry Point

Always include a direct-run entry point so the file works without pytest:

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```

## CI Environment Variables

Tests require these env vars — set them in the CI workflow and in local `.env` files:

```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export SPARK_LOCAL_IP=127.0.0.1
```
