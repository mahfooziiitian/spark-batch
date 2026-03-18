---
applyTo: "{**/test_*.py,**/*_test.py,**/conftest.py}"
---

# PySpark MongoDB — Test Instructions

## SparkSession Fixture

Always use a single session-scoped fixture to avoid JVM restart overhead.
Include the MongoDB Spark Connector JAR so integration tests can read/write MongoDB.

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("test-pyspark-mongodb")
        .master("local[2]")
        .config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.13:10.1.1",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

- `local[2]` — two threads; deterministic and fast.
- `shuffle.partitions=2` — default 200 is wasteful for test data.
- `ui.enabled=false` — skip Spark Web UI to speed up fixture creation.
- Log level `ERROR` — suppress all output except actual errors.

## Test Organisation

Group tests into classes by capability area:

```python
class TestMongoDBWrite:    ...  # write DataFrames to MongoDB collections
class TestMongoDBRead:     ...  # read from MongoDB collections
class TestMongoDBRoundTrip:...  # write then read back and verify
class TestDataFrame:       ...  # DataFrame transformations (no MongoDB)
```

## Assertions

Prefer `df.count()` over `len(df.collect())` — it avoids pulling all data to the driver:

```python
assert df.count() == 5
assert set(df.columns) == {"name", "age"}
assert df.filter(F.col("age") > 100).count() == 3
```

For single-row assertions, collect minimally:

```python
row = df.filter(F.col("name") == "Gandalf").first()
assert row["age"] == 1000
```

## MongoDB Test Conventions

- **Require a running MongoDB instance** from `docker compose up -d`.
- Use unique collection names per test or clean up in teardown to avoid cross-test pollution.
- Use `scope="session"` for the Spark fixture to avoid JVM restarts.
- For pure DataFrame tests that don't need MongoDB, skip the connector config.

## File I/O Tests

Use pytest's `tmp_path` fixture for Parquet / CSV roundtrip tests:

```python
def test_write_and_read_parquet(self, spark, tmp_path):
    path = str(tmp_path / "output.parquet")
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    df.write.mode("overwrite").parquet(path)
    read_back = spark.read.parquet(path)
    assert read_back.count() == 2
```

## Entry Point

Always include a direct-run entry point so the file works without pytest:

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```

## Running Tests

```bash
# Start MongoDB first
cd infra/docker && docker compose up -d && cd -

# Run tests
uv run pytest -v --tb=short
```

## CI Environment Variables

```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export SPARK_LOCAL_IP=127.0.0.1
export MONGO_URI=mongodb://127.0.0.1:27017
```
