---
applyTo: "tests/**/*.py"
---

# Testing Conventions

## Framework

- Use **pytest** as the test runner.
- Place all tests under the `tests/` directory, mirroring the `src/` structure.
- Name test files `test_<module>.py` and test functions `test_<behavior>`.

## SparkSession Fixture

- Create a shared `SparkSession` as a **session-scoped** pytest fixture to avoid repeated JVM startup:
  ```python
  import pytest
  from pyspark.sql import SparkSession

  @pytest.fixture(scope="session")
  def spark():
      session = (SparkSession.builder
                 .master("local[2]")
                 .appName("test")
                 .config("spark.sql.shuffle.partitions", "2")
                 .getOrCreate())
      yield session
      session.stop()
  ```
- Use `local[2]` (not `local[*]`) in tests for predictable parallelism.
- Set `spark.sql.shuffle.partitions` to a small number (e.g., 2) to speed up tests.

## Testing Kafka Logic Without a Broker

- **Mock Kafka reads** by creating DataFrames that mimic the Kafka schema:
  ```python
  from pyspark.sql.types import StructType, StructField, StringType, BinaryType, IntegerType, LongType, TimestampType

  kafka_schema = StructType([
      StructField("key", BinaryType()),
      StructField("value", BinaryType()),
      StructField("topic", StringType()),
      StructField("partition", IntegerType()),
      StructField("offset", LongType()),
      StructField("timestamp", TimestampType()),
  ])
  ```
- Build test DataFrames with `spark.createDataFrame(data, schema=kafka_schema)`.
- Test transformation functions (e.g., `get_latest_offset_from_batch`, `func`) by passing in-memory DataFrames.

## Testing Streaming Logic

- Test `foreachBatch` callback functions (`func(batch_df, batch_id)`) independently from the streaming query.
- Mock external sinks (JDBC writes, file writes) using `unittest.mock.patch` or by injecting a test sink.
- For integration tests with a live Kafka cluster, mark tests with `@pytest.mark.integration` and skip in CI unless the cluster is available.

## Assertions

- Use `DataFrame.collect()` to materialize results, then assert on the rows.
- For schema checks, compare `df.schema` against an expected `StructType`.
- Use `pytest.approx` for floating-point comparisons if needed.

## Test Organization

- Group related tests into classes when they share setup (e.g., `TestOffsetTracking`).
- Use `conftest.py` for shared fixtures (SparkSession, sample DataFrames).
- Keep tests fast — avoid large datasets; sample or create minimal test data.
