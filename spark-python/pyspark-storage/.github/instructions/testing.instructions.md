---
applyTo: "{**/test_*.py,**/*_test.py}"
---

# PySpark Storage Test Instructions

Use **pytest** and **pytest-mock**. Do **not** use `unittest` or `unittest.mock`
directly — always use the pytest-mock `mocker` fixture.

## Dependencies

```toml
[dependency-groups]
dev = [
    "pytest>=8.0",
    "pytest-mock>=3.14",
]
```

## SparkSession Fixture

```python
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .appName("storage-test-suite")
               .master("local[2]")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

## Mocking Storage Credentials

Use `mocker.patch.dict` to inject env vars:

```python
def test_reads_credentials_from_env(self, mocker):
    mocker.patch.dict("os.environ", {
        "AWS_ACCESS_KEY_ID": "test-key",
        "AWS_SECRET_ACCESS_KEY": "test-secret",
    })
    import os
    assert os.environ["AWS_ACCESS_KEY_ID"] == "test-key"
```

## Mocking Hadoop Configuration

```python
def test_hadoop_config_sets_endpoint(self, spark, mocker):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    mock_set = mocker.patch.object(hadoop_conf, "set")

    hadoop_conf.set("fs.s3a.endpoint", "http://localhost:9000")

    mock_set.assert_called_with("fs.s3a.endpoint", "http://localhost:9000")
```

## Mocking External Storage I/O

```python
def test_read_from_s3(self, spark, mocker):
    sample_df = spark.createDataFrame(
        [(1, "alice"), (2, "bob")], ["id", "name"])

    mock_read = mocker.patch.object(spark, "read")
    mock_read.option.return_value = mock_read
    mock_read.csv.return_value = sample_df

    result = spark.read.option("header", True).csv("s3a://bucket/data.csv")
    assert result.count() == 2
```

## Integration Tests with Local I/O

Use pytest's `tmp_path` for real file round-trip tests:

```python
class TestParquetRoundTrip:
    def test_write_and_read(self, spark, tmp_path):
        path = str(tmp_path / "output.parquet")
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        df.write.mode("overwrite").parquet(path)

        result = spark.read.parquet(path)
        assert result.count() == 2
        assert set(result.columns) == {"id", "val"}
```

## Test Organisation

```python
class TestStorageConfig:    ...  # credentials, endpoint, JAR loading
class TestStorageRead:      ...  # CSV, Parquet, JSON reads
class TestStorageWrite:     ...  # Parquet writes, partitioned writes
class TestAuthentication:   ...  # env auth, service principal, SAS, etc.
```

## pytest-mock Cheat Sheet

```python
mocker.patch.dict("os.environ", {"KEY": "value"})
mocker.patch("module.func", return_value="mocked")
mocker.patch.object(obj, "method", return_value="mocked")
mock_obj.assert_called_once_with(expected_arg)
spy = mocker.spy(module, "func")
```

## Entry Point

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```
