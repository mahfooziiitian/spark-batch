# Test Setup

## conftest.py

The test suite uses a **session-scoped** Spark fixture to avoid restarting the
JVM between tests:

```python title="tests/conftest.py"
--8<-- "tests/conftest.py"
```

Key configuration:

| Setting | Value | Why |
|---------|-------|-----|
| `master` | `local[2]` | Two threads — deterministic and fast |
| `shuffle.partitions` | `2` | Default 200 is wasteful for test data |
| `ui.enabled` | `false` | Skip Spark Web UI for speed |
| `setLogLevel` | `ERROR` | Suppress all output except errors |
| `arrow.pyspark.enabled` | `true` | Enable Arrow for UDF/conversion tests |

## Test Organisation

Tests are grouped into classes by capability:

```python
class TestSparkSession:       # version, master, app name
class TestDataFrame:          # create, filter, join, withColumn, groupBy
class TestSQL:                # temp views, aggregations, filters
class TestWindowFunctions:    # rank, running total, lag, lead
class TestParquetIO:          # write, read, partitioned write
```

## Assertions

Prefer `df.count()` over `len(df.collect())`:

```python
assert df.count() == 5
assert set(df.columns) == {"id", "name", "score"}
assert df.filter(F.col("id") > 3).count() == 2
```

## File I/O Tests

Use pytest's `tmp_path` fixture for automatic cleanup:

```python
def test_write_parquet(self, spark, tmp_path):
    path = str(tmp_path / "output.parquet")
    df.write.mode("overwrite").parquet(path)
    assert spark.read.parquet(path).count() == expected
```

## UDTF Tests — Serialization

UDTF classes must be defined **in the test file** (not imported) and registered
with `addPyFile`:

```python
spark.sparkContext.addPyFile(__file__)  # (1)!

@udtf(returnType="num: int, squared: int")
class SquareNumbers:
    def eval(self, n: int):
        for i in range(1, n + 1):
            yield (i, i * i)
```

1. Workers receive a copy of the test file so they can unpickle the UDTF class.

## Environment Variables

Set these in CI and locally:

```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export SPARK_LOCAL_IP=127.0.0.1
```

## Entry Point

Every test file includes a direct-run entry point:

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```
