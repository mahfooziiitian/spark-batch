---
applyTo: "{**/test_*.py,**/*_test.py}"
---

# PySpark Pandas Test Instructions

## SparkSession Fixture

Always use a single session-scoped fixture with Arrow enabled:

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .appName("test-suite")
               .master("local[2]")                                        # (1)
               .config("spark.sql.shuffle.partitions", "2")               # (2)
               .config("spark.ui.enabled", "false")                       # (3)
               .config("spark.sql.execution.arrow.pyspark.enabled", "true")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")                              # (4)
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
class TestPandasCreation:     ...  # createDataFrame from pandas, Arrow conversion
class TestPandasOnSpark:      ...  # ps.DataFrame ops, conversion, options
class TestPandasUDF:          ...  # @pandas_udf, vectorized UDFs
class TestArrowOptimization:  ...  # toPandas, createDataFrame with Arrow
class TestUDTF:               ...  # @udtf, table arguments, SQL registration
```

## Assertions

Prefer `df.count()` over `len(df.collect())` — it avoids pulling all data to the driver:

```python
assert df.count() == 5
assert set(df.columns) == {"id", "name", "score"}
assert df.filter(F.col("id") > 3).count() == 2
```

For pandas DataFrame assertions after `toPandas()`:

```python
pdf = df.toPandas()
assert len(pdf) == 5
assert list(pdf.columns) == ["id", "name", "score"]
pd.testing.assert_frame_equal(pdf, expected_pdf)
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
