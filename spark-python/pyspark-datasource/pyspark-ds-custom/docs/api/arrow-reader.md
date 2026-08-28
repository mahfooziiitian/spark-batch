# Arrow Reader — `RestApiArrowDataSource`

High-throughput batch reader that yields `pyarrow.RecordBatch` objects instead of
row tuples, providing up to **10x performance improvement** for large API responses.

## Format Name

```python
spark.read.format("restapi_arrow")
```

## Options

Same options as [`RestApiDataSource`](batch-reader.md) — all options are compatible.

## When to Use

!!! success "Good fit"
    - API responses with 1,000+ records per call
    - High-throughput ingestion pipelines
    - Scenarios where serialization overhead matters

!!! failure "Not a good fit"
    - Small APIs (< 100 records) — overhead of Arrow construction outweighs benefit
    - When you need partitioning strategies (use `restapi` format instead)

## How It Works

Instead of yielding one tuple per row:

```python
# Standard reader — one tuple per row (more serialization overhead)
def read(self, partition):
    for record in records:
        yield (record["id"], record["name"])
```

The Arrow reader yields entire `RecordBatch` objects:

```python
# Arrow reader — columnar batch (much less serialization)
def read(self, partition):
    import pyarrow as pa
    keys = pa.array([r["id"] for r in records], type=pa.int64())
    names = pa.array([r["name"] for r in records], type=pa.string())
    yield pa.RecordBatch.from_arrays([keys, names], schema=arrow_schema)
```

## Usage

```python
from custom_ds import create_spark_session, RestApiArrowDataSource

spark = create_spark_session("arrow-demo")
spark.dataSource.register(RestApiArrowDataSource)

df = spark.read.format("restapi_arrow") \
    .option("url", "http://localhost:9090/api/users") \
    .option("resultKey", "data") \
    .option("schema", "id LONG, name STRING, email STRING, age LONG") \
    .load()

df.show()
spark.stop()
```

## Type Mapping

| Spark Type | Arrow Type |
|---|---|
| `LongType` | `pa.int64()` |
| `StringType` | `pa.string()` |
| `DoubleType` | `pa.float64()` |
| `BooleanType` | `pa.bool_()` |

!!! note "PyArrow is required"
    The Python Data Source API requires `pyarrow>=14.0.0` as a runtime dependency.
    It's already listed in `pyproject.toml`.
