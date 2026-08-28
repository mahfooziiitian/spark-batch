# pyspark-ds-custom

Reference implementation of the **PySpark 4 Python Data Source API**
(`pyspark.sql.datasource`) — build custom Spark connectors in pure Python, no JVM/Scala
required. This project ships a reusable library (`custom_ds`) and runnable examples covering
batch reads, batch writes, streaming, and Spark SQL access.

## Requirements

- Python ≥ 3.11
- Java 17 (LTS)
- [uv](https://docs.astral.sh/uv/)

## Setup

```bash
uv sync
```

## Library (`src/custom_ds`)

### In-Memory Sources (demo/testing)

| Module | Provides |
|--------|----------|
| `custom_ds.session` | `create_spark_session()` — local `SparkSession` helper |
| `custom_ds.batch.simple_source` | `SimpleDataSource` — partitioned in-memory batch reader |
| `custom_ds.writer.simple_writer` | `SimpleSinkDataSource` — JSON-lines batch sink writer |
| `custom_ds.streaming.simple_stream_source` | `SimpleStreamDataSource` — incrementing counter stream |

### REST API Sources (real-world)

| Module | Provides |
|--------|----------|
| `custom_ds.restapi.rest_source` | `RestApiDataSource` — batch reader (HTTP GET → DataFrame) |
| `custom_ds.restapi.rest_writer` | `RestApiSinkDataSource` — batch writer (DataFrame → HTTP POST) |
| `custom_ds.restapi.rest_stream_source` | `RestApiStreamDataSource` — streaming reader (polls endpoint) |

### Utilities

| Module | Provides |
|--------|----------|
| `custom_ds.util.registration` | `register_all(spark)` — registers every data source at once |

```python
from custom_ds import create_spark_session, register_all

spark = create_spark_session("demo")
register_all(spark)

# Batch read from REST API
df = spark.read.format("restapi") \
    .option("url", "https://jsonplaceholder.typicode.com/users") \
    .load()
df.show()

# Batch read from in-memory source
spark.read.format("simple").option("numRows", 100).load().show()

spark.stop()
```

## Examples (`examples/`)

### Simple (in-memory) sources

```bash
uv run python examples/01_batch_read/read_simple_source.py
uv run python examples/02_batch_write/write_simple_sink.py
uv run python examples/03_streaming/stream_simple_source.py
uv run python examples/04_sql/query_custom_source_with_sql.py
```

### REST API sources (requires mock server)

```bash
# Terminal 1 — start mock API server
uv run python examples/mock_server/server.py

# Terminal 2 — run examples
uv run python examples/05_restapi_read/read_restapi_source.py
uv run python examples/06_restapi_write/write_restapi_sink.py
uv run python examples/07_restapi_stream/stream_restapi_source.py
uv run python examples/08_restapi_sql/query_restapi_with_sql.py
```

See `examples/README.md` for details on what each script demonstrates.

## Tests

```bash
uv run pytest -v
```

## Project Layout

```
pyspark-ds-custom/
├── src/custom_ds/
│   ├── batch/           # In-memory batch reader
│   ├── writer/          # JSON-lines batch sink
│   ├── streaming/       # Counter streaming source
│   ├── restapi/         # REST API batch reader, writer, and streaming source
│   ├── util/            # Registration helper
│   └── session.py       # SparkSession factory
├── examples/
│   ├── 01–04            # Simple in-memory examples
│   ├── 05–08            # REST API examples
│   └── mock_server/     # FastAPI mock server
├── tests/
├── pyproject.toml
└── .github/             # Modular Copilot instructions
```

## References

- [PySpark Python Data Source API tutorial](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html)
- [Announcing GA of the Python Data Source API (Databricks blog)](https://www.databricks.com/blog/announcing-general-availability-python-data-source-api)
- [Example Connectors Repository](https://github.com/databricks/pyspark-datasource-examples)
- [HuggingFace DataSource Connector](https://github.com/anandnalya/spark-huggingface)
