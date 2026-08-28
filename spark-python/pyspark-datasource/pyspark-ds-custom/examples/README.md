# Examples

Runnable demos of the PySpark 4 Python Data Source API, built on the
`custom_ds` library in `src/`. Each script is self-contained and runs
locally with `local[*]`.

| Folder | Demonstrates |
|--------|--------------|
| `01_batch_read/` | Registering and reading a partitioned batch `DataSource` |
| `02_batch_write/` | Writing a DataFrame via a custom `DataSourceWriter` sink |
| `03_streaming/` | Streaming reads with `SimpleDataSourceStreamReader` |
| `04_sql/` | Querying a custom data source through Spark SQL temp views |
| `05_restapi_read/` | Batch reading from a REST API endpoint (HTTP GET → DataFrame) |
| `06_restapi_write/` | Batch writing to a REST API endpoint (DataFrame → HTTP POST) |
| `07_restapi_stream/` | Streaming reads from a REST API with offset tracking |
| `08_restapi_sql/` | SQL analytics over REST API data via temp views |
| `09_restapi_partitioned/` | Parallel reads: URL-based and page-based partitioning |
| `10_community_sources/` | Using `pyspark-data-sources` community connectors (Fake, GitHub) |
| `11_uc_http_auth/` | Unity Catalog HTTP connection — secure credential injection |
| `mock_server/` | FastAPI mock server used by examples 05–09 |

## Run (simple in-memory sources)

```bash
uv run python examples/01_batch_read/read_simple_source.py
uv run python examples/02_batch_write/write_simple_sink.py
uv run python examples/03_streaming/stream_simple_source.py
uv run python examples/04_sql/query_custom_source_with_sql.py
```

## Run (REST API examples)

Start the mock server in one terminal, then run examples:

```bash
# Terminal 1 — start mock API
uv run python examples/mock_server/server.py

# Terminal 2 — run REST API examples
uv run python examples/05_restapi_read/read_restapi_source.py
uv run python examples/06_restapi_write/write_restapi_sink.py
uv run python examples/07_restapi_stream/stream_restapi_source.py
uv run python examples/08_restapi_sql/query_restapi_with_sql.py
uv run python examples/09_restapi_partitioned/partitioned_restapi_read.py
```

## Run (community data sources — `pyspark-data-sources` package)

These examples use the [`pyspark-data-sources`](https://github.com/allisonwang-db/pyspark-data-sources)
community library. Install with `uv sync` (already in dev deps).

```bash
# Fake data + GitHub PRs
uv run python examples/10_community_sources/fake_and_github.py

# Streaming fake data
uv run python examples/10_community_sources/streaming_fake_data.py

# SQL queries over synthetic data
uv run python examples/10_community_sources/sql_with_fake_data.py

# Mixed pipeline (community + custom) — requires mock server running
uv run python examples/10_community_sources/mixed_pipeline.py
```
