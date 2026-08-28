# Examples

Runnable demos of the PySpark 4 Python Data Source API. All examples run locally
with `local[*]` — no cluster required.

## Prerequisites

```bash
uv sync
```

## Mock Server

REST API examples (05–09) require the mock FastAPI server:

```bash
uv run python examples/mock_server/server.py
```

This starts on `http://localhost:9090` with these endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/users` | GET | 50 users (paginated by offset) |
| `/api/users/{id}` | GET | Single user by ID |
| `/api/posts` | GET | 100 posts (paginated by page) |
| `/api/events` | GET | Events since a given ID (streaming) |
| `/api/records` | POST | Accepts JSON array (write sink) |
| `/api/records` | GET | Returns all written records |

## Example Index

| # | Script | Demonstrates |
|---|--------|---|
| 01 | `01_batch_read/read_simple_source.py` | In-memory partitioned batch read |
| 02 | `02_batch_write/write_simple_sink.py` | JSON-lines file sink |
| 03 | `03_streaming/stream_simple_source.py` | Counter streaming source |
| 04 | `04_sql/query_custom_source_with_sql.py` | SQL over custom source |
| 05 | `05_restapi_read/read_restapi_source.py` | REST API batch read |
| 06 | `06_restapi_write/write_restapi_sink.py` | REST API batch write |
| 07 | `07_restapi_stream/stream_restapi_source.py` | REST API streaming read |
| 08 | `08_restapi_sql/query_restapi_with_sql.py` | SQL over REST API data |
| 09 | `09_restapi_partitioned/partitioned_restapi_read.py` | 3 partitioning strategies |
