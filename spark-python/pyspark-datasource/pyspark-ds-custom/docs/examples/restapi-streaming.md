# REST API Streaming

Poll a REST API endpoint for new records using Spark Structured Streaming.

## Run

```bash
# Start mock server (separate terminal)
uv run python examples/mock_server/server.py

# Run example (runs for 10 seconds)
uv run python examples/07_restapi_stream/stream_restapi_source.py
```

## Code

```python title="examples/07_restapi_stream/stream_restapi_source.py"
--8<-- "examples/07_restapi_stream/stream_restapi_source.py"
```

## Key Concepts

1. **Offset tracking** — the reader maintains an integer offset (`id` field)
2. **Polling** — each micro-batch calls `GET /events?since_id=<offset>&limit=10`
3. **Schema required** — streaming sources cannot auto-infer schema

## How Offset Tracking Works

```mermaid
sequenceDiagram
    participant Spark
    participant Reader
    participant API

    Note over Reader: initialOffset = {offset: 0}

    Spark->>Reader: read(start={offset: 0})
    Reader->>API: GET /events?since_id=0&limit=10
    API-->>Reader: [{id:1,...}, ..., {id:10,...}]
    Reader-->>Spark: 10 rows, {offset: 10}

    Spark->>Reader: read(start={offset: 10})
    Reader->>API: GET /events?since_id=10&limit=10
    API-->>Reader: [{id:11,...}, ..., {id:20,...}]
    Reader-->>Spark: 10 rows, {offset: 20}
```

## Expected Output

```
=== Streaming events from REST API (10s window) ===
-------------------------------------------
Batch: 0
-------------------------------------------
+---+--------+-------------------------+
| id|   event|              timestamp  |
+---+--------+-------------------------+
|  1|   login|2025-08-28T12:30:00+00:00|
|  2|purchase|2025-08-28T13:15:00+00:00|
...
```
