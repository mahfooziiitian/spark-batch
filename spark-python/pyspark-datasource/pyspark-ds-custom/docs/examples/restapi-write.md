# REST API Write

Write DataFrame rows to a REST API endpoint via HTTP POST.

## Run

```bash
# Start mock server (separate terminal)
uv run python examples/mock_server/server.py

# Run example
uv run python examples/06_restapi_write/write_restapi_sink.py
```

## Code

```python title="examples/06_restapi_write/write_restapi_sink.py"
--8<-- "examples/06_restapi_write/write_restapi_sink.py"
```

## Key Concepts

1. **Batch size** controls how many rows are sent per HTTP request
2. **CommitMessage** aggregation on the driver reports total rows/requests
3. Rows are serialized as a JSON array in each POST body

## Expected Output

```
=== Writing to REST API ===
+---+-------+
| id|  value|
+---+-------+
|  0| item-0|
|  1| item-1|
|  2| item-2|
|  3| item-3|
|  4| item-4|
+---+-------+
only showing top 5 rows

[restapi_sink] committed 20 rows via 2 HTTP requests
Write complete — check mock server /api/records endpoint for stored data
```
