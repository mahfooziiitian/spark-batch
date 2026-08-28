# REST API Read

Fetch data from a REST API endpoint into a Spark DataFrame.

## Run

```bash
# Start mock server (separate terminal)
uv run python examples/mock_server/server.py

# Run example
uv run python examples/05_restapi_read/read_restapi_source.py
```

## Code

```python title="examples/05_restapi_read/read_restapi_source.py"
--8<-- "examples/05_restapi_read/read_restapi_source.py"
```

## Key Concepts

1. **Register** the data source: `spark.dataSource.register(RestApiDataSource)`
2. **Read** with format API: `spark.read.format("restapi").option(...).load()`
3. **Navigate** nested JSON with `resultKey` — extracts the `"data"` array from the response

## Expected Output

```
=== Users from REST API ===
root
 |-- id: long (nullable = true)
 |-- name: string (nullable = true)
 |-- email: string (nullable = true)
 |-- city: string (nullable = true)
 |-- age: long (nullable = true)

+---+------------------+------------------------+---------------+---+
|id |name              |email                   |city           |age|
+---+------------------+------------------------+---------------+---+
|1  |Allison Hill      |travisgriffin@example.com|Michaelchester|56 |
|2  |Amanda Green      |dustin85@example.net    |Crystalberg    |44 |
...
```
