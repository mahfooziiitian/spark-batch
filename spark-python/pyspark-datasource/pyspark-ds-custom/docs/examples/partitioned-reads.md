# Partitioned Reads

Parallel data loading using URL-based and page-based partitioning strategies.

## Run

```bash
# Start mock server (separate terminal)
uv run python examples/mock_server/server.py

# Run example
uv run python examples/09_restapi_partitioned/partitioned_restapi_read.py
```

## Code

```python title="examples/09_restapi_partitioned/partitioned_restapi_read.py"
--8<-- "examples/09_restapi_partitioned/partitioned_restapi_read.py"
```

## Strategy Comparison

### Single (baseline)

```python
df = spark.read.format("restapi") \
    .option("url", "http://localhost:9090/api/users") \
    .option("resultKey", "data") \
    .load()
# → 1 partition, 1 HTTP call
```

### URL-based (parallel by endpoint)

```python
urls = "http://api/users/1,http://api/users/2,http://api/users/3"

df = spark.read.format("restapi") \
    .option("partitionStrategy", "urls") \
    .option("urls", urls) \
    .load()
# → 3 partitions, 3 concurrent HTTP calls
```

### Page-based (parallel by page)

```python
df = spark.read.format("restapi") \
    .option("partitionStrategy", "pages") \
    .option("url", "http://api/posts") \
    .option("totalPages", "4") \
    .option("pageSize", "25") \
    .option("resultKey", "data") \
    .load()
# → 4 partitions, 4 concurrent HTTP calls
```

## Expected Output

```
============================================================
Strategy: SINGLE (default)
============================================================
Partitions: 1
Rows: 50

============================================================
Strategy: URLS (3 URLs → 3 partitions)
============================================================
Partitions: 3
Rows: 3
+---------+-----+
|partition|count|
+---------+-----+
|        0|    1|
|        1|    1|
|        2|    1|
+---------+-----+

============================================================
Strategy: PAGES (4 pages × 25 rows = 100 rows)
============================================================
Partitions: 4
Rows: 100
+---------+-----+
|partition|count|
+---------+-----+
|        0|   25|
|        1|   25|
|        2|   25|
|        3|   25|
+---------+-----+
```
