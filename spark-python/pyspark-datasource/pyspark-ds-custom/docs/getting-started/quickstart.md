# Quick Start

Run your first custom data source in under a minute.

## 1. Start the Mock Server

```bash
uv run python examples/mock_server/server.py
```

This starts a FastAPI server on `http://localhost:9090` with endpoints for users, posts, and events.

## 2. Read from REST API

```python
from custom_ds import create_spark_session, RestApiDataSource

spark = create_spark_session("quickstart")
spark.dataSource.register(RestApiDataSource)

df = spark.read.format("restapi") \
    .option("url", "http://localhost:9090/api/users") \
    .option("resultKey", "data") \
    .load()

df.show(5, truncate=False)
spark.stop()
```

Or run the pre-built example:

```bash
uv run python examples/05_restapi_read/read_restapi_source.py
```

## 3. Write to REST API

```python
from pyspark.sql import functions as F
from custom_ds import create_spark_session, RestApiSinkDataSource

spark = create_spark_session("write-demo")
spark.dataSource.register(RestApiSinkDataSource)

df = spark.range(10).select(
    F.col("id"),
    F.concat(F.lit("item-"), F.col("id").cast("string")).alias("value"),
)

df.write.format("restapi_sink") \
    .option("url", "http://localhost:9090/api/records") \
    .option("batchSize", "5") \
    .mode("append") \
    .save()

spark.stop()
```

## 4. Query with SQL

```python
from custom_ds import create_spark_session, RestApiDataSource

spark = create_spark_session("sql-demo")
spark.dataSource.register(RestApiDataSource)

spark.read.format("restapi") \
    .option("url", "http://localhost:9090/api/users") \
    .option("resultKey", "data") \
    .load() \
    .createOrReplaceTempView("users")

spark.sql("SELECT name, email, age FROM users WHERE age > 40").show()
spark.stop()
```

## 5. Run Tests

```bash
uv run pytest -v
```

!!! success "All examples are self-contained"
    Each example script imports from `custom_ds`, registers its data source, and
    runs locally with `local[*]` — no cluster needed.
