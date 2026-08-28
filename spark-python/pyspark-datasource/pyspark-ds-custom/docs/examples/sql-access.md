# SQL Access

Query REST API data using Spark SQL via temporary views.

## Run

```bash
# Start mock server (separate terminal)
uv run python examples/mock_server/server.py

# Run example
uv run python examples/08_restapi_sql/query_restapi_with_sql.py
```

## Code

```python title="examples/08_restapi_sql/query_restapi_with_sql.py"
--8<-- "examples/08_restapi_sql/query_restapi_with_sql.py"
```

## Key Concepts

1. **Any DataSource-backed DataFrame** can be exposed as a temp view
2. **Downstream SQL consumers** are unaware of the underlying custom connector
3. Enables standard SQL analytics over external API data

## Pattern

```python
# Load from custom source
spark.read.format("restapi") \
    .option("url", "http://api/data") \
    .option("resultKey", "items") \
    .load() \
    .createOrReplaceTempView("my_api_data")

# Query with SQL — just like any other table
spark.sql("""
    SELECT category, COUNT(*) as cnt
    FROM my_api_data
    GROUP BY category
    ORDER BY cnt DESC
""").show()
```

!!! tip "Unity Catalog integration"
    On Databricks, you can save custom data source results as managed tables:
    ```python
    df.write.saveAsTable("main.default.api_data")
    ```
    This brings governance, lineage, and access control to your API data.

## Expected Output

```
=== SQL: All users over age 50 ===
+---+------------------+---------------------------+---+
|id |name              |email                      |age|
+---+------------------+---------------------------+---+
|1  |Allison Hill      |travisgriffin@example.com  |56 |
...

=== SQL: Count by city ===
+----------------+----------+
|city            |user_count|
+----------------+----------+
|Michaelchester  |3         |
...
```
