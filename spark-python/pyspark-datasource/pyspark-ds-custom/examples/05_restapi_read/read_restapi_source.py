"""Batch read — fetch data from a REST API using the `restapi` custom data source.

Key concepts:
    - Registering a Python DataSource that fetches from HTTP endpoints
    - Using `resultKey` to navigate nested JSON responses
    - Schema inference from the API response
    - Custom headers and query parameters via options

Prerequisites:
    Start the mock server first:
        uv run python examples/mock_server/server.py
"""

from __future__ import annotations

from custom_ds import create_spark_session
from custom_ds.restapi import RestApiDataSource

if __name__ == "__main__":
    spark = create_spark_session("restapi-batch-read")

    spark.dataSource.register(RestApiDataSource)

    # Read users from the mock API — `resultKey` extracts the "data" array from the response
    df = (
        spark.read.format("restapi")
        .option("url", "http://localhost:9090/api/users")
        .option("method", "GET")
        .option("resultKey", "data")
        .load()
    )

    print("=== Users from REST API ===")
    df.printSchema()
    df.show(10, truncate=False)
    print(f"Total rows: {df.count()}")

    spark.stop()
