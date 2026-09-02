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

import argparse
import os

from custom_ds import create_spark_session
from custom_ds.restapi import RestApiDataSource

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch read from REST API data source")
    parser.add_argument(
        "--url",
        default=os.environ.get("MOCK_SERVER_URL", "http://localhost:9090"),
        help="Mock server base URL (default: $MOCK_SERVER_URL or http://localhost:9090)",
    )
    args = parser.parse_args()

    spark = create_spark_session("restapi-batch-read")

    spark.dataSource.register(RestApiDataSource)

    df = (
        spark.read.format("restapi")
        .option("url", f"{args.url}/api/users")
        .option("method", "GET")
        .option("resultKey", "data")
        .load()
    )

    print("=== Users from REST API ===")
    df.printSchema()
    df.show(10, truncate=False)
    print(f"Total rows: {df.count()}")

    spark.stop()
