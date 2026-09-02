"""SQL access — query REST API data through Spark SQL using a temp view.

Key concepts:
    - Any DataSource-backed DataFrame works with createOrReplaceTempView
    - Spark SQL consumers are unaware of the underlying custom connector
    - Enables downstream SQL analytics over external API data

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
    parser = argparse.ArgumentParser(description="SQL queries over REST API data")
    parser.add_argument(
        "--url",
        default=os.environ.get("MOCK_SERVER_URL", "http://localhost:9090"),
        help="Mock server base URL (default: $MOCK_SERVER_URL or http://localhost:9090)",
    )
    args = parser.parse_args()

    spark = create_spark_session("restapi-sql")

    spark.dataSource.register(RestApiDataSource)

    (
        spark.read.format("restapi")
        .option("url", f"{args.url}/api/users")
        .option("resultKey", "data")
        .load()
        .createOrReplaceTempView("api_users")
    )

    print("=== SQL: All users over age 50 ===")
    spark.sql("SELECT id, name, email, age FROM api_users WHERE age > 50 ORDER BY age DESC").show(
        truncate=False
    )

    print("=== SQL: Count by city ===")
    spark.sql(
        "SELECT city, COUNT(*) as user_count FROM api_users GROUP BY city ORDER BY user_count DESC"
    ).show(10)

    spark.stop()
