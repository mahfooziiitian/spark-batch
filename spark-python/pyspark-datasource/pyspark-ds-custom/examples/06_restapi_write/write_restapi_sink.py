"""Batch write — POST DataFrame rows to a REST API using the `restapi_sink` data source.

Key concepts:
    - Implementing DataSourceWriter for HTTP POST endpoints
    - Batching rows into configurable-size JSON payloads
    - CommitMessage aggregation on the driver after all tasks complete

Prerequisites:
    Start the mock server first:
        uv run python examples/mock_server/server.py
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import functions as F

from custom_ds import create_spark_session
from custom_ds.restapi import RestApiSinkDataSource

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch write to REST API sink")
    parser.add_argument(
        "--url",
        default=os.environ.get("MOCK_SERVER_URL", "http://localhost:9090"),
        help="Mock server base URL (default: $MOCK_SERVER_URL or http://localhost:9090)",
    )
    args = parser.parse_args()

    spark = create_spark_session("restapi-batch-write")

    spark.dataSource.register(RestApiSinkDataSource)

    # Create sample data to write
    df = spark.range(20).select(
        F.col("id"),
        F.concat(F.lit("item-"), F.col("id").cast("string")).alias("value"),
    )

    print("=== Writing to REST API ===")
    df.show(5)

    # Write to the mock API endpoint — batchSize controls rows per HTTP request
    df.write.format("restapi_sink").option("url", f"{args.url}/api/records").option(
        "batchSize", "10"
    ).mode("append").save()

    print("Write complete — check mock server /api/records endpoint for stored data")

    spark.stop()
