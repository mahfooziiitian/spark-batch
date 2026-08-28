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

from pyspark.sql import functions as F

from custom_ds import create_spark_session
from custom_ds.restapi import RestApiSinkDataSource

if __name__ == "__main__":
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
    df.write.format("restapi_sink").option("url", "http://localhost:9090/api/records").option(
        "batchSize", "10"
    ).mode("append").save()

    print("Write complete — check mock server /api/records endpoint for stored data")

    spark.stop()
