"""Streaming read — poll a REST API for new events using `restapi_stream`.

Key concepts:
    - SimpleDataSourceStreamReader polling an HTTP endpoint
    - Offset tracking via a record field (e.g. `id`)
    - Micro-batch ingestion with console sink for visibility
    - Configurable offset parameter name

Prerequisites:
    Start the mock server first:
        uv run python examples/mock_server/server.py
"""

from __future__ import annotations

from custom_ds import create_spark_session
from custom_ds.restapi import RestApiStreamDataSource

if __name__ == "__main__":
    spark = create_spark_session("restapi-stream-read")

    spark.dataSource.register(RestApiStreamDataSource)

    stream_df = (
        spark.readStream.format("restapi_stream")
        .option("url", "http://localhost:9090/api/events")
        .option("offsetParam", "since_id")
        .option("offsetKey", "id")
        .option("limit", "10")
        .option("schema", "id LONG, event STRING, timestamp STRING")
        .load()
    )

    print("=== Streaming events from REST API (10s window) ===")

    query = (
        stream_df.writeStream.format("console")
        .outputMode("append")
        .trigger(processingTime="3 seconds")
        .start()
    )

    query.awaitTermination(timeout=10)
    query.stop()

    spark.stop()
