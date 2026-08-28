"""Streaming fake data — continuous synthetic data generation.

Uses the `pyspark-data-sources` FakeDataSource in streaming mode to generate
micro-batches of synthetic records, simulating a real-time ingestion pipeline.

Install:
    uv add pyspark-data-sources

Key concepts:
    - FakeDataSource supports both batch and streaming reads
    - `rowsPerMicrobatch` controls how many records each micro-batch produces
    - Can be piped into any Spark streaming sink (console, delta, memory, custom)
"""

from __future__ import annotations

from pyspark_datasources import FakeDataSource

from custom_ds import create_spark_session

if __name__ == "__main__":
    spark = create_spark_session("fake-streaming")

    spark.dataSource.register(FakeDataSource)

    # Stream synthetic data with a custom schema
    stream_df = (
        spark.readStream.format("fake")
        .schema("name string, email string, city string")
        .option("rowsPerMicrobatch", 5)
        .load()
    )

    print("=== Streaming synthetic data (10s window) ===")

    query = (
        stream_df.writeStream.format("console")
        .outputMode("append")
        .trigger(processingTime="3 seconds")
        .start()
    )

    query.awaitTermination(timeout=10)
    query.stop()

    spark.stop()
