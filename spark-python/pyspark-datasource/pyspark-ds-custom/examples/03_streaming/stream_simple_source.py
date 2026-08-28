"""Streaming read — consume the `simple_stream` counter source with a console sink.

Key concepts:
    - Implementing SimpleDataSourceStreamReader (initialOffset / read / readBetweenOffsets)
    - readStream.format(...).load() for structured streaming
    - Running a query for a bounded time with awaitTermination(timeout)
"""

from __future__ import annotations

from custom_ds import SimpleStreamDataSource, create_spark_session

if __name__ == "__main__":
    spark = create_spark_session("simple-stream-read")

    spark.dataSource.register(SimpleStreamDataSource)

    stream_df = spark.readStream.format("simple_stream").option("rowsPerBatch", 5).load()

    query = (
        stream_df.writeStream.format("console")
        .outputMode("append")
        .trigger(processingTime="2 seconds")
        .start()
    )

    query.awaitTermination(timeout=10)
    query.stop()

    spark.stop()
