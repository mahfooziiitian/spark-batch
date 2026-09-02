"""REST API — stream weather updates using RestApiStreamDataSource.

Key concepts:
    - Polling a weather API at regular intervals via streaming
    - Using micro-batch streaming to capture weather changes over time
    - Console sink for live monitoring of weather updates

Prerequisites:
    export OPENWEATHER_API_KEY=<your-free-api-key>
"""

from __future__ import annotations

import argparse
import os
import sys

from custom_ds import create_spark_session
from custom_ds.restapi import RestApiStreamDataSource

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream weather updates from OpenWeatherMap")
    parser.add_argument(
        "--api-key",
        default=os.environ.get("OPENWEATHER_API_KEY", ""),
        help="OpenWeatherMap API key (default: $OPENWEATHER_API_KEY)",
    )
    args = parser.parse_args()

    api_key: str = args.api_key
    if not api_key:
        print("ERROR: Pass --api-key or set OPENWEATHER_API_KEY environment variable")
        sys.exit(1)

    spark = create_spark_session("restapi-weather-stream")
    spark.dataSource.register(RestApiStreamDataSource)

    # Stream weather data — polls every trigger interval
    df = (
        spark.readStream.format("restapi_stream")
        .option("url", "https://api.openweathermap.org/data/2.5/weather")
        .option("params.q", "London")
        .option("params.units", "metric")
        .option("params.appid", api_key)
        .load()
    )

    print("=== Streaming London Weather (30s intervals, 2 min total) ===")
    query = (
        df.writeStream.format("console")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination(timeout=120)
    query.stop()

    spark.stop()
