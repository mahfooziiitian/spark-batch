"""REST API — read live weather data from OpenWeatherMap.

Key concepts:
    - Using RestApiDataSource with a real public API
    - Passing an API key via params.appid option
    - Schema definition for structured weather responses
    - Using resultKey to skip wrapper keys in the response

Prerequisites:
    export OPENWEATHER_API_KEY=<your-free-api-key>
    Sign up at: https://openweathermap.org/api
"""

from __future__ import annotations

import argparse
import os
import sys

from custom_ds import create_spark_session
from custom_ds.restapi import RestApiDataSource

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read live weather data from OpenWeatherMap")
    parser.add_argument(
        "--api-key",
        default=os.environ.get("OPENWEATHER_API_KEY", ""),
        help="OpenWeatherMap API key (default: $OPENWEATHER_API_KEY)",
    )
    args = parser.parse_args()

    api_key: str = args.api_key
    if not api_key:
        print("ERROR: Pass --api-key or set OPENWEATHER_API_KEY environment variable")
        print("  Sign up at: https://openweathermap.org/api")
        sys.exit(1)

    spark = create_spark_session("restapi-weather-read")
    spark.dataSource.register(RestApiDataSource)

    # Single city weather — API key passed as query parameter
    df = (
        spark.read.format("restapi")
        .option("url", "https://api.openweathermap.org/data/2.5/weather")
        .option("method", "GET")
        .option("params.q", "London")
        .option("params.units", "metric")
        .option("params.appid", api_key)
        .load()
    )

    print("=== London Weather (raw response) ===")
    df.printSchema()
    df.show(truncate=False)

    spark.stop()
