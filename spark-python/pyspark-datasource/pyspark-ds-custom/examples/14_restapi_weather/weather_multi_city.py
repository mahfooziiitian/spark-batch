"""REST API — read weather for multiple cities using URL-based partitioning.

Key concepts:
    - URL-based partitioning: one Spark task per city, fetched in parallel
    - Building per-city URLs with API key as query parameter
    - Extracting nested fields (main.temp, weather[0].description) via schema
    - Combining parallel partition results into a single DataFrame

Prerequisites:
    export OPENWEATHER_API_KEY=<your-free-api-key>
"""

from __future__ import annotations

import os
import sys
from urllib.parse import urlencode

from custom_ds import create_spark_session
from custom_ds.restapi import RestApiDataSource

if __name__ == "__main__":
    api_key = os.environ.get("OPENWEATHER_API_KEY", "")
    if not api_key:
        print("ERROR: Set OPENWEATHER_API_KEY environment variable")
        sys.exit(1)

    spark = create_spark_session("restapi-weather-cities")
    spark.dataSource.register(RestApiDataSource)

    base = "https://api.openweathermap.org/data/2.5/weather"
    cities = ["London", "Tokyo", "New York", "Sydney", "Mumbai"]

    # Build one URL per city — each becomes a separate Spark partition
    urls = ",".join(
        f"{base}?{urlencode({'q': city, 'units': 'metric', 'appid': api_key})}" for city in cities
    )

    df = (
        spark.read.format("restapi").option("partitionStrategy", "urls").option("urls", urls).load()
    )

    print(f"=== Weather for {len(cities)} Cities (parallel partitions) ===")
    df.printSchema()
    df.show(truncate=False)
    print(f"Partitions: {df.rdd.getNumPartitions()}")

    spark.stop()
