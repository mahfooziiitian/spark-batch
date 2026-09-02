"""Local-runnable variant of the UC HTTP auth example.

Uses environment variables instead of Unity Catalog credential injection,
so you can test the same WeatherApiSource locally. On Databricks, use
`databricks.connection` instead (see uc_weather_api.py).

Setup:
    export WEATHER_API_HOST=https://api.openweathermap.org
    export WEATHER_API_BASE_PATH=/data/2.5
    export WEATHER_API_TOKEN=your_openweathermap_api_key

Run:
    uv run python examples/11_uc_http_auth/weather_api_local.py
"""

from __future__ import annotations

import argparse
import os

from custom_ds import create_spark_session
from custom_ds.uc_auth import WeatherApiSource

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Local weather API via UC HTTP auth pattern")
    parser.add_argument(
        "--api-key",
        default=os.environ.get("WEATHER_API_TOKEN", ""),
        help="OpenWeatherMap API key (default: $WEATHER_API_TOKEN)",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("WEATHER_API_HOST", "https://api.openweathermap.org"),
        help="Weather API host (default: $WEATHER_API_HOST or https://api.openweathermap.org)",
    )
    parser.add_argument(
        "--base-path",
        default=os.environ.get("WEATHER_API_BASE_PATH", "/data/2.5"),
        help="Weather API base path (default: $WEATHER_API_BASE_PATH or /data/2.5)",
    )
    args = parser.parse_args()

    token: str = args.api_key
    if not token:
        print("⚠️  Pass --api-key or set WEATHER_API_TOKEN — API calls will fail with 401.")
        print("   Get a free key at: https://openweathermap.org/api")
        print()

    spark = create_spark_session("weather-api-local")
    spark.dataSource.register(WeatherApiSource)

    df = (
        spark.read.format("weather_api")
        .option("host", args.host)
        .option("base_path", args.base_path)
        .option("bearer_token", token)
        .option("cities", "Seattle,Portland,Denver")
        .load()
    )

    df.show(truncate=False)

    spark.stop()
