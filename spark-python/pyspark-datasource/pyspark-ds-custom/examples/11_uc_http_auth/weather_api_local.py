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

import os

from custom_ds import create_spark_session
from custom_ds.uc_auth import WeatherApiSource

spark = create_spark_session("weather-api-local")

spark.dataSource.register(WeatherApiSource)

# Read credentials from environment (mimics UC injection)
host = os.environ.get("WEATHER_API_HOST", "https://api.openweathermap.org")
base_path = os.environ.get("WEATHER_API_BASE_PATH", "/data/2.5")
token = os.environ.get("WEATHER_API_TOKEN", "")

if not token:
    print("⚠️  WEATHER_API_TOKEN not set — API calls will fail with 401.")
    print("   Get a free key at: https://openweathermap.org/api")
    print("   Then: export WEATHER_API_TOKEN=your_key")
    print()

df = (
    spark.read.format("weather_api")
    .option("host", host)
    .option("base_path", base_path)
    .option("bearer_token", token)
    .option("cities", "Seattle,Portland,Denver")
    .load()
)

df.show(truncate=False)

spark.stop()
