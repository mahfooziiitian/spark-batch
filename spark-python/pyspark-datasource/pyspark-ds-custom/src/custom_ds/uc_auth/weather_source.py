"""Weather API data source with Unity Catalog HTTP connection support.

This module implements a PySpark custom data source that reads weather data
from an external REST API. It is designed to work with Unity Catalog HTTP
connections (DBR 18.1+), where credentials are injected automatically via
the ``databricks.connection`` option.

The same source works locally when you pass ``host``, ``base_path``, and
``bearer_token`` as explicit options (e.g., from environment variables).

Architecture:
    - Each city gets its own InputPartition for parallel reads
    - Credentials come from injected options (never hardcoded)
    - Uses only stdlib (urllib) for HTTP — no extra deps on workers
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition


@dataclass
class CityPartition(InputPartition):
    """One partition per city for parallel weather fetches."""

    city: str


class WeatherApiReader(DataSourceReader):
    """Reads weather data from OpenWeatherMap-compatible APIs.

    Options consumed (injected by Unity Catalog or set explicitly):
        host: API base URL (e.g., https://api.openweathermap.org)
        base_path: Path prefix (e.g., /data/2.5)
        bearer_token: Authentication token
        cities: Comma-separated city names (default: Seattle,Portland,Denver)
    """

    def __init__(self, options: dict) -> None:
        self.host = options["host"]
        self.base_path = options.get("base_path", "")
        self.token = options.get("bearer_token", "")
        self.cities = options.get("cities", "Seattle,Portland,Denver").split(",")

    def partitions(self) -> list[InputPartition]:
        """One partition per city — enables parallel reads across workers."""
        return [CityPartition(city.strip()) for city in self.cities]

    def read(self, partition: InputPartition):
        """Fetch weather for a single city. Runs on workers.

        Uses only stdlib to avoid serialization issues with third-party libs.
        """
        import json
        import urllib.error
        import urllib.request
        from urllib.parse import quote

        city = cast(CityPartition, partition).city
        url = f"{self.host}{self.base_path}/weather?q={quote(city)}&units=metric"

        req = urllib.request.Request(url)  # noqa: S310
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310
                data = json.loads(resp.read().decode())
        except (urllib.error.URLError, TimeoutError) as e:
            raise RuntimeError(f"Weather API request failed for {city}: {e}") from e

        try:
            main = data["main"]
            weather = data["weather"][0]
        except (KeyError, IndexError, TypeError) as e:
            raise RuntimeError(f"Unexpected weather API response for {city}: {data}") from e

        yield (city, float(main["temp"]), int(main["humidity"]), weather["description"])


class WeatherApiSource(DataSource):
    """PySpark data source for weather APIs with UC HTTP connection support.

    Usage (Databricks with UC HTTP connection):
        spark.dataSource.register(WeatherApiSource)
        df = (spark.read.format("weather_api")
              .option("databricks.connection", "my_weather_api")
              .option("cities", "Seattle,Portland,Denver")
              .load())

    Usage (local with explicit credentials):
        df = (spark.read.format("weather_api")
              .option("host", "https://api.openweathermap.org")
              .option("base_path", "/data/2.5")
              .option("bearer_token", os.environ["API_KEY"])
              .option("cities", "Seattle,Portland,Denver")
              .load())

    Schema: city STRING, temperature DOUBLE, humidity INT, description STRING
    """

    @classmethod
    def name(cls) -> str:
        return "weather_api"

    def schema(self) -> str:
        return "city STRING, temperature DOUBLE, humidity INT, description STRING"

    def reader(self, schema) -> WeatherApiReader:
        return WeatherApiReader(self.options)
