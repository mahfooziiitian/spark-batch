"""Integration tests for WeatherApiSource against the real OpenWeatherMap API.

Run with:
    OPENWEATHER_API_KEY=<your-key> uv run pytest tests/test_weather_api_integration.py -v

Requires:
    - A valid OpenWeatherMap API key (free tier works)
    - Network access to api.openweathermap.org
    - PySpark installed (uv sync --extra spark)
"""

from __future__ import annotations

import os

import pytest

from custom_ds import WeatherApiSource, create_spark_session

pytestmark = [pytest.mark.pyspark, pytest.mark.integration]

API_KEY = os.environ.get("OPENWEATHER_API_KEY", "")
HOST = "https://api.openweathermap.org"
BASE_PATH = "/data/2.5"

skip_no_key = pytest.mark.skipif(not API_KEY, reason="OPENWEATHER_API_KEY not set")


@pytest.fixture(scope="module")
def spark():
    session = create_spark_session("test-weather-integration")
    session.dataSource.register(WeatherApiSource)
    yield session
    session.stop()


@skip_no_key
class TestWeatherApiIntegration:
    """Integration tests against the live OpenWeatherMap API."""

    def test_read_single_city(self, spark):
        """Read weather for a single city from the real API."""
        df = (
            spark.read.format("weather_api")
            .option("host", HOST)
            .option("base_path", BASE_PATH)
            .option("bearer_token", API_KEY)
            .option("cities", "London")
            .load()
        )

        assert df.count() == 1
        row = df.collect()[0]
        assert row["city"] == "London"
        assert isinstance(row["temperature"], float)
        assert isinstance(row["humidity"], int)
        assert 0 <= row["humidity"] <= 100
        assert len(row["description"]) > 0

    def test_read_multiple_cities(self, spark):
        """Read weather for multiple cities — one partition per city."""
        cities = "London,Tokyo,New York"
        df = (
            spark.read.format("weather_api")
            .option("host", HOST)
            .option("base_path", BASE_PATH)
            .option("bearer_token", API_KEY)
            .option("cities", cities)
            .load()
        )

        assert df.count() == 3
        rows = {r["city"]: r for r in df.collect()}
        for city in ["London", "Tokyo", "New York"]:
            assert city in rows, f"Missing city: {city}"
            assert -60 <= rows[city]["temperature"] <= 60
            assert 0 <= rows[city]["humidity"] <= 100

    def test_schema_matches(self, spark):
        """Schema is city STRING, temperature DOUBLE, humidity INT, description STRING."""
        df = (
            spark.read.format("weather_api")
            .option("host", HOST)
            .option("base_path", BASE_PATH)
            .option("bearer_token", API_KEY)
            .option("cities", "Paris")
            .load()
        )

        field_names = [f.name for f in df.schema.fields]
        assert field_names == ["city", "temperature", "humidity", "description"]

    def test_invalid_api_key_fails(self, spark):
        """Invalid API key should raise an error."""
        df = (
            spark.read.format("weather_api")
            .option("host", HOST)
            .option("base_path", BASE_PATH)
            .option("bearer_token", "invalid-key-12345")
            .option("cities", "London")
            .load()
        )

        with pytest.raises(Exception):  # noqa: B017
            df.collect()

    def test_nonexistent_city_fails(self, spark):
        """Non-existent city should raise an error."""
        df = (
            spark.read.format("weather_api")
            .option("host", HOST)
            .option("base_path", BASE_PATH)
            .option("bearer_token", API_KEY)
            .option("cities", "Xyzzyville99999")
            .load()
        )

        with pytest.raises(Exception):  # noqa: B017
            df.collect()

    def test_temperature_is_reasonable(self, spark):
        """Temperature should be within a reasonable range (metric)."""
        df = (
            spark.read.format("weather_api")
            .option("host", HOST)
            .option("base_path", BASE_PATH)
            .option("bearer_token", API_KEY)
            .option("cities", "Dubai,Reykjavik")
            .load()
        )

        rows = df.collect()
        assert len(rows) == 2
        for row in rows:
            assert -60 <= row["temperature"] <= 60, (
                f"{row['city']}: temp {row['temperature']} out of range"
            )
