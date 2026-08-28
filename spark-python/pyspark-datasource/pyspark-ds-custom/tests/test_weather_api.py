"""Tests for WeatherApiSource with a local mock weather server."""

from __future__ import annotations

import threading
import time

import pytest
import uvicorn
from fastapi import FastAPI, Header, HTTPException

from custom_ds import WeatherApiSource, create_spark_session

# ---------------------------------------------------------------------------
# Mock Weather API server
# ---------------------------------------------------------------------------
mock_weather_app = FastAPI()

MOCK_WEATHER_DATA = {
    "Seattle": {"temp": 12.5, "humidity": 78, "description": "overcast clouds"},
    "Portland": {"temp": 14.2, "humidity": 65, "description": "light rain"},
    "Denver": {"temp": 22.1, "humidity": 30, "description": "clear sky"},
}


@mock_weather_app.get("/data/2.5/weather")
def get_weather(q: str, units: str = "metric", authorization: str = Header(None)):
    if authorization != "Bearer test-token-123":
        raise HTTPException(status_code=401, detail="Unauthorized")
    city_data = MOCK_WEATHER_DATA.get(q)
    if not city_data:
        raise HTTPException(status_code=404, detail=f"City not found: {q}")
    return {
        "name": q,
        "main": {"temp": city_data["temp"], "humidity": city_data["humidity"]},
        "weather": [{"description": city_data["description"]}],
    }


@pytest.fixture(scope="module")
def weather_server():
    """Start mock weather server on port 19094."""
    config = uvicorn.Config(mock_weather_app, host="127.0.0.1", port=19094, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    time.sleep(1)
    yield "http://127.0.0.1:19094"
    server.should_exit = True


@pytest.fixture(scope="module")
def spark():
    session = create_spark_session("test-weather-api")
    session.dataSource.register(WeatherApiSource)
    yield session
    session.stop()


class TestWeatherApiSource:
    """Test WeatherApiSource with mock server (simulates UC credential injection)."""

    def test_read_all_cities(self, spark, weather_server):
        """Reads weather for multiple cities via partitioned reads."""
        df = (
            spark.read.format("weather_api")
            .option("host", weather_server)
            .option("base_path", "/data/2.5")
            .option("bearer_token", "test-token-123")
            .option("cities", "Seattle,Portland,Denver")
            .load()
        )

        assert df.count() == 3
        rows = {r["city"]: r for r in df.collect()}
        assert rows["Seattle"]["temperature"] == 12.5
        assert rows["Portland"]["humidity"] == 65
        assert rows["Denver"]["description"] == "clear sky"

    def test_single_city(self, spark, weather_server):
        """Single city partition."""
        df = (
            spark.read.format("weather_api")
            .option("host", weather_server)
            .option("base_path", "/data/2.5")
            .option("bearer_token", "test-token-123")
            .option("cities", "Denver")
            .load()
        )

        assert df.count() == 1
        row = df.collect()[0]
        assert row["city"] == "Denver"
        assert row["temperature"] == 22.1

    def test_unauthorized_fails(self, spark, weather_server):
        """Missing or wrong token raises an error."""
        df = (
            spark.read.format("weather_api")
            .option("host", weather_server)
            .option("base_path", "/data/2.5")
            .option("bearer_token", "wrong-token")
            .option("cities", "Seattle")
            .load()
        )

        with pytest.raises(Exception):  # noqa: B017
            df.collect()

    def test_schema(self, spark, weather_server):
        """Verify output schema matches expected fields."""
        df = (
            spark.read.format("weather_api")
            .option("host", weather_server)
            .option("base_path", "/data/2.5")
            .option("bearer_token", "test-token-123")
            .option("cities", "Seattle")
            .load()
        )

        field_names = [f.name for f in df.schema.fields]
        assert field_names == ["city", "temperature", "humidity", "description"]
