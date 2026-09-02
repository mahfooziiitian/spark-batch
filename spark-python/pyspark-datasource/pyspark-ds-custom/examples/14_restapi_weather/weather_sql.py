"""REST API — weather data with SQL queries and transformations.

Key concepts:
    - Reading weather data into a temp view for SQL analysis
    - Using the WeatherApiSource (custom data source) for structured output
    - SQL aggregations and filtering on live API data
    - Comparing both RestApiDataSource and WeatherApiSource approaches

Prerequisites:
    export OPENWEATHER_API_KEY=<your-free-api-key>
"""

from __future__ import annotations

import argparse
import os
import sys

from custom_ds import WeatherApiSource, create_spark_session

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weather SQL analytics via WeatherApiSource")
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

    spark = create_spark_session("restapi-weather-sql")
    spark.dataSource.register(WeatherApiSource)

    # Read weather using WeatherApiSource — returns clean schema
    df = (
        spark.read.format("weather_api")
        .option("host", "https://api.openweathermap.org")
        .option("base_path", "/data/2.5")
        .option("bearer_token", api_key)
        .option("cities", "London,Tokyo,New York,Sydney,Mumbai,Dubai,Paris")
        .load()
    )

    df.createOrReplaceTempView("weather")

    print("=== All Cities ===")
    df.show(truncate=False)

    # Hottest and coldest
    print("=== Hottest City ===")
    spark.sql("""
        SELECT city, temperature, description
        FROM weather
        ORDER BY temperature DESC
        LIMIT 1
    """).show(truncate=False)

    print("=== Coldest City ===")
    spark.sql("""
        SELECT city, temperature, description
        FROM weather
        ORDER BY temperature ASC
        LIMIT 1
    """).show(truncate=False)

    # Average temperature and humidity
    print("=== Averages ===")
    spark.sql("""
        SELECT
            ROUND(AVG(temperature), 1) AS avg_temp_c,
            ROUND(AVG(humidity), 0)    AS avg_humidity,
            COUNT(*)                   AS city_count
        FROM weather
    """).show()

    # Cities above 25°C
    print("=== Hot Cities (> 25°C) ===")
    spark.sql("""
        SELECT city, temperature, humidity, description
        FROM weather
        WHERE temperature > 25
        ORDER BY temperature DESC
    """).show(truncate=False)

    spark.stop()
