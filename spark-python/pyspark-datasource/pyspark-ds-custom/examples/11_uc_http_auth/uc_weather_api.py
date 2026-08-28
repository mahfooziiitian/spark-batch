"""Unity Catalog HTTP Connection — credential injection for REST APIs.

Demonstrates how to authenticate a PySpark custom data source with an
external HTTP API using a Unity Catalog HTTP connection. The data source
code never contains hardcoded tokens — credentials are injected at runtime.

Requirements:
    - Databricks Runtime 18.1+ (Unity Catalog HTTP connection injection)
    - A Unity Catalog HTTP connection (e.g., `my_weather_api`)
    - MANAGE permission on the connection

Architecture:
    ┌─────────────┐     ┌──────────────────────┐     ┌─────────────────┐
    │ Spark Driver │────▶│ Unity Catalog        │────▶│ External API    │
    │             │     │ (credential inject)  │     │ (weather, etc.) │
    └─────────────┘     └──────────────────────┘     └─────────────────┘

    1. User specifies `databricks.connection` option
    2. Spark retrieves short-lived credentials from Unity Catalog
    3. Credentials (host, base_path, bearer_token) injected into options
    4. Reader uses injected credentials — no secrets in code

Note:
    This example cannot run locally without a Databricks workspace.
    See `weather_api_local.py` for a local-runnable variant with env vars.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from custom_ds.uc_auth import WeatherApiSource

# --- On Databricks (DBR 18.1+) ---
spark = SparkSession.builder.appName("uc-http-auth").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Register the data source
spark.dataSource.register(WeatherApiSource)

# Unity Catalog injects host, base_path, and bearer_token automatically
df = (
    spark.read.format("weather_api")
    .option("databricks.connection", "my_weather_api")
    .option("cities", "Seattle,Portland,Denver")
    .load()
)

df.show(truncate=False)
df.printSchema()

# SQL access
df.createOrReplaceTempView("weather")
spark.sql("""
    SELECT city, temperature, description
    FROM weather
    WHERE temperature > 15
    ORDER BY temperature DESC
""").show()

spark.stop()
