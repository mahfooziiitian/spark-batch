"""Mixed pipeline — combine community and custom data sources in one session.

Shows how multiple Python Data Sources (both community and custom-built)
coexist in the same SparkSession, enabling hybrid pipelines that read from
APIs, generate test data, and write results — all through the same format API.

Install:
    uv add pyspark-data-sources

Prerequisites:
    Start the mock server: uv run python examples/mock_server/server.py

Key concepts:
    - All Python Data Sources share the same registration mechanism
    - You can mix sources from different libraries in a single pipeline
    - Enables test data generation + real API reads + custom sinks
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark_datasources import FakeDataSource

from custom_ds import RestApiDataSource, RestApiSinkDataSource, create_spark_session

if __name__ == "__main__":
    spark = create_spark_session("mixed-pipeline")

    # Register both community and custom sources
    spark.dataSource.register(FakeDataSource)
    spark.dataSource.register(RestApiDataSource)
    spark.dataSource.register(RestApiSinkDataSource)

    # -------------------------------------------------------------------------
    # Step 1: Generate synthetic data with FakeDataSource
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("Step 1: Generate synthetic records with Faker")
    print("=" * 60)

    df_fake = (
        spark.read.format("fake")
        .schema("name string, email string, city string")
        .option("numRows", 10)
        .load()
    )
    df_fake = df_fake.withColumn("id", F.monotonically_increasing_id())
    df_fake.show(5, truncate=False)

    # -------------------------------------------------------------------------
    # Step 2: Read real data from REST API (custom source)
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("Step 2: Read users from REST API (custom_ds)")
    print("=" * 60)

    df_api = (
        spark.read.format("restapi")
        .option("url", "http://localhost:9090/api/users")
        .option("resultKey", "data")
        .option("schema", "id LONG, name STRING, email STRING, city STRING, age LONG")
        .load()
    )
    df_api.show(5, truncate=False)

    # -------------------------------------------------------------------------
    # Step 3: Combine and analyze
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("Step 3: Union both datasets and analyze")
    print("=" * 60)

    df_fake_normalized = df_fake.select(
        F.col("id").cast("long"),
        F.col("name"),
        F.col("email"),
        F.col("city"),
    )
    df_api_normalized = df_api.select("id", "name", "email", "city")

    df_combined = df_fake_normalized.unionByName(df_api_normalized)
    print(f"Combined dataset: {df_combined.count()} rows")

    # Create SQL view for analytics
    df_combined.createOrReplaceTempView("all_users")
    spark.sql("SELECT city, COUNT(*) as cnt FROM all_users GROUP BY city ORDER BY cnt DESC").show(
        10
    )

    # -------------------------------------------------------------------------
    # Step 4: Write combined results to REST API sink
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("Step 4: Write results to REST API (custom sink)")
    print("=" * 60)

    df_combined.select("id", F.col("name").alias("value")).write.format("restapi_sink").option(
        "url", "http://localhost:9090/api/records"
    ).option("batchSize", "20").mode("append").save()

    print("Pipeline complete!")

    spark.stop()
