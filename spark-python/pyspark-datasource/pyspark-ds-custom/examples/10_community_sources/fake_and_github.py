"""Community data sources — using the `pyspark-data-sources` library.

Demonstrates using community-built Python Data Source connectors from
the `pyspark-data-sources` package (https://github.com/allisonwang-db/pyspark-data-sources)
alongside our custom_ds library. Both use the same pyspark.sql.datasource API.

Install:
    uv add pyspark-data-sources

Key concepts:
    - Community connectors register the same way as custom ones
    - Multiple data sources can coexist in the same SparkSession
    - spark.dataSource.register() works identically for all Python Data Sources
"""

from __future__ import annotations

# Import community data sources
from pyspark_datasources import FakeDataSource, GithubDataSource

from custom_ds import create_spark_session

if __name__ == "__main__":
    spark = create_spark_session("community-sources")

    # Register community data sources
    spark.dataSource.register(FakeDataSource)
    spark.dataSource.register(GithubDataSource)

    # -------------------------------------------------------------------------
    # 1. Fake Data Source — generate synthetic test data with Faker
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("FakeDataSource — synthetic test data (Faker)")
    print("=" * 60)

    # Default schema: name, date, zipcode, state
    df_fake = spark.read.format("fake").option("numRows", 5).load()
    df_fake.show(truncate=False)

    # Custom schema — field names must match Faker provider methods
    df_custom = (
        spark.read.format("fake")
        .schema("name string, email string, company string, city string")
        .option("numRows", 5)
        .load()
    )
    print("Custom schema:")
    df_custom.show(truncate=False)

    # -------------------------------------------------------------------------
    # 2. GitHub Data Source — read pull requests from a public repository
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("GithubDataSource — pull requests from apache/spark")
    print("=" * 60)

    df_github = spark.read.format("github").load("apache/spark")
    df_github.select("id", "title", "author", "created_at").show(5, truncate=False)

    print(f"Total PRs fetched: {df_github.count()}")
    df_github.printSchema()

    spark.stop()
