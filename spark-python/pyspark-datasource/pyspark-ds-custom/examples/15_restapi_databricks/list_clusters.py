"""REST API — list Databricks clusters using the Clusters API.

Key concepts:
    - Reading cluster metadata from the Databricks REST API
    - Auto-resolving host and token from a Databricks CLI profile
    - Filtering and transforming nested JSON with PySpark
    - Analyzing cluster configurations via SQL

Prerequisites:
    databricks auth login --profile dev

Usage:
    uv run python examples/15_restapi_databricks/list_clusters.py --profile dev
    # or: export DATABRICKS_PROFILE=dev
    # or: export DATABRICKS_HOST=... DATABRICKS_TOKEN=...
"""

from __future__ import annotations

from pyspark.sql import functions as F

from custom_ds import create_spark_session
from custom_ds.restapi import RestApiDataSource
from custom_ds.util.databricks_auth import get_databricks_auth, parse_profile_arg

if __name__ == "__main__":
    auth = get_databricks_auth(parse_profile_arg())

    spark = create_spark_session("restapi-databricks-clusters")
    spark.dataSource.register(RestApiDataSource)

    url = f"{auth.host}/api/2.0/clusters/list"

    df = (
        spark.read.format("restapi")
        .option("url", url)
        .option("method", "GET")
        .option("headers.Authorization", f"Bearer {auth.token}")
        .option("resultKey", "clusters")
        .load()
    )

    print("=== Databricks Clusters ===")

    clusters = df.select(
        F.col("cluster_id"),
        F.col("cluster_name"),
        F.col("state"),
        F.col("spark_version"),
        F.col("node_type_id"),
        F.col("creator_user_name").alias("creator"),
        F.col("cluster_source"),
    )

    clusters.show(truncate=False)
    print(f"Total clusters: {clusters.count()}")

    # Analytics
    clusters.createOrReplaceTempView("clusters")

    print("=== Clusters by State ===")
    spark.sql("""
        SELECT state, COUNT(*) AS count
        FROM clusters
        GROUP BY state
        ORDER BY count DESC
    """).show()

    print("=== Clusters by Spark Version ===")
    spark.sql("""
        SELECT spark_version, COUNT(*) AS count
        FROM clusters
        GROUP BY spark_version
        ORDER BY count DESC
    """).show(truncate=False)

    spark.stop()
