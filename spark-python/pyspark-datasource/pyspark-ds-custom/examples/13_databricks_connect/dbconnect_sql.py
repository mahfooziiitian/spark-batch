"""Databricks Connect — SQL queries over REST API data on a remote cluster.

Key concepts:
    - Registering a custom DataSource and creating temp views remotely
    - Auto-upload of custom_ds wheel via spark.addArtifact()
    - Aggregations and filters on API-sourced data
    - Identical API to local example 08

Prerequisites:
    - Databricks Connect configured (see README.md)
    - A REST API endpoint accessible from the cluster
"""

from __future__ import annotations

import os

from custom_ds import create_dbconnect_session
from custom_ds.restapi import RestApiDataSource

if __name__ == "__main__":
    spark = create_dbconnect_session("dbconnect-restapi-sql")

    spark.dataSource.register(RestApiDataSource)

    api_url = os.environ.get("REST_API_URL", "http://localhost:9090/api/users")

    # Read and register as temp view
    df = (
        spark.read.format("restapi")
        .option("url", api_url)
        .option("resultKey", "data")
        .option("schema", "id LONG, name STRING, email STRING, age LONG")
        .load()
    )

    df.createOrReplaceTempView("api_users")

    print("=== SQL Query: All Users (via Databricks Connect) ===")
    spark.sql("SELECT * FROM api_users").show(truncate=False)

    print("=== SQL Query: Count by Domain ===")
    spark.sql("""
        SELECT
            substring_index(email, '@', -1) AS domain,
            count(*) AS user_count
        FROM api_users
        WHERE email IS NOT NULL
        GROUP BY domain
        ORDER BY user_count DESC
    """).show(truncate=False)

    print("=== SQL Query: Age Statistics ===")
    spark.sql("""
        SELECT
            count(*) AS total,
            round(avg(age), 1) AS avg_age,
            min(age) AS min_age,
            max(age) AS max_age
        FROM api_users
        WHERE age IS NOT NULL
    """).show(truncate=False)

    spark.stop()
