"""REST API — list Databricks jobs using the Jobs API.

Key concepts:
    - Using RestApiDataSource to call the Databricks REST API
    - Auto-resolving host and token from a Databricks CLI profile
    - Bearer token authentication via headers.Authorization
    - Navigating nested JSON responses with resultKey
    - SQL analytics over Databricks workspace metadata

Prerequisites:
    databricks auth login --profile dev

Usage:
    uv run python examples/15_restapi_databricks/list_jobs.py --profile dev
    # or: export DATABRICKS_PROFILE=dev
    # or: export DATABRICKS_HOST=... DATABRICKS_TOKEN=...
"""

from __future__ import annotations

from pyspark.sql import functions as F

from custom_ds import create_spark_session
from custom_ds.restapi import RestApiDataSource
from custom_ds.util.databricks_auth import create_arg_parser, get_databricks_auth

if __name__ == "__main__":
    parser = create_arg_parser("List Databricks jobs via REST API")
    args = parser.parse_args()
    auth = get_databricks_auth(args.profile)

    spark = create_spark_session("restapi-databricks-jobs")
    spark.dataSource.register(RestApiDataSource)

    # -------------------------------------------------------------------------
    # 1. List all jobs
    # -------------------------------------------------------------------------
    url = f"{auth.host}/api/2.2/jobs/list"

    df = (
        spark.read.format("restapi")
        .option("url", url)
        .option("method", "GET")
        .option("headers.Authorization", f"Bearer {auth.token}")
        .option("params.limit", "100")
        .option("params.expand_tasks", "false")
        .option("resultKey", "jobs")
        .load()
    )

    print("=== Databricks Jobs ===")
    df.printSchema()

    # Select key fields
    jobs = df.select(
        F.col("job_id"),
        F.col("settings.name").alias("job_name"),
        F.col("settings.format").alias("format"),
        F.col("creator_user_name").alias("creator"),
        F.col("created_time"),
    )

    jobs.show(50, truncate=False)
    print(f"Total jobs: {jobs.count()}")

    # -------------------------------------------------------------------------
    # 2. SQL analytics over jobs
    # -------------------------------------------------------------------------
    jobs.createOrReplaceTempView("jobs")

    print("=== Jobs per Creator ===")
    spark.sql("""
        SELECT creator, COUNT(*) AS job_count
        FROM jobs
        GROUP BY creator
        ORDER BY job_count DESC
    """).show(truncate=False)

    print("=== Jobs by Format ===")
    spark.sql("""
        SELECT format, COUNT(*) AS count
        FROM jobs
        GROUP BY format
        ORDER BY count DESC
    """).show()

    spark.stop()
