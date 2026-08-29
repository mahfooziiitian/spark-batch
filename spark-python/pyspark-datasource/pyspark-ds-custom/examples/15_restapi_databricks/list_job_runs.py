"""REST API — list Databricks job runs with page-based partitioning.

Key concepts:
    - Auto-resolving host and token from a Databricks CLI profile
    - Fetching job run history with status and duration analysis
    - Bearer token authentication via headers option
    - SQL analytics on job run metadata (success rate, duration stats)

Prerequisites:
    databricks auth login --profile dev

Usage:
    uv run python examples/15_restapi_databricks/list_job_runs.py --profile dev
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

    spark = create_spark_session("restapi-databricks-runs")
    spark.dataSource.register(RestApiDataSource)

    url = f"{auth.host}/api/2.2/jobs/runs/list"

    # Single fetch — recent runs
    df = (
        spark.read.format("restapi")
        .option("url", url)
        .option("method", "GET")
        .option("headers.Authorization", f"Bearer {auth.token}")
        .option("params.limit", "100")
        .option("params.expand_tasks", "false")
        .option("resultKey", "runs")
        .load()
    )

    print("=== Recent Job Runs ===")

    runs = df.select(
        F.col("run_id"),
        F.col("run_name"),
        F.col("state.result_state").alias("result"),
        F.col("state.life_cycle_state").alias("lifecycle"),
        F.col("start_time"),
        F.col("end_time"),
        F.col("creator_user_name").alias("creator"),
        F.col("run_type"),
    )

    runs.show(20, truncate=False)
    print(f"Total runs fetched: {runs.count()}")

    # Analytics
    runs.createOrReplaceTempView("runs")

    print("=== Runs by Result State ===")
    spark.sql("""
        SELECT result, COUNT(*) AS count
        FROM runs
        GROUP BY result
        ORDER BY count DESC
    """).show()

    print("=== Runs by Creator ===")
    spark.sql("""
        SELECT creator, COUNT(*) AS count,
               SUM(CASE WHEN result = 'SUCCESS' THEN 1 ELSE 0 END) AS successes
        FROM runs
        GROUP BY creator
        ORDER BY count DESC
    """).show(truncate=False)

    print("=== Run Types ===")
    spark.sql("""
        SELECT run_type, COUNT(*) AS count
        FROM runs
        GROUP BY run_type
        ORDER BY count DESC
    """).show()

    spark.stop()
