"""Databricks Connect — batch read from a REST API on a remote cluster.

Key concepts:
    - Running custom Python Data Source reads via Databricks Connect
    - Registering a DataSource on a remote cluster
    - Auto-upload of custom_ds wheel via spark.addArtifact()
    - Identical API to local mode — only the session changes

Prerequisites:
    databricks auth login --profile dev

Usage:
    uv run python examples/13_databricks_connect/dbconnect_batch_read.py --profile dev
    # or: export DATABRICKS_PROFILE=dev
"""

from __future__ import annotations

import os

from custom_ds import create_dbconnect_session
from custom_ds.restapi import RestApiDataSource
from custom_ds.util.databricks_auth import parse_profile_arg

if __name__ == "__main__":
    profile = parse_profile_arg()
    if profile:
        os.environ["DATABRICKS_PROFILE"] = profile

    spark = create_dbconnect_session("dbconnect-restapi-read")

    spark.dataSource.register(RestApiDataSource)

    api_url = os.environ.get("REST_API_URL", "http://localhost:9090/api/users")

    # Batch read — identical to local example 05
    df = (
        spark.read.format("restapi")
        .option("url", api_url)
        .option("method", "GET")
        .option("resultKey", "data")
        .load()
    )

    print("=== REST API Batch Read (via Databricks Connect) ===")
    df.printSchema()
    df.show(10, truncate=False)
    print(f"Total rows: {df.count()}")

    spark.stop()
