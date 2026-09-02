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
from custom_ds.util.databricks_auth import create_arg_parser

if __name__ == "__main__":
    parser = create_arg_parser("Databricks Connect — REST API batch read")
    parser.add_argument(
        "--url",
        default=os.environ.get("REST_API_URL", "http://localhost:9090"),
        help="REST API base URL (default: $REST_API_URL or http://localhost:9090)",
    )
    args = parser.parse_args()
    if args.profile:
        os.environ["DATABRICKS_PROFILE"] = args.profile

    spark = create_dbconnect_session("dbconnect-restapi-read")

    spark.dataSource.register(RestApiDataSource)

    api_url: str = args.url

    # Batch read — identical to local example 05
    df = (
        spark.read.format("restapi")
        .option("url", f"{api_url}/api/users")
        .option("method", "GET")
        .option("resultKey", "data")
        .load()
    )

    print("=== REST API Batch Read (via Databricks Connect) ===")
    df.printSchema()
    df.show(10, truncate=False)
    print(f"Total rows: {df.count()}")

    spark.stop()
