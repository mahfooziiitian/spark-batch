"""Databricks Connect — batch write (POST) to a REST API from a remote cluster.

Key concepts:
    - Running custom Python Data Source writes via Databricks Connect
    - Auto-upload of custom_ds wheel via spark.addArtifact()
    - Batch size control for HTTP POST requests
    - Identical API to local example 06

Prerequisites:
    databricks auth login --profile dev

Usage:
    uv run python examples/13_databricks_connect/dbconnect_batch_write.py --profile dev
    # or: export DATABRICKS_PROFILE=dev
"""

from __future__ import annotations

import os

from custom_ds import create_dbconnect_session
from custom_ds.restapi import RestApiSinkDataSource
from custom_ds.util.databricks_auth import create_arg_parser

if __name__ == "__main__":
    parser = create_arg_parser("Databricks Connect — REST API batch write")
    parser.add_argument(
        "--url",
        default=os.environ.get("REST_API_URL", "http://localhost:9090"),
        help="REST API base URL (default: $REST_API_URL or http://localhost:9090)",
    )
    args = parser.parse_args()
    if args.profile:
        os.environ["DATABRICKS_PROFILE"] = args.profile

    spark = create_dbconnect_session("dbconnect-restapi-write")

    spark.dataSource.register(RestApiSinkDataSource)

    api_url: str = f"{args.url}/api/records"

    # Create sample data
    data = [(i, f"dbconnect-row-{i}") for i in range(1, 11)]
    df = spark.createDataFrame(data, schema="id LONG, value STRING")

    print("=== Data to POST (via Databricks Connect) ===")
    df.show(truncate=False)

    # Write to REST API — identical to local example 06
    (
        df.write.format("restapi_sink")
        .option("url", api_url)
        .option("batchSize", "5")
        .mode("append")
        .save()
    )

    print(f"Successfully posted {df.count()} rows to {api_url}")

    spark.stop()
