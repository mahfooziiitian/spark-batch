"""Databricks Connect — batch write (POST) to a REST API from a remote cluster.

Key concepts:
    - Running custom Python Data Source writes via Databricks Connect
    - Auto-upload of custom_ds wheel via spark.addArtifact()
    - Batch size control for HTTP POST requests
    - Identical API to local example 06

Prerequisites:
    - Databricks Connect configured (see README.md)
    - A REST API endpoint accessible from the cluster that accepts POST
"""

from __future__ import annotations

import os

from custom_ds import create_dbconnect_session
from custom_ds.restapi import RestApiSinkDataSource

if __name__ == "__main__":
    spark = create_dbconnect_session("dbconnect-restapi-write")

    spark.dataSource.register(RestApiSinkDataSource)

    api_url = os.environ.get("REST_API_URL", "http://localhost:9090/api/records")

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
