"""Partitioned REST API reads — parallel data loading with 3 strategies.

Key concepts:
    - Single partition: simple sequential fetch (default)
    - URL-based partitioning: one Spark task per URL, processed in parallel
    - Page-based partitioning: one Spark task per page, fetched concurrently

Prerequisites:
    Start the mock server first:
        uv run python examples/mock_server/server.py
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import functions as F

from custom_ds import create_spark_session
from custom_ds.restapi import RestApiDataSource

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Partitioned REST API reads")
    parser.add_argument(
        "--url",
        default=os.environ.get("MOCK_SERVER_URL", "http://localhost:9090"),
        help="Mock server base URL (default: $MOCK_SERVER_URL or http://localhost:9090)",
    )
    args = parser.parse_args()
    base_url: str = args.url

    spark = create_spark_session("restapi-partitioned")
    spark.dataSource.register(RestApiDataSource)

    # -------------------------------------------------------------------------
    # 1. Single partition (default) — one HTTP call
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("Strategy: SINGLE (default)")
    print("=" * 60)

    df_single = (
        spark.read.format("restapi")
        .option("url", f"{base_url}/api/users")
        .option("resultKey", "data")
        .option("schema", "id LONG, name STRING, email STRING, city STRING, age LONG")
        .load()
    )

    print(f"Partitions: {df_single.rdd.getNumPartitions()}")
    print(f"Rows: {df_single.count()}")
    df_single.show(5, truncate=False)

    # -------------------------------------------------------------------------
    # 2. URL-based partitioning — one task per URL, parallel
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("Strategy: URLS (3 URLs → 3 partitions)")
    print("=" * 60)

    urls = ",".join(f"{base_url}/api/users/{i}" for i in range(1, 4))

    df_urls = (
        spark.read.format("restapi")
        .option("partitionStrategy", "urls")
        .option("urls", urls)
        .option("schema", "id LONG, name STRING, email STRING, city STRING, age LONG")
        .load()
    )

    print(f"Partitions: {df_urls.rdd.getNumPartitions()}")
    print(f"Rows: {df_urls.count()}")
    df_urls.show(truncate=False)

    # Show partition distribution
    df_urls.select(F.spark_partition_id().alias("partition")).groupBy("partition").count().show()

    # -------------------------------------------------------------------------
    # 3. Page-based partitioning — one task per page, parallel
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("Strategy: PAGES (4 pages × 25 rows = 100 rows)")
    print("=" * 60)

    df_pages = (
        spark.read.format("restapi")
        .option("partitionStrategy", "pages")
        .option("url", f"{base_url}/api/posts")
        .option("totalPages", "4")
        .option("pageSize", "25")
        .option("resultKey", "data")
        .option("schema", "id LONG, title STRING, author STRING, views LONG")
        .load()
    )

    print(f"Partitions: {df_pages.rdd.getNumPartitions()}")
    print(f"Rows: {df_pages.count()}")
    df_pages.show(10, truncate=False)

    # Show partition distribution
    df_pages.select(F.spark_partition_id().alias("partition")).groupBy("partition").count().orderBy(
        "partition"
    ).show()

    spark.stop()
