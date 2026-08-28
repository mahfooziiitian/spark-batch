"""Spark-native parallel ingestion demo: RDD partitions drive page fetches.

Instead of a Python thread pool, page numbers are parallelized as a Spark
RDD (``numSlices`` partitions) and each partition's executor task fetches
its own page via ``flatMap`` — no separate thread pool needed, Spark's own
task scheduling provides the concurrency.

Pairs with ``mock_items_server.py`` (``GET /items?page=<n>``) started
separately:

    PYTHONPATH=src uv run python examples/ingestion/mock_items_server.py &
    PYTHONPATH=src uv run python examples/ingestion/parallel_with_spark_partitions.py
"""

import argparse
import json
import logging
import os
import sys
from typing import Dict, List

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME_11", "")
os.environ["PYSPARK_PYTHON"] = sys.executable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ]
)


def fetch_page_data(page: int, base_url: str, headers: Dict[str, str]) -> List[Dict]:
    """Fetch a single page and return its ``results`` records (empty list on error).

    Runs inside a Spark executor task, so failures must be swallowed rather
    than raised (a raised exception would fail the whole Spark job).
    """
    url = f"{base_url}?page={page}"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as exc:
        print(f"Page {page} failed: {exc}")  # executor stdout, not driver logger
        return []


def load_to_spark(
    spark: SparkSession, data: List[Dict], schema: StructType
) -> DataFrame:
    """Convert a list of dicts into a schema-typed Spark DataFrame."""
    if not data:
        raise ValueError("No data returned from API")
    rdd = spark.sparkContext.parallelize([json.dumps(record) for record in data])
    return spark.read.schema(schema).json(rdd)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8091/items")
    parser.add_argument("--pages", type=int, default=5, help="Number of pages to fetch")
    parser.add_argument("--num-slices", type=int, default=5, help="RDD partition count")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    headers = {"Authorization": "******"}

    spark = (
        SparkSession.builder.appName("PartitionedAPIIngestion")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        logger.info(
            "Fetching %d page(s) across %d RDD partition(s)",
            args.pages,
            args.num_slices,
        )
        page_numbers = list(range(1, args.pages + 1))
        rdd = spark.sparkContext.parallelize(page_numbers, numSlices=args.num_slices)
        results = rdd.flatMap(
            lambda page: fetch_page_data(page, args.base_url, headers)
        ).collect()

        df = load_to_spark(spark, results, SCHEMA)
        logger.info("Ingested %d record(s)", df.count())
        df.show(truncate=False)
        return 0
    except ValueError:
        logger.exception("Ingestion failed")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
