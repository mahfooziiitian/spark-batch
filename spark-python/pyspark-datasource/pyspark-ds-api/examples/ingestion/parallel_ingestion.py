"""Parallel page ingestion demo: ``ThreadPoolExecutor.map`` + Spark DataFrame.

The simplest parallel-fetch pattern — a fixed list of page URLs is mapped
across a thread pool, the resulting pages are flattened, and the combined
records are loaded into a Spark DataFrame with an explicit schema.

Pairs with ``mock_items_server.py`` (``GET /items?page=<n>``) started
separately:

    PYTHONPATH=src uv run python examples/ingestion/mock_items_server.py &
    PYTHONPATH=src uv run python examples/ingestion/parallel_ingestion.py
"""

import argparse
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

import requests
from pyspark.sql import SparkSession
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


def fetch_page(page_url: str, headers: Dict[str, str]) -> List[Dict]:
    """Fetch one page and return its ``results`` records (empty list on error)."""
    try:
        response = requests.get(page_url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException:
        logger.exception("Failed to fetch %s", page_url)
        return []


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8091/items")
    parser.add_argument("--pages", type=int, default=5, help="Number of pages to fetch")
    parser.add_argument("--max-workers", type=int, default=10)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    headers = {"Authorization": "******"}
    urls = [f"{args.base_url}?page={i}" for i in range(1, args.pages + 1)]

    logger.info("Fetching %d page(s) with %d worker(s)", len(urls), args.max_workers)
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        pages = list(executor.map(lambda url: fetch_page(url, headers), urls))

    flat_data = [record for page in pages for record in page]
    if not flat_data:
        logger.error("No data returned from API")
        return 1

    spark = (
        SparkSession.builder.appName("ParallelIngestion")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        rdd = spark.sparkContext.parallelize([json.dumps(r) for r in flat_data])
        df = spark.read.schema(SCHEMA).json(rdd)
        logger.info("Ingested %d record(s)", df.count())
        df.show(truncate=False)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
