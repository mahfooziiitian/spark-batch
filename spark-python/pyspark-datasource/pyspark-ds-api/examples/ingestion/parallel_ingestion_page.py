"""Parallel page ingestion demo: ``ThreadPoolExecutor.submit`` + Spark DataFrame.

Variant of ``parallel_ingestion.py`` using ``submit()``/``future.result()``
instead of ``executor.map()`` — useful when you need per-future error
handling or want to fetch pages that aren't known up front.

Pairs with ``mock_items_server.py`` (``GET /items?page=<n>``) started
separately:

    PYTHONPATH=src uv run python examples/ingestion/mock_items_server.py &
    PYTHONPATH=src uv run python examples/ingestion/parallel_ingestion_page.py
"""

import argparse
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
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


def fetch_page_data(
    page: int, base_url: str, headers: Dict[str, str], query_param: str = "page"
) -> List[Dict]:
    """Fetch a single page and return its ``results`` records (empty list on error)."""
    url = f"{base_url}?{query_param}={page}"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException:
        logger.exception("Page %d failed", page)
        return []


def parallel_fetch(
    base_url: str, headers: Dict[str, str], total_pages: int, max_workers: int = 10
) -> List[Dict]:
    """Fetch ``total_pages`` pages concurrently and flatten the results."""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(fetch_page_data, i, base_url, headers)
            for i in range(1, total_pages + 1)
        ]
        results = [f.result() for f in futures]
    return [record for page in results for record in page]


def load_to_spark(
    spark: SparkSession, data: List[Dict], schema: StructType
) -> DataFrame:
    """Convert a list of dicts into a schema-typed Spark DataFrame."""
    if not data:
        raise ValueError("No data returned from API")
    rdd = spark.sparkContext.parallelize([json.dumps(record) for record in data])
    return spark.read.schema(schema).json(rdd)


def ingest_api_parallel_to_spark(
    spark: SparkSession,
    base_url: str,
    headers: Dict[str, str],
    schema: StructType,
    total_pages: int,
    max_workers: int = 10,
) -> DataFrame:
    data = parallel_fetch(base_url, headers, total_pages, max_workers)
    return load_to_spark(spark, data, schema)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8091/items")
    parser.add_argument("--pages", type=int, default=5, help="Number of pages to fetch")
    parser.add_argument("--max-workers", type=int, default=10)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    headers = {"Authorization": "******"}

    spark = (
        SparkSession.builder.appName("ParallelAPIIngestion")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        logger.info(
            "Fetching %d page(s) with %d worker(s)", args.pages, args.max_workers
        )
        df = ingest_api_parallel_to_spark(
            spark,
            base_url=args.base_url,
            headers=headers,
            schema=SCHEMA,
            total_pages=args.pages,
            max_workers=args.max_workers,
        )
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
