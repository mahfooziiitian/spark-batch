"""Optimized ingestion demo: follow response-provided ``next`` links.

Unlike the page-number-based scripts in this folder, this variant never
guesses a page count up front — it follows the ``next`` URL returned in each
response until the server reports there are no more pages (``next: null``).
Fetching stays sequential (one page depends on the previous response), so
the "optimization" here is architectural: fewer requests than an
over-fetching fixed-page-count strategy, and no wasted work past the last
page.

Pairs with ``mock_items_server.py`` (``GET /items``) started separately:

    PYTHONPATH=src uv run python examples/ingestion/mock_items_server.py &
    PYTHONPATH=src uv run python examples/ingestion/pyspark_rest_optimized.py
"""

import argparse
import json
import logging
import os
import sys
from typing import Dict, List, Optional

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


def fetch_api_data(
    base_url: str,
    headers: Optional[Dict[str, str]] = None,
    pagination_key: str = "next",
    data_key: str = "results",
    max_pages: int = 100,
) -> List[Dict]:
    """Follow ``pagination_key`` links from ``base_url`` until exhausted."""
    all_data: List[Dict] = []
    url: Optional[str] = base_url
    page_count = 0

    while url and page_count < max_pages:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        json_data = response.json()
        all_data.extend(json_data.get(data_key, []))
        url = json_data.get(pagination_key)
        page_count += 1

    logger.info("Fetched %d page(s), %d record(s)", page_count, len(all_data))
    return all_data


def load_data_to_spark(spark: SparkSession, data: List[Dict], schema) -> DataFrame:
    if not data:
        raise ValueError("No data returned from API")
    rdd = spark.sparkContext.parallelize(data).map(json.dumps)
    return spark.read.schema(schema).json(rdd)


def ingest_from_api_to_spark(
    spark: SparkSession, api_url: str, headers=None, schema=None
) -> DataFrame:
    data = fetch_api_data(api_url, headers=headers)
    return load_data_to_spark(spark, data, schema)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8091/items")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    headers = {"Authorization": "******"}

    spark = (
        SparkSession.builder.appName("OptimizedAPIIngestion")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        df = ingest_from_api_to_spark(
            spark, args.base_url, headers=headers, schema=SCHEMA
        )
        logger.info("Ingested %d record(s)", df.count())
        df.show(truncate=False)
        return 0
    except (ValueError, requests.RequestException):
        logger.exception("Ingestion failed")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
