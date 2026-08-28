"""Baseline (non-parallel) REST API ingestion demo.

The simplest possible pattern: one HTTP request, one Spark DataFrame. No
pagination, no threads, no RDD partitioning — useful as a starting point
before reaching for the parallel variants in this folder.

Pairs with ``mock_items_server.py`` (``GET /items?page=<n>``) started
separately:

    PYTHONPATH=src uv run python examples/ingestion/mock_items_server.py &
    PYTHONPATH=src uv run python examples/ingestion/pyspark_rest_api.py
"""

import argparse
import json
import logging
import os
import sys

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8091/items?page=1")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    headers = {"Authorization": "******"}

    try:
        response = requests.get(args.url, headers=headers, timeout=10)
        response.raise_for_status()
        records = response.json().get("results", [])
    except requests.RequestException:
        logger.exception("Request to %s failed", args.url)
        return 1

    if not records:
        logger.error("No data returned from API")
        return 1

    spark = (
        SparkSession.builder.appName("APIIngestion")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        rdd = spark.sparkContext.parallelize([json.dumps(r) for r in records])
        df = spark.read.schema(SCHEMA).json(rdd)
        logger.info("Ingested %d record(s)", df.count())
        df.show(truncate=False)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
