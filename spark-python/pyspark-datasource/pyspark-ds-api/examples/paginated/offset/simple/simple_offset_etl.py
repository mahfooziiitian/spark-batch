"""Simple offset-pagination ETL demo.

Pairs with ``simple_offset_source.py`` (mock server on port 8081):

    PYTHONPATH=src uv run python examples/paginated/offset/simple/simple_offset_source.py &
    PYTHONPATH=src uv run python examples/paginated/offset/simple/simple_offset_etl.py
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from rest_ds.rest_api import read_api
from rest_ds.util.config_loader import load_config
from rest_ds.util.config_validator import ConfigValidationError

os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME_11", "")
os.environ["PYSPARK_PYTHON"] = sys.executable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_CONFIG = Path(__file__).parent / "simple_offset.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Path to the source YAML config (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("REST_API_Ingestion")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        logger.info("Loading config from %s", args.config)
        config = load_config(args.config)

        logger.info("Extracting data from API (offset pagination)...")
        read_api(spark, config)
        logger.info("Data extraction complete.")
        return 0
    except (FileNotFoundError, ConfigValidationError) as err:
        logger.error("%s", err)
        return 1
    except Exception:
        logger.exception("Extraction failed")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
