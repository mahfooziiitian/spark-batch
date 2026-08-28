"""Standalone extract demo: run any authentication scenario's YAML config through `read_api`.

This script is scenario-agnostic — pass `--config` pointing at any of the
`api_*.yaml` files under `examples/authentication/**` (basic, jwt, oauth2,
api_key, mtls) and it will authenticate, fetch, and preview the resulting
DataFrame using the shared library implementation in
`rest_ds.util.data_processor`.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from rest_ds.util.config_loader import load_config
from rest_ds.util.config_validator import ConfigValidationError
from rest_ds.util.data_processor import read_api

os.environ.setdefault("JAVA_HOME", os.environ.get("JAVA_HOME_11", ""))
os.environ["PYSPARK_PYTHON"] = sys.executable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_CONFIG = Path(__file__).parent / "api_key" / "query" / "api_key_query.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Path to a source YAML config (default: %(default)s)",
    )
    parser.add_argument(
        "--rows", type=int, default=100, help="Number of rows to preview with df.show()"
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    spark = SparkSession.builder.appName("REST_API_Ingestion").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.info("Loading config from %s", args.config)
        config = load_config(args.config)

        logger.info("Extracting data from API...")
        df = read_api(spark, config)

        logger.info("Extraction complete: %d record(s).", df.count())
        df.show(truncate=False, n=args.rows)
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
