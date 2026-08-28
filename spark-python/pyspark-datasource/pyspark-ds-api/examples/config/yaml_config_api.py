"""Minimal demo of loading a YAML extraction config and running it through
`rest_ds.rest_api.read_api`. Unlike the `authentication/` and `paginated/`
examples, this one has no auth or pagination — it exists purely to show the
config-loading + `read_api` call pattern shared by every example script.

Prerequisite: start the shared mock server first:

    PYTHONPATH=src uv run python examples/ingestion/mock_items_server.py
"""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from rest_ds.rest_api import read_api
from rest_ds.util.config_loader import load_config

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
os.environ["PYSPARK_PYTHON"] = sys.executable


def main():
    spark = SparkSession.builder.appName("REST_API_Ingestion").getOrCreate()
    config_path = Path(__file__).parents[0] / "ds.yaml"
    print(f"Loading config from {config_path}")
    config = load_config(config_path)
    print(f"Config loaded: {config}")
    print("Extracting data from API...")
    read_api(spark, config)
    print("Data extraction complete.")


if __name__ == "__main__":
    main()
