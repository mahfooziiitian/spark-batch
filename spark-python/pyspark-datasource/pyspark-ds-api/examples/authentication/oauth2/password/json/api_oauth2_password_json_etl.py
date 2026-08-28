import os
import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

from rest_ds.rest_api import read_api

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
os.environ["PYSPARK_PYTHON"] = sys.executable


def main():
    spark = SparkSession.builder.appName("REST_API_Ingestion").getOrCreate()
    config_path = Path(__file__).parents[0] / "api_oauth2_password_json_etl.yaml"
    print(f"Loading config from {config_path}")
    with open(file=config_path, mode="r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    print(f"Config loaded: {config}")
    print("Extracting data from API...")
    read_api(spark, config)
    print("Data extraction complete.")


if __name__ == "__main__":
    main()
