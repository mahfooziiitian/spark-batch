import os
import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

from authentication.etl_extract import read_api

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
os.environ["PYSPARK_PYTHON"] = sys.executable


def main():
    spark = SparkSession.builder.appName("REST_API_Ingestion").getOrCreate()
    config_path = Path(__file__).parents[0] / "api_basic_ds.yaml"
    print(f"Loading config from {config_path}")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    print(f"Config loaded: {config}")
    print("Extracting data from API...")
    df = read_api(spark, config)
    print(f"Data extracted: {df.show(truncate=False, n=100)}")
    print("Data extraction complete.")


if __name__ == "__main__":
    main()
