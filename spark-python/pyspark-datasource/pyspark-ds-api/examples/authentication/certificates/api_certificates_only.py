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
    config_path = Path(__file__).parents[0] / "api_certificates_only.yaml"
    print(f"Loading config from {config_path}")
    config = load_config(config_path)
    print(f"Config loaded: {config}")
    print("Extracting data from API.")
    df = read_api(spark, config)
    # generate_schema_from_df(config["extracts"]["extract"]["source"]["params"]["schema"], df)
    print("Data extracted.")
    df.show(truncate=False, n=100)
    print("Data extraction complete.")


if __name__ == "__main__":
    main()
