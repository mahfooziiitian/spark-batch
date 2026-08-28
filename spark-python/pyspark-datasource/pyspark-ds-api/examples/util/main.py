import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from rest_ds.util.config_loader import load_config
from rest_ds.util.data_processor import read_api

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
os.environ["PYSPARK_PYTHON"] = sys.executable


def main():
    spark = SparkSession.builder.appName("REST_API_Ingestion").getOrCreate()
    config_path = Path(__file__).parents[0] / "api_key_query.yaml"
    config = load_config(config_path)
    df = read_api(spark, config)
    df.show(truncate=False, n=100)


if __name__ == "__main__":
    main()
