import os
import sys
from pathlib import Path

import requests
import yaml
from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
os.environ["PYSPARK_PYTHON"] = sys.executable


def get_oauth_token(auth_config: dict, method_type: str):
    resp = requests.post(
        auth_config["tokenUrl"],
        data={
            "grant_type": "client_credentials",
            "client_id": auth_config["clientId"],
            "client_secret": auth_config["clientSecret"],
            "scope": auth_config["scope"],
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def main():
    # Initialize Spark
    spark = SparkSession.builder.appName("REST_API_Ingestion").getOrCreate()

    # Load YAML ds
    config_path = Path(__file__).parents[1] / "config/ds.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    auth_token = get_oauth_token(config["data_sources"]["api_source"]["authentication"])
    headers = {"Authorization": f"Bearer {auth_token}"}

    # Now fetch the secured data
    response = requests.get(
        config["data_sources"]["api_source"]["endpoint"], headers=headers
    )
    data = response.json()
    df = spark.createDataFrame(data)

    df.printSchema()

    # Now df is a Spark DataFrame, ready for processing
    df.show(truncate=False, n=100)


if __name__ == "__main__":
    main()
