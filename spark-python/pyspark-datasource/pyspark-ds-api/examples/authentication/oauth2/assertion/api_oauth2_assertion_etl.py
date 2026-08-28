import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from rest_ds.rest_api import read_api
from rest_ds.util.config_loader import load_config

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
os.environ["PYSPARK_PYTHON"] = sys.executable


def main():
    data_home = os.environ.get("DATA_HOME", "/tmp")
    certs_dir = Path(data_home) / "rest_api_ds" / "certs"
    if not (certs_dir / "assertion_client.pem").exists():
        print(
            f"Missing assertion signing cert under {certs_dir}. "
            "Run generate_assertion_certs.sh first, then start "
            "api_oauth2_assertion_source.py before this script."
        )
        sys.exit(1)

    spark = SparkSession.builder.appName("REST_API_Ingestion").getOrCreate()
    config_path = Path(__file__).parents[0] / "api_oauth2_assertion_etl.yaml"
    print(f"Loading config from {config_path}")
    config = load_config(config_path)
    print(f"Config loaded: {config}")
    print("Extracting data from API...")
    read_api(spark, config)
    print("Data extraction complete.")


if __name__ == "__main__":
    main()
