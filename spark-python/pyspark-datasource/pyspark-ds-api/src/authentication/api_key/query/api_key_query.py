import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from util.config_loader import load_config
from util.data_processor import read_api

# from authentication.etl_extract import read_api

# os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
# os.environ["PYSPARK_PYTHON"] = sys.executable


# def main():
#     spark = SparkSession.builder.appName("REST_API_Ingestion").getOrCreate()
#     config_path = Path(__file__).parents[0] / "api_key_query.yaml"
#     print(f"Loading config from {config_path}")
#     with open(file=config_path, mode="r", encoding="utf-8") as f:
#         config = yaml.safe_load(f)
#     print(f"Config loaded: {config}")
#     print("Extracting data from API...")
#     df = read_api(spark, config)
#     print(f"Data extracted: {df.show(truncate=False, n=100)}")
#     print("Data extraction complete.")


# if __name__ == "__main__":
#     main()


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
