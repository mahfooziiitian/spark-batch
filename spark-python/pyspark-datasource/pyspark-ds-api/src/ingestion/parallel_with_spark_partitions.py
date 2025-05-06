import json
import os
import sys
from typing import Dict, List

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# Set up Java and Python for PySpark
os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME_11", "")
os.environ["PYSPARK_PYTHON"] = sys.executable

# Create Spark session
spark = SparkSession.builder.appName("PartitionedAPIIngestion").getOrCreate()

# Define schema expected from API
schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ]
)

# URL and headers for API
api_url = "http://localhost:8000/items"
headers = {"Authorization": "Bearer YOUR_API_KEY"}


# Function to fetch one page
def fetch_page_data(page: int, base_url: str, headers: Dict) -> List[Dict]:
    url = f"{base_url}?page={page}"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json().get("results", [])
        else:
            print(f"Page {page} failed with status {response.status_code}")
            return []
    except Exception as e:
        print(f"Error fetching page {page}: {e}")
        return []


# Function to load list of dicts into Spark DataFrame
def load_to_spark(data: List[Dict], schema: StructType, spark: SparkSession):
    if not data:
        raise ValueError("No data returned from API")
    rdd = spark.sparkContext.parallelize([json.dumps(record) for record in data])
    return spark.read.schema(schema).json(rdd)


# --- Main ingestion logic using RDD partitions ---
def main():
    page_numbers = list(range(50))  # Assume 50 pages exist

    # Parallelize and fetch using Spark RDD
    rdd = spark.sparkContext.parallelize(page_numbers, numSlices=10)
    results = rdd.flatMap(lambda i: fetch_page_data(i + 1, api_url, headers)).collect()

    # Convert to DataFrame
    df = load_to_spark(results, schema, spark)
    df.show()

    # Optionally: Save, process, etc.
    # df.write.mode("overwrite").parquet("output_data/")


if __name__ == "__main__":
    main()
