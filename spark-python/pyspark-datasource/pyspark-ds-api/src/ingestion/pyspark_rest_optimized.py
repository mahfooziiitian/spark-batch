import json
import os
import sys
from typing import Dict, List, Optional

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
os.environ["PYSPARK_PYTHON"] = sys.executable


def fetch_api_data(
    base_url: str,
    headers: Optional[Dict] = None,
    params: Optional[Dict] = None,
    pagination_key: str = "next",
    data_key: str = "results",
    max_pages: int = 100,
) -> List[Dict]:
    all_data = []
    url = base_url
    page_count = 0

    while url and page_count < max_pages:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Failed to fetch data: {response.status_code}")
            break
        json_data = response.json()
        all_data.extend(json_data.get(data_key, []))
        url = json_data.get(pagination_key)
        page_count += 1

    return all_data


api_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ]
)


def load_data_to_spark(spark: SparkSession, data: List[Dict], schema: StructType):
    rdd = spark.sparkContext.parallelize(data)
    json_rdd = rdd.map(lambda record: json.dumps(record))
    return spark.read.schema(schema).json(json_rdd)


def ingest_from_api_to_spark(
    spark: SparkSession, api_url: str, headers=None, schema=None
):
    data = fetch_api_data(api_url, headers=headers)
    df = load_data_to_spark(spark, data, schema)
    return df


def main():
    spark = SparkSession.builder.appName("OptimizedAPIIngestion").getOrCreate()

    api_url = "http://localhost:8000/items"
    headers = {"Authorization": "Bearer your-token"}

    df = ingest_from_api_to_spark(spark, api_url, headers=headers, schema=api_schema)
    df.show()


if __name__ == "__main__":
    sys.exit(main())
