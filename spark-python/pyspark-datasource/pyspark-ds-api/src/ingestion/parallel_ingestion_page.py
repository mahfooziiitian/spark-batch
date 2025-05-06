import os
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
os.environ["PYSPARK_PYTHON"] = sys.executable


def fetch_page_data(
    page: int, base_url: str, headers: Dict, query_param: str = "page"
) -> List[Dict]:
    url = f"{base_url}?{query_param}={page}"
    response = requests.get(url, headers=headers, timeout=10)
    if response.status_code == 200:
        return response.json().get("results", [])
    else:
        print(f"Page {page} failed with status code {response.status_code}")
        return []


def parallel_fetch(
    base_url: str, headers: Dict, total_pages: int, max_workers: int = 10
) -> List[Dict]:
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(fetch_page_data, i, base_url, headers)
            for i in range(1, total_pages + 1)
        ]
        results = [f.result() for f in futures]
    return [record for page in results for record in page]


def load_to_spark(df_data: List[Dict], schema: StructType, spark: SparkSession):
    if not df_data:
        raise ValueError("No data returned from API")

    rdd = spark.sparkContext.parallelize(df_data)
    return spark.read.schema(schema).json(rdd)


def ingest_api_parallel_to_spark(
    spark: SparkSession,
    base_url: str,
    headers: Dict,
    schema: StructType,
    total_pages: int,
    max_workers: int = 10,
):
    data = parallel_fetch(base_url, headers, total_pages, max_workers)
    df = load_to_spark(data, schema, spark)
    return df


spark = SparkSession.builder.appName("ParallelAPIIngestion").getOrCreate()

schema = StructType(
    [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
)

api_url = "http://localhost:8000/items"
headers = {"Authorization": "Bearer YOUR_API_KEY"}

df = ingest_api_parallel_to_spark(
    spark,
    base_url=api_url,
    headers=headers,
    schema=schema,
    total_pages=50,
    max_workers=10,
)

df.show()
