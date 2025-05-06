import json

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

url = "https://api.example.com/data"
headers = {"Authorization": "Bearer YOUR_API_KEY"}

response = requests.get(url, headers=headers)
data = response.json()

spark = SparkSession.builder.appName("APIIngestion").getOrCreate()

# Option 1: If your JSON is a list of records
df = spark.read.json(spark.sparkContext.parallelize([json.dumps(data)]))

# Option 2: If it's already a list of dicts (common)
df = spark.read.json(spark.sparkContext.parallelize(data))


schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("timestamp", StringType(), True),
    ]
)

df = spark.read.schema(schema).json(spark.sparkContext.parallelize(data))
