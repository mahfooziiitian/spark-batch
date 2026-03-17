import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType,
)

schema = StructType([
    StructField("name",       StringType(),    nullable=True),
    StructField("email",      StringType(),    nullable=True),
    StructField("city",       StringType(),    nullable=True),
    StructField("mac",        StringType(),    nullable=True),
    StructField("created_at", TimestampType(), nullable=True),
    StructField("creditcard", StringType(),    nullable=True),
])

SAMPLE_DATA = [
    ("Alice", "alice@example.com", "New York", "00:1A:2B:3C:4D:5E", "2024-01-15 08:30:00", "4111-1111-1111-1111"),
    ("Bob",   "bob@example.com",   "London",   "00:1B:2C:3D:4E:5F", "2024-01-15 09:00:00", "4222-2222-2222-2222"),
    ("Carol", "carol@example.com", "Berlin",   "00:1C:2D:3E:4F:60", "2024-01-15 09:30:00", "4333-3333-3333-3333"),
]

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("spark-schema")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    input_path = os.environ.get("INPUT_PATH", "")
    if input_path:
        df = spark.read.json(input_path, schema=schema)
    else:
        df = spark.createDataFrame(SAMPLE_DATA, schema=schema)

    df.show(truncate=False)
    df.printSchema()

    print("simpleString :", schema.simpleString())
    print("field count  :", len(df.schema.fields))
    print("nullable map :", {f.name: f.nullable for f in df.schema.fields})
    print("dtypes       :", df.dtypes)

    spark.stop()

