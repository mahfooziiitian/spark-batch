import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType,
)

OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/schema_evolution")

# Schema v1 — original
schema_v1 = StructType([
    StructField("id",     LongType(),   nullable=False),
    StructField("name",   StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
])

# Schema v2 — adds 'region' column
schema_v2 = StructType([
    StructField("id",     LongType(),   nullable=False),
    StructField("name",   StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("region", StringType(), nullable=True),
])

DATA_V1 = [(1, "Alice", 100.0),          (2, "Bob",   200.0)]
DATA_V2 = [(3, "Carol", 150.0, "North"), (4, "Dave",  250.0, "South")]

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("schema-merge")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    path_v1 = f"{OUTPUT_PATH}/v1"
    path_v2 = f"{OUTPUT_PATH}/v2"

    df_v1 = spark.createDataFrame(DATA_V1, schema=schema_v1)
    df_v2 = spark.createDataFrame(DATA_V2, schema=schema_v2)

    df_v1.write.mode("overwrite").parquet(path_v1)
    df_v2.write.mode("overwrite").parquet(path_v2)

    print("=== Schema v1 ===")
    df_v1.printSchema()
    print("=== Schema v2 (added 'region') ===")
    df_v2.printSchema()

    # Read both partitions together — 'region' is null for v1 rows
    merged = (spark.read
              .option("mergeSchema", "true")
              .parquet(path_v1, path_v2))

    print("=== Merged schema (read-time mergeSchema) ===")
    merged.printSchema()
    merged.orderBy("id").show(truncate=False)

    print("total rows   :", merged.count())
    print("null regions :", merged.filter(F.col("region").isNull()).count())

    spark.stop()
