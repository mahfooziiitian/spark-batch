import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType,
)

OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/schema_compat")

# Schema v1 — original
SCHEMA_V1 = StructType([
    StructField("id",     LongType(),   nullable=False),
    StructField("name",   StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
])

# Schema v2 — adds one nullable column (backward-compatible change)
SCHEMA_V2 = StructType([
    StructField("id",     LongType(),   nullable=False),
    StructField("name",   StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("region", StringType(), nullable=True),
])

DATA_V1 = [(1, "Alice", 100.0),          (2, "Bob",   200.0)]
DATA_V2 = [(3, "Carol", 150.0, "North"), (4, "Dave",  250.0, "South")]

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("schema-backward-compat")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    path_v1 = f"{OUTPUT_PATH}/v1"
    path_v2 = f"{OUTPUT_PATH}/v2"

    spark.createDataFrame(DATA_V1, SCHEMA_V1).write.mode("overwrite").parquet(path_v1)
    spark.createDataFrame(DATA_V2, SCHEMA_V2).write.mode("overwrite").parquet(path_v2)

    # -- Backward compat: old reader (v1 schema) reads new data (v2 files)
    # Extra 'region' column is silently ignored.
    print("=== backward compat: v1 reader reads v2 data ===")
    df_back = spark.read.schema(SCHEMA_V1).parquet(path_v2)
    df_back.printSchema()
    df_back.show()

    # -- Forward compat: new reader (v2 schema) reads old data (v1 files)
    # 'region' was absent in v1 — it surfaces as null.
    print("=== forward compat: v2 reader reads v1 data ===")
    df_fwd = spark.read.schema(SCHEMA_V2).parquet(path_v1)
    df_fwd.printSchema()
    df_fwd.show()
    print("null regions :", df_fwd.filter(F.col("region").isNull()).count())

    # -- mergeSchema: read both partitions together in one DataFrame ------
    print("=== mergeSchema: read v1 + v2 together ===")
    merged = (spark.read
              .option("mergeSchema", "true")
              .parquet(path_v1, path_v2))
    merged.printSchema()
    merged.orderBy("id").show()

    spark.stop()
