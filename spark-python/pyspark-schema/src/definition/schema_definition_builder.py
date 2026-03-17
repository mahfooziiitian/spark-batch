import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StringType, IntegerType, DoubleType, BooleanType, TimestampType,
)

# -- Untyped builder: column types passed as DDL strings ------------------
schema_untyped = (StructType()
                  .add("order_id",   "long",      nullable=False)
                  .add("customer",   "string",    nullable=True)
                  .add("amount",     "double",    nullable=True)
                  .add("paid",       "boolean",   nullable=True)
                  .add("created_at", "timestamp", nullable=True))

# -- Typed builder: column types passed as DataType objects ---------------
schema_typed = (StructType()
                .add("order_id",   IntegerType(),   nullable=False)
                .add("customer",   StringType(),    nullable=True)
                .add("amount",     DoubleType(),    nullable=True)
                .add("paid",       BooleanType(),   nullable=True)
                .add("created_at", TimestampType(), nullable=True))

SAMPLE_DATA = [
    (1, "Alice", 120.50, True,  "2024-01-15 08:30:00"),
    (2, "Bob",   200.00, False, "2024-01-15 09:00:00"),
    (3, "Carol",  75.25, True,  "2024-01-15 09:30:00"),
]

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("schema-definition-builder")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(SAMPLE_DATA, schema=schema_typed)
    df.show(truncate=False)
    df.printSchema()

    print("simpleString :", schema_typed.simpleString())
    print("jsonValue    :", schema_typed.jsonValue())
    print("typeName     :", schema_typed.typeName())

    # Untyped and typed builders produce identical schemas
    print("schemas equal:", schema_untyped.simpleString() == schema_typed.simpleString())

    spark.stop()