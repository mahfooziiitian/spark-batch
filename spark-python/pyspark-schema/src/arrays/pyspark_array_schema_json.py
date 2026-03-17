import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType,
)

schema = StructType([
    StructField("rollno",  StringType(), nullable=False),
    StructField("name",    StringType(), nullable=True),
    StructField("metrics", StructType([
        StructField("age",    IntegerType(), nullable=True),
        StructField("height", FloatType(),   nullable=True),
        StructField("weight", IntegerType(), nullable=True),
    ]), nullable=True),
    StructField("address", StringType(), nullable=True),
])

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("array-schema-json")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print("=== simpleString ===")
    print(schema.simpleString())

    print("\n=== jsonValue (dict) ===")
    print(json.dumps(schema.jsonValue(), indent=2))

    print("\n=== json (string) ===")
    print(schema.json())

    print("\n=== typeName ===")
    print(schema.typeName())

    # JSON round-trip
    schema_back = StructType.fromJson(json.loads(schema.json()))
    print("\n=== round-trip equal ===", schema == schema_back)

    # Per-field type names
    print("\n=== field type names ===")
    for field in schema.fields:
        print(f"  {field.name:<10} {field.dataType.typeName():<12} nullable={field.nullable}")

    spark.stop()
