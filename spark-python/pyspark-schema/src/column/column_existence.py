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

STUDENTS = [
    ("001", "Alice", (23, 5.79, 67), "New York"),
    ("002", "Bob",   (16, 3.79, 34), "London"),
]

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("column-existence")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(STUDENTS, schema=schema)

    # fieldNames() — top-level column names only
    print("=== fieldNames() ===")
    print(df.schema.fieldNames())

    # Top-level column existence
    print("\n=== top-level existence ===")
    print("'name' present   :", "name" in df.schema.fieldNames())
    print("'missing' present:", "missing" in df.schema.fieldNames())

    # Exact StructField match (name + type + nullable must all match)
    print("\n=== StructField contains ===")
    target = StructField("name", StringType(), True)
    print("exact match:", target in df.schema.fields)

    # Nested field names via schema traversal
    print("\n=== nested field names (metrics) ===")
    metrics_fields = df.schema["metrics"].dataType.fieldNames()
    print(metrics_fields)
    print("'age' in metrics:", "age" in metrics_fields)
    print("'bmi' in metrics:", "bmi" in metrics_fields)

    # Full column type map
    print("\n=== column type map ===")
    for name, dtype in df.dtypes:
        print(f"  {name:<12} {dtype}")

    spark.stop()
