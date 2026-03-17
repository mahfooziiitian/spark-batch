import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
    ("003", "Carol", (7,  2.79, 17), "Berlin"),
]

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("array-schema-print")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(STUDENTS, schema=schema)

    print("=== printSchema() ===")
    df.printSchema()

    print("=== dtypes ===")
    for col_name, col_type in df.dtypes:
        print(f"  {col_name:<12} {col_type}")

    print("\n=== columns ===")
    print(df.columns)

    print("\n=== schema.json() ===")
    print(df.schema.json())

    # Nested field metadata
    metrics_type = df.schema["metrics"].dataType
    print("\n=== nested fields (metrics) ===")
    for f in metrics_type.fields:
        print(f"  {f.name:<10} {f.dataType.simpleString():<8} nullable={f.nullable}")

    # Flatten nested struct for display
    flat = df.select(
        F.col("rollno"),
        F.col("name"),
        F.col("metrics.age").alias("age"),
        F.col("metrics.height").alias("height"),
        F.col("address"),
    )
    print("\n=== flattened ===")
    flat.show(truncate=False)

    spark.stop()
