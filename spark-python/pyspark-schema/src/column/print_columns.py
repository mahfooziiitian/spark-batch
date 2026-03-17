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


def collect_column_paths(schema: StructType, prefix: str = "") -> list[str]:
    """Recursively collect all dot-notation column paths from a schema."""
    paths: list[str] = []
    for field in schema.fields:
        full = f"{prefix}.{field.name}" if prefix else field.name
        paths.append(full)
        if hasattr(field.dataType, "fields"):
            paths.extend(collect_column_paths(field.dataType, full))
    return paths


if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("print-columns")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(STUDENTS, schema=schema)

    print("=== df.columns (top-level only) ===")
    print(df.columns)

    print("\n=== df.dtypes ===")
    for col_name, col_type in df.dtypes:
        print(f"  {col_name:<12} {col_type}")

    print("\n=== df.printSchema() ===")
    df.printSchema()

    print("=== all dot-notation paths (incl. nested) ===")
    for path in collect_column_paths(df.schema):
        print(f"  {path}")

    print("\n=== nullable map ===")
    for field in df.schema.fields:
        print(f"  {field.name:<12} nullable={field.nullable}")

    # Select a nested column by dot-notation
    print("\n=== select nested column ===")
    df.select("rollno", "name", F.col("metrics.age").alias("age")).show()

    spark.stop()
