import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType,
    ArrayType,
)


def flatten_schema(schema: StructType, prefix: str = "") -> list[tuple[str, str]]:
    """
    Recursively walk *schema* and return (dot_path, type_simpleString) for
    every leaf field.  Nested StructType fields are recursed into;
    ArrayType fields are listed as-is (element type shown in simpleString).
    """
    paths: list[tuple[str, str]] = []
    for field in schema.fields:
        full = f"{prefix}.{field.name}" if prefix else field.name
        if isinstance(field.dataType, StructType):
            paths.extend(flatten_schema(field.dataType, full))
        else:
            paths.append((full, field.dataType.simpleString()))
    return paths


def flatten_df(df: DataFrame) -> DataFrame:
    """Select all leaf columns using dot-notation, producing a flat DataFrame.
    Underscores replace dots in the output column names."""
    return df.select([
        F.col(path).alias(path.replace(".", "_"))
        for path, _ in flatten_schema(df.schema)
    ])


schema = StructType([
    StructField("order_id", LongType(), nullable=False),
    StructField("customer", StructType([
        StructField("id",   LongType(),   nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("address", StructType([
            StructField("city",    StringType(), nullable=True),
            StructField("country", StringType(), nullable=True),
        ]), nullable=True),
    ]), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("tags",   ArrayType(StringType()), nullable=True),
])

SAMPLE_DATA = [
    (1, (10, "Alice", ("New York", "US")), 150.0, ["vip", "monthly"]),
    (2, (11, "Bob",   ("London",   "UK")), 200.0, ["new"]),
]

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("schema-flattening")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(SAMPLE_DATA, schema=schema)

    print("=== nested schema ===")
    df.printSchema()
    df.show(truncate=False)

    print("=== leaf paths ===")
    for path, type_str in flatten_schema(df.schema):
        print(f"  {path:<40} {type_str}")

    print("=== flattened DataFrame ===")
    flat = flatten_df(df)
    flat.printSchema()
    flat.show(truncate=False)

    # Explode array after flattening
    print("=== explode tags ===")
    (flat
     .withColumn("tag", F.explode(F.col("tags")))
     .drop("tags")
     .show(truncate=False))

    spark.stop()
