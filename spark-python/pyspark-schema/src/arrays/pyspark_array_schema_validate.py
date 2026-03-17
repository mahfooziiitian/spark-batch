import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType,
)

EXPECTED_SCHEMA = StructType([
    StructField("id",     LongType(),   nullable=False),
    StructField("name",   StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
])


def validate_schema(df: DataFrame, expected: StructType) -> list[str]:
    """Return validation error messages; empty list means the schema is valid."""
    errors: list[str] = []
    actual = {f.name: f for f in df.schema.fields}
    for field in expected.fields:
        if field.name not in actual:
            errors.append(f"Missing column: '{field.name}'")
        elif actual[field.name].dataType != field.dataType:
            errors.append(
                f"Type mismatch on '{field.name}': "
                f"expected {field.dataType.simpleString()}, "
                f"got {actual[field.name].dataType.simpleString()}"
            )
    return errors


def cast_to_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """Select and cast only the columns defined in *schema*."""
    return df.select([
        F.col(f.name).cast(f.dataType).alias(f.name)
        for f in schema.fields
    ])


VALID_DATA = [(1, "Alice", 100.0), (2, "Bob", 200.0)]

BAD_SCHEMA = StructType([
    StructField("id",     StringType(), nullable=True),
    StructField("name",   StringType(), nullable=True),
    StructField("amount", StringType(), nullable=True),
])
BAD_DATA = [("x", "Alice", "not-a-number"), ("y", "Bob", "bad")]

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("array-schema-validate")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df_valid = spark.createDataFrame(VALID_DATA, schema=EXPECTED_SCHEMA)
    print("=== validate correct DataFrame ===")
    errors = validate_schema(df_valid, EXPECTED_SCHEMA)
    print("Errors:", errors or "none ✓")

    df_bad = spark.createDataFrame(BAD_DATA, schema=BAD_SCHEMA)
    print("\n=== validate wrong-typed DataFrame ===")
    for err in validate_schema(df_bad, EXPECTED_SCHEMA):
        print("  ✗", err)

    # Cast to enforce the expected schema (nulls where cast fails)
    print("\n=== cast_to_schema ===")
    df_fixed = cast_to_schema(df_bad, EXPECTED_SCHEMA)
    df_fixed.printSchema()
    df_fixed.show()

    # Null-count check on the valid DataFrame
    print("=== null counts ===")
    null_counts = {col: df_valid.filter(F.col(col).isNull()).count()
                   for col in df_valid.columns}
    for col, count in null_counts.items():
        print(f"  {col}: {count}")

    spark.stop()
