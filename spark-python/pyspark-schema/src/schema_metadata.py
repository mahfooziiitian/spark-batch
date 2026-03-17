import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType, BooleanType,
)

# StructField accepts a 'metadata' dict for any key/value annotations.
# Common uses: PII tagging, column descriptions, data classifications.
schema = StructType([
    StructField("id", LongType(), nullable=False,
                metadata={"description": "Unique order identifier", "pii": False}),
    StructField("customer_email", StringType(), nullable=True,
                metadata={"description": "Customer email address", "pii": True,
                          "classification": "confidential"}),
    StructField("amount", DoubleType(), nullable=True,
                metadata={"description": "Order total in USD",
                          "pii": False, "unit": "USD"}),
    StructField("is_paid", BooleanType(), nullable=True,
                metadata={"description": "Payment status flag", "pii": False}),
])

SAMPLE_DATA = [
    (1, "alice@example.com", 99.99,  True),
    (2, "bob@example.com",   149.00, False),
    (3, "carol@example.com",  75.25, True),
]


def get_pii_columns(schema: StructType) -> list[str]:
    """Return names of all fields tagged with metadata pii=True."""
    return [f.name for f in schema.fields if f.metadata.get("pii") is True]


def mask_pii(df: DataFrame, schema: StructType) -> DataFrame:
    """Replace PII column values with a fixed mask string."""
    pii_cols = get_pii_columns(schema)
    for col in pii_cols:
        df = df.withColumn(col, F.lit("***REDACTED***"))
    return df


if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("schema-metadata")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(SAMPLE_DATA, schema=schema)
    df.show(truncate=False)
    df.printSchema()

    # Read per-field metadata
    print("=== field metadata ===")
    for field in df.schema.fields:
        print(f"  {field.name:<20} {field.metadata}")

    # Identify PII columns
    pii_cols = get_pii_columns(df.schema)
    print("\n=== PII columns ===", pii_cols)

    # Mask PII before writing to an untrusted sink
    print("\n=== masked DataFrame ===")
    masked = mask_pii(df, df.schema)
    masked.show(truncate=False)

    # Metadata survives JSON roundtrip
    import json
    restored = StructType.fromJson(json.loads(df.schema.json()))
    restored_pii = get_pii_columns(restored)
    print("=== PII cols after JSON roundtrip ===", restored_pii)

    spark.stop()
