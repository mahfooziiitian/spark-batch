import os
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DecimalType, DoubleType,
)

# DecimalType(precision, scale):
#   precision = total significant digits
#   scale     = digits after the decimal point
schema = StructType([
    StructField("order_id",    LongType(),         nullable=False),
    StructField("description", StringType(),       nullable=True),
    StructField("amount_dec",  DecimalType(18, 2), nullable=True),  # exact
    StructField("amount_dbl",  DoubleType(),       nullable=True),  # approximate
])

SAMPLE_DATA = [
    (1, "Coffee",    Decimal("3.14"),          3.14),
    (2, "Laptop",    Decimal("999.99"),         999.99),
    (3, "Cable",     Decimal("0.01"),           0.01),
    (4, "Big order", Decimal("123456789.99"),   123456789.99),
]

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("schema-decimal-type")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(SAMPLE_DATA, schema=schema)
    df.show(truncate=False)
    df.printSchema()

    # DecimalType preserves exact values; DoubleType accumulates floating-point error
    print("=== sum comparison ===")
    df.agg(
        F.sum("amount_dec").alias("total_exact"),
        F.sum("amount_dbl").alias("total_approx"),
    ).show()

    # Rounding to different scales
    print("=== rounding ===")
    df.select(
        F.col("description"),
        F.col("amount_dec"),
        F.round(F.col("amount_dec"), 0).alias("rounded_0dp"),
        F.col("amount_dec").cast(DecimalType(18, 4)).alias("cast_to_4dp"),
    ).show()

    # DecimalType field metadata
    dec_field = df.schema["amount_dec"]
    dt = dec_field.dataType
    print(f"precision={dt.precision}  scale={dt.scale}  simpleString={dt.simpleString()}")

    # Common financial types
    for label, dtype in [
        ("price (10,2)",    DecimalType(10, 2)),
        ("rate  (5,4)",     DecimalType(5, 4)),
        ("total (18,2)",    DecimalType(18, 2)),
        ("bignum (38,10)",  DecimalType(38, 10)),
    ]:
        print(f"  {label:<18} simpleString={dtype.simpleString()}")

    spark.stop()
