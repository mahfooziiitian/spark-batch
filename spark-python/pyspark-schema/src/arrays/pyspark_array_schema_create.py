import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, LongType,
    ArrayType,
)

# -- Array of primitives --------------------------------------------------
schema_tags = StructType([
    StructField("id",   LongType(),              nullable=False),
    StructField("name", StringType(),            nullable=True),
    StructField("tags", ArrayType(StringType()), nullable=True),
])

# -- Array of structs -----------------------------------------------------
score_schema = StructType([
    StructField("subject", StringType(), nullable=False),
    StructField("score",   DoubleType(), nullable=True),
])

schema_nested = StructType([
    StructField("id",     LongType(),              nullable=False),
    StructField("name",   StringType(),            nullable=True),
    StructField("scores", ArrayType(score_schema), nullable=True),
])

TAGS_DATA = [
    (1, "Alice", ["python", "spark", "sql"]),
    (2, "Bob",   ["java",   "scala"]),
    (3, "Carol", ["python", "pandas"]),
]

NESTED_DATA = [
    (1, "Alice", [("maths", 95.0), ("science", 88.0)]),
    (2, "Bob",   [("maths", 72.0), ("science", 91.0)]),
]

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("array-schema-create")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # -- Array of primitives
    df_tags = spark.createDataFrame(TAGS_DATA, schema=schema_tags)
    print("=== Array of primitives ===")
    df_tags.printSchema()
    df_tags.show(truncate=False)

    # -- Array of structs
    df_nested = spark.createDataFrame(NESTED_DATA, schema=schema_nested)
    print("=== Array of structs ===")
    df_nested.printSchema()
    df_nested.show(truncate=False)

    # Derive new columns using array functions
    df_enriched = (df_tags
                   .withColumn("tag_count", F.size(F.col("tags")))
                   .withColumn("first_tag",  F.element_at(F.col("tags"), 1))
                   .withColumn("has_spark",  F.array_contains(F.col("tags"), "spark")))
    print("=== Enriched (tag_count, first_tag, has_spark) ===")
    df_enriched.show(truncate=False)

    spark.stop()