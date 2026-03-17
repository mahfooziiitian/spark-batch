import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType,
    ArrayType,
)

schema = StructType([
    StructField("id",    IntegerType(),           nullable=False),
    StructField("name",  StringType(),            nullable=True),
    StructField("tags",  ArrayType(StringType()), nullable=True),
    StructField("score", IntegerType(),           nullable=True),
])

DATA = [
    (1, "Alice", ["python", "spark"], 85),
    (2, "Bob",   ["java",   "scala"], 90),
    (3, "Carol", ["python", "sql"],   78),
]

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("array-schema-update")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(DATA, schema=schema)
    print("=== original schema ===")
    df.printSchema()
    df.show(truncate=False)

    # Cast columns to wider types
    df_cast = (df
               .withColumn("id",    F.col("id").cast(LongType()))
               .withColumn("score", F.col("score").cast(DoubleType())))
    print("=== after cast (id→long, score→double) ===")
    df_cast.printSchema()

    # Add a derived column
    df_added = df.withColumn("tag_count", F.size(F.col("tags")))
    print("=== after adding 'tag_count' ===")
    df_added.show(truncate=False)

    # Rename a column
    df_renamed = df.withColumnRenamed("score", "grade")
    print("=== after renaming 'score' → 'grade' ===")
    print(df_renamed.columns)

    # Drop a column
    df_dropped = df.drop("tags")
    print("=== after dropping 'tags' ===")
    df_dropped.printSchema()

    # Reorder columns
    df_reordered = df.select("name", "id", "score", "tags")
    print("=== reordered columns ===")
    print(df_reordered.columns)

    spark.stop()
