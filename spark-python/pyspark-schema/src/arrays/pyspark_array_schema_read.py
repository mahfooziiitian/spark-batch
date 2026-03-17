import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

schema = (StructType()
          .add("name",              StringType(),          nullable=True)
          .add("languagesAtSchool", ArrayType(StringType()), nullable=True))

DATA = [
    ("James Smith",     ["Java", "Scala", "C++", "Pascal", "Spark"]),
    ("Michael Rose",    ["Spark", "Java", "C++", "Scala", "PHP"]),
    ("Robert Williams", ["CSharp", "VB", ".Net", "C#.net", ""]),
]

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("array-schema-read")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(DATA, schema=schema)
    df.show(truncate=False)
    df.printSchema()

    # slice — elements 2 and 3 (1-indexed)
    print("=== slice ===")
    (df.withColumn("languages", F.slice(F.col("languagesAtSchool"), 2, 3))
       .drop("languagesAtSchool")
       .show(truncate=False))

    # array_distinct — deduplicate elements inside each array
    print("=== array_distinct ===")
    (df.withColumn("languages", F.array_distinct(F.col("languagesAtSchool")))
       .drop("languagesAtSchool")
       .show(truncate=False))

    # array_contains — boolean membership test
    print("=== array_contains ===")
    (df.withColumn("knows_cpp", F.array_contains(F.col("languagesAtSchool"), "C++"))
       .select("name", "knows_cpp")
       .show())

    # array_join — concatenate elements into a string
    print("=== array_join ===")
    (df.withColumn("languages_csv", F.array_join(F.col("languagesAtSchool"), ", "))
       .select("name", "languages_csv")
       .show(truncate=False))

    # size — element count per row
    print("=== size ===")
    (df.withColumn("lang_count", F.size(F.col("languagesAtSchool")))
       .select("name", "lang_count")
       .show())

    # explode — one row per array element
    print("=== explode ===")
    (df.withColumn("language", F.explode(F.col("languagesAtSchool")))
       .select("name", "language")
       .show(truncate=False))

    spark.stop()