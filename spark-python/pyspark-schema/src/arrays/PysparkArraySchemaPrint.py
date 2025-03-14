# import SparkSession for creating a session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, slice, expr, array_distinct, array_contains, array_join

# and import struct types and data types
from pyspark.sql.types import StructType, StringType, ArrayType

if __name__ == '__main__':
    # create an app named schema print
    spark = (SparkSession.builder
             .appName("schema-print")
             .master("local[*]")
             .getOrCreate()
             )

    arrayStructureData = [
        ("James,,Smith", ["Java", "Scala", "C++", "Pascal", "Spark"]),
        ("Michael,Rose,", ["Spark", "Java", "C++", "Scala", "PHP"]),
        ("Robert,,Williams", ["CSharp", "VB", ".Net", "C#.net", ""])
    ]

    arrayStructureSchema = StructType() \
        .add("name", StringType()) \
        .add("languagesAtSchool", ArrayType(StringType()))

    df = spark.createDataFrame(
        spark.sparkContext.parallelize(arrayStructureData), arrayStructureSchema)

    df.show(truncate=False)
    df.printSchema()

    # Slice() function usage
    sliceDF = df.withColumn("languages", slice(col("languagesAtSchool"), 2, 3))\
        .drop("languagesAtSchool")

    sliceDF.printSchema()
    sliceDF.show(truncate=False)

    # Using slice() on Spark SQL expression
    df.withColumn("languages", expr("slice(languagesAtSchool, 2, 3)"))\
        .drop("languagesAtSchool")

    # array_except

    # array_distinct
    distinct_df = df.withColumn("result", array_distinct(col("languagesAtSchool")))
    distinct_df.show(truncate=False)

    # array_contains
    arr_contains_df = df.withColumn("result", array_contains(col("languagesAtSchool"), "C++"))
    arr_contains_df.show()

    # array_join
    arr_join_df = df.withColumn("result", array_join(col("languagesAtSchool"), ","))
    arr_join_df.show()
