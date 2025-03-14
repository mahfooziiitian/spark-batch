import os
import sys

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StringType, ArrayType, IntegerType
from pyspark.sql.functions import col, explode, collect_list

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("array_of_struct")
             .master("local[*]")
             .getOrCreate())

    arrayStructData = [
        Row("James", [Row("Java", "XX", 120), Row("Scala", "XA", 300)]),
        Row("Michael", [Row("Java", "XY", 200), Row("Scala", "XB", 500)]),
        Row("Robert", [Row("Java", "XZ", 400), Row("Scala", "XC", 250)]),
        Row("Washington", None)
    ]

    arrayStructSchema = (
        StructType()
        .add("name", StringType())
        .add("books_interested",
             ArrayType(
                 StructType()
                 .add("name", StringType())
                 .add("author", StringType())
                 .add("pages", IntegerType())
             )
             , True)
    )

    df = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructData),
                               arrayStructSchema)
    df.printSchema()
    df.show(truncate=False)

    df2 = df.select("name", explode(col("books_interested")))
    df2.printSchema()
    df2.show(truncate=False)
    df2.groupBy(col("name")).agg(collect_list(col("col")).alias("books_interested")).show(truncate=False)

# end main
