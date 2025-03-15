import os
import sys

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, flatten
from pyspark.sql.types import ArrayType, StringType, StructType

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("SparkByExamples.com")
        .master("local[1]")
        .getOrCreate()
    )

    arrayArrayData = [
        Row(name="James", subjects=[["Java", "Scala", "C++"], ["Spark", "Java"]]),
        Row(name="Michael", subjects=[["Spark", "Java", "C++"], ["Spark", "Java"]]),
        Row(name="Robert", subjects=[["CSharp", "VB"], ["Spark", "Python"]]),
    ]

    arrayArraySchema = (
        StructType()
        .add("name", StringType())
        .add("subjects", ArrayType(ArrayType(StringType())))
    )

    df = spark.createDataFrame(
        spark.sparkContext.parallelize(arrayArrayData), arrayArraySchema
    )
    df.printSchema()
    df.show(truncate=False)

    df.select(col("name"), flatten(col("subjects"))).show(truncate=False)

# end main
