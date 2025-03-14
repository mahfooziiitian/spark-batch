import os
import sys

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StringType, ArrayType
from pyspark.sql.functions import col, flatten

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("SparkByExamples.com")
             .master("local[1]")
             .getOrCreate())

    arrayArrayData = [
        Row("James", [["Java", "Scala", "C++"], ["Spark", "Java"]]),
        Row("Michael", [["Spark", "Java", "C++"], ["Spark", "Java"]]),
        Row("Robert", [["CSharp", "VB"], ["Spark", "Python"]])
    ]

    arrayArraySchema = (
        StructType()
        .add("name", StringType())
        .add("subjects", ArrayType(ArrayType(StringType())))
    )

    df = spark.createDataFrame(spark.sparkContext.parallelize(arrayArrayData), arrayArraySchema)
    df.printSchema()
    df.show(truncate=False)

    df.select(col("name"), flatten(col("subjects"))).show(truncate=False)

# end main
