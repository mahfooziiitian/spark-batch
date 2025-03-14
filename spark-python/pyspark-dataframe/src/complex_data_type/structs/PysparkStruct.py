import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    spark = SparkSession.builder.appName("struct").master("local[*]").getOrCreate()

    structureData = [
        (("James", "", "Smith"), "36636", "M", 3100),
        (("Michael", "Rose", ""), "40288", "M", 4300),
        (("Robert", "", "Williams"), "42114", "M", 1400),
        (("Maria", "Anne", "Jones"), "39192", "F", 5500),
        (("Jen", "Mary", "Brown"), "", "F", -1)
    ]
    structureSchema = StructType([
        StructField('name', StructType([
            StructField('firstname', StringType(), True),
            StructField('middle_name', StringType(), True),
            StructField('lastname', StringType(), True)
        ])),
        StructField('id', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('salary', IntegerType(), True)
    ])

    df = spark.createDataFrame(data=structureData, schema=structureSchema)
    df.printSchema()
    df.show(truncate=False)

    from pyspark.sql.functions import col, struct, when

    updatedDF = df.withColumn("OtherInfo",
                              struct(col("id").alias("identifier"),
                                     col("gender").alias("gender"),
                                     col("salary").alias("salary"),
                                     when(col("salary").cast(IntegerType()) < 2000, "Low")
                                     .when(col("salary").cast(IntegerType()) < 4000, "Medium")
                                     .otherwise("High").alias("Salary_Grade")
                                     )).drop("id", "gender", "salary")

    updatedDF.printSchema()
    updatedDF.show(truncate=False)
