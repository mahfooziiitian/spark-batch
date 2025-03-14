from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]") \
        .appName('SparkByExamples.com') \
        .getOrCreate()

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
            StructField('middlename', StringType(), True),
            StructField('lastname', StringType(), True)
        ])),
        StructField('id', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('salary', IntegerType(), True)
    ])

    df = spark.createDataFrame(data=structureData, schema=structureSchema)
    df.printSchema()
    df.show(truncate=False)

    updatedDF = df.withColumn("OtherInfo",
                              struct(col("id").alias("identifier"),
                                     col("gender").alias("gender"),
                                     col("salary").alias("salary"),
                                     when(col("salary").cast(IntegerType()).__lt__(2000), "Low")
                                     .when(col("salary").cast(IntegerType()).lt__(4000), "Medium")
                                     .otherwise("High").alias("Salary_Grade")
                                     )).drop("id", "gender", "salary")

    ddlSchemaStr = """`fullName` STRUCT<`first`: STRING, 
        `last`: STRING,`middle`: STRING >, 
        `age` INT, `gender` STRING"""

    ddlSchema = StructType().fromDDL(ddlSchemaStr)
    ddlSchema.printTreeString()
