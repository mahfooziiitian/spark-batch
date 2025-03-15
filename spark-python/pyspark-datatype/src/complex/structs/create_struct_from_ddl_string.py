from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, when
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("create_struct_from_ddl_string")
        .getOrCreate()
    )

    structure_data = [
        (("James", "", "Smith"), "36636", "M", 3100),
        (("Michael", "Rose", ""), "40288", "M", 4300),
        (("Robert", "", "Williams"), "42114", "M", 1400),
        (("Maria", "Anne", "Jones"), "39192", "F", 5500),
        (("Jen", "Mary", "Brown"), "", "F", -1),
    ]
    structure_schema = StructType(
        [
            StructField(
                "name",
                StructType(
                    [
                        StructField("firstname", StringType(), True),
                        StructField("middlename", StringType(), True),
                        StructField("lastname", StringType(), True),
                    ]
                ),
            ),
            StructField("id", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("salary", IntegerType(), True),
        ]
    )

    df = spark.createDataFrame(data=structure_data, schema=structure_schema)
    df.printSchema()
    df.show(truncate=False)

    updatedDF = df.withColumn(
        "OtherInfo",
        struct(
            col("id").alias("identifier"),
            col("gender").alias("gender"),
            col("salary").alias("salary"),
            when(col("salary").cast(IntegerType()).__lt__(2000), "Low")
            .when(col("salary").cast(IntegerType()).__lt__(4000), "Medium")
            .otherwise("High")
            .alias("Salary_Grade"),
        ),
    ).drop("id", "gender", "salary")

    ddlSchemaStr = """`fullName` STRUCT<`first`: STRING,
        `last`: STRING,`middle`: STRING >,
        `age` INT, `gender` STRING"""

    # ddlSchema = StructType().fromDDL(ddlSchemaStr)
    # ddlSchema.printTreeString()
