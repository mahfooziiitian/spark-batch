"""
Print a nested StructType schema and its JSON representation.
"""

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    schema = StructType(
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

    data = [
        (("James", "", "Smith"), "36636", "M", 3100),
        (("Michael", "Rose", ""), "40288", "M", 4300),
        (("Robert", "", "Williams"), "42114", "M", 1400),
        (("Maria", "Anne", "Jones"), "39192", "F", 5500),
        (("Jen", "Mary", "Brown"), "", "F", -1),
    ]

    df = spark.createDataFrame(data, schema)
    df.printSchema()
    df.show(truncate=False)
    print(df.schema.json())


if __name__ == "__main__":
    spark = get_spark("schema-print-json")
    main(spark)
    spark.stop()
