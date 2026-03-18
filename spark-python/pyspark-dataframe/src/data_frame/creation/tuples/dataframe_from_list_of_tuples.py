"""
Create a DataFrame from a list of tuples with an explicit column-name list.
Types are inferred from the Python values in the first row.
"""

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    data = [
        ("James", "", "Smith", "1991-04-01", "M", 3000),
        ("Michael", "Rose", "", "2000-05-19", "M", 4000),
        ("Robert", "", "Williams", "1978-09-05", "M", 4000),
        ("Maria", "Anne", "Jones", "1967-12-01", "F", 4000),
        ("Jen", "Mary", "Brown", "1980-02-17", "F", -1),
    ]
    columns = ["firstname", "middle_name", "lastname", "dob", "gender", "salary"]

    # --- inferred schema (column names only) ---
    df = spark.createDataFrame(data=data, schema=columns)
    df.printSchema()
    df.show(truncate=False)

    # --- explicit StructType (recommended for production) ---
    schema = StructType(
        [
            StructField("firstname", StringType(), nullable=True),
            StructField("middle_name", StringType(), nullable=True),
            StructField("lastname", StringType(), nullable=True),
            StructField("dob", StringType(), nullable=True),
            StructField("gender", StringType(), nullable=True),
            StructField("salary", IntegerType(), nullable=True),
        ]
    )
    df_typed = spark.createDataFrame(data=data, schema=schema)
    df_typed.printSchema()
    df_typed.show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("creation-from-tuples")
    main(spark)
    spark.stop()
