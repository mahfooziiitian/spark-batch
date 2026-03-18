"""
Column operations: printSchema, column listing, and type inspection.
"""

import os

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    json_path = os.environ.get("DATA_HOME", "/tmp") + "/file-data/json/students.json"

    df = spark.read.json(json_path)
    df.printSchema()
    print(df.columns)
    print(df.select("*").columns)
    print(df.schema["name"].dataType)


if __name__ == "__main__":
    spark = get_spark("dataframe-columns")
    main(spark)
    spark.stop()

    print(df.select("*").schema.names)

    print(df.select("*").schema.fieldNames())
