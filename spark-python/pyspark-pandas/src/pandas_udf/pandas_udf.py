import os

import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "C:\\Users\\mahfo\\.virtualenvs\\PysparkPandas-bh8Y5n2t\\Scripts\\python.exe"


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()


    @pandas_udf("col1 string, col2 long")
    def func(s1: pd.Series, s2: pd.Series, s3: pd.DataFrame) -> pd.DataFrame:
        s3['col2'] = s1 + s2.str.len()
        return s3

        # Create a Spark DataFrame that has three columns including a struct column.
    df = spark.createDataFrame(
            [[1, "a string", ("a nested string",)]],
            "long_col long, string_col string, struct_col struct<col1:string>")

    df.printSchema()
    df.select(func("long_col", "string_col", "struct_col")).printSchema()