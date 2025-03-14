import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, col, from_json
from pyspark.sql.types import StringType, MapType

os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk1.8.0_251"
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .appName('dataframe_to_json')
             .getOrCreate()
             )

    jsonString = """{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
    df = spark.createDataFrame([(1, jsonString)], ["id", "value"])
    df.printSchema()
    df.show(truncate=False)

    schema = MapType(StringType(), StringType()).json()
    df = df.withColumn("value", from_json(df.value, schema))
    df.printSchema()
    df.show(truncate=False)

    (df.withColumn("value", to_json(col("value")))
     .show(truncate=False))
