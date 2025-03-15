import os
import sys

from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":

    spark = SparkSession.builder.appName("example").getOrCreate()
    json_data_file = os.environ["DATA_HOME"] + "/file-data/json/students.json"
    df = spark.read.json(json_data_file)

    df.printSchema()

    print(df.columns)
    print(df.select("*").columns)

    print(df.schema["name"].dataType)

    print(df.select("*").schema.names)

    print(df.select("*").schema.fieldNames())
