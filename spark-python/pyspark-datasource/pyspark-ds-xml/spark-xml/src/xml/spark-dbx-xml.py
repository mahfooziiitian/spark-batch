import os
import sys

from pyspark.sql import SparkSession
os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("spark-db-xml")
        .getOrCreate()
    )
    dataHome = os.environ["DATA_HOME"]
    xmlFile = dataHome + "/file_data/xml/person.xml"
    movies = (
        spark.read.format("com.databricks.spark.xml")
        .option("rowTag", "person")
        .load(xmlFile)
    )

    movies.printSchema()
    movies.show(5)
