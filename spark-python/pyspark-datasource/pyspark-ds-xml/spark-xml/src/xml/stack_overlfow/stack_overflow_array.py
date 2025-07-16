import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, posexplode

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_8"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0")
        .appName("spark-db-xml")
        .getOrCreate()
    )
    dataHome = os.environ["DATA_HOME"]
    xmlFile = dataHome + "/file_data/xml/pos.xml"
    books = spark.read.format("xml").option("rowTag", "foo").load(xmlFile)

    books = (
        books.select(posexplode(col("bar")))
        .withColumnRenamed("col", "bar")
        .select("pos", "bar.sum", "bar.periods.start", "bar.periods.end")
    )
    books.show(truncate=False)
