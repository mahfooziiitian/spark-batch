import os
import sys

from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("spark-db-xml-attribute")
        .getOrCreate()
    )

    df = (
        spark.read.format("xml")
        .option("rowTag", "bk:book")
        .option("ignoreNamespace", "false")
        .load(f"{os.environ['DATA_HOME']}/file_data/xml/book.xml")
    )

    df.show()
