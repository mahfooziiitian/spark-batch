import os, sys
from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_8"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0")
        .appName("spark-db-xml-attribute")
        .getOrCreate()
    )

    df = (
        spark.read.format("xml")
        .option("rowTag", "bk:book")
        .option("ignoreNamespace", "true")
        .load(f"{os.environ['DATA_HOME']}/file_data/xml/book.xml")
    )

    df.show()
