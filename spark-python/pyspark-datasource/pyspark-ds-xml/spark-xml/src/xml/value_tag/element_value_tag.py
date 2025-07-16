import os
import sys

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
    dataHome = os.environ["DATA_HOME"]
    xmlFile = os.path.join(dataHome, "file_data", "xml", "books.xml")
    books_df = (
        spark.read.format("com.databricks.spark.xml")
        .option("rootTag", "books")
        .option("rowTag", "book")
        .option("attributePrefix", "attr_")
        .load(xmlFile)
    )

    books_df.printSchema()
    books_df.show(5)
    books_df.select("price._VALUE", "price.attr_currency").show()
