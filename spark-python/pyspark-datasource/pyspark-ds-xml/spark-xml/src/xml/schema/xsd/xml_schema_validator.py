import os
import sys

from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_8"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0")
        .appName("spark-xml-validator")
        .getOrCreate()
    )

    # data path
    dataHome = os.environ["DATA_HOME"]
    xmlFile = dataHome + "/file_data/xml/orders.xml"
    xmlXsd = dataHome + "/file_data/xml/orders.xsd"

    # adding spark context
    spark.sparkContext.addFile(xmlXsd)

    #
    orders = (
        spark.read.format("com.databricks.spark.xml")
        .option("rowTag", "Root")
        .option("rowValidationXSDPath", "orders.xsd")
        .load(xmlFile)
    )

    orders.printSchema()

    print(orders.schema)
