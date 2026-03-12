import os
import sys

from pyspark.sql import SparkSession

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark =get_spark_session(
        app_name="spark-db-xml-attribute",
        scala_version="2.12",
        spark_xml_version="0.18.0")

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
