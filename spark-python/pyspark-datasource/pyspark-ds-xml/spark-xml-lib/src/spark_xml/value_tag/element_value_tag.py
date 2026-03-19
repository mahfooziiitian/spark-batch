import os
import sys


from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark =get_spark_session(
        app_name="spark-db-xml-element-value",
        scala_version="2.12",
        spark_xml_version="0.17.0")
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
