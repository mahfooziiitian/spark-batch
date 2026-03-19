import os
import sys

from pyspark.sql.functions import col, posexplode

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark =get_spark_session(
        app_name="spark-db-xml-attribute",
        scala_version="2.12",
        spark_xml_version="0.18.0")
    dataHome = os.environ["DATA_HOME"]
    xmlFile = dataHome + "/file_data/xml/pos.xml"
    books = spark.read.format("xml").option("rowTag", "foo").load(xmlFile)

    books = (
        books.select(posexplode(col("bar")))
        .withColumnRenamed("col", "bar")
        .select("pos", "bar.sum", "bar.periods.start", "bar.periods.end")
    )
    books.show(truncate=False)
