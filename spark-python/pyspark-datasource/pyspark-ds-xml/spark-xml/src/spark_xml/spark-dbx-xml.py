import os
import sys


from spark_xml.util.session.spark_session_util import get_spark_session
os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark =get_spark_session(
        app_name="spark-db-xml-attribute",
        scala_version="2.12",
        spark_xml_version="0.18.0")
    dataHome = os.environ["DATA_HOME"]
    xmlFile = dataHome + "/file_data/xml/person.xml"
    movies = (
        spark.read.format("com.databricks.spark.xml")
        .option("rowTag", "person")
        .load(xmlFile)
    )

    movies.printSchema()
    movies.show(5)
