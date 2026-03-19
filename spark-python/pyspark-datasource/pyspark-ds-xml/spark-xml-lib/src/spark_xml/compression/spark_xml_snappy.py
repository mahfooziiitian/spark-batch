import os
import sys

from pyspark.sql import SparkSession

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark =get_spark_session(
        app_name="spark_xml_gzip_write",
        scala_version="2.12",
        spark_xml_version="0.18.0")
    dataHome = os.environ["DATA_HOME"]
    xmlFile = dataHome + "/file_data/xml/people.xml.snappy"

    # Example DataFrame
    data = [("John", 28), ("Anna", 23), ("Peter", 34)]
    columns = ["Name", "Age"]
    df1 = spark.createDataFrame(data, columns)

    # Write the DataFrame to a compressed XML file
    (
        df1.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "people")
        .option("rowTag", "person")
        .option("compression", "snappy")
        .save(xmlFile)
    )

    df2 = (
        spark.read.format("xml")
        .option("rowTag", "person")
        .option("compression", "snappy")
        .load(xmlFile)
    )

    # Show the data
    df2.show()
