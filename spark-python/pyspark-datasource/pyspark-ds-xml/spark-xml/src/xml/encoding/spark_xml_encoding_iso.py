import codecs
import os
import sys

from pyspark.sql import SparkSession


def write_utf_iso_xml(filename: str, content):
    with codecs.open(filename=filename, mode="w", encoding="ISO-8859-1") as file:
        file.write(content)


os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":

    xml_content = """<?xml version="1.0" encoding="ISO-8859-1"?>
    <catalog>
       <book id="bk101">
          <author>Barbarella, Matthew</author>
          <title>XML Developer's Guide</title>
          <genre>Computer</genre>
          <price>44.95</price>
          <publish_date>2000-10-01</publish_date>
          <description>An in-depth look at creating applications with XML.</description>
       </book>
    </catalog>
    """
    data_file = (
        f"{os.environ['DATA_HOME']}/file_data/xml/encoding/sample_data_utf_iso.xml"
    )
    write_utf_iso_xml(data_file, xml_content)

    spark = SparkSession.builder.appName("XML Encoding ISO").getOrCreate()

    df = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .option("encoding", "ISO-8859-1")
        .load(data_file)
    )

    df.show()

    (
        df.write.format("com.databricks.spark.xml")
        .option("rootTag", "catalog")
        .option("rowTag", "book")
        .option("encoding", "ISO-8859-1")
        .save(data_file + "_output")
    )
