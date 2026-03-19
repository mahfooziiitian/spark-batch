import os
import sys

import chardet

from spark_xml.util.session.spark_session_util import get_spark_session


def write_utf_8_xml(filename: str, content):
    with open(file=filename, mode="w", encoding="utf-8") as file:
        file.write(content)


os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":

    spark =get_spark_session(
        app_name="spark-db-xml-attribute",
        scala_version="2.12",
        spark_xml_version="0.18.0")
    
    xml_content = """<?xml version="1.0" encoding="utf-16"?>
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
        f"{os.environ['DATA_HOME']}/file_data/xml/encoding/sample_data_utf_8.xml"
    )
    write_utf_8_xml(data_file, xml_content)

    with open(data_file, mode="rb") as file:
        print(chardet.detect(file.read()))

    df = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .option("charset", "UTF-8")
        .option("multiLine", True)
        .load(data_file)
    )

    df.show(truncate=False)

    (
        df.write.mode("overwrite")
        .format("xml")
        .option("rootTag", "catalog")
        .option("rowTag", "book")
        .option("version", "1.0")
        .option("encoding", "UTF-8")
        .option("charset", "UTF-8")
        .save(f"{data_file}_output")
    )

    df = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .option("charset", "UTF-8")
        .option("multiLine", True)
        .load(f"{data_file}_output")
    )

    df.show(truncate=False)
