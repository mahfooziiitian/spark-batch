import codecs
import os
import sys
from pathlib import Path

import chardet
from pyspark.sql import SparkSession


def write_utf_8_xml(filename: str, content):
    with codecs.open(filename=filename, mode="w", encoding="utf-8") as file:
        file.write(content)


os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    xml_content = """<?xml version="1.0" encoding="utf-8"?>
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
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    data_file = str(data_home / "file_data" / "xml" / "encoding" / "sample_data_utf_8.xml")
    Path(data_file).parent.mkdir(parents=True, exist_ok=True)
    write_utf_8_xml(data_file, xml_content)

    with open(data_file, mode="rb") as file:
        print(chardet.detect(file.read()))

    spark = SparkSession.builder.master("local[*]").appName("XML Encoding UTF-8").getOrCreate()

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
