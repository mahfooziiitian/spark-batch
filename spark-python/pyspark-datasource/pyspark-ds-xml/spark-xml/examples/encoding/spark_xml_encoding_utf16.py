import codecs
import os
import sys
from pathlib import Path

import chardet
from pyspark.sql import SparkSession


def write_utf_16_xml(filename: str, content):
    with codecs.open(filename=filename, mode="w", encoding="utf-16") as file:
        file.write(content)


os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
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
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    data_file = str(data_home / "file_data" / "xml" / "encoding" / "sample_data_utf_16.xml")
    Path(data_file).parent.mkdir(parents=True, exist_ok=True)
    write_utf_16_xml(data_file, xml_content)

    with open(data_file, mode="rb") as file:
        print(chardet.detect(file.read()))

    spark = SparkSession.builder.master("local[*]").appName("XML Encoding UTF-16").getOrCreate()

    df = spark.read.format("xml").option("rowTag", "book").option("charset", "UTF-16").load(data_file)

    df.show(truncate=False)

    # NOTE: Spark's native XML writer always emits encoding="UTF-8" in the XML
    # declaration even when the bytes are written as UTF-16, so a UTF-16 output
    # is not self-consistent and cannot be read back reliably. The read above
    # already demonstrates consuming a real UTF-16-encoded source; here we
    # round-trip the write/read through Spark's default (UTF-8) so it succeeds.
    (
        df.write.mode("overwrite")
        .format("xml")
        .option("rootTag", "catalog")
        .option("rowTag", "book")
        .save(f"{data_file}_output")
    )

    df = spark.read.format("xml").option("rowTag", "book").load(f"{data_file}_output")

    df.show(truncate=False)
