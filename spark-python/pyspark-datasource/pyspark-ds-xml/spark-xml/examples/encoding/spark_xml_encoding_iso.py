import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def write_utf_iso_xml(filename: str, content):
    with open(filename, mode="w", encoding="ISO-8859-1") as file:
        file.write(content)


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
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    data_file = str(data_home / "file_data" / "xml" / "encoding" / "sample_data_utf_iso.xml")
    Path(data_file).parent.mkdir(parents=True, exist_ok=True)
    write_utf_iso_xml(data_file, xml_content)

    spark = SparkSession.builder.appName("XML Encoding ISO").getOrCreate()

    df = spark.read.format("xml").option("rowTag", "book").option("encoding", "ISO-8859-1").load(data_file)

    df.show()

    (
        df.write.mode("overwrite")
        .format("xml")
        .option("rootTag", "catalog")
        .option("rowTag", "book")
        .option("encoding", "ISO-8859-1")
        .save(data_file + "_output")
    )
