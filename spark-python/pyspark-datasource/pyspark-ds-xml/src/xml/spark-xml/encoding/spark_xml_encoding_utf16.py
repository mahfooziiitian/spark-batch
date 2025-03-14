import codecs
import os

from pyspark.sql import SparkSession

import chardet


def write_utf_16_xml(filename: str, content):
    with codecs.open(filename=filename, mode='w', encoding='utf-16') as file:
        file.write(content)


if __name__ == '__main__':
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
    data_file = f"{os.environ['DATA_HOME']}\\FileData\\Xml\\encoding\\sample_data_utf_16.xml"
    write_utf_16_xml(data_file, xml_content)

    with  open(data_file, mode='rb') as file:
        print(chardet.detect(file.read()))

    spark = (SparkSession.builder.master("local[*]")
             .config('spark.jars.packages', 'com.databricks:spark-xml_2.12:0.18.0')
             .appName("XML Encoding UTF-16").getOrCreate())

    df = (spark.read
          .format("xml")
          .option("rowTag", "book")
          .option("charset", "UTF-16")
          .option("multiLine", True)
          .load(data_file))

    df.show(truncate=False)

    (df.write
     .mode("overwrite")
     .format("xml")
     .option("rootTag", "catalog")
     .option("rowTag", "book")
     .option("version", "1.0")
     .option("encoding", "UTF-16")
     .option("charset", "UTF-16")
     .save(f"{data_file}_output"))

    df = (spark.read
          .format("xml")
          .option("rowTag", "book")
          .option("charset", "UTF-8")
          .option("multiLine", True)
          .load(f"{data_file}_output"))

    df.show(truncate=False)

