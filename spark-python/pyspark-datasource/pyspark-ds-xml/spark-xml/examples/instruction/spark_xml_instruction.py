import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable

SAMPLE_NOTES_XML = """<?xml version="1.0" encoding="UTF-8"?>
<notes>
  <row>
    <to>Tove</to>
    <from>Jani</from>
    <heading>Reminder</heading>
    <body>Don't forget me this weekend!</body>
  </row>
  <row>
    <to>Jani</to>
    <from>Tove</from>
    <heading>Re: Reminder</heading>
    <body>I will not!</body>
  </row>
</notes>
"""

if __name__ == "__main__":
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    xml_path = data_home / "file_data" / "xml" / "notes.xml"
    xml_path.parent.mkdir(parents=True, exist_ok=True)
    if not xml_path.exists():
        xml_path.write_text(SAMPLE_NOTES_XML, encoding="utf-8")
    xmlFile = str(xml_path)

    spark = SparkSession.builder.appName("instruction").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.format("xml").option("rowTag", "row").load(xmlFile)

    df.printSchema()
    df.show(truncate=False)

    spark.stop()
