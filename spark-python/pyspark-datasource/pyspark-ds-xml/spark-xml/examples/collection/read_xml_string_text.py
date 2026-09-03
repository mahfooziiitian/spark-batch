"""Fetch XML over HTTP, stage it as text, and read it with the built-in xml source."""

import os
import sys
import xml.etree.ElementTree as Et
from pathlib import Path

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable


if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.master("local[*]").appName("spark-xml").getOrCreate()

    # Sample list of strings
    import requests

    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    # ca_path = f"{data_home}/certificates/zscaler_root_ca.crt"
    url = "https://sample-files.com/downloads/data/xml/basic-structure.xml"
    data = requests.get(url=url)  # verify=ca_path).text

    data_xml = Et.fromstring(data)
    xml_data_no_header = Et.tostring(element=data_xml, xml_declaration=False).decode()
    print(xml_data_no_header)

    # Create a DataFrame from the list of strings
    df = spark.createDataFrame([data], "string").toDF("value")

    # Show the DataFrame
    df.show(truncate=False)

    data_path = str(data_home / "file_data" / "text" / "xml_api_collection")
    Path(data_path).parent.mkdir(parents=True, exist_ok=True)

    df.write.mode("overwrite").text(data_path)

    ds_option = {"multiLine": "true", "rowTag": "person"}

    spark.read.format("xml").options(**ds_option).load(data_path).show(truncate=False)
