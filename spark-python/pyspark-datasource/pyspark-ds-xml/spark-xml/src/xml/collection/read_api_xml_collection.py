import os
import sys
import tempfile

import chardet
import requests
from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable


def write_data_with_temp_file(xml_string: str) -> str:
    """
    Write data to temp file
    :return:
    """
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as temp_file:
        temp_file.write(xml_string.encode("utf-8"))
        temp_file_path = temp_file.name

    return temp_file_path


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]").appName("spark-db-xml").getOrCreate()
    )
    data_home = os.environ.get("DATA_HOME", "/tmp/data")
    ca_path = f"{data_home}/certificates/zscaler_root_ca.crt"
    url = "https://sample-files.com/downloads/data/xml/basic-structure.xml"
    data = requests.get(url=url, verify=ca_path).text
    print(data)
    options = {"rowTag": "person", "multiLine": True, "mode": "FAILFAST"}

    temp_file_path = write_data_with_temp_file(data)
    print(temp_file_path)

    print(chardet.detect(data.encode()))
    df = spark.read.format("xml").options(**options).load(temp_file_path)

    df.show(truncate=False)
