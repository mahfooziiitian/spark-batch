import os
import sys
import tempfile

import chardet
from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk1.8.0_251"
os.environ["PYSPARK_PYTHON"] = sys.executable


def write_data_with_temp_file(xml_string: str) -> str:
    """
    Write data to temp file
    :return:
    """
    with tempfile.NamedTemporaryFile(delete=False, suffix='.xml') as temp_file:
        temp_file.write(xml_string.encode('utf-8'))
        temp_file_path = temp_file.name

    return temp_file_path


if __name__ == '__main__':
    spark = (SparkSession.builder.master("local[*]")
             .config('spark.jars.packages', 'com.databricks:spark-xml_2.12:0.18.0')
             .appName("spark-db-xml").getOrCreate())
    import requests

    url = "https://thetestrequest.com/authors.xml"
    data = requests.get(url).text
    print(data)
    options = {
        "rowTag": "object",
        "multiLine": True,
        "mode": "FAILFAST"
    }

    temp_file_path = write_data_with_temp_file(data)
    print(temp_file_path)

    print(chardet.detect(data.encode()))
    df = spark.read.format("xml").options(**options).load(temp_file_path)

    df.show(truncate=False)
