import os
import sys

import pandas as pd
import requests

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark = get_spark_session(
        app_name="spark-db-xml-attribute",
        scala_version="2.12",
        spark_xml_version="0.17.0",
    )

    data_home = os.environ.get("DATA_HOME", "/tmp/data")
    ca_path = f"{data_home}/certificates/zscaler_root_ca.crt"
    url = "https://sample-files.com/downloads/data/xml/basic-structure.xml"
    data = requests.get(url=url, verify=ca_path).text
    print(data)

    pdf = pd.read_xml(data)
    print(pdf)

    sparkDF = spark.createDataFrame(pdf)
    sparkDF.printSchema()
    sparkDF.show()
