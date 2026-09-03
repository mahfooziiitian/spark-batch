import os
import sys
from pathlib import Path

import pandas as pd
import requests
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("spark-db-xml").getOrCreate()

    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    # ca_path = f"{data_home}/certificates/zscaler_root_ca.crt"
    url = "https://sample-files.com/downloads/data/xml/basic-structure.xml"
    data = requests.get(url=url)  # verify=ca_path).text
    print(data)

    pdf = pd.read_xml(data)
    print(pdf)

    sparkDF = spark.createDataFrame(pdf)
    sparkDF.printSchema()
    sparkDF.show()
