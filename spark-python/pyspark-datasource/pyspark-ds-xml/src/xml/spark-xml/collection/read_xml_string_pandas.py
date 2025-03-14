import os
import sys

from pyspark.sql import SparkSession
import pandas as pd
os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk1.8.0_251"
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == '__main__':
    spark = (SparkSession.builder.master("local[*]")
             .config('spark.jars.packages', 'com.databricks:spark-xml_2.12:0.18.0')
             .appName("spark-db-xml").getOrCreate())
    import requests
    url = "https://thetestrequest.com/authors.xml"
    data = requests.get(url).text
    print(data)

    pdf = pd.read_xml(data)
    print(pdf)

    sparkDF = spark.createDataFrame(pdf)
    sparkDF.printSchema()
    sparkDF.show()


