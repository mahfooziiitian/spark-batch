import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("spark-db-xml").getOrCreate()
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    xmlFile = str(data_home / "file_data" / "xml" / "books.xml.gz")
    books = spark.read.format("xml").option("rowTag", "book").load(xmlFile)

    books.printSchema()
    books.show(truncate=False)
