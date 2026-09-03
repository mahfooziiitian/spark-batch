import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from spark_xml.util.sample_data import ensure_person_xml

os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("spark-db-xml").getOrCreate()
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    xmlFile = str(ensure_person_xml(data_home / "file_data" / "xml" / "person.xml"))
    movies = spark.read.format("xml").option("rowTag", "person").load(xmlFile)

    movies.printSchema()
    movies.show(5)
