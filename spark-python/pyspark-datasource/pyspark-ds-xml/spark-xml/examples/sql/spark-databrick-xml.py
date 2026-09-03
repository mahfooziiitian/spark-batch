import os
from pathlib import Path

from pyspark.sql import SparkSession

from spark_xml.util.sample_data import ensure_movies_xml

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("spark-db-xml").getOrCreate()
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    xmlFile = str(ensure_movies_xml(data_home / "file_data" / "xml" / "movies.xml"))
    movies = spark.read.format("xml").option("rootTag", "collection").option("rowTag", "movie").load(xmlFile)

    # print(movies.rdd.partitions.length)

    movies.printSchema()
    movies.show(5)
