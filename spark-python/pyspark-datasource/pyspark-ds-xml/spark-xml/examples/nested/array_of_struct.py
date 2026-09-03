import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

from spark_xml.util.sample_data import ensure_books_catalog_xml

if __name__ == "__main__":
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    xmlFile = str(ensure_books_catalog_xml(data_home / "file_data" / "xml" / "nested" / "books_catalog.xml"))

    spark = SparkSession.builder.appName("array_of_struct").master("local[*]").getOrCreate()

    df = spark.read.format("xml").option("rowTag", "catalog").load(xmlFile)

    df.printSchema()

    (df.withColumn("book", explode(col("book"))).select("dt_creation", "book.*").show(truncate=False))
