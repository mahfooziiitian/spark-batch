import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, posexplode

from spark_xml.util.sample_data import ensure_pos_xml

os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("spark-db-xml").getOrCreate()
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    xmlFile = str(ensure_pos_xml(data_home / "file_data" / "xml" / "pos.xml"))
    books = spark.read.format("xml").option("rowTag", "foo").load(xmlFile)

    books = (
        books.select(posexplode(col("bar")))
        .withColumnRenamed("col", "bar")
        .select("pos", "bar.sum", "bar.periods.start", "bar.periods.end")
    )
    books.show(truncate=False)
