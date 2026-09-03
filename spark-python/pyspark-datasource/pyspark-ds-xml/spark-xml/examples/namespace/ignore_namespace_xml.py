import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from spark_xml.util.sample_data import ensure_book_namespace_xml

os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("spark-db-xml-attribute").getOrCreate()

    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    xml_file = str(ensure_book_namespace_xml(data_home / "file_data" / "xml" / "book.xml"))
    df = spark.read.format("xml").option("rowTag", "bk:book").option("ignoreNamespace", "true").load(xml_file)

    df.show()
