import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from spark_xml.util.sample_data import ensure_books_priced_xml

os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("spark-db-xml-attribute").getOrCreate()
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    xmlFile = str(ensure_books_priced_xml(data_home / "file_data" / "xml" / "value_tag" / "books.xml"))
    books_df = (
        spark.read.format("xml")
        .option("rootTag", "books")
        .option("rowTag", "book")
        .option("attributePrefix", "attr_")
        .load(xmlFile)
    )

    books_df.printSchema()
    books_df.show(5)
    books_df.select("price._VALUE", "price.attr_currency").show()
