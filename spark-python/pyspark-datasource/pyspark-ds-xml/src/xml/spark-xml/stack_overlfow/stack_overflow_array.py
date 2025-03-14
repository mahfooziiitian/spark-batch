import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import posexplode, col

os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk1.8.0_251"
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == '__main__':
    spark = (SparkSession.builder.master("local[*]")
             .config('spark.jars.packages', 'com.databricks:spark-xml_2.12:0.18.0')
             .appName("spark-db-xml").getOrCreate())
    dataHome = os.environ["DATA_HOME"]
    xmlFile = dataHome + "\\FileData\\Xml\\pos.xml"
    books = (spark.read
             .format("xml")
             .option("rowTag", "foo")
             .load(xmlFile))

    books = (books.select(posexplode(col("bar"))).withColumnRenamed("col", "bar")
             .select("pos", "bar.sum", "bar.periods.start", "bar.periods.end"))
    books.show(truncate=False)
