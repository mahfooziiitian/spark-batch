import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    dataHome = os.environ["DATA_HOME"]
    xmlFile = dataHome + "\\FileData\\Xml\\notes.xml"
    print(os.environ["JAVA_HOME"])

    spark = (SparkSession.builder
             .appName("instruction")
             .config('spark.jars.packages', 'com.databricks:spark-xml_2.12:0.18.0')
             .master("local[*]")
             .getOrCreate())

    df = (spark.read.format("xml")
          .option("rowTag", "row")
          .load(xmlFile)
          )

    df.printSchema()

    (df .show(truncate=False))
