import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

if __name__ == '__main__':
    dataHome = os.environ["DATA_HOME"]
    xmlFile = dataHome + "\\FileData\\Xml\\books.xml"
    print(os.environ["JAVA_HOME"])

    spark = (SparkSession.builder
             .appName("array_of_struct")
             .config('spark.jars.packages', 'com.databricks:spark-xml_2.12:0.18.0')
             .master("local[*]")
             .getOrCreate())

    df = (spark.read.format("com.databricks.spark.xml")
          .option("rowTag", "catalog")
          .load(xmlFile)
          )

    df.printSchema()

    (df.withColumn("book", explode(col("book")))
     .select("dt_creation", "book.*")
     .show(truncate=False))
