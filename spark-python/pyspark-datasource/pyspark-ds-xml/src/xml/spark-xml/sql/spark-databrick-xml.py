from pyspark.sql import SparkSession
import os

if __name__ == '__main__':
    spark = (SparkSession.builder.master("local[*]")
             .config('spark.jars.packages', 'com.databricks:spark-xml_2.12:0.18.0')
             .appName("spark-db-xml").getOrCreate())
    dataHome = os.environ["DATA_HOME"]
    xmlFile = dataHome + "\\FileData\\Xml\\movies.xml"
    movies = spark.read. \
        format("com.databricks.spark.xml"). \
        option("rootTag", "collection"). \
        option("rowTag", "movie"). \
        load(xmlFile)

    # print(movies.rdd.partitions.length)

    movies.printSchema()
    movies.show(5)
