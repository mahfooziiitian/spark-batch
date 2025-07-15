import os

from pyspark.sql import SparkSession

if __name__ == "__main__":
    exampleDir = os.path.join(os.environ["SPARK_HOME"], "examples/jars")
    exampleJars = [os.path.join(exampleDir, x) for x in os.listdir(exampleDir)]
    spark = (
        SparkSession.builder.config("spark.jars", ",".join(exampleJars))
        .appName("spark-db-xml")
        .getOrCreate()
    )
    dataHome = os.environ["DATA_HOME"]
    xmlFile = dataHome + "/file_data/xml/movies.xml"
    movies = (
        spark.read.format("com.databricks.spark.xml")
        .option("rootTag", "collection")
        .option("rowTag", "movie")
        .load(xmlFile)
    )

    # print(movies.rdd.partitions.length)

    movies.printSchema()
    movies.show(5)
