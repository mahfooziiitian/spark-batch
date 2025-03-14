# Imports
from pyspark import SparkConf
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Create a SparkConf object
    conf = (SparkConf().setAppName("MyApp")
            .setMaster("local[2]")
            .set("spark.executor.memory", "2g"))

    # Create a SparkSession object
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # Retrieve the SparkConf object from the SparkContext
    conf = spark.sparkContext.getConf()

    # Print the configuration settings
    print("spark.app.name = ", conf.get("spark.app.name"))
    print("spark.master = ", conf.get("spark.master"))
    print("spark.executor.memory = ", conf.get("spark.executor.memory"))
