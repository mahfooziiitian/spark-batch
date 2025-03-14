from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = (SparkSession.builder
             .appName("config-option")
             .master("local[*]")
             .getOrCreate())

    for config in spark.sparkContext.getConf().getAll():
        print(config)
