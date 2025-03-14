from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").getOrCreate()

    spark.range(1000).filter("id > 100").selectExpr("sum(id)").explain()
