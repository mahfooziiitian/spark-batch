package com.mahfooz.spark.partition

import org.apache.spark.sql.SparkSession

object CoalescePartition {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("CoalescePartition")
      .getOrCreate()

    val movies = spark.read.json(args(0))
    val singlePartitionDF = movies.coalesce(1)

    movies.write.partitionBy("produced_year").save("C:/tmp/output/movies")
  }
}
