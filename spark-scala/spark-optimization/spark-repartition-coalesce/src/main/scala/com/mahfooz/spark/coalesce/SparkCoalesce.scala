package com.mahfooz.spark.coalesce

import org.apache.spark.sql.SparkSession

object SparkCoalesce {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[5]")
      .appName("SparkCoalesce")
      .getOrCreate()

    val rdd1 = spark.sparkContext.parallelize(Range(0,25), 6)
    println("parallelize : "+rdd1.partitions.size)

    val rdd3 = rdd1.coalesce(4)
    println("Repartition size : "+rdd3.partitions.size)
  }

}
