package com.mahfooz.spark.shuffling

import org.apache.spark.sql.SparkSession

object SparkShuffling {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[5]")
      .appName("SparkShuffling")
      .getOrCreate()

    val df=spark.range(0,20)
    val df4 = df.groupBy("id").count()
    println(df4.rdd.getNumPartitions)

    val df3 = df.coalesce(2)
    println(df3.rdd.partitions.length)
  }

}
