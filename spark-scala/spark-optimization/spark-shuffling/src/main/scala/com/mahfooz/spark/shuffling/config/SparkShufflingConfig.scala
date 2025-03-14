package com.mahfooz.spark.shuffling.config

import org.apache.spark.sql.SparkSession

object SparkShufflingConfig {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .appName("SparkShufflingConfig")
      .master("local[*]")
      .getOrCreate()

    println(s"default value = ${spark.conf.get("spark.sql.shuffle.partitions")}")
    spark.conf.set("spark.sql.shuffle.partitions", 5)
    println(s"custom value = ${spark.conf.get("spark.sql.shuffle.partitions")}")

  }

}
