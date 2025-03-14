package com.mahfooz.spark.optimizer.shuffle

import org.apache.spark.sql.SparkSession

object Shuffling {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val flightData2015 = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(getClass.getResource("/csv/2015-summary.csv").getFile)

    flightData2015.sort("count").take(2)
  }
}
