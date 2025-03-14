package com.mahfooz.spark.dataframe.action

import org.apache.spark.sql.SparkSession

object Actions {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val flightData2015 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(getClass.getResource("/csv/2015-summary.csv").getFile)

    flightData2015.take(3).foreach(println)
  }
}
