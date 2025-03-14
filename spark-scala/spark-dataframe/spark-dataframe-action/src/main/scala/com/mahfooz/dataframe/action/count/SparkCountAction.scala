package com.mahfooz.dataframe.action.count

import org.apache.spark.sql.SparkSession

object SparkCountAction {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkCountAction")
      .getOrCreate()

    val flightData2015 = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("d:/data/flight-data/csv/2015-summary.csv")

    flightData2015.sort("count").explain()
  }

}
