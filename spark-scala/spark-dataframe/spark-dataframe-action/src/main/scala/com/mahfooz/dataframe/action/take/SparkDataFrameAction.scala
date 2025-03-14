package com.mahfooz.dataframe.action.take

import org.apache.spark.sql.SparkSession

object SparkDataFrameAction {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkDataFrameAction")
      .getOrCreate()

    val flightData2015 = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("d:/data/flight-data/csv/2015-summary.csv")

    flightData2015.take(3).foreach(record=> println(record.get(0)+" "+record.get(1)))

  }
}
