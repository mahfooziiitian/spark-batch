package com.mahfooz.spark.analytics.window.window

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, window}

object SparkSlidingWindow {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkSlidingWindow")
      .getOrCreate()

    val appleOneYearDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("d:/data/stocks/aapl-2017.csv")

    import spark.implicits._

    // 4 weeks window length and slide by one week each time
    val appleMonthlyAvgDF = appleOneYearDF
      .groupBy(window('Date, "4 week", "1 week"))
      .agg(avg("Close").as("monthly_avg"))

    // display the results with order by start time
    appleMonthlyAvgDF
      .orderBy("window.start")
      .selectExpr(
        "window.start",
        "window.end",
        "round(monthly_avg, 2) as monthly_avg"
      )
      .show(5)

  }
}
