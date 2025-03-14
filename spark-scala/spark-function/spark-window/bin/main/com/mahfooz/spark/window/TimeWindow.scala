package com.mahfooz.spark.window

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, window}

object TimeWindow {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(TimeWindow.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val appleOneYearDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("d:/data/stocks/aapl-2017.csv")

    // calculate the weekly average price using window function inside the groupBy transformation

    import spark.implicits._

    // 4 weeks window length and slide by one week each time
    val appleMonthlyAvgDF = appleOneYearDF
      .groupBy(window('Date, "4 week", "1 week"))
      .agg(avg("Close").as("monthly_avg"))

    // display the results with order by start time
    appleMonthlyAvgDF.orderBy("window.start")
      .selectExpr("window.start", "window.end",
        "round(monthly_avg, 2) as monthly_avg")
      .show(5)

  }

}
