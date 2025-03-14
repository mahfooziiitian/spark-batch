package com.mahfooz.spark.window

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, window}

object WindowStartEnd {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(WindowStartEnd.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val appleOneYearDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("d:/data/stocks/aapl-2017.csv")

    // calculate the weekly average price using window function inside the groupBy transformation

    import spark.implicits._

    // this is an example of the tumbling window, aka fixed window
    val appleWeeklyAvgDF = appleOneYearDF
      .groupBy(window('Date, "1 week"))
      .agg(avg("Close").as("weekly_avg"))

    appleWeeklyAvgDF
      .orderBy("window.start")
      .selectExpr(
        "window.start",
        "window.end",
        "round(weekly_avg, 2) as weekly_avg"
      )
      .show(5)

  }

}
