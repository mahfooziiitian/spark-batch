package com.mahfooz.spark.analytics.window.window

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, window}

object SparkWindow {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkWindow")
      .getOrCreate()

    val dataHome  = sys.env.getOrElse("DATA_HOME","stocks")

    val appleOneYearDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(s"${dataHome}/Processing/Spark/DataSources/Csv/stocks.csv")

    appleOneYearDF.printSchema

    import spark.implicits._

    // calculate the weekly average price using window function inside the groupBy transformation
    val appleWeeklyAvgDF = appleOneYearDF
      .groupBy(window('Date, "1 week"))
      .agg(avg("Close").as("weekly_avg"))

    appleWeeklyAvgDF.show

    // the result schema has the window start and end time
    appleWeeklyAvgDF.printSchema

    // display the result with ordering by start time and round up to 2 decimal points
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
