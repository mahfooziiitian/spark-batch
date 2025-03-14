package com.mahfooz.spark.window

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, window}

object SparkSimpleWindow {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(SparkSimpleWindow.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val appleOneYearDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("d:/data/stocks/aapl-2017.csv")

    // display the schema, the first column is the transaction date
    appleOneYearDF.printSchema

    // calculate the weekly average price using window function inside the groupBy transformation

    import  spark.implicits._

    // this is an example of the tumbling window, aka fixed window
    val appleWeeklyAvgDF = appleOneYearDF
      .groupBy(window('Date, "1 week"))
      .agg(avg("Close").as("weekly_avg"))

    // the result schema has the window start and end time
    appleWeeklyAvgDF.printSchema

    appleWeeklyAvgDF.show()

  }
}
