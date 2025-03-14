package com.mahfooz.dataframe.columns.group

import org.apache.spark.sql.SparkSession

object AggregationGroupBy {

  def main(args: Array[String]): Unit = {

    val sourceDataFile = "D:\\data\\flight-data\\csv\\flight-summary.csv"

    val spark = SparkSession.builder
      .appName(AggregationGroupBy.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(sourceDataFile)

    flight_summary
      .groupBy("origin_airport")
      .count()
      .show(5, false)

    import spark.implicits._

    flight_summary
      .groupBy('origin_state, 'origin_city)
      .count
      .where('origin_state === "CA")
      .orderBy('count.desc)
      .show(5)

  }
}
