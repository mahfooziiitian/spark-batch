package com.mahfooz.datafram.aggregation.groupby

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MultiAggregationMapPerGroup {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("MultiAggregationMapPerGroup")
      .getOrCreate()

    val flight_summary = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\flight-data\\csv\\flight-summary.csv")

    flight_summary.groupBy("origin_airport")
      .agg(
        "count" -> "count",
        "count" -> "min",
        "count" -> "max",
        "count" -> "sum")
      .show(5)
  }

}
