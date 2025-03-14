package com.mahfooz.datafram.aggregation.basic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, count, countDistinct}

object SparkCountAggregation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkCountAggregation")
      .getOrCreate()

    val flight_summary = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\flight-data\\csv\\flights.csv")

    flight_summary.select(countDistinct("origin_airport"),
      countDistinct("destination_airport"),
      count("*")).show

    flight_summary.select(count("origin_airport"),countDistinct("destination_airport"),
      approx_count_distinct("origin_airport", 0.05)).show

  }
}
