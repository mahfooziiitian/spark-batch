package com.mahfooz.spark.sql.aggregation.group

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MultiAggregationPerGroup {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("MultiAggrPerGroup")
      .getOrCreate()

    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("d:/data/flights/flight-summary.csv")

    flight_summary
      .groupBy("origin_airport")
      .agg(
        count("count").as("count"),
        min("count"),
        max("count"),
        sum("count")
      )
      .show(5)

    //Specifying Multiple Aggregations Using a Key-Value Map
    flight_summary
      .groupBy("origin_airport")
      .agg(
        "count" -> "count",
        "count" -> "min",
        "count" -> "max",
        "count" -> "sum"
      )
      .show(5)

  }
}
