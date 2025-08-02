package com.mahfooz.spark.sql.aggregation.count

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkCountAggr {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("CoalescePartition")
      .getOrCreate()

    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("d:/data/flights/flight-summary.csv")

    // use count action to find out number of rows in this data set
    println(flight_summary.count())

    flight_summary
      .select(count("origin_airport"), count("dest_airport").as("dest_count"))
      .show

  }
}
