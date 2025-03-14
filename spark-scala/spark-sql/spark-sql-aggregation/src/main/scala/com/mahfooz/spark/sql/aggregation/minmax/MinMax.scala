package com.mahfooz.spark.sql.aggregation.minmax

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MinMax {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("MinMax")
      .getOrCreate()

    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("d:/data/flights/flight-summary.csv")

    flight_summary.select(min("count"), max("count")).show

    flight_summary.select(sum("count")).show

    flight_summary.select(sumDistinct("count")).show

  }
}
