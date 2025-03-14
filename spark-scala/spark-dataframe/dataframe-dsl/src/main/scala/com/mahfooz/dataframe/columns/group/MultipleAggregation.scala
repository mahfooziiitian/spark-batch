package com.mahfooz.dataframe.columns.group

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{SparkSession, functions}

object MultipleAggregation {

  def main(args: Array[String]): Unit = {

    val sourceDataFile = "D:\\data\\flight-data\\csv\\flight-summary.csv"

    val spark = SparkSession.builder
      .appName(MultipleAggregation.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(sourceDataFile)

    flight_summary
      .groupBy("origin_airport")
      .agg(
        functions.count("count").as("count"),
        functions.min("count"),
        functions.max("count"),
        sum("count")
      )
      .show(5)

    //Specifying Multiple Aggregations Using a Key-Value Map
    flight_summary.groupBy("origin_airport")
      .agg(
        "count" -> "count",
        "count" -> "min",
        "count" -> "max",
        "count" -> "sum")
      .show(5)


  }
}
