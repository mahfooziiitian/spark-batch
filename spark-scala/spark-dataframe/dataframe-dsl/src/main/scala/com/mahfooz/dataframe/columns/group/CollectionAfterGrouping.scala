package com.mahfooz.dataframe.columns.group

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object CollectionAfterGrouping {

  def main(args: Array[String]): Unit = {

    val sourceDataFile = "D:\\data\\flight-data\\csv\\flight-summary.csv"

    val spark = SparkSession.builder
      .appName(CollectionAfterGrouping.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(sourceDataFile)

    import spark.implicits._

    val highCountDestCities = flight_summary.where('count > 5500)
      .groupBy("origin_state")
      .agg(collect_list("dest_city").
        as("dest_cities"))

    highCountDestCities.withColumn("dest_city_count", size('dest_cities)).
      show(5, false)
  }
}
