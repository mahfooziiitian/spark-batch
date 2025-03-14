package com.mahfooz.datafram.aggregation.groupby

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions.size

object SparkCollectionAsGroup {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkCollectionAsGroup")
      .getOrCreate()

    val flight_summary = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\flight-data\\csv\\flight-summary.csv")

    import spark.implicits._

    val highCountDestCities = flight_summary.where('count > 5500)
      .groupBy("origin_state")
      .agg(collect_list("dest_city").
        as("dest_cities"))

    highCountDestCities.withColumn("dest_city_count", size('dest_cities)).
      show(5, false)
  }

}
