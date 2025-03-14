/*

Performing aggregation with grouping is a two-step process.

 */
package com.mahfooz.spark.sql.aggregation.group

import org.apache.spark.sql.SparkSession

object GroupByAggregation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("GroupByAggregation")
      .getOrCreate()

    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("d:/data/flights/flight-summary.csv")

    flight_summary
      .groupBy("origin_airport")
      .count()
      .orderBy("count")
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
