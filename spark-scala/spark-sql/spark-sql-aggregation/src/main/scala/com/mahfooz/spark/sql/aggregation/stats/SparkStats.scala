/*

Skewness is a measure of the symmetry of the value distribution in a dataset.
Kurtosis is a measure of the shape of the distribution curve, whether the curve is normal, flat, or pointy.

 */
package com.mahfooz.spark.sql.aggregation.stats

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkStats {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("SparkStats")
      .getOrCreate()

    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("d:/data/flights/flight-summary.csv")

    flight_summary.select(skewness("count"), kurtosis("count")).show

    flight_summary
      .select(
        variance("count"),
        var_pop("count"),
        stddev("count"),
        stddev_pop("count")
      )
      .show
  }
}
