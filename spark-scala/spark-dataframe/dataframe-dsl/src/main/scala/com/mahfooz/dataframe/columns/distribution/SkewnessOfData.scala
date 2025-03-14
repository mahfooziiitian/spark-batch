/*

Skewness is a measure of the symmetry of the value distribution in a dataset.
In a normal distribution or bell-shaped distribution, the skew value is 0.
Positive skew indicates the tail on the right side is longer or fatter than the left side.
Negative skew indicates the opposite, where the tail of the left side is longer or fatter than the right side.

 */
package com.mahfooz.dataframe.columns.distribution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{kurtosis, skewness}

object SkewnessOfData {

  def main(args: Array[String]): Unit = {

    val sourceDataFile = "D:\\data\\flight-data\\csv\\flight-summary.csv"

    val spark = SparkSession.builder
      .appName(SkewnessOfData.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(sourceDataFile)

    flight_summary.select(skewness("count"), kurtosis("count")).show
  }
}
