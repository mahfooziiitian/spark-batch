/*

variance and standard deviation are used to measure the dispersion, or the spread, of the data.

 */
package com.mahfooz.dataframe.columns.distribution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{stddev, stddev_pop, var_pop, variance}

object Variance {

  def main(args: Array[String]): Unit = {

    val sourceDataFile = s"${sys.env("DATA_HOME")}/FileData/Csv/Flight/flight-summary.csv"

    val spark = SparkSession.builder
      .appName(Variance.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(sourceDataFile)

    print(flight_summary.rdd.getNumPartitions)

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
