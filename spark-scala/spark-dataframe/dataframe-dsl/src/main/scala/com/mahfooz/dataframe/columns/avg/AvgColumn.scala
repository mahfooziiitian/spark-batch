package com.mahfooz.dataframe.columns.avg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, count, sum}

object AvgColumn {

  def main(args: Array[String]): Unit = {

    val sourceDataFile = "D:\\data\\flight-data\\csv\\flight-summary.csv"

    val spark = SparkSession.builder
      .appName(AvgColumn.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(sourceDataFile)

    flight_summary.select(avg("count"), (sum("count") / count("count"))).show
  }
}
