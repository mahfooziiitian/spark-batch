package com.mahfooz.dataframe.columns.sum

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sumDistinct
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SumDistinct {

  def main(args: Array[String]): Unit = {

    val sourceDataFile = "D:\\data\\flight-data\\csv\\flight-summary.csv"

    val spark = SparkSession.builder
      .appName(SumDistinct.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(sourceDataFile)

    flight_summary.select(sumDistinct("count")).show

  }
}
