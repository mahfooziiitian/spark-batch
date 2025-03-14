package com.mahfooz.dataframe.columns.distinct

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, countDistinct}

object DistinctCount {

  def main(args: Array[String]): Unit = {

    val sourceDataFile = s"${sys.env("DATA_HOME")}/FileData/Csv/Flight/flight-summary.csv"

    val spark = SparkSession.builder
      .appName(DistinctCount.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(sourceDataFile)

    flight_summary
      .select(
        countDistinct("origin_airport"),
        countDistinct("dest_airport"),
        count("*")
      )
      .show
  }
}
