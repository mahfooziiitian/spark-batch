package com.mahfooz.dataframe.columns.count

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count

object CountRecordDataFrame {

  def main(args: Array[String]): Unit = {

    val sourceDataFile = "D:\\data\\flight-data\\csv\\flight-summary.csv"

    val spark = SparkSession.builder
      .appName(CountRecordDataFrame.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(sourceDataFile)

    flight_summary
      .select(count("origin_airport"), count("dest_airport").as("dest_count"))
      .show

    // use count action to find out number of rows in this data set
    println(flight_summary.count())

  }

}
