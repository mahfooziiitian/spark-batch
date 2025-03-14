package com.mahfooz.dataframe.columns.maxmin

import org.apache.spark.sql.{SparkSession, functions}

object MaxMin {

  def main(args: Array[String]): Unit = {

    val sourceDataFile = "D:\\data\\flight-data\\csv\\flight-summary.csv"

    val spark = SparkSession.builder
      .appName(MaxMin.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(sourceDataFile)

    flight_summary.select(functions.min("count"), functions.max("count")).show
  }
}
