package com.mahfooz.dataframe.columns.distinct

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{
  approx_count_distinct,
  count,
  countDistinct
}

object ApproxDistinctCount {

  def main(args: Array[String]): Unit = {

    val sourceDataFile = s"${sys.env("DATA_HOME")}/FileData/Csv/Flight/flight-summary.csv"

    val spark = SparkSession.builder
      .appName(ApproxDistinctCount.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(sourceDataFile)

    flight_summary
      .select(
        count("count"),
        countDistinct("count"),
        approx_count_distinct("count", 0.05)
      )
      .show

    // trying calling them separately
    flight_summary.select(countDistinct("count")).show

    // specify 1% estimation error
    flight_summary.select(approx_count_distinct("count", 0.01)).show

  }
}
