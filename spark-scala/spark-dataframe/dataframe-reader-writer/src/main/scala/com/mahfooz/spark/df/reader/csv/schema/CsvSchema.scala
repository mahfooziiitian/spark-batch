package com.mahfooz.spark.df.reader.csv.schema

import com.mahfooz.spark.df.schema.FlightTravelSchema
import org.apache.spark.sql.SparkSession

object CsvSchema {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .schema(FlightTravelSchema.myManualSchema)
      .load("D:\\data\\flight-data\\csv\\2010-summary.csv")
      .show(5)
  }
}
