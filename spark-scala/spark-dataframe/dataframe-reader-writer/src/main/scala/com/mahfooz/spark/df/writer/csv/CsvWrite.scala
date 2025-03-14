/*

sep

 */
package com.mahfooz.spark.df.writer.csv

import com.mahfooz.spark.df.schema.FlightTravelSchema
import org.apache.spark.sql.SparkSession

object CsvWrite {

  def main(args: Array[String]): Unit = {

    val sourceFileName="D:\\data\\flight-data\\csv\\2015-summary.csv"

    val spark = SparkSession
      .builder()
      .appName("CsvWrite")
      .master("local")
      .getOrCreate()

    val csvFile = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .schema(FlightTravelSchema.myManualSchema)
      .load(sourceFileName)

    csvFile.write
      .format("csv")
      .mode("overwrite")
      .option("sep", "\t")
      .save("D:\\data\\flight-data\\csv\\2010-summary-out.csv")
  }
}
