/*

sep

header

escape

inferSchema

ignoreLeadingWhiteSpace

ignoreTrailingWhiteSpace

nullValue

nanValue

compression

codec

dateFormat

timestampFormat

maxColumns

multiLine

 */
package com.mahfooz.spark.df.reader.csv

import org.apache.spark.sql.SparkSession

object CSVReaders {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    //Using format method
    spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter",",")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .load(getClass.getResource("/csv/2010-12-01.csv").getFile)
      .show(10)

    //Using csv method
    spark.read
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/csv/2010-12-01.csv").getFile)
      .show(10)
  }
}
