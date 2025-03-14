
package com.mahfooz.spark.csv.mode

import org.apache.spark.sql.SparkSession

object CsvReaderMode {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val dataHome = sys.env.getOrElse("DATA_HOME", "data")

    val flightDf = spark.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .load(s"${dataHome}/FileData/Csv/flight-summary.csv")

    flightDf.show()
  }
}
