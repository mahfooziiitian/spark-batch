package com.mahfooz.spark.dataset.transformation.filter

import com.mahfooz.spark.dataset.model.Flight
import org.apache.spark.sql.SparkSession

object Filters {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val flightsDF = spark.read
      .parquet(args(0))

    import spark.implicits._

    val flights = flightsDF.as[Flight]

    println(flights
      .filter(flight_row => originIsDestination(flight_row))
      .first())

  }

  def originIsDestination(flight_row: Flight): Boolean = {
    return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
  }
}
